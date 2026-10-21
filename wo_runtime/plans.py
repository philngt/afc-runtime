"""Request-scoped plan publication and queue visibility.

A manifest is prepared before task files are materialized. Only the final,
immutable commit marker makes those tasks executable. This is a publication
boundary for the compiled graph, not a snapshot of instance code or contracts.
"""

from __future__ import annotations

import hashlib
import json
from contextlib import ExitStack
from copy import deepcopy
from pathlib import Path
from typing import Any

from wo_runtime.artifacts import ArtifactConflictError, canonical_json_bytes, idempotent_write_json
from wo_runtime.core.pipeline import validate_tasks
from wo_runtime.core.validation import QueueContractError, safe_file, validate_identifier, validate_request_envelope
from wo_runtime.queue.locking import task_lock
from wo_runtime.workspace import Workspace


PLAN_SCHEMA = "request_plan.v1"
_TASK_AREAS = ("pending", "claimed", "done", "failed", "invalid")
_BINDING_FIELDS = ("id", "req_id", "capability", "stage", "depends_on", "inputs", "outputContract")
_FLAG_FIELDS = ("is_root", "manualOnly")


class PlanPublicationError(QueueContractError):
    """A managed request is not committed or its publication is inconsistent."""


class PlanConflictError(PlanPublicationError):
    """A request ID or task destination already belongs to different work."""


def _plan_dir(data_root: Path, req_id: str) -> Path:
    return safe_file(Path(data_root) / "plans", req_id, suffix="", field="request.id")


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PlanPublicationError(f"plan_unreadable:{path.name}") from exc
    if not isinstance(value, dict):
        raise PlanPublicationError(f"plan_invalid_object:{path.name}")
    return value


def _digest(document: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(document)).hexdigest()


def load_request_plan(
    data_root: Path, req_id: str, *, require_committed: bool = True,
) -> dict[str, Any] | None:
    """Return a verified manifest, or None only for an unmanaged request."""

    directory = _plan_dir(data_root, req_id)
    if not directory.exists():
        return None
    manifest = _read_object(directory / "manifest.json")
    body = {key: value for key, value in manifest.items() if key != "digest"}
    if (
        manifest.get("schema") != PLAN_SCHEMA
        or manifest.get("req_id") != req_id
        or manifest.get("digest") != _digest(body)
    ):
        raise PlanPublicationError(f"plan_manifest_mismatch:{req_id}")
    plan = manifest.get("plan")
    if not isinstance(plan, dict) or not isinstance(plan.get("tasks"), list):
        raise PlanPublicationError(f"plan_tasks_invalid:{req_id}")
    tasks = plan["tasks"]
    if not tasks or any(not isinstance(task, dict) or task.get("req_id") != req_id for task in tasks):
        raise PlanPublicationError(f"plan_tasks_invalid:{req_id}")
    try:
        ids = [validate_identifier(task.get("id"), "task.id") for task in tasks]
    except QueueContractError as exc:
        raise PlanPublicationError(f"plan_tasks_invalid:{req_id}") from exc
    if len(set(ids)) != len(ids):
        raise PlanPublicationError(f"plan_duplicate_tasks:{req_id}")
    if require_committed:
        marker = directory / "committed.json"
        if not marker.exists():
            raise PlanPublicationError(f"plan_uncommitted:{req_id}")
        if _read_object(marker) != {
            "schema": PLAN_SCHEMA, "req_id": req_id, "digest": manifest["digest"],
        }:
            raise PlanPublicationError(f"plan_commit_mismatch:{req_id}")
    return manifest


def _require_task_binding(manifest: dict[str, Any], task: dict[str, Any]) -> None:
    task_id = validate_identifier(task.get("id"), "task.id")
    if task.get("plan_digest") != manifest["digest"]:
        raise PlanPublicationError(f"plan_binding_mismatch:{task_id}")
    expected = next((item for item in manifest["plan"]["tasks"] if item["id"] == task_id), None)
    if expected is None:
        raise PlanPublicationError(f"plan_task_not_declared:{task_id}")
    flags_changed = any(
        (task.get(key) is not None and not isinstance(task[key], bool))
        or (expected.get(key) is not None and not isinstance(expected[key], bool))
        or bool(task.get(key)) != bool(expected.get(key))
        for key in _FLAG_FIELDS
    )
    if flags_changed or any(task.get(key) != expected.get(key) for key in _BINDING_FIELDS):
        raise PlanPublicationError(f"plan_task_changed:{task_id}")


def require_published_task(data_root: Path, task: dict[str, Any]) -> None:
    """Guard both schedulers and authoritative queue mutation boundaries."""

    req_id = validate_identifier(task.get("req_id"), "request.id")
    manifest = load_request_plan(data_root, req_id)
    if manifest is None:
        if "plan_digest" in task:
            raise PlanPublicationError(f"plan_missing:{req_id}")
        return
    _require_task_binding(manifest, task)


def task_is_published(data_root: Path, task: dict[str, Any]) -> bool:
    try:
        require_published_task(data_root, task)
        return True
    except (QueueContractError, OSError, TypeError):
        return False


def queue_trigger_is_published(path: Path) -> bool:
    """Filter queue watches without changing inbox or other trigger semantics.

    A queue file's ancestors supply its configured data root, including roots
    outside the instance checkout. No default instance path is assumed.
    """

    area = path.parent
    if area.parent.name != "tasks" or not (area.name in _TASK_AREAS or area.name.startswith("ready-")):
        return True
    try:
        task = _read_object(path)
    except PlanPublicationError:
        return True  # Existing malformed-task handling remains responsible.
    if "plan_digest" not in task:
        try:
            validate_identifier(task.get("req_id"), "request.id")
        except QueueContractError:
            return True  # Unmanaged trigger validation belongs to the queue.
    return task_is_published(area.parent.parent, task)


def require_complete_plan(data_root: Path, req_id: str, tasks: list[dict[str, Any]]) -> None:
    """Prevent a missing or duplicated sibling from producing a partial final."""

    manifest = load_request_plan(data_root, req_id)
    if manifest is None:
        for task in tasks:
            require_published_task(data_root, task)
        return
    expected = {task["id"] for task in manifest["plan"]["tasks"]}
    actual = [task.get("id") for task in tasks]
    if len(actual) != len(expected) or set(actual) != expected:
        raise PlanPublicationError(f"plan_task_set_mismatch:{req_id}")
    for task in tasks:
        _require_task_binding(manifest, task)


def _task_directories(data_root: Path) -> list[Path]:
    root = data_root / "tasks"
    return [root / name for name in _TASK_AREAS] + sorted(root.glob("ready-*"))


def request_has_state(data_root: Path, req_id: str) -> bool:
    """Protect existing requests from an unrelated inbox planning failure."""

    if _plan_dir(data_root, req_id).exists():
        return True
    if safe_file(data_root / "results", req_id, suffix=".final.json", field="request.id").exists():
        return True
    for directory in _task_directories(data_root):
        for path in directory.glob("*.json"):
            try:
                if _read_object(path).get("req_id") == req_id:
                    return True
            except PlanPublicationError:
                continue
    return False


def publish_plan(workspace: Workspace, request: dict[str, Any], plan: dict[str, Any]) -> bool:
    """Publish once or resume an interrupted publication; False means replay.

    The request lock serializes publishers. Task locks protect the collision
    check against concurrent claims/terminal writes. Prepared task files remain
    invisible to execution until the commit marker is published last.
    """

    validate_request_envelope(request)
    req_id = validate_identifier(request.get("id") or request.get("req_id"), "request.id")
    tasks = plan.get("tasks")
    if not isinstance(tasks, list):
        raise PlanPublicationError("plan_tasks_invalid")
    validate_tasks(tasks)
    for task in tasks:
        if task.get("req_id") != req_id or "plan_digest" in task or "claim" in task:
            raise PlanPublicationError(f"plan_task_invalid:{task.get('id')}")
        if any(task.get(key) is not None and not isinstance(task[key], bool) for key in _FLAG_FIELDS):
            raise PlanPublicationError(f"plan_task_flags_invalid:{task['id']}")
    if "request" in plan and plan["request"] != request:
        raise PlanPublicationError(f"plan_request_mismatch:{req_id}")
    body = {"schema": PLAN_SCHEMA, "req_id": req_id, "request": deepcopy(request), "plan": deepcopy(plan)}
    manifest = {**body, "digest": _digest(body)}
    data_root = workspace.data_root
    directory = _plan_dir(data_root, req_id)
    # Separate lock namespace; a live publisher's lock must not expire by age.
    with task_lock(data_root / "plans", req_id, purpose="plan-publish", stale_seconds=float("inf")):
        # A crash while staging the first manifest can leave only a directory
        # and temporary file. It is still hidden from readers, and may resume
        # after the same collision preflight as a new publication.
        existing = (
            load_request_plan(data_root, req_id, require_committed=False)
            if (directory / "manifest.json").exists() or (directory / "committed.json").exists()
            else None
        )
        if existing is not None and existing != manifest:
            raise PlanConflictError(f"plan_conflict:{req_id}")
        if (directory / "committed.json").exists():
            load_request_plan(data_root, req_id)
            return False
        task_ids = {task["id"] for task in tasks}
        bound = {task["id"]: {**task, "plan_digest": manifest["digest"]} for task in tasks}
        pending = data_root / "tasks" / "pending"
        with ExitStack() as locks:
            for task_id in sorted(task_ids):
                locks.enter_context(task_lock(data_root, task_id, purpose="plan-materialize", stale_seconds=float("inf")))
            for area in _task_directories(data_root):
                for path in area.glob("*.json"):
                    try:
                        queued = _read_object(path)
                    except PlanPublicationError:
                        if path.stem in task_ids:
                            raise PlanConflictError(f"plan_task_collision:{path.stem}")
                        continue
                    if queued.get("req_id") != req_id and path.stem not in task_ids:
                        continue
                    if not (
                        existing is not None and area == pending
                        and path.stem in bound and queued == bound[path.stem]
                    ):
                        raise PlanConflictError(f"plan_task_collision:{path.stem}")
            final = safe_file(data_root / "results", req_id, suffix=".final.json", field="request.id")
            if final.exists() or any(safe_file(data_root / "results", task_id, field="task.id").exists() for task_id in task_ids):
                raise PlanConflictError(f"plan_result_collision:{req_id}")
            try:
                idempotent_write_json(directory / "manifest.json", manifest)
                for task in tasks:
                    idempotent_write_json(safe_file(pending, task["id"], field="task.id"), bound[task["id"]])
                idempotent_write_json(directory / "committed.json", {
                    "schema": PLAN_SCHEMA, "req_id": req_id, "digest": manifest["digest"],
                })
            except ArtifactConflictError as exc:
                raise PlanConflictError(f"plan_artifact_conflict:{req_id}") from exc
    return True
