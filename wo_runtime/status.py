"""Read-only queue.v2 request and task status projection."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

from wo_runtime.core.validation import validate_identifier
from wo_runtime.plans import PlanPublicationError, load_request_plan, require_complete_plan, task_is_published


_TASK_AREAS = {
    "pending": "queued",
    "claimed": "processing",
    "done": "pending",
    "failed": "failed",
    "invalid": "failed",
}
_STATE_PRIORITY = {"pending": 0, "queued": 1, "processing": 2, "success": 3, "failed": 4}


@dataclass(frozen=True)
class TaskStatus:
    task_id: str
    request_id: str
    capability: str = ""
    stage: str = ""
    state: str = "pending"
    error: str = ""
    attempt: int = 0
    envelope: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "taskId": self.task_id,
            "requestId": self.request_id,
            "capability": self.capability,
            "stage": self.stage,
            "state": self.state,
            "error": self.error,
            "attempt": self.attempt,
        }


@dataclass(frozen=True)
class RequestStatus:
    request_id: str
    state: str
    tasks: tuple[TaskStatus, ...]
    successful: int
    total: int
    root_failure: TaskStatus | None = None

    @property
    def percent(self) -> int:
        return round(self.successful * 100 / self.total) if self.total else 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "requestId": self.request_id,
            "state": self.state,
            "progress": {
                "successful": self.successful,
                "total": self.total,
                "percent": self.percent,
            },
            "tasks": [task.as_dict() for task in self.tasks],
            "rootFailure": self.root_failure.as_dict() if self.root_failure else None,
        }


class RequestStatusReader:
    """Project request state from immutable results and materialized tasks.

    ``expected_tasks`` is an optional compatibility fallback for the short
    interval before a planner has materialized envelopes. Actual queue files
    and terminal results always override fallback state and metadata.
    """

    def __init__(self, data_root: Path | str):
        self.data_root = Path(data_root)

    def project(
        self,
        request_id: str,
        *,
        expected_tasks: Iterable[Mapping[str, Any]] = (),
    ) -> RequestStatus:
        safe_request_id = validate_identifier(request_id, "request.id")
        ordered_ids: list[str] = []
        projected: dict[str, TaskStatus] = {}
        try:
            manifest = load_request_plan(self.data_root, safe_request_id, require_committed=False)
            if manifest is not None:
                expected_tasks = manifest["plan"]["tasks"]
                try:
                    load_request_plan(self.data_root, safe_request_id)
                except PlanPublicationError:
                    waiting = tuple(self._from_envelope(safe_request_id, task, state="queued") for task in expected_tasks)
                    return RequestStatus(safe_request_id, "queued", waiting, 0, len(waiting))
        except PlanPublicationError:
            # A manifest being staged or an inconsistent publication must not
            # project partial terminal files as request success.
            return RequestStatus(safe_request_id, "queued", (), 0, 0)

        materialized = list(self._materialized_tasks(safe_request_id))
        complete_plan = True
        try:
            require_complete_plan(self.data_root, safe_request_id, [task for task, _ in materialized])
        except PlanPublicationError:
            complete_plan = False
        valid_materialized_ids = {
            task.get("id") for task, _ in materialized
            if task_is_published(self.data_root, task)
        }

        for value in expected_tasks:
            task_id = str(value.get("id") or value.get("task_id") or "").strip()
            if not task_id:
                continue
            task_id = validate_identifier(task_id, "task.id")
            if task_id in projected:
                continue
            envelope = dict(value)
            projected[task_id] = self._from_envelope(
                safe_request_id,
                envelope,
                state="pending",
            )
            ordered_ids.append(task_id)

        for envelope, state in materialized:
            if not task_is_published(self.data_root, envelope):
                continue
            task_id = validate_identifier(envelope.get("id"), "task.id")
            candidate = self._from_envelope(safe_request_id, envelope, state=state)
            current = projected.get(task_id)
            if current is None:
                ordered_ids.append(task_id)
                projected[task_id] = candidate
            elif _STATE_PRIORITY[candidate.state] >= _STATE_PRIORITY[current.state]:
                projected[task_id] = candidate

        for task_id, result in self._terminal_results(
            safe_request_id,
            known_task_ids=set(projected),
        ):
            if manifest is not None and (task_id not in projected or task_id not in valid_materialized_ids):
                continue
            if not complete_plan and manifest is None:
                continue
            current = projected.get(task_id)
            envelope = dict(current.envelope) if current else {"id": task_id}
            state = "success" if result.get("status") == "success" else "failed"
            candidate = self._from_envelope(
                safe_request_id,
                envelope,
                state=state,
                error=str(result.get("error") or ""),
                attempt=_nonnegative_int(result.get("attempt")),
            )
            if current is None:
                ordered_ids.append(task_id)
            projected[task_id] = candidate

        tasks = tuple(projected[task_id] for task_id in ordered_ids)
        successful = sum(task.state == "success" for task in tasks)
        failed = [task for task in tasks if task.state == "failed"]
        root_failure = next(
            (task for task in failed if not task.error.startswith("dep_failed:")),
            failed[0] if failed else None,
        )
        if failed:
            state = "failed"
        elif tasks and successful == len(tasks) and complete_plan:
            state = "success"
        elif any(task.state == "processing" for task in tasks):
            state = "processing"
        elif tasks:
            state = "queued"
        elif self.exists(safe_request_id):
            state = "queued"
        else:
            state = "not_found"
        return RequestStatus(
            request_id=safe_request_id,
            state=state,
            tasks=tasks,
            successful=successful,
            total=len(tasks),
            root_failure=root_failure,
        )

    def exists(self, request_id: str) -> bool:
        safe_request_id = validate_identifier(request_id, "request.id")
        if (self.data_root / "plans" / safe_request_id).exists():
            return True
        if (self.data_root / "inbox" / f"{safe_request_id}.json").is_file():
            return True
        if (self.data_root / "inbox" / "processed" / f"{safe_request_id}.json").is_file():
            return True
        results = self.data_root / "results"
        if results.is_dir():
            if (results / f"{safe_request_id}.final.json").is_file():
                return True
            for path in results.glob(f"{safe_request_id}.*.json"):
                payload = _read_object(path)
                task_id = str(payload.get("task_id") or path.stem)
                if _belongs_to_request(task_id, safe_request_id):
                    return True
        tasks_root = self.data_root / "tasks"
        if not tasks_root.is_dir():
            return False
        for directory in self._task_directories(tasks_root):
            for path in directory.glob("*.json"):
                if _read_object(path).get("req_id") == safe_request_id:
                    return True
        return False

    def _materialized_tasks(self, request_id: str) -> Iterable[tuple[dict[str, Any], str]]:
        tasks_root = self.data_root / "tasks"
        if not tasks_root.is_dir():
            return ()
        values: list[tuple[dict[str, Any], str]] = []
        for directory in self._task_directories(tasks_root):
            state = "queued" if directory.name.startswith("ready-") else _TASK_AREAS[directory.name]
            for path in sorted(directory.glob("*.json")):
                envelope = _read_object(path)
                if envelope.get("req_id") == request_id:
                    values.append((envelope, state))
        return values

    def _terminal_results(
        self,
        request_id: str,
        *,
        known_task_ids: set[str],
    ) -> Iterable[tuple[str, dict[str, Any]]]:
        results_root = self.data_root / "results"
        if not results_root.is_dir():
            return ()
        values: list[tuple[str, dict[str, Any]]] = []
        paths = {
            results_root / f"{task_id}.json"
            for task_id in known_task_ids
        }
        paths.update(results_root.glob(f"{request_id}*.json"))
        for path in sorted(paths):
            if not path.is_file():
                continue
            if path.name.endswith(".final.json"):
                continue
            result = _read_object(path)
            if result.get("status") not in {"success", "failed"}:
                continue
            task_id = str(result.get("task_id") or path.stem)
            if (
                task_id == request_id
                or (
                    task_id not in known_task_ids
                    and not _belongs_to_request(task_id, request_id)
                )
            ):
                continue
            try:
                task_id = validate_identifier(task_id, "task.id")
            except ValueError:
                continue
            values.append((task_id, result))
        return values

    @staticmethod
    def _task_directories(tasks_root: Path) -> list[Path]:
        directories = [tasks_root / name for name in _TASK_AREAS]
        directories.extend(sorted(tasks_root.glob("ready-*")))
        return [path for path in directories if path.is_dir()]

    @staticmethod
    def _from_envelope(
        request_id: str,
        envelope: Mapping[str, Any],
        *,
        state: str,
        error: str = "",
        attempt: int | None = None,
    ) -> TaskStatus:
        task_id = validate_identifier(
            envelope.get("id") or envelope.get("task_id"),
            "task.id",
        )
        return TaskStatus(
            task_id=task_id,
            request_id=request_id,
            capability=str(envelope.get("capability") or ""),
            stage=str(envelope.get("stage") or envelope.get("capability") or ""),
            state=state,
            error=error,
            attempt=(
                _nonnegative_int(envelope.get("attempt"))
                if attempt is None
                else attempt
            ),
            envelope=dict(envelope),
        )


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return value if isinstance(value, dict) else {}


def _belongs_to_request(task_id: str, request_id: str) -> bool:
    return task_id.startswith(f"{request_id}.")


def _nonnegative_int(value: Any) -> int:
    try:
        number = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return max(0, number)
