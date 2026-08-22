"""Reusable receipt-bound worker primitives for queue.v2 instances."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from wo_runtime.core.context import InputResolutionError, resolve_inputs
from wo_runtime.core.validation import QUEUE_CONTRACT_VERSION, validate_task_envelope
from wo_runtime.queue.commands import (
    ClaimReceipt,
    complete_claim,
    fail_claim,
    requeue_claim,
)
from wo_runtime.workspace import (
    Workspace,
    ensure_data_root,
    load_and_prepare,
    load_workspace,
)


class WorkerError(RuntimeError):
    """A receipt-bound worker operation cannot be completed safely."""


@dataclass(frozen=True)
class BoundClaim:
    task_id: str
    worker: str
    lease_id: str
    attempt: int
    path: Path
    lease_expires_at: str

    @classmethod
    def from_environment(
        cls,
        workspace: Workspace | Path | str | Any,
        *,
        data_root: Path | str | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> "BoundClaim | None":
        root, resolved_data_root = _workspace_roots(workspace, data_root=data_root)
        values = environ if environ is not None else os.environ
        raw = values.get("WO_RUNTIME_CLAIM_JSON", "").strip()
        bound_values = {
            "task_id": values.get("WO_RUNTIME_TASK_ID", "").strip(),
            "lease_id": values.get("WO_RUNTIME_CLAIM_LEASE", "").strip(),
            "attempt": values.get("WO_RUNTIME_CLAIM_ATTEMPT", "").strip(),
            "path": values.get("WO_RUNTIME_CLAIM_PATH", "").strip(),
            "lease_expires_at": values.get("WO_RUNTIME_CLAIM_EXPIRES_AT", "").strip(),
        }
        if not raw:
            if any(bound_values.values()):
                raise WorkerError("partial runtime claim binding without WO_RUNTIME_CLAIM_JSON")
            return None
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise WorkerError("invalid WO_RUNTIME_CLAIM_JSON") from exc
        if not isinstance(value, dict):
            raise WorkerError("runtime claim receipt must be an object")
        queue_contract = str(value.get("queue_contract") or "")
        if queue_contract != QUEUE_CONTRACT_VERSION:
            raise WorkerError("runtime claim receipt is not queue.v2")
        if value.get("status") not in {"claimed", "already_claimed"}:
            raise WorkerError("runtime claim receipt is not an active claim")
        try:
            attempt = int(value["attempt"])
        except (KeyError, TypeError, ValueError) as exc:
            raise WorkerError("runtime claim receipt has no valid attempt") from exc
        task_id = str(value.get("task_id") or "").strip()
        worker = str(value.get("worker") or "").strip()
        lease_id = str(value.get("lease_id") or "").strip()
        raw_path = str(value.get("path") or "").strip()
        lease_expires_at = str(value.get("lease_expires_at") or "")
        if (
            not task_id
            or not worker
            or not lease_id
            or not raw_path
            or not lease_expires_at
            or attempt < 0
        ):
            raise WorkerError("runtime claim receipt is incomplete")
        if bound_values["task_id"] and bound_values["task_id"] != task_id:
            raise WorkerError("runtime task binding disagrees with claim receipt")
        if bound_values["lease_id"] and bound_values["lease_id"] != lease_id:
            raise WorkerError("runtime lease binding disagrees with claim receipt")
        if bound_values["attempt"] and bound_values["attempt"] != str(attempt):
            raise WorkerError("runtime attempt binding disagrees with claim receipt")
        if (
            bound_values["lease_expires_at"]
            and bound_values["lease_expires_at"] != lease_expires_at
        ):
            raise WorkerError("runtime lease expiry binding disagrees with claim receipt")
        receipt_path = Path(raw_path)
        if not receipt_path.is_absolute():
            receipt_path = root / receipt_path
        receipt_path = receipt_path.resolve()
        claimed_root = (resolved_data_root / "tasks" / "claimed").resolve()
        if receipt_path.parent != claimed_root or receipt_path.name != f"{task_id}.json":
            raise WorkerError("claim receipt path is outside the claimed-task directory")
        if bound_values["path"]:
            bound_path = Path(bound_values["path"])
            if not bound_path.is_absolute():
                bound_path = root / bound_path
            if bound_path.resolve() != receipt_path:
                raise WorkerError("runtime path binding disagrees with claim receipt")
        return cls(
            task_id=task_id,
            worker=worker,
            lease_id=lease_id,
            attempt=attempt,
            path=receipt_path,
            lease_expires_at=lease_expires_at,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "worker": self.worker,
            "lease_id": self.lease_id,
            "attempt": self.attempt,
            "path": str(self.path),
            "lease_expires_at": self.lease_expires_at,
        }

    def queue_receipt(self) -> ClaimReceipt:
        return ClaimReceipt(
            status="claimed",
            path=str(self.path),
            task_id=self.task_id,
            worker=self.worker,
            lease_id=self.lease_id,
            attempt=self.attempt,
            lease_expires_at=self.lease_expires_at,
        )


class RuntimeTerminal:
    """Programmatic fenced terminal operations for a bound claim."""

    def __init__(
        self,
        workspace: Workspace | Path | str | Any,
        claim: BoundClaim,
        *,
        workspace_config: str = "runtime/workspace.json",
    ) -> None:
        root, resolved_data_root = _workspace_roots(workspace)
        if isinstance(workspace, Workspace):
            self.workspace = Workspace(
                root=root,
                config_path=workspace.config_path,
                config=workspace.config,
                data_root_raw=workspace.data_root_raw,
                data_root=resolved_data_root,
            )
        elif isinstance(workspace, (str, os.PathLike)):
            previous_cwd = Path.cwd()
            try:
                loaded = load_and_prepare(root, workspace_config)
            finally:
                os.chdir(previous_cwd)
            data_root = (
                loaded.data_root
                if loaded.data_root.is_absolute()
                else loaded.root / loaded.data_root
            )
            self.workspace = Workspace(
                root=loaded.root,
                config_path=loaded.config_path,
                config=loaded.config,
                data_root_raw=loaded.data_root_raw,
                data_root=data_root.resolve(),
            )
        else:
            loaded = load_workspace(root, workspace_config)
            self.workspace = Workspace(
                root=root,
                config_path=loaded.config_path,
                config=loaded.config,
                data_root_raw=str(resolved_data_root),
                data_root=resolved_data_root,
            )
            previous_cwd = Path.cwd()
            try:
                ensure_data_root(self.workspace)
            finally:
                os.chdir(previous_cwd)
        self.claim = claim

    def complete(self, result: Any) -> bool:
        try:
            return complete_claim(self.workspace, self.claim.queue_receipt(), result)
        except Exception as exc:
            raise WorkerError("runtime rejected the fenced completion") from exc

    def fail(
        self,
        error: str,
        *,
        recovery: Any | None = None,
        artifact_type: str = "",
    ) -> bool:
        try:
            return fail_claim(
                self.workspace,
                self.claim.queue_receipt(),
                str(error)[:2000],
                recovery=recovery,
                artifact_type=artifact_type,
            )
        except Exception as exc:
            raise WorkerError("runtime rejected the fenced failure") from exc

    def requeue(self, reason: str, *, increment_attempt: bool = False) -> None:
        try:
            accepted = requeue_claim(
                self.workspace,
                self.claim.queue_receipt(),
                reason=str(reason)[:500],
                increment_attempt=increment_attempt,
            )
        except Exception as exc:
            raise WorkerError("runtime rejected the fenced requeue") from exc
        if not accepted:
            raise WorkerError("runtime rejected the fenced requeue")


def load_bound_task(claim: BoundClaim) -> dict[str, Any]:
    try:
        value = json.loads(claim.path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkerError("could not load claimed task") from exc
    if not isinstance(value, dict) or value.get("id") != claim.task_id:
        raise WorkerError("claimed task does not match the receipt")
    try:
        validate_task_envelope(value)
    except Exception as exc:
        raise WorkerError("claimed task violates queue.v2") from exc
    meta = value.get("claim") if isinstance(value.get("claim"), dict) else {}
    try:
        meta_attempt = int(meta.get("attempt", -1))
        task_attempt = int(value.get("attempt", -1))
    except (TypeError, ValueError) as exc:
        raise WorkerError("claimed task fencing metadata has an invalid attempt") from exc
    if (
        str(meta.get("lease_id") or "") != claim.lease_id
        or str(meta.get("worker") or "") != claim.worker
        or meta_attempt != claim.attempt
        or task_attempt != claim.attempt
    ):
        raise WorkerError("claimed task fencing metadata does not match the receipt")
    return value


def resolve_task_inputs(
    workspace: Workspace | Path | str | Any,
    task: Mapping[str, Any],
    *,
    data_root: Path | str | None = None,
) -> dict[str, Any]:
    root, resolved_data_root = _workspace_roots(workspace, data_root=data_root)
    if isinstance(workspace, Workspace):
        runtime_workspace = Workspace(
            root=root,
            config_path=workspace.config_path,
            config=workspace.config,
            data_root_raw=workspace.data_root_raw,
            data_root=resolved_data_root,
        )
    else:
        loaded = load_workspace(root)
        runtime_workspace = Workspace(
            root=root,
            config_path=loaded.config_path,
            config=loaded.config,
            data_root_raw=str(resolved_data_root),
            data_root=resolved_data_root,
        )
    try:
        resolved, _warnings = resolve_inputs(
            dict(task),
            workspace=runtime_workspace,
            strict=True,
        )
    except InputResolutionError as exc:
        raise WorkerError(str(exc)) from exc
    return resolved


def _workspace_roots(
    workspace: Workspace | Path | str | Any,
    *,
    data_root: Path | str | None = None,
) -> tuple[Path, Path]:
    if isinstance(workspace, Workspace):
        root = workspace.root.resolve()
        raw_data = Path(data_root) if data_root is not None else workspace.data_root
    elif isinstance(workspace, (str, os.PathLike)):
        loaded = load_workspace(workspace)
        root = loaded.root.resolve()
        raw_data = Path(data_root) if data_root is not None else loaded.data_root
    elif hasattr(workspace, "root"):
        root = Path(workspace.root).resolve()
        if data_root is not None:
            raw_data = Path(data_root)
        elif hasattr(workspace, "data"):
            raw_data = Path(workspace.data)
        else:
            loaded = load_workspace(root)
            root = loaded.root.resolve()
            raw_data = loaded.data_root
    else:
        raise TypeError("workspace must be a Workspace, path, or workspace path carrier")
    resolved_data = raw_data if raw_data.is_absolute() else root / raw_data
    return root, resolved_data.resolve()
