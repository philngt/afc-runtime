from __future__ import annotations

import importlib
import json
import math
import os
import re
import sys
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from wo_runtime.core.context import build_context
from wo_runtime.core.contracts import load_contract
from wo_runtime.core.schema import validate_json_schema
from wo_runtime.core.validation import (
    QUEUE_CONTRACT_VERSION,
    QueueContractError,
    result_digest,
    safe_file,
    validate_capability,
    validate_identifier,
    validate_task_envelope,
)
from wo_runtime.queue.locking import QueueBusyError, task_lock
from wo_runtime.workspace import Workspace, load_and_prepare


BOUND_TASK_ENV = "WO_RUNTIME_TASK_ID"
BOUND_LEASE_ENV = "WO_RUNTIME_CLAIM_LEASE"
BOUND_ATTEMPT_ENV = "WO_RUNTIME_CLAIM_ATTEMPT"
BOUND_PATH_ENV = "WO_RUNTIME_CLAIM_PATH"
BOUND_EXPIRES_ENV = "WO_RUNTIME_CLAIM_EXPIRES_AT"


class QueueStateError(RuntimeError):
    pass


@dataclass(frozen=True)
class ClaimReceipt:
    status: str
    path: str = ""
    task_id: str = ""
    capability: str = ""
    worker: str = ""
    lease_id: str = ""
    attempt: int = 0
    claimed_at: str = ""
    lease_expires_at: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "queue_contract": QUEUE_CONTRACT_VERSION,
            "status": self.status,
            "path": self.path,
            "task_id": self.task_id,
            "capability": self.capability,
            "worker": self.worker,
            "lease_id": self.lease_id,
            "attempt": self.attempt,
            "claimed_at": self.claimed_at,
            "lease_expires_at": self.lease_expires_at,
        }


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _future_ts(seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=max(1.0, seconds))).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_ts(value: Any) -> datetime | None:
    try:
        return datetime.strptime(str(value or ""), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def _require_active_lease(meta: dict[str, Any], task_id: str) -> None:
    raw = str(meta.get("lease_expires_at") or "")
    expires = _parse_ts(raw)
    if expires is None:
        raise QueueStateError(f"claim_lease_expiry_invalid:{task_id}")
    if datetime.now(timezone.utc) >= expires:
        raise QueueStateError(f"claim_lease_expired:{task_id}")


def _workspace(args: Any) -> Workspace:
    return load_and_prepare(
        getattr(args, "workspace_root", None),
        getattr(args, "workspace_config", None),
    )


def _event(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise QueueStateError(f"invalid_json:{path}:{type(exc).__name__}:{exc}") from exc
    if not isinstance(payload, dict):
        raise QueueStateError(f"invalid_json_object:{path}")
    return payload


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        tmp.write_text(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        tmp.replace(path)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def _claim_meta(task: dict[str, Any]) -> dict[str, Any]:
    value = task.get("claim")
    return value if isinstance(value, dict) else {}


def _receipt_from_task(path: Path, task: dict[str, Any], *, status: str = "claimed") -> ClaimReceipt:
    claim = _claim_meta(task)
    return ClaimReceipt(
        status=status,
        path=str(path),
        task_id=str(task.get("id") or path.stem),
        capability=str(task.get("capability") or ""),
        worker=str(claim.get("worker") or ""),
        lease_id=str(claim.get("lease_id") or ""),
        attempt=int(claim.get("attempt", task.get("attempt", 0)) or 0),
        claimed_at=str(claim.get("claimed_at") or ""),
        lease_expires_at=str(claim.get("lease_expires_at") or ""),
    )


def _bound_value(explicit: Any, env_name: str) -> str:
    return str(explicit or os.environ.get(env_name) or "").strip()


def _bound_attempt(explicit: Any) -> int | None:
    raw = explicit if explicit not in (None, "") else os.environ.get(BOUND_ATTEMPT_ENV)
    if raw in (None, ""):
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise QueueStateError(f"invalid_claim_attempt:{raw!r}") from exc
    if value < 0:
        raise QueueStateError(f"invalid_claim_attempt:{value}")
    return value


def _validate_worker(worker_id: Any) -> str:
    worker = str(worker_id or "").strip()
    if not worker or len(worker) > 160 or any(ord(ch) < 32 for ch in worker):
        raise QueueStateError("invalid_worker_id")
    return worker


def _validate_task_result_hook(
    workspace: Workspace,
    task: dict[str, Any],
    result: Any,
    status: str,
) -> None:
    if status != "success":
        return
    hooks = workspace.config.get("instanceHooks") if isinstance(workspace.config.get("instanceHooks"), dict) else {}
    module_name = str(hooks.get("queue") or "").strip()
    if not module_name:
        return
    if str(workspace.root) not in sys.path:
        sys.path.insert(0, str(workspace.root))
    try:
        module = importlib.import_module(module_name)
        fn = getattr(module, "validate_task_result", None)
        if fn is not None:
            # Validation hooks are observers, not payload transforms. Give the
            # hook copies so an accidental mutation cannot make the committed
            # payload diverge from its precomputed result_digest.
            fn(deepcopy(task), deepcopy(result), status, workspace.data_root, workspace.root)
    except Exception as exc:
        raise QueueStateError(f"result_validation_failed:{type(exc).__name__}:{exc}") from exc


def _validate_declared_output_contract(
    workspace: Workspace,
    task: dict[str, Any],
    result: Any,
    status: str,
) -> None:
    """Apply a task's declared output contract before instance invariants."""

    if status != "success":
        return
    contract_id = str(task.get("outputContract") or "").strip()
    if not contract_id:
        return
    try:
        schema = load_contract(contract_id, task, workspace=workspace)
        validate_json_schema(result, schema)
    except Exception as exc:
        raise QueueStateError(
            f"output_contract_validation_failed:{contract_id}:{type(exc).__name__}:{exc}"
        ) from exc


def claim_task(
    workspace: Workspace,
    capability: str,
    worker_id: str,
    *,
    task_id: str | None = None,
    lease_id: str | None = None,
    lease_ttl_seconds: float | None = None,
) -> ClaimReceipt:
    capability = validate_capability(capability)
    worker_id = _validate_worker(worker_id)
    data_root = workspace.data_root
    ready = data_root / "tasks" / f"ready-{capability}"
    claimed = data_root / "tasks" / "claimed"
    claimed.mkdir(parents=True, exist_ok=True)
    (data_root / "events").mkdir(parents=True, exist_ok=True)
    try:
        ttl = (
            float(lease_ttl_seconds)
            if lease_ttl_seconds is not None
            else float(os.environ.get("QUEUE_LEASE_TTL_SECONDS", "3660"))
        )
    except (TypeError, ValueError) as exc:
        raise QueueStateError(f"invalid_lease_ttl:{lease_ttl_seconds!r}") from exc
    if not math.isfinite(ttl) or ttl <= 0:
        raise QueueStateError(f"invalid_lease_ttl:{ttl}")
    try:
        max_ttl = float(os.environ.get("QUEUE_MAX_LEASE_TTL_SECONDS", "86400"))
    except (TypeError, ValueError) as exc:
        raise QueueStateError("invalid_max_lease_ttl") from exc
    if not math.isfinite(max_ttl) or max_ttl <= 0:
        raise QueueStateError(f"invalid_max_lease_ttl:{max_ttl}")
    if ttl > max_ttl:
        raise QueueStateError(f"lease_ttl_exceeds_max:{ttl:g}>{max_ttl:g}")

    requested_lease = (
        validate_identifier(lease_id, "claim.lease_id", max_length=96)
        if lease_id
        else ""
    )

    bound_task_id = validate_identifier(task_id, "task.id") if task_id else ""
    if bound_task_id:
        existing_path = safe_file(claimed, bound_task_id, field="task.id")
        if existing_path.exists():
            existing = _read_json(existing_path)
            meta = _claim_meta(existing)
            expected_lease = requested_lease
            if (
                expected_lease
                and str(meta.get("lease_id") or "") == expected_lease
                and str(meta.get("worker") or "") == worker_id
                and str(existing.get("capability") or "") == capability
            ):
                _require_active_lease(meta, bound_task_id)
                return _receipt_from_task(existing_path, existing, status="already_claimed")
            raise QueueStateError(f"task_already_claimed:{bound_task_id}")
        candidates = [safe_file(ready, bound_task_id, field="task.id")]
    else:
        candidates = sorted(ready.glob("*.json")) if ready.is_dir() else []

    for path in candidates:
        if not path.exists() or not path.is_file() or path.name.startswith("."):
            continue
        parsed = _read_json(path)
        validate_task_envelope(parsed)
        parsed_task_id = validate_identifier(parsed.get("id"), "task.id")
        if parsed.get("manualOnly") is True:
            if bound_task_id:
                raise QueueStateError(f"manual_only_task:{parsed_task_id}")
            continue
        if path.name != f"{parsed_task_id}.json":
            raise QueueStateError(f"task_filename_mismatch:{path.name}:{parsed_task_id}")
        if str(parsed.get("capability") or "") != capability:
            raise QueueStateError(f"task_capability_mismatch:{parsed_task_id}:{capability}")
        with task_lock(data_root, parsed_task_id, purpose="claim"):
            if not path.exists():
                continue
            target = safe_file(claimed, parsed_task_id, field="task.id")
            try:
                path.replace(target)
            except OSError:
                continue
            attempt = int(parsed.get("attempt", 0) or 0)
            claimed_at = _ts()
            lease_expires_at = _future_ts(ttl)
            actual_lease = requested_lease or uuid.uuid4().hex
            parsed["attempt"] = attempt
            parsed["claim"] = {
                "lease_id": actual_lease,
                "worker": worker_id,
                "attempt": attempt,
                "claimed_at": claimed_at,
                "lease_expires_at": lease_expires_at,
                "capability": capability,
            }
            try:
                _write_json_atomic(target, parsed)
                _event(
                    data_root / "events" / "log.jsonl",
                    {
                        "event": "claim",
                        "task": parsed_task_id,
                        "worker": worker_id,
                        "lease_id": actual_lease,
                        "attempt": attempt,
                        "lease_expires_at": lease_expires_at,
                        "ts": claimed_at,
                    },
                )
            except Exception:
                parsed.pop("claim", None)
                try:
                    _write_json_atomic(target, parsed)
                    target.replace(path)
                except OSError:
                    pass
                raise
            return _receipt_from_task(target, parsed)
    return ClaimReceipt(status="idle", capability=capability, worker=worker_id)


def requeue_claim(
    workspace: Workspace,
    receipt: ClaimReceipt,
    *,
    reason: str,
    increment_attempt: bool = True,
) -> bool:
    if not receipt.task_id or not receipt.lease_id:
        return False
    data_root = workspace.data_root
    claimed_path = safe_file(data_root / "tasks" / "claimed", receipt.task_id, field="task.id")
    with task_lock(data_root, receipt.task_id, purpose="requeue"):
        if not claimed_path.exists():
            return False
        task = _read_json(claimed_path)
        meta = _claim_meta(task)
        if str(meta.get("lease_id") or "") != receipt.lease_id:
            return False
        if str(meta.get("worker") or "") != receipt.worker:
            return False
        if int(meta.get("attempt", -1)) != receipt.attempt or int(task.get("attempt", -1)) != receipt.attempt:
            return False
        result_path = safe_file(data_root / "results", receipt.task_id, field="task.id")
        if result_path.exists():
            return False
        attempt = int(task.get("attempt", 0) or 0) + (1 if increment_attempt else 0)
        task["attempt"] = attempt
        task.pop("claim", None)
        task["queue_contract"] = QUEUE_CONTRACT_VERSION
        _write_json_atomic(claimed_path, task)
        ready_path = safe_file(
            data_root / "tasks" / f"ready-{validate_capability(task.get('capability'))}",
            receipt.task_id,
            field="task.id",
        )
        ready_path.parent.mkdir(parents=True, exist_ok=True)
        claimed_path.replace(ready_path)
        _event(
            data_root / "events" / "log.jsonl",
            {
                "event": "claim_requeued",
                "task": receipt.task_id,
                "worker": receipt.worker,
                "lease_id": receipt.lease_id,
                "attempt": attempt,
                "reason": reason,
                "ts": _ts(),
            },
        )
        return True


def requeue(args: Any) -> int:
    task_id = getattr(args, "task_id", None)
    worker_id = getattr(args, "worker_id", None)
    reason = str(getattr(args, "reason", None) or "").strip()
    if not task_id or not worker_id or not reason:
        print(
            "usage: requeue <task-id> <worker-id> --lease-id <lease> --attempt <n> --reason <reason>",
            file=sys.stderr,
        )
        return 2
    try:
        lease_id, attempt, _ = _terminal_binding(args)
        ws = _workspace(args)
        safe_task_id = validate_identifier(task_id, "task.id")
        claimed_path = safe_file(ws.data_root / "tasks" / "claimed", safe_task_id, field="task.id")
        if not claimed_path.exists():
            raise QueueStateError(f"task_not_claimed:{safe_task_id}")
        task = _read_json(claimed_path)
        receipt = _receipt_from_task(claimed_path, task)
        if receipt.worker != _validate_worker(worker_id):
            raise QueueStateError(f"claim_worker_mismatch:{safe_task_id}")
        if receipt.lease_id != lease_id:
            raise QueueStateError(f"stale_claim_lease:{safe_task_id}")
        if receipt.attempt != attempt:
            raise QueueStateError(f"stale_claim_attempt:{safe_task_id}")
        ok = requeue_claim(
            ws,
            receipt,
            reason=reason,
            increment_attempt=not bool(getattr(args, "no_increment_attempt", False)),
        )
        if not ok:
            raise QueueStateError(f"claim_requeue_rejected:{safe_task_id}")
    except (QueueContractError, QueueStateError, QueueBusyError, OSError) as exc:
        print(f"requeue: {exc}", file=sys.stderr)
        return 1
    payload = {
        "queue_contract": QUEUE_CONTRACT_VERSION,
        "status": "requeued",
        "task_id": safe_task_id,
        "worker": receipt.worker,
        "lease_id": receipt.lease_id,
        "attempt": receipt.attempt + (0 if getattr(args, "no_increment_attempt", False) else 1),
        "reason": reason,
    }
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) if getattr(args, "json_output", False) else "ok")
    return 0


def claim(args: Any) -> int:
    if not getattr(args, "capability", None) or not getattr(args, "worker_id", None):
        print("usage: claim.sh <capability> <worker-id>", file=sys.stderr)
        return 2
    try:
        task_id = _bound_value(getattr(args, "task_id", None), BOUND_TASK_ENV)
        lease_id = _bound_value(getattr(args, "lease_id", None), BOUND_LEASE_ENV)
        receipt = claim_task(
            _workspace(args),
            args.capability,
            args.worker_id,
            task_id=task_id or None,
            lease_id=lease_id or None,
            lease_ttl_seconds=getattr(args, "lease_ttl_seconds", None),
        )
    except (QueueContractError, QueueStateError, QueueBusyError, OSError) as exc:
        print(f"claim.sh: {exc}", file=sys.stderr)
        return 1
    if getattr(args, "json_output", False):
        print(json.dumps(receipt.as_dict(), ensure_ascii=False, separators=(",", ":")))
    else:
        print(receipt.path if receipt.status in {"claimed", "already_claimed"} else "")
    return 0


def _terminal_commit(
    workspace: Workspace,
    payload: dict[str, Any],
    *,
    lease_id: str,
    attempt: int,
    expected_digest: str = "",
) -> bool:
    data_root = workspace.data_root
    task_id = validate_identifier(payload.get("task_id"), "task.id")
    worker = _validate_worker(payload.get("worker"))
    lease_id = validate_identifier(lease_id, "claim.lease_id", max_length=96)
    if attempt < 0:
        raise QueueStateError(f"invalid_claim_attempt:{attempt}")
    results_dir = data_root / "results"
    claimed_dir = data_root / "tasks" / "claimed"
    done_dir = data_root / "tasks" / "done"
    results_dir.mkdir(parents=True, exist_ok=True)
    done_dir.mkdir(parents=True, exist_ok=True)
    (data_root / "events").mkdir(parents=True, exist_ok=True)
    result_path = safe_file(results_dir, task_id, field="task.id")
    claimed_path = safe_file(claimed_dir, task_id, field="task.id")
    done_path = safe_file(done_dir, task_id, field="task.id")
    payload["queue_contract"] = QUEUE_CONTRACT_VERSION
    payload["lease_id"] = lease_id
    payload["attempt"] = attempt
    digest = result_digest(payload)
    payload["result_digest"] = digest
    if expected_digest and expected_digest != digest:
        raise QueueStateError(f"result_digest_mismatch:expected={expected_digest}:actual={digest}")

    with task_lock(data_root, task_id, purpose=f"terminal-{payload.get('status')}"):
        if result_path.exists():
            existing = _read_json(result_path)
            existing_digest = str(
                existing.get("result_digest")
                or result_digest(existing)
            )
            if (
                str(existing.get("lease_id") or "") == lease_id
                and int(existing.get("attempt", -1)) == attempt
                and existing_digest == digest
            ):
                if claimed_path.exists() and not done_path.exists():
                    lingering = _read_json(claimed_path)
                    lingering_meta = _claim_meta(lingering)
                    if str(lingering_meta.get("lease_id") or "") == lease_id:
                        claimed_path.replace(done_path)
                return True
            raise QueueStateError(f"terminal_result_conflict:{task_id}")
        if not claimed_path.exists():
            raise QueueStateError(f"task_not_claimed:{task_id}")
        task = _read_json(claimed_path)
        validate_task_envelope(task)
        if str(task.get("id") or "") != task_id:
            raise QueueStateError(f"claimed_task_id_mismatch:{task_id}")
        meta = _claim_meta(task)
        if str(meta.get("lease_id") or "") != lease_id:
            raise QueueStateError(f"stale_claim_lease:{task_id}")
        if int(meta.get("attempt", -1)) != attempt or int(task.get("attempt", -1)) != attempt:
            raise QueueStateError(f"stale_claim_attempt:{task_id}")
        if str(meta.get("worker") or "") != worker:
            raise QueueStateError(f"claim_worker_mismatch:{task_id}")
        _require_active_lease(meta, task_id)
        if done_path.exists():
            raise QueueStateError(f"done_without_terminal_result:{task_id}")
        terminal_status = str(payload.get("status") or "")
        _validate_declared_output_contract(
            workspace,
            task,
            payload.get("result"),
            terminal_status,
        )
        _validate_task_result_hook(
            workspace,
            task,
            payload.get("result"),
            terminal_status,
        )
        # The validation hook can perform I/O. Re-check the hard lease at the
        # actual commit boundary rather than relying only on the pre-hook check.
        _require_active_lease(meta, task_id)
        _write_json_atomic(result_path, payload)
        claimed_path.replace(done_path)
    _event(
        data_root / "events" / "log.jsonl",
        {
            "event": "complete",
            "task": task_id,
            "status": payload.get("status"),
            "worker": worker,
            "lease_id": lease_id,
            "attempt": attempt,
            "result_digest": digest,
            "ts": payload.get("ts") or _ts(),
        },
    )
    return False


def _terminal_binding(args: Any) -> tuple[str, int, str]:
    lease_id = _bound_value(getattr(args, "lease_id", None), BOUND_LEASE_ENV)
    attempt = _bound_attempt(getattr(args, "attempt", None))
    expected_digest = str(
        getattr(args, "result_digest", None)
        or getattr(args, "payload_digest", None)
        or ""
    ).strip()
    if not lease_id:
        raise QueueStateError("claim_lease_required: pass --lease-id or WO_RUNTIME_CLAIM_LEASE")
    if attempt is None:
        raise QueueStateError("claim_attempt_required: pass --attempt or WO_RUNTIME_CLAIM_ATTEMPT")
    return lease_id, attempt, expected_digest


def complete_claim(
    workspace: Workspace,
    receipt: ClaimReceipt,
    result: Any,
    *,
    expected_digest: str = "",
) -> bool:
    """Commit a successful result through the queue.v2 fenced boundary.

    The return value is ``True`` for an idempotent replay and ``False`` for a
    newly committed result.
    """

    payload: dict[str, Any] = {
        "task_id": receipt.task_id,
        "worker": receipt.worker,
        "status": "success",
        "result": result,
        "ts": _ts(),
    }
    return _terminal_commit(
        workspace,
        payload,
        lease_id=receipt.lease_id,
        attempt=receipt.attempt,
        expected_digest=expected_digest,
    )


def fail_claim(
    workspace: Workspace,
    receipt: ClaimReceipt,
    error: str,
    *,
    recovery: Any | None = None,
    artifact_type: str = "",
    expected_digest: str = "",
) -> bool:
    """Commit a failed result through the queue.v2 fenced boundary."""

    payload: dict[str, Any] = {
        "task_id": receipt.task_id,
        "worker": receipt.worker,
        "status": "failed",
        "result": None,
        "error": str(error),
        "ts": _ts(),
    }
    if recovery is not None:
        payload["recovery"] = recovery
    if artifact_type:
        payload["artifact_type"] = artifact_type
    return _terminal_commit(
        workspace,
        payload,
        lease_id=receipt.lease_id,
        attempt=receipt.attempt,
        expected_digest=expected_digest,
    )


def complete(args: Any) -> int:
    task_id = getattr(args, "task_id", None)
    raw_result = getattr(args, "json_result_value", None)
    worker_id = getattr(args, "worker_id", None) or "unknown"
    if not task_id or raw_result is None:
        print("usage: complete.sh <task-id> <json-result-value> [worker-id]", file=sys.stderr)
        return 2

    autocorrected = False
    try:
        result = json.loads(raw_result)
    except json.JSONDecodeError as exc:
        try:
            result = json.loads(worker_id)
        except json.JSONDecodeError:
            print(f"complete.sh: result argument must be strict JSON: {exc}", file=sys.stderr)
            return 2
        if not re.fullmatch(r"[A-Za-z0-9_.:@/+~-]{1,160}", raw_result):
            print(
                "complete.sh: result is not JSON and worker argument is JSON; refusing ambiguous argument order",
                file=sys.stderr,
            )
            return 2
        worker_id = raw_result
        autocorrected = True

    ts = _ts()
    payload: dict[str, Any] = {
        "task_id": task_id,
        "worker": worker_id,
        "status": "success",
        "result": result,
        "ts": ts,
    }
    if autocorrected:
        payload["argOrderAutocorrected"] = True
    try:
        lease_id, attempt, expected_digest = _terminal_binding(args)
        ws = _workspace(args)
        idempotent = _terminal_commit(
            ws,
            payload,
            lease_id=lease_id,
            attempt=attempt,
            expected_digest=expected_digest,
        )
    except (QueueContractError, QueueStateError, QueueBusyError, OSError) as exc:
        print(f"complete.sh: {exc}", file=sys.stderr)
        return 1
    if autocorrected and not idempotent:
        _event(
            ws.data_root / "events" / "log.jsonl",
            {
                "event": "complete_arg_order_autocorrected",
                "task": task_id,
                "worker": worker_id,
                "ts": ts,
            },
        )
    print("ok (idempotent)" if idempotent else "ok")
    return 0


def fail(args: Any) -> int:
    task_id = getattr(args, "task_id", None)
    error = getattr(args, "error", None)
    worker_id = getattr(args, "worker_id", None) or "unknown"
    recovery_raw = getattr(args, "recovery_json", None) or ""
    artifact_type = getattr(args, "artifact_type", None) or ""
    if not task_id or error is None:
        print("usage: fail.sh <task-id> <error-string> [worker-id]", file=sys.stderr)
        return 2

    ts = _ts()

    payload: dict[str, Any] = {
        "task_id": task_id,
        "worker": worker_id,
        "status": "failed",
        "result": None,
        "error": error,
        "ts": ts,
    }
    if artifact_type:
        payload["artifact_type"] = artifact_type
    if recovery_raw:
        try:
            payload["recovery"] = json.loads(recovery_raw)
        except Exception as exc:
            payload["recovery"] = {
                "repairMode": "manual_review",
                "reason": "invalid_recovery_json",
                "evidence": str(exc),
                "autoSafe": False,
            }
    try:
        lease_id, attempt, expected_digest = _terminal_binding(args)
        idempotent = _terminal_commit(
            _workspace(args),
            payload,
            lease_id=lease_id,
            attempt=attempt,
            expected_digest=expected_digest,
        )
    except (QueueContractError, QueueStateError, QueueBusyError, OSError) as exc:
        print(f"fail.sh: {exc}", file=sys.stderr)
        return 1
    print("ok (idempotent)" if idempotent else "ok")
    return 0


def promote(args: Any) -> int:
    raw_path = getattr(args, "task_json_path", None)
    if not raw_path or not Path(raw_path).exists():
        print("usage: promote.sh <existing-task-file>", file=sys.stderr)
        return 2

    ws = _workspace(args)
    data_root = ws.data_root
    path = Path(raw_path).resolve()
    pending_root = (data_root / "tasks" / "pending").resolve()
    if path.parent != pending_root:
        print(f"promote.sh: task path must be directly under {pending_root}", file=sys.stderr)
        return 1
    try:
        task = _read_json(path)
        validate_task_envelope(task)
        task_id = validate_identifier(task.get("id"), "task.id")
        capability = validate_capability(task.get("capability"))
        if path.name != f"{task_id}.json":
            raise QueueStateError(f"task_filename_mismatch:{path.name}:{task_id}")
    except (QueueContractError, QueueStateError) as exc:
        print(f"could not validate task {raw_path}: {exc}", file=sys.stderr)
        return 1

    try:
        context = build_context(str(task.get("id") or path.stem), workspace=ws)
        pack = context.get("contextPack")
        if isinstance(pack, dict) and pack.get("status") != "disabled":
            task["contextPack"] = pack
            _write_json_atomic(path, task)
    except Exception as exc:
        _event(
            data_root / "events" / "log.jsonl",
            {
                "event": "context_pack_warning",
                "task": path.stem,
                "capability": capability,
                "error": f"{type(exc).__name__}: {exc}",
                "ts": _ts(),
            },
        )

    ready = data_root / "tasks" / f"ready-{capability}"
    ready.mkdir(parents=True, exist_ok=True)
    (data_root / "events").mkdir(parents=True, exist_ok=True)
    target = safe_file(ready, task_id, field="task.id")
    if target.exists():
        print(f"promote.sh: target already exists: {target}", file=sys.stderr)
        return 1
    path.replace(target)
    _event(
        data_root / "events" / "log.jsonl",
        {
            "event": "promote",
            "task": task_id,
            "capability": capability,
            "ts": _ts(),
        },
    )
    print(target)
    return 0


def _count(path: Path) -> int:
    if not path.is_dir():
        return 0
    return len(list(path.glob("*.json")))


def _pad(value: str) -> str:
    return f"{value:<22}"


def status(args: Any) -> int:
    ws = _workspace(args)
    data_root = ws.data_root
    print(f"=== {ws.root.name} status ===")
    print(f"{_pad('inbox/')} : {_count(data_root / 'inbox')}")
    print(f"{_pad('inbox/processed/')} : {_count(data_root / 'inbox' / 'processed')}")
    print(f"{_pad('tasks/pending/')} : {_count(data_root / 'tasks' / 'pending')}")

    ready_dirs = sorted((data_root / "tasks").glob("ready-*")) if (data_root / "tasks").is_dir() else []
    ready_dirs = [path for path in ready_dirs if path.is_dir()]
    if not ready_dirs:
        print(f"{_pad('tasks/ready-*')} : (no ready dirs yet)")
    else:
        for path in ready_dirs:
            rel = path.relative_to(data_root)
            print(f"{_pad(str(rel) + '/')} : {_count(path)}")

    print(f"{_pad('tasks/claimed/')} : {_count(data_root / 'tasks' / 'claimed')}")
    print(f"{_pad('tasks/done/')} : {_count(data_root / 'tasks' / 'done')}")
    print(f"{_pad('results/')} : {_count(data_root / 'results')}")

    finalized = len(list((data_root / "results").glob("*.final.json"))) if (data_root / "results").is_dir() else 0
    if finalized > 0:
        print(f"{_pad('  └ finalized')} : {finalized}")

    print()
    print("--- last 5 events ---")
    event_log = data_root / "events" / "log.jsonl"
    if event_log.exists():
        lines = event_log.read_text(encoding="utf-8").splitlines()
        for line in lines[-5:]:
            print(line)
    else:
        print("(no events yet)")
    return 0
