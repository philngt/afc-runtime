from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any


QUEUE_CONTRACT_VERSION = "queue.v2"
MAX_ID_LENGTH = 240
MAX_CAPABILITY_LENGTH = 80

# Queue identifiers become single filesystem path components. Keep the portable
# contract intentionally narrower than the set of characters accepted by any
# one host filesystem.
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.@+~-]*$")
CAPABILITY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
WINDOWS_RESERVED_NAMES = {
    "con", "prn", "aux", "nul",
    *(f"com{index}" for index in range(1, 10)),
    *(f"lpt{index}" for index in range(1, 10)),
}


class QueueContractError(ValueError):
    """A queue/request envelope violates the runtime control-plane contract."""


def result_digest(payload: dict[str, Any]) -> str:
    """Return the canonical queue digest for a terminal result envelope."""
    stable = {
        key: value
        for key, value in payload.items()
        if key not in {"ts", "result_digest", "argOrderAutocorrected"}
    }
    encoded = json.dumps(stable, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validate_terminal_result_binding(
    task: Any,
    result: Any,
    *,
    expected_task_id: str | None = None,
) -> dict[str, Any]:
    """Validate that a terminal result is fenced by the current queue claim."""
    claim = validate_claim_binding(task, expected_task_id=expected_task_id)
    if not isinstance(result, dict):
        raise QueueContractError("result:not_object")
    task_id = validate_identifier(task.get("id"), "task.id")
    claim_lease = validate_identifier(claim.get("lease_id"), "claim.lease_id", max_length=96)
    claim_attempt = claim.get("attempt")
    task_attempt = task.get("attempt")
    if (
        isinstance(claim_attempt, bool)
        or not isinstance(claim_attempt, int)
        or claim_attempt < 0
        or isinstance(task_attempt, bool)
        or not isinstance(task_attempt, int)
        or task_attempt != claim_attempt
    ):
        raise QueueContractError("claim.attempt:task_mismatch")
    claim_worker = claim.get("worker")

    if result.get("queue_contract") != QUEUE_CONTRACT_VERSION:
        raise QueueContractError("result.queue_contract:not_queue_v2")
    if result.get("task_id") != task_id:
        raise QueueContractError("result.task_id:mismatch")
    if result.get("status") not in {"success", "failed"}:
        raise QueueContractError("result.status:invalid")
    result_lease = validate_identifier(result.get("lease_id"), "result.lease_id", max_length=96)
    result_attempt = result.get("attempt")
    if isinstance(result_attempt, bool) or not isinstance(result_attempt, int) or result_attempt < 0:
        raise QueueContractError("result.attempt:not_nonnegative_integer")
    result_worker = result.get("worker")
    if not isinstance(result_worker, str) or not result_worker:
        raise QueueContractError("result.worker:missing")
    if result_lease != claim_lease:
        raise QueueContractError("result.lease_id:claim_mismatch")
    if result_attempt != claim_attempt:
        raise QueueContractError("result.attempt:claim_mismatch")
    if result_worker != claim_worker:
        raise QueueContractError("result.worker:claim_mismatch")
    supplied_digest = result.get("result_digest")
    if not isinstance(supplied_digest, str) or supplied_digest != result_digest(result):
        raise QueueContractError("result.result_digest:mismatch")
    return result


def validate_claim_binding(
    task: Any,
    *,
    expected_task_id: str | None = None,
) -> dict[str, Any]:
    """Validate ownership metadata on a claimed Queue task."""
    validate_task_envelope(task)
    task_id = validate_identifier(task.get("id"), "task.id")
    if expected_task_id is not None and task_id != validate_identifier(expected_task_id, "task.id"):
        raise QueueContractError("task.id:expected_mismatch")
    claim = task.get("claim") if isinstance(task.get("claim"), dict) else {}
    validate_identifier(claim.get("lease_id"), "claim.lease_id", max_length=96)
    claim_attempt = claim.get("attempt")
    task_attempt = task.get("attempt")
    if (
        isinstance(claim_attempt, bool)
        or not isinstance(claim_attempt, int)
        or claim_attempt < 0
        or isinstance(task_attempt, bool)
        or not isinstance(task_attempt, int)
        or task_attempt != claim_attempt
    ):
        raise QueueContractError("claim.attempt:task_mismatch")
    claim_worker = claim.get("worker")
    if not isinstance(claim_worker, str) or not claim_worker:
        raise QueueContractError("claim.worker:missing")
    if claim.get("capability") != task.get("capability"):
        raise QueueContractError("claim.capability:task_mismatch")
    if not isinstance(claim.get("claimed_at"), str) or not claim.get("claimed_at"):
        raise QueueContractError("claim.claimed_at:missing")
    if not isinstance(claim.get("lease_expires_at"), str) or not claim.get("lease_expires_at"):
        raise QueueContractError("claim.lease_expires_at:missing")
    return claim


def validate_identifier(value: Any, field: str = "id", *, max_length: int = MAX_ID_LENGTH) -> str:
    if not isinstance(value, str):
        raise QueueContractError(f"{field}:not_string")
    text = value
    if not text:
        raise QueueContractError(f"{field}:missing")
    if text != text.strip():
        raise QueueContractError(f"{field}:unsafe_whitespace:{text!r}")
    if len(text) > max_length:
        raise QueueContractError(f"{field}:too_long:{len(text)}")
    if (
        text in {".", ".."}
        or text.endswith(".")
        or text.split(".", 1)[0].casefold() in WINDOWS_RESERVED_NAMES
        or not IDENTIFIER_RE.fullmatch(text)
    ):
        raise QueueContractError(f"{field}:unsafe:{text!r}")
    return text


def validate_capability(value: Any) -> str:
    if not isinstance(value, str):
        raise QueueContractError("capability:not_string")
    text = value
    if not text:
        raise QueueContractError("capability:missing")
    if text != text.strip():
        raise QueueContractError(f"capability:unsafe_whitespace:{text!r}")
    if len(text) > MAX_CAPABILITY_LENGTH:
        raise QueueContractError(f"capability:too_long:{len(text)}")
    if not CAPABILITY_RE.fullmatch(text):
        raise QueueContractError(f"capability:unsafe:{text!r}")
    return text


def validate_resource_id(value: Any, field: str) -> str:
    """Validate ids used to select a domain/pipeline descriptor filename."""
    return validate_identifier(value, field, max_length=120)


def safe_file(base: Path, identifier: Any, *, suffix: str = ".json", field: str = "id") -> Path:
    safe_id = validate_identifier(identifier, field)
    root = base.resolve()
    candidate = (root / f"{safe_id}{suffix}").resolve()
    if candidate.parent != root:
        raise QueueContractError(f"{field}:path_escape:{safe_id!r}")
    return candidate


def validate_request_envelope(request: Any) -> dict[str, Any]:
    if not isinstance(request, dict):
        raise QueueContractError("request:not_object")
    request_id = request.get("id") or request.get("req_id")
    validate_identifier(request_id, "request.id")
    for field in ("domainId", "domain_id", "pipelineId", "pipeline_id"):
        if request.get(field) not in (None, ""):
            validate_resource_id(request[field], f"request.{field}")
    source = request.get("source")
    if isinstance(source, dict):
        for field in ("domainId", "domain_id", "pipelineId", "pipeline_id"):
            if source.get(field) not in (None, ""):
                validate_resource_id(source[field], f"request.source.{field}")
    return request


def validate_task_envelope(task: Any, *, require_stage: bool = False) -> dict[str, Any]:
    if not isinstance(task, dict):
        raise QueueContractError("task:not_object")
    validate_identifier(task.get("id"), "task.id")
    validate_identifier(task.get("req_id"), "task.req_id")
    validate_capability(task.get("capability"))
    if task.get("queue_contract") != QUEUE_CONTRACT_VERSION:
        raise QueueContractError("task.queue_contract:not_queue_v2")
    for field in ("domainId", "domain_id", "pipelineId", "pipeline_id"):
        if task.get(field) not in (None, ""):
            validate_resource_id(task[field], f"task.{field}")
    if require_stage and not str(task.get("stage") or "").strip():
        raise QueueContractError("task.stage:missing")
    deps = task.get("depends_on")
    if not isinstance(deps, list):
        raise QueueContractError("task.depends_on:not_array")
    for index, dep in enumerate(deps):
        validate_identifier(dep, f"task.depends_on[{index}]")
    attempt = task.get("attempt", 0)
    if isinstance(attempt, bool):
        raise QueueContractError("task.attempt:not_integer")
    try:
        parsed_attempt = int(attempt)
    except (TypeError, ValueError) as exc:
        raise QueueContractError("task.attempt:not_integer") from exc
    if parsed_attempt < 0:
        raise QueueContractError("task.attempt:negative")
    return task
