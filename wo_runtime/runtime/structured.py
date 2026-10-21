"""Opt-in file protocol for host-owned structured task results.

The child produces only a candidate JSON value. Queue ownership, terminal
commits, retries, and process deadlines remain the supervisor's responsibility.
These helpers validate local files; they do not sandbox an untrusted child.
"""

from __future__ import annotations

import json
import math
import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from wo_runtime.artifacts import atomic_write_json
from wo_runtime.core.contracts import contract_path
from wo_runtime.core.schema import validate_json_schema
from wo_runtime.core.validation import validate_identifier
from wo_runtime.queue.commands import ClaimReceipt
from wo_runtime.worker import BoundClaim, load_bound_task, resolve_task_inputs
from wo_runtime.workspace import Workspace


MAX_RESULT_BYTES = 16 * 1024 * 1024


class StructuredResultError(RuntimeError):
    """Structured execution configuration or a candidate result is invalid."""


@dataclass(frozen=True)
class StructuredOutput:
    schema_path: Path
    context_path: Path
    output_path: Path
    schema: dict[str, Any]


def structured_output_enabled(descriptor: dict[str, Any]) -> bool:
    """Validate and select the additive ``exec.result_protocol`` setting."""

    spec = descriptor.get("exec", {})
    if not isinstance(spec, dict):
        raise StructuredResultError("exec must be an object")
    if "result_protocol" not in spec or spec["result_protocol"] == "worker":
        return False
    if spec["result_protocol"] != "json-file":
        raise StructuredResultError(
            "exec.result_protocol must be 'worker' or 'json-file' when declared"
        )

    args = spec.get("args")
    if not isinstance(args, list) or not all(isinstance(arg, str) for arg in args):
        raise StructuredResultError("json-file requires exec.args to be an array of strings")
    if "model_flag" in spec:
        model_flag = spec["model_flag"]
        if not isinstance(model_flag, list) or not all(isinstance(arg, str) for arg in model_flag):
            raise StructuredResultError("json-file requires exec.model_flag to be an array of strings")
    payload = spec.get("payload")
    if "payload" in spec and (not isinstance(payload, str) or not payload.strip()):
        raise StructuredResultError("json-file exec.payload must name a payload field or be 'none'")
    if "payload_position" in spec and spec["payload_position"] != "last":
        raise StructuredResultError("json-file supports only exec.payload_position='last'")
    for placeholder in ("{output_schema}", "{output_path}"):
        if not any(placeholder in arg for arg in args):
            raise StructuredResultError(f"json-file requires {placeholder} in exec.args")
    has_context_arg = any("{task_context}" in arg for arg in args)
    has_payload = (
        isinstance(payload, str)
        and bool(payload.strip())
        and payload != "none"
        and spec.get("payload_position", "last") == "last"
    )
    if not has_context_arg and not has_payload:
        raise StructuredResultError(
            "json-file requires {task_context} in exec.args or a named payload in last position"
        )
    return True


def _reject_constant(value: str) -> Any:
    raise ValueError(f"non-finite JSON number: {value}")


def _finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"non-finite JSON number: {value}")
    return parsed


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key!r}")
        result[key] = value
    return result


def _parse_json(value: str, source: Path) -> Any:
    try:
        return json.loads(
            value,
            parse_constant=_reject_constant,
            parse_float=_finite_float,
            object_pairs_hook=_unique_object,
        )
    except (ValueError, RecursionError) as exc:
        raise StructuredResultError(f"invalid JSON in {source}: {exc}") from exc


def _absolute_workspace(workspace: Workspace) -> Workspace:
    root = workspace.root.resolve()
    raw_data_root = workspace.data_root
    data_root = (
        raw_data_root if raw_data_root.is_absolute() else root / raw_data_root
    ).resolve()
    return Workspace(
        root=root,
        config_path=workspace.config_path,
        config=workspace.config,
        data_root_raw=workspace.data_root_raw,
        data_root=data_root,
    )


def prepare_structured_output(workspace: Workspace, receipt: ClaimReceipt) -> StructuredOutput:
    """Bind inputs and a schema snapshot to a fresh invocation directory."""

    try:
        workspace = _absolute_workspace(workspace)
        claim = BoundClaim.from_environment(
            workspace,
            environ={"WO_RUNTIME_CLAIM_JSON": json.dumps(receipt.as_dict())},
        )
        if claim is None:
            raise StructuredResultError("json-file requires an active queue claim")
        task = load_bound_task(claim)
        contract_id = task.get("outputContract")
        if not isinstance(contract_id, str) or not contract_id.strip():
            raise StructuredResultError("json-file task requires a nonempty outputContract")
        source_schema_path = contract_path(contract_id.strip(), task, workspace=workspace)
        schema = _parse_json(source_schema_path.read_text(encoding="utf-8"), source_schema_path)
        if not isinstance(schema, dict):
            raise StructuredResultError(f"outputContract schema must be an object: {source_schema_path}")
        inputs = resolve_task_inputs(workspace, task)

        task_id = validate_identifier(receipt.task_id, "task.id")
        parent = workspace.data_root / "runtime-invocations" / task_id
        if not parent.resolve().is_relative_to(workspace.data_root):
            raise StructuredResultError("structured invocation directory escapes the data root")
        parent.mkdir(parents=True, exist_ok=True)
        directory = Path(tempfile.mkdtemp(prefix=f"attempt-{receipt.attempt}-", dir=parent))
        schema_path = directory / "schema.json"
        context_path = directory / "context.json"
        output_path = directory / "candidate.json"
        # The host retains claim authority. Task/business metadata is still
        # available to the child, without the lease-bearing receipt fields.
        context_task = {key: value for key, value in task.items() if key not in {"claim", "_claim"}}
        atomic_write_json(schema_path, schema, pretty=True)
        atomic_write_json(context_path, {"task": context_task, "inputs": inputs}, pretty=True)
        return StructuredOutput(schema_path, context_path, output_path, schema)
    except StructuredResultError:
        raise
    except Exception as exc:
        raise StructuredResultError(f"could not prepare structured output: {type(exc).__name__}: {exc}") from exc


def _read_candidate(path: Path) -> str:
    """Read a bounded regular file without following a candidate symlink."""

    try:
        before = path.lstat()
    except FileNotFoundError as exc:
        raise StructuredResultError(f"structured result is missing: {path}") from exc
    if not stat.S_ISREG(before.st_mode):
        raise StructuredResultError(f"structured result must be a regular, non-symlink file: {path}")
    if before.st_size > MAX_RESULT_BYTES:
        raise StructuredResultError(f"structured result exceeds {MAX_RESULT_BYTES} bytes: {path}")

    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    # Avoid blocking on a special file substituted between lstat and open.
    flags |= getattr(os, "O_NONBLOCK", 0)
    descriptor = os.open(path, flags)
    with os.fdopen(descriptor, "rb") as handle:
        opened = os.fstat(handle.fileno())
        if not stat.S_ISREG(opened.st_mode) or not os.path.samestat(before, opened):
            raise StructuredResultError(f"structured result changed while opening: {path}")
        payload = handle.read(MAX_RESULT_BYTES + 1)
    if len(payload) > MAX_RESULT_BYTES:
        raise StructuredResultError(f"structured result exceeds {MAX_RESULT_BYTES} bytes: {path}")
    return payload.decode("utf-8")


def read_structured_result(prepared: StructuredOutput) -> Any:
    """Parse the child candidate and validate against the host's schema snapshot."""

    try:
        result = _parse_json(_read_candidate(prepared.output_path), prepared.output_path)
        validate_json_schema(result, prepared.schema)
        return result
    except StructuredResultError:
        raise
    except Exception as exc:
        raise StructuredResultError(f"structured result rejected: {type(exc).__name__}: {exc}") from exc
