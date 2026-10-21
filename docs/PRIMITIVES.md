# Reusable Runtime Primitives

Version `0.2.0` exposes a small Python boundary for workflow instances that
need queue-safe workers, immutable artifacts, contract validation, or status
projection without copying runtime mechanics into product code.

## Ownership boundary

| Runtime owns | Instance owns |
| --- | --- |
| Queue paths, claims, leases, fencing, terminal commits | Product handlers and domain policy |
| Generic JSON contract validation | Contract files and `outputContract` selection |
| Atomic and immutable artifact writes | Artifact meaning, retention, and presentation |
| Task/request status projection | UI wording and delivery behavior |
| Strict dependency-result resolution | Pipeline topology and input aliases |

The runtime must not import an instance package. Optional behavior is selected
through `runtime/workspace.json`, contract indexes, pipeline data, and instance
hooks.

## API map

| Module | Stable responsibility |
| --- | --- |
| `wo_runtime.worker` | Validate a supervisor claim, load the bound task, resolve dependency results, and perform fenced terminal operations |
| `wo_runtime.queue.commands` | Programmatic `complete_claim`, `fail_claim`, and `requeue_claim` operations using a `ClaimReceipt` |
| `wo_runtime.core.schema` | Load and validate the dependency-free JSON Schema subset used by task contracts |
| `wo_runtime.artifacts` | Atomic writes, idempotent immutable JSON, and verified content-addressed storage |
| `wo_runtime.status` | Read-only request/task state projection from queue envelopes and terminal results |
| `wo_runtime.core.context` | Context assembly and optional strict dependency-result resolution |

## Receipt-bound worker

A supervised worker using the default `worker` result protocol receives a JSON
claim receipt and matching scalar bindings in its environment. Treat those
values as one receipt; do not reconstruct them from a task id or scan the
claimed directory. The opt-in [`json-file` protocol](STRUCTURED-OUTPUT.md) keeps
that receipt in the host and requires only a candidate result from the child.

```python
from pathlib import Path

from wo_runtime.worker import (
    BoundClaim,
    RuntimeTerminal,
    WorkerError,
    load_bound_task,
    resolve_task_inputs,
)


def run(handler) -> None:
    workspace_root = Path.cwd()
    claim = BoundClaim.from_environment(workspace_root)
    if claim is None:
        raise WorkerError("worker requires a supervisor claim")

    terminal = RuntimeTerminal(workspace_root, claim)
    try:
        task = load_bound_task(claim)
        inputs = resolve_task_inputs(workspace_root, task)
        result = handler(task, inputs)
    except Exception as exc:
        terminal.fail(str(exc))
        raise
    terminal.complete(result)
```

`BoundClaim.from_environment` rejects partial bindings, receipt disagreements,
and paths outside `<dataRoot>/tasks/claimed`. `load_bound_task` verifies the
worker, lease, and attempt embedded in the claimed envelope. Strict input
resolution rejects missing, malformed, or unsuccessful dependency results.

`RuntimeTerminal.complete`, `fail`, and `requeue` preserve the same receipt.
They never move queue files directly. Complete and fail return `False` for a
new commit and `True` for an accepted idempotent replay. Requeue keeps the
attempt by default; pass `increment_attempt=True` only when the instance
intentionally consumes an attempt.

## Declared output contracts

Set `outputContract` on a task envelope to make successful terminal commits
validate the handler result. Contract ids resolve through the configured
instance and domain indexes, then the built-in runtime contracts.

The commit boundary runs in this order:

1. verify task, worker, lease, attempt, and active deadline;
2. load and validate the declared output contract;
3. run the optional instance `validate_task_result` hook with copies;
4. verify the lease again after hook I/O;
5. atomically write the terminal result and move the claimed task to `done`.

A validation failure writes no terminal result and leaves the claimed task
available for explicit recovery. Failed terminal results are not checked
against `outputContract`, because their payload is the runtime failure envelope.

The schema validator enforces the subset used by runtime contracts:

- `type`, `const`, and `enum`;
- object `properties`, `required`, `additionalProperties`, `minProperties`, and
  `maxProperties`;
- array `items`, `minItems`, `maxItems`, `uniqueItems`, `contains`,
  `minContains`, and `maxContains`;
- string `minLength`, `maxLength`, and `pattern`;
- numeric `minimum`, `maximum`, `exclusiveMinimum`, and `exclusiveMaximum`;
- `allOf`, `anyOf`, `oneOf`, and `if` / `then` / `else`.

This is intentionally not a complete JSON Schema implementation. `$ref` fails
closed, and unlisted keywords must not be relied on for enforcement.

## Artifact persistence

Use immutable or content-addressed writes for outputs that may be replayed:

```python
from wo_runtime.artifacts import ContentAddressedStore, idempotent_write_json

created = idempotent_write_json(
    data_root / "assets" / "manifest.json",
    {"schemaVersion": 1, "items": items},
)

store = ContentAddressedStore(data_root / "assets" / "objects")
artifact = store.write_bytes("source", payload, extension="bin")
```

`idempotent_write_json` returns `True` for a new artifact and `False` for an
equivalent replay. It rejects malformed or conflicting existing files.
Publication is exclusive, so a concurrent writer cannot be silently replaced.
`ContentAddressedStore` verifies existing bytes before returning the same
`ArtifactRef`; it never silently replaces corrupt content.

The lower-level `atomic_write_bytes`, `atomic_write_text`, and
`atomic_write_json` helpers use a same-directory temporary file, `fsync`, and
atomic replacement. They are suitable for mutable projections, not immutable
evidence that must reject conflicts.

## Status projection

`RequestStatusReader` derives state from materialized queue envelopes and
terminal results:

```python
from wo_runtime.status import RequestStatusReader

status = RequestStatusReader(data_root).project(request_id)
payload = status.as_dict()
```

The returned request state is one of `not_found`, `queued`, `processing`,
`success`, or `failed`. Progress counts only successful terminal tasks.
`expected_tasks` may be supplied as a compatibility fallback before planning,
but real queue files and terminal results always take precedence.

This reader is intentionally presentation-neutral. Instances decide how to
label stages, localize errors, poll, notify users, or expose the projection.

## Compatibility notes

- `queue.v2` remains the only accepted queue contract.
- Tasks without `outputContract` retain their previous terminal behavior.
- `resolve_inputs` keeps warning-mode behavior by default; workers opt into the
  strict boundary through `resolve_task_inputs` or `strict=True`.
- Instance hooks remain additive and run only after generic runtime validation.
- Product policy, provider selection, delivery receipts, and UI behavior remain
  outside this package.
