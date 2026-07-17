# Queue Contract

The runtime accepts one queue contract: `queue.v2`. Every active task and task
result must use it. The contract provides ownership, path-safety, idempotency,
and bounded-runtime guarantees.

## Safe envelopes and paths

- Request ids, task ids, dependency ids, domain ids, and pipeline ids must be
  portable single path components. Absolute paths, separators, `.` / `..`,
  whitespace padding, and control characters are rejected before planning or
  queue mutation.
- Tasks require `queue_contract: "queue.v2"`, `id`, `req_id`, `capability`,
  and `depends_on` (an array).
- `manualOnly: true` tasks cannot be claimed by the generic/model-worker claim
  command.

## Claim receipt

Claim one exact task and request a machine-readable receipt:

```bash
wo-runtime queue claim <capability> <worker-id> --task-id <task-id> --json
```

Long-running direct workers can declare their complete hard task budget at the
initial claim (for example, provider timeout times the maximum serial items,
plus cleanup margin):

```bash
wo-runtime queue claim <capability> <worker-id> --task-id <task-id> \
  --lease-ttl-seconds <hard-budget-seconds> --json
```

The receipt preserves `task_id`, `path`, `worker`, and `attempt`, and adds
`lease_id`, `claimed_at`, `lease_expires_at`, `status`, and
`queue_contract: "queue.v2"`. A direct claim uses `QUEUE_LEASE_TTL_SECONDS`
(default `3660`). A supervisor claim uses the resolved role runtime limit plus
60 seconds. Explicit and default TTLs are bounded by
`QUEUE_MAX_LEASE_TTL_SECONDS` (default `86400`); an oversized claim is rejected
without moving the task. The lease is fixed when claimed: reentrant claims and
heartbeats never renew or extend its hard deadline.

## Complete or fail

Complete and fail require the receipt ownership values:

```bash
wo-runtime queue complete <task-id> '<json-result>' <worker-id> \
  --lease-id <lease-id> --attempt <attempt>

wo-runtime queue fail <task-id> '<error>' <worker-id> [recovery-json] [artifact-type] \
  --lease-id <lease-id> --attempt <attempt>
```

`--result-digest <sha256>` optionally asserts the canonical terminal digest.
Successful terminal files store the same hash as `result_digest`. An exact
retry with the same lease, attempt, worker, status, and payload is idempotent;
a different payload or stale/unowned lease is rejected without overwrite.
If a worker crashes after writing the result but before moving the task to
`done`, orchestrator reconciliation accepts only a queue result whose task,
lease, attempt, worker, status, and recomputed `result_digest` match the current
claim. Unfenced or forged results are moved to `results/_invalid/`.

There is no legacy fallback, automatic contract upgrade, or unclaimed task
completion. Tasks without `queue_contract: "queue.v2"` are rejected.
Supervised children remain compatible because the supervisor pre-claims and
sets `WO_RUNTIME_TASK_ID`, `WO_RUNTIME_CLAIM_LEASE`,
`WO_RUNTIME_CLAIM_ATTEMPT`, `WO_RUNTIME_CLAIM_EXPIRES_AT`, and
`WO_RUNTIME_CLAIM_PATH`. Direct daemons and interactive roles must capture the
JSON receipt and pass its lease and attempt.

Orchestrator-generated dependency failures, canonical media aliases, and manual
human-review terminals use the same claim/result binding. They do not bypass
the queue contract.

## Owned requeue

Provider holds and other non-terminal releases use the queue command rather
than moving files manually:

```bash
wo-runtime queue requeue <task-id> <worker-id> \
  --lease-id <lease-id> --attempt <attempt> --reason <reason> \
  [--no-increment-attempt] [--json]
```

The command verifies task, worker, lease, and attempt ownership and refuses to
requeue a task that already has a terminal result.

## Supervisor binding and deadlines

For queue-task triggers, the supervisor claims the exact trigger before
launching a child. Product/task context, queue environment, heartbeat, and
terminal ownership therefore refer to the same task. Runtime limit resolution
is:

1. `runtime/fleet.json[role].max_runtime_seconds`;
2. `runtime/roles.json[role].max_runtime_seconds`;
3. supervisor `--invocation-timeout` / `SUPERVISOR_INVOCATION_TIMEOUT_SECONDS`.

The default fallback is 3600 seconds. The default process-group termination
grace is 10 seconds. `0` disables the supervisor deadline, while the default
queue lease TTL still bounds a direct claim.

## Optional instance hooks

Instance modules are selected in `runtime/workspace.json`:

```json
{
  "instanceHooks": {
    "planner": "instance.planner_hooks",
    "queue": "instance.queue_hooks",
    "orchestrator": "instance.orchestrator_hooks"
  }
}
```

- `before_plan_request(request, request_path, data_root, workspace_root) -> dict`
  runs after generic request/path validation and before DAG compilation. Its
  returned request is validated again.
- `validate_task_result(task, result, status, data_root, workspace_root)` runs
  before a successful owned terminal result is written; raising rejects it.
- `finalization_decision(req_id, tasks, root_task, root_result, final, **kwargs)`
  may return `{"action":"allow"}`, `{"action":"defer","reason":"..."}`,
  or `{"action":"reject","reason":"..."}`.
