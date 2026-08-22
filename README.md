# woafc-runtime

Reusable file-queue runtime for WO/AFC-style agent workspaces.

This repository owns runtime-neutral queue mechanics, runtime adapter plumbing,
generic orchestrator behavior, reusable core planning/context helpers, hooks,
and synthetic selftests. Product/domain policy stays in the instance repository.

Run from an instance checkout without installation:

```bash
vendor/woafc-runtime/bin/wo-runtime --help
```

Core commands for thin instances:

```bash
vendor/woafc-runtime/bin/wo-runtime core plan --request-file request.json --dry-run
vendor/woafc-runtime/bin/wo-runtime core plan-inbox --workspace-root "$PWD" --once
vendor/woafc-runtime/bin/wo-runtime core context <task-id>
vendor/woafc-runtime/bin/wo-runtime core contract-path <contract-id>
vendor/woafc-runtime/bin/wo-runtime core validate-dag --req-id <request-id>
```

Reusable Python boundaries are also available without installing the runtime:

```python
from wo_runtime.artifacts import ContentAddressedStore, idempotent_write_json
from wo_runtime.status import RequestStatusReader
from wo_runtime.worker import BoundClaim, RuntimeTerminal, load_bound_task, resolve_task_inputs
```

`BoundClaim` validates the supervisor receipt and claimed path,
`resolve_task_inputs` applies strict dependency resolution, and
`RuntimeTerminal` commits through the same queue.v2 fencing boundary as the
CLI. A task with `outputContract` is schema-validated by the runtime before its
instance result hook and before any terminal file is written.

The core layer reads instance data from `runtime/workspace.json`, `domains/`,
`pipelines/`, and `contracts/contract-index.json`. It must not contain product
family literals; pipeline selection, fanout defaults, input aliases, and context
contract/audit policy are instance data.

Documentation:

- `docs/CORE.md` - core planner/context command contract and required instance data.
- `docs/QUEUE.md` - safe paths, targeted leases, terminal idempotency, deadlines, and instance hooks.
- `docs/PRIMITIVES.md` - reusable Python APIs, ownership boundaries, contract validation, and worker examples.
- `docs/WORKFLOW-AUTHORING.md` - how an agent builds and verifies a new thin workflow instance.
- `docs/prompts/CREATE-SHORT-VIDEO-WORKFLOW.vi.md` - copy-ready Vietnamese example prompt.
- `wo_runtime/contracts/README.md` - runtime-neutral queue/result/context contracts.

## License

Copyright 2026 Ngô Tấn Phúc.

Licensed under the [Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for
attribution information.
