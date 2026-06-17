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

The core layer reads instance data from `runtime/workspace.json`, `domains/`,
`pipelines/`, and `contracts/contract-index.json`. It must not contain product
family literals; pipeline selection, fanout defaults, input aliases, and context
contract/audit policy are instance data.

Documentation:

- `docs/CORE.md` - core planner/context command contract and required instance data.
- `wo_runtime/contracts/README.md` - runtime-neutral queue/result/context contracts.
