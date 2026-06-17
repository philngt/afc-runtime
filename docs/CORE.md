# Runtime Core Planner And Context

`wo_runtime.core` is the reusable planning/context layer for thin product
instances. It compiles an instance request into a queue DAG by reading instance
data. It must stay domain-neutral: no product family, marketplace, locale, or
channel literals belong in runtime code.

## Commands

Run these from an instance checkout:

```bash
vendor/woafc-runtime/bin/wo-runtime core plan --workspace-root "$PWD" --request-file request.json --dry-run
vendor/woafc-runtime/bin/wo-runtime core plan --workspace-root "$PWD" --request-file request.json --write
vendor/woafc-runtime/bin/wo-runtime core plan-inbox --workspace-root "$PWD" --once
vendor/woafc-runtime/bin/wo-runtime core context --workspace-root "$PWD" <task-id>
vendor/woafc-runtime/bin/wo-runtime core contract-path --workspace-root "$PWD" <contract-id>
vendor/woafc-runtime/bin/wo-runtime core validate-dag --workspace-root "$PWD" --req-id <request-id>
```

Behavior:

- `plan --dry-run` prints the compiled DAG and writes nothing.
- `plan --write` writes task files to `.run/data/tasks/pending/`.
- `plan-inbox --once` compiles at most one `.run/data/inbox/*.json` request,
  writes tasks, moves the request to `inbox/processed/`, and appends a `planned`
  event.
- `context` builds a compact task context with task, resolved inputs, domain,
  pipeline, product kind, contract refs, guidance refs, audits, and artifact refs.
- `contract-path` resolves runtime, instance, and domain contract indexes.
- `validate-dag` checks one planned request for exactly one root task, duplicate
  ids, missing dependencies, and self-dependencies.

## Required Instance Data

The runtime reads `runtime/workspace.json` from the instance root. A thin
instance should define at least:

```json
{
  "workspaceRoot": ".",
  "defaultDataRoot": ".run/data",
  "domainsDir": "domains",
  "pipelinesDir": "pipelines",
  "contractsDir": "contracts",
  "defaultDomainId": "example-domain",
  "defaultPipelineId": "example-pipeline",
  "pipelineSelection": {
    "kindDefaults": {
      "product": "example-pipeline"
    }
  }
}
```

Common optional fields:

- `fanoutAliases`: maps logical axes such as `locale` or `channel` to request
  field names.
- `fanoutDefaults`: fallback values for fanout axes when request, pipeline, and
  domain defaults omit them.
- `contextContracts`: maps base, capability, stage, and product-kind contract ids
  into task contexts.
- `contextAudits`: maps base, capability, and stage audit names into task contexts.

## Pipeline Profiles

Pipeline profiles live under the instance `pipelinesDir` and are selected by
explicit request `pipelineId`, source `pipelineId`, `pipelineSelection`, or
domain/workspace defaults.

Supported planner fields:

- `extends`: inherit another pipeline profile, with child fields overriding parent
  fields and `defaults` merged shallowly.
- `defaults`: request defaults copied into planned task `product` when omitted.
- `stages`: ordered list of stage definitions.
- `number`: task stage suffix, for example `T1`, `T4.5`, or `T9`.
- `capability`: queue capability, used for `ready-<capability>/`.
- `stage`: worker-facing stage name.
- `dependsOn`: stage numbers that must complete before this stage can run.
- `fanout`: axis list such as `["locale"]` or `["locale", "channel"]`.
- `isRoot`: marks the final stage. For fanout stages, the alphabetically last
  task id is the single root.
- `inputAs` or `outputAs`: alias used when downstream stages bind this stage as
  an input.
- `inputsFrom`: additional upstream stages to bind as inputs without adding them
  as scheduling dependencies.
- `inputMap`: target-stage override for dependency aliases.
- `when` or `emitWhen`: simple data-driven condition for conditional stages.
- `taskFields`: extra fields copied into every emitted task for that stage.

## Contract Indexes

Runtime contracts live in `wo_runtime/contracts/`. Instance contracts live under
the configured `contractsDir`; domain contracts live under the selected domain
`resources.contractsDir`.

Add `contract-index.json` files to make ids data-driven:

```json
{
  "contracts": {
    "request.schema.json": "request.schema.json",
    "domain_specific.v1": "domain-specific.contract.json"
  }
}
```

Resolution order is instance contracts, domain contracts, then runtime contracts.
Domain profiles may still block external absolute resource paths through their
resolver policy.

## Invariants

- Runtime core writes only queue/runtime artifacts under the configured data root.
- Runtime core does not know product families, marketplace rules, safety guidance,
  provider choices, media layout policy, or dashboard behavior.
- A role invocation should call `plan-inbox --once` for planner behavior rather
  than reimplementing DAG logic in prompt text.
- Pipeline profiles are the source of truth for DAG topology, dependencies,
  fanout, root selection, and input aliases.
