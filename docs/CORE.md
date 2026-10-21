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
- `stages`: stage definitions; declaration order breaks ties between stages whose
  prerequisites are ready for compilation.
- `number`: task stage suffix, for example `T1`, `T4.5`, or `T9`.
- `capability`: queue capability, used for `ready-<capability>/`.
- `stage`: worker-facing stage name.
- `dependsOn`: stage numbers that must complete before this stage can run.
- `fanout`: axis list such as `["locale"]` or `["locale", "channel"]`.
- `isRoot`: marks the final stage. Exactly one emitted task may be marked, and
  every other emitted task must be its direct or transitive prerequisite.
- `inputAs` or `outputAs`: alias used when downstream stages bind this stage as
  an input.
- `inputsFrom`: additional upstream stages to bind as inputs without adding
  scheduling dependencies. Their emitted input tasks must already be direct or
  transitive `dependsOn` prerequisites.
- `inputMap`: target-stage override for dependency aliases.
- `when` or `emitWhen`: condition evaluated against request data and defaults at
  compile time, before any task runs.
- `taskFields`: extra fields copied into every emitted task for that stage.

### Compilation and conditional dependencies

The compiler validates all stage identities, capabilities, root markers, and
`dependsOn`/`inputsFrom` references before evaluating conditions. Duplicate stage
numbers, unknown references, self-references, and cycles in these references are
errors, including in stages whose conditions would be false.

Stages are compiled in a stable topological order using both `dependsOn` and
`inputsFrom`. An already valid declaration order is preserved. `inputsFrom`
affects compilation order only; it does not add a queue scheduling edge. Every
emitted input source must be reachable through the consumer's `depends_on`
chain, or planning fails with `input_without_dependency`. If the input is a
prerequisite, declare the necessary `dependsOn` relationship; a direct edge is
unnecessary when a transitive dependency already guarantees completion.

A reference to a known stage that emits no tasks is pruned. If all declared
`dependsOn` stages emit no tasks, the consumer is omitted too. If a declared
`dependsOn` stage emits tasks but none match a consumer variant's shared fanout
axes, that consumer variant is skipped instead of losing the dependency. A
consumer with no fanout axes matches all emitted variants of each dependency,
providing a join. Skips caused by empty fanout or unavailable dependency variants
are reported in the plan's `warnings`.

Conditions select the graph for the current request; they do not route on
results produced later or modify the graph while workers execute it.

### Root selection and branch coverage

Root selection happens after conditions and fanout expansion. An explicit root
must be a single emitted task whose dependency ancestry includes every other
emitted task. Multiple conditional `isRoot` declarations are allowed when only
one emits, and a fanout root is valid when it emits exactly one variant.
Multiple emitted roots are an error; use an instance-owned final join when
several branches contribute to the result.

When no emitted task is explicitly marked, the compiler selects the unique
terminal task: a task that no other task depends on. Multiple terminal tasks are
ambiguous and cause an error. The compiler never chooses a final result by task
id or declaration order. These profile compilation checks do not expand the
standalone `validate-dag` checks listed above.

### Example: unordered fanout and join

This profile deliberately lists the final stage first. The contract ids are
examples to define in the instance contract index.

```json
{
  "id": "example-fanout-join",
  "defaults": {"variants": ["a", "b"]},
  "stages": [
    {
      "number": "T3", "capability": "finalize", "stage": "finalize",
      "dependsOn": ["T2"], "inputsFrom": ["T1"], "isRoot": true,
      "taskFields": {"outputContract": "example.final.v1"}
    },
    {
      "number": "T2", "capability": "transform", "stage": "transform",
      "dependsOn": ["T1"], "fanout": ["variant"], "inputAs": "parts",
      "taskFields": {"outputContract": "example.part.v1"}
    },
    {
      "number": "T1", "capability": "prepare", "stage": "prepare",
      "inputAs": "source",
      "taskFields": {"outputContract": "example.source.v1"}
    }
  ]
}
```

For request `REQ`, emission order is `REQ.T1`, `REQ.T2.a`, `REQ.T2.b`, then
`REQ.T3`. The final task waits for both transform variants. Its extra `T1` input
is already protected by the transitive dependency through `T2`.

### Migrating existing profiles

Already ordered profiles with one final task covering their branches retain
their emission order. Replace reliance on an alphabetically selected fanout
root or a last-listed fallback root with a final task that depends on all
required branches. Fix unknown or invalid references even inside inactive
stages; they are no longer silently ignored. Review `inputsFrom` bindings for a
real dependency path, and run `core plan --dry-run` for representative condition
and fanout combinations before deploying the profile.

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

## Shared schema, artifact, and status primitives

- `wo_runtime.core.schema` loads and validates the dependency-free JSON Schema
  subset used by runtime and instance contracts. All `$ref` values fail closed.
- `wo_runtime.artifacts` provides atomic JSON/text/byte writes, idempotent
  immutable JSON writes, and a verified content-addressed store.
- `wo_runtime.status.RequestStatusReader` projects request/task state from
  materialized task envelopes and terminal results. Callers may provide neutral
  expected task envelopes only as a pre-planning compatibility fallback.
- `wo_runtime.core.context.resolve_inputs(..., strict=True)` turns missing,
  malformed, or unsuccessful dependency results into execution errors. The
  default warning mode remains available for diagnostic context assembly.

## Invariants

- Runtime core writes only queue/runtime artifacts under the configured data root.
- Runtime core does not know product families, marketplace rules, safety guidance,
  provider choices, media layout policy, or dashboard behavior.
- A role invocation should call `plan-inbox --once` for planner behavior rather
  than reimplementing DAG logic in prompt text.
- Pipeline profiles are the source of truth for DAG topology, dependencies,
  fanout, root selection, and input aliases.
