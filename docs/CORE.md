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
- `plan --write` publishes a complete request plan and its pending task files
  under the configured data root (default `.run/data`).
- `plan-inbox --once` compiles at most one `.run/data/inbox/*.json` request,
  publishes the plan, moves the request to `inbox/processed/`, and appends a
  `planned` or `plan_replayed` event.
- `context` builds a compact task context with task, resolved inputs, domain,
  pipeline, product kind, contract refs, guidance refs, audits, and artifact refs.
- `contract-path` resolves runtime, instance, and domain contract indexes.
- `validate-dag` checks one planned request for exactly one root task, duplicate
  ids, missing dependencies, self-dependencies, and cycles across multiple tasks.

## Request plan publication

`plan --write` and `plan-inbox` validate the full DAG before publication. The
runtime writes an immutable `plans/<request-id>/manifest.json`, materializes
each task in `tasks/pending/`, then writes `plans/<request-id>/committed.json`
last. Every managed task carries the manifest's SHA-256 as `plan_digest`.
The manifest and marker use `request_plan.v1`. Paths are relative to the
configured data root.

The marker is the visibility boundary: queue mutation, scheduler discovery,
orchestration, and status readers reject or defer uncommitted work. Request
finalization also requires the complete task set declared by the manifest;
a missing sibling cannot make a partial request appear complete.

Publication is replay-safe:

- Repeating an identical request and compiled plan is a no-op after commitment,
  even when tasks have moved to ready, claimed, or done.
- If publication stops before the marker, rerunning the same input resumes the
  immutable pending files and publishes the marker after all tasks exist.
- A different request or compiled plan with the same request ID is rejected.
  Existing task destinations and results are also protected from overwrite.
- Inbox conflicts emit `plan_rejected` and archive the incoming request without
  replacing existing work or its final result. Interrupted I/O leaves the input
  in the inbox for retry. A parse/compile failure creates a failed final only
  when the request ID has no existing state.

Request publication locks do not expire while their valid owner process is
alive. Dead owners can be recovered immediately; incomplete lock metadata has
a 15-minute grace period before recovery. A retry does not recreate missing
task files after commitment: that inconsistency remains blocked for inspection.

This boundary binds the compiled graph and the task fields used for scheduling,
input references, output-contract ID, and root/manual behavior. It does not pin
contract file contents, instance code, guidance, provider configuration, or all
custom task fields. Execution snapshots are a separate future phase described
in [Evolution](EVOLUTION.md).

Existing `queue.v2` tasks without a request manifest remain supported. A managed
task cannot opt out by removing `plan_digest`; the request directory still
requires the binding. A task with a digest but no manifest is also rejected.

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
