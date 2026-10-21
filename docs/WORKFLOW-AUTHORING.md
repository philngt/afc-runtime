# Workflow Instance Authoring Guide

This guide is the runtime-side instruction for an engineering agent asked to
build a new workflow instance. The desired output is a runnable, tested thin
instance that consumes an exact `woafc-runtime` commit. It is not a proposal,
diagram, or product-specific branch inside this repository.

## Runtime and instance boundary

Keep this split explicit throughout the implementation:

| Runtime responsibility | Instance responsibility |
| --- | --- |
| Queue paths, claims, leases, fencing, retries, terminal commits | Domain stages, handlers, source and delivery policy |
| Generic planner, context, contract loading, status projection | Workspace, pipeline, roles, fleet, and runtime descriptors |
| Atomic and immutable artifact primitives | Artifact schemas, meaning, retention, and presentation |
| Provider-neutral process adapter plumbing | Provider selection, models, binary descriptors, and non-secret defaults |
| Synthetic runtime selftests | Product fixtures, end-to-end tests, and release gates |

Do not add product names, source allowlists, locales, channels, provider names,
model choices, media layouts, or safety thresholds to `wo_runtime/`. If a
missing primitive is reusable across unrelated workflows, implement and test it
in the runtime separately; otherwise keep it in the instance.

## Information to derive from the brief

Before writing the DAG, turn the user brief into explicit instance data:

- workflow id, domain id, product kind, and default pipeline;
- accepted ingress forms and request idempotency key;
- source classes, host allowlists, freshness rules, and frozen evidence format;
- output artifacts and the contract for every stage;
- fanout axes such as language, channel, format, or customer;
- human-review points and the conditions for automatic routing;
- delivery destinations and the receipt that proves each delivery;
- provider, model, concurrency, timeout, retry, and offline/network policy;
- success, quality, security, privacy, and observability gates.

Record safe defaults in `config/*.json`, `knowledge/`, domain profiles, or
pipeline data. Record an unresolved product decision as an explicit blocking
item; do not hide it in prompt text or hardcode it in runtime code.

## Minimum instance layout

A thin Python instance normally contains:

```text
<workflow>/
├── AGENTS.md
├── README.md
├── config/                    # non-secret operational and provider decisions
├── contracts/
│   └── contract-index.json
├── domains/<domain-id>/
│   └── domain.json
├── knowledge/                 # editorial/domain guidance and source policy
├── pipelines/<pipeline-id>.json
├── runtime/
│   ├── runtime-pin.json
│   ├── workspace.json
│   ├── roles.json
│   ├── fleet.json
│   └── runtimes/<name>.json
├── scripts/runtime/
│   ├── task-runner.py
│   ├── validate-instance.py
│   └── selftest.py
├── src/<instance_package>/
├── tests/
└── vendor/woafc-runtime/      # exact pinned source, submodule, or release export
```

Additional folders are product choices. Keep generated queue state under the
configured data root, normally `.run/data`, and keep it out of source control.

## Build sequence

### 1. Freeze the runtime boundary

Vendor or otherwise resolve one exact runtime commit. Add
`runtime/runtime-pin.json` with the runtime name, version, commit, source path,
CLI path, and `queue.v2`. The pin and vendored marker must agree. Never claim
reproducibility from a moving branch name alone.

### 2. Define workspace data

Create `runtime/workspace.json` with the workspace root, data root, domain,
pipeline, contract, role, fleet, and descriptor locations. Put fanout aliases,
pipeline selection, context contracts, context audits, and instance hook module
names here when needed.

Create one domain profile and one default pipeline. Product selection and
defaults must be data-driven so a second workflow can reuse the runtime without
runtime conditionals.

### 3. Design contracts before handlers

Define a strict request contract and a strict output contract for every stage.
Register stable ids in `contracts/contract-index.json`. Prefer additive versioned
contracts; do not silently change the meaning of an existing id.

Every successful task should declare `outputContract`. The runtime validates it
before the optional instance result hook and before writing the terminal file.
Use the instance hook only for cross-artifact or domain invariants that JSON
Schema cannot express.

Provider structured output can improve contract adherence, but valid structure
does not prove factual correctness, artifact quality, or successful delivery.
Keep runtime validation, claim receipts, and instance correctness checks at the
commit boundary.

### 4. Compile the pipeline DAG from data

Each pipeline stage should declare:

- a stable stage number, capability, and worker-facing name;
- scheduling dependencies and any extra `inputsFrom` bindings;
- input/output aliases rather than handler-specific path assumptions;
- fanout axes, conditions, timeout/retry policy, and root selection;
- the output contract and only the task fields required by that stage.

Choose task boundaries where work needs its own output contract, retry,
parallel execution, or human gate. A task can autonomously use several tools,
inspect evidence, and refine its result within one attempt. A separate queue
task is unnecessary for each reasoning step; use durable checkpoints where
independent scheduling or recovery is useful.

Stage declarations may appear in any order. The compiler validates all stage
references before conditions and sorts prerequisites before consumers, retaining
an already valid declaration order. `inputsFrom` binds an existing upstream
result without adding a scheduling edge; the source must already be a direct
or transitive `dependsOn` prerequisite. Otherwise planning fails with
`input_without_dependency`.

Conditions and fanout are resolved at compile time from the request and
defaults. References to known stages that emit no tasks are pruned; a consumer
is omitted when all its declared dependencies are omitted. An active dependency
without a compatible fanout variant skips that consumer variant. These rules
do not provide routing on later task results.

There must be one emitted root whose dependency ancestry covers every emitted
branch. An explicit fanout root is valid only when one variant emits; mutually
exclusive conditional roots are valid when only one emits. Without an explicit
emitted root, the compiler requires one terminal task. Add a final join for
multiple branches instead of relying on alphabetical or declaration order.
Unknown references and cycles in stage references fail before workers run.
See [Core planner](CORE.md#example-unordered-fanout-and-join) for an unordered
fanout/join example and migration guidance. These compiler guarantees do not
change the standalone `validate-dag` command's documented scope.

### 5. Configure roles, fleet, and providers

`runtime/roles.json` describes queue watches and role behavior.
`runtime/fleet.json` selects a runtime, replica count, and hard runtime budget for
each role. `runtime/runtimes/<name>.json` describes how that instance launches a
runtime binary or dispatcher.

Provider secrets never belong in runtime source, descriptor JSON, normal config,
or prompts. Store model and reasoning choices in instance config. Inject tokens
or CLI auth profiles through the host secret manager and only into the service
that needs them. If multiple providers are enabled, require provider-specific
environment allowlisting before injecting credentials.

### 6. Implement one-pass receipt-bound workers

A role invocation claims or receives exactly one task, performs one pass,
commits or explicitly requeues it, reports briefly, and exits. Use the runtime
worker boundary rather than scanning or moving queue files:

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
    root = Path.cwd()
    claim = BoundClaim.from_environment(root)
    if claim is None:
        raise WorkerError("a supervised worker requires a bound claim")
    terminal = RuntimeTerminal(root, claim)
    try:
        task = load_bound_task(claim)
        inputs = resolve_task_inputs(root, task)
        result = handler(task, inputs)
    except Exception as exc:
        terminal.fail(str(exc))
        raise
    terminal.complete(result)
```

Workers must consume frozen inputs, write attempt/claim-bound artifacts, and be
safe to replay. Use content-addressed or idempotent immutable writes for evidence
and deliverables. A stale lease, missing dependency result, conflicting replay,
or invalid output contract must fail closed.

### 7. Add product hooks and delivery proof

Use instance hooks for request normalization, domain result invariants, or final
release decisions. Hooks must not mutate terminal payloads and must not bypass
queue fencing.

Delivery is complete only when the configured destination returns the required
real receipt. Store the receipt in a typed terminal artifact. A local file,
rendered video, attempted API call, or provider response without the required
receipt is not delivery success.

### 8. Expose neutral status

Use `RequestStatusReader` for queue-derived progress and map it to product/UI
language in the instance. Do not infer success from expected task names when
materialized envelopes or terminal results exist. Preserve the original root
failure rather than replacing it with a cascade of dependency failures.

## Required verification

Create deterministic fixtures that do not require live provider credentials.
The instance must have a single selftest command that runs its static checks and
the runtime adapter checks. At minimum, verify:

1. runtime pin and vendored commit agree;
2. all JSON files parse and all referenced files exist;
3. contract ids resolve and representative valid/invalid artifacts behave as
   expected;
4. representative condition and fanout combinations compile to the expected
   DAG, with input dependency paths and one root covering every emitted branch;
5. every fleet role has a valid runtime descriptor and hard runtime budget;
6. a receipt-bound worker accepts a valid claim and rejects partial, stale, or
   escaping bindings;
7. missing/failed dependency results stop downstream execution;
8. terminal writes are fenced, idempotent, and contract-validated before write;
9. restart/retry does not overwrite an earlier attempt or immutable artifact;
10. delivery cannot pass without its real receipt;
11. secrets are absent from committed files, prompts, artifacts, logs, and child
    processes that do not need them;
12. product release gates run against real output metadata, not filenames or
    mocked success flags.

Typical commands are:

```bash
vendor/woafc-runtime/bin/wo-runtime selftest
vendor/woafc-runtime/bin/wo-runtime core plan \
  --workspace-root "$PWD" --request-file fixtures/request.json --dry-run
vendor/woafc-runtime/bin/wo-runtime core validate-dag \
  --workspace-root "$PWD" --req-id <request-id>
PYTHONPATH=src python3 -m unittest discover -s tests -v
python3 scripts/runtime/selftest.py
```

If the product creates media or deployable packages, add its native validation,
snapshot/render, metadata, size, and delivery smoke gates after these generic
checks.

## Definition of done

The authoring task is complete only when:

- the new instance is runnable from a clean checkout with documented host
  prerequisites;
- configuration, contracts, DAG, workers, hooks, and tests are implemented;
- a fixture request can be planned and driven through representative terminal
  states without manual queue-file movement;
- failure, retry, replay, cancellation, and delivery-receipt behavior are
  proven;
- every required command passes and the README explains how to operate it;
- the handoff lists exact commands run, assumptions retained in config, and any
  external credentials or live delivery intentionally not exercised.

For a complete natural-language example, see
`docs/prompts/CREATE-SHORT-VIDEO-WORKFLOW.vi.md`.
