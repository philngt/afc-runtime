# Agent Runtime Guidance

Domain: reusable **WO/AFC file-queue runtime**.

This repository is runtime-neutral. It owns queue mechanics, the generic
orchestrator, runtime adapter plumbing, reusable planner/context helpers,
runtime-level contracts, hooks, and synthetic selftests. Product/domain policy
belongs in instance repositories.

## Ownership

This repo owns:

- `bin/wo-runtime`
- `wo_runtime/`
- `wo_runtime/contracts/`
- `docs/`
- runtime-neutral selftests in `wo_runtime/testing.py` and
  `wo_runtime/runtime/selftest.py`

Instance repositories own:

- `runtime/workspace.json`
- `runtime/roles.json`
- `runtime/runtimes/*.json`
- `runtime/fleet.json`
- `domains/`
- `pipelines/`
- instance `contracts/`
- media/provider scripts, dashboards, skills, examples, and product docs

## Rules

- Keep runtime code domain-neutral. Do not add product-family, marketplace,
  locale, channel, provider, media-layout, safety-policy, or listing-policy
  literals to `wo_runtime/`.
- Queue contracts in `wo_runtime/contracts/` are the reusable compatibility
  surface. Do not rename existing fields; prefer optional additive fields.
- A role invocation performs exactly one pass, reports briefly, and stops. The
  runtime schedules later work.
- Runtime code must resolve workspace settings through `wo_runtime.workspace`.
  Do not hardcode an instance path except for documented defaults such as
  `.run/data`.
- Runtime artifacts belong under the configured data root. Heartbeats belong in
  `<dataRoot>/heartbeats/`; legacy `.run/heartbeats/` support is compatibility
  only.
- Add instance-specific behavior through instance data or hooks, not through
  runtime conditionals.
- Runtime descriptors are data. Adding a runtime should normally require only an
  instance `runtime/runtimes/<name>.json` descriptor and selftest coverage.

## Creating workflow instances

When a user asks to build a new workflow based on this repository:

1. Read `docs/WORKFLOW-AUTHORING.md` before creating files.
2. Create or modify a thin instance repository; keep product and provider policy
   out of `wo_runtime/`.
3. Implement the runnable workflow, contracts, tests, and validation gates. Do
   not stop at an architecture proposal when the user asked to build it.
4. Pin the exact runtime commit used by the instance and preserve `queue.v2`
   receipt fencing.
5. Use `docs/prompts/CREATE-SHORT-VIDEO-WORKFLOW.vi.md` as a concrete prompt
   example, adapting its product requirements rather than copying them into the
   runtime core.

## Commands

Run from this repo:

```bash
bin/wo-runtime selftest
python3 -m py_compile $(find wo_runtime -name '*.py' | sort)
```

Run from an instance checkout:

```bash
vendor/woafc-runtime/bin/wo-runtime ensure-data-root --workspace-root "$PWD"
vendor/woafc-runtime/bin/wo-runtime runtime selftest --workspace-root "$PWD"
vendor/woafc-runtime/bin/wo-runtime core plan --workspace-root "$PWD" --request-file request.json --dry-run
vendor/woafc-runtime/bin/wo-runtime core plan-inbox --workspace-root "$PWD" --once
vendor/woafc-runtime/bin/wo-runtime core context --workspace-root "$PWD" <task-id>
vendor/woafc-runtime/bin/wo-runtime core validate-dag --workspace-root "$PWD" --req-id <request-id>
```

## Before Commit

- Run `git diff --check`.
- Run `bin/wo-runtime selftest` for runtime behavior changes.
- Run `vendor/woafc-runtime/bin/wo-runtime runtime selftest --workspace-root
  "$PWD"` from at least one instance when changing runtime adapters,
  descriptors, supervisor behavior, or workspace resolution.
- Keep code changes scoped to runtime reuse. Generic authoring instructions and
  clearly labeled prompt examples may live in `docs/`; product implementation,
  operating policy, checklists, and skills belong in the instance/template repo.
