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
- Keep changes scoped to runtime reuse. Move instance-building guidance,
  checklists, skills, and product-specific docs to the instance/template repo.
