Runtime Contracts
=================

This directory contains reusable engine contracts for the file-queue runtime.
They describe only the generic queue and orchestration surface that the runtime
needs across instances.

The product instance may keep stricter or richer schemas in its own
`contracts/` directory. For example, an instance-level `task.schema.json` can
restrict `capability` to product roles, while this runtime-level schema keeps
`capability` open because adapter descriptors define capabilities per
workspace.

Contract surface:

- `request.schema.json`, `task.schema.json`, and `result.schema.json` define the
  domain-neutral queue envelope.
- `asset-manifest.schema.json` defines a reusable generated-asset inventory for
  image, audio, video, book, and package workflows.
- `recovery.schema.json`, `task-context.schema.json`, and
  `audit-report.schema.json` are shared workflow artifacts that avoid concrete
  product/channel enums.
- Instance schemas remain the compatibility surface for product-specific
  validation until those constraints are split into separate instance contracts.

Queue ownership and mutation semantics are versioned as `queue.v2`; see
`docs/QUEUE.md`. Existing envelope field names remain stable. Claimed tasks
add `claim.lease_id`, `claim.attempt`, `claim.claimed_at`, and
`claim.lease_expires_at`; owned terminal results add `lease_id`, `attempt`, and
`result_digest`.
