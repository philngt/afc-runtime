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

V1 ownership:

- `request.schema.json`, `task.schema.json`, and `result.schema.json` define the
  domain-neutral queue envelope.
- `recovery.schema.json`, `task-context.schema.json`, and
  `audit-report.schema.json` are shared workflow artifacts that avoid concrete
  product/channel enums.
- Instance schemas remain the compatibility surface for product-specific
  validation until those constraints are split into separate instance contracts.
