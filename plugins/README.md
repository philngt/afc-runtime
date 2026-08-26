# Optional AFC runtime plugins

This directory contains channel- or service-specific integrations that benefit
from sharing one repository and commit pin with `afc-runtime` but do not belong
inside the channel-neutral `wo_runtime` package.

Rules for every plugin:

- it is disabled until an instance explicitly starts it;
- dependencies flow from the plugin to stable runtime contracts, never from
  `wo_runtime` to the plugin;
- product policy, credentials, allowlists, models, and approval decisions stay
  in the consuming instance or its secret manager;
- it is packaged and tested independently;
- adding a plugin does not add a dynamic plugin loader to the runtime core.

Available scaffolds:

- [`telegram-assistant`](telegram-assistant/README.md) — optional Telegram
  ingress for one workflow's admin assistant.
