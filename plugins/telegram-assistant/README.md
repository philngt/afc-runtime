# afc-telegram-assistant

Optional Telegram ingress for an AFC workflow's admin assistant.

The plugin lives in the `afc-runtime` monorepo but remains a separate Python
package. The runtime owns channel-neutral execution primitives, while this
directory owns Telegram transport behavior. A workflow instance supplies its
authorization, storage, approval, prompt, and runtime-backend adapters.

## Status

The plugin owns the dependency-free Bot API transport, single-workspace process
lifecycle, clean signal handling, and runtime-neutral assistant contracts. The
host workflow injects one explicit gateway factory that owns authorization,
SQLite state, approval rules, prompts, and runtime backends.

## Dependency direction

```text
workflow instance
├── wo_runtime                          channel-neutral runtime core
└── plugins/telegram-assistant          optional Telegram transport
```

The plugin may consume a stable AFC assistant contract. `wo_runtime` must not
import this plugin.

## Initial safety contract

- disabled unless explicitly started by a workflow;
- one workspace per process;
- exact user and private-chat allowlist, with missing configuration denied;
- outbound long polling by default, with no public listener;
- Telegram content is untrusted data, never runtime instructions;
- workspace writes and human approvals remain host policy;
- provider credentials are never forwarded to the Telegram transport or agent
  subprocesses that do not require them.

## Development

From this directory:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
PYTHONPATH=src python3 -m compileall -q src tests
PYTHONPATH=src python3 -m afc_telegram_assistant --version
```

Consumers pin the enclosing `afc-runtime` repository and start the plugin
explicitly:

```bash
python plugins/telegram-assistant/bin/afc-telegram-assistant run \
  --workspace-root /absolute/workspace \
  --factory workflow.telegram.plugin:create_gateway
```

Arguments after `--` are delivered only to the explicit host factory. Dynamic
plugin discovery is intentionally absent: one process loads one named adapter
and binds one resolved workspace.
