# AFC Telegram Assistant coding notes

- This directory is an optional Telegram ingress plugin. It depends on stable
  assistant/runtime contracts; `wo_runtime` must never depend on this plugin.
- Keep provider implementations and literals such as Codex, Claude, or AGY out
  of the transport layer. A host workflow injects its configured backends.
- Bind one running plugin process to exactly one workspace until an explicit,
  authenticated multi-workspace routing design is approved.
- Long polling is the safe default. Do not add a public webhook listener or
  broaden a host workspace's loopback-only boundary implicitly.
- Fail closed when the bot token or exact user/chat allowlist is absent. Never
  write tokens to source, config, command arguments, logs, traces, or child
  environments.
- Treat Telegram messages and attachments as untrusted input. The host owns
  authorization, approval, retention, and workspace-write policy.
- The plugin may own transport cursors and delivery receipts. Workflow/session
  truth remains in the host's authoritative store through an injected adapter.
- Keep the Python runtime dependency-free unless the standard library cannot
  safely meet a demonstrated requirement.
- Prefer additive, versioned contracts and preserve compatibility aliases until
  every pinned workflow has migrated.
- Run `PYTHONPATH=src python3 -m unittest discover -s tests -v` and
  `PYTHONPATH=src python3 -m compileall -q src tests` after changes.
