# woafc-runtime

Reusable file-queue runtime for WO/AFC-style agent workspaces.

This repository owns runtime-neutral queue mechanics, runtime adapter plumbing,
generic orchestrator behavior, hooks, and synthetic selftests. Product/domain
policy stays in the instance repository.

Run from an instance checkout without installation:

```bash
vendor/woafc-runtime/bin/wo-runtime --help
```
