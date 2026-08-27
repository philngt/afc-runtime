# woafc-runtime

Reusable file-queue runtime for WO/AFC-style agent workspaces.

This repository owns runtime-neutral queue mechanics, runtime adapter plumbing,
generic orchestrator behavior, reusable core planning/context helpers, hooks,
and synthetic selftests. Product/domain policy stays in the instance repository.

Run from an instance checkout without installation:

```bash
vendor/woafc-runtime/bin/wo-runtime --help
```

Core commands for thin instances:

```bash
vendor/woafc-runtime/bin/wo-runtime core plan --request-file request.json --dry-run
vendor/woafc-runtime/bin/wo-runtime core plan-inbox --workspace-root "$PWD" --once
vendor/woafc-runtime/bin/wo-runtime core context <task-id>
vendor/woafc-runtime/bin/wo-runtime core contract-path <contract-id>
vendor/woafc-runtime/bin/wo-runtime core validate-dag --req-id <request-id>
```

Reusable Python boundaries are also available without installing the runtime:

```python
from wo_runtime.artifacts import ContentAddressedStore, idempotent_write_json
from wo_runtime.status import RequestStatusReader
from wo_runtime.worker import BoundClaim, RuntimeTerminal, load_bound_task, resolve_task_inputs
```

`BoundClaim` validates the supervisor receipt and claimed path,
`resolve_task_inputs` applies strict dependency resolution, and
`RuntimeTerminal` commits through the same queue.v2 fencing boundary as the
CLI. A task with `outputContract` is schema-validated by the runtime before its
instance result hook and before any terminal file is written.

The core layer reads instance data from `runtime/workspace.json`, `domains/`,
`pipelines/`, and `contracts/contract-index.json`. It must not contain product
family literals; pipeline selection, fanout defaults, input aliases, and context
contract/audit policy are instance data.

## Optional plugins

Channel-specific integrations live as separately packaged, opt-in components
under `plugins/`, outside the channel-neutral `wo_runtime` package. A plugin may
depend on stable runtime contracts; the runtime core must never import a plugin.

The first optional plugin is
[`plugins/telegram-assistant`](plugins/telegram-assistant/README.md). It defines
the Bot API transport, assistant compatibility contract, process lifecycle, and
safety boundary for a single-workspace Telegram admin assistant. It remains
disabled until an instance explicitly starts it with a named host adapter; the
instance continues to own authorization, SQLite truth, approvals, and backends.

Keeping the plugin in this monorepo lets an instance pin one runtime commit while
the assistant contract is still evolving. It can be split into an independently
versioned repository later if it gains non-AFC consumers or a separate release
and security cadence.

## Build a new workflow instance

Use this repository as a pinned runtime dependency, not as the place for product
logic. A new workflow should be a thin instance that owns its configuration,
domain knowledge, DAG, contracts, provider descriptors, handlers, delivery
policy, and product tests.

At minimum, the instance should provide:

```text
config/                         non-secret operational/provider decisions
contracts/                      request and stage output contracts
domains/                        domain profiles and source policy
knowledge/                      product/editorial guidance
pipelines/                      data-driven DAG definitions
runtime/runtime-pin.json        exact runtime version and commit
runtime/workspace.json          instance paths, defaults, hooks, and context
runtime/roles.json              queue watches and role behavior
runtime/fleet.json              runtime, replicas, and hard budgets per role
runtime/runtimes/*.json         provider/dispatcher launch descriptors
scripts/runtime/                task runner, static validator, and selftest
src/                            receipt-bound product handlers
tests/                          deterministic fixtures and failure/replay tests
vendor/woafc-runtime/           exact pinned runtime source
```

The authoring agent must implement and verify a runnable instance rather than
stop at a proposal. Preserve `queue.v2` claim receipts, declare
`outputContract` for successful tasks, resolve dependency results strictly,
write immutable or content-addressed evidence, and require real delivery
receipts. Keep provider tokens outside the repository and inject them only into
the service that needs them.

The full construction sequence, validation matrix, and definition of done are
in [`docs/WORKFLOW-AUTHORING.md`](docs/WORKFLOW-AUTHORING.md).

### Copy-ready example prompt

The following shortened prompt creates a workflow similar in shape to a
bilingual short-video production instance. The complete prompt is in
[`docs/prompts/CREATE-SHORT-VIDEO-WORKFLOW.vi.md`](docs/prompts/CREATE-SHORT-VIDEO-WORKFLOW.vi.md).

```text
Hãy xây dựng hoàn chỉnh một workflow instance mới để sản xuất short video từ
GitHub Trending, repository chính thức, release notes, tài liệu kỹ thuật,
whitepaper và arXiv, dựa trên:

https://github.com/philngt/afc-runtime

Đọc AGENTS.md, docs/WORKFLOW-AUTHORING.md, docs/CORE.md, docs/QUEUE.md và
docs/PRIMITIVES.md trước khi sửa file. Hãy triển khai workflow runnable, không
chỉ viết proposal.

Yêu cầu:
- Tách product policy khỏi wo_runtime; giữ quyết định không bí mật trong
  config/*.json, knowledge/, domains/, pipelines/ và contracts/.
- Pin chính xác runtime commit; dùng queue.v2 receipt-bound workers và không di
  chuyển queue file thủ công.
- Xây dựng pipeline data-driven cho source discovery, evidence, editorial,
  risk/approval, fanout vi-VN/en-US, video production, aggregate, Telegram
  delivery và finalization.
- Mỗi successful task khai báo outputContract. Evidence và deliverable phải
  immutable hoặc content-addressed và tách theo attempt/claim.
- Video phải native 1080x1920 ở 30 fps và qua HyperFrames check, snapshot,
  render cùng ffprobe gate. TTS offline là mặc định; gTTS chỉ dùng cho tiếng
  Việt khi config cho phép và Piper là fallback offline.
- Provider/model là config của instance. Không đặt Codex, Claude hoặc AGY token
  trong repo hay prompt; host phải inject secret đúng service.
- Chỉ đánh dấu delivery thành công khi Telegram trả về hai message id thật.
- Tạo static validator, deterministic fixtures, unit tests và runtime selftest.
  Chứng minh invalid contract, missing dependency, stale claim, retry/replay,
  immutable conflict và missing delivery receipt đều fail closed.

Chạy và báo chính xác kết quả:
PYTHONPATH=src python3 -m unittest discover -s tests -v
python3 scripts/runtime/selftest.py
vendor/woafc-runtime/bin/wo-runtime selftest

Không báo hoàn tất nếu acceptance criteria bắt buộc chưa đạt hoặc live receipt
đang bị giả lập.
```

Documentation:

- `docs/CORE.md` - core planner/context command contract and required instance data.
- `docs/QUEUE.md` - safe paths, targeted leases, terminal idempotency, deadlines, and instance hooks.
- `docs/PRIMITIVES.md` - reusable Python APIs, ownership boundaries, contract validation, and worker examples.
- `docs/WORKFLOW-AUTHORING.md` - how an agent builds and verifies a new thin workflow instance.
- `docs/prompts/CREATE-SHORT-VIDEO-WORKFLOW.vi.md` - copy-ready Vietnamese example prompt.
- `plugins/telegram-assistant/README.md` - optional Telegram admin-assistant plugin boundary.
- `wo_runtime/contracts/README.md` - runtime-neutral queue/result/context contracts.

## License

Copyright 2026 Ngô Tấn Phúc.

Licensed under the [Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for
attribution information.
