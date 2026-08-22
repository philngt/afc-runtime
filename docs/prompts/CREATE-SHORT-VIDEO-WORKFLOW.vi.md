# Prompt mẫu: tạo workflow sản xuất short video

Sao chép khối prompt dưới đây và thay các giá trị trong ngoặc nhọn nếu cần.
Prompt cố ý yêu cầu agent triển khai và kiểm thử một instance riêng, không đưa
logic sản phẩm vào runtime core.

```text
Hãy xây dựng hoàn chỉnh một workflow instance mới để sản xuất short video từ
GitHub Trending, repository chính thức, release notes, tài liệu kỹ thuật,
whitepaper và arXiv. Dùng repository nền:

https://github.com/philngt/afc-runtime

Làm việc như một engineering agent: hãy đọc AGENTS.md,
docs/WORKFLOW-AUTHORING.md, docs/CORE.md, docs/QUEUE.md và
docs/PRIMITIVES.md trước khi sửa file. Hãy triển khai workflow runnable, không
chỉ viết proposal hoặc pseudocode.

Mục tiêu sản phẩm

- Mỗi request nhận một chủ đề, URL hoặc một lượt thu thập nguồn mới.
- Thu thập và đóng băng bằng chứng từ GitHub Trending, repository/organization
  chính thức, README, release, changelog, advisory, whitepaper, arXiv và tài
  liệu chính chủ.
- Chọn một góc kể có discovery arc rõ ràng, kiểm tra trùng lặp và rủi ro, rồi
  tạo short video dọc bằng tiếng Việt và tiếng Anh.
- Đầu ra video native 1080x1920, 30 fps, có voice, caption, provenance và một
  package tổng hợp có thể kiểm toán.
- Gửi hai video qua Telegram và chỉ đánh dấu delivery thành công khi có hai
  message id thật.

Ranh giới kiến trúc

- Giữ woafc-runtime nguyên tính domain-neutral. Product policy phải nằm trong
  instance qua config/*.json, knowledge/, domains/, pipelines/, contracts/,
  hooks và worker handlers.
- Vendor hoặc pin chính xác một commit runtime trong runtime/runtime-pin.json;
  không chỉ trỏ vào branch main.
- Dùng queue.v2 và receipt-bound worker. Không di chuyển queue file thủ công,
  không complete task không có lease/attempt/worker hợp lệ.
- Mỗi successful task phải khai báo outputContract. Runtime phải validate
  contract trước instance hook và trước terminal write.
- Evidence, snapshot và deliverable phải immutable hoặc content-addressed,
  tách theo task/attempt/claim để retry không ghi đè lần trước.

Hãy tạo tối thiểu

- AGENTS.md và README vận hành cho instance.
- runtime/workspace.json, runtime/roles.json, runtime/fleet.json,
  runtime/runtimes/*.json và runtime/runtime-pin.json.
- domain profile, knowledge/source policy và một pipeline data-driven.
- contract-index cùng schema chặt cho request và đầu ra từng stage.
- Python package trong src/, receipt-aware task runner, instance hooks, static
  validator, runtime selftest và unit tests.
- Fixture nguồn/evidence/video metadata để test không phụ thuộc credential hoặc
  network thật.

Pipeline tham chiếu

1. source-discovery: thu thập ứng viên, canonicalize URL, đóng băng snapshot và
   provenance; fail rõ ràng nếu không đủ nguồn chính.
2. research-evidence: lập evidence pack, liên kết từng claim với nguồn và đánh
   dấu mức độ đầy đủ.
3. editorial-angle: chọn hook, discovery arc, thông tin mới và phạm vi không
   được phóng đại.
4. risk-review: kiểm tra trùng lặp, factual risk, license/safety và quyết định
   auto/ask/block bằng dữ liệu.
5. approval-gate: tiêu thụ quyết định operator bền vững khi mode là ask.
6. locale-production: fanout vi-VN/en-US, tạo script, narration, caption, scene
   spec, audio và video cho từng locale.
7. aggregate-package: join đủ hai locale, tạo manifest và release evidence.
8. telegram-delivery: upload từng video bằng outbox idempotent và lưu receipt có
   message id thật.
9. delivery-finalize: chỉ thành công khi package hợp lệ và đủ hai receipt.

Cho phép thay đổi topology nếu có lý do tốt hơn, nhưng DAG phải data-driven,
không có dependency thiếu, không có self-cycle và chỉ có một logical root.

Media và TTS

- HyperFrames phải render native 1080x1920 ở 30 fps và qua check, snapshot,
  render, ffprobe cùng release gate thực.
- TTS offline là mặc định. Nếu sản xuất tiếng Việt cần network TTS thì chỉ dùng
  gTTS khi config cho phép và giữ Piper làm fallback offline; không tự thêm cloud
  fallback khác.
- Không coi filename, file rỗng, mock success hoặc metadata tự khai là bằng chứng
  video đã đạt gate.

Provider và secrets

- Provider selection, model, reasoning effort, timeout và concurrency là dữ
  liệu của instance; không hardcode vào wo_runtime.
- Với Codex, dùng structured output schema và không pin model bằng CLI flag;
  model được quyết định qua config.
- Có thể thêm descriptor cho Claude hoặc AGY, nhưng không đặt token trong repo,
  JSON, prompt hay .env chung. Secret do host inject đúng service và phải có
  env allowlist riêng cho từng provider.
- .env của instance chỉ được chứa TELEGRAM_BOT_API và TELEGRAM_ADMIN_ID.

Acceptance criteria

- Static validator chứng minh runtime pin, descriptor, contract index, provider
  config, source allowlist và pipeline đều hợp lệ.
- Dry-run một fixture request tạo đúng DAG, đúng fanout vi-VN/en-US và đúng một
  root; validate-dag pass.
- Unit tests bao phủ input thiếu/failed, invalid output contract, stale claim,
  retry/replay, immutable conflict, human approval và delivery thiếu receipt.
- Chạy và báo kết quả chính xác của:

  PYTHONPATH=src python3 -m unittest discover -s tests -v
  python3 scripts/runtime/selftest.py
  vendor/woafc-runtime/bin/wo-runtime selftest

- Nếu môi trường có đầy đủ media dependency, chạy thêm HyperFrames check,
  snapshot, render và ffprobe trên fixture đại diện. Nếu thiếu credential hoặc
  binary bên ngoài, fixture test vẫn phải pass và handoff phải nêu rõ live gate
  nào chưa chạy; không giả lập receipt thật.

Trong quá trình làm, hãy giữ các quyết định không bí mật trong config/*.json và
knowledge/, bảo toàn queue.v2 fenced receipts, không sửa hoặc xóa dữ liệu queue
thật, và không báo hoàn tất nếu các acceptance criteria bắt buộc chưa đạt.
```
