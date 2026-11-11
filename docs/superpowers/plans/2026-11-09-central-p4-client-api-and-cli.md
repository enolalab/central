# Central P4 — Public Client API and CLI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Hoàn thiện một API công khai có tài liệu để CLI và client độc lập vận hành toàn bộ Central; không xây hoặc đóng gói UI.

**Architecture:** Một daemon local sở hữu state, scheduling, approval, budget và verification. HTTP handlers ánh xạ public DTO sang application use cases; CLI và client ngoài dùng cùng API. Client không đọc DB, không quản lý state machine và không nói trực tiếp với Codex thay Central.

**Tech Stack:** Rust/Tokio/Axum/Clap hiện có; HTTP JSON `/v1`, OpenAPI 3.1.2 và SSE. Không React, Canvas, Vite, frontend bundle hoặc Node ở runtime.

**Spec:** `docs/superpowers/specs/2026-11-09-central-mvp-design.md`, phạm vi CLI/API sửa ngày 2026-11-10; prerequisite P3 và contracts plan tổng. Plan này thay `2026-11-09-central-p4-office-and-release.md`.

## Global Constraints

- Local-first, Rust, cá nhân trước và có khả năng mở rộng.
- Central không phải agent hay harness.
- Không xây hoặc đóng gói UI; CLI là client chính thức, client ngoài dùng API có tài liệu.
- API mở cho tích hợp, vẫn xác thực/cấp quyền; MVP loopback/local, một user.
- Mọi sửa đổi kế hoạch cần user duyệt; chat không tự cấp approval.
- Hai vòng sửa sau lần verify đầu tiên, cấu hình được theo task.
- Một ngân sách chung cho mọi vai trò; MVP không có quỹ dự phòng.
- Usage không xác định khi có giới hạn tiền/token: giữ chờ việc mới trong run đó.
- Sau restart chờ `central resume`; pause/hold để attempt đang chạy hoàn thành.
- Windows x64 trước; không HA, worker remote hoặc RBAC tổ chức trong MVP.
- Real-model test mặc định gpt-6-luna/low; không fallback model âm thầm.
- Không đưa credentials vào URL/log/event/OpenAPI examples; secret answer chỉ ở memory.
- Không tự xóa evidence/workspace; GC phải tường minh và không xóa blob còn được tham chiếu.

## Review Focus

1. Client retry sau mất response: cùng command ID/payload trả receipt cũ, payload khác trả 409; không gửi lại agent mù — bước 1, 7, 15.
2. Hai client cùng duyệt một target: revision/hash/epoch cũ trả 409 và không cấp quyền — bước 1, 7, 15.
3. Snapshot cạnh commit hoặc reconnect sau restart: cursor không làm mất event, instance đổi yêu cầu resnapshot, không tự resume — bước 1, 5, 15.
4. Browser origin lạ, client chỉ đọc hoặc agent token: không vượt quyền owner; SSE vẫn cần auth và không nhận token qua query — bước 1, 3, 13.
5. Artifact/log path escape, dữ liệu lớn hoặc secret: chỉ đọc tài nguyên thuộc run, phân trang/bounded stream và redaction — bước 1, 5, 15.

---

### Task 1: API công khai hoàn chỉnh cho CLI và client độc lập

**Deliverable duy nhất:** Client chỉ dựa vào tài liệu công khai có thể kết nối,
giao việc, theo dõi, phê duyệt, xử lý gián đoạn và đọc kết quả đã verify qua daemon.
CLI làm được cùng các thao tác. P4 có đúng một task; các bước dưới đây là checklist
triển khai và kiểm chứng bên trong task, không phải các task UI riêng.

**Files:**

- Create `contracts/openapi-v1.yaml`, `contracts/events-v1.schema.json`, `contracts/examples/` cho payload đã redaction.
- Create `crates/central-server/src/api/{mod.rs,dto.rs,errors.rs,read_models.rs}` cho wire contract riêng và projection; không serialize thẳng toàn bộ `RunState` ra public API.
- Create `crates/central-server/src/http/{meta.rs,clients.rs,events.rs,plans.rs,tasks.rs,chat.rs,profiles.rs,artifacts.rs,logs.rs,usage.rs,recovery.rs,sync.rs,results.rs,maintenance.rs}`; extend `http/{mod.rs,auth.rs,commands.rs,runs.rs,inbox.rs,attempts.rs}`.
- Modify `crates/central-server/src/{bootstrap.rs,scan.rs}` để nối gateway/profile registry thật và nhận kết quả; reuse adapter P2/P3, không tạo model loop.
- Modify `crates/central-application/src/{runtime.rs,leader.rs,handover.rs,quality_loop.rs,integration.rs}` tại boundary cần nối; create `client_access.rs`, `conversation.rs`, `user_sync.rs`, `maintenance.rs` cho use case còn thiếu. Mọi thay đổi runtime vẫn qua application.
- Extend `crates/central-application/src/ports/store.rs` và `crates/central-infrastructure/src/storage/{duckdb_store.rs,codec.rs}`; create migration `002_public_api.sql` cho client credential hash, conversation và dữ liệu mới cần persistence. Không sửa ý nghĩa migration đã phát hành.
- Modify `crates/central-cli/src/{main.rs,client.rs,commands.rs}`; create `crates/central-cli/tests/api_parity.rs`.
- Create `crates/central-server/tests/{api_contract.rs,api_auth.rs,api_events.rs,api_workflow.rs,artifact_retention.rs,release_security.rs}`; update `tests/local_runtime.rs` cho contract và 404 ngoài API.
- Create `examples/client.ps1`, `examples/browser-client.mjs`, `docs/{api.md,client-integration.md,getting-started.md,recovery.md,validation/p4-api.md}`, `README.md`, `scripts/{build-release.ps1,test-mvp-e2e.ps1}`.

**Interfaces:**

- `CommandEnvelopeV1 { id: UUID, expected_revision: u64|null, command: OperatorCommandV1 }` giữ cách đặt tên/JSON hiện có của các lệnh CLI; ánh xạ allowlist lệnh user sang domain, không công bố mọi system/agent command như quyền operator.
- `ReceiptV1 { command_id, run_id, revision, result, payload_hash }` là commit receipt. HTTP 200 không có nghĩa task đã pass; `dispatch` nếu có là kết quả bước sau commit, không đảo ngược receipt khi dispatch lỗi.
- `RunSnapshotV1 { instance_epoch: UUID, revision: u64, last_seq: u64, run: RunViewV1 }`; `TaskViewV1`, `AttemptViewV1`, `PlanViewV1`, `InboxViewV1`, `ArtifactViewV1`, `UsageViewV1`, `RecoveryViewV1`, `ChatMessageViewV1`, `ProfileViewV1` có trường cụ thể trong OpenAPI.
- `EventViewV1 { seq: u64, run_id: UUID, kind: string, at: UTC-ms, payload: object }`; seq tăng theo run và tồn tại qua restart, không dựa timestamp hoặc index của mảng client.
- `PageV1<T> { items: T[], next_cursor: string|null }`; default limit 100, max 500, cursor opaque và bound đúng collection/run/filter. Sai cursor/limit trả 400.
- `ApiErrorV1 { error: { code: string, message: string, details: object } }`; tối thiểu 400 validation, 401 auth_required, 403 forbidden, 404 not_found, 409 stale_revision/stale_target/command_conflict, 503 capability_unavailable. Không dùng message tự do làm mã lỗi.
- `ClientCredentialV1 { client_id, name, scope: read|control, expires_at, revoked }`; secret chỉ trả một lần khi owner cấp. Lưu hash, so sánh an toàn, không chứa credential trong list DTO. `read` không ghi; `control` được điều khiển workflow theo quyền user nhưng không quản lý credential/daemon. Agent bridge vẫn scoped theo vai trò và attempt, không được tự approve plan.
- Store thêm `snapshot_with_cursor(RunId)` và `events_after(RunId, after:u64, limit:u32)` với DTO application độc lập HTTP; snapshot và cursor lấy trong cùng read transaction. Giữ `events(RunId)` hiện có cho consumer nội bộ trong lúc chuyển đổi.
- SSE có event ID là durable seq, hỗ trợ `after`/`Last-Event-ID`, keepalive 15 giây và backpressure. Gap/cursor không hợp lệ hoặc đổi instance phát `resync_required`; client refetch snapshot. Event chỉ là notification/dữ liệu công khai, client không phải replay domain để tính trạng thái.

**API surface phải được mô tả và kiểm thử:**

| Surface | Hành vi |
| --- | --- |
| `GET /v1/meta`, `GET /v1/openapi.json` | Version, instance, capability thực tế và contract đã công bố; không chứa secret |
| `POST /v1/commands?run=<id>` | Giữ command gateway hiện có; CreateRun không có run query; idempotency và concurrency ở server |
| `GET /v1/runs`, `GET /v1/runs/{id}` | Danh sách có phân trang và snapshot nhất quán |
| `GET /v1/runs/{id}/tasks`, `/tasks/{task}`, `/attempts/{attempt}` | Trạng thái, dependency, profile/input binding, verification và nguyên nhân chờ |
| `GET /v1/runs/{id}/plans`, `/plans/{hash}`, `/plans/diff?from=...&to=...` | Revision/diff/criteria và target duyệt chính xác; không dùng latest ngầm |
| `GET /v1/runs/{id}/events`, `/events/stream` | Event page theo cursor và SSE được xác thực |
| `GET /v1/runs/{id}/chat`, `POST /v1/runs/{id}/chat` | Hội thoại leader bền vững; gửi message có command ID, qua budget/pause gate và không tự approve |
| `GET /v1/profiles`, `POST /v1/profiles`, `GET /v1/profiles/{id}` | Khả năng supported/unsupported/unknown, cấu hình do user cấp; profile snapshot của attempt cũ không đổi |
| Inbox/answer/reconcile endpoints hiện có | Giữ behavior, đưa vào contract; revision/preview hash/provider epoch bảo vệ target |
| `GET /v1/runs/{id}/usage`, `POST /v1/runs/{id}/usage/resolve`, `GET /v1/runs/{id}/recovery` | Ledger observed/estimated/unknown, admission reason, audited gap resolution và recovery actions |
| `GET /v1/runs/{id}/artifacts`, `/artifacts/{artifact}`, `/artifacts/{artifact}/content`, `/logs` | ID-scoped metadata/content/log có provenance, bounded reads, kiểm tra containment/hash |
| `POST /v1/runs/{id}/sync` | Queue commit user qua application, không để CLI ghi queue file trực tiếp; không checkout/stash/pull |
| `POST /v1/runs/{id}/attempts/{attempt}/results`, `/handover` | Import có binding/candidate validation và ghi nhận thật; scoped bridge đúng vai trò hoặc owner import, không chỉ in validated |
| `GET/POST /v1/clients`, `POST /v1/clients/{id}/revoke` | Owner-only; CLI cấp/thu hồi quyền client, control token không tự cấp token khác |
| `POST /v1/maintenance/export`, `/gc` | Owner-only, dry-run trước khi xóa; dùng cùng use case với CLI; không export credential |

Các endpoint POST chuyên biệt dùng cùng command identity/receipt/revision semantics
như command gateway; không tạo write path bỏ qua application. Client control không
được giả vai worker/verifier. Read endpoints không làm phát sinh inference.
Giữ `/healthz` tối giản và owner-only shutdown hiện có. Ngoài API trả 404; không
serve UI, không còn lệnh `central ui` hoặc đường static assets.

- [ ] **1. Viết contract và tests đỏ:** định nghĩa payload/DTO trong OpenAPI 3.1.2 và event schema; tests `independent_client_contract`, `retry_same_command_returns_same_receipt`, `retry_changed_payload_conflicts`, `two_clients_stale_approval_conflicts`, `snapshot_cursor_has_no_commit_gap`, `restart_requires_resnapshot_not_resume`, `read_client_cannot_mutate`, `agent_cannot_approve_plan`, `revoked_client_cannot_reconnect`, `untrusted_origin_rejected`, `artifact_escape_and_secret_redaction`. Assertions tối thiểu: receipt replay giống hệt; changed payload/stale target 409; no-auth 401; wrong scope/origin 403; duplicate external sends 0; inference during boot 0; missing usage hold tồn tại sau resume.
- [ ] **2. Chạy đỏ:** `cargo test -p central-server --test api_contract --test api_auth --test api_events --test api_workflow`; FAIL vì public API chưa có, không do fixture sai hoặc model/auth cá nhân.
- [ ] **3. Nối auth cho client:** implement credential store/use case và `/v1/clients`; chỉ owner local token được quản lý client. Host đúng loopback/port; Origin allowlist do owner cấu hình, hỗ trợ preflight cho Authorization/Content-Type/Last-Event-ID và từ chối origin lạ cả read/stream. CLI không có Origin vẫn dùng bearer. Không token query, wildcard CORS, cookie bootstrap hoặc tự mở LAN.
- [ ] **4. Chạy auth tests:** `cargo test -p central-server --test api_auth`; owner cấp token, read/control/bridge khác quyền, revoke có hiệu lực với request mới và đóng stream đang mở, secret không xuất hiện trong lỗi/log.
- [ ] **5. Implement read models và events:** map state sang DTO có capability/hold/unknown rõ; đọc snapshot+cursor atomically, query seq từ bảng events hiện có. History/log/artifact phân trang và giới hạn kích thước; replay SSE/JSON dùng cùng event projection. Đổi instance luôn yêu cầu client tải snapshot recovery mới. GET không thay state hoặc tạo agent process.
- [ ] **6. Chạy contract/event tests:** `cargo test -p central-server --test api_contract --test api_events`; body khớp schema, ordered replay không mất event cạnh commit, last_seq tồn tại qua restart, slow subscriber nhận resync thay unbounded buffer. Test log/content escape, unknown event kind và invalid cursor.
- [ ] **7. Hoàn thiện command boundary:** giữ receipt/idempotency hiện có, stable error codes, expected revision và exact target guard cho approval/revision/inbox. Implement API chat/profile/sync/usage/import còn thiếu qua application; bỏ CLI-side sync queue write. Public commands không cấp quyền system/agent; user import kết quả không tự mark pass.
- [ ] **8. Nối workflow thật:** wire Codex gateway, profile registry, event/result observation và quality/integration services hiện có vào daemon. Worker result được seal, verifier verdict gắn đúng candidate/input/criteria và integration có receipt. Nếu prerequisite P2/P3 chưa đủ thì hoàn thiện phần nối này trước nghiệm thu; không coi fake gateway hoặc validated-only import là kết quả sản phẩm.
- [ ] **9. Hoàn thiện CLI:** mọi operator action của API có CLI tương ứng, gồm chat/history, plan show/diff, profiles, clients create/list/revoke, events/logs/artifacts, usage resolution, recovery/sync/result/handover và explicit export/GC. `--json` output ổn định, `--follow` cho event/log; endpoint discovery/version check; lỗi nonzero, secret không qua argv/history. CLI chỉ gọi API, không mở DB hoặc ghi queue file.
- [ ] **10. Chạy parity/workflow tests:** `cargo test -p central-cli --test api_parity` và `cargo test -p central-server --test api_workflow`; HTTP client và CLI cùng quan sát revision/receipt/lỗi, nhánh parked không dispatch; scripted harness hoàn thành worker/verifier/integration qua daemon command surface. Bằng chứng fake ghi rõ phạm vi.
- [ ] **11. Viết tài liệu và ví dụ tích hợp:** OpenAPI tải được, auth/discovery/errors/pagination/versioning và SSE được giải thích trong `docs/api.md`/`client-integration.md`. `examples/client.ps1` dùng HTTP, không import crate hay đọc DB; `browser-client.mjs` dùng fetch Authorization + stream parser, không EventSource với token trên URL. Token chỉ giữ memory trong ví dụ browser. Thử browser origin được phép/lạ và ghi rõ môi trường đã kiểm tra; không cam kết mọi website/browser đều nối localhost được.
- [ ] **12. Kiểm tra client độc lập:** chạy ví dụ theo hướng dẫn từ state-dir fixture sạch; tạo run, duyệt plan đúng hash, xem inbox/evidence, đọc event rồi reconnect từ cursor. Chỉ dùng credential owner cấp và API công bố; chứng minh chạy được khi không có frontend bundle. Ghi kết quả trong `docs/validation/p4-api.md`.
- [ ] **13. Kiểm thử lifecycle/security:** `cargo test -p central-server --test artifact_retention --test release_security`; assertions: referenced artifact/active-or-unknown workspace không bị GC; token/revocation/Origin đúng; release không có fake runtime; `/` và `/ui` 404. Document backup offline: daemon dừng và owner lock free, copy DB + WAL hiện có + artifact store cùng nhau; export báo cáo không được gọi là DB backup.
- [ ] **14. Đóng gói CLI/API:** Windows zip có `central.exe`, `central-server.exe`, contract, tài liệu và notices; không UI hoặc Node runtime. `build-release.ps1` dùng toolchain đã pin và kiểm tra DLL cần thiết. Daemon chạy hidden/detached, config/version mismatch báo rõ; không sửa Git auth/config cá nhân.
- [ ] **15. Chạy gate đầu-cuối:** `pwsh -File scripts/test-mvp-e2e.ps1` với fixture repo và scripted harness có khả năng hoàn tất công việc: plan -> 2 worker -> verify độc lập -> integration; kill/restart daemon child tại barrier; boot zero inference; reconnect/resnapshot; explicit resume; provenance đúng, không gửi approval stale, không lặp external action. CLI và independent HTTP client cùng tham gia gate, không dùng private test mutation để thay cho API sản phẩm.
- [ ] **16. Kiểm tra live có giới hạn:** sau fake suite, một lần opt-in Codex gpt-6-luna/low trên fixture-owned scratch, ghi số turn/usage hoặc unknown; dừng khi quota/auth lỗi. Xác minh worker -> verifier -> integration thật qua daemon. Nếu không chạy được, ghi unverified và không tuyên bố live/MVP gate đã đạt; không chuyển model hoặc fake âm thầm. Reboot thủ công ghi riêng, không suy từ process kill.
- [ ] **17. Chạy kiểm tra cuối:** `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, build release và gate CLI/API. Contract tests validate OpenAPI/schema và response thực, kể cả error/stream; không chỉ kiểm tra file tồn tại. Smoke zip Windows không có Rust/Node; ghi evidence, capability chưa hỗ trợ và giới hạn accounting trong `docs/validation/p4-api.md`.
- [ ] **18. Commit deliverable:** `feat: expose documented client API and complete CLI workflow`; chỉ stage file của task. Commit hoàn tất khi CLI/client ngoài dùng được runtime và gates bắt buộc đạt, không khi chỉ có route/schema.

## Ranh giới phiên bản và tài liệu

P4 là việc phát hành public v1 đầu tiên; CLI hiện tại được cập nhật cùng contract.
Sau khi v1 công bố: field/endpoint mới phải tương thích; breaking change dùng major
mới, có migration notes. `/v1/meta` công bố capability thực tế; version/schema không
được ngụ ý harness hỗ trợ một chức năng chưa kiểm chứng. OpenAPI/SSE auth cũng là
contract cho UI bên ngoài, không hứa cung cấp UI hoặc SDK ở chặng sau.

Nguồn: [OpenAPI](https://spec.openapis.org/oas/v3.1.2.html),
[SSE](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events),
[Fetch](https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API/Using_Fetch).
