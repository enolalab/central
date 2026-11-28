# Leader Chat Convergence — Implementation Plan

> **For agentic workers:** Thực hiện từng task theo thứ tự. Steps dùng checkbox (`- [ ]`). Mỗi task kết thúc bằng test xanh và một commit riêng.

**Goal:** Chat với leader dễ đọc (markdown), có lối ra rõ ràng (nút Lập kế hoạch do user bấm), và có thể duyệt hoặc yêu cầu sửa kế hoạch.

**Spec:** `docs/superpowers/specs/2026-11-26-leader-chat-convergence.md` (user duyệt 2026-11-26).

**Tech Stack:** Rust/Axum hiện có; web JS thuần, không bước build; vendor marked + DOMPurify dạng file tĩnh.

## Global Constraints

- Chat không bao giờ đổi kế hoạch; đề xuất chỉ thành pending; chỉ user duyệt theo hash.
- Không đổi schema `LeaderReplyV1` hoặc `PlanDocumentV1`.
- Event cũ phải đọc được (`#[serde(default)]` cho mọi trường mới).
- CSP giữ nguyên `script-src 'self'`; không tải tài nguyên ngoài.
- Không thêm Node/bundler ở runtime hoặc build.

## Review Focus

1. Lượt `discuss` không bao giờ tạo `pending_plan`, kể cả khi model trả JSON đề xuất — Task 3.
2. Event cũ không có `intent` vẫn replay đúng và được coi là `discuss` — Task 3.
3. `revise` khi kế hoạch đã duyệt hoặc đã đổi → 409, không tạo đề xuất — Task 5.
4. HTML/script trong tin nhắn leader không thực thi — Task 1.

---

### Task 1: Render markdown cho tin nhắn leader

**Files:**
- Create: `crates/central-server/web/vendor/marked.min.js`, `purify.min.js`, `LICENSE-marked`, `LICENSE-dompurify`, `README.md` (phiên bản cố định, nguồn, checksum SHA-256)
- Modify: `crates/central-server/src/http/web.rs` (route `/app/vendor/marked.js`, `/app/vendor/purify.js`), `crates/central-server/src/http/mod.rs`
- Modify: `crates/central-server/web/index.html` (thêm 2 thẻ `<script defer>` trước `workspace.js`)
- Modify: `crates/central-server/web/workspace.js` (`renderMessage`), `workspace.css` (style cho `h1–h4`, `ul/ol`, `code/pre`, `table` trong `.message-body`)
- Test: `crates/central-server/tests/release_security.rs`

- [ ] Tải bản phát hành ổn định của marked và DOMPurify, ghi phiên bản và checksum vào `vendor/README.md`.
- [ ] Viết test trước: GET `/app/vendor/marked.js` và `/app/vendor/purify.js` trả 200, `content-type` JS, có CSP header như asset khác; `index.html` tham chiếu đúng hai route.
- [ ] Thêm route dùng `include_str!` như các asset hiện có.
- [ ] Trong `renderMessage`: nếu `message.role === 'leader'` và `window.marked`/`window.DOMPurify` có sẵn → `DOMPurify.sanitize(marked.parse(body), { ALLOWED_TAGS: [...], FORBID_TAGS: ['img','style','script','iframe'], ALLOWED_ATTR: ['href','title'] })`; ngược lại giữ logic tách đoạn hiện tại. Tin nhắn user, notice và `renderReplyPreview` giữ thuần text.
- [ ] Hook DOMPurify `afterSanitizeAttributes`: thẻ `a` có `target="_blank"` và `rel="noopener noreferrer"`; chỉ cho `href` scheme `http`, `https`.
- [ ] Kiểm tra thủ công trên trình duyệt với tin nhắn chứa `**đậm**`, danh sách, code, bảng, và `<img src=x onerror=alert(1)>`, `<script>`: render đúng, không thực thi.
- [ ] `cargo test -p central-server --test release_security` xanh. Commit.

### Task 2: Prompt thảo luận mới (`discuss`)

**Files:**
- Modify: `crates/central-application/src/discussion.rs` (`prompt`)
- Test: unit test trong cùng file (module `tests`)

- [ ] Viết test trước: prompt `discuss` (a) không chứa hướng dẫn JSON `"proposal"`; (b) có các chỉ thị: ngắn gọn, rào đón một lần, không lặp lượt trước, tự chọn khi user không chọn, kết thúc bằng bước tiếp theo cụ thể, gợi ý bấm **Lập kế hoạch** khi đã có việc làm được, được dùng markdown nhẹ; (c) khi `canInspect=false`, có chỉ thị nói rõ không đọc được workspace và lý do.
- [ ] Viết lại phần chỉ thị theo spec §4.2. Giữ nguyên phần dữ liệu workspace và lịch sử.
- [ ] Lý do không đọc được workspace: thêm tham số `inspect_reason: Option<&str>` (ví dụ "provider không phải Codex", "workspace không phải checkout local"), được `chat.rs` tính cùng chỗ với `workspace_read_only`.
- [ ] `cargo test -p central-application discussion` xanh. Commit.

### Task 3: `intent` cho lượt leader (domain + API)

**Files:**
- Modify: `crates/central-domain/src/run.rs` (`LeaderDiscussion`: thêm `intent: DiscussionIntent` với `#[serde(default)]`; enum `Discuss` (default) | `Plan` | `Revise`, serde `snake_case`)
- Modify: `crates/central-domain/src/command.rs` (`QueueLeaderDiscussion` thêm `#[serde(default)] intent`)
- Modify: `crates/central-domain/src/decision.rs` (`queue_leader_discussion`, `complete_leader_discussion`)
- Modify: `crates/central-server/src/http/chat.rs` (`PostChatBody` thêm `intent` và `assume: bool`, cả hai tùy chọn)
- Modify: `crates/central-server/src/api/read_models.rs` nếu read model không serialize thẳng `LeaderDiscussion`
- Test: `crates/central-domain/tests/plan_lifecycle.rs`

- [ ] Viết test trước:
  - Deserialize JSON `LeaderDiscussion` và `QueueLeaderDiscussion` không có `intent` → `Discuss`.
  - `CompleteLeaderDiscussion` có `proposal` cho lượt `Discuss` → hoàn tất lượt, giữ `reply`, **không** tạo `pending_plan`, không phát event `PlanProposed`.
  - Cùng tình huống với lượt `Plan` → tạo `pending_plan` như hiện nay.
  - Queue `Plan` hoặc `Revise` khi đã có `active_plan` → `InvalidCommand`.
  - Queue `Revise` khi không có `pending_plan` → `InvalidTransition` (map 409).
- [ ] Cài đặt: `queue_leader_discussion` kiểm tra điều kiện và lưu `intent`; `complete_leader_discussion` bỏ qua `proposal` khi `intent == Discuss`.
- [ ] `chat.rs`: parse `intent` (giá trị lạ → 422), truyền vào command và vào `discussion::prompt`. Chưa đổi prompt cho `plan`/`revise` (Task 4, 5).
- [ ] `cargo test -p central-domain` và `cargo test -p central-server` xanh. Commit.

### Task 4: Chế độ lập kế hoạch (`plan`)

**Files:**
- Modify: `crates/central-application/src/discussion.rs` (nhánh prompt `plan`, tham số `assume`)
- Modify: `crates/central-server/web/workspace.js` (nút, trạng thái, gửi `intent`), `workspace.css`, `index.html` (nút mới)
- Test: unit test trong `discussion.rs`

- [ ] Viết test trước cho prompt `plan`: có hướng dẫn JSON đề xuất hiện tại; có 5 rào chắn của spec §4.3 (chỉ câu chặn, ≤3 câu kèm mặc định, không mở chủ đề mới, không hỏi lại, xuất JSON có giả định khi hết câu chặn). Với `assume=true`: có chỉ thị cấm hỏi, bắt buộc trả JSON, ghi điểm chưa rõ thành giả định.
- [ ] Cài đặt prompt. Nội dung ô nhập trống được thay bằng mô tả mặc định "Lập kế hoạch từ cuộc trao đổi" trước khi queue (giữ validation không rỗng của domain).
- [ ] UI:
  - Nút **Lập kế hoạch** cạnh Gửi; hiện khi có ≥1 lượt, không có `active_plan`, không có lượt đang chạy, không ở chế độ lập kế hoạch.
  - Chế độ lập kế hoạch = lượt cuối có `intent` `plan`/`revise` và `proposal` rỗng. Khi đó: thanh trạng thái "Đang lập kế hoạch: leader có thể hỏi các câu cần thiết"; ô nhập gửi cùng `intent`; hiện nút **Lập ngay với giả định** (`assume: true`) và **Quay lại thảo luận** (chỉ đổi `intent` của tin kế tiếp về `discuss`).
- [ ] Kiểm tra live với leader Codex trên một cuộc trao đổi ngắn: bấm Lập kế hoạch → có câu hỏi chặn hoặc đề xuất; bấm Lập ngay với giả định → có `pending_plan` trong một lượt. Ghi kết quả vào phần nghiệm thu cuối file này.
- [ ] Test xanh. Commit.

### Task 5: Kế hoạch nổi bật và "Yêu cầu sửa" (`revise`)

**Files:**
- Modify: `crates/central-application/src/discussion.rs` (nhánh prompt `revise`: đưa `document` của `pending_plan` và góp ý)
- Modify: `crates/central-server/web/workspace.js` (`renderMessage` thẻ kế hoạch, `showPlan`, thanh trạng thái), `workspace.css`
- Test: `discussion.rs` unit test; `crates/central-domain/tests/plan_lifecycle.rs`

- [ ] Viết test trước: prompt `revise` chứa document kế hoạch đang chờ và góp ý; domain: hoàn tất lượt `Revise` với kế hoạch mới → bản cũ `Superseded`, `pending_plan` là hash mới.
- [ ] Thẻ kế hoạch trong chat: hiển thị mục tiêu, số task, nút "Xem chi tiết"; thanh trên cùng "Có kế hoạch chờ duyệt" khi có `pending_plan`.
- [ ] `showPlan`: thêm nút **Yêu cầu sửa** → ô nhập góp ý (bắt buộc) → POST `leader-chat` với `intent: revise`. 409 → tải lại, báo "Kế hoạch đã thay đổi. Mở lại để xem bản mới."
- [ ] Test xanh. Commit.

### Task 6: Chỉ báo quyền đọc mã

**Files:**
- Modify: `crates/central-server/src/http/chat.rs` (tách hàm tính `workspace_read_only` + lý do để tái dùng), handler snapshot/read model cho web
- Modify: `crates/central-server/web/workspace.js` (`renderChat` header), `workspace.css`
- Test: `crates/central-server/tests/api_workflow.rs` hoặc test đơn vị cho hàm tính quyền

- [ ] Viết test trước cho hàm tính quyền: local checkout + `codex` → đọc được; provider khác hoặc không phải checkout → không, kèm lý do.
- [ ] Đưa `leader_workspace_access: { readable, reason }` vào dữ liệu web nhận được (ưu tiên route `/app/...` hiện có, không đổi `/v1` nếu không cần; nếu đổi `/v1` thì cập nhật `contracts/openapi-v1.json`).
- [ ] Header chat hiển thị "Đọc được mã workspace" / "Không đọc được mã workspace · <lý do>".
- [ ] Test xanh. Commit.

### Task 7: Nghiệm thu

- [x] `cargo test --workspace` và `cargo clippy --workspace -- -D warnings` xanh.
- [x] Đối chiếu từng tiêu chí ở spec §6 (1–7 có bằng chứng test; 8 hoãn live theo quyết định của user).
- [x] Cập nhật `docs/getting-started.md` (mục chat với leader: nút Lập kế hoạch, Yêu cầu sửa, chỉ báo quyền đọc mã).
- [x] Ghi kết quả nghiệm thu bên dưới.

## Trạng thái nghiệm thu

- Task 4 (chế độ `plan`): kiểm tra live với leader Codex — **chưa xác minh** (user hoãn; không chạy live trong lượt này, không khẳng định thành công).
- Task 7 (2026-11-28): `cargo test --workspace` xanh — 44 suite ok, 340 passed, 0 failed; `cargo clippy --workspace -- -D warnings` xanh sau một fix tối thiểu (`#[allow(clippy::too_many_arguments)]` cho `queue_leader_discussion` ở `crates/central-domain/src/decision.rs` — Task 3 thêm tham số `intent` thứ 8, vượt ngưỡng mặc định 7; cùng pattern `allow` đã có 7 chỗ trong file).
  Đối chiếu spec §6: (1) markdown + CSP/route có test `release_security::vendor_markdown_assets_are_served_with_csp_and_referenced_by_index`; lọc XSS phía client dùng DOMPurify, không có test JS (không có harness JS) — chỉ kiểm tra `node --check` + đọc config `ALLOWED_TAGS`/`FORBID_TAGS`. (2) `plan_lifecycle::discuss_turn_ignores_proposal`. (3) `plan_lifecycle::plan_turn_creates_pending_plan`, `plan_and_revise_are_rejected_when_a_plan_is_active`, `revise_requires_a_pending_plan`; UI `planningIntent` chỉ kiểm tra tĩnh. (4) "Lập ngay với giả định" một lượt ra đề xuất — **chưa xác minh** (cần live với Codex, user hoãn). (5) `plan_lifecycle::revise_turn_supersedes_pending_plan`. (6) `plan_lifecycle::missing_intent_deserializes_as_discuss`. (7) `http::chat::tests::{local_checkout_with_codex_is_readable, non_codex_provider_is_not_readable_with_reason, non_checkout_is_not_readable_with_reason}`; header UI chỉ kiểm tra tĩnh. (8) phát lại chat "trí tuệ BOT" — **chưa xác minh** (user đã chọn "test tự động, live chưa xác minh"; không chạy live, không khẳng định thành công).
