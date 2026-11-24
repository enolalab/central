# OpencodeGateway spec (opencode 2.2.7, REST polling)

Trạng thái: đã triển khai (`crates/central-infrastructure/src/opencode/`,
`RouterGateway` + `opencode.json` trong `crates/central-server/src/bootstrap.rs`,
`providers` trên `GET /v1/meta`). Turn flow + auth + permission LIST đã
kiểm chứng live 2026-11-17; chu trình reply/reject và interrupt là
schema-captured (lab không hỏi, rules mặc định auto-approve).

## 1. Transport

Mỗi session sở hữu một `opencode serve` trên loopback:
`serve --hostname 127.0.0.1 --port 0 --stdio` + `OPENCODE_PASSWORD=<secret>`
trong child env. `--port 0` cho server tự chọn cổng, dòng stdout đầu là
`{"url": ...}` (machine-readable); stdin giữ mở (đóng là server thoát, đã
kiểm chứng). Auth: HTTP Basic `opencode:<secret>` mọi request.

Không dùng SSE (`/api/event` đã dò, quiet khi idle) — `observe()` poll
`GET message` + `GET permission` rồi diff theo seen-set, nên poll lặp không
bao giờ fabricate observation thứ hai. Không có server dùng chung giữa các
session: cô lập theo process.

## 2. Vòng đời một turn

`POST /api/session` {title, model: Model.Ref {providerID, id}} →
`POST /session/{id}/prompt` {text} →
poll `GET /session/{id}/message`: assistant message có `time.completed`
là terminal (streaming có `completed: null`, không tính). Timeout sau write
prompt = OutcomeUnknown, không resend. **Model BẮT BUỘC đi theo session
create** (verbatim từ profile, tách `provider/id` thành Model.Ref): session
không model + prompt text-only được server nhận (200 + user msg id) nhưng
turn không bao giờ completed — attempt kẹt Running với 0 observation
(verified 21/11, fix trong `client.rs::create_session`; prompt giữ nguyên
`{text}` đúng wire đã verify). Bare id không provider bị server 400
(`Expected Model.Ref`). Session create echo lại model đã chọn — gateway
verify echo fail-fast trước khi gửi prompt; message verify theo cặp
(providerID, id) qua `check_model_ref_match` — mismatch là capability
error. Alias free (`opencode/*-free`) được server resolve sang backend id
(vd `gemini-4-neon`, `providerID: opencode`) và wire CHỈ báo backend, không
echo alias — mọi turn alias đều rớt exact-match cho đến khi có quota thật
kiểm chứng success-path. `finish: error` vẫn mang `time.completed` nên
gateway ghi server error vào evidence thay vì success trống.

Usage từ message tokens {input, output, reasoning, cache{read, write}} →
note evidence-only, không vào ledger.

## 3. Approval (mapping)

`GET /session/{id}/permission` → `Permission.Request`
{id, sessionID, action, resources[], message} → inbox `Approval`
(item = permission id, turn rỗng — opencode không có turn trên permission).
`POST .../permission/{id}/reply`: domain `approved` → `once`;
`declined:<feedback>` → `reject` + message. Không bao giờ gửi `always`
(standing permission là owner act, không phải agent side effect). Consume
Once TRƯỚC khi write; write-fail → OutcomeUnknown, chờ server re-ask.

## 4. Struct (mirror Codex/MuseGateway)

```rust
struct OpencodeConfig { binary, cli_version, allowed_versions,
    request_timeout_ms, allow_provider_model_fallback: false,
    extra_args, surveyed_models }
struct OpencodeGateway { config, epoch,
    sessions: Mutex<HashMap<AttemptId, ManagedSession>>, supervisor, piped }
struct AttachedTransport { client: OpencodeClient, server_session,
    stdin_hold (giữ mở), seen_completed: HashSet, seen_permissions: HashSet }
```

Tái dùng: `codex::PendingRequestTable` (Once scope), `sanitize_text` +
`is_secret_key_in` (redact), inbox key 6-part
(`provider:epoch:thread:turn:item:request`, turn rỗng cho permission).
`Capabilities{provider:"opencode", supports_resume:true, features cố định
+ model:<id> CHỈ từ surveyed_models}`. `SessionRef{provider:"opencode",
epoch, thread_id = server session id}`. cancel: interrupt best-effort +
drop stdin + terminate identity-checked; reconcile luôn UnknownOutcome.

## 5. Provider routing

- `opencode.json` trong state dir, shape `{ "binary": "opencode",
  "cli_version": "2.2.7", "surveyed_models": [...], "extra_args": [...] }`
  (`extra_args` append sau flags loopback cố định; không đổi bind).
- `RouterGateway` route theo `profile.provider` / session provider (đã có
  sẵn từ MuseGateway). Thiếu file / version fail → fail-closed cho provider
  `opencode`.

## 6. Test plan

contracts JSON validity + pin; normalize permission (provider/key/preview);
capabilities (allowlist, model chỉ từ surveyed, never fallback);
loopback stub e2e: open (URL parse + version pin + session create, auth
Basic) → send (prompt, turn = user msg id) → observe (completed + usage +
permission, dedup qua seen-set) → drain → answer (`once` on wire, Once
scope) → cancel. Live lab (cần opencode auth): prompt thật + ép permission
deny + interrupt.

## 7. Unresolved (cập nhật sau lab approval 2026-11-17)

Đã kiểm chứng thêm: permission deny đầy đủ — edit ask-rule → request →
reply `reject` + message → request resolve, model re-ask request mới
(Once scope khớp). Interrupt live: `POST .../interrupt` →
`{"interrupted":true}`, turn dừng. Surveyed: `hy4-flash-free` (default,
turn-proven). REST body lớn nhất đo được ~5 KB; giữ trần 8MiB, poll lỗi
→ reconcile-required, không parse nửa vời.

Lab 21/11 (xem validation 2026-11-21): model BẮT BUỘC đi theo session
create (đã fix; lần đầu fix nhầm vào prompt — probe tay "thành công" là false
positive vì trial quota chạy trên backend default). Alias free resolve sang
backend `gemini-4-neon` trên wire; 402 `provider.quota` hết free trial sau 3
probe direct thành công. Turn live thành công đầu tiên qua daemon: run
`593afbed`, model local `ollama/qwen3:4b` (free, không quota), 7 observations
match pass, artifact `hello.txt` đúng 5 bytes (`sha256:3733cd97…`), result
import seal `awaiting_verification` với hash thật, verifier độc lập đọc
bytes xác nhận PASS (lúc đó `SubmitVerification` chưa có đường ra public
API — đã lấp sau đó bằng `POST /v1/runs/{id}/verdicts`, xem validation
2026-11-21 §5). Phụ thêm: task `profileId: null` ở live mode (không
fake) resolve ra None → attempt kẹt Queued im lặng (phải ghim đúng profile
id); scan không tự observe — observation chỉ vào log khi gọi
`POST /v1/runs/{id}/inbox/reconcile` với đúng session ref (gateway epoch
trong store, không phải daemon epoch từ `/v1/meta`), CLI chưa có lệnh này
(chỉ có attempt-reconcile không poll message); serve child CWD = worktree
attempt (đúng).
