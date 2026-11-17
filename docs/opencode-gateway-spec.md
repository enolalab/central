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

`POST /api/session` {title} → `POST /session/{id}/prompt` {text} →
poll `GET /session/{id}/message`: assistant message có `time.completed`
là terminal (streaming có `completed: null`, không tính). Timeout sau write
prompt = OutcomeUnknown, không resend. Model trong message (`model.id`)
verify với profile — mismatch là capability error.

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
