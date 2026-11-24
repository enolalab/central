# MuseGateway spec (muse 1.4.23, MSP stdio)

Trạng thái: đã triển khai (`crates/central-infrastructure/src/muse/`,
`RouterGateway` trong `crates/central-server/src/bootstrap.rs`, `muse.json`,
`providers` trên `GET /v1/meta`). Serve handshake live đã kiểm chứng trên
echo cho 1.4.23 (17/11), 1.4.24 (21/11) và 1.4.25 (23/11) — xem
`contracts/muse/1.4.25/`. RequestReceipt+decide đã kiểm chứng LIVE trên meta
(run `26f3d0ed`: approval/request → inbox → approve → decide applied as
`human_approval`); userInput/question vẫn schema-only (chưa model nào hỏi
live).

## 1. Transport

`muse serve` (MSP session host qua stdio, JSON-RPC 2.0 JSONL). Client sở hữu stdin/stdout.
`attach --stdio SESSION_ID` dùng cho reconcile/re-attach. Loại `muse acp` (mất ngữ nghĩa
MSP) và `muse exec --json` (one-shot, không answer được; giữ làm fallback batch sau này).

Vòng đời một turn: `initialize` → `session/start` (commandId UUIDv7, modelId verbatim,
approvalMode select-never-create) → `turn/start` (commandId UUIDv7 mới, ifBusy mặc định
`queue`) → terminal `turn/completed` + usage từ `session/tokenUsage` /
`session/contextUsage` (evidence-only, không vào ledger).

## 2. Approval 2 bước (khác Codex duy nhất ở answer)

Server `approval/request` / `userInput/request` → client trả `RequestReceipt {}` (ack
presentation, SS5.3.3) → quyết định đi bằng lệnh riêng `approval/decide`
(approvalId + choiceId + requirementId race-guard) / `userInput/answer`
(userInputId + answers[]). Thứ tự bắt buộc: consume Once TRƯỚC, rồi ghi Receipt,
rồi ghi decide/answer. Ghi lệnh thất bại sau consume → OutcomeUnknown, chờ server
re-ask, không resend. Không bao giờ trả lời inline trong response như Codex.

## 3. Struct (mirror CodexGateway)

```rust
struct MuseConfig {
    binary: String,              // "muse", resolve trên PATH
    cli_version: String,         // allowlist-check ["1.4.20","1.4.23","1.4.24","1.4.25"], pin "1.4.23"
    surveyed_models: Vec<String>,// default rỗng → chưa advertise model nào
    serve_args: Vec<String>,     // default ["serve"]
    provider: &'static str,      // "muse"
    notification_capacity: usize,// 256 (kế thừa Codex tới khi đo MSP)
    request_timeout_ms: u64,
    allow_provider_model_fallback: bool, // luôn false
}
struct MuseGateway { config, epoch, sessions: Mutex<HashMap<AttemptId, MuseSession>>, supervisor, piped }
struct MuseSession { transport: Option<MspTransport>, server_session: String, local_thread: String,
                     pending: PendingRequestTable, identity: ProcessIdentity }
```

Giữ nguyên: trait `AgentGateway` 7 ops + `drain_requests`; `Capabilities{provider:"muse",
supports_resume:true, features cố định + model:<id> CHỈ từ surveyed_models}`;
`SessionRef{provider:"muse", instance_epoch, thread_id}`; inbox key 6-part
`provider:epoch:thread:turn:item:request`; `normalize_request` Approval/Question +
redact-at-source + preview truncate 4000ch; `PendingRequestTable` Seen→Drained +
consume Once; cancel identity-checked + stale-epoch Conflict; reconcile luôn
UnknownOutcome. Tái dùng `wire.rs` redaction + `requests.rs` PendingRequestTable.

Reasoning effort: 8 tier `none|minimal|low|medium|high|xhigh|max|ultra`, per-turn
override session default, đi verbatim không clamp. Model list đọc từ
`model/list` (query) nhưng advertise vào capabilities CHỈ sau survey.

## 4. Provider routing

- `muse.json` trong state dir, shape mirror `codex.json`:
  `{ "binary": "muse", "cli_version": "1.4.20", "surveyed_models": [...] }`.
  Thiếu file / binary không có trên PATH / version fail → `UnavailableGateway`
  fail-closed cho provider `muse` (giữ hành vi cũ khi cả 2 thiếu).
- `RouterGateway { map: HashMap<provider, Arc<dyn AgentGateway>> }` implement
  `AgentGateway`, route theo `profile.provider` TRƯỚC `resolve_profile`.
  Provider lạ → Capability error, không fake output.
- `GET /v1/meta` thêm danh sách provider đã cấu hình.

## 5. Test plan (full parity, mirror suite Codex)

capabilities (allowlist pass/fail, model chỉ từ surveyed_models, mismatch →
ModelUnavailable, never fallback); open/send (PipedSpawner vs supervisor-only,
epoch keying, không giữ mutex qua await, sandbox downgrade → cancel +
OutcomeUnknown); observe (pump trước, closed/overflow → reconcile-required,
tokenUsage → usage_note, benign notif drop, unattributed → 503); answer
(epoch/empty/Secret reject, consume-trước-write, Receipt+decide 2 bước,
write-fail → OutcomeUnknown + re-ask); cancel/reconcile (stale-epoch Conflict,
identity-checked kill, luôn UnknownOutcome); inbox (key 6-part, dedup,
matches); redaction (hash trên text đã redact); riêng MSP (requirementId
race-guard, commandId UUIDv7 bắt buộc, session/resume durability).

## 6. Unresolved (cập nhật sau lab approval 2026-11-17, meta provider)

Đã kiểm chứng thêm: shell approval đầy đủ — `approval/request`
(choiceId/decision/label/scope + object `currentRequirementId`) →
RequestReceipt → `approval/decide` abort + feedback (turn cancelled, agent
được báo). `approved` map `allow_once`, không bao giờ chạm
`approvedPolicyAmendment`. File-write trong workspace composed KHÔNG hỏi
(onRequest); shell dưới `promptUnmatched` thì hỏi. Surveyed:
`muse-spark-1.3` (turn-proven). userInput/request: KHÔNG kích hoạt sau 3
meta turns (model hỏi bằng text thường thay vì gọi tool) — path gateway
giữ nguyên mẫu approval đã verify, vẫn schema-only có lý do. FrameCap đo
thực tế: frame lớn nhất ~2 KB, giữ 8MiB + unit test close-on-oversize.
