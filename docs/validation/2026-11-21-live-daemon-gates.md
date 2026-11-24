# Live-daemon baseline — 2026-11-21 (commit `621fafd`)

Commit `621fafd` (Muse + opencode harness gateways + provider routing).
Windows evidence trước đó thuộc phiên bản cũ hơn — chưa re-run trên commit này.

## 1. Gate trên commit hiện tại (Linux, verified 2026-11-21)

- `cargo fmt --check` → pass (exit 0).
- `cargo clippy --workspace --all-targets -- -D warnings` → pass, no warnings.
- `cargo test --workspace` → **286 passed, 0 failed** (toàn bộ suite `ok`,
  1 ignored trong `process_ownership`), bao gồm `muse_gateway` 5 passed và
  `opencode_gateway` 5 passed.
- Script gate `.ps1` (`test-mvp-e2e`, `test-workflow-recovery`,
  `test-codex-single-task`) là Windows-only; máy này không có `pwsh` nên
  không re-run — kết quả Windows gần nhất xem
  `2026-11-15-windows-live-acceptance.md` (thuộc cây trước commit này).

## 2. Version matrix tại máy kiểm chứng (fail-closed là hành vi đúng)

| Harness | Cài đặt | Allowlist | Daemon |
| --- | --- | --- | --- |
| codex | 0.169.0 | 0.167.0-alpha.3.2, 0.167.1 | fail-closed (capability error, dispatch defer) |
| muse | 1.4.24 | 1.4.20, 1.4.23, 1.4.24 (mở trong đợt này) | mở được gateway; live turn cần provider auth của owner |
| opencode | 2.2.7 | 2.2.7 | mở được gateway; live turn cần provider auth của owner |

Capture muse 1.4.24 (đợt này, quyết mở allowlist có capture): schema export
offline — trùng khớp 75 methods / 47 notifications / 2 requests / 39 error
codes với capture 1.4.20+1.4.23 (kể cả def `ApprovalCuaBrowser`), toàn bộ
subset đã kiểm (8 clientMethods + 2 serverRequests + 9 notifications) còn
nguyên; echo handshake live pass full chain initialize → session/start
(UUIDv7 commandId bị server enforce) → turn/start → turn/completed +
tokenUsage + contextUsage. Approval/userInput decide vẫn schema-only (echo
không hỏi). Codex 0.169.0 vẫn fail-closed — chưa có capture. Không đụng
`~/.codex` và không gọi model trong đợt kiểm chứng này.

## 3. Luồng Codex đầu-cuối qua daemon (code + e2e gate, không phải claim live mới)

Map theo code hiện tại (`scheduler.rs`, `dispatcher.rs`, `quality_loop.rs`,
CLI, `test-mvp-e2e.ps1`):

1. Owner setup một lần: state dir, `codex.json` (binary + version + surveyed
   models), profiles, `central sync` base commit — **operator**.
2. `plan import` + `plan approve` — **operator** (approve đúng hash, stale 409).
3. Dispatch worker qua scan loop → gateway → harness — **tự động**.
4. Approval/question giữa turn → inbox; `approve`/`decline`/`answer` —
   **operator** qua CLI/API.
5. `reconcile` sau turn (passive, unknown stays unknown) — **operator**.
6. `result import` (WorkerResultV1 → seal, park `AwaitingVerification`) —
   **operator** soạn + import; không có bước tự dịch harness output → result.
7. Verdict verifier → `handover import` → `Succeeded`/repair/leader —
   **operator** (chạy verifier ngoài + import). **Đây là khoảng trống tự động
   lớn nhất**: scheduler/dispatcher chỉ sinh `Worker` (và repair/fallback);
   KHÔNG có `WorkRole::Verifier` nào được dispatch qua gateway —
   `dispatcher.rs` chặn integration sang mechanical path, còn verifier thì chỉ
   tồn tại qua import tay. Cross-check: Windows live 15/11 verifier cũng chạy
   bằng direct CLI ngoài daemon (6 attempt blocked), khớp thiết kế này.
8. Integration (approved IntegrationSpec) chạy mechanical, 0 sends —
   **tự động** khi dependencies `Succeeded`; kill/restart giữa chừng giữ
   zero-send-at-boot + resume tường minh (`p4-api.md` step 15 PASS trên fake).

Muốn điều phối tự động thật: cần daemon tự sinh verifier attempt qua gateway
+ tự dịch harness output thành result/handover — cả hai đều chưa tồn tại.

## 4. Muse / opencode qua Central (đợt này: test + probe, không live turn)

- `muse serve --help` / `opencode serve --help` / `codex --help` → mặt
  transport đúng như spec (MSP stdio; loopback `--port 0 --stdio`; app-server).
- Opencode `loopback_wire_speaks_verified_protocol` (stub server): bao phủ
  open → send → observe (completed + usage + permission) → answer (`once`)
  → cancel teardown. Muse KHÔNG có test tương đương: 5 test chỉ bao
  contracts/normalize/capabilities/frame-cap; đường answer 2 bước
  (Receipt + `approval/decide`/`userInput/answer`), cancel identity-checked và
  Q&A secret-path chưa có stub-transport test — là test gap cần lấp trước khi
  claim parity với Codex.
- Lab 2026-11-17 trong hai spec (muse approval abort, opencode deny +
  interrupt) không có artifact máy-chấm được trong repo — giữ nguyên như
  lab-note, không nâng thành gate claim. `getting-started.md` (schema-captured)
  và spec (live-exercised) đang lệch nhau ở điểm này; file này là chuẩn cho
  đến khi có evidence mới.
- Cancel/recovery ở tầng daemon (epoch rotate, park `AwaitingResume`,
  zero-send-at-boot, resume tường minh) được bao bởi `workflow_recovery` + e2e gate,
  gateway-agnostic — áp dụng cho cả ba provider mà không cần re-verify
  từng provider.

## 5. Live opencode 21/11 — dispatch thật, turn thành công bằng ollama local

Auth: `opencode auth list` → OpenCode Console stored. Model free phản hồi
nhanh đã thử direct (`opencode run`, vài giây): `hy4-flash-free` (PROBE-OK),
`deepseek-v4.1-flash-free` (D-OK), `mimo-v2.6-flash-free` (M-OK),
`ollama/qwen3:4b` (O-OK). Ollama local (`qwen3:4b`, `qwen2.5:7b/3b` đã cài)
là đường free vĩnh viễn, không quota.

- Dispatch qua daemon thành công sau khi ghim `profileId` đúng vào task
  (live mode `profileId: null` → resolver None → kẹt Queued im lặng; fake
  mode mới có dev_profile fallback). Serve child CWD = worktree attempt
  (đúng).
- Bug thật đã fix (2 vòng): (1) prompt/session không gửi `model` → server
  nhận 200 nhưng turn không bao giờ completed (run `61f57f5f`, `25890ea6`).
  Lần đầu fix nhầm `model` vào prompt — probe tay "thành công" là false
  positive (trial quota chạy trên backend default). Đúng: model đi theo
  session create dạng Model.Ref `{providerID, id}` (bare id bị 400
  `Expected Model.Ref`), gateway verify echo fail-fast + verify message theo
  cặp qua `check_model_ref_match`. Fix: `client.rs::create_session`,
  `version.rs::split/check_model_ref_match` (+test), stub test assert body,
  `finish: error` ghi server message vào evidence.
  Gates sau fix: fmt + clippy sạch, workspace pass (muse 6 + opencode 5 +
  version unit mới).
- Run `cb8cbde6` (alias `opencode/deepseek-v4.1-flash-free`): turn completed
  sau ~8s với `finish: error`, 402 `provider.quota` (free trial hết sau 3
  probe direct). Wire báo backend `gemini-4-neon`, không echo alias →
  exact-match rớt loud (fail-closed đúng thiết kế).
- Run `593afbed` (model `ollama/qwen3:4b`, session-model đúng): **turn live
  thành công đầu tiên qua daemon** — 7 observations (`turn completed`,
  usage notes, match pass, không capability error), artifact `hello.txt`
  đúng 5 bytes `HELLO` (`sha256:3733cd97…`), `result import` seal
  `awaiting_verification` với hash thật (input = sha256 prompt lấy từ DB,
  candidate = sha256 file).
- Run `92b80284` (lặp lại full loop, task mới `answer.txt` = `42`):
  finish `stop`, 2 observations, artifact đúng 2 bytes
  (`sha256:73475cb4…`), seal `awaiting_verification` — worker→seal qua
  daemon đã repeatable với model local.
- Vận hành: scan không tự observe; observation chỉ vào log qua
  `POST /v1/runs/{id}/inbox/reconcile` với đúng session ref (gateway epoch
  trong store, không phải daemon epoch từ `/v1/meta`) — CLI chưa có lệnh
  này, phải curl tay. Drain chạy TRƯỚC observe trong cùng reconcile nên
  request mới vào inbox ở lần reconcile kế tiếp (lag 1 vòng).
- **Full chain opencode qua daemon (run `f5bbd4a3`, 2 tasks)**: worker
  (ollama/qwen3:4b) → `answer.txt` đúng `42` → seal → verdict pass qua
  endpoint mới → task c101 `Succeeded` → integration cơ học tự dispatch
  (`git cat-file -e HEAD:base.txt` exit 0, evidence hash cả hai đầu) →
  `IntegrationPromoted`, good ref `c20dbbc1` → `13b90d43` chứa answer.txt,
  task c102 `Succeeded`. Không model, không tay can thiệp giữa chừng
  ngoài import/verdict (đúng vai operator).
- **Verdict surface đã lấp (sau 2 run live trên)**: `POST
  /v1/runs/{id}/verdicts` + `central verdict import --run --task --attempt
  --file` (`contracts/verdict-submit-v1.schema.json`). Daemon dựng lại seal
  từ `WorkerResultRecorded` events (restart-safe, không dựa service
  in-memory), bind exact subject/criteria/input, commit `SubmitVerification`
  (pass → `Succeeded`, fail → repair/leader, blocked → parked). Test:
  pass→Succeeded + re-verdict 409, sai subject 409 + vẫn parked, fail thiếu
  criteria 400, read scope 403 (`api_workflow.rs`: `verdict_pass…`,
  `verdict_guards…`; unit parser trong `quality_loop.rs`). Kèm fix nhỏ:
  `VerificationOutcome` nhận thêm alias lowercase đúng schema đã công bố
  (`Pass` PascalCase trong stored events giữ nguyên).
- Code chưa commit (tính đến cuối file): verdict surface + session
  Model.Ref + error evidence + cancel đúng session + scan gửi outbox +
  tolerate-notif + jsonrpc member + capture 1.4.25.

## 6. Live Muse qua daemon (1.4.24 → tự update 1.4.25 giữa chừng)

- Lab cài `muse 1.4.24` (allowlisted) + auth sẵn provider `meta`.
  Capture 1.4.24: schema export trùng khớp 75/47/2/39 + full subset,
  error codes trùng, echo handshake pass.
- Giữa đợt kiểm chứng, binary tự update lên **1.4.25** → daemon
  fail-closed đúng thiết kế (`user agent does not contain 1.4.24`).
  Capture 1.4.25 (schema + echo chain + 39 codes, `contracts/muse/1.4.25/`),
  mở allowlist — xong mới chạy tiếp được.
- Bug thật (chặn mọi turn Muse): frame JSON-RPC thiếu member
  `jsonrpc` → server trả -32600 ở `turn/start` (initialize/session-start
  dễ tính nên lọt). Fix: `encode_request/response/notification` luôn gửi
  `"jsonrpc":"2.0"` + unit test (`wire.rs`), codex tests vẫn xanh (12/12).
- Run `5b8378a9` (model `muse-spark-1.3`, meta): dispatch → turn completed,
  63 observations, artifact `muse.txt` đúng 4 bytes `MUSE` → seal →
  verdict pass qua endpoint mới → task d101 `Succeeded`. Lần approve sau
  (run `26f3d0ed`, shell task): `approval/request` vào inbox Pending →
  approve qua CLI → gateway gửi Receipt + `approval/decide` → server apply
  `decision: approved` (`human_approval`, xem transcript session) →
  artifact `pyver.txt` đúng → seal → verdict → d103 `Succeeded`.
- Hủy live cả 2 provider: `stop --run` giữa turn → serve child chết,
  attempt `Cancelled` (opencode run `1353738f`, muse run `438f5ad4`).
  Trước fix cancel (blank session ref) attempt treo `UnknownOutcome` +
  leak child — đã fix (`runtime.rs` dựng ExecutionRef từ session binding,
  test `cancel_targets_the_attempt_live_session`).
- Phục hồi live (opencode): kill -9 daemon giữa turn → restart park
  `AwaitingResume`, boot zero-send → resume tường minh → reconcile ra
  `UnknownOutcome` trung thực, attempt giữ nguyên (không force). Cơ chế
  này provider-agnostic (cùng code path cho muse).
- Chưa live: `userInput/question` (2 model đều không hỏi),
  permission/opencode (rule auto-allow trong worktree nên không hỏi).
  Đường answer/decline/reconcile của cả hai đã có stub/contract test.
- ĐÃ FIX sau đợt live trên (chưa commit): scan nền gửi outbox mỗi tick
  (`scheduler.tick` flush qua `dispatch_once` với secrets vault; test
  `scheduler_tick_sends_pending_cancel_without_manual_dispatch`; live
  smoke run `8a35eb99` worker→seal→verdict→Succeeded bình thường, không
  hồi quy dispatch). Reconcile chịu notif lạ: poll chỉ có notification
  ngoài session trả `Ok(empty)` + giữ lại buffer thay vì fail cả loạt;
  session thật mất mới báo `unknown session` như cũ; overflow vẫn là
  backstop (test `foreign_notifications_never_sink_our_turn`).
- Còn hở (chưa làm): CLI thiếu inbox-reconcile (phải curl tay);
  `userInput/question` chưa model nào hỏi live.

## 7. Doc fix kèm theo (comment/pin stale từ commit Muse/opencode)

- `muse/version.rs`, `muse/gateway.rs`: pin 1.4.23 + handshake live 17/11
  (trước đó ghi 1.4.20 / "NOT live-exercised").
- `opencode/gateway.rs`: đồng bộ với spec (deny/interrupt đã lab 17/11,
  không artifact máy-chấm).
- `muse-gateway-spec.md` §4: pin `1.4.23` (allowlist giữ `1.4.20`).
