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

## 7. Giả lập user vận hành cụm mới từ đầu (24/11, run `38936210`)

Đóng vai user chỉ dùng CLI công khai (+ curl duy nhất cho inbox-reconcile
vì CLI chưa có lệnh này) trên cụm mới hoàn toàn, model `ollama/qwen3:4b`
free local, commit `c6935e6`:

- `daemon start` → `meta` (api v1, providers `[opencode]`, profiles 0) →
  `profiles create` (model advertised `supported`) → `init` → `sync` →
  `plan import/approve` → scan tự dispatch, không cần `POST /dispatch`.
- Worker turn ~2 phút: `logs` trống cho đến khi `reconcile` (attempt) +
  inbox-reconcile (curl) kéo observations; artifact `NOTES.md` đúng 2 bytes
  `GOALS`. Verdict sai attempt id bị từ chối `foreign verdict` đúng thiết
  kế; verdict đúng → task `Succeeded`.
- Lần 1 model kẹt giữa reasoning (serve idle, 0 observation sau 10 phút):
  `stop --run` → attempt `Cancelled`, serve child bị kill (không leak như
  trước fix). Thử lại task ngắn gọn thì pass — bài học user: task càng cụ
  thể/ngắn càng ít kẹt.
- `budget`/`set-limits` (max_attempts 5, gate open), `pause`/`resume`,
  `events` (13 items + cursor), `recovery` (0 actions khi healthy),
  `daemon shutdown` → restart → run park `AwaitingResume`, resume mở lại,
  attempt Completed giữ nguyên. `artifacts` trống (đúng: worktree file
  không phải artifact blob).
- Ma sát user ghi nhận (không chặn): (1) tự viết plan JSON dễ sai schema
  (thông báo lỗi tốt, nêu rõ field lạ); (2) không có `central
  inbox-reconcile` — phải curl với session ref lấy từ status, gồm cả epoch
  gateway; (3) không có cancel từng attempt (chỉ `stop` cả run); (4)
  `logs --attempt` với attempt id sai trả rỗng im lặng thay vì 404.

## 8. Doc fix kèm theo (comment/pin stale từ commit Muse/opencode)

- `muse/version.rs`, `muse/gateway.rs`: pin 1.4.23 + handshake live 17/11
  (trước đó ghi 1.4.20 / "NOT live-exercised").
- `opencode/gateway.rs`: đồng bộ với spec (deny/interrupt đã lab 17/11,
  không artifact máy-chấm).
- `muse-gateway-spec.md` §4: pin `1.4.23` (allowlist giữ `1.4.20`).

## 9. Live smoke: run profiles + leader dispatch (24/11, commit hiện tại)

- `profiles use --role worker|leader` đều `applied`; snapshot trả
  `worker_profile` + `leader_profile` đúng id đã bind.
- Task không pin: worker dispatch qua run default, không cần dispatch
  tay (scan tick tự gửi qua gateway tới serve child, session attached).
- `leader ask` → attempt id; scan dispatch leader qua leader profile,
  serve child riêng, session riêng; task giữ `Pending` suốt review.
- Phát hiện khi live: leader active trước thì worker không queue
  (`covering_attempt` tính cả leader) — đã ghi thành covering rule
  trong `docs/api.md`.
- `stop` hủy cả worker lẫn leader attempt, child bị reap ở tick kế
  (cancel async qua outbox, không instant) — không leak.
- Chưa xong live: cả 2 đợt turn opencode+ollama đều kẹt sau hoạt động
  đầu (worker đã ghi file, serve idle `ep_poll`, ollama unload model,
  log không thêm) — cùng pattern kẹt đã ghi ở §7, do phía
  opencode/ollama chứ không phải dispatch (dispatch + session attach
  đều pass). Hoàn tất turn → reconcile → seal → verdict live còn chờ
  đường model ổn định; black-box `leader_review_dispatches_through_run_leader_profile`
  đã cover dispatch qua HTTP + scan trong suite.
- Đợt 3 (24/11 tối, run `6f26aa00`, leader-only, prompt no-tools 1 câu):
  UNVERIFIED. Dispatch 19:06 (serve child + session + MCP connect trong
  log), model có load GPU, nhưng log im từ 19:06:57Z; serve child chết
  im lặng giữa 19:12–19:15 (không error trong opencode.log, daemon.log,
  dmesg/journal), attempt vẫn `Running` (stale). Timeout 8 phút → `stop`:
  attempt `Cancelled`, không còn serve child. Kết luận giữ nguyên:
  dispatch pass không phải turn pass; completion live của leader turn
  vẫn chưa được chứng minh.

## 10. Muse userInput/question (24/11 tối)

- Code path đầy đủ và đã có test round-trip mới
  `question_round_trip_reaches_the_wire` (stub server hỏi
  `userInput/request` giữa turn → drain ra Question đúng preview →
  answer thành đúng 1 frame `userInput/answer` với `selectedLabel` →
  turn vẫn completed). Pass. Lần đầu viết test còn bắt được đúng
  protocol MSP two-step (receipt ack trước, decision sau) — đã sửa stub
  cho đọc tiếp frame 2 và trả lời command.
- Live qua daemon (run `551e5bcb`, profile muse/muse-spark-1.3):
  dispatch pass, session bind, serve child sống, reconcile thấy
  Running. Nhưng session CLI đóng băng từ lúc dispatch (12 phút không
  file mới, không question, inbox trống) — model không hoạt động,
  nghi thiếu owner auth (`~/.muse` trống, không key env).
- `stop`: attempt `Cancelled`, serve child bị reap — dọn sạch đã xác minh.
- Kết luận: nửa vòng gateway (hỏi → đáp) đã chứng minh bằng test;
  vòng live (model thật tự hỏi) UNVERIFIED — bị chặn trước cả bước hỏi,
  cần owner auth rồi thử lại với task ép hỏi.

## 11. opencode permission live (24/11 tối)

- Nửa vòng gateway đã có cover: loopback test
  `loopback_wire_speaks_verified_protocol` (permission request → answer
  Once scope → `once` trên wire) + lab 17/11 reply/reject. LIST poll
  live-verified từ trước.
- Ép hỏi live: run `bf43a926` task shell `echo PERM-PROBE > probe.txt`
  — bash CHẠY KHÔNG HỎI (auto-approve cả shell, probe.txt đúng nội
  dung), turn sau đó kẹt im như pattern cũ. Run `4a2d5809` thêm
  `opencode.json` (`{"permission": {"bash": "ask"}}`) vào fixture —
  bash CHƯA BAO GIỜ CHẠY trong 12 phút (không probe.txt, inbox trống),
  serve idle, ollama unload: cùng pattern kẹt, không kết luận được rule
  có hiệu lực hay không.
- `stop` cả hai run: attempt `Cancelled`, không còn serve child.
- Kết luận: vòng hỏi→duyệt→tiếp tục live của permission vẫn
  UNVERIFIED — bị chặn bởi turn kẹt (lần 5 liên tiếp tối nay, cả
  opencode lẫn muse), không phải bởi Central (dispatch/attach/stop đều
  pass). Cần đường turn ổn định rồi thử lại ask-rule.

## 12. Tách biến model nặng vs harness kẹt (24/11 tối)

- Giả thiết: turn kẹt vì `qwen3:4b` nặng/chậm. Thử `qwen2.5:3b`
  (1.9 GB, profile `live-qwen25`, run `fdff6e1f`, cùng ask-rule +
  cùng task shell).
- Kết quả sau 10 phút: worktree mới TRỐNG (probe `PERM-PROBE` thấy ở
  poll là file cũ của run trước), inbox trống, serve child sống nhưng
  idle (6 s CPU/10 phút), ollama không load model — ĐÚNG pattern kẹt
  của `qwen3:4b`. `stop`: attempt `Cancelled`, không còn serve child.
- Kết luận: biến model bị LOẠI — kẹt không phụ thuộc model (3b cũng
  như 4b). Blocker nằm ở phía harness (tương tác opencode serve ↔
  ollama hoặc prompt/môi trường), và signature kẹt của Muse giống hệt
  nên cũng chưa chắc do auth. Vòng hỏi→duyệt permission live vẫn
  UNVERIFIED; bước tiếp theo là chẩn đoán stall ở tầng harness thay vì
  đổi model.

## 13. Sửa kết luận kẹt: turn xong trong ~90s, Central thụ động (24/11 tối)

- §12 sai ở điểm "harness kẹt": bắt quả tang qua REST của serve cho
  run `e1301c86` (attempt `fa28821f`, model `qwen2.5:3b`) — 6 chat call
  200 trong 42 giây, session `outcome: succeeded`, permission list
  TRỐNG. Turn hoàn tất phía serve; Central vẫn `Running` vì scan KHÔNG
  observe tự động — chỉ inbox-reconcile (curl-only, chưa có CLI) mới
  kéo observation (`observations: 1`, 2 event `ObservationRecorded`).
  Đúng thiết kế passive (§3 bước 5), nhưng là ma sát đã ghi ở §7.
- Vì sao model không làm gì: prompt ~60k token bị ollama cắt còn ~2k
  (`truncating input prompt`, llama-server `-c 4096`). Run `ccb0e3d8`
  gọi tool `read` vào path ảo `.ag-1442977557` → error → chat suông
  hết turn (`succeeded` mà không artifact, không permission). Sáng cùng
  truncation nhưng may mắn grounding được nên full loop.
- Permission live VẪN UNVERIFIED: tối nay không turn nào thử gated
  action (model còn chưa grounding xong). Muốn ép được phải hết cắt
  context (tăng ctx ollama — quyết định của owner, chưa đụng).
- `stop` dọn sạch attempt + serve child như mọi khi.

## 14. Root cause clarity: 846 skills phình prompt 13 lần (24/11 tối)

- Chuỗi chứng cứ: prompt serve ~60k token trong khi direct run cùng
  model cũng 60k — đều auto-compact ngay call đầu → model làm theo
  template summary (ghi file summary rác, gọi tool vào path ảo,
  chat suông) → turn `succeeded` mà task không xong.
- Tách biến bằng `HOME=/tmp/fakehome` (không skills): prompt còn
  4666 token, model trả đúng `DIRECT-OK`, exit 0. Chênh 60k−4.7k đến
  từ 846 skills trong `~/.agents/skills` (3.5 MB) mà opencode tự load.
- Kết luận: blocker của mọi gap live tối nay là config bloat phía
  owner, KHÔNG phải Central (dispatch/observe/reply đều pass),
  không phải auth opencode, không phải model yếu, không phải
  truncation tự thân (direct run cùng cắt mà xong khi prompt gọn).
- Permission live vẫn UNVERIFIED nhưng đường đã thông: model
  grounded sẽ thử gated action → serve park → Central drain qua
  inbox-reconcile → approve → tiếp tục. Cần prune skills (hoặc ctx
  vừa 60k+) rồi chạy lại probe ask-rule.
- Giữ nguyên `qwen2.5:3b-64k` (load được `-c 32768`, có ích khi prompt
  gọn) và fixture ask-rule `c1ecbd6` cho lần thử lại.

## 15. Tách biến skills bằng symlink (24/11 tối, theo ý kiến owner)

- Owner cho rằng skills không phải thủ phạm nên không prune. Kiểm
  chứng bằng `HOME=/tmp/fakehome2` chỉ symlink `.agents` (không MCP,
  không config khác): prompt vọt lại 59070/59108 token (so với 4666
  khi HOME sạch). Skills là delta duy nhất — kết luận bloat đứng vững
  về số liệu; quyết định không prune là của owner, tôn trọng.
- Chi tiết mới: call đầu 4666 rồi mới 59k — skill injection bất đồng
  bộ (prompt đầu đi trước khi skills load xong). Model sau đó gọi
  `read summary.json` (ảo giác từ template) lỗi rồi chat suông.
- Hệ quả cho gap permission: không prune thì cần ctx vừa 60k+ (VRAM
  hiện tại ollama chỉ dám cấp 32k cho 3b) hoặc giảm skills phía
  opencode. Vòng hỏi→duyệt live vẫn UNVERIFIED vì model chưa bao giờ
  grounding đủ để thử gated action.

## 16. Vòng permission live hoàn chỉnh + 2 bug (24/11 tối)

- Hỏi: run `8f022556` (ask-rule bash+edit) model thử
  `shell [echo PERM-PROBE > probe.txt]` → Central drain thành inbox
  `Approval/Pending` đúng preview qua reconcile. Đây là lần đầu thấy
  permission thật qua daemon.
- Duyệt: BUG — `parse_key` từ chối part rỗng, mà key permission
  opencode luôn có `turn_id` rỗng → 400 mọi approve/answer. Đã sửa
  (cho phép turn rỗng, giữ chặn các part khác) + 2 unit test
  (`opencode_permission_keys_parse_with_empty_turn`,
  `malformed_keys_stay_rejected`). Rebuild + restart daemon rồi duyệt
  lại: `applied` rev 10.
- Tiếp tục: reply `once` tới serve (permission list về `[]`),
  bash CHẠY, `probe.txt` đúng `PERM-PROBE`. Vòng hỏi→duyệt→thực thi
  live VERIFIED.
- Đuôi turn: attempt `UnknownOutcome` (reconcile dồn dập của tôi đua
  với reply send) rồi model park sau tool (0 call 6+ phút, serve
  idle) — session vẫn mở phía serve. Ma sát quan sát/thiết kế, không
  thuộc vòng permission.
- Dọn: BUG/GAP thứ hai — stop chỉ enqueue cancel cho
  Running/Dispatching/Waiting*/(không có UnknownOutcome) nên child
  của attempt `UnknownOutcome` sống sót sau stop (serve 779204 sống
  14+ phút). Đã sửa (UnknownOutcome vào cancel set, kill có kiểm tra
  identity) + test `stop_cancels_unknown_outcome_attempts`. Chưa kịp
  live-verify fix này vì child cũ chết theo restart daemon; kill-machinery
  đã proven trên Running attempts, unit test cover decision.
- Bài học vận hành: KHÔNG restart daemon giữa turn live (giết serve
  child, approve sau đó ghi vào session chết).
- Gate: fmt sạch, clippy sạch, `cargo check` sạch, full suite 44
  binary xanh (1 lỗi compile thoáng qua ở lần chạy đầu, 3 lần sau +
  check đều sạch — chưa tái hiện). Commit gộp sau theo yêu cầu.

## 17. Leader completion → worker proceeds (24/11 tối, run `76e546c9`)

- `leader ask` → dispatch qua leader profile, Running. Turn xong
  phía serve trong ~60s (`succeeded`, đúng một câu review
  grounded về seen.txt). Observation kéo về qua inbox-reconcile.
- Handover import: 1 verified fact + 1 unverified note đúng thiết kế
  (transcript serve không算 durable evidence).
- Phát hiện gap: không có gì kết thúc leader attempt → worker đói
  mãi (covering rule tính cả leader review). Đóng bằng lệnh mới
  `CompleteLeaderReview` (+ `POST /v1/runs/{id}/leader/finish` +
  `central leader finish`): attempt → Completed, task giữ nguyên,
  kèm cancel intent để reap serve child. `leader finish` live → attempt
  Completed, task Pending, child bị reap.
- Sau resume (restart park run theo fail-safe): worker `17b60575`
  dispatch qua run default → Running → ghi `seen.txt` đúng, session
  `succeeded`. Chuỗi leader→worker khép kín live.
- Stop UnknownOutcome live: KHÔNG dựng được instance sạch (cần race
  đúng lúc reply/observe; không chế tạo cưỡng bức). Đã cover bằng unit
  test `stop_cancels_unknown_outcome_attempts` + kill-machinery proven
  live trên Running/WaitingForApproval. Ghi nhận là đường live duy
  nhất còn thiếu proof trực tiếp.

## 18. Muse userInput/question live (24/11 tối, run `f225db5b`)

- `muse exec` trực tiếp trả đúng `MUSE-DIRECT` → model ổn định, không
  phải auth (nghi vấn cũ bị bác).
- Worker task mơ hồ → model hỏi tiếng Việt 3 câu (tên file, nội dung,
  vị trí) → inbox `Question/Pending` → `central answer` applied rev 26
  → attempt `WaitingForInput` → Running, entry Resolved → model tạo
  `ANSWER.md` đúng nội dung (`MUSE-ANSWER`). Vòng hỏi→đáp→hành động
  VERIFIED live.
- Đuôi turn: model im sau khi hành động (session files đóng băng),
  serve child còn sống → `stop` dọn sạch (Cancelled + reap). Cùng
  pattern slow-close đã thấy ở opencode.
- Push 25/11: `c6935e6..a90627b` lên `origin/main` sau gate sạch trên
  cây không WIP (fmt/clippy/44 binary test xanh). Feature finish +
  evidence §§17–18 còn treo uncommitted.

## 19. Slow-close ba mốc thời gian (25/11, run timing opencode+Muse)

- opencode/qwen2.5:3b (run `36e47804`, task ghi ok.txt): dispatch
  10:22:03 → tool-call + chat 10:22:21–22 → serve `succeeded`
  10:22:25 (T0+22s, KHÔNG tạo artifact: model gọi `write` với input
  rỗng → error → chat). Central ghi observation lúc 10:26:50 sau
  reconcile tay (T0+4m47s). Provider xong trong 22s; 4m25s còn lại
  là Central chưa poll — phân biệt xong.
- Muse/muse-spark-1.3 (run `3f0968ec`, task ghi done.txt): bind
  10:27:26 → `done.txt` đúng `DONE` lúc 10:27:33 (T0+7s, grounded,
  hành động thật) → journal im từ 10:28:25 (đuôi slow-close) →
  crash OOM giết serve. Central không bao giờ reconcile kịp.
- Kết luận: provider (cả hai) hành động/xong trong vài chục giây;
  "kẹt" quan sát được trước đây = Central thụ động + đuôi slow-close
  phía model sau khi đã hành động. Không phải nghẽn dispatch.

## 20. Auto-observe bounded trong scan tick (code + test, chưa deploy)

- Thiết kế (giữ nguyên passive): cuối `Scheduler::tick`, với mỗi
  attempt `Running` có session binding thì drain requests + observe
  messages, commit qua `RecordExternalRequest`/`RecordObservation`
  (cả hai dedup sẵn, MAX_OBSERVATIONS=4096 có evict). Không answer,
  không resend, không mark terminal; poll lỗi thì bỏ qua tick đó
  (attempt giữ nguyên status — UnknownOutcome vẫn cần reconcile tay).
  Attempt parked/non-Running bị skip (drain approval tự park nó).
- Test mới trong `scheduler.rs` (24/24 xanh): observe+drain ghi
  đúng, closed transport không ghi gì và không mark terminal, event
  muộn ghi đúng 1 lần. Restart: binding epoch cũ không match session
  mới → gateway trả rỗng/lỗi → skip, không fabricate (covered bởi
  closed-transport test).
- Chưa restart daemon để nạp (đợi quyết định deploy).

## 21. Release thử 0.1.0 + ma trận provider (25/11)

- `cargo build --release` (JOBS=4, tránh OOM): pass 12m45s,
  `target/release/central` 43.7 MB, `central 0.1.0`.
- Ma trận tại máy kiểm chứng: opencode 2.2.7 allowlisted, live
  full loop (hỏi→duyệt→thực thi 24/11); muse 1.4.25 allowlisted,
  live dispatch + userInput loop (24/11); codex 0.169.0 KHÔNG tương
  thích (allowlist giữ 0.167.0-alpha.3.2 + 0.167.1) → fail-closed
  đúng thiết kế, cần capture mới trước khi mở; ollama local
  (qwen3:4b, qwen2.5:3b/7b, qwen2.5:3b-64k) free, không auth.
- WIP treo: auto-observe (code+test xong, chưa commit/deploy);
  inbox-reconcile thiếu CLI (curl-only); stop-UnknownOutcome live
  chưa proof trực tiếp (unit test + kill-machinery proven); đuôi
  slow-close sau hành động (cả 2 provider); prompt 60k do skills
  (owner quyết không prune); /tmp làm state dir bị dọn sau crash —
  nên chuyển state ra chỗ bền.
- Commit đề xuất tiếp theo: auto-observe + evidence §§19–21 (hiện
  treo: `scheduler.rs`, `tests/scheduler.rs`, doc này).

## 22. Auto-observe live proof + metric lesson (25/11)

- Run `07d8144a` (opencode, KHÔNG reconcile tay lần nào): turn xong
  phía serve, scan tick tự ghi `ObservationRecorded` ("turn
  completed: stop, usage in=2046 out=60") vào durable log. Vòng
  Central-thấy-turn-xong đã tự động — đúng mục đích auto-observe.
- Bài học đo: snapshot `status` KHÔNG mang observations (projection
  curated) nên poll `obs: 0` là mù; events mới là nguồn thật. Mọi kết
  luận "không observe" trước đây phải đối chiếu events.
- Turn này model vẫn chat suông (out=60, không artifact) — cùng bệnh
  grounding, không liên quan observe.
