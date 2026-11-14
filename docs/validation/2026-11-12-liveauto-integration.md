# Nghiệm thu live 2026-11-12 — scheduler-auto integration + usage ledger

- Host: Linux x86_64, Rust/Cargo 1.99.1. Codex CLI `codex-cli 0.167.1`
  (allowlisted; subset tại `contracts/codex/0.167.1/`).
- Revision code lúc chạy: sau `59ce19e` + worktree hiện tại (chưa commit);
  fix integration (`git.rs`, `workspace/mod.rs`) + 2 test mới đã nằm trong
  binary daemon dùng cho leg scheduler-auto retry.
- Model/effort yêu cầu: `gpt-6-luna` / low, không fallback. Profile
  `live-codex` qua `POST /v1/profiles` trả capability `supported` với
  detail `model 'gpt-6-luna' advertised by 'codex'`. Giao tiếp qua gateway
  stdio/RPC của daemon (`codex.json` trong state-dir:
  `{binary: codex, cli_version: 0.167.1, surveyed_models: [gpt-6-luna]}`).
  Thiếu `codex.json` daemon từ chối an toàn (`UnavailableGateway`,
  capability `unknown`, dispatch 0) — đã gặp 1 lần khi quên file, không
  sinh process agent nào.
- Ngân sách: mỗi turn là 1 codex turn có giới hạn; worker 1 turn, verifier
  1 turn; dừng ngay khi lỗi (không quota/auth lỗi trong lần chạy này).
  Tổng 3 live turn trong ngày (1 worker run usage cũ + 1 worker + 1 verifier
  run auto2).

## Run scheduler-auto: `6db140c7-53a8-4be3-8a0c-ea91c8cea979`

State dir: `/tmp/central-liveauto-auto2` (giữ lại). Fixture repo:
`/tmp/central-autofix-auto2` (git, base `6bc3ecb1`, file `calc.py` chưa
guard + `test_calc.py` assert `add(1,2)==3`). Scan 15 s.

1. `CreateRun` → `ProposePlan` → `ApprovePlan` (rev 3). Plan: task 0001
   implementation (profile live-codex, verify method `check`, criteria
   `calc.py rejects booleans` + `add(1,2)==3 passes`, maxRepairRounds 1);
   task 0002 integration (`dependsOn` + `dataDependsOn` 0001,
   `integration.goodRef = refs/heads/central-good`, baseline
   `python3 test_calc.py`, acceptance `python3 -c <bool-guard>`).
2. `POST /sync` (base), `POST /dispatch`. Scheduler dispatch worker qua
   gateway: attempt `7b9d8806`, session thread
   `b0dc92c3-de4c-4712-965c-859401ed4a2c`, instance epoch `41fdd378…`.
3. Reconcile trong turn (verbose, mỗi 10 s) ghi nhận:
   - `turn/started`, `turn/completed` (event `ObservationRecorded`,
     dedup `turn/completed:…::646`).
   - 3 `thread/tokenUsage/updated`:
     `total=22013 in=21921 cached_in=11520 out=92`,
     `total=44276 in=44059 cached_in=33280 out=217`,
     `total=66609 in=66349 cached_in=55296 out=260`
     (context 258400, reasoning 0; "observed on the wire, not ledgered").
   - Worktree worker: `calc.py` có bool guard, `test_calc.py` giữ nguyên.
4. Daemon chết giữa turn (timeout của harness ngoài, không phải crash
   product — log chỉ có startup lines; không orphan process codex nào của
   run này). Restart cùng state-dir: recovery `runs_seen=1
   awaiting_resume=1` → `Resume` (lệnh unit qua `/v1/commands?run=`) →
   run `Running`. Đây đồng thời là bằng chứng restart/recovery: turn đã
   `completed` trước khi daemon chết nên không mất.
5. Import worker result qua
   `POST /v1/runs/{id}/attempts/{aid}/results` (200, `awaiting_verification`):
   input `sha256(objective)=2afd6027…`, candidate
   `sha256(calc.py bytes)=2ce8d715…`, criteria `e8e4157d…` (= criteria của
   task trong snapshot).
6. Verifier độc lập: `codex exec --json -m gpt-6-luna -c
   model_reasoning_effort="low" --skip-git-repo-check` trên BẢN COPY
   (`/tmp/verify-auto2`, read-only, không đụng worktree sealed). Verdict
   thật của model: `PASS` + findings + `sha256(calc.py)=2ce8d715…`
   (khớp sealed). Thread verifier
   `01a1cd8d-011b-77a0-ad2b-f2da657cb584` ≠ worker thread (độc lập,
   domain check qua). Usage verifier turn: in 62907/cached 43648/out 288
   (ghi nhận, không fold vì ngoài daemon).
7. `SubmitVerification` qua `POST /v1/commands?run=` (actor owner = User):
   200 → task 0001 `Succeeded`, `verified_outputs[0001]=2ce8d715…`.
8. Scheduler dispatch task integration 0002 (attempt `c4346b7b`) → **Failed**
   do bug product (xem dưới). Task Failed giữ nguyên làm bằng chứng.
9. Fix bug + 2 regression test (xem dưới). Thêm task 0003 (cùng spec) qua
   plan revision (Propose+Approve, rev 109) — operator action hợp lệ;
   task Failed cũ không bị sửa (by design: revision chỉ chạm Pending).
10. Scheduler tự dispatch 0003 (attempt `2f8f374f`, **không gọi endpoint
    integration thủ công**): seal → prepare → checks (`python3 test_calc.py`
    exit=0, acceptance exit=0) → CAS **tạo mới** `refs/heads/central-good`
    (`previous_head: ""`, `new_head: 1e5a385e…`). Event
    `IntegrationPromoted` (seq 68) + task 0003 `Succeeded` +
    `verified_outputs[0003]=1e5a385e…`.
11. Xác minh trong repo: `git rev-parse refs/heads/central-good` =
    `1e5a385e135c4ce4c4eb5964c628677e5b3fe4a0` ✓.

## Bug phát hiện bởi live run + fix

1. **First-promotion tới good ref chưa tồn tại luôn lỗi.** Scheduler resolve
   `expected_parent: None` bằng `rev-parse --verify <ref>` → git báo
   `fatal: Needed a single revision` → round lỗi → `AttemptFailed`
   (retry policy mặc định hết ngay). Mọi repo mới (ref Central chưa từng
   tồn tại) đều dính.
2. **Leak slot in-memory + branch/worktree tạm khi lỗi sau prepare.**
   Mọi `?` sau `prepare_integration` (check-runner error, rev-parse, …)
   đều bỏ qua `finish_integration` → round sau 409 `already has an
   in-flight integration` + sót branch `central-integration/*` trong repo.
   Đã quan sát cả hai (409 + branch sót) trước khi fix.

Fix (không đổi contract API/port):

- `Git::try_read_ref` → `None` khi đúng diagnostic missing-ref, lỗi khác
  vẫn propagate (repo hỏng không giả vờ thành ref mới).
- `promote`: missing ≡ `""` khi so gate — (`""`,`""`)+pass = tạo mới;
  (`pin`,`""`) = conflict; mọi case cũ giữ nguyên (`gate_promotion`
  không đổi).
- `update_ref_cas` với old `""` dùng zero-OID (creation CAS của git: tạo
  khi chưa có, từ chối khi có concurrent create).
- `auto_integrate`: cả đuôi sau prepare chạy như một round; lỗi → 
...[truncated 3010 chars]