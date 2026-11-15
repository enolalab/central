# Windows live acceptance — 2026-11-15 (fixture mới, chuỗi chưa hoàn tất)

Fixture mới (fixture-owned, không đụng fixture cũ):
`C:\Users\Administrator\AppData\Local\Temp\central-live-daemon-20261115-accept`
Fixture cũ `central-live-daemon-06e3a1fac9654dada8b76f7bac2f9bc3` được giữ
nguyên, không resend turn cũ.

## Binary và cấu hình (đã xác minh bằng hash)

- `central-server.exe` SHA-256
  `4eed999f000ed62686ef82a566e8ec0d203643cf9100cb21cd` (build lại trên VM
  từ cây Linux hiện tại: fix observe/process-notification + hỗ trợ
  `windows_sandbox` trong `codex.json`).
- `central.exe` SHA-256
  `9b1b078507200c476b54506f01933377396adc9aace7a0f31f7419f0c6eb0d11`.
- Codex CLI `0.167.1` native
  (`.../codex-win32-x64/vendor/x86_64-pc-windows-msvc/bin/codex.exe`),
  model `gpt-6-luna`/`low`, không fallback, override
  `windows_sandbox: unelevated` trong `state/codex.json`.
- Phát hiện quan trọng giữa chừng: 4 attempt đầu Failed với `readOnly`
  vì binary trên VM thiếu `-c windows.sandbox=...` (cây VM đã bị restore
  sạch `bootstrap.rs` sau build 14/11, mất `app_server_args`). Đã copy
  `bootstrap.rs` + `gateway.rs` từ Linux (hash khớp `1d1eca4f…`,
  `82b4df97…`), rebuild, daemon mới nhận override và báo
  `workspaceWrite`. Fix này thuộc Central, đã có test hồi quy trên Linux.

## Turn 1 — worker qua daemon: PASS ở tầng turn

- Run `be64e2b9-e839-47a9-8f08-028f4fa21818`, attempt
  `4c1cb714-f1b3-4d82-ae29-abaca6820d23` (task `...a1`, profile `...a9`).
- `thread/start` báo `workspaceWrite`; `turn/completed` đã được observe.
- `POST /v1/runs/{id}/inbox/reconcile` trả **HTTP 200** với body đầy đủ
  — kiểm chứng live cho fix observe (trước đây 503 vì notification
  `account/updated`, `thread/started` không threadId). Harness đã lưu
  toàn bộ body lỗi/thành công vào file `w-reconcile-*.json|err`.
- Candidate seal qua `result import` (rev 224, `awaiting_verification`):
  `candidatePath: worktrees/4c1cb714-...-0/calc.py`,
  `candidateHash: 13c709cbf0340181e1b1dc45d8029ad685c4a2dba838922951301c0ece73cc5e`
  (sha256 bytes file),
  `inputHash: 9553fe2686874347c0385e0ff8634a8b274ee6aabd1ee4916d0efc422a2c8724`
  (= sha256 rendered input), `criteriaHash:
  722449083380e1a2218f15374a7c014fc975d052e36f9e855bd8b8ce1c3da5e1`.
- Token usage của worker: Central không quan sát được — ghi nhận unknown.

## Turn 2 — verifier 1 (direct CLI): Blocked trung thực (rev 225)

- Thread `01a1e0e2-49c7-7f42-8328-d1ee51afc0ed`, 12,937 tokens.
- Không chạy được check nào (pwsh bị policy chặn trước khi start).

## Turn 3 — verifier 2 (direct CLI + unelevated): Blocked trung thực (rev 226)

- Thread `01a1e0e5-df81-76a1-82ad-52d8f840c0dd`, 7,288 tokens, sandbox
  `workspaceWrite` (xác nhận `-c windows.sandbox` có tác dụng cả đường exec).
- Đọc được file nhưng `python` (Store alias lúc đó) và `certutil` trong
  sandbox đều không chạy — không có assertion evidence nào.

## Turn 4 — verifier 3 (cmd-only + python path đầy đủ): Blocked (rev 227)

- Thread `01a1e108-6a5c-7e92-87f6-7b7a1118ee88`, 10,029 tokens.
- Chỉ lệnh `echo` trần chạy được; mọi lệnh có quote lồng nhau đều vỡ
  (`The directory name is invalid`). Python thật và certutil đều không
  thực thi được — không có check/SHA nào.

## Turn 5 — verifier 4 (node script): Blocked (rev 228)

- Thread `01a1e197-b96e-7d33-ba14-69b7530d0afb`, 15,963 tokens.
- Model viết đúng node script kiểm tra, nhưng lệnh gọi node có path
  quote đều vỡ qua sandbox runner. Không check nào chạy, không SHA.

## Turn 6 — verifier 5 (short path 8.3): Blocked (rev 229)

- Thread `01a1e19c-442c-77e0-ae77-9bd05d15ce1b`, 8,406 tokens.
- Mọi lệnh trần không quote chạy được (`type` đọc đúng file) nhưng
  `node.exe` thoát `0xC0000142` — cùng lớp lỗi với pwsh/python.

## Turn 7 — verifier 6 (2 lệnh trần): Blocked (rev 230)

- Thread `01a1e19d-f747-76a2-9250-0dd58a99b793`, 7,078 tokens.
- `type calc.py` đọc đúng; `node.exe` vẫn `0xC0000142`.
- Model kết luận static (bool PASS theo code, add FAIL vì thiếu evidence
  thực thi) — operator submit **Blocked** (không phải Fail lên candidate),
  vì không có check nào thực sự chạy.

## Recipe check không cần model (operator tự chạy, KHÔNG phải verdict độc lập)

- `C:\PROGRA~1\nodejs\node.exe C:\Users\Administrator\AppData\Local\Temp\vcheck.js`
  (node spawn python bằng argv array, hash bằng node:crypto, chạy ngoài
  sandbox qua SSH): `SHA-256: 13c709cb...` (khớp sealed candidate),
  `ADD_PASS`, `BOOL_PASS (True, 1)`, `BOOL_PASS (1, False)`,
  `ALL_PASS`, `PYTHON_EXIT=0`.
- Chứng minh candidate ĐÚNG và recipe chạy được — nhưng đây là operator
  tự chạy, không thay thế verifier model độc lập.

## Turn 8 — verifier 7 (được duyệt — PASS, CHỐT)

- Thread `01a1e1a9-2820-75a2-8636-91e066e51f27`, 3,188 tokens,
  sandbox `workspaceWrite`.
- Chạy đúng 1 lệnh trần duy nhất (không quote, short path 8.3):
  `C:\PROGRA~1\Python313\python.exe ...\vcheck2.py` → exit 0.
- Kết quả thực thi: `SHA256=13c709cb...` (khớp sealed candidate),
  `ADD_PASS`, `BOOL_PASS (True, 1)`, `BOOL_PASS (1, False)`,
  `ALL_PASS`.
- Verdict **Pass** với executed evidence đã submit (rev 231).
- Task worker → **Succeeded**; `verified_outputs` bind đúng
  `candidate_hash` (worker→verifier provenance đầy đủ).

## Git integration + promotion (mechanical, không tốn model turn)

- Task integration `...a2` tự dispatch sau khi worker Succeeded;
  attempt `91077b0b...` Completed.
- Candidate seal thành commit
  `28da318 "central: seal candidate for attempt 4c1cb714..."`;
  approved checks (`git cat-file -e HEAD:calc.py` baseline + acceptance)
  pass; `RecordIntegration` đã dời `refs/heads/central-good` → `28da318`
  (master giữ base `0962eed`).
- `git show central-good:calc.py` chứa đúng file đã fix (guard bool).
- Repo là fixture-owned; không động đến source repo thật.

## Kết luận chuỗi (HOÀN TẤT — đủ điều kiện chốt MVP)

- 1 worker (daemon, workspaceWrite, reconcile 200, seal rev 224) ✓
- 1 verifier độc lập (direct CLI, thread khác, Pass với executed
  evidence + SHA khớp, rev 231) ✓
- Git integration + promotion đúng nội dung ✓
- Tổng 8 model turn, tất cả đều được duyệt từng cái; no-model work
  (6 reconcile-200, 3 Blocked trung thực, integration) ghi log đầy đủ.
- Code Central sửa trong đợt này: gateway observe (bỏ qua
  process-notification) + `windows_sandbox` override trong bootstrap
  (đang là uncommitted changes, binary acceptance đã bao gồm).
- Khuyến nghị: CHỐT MVP.
