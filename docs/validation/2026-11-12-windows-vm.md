# Windows VM verification — 2026-11-12 (partial, 6/9 gates)

## Environment (observed, not inferred)
- VM: Windows 11 Enterprise Eval on KVM (`/home/k0walski/.central-vm/`), SeaBIOS + i440fx IDE, SSH 127.0.0.1:2223 (Administrator).
- Toolchain (observed via SSH): pwsh 7.7.0, rustc/cargo 1.99.1 via `C:\central\.toolchain\env.ps1`,
  gcc 16.2.0 (MinGW-W64 x86_64-ucrt-posix-seh, Brecht Sanders r3) at `.toolchain\mingw64\bin`.
- Repo under test: `C:\central` @ `59ce19e` + dirty tree (~38 entries, mirrors host working tree).
- VM resources at time of run: disk grown to 140GB (F: 80GB, target junctioned), RAM 12GB.

## Gate verdicts (verbatim from VM logs)

1. runtime-recovery — `C:\gate7.log` (2026-11-12 11:48):
   `RUNTIME RECOVERY GATE: PASS`
2. workflow-recovery — `C:\gate-wf5.log` (2026-11-12 12:12):
   `WORKFLOW RECOVERY GATE: PASS`
3. codex single-task (fake) — `C:\gate-codex2.log` (2026-11-12 14:57):
   `GATE PASS: one Codex task verified with immutable provenance` (`"mode": "fake"`)
4. gate-modes — `C:\gate-modes2.log` (2026-11-12 15:03):
   `ok: -RealModel rejected with an actionable message (exit 1)`
5. artifact_contract — `C:\t-art3.log` (2026-11-12 14:56):
   `test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out`
   (Note: `C:\t-art2.log` holds the older `libduckdb-sys` build failure from the disk-full episode; superseded by t-art3.)
6. mvp-e2e — `C:\gate-e2e6.log` (2026-11-12 16:38, full file 607B):
   `good ref promoted: 736c2665545e173a675a024ff761e0ba9c479b24`
   `E2E PASS: plan, 2 workers, independent verify, integration, restart barrier, provenance.`

## DuckDB gate
- Run 1 — `C:\gate-duck.log`: **FAILED (exit 1)**. Report
  `C:\Users\Administrator\AppData\Local\Temp\duckdb-gate-report-3964.json`:
  all 8 functional checks true (atomicity, recovery, duplicate, lock, io_failure,
  zero_lost_ack, starvation, reads), p95 32.29ms (<= 100), p99 40.2ms (<= 500),
  writes 1525/1525, reads 299/299 — but `sustained_writes_per_s = 24.92 < 25.0`
  (`spikes/duckdb-runtime-gate/src/main.rs:634-644`). Marginal perf miss on the
  4-vCPU qcow2 VM; NOT a correctness failure. No criterion changed.
- Run 2 (variance check, same machine/thresholds) → `C:\gate-duck2.log`: **FAILED (exit 1)**,
  `sustained_writes_per_s = 24.88` (report PID 3228). Consistent ~24.9 cap on 4 vCPU — not variance.
- Machine change (NOT a criterion change): VM bumped 4 → 8 vCPU
  (`run-bios.sh` `-smp 8`; host has 20 threads free). Threshold stays 25.0/s.
  Cautionary event: first reboot attempt used default boot `d` (CD first); killed via
  QEMU monitor before any Setup interaction. Disk verified intact (Windows login
  screen with Administrator + central users via screendump; repo/state untouched).
  Correct reboot with boot `c` (disk first). Guest reports 8 logical processors (SSH).
- Run 3 (8 vCPU, same thresholds) → `C:\gate-duck3.log`: **FAILED (exit 1)**,
  `sustained_writes_per_s = 24.52`, p95 52.04ms, p99 62.25ms, writes 1502/1502.
  Worse than 4-vCPU runs → suspect interference: guest top CPU was MsMpEng
  (Windows Defender real-time scan) at gate time. Next: verify/add Defender
  exclusions, confirm MsMpEng idle, rerun (run 4). Threshold unchanged.
- Defender exclusions added: `C:\central`, `%TEMP%` (verified via Get-MpPreference).
  MsMpEng measured idle (0.0625 CPU-s / 5 s ≈ 1.2% of one core) before rerun.
- Run 4 (8 vCPU + exclusions, same thresholds) → `C:\gate-duck4.log`: **FAILED (exit 1)**,
  `sustained_writes_per_s = 24.49`, p95 53.32ms, p99 62.86ms, writes 1500/1500.
  Exclusions verified present, MsMpEng idle — bottleneck is NOT vCPU/Defender.
- Verdict: 4-run series 24.92 / 24.88 / 24.52 / 24.49 — VM storage stack
  (qcow2 over IDE, single-writer fsync ~40.8ms) caps below the 25.0/s bar.
  All 8 correctness checks green in all 4 runs; product code identical to the
  2026-11-09 bare-metal PASS (Ryzen 9 7900, 25.8/s offered). No criterion changed,
  no product edit made to chase the number. DuckDB throughput on THIS VM =
  environment-blocked; recorded as such. (A virtio-blk migration was considered
  and rejected: guest lacks viostor drivers → INACCESSIBLE_BOOT_DEVICE risk to
  the only VM. A RAM-disk run was rejected: it would invalidate durability.)
- WAIVER (user decision 2026-11-13): block này được chấp nhận và bỏ qua.
  Căn cứ: adoption evidence là bare-metal PASS 2026-11-09
  (`docs/validation/duckdb-gate.md`); 4 runs VM chứng minh product đúng
  (8/8 correctness xanh), chỉ thiếu ~2% throughput do storage ảo.

## Release + zip smoke
- Run 1 — `C:\gate-rel.log`: binaries staged, no Node/UI payloads, but zip MISSED
  MinGW runtime DLLs. Zip smoke caught it: both exes died silent exit 1 =
  loader `0xC0000135 STATUS_DLL_NOT_FOUND` (proven via file-redirect probes).
- Fix (host `scripts/build-release.ps1` + synced to VM): stage
  `libstdc++-6.dll`, `libgcc_s_seh-1.dll`, `libwinpthread-1.dll` beside the exes.
  Minimal set proven by elimination (libssp/libatomic NOT needed).
- Run 2 — `C:\gate-rel2.log`: `central release build (server 0.1.0, Rust rustc
  1.99.1)`, `Finished release profile in 0.92s` (cached), `binaries staged;
  checking for unexpected Node/UI payloads...`,
  `release zip: C:\central\dist\central-windows-x64.zip` (57,409,951 bytes).
- Zip smoke (fresh `C:\smoke2`, default PATH, no toolchain/Rust/Node, no manual
  DLL copy): contents = contracts, docs, examples, central-server.exe,
  central.exe, 3 DLLs, THIRD-PARTY-NOTICES.txt.
  `central.exe --version` → `central 0.1.0`. `central-server.exe --version` →
  `central-server: unknown argument: --version` (correct: server takes
  `--state-dir/--listen/--help`, proven from `crates/central-server/src/main.rs`).
- Functional smoke: `central-server.exe --state-dir C:\smoke2-state --listen
  127.0.0.1:7879` + `central.exe --state-dir C:\smoke2-state meta` →
  `{"api":"v1","capabilities":{"fake_agent":false,...},"version":"1.0.0"}`.
  Release has NO fake agent. Server stopped after smoke.
- Independent client (`examples/client.ps1`, pure HTTP PowerShell, no crates/DB)
  against the ZIP binaries → `C:\gate-client.log`: `daemon api=v1`,
  create run, chat seq=1, plan approved at revision 3, inbox 0, plans 1 active,
  events paged + opaque-cursor resume + epoch reconnect ok, usage/recovery read;
  final: `independent client finished: create, chat, plan approve, inbox,
  events, usage and recovery over HTTP only.` Server + client ran in ONE SSH
  session (sshd job kills children on disconnect — separate sessions do not
  work). Wrapper `C:\run-client.ps1` used for env+Bypass; scratch removed after.

## Reboot acceptance — PASS (2026-11-12 ~19:15 EST guest time)
- Method: OS-initiated `shutdown /r /t 5` via SSH (full guest-OS reboot, NOT
  process kill/restart). SSH dropped; ~20 min black-screen boot at 213% CPU
  (same slow-qcow2 pattern as first boot); login screen confirmed via QEMU
  screendump (Administrator + central users); SSH returned on its own.
- Post-reboot: `C:\smoke2\central.exe --version` → `central 0.1.0`;
  repo still @ `59ce19e`; gate logs intact (rel2 258B, duck4 1298B, e2e6 607B).
  Nothing reinstalled, no state lost.
- Note: initiated by command, not a human finger on reset — but the guest
  kernel reloaded and every process (incl. sshd) restarted by the OS itself.

## Live Codex attempt — AUTH STOP, 0 turns (`C:\live-smoke.log`)
- `codex-cli 0.166.0` installed via npm as test dep (matches Linux host).
  No auth on VM; none created/retried/borrowed.
- Fixture canonical files in `C:\live-smoke` (input `BA1A531F…F18BC772`).
  Single bounded worker attempt (`--json -m gpt-6-luna
  -c model_reasoning_effort=low -s workspace-write --skip-git-repo-check`):
  `turn.failed` — `401 Unauthorized` from api.openai.com (ws + https).
- Usage at auth stop: model `gpt-6-luna`, effort `low` requested, turns `0`,
  tokens `unknown` (no auth, stopped per rule, no quota consumed).
- UPDATE: user logged in on the VM (READY check passed). 3-turn budget spent:
  turn 1 worker refused (read-only), turn 2 with `--approve-for-me -C`
  refused (read-only, no approval attempted), turn 3 diagnostic confirms
  read-only sandbox despite `workspace-write`. Fixture hash unchanged.
  Live worker on Windows: BLOCKED by CLI sandbox, auth OK.
  Detail: `docs/validation/p2-codex.md`.

## Browser client smoke — PASS (`C:\gate-browser.log`, Node v26.12.1)
- Driver `/tmp/driver-browser.mjs` (host) imports the real
  `examples/browser-client.mjs` via `file://` URL and drives ONLY its exports
  (`setToken/discover/snapshot/eventsAfter/streamEvents`) + raw fetch for
  negative paths. Daemon = ZIP binaries, fresh state dir, `origins.json`
  `{"origins":["http://localhost:3000"]}` written before boot. Server + driver
  ran in ONE SSH session (sshd job kills children on disconnect).
- `discover api v1`, create run, `snapshot control=Running`, `eventsAfter`,
  `streamEvents`: RunCreated replayed + PlanProposed pushed LIVE (chat is
  discussion-only, correctly absent from the event stream — verified in
  `read_models.rs`: `project_chat_message` vs `project_event`).
- Origin enforcement: strange `Origin` → 403 `forbidden`; listed → 200;
  missing → 200. Query-string token ignored → 401. Final: `BROWSER SMOKE PASS`.
- Environment note: VM had no Node; installed `OpenJS.NodeJS 26.12.1` via
  winget purely as test dependency. The shipped zip stays Node-free
  (proven by the earlier no-toolchain smoke).

## Full workspace suite (`cargo test --workspace --locked --no-fail-fast`)
- Run 1 (`-j 4`, `C:\gate-test.log`): **BUILD FAILED, `CARGO_EXIT:101`** —
  GNU ld `memory exhausted` linking test binaries (same OOM class as the
  earlier e2e link failure at 6GB RAM; 12GB RAM still too little for 4
  concurrent huge debug links). No test result is claimed from run 1.
- Run 2 (`-j 2`, `C:\gate-test2.log`): all suites green EXCEPT 3 test bugs
  (no product failure):
  `integration_api` 1+2 fail — approved checks hardcoded `sh` (`program not
  found` on Windows); `release_security` 1 fail — stale 404 for `/` (product
  correctly returns P1 503); `scheduler` 1 fail —
  `dispatch_provisions_isolated_scratch_per_attempt` compared non-canonical
  root against `\\?\`-prefixed recorded paths.
- Fixes (host, test-only, fmt-clean; synced to VM):
  `scheduler.rs` canonicalize root before `starts_with` (mirrors product
  containment); `release_security.rs` `/` → 503 + `capability_unavailable`
  (mirrors `api_contract.rs`); `integration_api.rs` cfg-gated checks
  (`cmd /C ...` on Windows, `sh -c ...` elsewhere).
- Run 3 (`-j 2`, `C:\gate-test3.log`, only the 3 fixed suites): ALL GREEN —
  scheduler 16/16, integration_api 3/3 (59.84s), release_security 3/3;
  the 4 previously-failing assertions pass by name. Combined with run 2,
  the FULL workspace suite is green on Windows.
- `cargo clippy --workspace --all-targets --locked -j 2 -- -D warnings`
  (`C:\gate-clippy.log`): **BUILD FAILED, no lint result** — F: drive full
  (0.4MB free); `ar` died writing `libduckdb.a` for clippy's own fingerprint
  (`No space left on device`). F: held 82GB debug (5 libduckdb generations
  ~24GB + 52GB deps).
- Freed 16GB: deleted 3 empty stub + 1 failed clippy-partial + 2 oldest
  complete libduckdb fingerprints (kept 2 newest: `af77b2`, `b01ca1`) +
  `debug/incremental` cache. If a kept fingerprint proves wrong, cargo
  rebuilds automatically (time cost only).
- Clippy run 2 (`-j 2`, `C:\gate-clippy2.log`): **PASS, `CLIPPY_EXIT:0`** —
  `Finished dev profile in 26m 43s`, zero warnings with `-D warnings` on the
  full workspace incl. the 3 fixed test files. (rustfmt absent from portable
  toolchain — fmt verified clean on host for the byte-identical synced files.)

## Host tree note (explains "uncommitted" lines in reports)
Host `/home/k0walski/Code/central` @ `59ce19e` with 34 modified + 4 untracked (`boot.log`,
`crates/central-domain/tests/integration_decision.rs`,
`docs/validation/2026-11-12-liveauto-integration.md`, `skills/` pre-existing).
The "uncommitted" wording is accurate for post-59ce19e work; no commit made without explicit authorization.
