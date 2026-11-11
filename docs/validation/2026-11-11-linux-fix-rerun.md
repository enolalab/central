# Linux fix rerun — 2026-11-11 (containment + Clippy, live Codex smoke)

Base revision: `a00a2b5fda46887fdd9dda8fbe2b9b32868d2cdf`.
Working tree at writing time: 3 files modified, uncommitted.
Update 2026-11-11 (late): all of it committed as `12e0217`
(user-authorized; `skills/` left untracked). Only edits after `12e0217`
remain uncommitted.
Host: Linux `x86_64`; Rust `1.99.1 (58cf8f9c3 2026-10-13)`, Cargo `1.99.1`.
Codex CLI: `0.167.1` (earlier rerun reported `0.166.0`; host has since moved).
`CXXFLAGS=-g0` only trims C++ debug symbols for the bundled DuckDB build;
test profile, debug assertions and test assertions stay enabled.

Prior report `docs/validation/2026-11-11-linux-rerun.md` is kept unchanged;
this file records only the fix rerun on top of it.

## Code changes (uncommitted)

- `crates/central-infrastructure/src/checks/runner.rs`: `normalize_join`
  kept the absolute root (`/` on Unix, `C:/` for a drive prefix) instead of
  dropping it, so an absolute worktree inside the work root proves
  containment; traversal (`..`), NUL, device namespaces (`//?/`, `//./`)
  and NTFS streams (`:` beyond a `C:/` drive prefix) still fail closed.
  `contained()` now detects absolute paths itself instead of relying on
  `Path::is_absolute` (which misses `C:/...` on Linux). Canonical
  comparison when the path exists still blocks symlink/junction escapes.
  Added 4 unit tests (`absolute_unix_path_keeps_leading_root`,
  `lexical_dot_segments_resolve_without_escaping`,
  `drive_prefix_is_preserved_lexically`,
  `device_namespace_and_stream_syntax_fail_closed`).
- `crates/central-infrastructure/tests/process_ownership.rs`: the three
  Windows-only imports and the `fail_assign` helper are now
  `#[cfg(windows)]`; portable consumers unchanged.
- `crates/central-infrastructure/tests/artifact_contract.rs`: the four
  Windows-only imports (`CheckRequest`, `CheckRunner`, `ApprovedCommand`,
  `SupervisedCheckRunner`) are now `#[cfg(windows)]`.

No assertion was loosened and no containment was widened to force green:
the previously failing integration test passes unmodified.

## Mandatory gates (this host, after the fix)

- `cargo fmt --check`: **PASS**, exit 0 (after one `cargo fmt` to apply
  the new code's own formatting).
- `CXXFLAGS=-g0 cargo clippy --workspace --all-targets --locked -j 12 -- -D warnings`:
  **PASS**, exit 0. The four Linux diagnostics from the prior rerun are gone.
- `CXXFLAGS=-g0 cargo test --workspace --locked --no-fail-fast -j 12`:
  **PASS**, exit 0: **241 passed, 0 failed, 1 ignored**
  (prior: 236 passed, 1 failed, 1 ignored; +1 fixed integration test,
  +4 new runner unit tests). The 1 ignored test is the opt-in
  `installed_cli_stdio_smoke`, run separately below.
- Installed CLI smoke (ignored test, run explicitly):
  `CENTRAL_CODEX_SMOKE=1 target/debug/deps/process_ownership-edfc1f0409d3c3f9 installed_cli_stdio_smoke --ignored --exact --nocapture`:
  **PASS**, exit 0 — `codex version: codex-cli 0.167.1`,
  `initialize ok (no inference performed)`. Proves CLI execution plus an
  app-server `initialize` response only; not live workflow evidence.

Deterministic suites observed green in the full run include `api_auth` 8,
`api_contract` 8, `api_events` 7, `api_workflow` 6, `artifact_retention` 3,
`release_security` 3, CLI `api_parity` 3, `local_runtime` 10,
`workflow_recovery` 7, `store_contract` 10, `quality_loop` 16,
`git_workspace` 4, `git_integration` 4.

## Live Codex smoke (bounded, fixture-owned scratch only)

- Scratch: fresh `mktemp -d /tmp/central-live-smoke-XXXXXX`
  (`/tmp/central-live-smoke-TXaKfA`), canonical `calc.py`
  (`sha256:ba1a531f…f18bc772`) + `test_calc.py`; real repo never sent,
  personal auth/config untouched (`--ephemeral`, `-c` overrides only).
- Budget set before running: at most 2 live turns (inside the P2 max-3
  budget), `timeout 120` per turn, model `gpt-6-luna`, effort low via
  `-c model_reasoning_effort='"low"'`, no silent fallback; stop rule: halt
  on first quota/auth error.
- Turn 1 (worker, `--sandbox workspace-write`, `-C` scratch):
  edit `calc.py` to reject booleans, keep `add(1,2)==3`. Exit 0.
  Usage: `input_tokens=62984 cached_input_tokens=53120
  output_tokens=187`. Result `calc.py`
  `sha256:2ce8d715…6e503b` (hash differs from input: edit proven).
  Local check: `add(1,2)==3` ok, `add(True,2)` raises `TypeError`.
- Turn 2 (independent verifier, `--sandbox read-only`, no edits):
  verdict `PASS` on both criteria. Exit 0.
  Usage: `input_tokens=41378 cached_input_tokens=31872 output_tokens=133`.
- Total: **2 turns**, both exit 0, no quota/auth error, no fallback flag
  in output. Honest limit: `--json turn.completed` carries no model field,
  so the observed model is recorded as requested-`gpt-6-luna`/low rather
  than independently confirmed from the wire output.
- Scratch removed after hashing; helper probe `/tmp/check_bool.py`
  removed as well. Nothing was written to the repo by the smoke.

## Wiring / GC spot-check (code read, no behavior change)

- Daemon wires the real `CodexGateway` when `codex.json` is present and
  falls back to `UnavailableGateway` (capability_unavailable, never fake)
  otherwise; result/handover import seals and commits through
  `Runtime::execute` (durable `RecordWorkerResult`, never validated-only).
  P4 step-8 wiring is present; the live end-to-end daemon workflow
  (worker → verifier → integration through the daemon with a live model)
  was **not** executed this turn — recorded unverified below.
- GC stays dry-run-only by contract (`POST /v1/maintenance/gc` refuses
  `dry_run:false` with 400; `gc_dry_run` skips non-`Stopped` runs,
  unknown usage/outcomes and caller-flagged referenced ids). No real
  deletion exists, so referenced/active/unknown data cannot be deleted
  through this surface. No change made.

## Live daemon probe (release-like binary, truthful config) — 2026-11-11

Binary: `target/debug/central-server` built WITHOUT `test-support`
(release behavior: `--fake-agent` refused, real gateway or nothing).
State dir: fresh `mktemp -d /tmp/central-live-daemon-XXXXXX`
(fixture-owned, removed after); `codex.json` truthful:
`{"binary":"codex","cli_version":"0.167.1","surveyed_models":["gpt-6-luna"]}`.
No personal auth/config touched; owner token minted by the daemon itself.

Observed through the product binary + API (no inference possible):

- Daemon stderr at boot:
  `central-server: gateway: codex CLI version not allowlisted: ignoring harness config`
  (allowlist is exactly `["0.167.0-alpha.3.2"]`; installed is `0.167.1`).
- `GET /v1/meta` → 200, `fake_agent:false`, `profiles:0`.
- `POST /v1/profiles` (`codex`/`gpt-6-luna`/`low`) → 200 with
  `capability: {"status":"unknown","detail":"gateway did not report
  capabilities: capability error: no agent gateway is configured in
  this build"}` — honest unknown, never a silent substitution.
- `CreateRun` → applied, revision 1; fixture 2-task plan proposed
  (revision 2) and approved by exact hash (revision 3).
- `POST /v1/runs/{id}/dispatch` → 200
  `{"dispatched":0,"still_pending":0,"unknown":0}`; both tasks stay
  `Pending` with `profile_id:null` and no attempts/sessions.
- Daemon child processes after dispatch: none; `pgrep -f codex` PID set
  identical before/after → the daemon spawned zero agent processes.
- `POST /v1/shutdown` → 200, daemon exit 0.

Conclusion: the live daemon workflow (worker → verifier → integration
through the daemon with model `gpt-6-luna`) is **BLOCKED on this host
by the pinned CLI version allowlist**, proven through the real
binary and API — not inferred from code reading. The daemon refuses
safely (unknown capability, zero sends, zero spawns) instead of
speaking an unvalidated protocol. Remediation is either installing the
pinned `0.167.0-alpha.3.2` CLI build or extending the allowlist with a
new protocol capture + subset (a product change needing re-validation;
not done here to force green). The direct-CLI 2-turn smoke above shows
the model account itself works; the product path stays closed until
the version question is resolved properly.

## Not rerun / blockers (no claim made)

- DuckDB suitability spike (`scripts/test-duckdb-gate.ps1`): Windows
  PowerShell gate, not executed on this host; `cargo test` builds (not
  runs) the spike binary. Historical Windows evidence stands as-is.
- Windows/PowerShell gates, release packaging, zip smoke without
  Rust/Node, manual reboot acceptance: no `pwsh` or `.toolchain/env.ps1`
  on this host — not rerun, not inferred from Linux results.
- Live P4 daemon workflow with model (worker → verifier → integration
  through the daemon): **BLOCKED + UNVERIFIED** — the earlier probe proved
  the allowlist cause; the transport wiring below removes that cause but
  the live worker leg still needs dispatcher→worktree cwd wiring (see
  transport section). Only the direct-CLI 2-turn smoke is proven.
  No live/MVP gate is claimed.
- Protocol probe 2026-11-11 (late, installed CLI `0.167.1`): initialize
  result keys match the pinned subset; `thread/start` returns observed
  model `gpt-6-luna` + `reasoningEffort low`; `turn/start` response and
  `turn/completed` shapes match; usage arrives via
  `thread/tokenUsage/updated` (turn 1: in 21601/cached 11520/out 8).
  New vs fixture: `remoteControl/status/changed`, `account/updated`,
  `mcpServer/*`, `thread/tokenUsage/updated`, `account/rateLimits/updated`
  notifications; approval surface
  (`item/commandExecution/requestApproval`, `item/tool/requestUserInput`)
  still present in 0.167.1 schemas. Approval request+cancel answer NOT
  verified live (model answered in text, never requested approval);
  gateway answer path (`respond` + Once table) has no live proof and no
  dedicated unit test. Full wire details in the protocol-probe section
  below (appended 2026-11-11 late).
- Plan files: original `- [ ]` boxes preserved; a dated
  `Trạng thái nghiệm thu 2026-11-11` section was appended to each P1–P4
  plan (committed in `12e0217`) with new evidence-backed checkboxes only.

## Protocol verification of installed CLI 0.167.1 (2026-11-11 late)

Bounded probe: raw JSON-RPC over `codex app-server` stdio on
fixture-owned scratch (`/tmp/central-proto-*`, removed after), ephemeral
threads (nothing persisted), model `gpt-6-luna`, effort low,
`allowProviderModelFallback:false`. No repo sent, no config modified.

- `initialize` → result keys exactly
  `[codexHome, platformFamily, platformOs, userAgent]`, matching the
  pinned subset; `userAgent` reports `0.167.1`.
- `initialized` notification accepted; interleaved server notifications
  `remoteControl/status/changed` and `account/updated` observed (absent
  from the 0.167.0-alpha.3.2 fixtures — the gateway must tolerate unknown
  notification methods, which `parse_frame`/`ingest_wire_frame` do by
  buffering them as opaque events).
- `thread/start` → observed `model: gpt-6-luna`,
  `reasoningEffort: low`, `approvalPolicy: on-request`,
  `sandbox: workspaceWrite`, `ephemeral: true` honored; shape matches the
  subset (`thread{id,model,modelProvider}` + top-level `model`).
- `turn/start` → immediate response `{turn:{id,status:inProgress}}`,
  then `turn/completed` with full result items (`PROBE-OK` text,
  `status:completed`, `durationMs:6132`). Usage is NOT top-level in
  `turn/completed`; it arrives via `thread/tokenUsage/updated`
  (turn 1: `inputTokens 21601, cachedInputTokens 11520, outputTokens 8,
  totalTokens 21609, modelContextWindow 258400`). Gateway usage harvesting
  must read tokenUsage notifications.
- `codex app-server generate-json-schema --experimental` works read-only
  (47 top-level + 423 v2 files captured to `/tmp/schema-0.167.1`, kept
  outside the repo): `ThreadStartParams` keeps
  `model/cwd/sandbox/approvalPolicy/ephemeral/allowProviderModelFallback/config`;
  both approval methods
  (`item/commandExecution/requestApproval`, `item/tool/requestUserInput`)
  still present in `ServerRequest.json`.
- NOT verified: a live approval request + cancel answer (the model
  answered in text both times and never emitted a server request), and
  `thread/resume` (not exercised). Gateway `answer()` (`respond` + Once
  table in `PendingRequestTable`) has no live proof and no dedicated unit
  test in `requests.rs`.
- Transport gap confirmed in code: `CodexGateway::send` returns
  `DefinitelyNotSent` ("live stdio transport not attached"); `open()`
  spawns the app-server with `Stdio::null()`, so no RPC can ride the
  supervised process today. Wiring needs a supervised piped-spawn path
  (open design question — asked to user separately).

Windows gates: `pwsh` absent (exit 127), no `.toolchain/` — packaging,
zip smoke and reboot acceptance cannot run here; recorded as blockers,
never inferred.

## Transport wiring 2026-11-11 (late): stdio/RPC attached to the gateway

Scope decided with user: `spawn_piped` on the existing supervisor shape
(as a separate infra trait, so no tokio leaks into application ports),
allowlist extended to the verified `0.167.1`, wiring + acceptance now.

- `PipedSpawner`/`PipedStdio` (`process/piped.rs`): piped spawn shares
  the exact supervision core (kernel creation time, job assignment,
  identity-checked kill); default trait body refuses honestly.
  `OwnedProcessSupervisor` implements it; `spawn()` behavior unchanged.
- `CodexGateway`: optional `with_piped` attach (legacy callers keep
  supervision-only and `send` still refuses loudly). `open` supervises +
  stores pipes/profile/workspace; `send` runs initialize → thread/start
  (or resume) → turn/start with observed-model and CLI-build checks
  (mismatch = capability error/Rejected, never fallback). `observe`
  pumps the wire first; `drain_requests` correlates on the server thread
  id; `answer` shapes the wire decision strictly from the domain verdict
  (`approved`→accept, `declined:*`→cancel, questions need server ids) and
  writes it through the owning session; `cancel` drops the transport and
  kills by handle; `reconcile` reports transport state, outcome stays
  UnknownOutcome without completed-turn evidence. Usage arrives via
  `thread/tokenUsage/updated`, not `turn/completed` (feed the ledger via
  the audited user resolve path; no automatic fill).
- Maintained tests: `attached_transport_speaks_verified_protocol`
  (duplex open→send→observe→drain→answer-cancel-on-wire→completion) and
  `piped_spawn_echoes_and_kills_by_handle` (real `cat` echo + exactly-once
  kill). Full gates after wiring: fmt ok, clippy `-D warnings` ok,
  `cargo test --workspace`: **243 passed, 0 failed, 1 ignored**.
- Contracts: `contracts/codex/0.167.1/{manifest,protocol-subset}.json`
  (live-verified, generatedFromLiveCli:true); allowlist is now
  `["0.167.0-alpha.3.2", "0.167.1"]`.
- Wired daemon probe (release-like binary, truthful `codex.json`
  0.167.1, fresh state dir, removed after): gateway line now reads
  `codex gateway configured`; profile `gpt-6-luna`/low → capability
  `supported` ("model advertised by codex"); empty dispatch → 0 sends,
  zero daemon children, zero new codex PIDs, shutdown exit 0.
- Still open (NOT claimed): a live worker leg through the daemon —
  `dispatcher` passes only a workspace *label* (`workspace_label`, "no
  directory is created here"), so a live `thread/start` has no real
  fixture-owned cwd yet — SUPERSEDED below: per-attempt scratch
  provisioning landed in the dispatcher this session (honest scratch, not
  Git sealing), and leg 1 ran live on it. Approval request+cancel still
  unverified against the real server (gateway path is e2e-tested on duplex).

## Live acceptance leg 1: worker → sealed → verifier → Succeeded (2026-11-11 late)

State dir (kept): `/tmp/central-livework-hw4maaks` (run
`dae039d0-08c3-48e1-a4f2-6b238453497a`). Daemon = release-like binary with
the wired gateway, cwd `/tmp`, scan 500ms, truthful `codex.json`
(`0.167.1`, surveyed `gpt-6-luna`).

- Worker dispatch: scan loop queued + dispatched one implementation task
  (attempt `b5384114`, pinned profile `gpt-6-luna`/low) into provisioned
  scratch `attempt-scratch/<run>/<attempt>`. The model created `calc.py`
  (`sha256:2ce8d715…`, bool-rejecting add) and `test_calc.py`
  (`sha256:50bc95b7…`) and executed Python in the scratch
  (`__pycache__/calc.cpython-314.pyc` present). Owner re-ran the
  assertions directly: `add(1,2)==3` ok, `add(True,1)` raises TypeError.
  Zero approvals were requested; nothing ran outside the scratch.
- Usage for the daemon worker turn: **unknown** — the probe polled the
  wrong session source so no reconcile ever ran during the turn, and the
  post-restart gateway had no live session to read from. No numbers were
  invented; the ledger holds zero observed tokens (no limits were set, so
  no hold triggered — correct per design). Equivalent direct-CLI turns
  recorded usage earlier (worker in≈62984/out 187; verifier in 21709/out 45).
- Restart/recovery: daemon killed + rebooted on the same state dir →
  run parked `AwaitingResume`, recovery endpoint lists the explicit
  resume action plus the stale-session finding (`reconcile before
  resume`), zero sends before the explicit `resume` (revision 7,
  `Running`). No orphaned agent processes after either shutdown
  (verified via `ps`; only third-party processes remain).
- Result import (owner, after inspecting the real bytes): `POST
  .../attempts/.../results` → revision 8, sealed `candidate_hash
  2ce8d715…` (== file bytes), `criteria_hash e8e4157d…` (==
  server-computed task criteria), status `awaiting_verification`.
  `input_hash` = sha256 of the task objective text (owner-computed and
  stated; the seal binds it consistently for the verdict).
- Verifier (independent live thread `01a1ca90`, separate app-server
  session, read-only sandbox, ephemeral, `gpt-6-luna`/low, no edits):
  judged byte-identical copies (`sha256` match) → `CRITERION 1: PASS`,
  `CRITERION 2: PASS` with evidence lines; usage in 21709/out 45.
- Verdict via `SubmitVerification` (owner): subject + criteria hashes
  match the seal, evidence cites the sealed input hash, findings name the
  independent thread and the owner re-run, `verifier_thread` differs from
  the worker session (independence check passes), no same-session
  override, tracked source untouched. Revision 9 → task **Succeeded**,
  `repair_rounds_used: 0`.
- NOT done: Git integration through the daemon (prepare/promote have no
  daemon caller — port + contract tests only). Next build item, not a
  claim. Windows/packaging/reboot gates unchanged (no `pwsh` on host).

## Assessment

The two Linux gates that were red are now green on this host with the
containment fix and `cfg(windows)` gating; transport wiring and
dispatcher scratch provisioning are implemented with maintained tests
(full suite green). Proven live: worker → sealed → verifier →
Succeeded through the daemon/API (leg 1), plus the restart barrier.
MVP acceptance is still **not** complete: Git integration through the
daemon, usage ledgering of live turns, Windows/release gates and reboot
acceptance remain unverified, so no plan or the MVP may be marked
accepted on this evidence.

Machine-local raw logs from the prior run remain under `/tmp` as listed
in `2026-11-11-linux-rerun.md`; this rerun's full-suite output was
observed in-session (exit 0) and the live-smoke JSONL was observed
in-session (both turns exit 0).
