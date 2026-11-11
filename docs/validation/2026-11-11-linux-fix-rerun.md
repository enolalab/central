# Linux fix rerun — 2026-11-11 (containment + Clippy, live Codex smoke)

Base revision: `a00a2b5fda46887fdd9dda8fbe2b9b32868d2cdf`.
Working tree: 3 files modified, uncommitted (see git status below); no commits made.
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
  through the daemon): **BLOCKED + UNVERIFIED** — the probe above proves
  the exact cause (installed `0.167.1` outside the pinned allowlist;
  daemon reports unknown capability, dispatches nothing, spawns nothing).
  Only the direct-CLI 2-turn smoke is proven. No live/MVP gate is claimed.
- Plan files (`docs/superpowers/plans/*.md`) left untouched: their
  `- [ ]` boxes predate this session and acceptance status lives in
  `docs/validation/`; no box was ticked to force completion.

## Assessment

The two Linux gates that were red are now green on this host with the
containment fix and `cfg(windows)` gating, and a bounded 2-turn live
Codex smoke (worker edit + independent verifier pass) is proven with
usage recorded. MVP acceptance is still **not** complete: the live
daemon workflow, Windows/release gates and reboot acceptance remain
unverified, so no plan or the MVP may be marked accepted on this evidence.

Machine-local raw logs from the prior run remain under `/tmp` as listed
in `2026-11-11-linux-rerun.md`; this rerun's full-suite output was
observed in-session (exit 0) and the live-smoke JSONL was observed
in-session (both turns exit 0).
