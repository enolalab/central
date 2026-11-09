# P1 durable runtime — validation evidence (Task 4: daemon/API/CLI)

Date: 2026-11-09. Toolchain: Rust 1.99.1 GNU portable
(`.toolchain/env.ps1`). No live-model calls were made.

## What shipped

- `central-server` daemon: single-owner file lock (`daemon.lock`,
  fs2) acquired **before** DuckDB opens; loopback HTTP API
  (`POST /v1/commands`, `GET /v1/runs`, `GET /v1/runs/{id}`,
  `POST /v1/runs/{id}/dispatch`, `POST /v1/shutdown`, `/healthz`);
  owner token in `daemon.token`, discovery in `server.json`; command
  bodies capped at 1 MiB; every non-API path returns a clear 503
  until the P4 UI exists (the daemon stays healthy).
- `central` CLI: `daemon start|status|shutdown`, `init`, `start`,
  `status`, `plan import`, `plan approve`, `pause`, `stop`, `resume`.
  The CLI never opens the database; daemon start spawns the server
  detached/hidden with an argv array (no shell string), so closing
  the CLI cannot kill the daemon.
- `Runtime::recover` / `Runtime::dispatch_once` in
  `central-application`, backed by guarded DuckDB outbox transitions
  in `central-infrastructure`.

## Decisions recorded

- **Target-run routing:** `CommandEnvelope` carries no run id (by
  contract), so non-create commands address their run with
  `POST /v1/commands?run=<uuid>`. `CreateRun` takes no target; the
  daemon allocates the run id and returns it in the receipt.
- **Actor identity:** the `Actor` is derived from the bearer token
  (persisted owner in `owner.json`). An `actor` field in the request
  body is ignored for identity — see
  `auth_cannot_be_claimed_in_body`.
- **Exit codes:** daemon/CLI report an already-owned state directory
  with exit code 3 (`AlreadyRunning`); storage and other startup
  failures exit 2 and never serve success.
- **Dispatch order:** `pending()` returns intents ordered by UUID;
  dispatch treats each intent independently (guarded claim, evidence
  or unknown), so results do not depend on that order. Covered by
  `dispatch_is_order_insensitive_across_pending_intents`.
- **Fake agent:** `tests/fixtures/fake_agent.rs` is compiled only
  under the `test-support` feature. Release builds reject
  `--fake-agent` (exit 2), report `fake_agent:false` on `/healthz`,
  and 404 on `/v1/test/fake-sends`; their gateway refuses every send
  instead of fabricating output.

## Task 3 carry-overs — dispositions

1. Guarded outbox transitions: dispatch/recovery only use
   `Store::transition_outbox` (Pending→Dispatching→Completed on
   evidence, →UnknownOutcome on ambiguity). The raw
   `DuckDbStore::mark_outbox_status` remains for store-level tests
   only and is not called from dispatch code; direct
   Pending→Completed transitions are rejected, and `validate_batch`
   rejects non-Pending intent inserts.
2. `inject_commit_failure` is behind
   `central-infrastructure/test-support`; the daemon cannot reach it.
3. Dispatch is order-insensitive w.r.t. `pending()` ordering (above).
4. Target-run routing via `?run=` (above).

## Evidence

- `cargo test --workspace`: all suites green — domain
  `plan_lifecycle` 11, application `command_runtime` 5,
  application `recovery_dispatch` 5, infrastructure
  `store_contract` 10, server `local_runtime` 5.
- `cargo clippy --workspace --all-targets -- -D warnings`: clean.
  `cargo fmt --check` on the five P1 crates: clean (the pre-existing
  spike files are unformatted at HEAD and were left untouched).
- `pwsh -File scripts/test-runtime-recovery.ps1`: PASS — create →
  approve → hard-kill crash → reopen (both runs `AwaitingResume`,
  zero fake sends/cancels, gated dispatch sends nothing, no duplicate
  events) → explicit `resume` → pause/resume append exactly 3 events
  → independent run untouched → UI 503 with healthy daemon →
  shutdown.
- Restart-barrier specifics: `Dispatching` attempts and intents found
  at startup become `UnknownOutcome` and are never resent
  (`recovery_marks_dispatching_unknown_and_requires_resume`,
  `ambiguous_dispatch_becomes_unknown_and_is_not_resent`).

## Limits

- This completes P1 only. It is not MVP completion and there is no
  real model/harness integration: P1's only domain effect is
  `CancelAttempt`, and no P1 command creates attempts yet (the
  scheduler arrives in later plans), so end-to-end fake *send*
  evidence comes from the application-level dispatch tests, while
  the daemon gate proves zero-dispatch-before-resume and workflow
  continuation over HTTP.
- Token storage is a plaintext file inside the state directory
  (owner-only by directory placement); OS ACL hardening is future
  work. The daemon inherits the CLI's inheritable handles at spawn;
  callers must not wait on a captured CLI pipe (the recovery script
  documents the `Start-Process` + `WaitForExit()` pattern).
