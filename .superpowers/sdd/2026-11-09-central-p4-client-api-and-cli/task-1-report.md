# P4 Task 1 report — Public Client API + CLI (finish)

Date: 2026-11-11. Baseline: `0e22a66` (no P4 commit). Toolchain: pinned
`.toolchain/env.ps1` (Rust 1.99.1 GNU portable); workspace cargo commands
run with `-j 2`. Worker: finisher, no subagents spawned, no reviewer.

Note: a prior worker left an unfinished draft of this report (truncated
mid-step-13 with non-UTF8 bytes). This file completes and replaces that
draft; every claim below was re-verified in this session.

## Starting state (inherited, verified not assumed)

The tree contained broad uncommitted work: domain serde-defaults,
application use cases (`client_access`, `conversation`, `maintenance`,
`user_sync`, `ports/sync`), store ports + codec/duckdb/writer + migration
`002_public_api.sql`, server `api/` DTO layer + `http/` modules, CLI
`client/commands/main` changes, `contracts/openapi-v1.yaml+json` + events
schema + examples, 6 new server test files (+ `common/`), CLI parity
suite, `README.md`, `examples/`, `scripts/`, `docs/` additions. I audited
it (build + every new suite + full workspace) instead of redoing it.

My session made zero code edits: the tree was already green, including
the `local_runtime::DispatchFailStore` forwarding fix
(`snapshot_with_cursor` + `events_after` delegation at
`crates/central-server/tests/local_runtime.rs:622-640`) and the clippy
fixes (`is_multiple_of`, `sort_by_key`, async `load_run`,
`tick_all` allow). No RED state remained, so no new RED/GREEN cycle was
owed (nothing was "not yet covered").

## Gates run IN this session (all green, in-commit)

- `cargo fmt --check` → clean, exit 0.
- `cargo clippy --workspace --all-targets -j 2 -- -D warnings` → exit 0,
  no warnings.
- `cargo test -p central-server --test api_contract --test api_auth
  --test api_events` → 8+8+6 pass, 0 fail.
- `cargo test -p central-server --test api_workflow
  --test artifact_retention --test release_security` → 6+3+3 pass.
- `cargo test -p central-cli --test api_parity` → 3 pass.
- `cargo test --workspace -j 2` → exit 0, 0 failed (incl.
  `local_runtime` 9/9, `workflow_recovery` 7/7, all P1-P3 suites;
  1 ignored: opt-in live CLI smoke needing installed Codex CLI + auth).
- `cargo build --release -j 2` → exit 0.
- `pwsh -File scripts/test-mvp-e2e.ps1` → `E2E PASS`, exit 0. Observed:
  stale approval refused 409; 2 workers dispatched; boot zero inference,
  run parked; old cursor refused with `resync_required`; worker→verifier
  provenance for A and B; integration verified through the daemon.
- `pwsh -File scripts/build-release.ps1` → exit 0:
  `dist/central-windows-x64.zip` containing `central.exe`,
  `central-server.exe`, contracts (yaml+json+schema+examples), 4 docs,
  2 examples, THIRD-PARTY-NOTICES; script gate confirms no UI/Node
  payloads. The zip itself is a build artifact and is NOT committed.

## Per-step evidence (brief steps 1-18)

- Steps 1/2 (contract + RED): inherited. RED was not re-observed
  (reverting the tree to reproduce it would be destructive); all four
  suites named in step 2 are GREEN in-session (counts above).
- Steps 3/4 (auth): `api_auth` 8/8 — owner issue/list, no query token,
  read/control/bridge separation, revoke incl. open streams,
  agent-cannot-approve, untrusted origin 403, no secret in errors/logs.
- Steps 5/6 (read models/events): `api_contract` 8/8 + `api_events` 6/6
  — identical receipt replay, changed-payload 409, stale-target 409,
  gap-free snapshot cursor, restart resnapshot, ordered replay at commit
  edge, `last_seq` across restart, resync for slow subscribers, 400 on
  bad cursor/limit, unknown-kind tolerance.
- Step 7 (command boundary): `api_contract` (receipt/idempotency/
  revision) + `api_workflow` (chat/profile/sync/usage/import through
  application; public commands grant no system rights; result import
  parks without passing).
- Step 8 (real workflow wiring): profile registry wired into the daemon
  — `refresh_profiles` runs on every `tick_all` pass and the resolver
  serves pinned tasks from the owner registry (`scan.rs:118-193`);
  unpinned waits rather than substituting; `bootstrap.rs:548-562`
  spawns the scan loop with the real gateway selector (Codex gateway
  when owner `codex.json` exists, deferring `UnavailableGateway`
  otherwise). Queues-never-dispatches ends here; e2e proves dispatch →
  seal → verify → integrate on the daemon surface.
- Step 9 (CLI): `commands.rs` covers the operator surface; `api_parity`
  3/3; CLI is API-only — zero matches for duckdb/rusqlite/file-write/
  queue writes in `crates/central-cli/src`.
- Step 10 (parity/workflow): `api_parity` + `api_workflow` green;
  fake-harness scope stated in `docs/validation/p4-api.md`.
- Steps 11/12 (docs/examples/independent client): `examples/client.ps1`
  is HTTP-only (owner token read from state-dir file into a Bearer
  header, never URL/logged); `examples/browser-client.mjs` uses fetch +
  Authorization header + manual stream reader (no EventSource-with-token;
  token memory-only, `URLSearchParams` carries only `after`/`known_epoch`).
  Step-12 run evidence is in `docs/validation/p4-api.md`; this session's
  e2e re-proved the same surface via CLI + raw HTTP.
- Step 13 (lifecycle/security): `artifact_retention` 3/3 (escape +
  redaction, dry-run spares live/unknown work, export has no
  credentials) + `release_security` 3/3 (minimal healthz, owner-only
  shutdown, no UI/fake runtime) + `local_runtime` 9/9 (contract + 404
  outside API).
- Step 14 (packaging): release script exit 0, zip contents listed above;
  no UI, static assets, or Node runtime.
- Step 15 (e2e gate): PASS in-session, exit 0 (output quoted above).
- Step 16 (bounded live check): UNVERIFIED opt-in — `codex` is not on
  PATH in this sandbox and personal config was not touched; no model
  fallback engaged, no live claim made. Recorded as unverified in
  `docs/validation/p4-api.md` ("Step 16 - live model (opt-in,
  UNVERIFIED)").
- Step 17 (final checks): fmt + clippy + workspace tests + release build
  + e2e + release packaging all green in-session (this commit).
- Step 18 (commit): `feat: expose documented client API and complete CLI
  workflow`; stages task files only — `docs/` (user's own edits) and
  `dist/` (build artifact) excluded.

## Carry-over dispositions (ledger rulings)

1. Serde replay-compat defaults: PRESENT — `Event::VerificationDisputed`
   carries `#[serde(default)]` (`event.rs:200`, via
   `default_disputed_decision`); release ships only with them.
2. Profile registry into daemon: WIRED (step 8 evidence above);
   scan-queues-but-never-dispatches is closed.
3. Durable worker-done command: PRESENT — `Command::RecordWorkerResult`
   (`command.rs:249-257`); worker completion has a durable path.
4. Credentials: CLEAN — examples use `<owner-token>`/`<client_id>.<secret>`
   placeholders only; issuance secret returned once; hash-only storage;
   `api_auth` proves no secret in errors/logs; browser token memory-only.
5. No UI/static/Node in release: CONFIRMED — zip entry list + script gate
   + `release_security` suite; `/` and `/ui` return 404 JSON.

## Commit note

`.superpowers/` is git-ignored by repo convention; this report is
force-added (`git add -f`) as the task's evidence deliverable while
`docs/` user edits and the `dist/` zip stay out of the commit.

## Fix report — review round 1 (2026-11-11, HEAD `1229858`)

Finding (Important, controller decision): `decode_cursor`
(`crates/central-server/src/api/dto.rs`) returned undocumented code
`invalid_cursor` on 400s, outside the documented stable set (OpenAPI
`ApiErrorV1.code` enum + `docs/api.md`: `validation_failed`,
`auth_required`, `forbidden`, `not_found`, `stale_revision`,
`stale_target`, `command_conflict`, `conflict`, `resync_required`,
`capability_unavailable`, `transport_error`, `internal_error`).
Status was already the correct 400. Fix: return the documented
`validation_failed` here (stable set kept minimal, no new code).

Test-first: updated `invalid_cursor_is_400_not_500`
(`crates/central-server/tests/api_events.rs`) to assert
`code == "validation_failed"` BEFORE the fix — ran it, failed for the
right reason (`left: Some("invalid_cursor"), right:
Some("validation_failed")`). Then replaced all 4 `invalid_cursor`
producers in `decode_cursor` with `validation_failed`; test passes.

Sweep for other undocumented producers: grepped every string literal
passed to `error_response` in `crates/central-server/src` and compared
against the OpenAPI enum + `docs/api.md` list. Every other code in use
(`validation_failed`, `auth_required`, `forbidden`, `not_found`,
`internal_error`, `resync_required`, `conflict` via `AppError::code`)
is documented. `invalid_cursor` was the sole outlier; no other fix owed.

Gates re-run (all exit 0): `cargo fmt --check` clean;
`cargo clippy --workspace --all-targets -j 2 -- -D warnings` clean;
`cargo test -p central-server --test api_contract --test api_events`
(8+6 pass); `cargo test --workspace -j 2` full green (incl. the
updated cursor test). Out of scope items untouched: conflict-code
sniffing, profile fallback ids, snapshot transaction comment,
build-release filename gate, `docs/superpowers/**`.

Commit: `fix: use documented validation code for bad cursors` staging
only `dto.rs`, `api_events.rs`, and this report (force-added).
