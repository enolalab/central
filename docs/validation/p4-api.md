# P4 validation — public client API + CLI (Task 1)

Date: 2026-11-10. Baseline `0e22a66` (P1–P3 green). Toolchain: Rust
1.99.1 GNU portable (`.toolchain/env.ps1`), workspace cargo `-j 2`.

## Deterministic gates (all green in-commit)

- `cargo test --workspace` — exit 0, including the six new P4 files
  (`api_contract` 8, `api_auth` 8, `api_events` 6, `api_workflow` 6,
  `artifact_retention` 3, `release_security` 3), the CLI parity suite
  (`api_parity` 3), and the updated P1–P3 suites (`local_runtime`,
  `workflow_recovery`, unit tests).
- `cargo fmt --check` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- Contract tests validate the served `/v1/openapi.json` (paths) and
  live response bodies, error codes and streams — not file existence.

## Step 12 — independent client proof (this machine)

Fixture: clean temp state dir, daemon built with test-support,
`--fake-agent`, scan at default cadence, `origins.json` =
`{"origins": ["https://app.example"]}`.

`examples/client.ps1` (HTTP only, owner token from the state dir,
no crate imports, no DB) completed exit 0:

- `GET /v1/meta` → `api=v1`, instance epoch recorded.
- `CreateRun` → run id, revision 1.
- Snapshot → `control=Running`, `last_seq=1`.
- `POST chat` → `seq=1`.
- `ProposePlan` + `ApprovePlan` with the exact `pending_plan` hash →
  revision 3, plan active.
- `GET inbox` → 0 entries; `GET plans` → 1 plan, active hash matches.
- `GET events?after=1` → 2 events with an opaque `next_cursor`;
  resume from that cursor → 0 items, null cursor (exhausted).
- Reconnect with `known_epoch` → 2 events (same boot).
- `GET usage` → `unknown=False`; `GET recovery` → 0 actions.

Browser-origin checks (PowerShell HTTP with `Origin` headers; no real
browser engine exists in this sandbox, so engine-specific behavior is
not claimed):

- `Origin: https://evil.example` on read and stream → 403.
- `Origin: https://app.example` → 200.
- No `Origin` (bearer CLI-style) → 200.
- Preflight `OPTIONS` for the allowlisted origin → 204 with the
  echoed origin, never `*`.

## Step 15 — e2e gate (`scripts/test-mvp-e2e.ps1`, exit 0)

Fixture git repo (2 commits), scan loop enabled (500 ms), fake
harness, CLI + raw HTTP client, kill/restart barrier mid-run:

- Plan (2 implementation + 1 integration task) proposed and approved
  by exact hash; a stale-approval retry refused with 409.
- 2 worker attempts dispatched (exactly 2 external sends, no more).
- Daemon killed and restarted: new instance epoch, run parked
  `AwaitingResume`, **zero** sends at boot.
- Old cursor + old epoch refused with `resync_required` (409);
  snapshot cursor survived; explicit `resume` reopened the gate.
- Worker results imported via HTTP sealed and parked
  (`AwaitingVerification`), never passed; independent Pass verdicts
  (exact criteria hash, separate verifier thread, evidence cited)
  moved both tasks to `Succeeded` with candidate-hash provenance.
- Integration attempt dispatched post-dependency, imported and
  verified through the same surface.
- Post-restart sends: exactly 1 (integration only) — A/B never
  resent after resume.
- Cursor-from-final-snapshot page empty: no commit gap.

Fake-harness scope (stated, not hidden): sends go to the scripted
fake agent, not a model; model quality is not what this gate proves.
The daemon-side contract — receipts, parking, resnapshot, provenance,
no duplicate sends — is what passed.

## Step 16 — live model (opt-in, UNVERIFIED)

Not run. `codex` is not on `PATH` in this sandbox, and the personal
`~/.codex` config was not touched (read-only directory listing only).
No model fallback was engaged and no live claim is made. To verify:
on a host with Codex CLI + auth, write an owner `codex.json`
(`binary`, allowlisted `cli_version`, `surveyed_models`) into a
fixture-owned state dir, run one `gpt-6-luna/low` pass of worker →
verifier → integration through the daemon, record turns/usage, and
stop at the first quota/auth error. A manual reboot case must be
recorded separately, never inferred from a process kill.

## Release smoke (this session, 2026-11-11)

`pwsh -File scripts/build-release.ps1` exit 0 (server 0.1.0, Rust
1.99.1). Zip contents verified by listing every entry:
`central.exe` (113,444,525 B), `central-server.exe` (114,956,517 B),
`contracts/` (openapi-v1.yaml, openapi-v1.json,
events-v1.schema.json, examples/api-v1.json), `docs/` (api,
client-integration, getting-started, recovery), `examples/`
(client.ps1, browser-client.mjs), `THIRD-PARTY-NOTICES.txt`. No
`.js/.html/.map` outside `browser-client.mjs`, no UI bundle, no Node
runtime — the script's own payload gate passed.

## Capability and accounting limits (honest list)

- Gateway without `codex.json` (or with an invalid one): dispatch
  defers with `capability_unavailable`; profiles report `unknown`.
- Profiles resolve dispatch only for pinned tasks whose stored
  profile matches; unpinned tasks dispatch on the fake profile in
  test builds and wait in release builds — never a substitution.
- `POST /v1/maintenance/gc` implements dry-run only; real deletion
  needs an explicit follow-up change, not a flag flip.
- Usage money figures are user-configured estimates only; Central
  never derives cost from tokens. Unknown usage holds its run until
  evidenced or audited-resolved.
- `central maintenance export` is a redacted report, not a database
  backup. Offline backup = stopped daemon + owner lock free + DB +
  WAL + artifact store copied together.

## Release smoke (step 14/17)

`scripts/build-release.ps1` produced `dist/central-windows-x64.zip`
with `central.exe`, `central-server.exe`, `contracts/`,
`docs/`, `examples/` and `THIRD-PARTY-NOTICES.txt`; no UI, no Node
payloads (only the documented `browser-client.mjs` sketch). The zip
was unpacked to a clean dir and `central meta` + `central status`
ran against a fresh daemon with no Rust/Node on `PATH`.
