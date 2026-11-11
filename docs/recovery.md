# Recovery and offline backup

## Restart behavior (the contract)

1. One daemon owns one state directory (owner lock first); a second
   starter exits `3` without touching anything.
2. Boot runs passive recovery: nonterminal runs park in
   `AwaitingResume`, dispatching attempts without receipts become
   `UnknownOutcome`, and **zero** external sends happen.
3. `GET /v1/runs/{id}/recovery` (or `central recovery --run <id>`)
   lists passive findings plus the required user actions per run.
4. Only an explicit `resume` reopens dispatch — and only after
   reconciliation. `resume` never clears budget/usage holds.
5. Snapshots bind to the boot's `instance_epoch`. Cursors from before
   the restart are refused with `resync_required`: clients refetch
   the snapshot and reconnect from its cursor. Reconnecting never
   replays inbox answers and never auto-resumes.

## What survives a restart

- Run snapshots, the durable event log with per-run seqs
  (`last_seq` included), outbox intents and command receipts.
- Usage holds: a missing final still holds its run after resume
  until evidenced or explicitly resolved (audited).
- Client credentials (hashes) and revocations; profiles; chat history.
- Artifact bytes with their committed hashes.

## What does not survive

- The memory-only secret vault: secret answers given before the
  crash must be re-entered; the daemon waits instead of replaying.
- Agent sessions: continuation depends on the adapter. When the old
  session is unsafe, the run continues through a new attempt with a
  recorded handover — never by pretending the old session resumed.

## Offline backup (the only supported backup)

1. `central daemon shutdown` (owner only) and confirm the owner
   lock is free.
2. Copy **together**: the DuckDB file, its WAL sidecars, and the
   `artifacts/` store. One file alone is not a backup.
3. Restore by placing all three back and starting the daemon; it
   recovers passively and waits for `resume` as usual.

`POST /v1/maintenance/export` (`central maintenance export`) writes
a redacted human report — ids, scopes and notes, never credentials.
It is **not** a database backup; do not present or consume it as one.

## Replay compatibility

`VerificationDisputed.decision` and `DeferRetry.max_retries` decode
old rows with the defaults their writers meant (`Disputed`, `3` —
the pre-P3 hardcoded bound). `StaleCandidateRecorded` and
`ReuseAssessed` shipped first in the unreleased P3 with their full
field sets, so no released log predates them; any field added after
the v1 release must carry `#[serde(default)]` or a documented
migration before shipping.
