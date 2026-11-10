# P3 workflow gate: reliable recovery (Task 6)

Deterministic gate, fake gateway + virtual clock, no live models. The gate
proves crash barriers converge without side effects and that the scheduler
actually runs inside the daemon.

## Barrier matrix

| Barrier (fixture) | Crash point | Boot result | External effects |
| --- | --- | --- | --- |
| `before_commit.json` | send issued, `AttemptRunning` receipt never committed | attempt + intent `Dispatching` -> `UnknownOutcome`, run `AwaitingResume` | `duplicate_external_actions == 0` |
| `after_send.json` | outbox send (cancel/answer) done, completion receipt missing | intent + attempt -> `UnknownOutcome`; resume never replays | `duplicate_external_actions == 0`, unknown never forced to `Failed` |
| `after_git_promote.json` | ref moved to candidate, promotion receipt missing | `recover_receipt` converges from the actual ref head | `git_commit_count` unchanged, ref never rewritten by recovery |

Fixtures kill only the daemon child they spawned, by PID, at the named
barrier (`kill_scope: own-child-only`, `live_models: false`).

## Tests

- `crates/central-server/tests/workflow_recovery.rs` (5 tests, all green):
  `send_succeeded_receipt_missing_never_resends`,
  `pending_approval_epoch_changed_stays_stale`,
  `candidate_published_without_metadata_is_orphan`,
  `promote_done_receipt_missing_converges`,
  `boot_requires_resume_even_when_evidence_complete`.
  Core assertions verbatim: `assert_eq!(duplicate_external_actions, 0)`,
  `assert_eq!(inference_calls_during_boot, 0)`,
  `assert_eq!(hold_after_resume, UsageUnknown)`,
  `assert_eq!(git_commit_count, expected_count)`.
- `scripts/test-workflow-recovery.ps1`: barrier matrix checks, the
  in-process suite, then a real daemon kill-restart (own child only):
  setup with `CENTRAL_SCAN_MS=0`, restart with the scan enabled, boot
  parks with zero sends, explicit resume, scan queues + dispatches both
  implementation tasks (`fake-sends >= 2`), passive reconcile stores
  evidence and resends nothing. Verifier/integration progression stays in
  the `quality_loop` / `git_integration` suites; the live Luna/low smoke
  is opt-in only (`CENTRAL_LIVE_SMOKE=1` fails closed: no live harness
  exists, so it stops instead of calling any model).

## P3 trace (input/plan/attempt/profile/verification per fixture output)

- Input: attempt inputs are stored at queue time (`attempt_inputs`) and
  never rewritten; reuse re-verdict re-renders from the same binding.
- Plan: active plan hash binds every attempt (`bound_plan`); revision
  proposals hold impacted queued descendants, running attempts keep
  their old binding.
- Attempt: every attempt carries task/profile binding plus (when
  dispatched) the harness session (`attempt_sessions`); unknown attempts
  hold descendants (`blocked_by_unknown`) and resources.
- Profile: frozen per attempt; resume of the same session requires
  binding + input + profile match (`can_resume_session`), else a new
  attempt with a handover after the old outcome is known.
- Verification: quality loop verdicts bind task/candidate/criteria;
  stale evidence never passes; the gate trace ends at the verified
  outputs, not at exit codes.

## Scan-loop disposition (ledger carry-over)

T1 delivered pure `should_tick`/`tick`; this task wires them into the
daemon: a background loop (`CENTRAL_SCAN_MS`, default 5000, `0`
disables) rescans every `Running` run plus an on-event wake after each
write endpoint. Parked runs are skipped and dispatch rechecks every
gate, so the loop can never send before an explicit resume. Without a
profile registry the resolver yields `None` and dispatch defers (queueing
still proceeds); under `--fake-agent` only, unpinned tasks use the
default low profile the fake harness advertises. `local_runtime` spawns
set `CENTRAL_SCAN_MS=0` so their exact event counts stay deterministic;
scan timing is covered here instead.

## Remaining external-effect limits

- Central has no exactly-once outbox: an ambiguous send is
  `UnknownOutcome`, never auto-retried; cancels are requests, terminal
  only on evidence.
- Same-session resume needs gateway support (`supports_resume`) plus a
  profile/input match; otherwise the new attempt waits for the old
  outcome, then continues with a handover.
- The fake gateway cannot prove real-harness session semantics; reboot
  and manual Windows acceptance are recorded separately from process
  kills, not inferred from them.
- Secret answers live memory-only: a restart drops every reference, and
  resumed runs wait for re-entry instead of replaying secrets.

## Evidence

- `cargo test -j 2 -p central-server --test workflow_recovery`: 5/5.
- `pwsh -File scripts/test-workflow-recovery.ps1`: PASS.
- `cargo test -j 2 --workspace`: exit 0.
- `cargo fmt --check`, `cargo clippy -j 2 --workspace --all-targets -- -D warnings`: clean.
