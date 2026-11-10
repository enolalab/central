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

- `crates/central-server/tests/workflow_recovery.rs` (7 tests, all green):
  `send_succeeded_receipt_missing_never_resends`,
  `pending_approval_epoch_changed_stays_stale`,
  `candidate_published_without_metadata_is_orphan`,
  `promote_done_receipt_missing_converges`,
  `boot_requires_resume_even_when_evidence_complete`,
  `reconcile_terminal_verdict_persists_old_outcome` (a reconciled
  `Completed` verdict persists `Pass` + detail in the observation and
  answers the handover precondition via `reconciled_outcome`; an
  `UnknownOutcome` verdict stays unforced; the attempt status is never
  rewritten),
  `scan_tick_all_leaves_parked_run_untouched` (one real `tick_all`
  pass dispatches the running run and leaves the parked run's
  control/attempts/holds/observations/revision identical).
  Core assertions verbatim: `assert_eq!(duplicate_external_actions, 0)`,
  `assert_eq!(inference_calls_during_boot, 0)`,
  `assert_eq!(hold_after_resume, UsageUnknown)`,
  `assert_eq!(git_commit_count, expected_count)`.
- `scripts/test-workflow-recovery.ps1`: barrier matrix checks, the
  in-process suite, then real daemon kill-restarts (own child only)
  against a 4-task plan (implementation x2 -> verifier -> integration):
  setup with `CENTRAL_SCAN_MS=0` (plus a second run parked and never
  resumed), first crash, restart with the scan enabled, boot parks with
  zero sends, explicit resume, scan queues + dispatches both
  implementation tasks (`fake-sends >= 2`, exactly one attempt each)
  while verifier/integration stay held with zero attempts, second crash
  with those stages pending, restart (fresh process, fresh counter:
  zero sends while parked), resume, counter stays 0 and downstream
  still held, passive reconcile stores evidence and resends nothing,
  and the unrelated run is still `AwaitingResume` with zero attempts.
  Startup/readiness handling (fix round 1): every `daemon start` is
  preceded by a pre-flight that stops ONLY the pid recorded in that
  state dir's `server.json` when it is alive but answers nothing on
  `healthz` (a zombie holding the owner lock); a healthy daemon is
  never touched. The CLI itself never abandons a half-started daemon:
  on readiness-timeout it re-checks once, then kills its own child
  before failing, so a never-healthy process cannot linger lock-held
  with no listener. Dispatch readiness polls too: the sends counter
  increments before the attempt's `Running` commit lands, so the gate
  refreshes the view until both attempts converge to `Running`
  (deadline, then honest failure) instead of asserting on the view
  read next to `sends >= 2` — that stale view (one `Running`, one
  `Dispatching`) failed a prior run.
  The live Luna/low smoke is opt-in only (`CENTRAL_LIVE_SMOKE=1`
  fails closed: no live harness exists, so it stops instead of calling
  any model).

## Explicit deferral: verifier/integration progression

The gate above proves the daemon dispatches what is ready and holds
the rest across two crashes — it does not drive verifier/integration
*progression*. The fake harness never completes attempts (no worker
results or verdicts flow through the daemon CLI/HTTP surface, and both
`result`/`handover` imports are validate-only), so no verdict can ever
satisfy the downstream dependencies in this script. Full stage
progression — verdicts routing tasks to `Succeeded` and opening the
verifier/integration gates — stays covered in-process by the
`quality_loop` / `git_integration` suites. Driving those stages through
a live daemon needs a harness that completes work (verdicts over the
command surface); that is P4 work, not claimed here.

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

- `cargo test -j 2 -p central-server --test workflow_recovery`: 7/7.
- `pwsh -File scripts/test-workflow-recovery.ps1`: PASS.
- `cargo test -j 2 --workspace`: exit 0.
- `cargo fmt --check`, `cargo clippy -j 2 --workspace --all-targets -- -D warnings`: clean.
