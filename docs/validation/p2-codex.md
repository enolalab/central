# P2 Codex validation (Task 5)

Evidence for one Codex task verified with immutable provenance. Nothing here
rests on agent word alone: every claim cites a hash, an exit code, or a test
count reproducible from this repo.

## What was proven (fake mode, no model)

Gate: `pwsh -File scripts/test-codex-single-task.ps1` → **GATE PASS**.

- Fixture `tests/fixtures/tiny-repo` (one function `add`, one test
  `test_add`) materialized into fixture-owned temp scratch only. The real
  repo was never sent anywhere and no production worktree was touched.
- Worker edit (scripted): input `sha256:4a48a969…591848891` →
  candidate `sha256:0acb989f…8142b169168b4` (hashes differ, edit proven).
- Independent verifier (separate read of the sealed candidate):
  `verifier uses the sealed candidate`, `contract add(1,2)==3 preserved`,
  `one fixture test intact` — all `ok`.
- Approved check `cmd.exe /C exit 0` → exit `0`. Recorded as evidence,
  not as a verdict.
- Restart barrier: PID snapshot of `codex*` processes before vs after —
  zero new agent processes, `agentInference: 0`, `turns: 0`.
  `zero agent inference before resume` → `ok`.

## Deterministic contract suites (fakes, no model)

`cargo test -p central-application --test verification` → **5 passed**:

- `exit_zero_is_not_task_success`: worker-done parks in
  `AwaitingVerification`, never `Succeeded`; an exit-zero `CheckEvidence`
  changes nothing.
- `verifier_uses_sealed_candidate`: unsealed subject refused, path escape
  refused, bound pass accepted with `candidate_hash == sealed`,
  `after_pass == Succeeded`.
- `mismatched_input_requires_reverify`: wrong input/criteria verdicts fail
  with a `reverify` conflict; status stays parked.
- `fail_consumes_repair_rounds_then_escalates`: two fails park for retry,
  third escalates to `Failed`; `Blocked` consumes no round.
- `evidence_for_returns_attempt_scoped_evidence`: T4-flagged `evidence_for`
  now covered — only the attempt's own observations and artifact hashes.

`cargo test -p central-infrastructure --test artifact_contract` →
**4 passed**:

- `artifact_root_and_hash_checked`: escaping/absolute paths refused, wrong
  expected hash aborts with no blob left, tampered blob fails re-hash on read.
- `orphan_is_not_committed_artifact`: crashed blob is not readable as
  committed; sweep removes exactly it; referenced blobs are never deleted.
- `unapproved_program_is_forbidden` / `approved_check_captures_evidence` /
  `mismatched_plan_hash_is_rejected`: only the exact approved program+argv
  runs, and only for the plan the runner was bound to (a request naming
  another plan is refused even with identical argv); cwd escapes refused;
  stdout hash equals `sha256("hi\r\n")`.

Full `cargo test --workspace -j 2`: every suite `ok`, 0 failures.
`cargo clippy --workspace --all-targets`: no warnings.
`cargo fmt --check`: clean.

## Live smoke: manual host steps (the gate script never does this)

`scripts/test-codex-single-task.ps1` is fake-only: it drives no live model
turn, enforces no turn cap, and performs no quota/auth stop. Passing
`-RealModel` is rejected with guidance pointing here — it never falls back
to a silent fake pass. The live proof below is a manual host procedure, to
be run where a `codex` CLI resolves with auth:

1. Turn budget: at most 3 agent turns total — one worker edit turn on the
   materialized `tests/fixtures/tiny-repo` scratch copy (one function, one
   test) plus the independent verifier pass.
2. Stop rule: stop immediately on quota/auth errors; never retry with
   different credentials, and never send the real repo — fixture-owned temp
   scratch only.
3. Usage capture: record model/effort/turns/tokens, or `unknown` with the
   hold reason (e.g. no CLI, no auth, quota stop), and append the observed
   values here.

Status: live smoke **unverified** — no `codex` CLI on `PATH` in this
environment, so no live turn was attempted, no quota was consumed, and no
personal auth/config was touched. The deterministic evidence above does not
stand in for it.
