# In-flight tool crash and resume — 2026-11-09

## Configuration

Codex CLI 0.167.0-alpha.3.2, existing ChatGPT authentication, requested model
`gpt-6-luna`, reasoning effort `low`. The model is available in the local Codex
catalog. Official model description: https://developers.openai.com/api/docs/models/gpt-6-luna

Script: `crash-probe.mjs`. This is a disposable experiment, not product code.
Run with `node ./crash-probe.mjs` on Windows. It creates a unique local workspace
and evidence directory per run and performs up to two real model turns. It
deliberately terminates only its own Codex CLI process after observing an actual
tool checkpoint. A 120-second per-process timeout provides bounded cleanup.

The controlled helper writes START/checkpoint, waits 20 seconds, then writes
FINISH. Recovery has a distinct `finish` operation and checks whether the old
process is still alive; it does not rerun the original operation. This supplied
task-specific recovery logic is essential to interpreting the result.

## Successful experiment

Evidence: `results/crash-2026-11-09T02-39-01-410Z/report.json` and sibling JSONL
streams. Wall-clock duration about 49 seconds, including 22 seconds deliberately
waiting to reconcile the old process before resume.

1. The model actually invoked the shell; JSONL records `command_execution` with
   `status: in_progress`. The helper saved a checkpoint with `phase: started`.
2. The probe force-killed only the Codex CLI (`SIGKILL`), not its process tree.
3. The controlled child was no longer alive immediately afterward. The saved
   checkpoint remained readable and incomplete even after its normal deadline.
4. A new CLI process resumed the exact session ID.
5. The agent executed the helper's inspection command. It observed the incomplete
   checkpoint, no live old process, and only START in the action log.
6. The agent called the helper's recovery operation, which produced the final
   checkpoint. CLI exited 0; final answer reported `recovered: true`.
7. Local checks verified the original nonce, `phase: done`, one START and one
   FINISH. Actions were exactly `START`, `RECOVER`, `FINISH`.

## Usage and earlier failed setup

The successful resume event reports:

- input_tokens: 44,695
- cached_input_tokens: 40,192
- output_tokens: 391
- reasoning_output_tokens: 0

The interrupted invocation did not emit `turn.completed`, so its final usage is
unknown from this JSONL stream. These values are not a complete total or a dollar
cost. A smaller model/low effort does not guarantee fewer input tokens; agent
context, history and repeated tool turns still contribute.

An earlier setup attempt in `results/crash-2026-11-09T02-38-05-272Z/` did not
reach tool execution: the CLI rejected the command with `blocked by policy`.
That completed turn reported 30,436 input tokens (26,624 cached) and 213 output
tokens. It is additional usage, separate from the successful crash experiment.

Investigation found that `--ignore-user-config` omitted the user's Windows
sandbox selection. Explicitly restoring the same `windows.sandbox="elevated"`
while keeping workspace-write sandbox and approval policy unchanged allowed the
next experiment to execute the tool. No sandbox bypass was used. The successful
run also capped the skills catalog and disabled automatic skills instructions
for the narrow disposable task; these are context controls, not access controls.

## Meaning for Central

- In this configuration, resuming a session after abrupt CLI termination can
  preserve enough conversation context to perform explicit reconciliation and
  task-specific recovery.
- The filesystem checkpoint and external checks, not the agent's final claim,
  establish success for this probe.
- Session resume did not continue the dead OS process. It started new tool calls
  to inspect and complete saved work.
- Process behavior differs from the earlier OpenCode server probe: this Codex
  helper died with the CLI, while that OpenCode helper survived server death.
  Neither observation is a universal cancellation guarantee.
- Missing final usage during a crash is a budget/accounting uncertainty Central
  must represent explicitly.

## Limits

This is a single controlled local shell task. Checkpoint/recovery operations and
resume instructions were supplied by the experiment. It does not establish
automatic recovery of arbitrary code migrations, exactly-once external effects,
detached-process cancellation, reboot/power-loss durability, or monetary savings.
PID liveness is sufficient for this bounded helper experiment, not a production
process identity/ownership protocol. No real project code was sent or changed.
