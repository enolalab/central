# Codex findings — 2026-11-09 (Asia/Bangkok)

## Tested configuration

- Codex CLI: `0.167.0-alpha.3.2` (existing desktop-bundled executable).
- Authentication: existing ChatGPT login, confirmed with `codex login status`.
- Requested model: `gpt-6.1-sol`; requested reasoning effort: `medium`.
- Read-only sandbox; disposable workspace; no project source in prompt.
- Separate CLI process for each of the two requests. No mock provider.

## Observations

| Check | Result |
| --- | --- |
| Real inference | CLI exited 0 and returned schema-constrained JSON with the correct random nonce and sum 42. About 13.9 seconds. |
| Structured events | Both calls emitted thread.started, turn.started, item.completed and turn.completed as JSONL. |
| Resume | New CLI process resumed the exact same session ID and returned the original nonce without it being included in the second prompt. About 6.5 seconds. |
| Tools | No tool items were observed in either JSONL stream. |
| Usage | Both turn.completed events contained token usage. A monetary cost was not provided by these events. |

Actual identifiers, answers, timings and usage are in `results/report.json`.
Do not sum usage figures or infer billing without establishing their semantics.
Even a short prompt can have substantial input usage from agent context and
conversation history; prompt length is not a reliable budget estimate.

## Implications for Central (proposed)

- An initial adapter can investigate exec JSONL, schema-constrained outputs,
  durable session IDs and explicit-ID resume, using child processes from Rust.
- Persist both Central attempt IDs and Codex session IDs. A session existing is
  not proof a task passed verification.
- CLI exit status, final output and turn events provide complementary evidence;
  task-specific acceptance checks remain necessary.
- Use real token telemetry where available. Monetary budgets require additional
  pricing/usage semantics and must not be represented as a guaranteed hard cap.
- This CLI is an alpha version. Pin/record version and test compatibility.
- Evaluate app-server separately if interactive approvals/questions, per-session
  control or richer orchestration cannot be served cleanly by exec.

## Not established

This is continuation after a completed turn, not resumption of an interrupted
turn. It does not establish crash recovery during tool execution, cancellation
of process trees, pending approvals/questions, parallel worker isolation, skill
or subagent control, verifier/integration workflows, machine reboot durability
or replacing one agent's context with another. No production adapter was built.
