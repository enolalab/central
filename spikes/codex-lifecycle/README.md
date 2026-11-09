# Codex real-model spike

Disposable investigation for Central, not its Rust implementation.
The current MVP integration target is Codex, replacing OpenCode. Earlier
OpenCode experiments remain in the sibling directory as historical evidence.

## Run

Requirements: Windows, Node 20+, Codex CLI on PATH, usable Codex authentication.
Run `node ./live-probe.mjs` in this directory. Optional environment variable
`CENTRAL_CODEX_BINARY` selects a Codex executable.

This performs **two real model requests** against `gpt-6.1-sol` at medium effort,
using the existing authenticated Codex account. Account usage applies.

The CLI runs with read-only sandbox, skips the Git requirement, ignores user
config and rules, and disables multi-agent. The prompt asks it not to use any
tools. No tool items were observed; read-only does not itself disable all tools.
Auth remains in the user's Codex home: credentials are not copied into this
directory. Normal session history is persisted there so resume can be tested.
Workspace and answer/event files are local to the spike. No project source is
included in the prompt, although Codex adds its normal agent context.

The initial call returns schema-constrained JSON with a random nonce. After the
CLI exits, a new process resumes the exact session ID and must return the same
nonce from history, without receiving it in the second prompt. Local checks
compare parsed content, session IDs, exit status and absence of tool items.

The script has a 120-second deadline per invocation and kills only its own
process tree on timeout. This deadline is a safety bound, not a cancellation
or recovery experiment. No retries are performed by the script.

Outputs in `results/`: `report.json`, `schema.json`, answer JSON, JSONL event
streams and CLI stderr logs. Consult `FINDINGS.md` before drawing conclusions.

Official documentation used:
https://developers.openai.com/blog/eval-skills

Actual installed CLI help was also inspected for exec/resume syntax.

## Crash during tool execution

`node ./crash-probe.mjs` runs a separate Luna/low experiment with a controlled
local shell helper, force-kills the CLI after a checkpoint, and resumes the exact
session with explicit reconciliation instructions. Each run stores new evidence
under `results/crash-<timestamp>/`. This performs real model requests and account
usage applies. See `CRASH-FINDINGS.md` for observations and important limitations.
