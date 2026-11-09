# Codex app-server approval/question probe

Disposable investigation of Codex's protocol, **not Central**. No Central daemon,
scheduler, persistence implementation or production adapter exists here.

## Reproduce

Windows, Node 20+, PowerShell, tar, Codex CLI and existing usable Codex auth.
Tested CLI: `0.167.0-alpha.3.2`. Run `./setup.ps1` once to fetch checksum-pinned
`ws@8.22.2` and generate protocol schemas from the installed CLI. Then run
`node ./probe.mjs` in this directory. `CENTRAL_CODEX_BINARY` can select the runtime
executable; schema generation uses `codex` on PATH, so keep versions aligned.

The probe makes two real agent turns with **gpt-6-luna / low**, each involving a
tool and final answer. Existing ChatGPT account usage applies; there is no
automatic model fallback. Each run creates unique workspace and results paths.
Source from the real project is not included in prompts. Normal Codex agent
context is still included. No credentials are copied into this directory.

The probe starts its own app-server on a random loopback port, requires an
in-memory capability token (only its SHA-256 verifier is passed to the server),
and cleans up the server tree at the end. WebSocket is experimental in current
documentation. Do not point the probe at the user's running desktop server.

### Command approval case

- Create a workspace-write thread with `untrusted` approval policy and user
  reviewer. Ask the model to run a harmless helper that writes one local marker.
- Capture `item/commandExecution/requestApproval`; confirm no marker exists.
- Read waiting state, close the WebSocket, wait 500 ms, initialize a new client,
  and `thread/resume` the same ID. Observe replay within a three-second window.
- The **test client** accepts this predefined harmless helper once. It does not
  grant session-wide approval or install persistent rules. The real user is not
  being prompted during this scripted protocol test.
- Check resolution notification, completed turn and exact marker contents.

### Structured question case

- Create a new thread and start a Plan-mode turn, explicitly instructing the
  model to invoke its structured `request_user_input` tool with Alpha/Beta.
- Capture `item/tool/requestUserInput`, read waiting state, disconnect and
  reconnect/resume as above.
- The **test client** supplies the scripted answer Alpha. Check resolved request,
  completed turn and final agent message Alpha.

Only the client connection is replaced. The same Node controller process and
Codex app-server process stay alive. This does **not** test a controller process
crash, app-server restart, machine reboot, or durable Central approvals.

## Artifacts

`results/<timestamp>/report.json`: case outcomes and waiting/replay observations.
`rpc.jsonl`: bidirectional JSON-RPC trace, without WebSocket auth headers.
`server.log`: local server diagnostics. No real user answers or secret inputs are
used; inspect before sharing traces from future experiments with sensitive data.

Protocol generated in `schema/` is version-specific and excluded from source
control, along with `vendor/` and `workspace/`. See `FINDINGS.md` for interpretation.

Official protocol reference: https://learn.chatgpt.com/docs/app-server
