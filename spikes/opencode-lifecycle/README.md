# OpenCode lifecycle spike (throwaway)

Protocol experiment for Central, not product code or a production adapter.
Central remains planned as a Rust application. This experiment uses Node's
built-in HTTP/process/file APIs to investigate the external tool first.

## Environment and reproduction

- Windows x64; OpenCode **1.19.5**, pinned npm binary distribution.
- Node.js with built-in fetch (Node 20+ recommended), PowerShell and tar.
- Run `./setup.ps1` in this directory to download and verify the pinned binary.
- Run `node ./probe.mjs` in this directory.
- Outputs: `results/report.json` and the actual server's `results/openapi.json`.
- Binary and local experiment data stay in `vendor/`, `state/`, `workspace/`.
- OpenCode uses separate XDG directories and a loopback server with a random
  Basic Auth password. Provider credentials are not needed. The configured
  dummy provider points to a closed local port; no model inference is requested.
- Shell probes are controlled Node helpers that write local marker files.
  The command syntax intentionally targets PowerShell on this Windows host.
- The probe deliberately kills only servers it starts (and their process trees
  for cleanup). Do not point it at an existing OpenCode server.

## Experiments

1. Create a session and store a no-reply message; capture SSE events.
2. Disconnect the subscriber, create a session, and reconnect with a synthetic
   Last-Event-ID. Observe for one second, then recover the missed session by API.
   This tests a bounded case, not a general proof of replay guarantees.
3. Force-kill the server tree and restart with the same data directories.
   Query the original session and saved message.
4. Start a 12-second shell child, wait for its start marker, abort the session,
   and check both child liveness and the absence of the completion marker.
5. Start another shell child, kill only the server, restart, and compare stored
   tool state, session status, and the child's actual completion marker.

`passed: false` for the SSE replay check records an absent capability in the
observed window, not a failed overall experiment. The crash-reconciliation
check records observations; its pass flag does not certify automatic resume.
See `FINDINGS.md` for interpretation and remaining gaps.

## Not established

Real model invocation or interrupted inference continuation; permission/question
interaction; config/profile isolation under concurrency; token/cost accounting;
model/effort/skill/subagent behavior; machine reboot; power-loss durability;
remote workers; general process-tree cancellation; macOS/Linux behavior.

The restart test uses abrupt process termination, not a physical reboot or disk
failure. Do not extrapolate one successful shell abort to arbitrary subprocesses.

## References

- https://docs.opencode.ai/docs/server/
- https://docs.opencode.ai/docs/sdk/
- https://opencode.ai/docs/agents/
- https://opencode.ai/docs/models/

Documentation may cover different API generations. The saved OpenAPI document
and observed behavior of version 1.19.5 are the evidence for this spike.
