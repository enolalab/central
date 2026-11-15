# Windows live acceptance follow-up — 2026-11-14

Baseline: `1dd94b16384b7feba3372f68ea5bc06f785fbc02`.
Status: **Windows sandbox downgrade reproduced; a supervised direct-CLI
worker succeeded after explicit sandbox selection. Daemon live acceptance
and an independent model verifier remain outstanding.**

The existing VM SSH service at `127.0.0.1:2223` responded, but
`ssh -o BatchMode=yes ... Administrator@127.0.0.1` failed with
`Permission denied (publickey,password,keyboard-interactive)`.

Automatic approval review rejected a proposed use of the Administrator
password from the VM's existing unattended-install XML: that credential
source was not specifically authorized. The rejected command was not run.
On 2026-11-15 the user explicitly authorized this credential for the local
VM; the authorized SSH connection then succeeded. No credential was printed
or copied and VM authentication was not modified.

## Windows diagnostic (2026-11-15)

`scripts/test-windows-codex-preflight.ps1` creates its own temporary
workspace and invokes `codex sandbox` directly with a workspace permission
profile and a harmless `cmd.exe` marker write. It has a bounded child-process
timeout, captures command output, and cleans up only its own process tree and
fixture. It makes zero model calls and does not change saved configuration.

Run it as the same Windows user that runs `central-server`:

```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/test-windows-codex-preflight.ps1 -CodexBinary <native-codex-exe-or-cmd>
```

The optional `-WindowsSandbox elevated` or `unelevated` is a command-line
override, not an automatic fallback. `unelevated` has weaker network
isolation than `elevated`. An older CLI may not support the diagnostic's
permission-profile syntax; a CLI argument error is not proof that filesystem
writes are denied.

The current-mode preflight passed with the VM's existing CLI `0.166.0` and
the separately installed, allowlisted `0.167.1`. A later run with `0.167.1`
and explicit `unelevated` also passed when overwriting an existing fixture
file; `elevated` failed with `timed out after 15000ms connecting runner
pipe-in`. These results cover the direct command sandbox only; they do not
prove an app-server session's effective policy or sandbox containment.

A separate zero-turn app-server probe sent the same `thread/start` sandbox
request as Central. With the VM's current configuration, `0.167.1` reported
`{"type":"readOnly","networkAccess":false}` despite the requested
`workspace-write`. Process-only `windows.sandbox` overrides `unelevated`
and `elevated` each made `thread/start` report `workspaceWrite`; that
response alone does not establish that the runner can launch.

The repeatable zero-turn probe is `scripts/test-codex-sandbox-policy.mjs`
(Node is a development/test dependency, not a Central runtime dependency):

```powershell
node scripts/test-codex-sandbox-policy.mjs --binary <native-codex-exe> --cwd <fixture-dir>
node scripts/test-codex-sandbox-policy.mjs --binary <native-codex-exe> --cwd <fixture-dir> --windows-sandbox unelevated
```

Both commands ran on the VM against CLI `0.167.1`; the first returned
`readOnly` and exit 1, the second `workspaceWrite` and exit 0. Neither
started a model turn. A separate `command/exec` probe with `unelevated`
confirmed `cmd.exe` can start; Windows PowerShell exited `0xc0000142` and
the Store `pwsh.exe` alias failed with Windows error 1312. The portable
`C:\central\.toolchain\pwsh\pwsh.exe` was not installed in the VM.
`query user` reported no interactive Windows user. These observations do
not, by themselves, prove why the PowerShell token/logon session failed.
An additional no-model `command/exec` probe created its own sentinel in the
Windows user's home, outside the temporary work directory. The sandboxed
`cmd.exe` overwrite returned `Access is denied`, and the sentinel remained
unchanged before the probe removed it. This is one boundary check, not a
comprehensive assessment of filesystem or network isolation.

An attempted zero-turn `thread/resume` probe against an ephemeral thread
returned `no rollout found`; therefore the effective policy field on a live
resumed Windows thread remains unverified. Regression tests exercise both
resume and start with a scripted peer.

PowerShell `7.6.6` portable was downloaded to the VM user's TEMP from the
official PowerShell release. The zip SHA-256 matched the release API digest
(`02fe458be20493fbdf43f61ea20610b811ee6c738ab1676c61b9cfcd1a33c860`)
and `pwsh.exe` had a valid Microsoft Corporation Authenticode signature.
The same no-model app-server command probe of this native portable binary
exited `0xc0000142`, like Windows PowerShell 5.1; it does not establish that
the Store alias itself was the only cause. No PATH or saved user config was
changed. See [Microsoft's ZIP installation guidance](https://learn.microsoft.com/en-us/powershell/scripting/install/install-powershell-on-windows).

The live fixture smoke used three turns total, all requested `gpt-6-luna`
at `low`, with no model fallback explicitly configured:

1. Current configuration: file unchanged; model reported read-only policy.
   Usage: 35,283 input, 29,056 cached input, 185 output tokens.
2. Explicit `unelevated`: file unchanged. `cmd.exe` could read the fixture,
   but the PowerShell runner failed with Windows error 1312, and `apply_patch`
   failed. Usage: 167,345 input, 158,464 cached input, 657 output tokens.
3. Explicit `unelevated`, prompt constrained to `cmd.exe`: worker edited the
   fixture. Usage: 73,849 input, 69,888 cached input, 446 output tokens.
   SHA-256 changed from `4a48a969fa7f14314ae0384771e504955b2fe5d2933c3532737a671159848891`
   to `337fcc66b984f0231aba013160242f6fd348904f6937275a515d2614fabc5ef8`.
   The actual file was copied to Linux and checked independently: `add(1,2)`
   returns 3, and booleans in either argument raise `TypeError`.

The CLI did not report the observed model in the terminal usage events;
`gpt-6-luna` is the requested model, not independently established by those
events. The fixture and JSONL remain in the Windows user's TEMP directory
under `central-live-acceptance-b2376db786e64237a2baa3cbed7f5120`.
The verifier above is a deterministic local check, not an independent
model verifier or a daemon workflow. No live work was sent to the real repo.

The 37 product files changed between `59ce19e` and `57f3bbb` were compared
against the VM tree by SHA-256; all matched. This does not independently bind
an existing release binary to those sources.

## Product changes and deterministic checks

- The gateway verifies that `thread/start` and `thread/resume` report the
  effective sandbox `workspaceWrite` before sending `turn/start`. `readOnly`,
  broader policies, and missing policy are rejected. The app-server owned by
  that attempt is terminated; if termination cannot be confirmed the send
  becomes `OutcomeUnknown`, the process identity remains available for
  reconciliation, and no retry is forced.
- The owner-controlled `codex.json` accepts an optional `windows_sandbox`
  selection limited to `elevated` or `unelevated`. No override is selected
  by default; no existing user configuration is rewritten. The reduced
  isolation of `unelevated` is documented.
- The `0.167.1` protocol subset now records the effective sandbox and fixes a
  pre-existing JSON syntax error. A regression test parses the subset.
- Linux full workspace test (after the rejection/cleanup changes): **264
  passed, 0 failed, 1 ignored**. Format and Clippy (`-D warnings`) passed.
  The transport regression test also covers new/resumed threads, missing or
  unsafe policy, no `turn/start`, exactly one cleanup, and failed-termination
  reconciliation.
- Windows release build (`cargo build --release --locked -j 2 -p
  central-cli -p central-server`): **PASS**, 1m 13s with the VM's existing
  cache. The two previously clean source files were backed up and
  temporarily staged in `C:\central`, then restored after hash checks;
  `git status` confirms those two VM files are clean. Their staged SHA-256
  hashes matched this workspace's `gateway.rs` (`2855f15a…e56668`) and
  `bootstrap.rs` (`82b4df97…93b741`). Resulting isolated
  `central-server.exe` SHA-256: `8bb1aa9a2060c17e20955f1af0b1191964afa36fde88f9783252d41703a7f688`.
  The first source-copy build had to rebuild DuckDB, timed out after 20
  minutes, and its identified orphaned Cargo process tree was stopped.
  Neither build called a model.
- The freshly built release daemon was then exercised on Windows with an
  explicitly scripted fake app-server. The fake returned `readOnly` and
  wrote a marker if it received `turn/start`. Both default config and
  explicit `windows_sandbox: unelevated` produced a failed attempt and the
  expected guard log; the marker was absent in both runs. The fake also
  recorded its argv: the explicit override arrived at the child and no
  override appeared in the default case. This was a deterministic daemon
  integration test, not a model or real-Codex end-to-end run.
- After testing, no fixture server, fake app-server, or Cargo build remained
  running; the VM, which was off at the start of this follow-up, was shut down
  gracefully. The fixture and build evidence were retained in VM TEMP.
- Live daemon/independent-model verification remains outstanding and is not
  inferred from the Linux suite, the Windows build, or the direct-CLI smoke.

## Remaining execution sequence

1. Resolve the Windows PowerShell runner failure in the VM's noninteractive
   session. `unelevated` plus an explicit `cmd.exe` prompt enabled one worker
   edit, but it is not a general solution for arbitrary worker commands;
   `elevated` timed out. Preserve sandbox and approval routing.
2. The VM's global CLI is `0.166.0`; a separate, allowlisted `0.167.1` build
   is present in TEMP for the next fixture. It was not installed globally.
3. Set a fresh explicit live budget before another model call: the initial
   three-turn worker/verifier smoke budget has been exhausted. Use a
   fixture-owned repository, stop immediately on
   auth/quota errors, record requested/observed model, effort and usage. Use
   `gpt-6-luna` / low without fallback. Do not send the Central source repo.
4. Exercise the product daemon/API path, seal/hash the worker candidate, run
   an independent verifier, submit its real verdict, and demonstrate Git
   integration/promotion. Record every operator action separately from the
   automatic scheduler actions. Do not replace model results with scripted
   fake verdicts, or infer live Windows acceptance from Linux/fake gates.

Official documentation consulted (current guidance must still be checked
against the installed CLI build):

- [Windows sandbox](https://learn.chatgpt.com/docs/windows/windows-sandbox)
- [App-server Windows sandbox readiness/setup](https://learn.chatgpt.com/docs/app-server#windows-sandbox-setup-windowssandboxsetupstart)

No claim of a fixed sandbox or completed Windows live acceptance is made.

## Additional Windows daemon live attempt (2026-11-15)

The owner authorized at most one daemon worker turn and one independent
verifier turn. A fresh fixture used the current Windows release binary
(`central-server.exe` SHA-256 `8bb1aa9a2060c17e20955f1af0b1191964afa36fde88f9783252d41703a7f688`),
Codex CLI 0.167.1, requested `gpt-6-luna`/low and the explicit temporary
`windows_sandbox: unelevated` override. The daemon created run
`2da6fae5-3ba8-418e-9e55-b7b609b2ce33` and worker attempt
`2bfb69d8-a67b-4cd8-885d-d31d44eaaf1d`; its durable observations include
`turn/started`, but no `turn/completed`. During polling, an inbox reconcile
request returned HTTP 503. The harness did not retain the response body, so
the underlying transport or capability failure is **not established**.
The harness stopped at this unknown outcome and its cleanup terminated its
owned daemon/process tree; the missing completion is therefore not proof of
a daemon crash or of a model-side failure. The worker worktree's `calc.py`
remained unchanged when inspected. Token usage is unknown.

No retry or verifier turn was sent, and no result was imported or Git ref
promoted. A subsequent inspection started the daemon with `CENTRAL_SCAN_MS=0`
and did not resume the run: recovery reported `AwaitingResume`, while the
attempt remained `Running`. The preserved fixture is
`C:\Users\Administrator\AppData\Local\Temp\central-live-daemon-06e3a1fac9654dada8d76f7bac2f9bc3`.
A later investigation should retain HTTP error bodies and diagnose the 503
without resending the unresolved turn. Windows daemon-to-verifier-to-Git
live acceptance remains incomplete; the prior direct CLI worker smoke and
Linux integration evidence do not close that gap.
