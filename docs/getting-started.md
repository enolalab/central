# Getting started with Central

Central is a local coordinator: one daemon owns state, scheduling,
approval, budget and verification; the CLI and any independent client
drive it through the public v1 API. There is no bundled UI and no
remote access — loopback only, one user.

## Install

Unzip the release (see `scripts/build-release.ps1`) so `central.exe`
and `central-server.exe` sit next to each other, then:

```powershell
$env:CENTRAL_STATE_DIR = "$HOME\.central"
central daemon start
central meta
```

`central meta` prints the API version and capabilities and fails
fast when the CLI and daemon disagree — run it first whenever
something looks odd.

## Codex on Windows

Configure the owner-controlled `codex.json` in the Central state directory
with an allowlisted Codex binary, CLI version, and surveyed models. When
Windows sandbox setup is required, `windows_sandbox` can be set explicitly
to `elevated` or `unelevated`; omitting it keeps the CLI's own configuration.
For example, using a verified elevated setup:

```json
{"binary":"C:\\path\\to\\codex.exe","cli_version":"0.167.1","surveyed_models":["gpt-6-luna"],"windows_sandbox":"elevated"}
```

Central checks the effective thread sandbox and refuses to start a turn if
the server reports a policy other than `workspaceWrite`, or does not report
it. The owned app-server is terminated and the refusal is written to
`daemon.log`. If termination cannot be confirmed, the attempt is parked
as an unknown outcome for reconciliation. `unelevated` is an explicit legacy fallback with weaker network isolation
than `elevated`; Central does not select it automatically. Validate the
selected mode with `scripts/test-windows-codex-preflight.ps1` and
`scripts/test-codex-sandbox-policy.mjs` before enabling live work. See
[Windows sandbox](https://learn.chatgpt.com/docs/windows/windows-sandbox).

## Muse

Configure the owner-controlled `muse.json` in the Central state directory
with an allowlisted Muse binary, CLI version, and surveyed models.
Only surveyed models are advertised — an empty list advertises none,
so every profile resolves to `ModelUnavailable` until a smoke proves one.

```json
{"binary":"muse","cli_version":"1.4.23","surveyed_models":["muse-spark-1.3"]}
```

Surveyed means turn-proven: `muse-spark-1.3` ran a live turn through the
gateway (session/start → turn/start → completed). The catalog default
`muse-wonder-1.0` is listed by `model/list` but has not run a turn yet —
add it only after it proves itself the same way.

The daemon routes by `profile.provider`: a `muse` attempt only touches
the Muse session host (`muse serve`), never the Codex process. The same
holds for opencode below.

## opencode

Configure the owner-controlled `opencode.json` in the Central state directory
with an allowlisted opencode binary, CLI version, and surveyed models.

```json
{"binary":"opencode","cli_version":"2.2.7","surveyed_models":["hy4-flash-free"]}
```

Surveyed means turn-proven: `hy4-flash-free` (the default model) ran live
turns through the gateway, including a permission deny/re-ask cycle.

Each `opencode` attempt owns one `opencode serve` child on loopback (free
port, per-session secret in the child environment only). No SSE, no shared
server. `GET /v1/meta` lists the configured providers. The turn flow and
permission list are live-verified on 2.2.7; the permission reply/reject
cycle is schema-captured (see `contracts/opencode/2.2.7/`): treat first
live replies as probes.
`GET /v1/meta` lists the configured providers. The MSP turn flow is
live-verified on 1.4.23–1.4.25 (echo provider), and the approval
decide/answer cycle is live-verified on 1.4.25 via meta (`allow_once`
→ server-applied `human_approval`, see `docs/validation/`); only
`userInput`/question answering is schema-captured (see
`contracts/muse/1.4.25/`).

## First run (five minutes)

```powershell
central init --goal "Try the loop" --repository "D:/work/demo"
# -> { "run_id": "<id>", ... }

central plan import --run <id> --file plan.json
central plan show --run <id>
central plan approve --run <id>   # approves the pending hash exactly
central status --run <id>
central events --run <id>
```

Plans approve by exact hash: `plan approve --hash <hash>` approves
only that revision, and a stale CLI gets a 409 instead of approving
something the user did not review.

## Daily operation

```powershell
central inbox --run <id>                       # approvals and questions
central approve --run <id> --key <k> --preview-hash <h>
central answer --run <id> --key <k> --preview-hash <h> --message "go on"
# secrets never via argv: --secret-stdin reads from stdin
central chat --run <id> --message "Should we split task A?"
central usage ... # `central budget --run <id>` shows the ledger
central pause|resume|stop --run <id>
central sync --run <id> --commit <sha>         # daemon verifies read-only
central clients create --name laptop --scope read
```

Every command prints stable JSON and exits nonzero with the daemon's
stable error code on failure.

## After a restart

The daemon parks every nonterminal run as `AwaitingResume` and does
zero agent work at boot. Look, then resume explicitly:

```powershell
central recovery --run <id>    # findings + required actions
central resume --run <id>      # the only way dispatch reopens
central events --run <id> --after <last_seq>   # reconnect from the snapshot cursor
```

Old cursors from before the restart are refused with
`resync_required`: re-snapshot first, then reconnect.

## Backup (offline)

Stop the daemon, wait for the owner lock to free, then copy the
database file **plus** its WAL sidecars **plus** the artifact store
together. Copying one file while the daemon writes is not a backup.
`central maintenance export` is a redacted report for humans — never
call it a database backup.
