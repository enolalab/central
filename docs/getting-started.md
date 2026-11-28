# Getting started with Central

Central is a local coordinator: one daemon owns state, scheduling,
approval, budget and verification; the CLI and any independent client
drive it through the public v1 API. The bundled browser workspace also
uses the public API and runs on the same loopback address as the daemon.

## Local web workspace

Run `central local-web` to start the daemon and open the workspace in
your default browser. If a daemon is already running for the selected
state directory, the command opens its existing address. A new daemon
listens on `127.0.0.1:8765` by default. Pass `--listen 127.0.0.1:0`
for an available port. If the browser launcher is unavailable, open the
URL printed by the command.

Select **Kết nối** and enter a workflow control token, or choose
`daemon.token` from the state directory. The page keeps the token in
memory for the browser session. Open **Cài đặt agent** to add or edit a
named configuration with a provider, model and reasoning effort. Open
**Model Worker / Leader** when drafting a task to select the model for
each role. The choices come from the running daemon. Leader can use the
Worker model, and you can remember both choices for the project.

Central enables all catalog models by default for each configured
provider. To restrict a provider, turn off **Bật toàn bộ model trong catalog**
and enter the models to allow. Refresh the catalog snapshot with
`python3 scripts/refresh-model-catalog.py --state-dir <state-dir>` and
restart the daemon to activate the updated catalog. The refresh reads
catalogs without starting inference.

With the owner token, **Cấu hình provider và danh sách model** edits the
CLI version, catalog toggle and manual model list. Central loads these
settings on daemon startup and continues to enforce the CLI allowlist.
Provider CLI installation and sign-in are handled by the provider.

Use **+** to open a workspace from a local Git repository path. The
browser keeps a draft per open workspace. The default **Trao đổi** mode
sends messages to the selected leader, who can ask follow-up questions.
Leader replies render as sanitized markdown (links open in a new tab;
images and scripts are stripped), while your messages and the live
streaming preview stay plain text. The completed reply replaces the
preview, and a proposed plan appears only after validation.
Leader turns keep the recent conversation and run in their own scratch
directories. The chat header shows **Đọc được mã workspace** or
**Không đọc được mã workspace · <lý do>**: the leader reads workspace
code only for a local-checkout workspace with the Codex provider.
Once the chat has at least one turn and no plan is active, **Lập kế
hoạch** sends your draft (or a default prompt) with planning intent;
discussion alone never creates a plan. While planning, the status reads
"Đang lập kế hoạch: leader có thể hỏi các câu cần thiết", with **Lập
ngay với giả định** forcing a proposal in one turn and **Quay lại thảo
luận** returning to discussion. A pending plan appears inline under the
message that produced it (goal, task count, **Xem chi tiết**), and the
top bar reads "Có kế hoạch chờ duyệt". In the plan dialog, **Duyệt và
chạy** approves that exact hash to start the worker, and **Yêu cầu sửa**
sends your written feedback to the leader for a revised proposal; if the
plan changed meanwhile you get a prompt to reopen the new revision.
**Chạy ngay** remains available for a clear, one-step task.
Use **Công việc** or **Văn phòng** to see the same runs and inbox.

To request a review of an existing worker attempt, select it and choose
**Yêu cầu leader xem lại**. Results and independent verifier reports
still require their structured reports.
The web workspace can submit these reports and show recorded artifacts.
Applying changes still uses Central's separate Git integration workflow.

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

## ACP transport

Set `"transport": "acp"` in each provider's existing JSON file in the
Central state directory. Keep its model inventory and CLI version settings.
Muse Code and OpenCode run their respective `acp` subcommand. Codex runs
`codex-acp` from `PATH`, or the executable specified in `acp_binary`, and
passes the configured Codex executable to that adapter as `CODEX_PATH`.
Install the adapter before restarting Central.

New sessions use ACP after restart. Central can load ACP sessions when the
agent advertises that capability. Previously recorded native sessions
continue to route through their original transport for reconciliation.

Central surfaces session forms containing one unrestricted, non-sensitive
text field in Inbox. You can reply, decline, or cancel from the request
dialog. Agents can also ask follow-up questions in their ordinary chat
replies.

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
with an allowlisted Muse binary and CLI version. The optional
`surveyed_models` list adds model IDs to those found in the catalog.

```json
{"binary":"muse","cli_version":"1.4.23","surveyed_models":["muse-spark-1.3"]}
```

`muse-spark-1.3` ran a live turn through the gateway. Catalog entries
may still require sign-in or provider access when selected.

The daemon routes by `profile.provider`: a `muse` attempt only touches
the Muse session host (`muse serve`), never the Codex process. The same
holds for opencode below.

## opencode

Configure the owner-controlled `opencode.json` in the Central state directory
with an allowlisted opencode binary, CLI version, and any additional models.

```json
{"binary":"opencode","cli_version":"2.2.7","surveyed_models":["hy4-flash-free"]}
```

`hy4-flash-free` ran live turns through the gateway, including a
permission deny/re-ask cycle.

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
central profiles use --run <id> --role worker --profile <pid>  # run default agent+model
central profiles use --run <id> --role leader --profile <pid>  # leader turn profile
central leader ask --run <id> --task <t> --objective "review it"  # real dispatched turn
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
