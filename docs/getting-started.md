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
