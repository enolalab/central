#!/usr/bin/env pwsh
# Independent Central client in pure PowerShell: HTTP only, no crate
# imports, no database access. Drives one run end to end using only
# the published v1 contract.
#
# Usage:
#   $env:CENTRAL_STATE_DIR = "$HOME\.central"
#   .\examples\client.ps1
#
# The owner token is read from the state directory (owner-only file
# permissions); it is never placed in a URL, never logged, and never
# written anywhere by this script.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$stateDir = if ($env:CENTRAL_STATE_DIR) { $env:CENTRAL_STATE_DIR } else { '.central' }
$serverInfo = Get-Content (Join-Path $stateDir 'server.json') | ConvertFrom-Json
$base = "http://$($serverInfo.addr)"
$token = (Get-Content (Join-Path $stateDir 'daemon.token') -Raw).Trim()
$headers = @{ Authorization = "Bearer $token" }

function Invoke-Api($method, $path, $body = $null) {
    $params = @{ Uri = "$base$path"; Method = $method; Headers = $headers }
    if ($null -ne $body) {
        $params.ContentType = 'application/json'
        $params.Body = ($body | ConvertTo-Json -Depth 20 -Compress)
    }
    try {
        Invoke-RestMethod @params
    } catch {
        $detail = $_.ErrorDetails.Message
        Write-Host "API $method $path failed: $detail"
        throw
    }
}

function New-Command($command, $revision = $null) {
    @{ id = [guid]::NewGuid().ToString(); expected_revision = $revision; command = $command }
}

# 1. Discover: abort on a contract mismatch, never misread a daemon.
$meta = Invoke-Api GET '/v1/meta'
if ($meta.api -ne 'v1') { throw "daemon API mismatch: $($meta.api)" }
Write-Host "daemon api=$($meta.api) epoch=$($meta.instance_epoch)"

# 2. Create a run (the daemon allocates the id).
$receipt = Invoke-Api POST '/v1/commands' (New-Command @{ CreateRun = @{
    goal = 'Independent client smoke run'; repository = 'D:/work/demo' } })
$run = $receipt.run_id
Write-Host "run=$run revision=$($receipt.revision)"

# 3. Read the snapshot: revision + cursor for everything after.
$snapshot = Invoke-Api GET "/v1/runs/$run"
$cursor = $snapshot.last_seq
$epoch = $snapshot.instance_epoch
Write-Host "control=$($snapshot.state.control) last_seq=$cursor"

# 4. Post a discussion message (never an approval).
$message = Invoke-Api POST "/v1/runs/$run/chat" @{ body = 'Hello from the independent client.' }
Write-Host "chat seq=$($message.seq)"

# 5. Propose a plan and approve its exact hash (never an implicit latest).
$plan = @{
    version = 1; goal = 'Independent client smoke run'
    tasks = @(
        @{ id = '00000000-0000-0000-0000-0000000000b1'; kind = 'implementation'; objective = 'Do the thing';
           inputs = @(); scope = @(); outOfScope = @(); dependsOn = @(); dataDependsOn = @(); outputs = @();
           verify = @{ method = 'check'; criteria = @('done') }; evidence = @(); profileId = $null;
           permissions = @(); maxRepairRounds = 2; requiresApproval = @(); handover = '' }
    )
}
Invoke-Api POST "/v1/commands?run=$run" (New-Command @{ ProposePlan = @{ plan = $plan } }) | Out-Null
$pending = (Invoke-Api GET "/v1/runs/$run").state.pending_plan
$revision = (Invoke-Api GET "/v1/runs/$run").state.revision
$approval = Invoke-Api POST "/v1/commands?run=$run" (New-Command @{ ApprovePlan = @{ plan_hash = $pending } } $revision)
Write-Host "approved $pending at revision $($approval.revision)"

# 6. Read the inbox and the plan diff inputs a client needs.
$inbox = Invoke-Api GET "/v1/runs/$run/inbox"
Write-Host "inbox entries: $(@($inbox.entries).Count)"
$plans = Invoke-Api GET "/v1/runs/$run/plans"
Write-Host "plans: $(@($plans.items).Count), active: $($plans.active_plan)"

# 7. Page the events from the snapshot cursor: nothing lost at the edge.
$page = Invoke-Api GET "/v1/runs/$run/events?after=$cursor"
Write-Host "new events: $(@($page.items).Count), next cursor: $($page.next_cursor)"
if ($page.next_cursor) {
    $follow = Invoke-Api GET "/v1/runs/$run/events?after=$($page.next_cursor)"
    Write-Host "resume from opaque cursor: $(@($follow.items).Count) event(s), cursor: $($follow.next_cursor)"
}

# 8. Reconnect from the cursor with the epoch proof: a restarted daemon
#    answers resync_required instead of silently resuming.
try {
    $events = Invoke-Api GET "/v1/runs/$run/events?after=$cursor&known_epoch=$epoch"
    Write-Host "reconnect ok: $(@($events.items).Count) event(s)"
} catch {
    Write-Host '(daemon restarted mid-run: refetch the snapshot and reconnect — expected path)'
}

# 9. Usage and recovery stay readable without side effects.
$usage = Invoke-Api GET "/v1/runs/$run/usage"
$recovery = Invoke-Api GET "/v1/runs/$run/recovery"
Write-Host "usage unknown=$($usage.ledger.unknown), recovery actions: $(@($recovery.actions).Count)"

Write-Host 'independent client finished: create, chat, plan approve, inbox, events, usage and recovery over HTTP only.'
