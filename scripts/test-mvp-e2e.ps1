#!/usr/bin/env pwsh
# Central MVP end-to-end gate (P4 step 15): plan -> 2 workers -> independent
# verify -> integration through the real daemon, with a kill/restart barrier
# in the middle. Only the CLI and the public HTTP API are used — no private
# test mutations, no database access. The fake harness stands in for Codex;
# the script asserts the daemon-side contract (receipts, parking, resnapshot,
# provenance, no duplicate sends), not model quality.
#
# Usage: pwsh -File scripts/test-mvp-e2e.ps1
# Exit 0 when every gate passes; nonzero with the first failure.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path (Get-Location) '.toolchain/env.ps1')

function Fail($message) {
    Write-Host "E2E FAIL: $message"
    exit 1
}

function Invoke-Api($base, $token, $method, $path, $body = $null) {
    $params = @{ Uri = "$base$path"; Method = $method; Headers = @{ Authorization = "Bearer $token" } }
    if ($null -ne $body) {
        $params.ContentType = 'application/json'
        $params.Body = ($body | ConvertTo-Json -Depth 20 -Compress)
    }
    try {
        Invoke-RestMethod @params
    } catch {
        $status = $_.Exception.Response.StatusCode.value__
        $detail = $_.ErrorDetails.Message
        throw "HTTP $status $method $path : $detail"
    }
}

function Wait-For($description, $timeoutSec, [scriptblock]$probe) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        $result = & $probe
        if ($result) { return $result }
        Start-Sleep -Milliseconds 500
    }
    Fail "timed out waiting for: $description"
}

# ---------- build + fixture ----------
# The gate drives the fake harness, so the daemon builds with the
# test-support feature (release builds refuse --fake-agent by design;
# the release boundary is covered by release_security tests instead).
cargo build -j 2 -p central-cli -p central-server --features test-support
if ($LASTEXITCODE -ne 0) { Fail 'debug build failed' }

$work = Join-Path ([IO.Path]::GetTempPath()) "central-e2e-$(Get-Random)"
$stateDir = Join-Path $work 'state'
$repo = Join-Path $work 'repo'
New-Item -ItemType Directory -Path $stateDir, $repo | Out-Null
$env:CENTRAL_STATE_DIR = $stateDir

git -C $repo init -q
git -C $repo config user.email 'e2e@central.local'
git -C $repo config user.name 'e2e'
'v1' | Set-Content (Join-Path $repo 'file.txt')
git -C $repo add . ; git -C $repo commit -qm 'base'
'v2' | Set-Content (Join-Path $repo 'file.txt')
git -C $repo add . ; git -C $repo commit -qm 'user change'
$commit = (git -C $repo rev-parse HEAD).Trim()

$serverExe = Join-Path (Get-Location) 'target/debug/central-server.exe'
$cli = Join-Path (Get-Location) 'target/debug/central.exe'

$script:lastDaemonProc = $null

function Start-Daemon {
    # A killed daemon leaves server.json behind: remove it so readiness
    # always means this process.
    Remove-Item (Join-Path $stateDir 'server.json') -ErrorAction SilentlyContinue
    $proc = Start-Process $serverExe -ArgumentList @('--state-dir', $stateDir, '--listen', '127.0.0.1:0', '--fake-agent') `
        -PassThru -WindowStyle Hidden
    # Tracked before the health wait so a timeout still leaves the
    # process killable in the finally block (never orphan a lock holder).
    $script:lastDaemonProc = $proc
    $info = Wait-For 'daemon health' 30 {
        $file = Join-Path $stateDir 'server.json'
        if (Test-Path $file) {
            $addr = (Get-Content $file -Raw | ConvertFrom-Json).addr
            try {
                (Invoke-WebRequest "http://$addr/healthz" -TimeoutSec 2).StatusCode -eq 200 ? $addr : $null
            } catch { $null }
        }
    }
    return @{ Proc = $proc; Addr = $info }
}

function Stop-Daemon($daemon) {
    if ($daemon.Proc -and -not $daemon.Proc.HasExited) {
        $daemon.Proc.Kill()
        $daemon.Proc.WaitForExit(10000) | Out-Null
    }
}

# The scan loop stays ENABLED here (500 ms): production queueing and
# dispatch run exactly as deployed. Deterministic command-path tests
# cover the disabled case elsewhere.
$env:CENTRAL_SCAN_MS = '500'

try {
    # ---------- plan ----------
    $daemon = Start-Daemon
    $base = "http://$($daemon.Addr)"
    $token = (Get-Content (Join-Path $stateDir 'daemon.token') -Raw).Trim()

    $init = & $cli init --goal 'E2E: two workers then integration' --repository $repo | ConvertFrom-Json
    if (-not $init.run_id) { Fail "init failed: $($init | ConvertTo-Json -Compress)" }
    $run = $init.run_id
    Write-Host "run=$run"

    # Sync the fixture repo's base commit first: dispatching into a
    # real repository without a synced base is a deliberate deferral
    # (Central never invents the base commit), so the gate must
    # register the base before it can expect any dispatch.
    & $cli sync --run $run --commit $commit --repository $repo | Out-Null
    if ($LASTEXITCODE -ne 0) { Fail 'sync failed' }

    $plan = @{
        version = 1; goal = 'E2E: two workers then integration'
        tasks = @(
            @{ id = '00000000-0000-0000-0000-0000000000a1'; kind = 'implementation'; objective = 'Worker A';
               inputs = @(); scope = @(); outOfScope = @(); dependsOn = @(); dataDependsOn = @(); outputs = @();
               verify = @{ method = 'check'; criteria = @('a done') }; evidence = @(); profileId = $null;
               permissions = @(); maxRepairRounds = 2; requiresApproval = @(); handover = '' },
            @{ id = '00000000-0000-0000-0000-0000000000a2'; kind = 'implementation'; objective = 'Worker B';
               inputs = @(); scope = @(); outOfScope = @(); dependsOn = @(); dataDependsOn = @(); outputs = @();
               verify = @{ method = 'check'; criteria = @('b done') }; evidence = @(); profileId = $null;
               permissions = @(); maxRepairRounds = 2; requiresApproval = @(); handover = '' },
            @{ id = '00000000-0000-0000-0000-0000000000a3'; kind = 'integration'; objective = 'Integrate A and B';
               inputs = @(); scope = @(); outOfScope = @();
               dependsOn = @('00000000-0000-0000-0000-0000000000a1', '00000000-0000-0000-0000-0000000000a2');
               dataDependsOn = @('00000000-0000-0000-0000-0000000000a1'); outputs = @();
               verify = @{ method = 'check'; criteria = @('integrated') }; evidence = @(); profileId = $null;
               permissions = @(); maxRepairRounds = 2; requiresApproval = @(); handover = '';
               integration = @{ goodRef = 'refs/heads/central-good';
                   baseline = @(@{ program = 'git'; args = @('cat-file', '-e', 'HEAD:file.txt') });
                   affected = @();
                   acceptance = @(@{ program = 'git'; args = @('cat-file', '-e', 'HEAD:file.txt') }) } }
        )
    }
    $planFile = Join-Path $work 'plan.json'
    $plan | ConvertTo-Json -Depth 20 | Set-Content $planFile
    & $cli plan import --run $run --file $planFile | Out-Null
    if ($LASTEXITCODE -ne 0) { Fail 'plan import failed' }
    $pending = (Invoke-Api $base $token GET "/v1/runs/$run").state.pending_plan
    if (-not $pending) { Fail 'no pending plan after import' }
    & $cli plan approve --run $run --hash $pending | Out-Null
    if ($LASTEXITCODE -ne 0) { Fail 'plan approve failed' }

    # A stale approval (pre-approval revision) must conflict, never move the plan.
    $staleRevision = (Invoke-Api $base $token GET "/v1/runs/$run").state.revision - 1
    $staleBody = @{ id = [guid]::NewGuid().ToString(); expected_revision = $staleRevision;
        command = @{ ApprovePlan = @{ plan_hash = $pending } } }
    try {
        Invoke-Api $base $token POST "/v1/commands?run=$run" $staleBody | Out-Null
        Fail 'stale approval was accepted'
    } catch {
        if ("$_" -notmatch '409') { Fail "stale approval gave the wrong answer: $_" }
        Write-Host 'stale approval correctly refused with 409'
    }

    # ---------- dispatch two workers ----------
    $attempts = Wait-For 'two dispatched attempts' 60 {
        $snapshot = Invoke-Api $base $token GET "/v1/runs/$run"
        $list = @($snapshot.state.attempts | Where-Object { $_.status -in @('Running', 'Dispatching') })
        if ($list.Count -ge 2) { $list } else { $null }
    }
    $sends = (Invoke-Api $base $token GET '/v1/test/fake-sends').sends
    if ($sends -ne 2) { Fail "expected exactly 2 external sends, got $sends" }
    # Pair attempts to tasks explicitly: dispatch order is not task order.
    $snapshotNow = Invoke-Api $base $token GET "/v1/runs/$run"
    $attemptFor = @{}
    foreach ($attempt in $snapshotNow.state.attempts) {
        if (-not $attemptFor.ContainsKey($attempt.task_id)) {
            $attemptFor[$attempt.task_id] = $attempt.attempt_id
        }
    }
    $attemptA = $attemptFor['00000000-0000-0000-0000-0000000000a1']
    $attemptB = $attemptFor['00000000-0000-0000-0000-0000000000a2']
    if (-not $attemptA -or -not $attemptB) { Fail 'both worker attempts must be dispatched' }
    Write-Host "dispatched: $attemptA, $attemptB"

    # ---------- kill/restart barrier ----------
    $epochBefore = (Invoke-Api $base $token GET "/v1/runs/$run").instance_epoch
    $cursorBefore = (Invoke-Api $base $token GET "/v1/runs/$run").last_seq
    Stop-Daemon $daemon
    $daemon = Start-Daemon
    $base = "http://$($daemon.Addr)"
    $epochAfter = (Invoke-Api $base $token GET '/v1/meta').instance_epoch
    if ($epochAfter -eq $epochBefore) { Fail 'restart did not rotate the instance epoch' }
    $afterRestart = Invoke-Api $base $token GET "/v1/runs/$run"
    if ($afterRestart.state.control -ne 'AwaitingResume') {
        Fail "restart did not park the run: $($afterRestart.state.control)"
    }
    $sendsAfterBoot = (Invoke-Api $base $token GET '/v1/test/fake-sends').sends
    if ($sendsAfterBoot -ne 0) { Fail "boot sent $sendsAfterBoot time(s); boot must send nothing" }
    Write-Host 'boot zero inference confirmed; run parked'

    # Old cursor + old epoch: resnapshot required, never silent resume.
    try {
        Invoke-Api $base $token GET "/v1/runs/$run/events?after=$cursorBefore&known_epoch=$epochBefore" | Out-Null
        Fail 'old-epoch cursor was accepted'
    } catch {
        if ("$_" -notmatch '409') { Fail "old-epoch cursor gave the wrong answer: $_" }
        Write-Host 'old cursor correctly refused with resync_required'
    }
    $fresh = Invoke-Api $base $token GET "/v1/runs/$run"
    if ($fresh.last_seq -lt $cursorBefore) { Fail 'cursor did not survive the restart' }

    # Explicit resume reopens the gate (CLI participates here).
    & $cli resume --run $run | Out-Null
    if ($LASTEXITCODE -ne 0) { Fail 'explicit resume failed' }

    # ---------- worker results park, never pass ----------
    function Import-Result($attempt, $candidate) {
        $body = @{
            version = 1; attemptId = $attempt; inputHash = "input-$attempt";
            candidatePath = "out/$attempt.txt"; candidateHash = $candidate;
            evidencePaths = @(); summary = "scripted worker output for $attempt"
        }
        Invoke-Api $base $token POST "/v1/runs/$run/attempts/$attempt/results" $body
    }
    $sealA = Import-Result $attemptA 'candidate-a'
    $sealB = Import-Result $attemptB 'candidate-b'
    if ($sealA.sealed.status -ne 'awaiting_verification') { Fail 'result A did not park' }
    $parked = Invoke-Api $base $token GET "/v1/runs/$run"
    $taskA = @($parked.state.tasks | Where-Object { $_.task_id -eq '00000000-0000-0000-0000-0000000000a1' })[0]
    if ($taskA.status -ne 'AwaitingVerification') { Fail "task A is $($taskA.status), want AwaitingVerification" }

    # ---------- independent verification ----------
    function Submit-Pass($task, $attempt, $candidate) {
        $view = Invoke-Api $base $token GET "/v1/runs/$run/tasks/$task"
        $thread = (Invoke-Api $base $token GET "/v1/runs/$run/attempts/$attempt").session.thread_id
        $report = @{
            subjectHash = $candidate; criteriaHash = $view.criteria_hash; outcome = 'Pass'
            evidenceIds = @("input-for-$attempt"); failedCriteria = @()
            environment = 'e2e-scripted'; checkRefs = @()
            verifierThread = "verifier-for-$attempt"; verifierWroteTrackedSource = $false
            sameSessionOverride = $false; findings = "candidate $candidate meets the criteria"
        }
        if ($report.verifierThread -eq $thread) { $report.verifierThread = "$thread-verify" }
        $envelope = @{ id = [guid]::NewGuid().ToString(); expected_revision = $null;
            command = @{ SubmitVerification = @{ task = $task; attempt = $attempt; report = $report } } }
        Invoke-Api $base $token POST "/v1/commands?run=$run" $envelope | Out-Null
    }
    Submit-Pass '00000000-0000-0000-0000-0000000000a1' $attemptA 'candidate-a'
    Submit-Pass '00000000-0000-0000-0000-0000000000a2' $attemptB 'candidate-b'
    $verified = Wait-For 'both tasks succeeded' 30 {
        $snapshot = Invoke-Api $base $token GET "/v1/runs/$run"
        $states = @($snapshot.state.tasks | Where-Object { $_.kind -eq 'implementation' } | Select-Object -ExpandProperty status)
        if (($states | Where-Object { $_ -eq 'Succeeded' }).Count -eq 2) { $snapshot } else { $null }
    }
    $outputs = $verified.state.verified_outputs
    if (-not ($outputs | Where-Object { $_.candidate_hash -eq 'candidate-a' })) { Fail 'provenance for A missing' }
    if (-not ($outputs | Where-Object { $_.candidate_hash -eq 'candidate-b' })) { Fail 'provenance for B missing' }
    Write-Host 'worker -> verifier provenance confirmed for A and B'

    # ---------- integration completes through the mechanical path ----------
    # The integration task carries an approved IntegrationSpec, so the
    # scheduler auto-dispatches it and the workspace manager seals,
    # checks and CAS-promotes the good ref with no gateway turn and no
    # verdict: completion is the Succeeded task plus the moved ref.
    Wait-For 'integration task succeeded' 180 {
        $snapshot = Invoke-Api $base $token GET "/v1/runs/$run"
        $task = @($snapshot.state.tasks | Where-Object { $_.task_id -eq '00000000-0000-0000-0000-0000000000a3' })[0]
        if ($task.status -eq 'Succeeded') { $true } else { $null }
    } | Out-Null
    Write-Host 'integration task reached Succeeded through the mechanical path'

    # No gateway sends for integration (mechanical path sends nothing);
    # the proof is the moved good ref in the fixture repo.
    $totalSends = (Invoke-Api $base $token GET '/v1/test/fake-sends').sends
    if ($totalSends -ne 0) { Fail "expected 0 post-restart sends (mechanical integration), got $totalSends" }
    $goodRef = (git -C $repo rev-parse 'refs/heads/central-good').Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrEmpty($goodRef)) { Fail 'good ref was not created' }
    if ($goodRef -eq $commit) { Fail 'good ref did not move past the base' }
    Write-Host "good ref promoted: $goodRef"

    # Independent HTTP client replays the cursor from the snapshot: no gap.
    $final = Invoke-Api $base $token GET "/v1/runs/$run"
    $tail = Invoke-Api $base $token GET "/v1/runs/$run/events?after=$($final.last_seq)"
    if (@($tail.items).Count -ne 0) { Fail 'cursor does not cover the snapshot' }

    Write-Host 'E2E PASS: plan, 2 workers, independent verify, integration, restart barrier, provenance.'
} finally {
    if ($daemon) { Stop-Daemon $daemon }
    if ($script:lastDaemonProc -and -not $script:lastDaemonProc.HasExited) {
        $script:lastDaemonProc.Kill()
    }
    Remove-Item -Recurse -Force $work -ErrorAction SilentlyContinue
}
