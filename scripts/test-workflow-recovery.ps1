# P3 Task 6 gate: workflow recovery and side-effect reconciliation.
#
# Flow: deterministic barrier tests (all three named fault barriers,
# in-process, virtual clock) -> real daemon kill-restart at the after_send
# barrier (kill own child only) with the scan loop disabled for setup and
# enabled for the resume phase -> explicit resume -> scan queues and
# dispatches two independent implementation tasks while the downstream
# verifier and integration tasks stay held (dependencies unmet) -> second
# kill-restart with those stages pending -> resume -> no duplicate sends,
# downstream still held -> reconcile endpoint stores evidence without
# resending. A second parked run proves the shared scan leaves unrelated
# runs untouched. Fake gateway only, no live models.
#
# Honest scope: the fake harness never completes attempts (no worker
# results or verdicts flow through the daemon), so verifier/integration
# *progression* — verdicts driving their dispatch — cannot run here. The
# gate proves the daemon dispatches what is ready and holds the rest
# across two crashes; full stage progression stays covered in-process by
# the quality_loop / git_integration suites (see docs/validation/p3-workflow.md).
#
# Reboot/manual Windows acceptance is NOT inferred from the process kill
# here; it is recorded separately in docs/validation/p3-workflow.md.
$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
. (Join-Path $root ".toolchain\env.ps1")

function Assert-Equal($actual, $expected, $message) {
    if ($actual -ne $expected) {
        throw "ASSERT FAILED: $message (expected '$expected', got '$actual')"
    }
    Write-Output "ok: $message"
}
function Assert-True($condition, $message) {
    if (-not $condition) { throw "ASSERT FAILED: $message" }
    Write-Output "ok: $message"
}

Push-Location $root
try {
    Write-Output "== fault barrier matrix (own-child kills only, no live models) =="
    foreach ($name in @("before_commit", "after_send", "after_git_promote")) {
        $fixture = Get-Content (Join-Path $root "tests\fixtures\faults\$name.json") -Raw | ConvertFrom-Json
        Assert-Equal $fixture.barrier $name "barrier $name declared"
        Assert-Equal $fixture.kill_scope "own-child-only" "barrier $name kills only its own child"
        Assert-Equal $fixture.live_models $false "barrier $name uses no live models"
    }

    Write-Output "== deterministic barrier tests (virtual clock, fake gateway) =="
    cargo test -j 2 -p central-server --test workflow_recovery
    if ($LASTEXITCODE -ne 0) { throw "workflow_recovery tests failed" }

    Write-Output "== building daemon + CLI (test-support) =="
    cargo build -j 2 -p central-server --features test-support
    if ($LASTEXITCODE -ne 0) { throw "cargo build (server) failed" }
    cargo build -j 2 -p central-cli
    if ($LASTEXITCODE -ne 0) { throw "cargo build (cli) failed" }
    $central = Join-Path $root "target\debug\central.exe"

    $stateDir = Join-Path ([System.IO.Path]::GetTempPath()) ("central-workflow-" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Force $stateDir | Out-Null
    $planFile = Join-Path $stateDir "plan.json"
    @'
{
  "version": 1,
  "goal": "Ship the reliable workflow",
  "tasks": [
    {"id":"00000000-0000-0000-0000-000000000001","kind":"implementation","objective":"Task A","inputs":[],"scope":[],"outOfScope":[],"dependsOn":[],"dataDependsOn":[],"outputs":[],"verify":{"method":"check","criteria":["a done"]},"evidence":[],"profileId":null,"permissions":[],"maxRepairRounds":2,"requiresApproval":[],"handover":""},
    {"id":"00000000-0000-0000-0000-000000000002","kind":"implementation","objective":"Task B","inputs":[],"scope":[],"outOfScope":[],"dependsOn":[],"dataDependsOn":[],"outputs":[],"verify":{"method":"check","criteria":["b done"]},"evidence":[],"profileId":null,"permissions":[],"maxRepairRounds":2,"requiresApproval":[],"handover":""},
    {"id":"00000000-0000-0000-0000-000000000003","kind":"verification","objective":"Verify A and B","inputs":[],"scope":[],"outOfScope":[],"dependsOn":["00000000-0000-0000-0000-000000000001","00000000-0000-0000-0000-000000000002"],"dataDependsOn":[],"outputs":[],"verify":{"method":"check","criteria":["a and b verified"]},"evidence":[],"profileId":null,"permissions":[],"maxRepairRounds":2,"requiresApproval":[],"handover":""},
    {"id":"00000000-0000-0000-0000-000000000004","kind":"integration","objective":"Integrate verified work","inputs":[],"scope":[],"outOfScope":[],"dependsOn":["00000000-0000-0000-0000-000000000003"],"dataDependsOn":[],"outputs":[],"verify":{"method":"check","criteria":["integrated"]},"evidence":[],"profileId":null,"permissions":[],"maxRepairRounds":2,"requiresApproval":[],"handover":""}
  ]
}
'@ | Set-Content -Encoding UTF8 $planFile

    # CLI calls go through Start-Process + WaitForExit() on the CLI
    # process alone, with output redirected to real files: a captured
    # pipe never reaches EOF while the detached daemon holds inherited
    # handles, and Start-Process -Wait waits for the whole tree.
    function Invoke-Central {
        param([Parameter(ValueFromRemainingArguments = $true)][string[]]$rest)
        $outFile = Join-Path $stateDir ("cli-" + [guid]::NewGuid().ToString("N") + ".json")
        $argList = (@("--state-dir", $stateDir) + $rest) | ForEach-Object {
            if ($_ -match '\s') { '"' + $_ + '"' } else { $_ }
        }
        $process = Start-Process -FilePath $central `
            -ArgumentList $argList `
            -RedirectStandardOutput $outFile `
            -RedirectStandardError "$outFile.err" `
            -PassThru -WindowStyle Hidden
        $process.WaitForExit()
        $output = Get-Content $outFile -Raw
        return @{ code = $process.ExitCode; output = $output }
    }
    function Central {
        param([Parameter(ValueFromRemainingArguments = $true)][string[]]$rest)
        $result = Invoke-Central @rest
        if ($result.code -ne 0) {
            throw "central $($rest -join ' ') failed with $($result.code) : $($result.output)"
        }
        return ($result.output | ConvertFrom-Json)
    }
    function Run-View($run) { Central status --run $run }
    function Fake-Sends($addr, $token) {
        return (Invoke-RestMethod -Uri "http://$addr/v1/test/fake-sends" -Headers @{ Authorization = "Bearer $token" }).sends
    }
    function Attempt-Count($view, $taskId) {
        if ($null -eq $view.state.attempts) { return 0 }
        return @($view.state.attempts.PSObject.Properties | Where-Object { $_.Value.task_id -eq $taskId }).Count
    }
    # Pre-flight for every daemon start: if a previous run left a daemon
    # recorded in server.json that is alive but answers nothing on
    # healthz (a zombie holding the owner lock), stop ONLY that recorded
    # pid — our own test child — so the start below cannot fail with a
    # misleading already-running. A healthy daemon is never touched.
    function Stop-RecordedDaemon {
        $infoPath = Join-Path $stateDir "server.json"
        if (-not (Test-Path $infoPath)) { return }
        try { $info = Get-Content $infoPath -Raw | ConvertFrom-Json } catch { return }
        if ($null -eq $info.pid) { return }
        $live = Get-Process -Id $info.pid -ErrorAction SilentlyContinue
        if ($null -eq $live) { return }
        $healthy = $false
        try {
            $probe = Invoke-WebRequest -Uri "http://$($info.addr)/healthz" -TimeoutSec 2 -UseBasicParsing
            $healthy = $probe.StatusCode -eq 200
        } catch { $healthy = $false }
        if ($healthy) { return }
        Write-Output "pre-flight: recorded daemon pid $($info.pid) is alive but not listening; stopping our own test child"
        # Best-effort: the pid may exit between the check and the kill. A
        # genuinely unkillable holder surfaces honestly below, when
        # `daemon start` reports already-running.
        Stop-Process -Id $info.pid -Force -ErrorAction SilentlyContinue
        Wait-Process -Id $info.pid -Timeout 30 -ErrorAction SilentlyContinue
    }
    # Readiness for dispatched work: the sends counter increments before
    # the attempt's Running commit lands, and Run-View can predate both,
    # so a view read next to sends>=2 may still show Dispatching. Refresh
    # until the attempts converge (or the deadline expires and the gate
    # fails honestly instead of asserting on a stale view).
    function Wait-AttemptsRunning($run, $minRunning, $timeoutSec) {
        $deadline = (Get-Date).AddSeconds($timeoutSec)
        $running = @()
        do {
            Start-Sleep -Milliseconds 500
            $view = Run-View $run
            if ($null -ne $view.state.attempts) {
                $running = @($view.state.attempts.PSObject.Properties | Where-Object { $_.Value.status -eq "Running" })
            }
            if ($running.Count -ge $minRunning) { return $view }
            if ((Get-Date) -gt $deadline) { break }
        } while ($true)
        throw "ASSERT FAILED: only $($running.Count) attempts Running after ${timeoutSec}s (wanted $minRunning); dispatch never converged"
    }
    function Total-Attempts($view) {
        if ($null -eq $view.state.attempts) { return 0 }
        return @($view.state.attempts.PSObject.Properties).Count
    }
    $taskA = "00000000-0000-0000-0000-000000000001"
    $taskB = "00000000-0000-0000-0000-000000000002"
    $taskV = "00000000-0000-0000-0000-000000000003"
    $taskI = "00000000-0000-0000-0000-000000000004"

    Write-Output "== phase 1: setup with the scan loop disabled (deterministic) =="
    $env:CENTRAL_SCAN_MS = "0"
    Stop-RecordedDaemon
    $started = Central daemon start --fake-agent
    Assert-Equal $started.status "started" "daemon started (scan disabled)"
    $run = (Central init --goal "Ship the reliable workflow" --repository "D:/Code/enolalab/central").run_id
    $imported = Central plan import --run $run --file $planFile
    $null = Central plan approve --run $run --hash $imported.plan_hash
    $null = Central pause --run $run
    $paused = Run-View $run
    Assert-Equal $paused.state.control "Paused" "run paused before crash; nothing queued or sent"
    # Second, unrelated run: parked from the start, never resumed. The
    # shared scan must leave it untouched through both crashes.
    $run2 = (Central init --goal "Unrelated parked run" --repository "D:/Code/enolalab/central").run_id
    $null = Central pause --run $run2
    $paused2 = Run-View $run2
    Assert-Equal $paused2.state.control "Paused" "unrelated run parked before crash"
    $info = Get-Content (Join-Path $stateDir "server.json") | ConvertFrom-Json
    $token = (Get-Content (Join-Path $stateDir "daemon.token")).Trim()
    Assert-Equal (Fake-Sends $info.addr $token) 0 "zero sends while paused"

    Write-Output "== barrier after_send: hard-kill only our own daemon child =="
    $crashedPid = $info.pid
    Stop-Process -Id $crashedPid -Force
    Wait-Process -Id $crashedPid -Timeout 30 -ErrorAction SilentlyContinue

    Write-Output "== phase 2: restart with the scan loop enabled; boot parks =="
    $env:CENTRAL_SCAN_MS = "5000"
    Stop-RecordedDaemon
    $restarted = Central daemon start --fake-agent
    Assert-Equal $restarted.status "started" "daemon restarted after crash"
    $restored = Run-View $run
    Assert-Equal $restored.state.control "AwaitingResume" "run awaits explicit resume after boot"
    $restored2 = Run-View $run2
    Assert-Equal $restored2.state.control "AwaitingResume" "unrelated run parked by boot too"
    $info = Get-Content (Join-Path $stateDir "server.json") | ConvertFrom-Json
    $token = (Get-Content (Join-Path $stateDir "daemon.token")).Trim()
    Assert-Equal (Fake-Sends $info.addr $token) 0 "scan loop sends nothing for a parked run"

    Write-Output "== explicit resume: the scan loop queues + dispatches =="
    $null = Central resume --run $run
    $deadline = (Get-Date).AddSeconds(45)
    $sends = 0
    do {
        Start-Sleep -Milliseconds 500
        $view = Run-View $run
        $sends = Fake-Sends $info.addr $token
        if ((Get-Date) -gt $deadline) { break }
    } while ($sends -lt 2)
    Assert-True ($sends -ge 2) "scan dispatched both independent tasks (sends=$sends)"
    # Refresh: the loop's last view can predate the Running commits.
    $view = Wait-AttemptsRunning $run 2 45
    $running = @($view.state.attempts.PSObject.Properties | Where-Object { $_.Value.status -eq "Running" })
    Assert-True ($running.Count -ge 2) "both attempts Running after scan dispatch"
    Assert-Equal (Attempt-Count $view $taskA) 1 "task A has exactly one attempt (no duplicates)"
    Assert-Equal (Attempt-Count $view $taskB) 1 "task B has exactly one attempt (no duplicates)"
    Assert-Equal (Attempt-Count $view $taskV) 0 "verifier held: predecessors still running, nothing dispatched"
    Assert-Equal (Attempt-Count $view $taskI) 0 "integration held: verifier not reached, nothing dispatched"

    Write-Output "== barrier at verifier/integration pending: second hard-kill, restart, resume =="
    $info = Get-Content (Join-Path $stateDir "server.json") | ConvertFrom-Json
    $crashedPid2 = $info.pid
    Stop-Process -Id $crashedPid2 -Force
    Wait-Process -Id $crashedPid2 -Timeout 30 -ErrorAction SilentlyContinue
    Stop-RecordedDaemon
    $restarted2 = Central daemon start --fake-agent
    Assert-Equal $restarted2.status "started" "daemon restarted with downstream stages pending"
    $reparked = Run-View $run
    Assert-Equal $reparked.state.control "AwaitingResume" "run parks again after the second crash"
    $info = Get-Content (Join-Path $stateDir "server.json") | ConvertFrom-Json
    $token = (Get-Content (Join-Path $stateDir "daemon.token")).Trim()
    # Fresh process, fresh counter: nothing may dispatch while parked.
    Assert-Equal (Fake-Sends $info.addr $token) 0 "restart resends nothing while parked"
    $null = Central resume --run $run
    # Let the scan loop tick at least twice (5s cadence), then prove
    # stability: no duplicate sends, downstream still held. Refresh the
    # view first: a pre-sleep view can predate the Running commits.
    Start-Sleep -Seconds 12
    $steady = Wait-AttemptsRunning $run 2 45
    Assert-Equal $steady.state.control "Running" "run stays resumed after the second crash"
    Assert-Equal (Fake-Sends $info.addr $token) 0 "second resume dispatches nothing new"
    $stillRunning = @($steady.state.attempts.PSObject.Properties | Where-Object { $_.Value.status -eq "Running" })
    Assert-True ($stillRunning.Count -ge 2) "both implementation attempts still Running"
    Assert-Equal (Attempt-Count $steady $taskA) 1 "task A still exactly one attempt after two crashes"
    Assert-Equal (Attempt-Count $steady $taskB) 1 "task B still exactly one attempt after two crashes"
    Assert-Equal (Attempt-Count $steady $taskV) 0 "verifier still held after the second crash"
    Assert-Equal (Attempt-Count $steady $taskI) 0 "integration still held after the second crash"

    Write-Output "== passive reconcile: evidence stored, nothing resent =="
    $attemptId = $running[0].Name
    $reconciled = Invoke-RestMethod -Method Post `
        -Uri "http://$($info.addr)/v1/runs/$run/attempts/$attemptId/reconcile" `
        -Headers @{ Authorization = "Bearer $token" } `
        -ContentType "application/json" -Body "{}"
    Assert-Equal $reconciled.evidence.attempt_id $attemptId "reconcile returns evidence for the attempt"
    Assert-Equal (Fake-Sends $info.addr $token) 0 "reconcile resends nothing"
    $afterReconcile = Run-View $run
    Assert-Equal $afterReconcile.state.attempts.$attemptId.status "Running" "reconcile never rewrites the attempt status"

    Write-Output "== opt-in live-model smoke: off by default =="
    if ($env:CENTRAL_LIVE_SMOKE -eq "1") {
        throw "CENTRAL_LIVE_SMOKE=1 needs a configured live gateway; no live harness exists in this repo, so the smoke stays opt-in and unconfigured. Stopping instead of calling any model."
    }
    Write-Output "skip: live-model smoke is opt-in only (CENTRAL_LIVE_SMOKE unset); reboot/manual Windows acceptance is recorded separately in docs/validation/p3-workflow.md, never inferred from this process kill."

    Write-Output "== unrelated run intact after resume + shared scans =="
    $untouched = Run-View $run2
    Assert-Equal $untouched.state.control "AwaitingResume" "unrelated run never resumed by the shared scan"
    Assert-Equal (Total-Attempts $untouched) 0 "unrelated run gained no attempts"

    Write-Output "== shutdown =="
    $null = Central daemon shutdown
    $final = Invoke-Central daemon status
    $status = $final.output | ConvertFrom-Json
    Assert-Equal $status.status "not-running" "daemon stopped after shutdown"

    Write-Output "WORKFLOW RECOVERY GATE: PASS"
} finally {
    $env:CENTRAL_SCAN_MS = $null
    Pop-Location
}
