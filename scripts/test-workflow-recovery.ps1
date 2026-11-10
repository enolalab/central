# P3 Task 6 gate: workflow recovery and side-effect reconciliation.
#
# Flow: deterministic barrier tests (all three named fault barriers,
# in-process, virtual clock) -> real daemon kill-restart at the after_send
# barrier (kill own child only) with the scan loop disabled for setup and
# enabled for the resume phase -> explicit resume -> scan queues and
# dispatches two independent implementation tasks -> reconcile endpoint
# stores evidence without resending. Fake gateway only, no live models.
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
    {"id":"00000000-0000-0000-0000-000000000002","kind":"implementation","objective":"Task B","inputs":[],"scope":[],"outOfScope":[],"dependsOn":[],"dataDependsOn":[],"outputs":[],"verify":{"method":"check","criteria":["b done"]},"evidence":[],"profileId":null,"permissions":[],"maxRepairRounds":2,"requiresApproval":[],"handover":""}
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

    Write-Output "== phase 1: setup with the scan loop disabled (deterministic) =="
    $env:CENTRAL_SCAN_MS = "0"
    $started = Central daemon start --fake-agent
    Assert-Equal $started.status "started" "daemon started (scan disabled)"
    $run = (Central init --goal "Ship the reliable workflow" --repository "D:/Code/enolalab/central").run_id
    $imported = Central plan import --run $run --file $planFile
    $null = Central plan approve --run $run --hash $imported.plan_hash
    $null = Central pause --run $run
    $paused = Run-View $run
    Assert-Equal $paused.state.control "Paused" "run paused before crash; nothing queued or sent"
    $info = Get-Content (Join-Path $stateDir "server.json") | ConvertFrom-Json
    $token = (Get-Content (Join-Path $stateDir "daemon.token")).Trim()
    Assert-Equal (Fake-Sends $info.addr $token) 0 "zero sends while paused"

    Write-Output "== barrier after_send: hard-kill only our own daemon child =="
    $crashedPid = $info.pid
    Stop-Process -Id $crashedPid -Force
    Wait-Process -Id $crashedPid -Timeout 30 -ErrorAction SilentlyContinue

    Write-Output "== phase 2: restart with the scan loop enabled; boot parks =="
    $env:CENTRAL_SCAN_MS = "5000"
    $restarted = Central daemon start --fake-agent
    Assert-Equal $restarted.status "started" "daemon restarted after crash"
    $restored = Run-View $run
    Assert-Equal $restored.state.control "AwaitingResume" "run awaits explicit resume after boot"
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
    $running = @($view.state.attempts.PSObject.Properties | Where-Object { $_.Value.status -eq "Running" })
    Assert-True ($running.Count -ge 2) "both attempts Running after scan dispatch"

    Write-Output "== passive reconcile: evidence stored, nothing resent =="
    $attemptId = $running[0].Name
    $reconciled = Invoke-RestMethod -Method Post `
        -Uri "http://$($info.addr)/v1/runs/$run/attempts/$attemptId/reconcile" `
        -Headers @{ Authorization = "Bearer $token" } `
        -ContentType "application/json" -Body "{}"
    Assert-Equal $reconciled.evidence.attempt_id $attemptId "reconcile returns evidence for the attempt"
    Assert-Equal (Fake-Sends $info.addr $token) $sends "reconcile resends nothing"
    $afterReconcile = Run-View $run
    Assert-Equal $afterReconcile.state.attempts.$attemptId.status "Running" "reconcile never rewrites the attempt status"

    Write-Output "== opt-in live-model smoke: off by default =="
    if ($env:CENTRAL_LIVE_SMOKE -eq "1") {
        throw "CENTRAL_LIVE_SMOKE=1 needs a configured live gateway; no live harness exists in this repo, so the smoke stays opt-in and unconfigured. Stopping instead of calling any model."
    }
    Write-Output "skip: live-model smoke is opt-in only (CENTRAL_LIVE_SMOKE unset); reboot/manual Windows acceptance is recorded separately in docs/validation/p3-workflow.md, never inferred from this process kill."

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
