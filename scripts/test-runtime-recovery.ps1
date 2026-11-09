# End-to-end restart-barrier gate for P1 Task 4.
# Flow: create -> approve -> crash -> reopen -> zero dispatch ->
# explicit resume -> fake workflow continues. No duplicate events, and
# an independent run must survive untouched. No live-model calls: the
# daemon runs the fake agent from a test-support build.
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
    Write-Output "== building daemon + CLI (test-support) =="
    cargo build -p central-server --features test-support
    if ($LASTEXITCODE -ne 0) { throw "cargo build (server) failed" }
    cargo build -p central-cli
    if ($LASTEXITCODE -ne 0) { throw "cargo build (cli) failed" }
    $central = Join-Path $root "target\debug\central.exe"

    $stateDir = Join-Path ([System.IO.Path]::GetTempPath()) ("central-recovery-" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Force $stateDir | Out-Null
    $planFile = Join-Path $stateDir "plan.json"
    @'
{
  "version": 1,
  "goal": "Build the durable runtime",
  "tasks": [
    {"id":"00000000-0000-0000-0000-000000000001","kind":"implementation","objective":"Task A","inputs":[],"scope":[],"outOfScope":[],"dependsOn":[],"dataDependsOn":[],"outputs":[],"verify":{"method":"check","criteria":["a done"]},"evidence":[],"profileId":null,"permissions":[],"maxRepairRounds":2,"requiresApproval":[],"handover":""},
    {"id":"00000000-0000-0000-0000-000000000002","kind":"implementation","objective":"Task B","inputs":[],"scope":[],"outOfScope":[],"dependsOn":[],"dataDependsOn":[],"outputs":[],"verify":{"method":"check","criteria":["b done"]},"evidence":[],"profileId":null,"permissions":[],"maxRepairRounds":2,"requiresApproval":[],"handover":""},
    {"id":"00000000-0000-0000-0000-000000000003","kind":"verification","objective":"Task C","inputs":[],"scope":[],"outOfScope":[],"dependsOn":["00000000-0000-0000-0000-000000000001"],"dataDependsOn":["00000000-0000-0000-0000-000000000001"],"outputs":[],"verify":{"method":"check","criteria":["c done"]},"evidence":[],"profileId":null,"permissions":[],"maxRepairRounds":2,"requiresApproval":[],"handover":""}
  ]
}
'@ | Set-Content -Encoding UTF8 $planFile

    # CLI calls go through Start-Process + WaitForExit() on the CLI
    # process alone, with output redirected to real files. Anything
    # stronger blocks forever on the daemon: a captured pipe never
    # reaches EOF while the detached daemon holds inherited handles,
    # and Start-Process -Wait waits for the whole process tree.
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
    function CentralRaw {
        param([Parameter(ValueFromRemainingArguments = $true)][string[]]$rest)
        return (Invoke-Central @rest).code
    }
    function Run-View($run) { Central status --run $run }

    Write-Output "== daemon start =="
    $started = Central daemon start --fake-agent
    Assert-Equal $started.status "started" "daemon started"

    Write-Output "== second daemon start is AlreadyRunning (exit 3) =="
    $secondCode = CentralRaw daemon start --fake-agent
    Assert-Equal $secondCode 3 "second start exit code is AlreadyRunning"

    Write-Output "== create two runs, import + approve plan on run 1 =="
    $run1 = (Central init --goal "Build the durable runtime" --repository "D:/Code/enolalab/central").run_id
    $run2 = (Central init --goal "Independent run" --repository "D:/Code/enolalab/central").run_id
    Assert-True ($run1 -and $run2 -and $run1 -ne $run2) "two independent runs created"
    $imported = Central plan import --run $run1 --file $planFile
    $null = Central plan approve --run $run1 --hash $imported.plan_hash
    $before = Run-View $run1
    Assert-Equal $before.state.control "Running" "run 1 running before crash"
    $eventsBefore = $before.events.Count
    Assert-Equal $eventsBefore 3 "run 1 has create/propose/approve events"

    $info = Get-Content (Join-Path $stateDir "server.json") | ConvertFrom-Json
    $token = (Get-Content (Join-Path $stateDir "daemon.token")).Trim()

    Write-Output "== crash (hard kill, no shutdown) =="
    Stop-Process -Id $info.pid -Force
    Wait-Process -Id $info.pid -Timeout 30 -ErrorAction SilentlyContinue

    Write-Output "== reopen: recovery parks runs, zero dispatch =="
    $restarted = Central daemon start --fake-agent
    Assert-Equal $restarted.status "started" "daemon restarted after crash"
    $restored = Run-View $run1
    Assert-Equal $restored.state.control "AwaitingResume" "run 1 awaits explicit resume"
    Assert-Equal $restored.events.Count $eventsBefore "no duplicate events after reopen"
    $info = Get-Content (Join-Path $stateDir "server.json") | ConvertFrom-Json
    $fake = Invoke-RestMethod -Uri "http://$($info.addr)/v1/test/fake-sends" -Headers @{ Authorization = "Bearer $token" }
    Assert-Equal $fake.sends 0 "zero external sends before explicit resume"
    Assert-Equal $fake.cancels 0 "zero cancels before explicit resume"
    $dispatch = Central start --run $run1
    Assert-Equal $dispatch.dispatched 0 "dispatch gate stays closed while awaiting resume"
    $fake = Invoke-RestMethod -Uri "http://$($info.addr)/v1/test/fake-sends" -Headers @{ Authorization = "Bearer $token" }
    Assert-Equal $fake.sends 0 "still zero sends after gated dispatch attempt"

    $other = Run-View $run2
    Assert-Equal $other.state.goal "Independent run" "independent run intact after crash"
    Assert-Equal $other.state.control "AwaitingResume" "independent run also parked, not lost"

    Write-Output "== explicit resume: fake workflow continues =="
    $null = Central resume --run $run1
    $resumed = Run-View $run1
    Assert-Equal $resumed.state.control "Running" "run 1 running after explicit resume"
    $null = Central pause --run $run1
    $null = Central resume --run $run1
    $continued = Run-View $run1
    Assert-Equal $continued.events.Count ($eventsBefore + 3) "resume/pause/resume appended exactly 3 events, none duplicated"
    $dispatch = Central start --run $run1
    Assert-Equal $dispatch.still_pending 0 "no pending intents leak after resume"
    $otherAfter = Run-View $run2
    Assert-Equal $otherAfter.state.control "AwaitingResume" "independent run untouched by run 1 resume"
    Assert-Equal $otherAfter.events.Count 1 "independent run events untouched"

    Write-Output "== UI not built is a clear 503, daemon stays up =="
    try {
        Invoke-WebRequest -Uri "http://$($info.addr)/" -TimeoutSec 5 | Out-Null
        throw "expected 503 for /"
    } catch {
        Assert-Equal ([int]$_.Exception.Response.StatusCode) 503 "UI path returns 503"
    }
    $health = Invoke-RestMethod -Uri "http://$($info.addr)/healthz" -TimeoutSec 5
    Assert-Equal $health.status "ok" "daemon healthy after 503"

    Write-Output "== shutdown =="
    $null = Central daemon shutdown
    $final = Invoke-Central daemon status
    $status = $final.output | ConvertFrom-Json
    Assert-Equal $status.status "not-running" "daemon stopped after shutdown"

    Write-Output "RUNTIME RECOVERY GATE: PASS"
} finally {
    Pop-Location
}
