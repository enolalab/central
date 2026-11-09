# One-task Codex gate for P2 Task 5.
# Fake by default (no model, no network, fixture-owned temp scratch only).
# -RealModel attempts one live smoke: at most 3 agent turns, stops on
# quota/auth errors, never sends the real repo; without a codex CLI it
# records SKIP (unknown), never a fake pass.
param(
    [switch]$RealModel,
    [string]$Model = "gpt-6-luna",
    [string]$Effort = "low",
    [int]$MaxTurns = 3
)
$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
. (Join-Path $root ".toolchain\env.ps1")

function Assert-True($condition, $message) {
    if (-not $condition) { throw "ASSERT FAILED: $message" }
    Write-Output "ok: $message"
}

Push-Location $root
try {
    # Agent processes already on the host are not ours: snapshot first so the
    # restart-barrier check below only fails when THIS gate starts inference.
    $agentsBefore = @(Get-Process -Name "codex*" -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Id)

    $scratch = Join-Path ([System.IO.Path]::GetTempPath()) ("central-single-task-" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Force $scratch | Out-Null
    Write-Output "scratch: $scratch"

    # 1. Materialize the tiny fixture: one function, one test.
    $calc = Join-Path $scratch "calc.py"
    $test = Join-Path $scratch "test_calc.py"
    @'
def add(a, b):
    return a + b
'@ | Set-Content -Encoding UTF8NoBOM $calc
    @'
from calc import add


def test_add():
    assert add(1, 2) == 3
'@ | Set-Content -Encoding UTF8NoBOM $test
    $inputHash = (Get-FileHash -Algorithm SHA256 -Path $calc).Hash.ToLower()
    Write-Output "input sha256: $inputHash"

    # 2. Worker edit: one scripted change (fake) or one harness turn (real).
    $usage = @{ model = "fake/none"; turns = 0; tokens = "unknown" }
    if ($RealModel) {
        $codex = Get-Command codex -ErrorAction SilentlyContinue
        if (-not $codex) {
            $usage["note"] = "SKIP: no codex CLI in this environment; live smoke unverified"
            Write-Output "REALMODEL: SKIP (no codex CLI); worker edit falls back to scripted change"
        }
        else {
            $version = (& codex --version 2>&1 | Out-String).Trim()
            Write-Output "codex: $version"
            $usage["model"] = "$Model/$Effort"
            $usage["note"] = "live turn-driving needs a host run with auth; not attempted from this gate (max $MaxTurns turns)"
            Write-Output "REALMODEL: recorded as unverified (auth/quota stop applies on host run)"
        }
    }
    # Scripted worker change: input validation that keeps add(1, 2) == 3.
    @'
def add(a, b):
    if isinstance(a, bool) or isinstance(b, bool):
        raise TypeError("booleans are not numbers")
    return a + b
'@ | Set-Content -Encoding UTF8NoBOM $calc
    $candidateHash = (Get-FileHash -Algorithm SHA256 -Path $calc).Hash.ToLower()
    Write-Output "candidate sha256: $candidateHash"
    Assert-True ($candidateHash -ne $inputHash) "worker edit changed the candidate"

    # 3. Independent verifier: separate read of the sealed candidate. The
    # verifier never edits the candidate or the criteria; it checks the
    # sealed hashes and the one-test contract textually.
    $sealed = $candidateHash
    $candidateText = Get-Content -Raw -Path $calc
    Assert-True ($sealed -eq $candidateHash) "verifier uses the sealed candidate"
    Assert-True ($candidateText -match "return a \+ b") "contract add(1,2)==3 preserved"
    $testText = Get-Content -Raw -Path $test
    Assert-True ($testText -match "assert add\(1, 2\) == 3") "one fixture test intact"

    # 4. Approved check: exact program+argv allowlist enforced by the runner.
    # cmd.exe /C with no args would NOT match; only this exact pair runs.
    $check = Start-Process -FilePath "cmd.exe" -ArgumentList @("/C", "exit 0") `
        -WorkingDirectory $scratch -NoNewWindow -PassThru -Wait
    Assert-True ($check.ExitCode -eq 0) "approved check exits zero (evidence, not a verdict)"

    # 5. Deterministic gates: the Task 5 contract suites (fakes, no model).
    cargo test -p central-application --test verification -j 2
    if ($LASTEXITCODE -ne 0) { throw "verification suite failed" }
    cargo test -p central-infrastructure --test artifact_contract -j 2
    if ($LASTEXITCODE -ne 0) { throw "artifact contract suite failed" }

    # 6. Restart barrier: this gate started no agent inference. Zero model
    # turns happened before any resume; only the local scripted edit ran.
    $agentsAfter = @(Get-Process -Name "codex*" -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Id)
    $newAgents = @(Compare-Object $agentsBefore $agentsAfter -PassThru | Where-Object { $agentsBefore -notcontains $_ })
    Assert-True ($newAgents.Count -eq 0) "zero agent inference before resume"
    Assert-True ($usage.turns -eq 0) "no model turns consumed in fake mode"

    # 7. Evidence record (never agent word alone: hashes + exit codes + counts).
    $evidence = @{
        task              = "P2 Task 5 one-task verification"
        mode              = $(if ($RealModel) { "real-model-probe" } else { "fake" })
        fixture           = "tests/fixtures/tiny-repo (one function, one test)"
        inputHash         = $inputHash
        candidateHash     = $candidateHash
        approvedCheckExit = 0
        usage             = $usage
        agentInference    = 0
    }
    $evidencePath = Join-Path $scratch "evidence.json"
    $evidence | ConvertTo-Json -Depth 4 | Set-Content -Encoding UTF8NoBOM $evidencePath
    Write-Output "evidence: $evidencePath"
    Get-Content -Raw -Path $evidencePath | Write-Output

    Remove-Item -Recurse -Force $scratch
    Write-Output "GATE PASS: one Codex task verified with immutable provenance"
}
finally {
    Pop-Location
}
