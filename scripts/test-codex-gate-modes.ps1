# Regression: the one-task gate is fake-only.
# -RealModel must fail with an actionable message, never a silent fake pass.
# The live procedure (turn budget, stop rule, usage capture) lives as manual
# host steps in docs/validation/p2-codex.md.
param()
$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
. (Join-Path $root ".toolchain\env.ps1")

Push-Location $root
try {
    $gate = Join-Path $root "scripts\test-codex-single-task.ps1"
    $proc = Start-Process -FilePath "pwsh" `
        -ArgumentList @("-NoProfile", "-File", $gate, "-RealModel") `
        -NoNewWindow -Wait -PassThru `
        -RedirectStandardOutput (Join-Path ([System.IO.Path]::GetTempPath()) "gate-modes-realmodel.out") `
        -RedirectStandardError (Join-Path ([System.IO.Path]::GetTempPath()) "gate-modes-realmodel.err")
    $text = @(
        Get-Content -Raw -Path (Join-Path ([System.IO.Path]::GetTempPath()) "gate-modes-realmodel.out") -ErrorAction SilentlyContinue
        Get-Content -Raw -Path (Join-Path ([System.IO.Path]::GetTempPath()) "gate-modes-realmodel.err") -ErrorAction SilentlyContinue
    ) -join "`n"
    if ($proc.ExitCode -eq 0) {
        throw "ASSERT FAILED: -RealModel exited 0 (silent fake pass); the fake-only gate must reject it"
    }
    if ($text -notmatch "fake-only" -or $text -notmatch "p2-codex\.md") {
        throw "ASSERT FAILED: -RealModel rejection is not actionable (exit $($proc.ExitCode)): $text"
    }
    Write-Output "ok: -RealModel rejected with an actionable message (exit $($proc.ExitCode))"
}
finally {
    Pop-Location
}
