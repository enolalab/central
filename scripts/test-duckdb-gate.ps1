#Requires -Version 7.0
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
if (Test-Path (Join-Path $root '.toolchain\env.ps1')) {
    . (Join-Path $root '.toolchain\env.ps1')
    # This machine has no MSVC: plain channel "1.99.1" resolves to the MSVC
    # host, so select the installed GNU toolchain (exactly 1.99.1) instead.
    $env:RUSTUP_TOOLCHAIN = 'stable-x86_64-pc-windows-gnu'
}
Set-Location $root

# Hardware / filesystem evidence for the report.
$cpu = (Get-CimInstance Win32_Processor | Select-Object -First 1).Name
$cores = $env:NUMBER_OF_PROCESSORS
$drive = Get-PSDrive -Name ([IO.Path]::GetPathRoot($root).Substring(0,1)) -ErrorAction SilentlyContinue
$fs = if ($null -ne $drive) { 'local disk (see docs/validation/duckdb-gate.md)' } else { 'unknown' }
$env:GATE_HARDWARE = "$cpu; $cores logical cores; fs=$fs; target x86_64-pc-windows-gnu"

cargo build --release -p duckdb-runtime-gate
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$exe = Join-Path $root 'target\release\duckdb-runtime-gate.exe'
$tmpJson = Join-Path ([IO.Path]::GetTempPath()) "duckdb-gate-report-$PID.json"
& $exe 2> (Join-Path ([IO.Path]::GetTempPath()) "duckdb-gate-$PID.log") | Tee-Object -FilePath $tmpJson
$code = $LASTEXITCODE
Write-Host "gate exit code: $code"
if ($code -ne 0) {
    Write-Error "DuckDB gate FAILED (exit $code). See $tmpJson and docs/validation/duckdb-gate.md"
    exit $code
}
$report = Get-Content $tmpJson -Raw | ConvertFrom-Json
if (-not $report.gate_pass) {
    Write-Error 'DuckDB gate FAILED: gate_pass is false'
    exit 1
}
Write-Host 'DuckDB gate PASSED'
exit 0
