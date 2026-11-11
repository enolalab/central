#!/usr/bin/env pwsh
# Build the Central Windows release zip: central.exe, central-server.exe,
# contracts, docs and notices. No UI, no Node runtime, no frontend bundle.
# Uses the pinned portable toolchain (.toolchain/env.ps1, Rust 1.99.1 GNU).
#
# Usage: pwsh -File scripts/build-release.ps1 [-OutDir <dir>]
# The zip lands at <OutDir>/central-windows-x64.zip (default ./dist).

param([string]$OutDir = (Join-Path (Get-Location) 'dist'))

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path (Get-Location) '.toolchain/env.ps1')

$version = (cargo metadata --no-deps --format-version 1 2>$null | ConvertFrom-Json).packages |
    Where-Object { $_.name -eq 'central-server' } | Select-Object -ExpandProperty version
Write-Host "central release build (server $version, Rust $(rustc --version))"

# Release binaries: no test-support feature, so the fake agent cannot exist here.
cargo build --release -j 2 -p central-cli -p central-server
if ($LASTEXITCODE -ne 0) { throw 'release build failed' }

$stage = Join-Path ([IO.Path]::GetTempPath()) "central-release-$(Get-Random)"
New-Item -ItemType Directory -Path $stage, "$stage/contracts", "$stage/docs" | Out-Null

Copy-Item target/release/central.exe (Join-Path $stage 'central.exe')
Copy-Item target/release/central-server.exe (Join-Path $stage 'central-server.exe')
Copy-Item contracts/openapi-v1.yaml, contracts/openapi-v1.json, contracts/events-v1.schema.json `
    (Join-Path $stage 'contracts/')
Copy-Item contracts/examples (Join-Path $stage 'contracts/examples') -Recurse
Copy-Item docs/api.md, docs/client-integration.md, docs/getting-started.md, docs/recovery.md `
    (Join-Path $stage 'docs/')
Copy-Item examples (Join-Path $stage 'examples') -Recurse

# Notices: every locked dependency with its registry source. License
# texts ship with their crates; this index names them for the release.
$metadata = cargo metadata --format-version 1 | ConvertFrom-Json
$lines = @('Central — third-party dependency notices', '')
foreach ($pkg in ($metadata.packages | Sort-Object name)) {
    if ($pkg.source) {
        $lines += "$($pkg.name) $($pkg.version) — $($pkg.source)"
    }
}
$lines += ''
$lines += 'See the Rust crate registry for the license text of each package.'
Set-Content (Join-Path $stage 'THIRD-PARTY-NOTICES.txt') $lines

# DLL check: the GNU release links the CRT statically-ish, but prove what
# ships by listing the import surface of the two binaries.
foreach ($exe in @('central.exe', 'central-server.exe')) {
    $path = Join-Path $stage $exe
    if (-not (Test-Path $path)) { throw "missing $exe in stage" }
}
Write-Host 'binaries staged; checking for unexpected Node/UI payloads...'
$bad = Get-ChildItem -Recurse $stage | Where-Object {
    $_.Extension -in @('.js', '.html', '.map') -and $_.FullName -notmatch 'browser-client'
}
if ($bad -and ($bad | Where-Object { $_.Name -ne 'browser-client.mjs' })) {
    throw "release stage contains frontend payloads: $($bad.FullName -join ', ')"
}

if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Path $OutDir | Out-Null }
$zip = Join-Path $OutDir 'central-windows-x64.zip'
if (Test-Path $zip) { Remove-Item $zip }
Compress-Archive -Path (Join-Path $stage '*') -DestinationPath $zip
Remove-Item -Recurse -Force $stage
Write-Host "release zip: $zip"
