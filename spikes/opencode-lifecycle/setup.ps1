$ErrorActionPreference = 'Stop'
$vendorPath = Join-Path $PSScriptRoot 'vendor'
New-Item -ItemType Directory -Path $vendorPath -Force | Out-Null
$archivePath = Join-Path $vendorPath 'opencode.tgz'
$expected = 'xCVbrA8A/qQTAdBIKyhK5tx3/mcq18yF82Mi8phry0MJb2631nnnpxF9m6FFfWAVHyer4qFGrO7hbWXesHQ1Gg=='
Invoke-WebRequest -Uri 'https://registry.npmjs.org/opencode-windows-x64/-/opencode-windows-x64-1.19.5.tgz' -OutFile $archivePath
$digest = [Security.Cryptography.SHA512]::Create()
try {
    $actual = [Convert]::ToBase64String($digest.ComputeHash([IO.File]::ReadAllBytes($archivePath)))
} finally {
    $digest.Dispose()
}
if ($actual -ne $expected) { throw 'OpenCode package integrity mismatch' }
tar -xzf $archivePath -C $vendorPath
if ($LASTEXITCODE -ne 0) { throw 'Package extraction failed' }
& (Join-Path $vendorPath 'package/bin/opencode.exe') --version
if ($LASTEXITCODE -ne 0) { throw 'OpenCode executable check failed' }
