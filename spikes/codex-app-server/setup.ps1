$ErrorActionPreference = 'Stop'
$vendorPath = Join-Path $PSScriptRoot 'vendor'
$wsPath = Join-Path $vendorPath 'ws'
New-Item -ItemType Directory -Path $wsPath -Force | Out-Null
$archivePath = Join-Path $vendorPath 'ws.tgz'
Invoke-WebRequest -Uri 'https://registry.npmjs.org/ws/-/ws-8.22.2.tgz' -OutFile $archivePath
$sha = [Security.Cryptography.SHA512]::Create()
try {
    $actual = [Convert]::ToBase64String($sha.ComputeHash([IO.File]::ReadAllBytes($archivePath)))
} finally {
    $sha.Dispose()
}
if ($actual -ne '/rEZ2ejown2N5gxVTHUu9bVVC7ttu0/Tgzf5hbTOixR3Fs7mbbXgn2DbdbcKgJOmpQMtrIUWivtYuVMnp/W/yA==') {
    throw 'ws package integrity mismatch'
}
tar -xzf $archivePath -C $wsPath
if ($LASTEXITCODE -ne 0) { throw 'ws extraction failed' }
codex app-server generate-json-schema --experimental --out (Join-Path $PSScriptRoot 'schema')
if ($LASTEXITCODE -ne 0) { throw 'Protocol schema generation failed' }
