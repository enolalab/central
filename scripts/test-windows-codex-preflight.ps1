# Native Windows sandbox diagnostic. No model turns, credentials, or saved
# configuration changes. Run with the same Windows user as central-server.
param(
    [string]$CodexBinary = 'codex',
    [ValidateSet('current', 'elevated', 'unelevated')]
    [string]$WindowsSandbox = 'current',
    [ValidateRange(1, 120)]
    [int]$TimeoutSeconds = 45,
    [switch]$ChildProbe,
    [string]$Fixture
)

$ErrorActionPreference = 'Stop'
if (-not $IsWindows) { throw 'This diagnostic requires native Windows and PowerShell 7.' }

if ($ChildProbe) {
    if (-not $Fixture -or -not (Test-Path -LiteralPath $Fixture -PathType Container)) {
        throw 'Missing fixture directory.'
    }
    Set-Location -LiteralPath $Fixture
    $cliArgs = @()
    if ($WindowsSandbox -ne 'current') {
        $cliArgs += @('-c', ('windows.sandbox="' + $WindowsSandbox + '"'))
    }
    # Test the native command sandbox directly, without asking a model to
    # interpret its own permissions. Never use full-access or disable sandbox.
    $cliArgs += @('-C', $Fixture, 'sandbox', '--permission-profile', ':workspace',
        '--', 'cmd.exe', '/d', '/c', 'echo CENTRAL_SANDBOX_OK>inside.txt')
    & $CodexBinary @cliArgs
    exit $LASTEXITCODE
}

$resolved = (Get-Command $CodexBinary -ErrorAction Stop).Source
$version = (& $CodexBinary --version | Out-String).Trim()
if ($LASTEXITCODE -ne 0) { throw 'Cannot read Codex CLI version.' }
$root = Join-Path ([IO.Path]::GetTempPath()) ('central-sandbox-preflight-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $root | Out-Null
# A worker must be able to modify files already present in its worktree.
# Creating a fresh file alone does not prove this (the two can have different ACLs).
Set-Content -LiteralPath (Join-Path $root 'inside.txt') -Value 'CENTRAL_BEFORE' -Encoding utf8NoBOM
$process = $null
try {
    $info = [Diagnostics.ProcessStartInfo]::new()
    $info.FileName = (Get-Command pwsh -ErrorAction Stop).Source
    $info.UseShellExecute = $false
    $info.RedirectStandardOutput = $true
    $info.RedirectStandardError = $true
    foreach ($arg in @('-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', $PSCommandPath,
        '-ChildProbe', '-Fixture', $root, '-CodexBinary', $resolved,
        '-WindowsSandbox', $WindowsSandbox)) {
        $info.ArgumentList.Add($arg)
    }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $info
    if (-not $process.Start()) { throw 'Cannot start diagnostic child.' }
    $stdout = $process.StandardOutput.ReadToEndAsync()
    $stderr = $process.StandardError.ReadToEndAsync()
    $timedOut = -not $process.WaitForExit($TimeoutSeconds * 1000)
    if ($timedOut) { $process.Kill($true); $process.WaitForExit() }
    $inside = Join-Path $root 'inside.txt'
    $wroteMarker = (Test-Path -LiteralPath $inside) -and
        ((Get-Content -LiteralPath $inside -Raw).Trim() -eq 'CENTRAL_SANDBOX_OK')
    $passed = -not $timedOut -and $process.ExitCode -eq 0 -and $wroteMarker
    [ordered]@{
        kind = 'windows-native-sandbox-write-preflight'
        cli_version = $version
        executable = $resolved
        windows_sandbox_override = $WindowsSandbox
        timed_out = $timedOut
        exit_code = $process.ExitCode
        workspace_write_proven = $wroteMarker
        passed = $passed
        model_turns = 0
        stdout = $stdout.GetAwaiter().GetResult()
        stderr = $stderr.GetAwaiter().GetResult()
        scope = 'Native command startup and overwrite of an existing in-workspace file only; not live workflow acceptance.'
    } | ConvertTo-Json -Depth 4
    if (-not $passed) { exit 1 }
}
finally {
    if ($process) {
        if (-not $process.HasExited) { $process.Kill($true); $process.WaitForExit() }
        $process.Dispose()
    }
    Remove-Item -LiteralPath $root -Recurse -Force
}
