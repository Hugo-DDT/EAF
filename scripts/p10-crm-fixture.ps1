param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('start', 'status', 'stop')]
    [string] $Action
)

$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$fixture = Join-Path $root 'demo\p10\crm-fixture.mjs'
$pidFile = Join-Path $env:TEMP 'eaf-p10-crm-fixture.pid'
$stdoutFile = Join-Path $env:TEMP 'eaf-p10-crm-fixture.stdout.log'
$stderrFile = Join-Path $env:TEMP 'eaf-p10-crm-fixture.stderr.log'

function Get-FixtureProcess {
    if (-not (Test-Path -LiteralPath $pidFile)) { return $null }
    $processId = [int](Get-Content -LiteralPath $pidFile -Raw).Trim()
    $candidate = Get-CimInstance Win32_Process -Filter "ProcessId = $processId" -ErrorAction SilentlyContinue
    if ($null -eq $candidate -or $candidate.CommandLine -notlike "*crm-fixture.mjs*") { return $null }
    return Get-Process -Id $processId -ErrorAction SilentlyContinue
}

switch ($Action) {
    'start' {
        if ([string]::IsNullOrWhiteSpace($env:EAF_P10_FIXTURE_TOKEN)) {
            throw 'Set EAF_P10_FIXTURE_TOKEN in the process environment before starting the fixture.'
        }
        if (Get-FixtureProcess) { throw 'The CRM fixture is already running; use status or stop.' }
        $node = Get-Command node -ErrorAction Stop
        $process = Start-Process -FilePath $node.Source -ArgumentList $fixture -PassThru -WindowStyle Hidden `
            -RedirectStandardOutput $stdoutFile -RedirectStandardError $stderrFile
        Set-Content -LiteralPath $pidFile -Value $process.Id -NoNewline
        Start-Sleep -Milliseconds 500
        if ($process.HasExited) {
            Remove-Item -LiteralPath $pidFile -ErrorAction SilentlyContinue
            Get-Content -LiteralPath $stderrFile -ErrorAction SilentlyContinue
            throw 'The CRM fixture exited during startup.'
        }
        Write-Output 'CRM fixture started on 127.0.0.1:19093.'
    }
    'status' {
        $process = Get-FixtureProcess
        if ($null -eq $process) {
            if (Test-Path -LiteralPath $pidFile) { Remove-Item -LiteralPath $pidFile }
            Write-Output 'CRM fixture is stopped.'
        } else {
            Write-Output 'CRM fixture is running on 127.0.0.1:19093.'
        }
    }
    'stop' {
        $process = Get-FixtureProcess
        if ($null -eq $process) {
            if (Test-Path -LiteralPath $pidFile) { Remove-Item -LiteralPath $pidFile }
            Write-Output 'CRM fixture is already stopped.'
        } else {
            Stop-Process -Id $process.Id
            Remove-Item -LiteralPath $pidFile
            Write-Output 'CRM fixture stopped. Its in-memory synthetic records were discarded.'
        }
    }
}
