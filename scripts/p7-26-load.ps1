[CmdletBinding()]
param(
    [ValidateRange(1, 1000)]
    [int] $SampleCount = 120,

    [ValidateRange(1, 64)]
    [int] $Concurrency = 12,

    [string] $OutputPath
)

# Windows PowerShell 将 Maven/JVM 写入 stderr 的普通告警包装成 ErrorRecord；继续收集输出并最终以 Maven exit code 判定。
$ErrorActionPreference = 'Continue'
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path

if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $OutputPath = Join-Path $repositoryRoot "docs/evidence/p7-26/load-$stamp.log"
} elseif (-not [System.IO.Path]::IsPathRooted($OutputPath)) {
    $OutputPath = Join-Path $repositoryRoot $OutputPath
}

$logDirectory = Split-Path -Parent $OutputPath
New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null

# 只启动 Testcontainers 中的本地确定性 HTTP/数据库用例，不访问 Provider 或业务系统。
$mavenArguments = @(
    '-B',
    '-ntp',
    '-pl', 'bootstrap',
    '-am',
    "-Dp7.load.samples=$SampleCount",
    "-Dp7.load.concurrency=$Concurrency",
    '-Dtest=P7LoadMeasurementTest',
    '-Dsurefire.failIfNoSpecifiedTests=false',
    'test'
)

Push-Location $repositoryRoot
try {
    & .\mvnw.cmd @mavenArguments 2>&1 | Tee-Object -FilePath $OutputPath
    $mavenExitCode = $LASTEXITCODE
} finally {
    Pop-Location
}

Write-Output "P7-26 local deterministic load log: $OutputPath"
if ($mavenExitCode -ne 0) {
    throw "P7-26 load run failed with Maven exit code $mavenExitCode. See $OutputPath"
}
