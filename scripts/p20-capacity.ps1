param(
    [ValidateSet('Backlog', 'Mixed')]
    [string]$Scenario = 'Backlog',
    [ValidateRange(1, 1024)]
    [int]$Clients = 1000,
    [ValidateRange(1, 4)]
    [int]$Workspaces = 4,
    [ValidateRange(2, 2)]
    [int]$Instances = 2,
    [string]$OutputDirectory = 'docs/evidence/p20-capacity'
)

$repoRoot = Split-Path -Parent $PSScriptRoot
$perWorkspace = [Math]::Ceiling($Clients / $Workspaces)
if ($perWorkspace -gt 256) {
    throw "Clients / Workspaces 不能超过每工作区队列上限 256。"
}

if ([System.IO.Path]::IsPathRooted($OutputDirectory)) {
    $outputPath = [System.IO.Path]::GetFullPath($OutputDirectory)
} else {
    $outputPath = [System.IO.Path]::GetFullPath((Join-Path $repoRoot $OutputDirectory))
}
New-Item -ItemType Directory -Force -Path $outputPath | Out-Null

$mavenArgs = @(
    '-B', '-ntp', '-pl', 'bootstrap', '-am',
    '-Dtest=P20CapacityTest',
    '-Dsurefire.failIfNoSpecifiedTests=false',
    '-Dp20.capacity.enabled=true',
    "-Dp20.capacity.scenario=$Scenario",
    "-Dp20.capacity.clients=$Clients",
    "-Dp20.capacity.workspaces=$Workspaces",
    "-Dp20.capacity.instances=$Instances",
    "-Dp20.capacity.output-dir=$outputPath",
    'test'
)

Push-Location $repoRoot
try {
    & .\mvnw.cmd @mavenArgs
    if ($LASTEXITCODE -ne 0) {
        throw "P20 $Scenario 容量检查失败，Maven exit code: $LASTEXITCODE"
    }
} finally {
    Pop-Location
}
