[CmdletBinding()]
param(
    [string] $EvidenceDirectory = 'docs/evidence/p7-27',

    # 仅用于全量 verify 已在同一源码状态通过后的复用；默认执行完整验证。
    [switch] $SkipVerify
)

# Windows PowerShell 会把原生工具写到 stderr 的告警包装成 ErrorRecord；最终以进程退出码与扫描报告判定。
$ErrorActionPreference = 'Continue'
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
if (-not [System.IO.Path]::IsPathRooted($EvidenceDirectory)) {
    $EvidenceDirectory = Join-Path $repositoryRoot $EvidenceDirectory
}
New-Item -ItemType Directory -Path $EvidenceDirectory -Force | Out-Null

$scannerVersion = '2.6.0'
$scannerDirectory = Join-Path $env:TEMP "eaf-p7-27-osv-v$scannerVersion"
$scannerPath = Join-Path $scannerDirectory 'osv-scanner_windows_amd64.exe'
$checksumPath = Join-Path $scannerDirectory 'osv-scanner_SHA256SUMS'
$sbomPath = Join-Path $EvidenceDirectory 'bom.json'

function Get-SourceSnapshotDigest {
    param(
        [Parameter(Mandatory = $true)][string] $RootPath,
        [Parameter(Mandatory = $true)][string] $EvidencePath
    )

    $root = [System.IO.Path]::GetFullPath($RootPath).TrimEnd('\', '/')
    $rootPrefix = $root + [System.IO.Path]::DirectorySeparatorChar
    $evidence = [System.IO.Path]::GetFullPath($EvidencePath).TrimEnd('\', '/')
    $excludedDirectories = @('.git', '.idea', 'target', 'node_modules')
    $manifestLines = [System.Collections.Generic.List[string]]::new()
    [long] $totalBytes = 0

    # 源码指纹覆盖仓库文件内容，排除凭据、构建输出、IDE 状态和已生成的证据目录。
    foreach ($file in Get-ChildItem -LiteralPath $root -File -Force -Recurse) {
        if (($file.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            continue
        }

        $fullPath = [System.IO.Path]::GetFullPath($file.FullName)
        if ($fullPath.StartsWith($evidence + [System.IO.Path]::DirectorySeparatorChar, [System.StringComparison]::OrdinalIgnoreCase)) {
            continue
        }

        $relativePath = $fullPath.Substring($rootPrefix.Length).Replace('\', '/')
        $segments = $relativePath.Split('/')
        if ($segments | Where-Object { $excludedDirectories -contains $_ }) {
            continue
        }
        if ($relativePath.StartsWith('docs/evidence/', [System.StringComparison]::OrdinalIgnoreCase)) {
            continue
        }
        if (($file.Name -eq '.env' -or $file.Name.StartsWith('.env.', [System.StringComparison]::OrdinalIgnoreCase)) -and $file.Name -ne '.env.example') {
            continue
        }

        $contentHash = (Get-FileHash -LiteralPath $fullPath -Algorithm SHA256).Hash.ToLowerInvariant()
        $manifestLines.Add("$relativePath`t$($file.Length)`t$contentHash")
        $totalBytes += $file.Length
    }

    $sortedLines = $manifestLines.ToArray()
    [System.Array]::Sort($sortedLines, [System.StringComparer]::Ordinal)
    $canonicalManifest = [System.String]::Join("`n", $sortedLines) + "`n"
    $digestBytes = [System.Text.Encoding]::UTF8.GetBytes($canonicalManifest)
    $sha256 = [System.Security.Cryptography.SHA256]::Create()
    try {
        $treeDigest = [System.BitConverter]::ToString($sha256.ComputeHash($digestBytes)).Replace('-', '').ToLowerInvariant()
    } finally {
        $sha256.Dispose()
    }
    return [pscustomobject]@{
        sha256 = $treeDigest
        fileCount = $sortedLines.Length
        byteCount = $totalBytes
        manifestText = $canonicalManifest
    }
}

function Get-RepositoryRelativePath {
    param([Parameter(Mandatory = $true)][string] $Path)

    $root = [System.IO.Path]::GetFullPath($repositoryRoot).TrimEnd('\', '/')
    $rootPrefix = $root + [System.IO.Path]::DirectorySeparatorChar
    $fullPath = [System.IO.Path]::GetFullPath($Path)
    if ($fullPath.StartsWith($rootPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        return $fullPath.Substring($rootPrefix.Length).Replace('\', '/')
    }
    return "<evidence-directory>/$([System.IO.Path]::GetFileName($fullPath))"
}

Push-Location $repositoryRoot
try {
    $sourceSnapshotBefore = Get-SourceSnapshotDigest -RootPath $repositoryRoot -EvidencePath $EvidenceDirectory
    $verificationStatus = 'PASSED'
    if (-not $SkipVerify) {
        # CI 默认先验证全部 Reactor，再从同一源码状态生成发布包。
        & .\mvnw.cmd -B -ntp verify 2>&1 | Tee-Object -FilePath (Join-Path $EvidenceDirectory 'p7-27-verify.log')
        $verifyExitCode = $LASTEXITCODE
        if ($verifyExitCode -ne 0) {
            throw "完整 verify 失败，退出码 $verifyExitCode。"
        }
    } else {
        $verificationStatus = 'NOT_RUN'
        $verifyExitCode = $null
        # 仅在复用已验证源码时构建报告所需 JAR，不把此分支当成全量回归证据。
        & .\mvnw.cmd -B -ntp -pl bootstrap -am '-DskipTests' package 2>&1 | Tee-Object -FilePath (Join-Path $EvidenceDirectory 'p7-27-package.log')
        $packageExitCode = $LASTEXITCODE
        if ($packageExitCode -ne 0) {
            throw "发布包构建失败，退出码 $packageExitCode。"
        }
    }

    # 聚合依赖仅写入构建输出目录；报告目录保留本次 CI 可归档的 JSON SBOM。
    $sbomArguments = @(
        '-B', '-ntp', '-pl', 'bootstrap', '-am',
        'org.cyclonedx:cyclonedx-maven-plugin:2.9.3:makeAggregateBom',
        '-Dcyclonedx.outputFormat=json',
        '-Dcyclonedx.outputName=p7-27-runtime-bom',
        '-Dcyclonedx.includeTestScope=false',
        '-Dcyclonedx.includeCompileScope=true',
        '-Dcyclonedx.includeProvidedScope=false',
        '-Dcyclonedx.skipAttach=true'
    )
    & .\mvnw.cmd @sbomArguments 2>&1 | Tee-Object -FilePath (Join-Path $EvidenceDirectory 'p7-27-sbom-generation.log')
    $sbomExitCode = $LASTEXITCODE
    if ($sbomExitCode -ne 0) {
        throw "CycloneDX SBOM 生成失败，退出码 $sbomExitCode。"
    }
    Copy-Item -LiteralPath (Join-Path $repositoryRoot 'target/bom.json') -Destination $sbomPath -Force

    # 首次使用时从官方发布页取扫描器，并强制与同一发布的 SHA256SUMS 比对。
    New-Item -ItemType Directory -Path $scannerDirectory -Force | Out-Null
    $releaseBase = "https://github.com/google/osv-scanner/releases/download/v$scannerVersion"
    if (-not (Test-Path -LiteralPath $scannerPath)) {
        Invoke-WebRequest -Uri "$releaseBase/osv-scanner_windows_amd64.exe" -OutFile $scannerPath
    }
    if (-not (Test-Path -LiteralPath $checksumPath)) {
        Invoke-WebRequest -Uri "$releaseBase/osv-scanner_SHA256SUMS" -OutFile $checksumPath
    }
    $checksumLine = Get-Content -LiteralPath $checksumPath | Where-Object { $_ -match '\*?osv-scanner_windows_amd64\.exe$' } | Select-Object -First 1
    if (-not $checksumLine) {
        throw 'OSV Scanner 官方校验文件缺少 Windows x64 二进制项。'
    }
    $expectedScannerHash = ($checksumLine -split '\s+')[0].ToLowerInvariant()
    $actualScannerHash = (Get-FileHash -LiteralPath $scannerPath -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($expectedScannerHash -ne $actualScannerHash) {
        throw 'OSV Scanner 二进制 SHA-256 与官方发布校验不一致。'
    }

    # 保存机器可读扫描结果；OSV Scanner 的成功退出码不替代漏洞数判定。
    $osvReportPath = Join-Path $EvidenceDirectory 'p7-27-osv-report.json'
    & $scannerPath scan source -L $sbomPath --format json --output-file $osvReportPath --verbosity info 2>&1 |
        Tee-Object -FilePath (Join-Path $EvidenceDirectory 'p7-27-osv-scan.log')
    $scannerExitCode = $LASTEXITCODE
    if ($scannerExitCode -ne 0) {
        throw "OSV Scanner 失败，退出码 $scannerExitCode。"
    }

    $bom = Get-Content -LiteralPath $sbomPath -Raw | ConvertFrom-Json
    $licenseCounts = @{}
    $missingLicenseCount = 0
    foreach ($component in $bom.components) {
        $declaredLicenses = @($component.licenses)
        if ($declaredLicenses.Count -eq 0) {
            $missingLicenseCount++
            continue
        }
        foreach ($entry in $declaredLicenses) {
            $licenseName = if ($entry.license.id) { $entry.license.id } elseif ($entry.license.name) { $entry.license.name } else { '[unknown]' }
            if (-not $licenseCounts.ContainsKey($licenseName)) {
                $licenseCounts[$licenseName] = 0
            }
            $licenseCounts[$licenseName]++
        }
    }
    $licenseInventory = [ordered]@{
        generatedAtUtc = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ssZ')
        componentCount = @($bom.components).Count
        componentsWithoutDeclaredLicense = $missingLicenseCount
        declaredLicenseEntries = $licenseCounts
        policyAssessment = '未配置组织批准的许可证允许/拒绝策略；本清单只汇总 SBOM 声明。'
    }
    $licenseInventory | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $EvidenceDirectory 'p7-27-license-inventory.json') -Encoding utf8

    $report = Get-Content -LiteralPath $osvReportPath -Raw | ConvertFrom-Json
    $vulnerabilities = @(
        foreach ($result in $report.results) {
            foreach ($package in $result.packages) {
                foreach ($vulnerability in $package.vulnerabilities) {
                    [pscustomobject]@{
                        Package = "$($package.package.name)@$($package.package.version)"
                        Id = $vulnerability.id
                        Summary = $vulnerability.summary
                    }
                }
            }
        }
    )
    $artifactPath = Join-Path $repositoryRoot 'bootstrap/target/eaf-bootstrap-0.1.0-SNAPSHOT.jar'
    $artifactHash = (Get-FileHash -LiteralPath $artifactPath -Algorithm SHA256).Hash.ToLowerInvariant()
    $bomHash = (Get-FileHash -LiteralPath $sbomPath -Algorithm SHA256).Hash.ToLowerInvariant()
    $sourceSnapshotAfter = Get-SourceSnapshotDigest -RootPath $repositoryRoot -EvidencePath $EvidenceDirectory
    if ($sourceSnapshotBefore.sha256 -ne $sourceSnapshotAfter.sha256) {
        throw '源码工作树在构建/扫描期间发生变化；本次产物不生成通过清单。'
    }
    $sourceSnapshot = $sourceSnapshotBefore
    $sourceManifestPath = Join-Path $EvidenceDirectory 'p7-27-source-manifest.tsv'
    [System.IO.File]::WriteAllText(
        $sourceManifestPath,
        $sourceSnapshot.manifestText,
        [System.Text.UTF8Encoding]::new($false)
    )
    $sbomDisplayPath = Get-RepositoryRelativePath -Path $sbomPath
    $manifestDisplayPath = Get-RepositoryRelativePath -Path $sourceManifestPath
    $runSummary = @(
        "OSV Scanner v$scannerVersion SHA256: $actualScannerHash",
        "Runtime SBOM components: $(@($bom.components).Count)",
        "Components without declared license: $missingLicenseCount",
        "Known vulnerability records: $($vulnerabilities.Count)",
        "Source snapshot files: $($sourceSnapshot.fileCount)",
        "Source snapshot SHA256: $($sourceSnapshot.sha256)",
        "SBOM SHA256: $bomHash",
        "Application JAR SHA256: $artifactHash"
    ) -join "`n"
    $runSummary | Set-Content -LiteralPath (Join-Path $EvidenceDirectory 'p7-27-supply-chain-summary.txt') -Encoding utf8

    $attestation = [ordered]@{
        schemaVersion = 1
        generatedAtUtc = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ssZ')
        source = [ordered]@{
            fingerprintKind = 'working-tree-content'
            sha256 = $sourceSnapshot.sha256
            fileCount = $sourceSnapshot.fileCount
            byteCount = $sourceSnapshot.byteCount
            manifestPath = $manifestDisplayPath
            manifestSha256 = $sourceSnapshot.sha256
            exclusions = @('**/.git/**', '**/.idea/**', '**/target/**', '**/node_modules/**', '**/docs/evidence/**', '.env', '.env.* except .env.example', 'reparse points')
        }
        verification = [ordered]@{
            command = '.\mvnw.cmd -B -ntp verify'
            status = $verificationStatus
            exitCode = $verifyExitCode
        }
        artifact = [ordered]@{
            path = 'bootstrap/target/eaf-bootstrap-0.1.0-SNAPSHOT.jar'
            bytes = (Get-Item -LiteralPath $artifactPath).Length
            sha256 = $artifactHash
        }
        sbom = [ordered]@{
            path = $sbomDisplayPath
            componentCount = @($bom.components).Count
            sha256 = $bomHash
        }
        vulnerabilityScan = [ordered]@{
            scanner = 'OSV Scanner'
            version = $scannerVersion
            binarySha256 = $actualScannerHash
            vulnerabilityCount = $vulnerabilities.Count
        }
        licenseMetadata = [ordered]@{
            componentCount = @($bom.components).Count
            componentsWithoutDeclaredLicense = $missingLicenseCount
            approvedPolicyAvailable = $false
        }
    }
    $attestation | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $EvidenceDirectory 'p7-27-attestation.json') -Encoding utf8
    Write-Output $runSummary

    if ($vulnerabilities.Count -gt 0) {
        $vulnerabilities | Format-Table Package, Id, Summary -AutoSize | Out-String | Write-Output
        throw "发现 $($vulnerabilities.Count) 条 OSV 漏洞记录；报告已保存，阻止本次供应链检查通过。"
    }
} finally {
    Pop-Location
}
