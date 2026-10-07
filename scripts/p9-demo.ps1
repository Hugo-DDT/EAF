[CmdletBinding()]
param(
    [string] $BaseUri = 'http://127.0.0.1:18080',
    [string] $WorkspaceId = '10000000-0000-4000-8000-000000000001',
    [string] $StatePath = 'demo/p9/p9-demo-state.json',
    [ValidateRange(5, 300)] [int] $TimeoutSeconds = 90
)

$ErrorActionPreference = 'Stop'
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$stateFile = if ([IO.Path]::IsPathRooted($StatePath)) { $StatePath } else { Join-Path $repositoryRoot $StatePath }
$token = $env:EAF_P9_DEMO_TOKEN
if ([string]::IsNullOrWhiteSpace($token)) { $token = $env:EAF_LOCAL_DEMO_TOKEN }
if ([string]::IsNullOrWhiteSpace($token)) {
    throw '请在进程环境中设置 EAF_P9_DEMO_TOKEN（本地示例可用 eaf-local-alice）；脚本不会读取或输出 .env。'
}

$headers = @{ Authorization = "Bearer $token" }
$workspaceUri = "$($BaseUri.TrimEnd('/'))/api/v1/workspaces/$WorkspaceId"
$state = [ordered]@{
    workspaceId = $WorkspaceId
    generatedAt = [DateTimeOffset]::UtcNow.ToString('O')
    documents = @()
    retrievalBaseline = @()
}
$documents = @(
    @{ Path = 'demo/p9/knowledge/产品与编号规则.md'; Title = '合成样例：产品与编号规则'; SourceRef = 'demo://p9/product-codes' },
    @{ Path = 'demo/p9/knowledge/续约与服务条款.md'; Title = '合成样例：续约与服务条款'; SourceRef = 'demo://p9/renewal-terms' },
    @{ Path = 'demo/p9/knowledge/投诉处理规范.md'; Title = '合成样例：投诉处理规范'; SourceRef = 'demo://p9/complaints' },
    @{ Path = 'demo/p9/knowledge/业务通知冲突样例.md'; Title = '合成样例：业务通知冲突'; SourceRef = 'demo://p9/conflicting-notices' }
)

foreach ($item in $documents) {
    $path = Join-Path $repositoryRoot $item.Path
    $content = [IO.File]::ReadAllText($path, [Text.Encoding]::UTF8)
    $sha256 = [Security.Cryptography.SHA256]::Create()
    try { $contentHash = [BitConverter]::ToString($sha256.ComputeHash([Text.Encoding]::UTF8.GetBytes($content))).Replace('-', '').ToLowerInvariant() }
    finally { $sha256.Dispose() }
    $keyMaterial = "$($item.SourceRef)|$contentHash"
    $sha256 = [Security.Cryptography.SHA256]::Create()
    try { $idempotencyHash = [BitConverter]::ToString($sha256.ComputeHash([Text.Encoding]::UTF8.GetBytes($keyMaterial))).Replace('-', '').ToLowerInvariant() }
    finally { $sha256.Dispose() }
    $idempotencyKey = 'p9-demo-' + $idempotencyHash
    $body = @{ title = $item.Title; sourceRef = $item.SourceRef; content = $content; metadata = @{ classification = 'synthetic-demo'; phase = 'P9' } } | ConvertTo-Json -Depth 5
    $document = Invoke-RestMethod -Method Post -Uri "$workspaceUri/knowledge/documents" -Headers ($headers + @{ 'Idempotency-Key' = $idempotencyKey }) -ContentType 'application/json; charset=utf-8' -Body $body
    $chunks = Invoke-RestMethod -Method Post -Uri "$workspaceUri/knowledge/documents/$($document.id)/chunks?assetVersion=$($document.version)&chunkingVersion=p9-structure-1" -Headers $headers
    $buildKey = "$idempotencyKey-index-p9-structure-1"
    $build = Invoke-RestMethod -Method Post -Uri "$workspaceUri/knowledge/documents/$($document.id)/index-builds?assetVersion=$($document.version)&chunkingVersion=p9-structure-1" -Headers ($headers + @{ 'Idempotency-Key' = $buildKey })
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    while ($build.status -notin @('READY', 'FAILED') -and [DateTimeOffset]::UtcNow -lt $deadline) {
        Start-Sleep -Milliseconds 750
        $build = Invoke-RestMethod -Method Get -Uri "$workspaceUri/knowledge/documents/$($document.id)/index-builds/$($build.id)" -Headers $headers
    }
    if ($build.status -ne 'READY') { throw "合成样例索引未就绪：$($item.SourceRef) ($($build.status))" }
    $publication = Invoke-RestMethod -Method Post -Uri "$workspaceUri/knowledge/documents/$($document.id)/publish?expectedVersion=$($document.rowVersion)&buildId=$($build.id)" -Headers ($headers + @{ 'Idempotency-Key' = "$idempotencyKey-publish" })
    $state.documents += [ordered]@{ title = $item.Title; sourceRef = $item.SourceRef; documentId = $document.id; version = $document.version; rowVersion = $publication.documentRowVersion; buildId = $build.id; status = $publication.documentStatus; chunkCount = @($chunks).Count; contentHash = $contentHash }
}

foreach ($query in @('标准续约产品编号 SKU-CLOUD-RENEW-12', '客户续约之前应该提前多久联系')) {
    foreach ($mode in @('VECTOR', 'HYBRID')) {
        $body = @{ query = $query; topK = 5; mode = $mode } | ConvertTo-Json
        $result = Invoke-RestMethod -Method Post -Uri "$workspaceUri/knowledge/search" -Headers $headers -ContentType 'application/json' -Body $body
        $state.retrievalBaseline += [ordered]@{ query = $query; mode = $mode; result = $result }
    }
}

$stateDirectory = Split-Path -Parent $stateFile
New-Item -ItemType Directory -Path $stateDirectory -Force | Out-Null
$state | ConvertTo-Json -Depth 30 | Set-Content -LiteralPath $stateFile -Encoding utf8
Write-Output "合成知识材料与 VECTOR/HYBRID 检索对照已保存到 $stateFile"
