#requires -Version 7.0
<#
.SYNOPSIS
    Creates a read-only, source-focused GAMMA-UNWIND ZIP audit snapshot.
.DESCRIPTION
    Input:  E:\TA Projects\Gamma Unwind
    Output: E:\TA Projects\Gamma Unwind Source Code Audit
    Does not change the application. Excludes embedded runtimes, output/data,
    live credential files, databases, caches, archives, and executable builds.
    Records included file SHA-256 and skipped paths in a ZIP manifest, adds
    a SHA256SUMS.txt inventory inside the ZIP (including the manifest hash),
    verifies every archived entry. Prints the final ZIP SHA-256 to the console,
    without creating a .zip.sha256 sidecar or any other external checksum file.
    Keep the script under the project scripts/ folder so it is also captured.
    Not a full backup and not a guarantee that source text contains no secrets.
.EXAMPLE
    .\Create-GammaUnwind-SourceSnapshot.ps1 -Preview
.EXAMPLE
    .\Create-GammaUnwind-SourceSnapshot.ps1
#>
[CmdletBinding()]
param(
    [switch]$Preview
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

# Explicitly use the NEW, exact ASCII folder names; no old installation paths.
Set-Location -LiteralPath 'E:\TA Projects\Gamma Unwind'
$sourceRoot = [System.IO.Path]::GetFullPath((Get-Location).Path).TrimEnd('\')
$auditRoot = [System.IO.Path]::GetFullPath('E:\TA Projects\Gamma Unwind Source Code Audit').TrimEnd('\')
# The script is a source artifact. Require its permanent scripts/ location;
# running a Downloads copy would create a snapshot without this snapshotter.
$installedScript = [System.IO.Path]::Combine($sourceRoot, 'scripts', 'Create-GammaUnwind-SourceSnapshot.ps1')
$currentScript = [System.IO.Path]::GetFullPath($PSCommandPath)
if (-not $currentScript.Equals($installedScript, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw ('Install the script at ' + $installedScript + ' before running.')
}
$manifestEntryName = '__SOURCE_SNAPSHOT__/MANIFEST.json'
$hashListEntryName = '__SOURCE_SNAPSHOT__/SHA256SUMS.txt'

if ($auditRoot.Equals($sourceRoot, [System.StringComparison]::OrdinalIgnoreCase) -or
    $auditRoot.StartsWith(($sourceRoot + '\'), [System.StringComparison]::OrdinalIgnoreCase)) {
    throw 'Refusing an audit destination inside the source tree.'
}
$sourceInfo = Get-Item -LiteralPath $sourceRoot -Force
if (-not $sourceInfo.PSIsContainer -or
    (($sourceInfo.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0)) {
    throw 'Source path must be a real, existing directory (no junction/symlink).'
}

# Directory exclusions are intentionally conservative, source-oriented.
# Most importantly: do NOT exclude 'scripts' or 'gammad/store' (schema.sql).
$excludedDirs = [System.Collections.Generic.HashSet[string]]::new(
    [System.StringComparer]::OrdinalIgnoreCase
)
@(
    '.git', '.hg', '.svn', 'node_modules', '__pycache__', '.venv', 'venv',
    '.pytest_cache', '.mypy_cache', '.ruff_cache', '.tox', '.nox',
    'dist', 'build', 'release', 'target', 'out', 'outputs',
    'logs', 'log', 'tmp', 'temp', 'cache', '.cache', 'coverage', 'htmlcov',
    'backup', 'backups', 'snapshots', 'evidence',
    'data', 'database', 'databases', 'sessions', 'vault',
    'secrets', 'credentials', 'tokens', '__SOURCE_SNAPSHOT__'
) | ForEach-Object { [void]$excludedDirs.Add($_) }

$allowedExtensions = [System.Collections.Generic.HashSet[string]]::new(
    [System.StringComparer]::OrdinalIgnoreCase
)
@(
    '.py', '.pyw', '.pyi', '.ps1', '.psm1', '.psd1', '.ps1xml', '.sh', '.bat', '.cmd',
    '.js', '.jsx', '.mjs', '.cjs', '.ts', '.tsx', '.mts', '.cts',
    '.vue', '.svelte', '.astro', '.css', '.scss', '.sass', '.less',
    '.html', '.htm', '.json', '.jsonc', '.yaml', '.yml', '.toml', '.ini',
    '.cfg', '.conf', '.properties', '.xml', '.xsd', '.sql', '.proto',
    '.md', '.markdown', '.rst', '.txt', '.lock', '.svg',
    '.c', '.h', '.cpp', '.hpp', '.cc', '.cxx', '.rs', '.go',
    '.cs', '.csproj', '.sln', '.fs', '.fsproj', '.java', '.kt', '.gradle',
    '.rb', '.php', '.graphql', '.gql', '.yacc', '.l',
    '.png', '.jpg', '.jpeg', '.webp', '.ico', '.gif', '.drawio', '.mmd', '.mermaid', '.excalidraw'
) | ForEach-Object { [void]$allowedExtensions.Add($_) }

$extensionlessNames = [System.Collections.Generic.HashSet[string]]::new(
    [System.StringComparer]::OrdinalIgnoreCase
)
@('Dockerfile', 'Containerfile', 'Makefile', 'GNUmakefile', 'Procfile',
  'LICENSE', 'NOTICE', 'CHANGELOG', 'VERSION', 'EDITION', '.gitignore', '.gitattributes',
  '.editorconfig', '.dockerignore', '.prettierignore', '.prettierrc',
  '.eslintrc', 'Pipfile', 'Gemfile', 'Justfile') |
    ForEach-Object { [void]$extensionlessNames.Add($_) }

# Real installed configuration files can contain broker app secrets or tokens.
# Templates/defaults and build manifests are included; live runtime config is not.
$configurationExtensions = [System.Collections.Generic.HashSet[string]]::new(
    [System.StringComparer]::OrdinalIgnoreCase
)
@('.json', '.jsonc', '.yaml', '.yml', '.toml', '.ini', '.cfg', '.conf', '.properties') |
    ForEach-Object { [void]$configurationExtensions.Add($_) }

$maxFileBytes = 25MB
$maxTotalBytes = 500MB
$maximumSkipDetails = 5000
$included = [System.Collections.Generic.List[object]]::new()
$skipped = [System.Collections.Generic.List[object]]::new()
$skippedCount = 0
$truncatedSkipped = 0
[long]$totalBytes = 0

function Add-Skipped {
    param([string]$RelativePath, [string]$Reason)
    $script:skippedCount++
    if ($script:skipped.Count -ge $script:maximumSkipDetails) {
        $script:truncatedSkipped++
        return
    }
    $script:skipped.Add([ordered]@{ path = $RelativePath; reason = $Reason })
}

$pending = [System.Collections.Generic.Stack[System.IO.DirectoryInfo]]::new()
$pending.Push([System.IO.DirectoryInfo]::new($sourceRoot))
while ($pending.Count -gt 0) {
    $folder = $pending.Pop()
    foreach ($item in @(Get-ChildItem -LiteralPath $folder.FullName -Force -ErrorAction Stop)) {
        $relative = [System.IO.Path]::GetRelativePath($sourceRoot, $item.FullName).Replace('\', '/')
        if ($relative.StartsWith('../') -or $relative.StartsWith('/') -or $relative.Contains(':')) {
            throw 'Unexpected path outside the project during source enumeration.'
        }
        if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            Add-Skipped $relative 'ReparsePoint'
            continue
        }
        if ($item.PSIsContainer) {
            $rootComponent = $relative.Split('/')[0]
            if ($excludedDirs.Contains($item.Name) -or
                ($rootComponent -ieq 'python')) {
                Add-Skipped ($relative + '/') 'ExcludedGeneratedRuntimeDataOrSecretDirectory'
                continue
            }
            $pending.Push([System.IO.DirectoryInfo]$item)
            continue
        }
        if (-not ($item -is [System.IO.FileInfo])) {
            Add-Skipped $relative 'NotARegularFile'
            continue
        }
        $name = $item.Name
        $extension = [System.IO.Path]::GetExtension($name)
        # Allow only explicit environment templates. Live .env and backups
        # remain excluded, including .env.*.bak and files under secret dirs.
        $safeEnvTemplate = $name -match '(?i)^\.env\.(?:example|sample|template)$'
        if (($name -match '(?i)^\.env(?:\.|$)' -and -not $safeEnvTemplate) -or
            $extension -match '(?i)^\.(?:db|db3|sqlite|sqlite3|pem|key|p12|pfx|p8|der|crt|cer|jks|keystore|kdbx|log|zip|7z|tar|gz|exe|dll|pyd|so|dylib|bin|bak|old|tmp|pyc|pyo|whl|parquet|feather|woff|woff2|ttf|otf)$') {
            Add-Skipped $relative 'SensitiveOrGeneratedArtifact'
            continue
        }
        if ($name -match '(?i)(^|[._-])(?:token|tokens|session|secrets?|passwords?|credentials?|private[_-]?key)([._-]|$)' -and
            -not ($extension -in @('.py', '.pyi', '.ps1', '.ts', '.tsx', '.js', '.jsx', '.rs', '.go', '.cs', '.sh'))) {
            Add-Skipped $relative 'PotentialCredentialFile'
            continue
        }
        if ($configurationExtensions.Contains($extension)) {
            $knownBuildConfig = $name -match '(?i)^(?:package(?:-lock)?\.json|tsconfig.*\.json|jsconfig.*\.json|Cargo\.toml|rust-toolchain\.toml|pyproject\.toml|.*schema.*|.*\.lock)$'
            $templateConfig = $name -match '(?i)(?:example|sample|template|default|\.dist)(?:[._-]|$)'
            if ($name -match '(?i)^(?:config|configuration|settings|broker|fyers|account|auth|profile)(?:[._-]|$)' -and
                -not $knownBuildConfig -and -not $templateConfig) {
                Add-Skipped $relative 'LiveRuntimeConfigurationExcluded'
                continue
            }
        }
        # Include real documentation, including the existing DOCX setup guide.
        # Do not accept arbitrary executable or database data in docs/.
        $isDocumentation = $relative.StartsWith('docs/', [System.StringComparison]::OrdinalIgnoreCase)
        $documentationExtensions = @('.pdf', '.docx', '.xlsx', '.pptx', '.odt', '.ods', '.odp')
        if (-not $allowedExtensions.Contains($extension) -and
            -not $extensionlessNames.Contains($name) -and
            -not $safeEnvTemplate -and
            -not ($isDocumentation -and $extension -in $documentationExtensions)) {
            Add-Skipped $relative 'NotSourceOrSupportedAsset'
            continue
        }
        if ($item.Length -gt $maxFileBytes) {
            Add-Skipped $relative 'AbovePerFileSizeLimit'
            continue
        }
        $totalBytes += $item.Length
        if ($totalBytes -gt $maxTotalBytes) {
            throw 'Source selection exceeds 500 MiB. Review exclusions; no ZIP was created.'
        }
        $included.Add([pscustomobject]@{
            RelativePath = $relative
            FullName = $item.FullName
            Length = [long]$item.Length
            LastWriteTimeUtc = $item.LastWriteTimeUtc
        })
    }
}

# Fail closed if the script or existing docs folder would be omitted.
# Snapshot source should always include the exact script that created it.
$scriptRelative = 'scripts/Create-GammaUnwind-SourceSnapshot.ps1'
if (@($included | Where-Object { $_.RelativePath -ieq $scriptRelative }).Count -ne 1) {
    throw 'Snapshot script was not selected. Refusing an incomplete source audit.'
}
$docsPath = Join-Path $sourceRoot 'docs'
if (Test-Path -LiteralPath $docsPath -PathType Container) {
    if (@($included | Where-Object { $_.RelativePath.StartsWith('docs/', [System.StringComparison]::OrdinalIgnoreCase) }).Count -eq 0) {
        throw 'docs/ exists but no documentation files were selected. Refusing an incomplete audit.'
    }
}

if ($included.Count -eq 0) {
    throw 'No source files selected. Refusing to generate an empty/misleading snapshot.'
}
$orderedSources = @($included | Sort-Object RelativePath)
Write-Host ('PROJECT=' + $sourceRoot)
Write-Host ('AUDIT_DIRECTORY=' + $auditRoot)
Write-Host ('SELECTED_SOURCE_FILES=' + $orderedSources.Count)
Write-Host ('SELECTED_SOURCE_BYTES=' + $totalBytes)
Write-Host ('SKIPPED_FILES_OR_DIRECTORY_ROOTS=' + $skippedCount)
Write-Host ('SKIPPED_DETAIL_TRUNCATED=' + $truncatedSkipped)
if ($Preview) {
    Write-Host 'STATUS: PREVIEW_ONLY__NO_FILES_WRITTEN'
    $orderedSources | Select-Object -First 15 RelativePath, Length | Format-Table -AutoSize
    return
}

if (-not (Test-Path -LiteralPath $auditRoot -PathType Container)) {
    New-Item -ItemType Directory -Path $auditRoot -Force | Out-Null
}
$auditInfo = Get-Item -LiteralPath $auditRoot -Force
if (($auditInfo.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
    throw 'Audit directory is a junction/symlink. Refusing archive write.'
}
$indiaZone = [System.TimeZoneInfo]::FindSystemTimeZoneById('India Standard Time')
$nowIndia = [System.TimeZoneInfo]::ConvertTimeFromUtc([DateTime]::UtcNow, $indiaZone)
$timePart = $nowIndia.ToString('yyyy-MM-dd_HHmmss_fff')
$suffix = [guid]::NewGuid().ToString('N').Substring(0, 8)
$zipName = 'Gamma Unwind Source Code ' + $timePart + '_IST_' + $suffix + '.zip'
$zipPath = Join-Path $auditRoot $zipName
$manifestFiles = [System.Collections.Generic.List[object]]::new()
$archive = $null
$fileStream = $null
$complete = $false

try {
    $fileStream = [System.IO.FileStream]::new(
        $zipPath,
        [System.IO.FileMode]::CreateNew,
        [System.IO.FileAccess]::Write,
        [System.IO.FileShare]::None
    )
    $archive = [System.IO.Compression.ZipArchive]::new(
        $fileStream, [System.IO.Compression.ZipArchiveMode]::Create, $false
    )
    foreach ($source in $orderedSources) {
        $sourceHash = (Get-FileHash -LiteralPath $source.FullName -Algorithm SHA256).Hash.ToUpperInvariant()
        [void][System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
            $archive,
            $source.FullName,
            $source.RelativePath,
            [System.IO.Compression.CompressionLevel]::Optimal
        )
        $updated = Get-Item -LiteralPath $source.FullName -Force
        if ($updated.Length -ne $source.Length -or
            $updated.LastWriteTimeUtc -ne $source.LastWriteTimeUtc) {
            throw ('Source file changed during snapshot: ' + $source.RelativePath)
        }
        $manifestFiles.Add([ordered]@{
            path = $source.RelativePath
            bytes = [long]$source.Length
            sha256 = $sourceHash
        })
    }
    $manifest = [ordered]@{
        schema = 'gamma-unwind-source-audit-v3'
        purpose = 'Source-focused audit snapshot, not a complete installation or user-data backup'
        scope = 'Installed Gamma Unwind source folder only; disposable R1.8 candidate ZIPs stored elsewhere are not included'
        createdIST = ($nowIndia.ToString('yyyy-MM-ddTHH:mm:ss.fff') + '+05:30')
        sourceDirectory = $sourceRoot
        auditDirectory = $auditRoot
        archiveFile = $zipName
        hashAlgorithm = 'SHA256'
        checksumEntry = $hashListEntryName
        checksumCoverage = 'All selected source entries and the snapshot manifest; checksum file itself excluded'
        externalChecksumFile = $false
        wholeZipHash = 'SHA256 printed to console after ZIP verification, never stored inside ZIP'
        includesDocumentation = $true
        includesSnapshotScript = $true
        includedCount = $manifestFiles.Count
        includedBytes = [long]$totalBytes
        skippedCount = $skippedCount
        skippedDetailTruncated = $truncatedSkipped
        files = @($manifestFiles.ToArray())
        skipped = @($skipped.ToArray())
        note = 'Review the excluded-path inventory; source text itself may still contain secrets. Do not publish this archive.'
    }
    $manifestJson = $manifest | ConvertTo-Json -Depth 10
    $encoding = [System.Text.UTF8Encoding]::new($false)
    $manifestBytes = $encoding.GetBytes($manifestJson)
    $manifestEntry = $archive.CreateEntry($manifestEntryName, [System.IO.Compression.CompressionLevel]::Optimal)
    $manifestWriter = $manifestEntry.Open()
    try {
        $manifestWriter.Write($manifestBytes, 0, $manifestBytes.Length)
    }
    finally {
        $manifestWriter.Dispose()
    }

    # GNU-style internal SHA256 listing. It cannot contain its own hash or
    # the final ZIP hash (both would be self-referential). Include MANIFEST.
    $checksumLines = [System.Collections.Generic.List[string]]::new()
    foreach ($item in $manifestFiles) {
        $checksumLines.Add(([string]$item.sha256 + ' *' + [string]$item.path))
    }
    $manifestHasher = [System.Security.Cryptography.SHA256]::Create()
    try {
        $manifestSha = [System.Convert]::ToHexString($manifestHasher.ComputeHash($manifestBytes))
    }
    finally {
        $manifestHasher.Dispose()
    }
    $checksumLines.Add(($manifestSha + ' *' + $manifestEntryName))
    $checksumText = ($checksumLines -join "`n") + "`n"
    $checksumBytes = $encoding.GetBytes($checksumText)
    $checksumEntry = $archive.CreateEntry($hashListEntryName, [System.IO.Compression.CompressionLevel]::Optimal)
    $checksumWriter = $checksumEntry.Open()
    try {
        $checksumWriter.Write($checksumBytes, 0, $checksumBytes.Length)
    }
    finally {
        $checksumWriter.Dispose()
    }
    $archive.Dispose()
    $archive = $null
    $fileStream = $null

    # Verify all entries, including that the internal SHA256SUMS.txt really
    # lists the exact source inventory AND the snapshot manifest.
    $zipCheck = [System.IO.Compression.ZipFile]::OpenRead($zipPath)
    try {
        if ($zipCheck.Entries.Count -ne ($manifestFiles.Count + 2)) {
            throw 'ZIP entry count differs from the source and checksum manifests.'
        }
        $zipEntries = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
        foreach ($entry in $zipCheck.Entries) {
            if (-not $zipEntries.Add($entry.FullName)) {
                throw ('Duplicate ZIP entry: ' + $entry.FullName)
            }
        }
        foreach ($item in $manifestFiles) {
            $entry = $zipCheck.GetEntry([string]$item.path)
            if ($null -eq $entry -or $entry.Length -ne [long]$item.bytes) {
                throw ('ZIP missing a source file or size mismatch: ' + $item.path)
            }
            $hasher = [System.Security.Cryptography.SHA256]::Create()
            $entryStream = $null
            try {
                $entryStream = $entry.Open()
                $actualSha = [System.Convert]::ToHexString($hasher.ComputeHash($entryStream))
            }
            finally {
                if ($null -ne $entryStream) { $entryStream.Dispose() }
                $hasher.Dispose()
            }
            if ($actualSha -ne [string]$item.sha256) {
                throw ('ZIP entry SHA256 mismatch: ' + $item.path)
            }
        }
        $checkManifest = $zipCheck.GetEntry($manifestEntryName)
        $checkSums = $zipCheck.GetEntry($hashListEntryName)
        if ($null -eq $checkManifest -or $null -eq $checkSums) {
            throw 'Internal snapshot manifest or SHA256SUMS.txt missing.'
        }
        $hasher = [System.Security.Cryptography.SHA256]::Create()
        try {
            $stream = $checkManifest.Open()
            try { $actualManifestHash = [System.Convert]::ToHexString($hasher.ComputeHash($stream)) }
            finally { $stream.Dispose() }
        }
        finally { $hasher.Dispose() }
        if ($actualManifestHash -ne $manifestSha) { throw 'Internal manifest hash mismatch.' }
        $stream = $checkSums.Open()
        try {
            $buffer = [System.IO.MemoryStream]::new()
            try {
                $stream.CopyTo($buffer)
                $actualChecksumBytes = $buffer.ToArray()
            }
            finally { $buffer.Dispose() }
        }
        finally { $stream.Dispose() }
        if ($encoding.GetString($actualChecksumBytes) -cne $checksumText) {
            throw 'Internal SHA256SUMS.txt mismatch.'
        }
    }
    finally {
        $zipCheck.Dispose()
    }
    $zipSha = (Get-FileHash -LiteralPath $zipPath -Algorithm SHA256).Hash.ToUpperInvariant()
    $complete = $true
    Write-Host ('PASS: SOURCE_SHA256_VERIFIED=' + $manifestFiles.Count + '/' + $manifestFiles.Count)
    Write-Host ('PASS: INTERNAL_SHA256SUMS_VERIFIED=' + ($manifestFiles.Count + 1) + '/' + ($manifestFiles.Count + 1))
    Write-Host ('SNAPSHOT_ZIP=' + $zipPath)
    Write-Host ('SNAPSHOT_SHA256=' + $zipSha)
    Write-Host 'EXTERNAL_CHECKSUM_FILES=NONE'
    Write-Host 'STATUS: GAMMA_SOURCE_SNAPSHOT_CREATED_AND_VERIFIED'
}
finally {
    if ($null -ne $archive) { $archive.Dispose() }
    if ($null -ne $fileStream) { $fileStream.Dispose() }
    if (-not $complete) {
        if (Test-Path -LiteralPath $zipPath -PathType Leaf) {
            Remove-Item -LiteralPath $zipPath -Force
        }
    }
}
