[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$PackageRoot,

    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$Destination
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$resolvedPackageRoot = (Resolve-Path -LiteralPath $PackageRoot).Path
$packageBin = Join-Path -Path $resolvedPackageRoot -ChildPath "bin"
if (-not (Test-Path -LiteralPath $packageBin -PathType Container)) {
    throw "External package runtime directory does not exist: $packageBin"
}

$runtimeFiles = @(Get-ChildItem -LiteralPath $packageBin -Filter "*.dll" -File)
if ($runtimeFiles.Count -eq 0) {
    throw "External package runtime directory contains no DLLs: $packageBin"
}

if (-not (Test-Path -LiteralPath $Destination -PathType Container)) {
    New-Item -ItemType Directory -Path $Destination -Force | Out-Null
}
$resolvedDestination = (Resolve-Path -LiteralPath $Destination).Path

$copiedCount = 0
foreach ($runtimeFile in $runtimeFiles) {
    $destinationFile = Join-Path -Path $resolvedDestination -ChildPath $runtimeFile.Name
    $copyRequired = $true

    if (Test-Path -LiteralPath $destinationFile -PathType Leaf) {
        $existingFile = Get-Item -LiteralPath $destinationFile
        if ($existingFile.Length -eq $runtimeFile.Length) {
            $sourceHash = (Get-FileHash -LiteralPath $runtimeFile.FullName -Algorithm SHA256).Hash
            $destinationHash = (Get-FileHash -LiteralPath $destinationFile -Algorithm SHA256).Hash
            $copyRequired = $sourceHash -ne $destinationHash
        }
    }

    if ($copyRequired) {
        Copy-Item -LiteralPath $runtimeFile.FullName -Destination $destinationFile -Force
        $copiedCount++
    }
}

Write-Host "External runtime sync: package='$resolvedPackageRoot', copied=$copiedCount, total=$($runtimeFiles.Count)"
