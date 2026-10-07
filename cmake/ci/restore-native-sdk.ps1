param(
  [Parameter(Mandatory=$true)]
  [ValidateSet('windows-x64', 'linux-x64', 'linux-arm64', 'macos-arm64', 'android-arm64-v8a', 'ios-arm64', 'ios-simulator-arm64')]
  [string]$Rid
)
$ErrorActionPreference = 'Stop'

if ([string]::IsNullOrWhiteSpace($env:GITHUB_TOKEN)) {
  throw 'GITHUB_TOKEN with read:packages is required'
}
$repositoryRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$restoreRoot = Join-Path $repositoryRoot 'build/native-sdk'
$packages = if ($env:QIGAO_NUGET_PACKAGES) {
  [IO.Path]::GetFullPath($env:QIGAO_NUGET_PACKAGES)
} else {
  Join-Path $repositoryRoot 'stage/nuget'
}
$project = Join-Path $restoreRoot 'flowmq-native-sdk-restore.csproj'
New-Item -ItemType Directory -Path $restoreRoot -Force | Out-Null

# Follow SaltsUtils: resolve floating versions anew; assets own the selected paths.
@'
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net8.0</TargetFramework>
    <RestorePackagesWithLockFile>false</RestorePackagesWithLockFile>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Salts.Native" Version="*" />
  </ItemGroup>
</Project>
'@ | Set-Content -LiteralPath $project -Encoding utf8NoBOM

dotnet restore $project --packages $packages --configfile (Join-Path $repositoryRoot 'cmake/native-sdk.nuget.config') --no-cache --force-evaluate
if ($LASTEXITCODE -ne 0) { throw 'Failed to restore the latest Salts SDK' }
$assets = Get-Content -LiteralPath (Join-Path $restoreRoot 'obj/project.assets.json') -Raw | ConvertFrom-Json -AsHashtable

$keys = @($assets.libraries.Keys | Where-Object { $_.StartsWith('Salts.Native/', [StringComparison]::OrdinalIgnoreCase) })
if ($keys.Count -ne 1) { throw 'Expected one resolved Salts.Native package' }
$packageRoot = Join-Path $packages $assets.libraries[$keys[0]].path
$target = Join-Path $packageRoot "sdk/$Rid"
$config = Join-Path $target 'lib/cmake/Salts/SaltsConfig.cmake'
if (-not (Test-Path -LiteralPath $config -PathType Leaf)) { throw "Missing restored SDK file: $config" }

# Stable links let IDEs use a new SDK without inheriting another shell environment.
# Never replace a real SDK directory or recursively remove a link target.
$link = Join-Path $repositoryRoot "stage/dependencies/salts/$Rid"
New-Item -ItemType Directory -Path (Split-Path $link -Parent) -Force | Out-Null
$existing = Get-Item -LiteralPath $link -Force -ErrorAction SilentlyContinue
$linkType = if ($IsWindows) { 'Junction' } else { 'SymbolicLink' }
if ($existing -and $existing.LinkType -ne $linkType) {
  throw "Refusing to replace non-$linkType SDK path: $link"
}
if ($existing -and $existing.Target -ne $target) {
  Remove-Item -LiteralPath $link -Force
  $existing = $null
}
if (-not $existing) {
  New-Item -ItemType $linkType -Path $link -Target $target | Out-Null
}
$env:SALTS_ROOT = $link
Write-Host "Restored $($keys[0]) for $Rid at $link"
