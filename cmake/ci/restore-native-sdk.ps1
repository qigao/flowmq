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
    <PackageReference Include="SaltsUtils.Native" Version="*" />
  </ItemGroup>
</Project>
'@ | Set-Content -LiteralPath $project -Encoding utf8NoBOM

dotnet restore $project --packages $packages --configfile (Join-Path $repositoryRoot 'cmake/native-sdk.nuget.config') --no-cache --force-evaluate
if ($LASTEXITCODE -ne 0) { throw 'Failed to restore the latest Salts and SaltsUtils SDKs' }
$assets = Get-Content -LiteralPath (Join-Path $restoreRoot 'obj/project.assets.json') -Raw | ConvertFrom-Json -AsHashtable

$sdks = @(
  @{ Package = 'Salts.Native'; Name = 'Salts'; Directory = 'salts'; Environment = 'SALTS_ROOT' },
  @{ Package = 'SaltsUtils.Native'; Name = 'SaltsUtils'; Directory = 'salts-utils'; Environment = 'SALTS_UTILS_ROOT' }
)
# Validate both SDKs for the requested RID before changing either stable link.
foreach ($sdk in $sdks) {
  $keys = @($assets.libraries.Keys | Where-Object { $_.StartsWith("$($sdk.Package)/", [StringComparison]::OrdinalIgnoreCase) })
  if ($keys.Count -ne 1) { throw "Expected one resolved $($sdk.Package) package" }
  $packageRoot = Join-Path $packages $assets.libraries[$keys[0]].path
  $sdk.Target = Join-Path $packageRoot "sdk/$Rid"
  $sdk.Resolved = $keys[0]
  $config = Join-Path $sdk.Target "lib/cmake/$($sdk.Name)/$($sdk.Name)Config.cmake"
  if (-not (Test-Path -LiteralPath $config -PathType Leaf)) { throw "Missing restored SDK file: $config" }
}

# Stable links let IDEs use a new SDK without inheriting another shell environment.
# Never replace a real SDK directory or recursively remove a link target.
foreach ($sdk in $sdks) {
  $link = Join-Path $repositoryRoot "stage/dependencies/$($sdk.Directory)/$Rid"
  New-Item -ItemType Directory -Path (Split-Path $link -Parent) -Force | Out-Null
  $existing = Get-Item -LiteralPath $link -Force -ErrorAction SilentlyContinue
  $linkType = if ($IsWindows) { 'Junction' } else { 'SymbolicLink' }
  if ($existing -and $existing.LinkType -ne $linkType) {
    throw "Refusing to replace non-$linkType SDK path: $link"
  }
  if ($existing -and $existing.Target -ne $sdk.Target) {
    Remove-Item -LiteralPath $link -Force
    $existing = $null
  }
  if (-not $existing) {
    New-Item -ItemType $linkType -Path $link -Target $sdk.Target | Out-Null
  }
  Set-Item -LiteralPath "Env:$($sdk.Environment)" -Value $link
  Write-Host "Restored $($sdk.Resolved) for $Rid at $link"
}
