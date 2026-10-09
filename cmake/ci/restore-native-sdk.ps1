param(
  [Parameter(Mandatory=$true)]
  [ValidateSet('windows-x64', 'linux-x64', 'linux-arm64', 'macos-x64', 'macos-arm64', 'android-arm64-v8a', 'ios-arm64', 'ios-simulator-arm64')]
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

# Resolve the requested Salts 2.3.0 / SaltsUtils 4.3.0 prerelease or stable
# on every restore. Fresh project.assets.json owns the selected package paths.
@'
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net8.0</TargetFramework>
    <RestorePackagesWithLockFile>false</RestorePackagesWithLockFile>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Salts.Native" Version="2.3.0-*" Condition="'$(UseSaltsCandidate)' != 'true'" />
    <PackageReference Include="SaltsUtils.Native" Version="4.3.0-*" />
  </ItemGroup>
  <ItemGroup Condition="'$(FlowMQSaltsReleaseVersion)' != ''">
    <PackageReference Update="Salts.Native" Version="$(FlowMQSaltsReleaseVersion)" />
  </ItemGroup>
</Project>
'@ | Set-Content -LiteralPath $project -Encoding utf8NoBOM

$useCandidate = -not [string]::IsNullOrWhiteSpace($env:SALTS_CANDIDATE_ROOT)
$saltsRelease = ''
if (-not $useCandidate) {
  $saltsRelease = $env:FLOWMQ_SALTS_RELEASE_VERSION
  if ([string]::IsNullOrWhiteSpace($saltsRelease)) {
    # NuGet X.Y.Z-* also selects incomplete Linux-only rc.sha candidates.
    # Discover only official numeric rc.N or stable GitHub Releases.
    $env:GH_TOKEN = $env:GITHUB_TOKEN
    $saltsRelease = @(gh api 'repos/qigao/salts/releases?per_page=100' |
      python (Join-Path $repositoryRoot 'cmake/ci/select-salts-release.py') '2.3.0')
    if ($LASTEXITCODE -ne 0 -or $saltsRelease.Count -ne 1) {
      throw 'No complete official Salts 2.3.0 RC/stable release'
    }
    $saltsRelease = [string]$saltsRelease[0]
  }
  if ($saltsRelease -notmatch '^2\.3\.0(-rc\.[1-9][0-9]*)?$') {
    throw "Invalid Salts release selection: $saltsRelease"
  }
  Write-Host "Selecting published Salts.Native $saltsRelease"
}
dotnet restore $project --packages $packages --configfile (Join-Path $repositoryRoot 'cmake/native-sdk.nuget.config') --no-cache --force-evaluate "-p:UseSaltsCandidate=$($useCandidate.ToString().ToLowerInvariant())" "-p:FlowMQSaltsReleaseVersion=$saltsRelease"
if ($LASTEXITCODE -ne 0) { throw 'Failed to restore Salts 2.3.0-* and SaltsUtils 4.3.0-* SDKs' }
$assets = Get-Content -LiteralPath (Join-Path $restoreRoot 'obj/project.assets.json') -Raw | ConvertFrom-Json -AsHashtable

$sdks = @(
  @{ Package = 'Salts.Native'; Name = 'Salts'; Directory = 'salts'; Environment = 'SALTS_ROOT' },
  @{ Package = 'SaltsUtils.Native'; Name = 'SaltsUtils'; Directory = 'salts-utils'; Environment = 'SALTS_UTILS_ROOT' }
)
# Validate both SDKs for the requested RID before changing either stable link.
foreach ($sdk in $sdks) {
  if ($useCandidate -and $sdk.Package -eq 'Salts.Native') {
    $sdk.Target = $env:SALTS_CANDIDATE_ROOT
    $sdk.Resolved = "Salts.Native/candidate-$env:SALTS_CANDIDATE_SHA"
  } else {
    $keys = @($assets.libraries.Keys | Where-Object { $_.StartsWith("$($sdk.Package)/", [StringComparison]::OrdinalIgnoreCase) })
    if ($keys.Count -ne 1) { throw "Expected one resolved $($sdk.Package) package" }
    if ($sdk.Package -eq 'Salts.Native' -and $keys[0] -ne "Salts.Native/$saltsRelease") {
      throw "Resolved unqualified Salts.Native $($keys[0]); expected $saltsRelease"
    }
    $packageRoot = Join-Path $packages $assets.libraries[$keys[0]].path
    $sdk.Target = Join-Path $packageRoot "sdk/$Rid"
    $sdk.Resolved = $keys[0]
  }
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
  if ($env:GITHUB_ENV) {
    "$($sdk.Environment)=$link" >> $env:GITHUB_ENV
    $versionVariable = if ($sdk.Package -eq 'Salts.Native') { 'FLOWMQ_SALTS_RESOLVED_VERSION' } else { 'FLOWMQ_SALTS_UTILS_RESOLVED_VERSION' }
    "$versionVariable=$($sdk.Resolved)" >> $env:GITHUB_ENV
  }
  Write-Host "Restored $($sdk.Resolved) for $Rid at $link"
}
