#!/usr/bin/env python3
"""Select the highest published complete Salts.Native release for one X.Y.Z.

The NuGet floating expression X.Y.Z-* also admits internal rc.sha<SHA>
Linux-only packages, which can sort above public rc.N and break downstream
platforms. Keep the declared PackageReference floating, but dynamically select
only an official GitHub Release carrying its matching nonempty .nupkg.
Modeled on qigao/salts-utils/cmake/ci/select-salts-release.py (#1089).
"""
import json
import re
import sys

if len(sys.argv) != 2 or re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", sys.argv[1]) is None:
    raise SystemExit("Usage: select-salts-release.py X.Y.Z")
target = sys.argv[1]
candidate_pattern = re.compile(r"v" + re.escape(target) + r"-rc\.([1-9][0-9]*)")
releases = json.load(sys.stdin)
if not isinstance(releases, list):
    raise SystemExit("Expected GitHub Releases list")

eligible = []
for release in releases:
    if release.get("draft"):
        continue
    tag = release.get("tag_name", "")
    if tag == "v" + target and release.get("prerelease") is False:
        rank = (1, 0)
    elif (match := candidate_pattern.fullmatch(tag)) and release.get("prerelease") is True:
        rank = (0, int(match.group(1)))
    else:
        continue
    version = tag[1:]
    if not any(
        asset.get("name") == "Salts.Native." + version + ".nupkg"
        and asset.get("size", 0) > 0
        for asset in release.get("assets", [])
    ):
        continue
    eligible.append((rank, version))

if not eligible:
    raise SystemExit("No published full Salts.Native " + target + " RC/stable release")
print(max(eligible)[1])
