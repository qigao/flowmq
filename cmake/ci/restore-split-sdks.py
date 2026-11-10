"""Restore the exact native SDKs selected by FlowMQ's CMake build."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
from tempfile import TemporaryDirectory


VERSIONS = json.loads(
    (Path(__file__).resolve().parents[1] / "sdk-versions.json").read_text()
)
ROOTS = {
    "salts-base": "SALTS_BASE",
    "cmeta": "CMETA",
    "salts-cnet": "SALTSCORE",
    "salts-utils": "SALTS_UTILS",
}
CNET_PROFILES = {
    "linux-clang-x64": "linux-x64",
    "windows-llvm-mingw-x64": "windows-x64",
    "macos-clang-arm64": "macos-arm64",
    "android-arm64": "android-arm64",
}
UTILS_IDL_COMMIT = "e28c279508e052f4f121226be72e9cb1ba481c92"
UTILS_IDL_SHA256 = "1e11a6cdd5b6b38d92628bf1473cc23856b88e9f9c3f0d4b1a0112a2e18ac97e"


def download(url, path):
    subprocess.run(
        ["curl", "--fail", "--silent", "--show-error", "--location",
         "--retry", "3", "--output", str(path), url],
        check=True,
    )


def verify(path, expected):
    with path.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    if digest != expected:
        raise ValueError(f"SHA-256 mismatch for {path.name}: {digest}")


def restore(repo, asset, destination):
    base_url = f"https://github.com/qigao/{repo}/releases/download/v{VERSIONS[repo]}"
    with TemporaryDirectory() as temporary:
        archive = Path(temporary) / asset
        checksums = Path(temporary) / "SHA256SUMS"
        download(f"{base_url}/SHA256SUMS", checksums)
        hashes = {
            name.lstrip("*"): digest
            for digest, name in (line.split(maxsplit=1) for line in checksums.read_text().splitlines() if line)
        }
        download(f"{base_url}/{asset}", archive)
        verify(archive, hashes[asset])
        destination.mkdir(parents=True, exist_ok=True)
        with tarfile.open(archive) as payload:
            payload.extractall(destination, filter="data")


def restore_sdk(repo, profile, destination):
    platform = CNET_PROFILES[profile] if repo == "salts-cnet" else profile
    restore(repo, f"{repo}-{VERSIONS[repo]}-{platform}.tar", destination)
    if repo == "salts-utils":
        # The 0.1.0 tar omitted this file. Install it from the upstream packaging
        # fix until the SDK lock moves to a release that includes it.
        module = destination / "lib/cmake/SaltsUtils/IDL.cmake"
        download(f"https://raw.githubusercontent.com/qigao/salts-utils/{UTILS_IDL_COMMIT}/cmake/IDL.cmake", module)
        verify(module, UTILS_IDL_SHA256)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=CNET_PROFILES, required=True)
    parser.add_argument("--triplet", required=True)
    parser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    destination = args.destination.resolve()
    environment = {}
    for repo, key in ROOTS.items():
        sdk = destination / args.profile / repo
        restore_sdk(repo, args.profile, sdk)
        environment[f"{key}_ROOT"] = sdk.as_posix()
        environment[f"{key}_VERSION"] = VERSIONS[repo]
    if args.profile == "android-arm64":
        for repo in ("salts-base", "cmeta", "salts-utils"):
            host = destination / "linux-clang-x64" / repo
            restore_sdk(repo, "linux-clang-x64", host)
            environment[f"{ROOTS[repo]}_HOST_ROOT"] = host.as_posix()
    restore("vcpkg-cache", f"installed-{args.triplet}.tar.xz", destination)
    environment["FLOWMQ_DEPENDENCY_PREFIX"] = (
        destination / "vcpkg_installed" / args.triplet
    ).as_posix()
    output = "".join(f"{key}={value}\n" for key, value in environment.items())
    if "GITHUB_ENV" in os.environ:
        with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as github_env:
            github_env.write(output)
    print(output, end="")


if __name__ == "__main__":
    main()
