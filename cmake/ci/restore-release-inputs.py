"""Select release-qualified FlowMQ build inputs using the shared vcpkg-cache reader."""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile


def api(repo, endpoint):
    return json.loads(subprocess.check_output(["gh", "api", f"repos/qigao/{repo}/{endpoint}"]))


def version(key):
    value = os.environ[key + "_VERSION"]
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", value):
        raise ValueError(f"{key}_VERSION must select an exact stable release")
    return value


def commit_of(repo, release_version):
    ref = api(repo, f"git/ref/tags/v{release_version}")["object"]
    if ref["type"] == "tag":
        ref = api(repo, "git/tags/" + ref["sha"])["object"]
    if ref["type"] != "commit":
        raise ValueError(f"{repo}: release tag must point to a commit")
    return ref["sha"]


class Inputs:
    def __init__(self, output):
        self.output = output.resolve()
        self.output.mkdir(parents=True, exist_ok=True)
        self.infrastructure = Path(os.environ["VCPKG_CACHE_REPOSITORY_ROOT"]).resolve()
        expected = f"v{version('VCPKG_CACHE')}^{{commit}}"
        revision = subprocess.check_output(
            ["git", "-C", str(self.infrastructure), "rev-parse", "HEAD", expected], text=True
        ).splitlines()
        if revision[0] != revision[1]:
            raise ValueError("vcpkg-cache checkout does not match VCPKG_CACHE_VERSION")
        transport_path = self.infrastructure / "scripts/setup-compiler-toolchain.py"
        spec = importlib.util.spec_from_file_location("transport", transport_path)
        self.transport = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.transport)
        self.reader = self.infrastructure / "scripts/restore-native-dependency.py"
        self.roots = {}
        self.target_sdks = {}

    def cache(self, triplet):
        descriptor = Path("cmake/dependencies") / f"{triplet}.lock.json"
        lock = self.output / f"vcpkg-cache-{version('VCPKG_CACHE')}-{triplet}.json"
        subprocess.run(
            [sys.executable, str(self.reader), "resolve-release", str(descriptor),
             "--output-lock", str(lock)], check=True,
        )
        digest = json.loads(lock.read_text(encoding="utf-8"))["sha256"]
        destination = self.output / f"vcpkg-{triplet}-{digest[:12]}"
        receipt = json.loads(subprocess.check_output(
            [sys.executable, str(self.reader), "locked", str(lock),
             "--destination", str(destination), "--reuse", "--payload", "vcpkg_installed",
             "--require", f"{triplet}/share/llhttp/llhttp-config.cmake"],
        ))
        self.roots["VCPKG_INSTALLED_DIR"] = Path(receipt["root"])

    def release(self, repo, selected):
        release = api(repo, f"releases/tags/v{selected}")
        if release["draft"] or release["prerelease"] or release["tag_name"] != f"v{selected}":
            raise ValueError(f"{repo}: expected published stable release {selected}")
        return release

    def download(self, release, name, digest=None, size=None):
        asset, = (item for item in release["assets"] if item["name"] == name)
        sha = asset["digest"].removeprefix("sha256:")
        if (digest is not None and sha != digest) or (size is not None and asset["size"] != size):
            raise ValueError(f"{name}: release asset does not match manifest")
        path = self.output / "downloads" / name
        if path.is_file():
            with path.open("rb") as stream:
                if hashlib.file_digest(stream, "sha256").hexdigest() != sha:
                    raise ValueError(f"{name}: cached release asset digest mismatch")
        else:
            self.transport.verified_download(asset["browser_download_url"], path, sha, asset["size"])
        if path.stat().st_size != asset["size"]:
            raise ValueError(f"{name}: release asset size mismatch")
        return path, sha

    def sdk(self, repo, key, profile, host=False):
        selected = version(key)
        release = self.release(repo, selected)
        path, _ = self.download(release, f"{repo}-{selected}-manifest.json")
        manifest = json.loads(path.read_bytes())
        repository = f"qigao/{repo}"
        identity_ok = (manifest["name"] == "TurboDB" and manifest["tag"] == release["tag_name"]
                       if repo == "turbodb" else manifest["repository"] in (
                           repository, f"https://github.com/{repository}"))
        if (not identity_ok or manifest["version"] != selected
                or manifest["commit"] != commit_of(repo, selected)):
            raise ValueError(f"{repo}: manifest identity mismatch")
        if repo in ("salts-base", "cmeta"):
            if (manifest["schemaVersion"] != 1 or manifest["tag"] != release["tag_name"]
                    or manifest["ci"]["conclusion"] != "success"):
                raise ValueError(f"{repo}: invalid producer manifest")
            row, = (row for row in manifest["sdks"] if row["platform"] == profile)
            name = row["file"]
            run_id, tree = manifest["ci"]["runId"], manifest["sourceTree"]
        else:
            if manifest["schema"] != 1:
                raise ValueError(f"{repo}: invalid producer manifest schema")
            row, = (row for row in manifest["sdks"] if row["profile"] == profile)
            name = row["file"] if repo == "turbodb" else row["asset"]
            run_id, tree = manifest["producer_run"], manifest["tree"]
        run = api(repo, f"actions/runs/{run_id}")
        for commit in {run["head_sha"], manifest["commit"]}:
            if api(repo, f"git/commits/{commit}")["tree"]["sha"] != tree:
                raise ValueError(f"{repo}: producer source tree mismatch")
        if run["conclusion"] != "success":
            raise ValueError(f"{repo}: producer CI did not succeed")

        cache, base = version("VCPKG_CACHE"), version("SALTS_BASE")
        if repo == "salts-base":
            matches = manifest["dependencies"]["release"] == f"v{cache}"
        elif repo == "salts-cnet":
            meta = row["sdk_manifest"]
            matches = (meta["cache_version"], meta["base_version"]) == (cache, base)
        elif repo == "turbodb":
            meta = row["sdk_manifest"]
            requires = {
                dep["key"]: (dep["version"], dep["sha256"]) for dep in meta["dependencies"]
            }
            matches = meta["cache_version"] == cache and requires == self.target_sdks
        else:
            deps = manifest["dependencies"]
            matches = (deps["vcpkg-cache"], deps["SaltsBase"]) == (cache, base)
        if not matches:
            raise ValueError(f"{repo}: SDK Base/cache versions differ from build inputs")

        archive, sha = self.download(release, name, row["sha256"], row.get("sizeBytes"))
        root = self.output / f"{archive.stem}-{sha[:12]}"
        with tarfile.open(archive) as package:
            package.extractall(root, filter="data")
        self.roots[f"{key}{'_HOST' if host else ''}_ROOT"] = root
        if not host:
            self.target_sdks[key] = selected, sha
        print(f"{repo} {profile} producer {run_id} sha256 {sha}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--triplet", required=True)
    parser.add_argument("--sdk-profile", required=True)
    parser.add_argument("--cnet-profile", required=True)
    parser.add_argument("--host-sdk-profile")
    parser.add_argument("--destination", type=Path, default=Path("build/release-inputs"))
    args = parser.parse_args()
    inputs = Inputs(args.destination)
    inputs.cache(args.triplet)
    for repo, key in (("salts-base", "SALTS_BASE"), ("cmeta", "CMETA"),
                      ("salts-utils", "SALTS_UTILS"), ("salts-cnet", "SALTS_CNET")):
        inputs.sdk(repo, key, args.cnet_profile if repo == "salts-cnet" else args.sdk_profile)
        if args.host_sdk_profile and repo != "salts-cnet":
            inputs.sdk(repo, key, args.host_sdk_profile, host=True)
    inputs.sdk("turbodb", "TURBODB", args.sdk_profile)
    with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as output:
        for key, root in inputs.roots.items():
            print(f"{key}={root.as_posix()}", file=output)


if __name__ == "__main__":
    main()
