# FlowMQ.Native

`FlowMQ.Native` is built, qualified, packaged, and published by `qigao/flowmq`.

The package contains Release SDK profiles for Linux x64, Windows x64, the macOS publisher architecture, and Android arm64-v8a (API 26). Linux, Windows, and macOS run package-consumer qualification; Android is compile/link qualified.

First-party product dependencies float to the latest published stable producer SDKs:

- `Salts.Native`
- `SaltsUtils.Native`

FlowMQ does not encode those resolved producer versions into the packed
`FlowMQ.Native` dependency metadata. Consumers restore the latest producer
packages explicitly and provide `SALTS_ROOT` / `SALTS_UTILS_ROOT` to CMake.
The FlowMQ package version identifies the FlowMQ release itself; it is not a
constraint on first-party producer package versions.

Third-party dependencies come from this repository's root `vcpkg.json`; `qigao/vcpkg-cache` supplies only the shared vcpkg setup/cache infrastructure.

PR and `main` qualification runs pack CI artifacts using:

```text
<flowmq-version>-ci.<github-run-number>.<run-attempt>
```

Those CI packages are workflow artifacts only; they are not pushed to GitHub Packages.
Publishing is tag-only. A pushed `vX.Y.Z` tag must exactly match the FlowMQ project
version and publishes `FlowMQ.Native X.Y.Z`.

Each SDK profile records source/dependency provenance in `flowmq-sdk-manifest.txt`.
