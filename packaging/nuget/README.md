# FlowMQ.Native

`FlowMQ.Native` is built, qualified, packaged, and published by `qigao/flowmq`.

The package contains Release SDK profiles for Linux x64, Windows x64, the macOS publisher architecture, and Android arm64-v8a (API 26). Linux, Windows, and macOS run package-consumer qualification; Android is compile/link qualified.

Exact product dependencies:

- `Salts.Native 1.8.2`
- `SaltsUtils.Native 4.1.2`

Third-party dependencies come from this repository's root `vcpkg.json`; `qigao/vcpkg-cache` supplies only the shared vcpkg setup/cache infrastructure.

PR and `main` qualification runs pack CI artifacts using:

```text
<flowmq-version>-ci.<github-run-number>.<run-attempt>
```

Those CI packages are workflow artifacts only; they are not pushed to GitHub Packages.
Publishing is tag-only. A pushed `vX.Y.Z` tag must exactly match the FlowMQ project
version and publishes `FlowMQ.Native X.Y.Z`.

Each SDK profile records source/dependency provenance in `flowmq-sdk-manifest.txt`.
