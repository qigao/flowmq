# FlowMQ.Native

`FlowMQ.Native` is built, qualified, packaged, and published by `qigao/flowmq`.

The package contains Release SDK profiles for Linux x64, Windows x64, the macOS publisher architecture, and Android arm64-v8a (API 26). Linux, Windows, and macOS run package-consumer qualification; Android is compile/link qualified.

Exact product dependencies:

- `Salts.Native 1.8.0`
- `SaltsUtils.Native 4.1.0`

Third-party dependencies come from this repository's root `vcpkg.json`; `qigao/vcpkg-cache` supplies only the shared vcpkg setup/cache infrastructure.

CI package versions use:

```text
<flowmq-version>-ci.<github-run-number>.<run-attempt>
```

Each SDK profile records source/dependency provenance in `flowmq-sdk-manifest.txt`.
