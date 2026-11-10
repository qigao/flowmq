# FlowMQ.Native

The package contains Release SDK profiles for Linux x64, Windows x64 (llvm-mingw),
macOS arm64, and Android arm64-v8a (API 26). CI runs native CTests on the host
profiles and compiles and links the Android profile.

Each profile's `flowmq-sdk-manifest.txt` records the source commit and the exact
SaltsBase, CMeta, SaltsCore (salts-cnet), and SaltsUtils versions. Restore the
matching SDKs and provide their roots to CMake through `SALTS_BASE_ROOT`,
`CMETA_ROOT`, `SALTSCORE_ROOT`, and `SALTS_UTILS_ROOT`. Provide the matching
third-party dependency prefix through `CMAKE_PREFIX_PATH`. Deploy the CNet
runtime library alongside the FlowMQ, SaltsBase, and DataBind runtime libraries.

PR builds use `<flowmq-version>-ci.<github-run-number>.<run-attempt>` and upload
the package as an Actions artifact. A pushed tag matching the FlowMQ project
version publishes `FlowMQ.Native` to GitHub Packages and Releases.
