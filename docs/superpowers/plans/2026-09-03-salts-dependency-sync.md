# Salts Dependency Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace FlowMQ's Rocida and TurboParser dependencies with the installed Salts and SaltsUtils Debug/Release SDKs, without compatibility aliases or default-path fallback.

**Architecture:** FlowMQ resolves `Salts` and `SaltsUtils` from profile-specific environment roots and links their exported `Salts::*` targets. FMP code generation uses SaltsUtils' header-only TBE wire output; FlowMQ validates borrowed wire views directly and no longer exposes or links the removed DataBind typed codec.

**Tech Stack:** C11, CMake Presets, Salts Core/CMeta/CSTL/CNet/TinyTest, SaltsUtils TBE wire/schema compiler, MSVC AddressSanitizer, CTest.

**Spec:** `AGENTS.md`, `CMakeUserPresets.json`, installed package contracts under `C:/projects/cpp/external/pkgs/salts/{debug,release}` and `C:/projects/cpp/external/pkgs/salts-utils/{debug,release}`, and SaltsUtils commit `d4f4729`.

## Global Constraints

- Windows profiles use `salts/debug|release` and `salts-utils/debug|release`; Android profiles use the corresponding `*-android` roots.
- Map `Rocida::Core`, `Rocida::CMeta`, `Rocida::STL`, `Rocida::CNet`, and `Rocida::TinyTest` to `Salts::Core`, `Salts::CMeta`, `Salts::CSTL`, `Salts::CNet`, and `Salts::TinyTest`.
- Define `SALTS_ROOT` and `SALTS_UTILS_ROOT` exactly once per profile; do not add either to `CMAKE_PREFIX_PATH`.
- Resolve first-party packages with explicit roots and `NO_DEFAULT_PATH`; missing or incompatible packages fail during configure.
- Do not create Rocida/TurboParser aliases or patch installed packages.
- Preserve the FMP/1 schema and binary prefix. The removed DataBind JSON/owned-record API is intentionally replaced by zero-copy TBE wire views/builders.

### Task 1: Verify the installed SDK boundary

**Files:**
- Read: `C:/projects/cpp/external/pkgs/salts/{debug,release}/lib/cmake/Salts/SaltsTargets.cmake`
- Read: `C:/projects/cpp/external/pkgs/salts-utils/{debug,release}/lib/cmake/SaltsUtils/SaltsUtilsTargets.cmake`
- Read: SaltsUtils commit `d4f4729`

- [x] Verify Salts exports Core, CMeta, CSTL, CNet, TinyTest, and concrete parser targets.
- [x] Verify SaltsUtils exports `Salts::TbeSchema` and installs `tbe_compiler`.
- [x] Verify SaltsUtils deliberately removes TurboParser Parser/DataBind and that header-only C generation avoids `data_bind.h` and `tbe_typed.h`.

### Task 2: Migrate package roots and target graph

**Files:**
- Modify: `CMakeUserPresets.json`
- Modify: `CMakeLists.txt`
- Modify: `cmake/CmakeUtils.cmake`
- Modify: module, test, benchmark, and media-provider CMake files

- [x] Rename profile roots to `SALTS_ROOT` and `SALTS_UTILS_ROOT`.
- [x] Add strict root validation and explicit `find_package(Salts)` / `find_package(SaltsUtils)` calls.
- [x] Replace all imported targets with their canonical `Salts::*` targets.
- [x] Resolve `tbe_compiler` only from `SALTS_UTILS_ROOT/bin`.

### Task 3: Migrate FMP to the current TBE boundary

**Files:**
- Modify: `flowmq/extensions/media_provider/CMakeLists.txt`
- Modify: `flowmq/include/flowmq_media_provider.h`
- Modify: `flowmq/extensions/media_provider/src/flowmq_media_provider.c`
- Modify: `flowmq/tests/media_provider/test_flowmq_media_provider_schema.c`

- [x] Stop requesting the removed typed companion source from `tbe_compiler`.
- [x] Accept generated `Provider*V1_view_t` values in validators and validate borrowed TBE slices without allocation.
- [x] Replace DataBind JSON round-trip tests with TBE builder/view tests that cover valid, invalid, truncated, and routing cases.
- [x] Keep schema id/version, enum values, field order, and fixed `schema_version + message_kind` prefix unchanged.

### Task 4: Update package metadata and documentation

**Files:**
- Modify: `cmake/FlowMQConfig.cmake.in`
- Modify: active README/architecture/protocol documentation and `AGENTS.md`

- [x] Export strict Salts and SaltsUtils dependency discovery.
- [x] Describe `Salts::TbeSchema` as the public header dependency and the wire-view FMP API.
- [x] Require no active Rocida or TurboParser references outside historical plans.

### Task 5: Verify Debug, Release, install, and performance

- [x] List configure/build/test presets.
- [x] Fresh configure, clean build, and test Release.
- [x] Fresh configure, clean build, and test Debug/ASan.
- [x] Install Release and verify a package consumer resolves Salts and SaltsUtils.
- [x] Run the opt-in ZMQ comparison three times in Release and report medians.
