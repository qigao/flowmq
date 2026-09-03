# FlowMQ Source Layout Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align FlowMQ's physical source tree and CMake ownership with the existing Protocol/Core/Transport architecture while preserving the public API, FMQ wire format, caller-driven execution model, and measured data-path behavior.

**Architecture:** Consolidate production code below `flowmq/`, keep installed public headers flat, and group private implementation by protocol, core pattern/session state, runtime, transport, and security responsibility. Remove the advanced ESB helper modules that have no production callers instead of continuing to compile them into `FlowMQ::Core`; keep FES/1 and FMP/1 protocol/schema support.

**Tech Stack:** C11, CMake Presets, TinyTest/CTest, Rocida CMeta/STL/CNet, TurboParser DataBind/TBE.

**Spec:** `docs/ARCHITECTURE.md` and `docs/GLOBAL_PROTOCOL_ARCHITECTURE.md`

## Global Constraints

- Preserve all installed header names and all existing public function signatures.
- Preserve FMQ/6, FMS/3, FES/1, and FMP/1 wire/schema behavior.
- Preserve caller-driven single-owner progress; do not introduce threads, actors, reactive APIs, factories, or runtime backend indirection.
- Keep `flowmq_socket.c` behavior intact in this mechanical phase; file decomposition requires a separate benchmarked change.
- Keep `CMakeUserPresets.json` version-controlled and validate with the public Windows Release and development presets.
- Do not modify or depend on the sibling TurboUtils working tree's uncommitted changes.

---

### Task 1: Establish the consolidated production layout

**Files:**
- Move: `flowmq/protocol/include/*.h` to `flowmq/include/`
- Move: `flowmq/protocol/src/*` to `flowmq/src/protocol/`
- Move: `patterns/src/flowmq_pattern*` and `flowmq_subscription_set*` to `flowmq/src/core/pattern/`
- Move: `patterns/src/flowmq_flow_control*` and `flowmq_reconnect*` to `flowmq/src/core/session/`
- Move: `patterns/src/flowmq_stream_decoder*` to `flowmq/src/protocol/stream/`
- Move: `patterns/src/flowmq_core.c` to `flowmq/src/core/`
- Move: `patterns/src/flowmq_socket.c` to `flowmq/src/runtime/`
- Move: `patterns/src/flowmq_transport.c` to `flowmq/src/transport/`
- Move: `patterns/src/flowmq_cnet_transport*` to `flowmq/src/transport/cnet/`
- Move: `patterns/src/flowmq_tls_identity_map.c` to `flowmq/src/security/`
- Move: `application/schema`, `application/src`, and its public tests to `flowmq/extensions/media_provider/`

**Interfaces:**
- Consumes: the existing flat include names such as `flowmq_protocol.h`, `flowmq_socket.h`, and `flowmq_media_provider.h`.
- Produces: the same include names and C symbols from responsibility-aligned private directories.

- [ ] **Step 1: Create each destination directory explicitly.**

Run `New-Item -ItemType Directory -Force` only for the exact directories listed above and verify each resolved path is below the FlowMQ repository root.

- [ ] **Step 2: Move tracked production files without changing their contents.**

Use explicit source/destination pairs so no wildcard can move unrelated files. Verify `git diff --find-renames --stat` reports renames rather than content rewrites.

- [ ] **Step 3: Confirm include-name stability.**

Run:

```powershell
rg.exe -n '^#include "flowmq_' flowmq examples
```

Expected: all project includes continue to use the existing flat header names; no source includes repository-relative paths.

- [ ] **Step 4: Commit the pure file relocation.**

```text
refactor(layout): align sources with module ownership
```

### Task 2: Remove unintegrated advanced ESB helpers

**Files:**
- Delete: `patterns/src/flowmq_circuit_breaker.*`
- Delete: `patterns/src/flowmq_priority_queue.*`
- Delete: `patterns/src/flowmq_saga.*`
- Delete: `patterns/src/flowmq_scatter_gather.*`
- Delete: `patterns/src/flowmq_stream_partition.*`
- Delete: their five matching test files
- Delete: `flowmq/ESB_ADVANCED_PATTERNS.md`
- Delete: `flowmq/ESB_INTEGRATION_TEST_PLAN.md`

**Interfaces:**
- Consumes: repository evidence that each helper is referenced only by its own implementation and private test.
- Produces: a core library containing only behavior used by the ZeroMQ-style socket/runtime or installed protocol/schema APIs.

- [ ] **Step 1: Re-run the production-caller scan.**

```powershell
rg.exe -l --glob '*.c' --glob '*.h' '#include "flowmq_(circuit_breaker|priority_queue|saga|scatter_gather|stream_partition)\.h"' .
```

Expected: only each implementation and matching test appear.

- [ ] **Step 2: Delete the private helper implementations, tests, and stale helper-specific documents.**

Use an explicit patch containing every deleted path; do not recursively delete `patterns/`.

- [ ] **Step 3: Confirm no symbol or document reference remains.**

```powershell
rg.exe -n 'flowmq_(circuit_breaker|priority_queue|saga|scatter_gather|stream_partition)|ESB_ADVANCED_PATTERNS|ESB_INTEGRATION_TEST_PLAN' .
```

Expected: no matches outside historical plan documents.

- [ ] **Step 4: Commit the dead-module removal.**

```text
refactor(core): remove unintegrated ESB helpers
```

### Task 3: Make CMake targets own their physical modules

**Files:**
- Create: `flowmq/src/protocol/CMakeLists.txt`
- Create: `flowmq/src/core/CMakeLists.txt`
- Create: `flowmq/src/transport/CMakeLists.txt`
- Create: `flowmq/extensions/media_provider/CMakeLists.txt`
- Modify: `flowmq/CMakeLists.txt`
- Modify: root `CMakeLists.txt`
- Remove: obsolete `flowmq/protocol/CMakeLists.txt`, `patterns/CMakeLists.txt`, and `application/CMakeLists.txt`

**Interfaces:**
- Consumes: existing targets `FlowMQ::Protocol`, `FlowMQ::Core`, `FlowMQ::Transport`, and the final `FlowMQ::FlowMQ` shared library.
- Produces: the same target aliases and link direction, with source manifests declared beside owned files.

- [ ] **Step 1: Recreate the Protocol target beside `flowmq/src/protocol`.**

Keep `Rocida::Core` public and `Rocida::CMeta` private. Export the protocol source/header lists to the final shared target without adding network dependencies.

- [ ] **Step 2: Recreate the Core target beside `flowmq/src/core`.**

Compile pattern/session/security sources, link `FlowMQ::Protocol` and `Rocida::Core` publicly, and keep `Rocida::STL` private.

- [ ] **Step 3: Recreate the Transport target beside `flowmq/src/transport`.**

Compile runtime/socket, transport configuration, and the thin CNet adapter; retain the private `Rocida::CNet` dependency and the current internal segmented-encoder include boundary.

- [ ] **Step 4: Relocate media-provider generation beside its extension.**

Preserve schema id/version, generated file names, `flowmq_media_provider_codegen`, install destinations, and TurboParser tool invocation exactly.

- [ ] **Step 5: Update the root shared-library source aggregation.**

Keep one installed shared target and the existing public/private link dependencies. Do not introduce object libraries or backend function tables in this phase.

- [ ] **Step 6: Configure from the public Release preset.**

```text
cmake --fresh --preset win-release-user
```

Expected: configure succeeds with the same dependency roots and generated FMP/1 outputs.

- [ ] **Step 7: Commit the target ownership update.**

```text
refactor(cmake): colocate FlowMQ module targets
```

### Task 4: Align tests, benchmarks, and architecture documentation

**Files:**
- Move: protocol tests to `flowmq/tests/protocol/`
- Move: core/runtime/transport tests to matching `flowmq/tests/` subdirectories
- Move: protocol/runtime benchmarks to `flowmq/benchmarks/` subdirectories
- Create/Modify: consolidated test and benchmark `CMakeLists.txt` files
- Modify: `docs/ARCHITECTURE.md`
- Modify: `docs/GLOBAL_PROTOCOL_ARCHITECTURE.md`
- Modify: any README path references returned by `rg.exe`

**Interfaces:**
- Consumes: existing TinyTest executable names, CTest labels, benchmark target names, and architecture contracts.
- Produces: unchanged test/benchmark CLI names with paths matching the production modules they validate.

- [ ] **Step 1: Move tests and benchmarks by ownership.**

Preserve source contents and target names; update only path-based CMake registration and IDE folder grouping.

- [ ] **Step 2: Update architecture documentation.**

Document the physical layout, keep the single-owner/CNet and hot-path CFlow/CMeta boundaries unchanged, and remove references to the deleted private ESB helpers.

- [ ] **Step 3: Run targeted module tests.**

```text
ctest --preset win-release-user -L flowmq-protocol
ctest --preset win-release-user -L flowmq-core
ctest --preset win-release-user -L flowmq-transport
```

Expected: all remaining module tests pass.

- [ ] **Step 4: Run the complete Release suite.**

```text
cmake --build --preset win-release-user
ctest --preset win-release-user
```

Expected: every registered FlowMQ test passes.

- [ ] **Step 5: Run the development/ASan suite.**

```text
cmake --fresh --preset win-dev-user
cmake --build --preset win-dev-user
ctest --preset win-dev-user
```

Expected: every registered FlowMQ test passes without sanitizer findings.

- [ ] **Step 6: Compare the Release socket benchmark against the pre-refactor baseline.**

Run the existing `bench_flowmq_socket` target with the same payload filters and sample counts used by the committed benchmark. Treat noise as inconclusive; any repeatable regression requires reverting the structural change that caused it.

- [ ] **Step 7: Commit the test and documentation alignment.**

```text
refactor(layout): align verification with FlowMQ modules
```

### Task 5: Final structural verification

**Files:**
- Verify: all changed and deleted paths
- Verify: generated/install manifests

**Interfaces:**
- Consumes: completed Tasks 1-4.
- Produces: a reviewable directory-only refactor with no public or runtime semantic changes.

- [ ] **Step 1: Confirm obsolete top-level source directories are empty or gone.**

```powershell
fd.exe -td -d 3 .
```

Expected: no tracked production files remain below top-level `patterns/` or `application/`.

- [ ] **Step 2: Inspect rename detection and public surface.**

```text
git diff --find-renames --stat
git diff -- flowmq/include
```

Expected: public header contents are unchanged apart from path relocation; no public signature or enum value changes.

- [ ] **Step 3: Scan for stale paths and forbidden placeholders.**

```powershell
rg.exe -n 'patterns/src|application/src|flowmq/protocol/(src|include)|TODO|FIXME|HACK' CMakeLists.txt flowmq docs examples
```

Expected: no stale source path and no new placeholder comment.

- [ ] **Step 4: Review the final diff and commit only after all verification evidence is recorded.**

```text
refactor(layout): consolidate FlowMQ module structure
```
