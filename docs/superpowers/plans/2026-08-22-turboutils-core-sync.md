# TurboUtils Core Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make FlowMQ compile and test against the installed TurboUtils package after its public export macros, string aliases, and containers changed.

**Architecture:** Keep FlowMQ's public ABI and protocol behavior unchanged. Give FlowMQ its own producer/consumer visibility macros modeled on TurboUtils, migrate owned/view strings mechanically to `tstr`/`vstr`, and treat `TurboUtils::STL` as a private implementation dependency with explicit raw-byte ownership and capacity limits.

**Tech Stack:** C11, CMake Presets, MSVC/Ninja, TurboUtils::Core, TurboUtils::STL, TurboUtils::TinyTest.

**Spec:** User request in the 2026-08-22 session plus repository `AGENTS.md`.

## Global Constraints

- Preserve current public function names, struct layouts, wire formats, return-value semantics, and ownership.
- Use `FLOWMQ_API`/`FLOWMQ_C_API` for FlowMQ DLL state; do not reuse `TURBO_API`, which is `dllimport` for Core consumers.
- Replace legacy `tstr_t`/`tstr_v` and `tstr_v_*` with installed `tstr`/`vstr` and `vstr_*`.
- Link container consumers to `TurboUtils::STL`; include only installed `turbostl/*` headers.
- Initialize raw containers with size, alignment, and an existing configuration-derived hard limit.
- Convert `turbo_stl_status` failures at FlowMQ boundaries to existing `TURBO_E*` results.

---

### Task 1: Public ABI macros and strings

**Files:**
- Create: `flowmq/protocol/include/flowmq_export.h`
- Modify: `CMakeLists.txt`
- Modify: `flowmq/CMakeLists.txt`
- Modify: `flowmq/protocol/CMakeLists.txt`
- Modify: all public headers reported by `rg.exe -l '\bCXX_C_API\b' flowmq/include flowmq/protocol/include`
- Modify: C sources, tests, examples, benchmarks, and maintained documentation reported by `rg.exe -l -e '\btstr_t\b' -e '\btstr_v\b' -e '\btstr_v_' .`

**Interfaces:**
- Consumes: `TURBO_API` only through TurboUtils declarations.
- Produces: `FLOWMQ_API`, `FLOWMQ_C_API`, and public FlowMQ signatures using `tstr`/`vstr`.

- [x] **Step 1: Verify the compatibility regression fails**

Run:
```powershell
cmake --build --preset win-release-user --target flowmq_protocol
```

Expected: compilation fails on missing `tstr_t`, `tstr_v`, and `CXX_C_API`.

- [x] **Step 2: Add FlowMQ-owned visibility composition**

`flowmq_export.h` defines a default ELF visibility `FLOWMQ_API`, composes `FLOWMQ_C_API` with `extern "C"`, and leaves Windows producer/consumer state to CMake. The shared `flowmq` target receives `FLOWMQ_API=__declspec(dllexport)` privately and publishes `FLOWMQ_API=__declspec(dllimport)` to Windows consumers.

- [x] **Step 3: Migrate legacy string names**

Apply exact token mappings:
```text
tstr_t              -> tstr
tstr_v              -> vstr
tstr_v_from_buf     -> vstr_from_buf
tstr_v_from_cstr    -> vstr_from_cstr
tstr_v_eq           -> vstr_eq
tstr_v_utf8_valid   -> vstr_utf8_valid
TSTR_NULL           -> NULL
```

Do not edit `AGENTS.md`; it is repository policy supplied by the user rather than product documentation.

- [x] **Step 4: Build and test the protocol slice**

Run:
```powershell
cmake --build --preset win-release-user --target flowmq_protocol test_flowmq_protocol test_flowmq_protocol_esb
ctest --preset win-release-user -R "flowmq_protocol"
```

Expected: both protocol tests pass and public declarations compile with the new aliases.

### Task 2: TurboUtils::STL container migration

**Files:**
- Modify: `CMakeLists.txt`, `flowmq/CMakeLists.txt`, `patterns/CMakeLists.txt`
- Modify: `flowmq/src/flow_fmq.c`, `flowmq/src/fmq_broker.c`, `flowmq/src/fmq_deployment.c`, `flowmq/src/fmq_pubsub.c`, `flowmq/src/fmq_retry.c`
- Modify: `patterns/src/flowmq_priority_queue.h`, `patterns/src/flowmq_priority_queue.c`, `patterns/src/flowmq_router_endpoint.c`, `patterns/src/flowmq_saga.h`, `patterns/src/flowmq_saga.c`, `patterns/src/flowmq_scatter_gather.h`, `patterns/src/flowmq_scatter_gather.c`, `patterns/src/flowmq_stream_partition.h`, `patterns/src/flowmq_stream_partition.c`, `patterns/src/flowmq_subscription_set.h`, `patterns/src/flowmq_subscription_set.c`

**Interfaces:**
- Consumes: `TurboUtils::STL`, `turbostl/vec.h`, `deque.h`, `hash_map.h`, `hash_set.h`, and `typed.h`.
- Produces: the same FlowMQ public behavior with STL-private storage.

- [x] **Step 1: Replace removed container headers and add target linkage**

Use specific `turbostl/*` headers for raw types and `turbostl/typed.h` where `TURBO_*_DEFINE` is used. Link `flowmq_core`, `flowmq_transport`, and the shared `flowmq` target privately to `TurboUtils::STL`.

- [x] **Step 2: Migrate raw initializers**

For vectors/deques use `*_init_bytes(handle, sizeof(type), _Alignof(type), limit)`. For hash maps use `turbo_hash_map_init_bytes` with key/value size and alignment, the existing hash/equality callbacks, and the domain capacity. Migrate the legacy hash-backed set to `turbo_hash_set_t`/`turbo_hash_set_*`.

- [x] **Step 3: Preserve error semantics and rollback paths**

Compare STL results with `TURBO_STL_OK`; keep existing FlowMQ-facing `TURBO_ENOMEM`, `TURBO_ENOSPC`, and `TURBO_EPROTO` conversions. Check every push/put/resize used in transactional rollback paths.

- [x] **Step 4: Build and run adjacent container-owning tests**

Run:
```powershell
cmake --build --preset win-release-user --target flowmq_core flowmq_transport
ctest --preset win-release-user -R "priority_queue|subscription_set|stream_partition|saga|scatter_gather|broker|pubsub|retry|deployment"
```

Expected: selected tests pass with no legacy include or initializer diagnostics.

### Task 3: Full compatibility verification

**Files:**
- Modify only files required by compiler/test findings from Tasks 1-2.

**Interfaces:**
- Consumes: completed public ABI/string and private STL migrations.
- Produces: a buildable/installable FlowMQ package against the current TurboUtils exports.

- [x] **Step 1: Reconfigure and build every default target**

Run:
```powershell
cmake --fresh --preset win-release-user
cmake --build --preset win-release-user
```

Expected: exit code 0 with no compiler errors.

- [x] **Step 2: Run the complete Release test preset**

Run:
```powershell
ctest --preset win-release-user
```

Expected: zero failed tests.

- [x] **Step 3: Verify installation/export surface**

Run:
```powershell
cmake --install build/Msvc-Release --prefix build/install-check-core-sync
```

Expected: `flowmq_export.h` is installed and `FlowMQ::FlowMQ` exports the consumer-side `FLOWMQ_API` definition.

- [x] **Step 4: Enforce legacy-symbol and diff gates**

Run:
```powershell
rg.exe -n -g '!AGENTS.md' -e '\bCXX_C_API\b' -e '\btstr_t\b' -e '\btstr_v\b' -e '\btstr_v_' -e '#include[[:space:]]*[<"]turbo_(containers|vec|hash|set|deque|heap)\.h' .
git diff --check
```

Expected: `rg.exe` finds no maintained source/documentation matches, and `git diff --check` exits 0.
