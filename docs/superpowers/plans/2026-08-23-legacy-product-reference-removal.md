# Legacy Product Reference Removal Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remove obsolete product-layer source, compatibility naming, examples, and documentation while retaining the current FlowMQ protocol/runtime and its supported benchmark.

**Architecture:** The supported surface remains the root CMake target graph, public `flowmq_*` headers, protocol implementation, CoroNet runtime, and ORM-backed application layer. Historical source trees that are absent from target and install lists are deleted; the one compiled compatibility shim is folded into direct protocol names.

**Tech Stack:** C11, CMake Presets, TurboUtils, TurboParser, TurboNet, TurboDB ORM, TinyTest.

---

### Task 1: Remove dead build and source references

**Files:**
- Modify: `cmake/CmakeUtils.cmake`
- Delete: obsolete unbuilt headers, sources, examples, and subsystem-only documents
- Delete: obsolete examples and their unused configuration

1. Confirm deleted files are absent from current target and install source lists.
2. Delete the stale external-runtime dependency hook.
3. Delete the historical files with `apply_patch`.
4. Run `rg.exe` across CMake and source lists to ensure no deleted path remains referenced.

### Task 2: Fold the protocol compatibility shim into native names

**Files:**
- Modify: `flowmq/protocol/src/flowmq_protocol.c`
- Modify: `flowmq/protocol/tests/test_flowmq_protocol.c`
- Modify: `patterns/tests/test_flowmq_core.c`
- Modify: `patterns/tests/test_flowmq_pattern.c`

1. Remove compatibility constants, typedefs, and function-name macros.
2. Rename internal helpers and definitions directly to the public protocol namespace.
3. Preserve encoded bytes, validation rules, ownership, and error codes.
4. Build and run protocol tests.

### Task 3: Remove obsolete examples and documentation claims

**Files:**
- Modify: `README.md`
- Modify: `flowmq/README.md`
- Modify: current protocol and architecture documents
- Delete: documents that describe only removed subsystems

1. Remove claims and links to removed implementations.
2. Keep current wire protocol, runtime ownership, and build instructions intact.
3. Rename the retained protocol benchmark target to match its source.
4. Scan tracked text for old product identifiers and deleted paths.

### Task 4: Verify supported build surface

**Files:**
- Verify: `CMakeUserPresets.json`
- Verify: `presets/Compilers.json`
- Verify: root and component `CMakeLists.txt`

1. Configure the relevant debug preset.
2. Build the protocol/runtime targets and benchmark when enabled.
3. Run the closest CTest suites.
4. Re-run zero-reference scans and inspect `git diff --check` plus final status.
