# Global Protocol Refactor Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 将 FMQ、FMS、FES 与 FMP 收敛到一个不可变全局协议目录，删除 FMQ/5 与旧 ESB transport 扩展，只保留类 ZeroMQ socket wire。

**Architecture:** FMQ/6 是唯一 transport framing；FMS/3 是 HELLO security envelope；FES/1 与 FMP/1 都是 FMQ DATA 上层协议。公开目录只读且无分配，CMeta 仅在内部生成目录与固定字段逻辑，socket hot path 不使用 CFlow 或动态 dispatch。

**Tech Stack:** C11、TurboUtils/Rocida Core、CMeta Schema/Replay、TurboParser DataBind/TBE、TinyTest、CMake Presets。

---

### Task 1: Global immutable protocol catalog

**Files:**
- Create: `flowmq/protocol/include/flowmq_protocol_catalog.h`
- Create: `flowmq/protocol/src/flowmq_protocol_catalog.c`
- Create: `flowmq/protocol/src/flowmq_protocol_catalog_schema.h`
- Modify: `flowmq/protocol/CMakeLists.txt`
- Test: `flowmq/protocol/tests/test_flowmq_protocol.c`

- [x] Add failing catalog tests for all four family/version/layer/magic rows and invalid lookup.
- [x] Define public family/layer/capability constants without exposing CMeta headers.
- [x] Generate one immutable descriptor table from a private CMeta Schema/Replay definition.
- [x] Run `test_flowmq_protocol` and confirm the new tests pass.

### Task 2: Make FMQ/6 transport-only

**Files:**
- Modify: `flowmq/protocol/include/flowmq_protocol.h`
- Modify: `flowmq/protocol/src/flowmq_protocol.c`
- Modify: `patterns/src/flowmq_pattern.c`
- Modify: `patterns/tests/test_flowmq_pattern.c`
- Test: `flowmq/protocol/tests/test_flowmq_protocol.c`

- [x] Add failing tests that FMQ/5, former ESB pattern values, and former ESB frame kinds are rejected.
- [x] Move FMQ version and public pattern/frame definitions under the catalog contract.
- [x] Delete ESB branches and trailing-TLV consumption from the core codec and pattern validator.
- [x] Run protocol and pattern tests.

### Task 3: Split FMS/3 security

**Files:**
- Create: `flowmq/protocol/include/flowmq_security.h`
- Create: `flowmq/protocol/src/flowmq_security.c`
- Modify: `flowmq/protocol/include/flowmq_protocol.h`
- Modify: `flowmq/protocol/src/flowmq_protocol.c`
- Modify: `patterns/src/flowmq_pattern.c`
- Modify: `flowmq/protocol/tests/test_flowmq_protocol.c`

- [x] Change tests to the dedicated `flowmq_security_*` API and observe the compile failure.
- [x] Move FMS constants, types, validation and codec out of FMQ core.
- [x] Delete old `flowmq_protocol_security_*` symbols rather than adding aliases.
- [x] Run protocol and handshake tests.

### Task 4: Replace ESB transport extension with FES/1 payload protocol

**Files:**
- Delete: `flowmq/protocol/include/flowmq_protocol_esb.h`
- Delete: `flowmq/protocol/src/flowmq_protocol_esb.c`
- Delete: `flowmq/protocol/tests/test_flowmq_protocol_esb.c`
- Create: `flowmq/protocol/include/flowmq_esb.h`
- Create: `flowmq/protocol/src/flowmq_esb.c`
- Create: `flowmq/protocol/tests/test_flowmq_esb.c`
- Modify: `flowmq/protocol/CMakeLists.txt`
- Modify: `flowmq/protocol/tests/CMakeLists.txt`
- Modify: `patterns/src/flowmq_scatter_gather.h`
- Modify: `patterns/src/flowmq_stream_partition.h`
- Modify: `patterns/src/flowmq_saga.h`

- [x] Write failing FES/1 tests for scatter, stream, saga, priority, circuit-breaker and embedding inside FMQ DATA.
- [x] Define a transport-neutral message struct, fixed header, strict kind-specific TLV schema and borrowed decode views.
- [x] Implement bounded encode/decode with network byte order, duplicate/unknown/trailing rejection and no compatibility mode.
- [x] Remove local-state dependencies on the deleted transport extension.
- [x] Run FES and adjacent pattern-state tests.

### Task 5: Unify media-provider version ownership

**Files:**
- Modify: `flowmq/include/flowmq_media_provider.h`
- Modify: `application/src/flowmq_media_provider.c`
- Modify: `application/schema/flowmq_media_provider_v1.schema`
- Modify: `application/tests/test_flowmq_media_provider_schema.c`

- [x] Add a test that FMP/1 routing uses the global catalog version.
- [x] Replace hard-coded schema versions in validators with the global FMP version constant.
- [x] Rename the documented family from ambiguous FMS/1 to FMP/1 and correct the stale FMQ v3 statement.
- [x] Run media-provider schema tests.

### Task 6: Public surface, docs and verification

**Files:**
- Modify: `flowmq/include/flowmq.h`
- Modify: `README.md`
- Modify: `flowmq/README.md`
- Modify: `docs/PROTOCOL_SPEC.md`
- Modify: `docs/FMQ_WIRE_PROTOCOL.md`
- Modify: `docs/ARCHITECTURE.md`
- Modify/Delete: stale ESB documentation that describes removed APIs.

- [x] Export catalog/security/FES headers through the aggregate public surface and package install lists.
- [x] Update normative documents to FMQ/6, FMS/3, FES/1 and FMP/1; delete stale ESB implementation documents.
- [x] Run CodeGraph affected analysis for changed protocol files.
- [x] Configure/build/test Debug with ASan, then configure/build/test Release.
- [x] Run the release FlowMQ/libzmq benchmark three times and report medians.
- [x] Review the final diff for stale old symbols, versions, generated artifacts and unrelated changes.
