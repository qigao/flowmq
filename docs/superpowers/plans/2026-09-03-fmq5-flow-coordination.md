# FMQ/5 Flow Coordination Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 FlowMQ TCP/TLS wire 直接升级到 FMQ/5，以强制 SETTINGS 协商和接收方累计 credit 协调发送速率。

**Architecture:** 固定 header 保持 32 bytes，新控制帧由 CMeta schema 生成布局与 codec；每个 peer 在 caller-driven owner lane 内持有双向累计 credit 状态。控制帧走优先直写路径，DATA admission 同时受本地 HWM 和远端 credit 约束，不引入线程、锁、FEC 或 CFlow 热路径。

**Tech Stack:** C11、CMeta Schema/Replay、CNet TCP/TLS、TinyTest、CMake Presets、MSVC/ASan、libzmq benchmark

**Spec:** `docs/FMQ5_FLOW_COORDINATION.md`

## Global Constraints

- FMQ wire version 固定为 `5`，不得接受、协商或回退到 FMQ/4。
- TCP/TLS 不发送 FEC repair symbol。
- socket 和 peer 状态只由调用 `send`、`recv` 或 `poll` 的 owner lane 推进。
- 控制帧不消耗 data credit，并优先于 queued DATA。
- CFlow 不进入 codec、admission 或 per-message 热路径。
- 所有新增协议错误 fail fast 为明确 Turbo error。

---

### Task 1: FMQ/5 control frame codec

**Files:**
- Create: `flowmq/protocol/src/flowmq_protocol_control_schema.h`
- Modify: `flowmq/protocol/include/flowmq_protocol.h`
- Modify: `flowmq/protocol/src/flowmq_protocol.c`
- Modify: `flowmq/protocol/CMakeLists.txt`
- Test: `flowmq/protocol/tests/test_flowmq_protocol.c`

**Interfaces:**
- Consumes: CMeta `Schema`/`Replay` and existing big-endian packet helpers.
- Produces: `flowmq_protocol_settings_encode/decode()` and `flowmq_protocol_flow_update_encode/decode()` with fixed 32/24-byte payloads.

- [x] **Step 1: Write failing protocol tests**

Add TinyTest cases that round-trip every field, reject zero/unknown SETTINGS values, reject malformed FLOW_UPDATE values, and verify SETTINGS/FLOW_UPDATE frame constraints.

- [x] **Step 2: Run the focused test and verify RED**

Run: `cmake --build --preset win-dev-user --target test_flowmq_protocol && ctest --preset win-dev-user -R ^test_flowmq_protocol$ --output-on-failure`

Expected: compile failure because the FMQ/5 control structs and functions do not exist.

- [x] **Step 3: Add the CMeta schema and public protocol types**

Define schema rows `(member, width, offset)` for SETTINGS and FLOW_UPDATE, generate offsets and codec statements with `Replay`, bump API version to `3`, wire version to `5`, and assign frame kinds `32` and `33` after the ESB-reserved `7..31` range.

- [x] **Step 4: Implement strict encode/decode and frame validation**

Require exact payload sizes and semantic invariants; update core-kind validation without treating the ESB `7..19` range as core kinds.

- [x] **Step 5: Re-run the focused protocol test**

Expected: `test_flowmq_protocol` passes under the Debug/ASan preset.

### Task 2: Deterministic cumulative-credit state

**Files:**
- Create: `patterns/src/flowmq_flow_control.h`
- Create: `patterns/src/flowmq_flow_control.c`
- Create: `patterns/tests/test_flowmq_flow_control.c`
- Modify: `patterns/CMakeLists.txt`
- Modify: `patterns/tests/CMakeLists.txt`

**Interfaces:**
- Consumes: negotiated SETTINGS values and monotonic nanosecond timestamps.
- Produces: owner-only helpers for initialization, send admission/commit, receive commit, application consumption, update scheduling, and remote FLOW_UPDATE validation.

- [x] **Step 1: Write failing state-machine tests**

Cover exact credit exhaustion, one-byte overflow rejection, update by quantum, update by 10 ms deadline, monotonic remote update validation, generation fencing, and `UINT64_MAX` overflow.

- [x] **Step 2: Run the focused test and verify RED**

Run the new `test_flowmq_flow_control` target; expect compile failure because the internal API is absent.

- [x] **Step 3: Implement the minimal owner-only state machine**

Use checked subtraction/addition, cumulative counters and absolute deadlines only. Do not allocate, lock, log or call transport from the module.

- [x] **Step 4: Re-run the focused state-machine test**

Expected: all credit state tests pass under Debug/ASan.

### Task 3: Socket handshake and send-side credit

**Files:**
- Modify: `patterns/src/flowmq_socket.c`
- Test: `patterns/tests/test_flowmq_socket.c`

**Interfaces:**
- Consumes: Task 1 control codec and Task 2 credit state.
- Produces: strict `HELLO -> SETTINGS -> READY`, DATA admission fenced by negotiated frame size and cumulative credit.

- [x] **Step 1: Add failing loopback tests**

Add cases proving peers are not ready before SETTINGS, DATA stops at receiver credit, `poll(POLLOUT)` drops when credit is exhausted, and application receive plus owner progress restores credit.

- [x] **Step 2: Run `test_flowmq_socket` and verify RED**

Expected: new tests fail because current readiness only checks HELLO and send admission ignores remote credit.

- [x] **Step 3: Implement SETTINGS progression and validation**

Derive a nonzero local generation from the CNet connection handle; send SETTINGS after HELLO completion; reject duplicate/out-of-order control and DATA frames.

- [x] **Step 4: Integrate whole-message send credit**

Check remote frame limit and remaining credit before admission. Charge one-part messages immediately and multipart only when the staged transaction commits. Preserve PUB filtering and PUSH/REQ/REP FSM semantics.

- [x] **Step 5: Re-run socket tests**

Expected: handshake and send-credit tests pass without regressing existing pattern tests.

### Task 4: Receive accounting and FLOW_UPDATE progression

**Files:**
- Modify: `patterns/src/flowmq_socket.c`
- Test: `patterns/tests/test_flowmq_socket.c`

**Interfaces:**
- Consumes: local receive window and Task 2 update scheduling.
- Produces: DATA receive limit enforcement and priority FLOW_UPDATE emission after actual application consumption.

- [x] **Step 1: Add failing receive-accounting tests**

Cover multipart payload accounting, ROUTER identity exclusion, update coalescing, interval-triggered small update, stale generation rejection, and malicious DATA beyond advertised credit.

- [x] **Step 2: Run `test_flowmq_socket` and verify RED**

Expected: receive accounting tests fail because queued messages do not retain wire-credit byte counts.

- [x] **Step 3: Attach credit metadata to queued messages**

Store peer index, local generation and actual DATA payload byte count; increment consumed credit only after successful whole-message receive.

- [x] **Step 4: Emit priority cumulative FLOW_UPDATE**

When quantum or deadline is reached, encode one FLOW_UPDATE and send it before queued DATA. Advance advertised credit only after CNet accepts the control write; retry `TURBO_EBUSY` on later owner progress.

- [x] **Step 5: Re-run socket and adjacent pattern tests**

Expected: credit tests, TCP/TLS tests and existing socket-pattern tests pass under Debug/ASan.

### Task 5: Normative documentation and performance verification

**Files:**
- Modify: `docs/FMQ_WIRE_PROTOCOL.md`
- Modify: `docs/ARCHITECTURE.md`
- Modify: `docs/PROTOCOL_SPEC.md`
- Modify: `README.md`
- Test: `patterns/benchmarks/bench_flowmq_socket.c`

**Interfaces:**
- Consumes: the completed FMQ/5 implementation and existing libzmq benchmark mode.
- Produces: one documented wire contract plus repeatable correctness/performance evidence.

- [x] **Step 1: Replace FMQ/4 normative text with FMQ/5**

Document SETTINGS/FLOW_UPDATE layout, strict ordering, cumulative formulas, control priority, single-owner progression, and the absence of TCP/TLS FEC or legacy fallback.

- [x] **Step 2: Run Debug/ASan verification**

Run configure/build plus focused protocol, flow-control, socket and adjacent pattern tests, then the full Debug CTest suite.

- [x] **Step 3: Run Release verification**

Build `win-release-user`, run the full Release CTest suite, then run the FlowMQ/libzmq socket benchmark with identical payload and batch settings.

- [x] **Step 4: Check patch hygiene**

Run `git diff --check`, inspect only files listed by this plan, and report measured one-way/batch throughput plus any remaining risk without claiming unsupported FEC or background progress.
