# FMQ/6 Caller-Driven Reconnect Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 为 TCP/TLS 连接补齐类 ZeroMQ 自动重连，同时保持 CNet 单调用线程、无后台线程、无 Actor/Reactive API。

**Architecture:** socket 持有固定容量的 outbound endpoint 表，endpoint 是 URI 与退避策略的主事实源；peer 只持有一次 CNet connection session。连接终止时旧 peer 按现有 generation/credit/multipart 规则退休，endpoint 独立调度新的 peer，避免跨 session 继承可变状态。重连仅由 `flowmq_send()`、`flowmq_recv()` 或 `flowmq_poll()` 驱动。

**Tech Stack:** C11、Rocida CNet、TurboUtils memory primitives、TinyTest、CMake Presets。

---

### Task 1: Define ZeroMQ-compatible reconnect policy

**Files:**
- Modify: `flowmq/include/flowmq_socket.h`
- Modify: `patterns/src/flowmq_reconnect.c`
- Test: `patterns/tests/test_flowmq_reconnect.c`
- Test: `patterns/tests/test_flowmq_socket.c`

- [x] Add failing tests for `FLOWMQ_RECONNECT_IVL=18`, `FLOWMQ_RECONNECT_IVL_MAX=21`, defaults, validation and fixed/backoff behavior.
- [x] Change the internal helper so max `0` means fixed interval; socket option `-1` disables reconnect and interval `0` permits an immediate next attempt.
- [x] Preserve the existing pre-runtime option boundary and explicit error semantics.

### Task 2: Separate endpoint ownership from peer sessions

**Files:**
- Modify: `patterns/src/flowmq_socket.c`
- Test: `patterns/tests/test_flowmq_socket.c`

- [x] Add a fixed-capacity endpoint table with URI, active-session ownership, deadline and reconnect policy.
- [x] Associate outbound peers with endpoint slots; accepted peers remain endpoint-free.
- [x] On terminal CNet state, retire only the peer session and schedule the endpoint independently.
- [x] Reset backoff only after CNet reports `CONNECTED`.

### Task 3: Drive reconnect without threads

**Files:**
- Modify: `patterns/src/flowmq_socket.c`
- Test: `patterns/tests/test_flowmq_socket.c`

- [x] Add one bounded reconnect pass to `flowmq_socket_drive()`; never loop internally until success.
- [x] Recreate TCP/TLS connections through the same observer and handshake path as the initial connect.
- [x] Verify PAIR capacity, XSUB subscription replay and per-session generation fencing remain intact.

### Task 4: Document and verify

**Files:**
- Modify: `flowmq/include/flowmq_socket.h`
- Modify: `docs/ARCHITECTURE.md`
- Modify: `docs/FMQ_WIRE_PROTOCOL.md`
- Modify: `README.md`

- [x] Document caller-driven progress, option defaults, retry randomization, state ownership and failure behavior.
- [x] Run focused reconnect/socket tests, Debug ASan suite and Release suite.
- [x] Run the release FlowMQ/libzmq benchmark and compare against the current baseline.
- [x] Run CodeGraph affected analysis and inspect the final diff for stale or unrelated changes.
