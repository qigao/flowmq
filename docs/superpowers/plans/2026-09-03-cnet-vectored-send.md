# CNet Vectored Copy / FlowMQ Segmented Send Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a bounded caller-driven `cnet_sendv()` contract and use it to remove FlowMQ's intermediate contiguous send copies without changing FMQ/6 bytes, socket patterns, or the single-thread progress model.

**Spec path:** This document is the ownership and migration specification. Public CNet wording is mirrored in `C:\projects\cpp\turbonet\turbo-utils\cnet\include\cnet\cnet.h` and `C:\projects\cpp\turbonet\turbo-utils\cnet\README.md`; FlowMQ architectural consequences are mirrored in `docs/CNET_TCP_TLS_ARCHITECTURE.md`.

**Architecture:** Keep NativeIO, coroutine ownership, TCP/TLS completion, and the one-write-per-connection state machine unchanged. `cnet_sendv()` borrows non-empty segments only during the call, validates their checked total, and copies them in order directly into the existing final command slot. FlowMQ encodes FMQ/6 headers into bounded reusable framing storage, borrows the application payload as protocol segments, and either sends those segments immediately or flattens them once into an already-required outbound `mem_buffer_t`. Queued frames are submitted to CNet as one vector, so the old batch scratch concatenation disappears.

**Tech stack:** C11, Rocida CNet/NativeIO, TurboUtils retained buffers, TinyTest, CMake presets, FMQ/6, OpenSSL-backed CNet TLS.

## Global Constraints

- No I/O worker thread, Actor API, Reactive API, or CFlow operator enters the data path. All mutation and progress remain on the caller-owned CNet/FlowMQ lane.
- FMQ/6 wire bytes and all ZMQ-like public FlowMQ pattern semantics remain unchanged.
- Existing `cnet_send()` remains source- and behavior-compatible. `cnet_sendv()` is additive and has the same completion callback/error semantics.
- The unrelated dirty TLS certificate-digest changes already present in `turbo-utils` must be preserved and excluded from this phase's scoped commit/staging.
- Every allocation/capacity is bounded; all size addition is checked before any slot or socket state is committed.
- Tests are written and observed failing before the matching production change.

## Ownership, Lifetime, Capacity, and Shutdown Protocol

- **Data unit / fact source:** one logical ordered CNet write consists of `segment_count` immutable, non-empty byte ranges. Their concatenation is the only payload fact source; `total_size` is derived with checked addition.
- **Producer / consumer:** the CNet client API caller is the single producer and progress owner; the owner coroutine is the single consumer. This is single-threaded cardinality, not MPSC and not a cross-thread safety promise.
- **Borrow:** the segment descriptor array and every segment backing range remain caller-owned and are borrowed only until `cnet_sendv()` returns. A successful call has already copied all bytes into one fixed command slot.
- **Queue state:** `FREE -> QUEUED -> BORROWED_BY_OWNER -> FREE`. TCP/Pipe/UDP keep the borrowed slot until terminal NativeIO completion. TLS keeps it until all plaintext is accepted and generated ciphertext is flushed. Existing cancellation/failure paths release the slot exactly once.
- **Ordering / completion:** segment order is byte order. One admitted vector produces exactly one ordered write and at most one successful `observer.on_send(connection, total_size)` callback. A second write remains `TURBO_EBUSY` until that event is observed.
- **Capacity:** `segment_count > 0`; every segment has non-NULL data and positive size. Since every segment is non-empty, `segment_count` is bounded by `max_send_bytes`. Checked total must be positive and at most `max_send_bytes`; the fixed command queue remains the retained-byte hard bound.
- **Admission failure:** invalid descriptors return `TURBO_EINVAL`; arithmetic or configured size overflow returns `TURBO_EMSGSIZE`; queue/state errors preserve caller ownership and commit no write state.
- **Shutdown:** no new retained external object is introduced. Existing stop/cancel/drain logic owns and releases the copied command slot, so quiescence and destruction preconditions remain unchanged.
- **Observability:** existing command `queued_bytes`, peaks, rejected command count, and rejected byte count use the derived total size; vector and contiguous writes share the same counters.

## Task 1: CNet command queue vector-copy contract (TDD)

**Files:**

- Modify: `C:\projects\cpp\turbonet\turbo-utils\cnet\tests\cnet_command_test.c`
- Modify: `C:\projects\cpp\turbonet\turbo-utils\cnet\src\cnet_command.h`
- Modify: `C:\projects\cpp\turbonet\turbo-utils\cnet\src\cnet_command.c`

1. Add TinyTest cases proving ordered concatenation, source mutation after publish, invalid NULL/empty segments, checked total mismatch, oversize rejection counters, and no slot consumption on failure.
2. Run only `cnet_command_test` and record the expected compile/test failure because vector descriptors are not implemented.
3. Extend the internal command descriptor with optional borrowed segments while retaining the existing contiguous form.
4. Refactor publication so validation happens before slot claim, then copy either one contiguous range or all segments directly into the same final entry payload.
5. Re-run `cnet_command_test` and the existing command queue suite.

## Task 2: Add public caller-driven `cnet_sendv()` for TCP and TLS (TDD)

**Files:**

- Modify: `C:\projects\cpp\turbonet\turbo-utils\cnet\include\cnet\cnet.h`
- Modify: `C:\projects\cpp\turbonet\turbo-utils\cnet\src\cnet_client.c`
- Modify: `C:\projects\cpp\turbonet\turbo-utils\cnet\src\cnet_shards.h`
- Modify: `C:\projects\cpp\turbonet\turbo-utils\cnet\src\cnet_shards.c`
- Modify: `C:\projects\cpp\turbonet\turbo-utils\cnet\tests\cnet_api_test.c`
- Modify: `C:\projects\cpp\turbonet\turbo-utils\cnet\tests\cnet_tls_test.c`
- Modify: `C:\projects\cpp\turbonet\turbo-utils\cnet\tests\cnet_header_cpp_test.cpp`
- Modify: `C:\projects\cpp\turbonet\turbo-utils\cnet\README.md`

1. Add public TCP and TLS tests that send multiple source ranges, mutate the sources immediately after successful admission, and assert exact concatenated peer bytes plus one total-size completion.
2. Add validation tests for zero count, NULL descriptors/data, zero-sized segments, checked overflow/oversize, stale handles, and busy writes without state commitment.
3. Run the focused CNet API/TLS targets and observe failure because `cnet_const_buffer` / `cnet_sendv` do not exist.
4. Add the public descriptor and contract, route the checked vector through client/shards into the vector-aware command queue, and leave owner/NativeIO/TLS completion code untouched.
5. Update the C++ header compile contract and CNet README.
6. Configure/build/test Debug-ASan and Release with the repository presets, then install both profiles used by FlowMQ.

## Task 3: Add allocation-free internal segmented encoding (TDD)

**Files:**

- Modify: `flowmq/protocol/tests/test_flowmq_protocol.c`
- Modify: `flowmq/protocol/tests/CMakeLists.txt`
- Modify: `flowmq/protocol/src/flowmq_protocol_internal.h`
- Modify: `flowmq/protocol/src/flowmq_protocol.c`
- Modify: `flowmq/benchmarks/bench_flowmq_protocol.c`

1. Add a test that provides exact bounded segment/framing storage, compares flattened bytes against the hand-checked contiguous encoder output, verifies payload segments alias the input, and checks undersized descriptor/framing failures leave outputs empty.
2. Run `test_flowmq_protocol` and observe failure because the internal provided-storage encoder does not exist.
3. Extract one checked layout calculation and one segment-filling routine. Keep the public allocating segmented API as a wrapper while exposing only the provided-storage variant to FlowMQ internals.
4. Add a benchmark that separates contiguous-copy encoding from provided-storage segmented framing for 64-byte and 64-KiB payloads; assertions stay outside timed blocks.
5. Re-run protocol tests and benchmark in Release.

## Task 4: Replace FlowMQ scratch concatenation with segmented submission (TDD)

**Files:**

- Modify: `patterns/src/flowmq_socket.c`
- Modify: `patterns/tests/test_flowmq_socket.c`
- Modify: `patterns/benchmarks/bench_flowmq_socket.c`
- Modify: `docs/CNET_TCP_TLS_ARCHITECTURE.md`

1. Add socket tests for a payload spanning multiple FMQ/6 packets, queued multi-message batching, PUB fanout, and TLS delivery through the same segmented path. Each test asserts payload bytes and message boundaries, not private fields.
2. Add 64-KiB one-way and queued batch benchmark cases while preserving the existing 64-byte libzmq reference.
3. Run the focused socket tests and record the baseline behavior/performance before production rewiring.
4. Replace the per-socket ~1-MiB contiguous scratch allocation with fixed bounded framing and descriptor storage derived from FMQ/6 limits.
5. Route immediate frames through the allocation-free segmented encoder and `cnet_sendv`. When a peer already has an in-flight write, flatten segments exactly once into its outbound retained buffer.
6. Route queued flush batches as one vector of existing complete encoded buffers. Release those buffers only after synchronous `cnet_sendv` admission succeeds, preserving HWM/credit/accounting state transitions.
7. Update the architecture document with the copy count, ownership invalidation points, single-owner cardinality, and why NativeIO/TLS remain contiguous internally.

## Task 5: Verification, benchmark decision, and scoped delivery

**Files:** all files above; no unrelated dirty files.

1. Run FlowMQ Debug-ASan focused protocol/socket/TLS tests, then the full Debug CTest suite.
2. Run FlowMQ Release full CTest and at least five independent socket benchmark repeats. Report medians/MAD or the existing TinyTest sample metrics without pooling independent runs.
3. Compare before/after 64-byte, 64-KiB, and 64-message batch throughput. A regression over 10% in an existing metric requires rollback or an explicit evidence-backed explanation; no speedup claim is made without measured data.
4. Inspect `git diff --check`, CodeGraph affected files, and both repository statuses. Confirm owner/NativeIO/TLS state-machine files were not changed for vector send.
5. Stage/commit only this phase's TurboUtils hunks (excluding the pre-existing TLS certificate-digest and repository-maintenance changes), then commit FlowMQ against the installed verified CNet version. Push only after both commits are reproducible and cleanly scoped.
