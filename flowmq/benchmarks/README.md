# FlowMQ benchmarks

Build the standalone benchmark with the repository Release preset:

```powershell
cmake --preset win-release-user -DBUILD_BENCHMARKS=ON
cmake --build --preset win-release-user --target bench_fmq
```

Run all cases or select one latency/throughput profile:

```powershell
.\build\Msvc-Release\bin\bench_fmq.exe
.\build\Msvc-Release\bin\bench_fmq.exe --filter "64-byte serialized round trip"
.\build\Msvc-Release\bin\bench_fmq.exe --filter "64-KiB serialized round trip"
.\build\Msvc-Release\bin\bench_fmq.exe --filter "DEALER ROUTER"
.\build\Msvc-Release\bin\bench_fmq.exe --filter "PUB SUB"
.\build\Msvc-Release\bin\bench_fmq.exe --filter "PUSH PULL"
.\build\Msvc-Release\bin\bench_fmq.exe --filter "async micro-batch"
```

Enable the benchmark-only send owner-lane profiler for one run:

```powershell
$env:FLOWMQ_BENCH_PROFILE='1'
.\build\Msvc-Release\bin\bench_fmq.exe --filter "64-byte serialized round trip"
Remove-Item Env:FLOWMQ_BENCH_PROFILE
```

Run the 64-KiB batch-size A/B matrix with profiling:

```powershell
$env:FLOWMQ_BENCH_PROFILE='1'
foreach ($messages in 2, 4, 8) {
  $env:FLOWMQ_BENCH_LARGE_BATCH_MESSAGES="$messages"
  .\build\Msvc-Release\bin\bench_fmq.exe --no-color --filter "64-KiB batch API"
}
Remove-Item Env:FLOWMQ_BENCH_LARGE_BATCH_MESSAGES
Remove-Item Env:FLOWMQ_BENCH_PROFILE
```

`FLOWMQ_BENCH_LARGE_BATCH_MESSAGES` applies only to the three 64-KiB batch API cases. It accepts
exactly `2`, `4`, or `8`; an unset variable keeps the default of eight messages (512 KiB) per
sample, while any other value fails the selected benchmark. Each A/B case uses one batch of the
selected size for warmup and keeps the measured sample count fixed, so compare both messages/s and
MiB/s rather than total elapsed time.

Compare CoroNet user-space receive buffer capacities under the same host-driven execution topology:

```powershell
$env:FLOWMQ_BENCH_WINDOWS_TIMER_RESOLUTION='1'
foreach ($bytes in 131072, 262144, 524288) {
  $env:FLOWMQ_BENCH_STREAM_RECV_BUFFER_BYTES="$bytes"
  .\build\Msvc-Release\bin\bench_fmq.exe --no-color --filter "64-KiB batch API"
}
Remove-Item Env:FLOWMQ_BENCH_STREAM_RECV_BUFFER_BYTES
Remove-Item Env:FLOWMQ_BENCH_WINDOWS_TIMER_RESOLUTION
```

The variable accepts exactly 128, 256, or 512 KiB in bytes and applies only to the three 64-KiB
Batch API cases. When set, the benchmark creates and drives a receiver CoroNet context, configures
its stream receive buffers before creating the facade, and passes it through the non-owning
Application Facade execution API. `FMQ_BENCH_RECEIVER_TUNING` records both capacity and execution
topology. When unset, the historical private-context path and CoroNet's 128-KiB default remain
unchanged. Compare buffer sizes only within the borrowed-context matrix; private and borrowed
execution placement are separate variables.

On Windows, isolate scheduler timer effects with a benchmark-only process setting:

```powershell
$env:FLOWMQ_BENCH_WINDOWS_TIMER_RESOLUTION='1'
.\build\Msvc-Release\bin\bench_fmq.exe --no-color --filter "64-KiB batch API"
Remove-Item Env:FLOWMQ_BENCH_WINDOWS_TIMER_RESOLUTION
```

The variable accepts exactly `0` or `1`. Timer resolution uses paired `timeBeginPeriod(1)` and
`timeEndPeriod(1)` calls around each selected benchmark case. Stable
`FMQ_BENCH_WINDOWS_TUNING` output records the setting that was actually applied. CPU-affinity
experiments must pin individual IOCP and CoroNet loop thread roles; restricting the whole process
to one CPU starves those roles and is not a valid production configuration.

The REQ/REP cases measure one serialized TCP request/reply per sample through two FlowMQ
application facades and their minimal typed graph bridges. Their byte throughput counts
application payload in both directions.

The DEALER/ROUTER, PUB/SUB, and PUSH/PULL cases measure batches of 64 one-way 64-byte messages.
Their operation throughput is messages/s and byte throughput counts one-way application payload.
Each batch waits until the receiving callback has observed every message, so the result covers both
send submission and delivery to the application facade. Setup, connection/subscription
establishment, and warmup are outside all timed blocks. Stable `FMQ_BENCH_RESULT` lines report
throughput plus nearest-rank P50/P95/P99 round-trip or batch latency for same-runner trend
comparisons.

Explicit batch API cases also emit `FMQ_BENCH_STAGE_RESULT` to locate end-to-end tail latency:

- `submit_*` measures entry into `turbo_flow_fmq_app_send_batch` through its return.
- `first_receive_after_submit_*` measures from Batch API return to the first receiver callback.
  A zero value is a sentinel meaning that the first callback ran before the Batch API returned;
  `first_receiver_before_submit_return_samples` counts those samples.
- `receive_batch_span_*` measures from the first receiver callback to the last callback for that
  sample.

The three intervals overlap when the receiver starts before Batch API return and therefore must not
be added together. For the default 512-KiB samples, observed 15-20 ms outliers have occurred in
`receive_batch_span`, while Batch API submission and initial receiver arrival remained below 1 ms.
This locates the observed stall after receiver processing starts; it does not by itself distinguish
IOCP completion/rearm delay, owner-loop scheduling, TCP flow control, or callback processing.

In a three-run Windows matrix, a 512-KiB user-space receive buffer reduced
`receive_batch_span_p99` to about 0.10-0.20 ms and produced no slow samples in that stage. It did
not remove end-to-end tail latency: occasional 10-25 ms stalls moved to
`first_receive_after_submit`, and median throughput was lower than the 256-KiB configuration in all
three patterns. The result supports keeping receive-buffer size workload-configurable; it does not
support changing the production default or attributing the remaining stalls to kernel socket
buffer exhaustion.

The asynchronous micro-batch cases also wait for every accepted per-message completion. They
therefore measure the facade delivery boundary plus receiver observation, rather than queue
admission alone. Their configured batch size matches the 64-message benchmark burst.

Profiled runs add an `FMQ_PROFILE_RESULT` line. Its values are per-request averages in nanoseconds:
`enqueue` ends when the request becomes visible in the send queue; `owner_wait` ends when the owner
drain dequeues it; `post_call` is the overlapping subset spent inside `coro_post`; `owner_dispatch`
ends at the first socket send; `completion` ends immediately before completion notification; and
`waiter_wake` ends when the synchronous sender resumes. Serialized sends also report request-level
`socket_send`, `socket_thread_cpu`, and `socket_estimated_off_cpu`. A batch shares one socket write,
so these request-level socket counters exclude shared writes instead of attributing the same wall
time to every request.

Batch and asynchronous micro-batch runs add `FMQ_PROFILE_BATCH_RESULT`. It reports actual socket
call, frame, and iovec-segment totals plus per-call socket wall time, owner OS-thread CPU time, and
estimated off-CPU time. Its lifecycle averages are per batch: `build` covers the entire request
build through queue visibility; `payload_prepare` covers buffer allocation/retention, copying, and
message-view setup; `batch_submit` covers the synchronous Core batch call, including its producer;
`adapter_consume` is the subset spent encoding and constructing FlowMQ send requests;
`dispatch_overhead` is the remainder after producer and adapter work; `enqueue_prepare` covers iovec preparation
and queue commit through visibility; `owner_wait` ends when the owner dequeues the batch;
`owner_work` covers owner-side selection, authorization, socket calls, and completion; and
`waiter_wake` ends when the submitting thread resumes. Because the coroutine may yield while other
owner-lane work runs, the CPU value can include unrelated loop work and the off-CPU value is a
conservative estimate rather than exact per-coroutine time. A platform without a thread CPU clock
reports `socket_cpu_samples=0`.

Profiled batch runs also add `FMQ_PROFILE_STAGE_RESULT` for matrix comparisons. `encode_ns` combines
per-batch payload allocation/materialization/copy with adapter encode and send-request construction.
`queue_ns` combines contiguous queue preparation/commit and queue-visible-to-owner-dequeue wait
time. `socket_ns` is per actual socket call, not per message. `completion_ns` is the
completion-signal-to-submitting-thread-resume time. These are diagnostic slices: owner selection,
authorization, and completion work outside the socket call are visible in `avg_owner_work_ns` in
`FMQ_PROFILE_BATCH_RESULT`, so the stage values must not be added and treated as `total_ns`.

Windows reports thread CPU in coarse accounting ticks, so prefer the longer one-way cases when
interpreting CPU/off-CPU ratios; the profiler derives off-CPU from aggregate totals rather than
subtracting individual messages.
Profiling is disabled by default because the additional clock reads and atomic aggregation perturb
the measured throughput.

Windows TCP currently performs one explicit receive thread-hop: an IOCP worker dequeues
`GetQueuedCompletionStatus`, publishes the completion to CoroNet's MPMC queue, and schedules a
`coro_post`; the owning context loop drains that queue and resumes the receive coroutine. FlowMQ
execution lane affinity selects a stable CoroNet context and does not set OS CPU affinity.

The current CoroNet socket API does not expose `SO_RCVBUF` or `SO_SNDBUF`. Its
`coro_context_set_stream_recv_buffer_size` setting controls user-space ping-pong receive buffers,
not the kernel TCP buffers. Kernel buffer A/B therefore requires a versioned CoroNet socket-option
API applied consistently to connected and accepted sockets; the benchmark does not silently
substitute the user-space buffer setting.
