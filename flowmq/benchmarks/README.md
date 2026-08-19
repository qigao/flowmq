# FlowMQ benchmarks

The standalone benchmark measures the independent FMQ v3 protocol codec. It
does not link or exercise TurboFlow.

Build with the repository Release preset:

```powershell
cmake --preset win-release-user -DBUILD_BENCHMARKS=ON
cmake --build --preset win-release-user --target bench_fmq
```

Run all contiguous encode/decode cases:

```powershell
build\Msvc-Release\bin\bench_fmq.exe --no-color
```

The benchmark covers 64-byte and 64-KiB application payloads. Reported byte
throughput counts application payload bytes, excluding FMQ framing metadata.
Allocation and payload copying performed by `flowmq_protocol_encode_frame()`
are inside the encode timing. Decode reuses one encoded input; cleanup remains
inside the timing because fragmented payloads may own reassembly storage.
