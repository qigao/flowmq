# FlowMQ

[![CI](https://github.com/qigao/flowmq/actions/workflows/native-sdk-release.yml/badge.svg)](https://github.com/qigao/flowmq/actions/workflows/native-sdk-release.yml)
[![Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**Pattern-oriented messaging for C11 over TCP, TLS, IPC, WebSocket, and KCP.**

FlowMQ provides a ZeroMQ-style context/socket API with its own FMQ/6 wire
protocol. Applications own socket progress through `send`, `recv`, and `poll`,
with CNet handling connections and transport completion on the calling thread.

This README describes `main`. Build each revision with its accompanying
[SDK version lock](cmake/sdk-versions.json).

## Contents

- [Features](#features)
- [Build from source](#build-from-source)
- [Usage](#usage)
- [CMake integration](#cmake-integration)
- [Examples](#examples)
- [Runtime model](#runtime-model)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Messaging patterns** with multipart messages, routing identities, and prefix
  subscriptions.
- **TCP and TLS** through `tcp://` and `tls://` endpoints, including certificate
  and hostname verification through CNet.
- **Local IPC** through `ipc://name` on Windows and `ipc:///absolute/path` on
  POSIX, using named pipes or Unix-domain sockets respectively.
- **WebSocket** through `ws://host:port/path` or `wss://host:port/path`, with
  one binary FMQ/6 frame per WebSocket message.
- **KCP** through `kcp://address:port?conv=42`, using reliable ordered messages
  over UDP with an explicit conversation ID.
- **Bounded flow control** with message and byte high-water marks,
  receiver-driven credit, reconnect backoff, and heartbeat deadlines.
- **Explicit memory ownership** with copying and retained-slice send APIs, plus
  copying, contiguous-slice, and vector-slice receive APIs.
- **Caller-owned concurrency** with independent socket owners, shared-wait
  client lanes, and reuse-port listeners where the backend supports them.
- **Application codecs** for FES/1 envelopes and FMP/1 media-provider payloads,
  backed by a shared protocol catalog and generated DataBind bindings.

| Pattern | Socket types | Typical use |
| --- | --- | --- |
| Pair | `PAIR` | One-to-one communication |
| Request/reply | `REQ`, `REP` | Alternating requests and replies |
| Asynchronous routing | `DEALER`, `ROUTER` | Concurrent requests and explicit peer routing |
| Publish/subscribe | `PUB`, `SUB` | Topic-prefix fanout |
| Extended pub/sub | `XPUB`, `XSUB` | Subscription events and commands |
| Pipeline | `PUSH`, `PULL` | Work distribution and collection |

## Build from source

### Requirements

- A C11 compiler and a C++17 compiler for the test suite. Native CI uses Clang
  on Linux and macOS, and llvm-mingw on Windows.
- CMake 3.25+, Ninja, Python 3.12+, Git, and curl.
- The released SDKs selected by the version lock.

| Dependency | Version | Role | SDK root variable |
| --- | --- | --- | --- |
| [salts-base](https://github.com/qigao/salts-base) | 0.1.5 | Buffers, strings, clocks, threads, NativeIO, and TinyTest | `SALTS_BASE_ROOT` |
| [cmeta](https://github.com/qigao/cmeta) | 0.1.5 | Metadata, Schema/Replay, and CSTL | `CMETA_ROOT` |
| [salts-cnet](https://github.com/qigao/salts-cnet) | 0.1.8 | Stream, WebSocket, and KCP transport engines | `SALTSCORE_ROOT` |
| [salts-utils](https://github.com/qigao/salts-utils) | 0.1.2 | DataBind and the host `salts-idlc` generator | `SALTS_UTILS_ROOT` |
| [vcpkg-cache](https://github.com/qigao/vcpkg-cache) | 0.1.3 | Prebuilt dependencies including llhttp and GmSSL, plus Windows compiler setup | — |

The restore script downloads the pinned release archives with curl and checks
their SHA-256 digests. The salts-cnet SDK publishes the `SaltsCore` CMake package
and the `Salts::CNet` target.

### Linux x64 quick start

```sh
git clone https://github.com/qigao/flowmq.git
cd flowmq

python cmake/ci/restore-split-sdks.py --profile linux-clang-x64 \
  --triplet x64-linux --destination build/dependencies

deps="$PWD/build/dependencies"
export SALTS_BASE_ROOT="$deps/linux-clang-x64/salts-base"
export CMETA_ROOT="$deps/linux-clang-x64/cmeta"
export SALTSCORE_ROOT="$deps/linux-clang-x64/salts-cnet"
export SALTS_UTILS_ROOT="$deps/linux-clang-x64/salts-utils"

cmake -S . -B build/flowmq -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON \
  -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
  -DCMAKE_PREFIX_PATH="$deps/vcpkg_installed/x64-linux"
cmake --build build/flowmq --parallel 2
ctest --test-dir build/flowmq -LE benchmark --no-tests=error --output-on-failure
```

### Other platforms

| Platform | Restore profile | Dependency triplet | CI qualification |
| --- | --- | --- | --- |
| Linux x64 | `linux-clang-x64` | `x64-linux` | Build, CTest, and install |
| Windows x64 | `windows-llvm-mingw-x64` | `qigao-x64-windows-llvm-mingw` | Build, CTest, and install |
| macOS arm64 | `macos-clang-arm64` | `arm64-osx` | Build, CTest, and install |
| Android arm64 | `android-arm64` | `arm64-android` | Cross-build, link, and install |

Use the matching profile and triplet when restoring dependencies. Windows
requires the llvm-mingw compiler and SDK runtime directories on `PATH`. Android
builds also require the NDK and Linux host SDK roots in `SALTS_BASE_HOST_ROOT`,
`CMETA_HOST_ROOT`, and `SALTS_UTILS_HOST_ROOT` for code generation. The restore
script downloads those host SDKs when selecting `android-arm64`.

The [native SDK workflow](.github/workflows/native-sdk-release.yml) contains the
complete compiler, toolchain, and runtime setup for each platform.

### Build options

| CMake option | Default | Purpose |
| --- | --- | --- |
| `BUILD_EXAMPLES` | `ON` | Build the programs in `examples/` |
| `BUILD_TESTS` | `OFF` | Build and register the formal CTest suite |
| `FLOWMQ_BUILD_ZMQ_BENCHMARK` | `OFF` | Enable comparisons with the pinned libzmq dependency |

## Usage

This complete loopback example exchanges one message between two `PAIR` sockets
on the same thread. Nonblocking send/receive calls and `flowmq_poll()` advance
both sides of the connection.

```c
#include <flowmq_socket.h>
#include <core/error.h>

#include <stdio.h>
#include <string.h>

enum { PROGRESS_LIMIT = 10000 };

int main(void) {
  static const char payload[] = "hello";
  char endpoint[128] = {0};
  char received[16] = {0};
  size_t endpoint_size = 0;
  size_t received_size = 0;
  size_t ready = 0;
  int send_status = SALTS_EBUSY;
  int recv_status = SALTS_EBUSY;
  int result = 1;
  flowmq_ctx_t *ctx = flowmq_ctx_new();
  flowmq_socket_t *receiver = NULL;
  flowmq_socket_t *sender = NULL;
  flowmq_pollitem_t items[2];

  if (ctx == NULL) goto cleanup;
  receiver = flowmq_socket(ctx, FLOWMQ_PAIR);
  sender = flowmq_socket(ctx, FLOWMQ_PAIR);
  if (receiver == NULL || sender == NULL) goto cleanup;
  if (flowmq_bind(receiver, "tcp://127.0.0.1:0") != SALTS_OK) goto cleanup;
  if (flowmq_last_endpoint(receiver, endpoint, sizeof(endpoint),
                           &endpoint_size) != SALTS_OK)
    goto cleanup;
  if (flowmq_connect(sender, endpoint) != SALTS_OK) goto cleanup;

  items[0] = (flowmq_pollitem_t){.socket = sender};
  items[1] = (flowmq_pollitem_t){.socket = receiver};
  for (size_t i = 0; i < PROGRESS_LIMIT && recv_status == SALTS_EBUSY; ++i) {
    if (flowmq_poll(items, 2, 0, &ready) != SALTS_OK) goto cleanup;
    if (send_status == SALTS_EBUSY)
      send_status = flowmq_send(sender, payload, sizeof(payload) - 1,
                                FLOWMQ_DONTWAIT);
    if (send_status != SALTS_OK && send_status != SALTS_EBUSY) goto cleanup;
    if (send_status == SALTS_OK)
      recv_status = flowmq_recv(receiver, received, sizeof(received),
                                &received_size, FLOWMQ_DONTWAIT);
  }
  if (recv_status != SALTS_OK || received_size != sizeof(payload) - 1 ||
      memcmp(received, payload, received_size) != 0)
    goto cleanup;
  puts(received);
  result = 0;

cleanup:
  if (sender != NULL && flowmq_close(sender) != SALTS_OK) result = 1;
  if (receiver != NULL && flowmq_close(receiver) != SALTS_OK) result = 1;
  if (ctx != NULL && flowmq_ctx_term(ctx) != SALTS_OK) result = 1;
  return result;
}
```

A successful send records local message acceptance. Applications can carry
correlation IDs and delivery acknowledgements in their payload protocol.

## CMake integration

Install the configured build into a local prefix.

```sh
cmake --install build/flowmq --prefix "$PWD/stage/flowmq"
```

Link the installed shared library through `FlowMQ::FlowMQ`.

```cmake
cmake_minimum_required(VERSION 3.25)
project(flowmq_client LANGUAGES C)

find_package(FlowMQ CONFIG REQUIRED)

add_executable(client main.c)
target_compile_features(client PRIVATE c_std_11)
target_link_libraries(client PRIVATE FlowMQ::FlowMQ)
```

Keep the four SDK root variables exported and add the FlowMQ installation
prefix to the consuming project's `CMAKE_PREFIX_PATH`. Its direct public link
dependencies are `Salts::Core` and `Salts::DataBind`. Deploy the required SDK
shared libraries alongside FlowMQ and make them available to the platform
loader.

| Target | Availability | Purpose |
| --- | --- | --- |
| `FlowMQ::FlowMQ` | Installed SDK and build tree | Complete public shared library |
| `FlowMQ::Protocol` | Build tree | Protocol catalog and wire codecs |
| `FlowMQ::Core` | Build tree | Pattern/session state and application core |
| `FlowMQ::Transport` | Build tree | Socket facade and CNet transport adapters |

## Examples

| Source | Executable | Demonstrates |
| --- | --- | --- |
| [protocol_roundtrip.c](examples/protocol_roundtrip.c) | `flowmq_protocol_roundtrip` | FMQ/6 frame encoding and decoding |
| [segmented_frame.c](examples/segmented_frame.c) | `flowmq_segmented_frame` | Segmented frame encoding |
| [security_hello.c](examples/security_hello.c) | `flowmq_security_hello` | FMS/3 security-envelope encoding |
| [multicore_reqrep.c](examples/multicore_reqrep.c) | `flowmq_multicore_reqrep` | Independent worker contexts and REQ/REP sockets |

With `BUILD_EXAMPLES=ON` and `BUILD_TESTS=ON`, run the multicore example's
1-, 4-, and 16-worker cases through CTest.

```sh
ctest --test-dir build/flowmq -R '^test_flowmq_multicore_reqrep_' \
  --no-tests=error --output-on-failure
```

## Runtime model

Each started socket belongs to one thread. Its owner advances connections,
queues, reconnects, and deadlines through socket operations. Independent
threads should create their own contexts and sockets, with each owner closing
its resources before terminating its context.

| Capability | Public entry point | Contract |
| --- | --- | --- |
| Socket I/O | [flowmq_socket.h](flowmq/include/flowmq_socket.h) | `bind`, `connect`, `send`, `recv`, and `poll` run on the owning thread |
| Shared completion wait | [flowmq_owner.h](flowmq/include/flowmq_owner.h) | TCP, IPC, WS, WSS and KCP client sockets use `FLOWMQ_DONTWAIT`, `flowmq_owner_poll()`, and `flowmq_owner_close_socket()` |
| Reuse-port listeners | `FLOWMQ_REUSE_PORT` | Configure independent listener sockets before binding the same numeric TCP endpoint |
| Peer admission limits | [flowmq_peer_pool.h](flowmq/include/flowmq_peer_pool.h) | Enable bounded connecting/ready peer capacity before starting a socket |
| Remote endpoint selection | [flowmq_destination.h](flowmq/include/flowmq_destination.h) | Choose an endpoint from a caller-owned snapshot before connecting |
| TLS routing authorization | [flowmq_tls_identity_map.h](flowmq/include/flowmq_tls_identity_map.h) | Bind verified certificate fingerprints to allowed ROUTER HELLO identities using required client certificates |

### WebSocket and KCP

| Transport | Endpoint example | Limits and security |
| --- | --- | --- |
| WS | `ws://127.0.0.1:8080/fmq` | HTTP/1.1 Upgrade with `fmq.v6`; 1 MiB per FMQ part |
| WSS | `wss://broker.example:443/fmq` | WS over verified CNet TLS; uses the existing CA, certificate, and server-name options |
| KCP | `kcp://127.0.0.1:9000?conv=42` | Numeric IPv4 or bracketed IPv6, matching nonzero 32-bit conversation ID, 64 KiB per FMQ part |

WS/WSS validates the requested resource (including the query), limits Upgrade
headers to 8 KiB, and applies a one-second Upgrade deadline. CNet handles
WebSocket framing, masking, fragmentation, and control frames. WSS also
supports the existing required client-certificate and ROUTER identity policies.

KCP currently uses plain sessions. Conversation IDs select a session and
provide no encryption or authentication. When both FMQ heartbeat interval
and timeout are enabled, a KCP peer expires after their sum without an FMQ
frame, including when an unacknowledged write occupies the send lane.
KCP send completion follows acknowledgement of
the logical message by KCP, while `flowmq_send()` reports local admission.

WS/WSS and KCP clients can share an Owner through `flowmq_owner_poll()`.
The Owner includes the KCP timer deadline in its shared NativeIO wait. Public
`flowmq_bind()` uses standalone sockets, with progress through `flowmq_poll()`.
WS/WSS and KCP use copied `flowmq_send()`.
Peer pools and retained sends return `SALTS_ENOTSUP` for these transports.

### Message ownership

IPC supports copied sends via `flowmq_send()`. IPC clients can share a wait
with TCP clients through `flowmq_owner_poll()`. IPC listeners use standalone
socket progress. IPC returns `SALTS_ENOTSUP` for retained sends, peer pools,
and port reuse. Remote destination selection accepts TCP/TLS endpoints. On
POSIX, bind in an application-controlled directory and remove the socket
pathname after closing the listener. OS permissions govern endpoint access.

`flowmq_send()` copies borrowed caller bytes. `flowmq_send_slice()` retains
canonical slice ownership through transport completion. Keep retained backing
bytes immutable until completion.

`flowmq_recv()` copies into caller storage. `flowmq_recv_slice()` transfers a
contiguous slice, and `flowmq_recv_slicev()` transfers a bounded payload vector.
Release every returned slice with `mem_slice_release()` before closing the
socket. Successful dequeue advances multipart state, flow credit, and queue
occupancy. Public headers specify capacity errors and ownership on failure.

Configure high-water marks, heartbeat settings, TLS credentials, and admission
policies before the first bind/connect. Reconnect establishes a fresh session
with its own generation, credit, decoder, and multipart state. Applications
own delivery retry and idempotency policies.

## Documentation

| Guide | Topics |
| --- | --- |
| [Implementation layout](flowmq/README.md) | Source directories and module boundaries |
| [Architecture](docs/ARCHITECTURE.md) | Ownership, shutdown, and runtime design |
| [TCP/TLS transport](docs/CNET_TCP_TLS_ARCHITECTURE.md) | Pattern semantics, CNet integration, and peer pools |
| [Protocol index](docs/PROTOCOL_SPEC.md) | Protocol families and implementation mapping |
| [FMQ/6 wire protocol](docs/FMQ_WIRE_PROTOCOL.md) | Frames, negotiation, multipart, credit, and heartbeats |
| [FES/1 application protocol](docs/FES_APPLICATION_PROTOCOL.md) | Application message kinds and metadata envelopes |
| [FMP/1 schema](flowmq/extensions/media_provider/schema/flowmq_media_provider_v1.schema) | Generated media-provider wire bindings |
| [Lane scaling](docs/FLOWMQ_LANE_SCALING.md) | Workloads, measurements, and multicore tradeoffs |
| [FlowMQ/libzmq comparison](docs/FLOWMQ_ZMQ_COMPARISON.md) | TCP comparison methodology and recorded results |
| [Peer-pool qualification](docs/PEER_POOL_QUALIFICATION.md) | Capacity, lifetime, and benchmark evidence |

Use the public headers for current API contracts. Benchmark reports record
their own source revision, SDK versions, workload, and measurement limits.

## Contributing

Open an [issue](https://github.com/qigao/flowmq/issues) with a reproducible case,
platform details, and the source and SDK versions involved. For changes, follow
the [repository development guidance](AGENTS.md), build the complete configured
graph, and run the relevant formal CTests. Keep protocol changes synchronized
with their specifications and examples.

## License

FlowMQ is licensed under [Apache 2.0](LICENSE). See [NOTICE](NOTICE) and
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for attribution and third-party
license information.
