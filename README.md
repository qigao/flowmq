# FlowMQ

[![CI](https://github.com/qigao/flowmq/actions/workflows/native-sdk-release.yml/badge.svg)](https://github.com/qigao/flowmq/actions/workflows/native-sdk-release.yml)
[![Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**Pattern-oriented messaging for C11 over TCP, TLS, IPC, WebSocket, and KCP.**

FlowMQ provides a ZeroMQ-style context/socket API with its own FMQ/6 wire
protocol. Applications own socket progress through `send`, `recv`, and `poll`,
with CNet handling connections and transport completion on the calling thread.

This README describes `main`. The [releases](https://github.com/qigao/flowmq/releases)
provide packaged SDKs and exact dependency versions.

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
- **KCP** with reliable ordered messages over UDP, using an explicit
  conversation ID or CNet pre-shared-key authentication.
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
- CMake 3.25+, Ninja, Python 3.12+, Git, GitHub CLI, and curl.
- The release-qualified SDK versions are listed in [.env.example](.env.example).
  Set the same version variables in the shell or in the repository CI settings.

### Upstream dependency versions

The build consumes SaltsBase, CMeta, SaltsUtils, SaltsCNet, TurboDB ORM, and
the vcpkg-cache native dependency bundle. Select exact release versions.
The restore command verifies SDK digests and source qualification, and
writes the selected roots for the parent build. For Linux x64:

```sh
export VCPKG_CACHE_VERSION=0.1.3 SALTS_BASE_VERSION=0.1.8 CMETA_VERSION=0.1.7
export SALTS_UTILS_VERSION=0.1.5 SALTS_CNET_VERSION=0.1.15 TURBODB_VERSION=0.1.2
export VCPKG_TARGET_TRIPLET=x64-linux
export VCPKG_CACHE_REPOSITORY_ROOT="$PWD/build/vcpkg-cache"
export GH_TOKEN="$(gh auth token)"
git clone --branch "v$VCPKG_CACHE_VERSION" --depth 1 \
  https://github.com/qigao/vcpkg-cache.git "$VCPKG_CACHE_REPOSITORY_ROOT"
mkdir -p build
export GITHUB_ENV="$PWD/build/flowmq-build.env"
: > "$GITHUB_ENV"
python cmake/ci/restore-release-inputs.py \
  --triplet "$VCPKG_TARGET_TRIPLET" --sdk-profile linux-clang-x64 --cnet-profile linux-x64
while IFS= read -r field; do export "$field"; done < "$GITHUB_ENV"

cmake --preset ci-llvm-native-release-user -DBUILD_EXAMPLES=ON
cmake --build --preset ci-llvm-native-release-user
ctest --preset ci-llvm-native-release-user -LE benchmark --no-tests=error --output-on-failure
cmake --build --preset install-ci-llvm-native-release-user
```

| Platform | SDK profile | CNet profile | Dependency triplet |
| --- | --- | --- | --- |
| Linux x64 | `linux-clang-x64` | `linux-x64` | `x64-linux` |
| Windows x64 | `windows-llvm-mingw-x64` | `windows-x64` | `qigao-x64-windows-llvm-mingw` |
| macOS arm64 | `macos-clang-arm64` | `macos-arm64` | `arm64-osx` |
| Android arm64 | `android-arm64` | `android-arm64` | `arm64-android` |

Windows needs llvm-mingw and the installed SDK runtime DLL directories on
`PATH`. For Android, set the NDK root and add `--host-sdk-profile
linux-clang-x64` to select the three host generator SDKs. The
[native SDK workflow](.github/workflows/native-sdk-release.yml) uses runtime
CTest on Windows, Linux, and macOS and cross-builds and installs on Android.
The release includes per-platform SDK archives and `SHA256SUMS`.

### Build options

| CMake option | Default | Purpose |
| --- | --- | --- |
| `BUILD_EXAMPLES` | `ON` | Build the programs in `examples/` |
| `BUILD_TESTS` | `OFF` | Build and register the formal CTest suite |
| `BUILD_ZMQ_BENCHMARK` | `OFF` | Enable comparisons with the pinned libzmq dependency |

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
| `FlowMQ::Delivery` | Installed SDK (Delivery component) and build tree | Transactional SQLite outbox and inbox via TurboDB ORM |
| `FlowMQ::MDP` | Installed SDK and build tree | Single-node service RPC, bounded worker slots, cancellation and request deadlines |
| `FlowMQ::RPC` | Installed SDK and build tree | JSON-RPC 2.0 client and worker method dispatch over MDP |
| `FlowMQ::Protocol` | Build tree | Protocol catalog and wire codecs |
| `FlowMQ::Core` | Build tree | Pattern/session state and application core |
| `FlowMQ::Transport` | Build tree | Socket facade and CNet transport adapters |

### Single-node MDP

For service RPC, link `FlowMQ::MDP` and include `flowmq_mdp.h`. The
[single-node echo example](examples/mdp_echo.c) defaults to TCP. The client
and worker ROUTER endpoints independently support TCP, WS, plain KCP, or local
IPC. IPC uses a named pipe on Windows (`ipc://name`) and a Unix-domain socket
on POSIX (`ipc:///absolute/path`). Use distinct endpoints for the two listeners.
For example, run the following from the preset's binary directory in the
configured SDK runtime environment to use a WS client and a KCP worker.

```sh
flowmq_mdp_echo hello "ws://127.0.0.1:0/fmq" "kcp://127.0.0.1:0?conv=42"
```

The MDP CTest label covers registration, dispatch and echo replies across all
sixteen transport pairs. The lifecycle and fault tests run over TCP.
See the [MDP protocol and lifecycle](docs/FLOWMQ_MDP_DISTRIBUTED_DESIGN.md#51-协议范围).

A single broker can dispatch the same service to local IPC workers and remote
TCP workers at the same time. Bind one worker ROUTER per endpoint and pass
both to `flowmq_mdp_broker_open_listeners`. Set each socket's peer budget
before binding. The default is four for existing callers. The broker's
`workers` setting limits registrations across all listeners. For example,
given three ordinary ROUTERs, a bound client frontend and an initialized
broker configuration, the following admits up to 32 local and 96 TCP peers.

```c
flowmq_mdp_worker_listener listeners[] = {
    {local_router, authorize_local_worker, local_acl},
    {tcp_router, authorize_remote_worker, remote_acl}
};
config.workers = 128;
int rc = flowmq_socket_set_peer_capacity(local_router, 32);
if (rc == SALTS_OK) rc = flowmq_socket_set_peer_capacity(tcp_router, 96);
if (rc == SALTS_OK) rc = flowmq_bind(local_router, local_ipc_uri);
if (rc == SALTS_OK) rc = flowmq_bind(tcp_router, tcp_uri);
if (rc == SALTS_OK)
    rc = flowmq_mdp_broker_open_listeners(front, listeners, 2, &config, &broker);
```

On success, the I/O owner polls all three ROUTERs and steps the broker. Each
listener supplies its own authorization callback. A reply travels over the
listener that dispatched the task, including when two listeners see the same
worker routing identity. The owner keeps the sockets and callbacks alive
until broker shutdown. The [mixed-listener regression](mdp/tests/test_mdp.c)
runs both transports concurrently.

On the broker I/O owner, install service placement policies before the first
`flowmq_mdp_broker_step`. The broker borrows the immutable array, names and
callback contexts until close. Services omitted from the table use round robin.

```c
static const flowmq_mdp_service_policy policies[] = {
    {.service = "quote", .placement = FLOWMQ_MDP_LEAST_LOAD},
    {.service = "order", .placement = FLOWMQ_MDP_EXCLUSIVE}
};
int rc = flowmq_mdp_broker_set_policies(broker, policies, 2);
```

Least-load placement compares occupied slots divided by registered capacity.
For cacheable reads, choose `FLOWMQ_MDP_CACHE_RING`, a nonzero load limit
of up to 100 percent, and a trusted `route_key` callback that validates the
request and supplies a stable 64-bit hash. The broker tries eligible peers
clockwise around a ring derived from listener ordinal and worker identity.
For exclusive services, a second owner is rejected. After retirement the
service pauses. The deployment calls `flowmq_mdp_broker_resume_service` only
after confirming that the old owner stopped or fencing its writes.

For an operation addressed to every member, provide a snapshot of member IDs
and worker sessions to `flowmq_mdp_fanout_start`. Each member registers its
dedicated service, and the listener ACL binds that service to its authenticated
identity. Poll the operation for succeeded, failed and missing counts, then
inspect each member's outcome. A retry after an unknown result needs the
service's idempotency contract. See the [fanout API and usage example](mdp/include/flowmq_mdp_fanout.h).

### JSON-RPC on a worker

Link `FlowMQ::RPC` and include `flowmq_rpc.h`. The client selects the MDP
service and supplies the JSON method, optional object or array parameters, and
an ID to `flowmq_rpc_call`. A null C pointer for the ID sends a notification.
An explicit JSON null is an ID and receives a reply. Each MDP call carries
one UTF-8 document of at most 4096 bytes, containing one request or a batch of
up to 32 requests for the same service.

The broker routes by service. On the worker, register the service's method
table with `flowmq_rpc_server_open` and call `flowmq_rpc_execute` on a business
thread. Each invocation claims one available MDP task, runs its methods and
completes the slot. The I/O owner keeps polling the socket and calling
`flowmq_mdp_worker_step` while handlers run. A handler borrows its input for
the callback and transfers ownership of its result or error data to the adapter.
An actor can drive execution if the application already uses one.

The client takes the MDP result with `flowmq_mdp_take_result`. For
`FLOWMQ_MDP_RESULT`, call `flowmq_rpc_decode` with the original IDs. It checks
the IDs, accepts reordered batch replies and returns an owned JSON object or
array to release with `json_free`. An all-notification completion returns a
null pointer. Numeric IDs round-trip as exact decimal tokens. Use string IDs
when another application may reformat numbers. A business error appears in
the JSON `error` member. A missed deadline after admission can yield
`FLOWMQ_MDP_UNKNOWN`, even if a handler committed its transaction. For writes,
the service can store a business idempotency key, its effects and the result
in one transaction, then look up that key when the outcome is uncertain.

The following invocation runs the client, broker and worker locally and prints
`hello`. It uses a WebSocket frontend and a KCP worker connection. See the
[runnable echo example](examples/mdp_echo.c) and the [public C API](rpc/include/flowmq_rpc.h).

```sh
flowmq_mdp_echo --rpc hello "ws://127.0.0.1:0/fmq" "kcp://127.0.0.1:0?conv=42"
```

### Deferred RPC workers

The [quote example](examples/rpc_quote.c) starts a broker and three workers.
Run `flowmq_rpc_quote` in the configured runtime environment. The quote worker
requests price and delivery concurrently. Each active ticket has its own
CFlow Statechart instance on a shared serial executor. The RPC owner decodes
each reply and sends a typed amount event to that instance. The chart handles
either arrival order and publishes the total, a component failure or
cancellation. The RPC owner observes that decision and finishes the ticket.
The example links `CMeta::CFlow` and prints `quote.total = 42`.

Run `flowmq_rpc_quote --concurrent` to exercise two concurrent requests with
delivery arriving first. A controlled clock expires one request while it
awaits price. The other request and a new request that reuses the released
slot both return 42. CTest runs both variants under the `flowmq-rpc` label.

Use `flowmq_rpc_engine_open` when a method can suspend between downstream
calls. Configure the worker's monotonic clock, the maximum active documents,
and the deferred method names. A classifier can attach an ordered business key
and a priority to a validated member. Synchronous methods may share the same
worker by passing their existing `flowmq_rpc_server` at open. A batch occupies
one MDP slot until its members settle in order.

For example, this configuration selects a stable logical partition for each
key and serves three high-priority ready steps per normal-priority step when
both levels stay busy in that partition. Supply it as the engine's `schedule`
and set `classify` to derive the canonical key and priority from trusted
method policy. Set `ordered` on every method that must serialize on that key.

```c
static const uint32_t partitions[] = {10, 20, 30};
static const unsigned quotas[] = {3, 1};
flowmq_rpc_scheduler_config schedule = {
    .capacity = 32, .per_key_limit = 4,
    .partitioning = FLOWMQ_RPC_HASH_RING, .partition_count = 3,
    .partition_ids = partitions, .virtual_nodes = 16,
    .queue_policy = FLOWMQ_RPC_QUEUE_PRIORITY,
    .priority_levels = 2, .priority_weights = quotas
};
```

An ordered key stays held across downstream waits. Other ready keys can run.
The default is round robin across ready keys within one partition. Select
`FLOWMQ_RPC_QUEUE_FIFO` to dispatch by readiness time instead. See the
[scheduler API](rpc/include/flowmq_rpc_scheduler.h) for capacity and ownership.

Drive the engine and its downstream client on one owner. Keep polling the
sockets and driving the MDP steps, including while methods are suspended.
Call `flowmq_rpc_engine_poll` with a bounded turn budget. On
`FLOWMQ_RPC_START`, retain the ticket and submit independent child calls as
one `flowmq_rpc_engine_fork` group. A tag identifies each child. Handle each
`FLOWMQ_RPC_CHILD` by inspecting the MDP outcome, then decoding a successful
completion with the original child JSON ID. Once the method's join condition
is satisfied, call `flowmq_rpc_engine_finish` with its owned result or error.
`FLOWMQ_RPC_PROGRESS` means internal work advanced. `SALTS_EBUSY` means the
owner can wait for I/O, an external business completion or the next deadline.

An Actor or pool can receive a copied ticket and owned business inputs and
send the result back to the engine owner. On `FLOWMQ_RPC_CANCEL`, quiesce
external work and finish the ticket to acknowledge cancellation and release
its ordered key. Use `flowmq_rpc_engine_drain` to let admitted methods finish
while rejecting new work. At a shutdown deadline use `flowmq_rpc_engine_stop`,
drain cancellation events and close after outstanding tickets are acknowledged.
Allocate independent downstream execution capacity when all parent slots can
wait on the same dependency. The [RPC integration tests](rpc/tests/test_rpc.c)
show a parent joining two out-of-order child RPCs, another order completing
concurrently, and a suspended batch resuming its remaining members.

### Persistent one-way messages

Use `find_package(FlowMQ CONFIG REQUIRED COMPONENTS Delivery)` and link
`FlowMQ::Delivery`. Configure TurboDB through `TURBODB_ROOT` when building
FlowMQ. Deploy the matching SQLite driver, ORM and their runtime libraries.
The host loads the trusted SQLite plugin into its ORM runtime before opening
a store. On Windows the plugin loader searches the application directory for
dependent DLLs. See [the delivery API](delivery/include/flowmq_delivery.h).

Provision a separate database for each producer/inbox stream and role with
`flowmq_delivery_open(..., 1, ...)`. Use `create=0` on restart. Persist the
assigned positive 63-bit stream ID in deployment configuration. A replacement
inbox must recover the original database, including its receive watermark.
The record and byte limits bound pending encoded data. Budget SQLite file,
index, WAL and checkpoint overhead separately. Operators must provision enough
disk space for commits and reclamation.

1. The producer calls `flowmq_delivery_enqueue`. Success means the entire
   multipart payload and its sequence have committed to the WAL.
2. The storage owner reads the first pending item with `flowmq_delivery_peek`.
   The socket owner sends its frame as one FlowMQ DATA part and retries that
   same record after a bounded retry interval until the inbox acknowledges it.
   Release the item after use. A successful socket send leaves the record pending.
3. After authenticating and authorizing the remote endpoint for this stream,
   pass the complete frame to `flowmq_delivery_accept`. Send the returned ACK
   only on success. On the sender, pass that authorized ACK to
   `flowmq_delivery_settle`. Stop on a protocol, identity or storage error.
4. Consume the inbox head with `flowmq_delivery_consume_apply` for changes in
   the same database. Its callback uses the supplied ORM transaction. For an
   external service use an idempotency key and call `flowmq_delivery_consume`
   after successful processing. A crash between those operations permits redelivery.

The complete DATA envelope is `FMD1`, an eight-byte stream ID, an eight-byte
sequence and one to 64 parts. Each part has a four-byte length followed by its
bytes. Empty parts are valid. Integers use network byte order. The encoded
envelope is limited to 1 MiB and must fit the socket's configured byte limits.
An ACK is exactly `FMA1` plus the same stream ID and sequence. These are
application envelopes carried over FMQ/6. Keep database operations on the
storage owner and socket calls on its I/O owner. A delivery item owns its
buffer and may move between owners. Its part views expire on release.

## Examples

| Source | Executable | Demonstrates |
| --- | --- | --- |
| [protocol_roundtrip.c](examples/protocol_roundtrip.c) | `flowmq_protocol_roundtrip` | FMQ/6 frame encoding and decoding |
| [segmented_frame.c](examples/segmented_frame.c) | `flowmq_segmented_frame` | Segmented frame encoding |
| [security_hello.c](examples/security_hello.c) | `flowmq_security_hello` | FMS/3 security-envelope codec round trip |
| [multicore_reqrep.c](examples/multicore_reqrep.c) | `flowmq_multicore_reqrep` | TCP loopback REQ/REP with a context and socket per server worker |

Run the TCP REQ/REP example with eight workers and 100 requests per worker.

```sh
build/flowmq/bin/flowmq_multicore_reqrep 8 100
```

The example sets `FLOWMQ_RECONNECT_DISABLED` to exercise one connection per
worker. The FMS/3 example uses the codec directly. Socket identity policies
use the verified TLS certificate and the FMQ HELLO identity.

With `BUILD_EXAMPLES=ON` and `BUILD_TESTS=ON`, run the multicore example's
1-, 4-, and 16-worker cases through CTest.

```sh
ctest --test-dir build/flowmq -R '^test_flowmq_multicore_reqrep_' \
  --no-tests=error --output-on-failure
```

See the integration tests for [TLS and reconnect](flowmq/tests/runtime/test_flowmq_socket.c),
[IPC](flowmq/tests/transport/test_flowmq_ipc.c), and
[WS/WSS and KCP](flowmq/tests/transport/test_flowmq_message_transport.c).

## Runtime model

Each started socket belongs to one thread. Its owner advances connections,
queues, reconnects, and deadlines through socket operations. Independent
threads should create their own contexts and sockets, with each owner closing
its resources before terminating its context.

Configure outbound recovery with `FLOWMQ_RECONNECT` before binding or
connecting. The default allows 32 attempts per episode within 30 seconds,
with jittered backoff from a nominal 100 ms up to 1 second.
`FLOWMQ_RECONNECT_DISABLED` permits a single initial dial.

| Capability | Public entry point | Contract |
| --- | --- | --- |
| Socket I/O | [flowmq_socket.h](flowmq/include/flowmq_socket.h) | `bind`, `connect`, `send`, `recv`, and `poll` run on the owning thread |
| Shared completion wait | [flowmq_owner.h](flowmq/include/flowmq_owner.h) | TCP, IPC, WS, WSS and KCP client sockets use `FLOWMQ_DONTWAIT`, `flowmq_owner_poll()`, and `flowmq_owner_close_socket()` |
| Reuse-port listeners | `FLOWMQ_REUSE_PORT` | Configure independent listener sockets before binding the same numeric TCP endpoint |
| Peer admission limits | [flowmq_peer_pool.h](flowmq/include/flowmq_peer_pool.h) | Enable bounded connecting/ready peer capacity before starting a TCP or TLS socket |
| Remote endpoint selection | [flowmq_destination.h](flowmq/include/flowmq_destination.h) | Choose a TCP or TLS endpoint from a caller-owned snapshot before connecting |
| TLS routing authorization | [flowmq_tls_identity_map.h](flowmq/include/flowmq_tls_identity_map.h) | On TLS and WSS ROUTER sockets, match verified peer certificate fingerprints to HELLO identities. Listeners require client certificates |

### WebSocket and KCP

| Transport | Endpoint example | Limits and security |
| --- | --- | --- |
| WS | `ws://127.0.0.1:8080/fmq` | HTTP/1.1 Upgrade with `fmq.v6`; 1 MiB per FMQ part |
| WSS | `wss://broker.example:443/fmq` | WS over verified CNet TLS; uses the existing CA, certificate, and server-name options |
| Plain KCP | `kcp://127.0.0.1:9000?conv=42` | Matching nonzero 32-bit conversation ID, 64 KiB per FMQ part |
| Authenticated KCP | `kcp://127.0.0.1:9000?auth=psk` | Matching 32-byte PSK, CNet-derived conversation ID, 64 KiB per FMQ part |

FlowMQ parses the HTTP/1.1 Upgrade with llhttp and computes the
`Sec-WebSocket-Accept` value with GmSSL SHA-1. It validates the requested
resource, including its query, limits headers to 8 KiB, and applies a
one-second Upgrade deadline. The listener combines subprotocol offers from
repeated request fields. It responds to malformed requests, unknown resources,
unsupported versions and oversized headers before disconnecting. An unsupported
version receives `426` with the supported WebSocket version.
Each binary WebSocket message carries one complete FMQ frame. FMQ multipart
flags link the individual frames.
CNet handles WebSocket masking, fragmentation, and control frames.
WSS supports required client certificates and certificate-to-HELLO
identity policies for ROUTER sockets. A ROUTER with an identity policy
sends SETTINGS only after it authorizes the peer's HELLO identity.

Both KCP modes accept numeric IPv4 and bracketed IPv6 endpoints. Plain KCP
uses an unencrypted UDP transport. For authenticated KCP, provision the same
random 32-byte key on both sockets using `FLOWMQ_KCP_PSK` before bind or
connect. This write-only option copies the key and restricts the socket to
`?auth=psk` endpoints. The endpoint URI carries only the mode.

CNet authenticates key possession and encrypts KCP records. FlowMQ starts
HELLO/SETTINGS after CNet establishes the session. The five-second handshake
deadline covers both steps. A holder of the shared key can claim any FMQ
HELLO identity. Certificate-to-HELLO policies apply to TLS and WSS.

When both FMQ heartbeat interval
and timeout are enabled, a KCP peer expires after their sum without an FMQ
frame, including when an unacknowledged write occupies the send lane.
With reconnect enabled, a client opens a fresh KCP session after expiry,
repeats FMQ negotiation, and restores its subscriptions. KCP send completion
follows acknowledgement of the logical message by KCP, while `flowmq_send()`
reports local admission.

WS/WSS and KCP clients can share an Owner through `flowmq_owner_poll()`.
The Owner includes the KCP timer deadline in its shared NativeIO wait. Public
`flowmq_bind()` uses standalone sockets, with progress through `flowmq_poll()`.
WS/WSS and KCP use copied `flowmq_send()`.
Peer pools and retained sends return `SALTS_ENOTSUP` for these transports.

### Message ownership

IPC uses copied sends and supports shared client progress through
`flowmq_owner_poll()`. Public IPC listeners use standalone progress. IPC
returns `SALTS_ENOTSUP` for peer pools and port reuse. On POSIX, bind in an
application-controlled directory and remove the socket pathname after closing
the listener. OS permissions govern endpoint access.

`flowmq_send()` copies borrowed caller bytes. On TCP and TLS,
`flowmq_send_slice()` retains canonical slice ownership through transport
completion. Keep retained backing bytes immutable until completion. CNet
encrypts the retained plaintext when sending over TLS.

`flowmq_recv()` copies into caller storage. `flowmq_recv_slice()` returns a
contiguous slice and coalesces a multi-range payload when needed.
`flowmq_recv_slicev()` transfers the queued payload ranges.
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
| [Architecture](docs/ARCHITECTURE.md) | Protocol/transport/pattern responsibilities, capability matrix, ownership and shutdown |
| [MDP and distributed capabilities](docs/FLOWMQ_MDP_DISTRIBUTED_DESIGN.md) | Planned prerequisites, module boundaries, delivery stages, and acceptance criteria |
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
