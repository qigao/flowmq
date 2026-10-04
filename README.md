# FlowMQ

FlowMQ 是 C11 的 pattern-oriented messaging library。它提供 FMQ/6 wire codec、pattern/session
状态、应用层能力，以及基于 Salts CNet 的 TCP/TLS socket runtime。

当前 transport 范围是明确且封闭的：

- `FLOWMQ_TRANSPORT_TCP`：有序 TCP 字节流。
- `FLOWMQ_TRANSPORT_TLS`：经证书与主机名校验的 TLS 字节流。
- UDP、KCP、Pipe、WebSocket 与 WSS 不属于当前 API，也没有静默 fallback。

## CMake targets

| Target | 用途 |
| --- | --- |
| `FlowMQ::Protocol` | build-tree FMQ/6、FMS/3、FES/1 codec 与全局协议目录 |
| `FlowMQ::Core` | build-tree pattern/session 与应用核心 |
| `FlowMQ::Transport` | build-tree ZeroMQ-style socket 与 CNet TCP/TLS runtime |
| `FlowMQ::FlowMQ` | 唯一安装 target；合并上述公开能力 |

> Branch/release boundary: `v1.2.0` 指向 #100 的 exact release head，已经包含 owner-lane、reuse-port、retained TLS、producer-owned/segmented receive 与公开 `flowmq_recv_slicev()`。当前 `main` 更新：#101/#102 增加 atomic retained PUB/XPUB fanout，#104 增加 generated DataBind ChannelPlan/ServicePlan qualification。除非显式标记 release，下面的行为描述以 current `main` 为准。

安装包不固定 Salts/SaltsUtils 版本，始终消费 latest published stable SDK；公开链接依赖是
`Salts::Core` 与 SaltsUtils 提供的 `Salts::DataBind`；`Salts::CNet`、`Salts::NativeIO`、
`Salts::CSTL` 和 `Salts::CMeta` 都是实现私有依赖。FMP/1 保持现有 header-only wire view/builder ABI，
但生成入口统一使用 DataBind `salts-idlc`，不再依赖已废止的 TBE producer target/tool。

## Caller-driven transport

旧的 callback endpoint API 已删除。普通 socket 保留 ZeroMQ 风格的
`context/socket + bind/connect + send/recv + poll`，入口是 `flowmq_socket.h`。
FlowMQ 1.2 另外提供显式 owner-lane API（`flowmq_owner.h`），用于一个调用线程同时拥有
多个 socket 并共享一次 NativeIO wait。

CNet 仍由 owner 线程直接推进，不创建 worker/progress thread，也不把同一个 socket
包装成 MPSC、Actor 或 Reactive stream。TCP 与 verified TLS 使用同一 peer、decoder、FSM
和 message queue 路径；TLS 只在连接适配层增加证书与主机名验证。

### Owner lane：多 socket 共用一次 wait

FlowMQ 有两种互斥、生命周期固定的 progress 模式：

```text
standalone socket:
  flowmq_socket()
      -> one socket-owned CNet/NativeIO wait
      -> flowmq_poll()

owner-lane socket:
  flowmq_owner_new()
      -> one owner-owned NativeIO backend
      -> flowmq_owner_socket() A
      -> flowmq_owner_socket() B
      -> ...
      -> flowmq_owner_poll()  // one shared wait
```

owner lane **不是线程池**。它不会创建线程，也不会迁移 connection；所有 owner-created
socket 都必须由调用 `flowmq_owner_poll()` 的同一线程串行拥有。一个 owner poll cycle 会：

1. 对每个 live lane-local CNet client 执行一次 external advance；
2. 取 CNet deadline、reconnect、heartbeat、FLOW_UPDATE 与 caller timeout 的最小值；
3. 对共享 NativeIO backend 执行一次 observe；
4. 把每个 completion 精确路由回唯一 owning socket；
5. 对每个 live socket 执行一次 FlowMQ-local reconnect/control/flush progress；
6. 用与普通 `flowmq_poll()` 相同的 readiness 规则返回 `POLLIN/POLLOUT/POLLERR`。

不会在 routed batch 后额外执行第二次 CNet advance，也不会通过 central callback/mailbox
把每条消息绕回另一条线程。

```c
#include <flowmq_owner.h>

flowmq_ctx_t *ctx = flowmq_ctx_new();
flowmq_owner_config_t cfg = FLOWMQ_OWNER_CONFIG_INIT;
cfg.socket_capacity = 4;

flowmq_owner_t *owner = flowmq_owner_new(ctx, &cfg);
flowmq_socket_t *a = flowmq_owner_socket(owner, FLOWMQ_PAIR);
flowmq_socket_t *b = flowmq_owner_socket(owner, FLOWMQ_DEALER);

/* 当前 owner-lane product slice 是 TCP client-side。 */
flowmq_connect(a, "tcp://127.0.0.1:7001");
flowmq_connect(b, "tcp://127.0.0.1:7002");

flowmq_pollitem_t items[] = {
    {.socket = a, .events = FLOWMQ_POLLIN | FLOWMQ_POLLOUT},
    {.socket = b, .events = FLOWMQ_POLLIN | FLOWMQ_POLLOUT},
};
size_t ready = 0;
flowmq_owner_poll(owner, items, 2, 100, &ready);

/* canonical owner-lane I/O: nonblocking send/recv + owner poll */
flowmq_send(a, "hello", 5, FLOWMQ_DONTWAIT);

flowmq_owner_close_socket(owner, a);
flowmq_owner_close_socket(owner, b);
flowmq_owner_term(owner);
flowmq_ctx_term(ctx);
```

owner-lane API 的边界仍然保持窄：

- owner-created socket 不能传给普通 `flowmq_poll()`；会 fail closed；
- owner-created socket 不能直接 `flowmq_close()`；使用 `flowmq_owner_close_socket()`；
- owner 存在时 `flowmq_ctx_term()` 返回 `SALTS_EBUSY`；
- owner-created socket 当前不支持 `flowmq_bind()`；它仍是 client-side shared-wait API；
- 阻塞 `send/recv` 如果需要 transport progress 会 fail closed；owner lane 的 canonical 路径是
  `FLOWMQ_DONTWAIT + flowmq_owner_poll()`。

### 同端口多核 listener：独立 owner + reuse-port

server/listener 多核采用另一条显式路径：多个 **ordinary、彼此独立的 owner-affine socket**
在不同 owner thread/core 上设置 `FLOWMQ_REUSE_PORT=1`，然后 bind 同一个 numeric TCP endpoint。

```text
owner/core 0
  ordinary FlowMQ socket 0
  CNet listener 0 --+
                    +-- same tcp://host:port
owner/core 1        |   SO_REUSEPORT
  ordinary FlowMQ socket 1
  CNet listener 1 --+
```

`FLOWMQ_REUSE_PORT` 是 startup-only `int` 选项，只接受 0/1；默认 0。
值为 1 时，`flowmq_bind()` 通过 released CNet `cnet_listener_init_ex()` 请求
`SO_REUSEPORT`。平台/backend 不支持时原样返回 `SALTS_ENOTSUP`，不会退化成
`SO_REUSEADDR` 或普通 bind。accepted connection 从 admission 到 terminal callback
始终属于实际 accept 它的 socket/owner；FlowMQ 不做 native-handle handoff、connection
migration 或 central message dispatcher。

```c
flowmq_ctx_t *ctx = flowmq_ctx_new();
flowmq_socket_t *service = flowmq_socket(ctx, FLOWMQ_REP);
int reuse_port = 1;

flowmq_setsockopt(service, FLOWMQ_REUSE_PORT,
                  &reuse_port, sizeof(reuse_port));
flowmq_bind(service, "tcp://127.0.0.1:7000");
```

多个 listener 的 pattern FSM、peer registry、HWM/credit、subscriptions、queues 与 diagnostics
仍完全独立；需要跨 lane 的全局业务操作时必须由应用显式编排。

#61 已证明独立 FlowMQ owner 在不同 physical core 上可获得约 1.79x–1.95x 的双 owner
throughput；#67/#69 的 one-owner/two-socket shared-wait qualification 证明 owner lane 相比旧
multi-socket `flowmq_poll()` sleep loop 属于明显更高效的执行类别。#72/#73 进一步证明
same-endpoint reuse-port listener 能在饱和的大消息 service workload 下获得真实多核收益：
64 KiB pipelined DEALER→REP workload 的 two-owner throughput paired median 为约 **1.451x**，
p99 为约 **0.615x**。小消息结果更依赖 workload/load-generator progress 形状，因此不宣称
reuse-port 可以线性扩展。上述收益都不依赖隐藏 worker 或 connection migration。

发送 API 有两个明确的 ownership surface：`flowmq_send()` 在返回成功前复制 borrowed
caller bytes；`flowmq_send_slice()` 接受 canonical owned slice，并把 retained ownership 保持到
对应 CNet terminal。retained `FLOWMQ_SNDMORE` part 成功后 caller 可立即释放自己的 slice 引用，
socket 在有界 staging 中保存 canonical ranges；final part 把完整 multipart 作为一个不超过
`CNET_RETAINED_VECTOR_MAX` ranges 的 logical retained write 提交。copied DATA 与 retained DATA
不在同一 multipart transaction 中混用；ROUTER routing-id envelope 仍可先通过普通 copy API
选择 peer。超出 aggregate SG/send bound 显式失败，不 split、不 flatten、不 copy fallback；
disconnect/cancel/close 精确释放 staged retains。

current `main` 的 PUB/XPUB retained multipart 已不再 fail-closed。第一 retained part 冻结当时
ready 且 subscription-matching 的 peer generation snapshot；final part 重新验证 generation、
完整 message HWM/credit/frame bound 与 peer queue slot，在任何 peer queue 可见前完成所有
fallible allocation/range clone。commit 后每个仍 eligible peer 只持有同一 shared retained
publication 的一个 bounded reference，并在自己的 owner progress 中独立提交 exactly one
`cnet_send_slicev()` logical write。snapshot 后的 subscription change 不改写当前 multipart
peer set；final commit 时已失效/不可 admission 的 peer 按既有 PUB mute/drop 语义被省略。
不存在 retained-to-copy fallback。该 atomic fanout 是 #101/#102 之后的 current-main contract，
不属于 `v1.2.0` tag。

TCP 与 verified TLS 的 retained send 都沿 CNet retained plaintext cursor 消费 genuinely
discontiguous framing/payload ranges，不增加 plaintext flatten/copy fallback。该 API 不是小消息
的默认替代：64-byte retained 会承担额外 slice/refcount/framing 固定成本，而较大 owned payload
可通过消除 admission copy 获益；普通/小 borrowed payload 继续优先使用 `flowmq_send()`。

接收侧现在有三个显式 surface：

- `flowmq_recv()`：兼容 copy surface。对于 owned/segmented DATA，直接从 queue 持有的 payload
  ranges 拷贝到 caller storage；必需的 caller copy 是该 API 的语义。
- `flowmq_recv_slice()`：保持“一个 contiguous canonical slice”的 legacy contract。单 range
  owned DATA 直接转移；真正 multi-range DATA 会在消费 flow credit/FSM 之前执行一次 targeted
  coalesce，失败时 queued message 保持完全不变。
- `flowmq_recv_slicev()`：bounded vector ownership surface。caller 提供空的 `mem_slice_t`
  descriptor array；single-range 直接返回 1 个 owner，multi-range 可直接转移已经由 inbound
  queue 持有的 canonical payload vector，不为 vector 形状强制 coalesce。capacity 不足返回
  `SALTS_ENOBUFS` 并报告 required count，不消费 queued part/FSM/credit。

producer-owned receive fast path 目前针对 plaintext TCP 的可资格形状：CNet backing 可被 bounded
owned stream 保留，完整 DATA packet 再投影为一个或多个 canonical payload ranges。non-DATA、
bounded projection overflow，或超出 owned-stream fast path 的 TCP segmentation/coalescing 形状
继续走显式 copied decoder fallback；文档和 benchmark 不把这些形状伪装成 zero-copy。
`recv_slicev` 的 1 MiB qualification 因此同时记录 true multi-range vector 与 explicit fallback，
而不是声称所有 TCP callback shape 都零拷贝。flow credit、HWM occupancy、`RCVMORE` 与
REQ/REP FSM 都只在成功 dequeue 时推进，不会因为应用继续持有 returned slices 而冻结 transport
credit；所有 returned `mem_slice_t` 必须由 caller `mem_slice_release()`。

`FLOWMQ_RECONNECT_IVL=18` 与 `FLOWMQ_RECONNECT_IVL_MAX=21` 采用 ZeroMQ 的编号和
连接级退避语义：IVL 默认 100ms，`-1` 禁止重连，`0` 表示下一轮 owner progress 立即
尝试；IVL_MAX 默认 `0`，表示固定 IVL，设置为不小于 IVL 的正值后按上限做指数退避。
实际间隔会随机化以降低重连风暴。断线只调度 endpoint，真正的 TCP/TLS connect、
HELLO/SETTINGS 与订阅重放仍由应用后续调用普通 `send/recv/poll` 或 owner-lane
`flowmq_owner_poll()` 推进，不创建 timer thread。

`FLOWMQ_SNDHWM`/`FLOWMQ_RCVHWM` 使用 `int` 消息数，扩展选项
`FLOWMQ_SNDHWM_BYTES`/`FLOWMQ_RCVHWM_BYTES` 使用 `size_t` payload 字节数。四项都必须在
首次 bind/connect 前设置且不能为零；普通发送达到 HWM 返回 `SALTS_ENOBUFS`，PUB/XPUB
按 peer 丢弃无法接纳的 publication。

`FLOWMQ_FLOW_UPDATE_QUANTUM` 使用 `size_t`，默认由 receive byte HWM 推导；
`FLOWMQ_FLOW_UPDATE_IVL` 使用正 `int` 毫秒值，默认 10 ms。二者必须在首次 bind/connect 前
设置。DATA 同时受本地 HWM 与对端累计 credit 约束；控制帧不计 credit。

`FLOWMQ_HEARTBEAT_IVL`/`FLOWMQ_HEARTBEAT_TIMEOUT` 使用非负 `int` 毫秒值，也必须在首次
bind/connect 前设置。IVL 默认为 `0`（禁用）；启用 IVL 且未显式设置 TIMEOUT 时，TIMEOUT
等于 IVL。心跳与断线检测没有后台线程，只在 ordinary socket 的 `send/recv/poll` 或
owner-lane 的 `flowmq_owner_poll()` 中推进。
FMQ/6 PING 不携带对端 TTL，因此当前没有伪装提供 `FLOWMQ_HEARTBEAT_TTL`。
FMQ/6 在 HELLO 后强制 SETTINGS，并以 receiver-driven cumulative credit 协调 DATA；
TCP/TLS 不发送同流 FEC repair symbol。credit 与 heartbeat 都由调用线程推进。

重连创建全新的 peer session：generation、credit、decoder 与 multipart 状态不会跨连接
继承；旧 peer 尚未完成的 outbound 数据也不会自动重播。需要业务级确认或重试时，应在
DATA payload 层携带 correlation/idempotency 信息。

multipart 接收后可用 `flowmq_getsockopt(socket, FLOWMQ_RCVMORE, ...)` 判断是否还有下一
part。该查询返回最近一次成功 `flowmq_recv()` 或 `flowmq_recv_slice()` 的 `MORE` 状态，
不推进网络或 pattern FSM。

ROUTER 的 routing identity 与 ZeroMQ 一样表示当前 live peer，不是可持久化的 session
token；同 identity 重连后的 delayed reply 会指向新 session。单条 outbound multipart
内部会绑定 connection generation，peer 断线后取消，不能把剩余 parts 交给 replacement。

需要诊断慢 peer/backpressure 时，可用
`flowmq_router_peer_status(router, identity, identity_size, &status)` 读取当前 live session 的
只读快照。该查询不推进网络。它公开 admission/completion/rejection、当前及峰值 outstanding
payload、剩余 send credit，以及 connected/ready；不会公开内部 slot、generation、CNet
handle 或累计 wire counter。这里 outstanding 表示已经被 FlowMQ 接纳、但 CNet send completion
尚未确认的应用 DATA，因此可能在远端 credit 仍为 0 时已经回到 0。同 identity 重连后是新
session，统计重新开始。

### TLS certificate 与 HELLO identity 绑定

TLS 只证明 peer 持有受信证书；HELLO identity 仍是 peer 自己声明的 routing identity。需要
授权语义的 TLS ROUTER 应在 bind 前设置 `FLOWMQ_TLS_IDENTITY_POLICY`，把 CNet 已验证证书的
canonical SHA-256 fingerprint（`sha256:` 加 64 个小写十六进制字符）精确绑定到允许的 HELLO
identity。FlowMQ 在 identity 进入 peer table 前校验元组，不改变 FMQ/6 wire 格式。

policy setter 同步校验并复制所有 binding 字符串，调用者可在成功返回后释放输入。启用 policy
的 socket 必须是 ROUTER，且只能 bind 已启用 required client certificate 的 `tls://` listener；
组合不合法返回 `SALTS_EINVAL`，runtime 已初始化返回 `SALTS_EBUSY`。未授权连接只关闭该 peer，
不会把错误写入 ROUTER 的全局 async error；可用 `FLOWMQ_TLS_IDENTITY_REJECTIONS` 读取饱和
`uint64_t` 计数。policy 是启动期配置，不支持热替换；证书轮换可把新旧两个 fingerprint 同时
映射到同一 identity，然后重启应用。

```c
#include <flowmq_socket.h>
#include <flowmq_tls_identity_map.h>
#include <salts_error.h>

#include <string.h>

int configure_authenticated_router(flowmq_socket_t *router,
                                   const char *ca_file,
                                   const char *cert_file,
                                   const char *key_file) {
  static const flowmq_tls_identity_binding_t bindings[] = {{
      sizeof(flowmq_tls_identity_binding_t),
      "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
      "raft-node-1"}};
  flowmq_tls_identity_map_config_t policy =
      FLOWMQ_TLS_IDENTITY_MAP_CONFIG_INIT;
  int required = 1;
  int status;

  policy.bindings = bindings;
  policy.binding_count = sizeof(bindings) / sizeof(bindings[0]);
  status = flowmq_setsockopt(router, FLOWMQ_TLS_CA_FILE, ca_file,
                             strlen(ca_file));
  if (status == SALTS_OK)
    status = flowmq_setsockopt(router, FLOWMQ_TLS_CERT_FILE, cert_file,
                               strlen(cert_file));
  if (status == SALTS_OK)
    status = flowmq_setsockopt(router, FLOWMQ_TLS_KEY_FILE, key_file,
                               strlen(key_file));
  if (status == SALTS_OK)
    status = flowmq_setsockopt(router,
                               FLOWMQ_TLS_REQUIRE_CLIENT_CERTIFICATE,
                               &required, sizeof(required));
  if (status == SALTS_OK)
    status = flowmq_setsockopt(router, FLOWMQ_TLS_IDENTITY_POLICY, &policy,
                               sizeof(policy));
  return status;
}
```

```c
#include <flowmq_socket.h>
#include <salts_error.h>

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

## 构建与验证

```powershell
cmake --preset win-dev-user
cmake --build --preset win-dev-user
ctest --preset win-dev-user --output-on-failure
```

新数据路径 benchmark：

```powershell
cmake --fresh --preset win-release-user -DFLOWMQ_BUILD_ZMQ_BENCHMARK=ON
cmake --build --preset win-release-user --target bench_flowmq_socket
```

ZeroMQ 由 `vcpkg.json` 安装；公平对比必须使用 Release preset，使 FlowMQ 与
libzmq 都链接 Release 产物。GitHub Actions 通过
`qigao/vcpkg-cache/.github/actions/setup-vcpkg-cache@master` 以只读模式消费共享的
GitHub Packages 二进制缓存，并使用 cache-only gate 防止 CI 静默回退到本地重编依赖。

交叉编译时 target SDK 与 host code-generation 工具必须分开。设置
`SALTS_ROOT` / `SALTS_UTILS_ROOT` 指向目标平台 SDK，同时设置
`SALTS_HOST_ROOT` / `SALTS_UTILS_HOST_ROOT` 指向构建主机 SDK；FlowMQ 使用 host
SaltsUtils 中的 `salts-idlc` 生成 FMP/1 头文件，再用 target Salts/SaltsUtils 编译和链接。
交叉编译必须显式提供 host roots；native build 的 host tool 则来自同一已解析的 SDK profile。

详细所有权和关闭顺序见 [架构说明](docs/ARCHITECTURE.md)，wire 契约见
[FMQ/6 协议](docs/FMQ_WIRE_PROTOCOL.md)。
