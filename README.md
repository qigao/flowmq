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

> Branch/release boundary: `v1.2.1` 在 `v1.2.0` 的 owner-lane、reuse-port、retained TLS、producer-owned/segmented receive 与公开 `flowmq_recv_slicev()` 基础上，包含 #101/#102 的 atomic retained PUB/XPUB fanout、#104 的 generated DataBind ChannelPlan/ServicePlan qualification，以及 #111/#112 的 CMeta ownership、最新 Salts SDK 适配与显式多 owner 验证。除非显式标记 release，下面的行为描述以 current `main` 为准。

安装包不固定 Salts/SaltsUtils 版本，始终消费 latest published stable SDK；公开链接依赖是
`Salts::Core` 与 SaltsUtils 提供的 `Salts::DataBind`；`Salts::CNet`、`Salts::CNetManager`、`Salts::NativeIO`、
`Salts::CSTL` 和 `Salts::CMeta` 都是实现私有依赖。FMP/1 保持现有 header-only wire view/builder ABI，
但生成入口统一使用 DataBind `salts-idlc`，不再依赖已废止的 TBE producer target/tool。

BoringSSL 已退出 FlowMQ 的依赖与链接配置。TLS 提供者由发布的 Salts SDK 管理，
FlowMQ 通过 `Salts::CNet` 使用 TCP/TLS；manifest 声明 `zeromq` 以及已安装 SaltsUtils
导出配置所需的 `lua`、`quickjs-ng`，证书及主机名
校验继续由 CNet 负责。

本集成分支要求 Salts #1001 候选 SDK 提供 `Salts::CNetManager` 和
`<cnet/manager.h>`，尚不能仅用 latest published stable 构建。每个 socket
client 使用固定容量的 owner-local manager，接管 connect/adopt 的绑定与终态记录，
queued parts 消费结束后才释放额外 context hold。外部 owner 通过有界 `advance`
提交关闭，仍由既有 NativeIO/CNet 路径观察真实终态。消息、路由、重连、TLS 策略和
peer 协议状态继续归 FlowMQ；没有新增跨线程队列或改变 wire/API。
部署时需带上候选 SDK 的 `cnet_manager` 共享库。回滚只需撤销内部适配及私有链接依赖，
无需数据迁移。`test_flowmq_socket` 额外覆盖 64 次断开后消费排队消息、再复用 peer 的循环。

候选 SDK 主机验收通过 `native-sdk-release.yml` 的手动入口运行，同时指定
`salts_candidate_run_id` 和完整的 `salts_candidate_sha`。生产任务必须是成功完成、
保留 SDK artifacts 的 Salts CI 手动任务。Linux、Windows、macOS 消费该候选包并运行
正式 CTest，SaltsUtils 继续解析最新发布包。此入口跳过交叉编译、性能脚本、打包及发布。

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

自 `v1.2.1` 起，PUB/XPUB retained multipart 已不再 fail-closed。第一 retained part 冻结当时
ready 且 subscription-matching 的 peer generation snapshot；final part 重新验证 generation、
完整 message HWM/credit/frame bound 与 peer queue slot，在任何 peer queue 可见前完成所有
fallible allocation/range clone。commit 后每个仍 eligible peer 只持有同一 shared retained
publication 的一个 bounded reference，并在自己的 owner progress 中独立提交 exactly one
`cnet_send_slicev()` logical write。snapshot 后的 subscription change 不改写当前 multipart
peer set；final commit 时已失效/不可 admission 的 peer 按既有 PUB mute/drop 语义被省略。
不存在 retained-to-copy fallback。该 atomic fanout 来自 #101/#102，随 `v1.2.1` 发布，
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
#include <cmeta_error.h>

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

参考 [SaltsUtils](https://github.com/qigao/salts-utils) 的恢复流程，先安装 .NET SDK 8、
PowerShell 7，并在父环境提供具有 `read:packages` 权限的 `GITHUB_TOKEN`。
脚本不自动读取 `.env`，也不记录凭据。Windows 在 Visual Studio developer 环境中运行：

```powershell
./cmake/ci/restore-native-sdk.ps1 -Rid windows-x64
cmake --preset win-release-user -DBUILD_TESTS=ON
cmake --build --preset win-release-user
ctest --preset win-release-user --output-on-failure
```

Linux 使用 `pwsh -File cmake/ci/restore-native-sdk.ps1 -Rid linux-x64`，随后运行
`linux-release-user` 的 configure/build/test preset。

Salts.Native 与 SaltsUtils.Native 均使用 `Version="*"`，每次 restore 通过
`--no-cache --force-evaluate` 解析最新稳定版；不限制版本、不回退旧包。
NuGet assets 是版本和路径的事实源，包默认存放于 `stage/nuget`，可通过
`QIGAO_NUGET_PACKAGES` 指定缓存位置。Release presets 消费
`stage/dependencies/salts/<RID>` 与 `stage/dependencies/salts-utils/<RID>` 的稳定链接；configure 本身不下载 SDK，
且清除旧的包路径缓存。恢复步骤只更新链接，拒绝覆盖已有的普通 SDK 目录。

SaltsUtils 由各 preset 的 `SALTS_UTILS_ROOT` 指向恢复的发布 SDK，无需本地重编译。
[SaltsUtils 4.2.0](https://github.com/qigao/salts-utils/releases/tag/v4.2.0) 已适配
Salts 2.1；其生成接口有不兼容调整，升级后必须重新生成 IDL 并重新编译消费者。
重新发布的 4.2.0 已移除 Lua/QuickJS 的自动查找，FlowMQ 无需这两个运行时，
manifest 也不再声明它们。已缓存旧 4.2.0 的环境需重新恢复该版本的包内容；
本次包 SHA-256 为 `3993b8c5b7245daabf51633f20728fb38ccbdaf568666681e0ba3f6a63ea82ca`。
这些版本和哈希仅记录验证环境，不是依赖约束。
首次配置需显式启用 `BUILD_TESTS`；共享 preset 的 `ENABLE_TESTS` 不是本项目的测试开关。

发布 SDK 包含 Release 库。`win-dev-user` / `linux-dev-user` 仍要求在既有
`PKG_ROOT` 路径安装匹配的 Debug SDK，不能混用旧版或 Release 库。
Salts 的 Core 头文件与平台调用已采用当前 `cmeta_*` API；升级后需重新构建 FlowMQ
及其消费者。FlowMQ 公开函数、消息格式与 caller-driven 线程模型保持不变。
FMP/1 由当前 SaltsUtils 的 `salts-idlc` 生成；工具更新会触发头文件重新生成。

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

多核 owner 基准使用现有公开 API：每个工作线程在本线程创建独立 context，
创建、推进并关闭自己的 PAIR sockets。固定 4 条回环连接（8 个 socket），
分配给 1/2/4 个 owner；每条连接的收发两端位于同一 owner。
这验证的是独立 socket 工作负载的并行吞吐，不是单个 ROUTER/PUB 的跨核分片。
生产 runtime、公开 API 和 wire 格式没有因此改变。

业务接入示例见 [multicore_reqrep.c](examples/multicore_reqrep.c)：主线程拥有一组
REQ sockets，每个 worker 在自己的线程创建 context 与 REP socket，绑定独立的
回环 TCP 端口。主线程统一 `flowmq_poll` 推进所有 REQ sockets；只推进当前等待
应答的 socket 会延迟其他请求发出。每轮向每个 worker 发一个请求，逐条校验
应答中的 worker 标识、序号与单 part 边界，全部完成后才开始下一轮。

示例参数依次为 worker 数（1–16，默认 4）、每 worker 请求数（1–10000，默认 100）、
等待超时毫秒（1–60000，默认 5000）。worker 的单次收发、最终排空，以及客户端
整轮等待分别受该超时限制。发送和接收 HWM 均为 1，关闭自动重连，队列满时
继续推进至成功或超时；首个错误通过 atomic 传播，其他线程停止等待。
endpoint 仅在启动时通过 mutex 发布，消息通过 TCP 复制传递，无跨线程 socket/context。
每个 owner 在本线程关闭资源；主线程收齐应答后通知 worker 停止，等 worker 退出后
再关闭客户端，最后 join 全部线程。示例状态空间为 O(workers)，完整请求数为
workers × requests，不引入业务线程池或新的公开 API。

在下述修复版 SDK preset 配置完成后：

```powershell
cmake --build --preset win-release-local-sdk --target flowmq_multicore_reqrep
ctest --preset win-release-local-sdk -R '^test_flowmq_multicore_reqrep_' -V
```

CTest 覆盖 1/4/16 个 worker，每个处理 100 条请求；程序只有应答全部验证且所有
owner 成功关闭后才返回成功。在配置了相同 SDK/vcpkg runtime PATH 的开发者终端，
可直接运行 `build/Msvc-Release-LocalSDK/bin/flowmq_multicore_reqrep.exe 4 100 5000`，
成功输出 `workers=4 replies=400 verified; all owners closed`。
此例展示独立 endpoint 的多核业务接入，不代表单个逻辑 ROUTER 的跨核分片；
其逐轮同步与低占用等待策略也不作为吞吐基准。

本次验证使用含下述 CNet 修复的本地 SDK。先在 Salts 仓库按其构建说明安装 SDK；
本机安装入口为 `cmake --build --preset install-win-release-ci`，产物位于
`../salts/stage/sdk/windows-x64`。随后在上述 Windows 开发者环境运行：

```powershell
$env:FLOWMQ_LOCAL_SALTS_ROOT = (Resolve-Path ../salts/stage/sdk/windows-x64).Path
cmake --preset win-release-local-sdk -DBUILD_TESTS=ON
cmake --build --preset win-release-local-sdk
ctest --preset win-release-local-sdk -LE benchmark --output-on-failure
ctest --preset win-release-local-sdk -R '^bench_flowmq_socket_owners_' -V --output-log build/Msvc-Release-LocalSDK/owner-scaling.log
```

`win-release-local-sdk` 使用独立 build/install 目录，不替换发布 SDK 的链接；
`FLOWMQ_LOCAL_SALTS_ROOT` 必须指向完整 SDK，缺失时配置失败。
发布包验证仍使用 `win-release-user`；Linux 对应使用 `linux-release-user`，
但下述修复尚未发布，不能假设 restore 得到的包已包含它。
基准同样保留为正式 CTest 项，失败不会被跳过或标记成预期成功。
每批 16 条消息，每条含两个等长 part，总 payload 为 64 B 或 64 KiB。
所有消息检查长度、内容、连接身份、序号和 `RCVMORE`；预热同时验证发送 HWM
拒绝与恢复，关闭和 context 释放的返回值也纳入结果。

每个负载重复 3 次，轮换 1/2/4 owner 的运行顺序。TCP 64 B 默认每连接
4096 批，TCP 64 KiB 和 TLS 64 B 为 256 批，TLS 64 KiB 为 32 批；
同一负载的所有 owner 配置使用相同总连接数、消息数、字节数和窗口。
可用父环境 `FLOWMQ_OWNER_BENCH_ROUNDS`（1–4096）覆盖批数，
`FLOWMQ_OWNER_BENCH_REPEATS`（1–9）覆盖重复次数；非法值直接失败。

只汇总成功用例的 `OWNER_RESULT` CSV 行：吞吐按完整应用消息及 payload
计一次，不计协议/TLS 头；墙钟从统一放行到最后一个 owner 完成，排除建连、
握手、预热和关闭，包含 FlowMQ 编解码及消息校验成本。
P99 是从每条消息首次发送尝试到收齐最后一个 part 的实测延迟，包含排队，
不把批次平均耗时冒充单消息延迟。CPU 为该测量阶段整个进程的 CPU 秒数；
RSS 为进程生命周期峰值，不是每行独立的内存增量。线程未绑核，Windows
CPU 时间存在计量粒度限制。诊断适配仅在 benchmark 中使用系统统计 API。

2026-10-07 合并主线前（`f3192c1`）的本机测量：Windows Release、Salts 2.1.0 加本地 CNet TLS 缓冲区修复、
Ryzen 9 7940HX（16 核 / 32 逻辑处理器），四类负载三轮吞吐中位数如下。
版本仅记录测量环境，不构成依赖版本限制；所有行均使用同一修复版 SDK。

| 负载 | 1 owner | 2 owners | 4 owners | 4 / 1 |
| --- | ---: | ---: | ---: | ---: |
| TCP 64 B（消息/s） | 684,105 | 1,281,235 | 2,444,099 | 3.57× |
| TCP 64 KiB（MiB/s） | 1,772.93 | 2,169.20 | 3,006.44 | 1.70× |
| TLS 64 B（消息/s） | 10,519 | 13,142 | 19,957 | 1.90× |
| TLS 64 KiB（MiB/s） | 14.64 | 20.06 | 40.20 | 2.75× |

上述倍数计算为 4-owner 吞吐中位数 / 1-owner 吞吐中位数。
收益是吞吐而非普遍降延迟：TCP 64 B 的 P99 中位数从 25.5 增至 29.7 µs，
TCP 64 KiB 从 717.8 增至 1828.7 µs，TLS 64 B 从 2063.4 增至 4877.8 µs，
TLS 64 KiB 从 75.48 增至 112.33 ms。
TLS 小消息的进程 CPU 秒数中位数也从 1.562 增至 3.234。
三轮样本存在波动（例如单 owner TCP 64 KiB 为 1072.48–1793.65 MiB/s），
中位数仅用于本机扩展性观察，不能用前后不同运行的绝对值判定补丁加速。
本机回环结果不能外推为远程网络、单连接或共享逻辑 socket 的性能承诺。

**HIGH｜事实：CNet TLS 接收缓冲区修复已合并上游，尚未发布。**
修复见 [Salts PR #993](https://github.com/qigao/salts/pull/993)；当前发布的 Salts 2.1.0
尚不包含该补丁，因此 TLS 完整验证仍使用 `win-release-local-sdk`。
原 SDK 的 TLS 64 KiB 持续负载曾在单 owner 下返回 `SALTS_EPROTO`。
`../salts/cnet/src/cnet_owner.c` 将 TLS `read_buffer` 交给异步 NativeIO 接收，
而 `../salts/cnet/src/cnet_tls.c` 同时用它承载解密结果和未消费的明文后缀，
两种生命周期会重叠。修复在私有 TLS engine 内分配独立明文区，普通读取、
无需求探测和部分读取共用该明文区，transport 缓冲区专供接收至完成被消费。
每 TLS 会话增加 `tls_io_buffer_bytes`（FlowMQ 默认 17 KiB），无热路径新增分配；
初始化容量溢出或分配失败仍立即报错，释放随 engine 统一完成。
公开 ABI、TLS 校验、wire 格式和单 owner 进度模型不变。

`../salts/cnet/tests/cnet_tls_test.c` 新增 TLS 1.2/1.3 × 普通读取/探测四个
缓冲区隔离用例，覆盖部分明文读取期间的 transport 存储变化。
修复版 SDK 实测 CNet 35/35、FlowMQ 普通回归 17/17（含三个跨线程 REQ/REP
示例用例）、性能用例 4/4 通过。
合并主线 `4a48261` 后，保留 owner lane、共享等待、owned receive 和 retained fanout，
并使用重新发布的 SaltsUtils 4.2.0 重新生成 FMP/1 与 ChannelPlan/ServicePlan 测试绑定。
两个 SDK profile 均已全量构建，不再安装 Lua/QuickJS。修复版 Salts 的 22 项可执行行为
测试全部通过（5 项重点回归、17 项相邻回归，含三个多核示例）；未执行主线的源码
marker 检查 `test_flowmq_segmented_receive_contract`，它不计入上述行为测试结果。
纯发布版组合（Salts 2.1.0 + SaltsUtils 4.2.0）的协议、ESB、流解码、FMP/1 schema
和 DataBind projection 测试 5/5 通过，此结果不覆盖尚待 Salts 发版的 TLS 修复。
前述吞吐表和性能用例结果来自合并主线前的 SaltsUtils 本地构建环境，未在当前实现上
重测，不能作为当前主线的性能结论。
原始 CSV 位于本次构建目录的 `owner-scaling-fixed.log` 与 `owner-tls-fixed.log`；
这些日志为本地产物，可用上述 CTest 命令重新生成。
Linux/macOS 与并发 sanitizer 尚未验证。

详细所有权和关闭顺序见 [架构说明](docs/ARCHITECTURE.md)，wire 契约见
[FMQ/6 协议](docs/FMQ_WIRE_PROTOCOL.md)。

### #1001 candidate CI

Until CNetManager is published, `cmake/ci/salts-candidate.json` pins the successful
Salts producer run and commit used by branch/PR host qualification. The restore
action validates run provenance and the SDK manifest before use. Linux, Windows
and macOS build the full configured graph, run CTest and install the SDK.
Candidate runs skip cross packaging and publication; tags cannot select a
candidate. Remove the temporary pin after the required SDK is published and
validate the ordinary released dependency graph before releasing this project.
