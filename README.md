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

安装包的公开依赖是 `Salts::Core` 与 SaltsUtils 的 `Salts::TbeSchema`；
`Salts::CNet`、`Salts::CSTL` 和 `Salts::CMeta` 是实现私有依赖。FMP/1 使用
header-only TBE wire view/builder，不依赖 JSON typed codec。

## Caller-driven transport

旧的 callback endpoint API 已删除。新的公开边界只保留 ZeroMQ 风格的
`context/socket + bind/connect + send/recv + poll`，入口是 `flowmq_socket.h`。

CNet 仍由 socket owner 线程直接推进，不创建 worker/progress thread，也不把同一个 socket
包装成 MPSC、Actor 或 Reactive stream。TCP 与 verified TLS 使用同一 peer、decoder、FSM
和 message queue 路径；TLS 只在连接适配层增加证书与主机名验证。

`FLOWMQ_RECONNECT_IVL=18` 与 `FLOWMQ_RECONNECT_IVL_MAX=21` 采用 ZeroMQ 的编号和
连接级退避语义：IVL 默认 100ms，`-1` 禁止重连，`0` 表示下一轮 owner progress 立即
尝试；IVL_MAX 默认 `0`，表示固定 IVL，设置为不小于 IVL 的正值后按上限做指数退避。
实际间隔会随机化以降低重连风暴。断线只调度 endpoint，真正的 TCP/TLS connect、
HELLO/SETTINGS 与订阅重放仍由应用后续调用 `send/recv/poll` 推进，不创建 timer thread。

`FLOWMQ_SNDHWM`/`FLOWMQ_RCVHWM` 使用 `int` 消息数，扩展选项
`FLOWMQ_SNDHWM_BYTES`/`FLOWMQ_RCVHWM_BYTES` 使用 `size_t` payload 字节数。四项都必须在
首次 bind/connect 前设置且不能为零；普通发送达到 HWM 返回 `SALTS_ENOBUFS`，PUB/XPUB
按 peer 丢弃无法接纳的 publication。

`FLOWMQ_FLOW_UPDATE_QUANTUM` 使用 `size_t`，默认由 receive byte HWM 推导；
`FLOWMQ_FLOW_UPDATE_IVL` 使用正 `int` 毫秒值，默认 10 ms。二者必须在首次 bind/connect 前
设置。DATA 同时受本地 HWM 与对端累计 credit 约束；控制帧不计 credit。

`FLOWMQ_HEARTBEAT_IVL`/`FLOWMQ_HEARTBEAT_TIMEOUT` 使用非负 `int` 毫秒值，也必须在首次
bind/connect 前设置。IVL 默认为 `0`（禁用）；启用 IVL 且未显式设置 TIMEOUT 时，TIMEOUT
等于 IVL。心跳与断线检测没有后台线程，只在 owner 调用 `send`、`recv` 或 `poll` 时推进。
FMQ/6 PING 不携带对端 TTL，因此当前没有伪装提供 `FLOWMQ_HEARTBEAT_TTL`。
FMQ/6 在 HELLO 后强制 SETTINGS，并以 receiver-driven cumulative credit 协调 DATA；
TCP/TLS 不发送同流 FEC repair symbol。credit 与 heartbeat 都由调用线程推进。

重连创建全新的 peer session：generation、credit、decoder 与 multipart 状态不会跨连接
继承；旧 peer 尚未完成的 outbound 数据也不会自动重播。需要业务级确认或重试时，应在
DATA payload 层携带 correlation/idempotency 信息。

multipart 接收后可用 `flowmq_getsockopt(socket, FLOWMQ_RCVMORE, ...)` 判断是否还有下一
part。该查询返回最近一次成功 `flowmq_recv()` 的 `MORE` 状态，不推进网络或 pattern FSM。

ROUTER 的 routing identity 与 ZeroMQ 一样表示当前 live peer，不是可持久化的 session
token；同 identity 重连后的 delayed reply 会指向新 session。单条 outbound multipart
内部会绑定 connection generation，peer 断线后取消，不能把剩余 parts 交给 replacement。

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
libzmq 都链接 Release 产物。

详细所有权和关闭顺序见 [架构说明](docs/ARCHITECTURE.md)，wire 契约见
[FMQ/6 协议](docs/FMQ_WIRE_PROTOCOL.md)。
