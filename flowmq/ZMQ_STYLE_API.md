# FlowMQ ZMQ-like Application API

FlowMQ Application API 为单个 FMQ pattern endpoint 提供类似 ZeroMQ socket 的薄封装：创建、
启动、发送、回调接收、停止和销毁。它适合不需要自行编写高级 graph 的应用入口；底层使用同一个
FMQ adapter、最小 typed graph bridge 和 CoroNet endpoint，没有额外 socket 或协议状态。默认同步模式不增加队列或
工作线程；显式配置 async send 时增加一条有界队列和一个保序发送 worker。

这不是 libzmq 兼容层。FlowMQ 不实现 ZMTP，不兼容 ZeroMQ wire/API，也不能与 ZeroMQ peer 直接
通信。这里的 “ZMQ-like” 只表示复用 PUB/SUB、PUSH/PULL、REQ/REP、ROUTER/DEALER 和 PAIR 的
通信模式。

## Build and run

启用 `BUILD_EXAMPLES` 后构建三个示例：

```powershell
cmake --preset win-release-user -DBUILD_EXAMPLES=ON
cmake --build --preset win-release-user --target flowmq_zmq_style_pub_sub flowmq_zmq_style_req_rep flowmq_zmq_style_router_dealer
```

YAML host 入口也会随 `BUILD_EXAMPLES` 构建：

```powershell
cmake --build --preset win-release-user --target flowmq_yaml_endpoint
build\Msvc-Release\bin\flowmq_yaml_endpoint.exe `
  flowmq\examples\zmq_style.yml fmq.example.pub 1000
```

命令格式为 `<yaml> <adapter> [duration_ms] [payload]`。它负责读取 YAML、调用统一 resolver，
再从指定 adapter 创建并启动一个 Application facade。`PUB`/`PUSH` 可发送 payload；其他 pattern
需要对应 peer 才能完成实际通信。`fmq.yml` 是产品装配配置，包含 management 和 durable store 引用，
仍需宿主注入对应 provider，不能由这个轻量入口独立启动。

每个示例在一个进程内创建一对 endpoint，默认使用本机 TCP 端口 7711、7712 和 7713。可用第一个
参数覆盖端口：

```powershell
.\build\Msvc-Release\bin\flowmq_zmq_style_pub_sub.exe 17711
.\build\Msvc-Release\bin\flowmq_zmq_style_req_rep.exe 17712
.\build\Msvc-Release\bin\flowmq_zmq_style_router_dealer.exe 17713
```

源码位于：

- [PUB/SUB](examples/zmq_style_pub_sub.c)：按 topic prefix fan-out；
- [REQ/REP](examples/zmq_style_req_rep.c)：REP callback 内同步生成回复；
- [ROUTER/DEALER](examples/zmq_style_router_dealer.c)：detach route 后在 callback 外延迟回复；
- [YAML endpoints](examples/zmq_style.yml)：同一组 endpoint 的 YAML v1 配置。

## Conceptual mapping

| ZeroMQ concept | FlowMQ Application API | Difference |
| --- | --- | --- |
| context + socket type | `turbo_flow_fmq_app_create()` / `_ex()` | 每个 app 拥有一个最小 graph 和一个 FMQ endpoint；`_ex` 可绑定 host execution |
| `bind` / `connect` | `endpoint.mode` + transport fields | 在 create 时固定，start 时建立 listener/connection |
| `zmq_send` | `turbo_flow_fmq_app_send()` | payload 被复制并发布到 graph input |
| asynchronous send | `turbo_flow_fmq_app_send_async()` | copied admission；单 worker 聚合 micro-batch 并回调 completion |
| blocking `zmq_recv` | `options.on_message` | receive 是 CoroNet owner lane 上的 borrowed callback |
| multipart metadata | `turbo_flow_msg_t` + FMQ accessors | topic、identity、correlation 和 route 是类型化 metadata |
| `zmq_close` | `turbo_flow_fmq_app_stop()` + `destroy()` | stop 可重复；destroy 会在需要时先 stop |

默认 Application API 不暴露独立 context，也没有阻塞 `recv`。`turbo_flow_fmq_app_create_ex()` 可将
endpoint 放到 host 已经驱动的 borrowed CoroNet context 或 thread-pool lane；它不改变 callback 或
send 语义。需要 processor、subgraph、queue、持久化或运行时跨 lane 调度时，仍应直接注册 FMQ
adapter 并编译完整 graph；不要在 callback 中另造一套消息循环。

## Pattern contract

| Pattern | Callback | `app_send` | Receive/reply rule |
| --- | --- | --- | --- |
| PUB | 禁止 | 支持 | 当前匹配 SUB fan-out；无匹配返回 `TURBO_ENOTCONN` |
| SUB | 必须 | 不支持 | `topic` 是订阅前缀，空字符串匹配全部 topic |
| PUSH | 禁止 | 支持 | 对 eligible PULL round-robin |
| PULL | 必须 | 不支持 | 每条消息只交给一个 PULL |
| REQ | 可选 | 支持 | 同一 endpoint 仅允许一个 outstanding request |
| REP | 必须 | 不支持 | callback 修改当前 message，返回时同步回复 |
| ROUTER | 可选 | 支持 | 可 detach 收到的 route，再用 `send_message` 延迟回复 |
| DEALER | 可选 | 支持 | `identity` 必须配置且 live peer 间唯一 |
| PAIR | 可选 | 支持 | bind 端最多一个 live peer |
| XPUB/XSUB | 可选 | 支持 | callback 同时承载 data 和显式 subscription event |

发送前必须成功 start。connect endpoint 的 start 成功只表示运行时已启动，不保证远端握手已经完成；
在连接或 SUB subscription 尚未生效时，send 可以返回 `TURBO_ENOTCONN`。示例只对这个明确的瞬态
错误做有界重试，不重试配置、状态或 callback 错误。

## Lifecycle

```c
turbo_flow_fmq_config_t endpoint = TURBO_FLOW_FMQ_CONFIG_INIT;
turbo_flow_fmq_app_options_t options = TURBO_FLOW_FMQ_APP_OPTIONS_INIT;
turbo_flow_fmq_app_t *app = NULL;

endpoint.pattern = TURBO_FLOW_FMQ_REQ;
endpoint.mode = TURBO_FLOW_FMQ_CONNECT;
endpoint.transport = TURBO_FLOW_FMQ_TCP;
endpoint.host = "127.0.0.1";
endpoint.port = 7712;
options.on_message = on_reply;

int rc = turbo_flow_fmq_app_create(&endpoint, &options, &app);
if (rc == TURBO_OK) rc = turbo_flow_fmq_app_start(app);
if (rc == TURBO_OK) rc = turbo_flow_fmq_app_send(app, "hello", 5u);
if (app) (void)turbo_flow_fmq_app_stop(app);
turbo_flow_fmq_app_destroy(app);
```

`endpoint` 和 `options` 都必须使用版本化 INIT 宏初始化。Application 在 create 时复制并固化 graph
所需配置；host 必须串行化同一 app 的 create/start/stop/destroy 生命周期。`start()` 重复调用返回
`TURBO_EALREADY`，未 start 就 send 返回 `TURBO_EBUSY`，不支持发送的 pattern 返回
`TURBO_ENOTSUP`。

### Host-driven CoroNet execution

需要让多个 endpoint 复用 host 的 CoroNet 调度资源时，使用
`turbo_flow_fmq_app_create_ex(endpoint, options, execution, out)`。execution 设为
`TURBO_FLOW_CORONET_EXECUTION_BORROWED_CONTEXT` 并提供 `context`，或设为
`TURBO_FLOW_CORONET_EXECUTION_POOL_LANE` 并提供 `pool` 和稳定 `lane`。

`BORROWED_CONTEXT` 和 `POOL_LANE` 始终由 host 持有；create 失败、app stop 或 app destroy 都不会停止或
销毁它们。host 必须在调用 create/start 前启动 owner loop，并保持其运行直到所有绑定 facade 完成
destroy，然后才能停止 runner 并销毁 context/pool。`OWNED_CONTEXT` 在 Facade `_ex` 中返回
`TURBO_EINVAL`，因为 create 失败无法安全表达部分所有权转移；需要 adapter 接管 context 时使用底层
`turbo_flow_fmq_register_adapter_ex()`。完整的可编译 runner、binding 和销毁顺序见
[`test_fmq.c`](tests/test_fmq.c) 中的 `keeps explicitly borrowed facade contexts owned by the host`。

PUB、PUSH、DEALER 可在首次 `start()` 前调用
`turbo_flow_fmq_app_configure_async_send()`。配置同时限制队列 item 数、payload bytes、micro-batch 大小和
linger；`send_async()` 返回 `TURBO_OK` 后 facade 已取得副本所有权，非空 completion 最终调用一次。
队列满返回 `TURBO_ENOSPC` 且不调用 completion。`stop()` 会排空已接收消息；completion 不得对同一 app
调用 `stop()` 或 `destroy()`。

## Receive ownership

`on_message(app, message, ctx)` 中的 message、payload 和 FMQ view 都只借用到 callback 返回。callback
返回非 `TURBO_OK` 会使当前 graph attempt 失败。不可从同一个 callback 重入该 app 的 lifecycle 或
send API。

读取 metadata 使用：

```c
tstr_v topic;
tstr_v identity;
uint64_t correlation_id;

int topic_rc = turbo_flow_fmq_message_topic(message, &topic);
int identity_rc = turbo_flow_fmq_message_identity(message, &identity);
int correlation_rc =
    turbo_flow_fmq_message_correlation_id(message, &correlation_id);
```

这些 view 不拥有内存。普通异步处理可用 `turbo_flow_msg_clone()` 建立 owned message；ROUTER delayed
reply 必须先 detach route，再 clone 或 move。

## Synchronous REP

REP 是严格同步设计。callback 用 `turbo_flow_fmq_app_message_set_payload_copy()` 替换请求 payload，
并返回 `TURBO_OK`；facade 在同一次 graph dispatch 中复用 correlation 和 peer session 发出回复：

```c
static int on_request(turbo_flow_fmq_app_t *app, turbo_flow_msg_t *message, void *ctx) {
  static const char reply[] = "world";
  (void)app;
  (void)ctx;
  return turbo_flow_fmq_app_message_set_payload_copy(message, reply, sizeof(reply) - 1u);
}
```

REP callback 不可保存请求并稍后回复，也不可调用 `app_send`。需要 worker 调度或 delayed reply 时使用
ROUTER/DEALER。

## Delayed ROUTER reply

ROUTER 收到的 request 带有 generation-fenced route。callback 中先 detach，再 clone：

```c
int rc = turbo_flow_fmq_message_detach_router_route(message);
if (rc == TURBO_OK) rc = turbo_flow_msg_clone(&pending, message);
```

之后可在 callback 外修改 owned payload 并发送：

```c
rc = turbo_flow_fmq_app_message_set_payload_copy(&pending, reply, reply_size);
if (rc == TURBO_OK) rc = turbo_flow_fmq_app_send_message(router, &pending);
turbo_flow_msg_cleanup(&pending);
```

route 绑定 `owner_instance_id + session_id + session_generation`。peer 断线、同 identity 重连或 ROUTER
restart 后，旧 route 返回 `TURBO_ENOTCONN`。route 是易失运行时 capability，不可写入 Redis/SQLite
后重放。

## YAML endpoint configuration

直接 C config 适合嵌入式创建；部署配置应使用 YAML resolver，并从 immutable resolved snapshot 创建：

```c
turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;
turbo_flow_fmq_app_t *app = NULL;

int rc = turbo_flow_fmq_app_create_resolved(resolved, "fmq.example.sub", &options, &app,
                                            &error);
```

host 负责读取 `.yml` 并调用 `turbo_flow_config_resolve_yaml()`；Application facade 不自行读文件，也不
绕过统一 resolver。transport、host、port、pattern、topic、identity、timeout、heartbeat、reconnect
和 HWM 都是 FMQ provider 的 YAML 配置。graph 只引用 adapter 与 typed operation，不解释这些字段。

## Choosing the facade or a full graph

Application facade 用于一个 endpoint 与一个 host callback 的最短路径。出现以下任一需求时，直接使用
完整 graph：多个 endpoint、TurboFlow Policy processor、条件分支、HTTP/RPC/Redis/SQL enrichment、
Disruptor lane 配置、memory/Redis Stream queue、持久化、重放、broker 或 XPUB/XSUB proxy。两种入口
共享同一个 FMQ adapter 和 pattern 实现，因此不存在第二套 client runtime。
