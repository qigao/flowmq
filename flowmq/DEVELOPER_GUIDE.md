# FlowMQ 开发指南

FlowMQ 提供类似 ZeroMQ 的消息模式，但使用自己的 FlowMQ wire v3，不与 ZeroMQ wire protocol 兼容。
它既可以作为简单的 Application facade 使用，也可以注册为 TurboFlow Graph adapter。

## 1. 选择接口层级

- Application facade：单 endpoint、类似 ZeroMQ 的 `create/start/send/callback/stop`，适合普通应用。
- Resolved Application：endpoint 来自 YAML，生命周期仍由应用管理。
- Graph adapter：需要多个 stage、RuleSet、Queue、executor 或产品级组合时使用。
- Secure adapter：显式注入 auth provider、SecurityRealm 和 key provider，启用 wire v3 认证与默认拒绝 ACL。

建议从 Application facade 开始；只有业务确实需要 Graph 拓扑时再使用 adapter API。

## 2. 构建与链接

仓库构建：

```powershell
cmake --preset win-release-user
cmake --build --preset win-release-user --target tf_fmq
```

外部 CMake 项目：

```cmake
cmake_minimum_required(VERSION 3.25)
project(flowmq_example C)

find_package(TurboFlow CONFIG REQUIRED)

add_executable(flowmq_example main.c)
target_link_libraries(flowmq_example PRIVATE TurboFlow::FMQ)
```

公开入口是 `turbo_flow_fmq.h`，所有结构必须使用对应 `*_INIT` 宏初始化。

## 3. Pattern 配对

| BIND | CONNECT | 用途 |
| --- | --- | --- |
| PUB | SUB | topic fan-out |
| PUSH | PULL | pipeline / work distribution |
| REP | REQ | 严格 request/reply |
| ROUTER | DEALER | 带 identity 的异步 request/reply |
| PAIR | PAIR | 一对一双向连接 |
| XPUB | XSUB | 显式 subscription event 与高级 pub/sub |

不兼容的 pairing 在 HELLO 阶段失败，不会按其他 pattern 继续运行。

## 4. 可直接运行的示例

启用 `BUILD_EXAMPLES` 后构建：

```powershell
cmake --preset win-release-user -DBUILD_EXAMPLES=ON
cmake --build --preset win-release-user --target `
  flowmq_zmq_style_pub_sub `
  flowmq_zmq_style_req_rep `
  flowmq_zmq_style_router_dealer
```

运行：

```powershell
build\Msvc-Release\bin\flowmq_zmq_style_pub_sub.exe 7711
build\Msvc-Release\bin\flowmq_zmq_style_req_rep.exe 7712
build\Msvc-Release\bin\flowmq_zmq_style_router_dealer.exe 7713
```

完整、可编译源码：

- [PUB/SUB](examples/zmq_style_pub_sub.c)
- [REQ/REP](examples/zmq_style_req_rep.c)
- [ROUTER/DEALER](examples/zmq_style_router_dealer.c)

## 5. Application facade 生命周期

标准顺序：

1. 用 `TURBO_FLOW_FMQ_CONFIG_INIT` 初始化 endpoint。
2. 设置 pattern、mode、transport、host/port 或 pipe path。
3. receive 或 bidirectional pattern 配置 `on_message`。
4. `turbo_flow_fmq_app_create()`。
5. `turbo_flow_fmq_app_start()`。
6. 使用 `send`、`send_batch` 或 `send_message`。
7. `turbo_flow_fmq_app_stop()`。
8. `turbo_flow_fmq_app_destroy()`。

Application facade 直接拥有一个 graph-neutral FMQ endpoint Core，不创建、解析或启动 TurboFlow
Graph。需要拓扑组合时，Graph adapter 才把同一个 Core 接入 TurboFlow；两条入口都不会创建第二套
socket 或协议状态。
默认同步模式没有额外队列或 worker；只有显式配置异步发送后，facade 才创建一条有界队列和一个保序发送
worker。

Callback 中的 `turbo_flow_msg_t`、payload、topic 和 identity view 只借用到 callback 返回。若要跨线程、
Queue 或 callback 保存消息，必须 clone/retain 对应 owned message。

## 6. 发送与批量发送

三种发送模式针对不同的 producer 形态，不能只按接口名字互换：

| 模式 | API | 适用场景 | 主要权衡 |
| --- | --- | --- | --- |
| Serialized synchronous | `turbo_flow_fmq_app_send()` | 低频消息、低排队延迟、需要立即逐条状态，或 REQ 的严格 request/session 顺序 | 每条消息独立经过 Core admission 与发送边界；高频小包的固定开销最大 |
| Explicit Batch | `turbo_flow_fmq_app_send_batch()` | 调用方已经持有一组彼此独立的 PUB/PUSH/DEALER 消息，且可以等待整批完成 | 小包吞吐优先、没有额外 worker；返回可能是部分提交，必须检查 `submitted` |
| Async Micro-Batch | `configure_async_send()` + `turbo_flow_fmq_app_send_async()` | 消息逐条到达、producer 不应等待 socket delivery，或者需要有界削峰 | admission 与 delivery 分离；增加队列、worker 和可配置 linger，必须处理 `TURBO_ENOSPC` 与 completion |

这里的 Serialized 表示逐条同步提交，不是另一套 wire serialization。三种模式都使用相同的 FMQ/3
frame、pattern 状态机和已配置 transport，只改变 producer admission、批次边界及完成通知方式。

对于高频 64-byte 级消息，优先让 producer 形成 Explicit Batch；无法形成显式批次时再选择 Async
Micro-Batch。对于大 payload，batch item 上限不是推荐批量大小，应同时限制单批总字节数并测量
P95/P99。不同 pattern、transport 和 endpoint layout 的 coalescing 能力不同，最终参数必须以目标部署
环境的 benchmark 为准。

`turbo_flow_fmq_app_send()` 会复制 payload。成功表示消息到达本地发送交付边界，不表示远端业务已经处理或
持久化。

`turbo_flow_fmq_app_send_batch()` 支持 PUB、PUSH、DEALER：

- 单批最多 1024 项。
- payload 总量最多 64 MiB。
- 同步 batch 共享一次 TurboFlow publication admission 和 source lookup；payload 仍按项准备并在对应
  graph attempt 后立即清理，不会新增队列，也不会同时保留整批 payload。
- TCP connect endpoint 会合并多个 encoded frame 为一次 stream write。
- 返回错误时，失败项之前的消息可能已经提交；必须同时检查返回码与 `submitted`。
- REQ/REP 不支持 batch，以保持严格 correlation/session 顺序。

`turbo_flow_fmq_app_send_async()` 为 PUB、PUSH、DEALER 提供 copied admission 和异步 completion：

- 在首次 `start()` 前用 `TURBO_FLOW_FMQ_APP_ASYNC_SEND_CONFIG_INIT` 初始化配置，再调用
  `turbo_flow_fmq_app_configure_async_send()`；同一 app 只能配置一次。
- `queue_capacity` 和 `queue_capacity_bytes` 同时限制待发送数据；任一配额满时立即返回
  `TURBO_ENOSPC`，未接收所有权，也不会调用 completion。
- 队列配额约束等待 backlog；worker 当前持有的一个 active batch 另受 1024 项和 64 MiB 上限约束，
  因此总内存边界是 queue quota 加一个 active batch。
- `TURBO_OK` 只表示 facade 已拥有 payload 副本。单 worker 按 admission 顺序组成最多
  `batch_size` 项、64 MiB payload 的 micro-batch，并复用同步 batch 的交付边界。
- 非空 completion 对每个 accepted message 恰好调用一次。callback 运行在发送 worker，不得对同一 app
  调用 `stop()` 或 `destroy()`。
- `stop()` 先关闭 admission，再排空 accepted messages、等待 completion，最后停止底层 Flow。

`linger_ns` 是首条消息等待同批后续消息的上限；低延迟配置可设为 0，持续高吞吐场景应结合实际 producer
burst 调整 `batch_size` 与 linger。异步队列只负责内存中的发送削峰，不提供进程崩溃恢复。

TCP payload 达到 1 KiB 且 view 可证明位于 retained `mem_buffer_t` 内时，发送 request 只编码
header/identity/topic，并通过 scatter/gather 借用 payload；request 完成、取消或 shutdown 后统一释放
framing 与 buffer reference。小 payload 保留连续编码快路径，避免 iovec 准备成本。TLS、UDP、KCP、Pipe、
WS、WSS 仍显式使用各自的连续 transport encoder，不通过 `TURBO_ENOTSUP` 做隐式回退。

不要把 FMQ frame admission 当成 delivery ACK。需要 durable delivery 时，应显式组合 Queue/Storage provider。

## 7. REQ/REP 与 ROUTER/DEALER

REP callback 是同步回复边界。使用
`turbo_flow_fmq_app_message_set_payload_copy()` 替换 request payload，callback 返回 `TURBO_OK` 后回复。

ROUTER 支持延迟回复：

1. callback 中调用 `turbo_flow_fmq_message_detach_router_route()`。
2. clone message 后跨 worker 或 Queue 处理。
3. 设置回复 payload。
4. 调用 `turbo_flow_fmq_app_send_message()` 发送。

detached route 带 owner instance、session ID 和 generation fence。peer 断线、同 identity 重连或 ROUTER
重启后，旧 route 返回 `TURBO_ENOTCONN`。route 是易失的连接能力，不允许持久化重放。

## 8. Topic、identity 与 metadata

接收侧使用：

- `turbo_flow_fmq_message_topic()`
- `turbo_flow_fmq_message_identity()`
- `turbo_flow_fmq_message_correlation_id()`
- `turbo_flow_fmq_message_subscription()`

返回 view 依赖 message 生命周期。topic 最大 1024 bytes，identity 最大 255 bytes。完整 ingress message 的
payload、topic、identity、correlation 和 FMQ metadata 共享 retained buffer，可进入 Disruptor 和异步
executor；raw socket framing view 不跨 CoroNet owner lane。

## 9. YAML 与 Graph

完整 endpoint 配置见：

- [fmq.yml](examples/fmq.yml)
- [zmq_style.yml](examples/zmq_style.yml)

YAML 保存 transport、endpoint、pattern、topic、identity、timeout、heartbeat、reconnect、HWM 和 admission
policy。Graph 只引用 adapter/operation，例如：

```flow
source events adapter fmq.events.sub operation fmq.sub.receive
stage validate
stage publish adapter fmq.events.pub operation fmq.pub.send

stage main {
  events -> validate -> publish
}
```

注册 resolved endpoint 时，先创建 immutable `turbo_flow_resolved_config_t`，再调用
`turbo_flow_fmq_register_resolved_adapter()` 或 `turbo_flow_fmq_app_create_resolved()`。FlowMQ 不自行读取 YAML
文件，也不允许 host callback、schema pointer 或 execution owner 从 YAML 注入。

## 10. Transport

支持 TCP、TLS、UDP、KCP、Pipe、WS、WSS。pattern 与 transport 正交，但 transport 专属字段会严格校验：

- TLS/WSS：CoroNet TLS；client 必须验证服务端。
- WS/WSS：使用 `path`。
- Pipe：使用 pipe endpoint path，不使用 host/port。
- KCP：可配置 FEC backend、data/parity shard 和 payload 上限。
- UDP：bind 端显式使用 CoroNet per-peer session admission，以远端 IP/port 映射 HELLO 后续数据报；
  每个 peer 最多保留一个未读数据报，拥塞时保持 UDP 的丢包语义。multicast、TTL、loop、broadcast
  通过显式 option flag 设置；需要有序、重传、FEC 与 AEAD 时选择 KCP。
- TCP-backed transport：可配置 keepalive、linger、OS receive/send buffer 和 socket send HWM。

不适用于当前 transport 的 option 会失败，不会静默忽略。
buffer 分层、默认值、内存预算和调优边界见
[CoroNet Buffer Tuning Contract](../io/common/CORONET_BUFFER_TUNING.md)。

## 11. wire v3 安全

FlowMQ 只支持 wire v3，不存在 v2 fallback。secure endpoint 在 HELLO 中完成认证，并在 DATA 前执行 ACL：

- BIND 注入 auth provider、SecurityRealm、realm channel 和 auth method。
- CONNECT 注入 key provider、secret reference 和 auth method；endpoint identity 是认证 identity。
- credential 只借用到 HELLO encode/write 边界，随后释放并擦除 encoded buffer。
- 授权默认拒绝；认证失败或 ACL 拒绝不会降级为 trusted endpoint。

TLS/WSS secure endpoint 要求 TLS 1.3、服务端验证和 RFC 9266 exporter channel binding。TCP、UDP、KCP、
Pipe、WS 不提供 credential confidentiality，只能用于可信网络或额外安全隧道。

数据库不属于 FlowMQ endpoint。ACL/auth owner 由配置和 product composition root 选择；认证数据库只能由
HTTPS 认证服务访问。

## 12. 背压与慢 peer

至少配置一层有界资源：

- socket `send_hwm_bytes`
- FMQ `frame_hwm_messages` / `frame_hwm_bytes`
- PUB/XPUB per-peer HWM
- Queue/Storage provider capacity

admission policy 支持 fail、block 和 drop-oldest，但不同 pattern/endpoint 对策略有额外约束。PUB/XPUB 的
慢 peer 可 fail、drop-oldest 或 disconnect。生产代码应监听 `turbo_flow_fmq_event_fn`，记录连接、重连、
heartbeat timeout、HWM、drop、认证失败和授权拒绝；不要在高频 frame 事件中同步写日志。

## 13. 验证

```powershell
cmake --build --preset win-release-user --target test_fmq test_fmq_pubsub test_fmq_broker
ctest --preset win-release-user -L fmq-release --output-on-failure
```

性能基准与交付边界见 [benchmarks/README.md](benchmarks/README.md)，安全发布条件见
[RELEASE_GATE.md](RELEASE_GATE.md)。
