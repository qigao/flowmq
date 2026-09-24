# FlowMQ architecture

## 模块与依赖

```text
Application
    |
ZeroMQ-style context / socket API
    |
pattern FSM + routing + bounded message queues
    |
caller-driven TCP/TLS primitives
    |
FMQ/6 codec + Salts CNet
```

`FlowMQ::Protocol` 不依赖网络；`FlowMQ::Core` 保存 pattern/session 规则；
`FlowMQ::Transport` 私有依赖 `Salts::CNet`。当前 transport 只有 TCP/TLS，未实现
transport 会在配置边界 fail fast。

物理目录与上述 target 保持一致：

```text
flowmq/include/                         installed flat C API
flowmq/src/protocol/                    FMQ/FMS/FES codec 与 stream decoder
flowmq/src/core/pattern/                ZeroMQ pattern、FSM 与 subscription state
flowmq/src/core/session/                credit/HWM 与 reconnect policy
flowmq/src/runtime/                     context/socket/peer owner 与 caller progress
flowmq/src/transport/cnet/              CNet TCP/TLS 薄适配
flowmq/src/security/                    TLS principal/identity policy
flowmq/extensions/media_provider/       FMP/1 schema、TBE wire view 与 validator
flowmq/tests/、flowmq/benchmarks/       按相同责任分组的验证入口
```

原顶层 `patterns/` 混合了 ZeroMQ socket pattern、transport 和未接入 runtime 的 ESB helper，
现已取消。Core 只保留 socket/runtime 实际使用的 pattern 与 session 状态；Saga、通用 priority
queue、scatter/gather、stream partition 和 circuit breaker 不再编入主库。

## 执行模型

CNet 是调用者驱动的单-owner协程池，不是线程池。FlowMQ 不创建 progress thread，
也不把 socket 包装成 Actor/Reactive publisher：

- `start/bind/connect` 负责同步验证、资源创建与异步 I/O admission；
- `send/recv/poll` 在调用者线程推进 socket 和 CNet 状态；
- outbound endpoint 的重连 deadline 也只在这些调用中检查；到期后最多执行一次固定容量
  endpoint 表扫描并向 CNet admission 新 session，不创建 timer/progress thread；
- `DONTWAIT` 只尝试一次；普通 `send/recv` 遇到 would-block 时在当前调用栈分片推进所属
  socket，直到成功或 CNet progress 返回错误；
- 多 socket `poll` 对整个列表重复执行非阻塞 progress，以 1ms 有界间隔等待，直到请求的
  level-triggered 事件就绪或总 timeout 到期；
- CNet callback 只在调用者主动 progress 的 `send/recv/poll` 调用栈内同步执行；
- socket、decoder、route、pattern FSM 与队列由同一调用线程串行拥有；
- 同一个普通 socket 不允许跨线程并发使用；跨线程通信使用独立 socket。

每个 live peer 独占 heartbeat deadline、pending-PONG 和双向累计 credit 状态。
peer 的可变协议状态拆成三个独立维度，而不是一个乘积型大 FSM：

```text
lifecycle:
  FREE -> ALLOCATED -> CONNECTED
                       |      |
                       |      +-> CLOSING -> RETIRED -> FREE
                       +-> CLOSE_RETRY -> CLOSING
  ALLOCATED / CONNECTED / CLOSE_RETRY 也可由终止回调直接进入 RETIRED

handshake progress:
  HELLO_TX -> SETTINGS_TX
  HELLO_RX -> SETTINGS_RX

write lane:
  IDLE -> HELLO -> IDLE
       -> SETTINGS -> IDLE
       -> CONTROL -> IDLE
       -> DATA -> IDLE
```

HELLO/SETTINGS 的 TX 与 RX 是两条独立单调链，可以交错推进；只有 lifecycle 为
`CONNECTED` 且四个 handshake fact 全部成立时 peer 才是 READY。SETTINGS TX 至少依赖
HELLO_TX；CONTROL/DATA write lane 只在 READY 后 admission。一个 peer 同时只允许一个 CNet
write lane，send completion 根据 lane 唯一决定是否提交 HELLO_TX、SETTINGS_TX 或 DATA
in-flight 统计，不再维护 `write_busy + writing_*` 多套事实。

`CLOSE_RETRY` 表示 CNet close command 因 bounded command capacity 尚未 admission；
`CLOSING` 表示 close 已被 CNet 接受、等待 terminal callback。终止后先进入 `RETIRED`：
decoder、credit、subscription snapshot、multipart staging 和 outbound storage 已释放，但
socket inbound queue 仍可能保存引用该 generation 的已完成 message part，所以 RETIRED
不等于 FREE；只有这些 queued parts 消费完后 slot 才回到 FREE。

multipart receive/commit-pending、heartbeat/PONG、flow-credit counter、queue occupancy 和
generation fencing 仍是正交事实，不并入上述 lifecycle/handshake/write-lane 状态。
PING/PONG、FLOW_UPDATE 与 subscription sync 共用 CONTROL write lane，不进入应用 outbound
FIFO；收到任意合法 FMQ frame 会取消未应答 PING 的 timeout。所有 deadline 只在 owner 调用
`send/recv/poll` 时检查，不创建 timer thread，也不把 socket 变成 MPSC。

ROUTER 的 per-peer backpressure 事实保持 peer-owned。成功 admission 后，应用 payload 的
message/byte outstanding 计数同时覆盖 direct in-flight 与 retained outbound ring；只有 CNet
send completion 才从 current outstanding 扣除。远端 cumulative credit 是另一正交事实，因此
本地 outstanding 可以为 0 而 `send_credit_bytes` 仍为 0。公开
`flowmq_router_peer_status()` 只投影这些稳定诊断事实与 session-local monotonic counters，
不暴露 peer slot、generation、ring cursor 或 raw cumulative credit counter，也不执行任何
progress。peer retire 后不再可由该 API 查到；同 routing identity reconnect 得到全新统计。

Outbound endpoint 是 URI 与重连退避的主事实源；peer 是一次 CNet connection session。
终止回调先解除 endpoint 的 active session，再按既有 generation fencing 退休 peer，并为
endpoint 安排下一次 caller-driven attempt。旧 peer 的 decoder、credit、subscription snapshot、
multipart staging 与 outbound queue 都不会转移到新 peer；socket-owned XSUB desired
subscriptions 会从事实源重放。`FLOWMQ_RECONNECT_IVL` 默认 100ms，`-1` 禁用；
`FLOWMQ_RECONNECT_IVL_MAX=0` 使用固定间隔，正值启用有上限的指数退避。

CMeta 是 FlowMQ 静态语义的实现来源；CFlow 只用于 build/test control-plane
qualification，不参与逐消息数据热路径，也不是 installed FlowMQ 的执行依赖。

### CFlow ordering qualification

测试构建把一个抽象 send qualification token 投影成 CFlow unary MAP Graph：

```text
VALIDATE
  -> ROUTE_SELECTED
  -> LOCAL_CAPACITY_OK
  -> REMOTE_CREDIT_OK
  -> ENCODED
  -> ADMITTED
  -> CREDIT_COMMITTED
  -> IO_SUBMITTED
```

每个 stage 由 CMeta `FunctionDesc + FunctionAbi` 与 exact-ABI adapter 描述。FlowMQ 不把
真实 message、peer 或 socket state 搬进 Graph；token 只是验证步骤前置条件与 ordering 的
测试语义。错误重排（例如 `ADMITTED` 早于 `ENCODED`、`CREDIT_COMMITTED` 早于
`ADMITTED`、`IO_SUBMITTED` 早于 `CREDIT_COMMITTED`）必须产生 invalid
qualification state。

CMeta 当前 effect 粒度是 `STATEFUL / ASYNC / IO / MAY_FAIL / UNKNOWN`，没有独立的
READS_STATE/WRITES_STATE，因此 FlowMQ 对读取 mutable runtime state 的 qualification stage
保守声明为 STATEFUL；编码可声明 MAY_FAIL，CNet submission 声明 IO|MAY_FAIL。CFlow
normalization/optimizer 用这些 effect 作为 barrier，qualification test 比较 surface 与 optimized
结果并检查 effect-blocked fusion。

这个 CFlow target 只由测试链接 `Salts::CFlow`。生产 `FlowMQ::FlowMQ` 不链接 CFlow，
`flowmq_send/recv/poll` 也不执行 Graph、Plan 或 reflection lookup。

## Send 数据路径

```text
application message
  -> pattern FSM / peer selection
  -> local HWM + peer remote max_data admission
  -> FMQ/6 encode into socket-owned reusable scratch
  -> multipart parts retained in bounded socket-owned staging until final
  -> complete message transferred to peer-owned fixed descriptor ring
  -> single-part fast path may use direct cnet_send() when the peer is writable
  -> coalesce queued frames into one bounded CNet write
  -> later caller-driven cnet_client_poll()
```

成功 admission 只表示本地 socket 已接管消息。连接建立、CNet write、远端接收与业务处理
是不同完成边界。`DONTWAIT` 在消息数或 payload byte HWM 满时立即返回 `SALTS_ENOBUFS`；
普通 send 在调用线程内推进所属 socket 后重试。单个 part 或完整 multipart 永远不可能装入
byte HWM 时返回 `SALTS_EMSGSIZE`。远端累计 credit 耗尽时返回 `SALTS_ENOBUFS`；远端应用
消费 DATA 并由其 owner 发送 FLOW_UPDATE 后恢复。multipart 的 credit 在 final part 时按完整
payload 一次提交，失败不会暴露或接纳部分 message。
PUB/XPUB 的 mute peer 按 ZeroMQ 语义丢弃，PUSH/DEALER/REQ 等模式不静默丢弃。

## Receive 数据路径

```text
CNet borrowed receive view
  -> peer-owned bounded stream decoder
  -> FMQ/6 frame validation + SETTINGS/FLOW_UPDATE credit
  -> per-peer multipart staging
  -> socket-owned complete message
  -> recv/msg_recv
```

CNet view 只在 callback 内有效。跨出 callback 或主动 progress 调用边界的数据先复制到 socket-owned
buffer；完整 multipart 提交前只存在于对应 peer staging。完整消息提交同时检查可配置的消息数
与 payload byte HWM；容量不足时停止该 peer 的 receive demand，应用消费并再次 `poll` 后恢复。
只有真实 DATA payload 消耗 wire credit；ROUTER routing-id 和 XPUB subscription event 是本地
合成 part，不计 credit。应用取走 part 后增加累计 consumed_data，达到配置 quantum 或 deadline
后由下一次 owner progress 发布 FLOW_UPDATE。

## Pattern 状态

- REQ：`SEND_READY -> WAIT_REPLY -> SEND_READY`，非法 send/recv 与 multipart 方向交错立即
  返回 `SALTS_EPROTO`，不进入阻塞重试。
- REP：`RECV_READY -> SEND_REPLY -> RECV_READY`，reply 绑定最后一个 requester。
- PUSH/DEALER/REQ：eligible peer round-robin；peer busy 时不形成跨 peer HOL。
- PULL/SUB/DEALER/ROUTER：当前按网络完成顺序进入全局队列；严格 per-peer fair queue 尚未完成。
- PUB/XPUB：按 subscription prefix fan-out；每个 peer 独立 HWM/drop；SUB/XSUB 的动态
  subscribe/unsubscribe 通过每个 peer 的同步快照增量传播，新 session 从 socket desired
  subscription 集重放。
- ROUTER：receive 暴露 routing-id 首 part；send 消费 routing-id 首 part。
- multipart：sender 的 `SNDMORE` parts 先复制到 socket-owned 有界 staging，final part 对完整
  payload size、part slots 和 message HWM 做一次 admission，再原子转移到选定 peer outbound；
  receiver 也只观察全部 parts 或完全不观察。

精确 compatibility matrix 和迁移边界见
[CNet TCP/TLS 与 ZeroMQ socket 模型](CNET_TCP_TLS_ARCHITECTURE.md)。

## Bind/connect primitive

旧的 connect/router callback endpoint 已删除，不再作为迁移层或公开事实源。新的 socket
runtime 直接拥有 CNet client/listener、peer registry、decoder、pattern FSM 与有界消息队列；
bind/connect 只决定连接方向，不决定消息模式。每次成功 `flowmq_connect()` 还建立一个固定
容量的 outbound endpoint 记录；初次 admission 失败会直接返回错误且不保留记录，后续异步
FAILED/CLOSED 才进入自动重连。

## Shutdown

`flowmq_close()` 先关闭 listener，再以有界 timeout 停止并销毁 CNet client，随后释放
本地 queue、decoder、route/session、TLS secret 与 pool。当前公开 API 没有 linger 或
drain 策略；未完成的本地消息随 socket close 取消。

任何阶段都不得在线程外隐藏 progress，也不得在 callback 仍可能访问 owner state 时释放资源。

## 验证

当前验证覆盖 TCP、verified TLS、FMQ/6 SETTINGS/FLOW_UPDATE、累计 credit 耗尽与恢复、
deadline 更新、ROUTER identity、multipart 接收原子可见性、REQ/REP FSM、
PUB/SUB filter/fan-out、动态 XPUB/XSUB subscription event 与 reconnect replay、TCP/TLS
listener restart 后的 caller-driven reconnect、发送/接收 message/byte HWM、
阻塞/DONTWAIT 分流、多 socket timeout poll、RCVMORE、peer failure isolation、session
generation fencing 和 multipart 整体发送 admission。下一阶段继续覆盖严格 receive
fair-queue、可配置 shutdown 边界和更多 pattern benchmark。
