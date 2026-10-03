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
`FlowMQ::Transport` 私有依赖 `Salts::CNet` 与 `Salts::NativeIO`。NativeIO 只作为
实现细节支撑 ordinary per-socket wait 与 explicit owner-lane shared wait，不进入安装包的
public CMake dependency surface。当前 transport 只有 TCP/TLS，未实现 transport 会在配置边界
fail fast。

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
也不把 socket 包装成 Actor/Reactive publisher。

FlowMQ 1.2 有两个 execution domain：

```text
ordinary socket domain
  socket owns its CNet/NativeIO progress
  flowmq_poll() drives each socket independently

explicit owner-lane domain
  flowmq_owner_t owns one shared NativeIO backend
  N owner-created sockets each own one external-progress CNet client
  flowmq_owner_poll() advances all live clients and performs one shared observe
```

socket 创建时就决定 execution domain，之后不能 detach、迁移或在两个 poll surface 之间切换。
owner lane 是一个**同步 owner object**，不是 scheduler/worker pool。一个 connection 从 admission
到 terminal callback 固定属于同一个 lane-local CNet client。

普通 socket 继续遵守以下规则：

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

owner-lane socket 则由 `flowmq_owner_poll()` 推进：

```text
for each live socket:
    cnet_client_advance_external() exactly once
    collect CNet + FlowMQ-local next deadline

native_io_backend_observe(min_deadline) exactly once

for each completion:
    route to exactly one owning CNet client

for each live socket:
    FlowMQ reconnect/control/FLOW_UPDATE/subscription/flush progress exactly once

compute POLLIN/POLLOUT/POLLERR with the same readiness projection as ordinary poll
```

FlowMQ-local deadline projection至少包含 reconnect next-attempt、heartbeat ping/dead-peer deadline、
receiver FLOW_UPDATE deadline/quantum 与 caller timeout；因此正式 owner API 不依赖 #67
prototype 的固定 10ms wait。routed completion batch 后不立即执行第二次 CNet advance，
下一 cycle 才重新 advance，保持 owner-local progress 的单 pass 语义。

`flowmq_owner_t` 的 socket registry 与 NativeIO capacities 在创建时按 `socket_capacity`
硬上界预留；不会依据 CPU 数自动扩容，也不会 live resize。owner-created socket 不能放入
ordinary `flowmq_poll()`，不能直接 `flowmq_close()`，也不能迁移到另一个 owner。
该 API 仍只产品化 TCP client-side shared-wait progress。

listener/same-endpoint multicore 使用独立的 ordinary-socket composition，而不是把 listener
塞进 `flowmq_owner_t`：

```text
owner/core A                         owner/core B
ordinary socket A                    ordinary socket B
FLOWMQ_REUSE_PORT=1                  FLOWMQ_REUSE_PORT=1
CNet listener A ---- same port ---- CNet listener B
        \                              /
         +---- kernel accept hash -----+
```

`FLOWMQ_REUSE_PORT` 是 startup-only int 0/1。bind 统一调用 released CNet
`cnet_listener_init_ex()`；不支持 `SO_REUSEPORT` 的平台返回 `SALTS_ENOTSUP`，没有
`SO_REUSEADDR` 模拟或 silent fallback。每个 accepted connection 永久归属 accepting owner，
不会跨 owner 转移 native handle，也不会通过 central dispatcher 把每条消息重新汇聚到单线程。

#73 的 same-endpoint qualification 使用四条固定 client connection、一个 owner control 与两个
distinct-core owner candidate。饱和的 64 KiB pipelined DEALER→REP workload 在 exact
128/128 measured request distribution 下得到约 1.451x throughput paired median，p99 约
0.615x control；这证明 same-endpoint composition 可以获得真实多核收益，但不构成所有 payload
或 workload 都线性扩展的承诺。

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
FIFO；收到任意合法 FMQ frame 会取消未应答 PING 的 timeout。所有 deadline 只在 ordinary `send/recv/poll` 或显式 `flowmq_owner_poll()` 中检查，
不创建 timer thread，也不把 socket 变成 MPSC。

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

CFlow/CMeta 可服务于控制面配置、类型描述和 executor 组合，不参与逐消息数据热路径。

### CFlow ordering qualification

FlowMQ 在 test/control plane 上用 CMeta exact-ABI Function 描述与 CFlow Graph 验证一条抽象
send ordering：

```text
VALIDATE
  -> ROUTE
  -> LOCAL_CAPACITY
  -> REMOTE_CREDIT
  -> ENCODE
  -> ADMIT
  -> CREDIT_COMMIT
  -> IO_SUBMIT
```

这些 stage 是规范模型，不是 production message executor。validation/encoding 可以 fallible，
peer/credit/admission/commit 对 mutable state 保守标记为 STATEFUL，CNet submission 是 IO
barrier。qualification test 必须证明：

- canonical ordering 能通过 exact CMeta ABI adapter 投影并完成；
- ADMIT-before-ENCODE、CREDIT_COMMIT-before-ADMIT、IO-before-credit-commit 等错误顺序被模型拒绝；
- normalization/optimization 不跨 STATEFUL/IO effect barrier 重排；
- contract-equivalent mock adapter 可以替换实现而不改变 Graph topology；
- incompatible reflected callable 在 projection admission 阶段被拒绝。

该测试是 standalone target，只链接 released `Salts::CFlow` / `Salts::TinyTest`。
`FlowMQ::FlowMQ` 不链接 CFlow，`flowmq_send/recv/poll` 不执行 Graph/Plan traversal，也不做
per-message reflection lookup。现有 runtime source gate 与 installed package consumer 继续
分别约束这两个 production boundary。

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
     or owner-lane cnet_client_advance_external/shared NativeIO observe
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

ordinary socket 的 `flowmq_close()` 先关闭 listener，再以有界 timeout 停止并销毁 CNet
client，随后释放本地 queue、decoder、route/session、TLS secret 与 pool。当前公开 API
没有 linger 或 drain 策略；未完成的本地消息随 socket close 取消。

owner-lane socket 必须通过 `flowmq_owner_close_socket()` 关闭。关闭过程先禁止新的 reconnect
schedule，清空 pending retry，再对该 socket 的 live peer 发起 close；owner 继续对**整个 shared
backend** 执行 advance/observe/route，因此其它 lane-local socket 不需要暂停。只有目标 client
达到 `cnet_client_stop_external() == SALTS_OK` 后才释放 socket storage。最后一个 socket
关闭后 `flowmq_owner_term()` 才允许 close/destroy shared NativeIO backend，并释放 context
owner lease。

任何阶段都不得在线程外隐藏 progress，也不得在 callback 仍可能访问 owner state 时释放资源。

## 验证

当前验证覆盖 TCP、verified TLS、FMQ/6 SETTINGS/FLOW_UPDATE、累计 credit 耗尽与恢复、
deadline 更新、ROUTER identity、multipart 接收原子可见性、REQ/REP FSM、
PUB/SUB filter/fan-out、动态 XPUB/XSUB subscription event 与 reconnect replay、TCP/TLS
listener restart 后的 caller-driven reconnect、发送/接收 message/byte HWM、
阻塞/DONTWAIT 分流、多 socket timeout poll、RCVMORE、peer failure isolation、session
generation fencing 和 multipart 整体发送 admission。下一阶段继续覆盖严格 receive
fair-queue、可配置 shutdown 边界和更多 pattern benchmark。
