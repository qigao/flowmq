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
FMQ/5 codec + Rocida CNet
```

`FlowMQ::Protocol` 不依赖网络；`FlowMQ::Core` 保存 pattern/session 规则；
`FlowMQ::Transport` 私有依赖 `Rocida::CNet`。当前 transport 只有 TCP/TLS，未实现
transport 会在配置边界 fail fast。

## 执行模型

CNet 是调用者驱动的单-owner协程池，不是线程池。FlowMQ 不创建 progress thread，
也不把 socket 包装成 Actor/Reactive publisher：

- `start/bind/connect` 负责同步验证、资源创建与异步 I/O admission；
- `send/recv/poll` 在调用者线程推进 socket 和 CNet 状态；
- `DONTWAIT` 只尝试一次；普通 `send/recv` 遇到 would-block 时在当前调用栈分片推进所属
  socket，直到成功或 CNet progress 返回错误；
- 多 socket `poll` 对整个列表重复执行非阻塞 progress，以 1ms 有界间隔等待，直到请求的
  level-triggered 事件就绪或总 timeout 到期；
- CNet callback 只在调用者主动 progress 的 `send/recv/poll` 调用栈内同步执行；
- socket、decoder、route、pattern FSM 与队列由同一调用线程串行拥有；
- 同一个普通 socket 不允许跨线程并发使用；跨线程通信使用独立 socket。

每个 live peer 独占 heartbeat deadline、pending-PONG 和双向累计 credit 状态。
连接按 `HELLO -> SETTINGS -> READY` 推进；SETTINGS 完成前不允许 DATA。PING/PONG 与
FLOW_UPDATE 优先使用同一个串行 CNet write lane，不进入应用 outbound FIFO；收到任意合法
FMQ frame 会取消未应答 PING 的 timeout。所有 deadline 只在 owner 调用
`send/recv/poll` 时检查，不创建 timer thread，也不把 socket 变成 MPSC。

CFlow/CMeta 可服务于控制面配置、类型描述和 executor 组合，不参与逐消息数据热路径。

## Send 数据路径

```text
application message
  -> pattern FSM / peer selection
  -> local HWM + peer remote max_data admission
  -> FMQ/5 encode into socket-owned reusable scratch
  -> multipart parts retained in bounded socket-owned staging until final
  -> complete message transferred to peer-owned fixed descriptor ring
  -> single-part fast path may use direct cnet_send() when the peer is writable
  -> coalesce queued frames into one bounded CNet write
  -> later caller-driven cnet_client_poll()
```

成功 admission 只表示本地 socket 已接管消息。连接建立、CNet write、远端接收与业务处理
是不同完成边界。`DONTWAIT` 在消息数或 payload byte HWM 满时立即返回 `TURBO_ENOBUFS`；
普通 send 在调用线程内推进所属 socket 后重试。单个 part 或完整 multipart 永远不可能装入
byte HWM 时返回 `TURBO_EMSGSIZE`。远端累计 credit 耗尽时返回 `TURBO_ENOBUFS`；远端应用
消费 DATA 并由其 owner 发送 FLOW_UPDATE 后恢复。multipart 的 credit 在 final part 时按完整
payload 一次提交，失败不会暴露或接纳部分 message。
PUB/XPUB 的 mute peer 按 ZeroMQ 语义丢弃，PUSH/DEALER/REQ 等模式不静默丢弃。

## Receive 数据路径

```text
CNet borrowed receive view
  -> peer-owned bounded stream decoder
  -> FMQ/5 frame validation + SETTINGS/FLOW_UPDATE credit
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

- REQ：`SEND_READY -> WAIT_REPLY -> SEND_READY`，非法 send/recv 返回 FSM 错误。
- REP：`RECV_READY -> SEND_REPLY -> RECV_READY`，reply 绑定最后一个 requester。
- PUSH/DEALER/REQ：eligible peer round-robin；peer busy 时不形成跨 peer HOL。
- PULL/SUB/DEALER/ROUTER：当前按网络完成顺序进入全局队列；严格 per-peer fair queue 尚未完成。
- PUB/XPUB：按 subscription prefix fan-out；每个 peer 独立 HWM/drop；SUB/XSUB 的动态
  subscribe/unsubscribe 通过每个 peer 的同步快照增量传播。
- ROUTER：receive 暴露 routing-id 首 part；send 消费 routing-id 首 part。
- multipart：sender 的 `SNDMORE` parts 先复制到 socket-owned 有界 staging，final part 对完整
  payload size、part slots 和 message HWM 做一次 admission，再原子转移到选定 peer outbound；
  receiver 也只观察全部 parts 或完全不观察。

精确 compatibility matrix 和迁移边界见
[CNet TCP/TLS 与 ZeroMQ socket 模型](CNET_TCP_TLS_ARCHITECTURE.md)。

## Bind/connect primitive

旧的 connect/router callback endpoint 已删除，不再作为迁移层或公开事实源。新的 socket
runtime 直接拥有 CNet client/listener、peer registry、decoder、pattern FSM 与有界消息队列；
bind/connect 只决定连接方向，不决定消息模式。

## Shutdown

`flowmq_close()` 先关闭 listener，再以有界 timeout 停止并销毁 CNet client，随后释放
本地 queue、decoder、route/session、TLS secret 与 pool。当前公开 API 没有 linger 或
drain 策略；未完成的本地消息随 socket close 取消。

任何阶段都不得在线程外隐藏 progress，也不得在 callback 仍可能访问 owner state 时释放资源。

## 验证

当前验证覆盖 TCP、verified TLS、FMQ/5 SETTINGS/FLOW_UPDATE、累计 credit 耗尽与恢复、
deadline 更新、ROUTER identity、multipart 接收原子可见性、REQ/REP FSM、
PUB/SUB filter/fan-out、动态 XPUB/XSUB subscription event、发送/接收 message/byte HWM、
阻塞/DONTWAIT 分流、多 socket timeout poll、RCVMORE、peer failure isolation、session
generation fencing 和 multipart 整体发送 admission。下一阶段继续覆盖严格 receive
fair-queue、可配置 shutdown 边界和更多 pattern benchmark。
