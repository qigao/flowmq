# FlowMQ CNet TCP/TLS 与 ZeroMQ socket 模型

## 决策

FlowMQ 的公开目标是 ZeroMQ 的 context/socket 调用方式和消息模式语义，不采用
Actor、Reactive 或 callback-first API。FMQ/5 仍是独立 wire protocol，因此不承诺与
ZMTP/libzmq 二进制互通。

CNet 是 caller-driven、单 owner 的协程网络库。FlowMQ 不为 CNet 创建 progress
thread，也不把同一个 socket 变成 MPSC consumer。`bind`、`connect`、`send`、`recv`
与 `poll` 都是调用者线程上的普通函数；CNet callback 只作为内部完成通知，并在
调用者主动 progress 的 `send/recv/poll` 调用栈内同步执行。

## 线程与状态归属

- 一个普通 FlowMQ socket 与 ZeroMQ 普通 socket 一样，不允许多线程并发使用。
- socket 可在首次启动前迁移到另一线程；启动后由实际调用它的 owner 串行推进。
- connection、listener、decoder、pattern FSM、HWM queue 与 routing cursor 都属于
  socket owner，不使用 MPSC、Disruptor、mutex 或工作线程。
- 跨线程通信使用两个独立 socket 和后续的 `inproc://` transport，不向同一 TCP/TLS
  socket 跨线程投递命令。
- CFlow/CMeta 可用于控制面配置、类型描述和 executor 组合，不进入逐消息 TCP/TLS
  热路径。

## ZeroMQ 模式契约

TCP/TLS 第一阶段承载以下经典 socket 模式；bind/connect 方向不决定 pattern：

| Socket | Compatible peers | Send | Receive | Routing |
| --- | --- | --- | --- | --- |
| PAIR | PAIR | yes | yes | exactly one peer |
| PUB | SUB, XSUB | yes | no | subscribed fan-out, mute peer drops |
| SUB | PUB, XPUB | no | yes | fair queue, prefix subscriptions |
| PUSH | PULL | yes | no | round-robin, mute blocks/EAGAIN |
| PULL | PUSH | no | yes | fair queue |
| REQ | REP, ROUTER | alternating | alternating | request round-robin |
| REP | REQ, DEALER | alternating | alternating | reply to last requester |
| DEALER | ROUTER, REP, DEALER | yes | yes | round-robin/fair queue |
| ROUTER | DEALER, REQ, ROUTER | yes | yes | explicit routing-id envelope |
| XPUB | SUB, XSUB | yes | subscriptions | fan-out plus subscription events |
| XSUB | PUB, XPUB | subscriptions | yes | explicit subscription commands |

消息是离散且可 multipart；`SNDMORE` parts 由 socket 有界暂存，final part 成功时才把完整
消息原子转移到选定 peer outbound。`send` 成功只表示本地 socket 接管消息，不表示网络
发送或远端处理完成。REQ/REP 非法顺序返回明确 FSM
错误；`DONTWAIT` 在无法 admission/receive 时立即返回 would-block，普通 `send/recv`
则在调用者线程内分片推进所属 socket，直到操作成功或 progress 失败。

## 当前实现边界

旧的 connect/router callback endpoint 已删除。socket facade 直接拥有 CNet 与全部消息状态，
不再经过 endpoint callback、posted command 和二次 session 状态机。这样每次进度调用只有一个
socket owner lane，pattern FSM 与网络 peer 使用同一事实源。

## CNet 边界

`cnet_client_poll()` 可以一次推进该 client 的所有 connection，但 `cnet_listener_wait()`
目前是独立 wait primitive。FlowMQ 的 caller-driven `poll` 对列表中的 socket 执行非阻塞
progress，并以 1ms 有界间隔重复检查，因而具备统一 timeout 语义。CNet 后续若提供可组合
readiness/wait-set，可替换该等待策略以降低空闲唤醒延迟，但不得用隐藏线程掩盖这一边界。

## TLS

- TLS client 始终验证证书链与 hostname/IP，不提供 insecure fallback。
- TLS server 必须显式提供 certificate/key。
- mTLS 可验证客户端证书链；当前 socket 尚未把证书 fingerprint 绑定到 HELLO identity。
  因此 endpoint 只接受空 FMS/3 HELLO payload，不能宣称已启用业务 principal binding。
- 密钥与密码归 transport owner，销毁前清零；错误与日志不得输出 secret。

## 迁移与验证

已完成旧 callback endpoint 删除、context/socket facade、TCP/verified TLS、ROUTER envelope、
REQ/REP FSM、PUB/SUB、动态 subscription 增量同步、multipart receive staging、可配置
message/byte HWM、阻塞/DONTWAIT 分流、多 socket timeout poll 与 multipart 整体发送
admission、peer failure isolation、PAIR/ROUTER 唯一性和 session generation fencing。
Release libzmq 同 workload benchmark 已接入；后续重点是严格 receive fair queue 与明确的
可配置关闭语义。

验证至少包括：同线程 callback 证据、无 thread-create 符号、TCP 与 verified TLS
round trip、完整 pattern compatibility matrix、REQ/REP FSM、PUB/SUB filtering、
PUSH/DEALER round-robin、ROUTER identity、multipart atomicity、HWM/DONTWAIT，以及与
libzmq 相同 workload 的吞吐和延迟对比。
