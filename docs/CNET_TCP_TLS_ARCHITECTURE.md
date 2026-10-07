# FlowMQ CNet TCP/TLS 与 ZeroMQ socket 模型

## 决策

FlowMQ 的公开目标是 ZeroMQ 的 context/socket 调用方式和消息模式语义，不采用
Actor、Reactive 或 callback-first API。FMQ/6 仍是独立 wire protocol，因此不承诺与
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

多核应用可为每个 worker 创建独立 context/socket，由各线程自行推进 CNet。
可运行的 [REQ/REP 示例](../examples/multicore_reqrep.c) 使用主线程的一组 REQ
连接独立 worker REP endpoint；主线程必须统一推进全部客户端 socket，不能把
`send` admission 当作已发到网络。endpoint 在启动锁下发布，消息经 TCP 传递，
仅停止标志和首个错误通过 atomic 共享。客户端确认收齐全部应答后发出停止信号，
worker 保持最后一条应答的进度直到该信号，再在各自 owner 上销毁资源。
因此不改变 socket 单 owner 契约，也不要求为 FlowMQ 增加隐藏 progress thread。

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
发送或远端处理完成。REQ/REP 非法顺序和 multipart 方向交错立即返回 `SALTS_EPROTO`，不进入
阻塞重试；`DONTWAIT` 在无法 admission/receive 时立即返回 would-block，普通 `send/recv`
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

## 发送内存与所有权

`flowmq_send` 使用 FMQ/6 分段编码：协议头写入 socket 自有的有界 framing storage，payload
只在本次普通函数调用期间借用。分段数据在返回前复制到 socket 内存池的 `mem_buffer_t`，
因此调用者可在 `flowmq_send` 返回后立即修改或释放输入。立即发送通过 `cnet_send_buffer`
提交，排队 flush 使用 `mem_slice` 与 `cnet_send_slicev`，CNet 接受后保留不可变 backing buffer
的引用，不再把已排队 payload 平铺复制进 command slot。

一次 frame 最多 16 个 packet、32 个编码 segment。peer FIFO 仍有 1024 个 frame 槽位，
每次 flush 受 `CNET_RETAINED_VECTOR_MAX`（当前为 32）与 `max_encoded_size` 双重上限约束，
剩余 frame 在前一批完成后继续提交。批处理只改变内部写入边界，不改变消息顺序或 multipart
原子性。编码复制的时间复杂度为 O(encoded bytes)，flush 的描述符处理为 O(batch frames)。

缓冲区内容是发送字节的唯一事实源；完成前不可修改。临时 slice 在 admission 后释放，
成功时 FIFO 释放自身引用，CNet 在完成或取消后释放最终引用；失败时 FIFO、credit/HWM
保持原状。socket owner 单线程推进，保持每 peer 一次在途逻辑写入。TCP 使用 retained
scatter/gather，TLS 仍需加密并发送 ciphertext，不承诺 TLS 网络路径零拷贝。
`flowmq_close` 必须先停止并销毁 CNet client，再销毁 message pool。

此迁移要求重新构建消费者；公开 socket API、FMQ/6 数据格式与错误语义保持不变。
验证覆盖 TCP/TLS 下 64-part multipart、发送后输入覆写、队列顺序、HWM、重连和关闭；
批次上限变化的吞吐影响尚需专项 benchmark，不据此宣称性能提升。

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
caller-owned segmented framing、CNet bounded vector admission 与 Release libzmq 同 workload
benchmark 亦已接入；后续重点是严格 receive fair queue 与明确的可配置关闭语义。

验证至少包括：同线程 callback 证据、无 thread-create 符号、TCP 与 verified TLS
round trip、完整 pattern compatibility matrix、REQ/REP FSM、PUB/SUB filtering、
PUSH/DEALER round-robin、ROUTER identity、multipart atomicity、HWM/DONTWAIT，以及与
libzmq 相同 workload 的吞吐和延迟对比。
