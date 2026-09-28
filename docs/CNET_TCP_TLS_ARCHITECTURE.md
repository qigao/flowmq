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

`flowmq_send` 仍保持 borrowed-input/copy 契约：FMQ/6 协议头写入 socket 自有的
有界 framing storage，payload 只在本次普通函数调用期间借用。立即发送继续通过
`cnet_sendv` 在成功返回前复制到 CNet-owned storage，因此调用者可在 `flowmq_send`
返回后立即修改或释放输入；普通 `flowmq_send(const void *)` 不会偷偷升级成 borrowed
zero-copy。

`flowmq_send_slice` 是显式的 owned/retained immediate DATA surface。它只接受
canonical non-empty `mem_slice_t`，为 FMQ/6 header/identity/topic 单独分配一个
FlowMQ-owned framing buffer，而 payload ranges 继续引用调用者 slice 的原 backing；
最终用 `cnet_send_slicev` 一次 admission。成功后调用者可以立即
`mem_slice_release()` 自己的引用，CNet 在 logical send terminal 前保留 backing；
这段时间 backing bytes 以及 buffer 的 data/used/capacity 必须保持不可变。

该 retained surface 第一阶段故意保持 immediate-only：不接受
`FLOWMQ_SNDMORE` 用户 multipart staging，busy DATA lane 返回 would-block，而不是
复制进 outbound queue。ROUTER 仍先用现有 `flowmq_send(..., FLOWMQ_SNDMORE)`
提交 routing-id envelope，再用 `flowmq_send_slice(..., 0)` 提交 final DATA。
plaintext TCP 支持该路径；TLS 明确返回 `SALTS_ENOTSUP`，不会隐式转成 copy/encryption
fallback。PUB/XPUB 对 busy/mute peer 继续采用 drop 语义。

需要排队的完整 frame 只平铺一次到其 canonical `mem_buffer_t`。plaintext TCP flush
将这些 owner buffers 转成 `mem_slice_t` 并通过 `cnet_send_slicev` retained admission
提交；FlowMQ 随即释放自己的 slice/queue 引用，CNet 在 logical terminal 前保持 backing
ownership。一个 FlowMQ queued batch 最多使用 `CNET_RETAINED_VECTOR_MAX`（当前 32）
个 logical ranges；CNet 再按 `NATIVE_IO_VECTOR_MAX`（当前 16）切成 successive native
windows，期间不 flatten、不复制 payload，也不发布中间 send terminal。TLS 仍明确使用
copy/encryption path，不从 rejected retained-SG 隐式 fallback。

立即发送的数据路径从 `payload -> FlowMQ 1MiB scratch -> CNet slot` 缩短为
`payload -> CNet slot`，payload copy 从两次减为一次。queued TCP 的 frame payload
在 FlowMQ 排队时仅形成 canonical owned buffer，flush 不再额外 flatten。原来的每 socket
约 1MiB 连续 scratch allocation 已删除，替换为固定 framing 和 descriptor storage。

普通 `flowmq_send` 仍不是跨 poll 的 borrowed-send，也不改变原有完成语义。
只有显式 `flowmq_send_slice` 把 canonical owner lifetime 延伸到网络 terminal，而且通过
Salts Core refcount 表达，不保留裸 caller pointer。TLS 仍必须让加密层接受其明确的
copy/encryption plaintext path。任何 retained admission 失败都不留下 backing retain，
也不提交 peer credit/HWM；queued copy path 的 buffer 仍由原 owner 持有。

## TCP latency policy

FlowMQ plaintext TCP runtime 在 CNet client 启动后显式设置
`cnet_stream_socket_options.nodelay = 1`。这是 FlowMQ 的 messaging latency policy，
不是 CNet 的全局默认：Salts 保持 `nodelay=0` 时的 platform/Nagle default。

该策略是 retained-SG 跨 native window 的必要配套。64 KiB logical write 从 16 ranges
跨到 17/32 ranges 时，Linux 默认 Nagle 与 delayed ACK 可形成约 40 ms 的 latency cliff；
显式 `TCP_NODELAY` 后恢复到几十微秒，同时仍保持两个 NativeIO vector windows 和一个
CNet logical terminal。FlowMQ 不用 copy fallback 掩盖这一 transport effect。

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
