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

### 协议 READY 与重连退避

连接 endpoint 拥有退避状态，peer generation 拥有一次性的 READY 标记。
TCP/TLS `CONNECTED` 只表示 transport 建连成功；只有本地 HELLO/SETTINGS 的发送完成、
远端 HELLO/SETTINGS 的接收与校验全部成功，才重置该 endpoint 的退避。
TX-first 与 RX-first 均经过同一 READY gate。错误 pattern 或 TLS certificate/HELLO
identity 校验失败不能重置退避，也不能使 DATA 获得发送资格。

READY 是本地协议状态，不是对端应用执行确认。TLS 身份拒绝的重连测试由连接方
ROUTER 执行 identity policy，并观测它自己的 endpoint 退避；不能从被服务端拒绝的
客户端推断远端授权已成功或尚未成功。现有 FMQ/6 没有提供这样的远端确认。

重连不重放旧 DATA。旧 peer 关闭时取消绑定到它的未完成 multipart，释放暂存内容，
下一次发送返回 `SALTS_ENOTCONN`；新 READY 会话仅接受随后显式提交的新消息。
XSUB 的期望订阅重新同步属于控制面恢复，与 DATA replay 分开。
这些边界由 `test_flowmq_peer_state` 的握手排列测试及 `test_flowmq_socket` 的真实
TCP 错误 HELLO、TLS 身份拒绝、multipart 断连和 XSUB 重连测试覆盖（#118 / #120）。

### 已观察完成批次的错误边界

Owner 每轮只有一次 `native_io_backend_observe()`。一旦取出完成批次，即使某项
路由失败，也必须继续处理剩余事件，再推进各 live socket 的本地阶段，最后返回
第一个错误。路由函数可能先消费事件再返回错误，因此不能将该事件重试或交给
另一个 socket；完全无人认领的事件仍返回 `SALTS_EPROTO`，不能静默吞掉。

`test_flowmq_owner_fault` 使用三个实际 TCP ROUTER/DEALER 连接共享一个 Owner，
在同一次真实 observe 返回的首项或中间项被 CNet 消费后注入错误报告。
测试检查批次后续事件、retained payload 的单次释放、排队消息传递、发送计数、
流控额度恢复，以及关闭出错 socket 后相邻连接继续推进。
私有同步 dispatcher 仅用于该验收，不安装到 SDK、不保存在 Owner 中；普通
poll/close 仍直接选择默认批次路由。测试没有制造完成事件、替换 backend 或增加
Owner 的 observe 次数。这覆盖 #119 的真实批次故障边界，不代表 #125 的 SG
host lease、独立 SG completion、陈旧 generation 或跨平台验收已完成。

## 可选长期 peer pool（#123）

`flowmq_socket_set_peer_pool()` 在启动前显式启用每 socket 独立的 CNet
pool；默认关闭。Manager、pool 和协议状态由同一个 progress owner 驱动。
每条连接在 connect/adopt 前预留 physical/connecting 名额，完整 FMQ/6
HELLO/SETTINGS 双向完成、TLS 身份校验通过后才绑定真实 Manager BOUND
记录，并通过协议回调提交一个独占 pipe slot。该 lease 覆盖整个会话，
不是逐消息租借；DATA 仍使用既有队列、HWM 和协议 credit。

不选择跨 socket 共享 pool：现有 Manager 的归属是 socket，共享会引入
owner、凭据和 REQ/REP 状态迁移。pool key 使用内部稳定编号，区分 socket、
endpoint、transport、不可变 TLS 配置、pattern 及每次连接的唯一代际。
这些编号仅在所属 pool 内有效，不是凭据散列。连接代际不复用，因此未知的
远端身份不会在握手前获得 lease，旧 ROUTER 路由、订阅或事务不会迁入新会话。

真实终止回调先标记 pool terminal；尚未消费的接收消息继续保留 peer、
Manager context 与 pipe lease。最后一条消息消费后（或 socket 关闭丢弃后）
释放 lease，再释放 context。保留发送内存仍按 CNet send terminal 释放，
pool 不替代该协议。shutdown 先 seal，驱动真实终止并清空 lease，再销毁
pool、Manager 和 client。连接和 lease 数分别有界；字节容量继续由既有
发送/接收 HWM 管理，不引入新的等待队列、预热、重试或 DATA 重放。

主动 connect 满容量立即返回 `SALTS_ENOBUFS`；listener 满容量保留 OS
backlog，并继续推进已有连接。snapshot 从 CNet pool 读取连接/lease 状态，
不维护第二套容量计数。迁移可逐 socket 启用；回滚只需在下次创建 socket
时不启用，线上会话不支持更换 pool 配置。

## 发送内存与所有权

`flowmq_send` 仍保持 borrowed-input/copy 契约：FMQ/6 协议头写入 socket 自有的
有界 framing storage，payload 只在本次普通函数调用期间借用。立即发送在返回前
复制到 socket 池拥有的 buffer，再通过 `cnet_send_buffer` 提交并保留引用，因此调用者可在 `flowmq_send`
返回后立即修改或释放输入；普通 `flowmq_send(const void *)` 不会偷偷升级成 borrowed
zero-copy。

`flowmq_send_slice` 是显式的 owned/retained immediate DATA surface。它只接受
canonical non-empty `mem_slice_t`，为 FMQ/6 header/identity/topic 单独分配一个
FlowMQ-owned framing buffer，而 payload ranges 继续引用调用者 slice 的原 backing；
最终用 `cnet_send_slicev` 一次 admission。成功后调用者可以立即
`mem_slice_release()` 自己的引用，CNet 在 logical send terminal 前保留 backing；
这段时间 backing bytes 以及 buffer 的 data/used/capacity 必须保持不可变。

`flowmq_send_slice` 同时支持有界 retained multipart。每个成功的
`FLOWMQ_SNDMORE` part 都把其 canonical slice ownership 同步转移到 socket 的独立 retained
staging；调用者可立即释放自己的引用。retained staging 与 copied `send_staged` 不混用，
唯一例外是 ROUTER 可先用普通 `flowmq_send(..., FLOWMQ_SNDMORE)` 选择 routing-id envelope，
随后所有 DATA parts 使用 retained surface。

final retained part 到来时，FlowMQ 按 FMQ/6 wire 顺序组合 staged ranges 与 final ranges；
只有 aggregate range count 不超过 `CNET_RETAINED_VECTOR_MAX`（当前 32）且 aggregate encoded
bytes 在 CNet send bound 内时，才通过一次 `cnet_send_slicev` 提交。因此一个 retained
multipart message 仍只有一个 CNet logical send terminal。超限返回显式
`SALTS_EMSGSIZE`，不 split、不 flatten、不 copy；已经成功 staged 的前缀仍由 socket 持有，
可由调用者重试 final part，或在 disconnect/cancel/close 时精确释放。

PAIR/PUSH/DEALER/REQ/REP 与 ROUTER 的 retained multipart 都保持选定 peer/generation pinning；
REQ/REP FSM 只在 final aggregate admission 成功后完成本次 send transaction。retained
multipart PUB/XPUB 暂时 `SALTS_ENOTSUP`，因为 multi-peer atomic retained ownership 需要独立
设计；final-only retained publication 维持现有 mute/drop 语义。plaintext TCP 支持 retained
surface；TLS 仍明确返回 `SALTS_ENOTSUP`，不会隐式转成 copy/encryption fallback。

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

## 接收内存与所有权

当前 receive decode 之后存在两个明确的 copy boundary：

```text
flowmq_stream_decoder_next()
        -> frame.payload borrowed view
        -> flowmq_socket_stage_bytes()
             copy -> socket message_pool mem_buffer_t
        -> inbound ring owns mem_buffer_t
        -> flowmq_recv()
             copy -> caller storage
```

`flowmq_recv_slice` 只消除第二个边界，不伪装成 CNet-ingress zero-copy。成功 dequeue 时，
inbound ring 的唯一 canonical `mem_buffer_t` reference 直接转移到 caller 的
`mem_slice_t`；FlowMQ 不执行 retain+release 对，也不把 bytes 复制到新的 detached storage。
这使 zero-length part 也能保持同样的 ownership-transfer 规则。

receive credit、inbound HWM accounting、`RCVMORE` 与 pattern/REQ/REP FSM 都在 dequeue
成功时推进，与 `flowmq_recv` 保持一致；应用随后持有 slice 的时间不会占用 transport
credit 或 socket inbound occupancy。caller 必须提供空的输出 slice；非空 descriptor
直接返回 `SALTS_EINVAL` 且不修改原 owner，避免覆盖 live reference。对合法的空输出，
错误、DONTWAIT would-block 与非法 FSM 路径都保持为空且不取得 ownership。

Salts `1.8.3` 是本仓当前 released producer baseline，并已包含 CNet owned-receive #594/#597 的 producer-owned receive contract。本次依赖对齐仍不改变 FlowMQ runtime ownership，因此这里继续描述现有 socket `message_pool` owner；迁移到 CNet producer-owned backing 由 FlowMQ #46 单独实现和验证。

该 surface 的 lifetime 明确受 socket memory pool 约束。Salts 1.8.3 的 `mem_destroy()`
会直接销毁 pool slab，并不等待 outstanding pooled buffers，所以返回的 slice 可以跨后续
owner-thread `poll/send/recv` progress 持有，但必须在 `flowmq_close()` 前
`mem_slice_release()`。FlowMQ 不通过隐藏 copy 来制造 close 后 lifetime。若未来要求 slice
跨 socket/context destruction 存活，需要 Salts 提供 detachable/refcounted pool owner，
或另行定义显式 non-pooled receive ownership。

decoder payload -> message_pool 的第一次 copy 仍保留。是否让 stream decoder/CNet 直接产出
canonical owned receive backing 是独立的后续优化，因为它会改变 decoder consume/lifetime
边界，不应与 public recv-slice ABI 的第一阶段混合。

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

peer pool 的正式回归包括 `test_flowmq_peer_pool`（配置 0/1/full、
READY 前无 lease、终止后容量回收、残留 multipart 代际、握手期间关闭）、
`test_flowmq_socket_pool`（同一套 TCP/TLS、pattern、HWM、retained-send 用例）
和 `test_flowmq_owner_fault_pool`（真实共享 NativeIO 完成批次及邻接连接隔离）。
`test_flowmq_public_c11` / `test_flowmq_public_cpp17` 复用 package consumer，
检查共享库公开 ABI；仅启用测试时要求 C++ 编译器，产品仍是 C11。

Windows 使用已发布 Salts 2.3.0-rc.1 / Salts Utils 4.3.0-rc.1，通过 user
preset 构建和 CTest 验证；pool 生命周期与 Owner fault 用例各重复 20 次通过。
可在 VsDevCmd 环境复现：

```sh
cmake --build --preset win-release-user
ctest --preset win-release-user -R "^test_flowmq_" --output-on-failure
ctest --preset win-release-user -R "^test_flowmq_(peer_pool|owner_fault_pool)$" --repeat until-fail:20 --output-on-failure
```

#123 的 Windows 安装 SDK C11/C++17 consumer、TCP/TLS 1/2/4 progress owners
对照现已执行；原始数据、波动与复测、复现命令和剩余范围见
[peer pool 验收记录](PEER_POOL_QUALIFICATION.md)。跨平台 CI 实跑与独立的
retained-byte/分配成本证据仍待完成，当前测量不证明普遍性能收益。
