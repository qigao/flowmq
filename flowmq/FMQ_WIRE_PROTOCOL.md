# FMQ/3 与 FMS/3 Wire Protocol

FMQ/3 是 FlowMQ 唯一 socket framing；FMS/3 是可选但不可降级的 HELLO security
envelope。本文是 wire 字段、校验、分片、心跳、pattern、queue 和
backpressure 边界的唯一详细正文。

协议总索引见 [PROTOCOL_SPEC.md](PROTOCOL_SPEC.md)。安全决策、provider 生命周期、
ACL 和决策理由见 [ADR_FMQ_V3_SECURITY.md](ADR_FMQ_V3_SECURITY.md)。本文不定义
TFMP 或 TFCW 的应用字段。

KCP transport 的 TKSH/1、TKSR/1 与 TKF1/1 由
[KCP_TRANSPORT_PROTOCOL.md](KCP_TRANSPORT_PROTOCOL.md) 定义。

## 1. 分层与版本

FMQ/3 位于 CoroNet transport 之上，应用协议位于 FMQ `DATA` payload 之内：

```text
TFMP/1、TFCW/1
              |
       FMQ/3 DATA payload
              |
  FMQ/3 frame + FMS/3 HELLO
              |
       CoroNet transport
```

整数使用 network byte order（big-endian）。文本是无 NUL 的有界 UTF-8，BYTES
字段保持 binary-safe。decoder 必须拒绝版本错误、保留字段非零、长度不一致、
越界、乱序分片、重叠分片和 trailing bytes。

`TFMQ` 是固定 magic，version 固定为 `3`。version byte 不是 negotiation 字段；
其他版本必须返回协议错误，不得协商、fallback 或静默接受旧版本。

一次连接双方各发送一个 `HELLO`。只有 HELLO 完成且 pattern pairing 合法后才能
接受 `DATA`。可信 v3 的 HELLO payload 必须为空；配置 security binding 时必须
使用 FMS/3，trusted 与 secure 两种模式不互相降级。

## 2. Frame layout

每个 packet 为 32-byte header 加 identity、topic 和 packet payload：

| Offset | Size | Field | Constraint |
| ---: | ---: | --- | --- |
| 0 | 4 | magic | `TFMQ` |
| 4 | 1 | version | `3` |
| 5 | 1 | kind | `HELLO`, `DATA`, `PING`, `PONG`, `SUBSCRIBE`, `UNSUBSCRIBE` |
| 6 | 1 | sender pattern | `1..11`，见第 4 节 |
| 7 | 1 | packet flags | `FIRST=0x01`, `LAST=0x02`；其他 bit 必须为零 |
| 8 | 2 | identity length | 仅 FIRST 携带，最大 255 bytes |
| 10 | 2 | topic length | 仅 FIRST 携带，最大 1024 bytes |
| 12 | 4 | packet payload length | 最大 64 KiB |
| 16 | 8 | message ID | DATA 非零；其他 kind 为零 |
| 24 | 4 | complete payload length | 同一 message 的完整 payload 长度 |
| 28 | 4 | packet payload offset | 从零连续递增 |
| 32 | variable | fields | identity、topic、packet payload |

`max_frame_size` 限制完整 identity + topic + payload，不替代单 packet 上限。DATA
payload 超过 64 KiB 时必须按连续 offset 分片；identity 和 topic 只能出现在 FIRST
packet，后续 packet 必须为零长度。最后一个 packet 必须设置 LAST。空 payload 的
单 packet DATA 仍必须满足完整 frame 规则。

HELLO、PING、PONG、SUBSCRIBE 和 UNSUBSCRIBE 必须是单 packet。PING/PONG 不携带
identity、topic 或 payload；SUBSCRIBE/UNSUBSCRIBE 只携带 topic，不携带 identity
和 payload；所有 control frame 的 message ID 必须为零。

成功 decode 后，单 packet identity、topic 和 payload 是输入 buffer 的 borrowed view；
fragmented payload 由 decoded frame 持有，调用方必须执行
`flowmq_protocol_frame_cleanup()`。scatter/gather encode 只拥有 framing storage，
payload backing 必须保持到 send 完成。

## 3. FMS/3 security envelope

FMS/3 只允许作为 FMQ/3 HELLO payload 出现，magic 为 `FMS3`。非空 envelope 的
12-byte header 为：

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | magic `FMS3` |
| 4 | 1 | mode：`AUTH=1`、`ACCEPTED=2` |
| 5 | 1 | identity length |
| 6 | 1 | authentication method length |
| 7 | 1 | channel binding length |
| 8 | 4 | credential length |
| 12 | variable | identity、method、channel binding、credential |

identity 最大 255 bytes，method 最大 63 bytes，credential 最大 4096 bytes；
channel binding 只能为空或 32 bytes。`AUTH` 必须包含 identity、method、credential；
`ACCEPTED` 不包含 identity、method 或 credential，只可包含 channel binding；
`NONE` 只能用空 HELLO payload 表示，不能编码为非空 FMS3。

FMS/3 只描述 wire envelope，不决定 provider 是否接受 credential。配置 security
binding 后，server 必须在创建 live peer、加入 selector/route registry 或发布 graph
message 前完成认证、claimed identity 与 principal 一致性以及 CONNECT ACL。后续
SUBSCRIBE、READ、WRITE、EXECUTE 仍需 ACL 检查。credential 只在 provider lease 和
HELLO 边界内存在，消费或释放前必须清零。

TCP、UDP、Pipe、WS 的 FMS credential 需要可信网络或额外安全隧道。KCP 在 FMS/3
之下强制执行 PSK 认证与 TKSR/1 AEAD，因此具备 transport confidentiality 和
integrity；FMS/3 仍负责应用 principal 与 ACL，不能由 transport PSK 替代。TLS/WSS
必须验证证书、协商 TLS 1.3，并使用 RFC 9266 exporter channel binding。任何认证、
证书、exporter 或 binding 失败都必须关闭连接，不得回退到其他 transport 或 trusted
session。

## 4. Pattern registry

| Value | Pattern | Valid peer | Core contract |
| ---: | --- | --- | --- |
| 1 | PUB | SUB、XSUB | topic prefix fan-out |
| 2 | SUB | PUB、XPUB | subscription receive |
| 3 | PUSH | PULL | 单 peer work distribution |
| 4 | PULL | PUSH | 单 peer receive |
| 5 | ROUTER | DEALER | identity route、异步 reply |
| 6 | DEALER | ROUTER | 异步 request/reply |
| 7 | PAIR | PAIR | 一对一双向 |
| 8 | REQ | REP | 一个 outstanding request |
| 9 | REP | REQ | 当前 dispatch 内同步 reply |
| 10 | XPUB | SUB、XSUB | 显式 subscription event |
| 11 | XSUB | PUB、XPUB | subscription control/data |

不兼容 pairing 必须在 HELLO 阶段失败。PUB 无匹配 peer 返回 `TURBO_ENOTCONN`；
部分 fan-out 成功后失败不可回滚，也不得隐式重播。PUSH 选定 eligible peer 后，
传输结果不确定时不得改投其他 peer。

REQ 状态为 `READY -> WAIT_REPLY -> READY`。timeout 或断线后必须等待新 session
HELLO，旧 generation reply 不得完成新 request。REP request context 只借用到当前
dispatch 返回，不能 detach 或持久化。

ROUTER delayed reply 可 detach，但 route 必须绑定
`owner_instance_id + session_id + session_generation`。断线、同 identity 重连或
ROUTER restart 后旧 route 返回 `TURBO_ENOTCONN`，不得写入 durable store 后重放。

XPUB/XSUB subscription state 属于 peer session。XSUB 可在 reconnect 后重放 desired
subscriptions；完整 stop 会清除该状态。空 subscription prefix 匹配全部 topic，
session 断开会产生相应 UNSUBSCRIBE。

## 5. Heartbeat

heartbeat 使用 monotonic clock deadline。接收任何合法 FMQ frame 都刷新 receive
deadline；发送 PING 只推进下一次 PING deadline。下一动作只有 `WAIT`、`SEND_PING`、
send-side `EXPIRED` 和 receive-side `RECV_EXPIRED`。心跳不改变业务 ACK、delivery
或 completion 语义。

## 6. ACK 与 ownership

以下边界不可互相冒充：

| Signal | Meaning |
| --- | --- |
| graph publish success | 当前 graph attempt 成功 |
| frame admission | 本地有界发送边界接管 encoded frame |
| transport send success | CoroNet 完成一次写入 |
| storage accept | durable owner transaction 已提交 |
| delivery/completion | consumer/worker 完成且 storage settlement 成功 |

FMQ HWM 只限制本地内存，不表示远端接收、处理或持久化。进入 graph、queue、worker
或跨线程边界前，payload、topic、identity、correlation 和 FMQ metadata 必须转换为
owned `mem_buffer_t`。raw socket bytes、decoder buffer 和 frame view 只能留在
CoroNet owner lane。

## 7. Pattern queue 与 backpressure

queue、credit 和 HWM 是本地 pattern/session 状态，不进入 FMQ/3 frame，也不改变
第 2 节的 wire layout。所有 accepted frame 仍由全局 `frame_hwm_messages` /
`frame_hwm_bytes` admission 计费；只有最终 completion、drop 或 shutdown cancel
才能释放该全局 budget。

### 7.1 PUB/XPUB：bounded per-peer fan-out queue

通过 fan-out registration 启用的 PUB/XPUB 为每个 live SUB/XSUB peer 建立独立 FIFO
和独立 writer coroutine。topic matching、READ ACL 和 queue capacity 在 enqueue
前确定；同一 peer 内保持 publish order，一个慢 peer 的 transport wait 不阻塞其他
peer writer。

每个 peer 同时受 `peer_hwm_messages` 和 `peer_hwm_bytes` 约束。达到 HWM 时只执行配置
的 `slow_peer_policy`：

| Policy | Admission 与 peer 结果 |
| --- | --- |
| `FAIL` | 当前 fan-out admission 失败；已取得的 peer reservation 全部回滚，不能先写 fast peer |
| `DROP_OLDEST` | 只从饱和 peer 的队首取消足够旧 frame，再接纳当前 frame |
| `DISCONNECT` | 关闭饱和 peer 并取消其 backlog；其他已接纳 peer 继续发送 |

成功 publish 表示当前 frame 已进入所有仍被选中 peer 的 volatile queue，不表示任一
peer 已收到、处理或持久化。一个 frame 对多个 peer 共享 immutable payload ownership，
但每个 peer 独立报告 send/drop 结果；部分成功不可回滚，也不得自动重播。

未启用 fan-out registration 的 PUB/XPUB 保持 transport-write completion 语义，不得
静默切换为 queue-admission completion。启用 bounded fan-out 时，live peer identity
必须非空且在当前 adapter 内唯一，以便事件和 slow-peer policy 有稳定归属。

### 7.2 PUSH：global pending queue 与 per-peer write credit

PUSH 的全局 FIFO 是尚未绑定 PULL 的唯一事实源。dispatcher 使用 round-robin 选择
通过 READ ACL 且拥有 write credit 的 live PULL；每个 PULL 同时最多持有一个本地
write credit。busy peer 不参与后续选择，因此 slow PULL 只占用自己的 credit，不会
形成跨 peer head-of-line blocking。

全体 eligible peer 都 busy 时，单 frame 或完整 explicit batch 必须按原顺序恢复到
全局队首，等待任一 peer credit 释放或新 peer 加入。explicit batch 的候选 peer 数在
该批次开始时冻结，避免连接变化改变已经计算的 round-robin 映射。

frame 一旦离开全局 queue，必须绑定当前 adapter owner 内
`session_id + session_generation` 对应的 live route。该 route 断线、generation
改变或 transport write 结果不确定时，frame 失败为
`TURBO_ENOTCONN` 或实际 transport error；不得恢复为 unassigned，也不得改投其他
PULL。

一个 peer credit 可连续发送同一批次中分配给该 peer 的多个 frame。TCP write group
按 FIFO prefix 分块，每个 chunk 最多 64 个 iovec、目标上限 1 MiB，并进一步受
`send_hwm_bytes` 限制。frame 不因内部 chunk 边界拆到不同 peer；单 frame 大于 1 MiB
时仍作为一个有界于 `max_frame_size` 的 write，配置了 transport HWM 时 encoded
frame 必须不大于该 HWM，否则返回 `TURBO_ENOBUFS`。每个 chunk 完成后即可释放其中
frame 的 ownership，但 peer credit 直到该 peer queue drain 或失败后才释放。

### 7.3 Failure 与 shutdown

peer write 失败会关闭该 peer，已绑定在其 local queue 的 frame 以同一错误完成；
这些 frame 不返回全局 queue。尚未绑定的 global pending frame 仍可由其他 eligible
peer 获取。

stop/shutdown 顺序固定为：

1. 关闭全局 admission，唤醒并拒绝新的 BLOCK waiter。
2. 按 `frame_linger_ms` drain，或以 `TURBO_ESHUTDOWN` 取消 global pending。
3. interrupt live socket，使已绑定 peer lane 完成或取消。
4. 等待所有 peer reader/writer lane quiesce。
5. 清理 peer queue、route/session、budget 和 execution resources。

任何阶段都不得在 lane 仍可能访问 payload、socket 或 peer state 时提前释放其 owner。

## 8. Implementation evidence

规范实现位于 `protocol/include/flowmq_protocol.h`、`protocol/src/flowmq_protocol.c`
、`runtime/src/flowmq_pattern.c` 和 `src/flow_fmq.c`。对应测试覆盖 encode/decode、
fragmentation、security envelope、unknown version、malformed control frame、
pattern pairing、heartbeat deadline、slow-subscriber policy、PUSH round-robin、
route generation fencing 和 slow-PULL credit isolation，入口为
`protocol/tests/test_flowmq_protocol.c`、`runtime/tests/test_flowmq_pattern.c` 与
`tests/test_fmq.c`。
