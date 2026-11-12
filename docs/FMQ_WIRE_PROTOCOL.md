# FMQ/6 与 FMS/3 Wire Protocol

FMQ/6 是 FlowMQ 的 socket framing。FMS/3 定义可选 HELLO security envelope 的语法。
本文是 wire 字段、校验、分片、心跳、pattern、queue 和
backpressure 边界的唯一详细正文。

协议总索引见 [PROTOCOL_SPEC.md](PROTOCOL_SPEC.md)。本文定义 wire envelope，不定义应用 payload。

## 1. 分层与版本

应用协议位于 FMQ `DATA` payload 内。FlowMQ 在下列 CNet transport 上使用同一套
FMQ/6 帧格式。

| Transport | FMQ/6 封装 |
| --- | --- |
| TCP、TLS、IPC | 在有序字节流中连续传输 FMQ frame |
| WS、WSS | 完成 HTTP Upgrade 并选定 `fmq.v6` 后，每个 binary message 携带一个完整 FMQ frame |
| KCP | 普通或 PSK 会话建立后，每个 KCP 逻辑消息携带一个完整 FMQ frame |

整数使用 network byte order（big-endian）。文本是无 NUL 的有界 UTF-8，BYTES
字段保持 binary-safe。decoder 必须拒绝版本错误、保留字段非零、长度不一致、
越界、乱序分片、重叠分片和 trailing bytes。

`TFMQ` 是固定 magic，version 固定为 `6`。version byte 不是 negotiation 字段；
其他版本必须返回协议错误，不得协商、fallback 或静默接受旧版本。

一次连接双方各发送一个 `HELLO` 和一个 `SETTINGS`。只有 HELLO、SETTINGS 完成且
pattern pairing 合法后才能接受 `DATA`。当前 socket runtime 的 HELLO payload 为空。
TLS/WSS 的证书身份校验使用 HELLO 的 identity 字段。

## 2. Frame layout

每个 packet 为 32-byte header 加 identity、topic 和 packet payload：

| Offset | Size | Field | Constraint |
| ---: | ---: | --- | --- |
| 0 | 4 | magic | `TFMQ` |
| 4 | 1 | version | `6` |
| 5 | 1 | kind | core `1..6`, SETTINGS `32`, FLOW_UPDATE `33`；其他值非法 |
| 6 | 1 | sender pattern | `1..11`，见第 4 节 |
| 7 | 1 | packet flags | `FIRST=0x01`, `LAST=0x02`, `MORE=0x04`；其他 bit 必须为零 |
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

HELLO、PING、PONG、SUBSCRIBE、UNSUBSCRIBE、SETTINGS 和 FLOW_UPDATE 必须是单 packet。PING/PONG 不携带
identity、topic 或 payload；SUBSCRIBE/UNSUBSCRIBE 只携带 topic，不携带 identity
和 payload；SETTINGS/FLOW_UPDATE 只携带各自的固定 payload，不携带 identity 或
topic；所有 control frame 的 message ID 必须为零。

`MORE` 只允许用于 DATA，并且同一 DATA 的所有 packet 必须一致。`MORE=1` 表示该
application part 后还有 part；最后一个 part 使用 `MORE=0`。multipart 状态属于
socket pattern FSM，不把 packet fragmentation 暴露成 application part。

成功 decode 后，单 packet identity、topic 和 payload 是输入 buffer 的 borrowed view；
fragmented payload 由 decoded frame 持有，调用方必须执行
`flowmq_protocol_frame_cleanup()`。scatter/gather encode 只拥有 framing storage，
payload backing 必须保持到 send 完成。

### 2.1 SETTINGS 与 FLOW_UPDATE

每个方向的状态顺序固定为：

```text
transport ready -> HELLO -> SETTINGS -> READY -> DATA/FLOW_UPDATE
```

SETTINGS payload 为 32 bytes，全部字段使用 big-endian：

| Offset | Size | Field | Constraint |
| ---: | ---: | --- | --- |
| 0 | 4 | capabilities | 当前必须等于 `FLOW_CREDIT=0x00000001` |
| 4 | 4 | max frame size | 非零；发送方不得向该 peer 发送更大的 DATA part |
| 8 | 8 | session generation | 非零 |
| 16 | 8 | initial max data | 非零累计发送上限 |
| 24 | 4 | update quantum | `1..initial max data` |
| 28 | 4 | update interval ms | 非零 |

FLOW_UPDATE payload 为 24 bytes：

| Offset | Size | Field | Constraint |
| ---: | ---: | --- | --- |
| 0 | 8 | session generation | 必须匹配本方向 SETTINGS |
| 8 | 8 | consumed data | 单调不减 |
| 16 | 8 | max data | 单调不减且不小于 consumed data |

发送方始终满足 `sent_data <= max_data`。接收方在应用取走 DATA part 后按
`max_data = consumed_data + receive_hwm_bytes` 推进窗口。控制帧、ROUTER 合成的
identity part 和 XPUB 合成的 subscription event 不计 DATA credit。达到 quantum 或
最早未公布消费达到 interval 时，owner 的下一次 `send`、`recv` 或 `poll` 进度发送
累计 FLOW_UPDATE；控制帧优先于 queued DATA。

默认 quantum 为 `min(window, max(window / 4, 64 KiB))`，默认 interval 为 10 ms；可在
bind/connect 前通过 `FLOWMQ_FLOW_UPDATE_QUANTUM` 与 `FLOWMQ_FLOW_UPDATE_IVL` 调整。
重复 SETTINGS、SETTINGS 前 DATA、generation 不匹配、累计值倒退和越过已公布
`max_data` 都是协议错误。FMQ/6 不接受或回退到 FMQ/5。

## 3. FMS/3 security envelope

FMS/3 只允许作为 FMQ/6 HELLO payload 出现，magic 为 `FMS3`。非空 envelope 的
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

FMS/3 codec 负责 wire envelope 与字段校验。socket runtime 仅接收空 HELLO payload，
credential provider 和应用 ACL 尚未接入。若后续启用 FMS/3 provider，接纳 peer 前需验证
credential、claimed identity 与 principal 的一致性和 CONNECT ACL。应用操作再检查各自的
ACL，credential 在 provider lease 内使用，消费或释放前清零。

TLS/WSS client 由 CNet 验证证书链及服务器身份，listener 配置 certificate/key。
配置 `FLOWMQ_TLS_IDENTITY_POLICY` 的 ROUTER 在发送 SETTINGS 前将已验证的证书
SHA-256 指纹与 HELLO identity 匹配。配置该策略的 listener 必须要求客户端证书。
身份校验失败关闭当前连接。

## 4. Pattern registry

| Value | Pattern | Valid peer | Core contract |
| ---: | --- | --- | --- |
| 1 | PUB | SUB、XSUB | topic prefix fan-out |
| 2 | SUB | PUB、XPUB | subscription receive |
| 3 | PUSH | PULL | 单 peer work distribution |
| 4 | PULL | PUSH | 单 peer receive |
| 5 | ROUTER | DEALER、REQ、ROUTER | identity route、异步 reply |
| 6 | DEALER | ROUTER、REP、DEALER | 异步 request/reply |
| 7 | PAIR | PAIR | 一对一双向 |
| 8 | REQ | REP、ROUTER | 一个 outstanding request |
| 9 | REP | REQ、DEALER | reply 绑定最后一个 requester |
| 10 | XPUB | SUB、XSUB | 显式 subscription event |
| 11 | XSUB | PUB、XPUB | subscription control/data |

不兼容 pairing 必须在 HELLO 阶段失败。PUB/XPUB 无匹配 subscription 时成功丢弃；
已经接纳的 fan-out 不得隐式重播。PUSH 选定 eligible peer 后，
传输结果不确定时不得改投其他 peer。

REQ 状态为 `READY -> WAIT_REPLY -> READY`。REP 状态为
`WAIT_REQUEST -> SEND_REPLY -> WAIT_REQUEST`。绑定当前 transaction 的 peer 断线时，
socket 取消 transaction；下一个受影响的 send/receive 一次性返回 `SALTS_ENOTCONN`，
`flowmq_poll()` 在错误被消费前报告 `FLOWMQ_POLLERR`。新 session 完成 HELLO 后可继续，
旧 generation reply 不得完成新 request。状态迁移只在完整 multipart 的最后一个 part
成功 admission/receive 时提交。应用在错误 FSM phase 调用 send/receive，或在 multipart
中途切换方向，立即返回 `SALTS_EPROTO`，不得作为 would-block 重试。

ROUTER routing envelope 与 ZeroMQ 一样只公开 peer claimed identity，它是当前 live
session 的查找键，不是带 generation 的 opaque token。identity part 选中 peer 后，同一
outbound multipart 会 pin 到该 session generation；该 peer 中途断线时整条 multipart
取消，replacement 不会继承剩余 parts。跨消息的 delayed reply 若遇到同 identity 重连，
会路由到新的 live session；需要 request-session 隔离的应用必须使用不复用的 identity 或
在 payload 内携带并校验 correlation，不能只持久化 routing identity。当前 API 不提供
detached route handle。

XPUB/XSUB subscription snapshot 属于 peer session；XSUB desired subscriptions 属于 socket。
新 session 完成 HELLO/SETTINGS 后从 socket 状态重放 desired subscriptions；`flowmq_close()`
清除该状态。空 subscription prefix 匹配全部 topic，
session 断开会产生相应 UNSUBSCRIBE。

### 4.1 Reconnect session boundary

`FLOWMQ_RECONNECT` 给 TCP、TLS、IPC、WS/WSS 和 KCP 的 outbound endpoint 配置有界重连。
每次重连都要重新执行 `HELLO → SETTINGS → READY`，并创建新的 session generation、
credit window、decoder 和 pattern peer state。旧 session 未完成的 outbound frame 不会
转移或重播。需要交付保证的应用协议自行定义 correlation、确认与幂等处理。

CNet 从初次拨号起计算单轮预算，默认最多 32 次尝试、30 秒，采用 100 ms 起步、
1 秒封顶的指数退避与抖动。只有完整的 FMQ READY 才清零尝试次数和退避。
READY 后断线可开启新一轮预算。报告证书拒绝或 HELLO 身份拒绝时结束当轮重连。
`FLOWMQ_RECONNECT_DISABLED` 允许一次初始拨号，关闭自动重连。
进度由 socket 或显式 owner lane 驱动。

## 5. Heartbeat

heartbeat 使用 monotonic clock deadline。接收任何合法 FMQ frame 都取消当前未应答
PING 的 send-side timeout，并刷新 receive 与下一次 PING deadline。PING 真正进入 transport
后才启动 send-side timeout；后续 PING 不得延长同一个未应答 timeout。下一动作只有
`WAIT`、`SEND_PING`、send-side `EXPIRED` 和 receive-side `RECV_EXPIRED`。心跳不改变业务
ACK、delivery 或 completion 语义。

FMQ/6 的 PING/PONG 没有 payload，因此不携带 ZeroMQ `HEARTBEAT_TTL` 或 PING context。
socket facade 当前只提供 `FLOWMQ_HEARTBEAT_IVL` 与 `FLOWMQ_HEARTBEAT_TIMEOUT`；二者由
调用 `send`、`recv` 或 `poll` 的 owner 线程推进，没有后台 timer。应用停止调用进度函数时，
心跳和超时判定也会暂停。

## 6. ACK 与 ownership

以下边界不可互相冒充：

| Signal | Meaning |
| --- | --- |
| graph publish success | 当前 graph attempt 成功 |
| frame admission | 本地有界发送边界接管 encoded frame |
| transport send success | CNet 完成逻辑写入。KCP 的终态在逻辑消息得到 KCP ACK 后报告 |
| storage accept | durable owner transaction 已提交 |
| delivery/completion | consumer/worker 完成且 storage settlement 成功 |

FMQ HWM 只限制本地内存，不表示远端接收、处理或持久化。进入 graph、queue、worker
或跨线程边界前，payload、topic、identity、correlation 和 FMQ metadata 必须转换为
owned buffer。raw socket bytes、decoder buffer 和 frame view 只能留在 CNet owner lane。

## 7. Pattern queue 与 backpressure

发送和接收使用本地消息数与字节数 HWM。每个 peer 的发送占用包含已排队和在途的 DATA，
由发送完成或会话取消结算。接收占用在应用取走消息时更新。远端累计 credit 由第 2.1 节的
SETTINGS / FLOW_UPDATE 协调。

TCP/TLS 已负责可靠有序传输与拥塞控制；FMQ/6 不在同一 TCP/TLS stream 内发送 FEC
repair symbol。此类冗余不能绕过 TCP head-of-line blocking，只会消耗额外带宽和 CPU。

### 7.1 当前 socket queue 语义

当前 socket facade 为每个 live peer 保留有界 FIFO、独立 write 状态、message/byte HWM
和累计 credit。PUB/XPUB 在发送开始时冻结匹配 peer 集合；最终 part admission 时移除
已经饱和的 peer，没有 `FAIL`、`DROP_OLDEST` 或 `DISCONNECT` 等可配置 slow-peer
策略，也没有 READ/WRITE ACL。PUSH 直接从当前可写 PULL 中 round-robin 选择；不存在
可重放的 global pending queue，全体 peer 不可写时返回 `SALTS_EBUSY` 或
`SALTS_ENOBUFS`。

TCP/TLS 的逻辑写入可以使用 retained vector 或合批发送多个已编码 frame。
WS/WSS 和 KCP 保持一个逻辑消息携带一个完整 FMQ frame 的边界。成功 send 表示本地接纳。
peer 发生协议错误、断线或 write 失败时，已绑定该 peer 的未发送 frame 随旧会话结算。

### 7.2 当前 shutdown 语义

普通 socket 用 `flowmq_close()` 关闭，Owner socket 用 `flowmq_owner_close_socket()`。
两者在回收前推进传输关闭并结算在途资源。应用通过业务协议确认处理结果，并在关闭前完成确认。

## 8. Implementation evidence

规范实现位于 `flowmq/include/flowmq_protocol.h`、
`flowmq/src/protocol/flowmq_protocol.c`、`flowmq/src/core/session/flowmq_flow_control.c` 和
`flowmq/src/runtime/flowmq_socket.c`。对应测试覆盖 encode/decode、
fragmentation、security envelope、unknown version、malformed control frame、
pattern pairing、heartbeat deadline、累计 credit、generation fencing、quantum/deadline
更新、ROUTER identity 排除、TCP/TLS loopback/reconnect、XSUB subscription replay 和
pattern HWM，入口为
`flowmq/tests/protocol/test_flowmq_protocol.c`、`flowmq/tests/core/test_flowmq_flow_control.c`
与 `flowmq/tests/runtime/test_flowmq_socket.c`。
