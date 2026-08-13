# TurboFlow Management Protocol v1

本文件是 [协议索引](PROTOCOL_SPEC.md) 指定的 TFMP/1 与 TFMS/1.0、TFMS/1.1 唯一正文。
FMQ/3 framing 见 [FMQ_WIRE_PROTOCOL.md](FMQ_WIRE_PROTOCOL.md)。

本文定义的协议简称 **TFMP/1**。管理请求通过稳定身份 DEALER 发往 ROUTER；客户端可在
`max_inflight_per_target` 上限内流水化请求，并以 `correlation ID` 关联乱序响应。mutation
只使用 `ACCEPT_OPERATION`：服务端先返回 volatile 或 durable acceptance，随后由
`OPERATION_GET` 或 event 返回终态。该网络契约不提供同步 mutation 模式。

## 1. 决策背景

FMQ v3 是数据传输层，pattern 与 transport 由 YAML 组合。管理协议不能修改 FMQ v3
frame，也不能把 CoroNet socket、route 或进程内指针编码到远端消息中。管理通道需要避免
同步 request/reply 的 head-of-line blocking，同时保留幂等、operation、event 与故障域身份。

TFMP/1 采用三条彼此独立的契约：

| 契约 | FMQ pattern | 用途 | 事实源 |
| --- | --- | --- | --- |
| Management RPC | `DEALER -> ROUTER` | capability、查询、operation acceptance/result | 管理 owner/资源 owner |
| Live event | `PUB -> SUB` | 状态变化提示、operation 进度、故障通知 | 非事实源，仅派生通知 |
| Operation store | memory/SQLite/Redis/PG adapter | 幂等记录、长操作状态、可选 event journal | operation owner |

该分层遵循 [ZeroMQ Guide Chapter 7](https://zguide.zeromq.org/docs/chapter7/)
的 control/data 分离与“先固化 contract，再扩展实现”原则，但 TFMP/1 与 ZeroMQ
协议不兼容。

## 2. 不变量与非目标

1. FMQ 仍只支持 wire v3；TFMP/1 是 FMQ DATA payload 内的应用协议。
2. DEALER 可以在有界 inflight 表中并发多个请求；ROUTER 响应按 `correlation ID` 关联，
   不依赖收发顺序，也不能让一个长操作阻塞其他 correlation。
3. mutation 的网络响应只能是明确失败或 `ACCEPTED_VOLATILE`/`ACCEPTED_DURABLE`；
   终态由 operation 查询或 event 取得。
4. 管理 flow 与被管理 data flow 必须不同。目标执行 `drain` 时不得等待自己的 ROUTER dispatch。
5. transport 独立于协议，可选择 `pipe`、`tcp`、`tls`、`kcp`、`ws` 或 `wss`。raw UDP
   不进入 TFMP/1 产品验证矩阵，因为它不能提供 RPC 所需的可靠、有序会话语义。
6. TFMP 的 `client_id` 不是认证身份。TLS/WSS 使用各自安全会话；KCP 必须使用
   [KCP_TRANSPORT_PROTOCOL.md](KCP_TRANSPORT_PROTOCOL.md) 的认证握手、AEAD、replay window
   和认证 FEC。应用 authority/ACL 仍由 FMQ security owner 决定。
7. 所有 wire 数据有界、pointer-free、network byte order；未知 critical 字段 fail fast。
8. resource 状态由 resource owner 掌握；operation 状态由 operation owner 掌握；event
   只能从二者派生，不得反向修改事实。

## 3. 身份、版本与 capability

### 3.1 身份范围

| 字段 | 生命周期 | 语义 |
| --- | --- | --- |
| `authority_id` | 部署配置稳定 | 管理事实源的稳定名字，同一管理域内唯一 |
| `incarnation_id` | memory journal 启动时改变；durable journal epoch 内稳定 | 16-byte journal 身份，用于检测事件序列失效 |
| `target_uid` | 资源生命周期稳定 | flow/resource 的稳定 UID，不使用显示名或 route 作为身份 |
| `client_id` | 客户端配置稳定 | 幂等作用域的一部分，不是认证身份 |
| `request_id` | DEALER inflight 生命周期内唯一且非零 | 只用于请求/响应关联；响应可乱序 |
| `idempotency_key` | 一次语义命令稳定 | 与 `client_id` 共同唯一，重连或客户端重启后仍可复用 |
| `operation_id` | operation 生命周期稳定 | 服务端生成的 16-byte ID，可跨服务重启查询 |

事件顺序只在 `(authority_id, incarnation_id)` 内比较。memory replay 的 incarnation 在 owner
重启时改变；durable replay 从同一 snapshot 恢复 incarnation 与 sequence，直到 journal 被重置或
替换。`incarnation_id` 变化或 sequence 出现缺口时，SUB 必须通过 RPC 重取快照，不能把不同
journal epoch 的 sequence 拼接为全局顺序。

### 3.2 版本规则

- `major` 不兼容；TFMP/1 server 只接受 `major == 1`。
- `minor` 只允许增加 optional TLV、message kind 或 capability；不得改变既有字段语义。
- 每个请求都携带 major/minor。无需维护 connection-local negotiation 状态。
- `CAPABILITIES_GET` 返回 server 支持的 minor 范围、message kind、command type、限制与
  durability。未先查询 capability 仍可发请求；不支持的请求必须返回明确错误，不能断开后静默降级。
- FMQ HELLO 继续只协商 FMQ pattern/transport 契约，不承载 TFMP capability。

初始 capability ID：

| ID | 名称 | 保证 |
| ---: | --- | --- |
| 1 | `target_query` | target list/get 与稳定 catalog generation |
| 2 | `resource_query` | resource list/get/document |
| 3 | `conditional_command` | expected generation 条件写入 |
| 5 | `volatile_operation` | operation 已复制进入有界 owner queue，但重启可丢失 |
| 6 | `durable_operation` | operation 与幂等记录已原子持久化，可重启恢复 |
| 7 | `operation_cancel` | 仅取消尚未开始或显式支持取消的步骤 |
| 8 | `live_event` | best-effort PUB/SUB 提示与 gap detection |
| 9 | `event_replay` | 外部 journal 支持按 incarnation/sequence 重放 |

## 4. RPC wire envelope

每个 request 和 correlated response 的 payload 都由 40-byte header 加 canonical LTV body 组成：

```text
offset  size  field
0       4     magic "TFMP"
4       2     major (1)
6       2     minor
8       2     header size (40)
10      2     message kind
12      4     flags
16      8     correlation ID
24      4     body size
28      2     protocol status
30      2     disposition
32      8     reserved zero
```

请求的 `correlation ID` 是非零 `request_id`，`status` 与 `disposition` 必须为零。响应必须
复制 request ID，并设置 `RESPONSE` flag。只有 header 无法可信解码时，错误响应才允许
request ID 为零；若此时连 kind 也无法恢复，使用 response-only `PROTOCOL_ERROR` kind。
未知 flag、非零 reserved、长度不匹配或 trailing bytes 都是 malformed。

已定义 flag：

- `0x00000001 RESPONSE`
- `0x00000002 REPLAYED`
- `0x00000004 EVENT`

### 4.1 Canonical LTV

body 必须复用已安装 `TurboUtils::Parser` 导出的 `turbo_ltv_*` LTV wire：

```text
varint(length = 1 + value_size) | type:u8 | value
```

TFMP 不另写通用 TLV/LTV parser。`type` 的低 7 bit 是 field ID（1..127），最高 bit 是
`CRITICAL`。因此 required field 的 wire type 为 `0x80 | field_id`；optional field 只写
`field_id`。field 的具体 scalar 类型由 message schema 决定，不在每个 field 重复编码：

- `U16/U32/U64/I32` 使用固定长度 network byte order。
- `BOOL` 长度为 1，值只能为 0/1。
- `UTF8` 必须有效且不含 NUL；`BYTES` 保持 opaque。
- `NESTED` 是另一个 canonical LTV sequence。
- packed integer list 使用对应整数宽度连续编码。

canonical 约束由 TFMP schema adapter 在 `ltv_parse()` 结果上校验：

- field ID 必须递增；只有 schema 标记为 repeated 的字段可相邻重复。
- 未知 critical field 返回 `UNSUPPORTED_CAPABILITY`；未知 optional field 跳过。
- varint 必须使用最短编码，numeric/BOOL 必须具有精确长度。
- nesting 最大 4 层；body、field 数量与 repeated 数量均受 capability 中的上限约束。
- 幂等比较使用 canonical command body 的完整 bytes，不用 hash 碰撞代表相等。

`turbo_flow_tfmp_envelope_validate_schema()` 使用单一内建 registry 校验所有 RPC
kind、公共 response identity、nested target/resource/condition/event 和 command payload。
未知 optional 字段被跳过；未知 critical 字段或 command type 返回 `TURBO_ENOTSUP`，由
management adapter 映射为稳定的 `UNSUPPORTED_CAPABILITY` status。只有本文 registry
定义为 optional 的未知字段才能跳过；schema validator 不持有
body，也不把 resource/operation 状态带入 codec。

FMQ DATA 已经提供一条完整 payload，TFMP 使用 `turbo_ltv_peek_size()` zero-copy 切分字段，
使用 `turbo_ltv_build()` 编码，不再套 stream parser。TurboUtils 底层 LTV 已补齐
non-canonical/overflow build 检查与对应测试；TFMP 不安装或依赖独立 `LtvParser` target，也不在
FMQ 内复制一份 parser。

初始默认限制为 request/reply 各 64 KiB、每 body 64 fields、每页 60 items；一页预留
`catalog_generation`、`next_after_uid` 和两项公共 response identity，因此 repeated item 不能占满
64 fields。限制必须在实现中以命名常量和 YAML 上限表达，server 可通过 capability 宣告更小值。

### 4.2 Message kinds

响应通常使用相同 kind 加 `RESPONSE` flag。唯一例外是无法可信恢复请求 kind 的 malformed
frame，此时使用 response-only `PROTOCOL_ERROR`；客户端不得发送该 kind。

| ID | Kind | 请求/响应要点 |
| ---: | --- | --- |
| `0x0001` | `CAPABILITIES_GET` | 无请求 body；返回身份、版本、能力、limits、durability |
| `0x0002` | `HEALTH_GET` | 返回 management owner 状态与 uptime，不替代 resource status |
| `0x0010` | `TARGET_LIST` | 按 UID 排序、generation cursor 分页 |
| `0x0011` | `TARGET_GET` | 返回一个 target snapshot |
| `0x0020` | `RESOURCE_LIST` | 以 target UID 为范围，generation cursor 分页 |
| `0x0021` | `RESOURCE_GET` | 返回 metadata、generation、observed generation 与可选 conditions |
| `0x0022` | `RESOURCE_DOCUMENT_GET` | 返回 versioned pointer-free resource document |
| `0x0030` | `COMMAND_SUBMIT` | 条件写入、幂等与 operation acceptance |
| `0x0040` | `OPERATION_GET` | 返回 operation state、revision、result 或错误 |
| `0x0041` | `OPERATION_CANCEL` | capability-gated 条件取消 |
| `0x0050` | `EVENTS_GET` | capability-gated journal replay；没有 store 时返回 unsupported |
| `0x7fff` | `PROTOCOL_ERROR` | 仅用于无法恢复原请求 kind 的终态错误响应 |
| `0x8001` | `EVENT` | 只允许在 event PUB/SUB channel 使用 |

list 请求携带 `page_limit`、可选 `after_uid` 与 `catalog_generation`。server 按 UID 排序；
若 catalog generation 在翻页中改变，返回 `STALE_CURSOR`，客户端从第一页重试。该算法为
`O(n log n)` 建快照或 `O(log n + page)` 有序索引查询，且不要求 per-client 临时存储。

### 4.3 Body field registry

下表是 v1 的数值 registry。`R` 字段必须存在并将 field ID 的 `CRITICAL` bit 置 1；`O`
字段可省略。未列出的 field ID 保留。所有 RPC response 末尾都必须包含公共字段
`120 authority_id:UTF8 R` 与 `121 incarnation_id:BYTES(16) R`，并可包含
`122 diagnostic:UTF8 O`、`123 native_status:I32 O`、`124 retry_after_ms:U64 O`。
客户端控制流不得依赖后三者。

| Kind | Field ID | Name | Type | Req. |
| --- | ---: | --- | --- | --- |
| `CAPABILITIES` response | 1 | `minor_min` | U16 | R |
|  | 2 | `minor_max` | U16 | R |
|  | 3 | `capability_ids` | PACKED_U16 | R |
|  | 4 | `command_descriptor` | NESTED, repeated | O |
|  | 5 | `max_request_bytes` | U32 | R |
|  | 6 | `max_reply_bytes` | U32 | R |
|  | 7 | `max_body_fields` | U16 | R |
|  | 8 | `max_nesting` | U16 | R |
|  | 9 | `max_page_items` | U32 | R |
|  | 10 | `operation_durability` | U16 | R |
|  | 11 | `max_event_bytes` | U32 | O |
| `HEALTH` response | 1 | `owner_state` | U16 | R |
|  | 2 | `uptime_ms` | U64 | R |
|  | 3 | `catalog_generation` | U64 | R |
| `TARGET_LIST` request | 1 | `page_limit` | U32 | R |
|  | 2 | `after_uid` | UTF8 | O |
|  | 3 | `catalog_generation` | U64 | O |
| `TARGET_LIST` response | 1 | `catalog_generation` | U64 | R |
|  | 2 | `target` | NESTED, repeated | O |
|  | 3 | `next_after_uid` | UTF8 | O |
| `TARGET_GET` request | 1 | `target_uid` | UTF8 | R |
| `TARGET_GET` response | 1 | `target` | NESTED | R |
| `RESOURCE_LIST` request | 1 | `target_uid` | UTF8 | R |
|  | 2 | `page_limit` | U32 | R |
|  | 3 | `after_uid` | UTF8 | O |
|  | 4 | `catalog_generation` | U64 | O |
| `RESOURCE_LIST` response | 1 | `catalog_generation` | U64 | R |
|  | 2 | `resource` | NESTED, repeated | O |
|  | 3 | `next_after_uid` | UTF8 | O |
| `RESOURCE_GET` request | 1 | `target_uid` | UTF8 | R |
|  | 2 | `resource_uid` | UTF8 | R |
| `RESOURCE_GET` response | 1 | `resource` | NESTED | R |
| `RESOURCE_DOCUMENT_GET` request | 1 | `target_uid` | UTF8 | R |
|  | 2 | `resource_uid` | UTF8 | R |
| `RESOURCE_DOCUMENT_GET` response | 1 | `resource` | NESTED | R |
|  | 2 | `schema_id` | U16 | R |
|  | 3 | `schema_version` | U16 | R |
|  | 4 | `document` | BYTES | R |
| `COMMAND_SUBMIT` request | 1 | `client_id` | UTF8 | R |
|  | 2 | `idempotency_key` | UTF8 | R |
|  | 3 | `target_uid` | UTF8 | R |
|  | 4 | `command_type` | U16 | R |
|  | 5 | `reply_mode` | U16 | R |
|  | 6 | `required_durability` | U16 | R |
|  | 7 | `expected_generation` | U64 | O |
|  | 8 | `queue_timeout_ms` | U64 | R |
|  | 9 | `operation_timeout_ms` | U64 | R |
|  | 10 | `command_payload` | NESTED | O |
| `COMMAND_SUBMIT` response | 1 | `generation_before` | U64 | O |
|  | 2 | `generation_after` | U64 | O |
|  | 3 | `observed_generation` | U64 | O |
|  | 4 | `operation_id` | BYTES(16) | O |
|  | 5 | `operation_revision` | U64 | O |
| `OPERATION_GET` request | 1 | `operation_id` | BYTES(16) | R |
| `OPERATION_CANCEL` request | 1 | `client_id` | UTF8 | R |
|  | 2 | `idempotency_key` | UTF8 | R |
|  | 3 | `operation_id` | BYTES(16) | R |
|  | 4 | `expected_revision` | U64 | O |
| `OPERATION` response | 1 | `operation_id` | BYTES(16) | R |
|  | 2 | `operation_state` | U16 | R |
|  | 3 | `operation_revision` | U64 | R |
|  | 4 | `target_uid` | UTF8 | R |
|  | 5 | `submitted_unix_ms` | U64 | R |
|  | 6 | `updated_unix_ms` | U64 | R |
|  | 7 | `generation_before` | U64 | O |
|  | 8 | `generation_after` | U64 | O |
|  | 9 | `observed_generation` | U64 | O |
|  | 10 | `terminal_status` | U16 | O |
| `EVENTS_GET` request | 1 | `incarnation_id` | BYTES(16) | R |
|  | 2 | `after_sequence` | U64 | R |
|  | 3 | `page_limit` | U32 | R |
| `EVENTS_GET` response | 1 | `event` | NESTED, repeated | O |
|  | 2 | `last_sequence` | U64 | R |

`target` nested schema 固定为 `1 uid:UTF8 R, 2 kind:U16 R, 3 generation:U64 R,
4 observed_generation:U64 R, 5 state:U16 R`。`resource` nested schema在此基础上增加
`6 owner_uid:UTF8 R, 7 condition:NESTED repeated O`。condition 为
`1 type:U16 R, 2 status:BOOL R, 3 reason:U16 R, 4 revision:U64 R, 5 message:UTF8 O`。

target kind 固定为 `1 FLOW`；target state 固定为 `1 NEW, 2 PARSED, 3 COMPILED,
4 STARTED, 5 STOPPED, 6 FAILED`。resource kind 固定为 `1 CONNECTION, 2 QUEUE_BUFFER,
3 POOL, 4 RUNTIME, 5 SEGMENT, 6 PROTOCOL_AGGREGATE, 7 STORAGE, 8 RULE_SET,
9 SECURITY_REALM`，不得直接编码
进程内 `turbo_flow_resource_kind_t` 数值。resource state 固定为 `1 OBSERVED,
2 RECONCILING`，由 `observed_generation == generation` 判定；通用 metadata API 没有 condition
快照时省略 field 7，不伪造 condition。

`command_descriptor` 为 `1 command_type:U16 R, 2 reply_mode_mask:U16 R,
3 durability_mask:U16 R, 4 payload_schema_id:U16 R, 5 payload_schema_version:U16 R`。
初始 command type：`1 FLOW_PAUSE`、`2 FLOW_RESUME`、`3 FLOW_DRAIN`、
`0x0100 RESOURCE_QUIESCE`、`0x0101 RESOURCE_RESUME`、`0x0102 ENDPOINT_REPLACE`、
`0x0103 POOL_RESIZE`。前五种无 payload；endpoint payload 为
`1 host:UTF8 O, 2 path:UTF8 O, 3 port:U16 O`，且 schema 校验必须确保 transport 所需字段齐全；
pool payload 为 `1 pool_kind:U16 R, 2 parallelism:U32 R`。payload schema registry 固定为
`0 NONE, 1 ENDPOINT/v1, 2 POOL/v1`；pool kind 固定为 `1 THREAD, 2 CORO, 3 DISRUPTOR`，不得直接
编码进程内 `turbo_flow_pool_kind_t` 数值。

网络 `reply_mode` 固定为 `2 ACCEPT_OPERATION`；值 `1 WAIT_TERMINAL` 只供同进程
`turbo_flow_tfmp_management_service_execute()` 调用，不由 ROUTER stage 接受或 capability 宣告。
durability 为 `1 VOLATILE, 2 DURABLE`，capability/descriptor 中使用 bitmask `0x1/0x2`；owner state 为
`1 STARTING, 2 READY, 3 DRAINING, 4 STOPPED, 5 FAILED`。`OPERATION_GET` 成功只表示查询成功；
operation 自身失败由 body 的 `terminal_status` 表示，不能把它复制成 RPC header status。

## 5. 稳定 status、disposition 与两层 ACK

TFMP client 不得把平台相关 `TURBO_*` 值当作协议契约。adapter 在边界把内部错误映射为：

| Code | Status | 典型含义 |
| ---: | --- | --- |
| 0 | `OK` | 请求成功 |
| 1 | `INVALID_ARGUMENT` | 字段、范围或状态组合非法 |
| 2 | `UNSUPPORTED_VERSION` | major/minor 不受支持 |
| 3 | `UNSUPPORTED_CAPABILITY` | kind、command 或 critical 字段不受支持 |
| 4 | `NOT_FOUND` | target/resource/operation 不存在 |
| 5 | `CONFLICT` | generation 或 idempotency bytes 冲突 |
| 6 | `BUSY` | owner 正忙，可用同 key 有界重试 |
| 7 | `DEADLINE_EXCEEDED` | 在 mutation 开始前过期 |
| 8 | `RESOURCE_EXHAUSTED` | mailbox、dedup、page 或 body 上限耗尽 |
| 9 | `UNAVAILABLE` | owner/store 暂时不可用 |
| 10 | `INTERNAL` | 已验证请求触发内部不变量失败 |
| 11 | `STALE_CURSOR` | catalog/event cursor 已失效 |
| 12 | `CANCELED` | operation 确认取消 |
| 13 | `FAILED_PRECONDITION` | target 状态不允许该命令 |

响应可携带 diagnostic 与 `native_status`，但它们只用于诊断，不参与客户端控制流。

`disposition` 固定为：

| Value | 名称 | ACK 语义 |
| ---: | --- | --- |
| 0 | `NONE` | malformed 或未受理 |
| 1 | `COMPLETED` | query 或 mutation 已产生终态结果 |
| 2 | `ACCEPTED_VOLATILE` | operation 已复制进 owner queue，进程重启可丢失 |
| 3 | `ACCEPTED_DURABLE` | dedup 与 operation 已在同一事务提交，返回 operation ID |
| 4 | `FAILED` | 请求得到终态失败，不会在后台偷偷开始新 mutation |

因此 ACK 分两层：FMQ ROUTER 发送 correlated response；TFMP 的
`status + disposition` 说明业务是否完成、易失受理或持久受理。仅“写入 queue 成功”不能伪装
为 durable ACK；memory queue 只能返回 `ACCEPTED_VOLATILE`，SQLite/Redis/PG 只有在原子提交
成功后才能返回 `ACCEPTED_DURABLE`。

## 6. Command 与 operation

### 6.1 COMMAND_SUBMIT

mutation 请求必须包含：

- `client_id`、`idempotency_key`、`target_uid`、`command_type`
- `reply_mode = ACCEPT_OPERATION`
- `required_durability = VOLATILE | DURABLE`
- 可选 `expected_generation`
- `queue_timeout_ms`：相对 server 接收时间，在开始 mutation 前过期
- `operation_timeout_ms`：operation 自身的执行期限
- command-specific nested payload

不传输 client 的 absolute monotonic timestamp；server 在收到请求后用本机
`turbo_hrtime()` 转换相对 timeout。客户端在 acceptance 前断开时结果未知，只能以相同
`(authority_id, client_id, idempotency_key)` 重试或查询，不能生成新 key 猜测执行结果。

初始 command family 与现有 typed owner command 对齐：

- flow：pause、resume、drain
- resource：quiesce、resume、replace endpoint、resize pool
- workflow：需要多步 reconcile 的 resize/replace，使用 operation

command handler 只接收 versioned、pointer-free command。协议 adapter 不直接操作 socket、flow
内部结构或数据库。

单 Flow owner 声明并接受三种 Flow command 与四种 resource command。网络请求只接受
`reply_mode=ACCEPT_OPERATION`；未绑定 store 时只接受 `VOLATILE`，绑定
原子 blob store 后也接受 `DURABLE`。Flow command 的 `target_uid` 指向
绑定 Flow；resource command 的 `target_uid` 指向该 Flow 所有的稳定 resource UID，且必须携带
非零 `expected_generation`。它在
volatile operation 先把 canonical bytes 复制进有界 owner 并返回 `ACCEPTED_VOLATILE`；
durable operation 则先把 canonical
bytes、dedup identity 与 operation 状态原子提交，再返回 `ACCEPTED_DURABLE`。只有宿主随后串行调用
`turbo_flow_tfmp_management_service_run_one()` 才会 claim 并开始 mutation。`queue_timeout_ms` 在
claim 前检查，`operation_timeout_ms` 是 Flow drain 或 resource owner 的相对 deadline，其中 `0`
表示 nonblocking deadline，`UINT64_MAX` 表示无期限。宿主必须像其他 Flow
lifecycle/configuration command 一样串行调用该 owner。store 不可用或提交失败返回 `UNAVAILABLE`，
不得把 memory acceptance 伪装为 durable ACK，也不得降级执行。

### 6.2 幂等与容量

幂等作用域是 `(authority_id, client_id, idempotency_key)`：

1. 首次请求记录 canonical command bytes。
2. 完全相同的请求返回原始 disposition/result，并设置 `REPLAYED`。
3. key 相同而 command bytes 不同，返回 `CONFLICT`。
4. active record 永不淘汰；terminal record 在 `dedup_ttl_ms` 后过期。
5. 满容量且没有可过期 terminal record 时返回 `RESOURCE_EXHAUSTED`，不得执行后再丢记录。
6. durable operation 的 dedup record 与 operation record 必须同事务提交；做不到原子性就返回
   `UNAVAILABLE`，不得发送 durable ACK。

### 6.3 Operation 状态机

```text
ACCEPTED -> RUNNING -> SUCCEEDED
                    -> FAILED
                    -> CANCEL_REQUESTED -> CANCELED
ACCEPTED ---------------------------> CANCELED
```

每个 operation 保存 `operation_id`、target、command bytes、state、revision、submitted/updated
time、generation before/after、observed generation 与 terminal diagnostic。状态迁移使用单调
revision 做条件写入。

operation state 数值固定为 `1 ACCEPTED, 2 RUNNING, 3 SUCCEEDED, 4 FAILED,
5 CANCEL_REQUESTED, 6 CANCELED`。terminal state 是 3、4、6，之后不得回到 active state。

crash recovery 规则：

- `ACCEPTED` 可重新 claim。
- `RUNNING`/`CANCEL_REQUESTED` 表示副作用结果不确定；重启时保持 recovery-required，owner 在完成
  typed reconcile 前不能进入 READY，也不盲目重复 mutation。
- FLOW_PAUSE/RESUME/DRAIN 与 POOL_RESIZE 使用内建 typed inspector；RESOURCE_QUIESCE/RESUME 和
  ENDPOINT_REPLACE 只有在 resource owner 注入 inspector 后才在 command descriptor 中声明 durable。
- inspector 返回 APPLIED 时不重放，NOT_APPLIED 只在原 generation 仍成立时以 operation UUID 重试
  goal-state command，CONFLICT 只提交失败而不覆盖新状态。不可核验的副作用不能声明 durable。
- cancel 只在步骤尚未开始，或 owner 明确提供取消/补偿契约时成功。

### 6.4 Durable snapshot `TFMS/1.0` 与 `TFMS/1.1`

SQLite/Redis blob store 保存完整 owner snapshot，并以一次 atomic replace 提交；它不是追加日志。
top-level 使用 canonical TurboUtils LTV：

| Type | Value | 约束 |
| ---: | --- | --- |
| 1 | 8-byte header | `"TFMS" + major:u16(1) + minor:u16(0|1)` |
| 2 | authority UTF-8 | 必须与启动配置完全相同 |
| 3 | nested operation record | 可重复，最多 `dedup_capacity` 条 |
| 4 | 24-byte event metadata | 仅 1.1；incarnation UUID bytes + sequence:u64 |
| 5 | event record | 仅 1.1；可重复，最多 `event_capacity` 条 |

每个 type 3 record 的 nested LTV 按以下顺序编码：

| Type | Value | 约束 |
| ---: | --- | --- |
| 1 | client ID UTF-8 | 必填、有界、非空 |
| 2 | idempotency key UTF-8 | 必填、有界、非空 |
| 3 | canonical COMMAND_SUBMIT body | 必须是 `ACCEPT_OPERATION + DURABLE` |
| 4 | fixed 94-byte metadata | 必填，network byte order |
| 5 | cancel client ID | 与 6/7 同时出现 |
| 6 | cancel idempotency key | 与 5/7 同时出现 |
| 7 | canonical OPERATION_CANCEL body | 与 5/6 同时出现 |

metadata v1 布局：

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 2 | flags：bit 0 generation result，bit 1 operation；其余为零 |
| 2 | 2 | command status |
| 4 | 4 | signed native status 的二进制 u32 表示 |
| 8 | 24 | generation before/after/observed，各 u64 |
| 32 | 16 | operation UUID bytes |
| 48 | 2 | operation state |
| 50 | 8 | operation revision |
| 58 | 8 | submitted Unix ms |
| 66 | 8 | updated Unix ms |
| 74 | 8 | queue deadline Unix ms；`UINT64_MAX` 表示无限期 |
| 82 | 2 | terminal status |
| 84 | 2 | acceptance disposition；必须为 `ACCEPTED_DURABLE` |
| 86 | 8 | terminal Unix ms；非终态为零 |

每个 type 5 record 固定以 `sequence:u64 + category:u16` 开头，后接 canonical EVENT body。
record 必须按 sequence 严格递增，最后一条必须等于 type 4 的 sequence；空 journal 的 sequence
必须为零。EVENT body 的 incarnation/category 必须与 metadata 和 record header 一致。

loader 必须拒绝未知版本、authority 不匹配、非 canonical/越界 LTV、字段组合错误、无效状态和
超过 provider 上限的 snapshot；损坏或 provider 不可用直接使 owner 创建失败，不建立空 memory
owner。启用 durable event replay 时，1.0 snapshot 在 owner 返回前以一次 atomic replace 升级为
1.1，保留 operation 且以空 event journal 开始；未启用 durable replay 的 owner 拒绝 1.1，避免
静默丢失 journal。增加 minor 前必须先发布能够严格读取它的新 decoder。

## 7. Live event 与 replay

event 使用独立 FMQ PUB/SUB adapter，topic 为：

```text
tfmp/1/event/<category>
```

`category` 初始为 `lifecycle`、`resource`、`operation`、`fault`。authority、incarnation、target
在 body 中，不把用户输入直接拼入 topic。SUB 可按现有 FMQ prefix 订阅。

EVENT 使用同一 header，设置 `EVENT` flag，`correlation ID` 为当前 incarnation 内单调 sequence；
body 包含 authority/incarnation、event type、target UID、generation、operation ID（如适用）与
versioned payload。只保证同一 publisher incarnation 的发送顺序，不保证跨 authority 全局顺序。

EVENT body registry 为：`1 authority_id:UTF8 R, 2 incarnation_id:BYTES(16) R,
3 category:U16 R, 4 event_type:U16 R, 5 target_uid:UTF8 O, 6 generation:U64 O,
7 operation_id:BYTES(16) O, 8 operation_revision:U64 O, 9 emitted_unix_ms:U64 R,
10 payload_schema_id:U16 R, 11 payload_schema_version:U16 R, 12 payload:BYTES O`。
category 为 `1 LIFECYCLE, 2 RESOURCE, 3 OPERATION, 4 FAULT`；event type 是由
`(category, payload_schema_id, payload_schema_version)` 限定的 u16 registry。EVENT header 的
`status` 与 `disposition` 必须为零，`request_id` 语义不适用。

PUB fan-out、HWM drop、没有订阅者或连接断开都不能回滚已经提交的 owner 状态。event 是提示：

- 新订阅者先通过 RPC 取 snapshot，再开始应用大于 snapshot barrier 的 event；durable 模式把
  incarnation 与最新 sequence 作为 snapshot barrier metadata 持久化。
- sequence gap 或 incarnation 改变时丢弃派生视图并重新取 snapshot。
- 没有 `event_replay` capability 时，`EVENTS_GET` 返回 unsupported。
- 有 replay store 时，durable operation transition 与其 event 进入同一个 blob snapshot commit；
  live PUB 从 journal 派生，发送失败可重试但不会改变事实源。同步 resource/flow 副作用不与外部
  store 构成分布式事务，失败语义见第 13 节。

## 8. Runtime ownership 与线程模型

```text
stable DEALER clients
  bounded inflight table
        |
        v
FMQ ROUTER stage
  decode/validate
       |
       v
typed management command owner ---- operation store
       |
       +---- resource/flow owner command
       |
       +---- derived event/outbox ---- FMQ PUB
       |
       v
accept/query result -> encode correlated response
```

- ROUTER stage 是薄 adapter，只拥有 request/response bytes 与当前 route，不拥有 target 状态。
- management command owner 显式注入 target registry、operation store 与 event sink；不使用
  singleton 或 service locator。
- owner mailbox 有界，命令按值复制，并在指定 CoroNet owner lane 执行。现有
  `tf_coronet_actor_t` 可复用 placement、deadline、close/drain 语义，但其 `int status` reply
  不足以表达 typed result；接入异步 operation 时需要 typed result handle，不能借用 caller stack。
- owner 通过 CoroNet/FMQ ROUTER graph 的薄 stage 接收请求；operation execution 仍由宿主在
  同一 owner lane 显式调用 `run_one()`。任何替代 scheduler 都必须保留同一 dedup 事实源、单写者
  和 bounded mailbox，不能在 ROUTER stage 与 scheduler 各维护一份状态。
- 一个 target 的 mutation 串行化；不同 target 可分 lane。跨 target workflow 没有隐式事务，
  必须有显式 orchestrator、补偿与 operation 状态。
- storage adapter 是注入的薄接口。memory、SQLite、Redis、PG 共享同一 operation/dedup 状态模型，
  backend 类型不得进入 protocol command。

## 9. YAML 配置模型

配置源必须是 YAML，经现有 resolver 形成 immutable JSON projection 后再由 strict parser 校验。
transport 仍放在 `kind: fmq` adapter；TFMP policy 放在独立 `kind: fmq_management` channel。

该 channel 要求专用且已解析的 `pattern: router` FMQ adapter，拒绝 raw UDP、未知字段、
非 1/0 协议版本、超过 dedup 容量的 mailbox，以及大于 mailbox capacity 的 per-target inflight。
`operation_store: memory` 明确选择 volatile owner；其他值必须引用已解析的 `kind: blob_store`
channel。宿主再用 SQLite/Redis provider 的 `*_blob_store_create_resolved()` 创建 store，并通过
`turbo_flow_tfmp_management_service_create_configured()` 注入；引用缺失、backend 错误、store 不可用
均启动失败且不降级。`event_replay_store: memory` 只提供当前 incarnation 的有界 replay；外部
值必须引用同一个 `operation_store`，以保证 operation/event 单 snapshot outbox。拆分两个 store
会在启动时失败。
`flowmq/examples/fmq.yml` 给出 SQLite 装配配置。

YAML schema：

| YAML path | 必填 | 约束 |
| --- | --- | --- |
| `protocol_major` | 是 | 必须为 1 |
| `protocol_minor` | 是 | server 支持范围内 |
| `authority_id` | 是 | 非空、长度有界、配置域内唯一 |
| `rpc_adapter` | 是 | 指向专用 FMQ ROUTER adapter |
| `event_adapter` | 否 | 指向 FMQ PUB adapter；启用 live_event 时必填 |
| `mailbox_capacity` | 是 | 正整数、有编译期上限 |
| `max_request_bytes` / `max_reply_bytes` | 是 | 不超过 protocol 上限 |
| `max_inflight_per_target` | 是 | 正整数，且不超过 `mailbox_capacity` |
| `dedup_capacity` / `dedup_ttl_ms` | 是 | 正整数；active 不淘汰 |
| `operation_store` | 否 | `memory` 或 SQLite/Redis `kind: blob_store` channel reference |
| `event_replay_store` | 否 | `memory` 或与 `operation_store` 相同的 `blob_store` reference |
| `shutdown_timeout_ms` | 是 | close -> drain 的有界期限 |

未知字段、未知 adapter reference、raw UDP RPC、durable capability 无 store、event capability 无
publisher 都必须在启动时 fail fast。命令行和环境变量若覆盖这些值，仍需走同一 schema 校验。

## 10. 当前协议边界

TFMP/1 使用 `TFMP` magic、DEALER/ROUTER RPC 与独立 event PUB/SUB。实现只接受本文定义的
frame、schema 和 YAML 组合，不提供旧 frame、REQ/REP management adapter、兼容 wrapper、
version negotiation 或自动 transport fallback。

## 11. 候选方案与取舍

| 方案 | 优点 | 缺点 | 结论 |
| --- | --- | --- | --- |
| 固定 command/reply struct | header 简单 | capability、分页、operation/event 会导致固定 header 膨胀，minor 演进困难 | 不选 |
| JSON-only RPC | 可读、低频管理足够快 | canonical idempotency、整数精度、schema/size 约束更依赖 parser | 不选作 wire；YAML 仍是配置格式 |
| TurboUtils `TLVParser` frame | 已有 CRC、stream parser | 它是固定 `0xAA ... CRC ... 0x55` little-endian frame，不是通用 field TLV；与 FMQ frame 重叠 | 不用于 TFMP |
| 固定 header + `TurboUtils::Parser` canonical LTV | zero-copy framing/build、跨语言、可跳过 optional 字段 | 1-byte type 限制 field ID；schema 仍需 typed validation | 选择 |
| TurboUtils `SoaParser` | 固定宽度批量列式数据高效 | 依赖全局 schema registry、little-endian、缺少稀疏异构管理字段演进 | 不用于 TFMP RPC；metrics/event 若采用它必须建立独立 schema contract |
| DEALER/ROUTER + 有界 inflight | 请求可流水化，响应可乱序关联 | 客户端必须维护 correlation 与 unknown-outcome 语义 | 选择 |
| REQ/REP management | 客户端状态简单 | 长 operation 造成 head-of-line blocking，无法流水化 | 不选 |
| event 与 RPC 共用 ROUTER | endpoint 少 | 主动通知与 request correlation 混合，重连语义复杂 | 使用独立 PUB/SUB |
| event 自身作为事实源 | 实现看似简单 | HWM/drop/reconnect 会造成不可恢复状态分叉 | event 只做派生提示 |

性能取舍：management 是低频控制路径，canonical LTV 与 owner serialization 优先保证一致性。
只有 profiling 证明 codec 或 UID 排序占总耗时至少 20% 后，才考虑专用索引、arena 或 SIMD。

## 12. 实现准入与验证

以下测试全部通过前，不得宣称 TFMP/1 ready：

1. TurboUtils LTV conformance 与 TFMP golden vectors：最短 varint、大小端、每个 kind、unknown
   optional/critical、reserved、trailing、深度与所有 size limit；decoder fuzz/截断输入不越界。
2. version/capability：major 拒绝、minor additive、无静默 fallback、capability/command 组合矩阵。
3. DEALER/ROUTER：多 inflight、乱序 correlation、同步 mutation 拒绝、malformed/owner error
   response、断连 unknown outcome 与同 idempotency key 重试。
4. owner：同 target 串行、跨 target 隔离、mailbox 满、queued deadline、close/drain、started
   mutation 不被 caller timeout 伪取消。
5. idempotency：exact replay、bytes conflict、TTL、active 不淘汰、capacity exhaustion、客户端重连。
6. operation：每条合法/非法状态迁移、revision CAS、cancel、SQLite/Redis crash injection、重启恢复、
   Redis 断连不降级 memory。PG 接入时运行同一 contract suite。
7. event：prefix fan-out、无订阅者、HWM gap、incarnation change、snapshot barrier、journal replay。
8. transport：Pipe 最小回归，TCP 与 WebSocket 端到端；TLS/WSS/KCP 使用现有 transport contract
   suite。raw UDP 必须被配置拒绝。
9. current-only：REQ/REP management adapter、旧 frame、兼容 wrapper 和 transport fallback
   均被配置或 decoder 拒绝。

## 13. 已知风险

- **HIGH — 事实：**只有 `ACCEPT_OPERATION + DURABLE` 的 dedup/operation record 进入注入的
  原子 blob store。调用方不能把
  `memory` operation 的 acceptance response 解释成 durable ACK。
- **HIGH — 事实：**target mutation 与 blob store 不是同一事务。实现先提交 `RUNNING` 再执行副作用，
  终态提交失败会使 owner 进入 FAILED；重启后的残留 RUNNING 保持 recovery-required，必须由
  typed inspector 判定 APPLIED、NOT_APPLIED 或 CONFLICT，不能盲目重放。
- **MED — 事实：**客户端必须为每个 target 维护有界 correlation table；route 断开时未收到
  acceptance 的 mutation 是 unknown outcome，只能按相同 idempotency identity 对账。
- **MED — 推论：**同步 external store transaction 会阻塞 management owner lane。必须记录
  SQLite/Redis 管理负载的 P50/P95/P99；达到每秒 1000 次或占 owner lane 20% 以上时，
  再依据 profiling 拆分 storage execution placement。
- **MED — 事实：**TurboUtils 源码已补 canonical/overflow、无 partial build 与 stream view 生命周期
  测试；TFMP codec 通过已安装 `TurboUtils::Parser` 的公开 `turbo_ltv_*` API 接入，并在 schema
  边界再次校验最短 varint、字段顺序和数量，不链接私有 parser target。
- **LOW — 常用做法：**LTV 比固定 struct 增加 schema 维护成本。通过单一 field registry、golden
  vector 与严格 canonical encoder 控制，不为每个 backend 建独立 wire schema。
