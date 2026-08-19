# FlowMQ Credit Worker Protocol v1

本文件是 [协议索引](PROTOCOL_SPEC.md) 指定的 TFCW/1、TFBR/1 与 TFCS/1.0 唯一正文。
它只定义 credit-worker 应用与 durable claim 格式，不重复 FMQ/3 framing；wire 细节见
[FMQ_WIRE_PROTOCOL.md](FMQ_WIRE_PROTOCOL.md)。

`credit_worker + at_least_once` 必须通过显式 storage binding 创建；backend 不可用时
fail fast，不得隐式 fallback 到本地内存。本文不改变 FMQ/3 wire。

## 1. 决策背景

基础 ready-worker broker 把 `READY -> dispatch -> completion` 表达为一个隐式 message credit：
每个 worker 只有 `idle/busy` 两态，同一时刻最多一个 in-flight。它适合请求/回复和单任务 worker，
但不能为需要 pipeline 的 bulk worker 表达 message/byte 两个容量维度。

FMQ adapter HWM 只限制本进程排队内存；transport send success 只表示一次写入完成。两者都不知道
远端 worker 的可用 buffer，不能解释为 credit、accept ACK 或 delivery ACK。

TFCW/1 因此定义为配置驱动的高级应用协议，而不是 FMQ socket pattern：

- 使用普通 FMQ ROUTER/DEALER 连接和 CoroNet transport；
- credit、job correlation 与 worker lease 由单一 pattern owner 管理；
- payload 持久化继续由 Redis Stream 或显式 durable owner 管理；
- 无 credit 时非阻塞返回，不在 pattern 内建立隐藏临时队列；
- 不改变 FMQ/3 frame 或基础 pattern 状态机。

## 2. 候选方案

| 方案 | 结论 | 原因 |
| --- | --- | --- |
| 在 FMQ v3 增加 CREDIT control frame | 不选 | 改变冻结 wire v3，并把应用容量语义塞进 transport/pattern primitive |
| 在 PUSH/PULL 内自动缓存和重试 | 不选 | 产生第二持久化事实源，且 PUSH 无法区分远端接收、处理与落盘 |
| 把 HWM 当 credit | 不选 | HWM 属于本地内存 admission，不能代表远端容量或重连 generation |
| 每个并发槽创建一个 worker identity | 不选 | 连接数随并发度增长，不能表达 byte credit 或动态容量 |
| ROUTER/DEALER 上运行独立 Credit Worker 应用协议 | 选择 | 复用 live route、异步 reply 和 graph storage composition，不修改 FMQ v3 |

## 3. 拓扑与 owner

```text
client/source -> Redis Stream or durable fact source -> credit_worker owner -> FMQ ROUTER
                                                           |             |
                                                           |       FMQ DEALER worker
                                                           |             |
                                                           +--- completion/lease/credit
```

| 状态 | 唯一 owner | 持久化 |
| --- | --- | --- |
| payload、pending/in-flight delivery record | Redis Stream/durable owner | 由 backend contract 决定 |
| worker identity、live route、lease | credit_worker owner | 否；重连后重建 |
| available message/byte tokens、credit sequence | credit_worker owner | 否；session-scoped |
| request -> worker correlation | credit_worker owner | metadata only；payload 不复制进 broker |
| accept ACK | storage owner | durable backend commit 后才成立 |
| worker completion ACK | worker protocol + storage settlement bridge | completion 成功后才 ack/delete claim |
| retry/outbox logical state | claim storage owner | TFCS/1.0 LTV；与 claim disposition 同源提交 |

credit owner 的所有 mutation 必须在一个 host-serialized/CoroNet owner lane 执行。Observe、event
callback 和 graph processor 只能读取 snapshot 或提交 typed command，不能直接修改 credit。

## 4. TFCW/1 application envelope

TFCW/1 是普通 FMQ DATA payload，不是新的 FMQ frame kind。整数使用 network byte order，固定
40-byte header 后接 canonical TurboUtils LTV body：

```text
offset  size  field
0       4     magic "TFCW"
4       1     major = 1
5       1     minor = 0
6       2     kind
8       2     flags
10      2     reserved = 0
12      4     LTV body bytes
16      8     request ID; control messages use 0
24      8     credit sequence; non-credit messages use 0
32      8     sender monotonic timestamp hint; never used as an ordering fact source
```

初始 kind：

| Kind | 方向 | 用途 |
| --- | --- | --- |
| `READY` | worker -> broker | 注册 worker/service/live route，并提交本 session 第一笔 credit grant |
| `CREDIT` | worker -> broker | 增加已释放的 message/byte token |
| `HEARTBEAT` | worker -> broker | 刷新 lease，不隐式增加 credit |
| `JOB` | broker -> worker | request ID、logical address、metadata 与 payload |
| `COMPLETE` | worker -> broker | worker terminal success；触发 storage ACK |
| `FAIL` | worker -> broker | typed terminal/retryable failure；触发 drop/requeue policy |

READY/CREDIT body 必须包含 `worker_id`、`service`、`grant_messages`、`grant_bytes`。JOB body 使用
已有 TFBR logical address 表达跨 broker return identity，并携带 bounded metadata/payload。decoder
必须校验 canonical LTV、字段顺序/重复、header/body hard limit 和 kind-specific schema；未知 critical
字段、reserved bit 或 trailing bytes 返回 `TURBO_EPROTO`。

## 5. Credit 算法

message 与 byte credit 同时必填，不能只选一个维度：

```text
admissible(job) = available_messages >= 1
               && available_bytes >= encoded_job_bytes

dispatch(job):
  available_messages -= 1
  available_bytes    -= encoded_job_bytes
```

worker 发送的是增量 grant，不是“当前空闲量”快照。worker 每释放一个实际 buffer/job slot 后才
产生对应 token；broker completion 不自动返还 credit。这样 full-duplex 网络中晚到的 capacity
snapshot 不会覆盖 broker 已消费的 token。

每个 live FMQ route generation 独立维护：

- READY 的 `credit_sequence` 必须为 1；新 route 从零 credit 开始。
- 后续 CREDIT 必须为 `last_sequence + 1`，否则 sequence gap 返回 `TURBO_EPROTO` 并隔离该 session。
- `sequence == last_sequence` 且 grant bytes 完全相同为幂等重放；同 sequence 不同内容为冲突。
- 更旧 sequence 返回 `TURBO_EALREADY`，不改变状态。
- grant 后任一维度超过 YAML hard maximum 返回 `TURBO_ERANGE`，不截断、不饱和、不部分提交。
- route/worker incarnation 改变时旧 credit 全部失效，不能持久化或迁移到新 session。
- 旧 generation 尚有 in-flight 时，新 READY 先返回 `TURBO_EBUSY`；host 必须按 reliability 将旧
  requests drop/requeue 完毕后再接纳新 generation，不能让两个 route 同时拥有同一 worker ID。

调度在 service 内选择具有足够双维 credit 的 LRU worker。存在 live worker 但无足够 credit 时返回
`TURBO_FLOW_FMQ_EAGAIN`；没有 live worker 返回 `TURBO_ENOTCONN`；job 超过 hard maximum 返回
`TURBO_EMSGSIZE`；broker correlation 容量耗尽返回 `TURBO_ENOSPC`。

## 6. ACK 与失败语义

credit 不是 ACK。TFCW 保持两类业务 ACK：

1. `ACK_ACCEPT`：显式 durable transaction/XADD 已提交。它不表示 worker 收到。
2. `ACK_WORKER_COMPLETION`：worker COMPLETE 被当前 route generation 接受，并且 storage claim
   ack/delete 成功。transport send success、credit consumption 和 HWM admission 均不能生成该 ACK。

JOB send 失败时 correlation 返回 accepted/pending，credit token不自动恢复；该 session 的 credit
状态已不可信，应关闭或等待新的显式 grant。worker lease expiry 对该 worker 的全部 in-flight request
逐项产生 DROP/REQUEUE disposition，全部处理完后才能删除 worker generation。

## 7. 持久化多 claim 状态

存储与 settlement 契约：

- Redis Stream owner 通过 `turbo_flow_redis_stream_owner_create_ex()` 配置 bounded multi-claim；同一
  consumer 的 PEL 仍是事实源，requeue 不执行 XACK，restart 会按单调 pending cursor 恢复多个 entry；
- ack/requeue 只作用于对应 token，stale/double settlement 返回 `TURBO_EALREADY`；
- active claim 数量达到上限时返回 `TURBO_EBUSY`，不建立额外 payload 队列。
- `turbo_flow_fmq_credit_settlement_t` 在一个 host-serialized owner lane 内绑定
  `request_id -> claim_token`；completion、cancel 和 lease expiry 关闭 credit correlation 后同步执行
  storage ACK/requeue/drop，失败则保留 pending settlement 供显式 retry；
- Redis Stream 通过 `turbo_flow_claim_settler_t` 薄适配，不把 payload 或 backend 类型复制进 FMQ；
  drop 与 delivery ACK 使用不同 callback。
- Redis XACK 的 uncertain transport outcome 保留 active claim；显式 retry 通过 group-wide exact
  XPENDING 对账，ID 不存在才确认旧 XACK，仍由当前 consumer 持有才重发，已转移则返回
  `TURBO_EBUSY`，不 ACK 其他 consumer 的 claim。

durable bulk 边界：

- `turbo_flow_claim_settler_t` 暴露 bounded `load_state/commit_state`；FMQ 不依赖 Redis 类型；
- TFCS/1.0 LTV 严格校验 schema major/minor、记录边界、逻辑地址和 attempt 上限；
- dispatch 在向 host 暴露 worker route 前持久化 `INFLIGHT`；重启时只把它归一为 `PENDING`，
  不序列化 process-local route/token；
- completion outbox 保存 TFBR logical address；Redis 使用一次 EVAL 原子提交 snapshot + XACK，
  回复丢失后以完全相同 snapshot 重试可判定幂等成功；
- outbox publish 由 host 显式 confirm；COMPLETED/POISONED 到 TTL 后持久删除，避免 bounded table 耗尽。
- `turbo_flow_fmq_credit_durable_shutdown()` 先关闭 admission，再按配置选择有界 requeue/drop drain 或
  preserve-for-restart；backend failure 保持 quiesced 并允许同一调用重试。

因此 Redis Stream 可提供配置驱动的 durable `at_least_once`；未提供 durable
callbacks、绑定名不匹配、snapshot schema 错误或 backend 不可用时均 fail fast，不降级为 volatile。

## 8. YAML

```yaml
channels:
  bulk-workers:
    kind: fmq_pattern
    config:
      pattern: credit_worker
      scheduler: lru
      reliability: at_least_once
      service: bulk-jobs
      max_workers: 256
      max_inflight: 4096
      worker_lease_ms: 15000
      max_credit_messages_per_worker: 64
      max_credit_bytes_per_worker: 67108864
      max_job_bytes: 8388608
      storage_channel: redis.claims
      state_key: fmq:bulk-workers:state
      max_attempts: 3
      dedup_ttl_ms: 300000
      shutdown_policy: requeue
      shutdown_max_steps: 4096
```

`at_most_once` 不接受 durable-only 字段，并由原 create-resolved API 创建。`at_least_once` 要求显式
storage/state/retry/TTL/shutdown 契约，并由 durable create-resolved API 与同名 storage binding 创建。
unknown、wrong-type、zero、binding mismatch、credit 上限乘法溢出和 job bound 大于 worker byte bound
均启动失败。

## 9. 公开接口

`turbo_flow_fmq_credit_worker_config_t` 和 opaque `turbo_flow_fmq_credit_worker_t`
表达唯一 credit-worker owner，不把 single-credit broker API 变成行为可变的胖接口。owner 提供：

- create/create-resolved/destroy；
- READY/CREDIT/HEARTBEAT apply；
- nonblocking dispatch/reserve；
- complete/fail/cancel/expire-one；
- caller-owned bounded snapshot；
- TFCW encode/decode 与可选 graph transform operations。

易失性 at_most_once graph 由 FmqCreditWorker resource 和四个 inline typed operation 组成：

| Operation | Input | Success |
| --- | --- | --- |
| fmq.credit.control | READY/CREDIT/HEARTBEAT + worker route | 更新 owner 并 drop control message |
| fmq.credit.dispatch | pre-encoded JOB + client route | 扣减 credit，route 替换为 worker route |
| fmq.credit.complete | COMPLETE/FAIL + worker route | 完成 correlation，route 替换为 client route |
| fmq.credit.worker_input | control 或 COMPLETE/FAIL | 自动选择以上控制/完成路径 |

service 属于 credit_worker provider 配置，graph 节点只引用 resource。JOB 必须由上游 processor
按 TFCW/1 预编码；credit stage 不猜测业务 payload 到 TFCW 的映射。`at_least_once` graph
registration 要求 message-owned claim token projection；缺失该 projection 时明确返回
`TURBO_ENOTSUP`，不能从 message ID 合成，也不能把 graph success 当作 storage accept ACK。

## 10. 验证契约

发布验证必须覆盖：

- TFCW/1 canonical LTV、schema、truncation、overflow 和 unknown critical field；
- 双维 grant、duplicate/gap、generation fencing、LRU、多 inflight 与 lease expiry；
- `at_most_once`/`at_least_once` YAML 非法组合和 backend fail-fast；
- Redis PEL restart replay、独立 ack/requeue、settlement retry 与 bounded shutdown；
- READY、并行 JOB、独立 COMPLETE、worker reconnect、stale generation 和 credit 不自动返还；
- capacity、slow-worker、lost reply、claim disposition 与 coordinator restart 故障注入。

分位数使用 nearest-rank，在 warmup 后统一由 `FMQ_BENCH_RESULT` 输出。`test_fmq_broker` 测量纯
credit owner 的 4096 次 dispatch 与 4096 次 complete；`test_fmq` 测量 64-byte payload 经真实 TCP
DEALER -> ROUTER graph echo -> DEALER 的 256 次串行 round trip。绝对值受平台、构建类型和机器负载
影响，只能与同环境历史比较；benchmark 不以某台开发机的时间作为协议正确性断言。

TFCW 不定义应用认证或授权字段。使用 KCP 时必须服从
[KCP_TRANSPORT_PROTOCOL.md](KCP_TRANSPORT_PROTOCOL.md) 的认证会话、AEAD 与 replay 规则；
跨 Domain 的 authority/ACL 仍由 FMQ security owner 判定。
