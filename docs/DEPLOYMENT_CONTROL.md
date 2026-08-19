# FlowMQ deployment control and release replacement contract

本文件是 [协议索引](PROTOCOL_SPEC.md) 指定的 deployment control contract 唯一正文。
它定义 membership、fencing、release replacement 和 reconcile 的 owner 与状态边界，不新增
FMQ/3 frame；基础 framing 见 [FMQ_WIRE_PROTOCOL.md](FMQ_WIRE_PROTOCOL.md)。

deployment control 不改变 FMQ/3 frame 或 TFMP/1 wire，也不提供 dual-stack decoder、
gateway、mixed-version negotiation 或兼容 wrapper。

## 1. 决策背景

graph data plane 只应传输业务消息。registry、成员租约、broker 选择、滚动升级判断与 crash-window
reconcile 都会跨越进程或版本，若把这些状态放入 graph adapter，会产生多个事实源，并让 route、连接和
业务拓扑互相反向修改。

因此部署控制分成三类 owner：

| Owner | 主事实源 | 公开边界 |
| --- | --- | --- |
| Deployment controller | member lease、logical route primary、route generation | `turbo_flow_fmq_deployment_*` |
| TFMP operation owner | durable command、recovery-required、terminal result | `turbo_flow_tfmp_management_service_*` |

Discovery controller 仍只把已决议 snapshot 投影到固定 adapter slots；它不参与 membership 或 election。
Observe 只能导出上述 owner 的派生指标，不能写回 registry、route 或 operation。

## 2. Failure-domain controller

### 2.1 状态与身份

- `authority_id` 是管理域稳定名字。
- `authority_epoch` 由宿主的单一 authority/election 服务单调分配。controller 不自行猜测或回退 epoch。
- controller 创建时用 TurboUtils UUIDv7 生成 `authority_incarnation_id`；熵失败直接返回。
- member 使用稳定 `member_id` 加进程级 `incarnation_id`。旧 incarnation 不能 heartbeat、drain 或 leave
  新 incarnation。
- registry mutation 必须携带精确 `expected_registry_version`，因此一个 owner lane 上不存在 lost update。

宿主通过 TFMP DEALER/ROUTER 传递 pointer-free membership operation 和 snapshot；本模块不新增
wire kind，也不编码 socket 指针、CoroNet route 或本地 monotonic deadline。snapshot 只携带相对
`lease_remaining_ms`，该值不得跨 snapshot 比较。

### 2.2 Election 与 fencing

每个 member 绑定一个 failure domain 和 stable logical route。READY incumbent 在租约有效时保持 primary，
避免新成员上线造成无谓抢占。incumbent drain、leave 或 lease expiry 后：

1. 优先选择不同 failure domain 的 READY candidate；
2. 再按 priority 降序；
3. 同 priority 按 member ID 字典序，保证确定性。

primary、primary incarnation、endpoint 或可用性变化都会推进 `route_generation`。远端 peer 必须把
`authority_epoch + authority_incarnation_id + logical_route + route_generation + member incarnation`
作为 fencing token；旧 token 不能覆盖新 route。没有 candidate 时 route 仍保留并返回
`TURBO_ENOTCONN`，后续恢复继续推进同一个 generation。

同 authority epoch 的不同 authority incarnation，或同 registry version 的不同 normalized content，
由 snapshot comparator 判为 `SPLIT_BRAIN`。更低 epoch/version 判为 `STALE`。更高 epoch 是宿主已完成
fencing 后的新 authority；本地 controller 不替宿主实现分布式 consensus。

### 2.3 复杂度、线程与失败状态

成员与 route 各有显式上限（默认/最大 256）。控制面 apply/election 为 `O(m)`，snapshot 排序为
`O(m log m)`；这些路径不在数据热路径。所有调用由宿主串行化，不创建线程或 timer。

命令先校验 authority、version、capacity 与 generation 上限，再更新事实源。错误不推进 registry
version。lease tick 在移除成员前预检 route generation，避免部分过期。宿主可从 normalized snapshot
持久化/复制 registry；跨进程写 authority epoch 必须由外部强一致 election/fencing 服务负责。

## 3. Release replacement

一个 rolling replacement 只允许同一协议集合和同一持久化 schema 的二进制参与：

- FMQ/3、FMS/3、TFMP/1、TKSH/1、TKSR/1 与 TKF1/1 必须完全一致；
- TFMS 与 TFCS 持久化 schema 必须完全一致；
- resolved YAML 字段集合和必需 capability 必须完全一致；
- 任一项不同都必须先停止 admission、drain operation/outbox、关闭旧 owner，再由新 release
  独占打开 store 与 endpoint。

decoder 不接受其他版本，部署面不提供版本范围、mixed-version evaluator、dual-stack gateway
或自动转换。宿主必须在调用 deployment controller admission 前确认所有 live member 来自
同一 release；release identity 不进入 FMQ wire 或由 controller 推断。

## 4. Durable side-effect reconcile

operation store 与 Flow/resource mutation 不构成分布式事务。durable execution 的顺序为：

1. 原子提交 `ACCEPTED`；
2. claim 后原子提交 `RUNNING`；
3. 执行目标 mutation；
4. 原子提交 terminal operation 与可选 event outbox。

进程可能在 2 与 4 之间退出。恢复时 `RUNNING/CANCEL_REQUESTED` 保持 recovery-required，不自动改写为
失败，也不盲目重放。owner 在 recovery 清空前不能进入 READY。

每种可 durable command 必须有 typed inspector：

- FLOW_PAUSE/RESUME/DRAIN 从 runtime snapshot 核验目标 admission/drain 状态；
- POOL_RESIZE 从 pool UID、kind、parallelism 与 generation 核验；
- RESOURCE_QUIESCE/RESUME 和 ENDPOINT_REPLACE 必须由 resource owner 注入
  `turbo_flow_tfmp_reconcile_binding_t`，否则 command descriptor 不宣告 durable，该请求返回
  unsupported capability。

inspector 只返回三种结论：

- `APPLIED`：直接提交成功，不重复 mutation；
- `NOT_APPLIED`：只有原 expected generation 仍成立时，使用同 operation UUID 作为 idempotency key
  执行一次 goal-state command；
- `CONFLICT`：状态已前进到其他值，提交 terminal conflict，不覆盖新状态。

inspect/store 错误保留 recovery-required，调用方显式重试。若重放后再次在 terminal commit 前退出，
下一 owner 仍先 inspect；已生效结果会走 APPLIED，因此不会重复副作用。

## 5. 候选方案与取舍

| 方案 | 优点 | 风险 | 结论 |
| --- | --- | --- | --- |
| 把 membership 放进 graph stage | 配置项少 | data/control 事实源混合，split-brain 无边界 | 不选 |
| 每个 adapter 自行选 broker | 局部实现简单 | logical route 多主，无法统一 fencing | 不选 |
| controller 内实现分布式 consensus | 单包看似完整 | 重复造高风险共识/存储基础设施 | 不选；authority epoch 由宿主强一致服务提供 |
| 放宽 decoder 或引入 gateway | 可同时运行不同协议 | 非法 frame 被误接收，状态与安全边界分叉 | 禁止 |
| 同协议 rolling replacement | 保持服务连续 | 要求协议、store schema 和配置完全一致 | 选择 |
| 不同协议 stop/drain/replace | 单一事实源和 decoder | 有维护窗口 | 选择 |
| crash 后自动重放 RUNNING | 恢复快 | 非幂等副作用重复执行 | 禁止 |
| typed inspect + generation-checked goal state | 状态归属清楚，可重试 | 每类 resource 必须实现 inspector | 选择 |

## 6. Shutdown 与 replacement

replacement 顺序固定为：

1. controller 停止接纳新 member 和 management mutation；
2. drain inflight operation、event outbox 与 durable settlement；
3. 持久化 normalized snapshot 并关闭 endpoint/store；
4. 宿主校验 release identity、协议集合与 store schema，新 owner 取得更高 authority epoch 后启动；
5. 旧 epoch 永久 fenced，不得恢复 mutation admission。

失败时保持 admission closed，由同一 release identity 重试关闭或启动；不得自动启动另一协议、
另一 schema 或 gateway。验证至少覆盖跨 failure-domain failover、旧 member incarnation、
lease expiry、split-brain、不同 release identity 拒绝、drain 超时、mutation crash window、
无 inspector 的 durable 拒绝，以及 inspector 恢复不重复 command。

## 7. Release gate

本协议的 fencing、rolling manifest 和 durable reconcile 测试属于统一 `fmq-release`/`fmq-chaos`
测试集。准确命令、真实 Redis 前置条件和性能趋势规则只在
[RELEASE_GATE.md](RELEASE_GATE.md) 维护，不能用 Disabled live suite 代替发布验证。
