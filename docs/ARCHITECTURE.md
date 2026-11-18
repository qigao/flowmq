# FlowMQ architecture

在现有消息能力之上构建 MDP 与分布式服务的工作顺序、模块边界和验收契约见
[MDP 与分布式能力建设方案](FLOWMQ_MDP_DISTRIBUTED_DESIGN.md)。该方案描述待实现能力，
当前公开支持范围以下文和安装头文件为准。

## 模块与依赖

```text
Application
    |
ZeroMQ-style context / socket API
    |
runtime: peer lifecycle / bounded queues / progress / deadlines
    |                       |                         |
Core: pattern + session     Protocol: FMQ/6 codec     transport adapters
FSM / routing / credit      frame <-> bytes           CNet I/O / framing
                                                       |
                                             TCP / TLS / IPC / WS / WSS / KCP
```

`FlowMQ::Protocol` 不依赖网络；`FlowMQ::Core` 保存 pattern/session 规则；
`FlowMQ::Transport` 私有依赖 `Salts::CNet` 与 `Salts::NativeIO`。NativeIO 只作为
实现细节支撑 ordinary per-socket wait 与 explicit owner-lane shared wait，不进入安装包的
public CMake dependency surface。普通 socket 可 bind/connect TCP、TLS、IPC、WS/WSS 和 KCP。
普通 socket 的 peer 预算默认是 4，部署可在 bind/connect 前通过
`flowmq_socket_set_peer_capacity` 配置。socket 为每个槽位保存会话代际；
监听器满员时继续推进既有连接，释放槽位后再接纳新 peer。
共享 Owner 的公开入口提供 TCP、IPC、WS/WSS 和 KCP 客户端。各 transport 的发送与监听边界见
[根 README](../README.md#runtime-model)。

物理目录与上述 target 保持一致：

```text
flowmq/include/                         installed flat C API
flowmq/src/protocol/                    FMQ/FMS/FES codec 与 stream decoder
flowmq/src/core/pattern/                ZeroMQ pattern、FSM 与 subscription state
flowmq/src/core/session/                FMQ flow credit
flowmq/src/runtime/                     context/socket/peer、HWM 与 CNet recovery policy 接入
flowmq/src/transport/cnet/              CNet endpoint、stream、WebSocket 与 packet 适配
flowmq/src/security/                    TLS principal/identity policy
flowmq/extensions/media_provider/       FMP/1 schema、TBE wire view 与 validator
flowmq/tests/、flowmq/benchmarks/       按相同责任分组的验证入口
```

原顶层 `patterns/` 混合了 ZeroMQ socket pattern、transport 和未接入 runtime 的 ESB helper，
现已取消。Core 只保留 socket/runtime 实际使用的 pattern 与 session 状态；Saga、通用 priority
queue、scatter/gather、stream partition 和 circuit breaker 不再编入主库。

## 协议、传输与通信模式的职责（2026-11-17）

下面描述当前实现；后文带日期的接纳与性能记录保留其当时的验收范围。

| 归属 | 输入与输出 | 拥有的状态与边界 |
| --- | --- | --- |
| Protocol：FMQ/6 codec、stream decoder | 有界 bytes/segments ↔ HELLO、SETTINGS、DATA、订阅、心跳、FLOW_UPDATE frame | wire 字段、长度校验、FMQ packet 分片与重组；frame view 的借用期受输入存储约束，不拥有连接或业务路由 |
| Core：pattern | frame 类型、peer pattern、应用收发操作 → pairing、方向、路由策略、FSM 判定 | 提供不可变 pattern 描述符、REQ/REP、multipart 与 subscription set 的类型和迁移规则；状态实例、具体 peer 选择和副作用归 runtime |
| Core：session credit | SETTINGS、累计 FLOW_UPDATE、DATA admission/consumption → 可发送字节数 | 提供累计 DATA credit 的算法；每代状态实例归 runtime peer；控制帧和本地合成 envelope/event 不占 DATA credit |
| transport adapter / CNet | endpoint、FMQ 编码结果、I/O completion → 连接事件与接收 bytes/message | TCP/TLS/IPC 的字节流，WS Upgrade/binary message，KCP session/message，TLS 验证、实际读写与物理终态；不决定 FMQ READY、REQ/REP phase 或业务完成 |
| runtime socket / owner | 上述判定与事件 → 原子接纳、重试、取消、readiness | 唯一 peer/session owner；有界队列、generation、握手截止时间、HWM、decoder/retained 生命周期，以及一个调用者线程上的 progress |
| 应用协议 | DATA payload → 业务请求、响应与结果 | correlation、业务重试、幂等、确认和持久化；FMQ send 成功及 transport completion 均不能替代这些结果 |

依赖方向为 `Protocol ← Core ← runtime`，runtime 同时调用 transport adapter。FMQ codec
不依赖 CNet；transport 不根据 payload 选择通信模式。bind/connect 决定建立连接的角色，
pattern 决定消息方向和路由；例如连接方可以是 PULL，监听方可以是 PUSH。
同一 socket 的 transport 在 runtime 启动后固定，通信模式不能扩大该 transport 的能力。
FMQ 是 FlowMQ 自有 wire protocol；ZeroMQ 风格的 socket 名称与 API 不表示 ZMTP wire 互通。

### 传输能力

| 传输 | FMQ frame 边界 | 复制发送 | retained 发送 | owned 接收入口 | 公开 shared-owner connect | peer pool |
| --- | --- | --- | --- | --- | --- | --- |
| TCP | decoder 从任意分段的有序字节流还原 | 支持 | 支持 | CNet slice，可保留 backing | 支持 | 支持 |
| TLS | 同 TCP，CNet 负责加解密和证书校验 | 支持 | 支持，明文保持不可变至完成 | 从借用的解密数据复制 | 不支持 | 支持 |
| IPC | FMQ 流封装，名称/路径由平台适配 | 支持 | 不支持 | 复制 | 支持 | 不支持 |
| WS/WSS | Upgrade 选择 `fmq.v6`；每个 binary message 恰好一个完整 FMQ frame | 支持 | 不支持 | 复制 | 支持 | 不支持 |
| KCP | 每个 KCP 逻辑消息恰好一个完整 FMQ frame；每 DATA part 至多 64 KiB | 支持 | 不支持 | 复制 | 支持 | 不支持 |

各传输均支持普通 socket bind/connect；公开 owner socket 的 bind 仍返回 `SALTS_ENOTSUP`。
内部 shared-listener qualification 入口的支持范围见后文，不作为公开部署接口。
TCP 从第一个接收字节起使用 owned stream，不在握手后切换 decoder。完整输入可保留 CNet
backing；跨 callback 的碎片按固定块组装，contiguous slice 和 caller buffer 按 API 要求复制。
容量或协议失败时不会切换实现。WS message、KCP message、FMQ packet、FMQ frame、应用 multipart
分别是不同边界；适配层必须保留 frame 边界，不能把一次 I/O completion 当成一条应用消息。

### 会话、容量与关闭

```text
transport ready → 双向 HELLO/SETTINGS → FMQ READY → pattern DATA/control
                       超时/非法 pairing ↓          ↓ 断线或失败
                                  close → terminal drain → peer release
```

stream transport 在开始 FMQ HELLO 时启动一次 5 秒的 readiness 预算；WS/WSS 在 Upgrade
成功后开始。KCP 沿用 session 预留时启动的 5 秒总预算，涵盖 PSK 建立和 FMQ 握手。
预算不因部分 HELLO/SETTINGS、心跳或重复 progress 延长；完整 READY 才解除。
outbound endpoint 还受 CNet reconnect episode 截止时间约束，取较早者；accepted peer 没有
outbound retry episode，但同样必须按时完成 FMQ 握手。到期只关闭对应 peer，继续推进其他
peer 和 listener。ordinary 与 shared owner 使用同一判定；owner 的等待上限包含该 deadline。
所有计时由调用方 progress 驱动，应用不推进时不执行超时清理。

socket 最多占用四个 peer slot，退役但仍持有未消费 inbound 数据或 lease 的 slot 继续计入。
无 slot 时暂停新接纳，将连接留在 CNet/backlog，继续已有连接的读写、关闭和回收；
有 slot 后分配失败才报告内存错误。pool 的 connecting/physical 配额另外约束接纳，不能
用 pool snapshot 代替实际 reserve，也不能把容量压力传播为整个 socket 的进度故障。

接收 DATA 与 XPUB 合成事件共用一次 `stage → commit` 事务：输入先复制或取得 retained
所有权，完整消息才进入 socket FIFO。HWM 满时保存在 peer 的固定 staging 中，标记
`commit_pending`；源 frame 消费一次，订阅状态更新一次。应用取走消息后由 owner 重试
commit，处理已有 decoder 数据，再恢复 receive。该暂存仍受 part/byte 硬边界约束；
XPUB 事件虽然不占远端 DATA credit，仍占本地 receive HWM，应用必须消费。

复制 ROUTER 发送把 identity 和所有 payload parts 视为同一事务。非阻塞最终 admission
被拒绝时取消路由，允许下一条消息选择其他 peer；阻塞发送保留 route/generation 和已暂存
parts，在本线程推进到接纳或失败。断线取消时不会把剩余 parts 转给同 identity 的新会话。
retained 发送的失败保留/释放规则继续遵循公开 slice API，不把复制发送的取消规则套用到它。

关闭按 `停止新接纳/重连 → 请求关闭 → 路由所有物理终态 → 结算队列与引用 → 释放存储`
执行。CNet 拥有物理 I/O 终态，runtime 拥有协议事务和其内存；到达 deadline 不等于已经
可以销毁在途 buffer。接收 credit 在应用取走完整消息后归还，与物理发送完成分开结算。

### 本次重构的选择与验证

四个进度缺陷的共同原因是容量和事务状态分别在 listener、DATA/control 接收和 API 重试
分支中处理。选择在现有 runtime 内复用空闲 slot 查询、完整消息提交与会话握手预算，
明确 Protocol/Core/Transport 依赖方向；旧 ABI、旧行为和旧协议不构成保留分支的理由。
背压与 READY 规则只有一个归属；TCP owned 接收与借用入口按传输能力固定选择，
并共用唯一会话/frame 处理函数。owned 字节流组装和 frame 投影归 Protocol，测试只链接
`FlowMQ::Protocol`；runtime 持有 decoder 实例并提交状态和消息事务。代价是 socket runtime
仍是主要集成点，今后按已验证的职责继续拆分，无需新依赖。

payload 发送和 POLLOUT 共用 pattern 的 DATA 方向校验。XSUB 的订阅控制能力由 desired
subscriptions 同步使用，不能据此允许应用发送 DATA；复制和 retained payload API 都立即
拒绝该方向，避免进入没有目标路由的等待循环。

不提供旧版本协商、旧语义开关或复制 decoder 降级。未完成握手现在会按预算关闭，应用必须
持续推进并在预算内回答 HELLO/SETTINGS。DATA 采用唯一的 canonical packetization：非末包
恰好 64 KiB，末包恰好剩余字节；不接受更小的非末包或错误 LAST。现有 encoder 已按该规则
输出，手写对端必须同步。XSUB 的错误 DATA 发送直接返回 `SALTS_ENOTSUP`，订阅操作使用
订阅选项。部署时统一更新 runtime 与手写对端；若撤回实现，须整体重建会话，不能在在途
消息中切换接收状态。正式回归入口为
[`test_flowmq_progress.c`](../flowmq/tests/runtime/test_flowmq_progress.c)，同一组真实 loopback
用例以普通和 peer-pool 两个配置运行，覆盖 backlog、ROUTER multipart、TCP/WS XPUB 背压、
ordinary/shared-owner 静默握手回收与等待 deadline、READY 后的空闲连接、XSUB DATA 拒绝，
以及合并 HELLO/SETTINGS/空 DATA/multipart DATA 的处理。协议层
[`test_flowmq_owned_stream.c`](../flowmq/tests/protocol/test_flowmq_owned_stream.c) 覆盖数千次碎片
输入组装 1 MiB、跨帧尾块封存、retained 引用释放和容量拒绝。相邻协议、模式、credit、
owner 与 retained 测试继续保留。
通信模式的完整 pairing 与使用边界见 [wire 规范第 4 节](FMQ_WIRE_PROTOCOL.md#4-pattern-registry)。

**验证（事实，2026-11-17）：**Windows x64 / MSVC 19.44 Release，使用仓库锁定版本的
原生 SDK。最初四个 loopback 回归在原实现上全部失败；修复后扩展为七个用例，以普通和
peer-pool 两种配置运行，14/14 通过。完整构建通过，全部非 benchmark CTest 45/45
通过，用时 292.33 秒，包含协议、credit、TCP/TLS、IPC、WS/WSS、KCP、owner、公开 C11/C++17
consumer 与应用 codec 回归。移除了一个依赖源码 marker 的 CTest，并移除 CI 中同类的
收发行为字符串检查；行为由正式可执行测试验证，模块依赖约束仍单独检查。
协议目录及链接依赖迁移后重新完成全量构建，最后五项定向 CTest 5/5 通过，包含九个
owned-stream 用例、严格 packet 校验、stream decoder 和普通/池化运行时回归。
复现测试命令（先按根 README 配置 SDK 并完成构建）：

```text
ctest --test-dir build/review-msvc -R "^test_flowmq_(owned_stream|protocol|stream_decoder|progress(_pool)?)$" --output-on-failure -j 1
ctest --test-dir build/review-msvc -LE benchmark --output-on-failure -j 1
```

CTest 同时补齐 IPC 测试的 SDK runtime 路径和 transport label；消息传输套件含真实握手
到期等待，实测约 30.4 秒，将其总上限从 30 秒调整为 60 秒。
**MED / 验证限制：**并行回归曾出现 TLS 证书文件 I/O 和临时文件清理断言失败，相关测试
串行复跑与最终完整串行回归均通过；并发隔离问题尚未单独解决。Linux、macOS、Android
及性能 benchmark 本次未运行。

## Acceptor–Connector 接入决策（2026-10-10）

**当前状态：**内部 TCP 和 IPC shared listener 已验收。公开 `flowmq_bind()` 使用普通
socket，对 `flowmq_owner_socket()` 创建的 socket 返回 `SALTS_ENOTSUP`。
下面记录 2026-10-10 的 TCP 接纳决策及其验收范围。

**依据（事实，MED）：**[合批后的 progress 实测](FLOWMQ_BATCH_SWEEP.md#合批后的-progress-归因2026-10-10)
显示，Windows 单 peer 稳态下 manager 空调用约 27 ns；listener 空 readiness 检查约
1.2–1.3 µs。64 B / batch=128 中，两处 manager 合计约 0.42%，listener wait 约 6.67%。
因此优先研究复用共享等待以消除独立 listener readiness 检查；不据调用次数删除 manager
回收，也不把四个固定 peer slot 的扫描直接认作瓶颈。

候选方案及取舍：

| 方案 | 适配性与决定 |
| --- | --- |
| Acceptor–Connector，listener 与连接共享固定 owner 的 backend | 内部实现已采用；分离连接接纳与协议会话，复用 CNet 现有能力，合并等待 |
| 每隔 N 次 progress 检查 listener | 会改变新连接接纳延迟与公平性；固定拓扑测量不足以选择 N，不采用 |
| Leader/Followers，多线程轮流等待和处理 | 当前没有线程竞争或 worker 不均衡证据；任意线程处理当前 CNet 状态违反 owner 亲和，不直接套用 |
| 保持 ordinary socket + SO_REUSEPORT 多 owner | 保留现有部署方式；平台不支持时仍返回 ENOTSUP，不以隐藏 accept 线程补足 |

[ACE Acceptor–Connector](https://www.dre.vanderbilt.edu/~schmidt/ACE/book2/c7.html)
提供连接建立与会话服务分离的边界；
[Leader/Followers](https://www.dre.vanderbilt.edu/~schmidt/PDF/lf.pdf) 解决线程池中的等待与处理分工。
这里借用职责划分，不引入 ACE 库、虚表框架或第二套调度器。若未来业务处理成为瓶颈，
另测固定 owner 之间的负载分布，再评估有明确绑定规则的线程设计。

### 已有能力与责任

核对的 SDK 为 Salts `2.3.0-rc.1`，源码 `58ff08fc95b4aa1dc493c0b7080426b2c11d4959`；
事实源是其 `cnet/cnet.h`、`cnet/manager.h` 和 `cnet/src/cnet_listener.c`：

- CNet listener 可 attach 到同种 backend，借用 backend，不负责 observe/destroy。
  每个 listener 至多一个 external accept；重复 submit 返回同一个 generation-safe request。
  `SALTS_EALREADY` 表示已有终态结果待消费，不能再提交 accept。
- owner observe 一次后，把完成交给 `cnet_listener_route_external_completion()`。
  非本 listener 的事件返回 consumed=false；成功的 accepted child 先由 listener 保管。
  `accept_detached()` 随后 move 出描述符，再由 `cnet_manager_adopt()` 在最终 owner 上建立连接。
- FlowMQ socket 继续唯一拥有 peer、decoder、FSM、HWM、credit 和 reconnect 状态；
  manager 负责连接记录及回收。accept 的 request identity 只用于路由，不复制会话事实源。
- 已有 `cnet_handoff` 是有界连接接纳交接能力，不自带 worker 或唤醒。首版无需跨 owner
  交接，也不引入逐消息 MPSC 队列。当时的首版只验证 TCP listener 的 shared wait。

### 内部 progress 与容量协议

```text
固定 owner，所有调用串行：
  推进 CNet client；有 admission 容量的 listener 确保一个 accept 在途
  汇总 caller / CNet / FlowMQ deadlines
  一次 shared NativeIO observe
  路由 accept 和 client completions；每个完成最多消费一次
  消费已完成的 accept -> manager adopt -> 一次 FlowMQ local progress
  下一 owner cycle 才推进刚接纳的连接；不追加第二次 CNet advance
```

提交 accept 前，先预留一个 peer、可选 pool connecting slot 和 manager admission record；
每个 listener 最多持有一组预留，计入现有四 peer 的硬上限。无容量则不提交新 accept，
连接留在内核 backlog；进度循环仍推进已有连接。预留成功而 submit 失败时，走 manager
cancel、context hold release、pool/peer release 的明确回滚，不能泄漏 credit。
预留阶段尚无业务 DATA、decoder 输出或协议 READY；成功 adopt 才开始正常会话生命周期。
pool 的长生命周期 lease 仍在协议 READY 后获取，TCP accept 不提前发放 lease。

listener request 保存完整 slot/generation。在现有 socket 路由中先匹配该 identity，再交给
listener router；未匹配才尝试该 socket 的 client router，不按裸 slot 或 native handle 判定。
保留现有有界 socket registry 枚举，首版不另外维护全局索引。与现有 owner fault 规则一致：
路由发生错误后不把同一个完成交给另一个消费者；继续处理批次剩余事件及 local progress，
最终返回首个错误。每个 listener 每 cycle 最多接纳一个 child，避免连接突发占尽会话处理。

设 owner 的 socket 上限为 S、listener 上限 L≤S、peer 上限 P=4、每 socket 原 request
预算为 R。旧 backend 预算为 endpoints=2PS、requests=RS、completion batch=RS。
内部实现按 L=S 预留保守预算：endpoints=2PS+2S（listener 与 accepted-child 余量）、
requests=RS+S、completion batch=RS+S；乘加均 checked，超限返回 ERANGE。
这也增加 client-only owner 的预分配容量，没有改变每 socket 的四 peer 上限。listener 的 child 最多一个，
已经包含在其预留 peer 内，不能当作可无限增长的额外连接池。具体 backend 对 child escrow
的资源计数仍须通过容量耗尽测试核实，预算公式不是容量测试通过的证明。

### 关闭、迁移与验证门槛

**HIGH：**关闭先禁止新 accept/reconnect。若 accept 仍在途，SDK 的 listener close 会提交
cancel 并返回 EBUSY，owner 必须继续 shared observe/route，直到终态完成已消费，再重试
listener close/destroy。取消前已成功的 child 由 listener 清理，或已 move 后由 manager/peer
清理，不能双重关闭；预留 record 和 context hold 也必须终结。不能因为 client 已停止就把
仍有 listener 完成义务的 socket 从路由 registry 移除。所有 listener/client/manager 义务
消失后才销毁 socket，最后一个 socket 关闭后才销毁共享 backend。

影响范围是 runtime socket/owner、公开 owner 文档、transport 测试与 benchmark；协议 codec、
消息格式和应用 send/recv 契约保持原样。无需新依赖。先在内部验证接入，再一次性开放完整
TCP owner bind 能力，不安装占位 API。迁移由应用显式选择 owner domain；ordinary domain
继续可用。回滚需关闭该 owner 的 sockets 后重建 ordinary sockets，不能 live detach；不涉及
磁盘数据或 wire 格式迁移，也不以失败后的自动切换掩盖容量或生命周期错误。

开放前必须验证：无连接时 timeout 与 CPU、连接突发与每 listener 公平性、满 peer/pool 后
恢复、accept/attach/adopt 失败回滚、generation 复用、混合 accept/client 完成批次出错后继续
drain、pending accept 取消及 late completion、关闭一个 socket 时邻接 socket 继续传输。
保留现有 payload/FIFO/HWM/retained 生命周期回归；比较相同 TCP workload 下 ordinary 与
owner listener 的吞吐、进程 CPU、P99、接纳延迟与峰值资源，按目标平台分别测量。
此前固定拓扑 ablation 不覆盖这些门槛，其省略 listener 的结果不能替代实际 shared-listener 测量。

### SDK 接纳生命周期验证（2026-10-10）

**事实：**新增正式 CTest
[`test_flowmq_listener_external`](../flowmq/tests/transport/test_flowmq_listener_external.c)，
使用上述发布版 SDK、真实 loopback TCP 和 NativeIO completion，复用 FlowMQ 的 CNet
配置适配器及 `flowmq_owner_batch_route()`。10 个用例覆盖：

- 重复 submit 保持同一个 request；attach 后禁止独立 listener wait；pending close 返回
  EBUSY，终态路由前 destroy 仍被拒绝。
- request slot 复用时 generation 改变，旧 cancellation completion 不会消费新 request。
- endpoint 满导致 attach 失败、request 满导致 accept 提交失败后，释放容量即可重试。
- manager 的单 record/connection 容量拒绝额外预留；取消预留先归还 admission credit，
  context hold 释放后才 recycle；旧 managed identity 不能访问复用后的 record。
- accept terminal 待消费时 submit 返回 EALREADY；detached child 成功 move 到 manager 后，
  listener 可关闭并销毁，已有 session 仍完整接收 64 B，retained payload 恰好释放一次。
- 成功 accept 已 observe、尚未 route 时调用 close，仍须路由实际成功完成，随后由 listener
  销毁未 move 的 child；sealed-manager adopt 失败也会消费 child，且不伪造 state callback。
- 两个真实 cancellation 完成、以及真实 accept/client 混合完成，经有界收集交给同一个
  FlowMQ batch router。首个事件被真实消费者消费后注入错误，余下事件仍各被消费一次。
  这是对已观察事件的组合路由验证，不声称它们必然来自同一次 OS observe。

所有用例在 teardown 中 drain 并销毁 listener/manager/client，再检查 backend 的 active
requests 和 endpoint count 均为零，最后销毁 backend；这些计数不等价于 OS handle 泄漏检测。
Windows x64 / MSVC Release 连续 20 轮通过（200 个用例执行）；相邻 transport、peer pool、
owner、owner batch、owner fault 及其 pool 变体共 6 个 CTest 通过。

复现入口（Windows 从 VsDevCmd 环境运行）：

```text
cmake --build --preset win-release-user
ctest --preset win-release-user -R "^test_flowmq_listener_external$" --repeat until-fail:20 --output-on-failure
ctest --preset win-release-user -R "^test_flowmq_(transport|owner|owner_batch|owner_fault|owner_fault_pool|peer_pool)$" --output-on-failure
```

本地日志：`build/listener-sdk-build.log`、`build/listener-sdk-repeat.log`、
`build/listener-sdk-adjacent.log`。

这组测试仅确认 SDK 边界。后续 FlowMQ 内部集成的覆盖及限制见下节；SDK 测试不能替代
FlowMQ socket 的 peer/pool、FSM、credit、重连与 owner shutdown 联动验证。

### FlowMQ 内部 shared-listener 集成（2026-10-10）

`flowmq_socket_internal_bind_external()` 位于不安装的私有 header，只供集成测试和
benchmark 使用；要求 owner-created socket，接纳 TCP 和 POSIX IPC。公开 `flowmq_bind()` 仍按原契约
返回 ENOTSUP。实现复用普通 bind 的初始化、endpoint 和 listener options，不引入第二套
session 或后台线程；不把 qualification 入口安装为不完整的公开 API。

socket 保存最多一个预留 peer、完整 accept request identity 和待消费标记。
accept prepare 在 CNet advance 后、共享 observe 前执行；终态路由后，在一次 local progress
中 adopt，随后普通协议 callback、pool READY、credit、decoder、FIFO 和 retained 路径保持
原有归属。无 peer/pool 容量时不申请 child；submit 的 EBUSY/ENOBUFS 释放预留后等待下一轮。
回滚先 cancel manager record，再释放 context hold 和 pool/peer，不能用 release 代替 terminal。

POSIX IPC listener 借用同一 NativeIO backend，由 CNet 提交 Unix-domain accept。FlowMQ 用
`cnet_manager_adopt_ipc()` 接管已完成的 child。监听关闭完成后由应用清理 socket 路径，随后可
在同一 Owner 上创建新 socket 并重绑。Windows 命名管道共享监听由 SaltsBase #11 跟踪。

关闭把 `external_client_stopped` 与整个 socket 的 `external_stopped` 分开。
client 已停止但 listener cancel 尚未消费时，socket 继续留在 owner registry，只路由剩余
listener 完成；成功但未 move 的 child 由 listener 销毁。整个 socket 停止后才允许释放存储。

正式集成测试为 `test_flowmq_owner_listener` 及 `_pool`，各 8 个用例：公开契约/TCP 范围与
预算溢出、空闲等待与八次取消后重建、listener/connector 共存及关闭隔离、同 owner 两端通信、
四 peer 满额后接纳第五个、pool 单 slot 恢复、双 listener 突发传输、max_connecting 在协议
READY 前保持占用。复用现有 fault suite 生成 `test_flowmq_owner_listener_fault` 及 `_pool`，
在服务端 owner 上覆盖真实 DATA 完成批次出错后的继续路由、retained 释放、credit 和邻接
socket 存活。四个 target 连续 10 轮通过，共 200 次用例执行。
完整 core/transport 回归 35/35 通过（7 core、28 transport，含公开 C11/C++17 consumer
和 1/4/16-lane REQ/REP 示例），用时 219.88 秒。复现：

```text
cmake --build --preset win-release-user
ctest --preset win-release-user -R "^test_flowmq_owner_listener" --repeat until-fail:10 --output-on-failure
ctest --preset win-release-user -L "flowmq-(core|transport)" --output-on-failure
```

日志为 `build/listener-runtime-{build,repeat,regression}.log`。
实际 shared-listener 的九场景配对结果见
[pattern 性能报告](FLOWMQ_PATTERN_ROUTING.md#实际-shared-listener-内部实现2026-10-10)。

**剩余门槛 / HIGH：**尚未在 FlowMQ 层注入 attach/submit/adopt 资源失败，SDK 层的失败测试
不足以证明整个 socket 回滚链；SDK late-success close 已覆盖，但 FlowMQ 层尚未定点重放
同一时序。突发测试证明各 listener 能完成传输，不等于接纳公平性或 P99 的测量。
**MED：**CPU、接纳延迟及峰值资源仍待测；Linux/macOS 未运行。这些门槛满足后再审查开放
公开 bind，当前不把内部入口当作可部署的 API。

## 执行模型

CNet 是调用者驱动的单-owner协程池，不是线程池。FlowMQ 不创建 progress thread，
也不把 socket 包装成 Actor/Reactive publisher。

FlowMQ 1.2 有两个 execution domain：

```text
ordinary socket domain
  socket owns its CNet/NativeIO progress
  flowmq_poll() drives each socket independently

explicit owner-lane domain
  flowmq_owner_t owns one shared NativeIO backend
  N owner-created sockets each own one external-progress CNet client
  flowmq_owner_poll() advances all live clients and performs one shared observe
```

socket 创建时就决定 execution domain，之后不能 detach、迁移或在两个 poll surface 之间切换。
owner lane 是一个**同步 owner object**，不是 scheduler/worker pool。一个 connection 从 admission
到 terminal callback 固定属于同一个 lane-local CNet client。

普通 socket 继续遵守以下规则：

- `start/bind/connect` 负责同步验证、资源创建与异步 I/O admission；
- `send/recv/poll` 在调用者线程推进 socket 和 CNet 状态；
- outbound endpoint 的重连 deadline 也只在这些调用中检查；到期后最多执行一次固定容量
  endpoint 表扫描并向 CNet admission 新 session，不创建 timer/progress thread；
- `DONTWAIT` 只尝试一次；普通 `send/recv` 遇到 would-block 时在当前调用栈分片推进所属
  socket，直到成功或 CNet progress 返回错误；
- 多 socket `poll` 对整个列表重复执行非阻塞 progress，以 1ms 有界间隔等待，直到请求的
  level-triggered 事件就绪或总 timeout 到期；
- CNet callback 只在调用者主动 progress 的 `send/recv/poll` 调用栈内同步执行；
- socket、decoder、route、pattern FSM 与队列由同一调用线程串行拥有；
- 同一个普通 socket 不允许跨线程并发使用；跨线程通信使用独立 socket。

owner-lane socket 则由 `flowmq_owner_poll()` 推进：

```text
for each live socket:
    cnet_client_advance_external() exactly once
    collect CNet + FlowMQ-local next deadline

native_io_backend_observe(min_deadline) exactly once

for each completion:
    route to exactly one owning CNet client

for each live socket:
    FlowMQ reconnect/control/FLOW_UPDATE/subscription/flush progress exactly once

compute POLLIN/POLLOUT/POLLERR with the same readiness projection as ordinary poll
```

FlowMQ-local deadline projection至少包含 reconnect next-attempt/episode、FMQ handshake、heartbeat ping/dead-peer deadline、
receiver FLOW_UPDATE deadline/quantum 与 caller timeout；因此正式 owner API 不依赖 #67
prototype 的固定 10ms wait。routed completion batch 后不立即执行第二次 CNet advance，
下一 cycle 才重新 advance，保持 owner-local progress 的单 pass 语义。

`flowmq_owner_t` 的 socket registry 与 NativeIO capacities 在创建时按 `socket_capacity`
硬上界预留；不会依据 CPU 数自动扩容，也不会 live resize。owner-created socket 不能放入
ordinary `flowmq_poll()`，不能直接 `flowmq_close()`，也不能迁移到另一个 owner。
该 API 当前提供 TCP、IPC、WS/WSS 和 KCP client-side shared-wait progress。

公开的 listener/same-endpoint multicore 使用独立的 ordinary-socket composition，而不是把 listener
塞进 `flowmq_owner_t`：

```text
owner/core A                         owner/core B
ordinary socket A                    ordinary socket B
FLOWMQ_REUSE_PORT=1                  FLOWMQ_REUSE_PORT=1
CNet listener A ---- same port ---- CNet listener B
        \                              /
         +---- kernel accept hash -----+
```

`FLOWMQ_REUSE_PORT` 是 startup-only int 0/1。bind 统一调用 released CNet
`cnet_listener_init_ex()`；不支持 `SO_REUSEPORT` 的平台返回 `SALTS_ENOTSUP`，没有
`SO_REUSEADDR` 模拟或 silent fallback。每个 accepted connection 永久归属 accepting owner，
不会跨 owner 转移 native handle，也不会通过 central dispatcher 把每条消息重新汇聚到单线程。

#73 的 same-endpoint qualification 使用四条固定 client connection、一个 owner control 与两个
distinct-core owner candidate。饱和的 64 KiB pipelined DEALER→REP workload 在 exact
128/128 measured request distribution 下得到约 1.451x throughput paired median，p99 约
0.615x control；这证明 same-endpoint composition 可以获得真实多核收益，但不构成所有 payload
或 workload 都线性扩展的承诺。

2026-10-10 的 [Windows 固定连接数多 lane 测量](FLOWMQ_LANE_SCALING.md) 使用八条独立
TCP PAIR 连接、batch=128，连接两端固定在同一 ordinary owner 线程。八轮配对中，
8 lane 对 1 lane 的吞吐加速中位数为 64 B **5.032×**、1 KiB **6.576×**，进程 CPU/wall
约 7.8；对应 CPU/条增加约 54.9% 和 17.2%。该结果验证独立 owner 多核分片，不是
`flowmq_owner_t` listener shared wait、统一端口 accept 分配或跨机网络性能的测量。

随后新增了每线程一个 `flowmq_owner_t` 的 1/2/4/8 lane 配对验证，所有 lane 各自创建、
推进和销毁 context/owner/sockets，共用同一固定八连接 workload 与 ordinary 对照。
完整结果及 CPU/P99 见 [每线程 shared owner 报告](FLOWMQ_LANE_SCALING.md#每线程独立-shared-owner2026-10-10)。
该实验使用内部 TCP listener 入口，不扩展公开 bind 契约，也未验证非 PAIR 多 lane。

低负载的 lane 数和等待策略需另行选择。[定时突发 CPU 对照](FLOWMQ_LANE_SCALING.md#低负载-cpu-与等待策略2026-10-10)
在相同约 20.48 万条/秒发送计划下比较 1/8 lanes 和 idle spin/native wait：等待显著减少
CPU cycles，但 Windows 上计划释放到完成的 P99 增加到约 16–18 ms。当前保留调用方
选择 `owner_poll` timeout，不新增默认等待策略、隐式线程或跨 lane 状态迁移。
采用一线程一 context/owner；context 的生命周期计数不因多个 owner 而成为线程安全。
低 CPU 场景只等待真正需要的 POLLIN/待发送 POLLOUT，并使用最近的应用 deadline；
严格定时场景还需验证平台等待精度，不能用平均完成速率掩盖迟到。

### 固定 lane 直通与应用层边界

项目选择固定 owner/lane 内直接收发作为 FlowMQ 数据路径：业务在该 lane 调用
send/recv，并由同一线程推进 CNet。owner 负责状态与 I/O 归属，不隐含独立消费
线程。FlowMQ 不采用 Actor、应用 mailbox 或 executor 转交来执行每条消息的收发；
这类调度由消费方应用按自身需求组织，不进入 FlowMQ 生产 runtime 或公开接口。

多核扩展通过独立 context/owner 和连接分片实现。跨线程业务使用各自的 socket
进行消息通信或在应用层组织任务；外部线程不能直接操作已有 owner 的 socket，
互斥 Token 也不改变固定线程亲和。已有连接接纳 handoff 只承担连接进入最终 owner
的控制职责，不演变为逐消息的应用 mailbox。

有界协议收发队列、HWM/credit、retained buffer 和 CNet SG 批量提交继续归各自
owner 管理。这些能力承担协议顺序、背压与异步生命周期，不能因不采用应用
mailbox 而移除。[直通与 mailbox 对照](FLOWMQ_LANE_SCALING.md#同-lane-直通与跨线程-mailbox-对照2026-10-10)
保存选择依据和吞吐/CPU 取舍；本选择是项目边界，不是所有 Actor 应用的性能结论。

### CMeta 同步 Guard 的 buffer 池对照

同步方案先在私有 qualification 中验证，不改变公开 owner 的线程契约。
当前 SDK 的 `cmeta/ace_synchronization.h` 提供 Lockable/Guard；它不是带
FIFO/LIFO 排队语义的 ACE Token，也不是 NativeIO ACT。测试用 Platform mutex
实现 Lockable，比较独立池无 Guard、独立池有 Guard、共享池有 Guard。

每 lane 128 个预分配 backing；共享方案合并相同总容量，不增加资源预算。
所有池持有永久基础引用。借用时在同步范围内检查 refcount==1 并取得 retained
slice；全局游标与这次检查/retain 由同一 Guard 保护。取得 slice 后才释放 Guard，
随后在 lane 内写入内容和提交网络操作。其余线程仅能通过同一 Guard 获取借用，
不能凭缓存裸指针 retain；在途内容保持不可变，直到所有临时引用释放。Guard
不覆盖填充、CNet 调用、progress、终态回调或任何等待。无可用 buffer 返回 EBUSY，
owner 继续收包和推进 I/O，保留已经取得的部分 batch，不持锁自旋。

比较一次借用 1/16 个 buffer，收发 staging 仍统一为 16，应用窗口仍为 128。
每 lane 一个线程和一个独立 context/owner，两个逻辑数据源保留各自 FIFO。
准备到验证的 P99 从借用尝试开始计时，包含这次 Guard 等待；同时单独记录
进入 Guard 的累计与最大耗时、调用次数、buffer 探测和不足次数。成功与取消均
先释放 staging，再在原 owner drain 网络，join 全部 lane 后核对基础引用及
最终释放回调，最后销毁池和 mutex。测量不包含初始化、warmup 和 teardown。

[384 组测量](FLOWMQ_LANE_SCALING.md#cmeta-guard-与共享-buffer-池对照2026-10-10)
显示 8 lane 小消息的逐条共享 Guard 增加 CPU 与 P99，批量 16 显著摊薄成本但
没有稳定超过独立池。保持独立池默认；共享 Guard 仅留在私有资格验证中。

### retained SG 批量与 native 提交窗口

私有 direct qualification 对照 retained 批量 1/2/4/8/16，保持每 lane 一个
独立 owner、128 个 source buffer、16 条 staging 和 128 条应用窗口，不加入
共享 Guard、Actor 或应用 mailbox。公开 immediate slice admission 保持既有契约。

RC1 的 CNet 每个逻辑 write 最多接纳 32 ranges，NativeIO 每次提交最多 16 spans；
较大逻辑 vector 由 CNet 在同一 write 下继续提交。当前普通 PAIR 帧是 framing
加 payload 两段，所以 SG16 的逻辑提交减少不等于 native 操作减少。
[224 组 SG 扫描](FLOWMQ_LANE_SCALING.md#retained-sg-批量与-native-操作扫描2026-10-10)
显示 64 B 的 SG8/SG16 native 总操作数几乎相同，SG16 没有稳定吞吐收益；
64 KiB、8 lane 的 immediate SG 比 queued SG16 吞吐更高、排队 P99 更低。
暂不根据逻辑 write 数修改批量默认值，也不把 private queued admission 公开化。

诊断 getter 只允许 owner 线程读取 backend 累计统计，不推进、observe 或消费完成。
计数覆盖该 backend 的收、发与其他 native 操作，不能标为纯 DATA 写 syscall 数。
采样止于接收验证完成，随后关闭仍由同一 owner drain 全部 retained 引用。

### 历史 mailbox qualification 的范围

下述 mailbox 仅用于内部测试和性能对照，不安装、不作为产品架构候选继续推进。
之前讨论的 Monitor 与 buffer credit 等待方案也不纳入 FlowMQ 的应用转交层。

内部验证采用每 lane 一个有界 MPSC mailbox：业务 producer 只向预先绑定的 lane 发布
拥有引用的 slice，唯一 consumer 是该 lane 的 owner。socket/context/backend 始终在 owner
线程创建、推进和销毁。队列使用 Salts Disruptor worker 模式但只有一个 consumer，
不让多个 owner 竞争同一连接的操作。资格验证代码不安装，不增加公开 mailbox API。

协议先限定为最大 64 KiB 消息、128 个槽位、一次最多发布 16 条。每个 producer
预分配 64 个独立 backing buffer 并持有一份基础引用；仅在引用数回到 1（唯一 producer
持有，且无其他线程可凭裸指针重新 retain）时才允许重写。入队前创建 slice，成功
claim 后必须移动 slice 并 publish 恰好一次；claim 失败仍由 producer 释放 slice。
consumer 对照 FlowMQ copy-send 与 retained send_slice，接纳后释放自己的 slice 和
slot；SG 下 CNet 保持源引用直到终态，slot 释放不代表 backing 可以复用。busy/full 时 consumer 持有当前
slot 并继续网络进度，不丢弃，不另开无界 pending 队列。每 producer 保留 FIFO，
不同 producer 不承诺业务上的全局顺序。队列容量与 socket HWM 分别计入资源上限。

发布一批后唤醒对应 native backend；wake 失败发生在发布之后，不能把它当成未接纳
并重新提交同一批。单次内部 progress 必须返回给 mailbox 调度循环，避免公开 owner
poll 在无 socket readiness 时再次等待。空 owner 也须能等待并被唤醒。使用底层已有
持久、合并的 wake 信号衔接“检查队列→进入等待”的窗口，不以近似 empty 查询决定
是否通知；首轮不添加未经验证的软件 wake 去重。每轮有界消费后推进 I/O，避免大队列
饿死网络完成和控制 deadline。连接和队列同时空闲时才允许正超时等待。

关闭先停止并 join 全部 producer/wake 调用者，consumer drain 已发布数据及网络接收，
最后停止 owner，再销毁队列和 backing；错误路径先通知停止，join producer 后释放剩余
slots 的 slice。生产者注册/撤销、动态 socket 迁移、公开 shutdown API 仍不在
本次范围。验证要求覆盖满队列、引用归还与安全复用、乱序发布缺口、等待前/等待中唤醒和停止唤醒，
并对比同一 mailbox workload 的忙轮询与通知等待，记录完整内容、序号、CPU 和尾延迟。

### owner 内 retained SG 合批

SG 的实际接入使用现有 `flowmq_send_slice()`：FlowMQ 将 framing 与原 payload 分段，
交给 `cnet_send_slicev()`，由 CNet 管理 retained vector 的终态寿命。接收使用
`flowmq_recv_slicev()` 并直接遍历 ranges，不先 coalesce。复制 API 对照的后续发送
也可能使用 CNet SG，故对照比较的是 payload admission 方式，而不是有无系统 SG。
当前 retained PAIR 即时发送仍要求 peer write idle，不能由 Disruptor 批量发布推导出
网络也批量提交。跨消息 retained batching 必须保留每条 FMQ 消息边界、credit、
HWM、FIFO 与终态释放，不能用 SNDMORE 将独立消息合成一个 multipart 来替代。

当前内部 PAIR qualification 使用相同 outbound 存储比较单消息 flush 与最多 16
消息 SG flush，同时限制 32 个 CNet ranges 和既有 max_encoded_size。每个 slot
拥有完整 FMQ 消息的 publication；接纳时一次提交 credit/HWM，失败不留下引用。
首条 idle 消息直接提交，不等待凑批。后续只合并相邻 retained entries，遇到复制项、
消息数/range/字节边界即停止；CNet 接纳失败保留全部 slots 和引用，成功后 CNet
接管 backing 寿命，FlowMQ 以该批消息数/字节数处理一个逻辑完成。关闭复用现有
queued release 和 native terminal drain。公开 immediate slice 和 fanout 契约保持
原行为；私有 queued send 只接受 owner 上的 TCP PAIR 单部消息。

模式归属：NativeIO/CNet 是 Proactor 的完成事实源，FlowMQ owner 串行管理协议状态，
历史 mailbox 只用于测量应用交接成本；不引入 Leader/Followers、第二个 ACT 终态表或隐藏
executor。CMeta 继续提供已有类型/接口契约，本轮没有新增反射或动态策略需求，
不把运行时队列算法塞进 metadata/schema。RC2 skill 是审查指引，实际编译仍依据
当前 pinned SDK，不将 RC2 的头文件或测试清单视为已完成 SDK 升级。

每个 live peer 独占 heartbeat deadline、pending-PONG 和双向累计 credit 状态。
peer 的可变协议状态拆成三个独立维度，而不是一个乘积型大 FSM：

```text
lifecycle:
  FREE -> ALLOCATED -> CONNECTED
                       |      |
                       |      +-> CLOSING -> RETIRED -> FREE
                       +-> CLOSE_RETRY -> CLOSING
  ALLOCATED / CONNECTED / CLOSE_RETRY 也可由终止回调直接进入 RETIRED

handshake progress:
  HELLO_TX -> SETTINGS_TX
  HELLO_RX -> SETTINGS_RX

write lane:
  IDLE -> HELLO -> IDLE
       -> SETTINGS -> IDLE
       -> CONTROL -> IDLE
       -> DATA -> IDLE
```

HELLO/SETTINGS 的 TX 与 RX 是两条独立单调链，可以交错推进；只有 lifecycle 为
`CONNECTED` 且四个 handshake fact 全部成立时 peer 才是 READY。SETTINGS TX 至少依赖
HELLO_TX；CONTROL/DATA write lane 只在 READY 后 admission。一个 peer 同时只允许一个 CNet
write lane，send completion 根据 lane 唯一决定是否提交 HELLO_TX、SETTINGS_TX 或 DATA
in-flight 统计，不再维护 `write_busy + writing_*` 多套事实。

`CLOSE_RETRY` 表示 CNet close command 因 bounded command capacity 尚未 admission；
`CLOSING` 表示 close 已被 CNet 接受、等待 terminal callback。终止后先进入 `RETIRED`：
decoder、credit、subscription snapshot、multipart staging 和 outbound storage 已释放，但
socket inbound queue 仍可能保存引用该 generation 的已完成 message part，所以 RETIRED
不等于 FREE；只有这些 queued parts 消费完后 slot 才回到 FREE。

multipart receive/commit-pending、heartbeat/PONG、flow-credit counter、queue occupancy 和
generation fencing 仍是正交事实，不并入上述 lifecycle/handshake/write-lane 状态。
PING/PONG、FLOW_UPDATE 与 subscription sync 共用 CONTROL write lane，不进入应用 outbound
FIFO；收到任意合法 FMQ frame 会取消未应答 PING 的 timeout。所有 deadline 只在 ordinary `send/recv/poll` 或显式 `flowmq_owner_poll()` 中检查，
不创建 timer thread，也不把 socket 变成 MPSC。

ROUTER 的 per-peer backpressure 事实保持 peer-owned。成功 admission 后，应用 payload 的
message/byte outstanding 计数同时覆盖 direct in-flight 与 retained outbound ring；只有 CNet
send completion 才从 current outstanding 扣除。远端 cumulative credit 是另一正交事实，因此
本地 outstanding 可以为 0 而 `send_credit_bytes` 仍为 0。公开
`flowmq_router_peer_status()` 只投影这些稳定诊断事实与 session-local monotonic counters，
不暴露 peer slot、generation、ring cursor 或 raw cumulative credit counter，也不执行任何
progress。peer retire 后不再可由该 API 查到；同 routing identity reconnect 得到全新统计。

Outbound endpoint 持有 URI 和 CNet recovery state。CNet 根据每次拨号的票据管理尝试次数、
退避、deadline 和安全终止。FlowMQ 持有 FMQ READY 与消息状态，并使用保留 context 的
Manager attachment。peer 终止后，已入队的接收数据与未释放的 lease 仍属于原代际。
旧 peer 的 decoder、credit、subscription snapshot、multipart staging 与 outbound queue
不转移到新 peer。XSUB desired subscriptions 在新一代 READY 后从 socket 状态重放。
应用在启动前以 `FLOWMQ_RECONNECT` 配置单轮重连预算。

CFlow/CMeta 可服务于控制面配置、类型描述和 executor 组合，不参与逐消息数据热路径。


## Ownership authority / Salts #878

FlowMQ follows the ecosystem ownership split from Salts #878 without mapping
message/session lifetime to lexical RAII.

```text
CMeta
  DataDesc / FunctionDesc / ObjectRef
  = native value/function/object ownership semantics

CFlow
  managed values/results
  = canonical CMeta COPY/MOVE/DESTROY lifecycle
  callable capture
  = Graph-owned byte snapshot; transitive pointers/providers/code are borrowed

DataBind generator/compiler
  = decides where generated ownership moves and cleanup executes

FlowMQ runtime
  = message / peer / session / retained-send / owned-receive / CNet domain lifetime

Plugin, if a future dynamic provider needs it
  = explicit outer module/provider/code lease
```

The distinction is intentional. `flowmq_owned_stream_t`, retained multipart
staging, retained PUB/XPUB publication references, peer generations, queues,
credit/HWM state, reconnect deadlines, owner lanes, listeners and in-flight
CNet operations are genuine asynchronous/protocol ownership. Their teardown is
driven by completion, cancellation, retirement, close and owner quiescence, not
by C lexical scope.

Conversely, FlowMQ does not define a second native-value
`OWNED/SHARED/BORROWED` model. Lifecycle-bearing values used by CFlow
qualification are owned through the released CFlow/CMeta lifecycle surface.
Generated DataBind request/response cleanup belongs to the DataBind compiler
and canonical CMeta/provider lifecycle; FlowMQ consumes the generated
ServicePlan/ChannelPlan boundary rather than interpreting cleanup plans at
runtime.

`cmeta_callable.capture` is treated as an immutable inline byte snapshot.
Pointers, descriptors, providers or code reachable through those bytes remain
borrowed external dependencies and must outlive every Graph/Plan use. Managed
resources must not be hidden in capture bytes expecting automatic destruction.

The production boundary remains direct C: `FlowMQ::FlowMQ` does not link
CFlow, production message paths do not perform reflected-function lookup, and
FlowMQ does not acquire a Plugin dependency merely for lifecycle abstraction.
If a future Plugin-backed provider is added, its dependent descriptors,
callables, ObjectRefs and managed values must die before the final explicit
Plugin lease is released.

### CFlow ordering qualification

FlowMQ 在 test/control plane 上用 CMeta exact-ABI Function 描述与 CFlow Graph 验证一条抽象
send ordering：

```text
VALIDATE
  -> ROUTE
  -> LOCAL_CAPACITY
  -> REMOTE_CREDIT
  -> ENCODE
  -> ADMIT
  -> CREDIT_COMMIT
  -> IO_SUBMIT
```

这些 stage 是规范模型，不是 production message executor。validation/encoding 可以 fallible，
peer/credit/admission/commit 对 mutable state 保守标记为 STATEFUL，CNet submission 是 IO
barrier。qualification test 必须证明：

- canonical ordering 能通过 exact CMeta ABI adapter 投影并完成；
- ADMIT-before-ENCODE、CREDIT_COMMIT-before-ADMIT、IO-before-credit-commit 等错误顺序被模型拒绝；
- normalization/optimization 不跨 STATEFUL/IO effect barrier 重排；
- contract-equivalent mock adapter 可以替换实现而不改变 Graph topology；
- incompatible reflected callable 在 projection admission 阶段被拒绝。

该测试是 standalone target，只链接 released `Salts::CFlow` / `Salts::TinyTest`。
`FlowMQ::FlowMQ` 不链接 CFlow，`flowmq_send/recv/poll` 不执行 Graph/Plan traversal，也不做
per-message reflection lookup。现有 runtime source gate 与 installed package consumer 继续
分别约束这两个 production boundary。

## Pattern 路由与事务边界

Core 的不可变 `flowmq_pattern_desc_t` 分别描述 routing、subscription、mute 和 FSM。
当前 runtime 的回复目的地选择与 `POLLOUT` 仍通过 `fsm_class == REP` 判定；descriptor
也提供 `routing_class == REPLY_PEER`。现有 schema 中二者对应同一种 socket，因此尚无
行为冲突，但路由判定与事务规则仍有耦合，copy/multipart/retained 的 peer 扫描也有重复。

2026-10-10 评估了私有只读选择函数 `flowmq_socket_select_round_robin_peer()`，并将回复
目的地判定改为读取 routing class。**该候选因性能回退尚未排除，未纳入生产实现**；实验
快照、测量数据与决定见 [pattern 路由评估](FLOWMQ_PATTERN_ROUTING.md)。后续拆分必须保留
以下 round-robin 路径的已有接纳语义，不能用同一种“选择可写 peer”策略替换所有路径：

| 路径 | 选择条件 | 后续提交边界 |
| --- | --- | --- |
| multipart 首帧 | 第一个 ready peer，可尚无完整消息 credit | 首帧 staging 成功后固定 generation；final 才检查整条消息 |
| 单帧 copy | ready 且 credit/HWM/有界队列可接纳 | 调用点保留原 cursor 更新时机，编码后执行 admission |
| 单帧 retained | copy 条件加空闲 write lane、空 outbound ring | 准备 retained frame 后执行即时 admission |

没有 ready peer 或 retained lane 暂忙时返回 `SALTS_EBUSY`；存在 ready 但容量不足的
候选时，扫描失败仍优先返回 `SALTS_ENOBUFS`。已固定的 multipart、回复 peer 和 ROUTER
identity route 不参加重新负载均衡，继续校验原 generation。PUB/XPUB 保留订阅快照与
final fanout 接纳规则。队列、credit 和 payload 的权威所有者仍是当前 socket/peer，所有
步骤由同一个 progress owner 串行执行。

CNet 负责连接与异步 IO；Acceptor–Connector/Proactor 对应的传输职责不会替代这些消息
pattern 规则。拆分候选使用静态策略分类，扫描为 O(peer capacity)、额外空间 O(1)，不增加
逐消息函数指针表或另一套可变路由状态，不涉及公开 API、wire format 或配置迁移。

行为证据来自 `test_flowmq_socket` / `test_flowmq_socket_pool`：新增 PUSH 双 peer 场景覆盖
copy/retained 单帧绕过耗尽 credit 的 peer、multipart final 拒绝后保持目的地、credit 恢复后的
完整重试和 part 边界；既有测试覆盖 REP slot generation、ROUTER 慢 peer 隔离及 PUB 快照。
`bench_flowmq_pattern_dispatch` 已注册到 CTest benchmark profile，可验证 PUB、PUSH、ROUTER、
REQ/REP 与三种 POLLOUT 场景。它逐样本驱动网络并校验 payload，测得时间包含 progress 和
接收，不是纯路由分支成本，也不能与 lane/batch 吞吐数据直接比较。

## Send 数据路径

```text
application message
  -> pattern FSM / peer selection
  -> local HWM + peer remote max_data admission
  -> FMQ/6 encode into socket-owned reusable scratch
  -> multipart parts retained in bounded socket-owned staging until final
  -> complete message transferred to peer-owned fixed descriptor ring
  -> single-part fast path admits a socket-owned buffer via cnet_send_buffer()
  -> queued frames form bounded retained slices via cnet_send_slicev()
  -> later caller-driven cnet_client_poll()
     or owner-lane cnet_client_advance_external/shared NativeIO observe
```

成功 admission 只表示本地 socket 已接管消息。连接建立、CNet write、远端接收与业务处理
是不同完成边界。`DONTWAIT` 在消息数或 payload byte HWM 满时立即返回 `SALTS_ENOBUFS`；
普通 send 在调用线程内推进所属 socket 后重试。单个 part 或完整 multipart 永远不可能装入
byte HWM 时返回 `SALTS_EMSGSIZE`。远端累计 credit 耗尽时返回 `SALTS_ENOBUFS`；远端应用
消费 DATA 并由其 owner 发送 FLOW_UPDATE 后恢复。multipart 的 credit 在 final part 时按完整
payload 一次提交，失败不会暴露或接纳部分 message。
PUB/XPUB 的 mute peer 按 ZeroMQ 语义丢弃，PUSH/DEALER/REQ 等模式不静默丢弃。

## Receive 数据路径

TCP 接收只有一个 owned stream；其他传输根据其公开接收入口取得所有权，不能在失败时
把 TCP DATA 重放进 borrowed decoder：

```text
TCP CNet owned receive
  -> complete prefix: retain original backing
     fragmented prefix: assemble into fixed 128 KiB owned blocks
  -> owned frame decoder
       DATA: project retained payload ranges
       control / empty DATA: decode exact frame metadata
                                            \
TLS / IPC / WS / WSS / KCP borrowed receive    -> one session/pattern frame handler
  -> transport-specific framing             /    -> per-peer multipart staging
  -> bounded copied stream decoder               -> complete message queue
                                                      |
                        +-----------------------------+--------------------+
                        |                             |                    |
                  flowmq_recv()                 flowmq_recv_slice()   flowmq_recv_slicev()
                  copy to caller                contiguous output    move payload ranges
```

接收所有权入口在第一次 read 前按传输能力选择，HELLO/SETTINGS 与 DATA 不切换路径。
TCP callback 不是 FMQ packet/frame 边界：碎片从进入流时就按固定块组装，不把 callback
数量作为消息可接收性的条件。原 backing 和 assembly block 都由 peer owned stream 持有；
只有尚未发布的尾块可以追加。投影或消费前封存尾块，inbound queue 和 caller 保留的 payload
不可被后续输入修改。peer 关闭释放 stream；应用返回的 slice 仍须在 socket close 前释放。

**容量（计算）：**`max_frame_size = 1 MiB`，canonical DATA 最多 16 个 packet，wire 上限为
`1 MiB + 16 × 32 = 1,049,088 bytes`。owned stream 的未消费 byte 上限为两倍该值，用于
容纳当前 frame 和同次回调尾部；固定 descriptor 上限为 32。单个最大 frame 的碎片组装
最多占九个 128 KiB 块，已有尾前缀最多额外一个范围；加上 packet payload 边界后最多
25 个投影范围，仍小于 32。完整输入也可直接保留一个 producer backing。达到硬边界或
分配失败就结束该 peer，不切换 decoder、不重放 DATA、不报告伪成功。普通接收 HWM 满
则保留已接管的事务并暂停 demand，与硬边界错误分开。

`flowmq_recv()` 从 retained ranges 直接复制到 caller，不先 flatten 到中间 buffer。
`flowmq_recv_slice()` 显式要求一个 contiguous canonical slice，因此多范围消息在消费
credit/FSM 前合并一次，失败不改变 queued message。`flowmq_recv_slicev()` 直接转移 queue
持有的范围；capacity 不足只报告 required count，不推进 pattern/FSM/credit。这三种输出
表达不同的调用方需求，合并输出不作为数据路径失败后的替代策略。

完整 multipart 提交仍同时检查可配置的消息数与 payload byte HWM；容量不足时停止该 peer 的
receive demand，应用消费并再次 `poll` 后恢复。只有真实 DATA payload 消耗 wire credit；
ROUTER routing-id 和 XPUB subscription event 是本地合成 part，不计 credit。应用成功取走 part
后增加累计 consumed_data，达到配置 quantum 或 deadline 后由下一次 owner progress 发布
FLOW_UPDATE。returned slices 的 backing 可以跨后续 caller-driven progress 持有，但必须由
caller `mem_slice_release()`。

## Pattern 状态

- REQ：`SEND_READY -> WAIT_REPLY -> SEND_READY`，非法 send/recv 与 multipart 方向交错立即
  返回 `SALTS_EPROTO`，不进入阻塞重试。
- REP：`RECV_READY -> SEND_REPLY -> RECV_READY`，reply 绑定最后一个 requester。
- PUSH/DEALER/REQ：eligible peer round-robin；peer busy 时不形成跨 peer HOL。
- PULL/SUB/DEALER/ROUTER：当前按网络完成顺序进入全局队列；严格 per-peer fair queue 尚未完成。
- PUB/XPUB：按 subscription prefix fan-out；每个 peer 独立 HWM/drop；SUB/XSUB 的动态
  subscribe/unsubscribe 通过每个 peer 的同步快照增量传播，新 session 从 socket desired
  subscription 集重放。
- ROUTER：receive 暴露 routing-id 首 part；send 消费 routing-id 首 part。
- multipart：`flowmq_send()` 的 `SNDMORE` parts 使用 socket-owned copied staging；
  `flowmq_send_slice()` 的 retained parts 使用 canonical slice staging。final part 对完整 payload、
  part/range slots、HWM/credit 和 peer generation 做一次 transaction admission。current `main`
  的 PUB/XPUB retained fanout 从第一 part 冻结 matching peer snapshot，final commit 给每个仍
  eligible peer 原子排入同一 shared retained publication 的一个 bounded reference；每个 peer
  后续独立提交 exactly one CNet logical retained vector，没有 SG-to-copy fallback。receiver
  仍只观察全部 parts 或完全不观察。

精确 compatibility matrix 和迁移边界见
[CNet TCP/TLS 与 ZeroMQ socket 模型](CNET_TCP_TLS_ARCHITECTURE.md)。

## Bind/connect primitive

旧的 connect/router callback endpoint 已删除，不再作为迁移层或公开事实源。新的 socket
runtime 直接拥有 CNet client/listener、peer registry、decoder、pattern FSM 与有界消息队列；
bind/connect 只决定连接方向，不决定消息模式。每次成功 `flowmq_connect()` 还建立一个固定
容量的 outbound endpoint 记录；初次 admission 失败会直接返回错误且不保留记录，后续异步
FAILED/CLOSED 由 CNet recovery policy 按失败类型、次数和截止时间决定是否重连。

## Shutdown

ordinary socket 的 `flowmq_close()` 先关闭 listener，再以有界 timeout 停止并销毁 CNet
client，随后释放本地 queue、decoder、route/session、TLS secret 与 pool。当前公开 API
没有 linger 或 drain 策略；未完成的本地消息随 socket close 取消。

owner-lane socket 必须通过 `flowmq_owner_close_socket()` 关闭。关闭过程先禁止新的 reconnect
schedule，清空 pending retry，再对该 socket 的 live peer 发起 close；owner 继续对**整个 shared
backend** 执行 advance/observe/route，因此其它 lane-local socket 不需要暂停。只有目标 client
达到 `cnet_client_stop_external() == SALTS_OK` 后才释放 socket storage。最后一个 socket
关闭后 `flowmq_owner_term()` 才允许 close/destroy shared NativeIO backend，并释放 context
owner lease。

任何阶段都不得在线程外隐藏 progress，也不得在 callback 仍可能访问 owner state 时释放资源。

## 验证

当前验证覆盖 TCP、verified TLS、IPC、WS/WSS 和普通及 PSK KCP。FMQ/6 回归覆盖
SETTINGS/FLOW_UPDATE、累计 credit 耗尽与恢复、
deadline 更新、ROUTER identity、multipart 接收原子可见性、REQ/REP FSM、
PUB/SUB filter/fan-out、动态 XPUB/XSUB subscription event 与 reconnect replay、TCP/TLS
listener restart 后的 caller-driven reconnect、发送/接收 message/byte HWM、
阻塞/DONTWAIT 分流、多 socket timeout poll、RCVMORE、peer failure isolation、session
generation fencing、owned/segmented receive、`recv_slicev` retained-vector ownership，以及
current-main retained PUB/XPUB atomic fanout。下一阶段继续覆盖严格 receive fair-queue、
可配置 shutdown 边界和更多 pattern benchmark。
