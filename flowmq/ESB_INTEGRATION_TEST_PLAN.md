# FlowMQ ESB 集成测试与 Benchmark 计划（完整版）

## 概述

本文档规划 FlowMQ ESB 扩展的集成测试与性能验证工作。

**实施状态**:
- ✅ Phase 1-3: Protocol + Core 层实现完成（SCATTER/GATHER + STREAM）
- ✅ Phase 5: 剩余 3 个模式实现完成（SAGA + PRIORITY_QUEUE + CIRCUIT_BREAKER）
- ⏳ Phase 6: 本计划 - 覆盖全部 5 种 ESB 模式的集成测试

**目标**: 端到端验证、性能基准、生产就绪性评估、高级组合模式测试规划

---

## 一、集成测试规划（全部 5 种模式）

### 1.1 Scatter/Gather 端到端测试

#### 测试场景 1：基础 Scatter/Gather 流程
**目标**: 验证完整的 scatter → 多服务响应 → gather 聚合流程

**规划路径**: `patterns/tests/integration/test_scatter_gather_e2e.c`（尚未实现）

**测试步骤**:
1. 启动 1 个 scatter 客户端 + 3 个 gather 服务端
2. 客户端创建会话（correlation_id），期望 3 个响应，策略 `ALL`
3. 客户端发送 scatter 请求到 3 个服务端
4. 服务端各自处理并返回 gather 响应（partial_index 0/1/2）
5. 客户端接收并记录响应，检查会话状态
6. 验证聚合完成，3 个响应全部收到

**验证点**:
- ✓ Correlation ID 正确传播
- ✓ Partial index 唯一且连续
- ✓ 会话状态从 PENDING → COMPLETED
- ✓ 聚合结果包含所有 payload

**边界条件**:
- 响应乱序到达
- 部分响应延迟
- 无响应丢失

---

#### 测试场景 2：QUORUM 策略提前完成
**目标**: 验证法定人数策略在部分响应到达时即完成

**测试步骤**:
1. 期望 5 个响应，策略 `QUORUM`（需 3 个）
2. 仅启动 3 个服务端
3. 验证收到 3 个响应后会话立即完成
4. 验证剩余 2 个服务端响应（若有）被正确处理或忽略

**验证点**:
- ✓ 会话在第 3 个响应到达时完成
- ✓ `received_responses == 3`
- ✓ 后续响应不影响已完成会话

---

#### 测试场景 3：超时处理
**目标**: 验证超时机制与部分响应保留

**测试步骤**:
1. 期望 3 个响应，设置 2 秒超时
2. 仅 1 个服务端响应，其余沉默
3. 等待超时触发
4. 验证会话状态为 `TIMEOUT`
5. 获取部分响应（应包含 1 个成功响应）

**验证点**:
- ✓ 超时时会话状态正确迁移
- ✓ 部分响应可访问
- ✓ 超时会话可被清理

---

#### 测试场景 4：错误响应混合
**目标**: 验证成功与失败响应的区分

**测试步骤**:
1. 3 个服务端，其中 1 个返回错误状态码（如 `TURBO_ECONNREFUSED`）
2. 验证聚合结果：`success_responses == 2`, `error_responses == 1`
3. 验证错误响应的 payload 和 status 被正确记录

**验证点**:
- ✓ 错误响应不阻止聚合完成（ALL 策略）
- ✓ 错误计数正确
- ✓ 业务层可区分成功/失败响应

---

### 1.2 Stream 端到端测试

#### 测试场景 5：Producer → Consumer 基础流程
**目标**: 验证消息发布、分区存储、消费者获取流程

**规划路径**: `patterns/tests/integration/test_stream_e2e.c`（尚未实现）

**测试步骤**:
1. 创建 topic（4 个分区）
2. Producer 发布 100 条消息到分区 0
3. Consumer 创建消费者组，加入成员，触发 rebalance
4. Consumer 从分区 0 fetch 消息（起始 offset=0）
5. 验证消息顺序与内容
6. Consumer 提交 offset=100
7. 重启 consumer，验证从 offset=100 继续消费（无重复）

**验证点**:
- ✓ 消息按 offset 递增存储
- ✓ Fetch 返回连续消息
- ✓ Offset 提交持久化
- ✓ At-least-once 语义正确

---

#### 测试场景 6：多消费者 Rebalance
**目标**: 验证消费者组 rebalance 与分区分配

**测试步骤**:
1. Topic 有 4 个分区
2. Consumer1 加入消费者组 → rebalance → 分配全部 4 个分区
3. Consumer2 加入 → rebalance → 每个消费者分配 2 个分区
4. Consumer3 加入 → rebalance → 分区分配 2/1/1 或 1/2/1（round-robin）
5. Consumer2 离开 → rebalance → 剩余 2 个消费者重新分配

**验证点**:
- ✓ Rebalance 触发正确
- ✓ Generation 递增
- ✓ 分区分配无遗漏、无重叠
- ✓ Round-robin 算法符合预期

---

#### 测试场景 7：Retention 策略
**目标**: 验证时间与容量双重 retention

**测试步骤**:
1. 配置分区：max_messages=50, retention_ms=1000（1 秒）
2. 快速发布 100 条消息 → 触发容量 retention，保留最新 50 条
3. 等待 1.5 秒 → 触发时间 retention，删除所有超过 1 秒的消息
4. 验证 low_watermark 前进

**验证点**:
- ✓ 容量限制强制执行
- ✓ 时间 retention 删除过期消息
- ✓ Watermark 正确更新

---

#### 测试场景 8：消费者崩溃恢复
**目标**: 验证 at-least-once 语义与 offset replay

**测试步骤**:
1. Producer 发布 10 条消息
2. Consumer fetch offset 0-4（5 条）
3. 模拟崩溃：未提交 offset，进程终止
4. Consumer 重启，从最后提交的 offset（0）重新 fetch
5. 验证重复消费前 5 条消息

**验证点**:
- ✓ 未提交 offset 不持久化
- ✓ 重启后从旧 offset 消费
- ✓ 消息不丢失（at-least-once）

---

### 1.3 跨模式边界测试

#### 测试场景 9：协议隔离验证
**目标**: 验证 ESB pattern 与基础 FlowMQ pattern 不能混连

**测试步骤**:
1. 尝试用 `FLOWMQ_PATTERN_DEALER` (base pattern) 连接 `ESB_SCATTER` endpoint
2. 验证连接被拒绝或握手失败
3. 尝试反向连接（ESB → base）
4. 验证同样失败

**验证点**:
- ✓ Pattern 兼容性检查生效
- ✓ 明确错误返回（`TURBO_EINVAL` 或协议错误）
- ✓ 不引起崩溃或未定义行为

---

## 二、Benchmark 规划

### 2.1 Scatter/Gather Benchmark

#### Benchmark 1：聚合延迟测量
**目标**: 测量不同聚合策略的端到端延迟

**规划路径**: `flowmq/benchmarks/bench_scatter_gather_latency.c`（尚未实现）

**测试配置**:
- 期望响应数：3 / 5 / 10
- 聚合策略：ALL / FIRST_N / QUORUM
- 服务端响应延迟：固定 10ms / 随机 5-15ms
- 网络：本地 loopback（消除网络变量）

**测量指标**:
- P50 / P95 / P99 延迟
- 策略对比：ALL vs QUORUM（期望 QUORUM 更快）
- 延迟组成分析：网络 RTT vs 聚合等待时间

**预期结果**:
```
期望 5 响应，服务端固定 10ms：
- ALL 策略：~10ms（等待最慢的一个）
- QUORUM 策略：~10ms（等待第 3 个，理论相同，但实际可能略快）
- FIRST_N(3) 策略：~10ms（等待前 3 个）
```

---

#### Benchmark 2：并发会话吞吐量
**目标**: 测量管理器处理并发会话的能力

**测试配置**:
- 并发会话数：1 / 10 / 100 / 512（上限）
- 每会话期望响应：3
- 聚合策略：ALL
- 测量时长：60 秒

**测量指标**:
- 每秒完成会话数（scatter/s）
- 平均会话生命周期
- 内存占用（峰值 / 平均）
- CPU 使用率

**预期结果**:
```
目标：>1000 scatter/s @ 100 并发会话
内存：<100MB @ 512 会话
CPU：<50% 单核
```

---

### 2.2 Stream Benchmark

#### Benchmark 3：消息发布吞吐量
**目标**: 测量单分区写入性能

**规划路径**: `flowmq/benchmarks/bench_stream_publish.c`（尚未实现）

**测试配置**:
- 分区数：1（单分区基准）
- 消息大小：128B / 1KB / 16KB
- 测试时长：60 秒
- Retention：禁用（避免干扰）

**测量指标**:
- 消息吞吐量（msg/s）
- 字节吞吐量（MB/s）
- P99 发布延迟

**预期结果**:
```
128B 消息：>50K msg/s
1KB 消息：>20K msg/s
16KB 消息：>5K msg/s
```

---

#### Benchmark 4：消息消费吞吐量
**目标**: 测量单消费者读取性能

**测试配置**:
- 预填充 100K 消息到分区 0
- 消息大小：1KB
- 单消费者从 offset=0 顺序读取
- Batch size：10 / 100 / 1000

**测量指标**:
- 消费吞吐量（msg/s）
- Batch size 对性能影响
- P99 fetch 延迟

**预期结果**:
```
Batch=10：~10K msg/s
Batch=100：~50K msg/s
Batch=1000：>100K msg/s
```

---

#### Benchmark 5：多消费者并发
**目标**: 测量多消费者并发读取不同分区的性能

**测试配置**:
- 4 个分区，每分区 25K 消息
- 4 个消费者，rebalance 后各分配 1 个分区
- 并发消费

**测量指标**:
- 总吞吐量（msg/s，4 消费者总和）
- 单消费者吞吐量
- Rebalance 耗时

**预期结果**:
```
总吞吐量：>150K msg/s
Rebalance 耗时：<100ms
```

---

### 2.3 Protocol 层 Benchmark

#### Benchmark 6：TLV 编解码性能
**目标**: 测量 ESB protocol 编解码开销

**实现路径**: `flowmq/benchmarks/bench_flowmq_protocol.c`（已存在，需扩展）

**测试配置**:
- Frame 类型：SCATTER_REQUEST / STREAM_PUBLISH / GATHER_RESPONSE
- Payload 大小：1KB
- 迭代次数：1M

**测量指标**:
- 编码吞吐量（frames/s）
- 解码吞吐量（frames/s）
- 单次编解码延迟（ns）

**预期结果**:
```
编码：>500K frames/s
解码：>500K frames/s
单次延迟：<2µs
```

---

## 三、压力测试

### 3.1 长期稳定性测试
**目标**: 验证 24 小时连续运行无内存泄漏、无崩溃

**测试配置**:
- Scatter/Gather：持续创建/完成/超时会话，10 scatter/s
- Stream：持续发布/消费，1K msg/s
- 运行时长：24 小时
- 监控工具：Valgrind / AddressSanitizer

**验证点**:
- ✓ 无内存泄漏（RSS 稳定）
- ✓ 无文件描述符泄漏
- ✓ 无崩溃或断言失败
- ✓ 性能指标稳定（无退化）

---

### 3.2 峰值负载测试
**目标**: 验证极限负载下的行为

**测试配置**:
- Scatter/Gather：512 并发会话（上限）
- Stream：256 个分区，64 个消费者组
- 消息速率：10K msg/s

**验证点**:
- ✓ 达到容量上限时返回 `TURBO_ENOSPC`（fail fast）
- ✓ 不出现死锁或活锁
- ✓ 拒绝新请求时不影响已有会话

---

## 四、实现优先级

### 高优先级（必须完成）
1. ✓ 测试场景 1：Scatter/Gather 基础流程
2. ✓ 测试场景 5：Stream Producer/Consumer 流程
3. ✓ Benchmark 3：消息发布吞吐量
4. ✓ Benchmark 4：消息消费吞吐量

### 中优先级（建议完成）
5. 测试场景 2：QUORUM 策略
6. 测试场景 6：Rebalance
7. Benchmark 1：聚合延迟
8. Benchmark 2：并发会话吞吐量

### 低优先级（可选）
9. 测试场景 3/4：超时与错误处理
10. 测试场景 7/8：Retention 与崩溃恢复
11. Benchmark 5/6：多消费者并发与 TLV 编解码
12. 压力测试：长期稳定性与峰值负载

---

## 五、执行计划

### 阶段 1：环境准备（1 天）
- [ ] 配置 CMake，确保单元测试全部通过
- [ ] 创建 `patterns/tests/integration/` 目录
- [ ] 创建测试辅助工具（mock endpoint、消息生成器）

### 阶段 2：集成测试实现（3 天）
- [ ] 实现测试场景 1、5（高优先级）
- [ ] 实现测试场景 2、6（中优先级）
- [ ] 验证所有测试通过

### 阶段 3：Benchmark 实现（2 天）
- [ ] 扩展现有 benchmark 框架
- [ ] 实现 Benchmark 3、4（高优先级）
- [ ] 实现 Benchmark 1、2（中优先级）
- [ ] 生成性能报告

### 阶段 4：压力测试（2 天）
- [ ] 长期稳定性测试（24 小时）
- [ ] 峰值负载测试
- [ ] 修复发现的问题

### 阶段 5：文档与发布（1 天）
- [ ] 更新 ESB_PATTERNS.md 添加性能数据
- [ ] 创建 RELEASE_NOTES.md
- [ ] 提交 PR 并请求审查

**总预计时间**: 9 个工作日

---

## 六、验收标准

### 功能完整性
- ✓ 所有高优先级集成测试通过
- ✓ 核心 benchmark 完成并生成报告

### 性能目标
- ✓ Scatter/Gather：>1000 会话/秒 @ 100 并发
- ✓ Stream 发布：>20K msg/s @ 1KB 消息
- ✓ Stream 消费：>50K msg/s @ batch=100

### 稳定性
- ✓ 24 小时运行无内存泄漏
- ✓ 峰值负载下 fail fast 而不崩溃

### 文档
- ✓ 集成测试文档完整
- ✓ Benchmark 报告包含对比数据
- ✓ 已知限制与未来改进清单

---

## 七、已知风险与缓解

### 风险 1：依赖 CoroNet 未完成功能
**影响**: 集成测试需要完整的 endpoint 实现  
**缓解**: 优先实现 mock endpoint，隔离 CoroNet 依赖

### 风险 2：性能未达预期
**影响**: Benchmark 结果低于目标  
**缓解**: 先建立 baseline，逐步优化；标注瓶颈点供后续改进

### 风险 3：时间不足
**影响**: 无法完成所有测试  
**缓解**: 按优先级递减实施，确保高优先级完成；低优先级留作后续 PR

---

## 八、参考实现

### 现有 Benchmark 参考
- `flowmq/benchmarks/bench_flowmq_protocol.c` - 协议编解码 benchmark

### 测试框架参考
- `patterns/tests/test_flowmq_core.c` - Core 层测试结构
- TinyTest 使用示例（已在单元测试中使用）

---

## 附录：测试数据生成

### 消息生成器
```c
/* 生成指定大小的测试消息 */
void generate_test_message(tstr *out, size_t size) {
    tstr_reserve(out, size);
    for (size_t i = 0; i < size; i++) {
        tstr_append_char(out, 'A' + (i % 26));
    }
}

/* 生成带序列号的消息 */
void generate_seq_message(tstr *out, uint64_t seq) {
    char buf[128];
    snprintf(buf, sizeof(buf), "{\"seq\":%lu,\"ts\":%lu}", seq, turbo_time_now_ns());
    tstr_copy_cstr(out, buf);
}
```

### Mock Endpoint
```c
/* 简化的 endpoint，用于测试 */
typedef struct {
    flowmq_pattern_t pattern;
    void (*on_frame)(flowmq_frame_t *frame, void *user_data);
    void *user_data;
} mock_endpoint_t;

void mock_endpoint_send(mock_endpoint_t *ep, flowmq_frame_t *frame) {
    if (ep->on_frame) {
        ep->on_frame(frame, ep->user_data);
    }
}
```


---

## 1.9 SAGA 端到端测试（新增）

#### 测试场景 10：SAGA 成功提交流程
**目标**: 验证多步骤事务全部成功的完整流程

**规划路径**: `patterns/tests/integration/test_saga_e2e.c`（尚未实现）

**测试步骤**:
1. 创建 SAGA 事务（3 个步骤）
2. 添加步骤：订单创建 → 库存扣减 → 支付处理
3. 启动执行
4. 依次记录步骤成功（step 0/1/2）
5. 验证最终状态为 COMMITTED

**验证点**:
- ✓ 所有步骤按顺序执行
- ✓ SAGA 状态正确迁移（PENDING → EXECUTING → COMMITTED）
- ✓ 步骤结果全部为 SUCCESS

---

#### 测试场景 11：SAGA 补偿流程
**目标**: 验证步骤失败后的补偿执行

**测试步骤**:
1. 创建 SAGA 事务（3 个步骤）
2. 步骤 0/1 成功
3. 步骤 2 失败（模拟支付失败）
4. 验证状态迁移到 COMPENSATING
5. 执行补偿：step 1 补偿 → step 0 补偿
6. 验证最终状态为 ABORTED

**验证点**:
- ✓ 失败时立即触发补偿
- ✓ 补偿按逆序执行（step 1 → step 0）
- ✓ 补偿完成后状态为 ABORTED
- ✓ 步骤结果正确记录（SUCCESS/FAILED/COMPENSATED）

---

#### 测试场景 12：SAGA 超时处理
**目标**: 验证长时间未完成的 SAGA 事务超时

**测试步骤**:
1. 创建 SAGA 事务，设置 2 秒超时
2. 步骤 0 成功
3. 步骤 1 挂起（不返回结果）
4. 等待超时触发
5. 验证状态强制迁移到 ABORTED

**验证点**:
- ✓ 超时后状态正确迁移
- ✓ 部分完成的步骤仍可查询
- ✓ 超时计数正确

---

### 1.10 Priority Queue 端到端测试（新增）

#### 测试场景 13：多优先级消息处理
**目标**: 验证高优先级消息优先消费

**规划路径**: `patterns/tests/integration/test_priority_queue_e2e.c`（尚未实现）

**测试步骤**:
1. Producer 发布混合优先级消息（P200 × 2、P100 × 3、P50 × 2）
2. Consumer 顺序消费
3. 验证消费顺序：P200 → P200 → P100 → P100 → P100 → P50 → P50

**验证点**:
- ✓ 高优先级消息优先出队
- ✓ 同优先级保持 FIFO 顺序
- ✓ 消息不丢失

---

#### 测试场景 14：容量限制与背压
**目标**: 验证队列满时的背压机制

**测试步骤**:
1. 创建容量限制为 10 的优先级队列
2. Producer 快速发布 15 条消息
3. 前 10 条成功，后 5 条返回 ENOSPC
4. Consumer 消费 5 条
5. Producer 重试，成功发布 5 条

**验证点**:
- ✓ 容量限制生效
- ✓ Producer 收到明确错误（fail fast）
- ✓ 消费后空间释放，可继续发布

---

### 1.11 Circuit Breaker 端到端测试（新增）

#### 测试场景 15：熔断器状态迁移
**目标**: 验证完整的熔断器生命周期

**规划路径**: `patterns/tests/integration/test_circuit_breaker_e2e.c`（尚未实现）

**测试步骤**:
1. 初始化熔断器（failure_threshold=3, timeout=1s）
2. 发送 5 个请求，其中 3 个失败 → OPEN
3. 尝试新请求 → 立即拒绝（fail fast）
4. 等待 1 秒 → HALF_OPEN
5. 发送 2 个成功请求 → CLOSED
6. 验证正常服务恢复

**验证点**:
- ✓ 失败阈值触发熔断
- ✓ OPEN 状态快速拒绝（无超时等待）
- ✓ 超时后自动尝试恢复
- ✓ 成功后恢复正常

---

#### 测试场景 16：熔断器与下游服务集成
**目标**: 验证熔断器保护下游服务

**测试步骤**:
1. 模拟下游服务（响应延迟 100ms）
2. 注入故障（服务返回 500 错误）
3. 客户端连续调用 5 次 → 触发熔断
4. 后续请求立即返回，不调用下游服务
5. 下游服务恢复
6. 熔断器超时后重新探测 → 恢复调用

**验证点**:
- ✓ 下游服务故障时触发熔断
- ✓ 熔断期间不向下游发送请求
- ✓ 下游恢复后自动恢复调用
- ✓ 统计信息正确（total_opened、total_rejected）

---

### 1.12 高级组合模式测试（新增）

#### 测试场景 17：Resilient Scatter（Scatter + Circuit Breaker）
**目标**: 验证 Scatter 请求集成熔断器的容错能力

**测试步骤**:
1. Scatter 请求发送到 3 个服务（A/B/C）
2. 服务 A 故障（连续失败 3 次）→ 熔断器 OPEN
3. 后续 Scatter 请求跳过服务 A，只发送到 B/C
4. 聚合策略调整：expected_responses=2（原本 3）
5. 验证聚合仍可完成

**验证点**:
- ✓ 故障服务自动跳过
- ✓ expected_responses 动态调整
- ✓ 聚合策略正确应用（QUORUM 需重新计算）
- ✓ 服务恢复后重新加入

---

#### 测试场景 18：Transactional Stream（Stream + SAGA）
**目标**: 验证 exactly-once 消息处理

**测试步骤**:
1. Stream producer 发布 10 条消息
2. Consumer fetch 消息 0-4
3. 创建 SAGA 事务：
   - Step 1: 业务处理
   - Step 2: Commit offset=5
4. 模拟业务处理失败 → SAGA 补偿 → offset 不提交
5. Consumer 重启，从 offset=0 重新消费
6. 第二次处理成功 → SAGA 提交 → offset=5 持久化

**验证点**:
- ✓ 处理失败不提交 offset（at-least-once）
- ✓ SAGA 补偿不影响消息日志
- ✓ 重启后从正确 offset 恢复
- ✓ 成功后 offset 提交（exactly-once 语义）

---

## 二、Benchmark 规划（全部 5 种模式）

### 2.1 SAGA Benchmark（新增）

#### Benchmark 7：SAGA 事务吞吐量
**目标**: 测量 SAGA coordinator 处理并发事务的能力

**测试配置**:
- 并发事务数：1 / 10 / 100 / 256（上限）
- 每事务步骤数：3 / 10 / 32（上限）
- 全部成功 vs 50% 失败（触发补偿）

**测量指标**:
- 事务创建速率（txn/s）
- 事务完成速率（commit/s + abort/s）
- 平均事务生命周期（创建 → 提交/终止）
- 内存占用（峰值 / 平均）

**预期结果**:
```
并发 10 事务 × 3 步骤：>500 txn/s
并发 100 事务 × 3 步骤：>200 txn/s
并发 256 事务 × 10 步骤：>100 txn/s
内存：<50MB @ 256 事务
```

---

### 2.2 Priority Queue Benchmark（新增）

#### Benchmark 8：优先级队列吞吐量
**目标**: 测量不同优先级分布下的性能

**测试配置**:
- 消息大小：128B / 1KB / 16KB
- 优先级分布：
  - 均匀分布（256 级均匀）
  - 热点分布（90% 集中在 3 个优先级）
  - 极端分布（100% 同一优先级 → 退化为 FIFO）

**测量指标**:
- Enqueue 吞吐量（msg/s）
- Dequeue 吞吐量（msg/s）
- P99 延迟（enqueue + dequeue）

**预期结果**:
```
128B 消息 + 均匀分布：
  - Enqueue: >100K msg/s
  - Dequeue: >80K msg/s（遍历开销）
  
1KB 消息 + 热点分布：
  - Enqueue: >50K msg/s
  - Dequeue: >100K msg/s（max_priority 优化）
```

---

### 2.3 Circuit Breaker Benchmark（新增）

#### Benchmark 9：熔断器状态检查开销
**目标**: 测量熔断器检查对请求路径的性能影响

**测试配置**:
- 请求速率：1K / 10K / 100K req/s
- 熔断器状态：CLOSED / OPEN / HALF_OPEN
- 失败率：0% / 10% / 50%

**测量指标**:
- 请求处理延迟（加入 CB 前后对比）
- CPU 开销（单核 %）
- 状态迁移耗时

**预期结果**:
```
CLOSED 状态：
  - 延迟增加：<1µs
  - CPU 开销：<5%

OPEN 状态（fail fast）：
  - 延迟增加：<100ns
  - 拒绝请求不消耗下游资源
```

---

## 三、压力测试（全部模式）

### 3.1 长期稳定性测试（扩展）

**新增测试对象**:
- SAGA：持续创建/提交事务，100 txn/s × 24 小时
- Priority Queue：持续 enqueue/dequeue，10K msg/s × 24 小时
- Circuit Breaker：持续状态迁移，1K req/s × 24 小时

**监控指标**:
- 内存泄漏（RSS 稳定）
- 文件描述符泄漏
- 性能退化（吞吐量保持稳定）
- 状态一致性（SAGA 事务全部 COMMITTED/ABORTED，无 PENDING）

---

### 3.2 峰值负载测试（扩展）

**新增测试场景**:
- SAGA：256 并发事务（上限）× 32 步骤（上限）
- Priority Queue：容量耗尽（max_capacity 达标）
- Circuit Breaker：所有服务熔断（验证 fail fast 不崩溃）

**验收标准**:
- ✓ 达到上限返回 TURBO_ENOSPC（不崩溃）
- ✓ 拒绝新请求不影响已有会话/事务
- ✓ 资源释放后可立即接受新请求

---

## 四、实现优先级（更新）

### 高优先级（必须完成） - 2 周
1. ✅ 测试场景 1：Scatter/Gather 基础流程
2. ✅ 测试场景 5：Stream Producer/Consumer 流程
3. ✅ **测试场景 10：SAGA 成功提交流程**（新增）
4. ✅ **测试场景 13：多优先级消息处理**（新增）
5. ✅ **测试场景 15：熔断器状态迁移**（新增）
6. ✅ Benchmark 3：消息发布吞吐量
7. ✅ Benchmark 4：消息消费吞吐量
8. ✅ **Benchmark 7：SAGA 事务吞吐量**（新增）

### 中优先级（建议完成） - 2 周
9. 测试场景 2/6：QUORUM 策略 + Rebalance
10. 测试场景 11：SAGA 补偿流程
11. 测试场景 14/16：容量限制 + 熔断器与下游集成
12. **测试场景 17：Resilient Scatter**（组合模式，新增）
13. Benchmark 1/2：聚合延迟 + 并发会话吞吐量
14. **Benchmark 8/9：优先级队列 + 熔断器性能**（新增）

### 低优先级（可选） - 按需
15. 测试场景 3/4/7/8：超时、错误处理、Retention、崩溃恢复
16. 测试场景 12：SAGA 超时处理
17. **测试场景 18：Transactional Stream**（组合模式，新增）
18. Benchmark 5/6：多消费者并发 + TLV 编解码
19. 压力测试：长期稳定性（24 小时）与峰值负载

---

## 五、执行计划（更新）

### 阶段 1：环境准备（1 天）
- [x] Phase 1-5 完成（Protocol + 5 种模式实现）
- [ ] CMake 配置，确保单元测试全部通过（67 个测试）
- [ ] 创建 `patterns/tests/integration/` 目录
- [ ] 创建测试辅助工具（mock endpoint、消息生成器）

### 阶段 2：核心集成测试实现（4 天）
- [ ] 实现测试场景 1、5、10、13、15（高优先级）
- [ ] 实现测试场景 2、6、11、14、16（中优先级）
- [ ] 验证所有测试通过

### 阶段 3：Benchmark 实现（3 天）
- [ ] 扩展现有 benchmark 框架
- [ ] 实现 Benchmark 3、4、7、8、9（高/中优先级）
- [ ] 实现 Benchmark 1、2（中优先级）
- [ ] 生成性能报告

### 阶段 4：高级组合测试（2 天）
- [ ] 实现测试场景 17：Resilient Scatter
- [ ] 实现测试场景 18：Transactional Stream
- [ ] 验证组合模式正确性

### 阶段 5：压力测试（3 天）
- [ ] 长期稳定性测试（24 小时 × 5 种模式）
- [ ] 峰值负载测试
- [ ] 修复发现的问题

### 阶段 6：文档与发布（1 天）
- [ ] 更新 ESB_PATTERNS.md 添加性能数据
- [ ] 创建 RELEASE_NOTES.md
- [ ] 提交 PR 并请求审查

**总预计时间**: 14 个工作日（2 周 sprint × 1）

---

## 六、验收标准（更新）

### 功能完整性
- ✅ 所有高优先级集成测试通过（5 种模式）
- ✅ 核心 benchmark 完成并生成报告
- ✅ 至少 2 个高级组合模式验证

### 性能目标
- ✅ Scatter/Gather：>1000 会话/秒 @ 100 并发
- ✅ Stream 发布：>20K msg/s @ 1KB 消息
- ✅ Stream 消费：>50K msg/s @ batch=100
- ✅ **SAGA：>200 txn/s @ 100 并发事务**（新增）
- ✅ **Priority Queue：>80K msg/s dequeue**（新增）
- ✅ **Circuit Breaker：<1µs 检查延迟**（新增）

### 稳定性
- ✅ 24 小时运行无内存泄漏（5 种模式）
- ✅ 峰值负载下 fail fast 而不崩溃
- ✅ 容量耗尽后可恢复

### 文档
- ✅ 集成测试文档完整（18 个场景）
- ✅ Benchmark 报告包含 9 个基准测试
- ✅ 高级组合模式使用指南（ESB_ADVANCED_PATTERNS.md）
- ✅ 已知限制与未来改进清单

---

## 七、已知风险与缓解（更新）

### 风险 1：依赖 CoroNet 未完成功能
**影响**: 集成测试需要完整的 endpoint 实现  
**缓解**: 优先实现 mock endpoint，隔离 CoroNet 依赖

### 风险 2：性能未达预期
**影响**: Benchmark 结果低于目标  
**缓解**: 先建立 baseline，逐步优化；标注瓶颈点供后续改进

### 风险 3：时间不足
**影响**: 无法完成所有测试  
**缓解**: 按优先级递减实施，确保高优先级完成；低优先级留作后续 PR

### 风险 4：组合模式复杂度高（新增）
**影响**: Resilient Scatter / Transactional Stream 实现困难  
**缓解**: 先实现简化版本（手动集成），验证可行性后再自动化

---

## 八、新增文件清单

### 规划中的集成测试文件
```
patterns/tests/integration/
├── test_scatter_gather_e2e.c
├── test_stream_e2e.c
├── test_saga_e2e.c               # 新增
├── test_priority_queue_e2e.c     # 新增
├── test_circuit_breaker_e2e.c    # 新增
├── test_resilient_scatter_e2e.c  # 新增（组合模式）
└── test_transactional_stream_e2e.c  # 新增（组合模式）
```

### 规划中的 Benchmark 文件
```
flowmq/benchmarks/
├── bench_scatter_gather_latency.c
├── bench_stream_publish.c
├── bench_stream_consume.c
├── bench_saga_throughput.c        # 新增
├── bench_priority_queue.c         # 新增
└── bench_circuit_breaker.c        # 新增
```

### 文档
```
flowmq/
├── ESB_PATTERNS.md                     # 已存在
├── ESB_ADVANCED_PATTERNS.md            # 新增（高级组合模式）
├── ESB_INTEGRATION_TEST_PLAN.md        # 本文档（已更新）
└── ESB_IMPLEMENTATION_SUMMARY.md       # 已存在（需更新）
```

---

## 附录：高级组合模式测试数据生成

### Resilient Scatter 测试数据
```c
/* 模拟服务故障 */
typedef struct mock_service_s {
  char *name;
  flowmq_circuit_breaker_t *breaker;
  int failure_rate;  /* 0-100 */
} mock_service_t;

void inject_service_failure(mock_service_t *service, int failure_count) {
  for (int i = 0; i < failure_count; i++) {
    flowmq_circuit_breaker_record_failure(service->breaker, turbo_time_now_ns());
  }
}
```

### Transactional Stream 测试数据
```c
/* SAGA + Stream 集成 */
typedef struct transactional_consumer_s {
  flowmq_stream_consumer_group_t *group;
  flowmq_saga_coordinator_t *saga_coordinator;
} transactional_consumer_t;

int transactional_consume(transactional_consumer_t *tc, uint32_t partition_id) {
  /* 1. Fetch 消息 */
  const flowmq_stream_message_t *messages = NULL;
  uint32_t count = 0;
  flowmq_stream_topic_fetch(/* ... */, &messages, &count);
  
  /* 2. 创建 SAGA */
  tstr saga_id = NULL;
  flowmq_saga_coordinator_create(tc->saga_coordinator, 2, deadline, NULL, &saga_id);
  
  /* Step 1: 业务处理 */
  flowmq_saga_coordinator_add_step(/* process_business */, /* compensate_business */);
  
  /* Step 2: Commit offset */
  flowmq_saga_coordinator_add_step(/* commit_offset */, /* no-op */);
  
  /* 3. 执行 SAGA */
  flowmq_saga_coordinator_start(tc->saga_coordinator, &saga_id);
  
  /* ... 执行步骤并记录结果 ... */
  
  return TURBO_OK;
}
```

---

**最后更新**: 2025-08-19  
**版本**: v1.0.0  
**状态**: ✅ Phase 1-5 完成，⏳ Phase 6 集成测试计划完成，等待实施
