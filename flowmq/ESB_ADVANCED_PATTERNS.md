# FlowMQ ESB 高级模式与组合设计

## 一、已实现模式总览

### 基础 FlowMQ Pattern (1-11)
1. **REQ/REP** - 请求/响应
2. **ROUTER/DEALER** - 路由/分发
3. **PUB/SUB** - 发布/订阅
4. **PUSH/PULL** - 管道
5. **PAIR** - 双向对等

### ESB Pattern (12-21) - 已完整实现
6. **SCATTER/GATHER** - 请求分散与响应聚合
7. **STREAM** - Kafka 风格分区流
8. **SAGA** - 分布式事务补偿
9. **PRIORITY_QUEUE** - 优先级队列
10. **CIRCUIT_BREAKER** - 熔断器

---

## 二、可组合的高级模式（基于现有实现）

### 2.1 组合模式：优先级 + 熔断器 = **Rate-Limited Priority Queue**

**设计理念**：为每个优先级级别配置独立的熔断器，防止高优先级消息耗尽所有资源。

**实现方案**：
```c
typedef struct flowmq_rate_limited_priority_queue_s {
  flowmq_priority_queue_t base_queue;                     /* 基础优先级队列 */
  flowmq_circuit_breaker_t breakers[FLOWMQ_PRIORITY_LEVELS]; /* 每级一个熔断器 */
  uint32_t rate_limits[FLOWMQ_PRIORITY_LEVELS];          /* 每级速率限制 */
} flowmq_rate_limited_priority_queue_t;
```

**应用场景**：
- API 网关限流（VIP 用户高优先级，但仍需限制）
- 多租户消息队列（租户优先级 + 租户配额）

**优势**：
- ✅ 防止优先级反转（高优先级消息触发熔断后降级）
- ✅ 公平性保障（低优先级消息不会完全饿死）

---

### 2.2 组合模式：Stream + SAGA = **Transactional Stream**

**设计理念**：Stream 消息消费与 SAGA 补偿结合，实现 exactly-once 语义。

**实现方案**：
```
1. Consumer fetch 消息
2. 创建 SAGA 事务（步骤：process + commit_offset）
3. 执行业务处理（SAGA step 1）
4. 提交 offset（SAGA step 2）
5. 若失败 → SAGA 补偿（回滚业务 + 不提交 offset）
```

**应用场景**：
- 金融交易流处理（消息消费与账户扣款原子性）
- 订单流处理（库存扣减与状态更新原子性）

**优势**：
- ✅ Exactly-once 语义（避免重复消费）
- ✅ 可观测性（SAGA 记录每步执行状态）

---

### 2.3 组合模式：SCATTER + Circuit Breaker = **Resilient Scatter**

**设计理念**：Scatter 请求到多个服务时，为每个服务配置独立熔断器。

**实现方案**：
```c
typedef struct flowmq_resilient_scatter_s {
  flowmq_scatter_gather_manager_t scatter_manager;
  turbo_hash_map_t service_breakers;  /* Map<service_name -> circuit_breaker> */
} flowmq_resilient_scatter_t;
```

**逻辑**：
```
1. Scatter 请求前检查目标服务熔断器
2. 若 OPEN → 跳过该服务，减少 expected_responses
3. 若 CLOSED → 发送请求
4. 根据响应结果更新熔断器状态
```

**应用场景**：
- 多数据中心查询（某个数据中心故障时自动跳过）
- 微服务聚合查询（服务 A 挂了仍可从 B/C 获取数据）

**优势**：
- ✅ 自动降级（故障服务不影响整体聚合）
- ✅ 快速失败（不等待超时）

---

### 2.4 扩展模式：Stream + Priority Queue = **Priority Stream**

**设计理念**：Stream 分区内消息按优先级排序，而非严格 FIFO。

**实现方案**：
```c
typedef struct flowmq_priority_stream_partition_s {
  uint32_t partition_id;
  flowmq_priority_queue_t priority_queue;  /* 替代 deque */
  uint64_t high_watermark;
} flowmq_priority_stream_partition_t;
```

**挑战**：
- ⚠️ Offset 语义变化（不再是顺序递增）
- ⚠️ Rebalance 复杂度（需考虑优先级分布）

**应用场景**：
- 实时告警流（P0 > P1 > P2）
- 任务调度流（紧急任务优先执行）

---

### 2.5 扩展模式：SAGA + Scatter/Gather = **SAGA with Parallel Steps**

**设计理念**：SAGA 某个步骤内部使用 Scatter/Gather 并行调用多个服务。

**实现方案**：
```
SAGA Step 1: 创建订单（单服务）
SAGA Step 2: 并行库存扣减（Scatter 到 3 个仓库）
  - Gather 策略：QUORUM（至少 2 个成功）
SAGA Step 3: 支付（单服务）

失败补偿：
Step 3 失败 → 补偿 Step 2（Scatter 释放库存）→ 补偿 Step 1（取消订单）
```

**应用场景**：
- 跨数据中心 SAGA（并行提交多个数据中心）
- 复杂业务流程（某步需要多方确认）

**优势**：
- ✅ 提升吞吐量（并行执行子步骤）
- ✅ 容错能力（QUORUM 允许部分失败）

---

### 2.6 新模式：**Dead Letter Queue (DLQ)** - 基于 Priority Queue

**设计理念**：失败消息自动路由到 DLQ，避免阻塞正常消息。

**实现方案**：
```c
typedef struct flowmq_dlq_s {
  flowmq_priority_queue_t main_queue;
  flowmq_priority_queue_t dead_letter_queue;
  uint32_t max_retries;
  uint64_t retry_delay_ms;
} flowmq_dlq_t;
```

**逻辑**：
```
1. Consumer dequeue 消息
2. 处理失败 → 重新 enqueue（retry_count++）
3. retry_count >= max_retries → 移动到 DLQ
4. DLQ 消息可手动重试或定期清理
```

**应用场景**：
- 消息队列（失败消息不阻塞后续）
- 事件处理（毒消息隔离）

---

### 2.7 新模式：**Request Coalescing** - 基于 SCATTER + 时间窗口

**设计理念**：短时间内多个相同请求合并为一个 Scatter 请求。

**实现方案**：
```c
typedef struct flowmq_request_coalescer_s {
  turbo_hash_map_t pending_requests;  /* Map<request_key -> request_batch> */
  uint64_t coalesce_window_ms;        /* 合并窗口（如 100ms） */
  flowmq_scatter_gather_manager_t scatter_manager;
} flowmq_request_coalescer_t;
```

**逻辑**：
```
1. 请求到达，检查 pending_requests[key]
2. 若窗口内已有相同请求 → 加入 batch
3. 窗口结束 → 发送单个 Scatter 请求
4. 响应到达 → 分发给 batch 中所有等待者
```

**应用场景**：
- 缓存查询（多个客户端同时查同一 key）
- 数据库批量查询（减少 DB 压力）

**优势**：
- ✅ 减少后端调用（N 个请求 → 1 个 Scatter）
- ✅ 降低延迟（避免串行等待）

---

### 2.8 新模式：**Throttling Queue** - 基于 Priority Queue + Token Bucket

**设计理念**：消息出队受令牌桶限流控制。

**实现方案**：
```c
typedef struct flowmq_throttling_queue_s {
  flowmq_priority_queue_t queue;
  uint64_t tokens;              /* 当前令牌数 */
  uint64_t max_tokens;          /* 令牌桶容量 */
  uint64_t refill_rate_per_sec; /* 令牌补充速率 */
  uint64_t last_refill_ns;      /* 上次补充时间 */
} flowmq_throttling_queue_t;
```

**逻辑**：
```
dequeue:
  1. 计算并补充令牌（elapsed_time * refill_rate）
  2. 若 tokens < 1 → 返回 TURBO_EAGAIN（稍后重试）
  3. 若 tokens >= 1 → dequeue + tokens--
```

**应用场景**：
- API 限流（保护下游服务）
- 流量整形（平滑突发流量）

---

### 2.9 新模式：**Content-Based Router** - 基于 PUB/SUB + 表达式引擎

**设计理念**：根据消息内容动态路由到不同订阅者。

**实现方案**：
```c
typedef struct flowmq_content_router_s {
  turbo_vec_t subscriptions;  /* Vec<subscription_rule> */
} flowmq_content_router_t;

typedef struct subscription_rule_s {
  tstr_t expression;  /* 如 "price > 100 AND region == 'US'" */
  void *subscriber;
} subscription_rule_t;
```

**逻辑**：
```
1. Publisher 发送消息（JSON payload）
2. Router 遍历所有订阅规则
3. 评估表达式（使用 exprtk 引擎）
4. 匹配成功 → 转发给订阅者
```

**应用场景**：
- 事件总线（按事件类型/属性路由）
- 告警系统（按严重级别/来源过滤）

---

### 2.10 新模式：**Batch Aggregator** - 基于 Stream + 时间/大小窗口

**设计理念**：Stream 消息按批次聚合后统一处理。

**实现方案**：
```c
typedef struct flowmq_batch_aggregator_s {
  turbo_vec_t current_batch;
  uint32_t batch_size;        /* 大小触发（如 100 条）*/
  uint64_t batch_timeout_ms;  /* 时间触发（如 5 秒） */
  uint64_t batch_start_ns;
} flowmq_batch_aggregator_t;
```

**逻辑**：
```
1. 消息到达，加入 current_batch
2. 检查触发条件：
   - batch.size >= batch_size OR
   - now - batch_start_ns >= batch_timeout_ms
3. 触发 → 批量处理 + 清空 batch
```

**应用场景**：
- 日志聚合（批量写入 Elasticsearch）
- 数据库批量插入（减少网络开销）

---

## 三、实现优先级建议

### 高优先级（立即实施）
1. **Dead Letter Queue** - 生产环境必备，提升健壮性
2. **Resilient Scatter** - 与现有 Scatter/CB 集成，改动小
3. **Transactional Stream** - 解决 exactly-once 需求

### 中优先级（v1.1 实施）
4. **Rate-Limited Priority Queue** - 多租户场景需求
5. **Request Coalescing** - 性能优化显著
6. **Throttling Queue** - API 网关限流

### 低优先级（按需实施）
7. **Priority Stream** - 语义复杂，需求不明确
8. **SAGA with Parallel Steps** - 复杂业务场景
9. **Content-Based Router** - 需要表达式引擎支持
10. **Batch Aggregator** - 通用需求，可独立实现

---

## 四、技术实现挑战

### 4.1 性能挑战
- **Request Coalescing**: 需要高效的请求去重（hash map + 时间窗口）
- **Priority Stream**: Offset 管理变复杂（需要优先级索引）

### 4.2 语义挑战
- **Transactional Stream**: SAGA 失败后如何处理已消费但未提交的消息
- **Resilient Scatter**: 动态调整 expected_responses 可能导致聚合策略失效

### 4.3 资源挑战
- **Rate-Limited PQ**: 256 个熔断器 × N 个队列 = 内存占用大
- **Content-Based Router**: 表达式评估 CPU 开销

---

## 五、与竞品对比

| 模式 | FlowMQ ESB | Kafka | RabbitMQ | Apache Camel |
|------|------------|-------|----------|--------------|
| DLQ | 可实现 | ✓ | ✓ | ✓ |
| Content Router | 可实现 | ✗ | ✗ (需插件) | ✓ |
| Request Coalescing | 可实现 | ✗ | ✗ | ✓ |
| Throttling | 可实现 | ✓ (quota) | ✓ (rate limit) | ✓ |
| Transactional Stream | 可实现 | ✓ (原生) | ✗ | ⚠️ (复杂) |

**FlowMQ ESB 优势**：
- C API 低开销（适合嵌入式/高性能场景）
- 模式组合灵活（SAGA + Scatter 无缝集成）
- 统一协议栈（不需要多种中间件）

---

## 六、实现路线图

### Phase 7: Dead Letter Queue (1 周)
- [ ] `flowmq_dlq.h/c` - DLQ 核心实现
- [ ] 单元测试（10+ 测试）
- [ ] 文档与示例

### Phase 8: Resilient Scatter (1 周)
- [ ] 扩展 `flowmq_scatter_gather.c` 集成 CB
- [ ] 动态 expected_responses 调整
- [ ] 端到端测试

### Phase 9: Transactional Stream (2 周)
- [ ] SAGA + Stream 集成层
- [ ] Exactly-once 语义测试
- [ ] 性能 benchmark

### Phase 10: 其他高级模式（按需）
- 根据用户反馈与实际需求决定优先级

---

## 七、参考资料

### 企业集成模式
- [Enterprise Integration Patterns](https://www.enterpriseintegrationpatterns.com/)
  - Message Router、Content-Based Router、Aggregator、Dead Letter Channel
  
### 实现参考
- Apache Camel - 60+ EIP 实现
- Spring Integration - Java DSL for EIP
- MassTransit - .NET 消息框架

### 学术论文
- SAGA Pattern: [Sagas (1987)](https://www.cs.cornell.edu/andru/cs711/2002fa/reading/sagas.pdf)
- Circuit Breaker: [Release It! (Michael Nygard)](https://pragprog.com/titles/mnee2/release-it-second-edition/)
