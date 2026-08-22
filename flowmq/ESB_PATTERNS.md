# FlowMQ ESB 消息模式

## 概述

FlowMQ ESB（Enterprise Service Bus）扩展为 FlowMQ 引入了五种企业级消息模式，用于支持微服务架构中的复杂交互场景。这些模式基于 FlowMQ v3 协议，通过 TLV（Type-Length-Value）编码扩展 payload，保持与基础 FlowMQ 协议的向后兼容性。

### 设计原则

1. **Fail Fast** - 前置条件、状态不变量或资源约束不满足时立即返回明确错误，不自动降级
2. **明确所有权** - 每个业务状态只有一个主事实源，缓存和派生状态从主事实源推导
3. **无隐式 Fallback** - 只有协议或设计文档明确要求时才允许回退逻辑
4. **协议隔离** - ESB pattern（12-31）与基础 FlowMQ pattern（1-11）禁止跨类型连接

### ESB Pattern 范围

```c
/* ESB Pattern 范围：12-31 */
typedef enum {
    /* Scatter/Gather */
    FLOWMQ_PATTERN_ESB_SCATTER      = 12,
    FLOWMQ_PATTERN_ESB_GATHER       = 13,
    
    /* SAGA 分布式事务 */
    FLOWMQ_PATTERN_ESB_SAGA_COORDINATOR = 14,
    FLOWMQ_PATTERN_ESB_SAGA_PARTICIPANT = 15,
    
    /* Stream 流处理 */
    FLOWMQ_PATTERN_ESB_STREAM_PRODUCER  = 16,
    FLOWMQ_PATTERN_ESB_STREAM_CONSUMER  = 17,
    
    /* Priority Queue */
    FLOWMQ_PATTERN_ESB_PRIORITY_PRODUCER = 18,
    FLOWMQ_PATTERN_ESB_PRIORITY_CONSUMER = 19,
    
    /* Circuit Breaker */
    FLOWMQ_PATTERN_ESB_CB_CLIENT  = 20,
    FLOWMQ_PATTERN_ESB_CB_SERVICE = 21,
} flowmq_pattern_t;
```

---

## 一、SCATTER/GATHER 模式

### 1.1 概述

Scatter/Gather 模式用于将单个请求分发到多个服务实例，并聚合它们的响应。适用于：
- 并行查询多个数据源
- 广播命令到多个执行器
- 冗余服务调用以提升可用性

### 1.2 协议设计

#### Frame Kind
```c
FLOWMQ_FRAME_KIND_ESB_SCATTER_REQUEST  = 7   /* Scatter 请求 */
FLOWMQ_FRAME_KIND_ESB_GATHER_RESPONSE  = 8   /* Gather 部分响应 */
FLOWMQ_FRAME_KIND_ESB_GATHER_COMPLETE  = 9   /* Gather 聚合完成 */
```

#### TLV 字段
- `EXPECTED_RESPONSES` (Type=1): 期望响应数量（uint32）
- `AGGREGATION_POLICY` (Type=2): 聚合策略（uint8）
  - `FLOWMQ_GATHER_ALL = 0`: 等待所有响应
  - `FLOWMQ_GATHER_FIRST_N = 1`: 先到达 N 个响应
  - `FLOWMQ_GATHER_QUORUM = 2`: 达到法定人数（N/2 + 1）
- `PARTIAL_INDEX` (Type=3): 部分响应索引（uint32）

#### 状态机
```c
typedef enum {
    FLOWMQ_SCATTER_PENDING    = 0,  /* 等待响应 */
    FLOWMQ_SCATTER_COMPLETED  = 1,  /* 聚合完成 */
    FLOWMQ_SCATTER_TIMEOUT    = 2,  /* 超时 */
    FLOWMQ_SCATTER_CANCELLED  = 3,  /* 已取消 */
} flowmq_scatter_session_state_t;
```

### 1.3 使用示例

```c
#include <flowmq.h>
#include <flowmq_scatter_gather.h>

/* 初始化 Scatter/Gather 管理器 */
flowmq_scatter_gather_manager_t manager;
flowmq_scatter_gather_manager_init(&manager, 512, 5000);  /* 512 并发会话，5s 超时 */

/* 创建 scatter 会话 */
uint64_t correlation_id = 0;
uint64_t deadline_ns = turbo_time_now_ns() + 5000000000ULL;  /* 5 秒后超时 */
flowmq_scatter_gather_create_session(
    &manager,
    3,                        /* 期望 3 个响应 */
    FLOWMQ_GATHER_QUORUM,     /* 法定人数策略（至少 2 个） */
    deadline_ns,
    NULL,                     /* 用户上下文 */
    &correlation_id
);

/* 发送 scatter 请求到多个服务 */
flowmq_frame_t scatter_frame;
flowmq_protocol_esb_encode_scatter(
    &scatter_frame,
    correlation_id,
    3,                        /* expected_responses */
    FLOWMQ_GATHER_QUORUM,
    payload,
    topic
);
/* 发送到多个 endpoint... */

/* 接收部分响应 */
flowmq_frame_t response_frame;
/* 从 endpoint 接收... */

uint32_t partial_index = 0;
tstr response_payload = NULL;
flowmq_protocol_esb_decode_gather_response(&response_frame, &partial_index, &response_payload);

/* 记录响应 */
flowmq_scatter_gather_record_response(
    &manager,
    correlation_id,
    partial_index,
    &response_payload,
    TURBO_OK,
    turbo_time_now_ns()
);

/* 检查会话状态 */
flowmq_scatter_session_state_t state;
flowmq_scatter_gather_check_session(&manager, correlation_id, &state);

if (state == FLOWMQ_SCATTER_COMPLETED) {
    /* 聚合完成，获取最终结果 */
    flowmq_scatter_session_t session;
    flowmq_scatter_gather_finalize_session(&manager, correlation_id, &session);
    
    printf("Received %u/%u responses (%u success, %u errors)\n",
           session.received_responses, session.expected_responses,
           session.success_responses, session.error_responses);
    
    /* 处理聚合结果... */
    for (uint32_t i = 0; i < session.received_responses; i++) {
        printf("Response[%u]: %.*s (status=%d)\n",
               session.responses[i].index,
               (int)session.responses[i].payload.len,
               session.responses[i].payload.ptr,
               session.responses[i].status);
    }
    
    flowmq_scatter_session_cleanup(&session);
}

/* 处理超时 */
uint32_t timed_out = flowmq_scatter_gather_process_timeouts(&manager, turbo_time_now_ns(), NULL, NULL);

flowmq_scatter_gather_manager_destroy(&manager);
```

### 1.4 性能特性

- **并发会话上限**: 512 个（可配置，最大 512）
- **聚合策略开销**:
  - `ALL`: 必须等待所有响应，延迟 = max(所有响应时间)
  - `FIRST_N`: 延迟 = 第 N 个响应到达时间
  - `QUORUM`: 延迟 = 第 (N/2+1) 个响应到达时间
- **内存占用**: 每会话约 200 字节 + 响应 payload 总和

### 1.5 错误处理

- **部分失败**: 聚合策略达成时完成，即使部分响应失败（记录在 `error_responses` 中）
- **超时**: 未达成聚合策略前超时，状态迁移到 `TIMEOUT`，可获取部分响应
- **重复响应**: 相同 `partial_index` 的重复响应被拒绝，返回 `TURBO_EALREADY`
- **容量耗尽**: 并发会话达到上限时，创建失败返回 `TURBO_ENOSPC`

---

## 二、STREAM 模式

### 2.1 概述

Stream 模式提供 Kafka 风格的分区消息流，支持：
- 有序消息日志与 offset 追踪
- 消费者组与分区 rebalance
- At-least-once 投递语义
- 时间/容量双重 retention 策略

### 2.2 协议设计

#### Frame Kind
```c
FLOWMQ_FRAME_KIND_ESB_STREAM_PUBLISH = 10  /* 发布消息到分区 */
FLOWMQ_FRAME_KIND_ESB_STREAM_FETCH   = 11  /* 从分区获取消息 */
FLOWMQ_FRAME_KIND_ESB_STREAM_COMMIT  = 12  /* 提交消费 offset */
```

#### TLV 字段
- `PARTITION_ID` (Type=7): 分区 ID（uint32）
- `OFFSET` (Type=8): 消息 offset（uint64）
- `CONSUMER_GROUP` (Type=9): 消费者组 ID（string）

#### 数据结构
```c
/* Topic 配置 */
typedef struct {
    uint32_t partition_count;        /* 分区数量（最多 256） */
    uint32_t max_messages_per_part;  /* 每分区最大消息数 */
    uint64_t max_bytes_per_part;     /* 每分区最大字节数 */
    uint64_t retention_ms;           /* 保留时间（毫秒，0=永久） */
} flowmq_stream_config_t;

/* 消费者组 */
typedef struct {
    tstr group_id;                 /* 组 ID */
    uint64_t generation;             /* Rebalance 世代 */
    turbo_set_t members;             /* 成员列表（最多 32） */
    turbo_hash_map_t committed_offsets;  /* 已提交 offset */
    int rebalancing;                 /* 是否正在 rebalance */
} flowmq_stream_consumer_group_t;
```

### 2.3 使用示例

#### 2.3.1 Producer

```c
#include <flowmq_stream_partition.h>

/* 初始化 topic */
flowmq_stream_topic_t topic;
flowmq_stream_topic_init(
    &topic,
    "events",         /* topic 名称 */
    4,                /* 4 个分区 */
    10000,            /* 每分区最多 10000 条消息 */
    100 * 1024 * 1024,  /* 每分区最多 100MB */
    3600000           /* 保留 1 小时 */
);

/* 发布消息到分区 0 */
tstr payload = NULL;
tstr topic_name = NULL;
tstr_copy_cstr(&payload, "{\"event\":\"user_login\",\"user_id\":123}");
tstr_copy_cstr(&topic_name, "events");

uint64_t offset = 0;
int rc = flowmq_stream_topic_publish(
    &topic,
    0,                    /* partition_id */
    &payload,             /* 消息 payload（所有权转移） */
    &topic_name,          /* topic 名称（所有权转移） */
    turbo_time_now_ns(),  /* timestamp */
    &offset               /* 返回分配的 offset */
);

if (rc == TURBO_OK) {
    printf("Published to partition 0, offset=%lu\n", offset);
} else if (rc == TURBO_ENOSPC) {
    printf("Partition full, apply retention or increase capacity\n");
}

/* 应用 retention 策略 */
uint32_t removed = flowmq_stream_partition_apply_retention(
    &topic.partitions[0],
    turbo_time_now_ns()
);
printf("Removed %u expired messages\n", removed);

flowmq_stream_topic_destroy(&topic);
```

#### 2.3.2 Consumer

```c
/* 获取或创建消费者组 */
flowmq_stream_consumer_group_t *group = NULL;
flowmq_stream_topic_get_consumer_group(&topic, "analytics-group", &group);

/* 加入消费者组 */
flowmq_stream_consumer_group_join(group, "consumer-1", turbo_time_now_ns());

/* 触发 rebalance（分配分区） */
flowmq_stream_consumer_group_rebalance(group, topic.partition_count);

/* 获取分配的分区（示例：假设分到分区 0 和 1） */
uint32_t assigned_partitions[] = {0, 1};
uint32_t num_assigned = 2;

/* 从分区消费 */
for (uint32_t i = 0; i < num_assigned; i++) {
    uint32_t partition_id = assigned_partitions[i];
    
    /* 获取已提交 offset */
    uint64_t committed_offset = 0;
    flowmq_stream_consumer_group_get_offset(group, partition_id, &committed_offset);
    
    /* Fetch 消息（从 committed_offset 开始） */
    const flowmq_stream_message_t *messages = NULL;
    uint32_t count = 0;
    flowmq_stream_topic_fetch(
        &topic,
        partition_id,
        committed_offset,  /* 起始 offset */
        100,               /* 最多获取 100 条 */
        &messages,
        &count
    );
    
    /* 处理消息 */
    for (uint32_t j = 0; j < count; j++) {
        printf("[P%u O%lu] %.*s\n",
               partition_id, messages[j].offset,
               (int)messages[j].payload.len, messages[j].payload.ptr);
        
        /* 业务处理... */
    }
    
    /* 提交新 offset（at-least-once：处理后提交） */
    if (count > 0) {
        uint64_t new_offset = messages[count - 1].offset + 1;
        flowmq_stream_consumer_group_commit(group, partition_id, new_offset);
    }
}

/* 离开消费者组 */
flowmq_stream_consumer_group_leave(group, "consumer-1");
```

### 2.4 性能特性

- **分区数上限**: 256 个
- **消费者组上限**: 64 个（每 topic）
- **消费者/组上限**: 32 个成员
- **并发写入**: 每分区单线程写入（append-only）
- **并发读取**: 多消费者可并发读取不同分区
- **Rebalance 算法**: Round-robin 分配
- **Retention 开销**: O(已删除消息数)，使用 deque 弹出头部

### 2.5 投递语义

#### At-Least-Once（默认）
1. Consumer fetch 消息
2. 处理消息
3. Commit offset
4. 若步骤 2-3 间崩溃，重启后从旧 offset 重新消费（重复投递）

#### At-Most-Once（需应用层实现）
1. Consumer fetch 消息
2. **先 commit offset**
3. 处理消息
4. 若步骤 3 崩溃，消息丢失

#### Exactly-Once（不支持）
需要外部事务协调器（如 SAGA 模式）实现幂等处理。

### 2.6 错误处理

- **分区满**: 发布失败返回 `TURBO_ENOSPC`，需应用 retention 或扩容
- **无效分区 ID**: 返回 `TURBO_ERANGE`
- **Offset 超出范围**: Fetch 返回空结果（count=0）
- **消费者重复加入**: 返回 `TURBO_EALREADY`
- **Rebalance 期间**: 消费者应停止 fetch，等待 rebalance 完成

---

## 三、其他 ESB 模式（保留设计）

### 3.1 SAGA 分布式事务

**Frame Kind**: 13-15  
**TLV 字段**: `SAGA_ID`, `SAGA_STEP`, `SAGA_STATE`

用于实现分布式事务的补偿机制。Coordinator 协调多个 Participant 执行事务步骤，失败时按逆序执行补偿操作。

**状态机**:
```
SAGA_PENDING → SAGA_COMMITTED (所有步骤成功)
             → SAGA_COMPENSATING (部分失败，执行补偿)
             → SAGA_ABORTED (补偿完成)
```

### 3.2 Priority Queue

**Frame Kind**: 16-17  
**TLV 字段**: `PRIORITY`

基于优先级的消息队列。Producer 指定优先级（0-255），Consumer 按优先级从高到低消费。

**实现建议**: 使用堆（heap）或多级队列（per-priority queue）。

### 3.3 Circuit Breaker

**Frame Kind**: 18-19  
**TLV 字段**: `CB_STATE`, `FAILURE_COUNT`

熔断器模式，用于保护服务免受级联失败影响。Client 侧监控失败率，超过阈值时打开熔断器，拒绝后续请求。

**状态机**:
```
CLOSED (正常) → OPEN (熔断) → HALF_OPEN (探测) → CLOSED/OPEN
```

---

## 四、部署架构

### 4.1 典型拓扑

#### 单 Broker 拓扑
```
┌──────────┐
│ Producer │──┐
└──────────┘  │     ┌────────────────┐
              ├────→│  FlowMQ Broker │
┌──────────┐  │     │  (ESB Patterns)│
│ Consumer │──┘     └────────────────┘
└──────────┘              │
                          ↓
                   ┌─────────────┐
                   │  State      │
                   │  - Sessions │
                   │  - Partitions│
                   │  - Consumers│
                   └─────────────┘
```

#### 集群拓扑（未实现，规划）
```
        ┌─────────┐      ┌─────────┐
        │ Broker1 │←────→│ Broker2 │
        └─────────┘      └─────────┘
             ↑                ↑
             │                │
     ┌───────┴────────────────┴───────┐
     │                                │
┌─────────┐                      ┌─────────┐
│Producer │                      │Consumer │
└─────────┘                      └─────────┘
```

### 4.2 配置示例

```yaml
# fmq.yml - ESB 配置
server:
  endpoints:
    - uri: "tcp://0.0.0.0:5555"
      pattern: ESB_SCATTER
      security: PLAIN
    - uri: "tcp://0.0.0.0:5556"
      pattern: ESB_STREAM_PRODUCER
      security: CURVE

scatter_gather:
  max_sessions: 512
  default_timeout_ms: 5000

stream:
  topics:
    - name: "events"
      partitions: 8
      max_messages_per_partition: 100000
      max_bytes_per_partition: 1073741824  # 1GB
      retention_ms: 86400000  # 24 hours
    
    - name: "logs"
      partitions: 4
      max_messages_per_partition: 50000
      retention_ms: 3600000  # 1 hour

  consumer_groups:
    max_groups: 64
    max_members_per_group: 32
    session_timeout_ms: 30000
    rebalance_timeout_ms: 60000
```

### 4.3 监控指标

#### Scatter/Gather
- `scatter_active_sessions`: 当前活跃会话数
- `scatter_completed_total`: 完成会话总数
- `scatter_timeout_total`: 超时会话总数
- `scatter_response_time_p99`: 聚合响应时间 P99

#### Stream
- `stream_partition_count`: 分区总数
- `stream_messages_published_total`: 已发布消息总数
- `stream_messages_consumed_total`: 已消费消息总数
- `stream_consumer_groups`: 消费者组数量
- `stream_rebalance_total`: Rebalance 总次数
- `stream_lag_by_partition`: 每分区消费延迟（high_watermark - committed_offset）

---

## 五、测试与验证

### 5.1 单元测试覆盖

#### Protocol 层（`test_flowmq_protocol_esb.c`）
- ✓ ESB pattern 验证（12 种 pattern）
- ✓ 兼容性检查（禁止 ESB 与基础 pattern 混连）
- ✓ TLV 编解码（12 种字段类型）
- ✓ 边界检查（无效 pattern、超长 payload）

#### Core 层（`test_flowmq_scatter_gather.c`）
- ✓ 会话创建与销毁
- ✓ 三种聚合策略（ALL/FIRST_N/QUORUM）
- ✓ 部分响应记录
- ✓ 超时与取消
- ✓ 容量限制
- ✓ 错误响应处理

#### Core 层（`test_flowmq_stream_partition.c`）
- ✓ Topic/分区初始化
- ✓ 消息发布与 offset 分配
- ✓ 消息 fetch 与范围查询
- ✓ 容量/时间 retention
- ✓ 消费者组管理
- ✓ Rebalance 触发
- ✓ Offset 提交与查询
- ✓ At-least-once 语义

### 5.2 集成测试（Phase 4 规划）

- [ ] 端到端 scatter/gather 流程
- [ ] 多消费者并发消费与 rebalance
- [ ] 崩溃恢复与 offset replay
- [ ] 大流量下的性能验证（10K msg/s）
- [ ] 长期运行稳定性测试（24 小时）

### 5.3 Benchmark（Phase 4 规划）

参见 `flowmq/benchmarks/README.md`：
- Scatter 请求吞吐量
- Gather 聚合延迟（不同策略对比）
- Stream 发布/消费吞吐量
- Rebalance 耗时

---

## 六、迁移指南

### 6.1 从基础 FlowMQ 迁移

**不兼容变更**: 无。ESB pattern 是独立扩展，不影响现有 pattern 1-11。

**新增依赖**:
- Protocol 层：`flowmq_protocol_esb.h`
- Core 层：`flowmq_scatter_gather.h`, `flowmq_stream_partition.h`

### 6.2 与其他消息系统对比

| 特性 | FlowMQ ESB | Kafka | RabbitMQ | ZeroMQ |
|------|------------|-------|----------|--------|
| Scatter/Gather | ✓ | ✗ | ✗ (需 RPC) | ✗ (需手写) |
| Stream Partition | ✓ (简化) | ✓ (完整) | ✗ | ✗ |
| Consumer Group | ✓ | ✓ | ✗ | ✗ |
| At-least-once | ✓ | ✓ | ✓ | ✗ |
| Exactly-once | ✗ | ✓ (事务) | ✗ | ✗ |
| Broker-less | ✗ | ✗ | ✗ | ✓ |
| C API | ✓ | ✗ (仅 C++) | ✓ | ✓ |

---

## 七、未来扩展

### 7.1 短期（v1.0 后）
- SAGA 模式实现
- Priority Queue 实现
- Circuit Breaker 实现
- Stream exactly-once 语义（需事务日志）

### 7.2 长期
- 多 Broker 集群（分区复制）
- Stream compaction（按 key 去重）
- Schema registry（消息格式版本管理）
- 死信队列（Dead Letter Queue）

---

## 八、参考资料

### 8.1 内部文档
- [FlowMQ 协议规范](PROTOCOL_SPEC.md)
- [Wire Protocol](FMQ_WIRE_PROTOCOL.md)
- [架构文档](ARCHITECTURE.md)
- [开发指南](DEVELOPER_GUIDE.md)

### 8.2 相关标准
- [Enterprise Integration Patterns](https://www.enterpriseintegrationpatterns.com/) - Scatter-Gather, Content-Based Router
- [Kafka Protocol](https://kafka.apache.org/protocol) - Partition, Consumer Group
- [AMQP 1.0](https://www.amqp.org/) - 消息传递语义

### 8.3 实现参考
- `flowmq/protocol/include/flowmq_protocol_esb.h` - TLV 编解码
- `flowmq/runtime/src/flowmq_scatter_gather.c` - 会话管理
- `flowmq/runtime/src/flowmq_stream_partition.c` - 分区管理

---

## 附录 A：TLV 字段完整定义

```c
typedef enum {
    FLOWMQ_TLV_EXPECTED_RESPONSES  = 1,   /* uint32 - 期望响应数 */
    FLOWMQ_TLV_AGGREGATION_POLICY  = 2,   /* uint8  - 聚合策略 */
    FLOWMQ_TLV_PARTIAL_INDEX       = 3,   /* uint32 - 部分响应索引 */
    FLOWMQ_TLV_SAGA_ID             = 4,   /* string - SAGA 事务 ID */
    FLOWMQ_TLV_SAGA_STEP           = 5,   /* uint32 - SAGA 步骤 */
    FLOWMQ_TLV_SAGA_STATE          = 6,   /* uint8  - SAGA 状态 */
    FLOWMQ_TLV_PARTITION_ID        = 7,   /* uint32 - 分区 ID */
    FLOWMQ_TLV_OFFSET              = 8,   /* uint64 - 消息 offset */
    FLOWMQ_TLV_CONSUMER_GROUP      = 9,   /* string - 消费者组 */
    FLOWMQ_TLV_PRIORITY            = 10,  /* uint8  - 优先级 */
    FLOWMQ_TLV_CB_STATE            = 11,  /* uint8  - 熔断器状态 */
    FLOWMQ_TLV_FAILURE_COUNT       = 12,  /* uint32 - 失败计数 */
} flowmq_tlv_type_t;
```

## 附录 B：错误码

```c
TURBO_OK          = 0     /* 成功 */
TURBO_EINVAL      = -22   /* 无效参数 */
TURBO_ENOENT      = -2    /* 会话/消费者不存在 */
TURBO_EALREADY    = -114  /* 重复操作 */
TURBO_ENOSPC      = -28   /* 容量耗尽 */
TURBO_ERANGE      = -34   /* 超出范围（partition_id/offset） */
TURBO_ETIMEDOUT   = -110  /* 超时 */
TURBO_ECONNREFUSED = -111 /* 连接被拒绝（用于响应错误） */
```
