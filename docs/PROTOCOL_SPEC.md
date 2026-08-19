# FlowMQ 协议索引

本文是协议、持久化格式和配置契约的发布入口，只负责协议地图、分层关系和公共规则。
每个协议的字段与状态机只在
下表指定的唯一正文中维护，避免索引与专题文档产生两份不一致的规范。

## 1. 文档地图

| 文档 | 唯一正文 | 范围 |
| --- | --- | --- |
| [FMQ_WIRE_PROTOCOL.md](FMQ_WIRE_PROTOCOL.md) | FMQ/3、FMS/3 | socket frame、HELLO、分片、心跳、pattern、安全 envelope、queue/backpressure |
| [KCP_TRANSPORT_PROTOCOL.md](KCP_TRANSPORT_PROTOCOL.md) | TKSH/1、TKSR/1、TKF1/1 | KCP PSK handshake、AEAD、replay、Reed-Solomon FEC、调优参数 |
| [MANAGEMENT_PROTOCOL.md](MANAGEMENT_PROTOCOL.md) | TFMP/1、TFMS/1.0、TFMS/1.1 | DEALER/ROUTER management、operation、event、durable snapshot |
| [BULK_CREDIT_PROTOCOL.md](BULK_CREDIT_PROTOCOL.md) | TFCW/1、TFBR/1、TFCS/1.0 | credit worker、logical address、durable claim/retry/outbox |
| [ADR_FMQ_V3_SECURITY.md](ADR_FMQ_V3_SECURITY.md) | 安全决策记录 | trusted/secure v3、provider、ACL 与边界理由 |
| [DEPLOYMENT_CONTROL.md](DEPLOYMENT_CONTROL.md) | deployment control contract | membership、fencing、rolling upgrade、reconcile |

以下文档不是新的 wire protocol 正文：

| 文档 | 归类 | 用途 |
| --- | --- | --- |
| [README.md](README.md) | 产品概览 | package、pattern 选择和协议入口 |
| [ARCHITECTURE.md](ARCHITECTURE.md) | 架构说明 | owner、线程、生命周期和模块边界 |
| [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md) | 接入指南 | C API、graph、YAML 和示例 |
| [ZMQ_STYLE_API.md](ZMQ_STYLE_API.md) | facade API | ZeroMQ 风格的 pattern-level API；不承诺 ZMTP 兼容 |
| [RELEASE_GATE.md](RELEASE_GATE.md) | 发布验证契约 | Redis、chaos、persistence 和性能门槛 |

## 2. 协议清单

| 名称 | 层级 | 载体 | 事实源/owner |
| --- | --- | --- | --- |
| FMQ/3 | socket framing | CoroNet endpoint 的字节流/报文 | FlowMQ protocol decoder 与 peer session |
| FMS/3 | HELLO security envelope | FMQ/3 HELLO payload | security owner 与 BIND peer admission |
| TKSH/1、TKSR/1、TKF1/1 | secure KCP transport | UDP datagram | CoroNet KCP session owner |
| TFMP/1 | 管理应用协议 | FMQ/3 DATA payload | management owner 与 operation store |
| TFMS/1.0、1.1 | 管理持久化格式 | SQLite/Redis blob snapshot | operation owner |
| TFCW/1 | credit-worker 应用协议 | FMQ/3 DATA payload | credit worker owner |
| TFBR/1 | broker logical address | TFCW body、outbox、持久化记录 | broker/settlement owner |
| TFCS/1.0 | durable credit state | claim settler state blob | durable settlement owner |
| YAML 配置 | 本地配置契约 | YAML -> immutable resolved snapshot | config resolver |

只有 FMQ/3 定义 socket framing。TFMP 和 TFCW 不增加 frame kind；TFMS、
TFBR、TFCS 也不增加 socket frame。YAML 是本地配置输入，不是带版本的网络协议。

## 3. 分层关系

```text
YAML 或 C API
        |
        v
pattern/session owner
        |
        +-- TFMP/1、TFCW/1
        |       (FMQ DATA payload)
        |
        v
FMQ/3 frame + FMS/3 HELLO + PING/PONG
        |
        v
       CoroNet transport
        |
        +-- KCP: TKSH/1 -> TKSR/1 -> TKF1/1 -> UDP
```

管理、credit worker 和 deployment owner 不得成为新的 socket framing owner；它们只能
通过既有 pattern/session 边界发送 pointer-free application payload。graph、event、
storage view 和 observe 指标均为派生边界，不得反向推进协议事实源。

## 4. 公共编码与错误规则

除专题正文另有明确规定，协议字段遵循以下共同约束：

- 整数为 network byte order；文本为无 NUL、有界 UTF-8；BYTES 保持 binary-safe。
- 所有 decoder 必须校验完整输入、版本、保留字段、长度、字段组合和资源上限。
- 未知 critical 字段、无法恢复的 malformed 输入和协议版本不支持
  必须 fail fast；不得静默修复或 fallback。
- 本地 frame admission、transport send、storage accept、delivery/completion 是不同
  ACK 边界，不能互相冒充。
- raw socket bytes、decoder borrowed view 和临时 frame 不能跨 owner lane、graph、
  queue 或线程保存；跨边界必须转换为拥有内存的消息/记录。
- route、socket、coro、线程和 provider 指针不得进入任何网络或持久化格式；需要延迟
  回复时使用专题正文定义的 generation-fenced logical identity。

## 5. 协议集合

FMQ/3 只接受 version 3；KCP 只接受 TKSH/1、TKSR/1 和 TKF1/1；TFMP 只接受
TFMP/1。没有旧 frame、raw KCP、version negotiation、兼容 wrapper 或自动 transport
fallback。协议字段变化必须更新对应唯一正文、实现、测试和
[RELEASE_GATE.md](RELEASE_GATE.md)。

## 6. 实现与验证入口

| 协议 | 实现入口 | 主要测试 |
| --- | --- | --- |
| FMQ/3、FMS/3 | `protocol/`、`runtime/` | `protocol/tests/`、`runtime/tests/` |
| TKSH/1、TKSR/1、TKF1/1 | CoroNet `src/turbo_kcp*.c` | CoroNet `tests/test_kcp.c`、`benchmarks/test_bench_kcp.c` |
| TFMP/1 | `src/fmq_management_protocol.c`、`src/fmq_management.c` | `tests/test_fmq_management_protocol.c`、`tests/test_fmq_management.c` |
| TFCW/1、TFBR/1、TFCS/1.0 | `src/fmq_broker*.c` | `tests/test_fmq_broker.c` |
| deployment control | `src/fmq_deployment.c` | `tests/test_fmq_deployment.c` |
