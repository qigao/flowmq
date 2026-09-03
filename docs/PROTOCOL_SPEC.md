# FlowMQ protocol specifications

现行协议由 `flowmq_protocol_catalog.h` 的不可变目录统一定义：

| 文档 / Schema | Family | 层次与责任 |
| --- | --- | --- |
| [FMQ_WIRE_PROTOCOL.md](FMQ_WIRE_PROTOCOL.md) | FMQ/6 | frame、ZeroMQ pattern、HELLO/SETTINGS、DATA、FLOW_UPDATE、heartbeat、fragmentation、multipart |
| [FMQ_WIRE_PROTOCOL.md](FMQ_WIRE_PROTOCOL.md#3-fms3-security-envelope) | FMS/3 | FMQ HELLO security envelope 语法 |
| [FES_APPLICATION_PROTOCOL.md](FES_APPLICATION_PROTOCOL.md) | FES/1 | ESB application message kind、严格 metadata TLV 与 payload envelope |
| [media-provider schema](../application/schema/flowmq_media_provider_v1.schema) | FMP/1 | media-provider typed payload 与校验器 |

## 分层

```text
FES/1 envelope 或 FMP/1 typed payload
              -> FMQ/6 DATA frame
              -> Rocida CNet TCP 或 verified TLS byte stream

FMS/3 envelope -> FMQ/6 HELLO payload
```

FMQ/6 负责 framing、11 种 ZeroMQ pattern、连接协商、receiver-driven credit 与错误边界；
CNet 负责 TCP/TLS 连接、完整有序写入、receive view 和关闭完成。FES/FMP 不占用 FMQ pattern
或 frame-kind 空间，TLS 也不改变 FMQ frame 格式。

## 实现映射

| Family | Production implementation | Tests |
| --- | --- | --- |
| Catalog | `flowmq/protocol/src/flowmq_protocol_catalog.c` | `flowmq/protocol/tests/test_flowmq_protocol.c` |
| FMQ/6 | `flowmq/protocol/src/flowmq_protocol.c`、`patterns/src/flowmq_flow_control.c`、`patterns/src/flowmq_socket.c` | `flowmq/protocol/tests/`、`patterns/tests/` |
| FMS/3 | `flowmq/protocol/src/flowmq_security.c` | protocol 与 pattern handshake tests |
| FES/1 | `flowmq/protocol/src/flowmq_esb.c` | `flowmq/protocol/tests/test_flowmq_esb.c` |
| FMP/1 | generated TBE binding 与 `application/src/flowmq_media_provider.c` | `application/tests/test_flowmq_media_provider_schema.c` |
| TCP/TLS binding | `patterns/src/flowmq_socket.c`、`flowmq_cnet_transport.c` | `patterns/tests/test_flowmq_socket.c`、`test_flowmq_transport.c` |

任何 wire、schema 或 socket 行为变更都必须同步更新 catalog、对应规范与测试。FMQ/5、旧 ESB
transport extension、relaxed ESB decoder 和 `FMS/1` media 旧称均不属于支持面，也没有 fallback。

## 已定义但未接入 socket runtime 的能力

- FMS/3 已有 codec 与语法校验；当前 endpoint fail closed，只接受空 HELLO payload，尚无
  credential provider、principal/ACL 或 TLS certificate identity binding。
- FES/1 是可装入 DATA payload 的严格应用协议，现有 scatter/gather、stream、saga 与 circuit
  breaker 模块仍是本地状态 primitive，不提供新的 socket type 或 broker runtime。
- FMP/1 是可装入 DATA payload 的 typed application contract，不提供 dispatcher、worker 或
  durable broker。
- reconnect policy、receive-side heartbeat deadline 与 segmented frame encoder 已有独立
  helper/codec 测试，但尚未由 socket facade 全部配置或调用。TCP/TLS socket 当前使用连续
  scratch buffer 编码，并由调用者显式驱动进度与重连策略。
