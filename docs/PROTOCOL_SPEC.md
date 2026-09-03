# FlowMQ protocol specifications

现行协议由 `flowmq_protocol_catalog.h` 的不可变目录统一定义：

| 文档 / Schema | Family | 层次与责任 |
| --- | --- | --- |
| [FMQ_WIRE_PROTOCOL.md](FMQ_WIRE_PROTOCOL.md) | FMQ/6 | frame、ZeroMQ pattern、HELLO/SETTINGS、DATA、FLOW_UPDATE、heartbeat、fragmentation、multipart |
| [FMQ_WIRE_PROTOCOL.md](FMQ_WIRE_PROTOCOL.md#3-fms3-security-envelope) | FMS/3 | FMQ HELLO security envelope 语法 |
| [FES_APPLICATION_PROTOCOL.md](FES_APPLICATION_PROTOCOL.md) | FES/1 | ESB application message kind、严格 metadata TLV 与 payload envelope |
| [media-provider schema](../flowmq/extensions/media_provider/schema/flowmq_media_provider_v1.schema) | FMP/1 | media-provider TBE wire payload 与零拷贝校验器 |

## 分层

```text
FES/1 envelope 或 FMP/1 TBE wire payload
              -> FMQ/6 DATA frame
              -> Salts CNet TCP 或 verified TLS byte stream

FMS/3 envelope -> FMQ/6 HELLO payload
```

FMQ/6 负责 framing、11 种 ZeroMQ pattern、连接协商、receiver-driven credit 与错误边界；
CNet 负责 TCP/TLS 连接、完整有序写入、receive view 和关闭完成。FES/FMP 不占用 FMQ pattern
或 frame-kind 空间，TLS 也不改变 FMQ frame 格式。

## 实现映射

| Family | Production implementation | Tests |
| --- | --- | --- |
| Catalog | `flowmq/src/protocol/flowmq_protocol_catalog.c` | `flowmq/tests/protocol/test_flowmq_protocol.c` |
| FMQ/6 | `flowmq/src/protocol/flowmq_protocol.c`、`flowmq/src/core/session/flowmq_flow_control.c`、`flowmq/src/runtime/flowmq_socket.c` | `flowmq/tests/protocol/`、`flowmq/tests/core/`、`flowmq/tests/runtime/` |
| FMS/3 | `flowmq/src/protocol/flowmq_security.c` | protocol 与 pattern handshake tests |
| FES/1 | `flowmq/src/protocol/flowmq_esb.c` | `flowmq/tests/protocol/test_flowmq_esb.c` |
| FMP/1 | generated header-only TBE wire binding 与 `flowmq/extensions/media_provider/src/flowmq_media_provider.c` | `flowmq/tests/media_provider/test_flowmq_media_provider_schema.c` |
| TCP/TLS binding | `flowmq/src/runtime/flowmq_socket.c`、`flowmq/src/transport/cnet/flowmq_cnet_transport.c` | `flowmq/tests/runtime/test_flowmq_socket.c`、`flowmq/tests/transport/test_flowmq_transport.c` |

任何 wire、schema 或 socket 行为变更都必须同步更新 catalog、对应规范与测试。FMQ/5、旧 ESB
transport extension、relaxed ESB decoder 和 `FMS/1` media 旧称均不属于支持面，也没有 fallback。

## 已定义但未接入 socket runtime 的能力

- FMS/3 已有 codec 与语法校验；当前 endpoint fail closed，只接受空 HELLO payload，尚无
  credential provider、principal/ACL 或 TLS certificate identity binding。
- FES/1 是可装入 DATA payload 的严格应用协议，不声明新的 socket type 或 broker runtime。
  未被 socket/runtime 使用的 scatter/gather、stream partition、saga、priority queue 与 circuit
  breaker helper 已从主库移除。
- FMP/1 是可装入 DATA payload 的 wire application contract，不提供 dispatcher、worker 或
  durable broker。
- reconnect policy 与 receive-side heartbeat deadline 已由 socket facade 配置和调用；
  endpoint 断线后会在后续 caller progress 中创建全新 peer session。socket runtime 使用
  segmented frame encoder 与 `cnet_sendv()` 提交 framing/payload ranges；descriptor 仅在同步
  admission 调用期间借用，CNet 在返回成功前按顺序复制进一个有界 command slot。
