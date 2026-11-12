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
              -> CNet TCP / TLS / IPC / WS / WSS / KCP

FMS/3 codec -> 可独立编码的 FMQ/6 HELLO 安全信封
```

FMQ/6 负责 framing、11 种 ZeroMQ pattern、连接协商、receiver-driven credit 与错误边界；
CNet 负责传输会话、有序数据交付与关闭完成。FMQ 对各 transport 使用同一帧格式，
对应封装见 [FMQ/6 传输映射](FMQ_WIRE_PROTOCOL.md#1-分层与版本)。FES/FMP 使用普通 DATA
payload。

## 实现映射

| Family | Production implementation | Tests |
| --- | --- | --- |
| Catalog | `flowmq/src/protocol/flowmq_protocol_catalog.c` | `flowmq/tests/protocol/test_flowmq_protocol.c` |
| FMQ/6 | `flowmq/src/protocol/flowmq_protocol.c`、`flowmq/src/core/session/flowmq_flow_control.c`、`flowmq/src/runtime/flowmq_socket.c` | `flowmq/tests/protocol/`、`flowmq/tests/core/`、`flowmq/tests/runtime/` |
| FMS/3 | `flowmq/src/protocol/flowmq_security.c` | protocol 与 pattern handshake tests |
| FES/1 | `flowmq/src/protocol/flowmq_esb.c` | `flowmq/tests/protocol/test_flowmq_esb.c` |
| FMP/1 | generated header-only TBE wire binding 与 `flowmq/extensions/media_provider/src/flowmq_media_provider.c` | `flowmq/tests/media_provider/test_flowmq_media_provider_schema.c` |
| TCP/TLS/IPC binding | `flowmq_socket.c`、`flowmq_cnet_transport.c` | `test_flowmq_socket`、`test_flowmq_ipc` |
| WS/WSS/KCP binding | `flowmq_socket.c`、`flowmq_cnet_transport.c`、`flowmq_cnet_websocket.c` | `test_flowmq_message_transport`、`test_flowmq_kcp_impairment` |

任何 wire、schema 或 socket 行为变更都必须同步更新 catalog、对应规范与测试。FMQ/5、旧 ESB
transport extension、relaxed ESB decoder 和 `FMS/1` media 旧称均不属于支持面，也没有 fallback。

## 应用协议与 socket runtime 边界

- FMS/3 提供 codec 与语法校验，socket 握手只接收空 HELLO payload。TLS/WSS ROUTER
  可通过 `FLOWMQ_TLS_IDENTITY_POLICY` 把已验证证书的指纹与 HELLO identity 匹配。
  FMS/3 credential provider 与应用 ACL 尚未接入。
- FES/1 是可装入 DATA payload 的严格应用协议，不声明新的 socket type 或 broker runtime。
  未被 socket/runtime 使用的 scatter/gather、stream partition、saga、priority queue 与 circuit
  breaker helper 已从主库移除。
- FMP/1 是可装入 DATA payload 的 wire application contract，不提供 dispatcher、worker 或
  durable broker。
- `FLOWMQ_RECONNECT` 由 socket runtime 配置 CNet recovery policy。符合预算的重连
  创建全新 peer session，成功完成 FMQ HELLO/SETTINGS 后重置预算。
- `flowmq_send()` 在返回成功前接纳调用者字节的副本。TCP/TLS 的 `flowmq_send_slice()`
  使用 CNet retained vector，完成前持有 backing。
