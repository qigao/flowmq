# FlowMQ protocol specifications

本目录的现行 wire 契约如下：

| 文档 | Wire | 责任范围 |
| --- | --- | --- |
| [FMQ_WIRE_PROTOCOL.md](FMQ_WIRE_PROTOCOL.md) | FMQ/5 | frame、HELLO/SETTINGS、DATA、FLOW_UPDATE、订阅控制、heartbeat、fragmentation、multipart |
| [media-provider schema](../application/schema/flowmq_media_provider_v1.schema) | FMS/1 | 应用 payload schema 与校验器；未接入 socket runtime |

## 分层

```text
application payload / FMS/1
        -> FMQ/5 frame
        -> Rocida CNet TCP or verified TLS byte stream
```

FMQ/5 负责 framing、pattern/session、能力协商、receiver-driven credit 与错误边界；CNet 负责 TCP/TLS 连接、完整有序写入、
receive view 和关闭完成。TLS 不改变 FMQ frame 格式。

## 实现映射

| Wire | Production implementation | Tests |
| --- | --- | --- |
| FMQ/5 | `flowmq/protocol/src/flowmq_protocol.c`、`patterns/src/flowmq_flow_control.c`、`patterns/src/flowmq_socket.c` | `flowmq/protocol/tests/`、`patterns/tests/` |
| FMS/1 | generated TBE binding 与 `application/src/flowmq_media_provider.c` | `application/tests/test_flowmq_media_provider.c` |
| TCP/TLS binding | `patterns/src/flowmq_socket.c`、`flowmq_cnet_transport.c` | `patterns/tests/test_flowmq_socket.c`、`test_flowmq_transport.c` |

任何 wire 或 socket 行为变更都必须同步更新对应文档与测试。未在此入口列出的旧 transport
协议不属于当前 FlowMQ 支持面。

## 已设计但未接入 socket runtime 的能力

- ESB pattern 12..21、frame kind 7..19：已有 TLV codec 与本地状态结构，但公开
  `flowmq_socket()` 只接受 ZeroMQ 风格 pattern 0..10，没有 ESB transport dispatch。
- FMS/3 security envelope：已有 codec 与语法校验；当前 endpoint fail closed，只接受空
  payload，尚无 credential provider、principal/ACL 或 TLS certificate identity binding。
- FMS/1 media-provider：仅是可装入 DATA payload 的应用 schema/validator，不提供
  dispatcher、worker 或 broker。
- reconnect policy、receive-side heartbeat deadline 与 segmented frame encoder：已有独立
  helper/codec 测试，但尚未由 socket facade 配置或调用。TCP/TLS socket 当前使用连续
  scratch buffer 编码，并由调用者显式驱动重连策略。
