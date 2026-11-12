# FlowMQ implementation

本目录承载独立 `FlowMQ::FlowMQ` 的 protocol、runtime 与公开头文件；仓库根目录是唯一 CMake
入口。

- `flowmq/include`：独立 C API；聚合头为 `flowmq.h`。
- `flowmq/src/protocol`：全局协议目录、FMQ/6、FMS/3、FES/1 codec 与 bounded stream decoder。
- `flowmq/src/core`：pattern FSM、subscription 与接收端 credit。
- `flowmq/src/runtime`：context/socket/peer 状态、HWM、有界重连与 caller-driven progress。
- `flowmq/src/transport`：CNet TCP、TLS、IPC、WS/WSS 与 KCP 适配。
- `flowmq/src/security`：TLS principal/identity policy。
- `flowmq/extensions/media_provider`：FMP/1 schema、header-only wire binding 与 view validator。
- 唯一安装 target：`FlowMQ::FlowMQ`。
- 公开链接依赖：`Salts::Core`、`Salts::DataBind`。
- 私有实现依赖：`CMeta::CSTL`、`CMeta::CMeta`、`Salts::CNet`、`Salts::NativeIO`、llhttp 与 GmSSL。

公开网络边界是 ZeroMQ 风格 socket facade。owner 线程通过普通 socket 操作或
`flowmq_owner_poll()` 推进连接与 deadline。CNet recovery policy 管理重连票据和退避。构建、使用和部署入口见
[根 README](../README.md)，所有权与数据路径见
[ARCHITECTURE.md](../docs/ARCHITECTURE.md)。

FES/1 与 FMP/1 由应用在 FMQ DATA 中使用。FMS/3 提供安全信封 codec。
TLS/WSS 的 ROUTER 身份校验直接使用证书指纹与 HELLO identity。协议和运行时的分层见
[PROTOCOL_SPEC.md](../docs/PROTOCOL_SPEC.md)。
