# FlowMQ implementation

本目录承载独立 `FlowMQ::FlowMQ` 的 protocol、runtime 与公开头文件；仓库根目录是唯一 CMake
入口。

- `flowmq/protocol`：全局协议目录、FMQ/6 transport、FMS/3 security、FES/1 application envelope 与 heartbeat deadline。
- `patterns`：pattern/session、bounded decoder 与 caller-driven CNet TCP/TLS primitive。
- `flowmq/include`：独立 C API；聚合头为 `flowmq.h`。
- 唯一安装 target：`FlowMQ::FlowMQ`。
- 公开链接依赖：`Rocida::Core`、`TurboParser::Parser`、`TurboParser::DataBind`。
- 私有实现依赖：`Rocida::STL`、`Rocida::CNet` 与 TLS backend。

旧 callback endpoint 已删除。新的公开网络边界是 ZeroMQ 风格 socket facade；CNet 由调用
`send/recv/poll` 的 owner 线程直接推进连接、重连和 deadline，不创建 worker。构建、使用和部署入口见
[根 README](../README.md)，所有权与数据路径见
[ARCHITECTURE.md](../docs/ARCHITECTURE.md)。

FES/1、FMP/1、FMS/3 与 segmented encoder 中有一部分仅提供 codec、本地状态
或 helper，并不等于 socket runtime 已接入；reconnect 已由 socket runtime 调用。准确边界见
[PROTOCOL_SPEC.md](../docs/PROTOCOL_SPEC.md)。
