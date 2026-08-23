# FlowMQ implementation

本目录只承载独立 `FlowMQ::FlowMQ` 的 protocol、runtime 与公开头文件。仓库根目录是唯一 CMake
入口。

当前发布面：

- `flowmq/protocol`：FMQ v3 codec、fragmentation、security envelope、heartbeat deadline。
- `patterns`：pattern/session 状态、bounded stream decoder、CONNECT endpoint 和
  ROUTER/BIND endpoint。
- `flowmq/include`：独立 C API；聚合头为 `flowmq.h`。
- 唯一安装 target：`FlowMQ::FlowMQ`。
- 公开链接依赖：`TurboUtils::Core`、`TurboParser::Parser`。`TurboUtils::STL`、CoroNet 与 TLS backend 是构建时私有依赖。

`flowmq_router_endpoint_t` 当前提供完整的 `ROUTER(BIND) ↔ DEALER(CONNECT)` 路径。ROUTER 拥有
listener、peer registry、identity 唯一性与 generation-fenced route；DEALER 由
`flowmq_connect_endpoint_t` 拥有连接与 reconnect。`TCP/TLS/WS/WSS` 由相同 API 选择。

构建、使用示例和部署图见 [根 README](../README.md)，详细所有权与数据路径见
[ARCHITECTURE.md](../docs/ARCHITECTURE.md)。
