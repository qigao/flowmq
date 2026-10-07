# FlowMQ

FlowMQ 是 C11 的 pattern-oriented messaging library。它提供 FMQ/6 wire codec、pattern/session
状态、应用层能力，以及基于 Salts CNet 的 TCP/TLS socket runtime。

当前 transport 范围是明确且封闭的：

- `FLOWMQ_TRANSPORT_TCP`：有序 TCP 字节流。
- `FLOWMQ_TRANSPORT_TLS`：经证书与主机名校验的 TLS 字节流。
- UDP、KCP、Pipe、WebSocket 与 WSS 不属于当前 API，也没有静默 fallback。

## CMake targets

| Target | 用途 |
| --- | --- |
| `FlowMQ::Protocol` | build-tree FMQ/6、FMS/3、FES/1 codec 与全局协议目录 |
| `FlowMQ::Core` | build-tree pattern/session 与应用核心 |
| `FlowMQ::Transport` | build-tree ZeroMQ-style socket 与 CNet TCP/TLS runtime |
| `FlowMQ::FlowMQ` | 唯一安装 target；合并上述公开能力 |

安装包的公开依赖是 `Salts::Core` 与 SaltsUtils 的 `Salts::Schema`；
`Salts::CNet`、`Salts::CSTL` 和 `Salts::CMeta` 是实现私有依赖。FMP/1 使用
header-only TBE wire view/builder，不依赖 JSON typed codec。

## Caller-driven transport

旧的 callback endpoint API 已删除。新的公开边界只保留 ZeroMQ 风格的
`context/socket + bind/connect + send/recv + poll`，入口是 `flowmq_socket.h`。

CNet 仍由 socket owner 线程直接推进，不创建 worker/progress thread，也不把同一个 socket
包装成 MPSC、Actor 或 Reactive stream。TCP 与 verified TLS 使用同一 peer、decoder、FSM
和 message queue 路径；TLS 只在连接适配层增加证书与主机名验证。

`FLOWMQ_RECONNECT_IVL=18` 与 `FLOWMQ_RECONNECT_IVL_MAX=21` 采用 ZeroMQ 的编号和
连接级退避语义：IVL 默认 100ms，`-1` 禁止重连，`0` 表示下一轮 owner progress 立即
尝试；IVL_MAX 默认 `0`，表示固定 IVL，设置为不小于 IVL 的正值后按上限做指数退避。
实际间隔会随机化以降低重连风暴。断线只调度 endpoint，真正的 TCP/TLS connect、
HELLO/SETTINGS 与订阅重放仍由应用后续调用 `send/recv/poll` 推进，不创建 timer thread。

`FLOWMQ_SNDHWM`/`FLOWMQ_RCVHWM` 使用 `int` 消息数，扩展选项
`FLOWMQ_SNDHWM_BYTES`/`FLOWMQ_RCVHWM_BYTES` 使用 `size_t` payload 字节数。四项都必须在
首次 bind/connect 前设置且不能为零；普通发送达到 HWM 返回 `SALTS_ENOBUFS`，PUB/XPUB
按 peer 丢弃无法接纳的 publication。

`FLOWMQ_FLOW_UPDATE_QUANTUM` 使用 `size_t`，默认由 receive byte HWM 推导；
`FLOWMQ_FLOW_UPDATE_IVL` 使用正 `int` 毫秒值，默认 10 ms。二者必须在首次 bind/connect 前
设置。DATA 同时受本地 HWM 与对端累计 credit 约束；控制帧不计 credit。

`FLOWMQ_HEARTBEAT_IVL`/`FLOWMQ_HEARTBEAT_TIMEOUT` 使用非负 `int` 毫秒值，也必须在首次
bind/connect 前设置。IVL 默认为 `0`（禁用）；启用 IVL 且未显式设置 TIMEOUT 时，TIMEOUT
等于 IVL。心跳与断线检测没有后台线程，只在 owner 调用 `send`、`recv` 或 `poll` 时推进。
FMQ/6 PING 不携带对端 TTL，因此当前没有伪装提供 `FLOWMQ_HEARTBEAT_TTL`。
FMQ/6 在 HELLO 后强制 SETTINGS，并以 receiver-driven cumulative credit 协调 DATA；
TCP/TLS 不发送同流 FEC repair symbol。credit 与 heartbeat 都由调用线程推进。

重连创建全新的 peer session：generation、credit、decoder 与 multipart 状态不会跨连接
继承；旧 peer 尚未完成的 outbound 数据也不会自动重播。需要业务级确认或重试时，应在
DATA payload 层携带 correlation/idempotency 信息。

multipart 接收后可用 `flowmq_getsockopt(socket, FLOWMQ_RCVMORE, ...)` 判断是否还有下一
part。该查询返回最近一次成功 `flowmq_recv()` 的 `MORE` 状态，不推进网络或 pattern FSM。

ROUTER 的 routing identity 与 ZeroMQ 一样表示当前 live peer，不是可持久化的 session
token；同 identity 重连后的 delayed reply 会指向新 session。单条 outbound multipart
内部会绑定 connection generation，peer 断线后取消，不能把剩余 parts 交给 replacement。

```c
#include <flowmq_socket.h>
#include <cmeta_error.h>

#include <stdio.h>
#include <string.h>

enum { PROGRESS_LIMIT = 10000 };

int main(void) {
  static const char payload[] = "hello";
  char endpoint[128] = {0};
  char received[16] = {0};
  size_t endpoint_size = 0;
  size_t received_size = 0;
  size_t ready = 0;
  int send_status = SALTS_EBUSY;
  int recv_status = SALTS_EBUSY;
  int result = 1;
  flowmq_ctx_t *ctx = flowmq_ctx_new();
  flowmq_socket_t *receiver = NULL;
  flowmq_socket_t *sender = NULL;
  flowmq_pollitem_t items[2];

  if (ctx == NULL) goto cleanup;
  receiver = flowmq_socket(ctx, FLOWMQ_PAIR);
  sender = flowmq_socket(ctx, FLOWMQ_PAIR);
  if (receiver == NULL || sender == NULL) goto cleanup;
  if (flowmq_bind(receiver, "tcp://127.0.0.1:0") != SALTS_OK) goto cleanup;
  if (flowmq_last_endpoint(receiver, endpoint, sizeof(endpoint),
                           &endpoint_size) != SALTS_OK)
    goto cleanup;
  if (flowmq_connect(sender, endpoint) != SALTS_OK) goto cleanup;

  items[0] = (flowmq_pollitem_t){.socket = sender};
  items[1] = (flowmq_pollitem_t){.socket = receiver};
  for (size_t i = 0; i < PROGRESS_LIMIT && recv_status == SALTS_EBUSY; ++i) {
    if (flowmq_poll(items, 2, 0, &ready) != SALTS_OK) goto cleanup;
    if (send_status == SALTS_EBUSY)
      send_status = flowmq_send(sender, payload, sizeof(payload) - 1,
                                FLOWMQ_DONTWAIT);
    if (send_status != SALTS_OK && send_status != SALTS_EBUSY) goto cleanup;
    if (send_status == SALTS_OK)
      recv_status = flowmq_recv(receiver, received, sizeof(received),
                                &received_size, FLOWMQ_DONTWAIT);
  }
  if (recv_status != SALTS_OK || received_size != sizeof(payload) - 1 ||
      memcmp(received, payload, received_size) != 0)
    goto cleanup;
  puts(received);
  result = 0;

cleanup:
  if (sender != NULL && flowmq_close(sender) != SALTS_OK) result = 1;
  if (receiver != NULL && flowmq_close(receiver) != SALTS_OK) result = 1;
  if (ctx != NULL && flowmq_ctx_term(ctx) != SALTS_OK) result = 1;
  return result;
}
```

## 构建与验证

参考 [SaltsUtils](https://github.com/qigao/salts-utils) 的恢复流程，先安装 .NET SDK 8、
PowerShell 7，并在父环境提供具有 `read:packages` 权限的 `GITHUB_TOKEN`。
脚本不自动读取 `.env`，也不记录凭据。Windows 在 Visual Studio developer 环境中运行：

```powershell
./cmake/ci/restore-native-sdk.ps1 -Rid windows-x64
cmake --preset win-release-user
cmake --build --preset win-release-user
ctest --preset win-release-user --output-on-failure
```

Linux 使用 `pwsh -File cmake/ci/restore-native-sdk.ps1 -Rid linux-x64`，随后运行
`linux-release-user` 的 configure/build/test preset。

Salts.Native 使用 `Version="*"`，每次 restore 通过
`--no-cache --force-evaluate` 解析最新稳定版；不限制版本、不回退旧包。
NuGet assets 是版本和路径的事实源，包默认存放于 `stage/nuget`，可通过
`QIGAO_NUGET_PACKAGES` 指定缓存位置。Release presets 消费
`stage/dependencies/salts/<RID>` 的稳定链接；configure 本身不下载 SDK，
且清除旧的包路径缓存。恢复步骤只更新链接，拒绝覆盖已有的普通 SDK 目录。

SaltsUtils 继续由各 preset 的 `SALTS_UTILS_ROOT` 指定，必须针对当前 Salts 重新构建。
不能混用仍导入旧 `salts_*` 符号的 `salts-idlc` 与新版 Salts runtime。
其包配置还要求 Lua 与 QuickJS，已通过现有 vcpkg manifest 声明。

发布 SDK 包含 Release 库。`win-dev-user` / `linux-dev-user` 仍要求在既有
`PKG_ROOT` 路径安装匹配的 Debug SDK，不能混用旧版或 Release 库。
Salts 的 Core 头文件与平台调用已采用当前 `cmeta_*` API；升级后需重新构建 FlowMQ
及其消费者。FlowMQ 公开函数、消息格式与 caller-driven 线程模型保持不变。
FMP/1 由当前 SaltsUtils 的 `salts-idlc` 生成；工具更新会触发头文件重新生成。

新数据路径 benchmark：

```powershell
cmake --fresh --preset win-release-user -DFLOWMQ_BUILD_ZMQ_BENCHMARK=ON
cmake --build --preset win-release-user --target bench_flowmq_socket
```

ZeroMQ 由 `vcpkg.json` 安装；公平对比必须使用 Release preset，使 FlowMQ 与
libzmq 都链接 Release 产物。

多核 owner 基准使用现有公开 API：每个工作线程在本线程创建独立 context，
创建、推进并关闭自己的 PAIR sockets。固定 4 条回环连接（8 个 socket），
分配给 1/2/4 个 owner；每条连接的收发两端位于同一 owner。
这验证的是独立 socket 工作负载的并行吞吐，不是单个 ROUTER/PUB 的跨核分片。
生产 runtime、公开 API 和 wire 格式没有因此改变。

业务接入示例见 [multicore_reqrep.c](examples/multicore_reqrep.c)：主线程拥有一组
REQ sockets，每个 worker 在自己的线程创建 context 与 REP socket，绑定独立的
回环 TCP 端口。主线程统一 `flowmq_poll` 推进所有 REQ sockets；只推进当前等待
应答的 socket 会延迟其他请求发出。每轮向每个 worker 发一个请求，逐条校验
应答中的 worker 标识、序号与单 part 边界，全部完成后才开始下一轮。

示例参数依次为 worker 数（1–16，默认 4）、每 worker 请求数（1–10000，默认 100）、
等待超时毫秒（1–60000，默认 5000）。worker 的单次收发、最终排空，以及客户端
整轮等待分别受该超时限制。发送和接收 HWM 均为 1，关闭自动重连，队列满时
继续推进至成功或超时；首个错误通过 atomic 传播，其他线程停止等待。
endpoint 仅在启动时通过 mutex 发布，消息通过 TCP 复制传递，无跨线程 socket/context。
每个 owner 在本线程关闭资源；主线程收齐应答后通知 worker 停止，等 worker 退出后
再关闭客户端，最后 join 全部线程。示例状态空间为 O(workers)，完整请求数为
workers × requests，不引入业务线程池或新的公开 API。

在下述修复版 SDK preset 配置完成后：

```powershell
cmake --build --preset win-release-local-sdk --target flowmq_multicore_reqrep
ctest --preset win-release-local-sdk -R '^test_flowmq_multicore_reqrep_' -V
```

CTest 覆盖 1/4/16 个 worker，每个处理 100 条请求；程序只有应答全部验证且所有
owner 成功关闭后才返回成功。在配置了相同 SDK/vcpkg runtime PATH 的开发者终端，
可直接运行 `build/Msvc-Release-LocalSDK/bin/flowmq_multicore_reqrep.exe 4 100 5000`，
成功输出 `workers=4 replies=400 verified; all owners closed`。
此例展示独立 endpoint 的多核业务接入，不代表单个逻辑 ROUTER 的跨核分片；
其逐轮同步与低占用等待策略也不作为吞吐基准。

本次验证使用含下述 CNet 修复的本地 SDK。先在 Salts 仓库按其构建说明安装 SDK；
本机安装入口为 `cmake --build --preset install-win-release-ci`，产物位于
`../salts/stage/sdk/windows-x64`。随后在上述 Windows 开发者环境运行：

```powershell
$env:FLOWMQ_LOCAL_SALTS_ROOT = (Resolve-Path ../salts/stage/sdk/windows-x64).Path
cmake --preset win-release-local-sdk
cmake --build --preset win-release-local-sdk
ctest --preset win-release-local-sdk -LE benchmark --output-on-failure
ctest --preset win-release-local-sdk -R '^bench_flowmq_socket_owners_' -V --output-log build/Msvc-Release-LocalSDK/owner-scaling.log
```

`win-release-local-sdk` 使用独立 build/install 目录，不替换发布 SDK 的链接；
`FLOWMQ_LOCAL_SALTS_ROOT` 必须指向完整 SDK，缺失时配置失败。
发布包验证仍使用 `win-release-user`；Linux 对应使用 `linux-release-user`，
但下述修复尚未发布，不能假设 restore 得到的包已包含它。
基准同样保留为正式 CTest 项，失败不会被跳过或标记成预期成功。
每批 16 条消息，每条含两个等长 part，总 payload 为 64 B 或 64 KiB。
所有消息检查长度、内容、连接身份、序号和 `RCVMORE`；预热同时验证发送 HWM
拒绝与恢复，关闭和 context 释放的返回值也纳入结果。

每个负载重复 3 次，轮换 1/2/4 owner 的运行顺序。TCP 64 B 默认每连接
4096 批，TCP 64 KiB 和 TLS 64 B 为 256 批，TLS 64 KiB 为 32 批；
同一负载的所有 owner 配置使用相同总连接数、消息数、字节数和窗口。
可用父环境 `FLOWMQ_OWNER_BENCH_ROUNDS`（1–4096）覆盖批数，
`FLOWMQ_OWNER_BENCH_REPEATS`（1–9）覆盖重复次数；非法值直接失败。

只汇总成功用例的 `OWNER_RESULT` CSV 行：吞吐按完整应用消息及 payload
计一次，不计协议/TLS 头；墙钟从统一放行到最后一个 owner 完成，排除建连、
握手、预热和关闭，包含 FlowMQ 编解码及消息校验成本。
P99 是从每条消息首次发送尝试到收齐最后一个 part 的实测延迟，包含排队，
不把批次平均耗时冒充单消息延迟。CPU 为该测量阶段整个进程的 CPU 秒数；
RSS 为进程生命周期峰值，不是每行独立的内存增量。线程未绑核，Windows
CPU 时间存在计量粒度限制。诊断适配仅在 benchmark 中使用系统统计 API。

2026-10-07 本机事实：Windows Release、Salts 2.1.0 加本地 CNet TLS 缓冲区修复、
Ryzen 9 7940HX（16 核 / 32 逻辑处理器），四类负载三轮吞吐中位数如下。
版本仅记录测量环境，不构成依赖版本限制；所有行均使用同一修复版 SDK。

| 负载 | 1 owner | 2 owners | 4 owners | 4 / 1 |
| --- | ---: | ---: | ---: | ---: |
| TCP 64 B（消息/s） | 684,105 | 1,281,235 | 2,444,099 | 3.57× |
| TCP 64 KiB（MiB/s） | 1,772.93 | 2,169.20 | 3,006.44 | 1.70× |
| TLS 64 B（消息/s） | 10,519 | 13,142 | 19,957 | 1.90× |
| TLS 64 KiB（MiB/s） | 14.64 | 20.06 | 40.20 | 2.75× |

上述倍数计算为 4-owner 吞吐中位数 / 1-owner 吞吐中位数。
收益是吞吐而非普遍降延迟：TCP 64 B 的 P99 中位数从 25.5 增至 29.7 µs，
TCP 64 KiB 从 717.8 增至 1828.7 µs，TLS 64 B 从 2063.4 增至 4877.8 µs，
TLS 64 KiB 从 75.48 增至 112.33 ms。
TLS 小消息的进程 CPU 秒数中位数也从 1.562 增至 3.234。
三轮样本存在波动（例如单 owner TCP 64 KiB 为 1072.48–1793.65 MiB/s），
中位数仅用于本机扩展性观察，不能用前后不同运行的绝对值判定补丁加速。
本机回环结果不能外推为远程网络、单连接或共享逻辑 socket 的性能承诺。

**HIGH｜事实：CNet TLS 接收缓冲区所有权问题已在本地修复，尚未发布。**
原 SDK 的 TLS 64 KiB 持续负载曾在单 owner 下返回 `SALTS_EPROTO`。
`../salts/cnet/src/cnet_owner.c` 将 TLS `read_buffer` 交给异步 NativeIO 接收，
而 `../salts/cnet/src/cnet_tls.c` 同时用它承载解密结果和未消费的明文后缀，
两种生命周期会重叠。修复在私有 TLS engine 内分配独立明文区，普通读取、
无需求探测和部分读取共用该明文区，transport 缓冲区专供接收至完成被消费。
每 TLS 会话增加 `tls_io_buffer_bytes`（FlowMQ 默认 17 KiB），无热路径新增分配；
初始化容量溢出或分配失败仍立即报错，释放随 engine 统一完成。
公开 ABI、TLS 校验、wire 格式和单 owner 进度模型不变。

`../salts/cnet/tests/cnet_tls_test.c` 新增 TLS 1.2/1.3 × 普通读取/探测四个
缓冲区隔离用例，覆盖部分明文读取期间的 transport 存储变化。
修复版 SDK 实测 CNet 35/35、FlowMQ 普通回归 17/17（含三个跨线程 REQ/REP
示例用例）、性能用例 4/4 通过。
原始 CSV 位于本次构建目录的 `owner-scaling-fixed.log` 与 `owner-tls-fixed.log`；
这些日志为本地产物，可用上述 CTest 命令重新生成。
Linux/macOS 与并发 sanitizer 尚未验证。

详细所有权和关闭顺序见 [架构说明](docs/ARCHITECTURE.md)，wire 契约见
[FMQ/6 协议](docs/FMQ_WIRE_PROTOCOL.md)。
