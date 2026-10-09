# FlowMQ pattern 路由评估（2026-10-10）

本轮保留正式回归用例与 benchmark 入口，撤回生产路径拆分。正确性验证通过不代表性能
验证通过；下列数据尚不能把代码影响与主机运行波动分开，不能作为提速依据。

## 候选与决定

- 基线：`48bb97d` 的 `flowmq/src/runtime/flowmq_socket.c`。
- 候选：将三个 round-robin 扫描合并为私有只读函数，区分 READY、COPY、RETAINED
  接纳条件；回复目的地与 POLLOUT 改为读取 routing class，事务提交仍读取 FSM class。
  cursor、generation pin、credit、队列与 CNet admission 的原提交时机保持不变。
- 本地实验快照：`3d22b2c8ec4e33a31e87050f42f85708321d9c8c`，分支
  `work/ace-cnet23-pattern-selection-experiment`。快照包含候选、测试和加长窗口的 benchmark；
  未推送。最终迁移分支的生产 runtime 已恢复基线。
- **MED / 事实**：首批与补测的结果方向不稳定；补测多个场景的中位数偏慢，尚未排除
  回退。**推论**：可能有运行环境、代码布局或调用开销的影响，目前没有 profiler 证据
  可以区分，不能归因为 CNet、ACE 模式或某一个分支。

此候选没有新增分配、队列、线程或公开 API；撤回只恢复原私有分支，不涉及数据迁移。
后续若重新评估，应分别测量“只改 routing class”和“只抽取扫描函数”，并记录 CPU
频率、调度和进程 CPU 时间，避免将两个变量的组合结果直接归因于设计模式。

## 测量方法

Windows 11、Ryzen 9 7940HX、MSVC Release；依赖为发布的 Salts 2.3.0-rc.1 与
SaltsUtils 4.3.0-rc.1。环境详细信息沿用 [lane 测量记录](FLOWMQ_LANE_SCALING.md)。
使用真实 loopback TCP、64-byte payload、caller-driven progress；没有绑定 CPU，也没有
固定频率。构建和正确性测试不与 benchmark 并行。

每个网络场景 20,000 样本。首批 readiness 也是 20,000 样本，约只有 10–50 ms 累计
测量时间；PUSH POLLOUT 前后三轮中位数为 0.553/0.712 µs。因此补测将三种 readiness
场景增至每轮 1,000,000 样本，并把这个采样数保留在正式 benchmark 中。

两组测量各含基线/候选三轮。首批记录顺序 A1/B1/B2/A2/A3/B3；补测记录顺序 B1/A1/A2/A3/B2/B3，
A 为基线、B 为候选。每次切换只替换 runtime 候选差异，再构建同一完整 graph；两种版本
使用同一测试及 benchmark 源码。固定顺序和三轮样本不足以作显著性或因果结论。

[原始数值 CSV](flowmq-pattern-routing-windows-20261010.csv) 保存两组共 108 行，包含场景、
样本数、操作单位、每样本均值/极值、吞吐与对应本地日志名。`initial` 为首批，`extended`
为加长 readiness 窗口后的完整补测。下表为补测三轮的 **avg/sample 中位数（轮次范围）**，
单位 µs，括号不是消息延迟分位数：

| 场景 | 基线 | 候选 |
| --- | ---: | ---: |
| PUB 4-peer fanout | 52.370 (51.790–54.212) | 57.316 (51.899–57.437) |
| PUSH 4-peer round-robin | 111.362 (105.390–122.610) | 120.070 (118.557–123.179) |
| ROUTER identity one-way | 17.581 (16.873–26.691) | 23.009 (22.611–23.789) |
| REQ/REP roundtrip | 28.497 (27.999–35.624) | 39.287 (33.450–40.826) |
| PUB 2-of-4 filtered fanout | 29.112 (27.582–38.519) | 41.608 (29.181–41.996) |
| ROUTER slow-peer isolation | 18.421 (18.096–21.137) | 22.804 (22.206–26.463) |
| POLLOUT PUSH ready | 0.598 (0.506–0.683) | 0.659 (0.643–0.705) |
| POLLOUT PAIR credit-exhausted | 0.436 (0.426–0.603) | 0.598 (0.595–0.615) |
| POLLOUT REP reply-peer ready | 1.782 (1.531–1.871) | 2.018 (1.816–2.185) |

PUB 全 fanout/PUSH 每样本含四次接收，filtered PUB 含两次接收，REQ/REP 含请求和回复。
测量包括 progress、发送、接收及 payload 校验；不能用这些数字推算纯路由成本，也不能
与 batch/lane 吞吐数字直接比较。此次没有测量非 PAIR 的 libzmq 对照或多核扩展。

## 保留的回归与运行入口

新增 PUSH 双 peer 测试使用 4-byte receive credit：copy 与 retained 单帧绕过耗尽 credit
的 peer；multipart 先固定该 peer，final 返回 ENOBUFS；消费旧消息恢复 credit 后，只在
原 peer 完成 x/y 两个 part。测试同时检查健康 peer 没有收到 multipart、part 边界和无重复。
同一测试源用于普通 socket 与启用 peer pool 的两个正式 target。

从 VS developer environment 运行：

```powershell
cmake --build --preset win-release-user
ctest --preset win-release-user -L "flowmq-(core|transport)" --output-on-failure
ctest --preset bench-win-release-user -R "^bench_flowmq_pattern_dispatch$" -V
```

CTest 新入口强制关闭 smoke 模式，串行运行并设置 600 秒超时。全部 12 次记录的 benchmark
均通过 9 个场景、27 条断言；候选的 23 个传输及 2 个 pattern/FSM 测试亦通过。

最终保留版本完成完整 Release build，core/transport **30/30 通过**（201.54 秒），包括普通
及 pool socket、新 PUSH 回归、TLS、owner fault、公开 C11/C++17 与多核 REQ/REP 示例。
日志：`build/pattern-accepted-build.log`、`build/pattern-accepted-regression.log`。生产
`flowmq_socket.c` 与基线无差异；构建仅出现此前已有的 C5287/C4189 警告。上述记录只适用于
本机 Windows Release，不代表 Linux、macOS 或 sanitizer 验证。
