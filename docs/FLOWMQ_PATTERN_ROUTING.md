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

## Progress 与 listener 归因

后续代码审查发现，候选的选择函数只直接进入本组 PUSH 和 REQ 发送；PUB、ROUTER
定向发送及三个 POLLOUT 场景均不调用它。POLLOUT 测量实际包含 `socket_drive` 的
listener、client poll 和 local progress，并非单独的 readiness 谓词。

为检查这些路径，复用既有私有 transport probe/profile，新增
`bench_flowmq_pattern_probe` 与 `bench_flowmq_pattern_profile`。普通 dispatch target
仍链接无探针的 production transport。探针明确选择 coalesce mode 3（当前生产策略），
不使用历史默认 SG 策略。fixture 完成后推进 32 轮，再在 owner 线程读取各 socket 快照；
窗口内不重置计数、不保留 payload、不额外分配，每个场景结束才打印结果。

三轮 counter + 三轮 profile 共 54 组，保存于
[progress CSV](flowmq-pattern-progress-windows-20261010.csv)。每组均核对预期 DATA
消息数、payload 字节数、direct/queued admission、submitted ranges 和 queued 直方图，
且窗口内 `listener_ready=0`、`manager_work=0`。这些 write 计数表示成功的 CNet DATA
接纳，不表示 OS syscall、TCP packet 或业务确认；控制帧不计入 DATA write。

下表是三轮 profile 的中位数。drive/listener 计数按样本归一化；时间占比的分母是整个
测量窗口 wall time（包含 TinyTest 计时管理与结果输出），因此只作为带探针的阶段定位。
receive callback、manager 等嵌套阶段不能再与其父阶段相加。

| 场景 | drive/样本 | listener 检查/样本 | listener wait 占比 | client poll 占比 | local progress 占比 |
| --- | ---: | ---: | ---: | ---: | ---: |
| PUB 四 peer | 40.000 | 8.000 | 14.6% | 62.6% | 13.4% |
| PUSH 四 peer | 60.000 | 48.000 | 42.1% | 39.3% | 9.4% |
| ROUTER 定向 | 10.000 | 5.000 | 28.4% | 51.7% | 9.8% |
| REQ/REP | 14.001 | 7.000 | 25.4% | 57.4% | 7.8% |
| PUB 过滤 | 20.000 | 4.000 | 14.9% | 62.4% | 13.5% |
| ROUTER 隔离 | 13.000 | 5.000 | 26.7% | 51.7% | 11.5% |
| POLLOUT PUSH | 1.000 | 0.000 | 0.0% | 44.5% | 32.9% |
| POLLOUT PAIR credit 耗尽 | 1.000 | 0.000 | 0.0% | 49.2% | 25.1% |
| POLLOUT REP | 1.000 | 1.000 | 57.5% | 19.4% | 9.5% |

**事实：**PUSH fixture 的四个 PULL 各有 listener，PUB fixture 只有一个；每次 receive
至少推进整组两轮，PUSH 每次 send 前还推进一次。实测 48/8 次 listener 检查符合这个
拓扑差异。少量额外 drive 来自非阻塞收发重试，不宜强制解释为固定次数。

全部 54 组的 `coalesced_writes=0`；[queued write 直方图](flowmq-pattern-write-histogram-windows-20261010.csv)
仅出现 `frames_per_write=1`。DATA 的 logical writes 与消息数相等，本负载未形成
17 帧起步的小帧合批。不能用这组结果评价 batch=128 的吞吐收益。

**MED / 推论：**该基准中 listener 空检查及 progress 频率比 peer 选择抽取更值得优先
研究。此证据解释当前基准的成本构成，不等于已定位前述候选与基线差值的原因。

### 单变量：固定连接下省略 listener readiness

新增诊断入口 `bench_flowmq_pattern_no_listener`，复用 counter-only 程序及既有私有
progress mode 2。全部连接和 fixture 预热完成后，设置
`FLOWMQ_PATTERN_OMIT_LISTENER=1`；只省略 listener readiness 检查，保留两处 manager
advance、client poll、local progress、credit 与完整 payload 校验。正常 probe/profile
CTest 入口显式设置该值为 0，其他字符串值直接失败。

mode 2 无法接纳新连接，**只适用于此固定拓扑实验，不能部署为生产策略**。省略模式的
每组计数还要求 `listener_checks=0`。探针与环境开关均不进入普通生产 transport。

四轮配对按 baseline/omit、omit/baseline 交替顺序串行运行，共 72 组，见
[listener ablation CSV](flowmq-pattern-listener-ablation-windows-20261010.csv)。全部通过
DATA 接纳数、字节数、range 计数与消息内容检查。下表耗时为各自四轮 avg/sample 的
中位数（µs）；变化率是逐轮计算 `omit/baseline−1` 后取中位数，不能直接由前两列相除。

| 场景 | 正常检查 | 省略检查 | 配对耗时变化 | 省略后更快轮数 |
| --- | ---: | ---: | ---: | ---: |
| PUB 四 peer | 56.824 | 44.347 | −21.5% | 4/4 |
| PUSH 四 peer | 112.975 | 55.991 | −50.0% | 4/4 |
| ROUTER 定向 | 17.133 | 11.358 | −33.6% | 4/4 |
| REQ/REP | 27.961 | 20.004 | −28.6% | 4/4 |
| PUB 过滤 | 28.038 | 22.631 | −19.2% | 4/4 |
| ROUTER 隔离 | 19.039 | 12.453 | −34.0% | 4/4 |
| POLLOUT PUSH | 0.523 | 0.522 | −1.7% | 2/4 |
| POLLOUT PAIR credit 耗尽 | 0.427 | 0.446 | +0.7% | 1/4 |
| POLLOUT REP | 1.500 | 0.448 | −70.6% | 4/4 |

**事实与推论：**有 listener 的路径均四轮改善，两个无 listener 的 readiness 场景接近
不变；结合检查次数与阶段计时，支持 listener 空检查是这组固定拓扑负载的重要成本。
这比单纯比较两版路由代码的总耗时更接近可归因实验，但依然不是生产优化收益：真实
系统必须处理新连接、取消和 shutdown。改进方向是将 listener 纳入 owner/lane 的统一
等待与 completion 分发，并保持及时接纳，不能以删除检查替代协议设计。

### 本轮验证与复现

- Release 完整 build 通过；既有 production/probe/profile 三个 batch correctness 测试
  3/3 通过。生产 runtime 未修改，没有重复上一轮 30 项 core/transport 测试。
- 三轮 counter/profile：6 次 CTest 测试运行、54 场景全部通过；普通无探针 benchmark
  另通过。最终四轮 listener 配对：8 次 CTest 运行、72 场景全部通过。
- `FLOWMQ_BUILD_ZMQ_BENCHMARK=OFF` 下重新 configure/build，四个 pattern benchmark
  入口 4/4 通过；诊断库已从 ZMQ 条件块移出，不依赖 libzmq。随后恢复 ON 并完整构建，
  原有三个 batch correctness 再次 3/3 通过。
- 完整日志：`build/pattern-attribution-{1,2,3}.log`、
  `build/pattern-listener-{1,2,3,4}-{probe,no_listener}.log`、
  `build/pattern-without-zmq-tests.log`、`build/pattern-restored-regression.log`。

同一 VS developer environment 与现有 Release preset：

```powershell
cmake --build --preset win-release-user
ctest --preset bench-win-release-user -R "^bench_flowmq_pattern_(dispatch|probe|profile)$" -V
ctest --preset bench-win-release-user -R "^bench_flowmq_pattern_no_listener$" -V
```

后三个目标为诊断工具。计数、阶段时钟和普通无探针程序的绝对耗时不能混作同一性能
总体；本轮尚未测量真实 shared-listener 集成、生产接纳延迟、Linux/macOS 或 libzmq 的
非 PAIR 对照，也没有把已撤回的路由候选重新纳入生产实现。

## 实际 shared-listener 内部实现（2026-10-10）

此节是上述 ablation 之后的实际接入验证。新增
`bench_flowmq_pattern_shared_listener`，与普通 `bench_flowmq_pattern_dispatch` 复用同一
fixture、pattern 操作、payload/identity/credit 检查和计时循环。两者均链接无探针的
生产 transport；新目标通过不安装的 `flowmq_socket_internal_bind_external()` 接入
listener，仍可接纳新连接，未设置省略 listener 的诊断开关。

每个 fixture 的全部 2–5 个 sockets 放在一个显式 owner 中，owner 容量固定为 5。
普通版依次 poll 各 socket；共享版通过一次 owner poll 推进同组 sockets。两者均为调用者
单线程驱动、不创建 worker；TCP loopback、64 B、network 场景 20,000 samples、readiness
场景 1,000,000 samples。PUB/PUSH 每轮 4 个消息，过滤 PUB 和 REQ/REP 每轮 2 个消息，
ROUTER 每轮 1 个消息；表中单位是 µs/sample，不把多消息 sample 称作单消息延迟。
关闭路径现在也检查 socket、owner 和 context 的返回值。

环境延续本报告的 Windows x64/MSVC Release、Salts 2.3.0-rc.1。先单独通过共享版的
9 场景资格运行，再按 ordinary/shared、shared/ordinary 交替顺序串行做四轮配对；测量时
不并行运行 build 或回归测试。原始 72 行见
[shared-listener CSV](flowmq-pattern-shared-listener-windows-20261010.csv)。下表前两列是
四轮 avg/sample 中位数，变化率是逐轮 `shared/ordinary−1` 的中位数，使用 CSV 的输出精度。

| 场景 | ordinary µs/sample | shared owner µs/sample | 配对耗时变化 | shared 更快轮数 |
| --- | ---: | ---: | ---: | ---: |
| PUB 四 peer | 51.707 | 36.823 | −28.3% | 4/4 |
| PUSH 四 peer | 103.404 | 42.093 | −58.8% | 4/4 |
| ROUTER 定向 | 16.394 | 9.588 | −41.7% | 4/4 |
| REQ/REP | 26.959 | 16.427 | −39.2% | 4/4 |
| PUB 过滤 | 26.871 | 18.529 | −30.2% | 4/4 |
| ROUTER 隔离 | 17.962 | 10.812 | −39.1% | 4/4 |
| POLLOUT PUSH | 0.493 | 1.343 | +169.3% | 0/4 |
| POLLOUT PAIR credit 耗尽 | 0.435 | 0.643 | +49.2% | 0/4 |
| POLLOUT REP | 1.472 | 0.630 | −57.0% | 4/4 |

**事实：**六个网络场景均四轮更快；共享版保留真实 listener 和取消/接纳逻辑。
**推论：**结果支持继续验证固定 owner 的共享等待。相较 ordinary，不仅 listener wait
改变，client 的多个 backend observe 也合并了，完成路由及 local progress 顺序随 domain
改变；因此这不是仅替换 listener 的单变量实验，不能把全部收益归因于消除 WSAPoll。
四轮一致也不构成统计显著性或跨平台收益证明。

**MED / 事实：**PUSH、PAIR 的单 socket readiness 四轮均更慢。`bench_pollout()` 在普通版
只推进该 socket；owner 版按既有契约推进整条 lane，即使 poll items 仅含一个 socket。
PUSH 带 5 个 sockets、PAIR/REP 带 2 个；这部分是实际 API 工作范围及成本的差别，不是
相同工作量的 selector 微基准。REP 同时省去独立 listener readiness，实测总成本下降。
后续应评估应用是否反复轮询单 socket，不能通过跳过其他 owner sockets 来改变 progress
保证。原有 route-selection 候选仍未恢复。

验证：新内部集成/fault 四个 target 连续 10 轮通过，完整 core/transport 35/35 通过；
benchmark 首次资格运行及四轮配对全部通过。普通/probe/profile 使用同一改造后的 fixture。
改造后的 probe/profile 两个完整 benchmark 2/2 通过，production/probe/profile 三个 batch
correctness 测试 3/3 通过（日志 `build/listener-pattern-probes.log`、
`build/listener-pattern-batch.log`）。
复现入口：

```text
cmake --build --preset win-release-user
ctest --preset bench-win-release-user -R "^bench_flowmq_pattern_dispatch$" -V
ctest --preset bench-win-release-user -R "^bench_flowmq_pattern_shared_listener$" -V
```

日志：`build/listener-pattern-qualification.log`、
`build/listener-pattern-{1,2,3,4}-{dispatch,shared_listener}.log`。
该结果不包含进程 CPU、P99、接纳延迟或峰值资源；没有新的 ZMQ 非 PAIR 对照。
公开 owner bind 仍不开放，剩余失败注入和平台验证见
[架构门槛](ARCHITECTURE.md#flowmq-内部-shared-listener-集成2026-10-10)。
