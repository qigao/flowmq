# FlowMQ 固定连接数的多 lane 扩展性（2026-10-10）

## 范围与复现

本轮验证现有 ordinary socket 在多个固定 owner 线程上的扩展性。每个 lane 是一个
`cmeta_thread`，独立创建 context、socket、CNet client/backend 并执行全部收发、回调和关闭。
连接的两端都在同一个 lane 上；跨 lane 不共享 socket、decoder、credit 或可变 payload。
这里没有使用 `flowmq_owner_t` 的 shared backend，也没有接入 external listener、集中
Acceptor、跨 lane handoff 或 Leader/Followers。结果不能标作这些尚未测量的方案收益。

环境与上一轮 [batch/progress 测量](FLOWMQ_BATCH_SWEEP.md) 相同：Windows 11
10.0.26200、Ryzen 9 7940HX（16 核/32 逻辑处理器）、MSVC 14.44.35207 Release；
Salts 2.3.0-rc.1（58ff08fc95b4aa1dc493c0b7080426b2c11d4959）、
SaltsUtils 4.3.0-rc.1（049f8e39e1d19ff21e9825df7c497e40b75a8ddf）。
FlowMQ 生产库沿用 `7d4d9d3` 的运行时，本次只扩展 benchmark。
CPU 亲和未绑定，由 OS 调度；没有控制频率、SMT 分配或其他系统负载。

```powershell
cmake --build --preset win-release-user
ctest --preset win-release-user -R "^test_flowmq_socket_owners$" --output-on-failure
ctest --preset bench-win-release-user -R "^bench_flowmq_lane_scaling$" -V --output-log build/lane-scaling.log
```

需在 MSVC 开发环境运行。默认每连接 2,048 轮、八次重复；可通过正式测试的
`FLOWMQ_LANE_BENCH_ROUNDS`（1–4096）与 `FLOWMQ_LANE_BENCH_REPEATS`（1–9）
覆盖。改变参数的结果应另存，不与默认数据混合。

## 数据路径与测量协议

- 总量固定为 **8 条 TCP PAIR 连接 / 16 个 sockets**，平均分到 1/2/4/8 个线程；
  每个 socket 一条连接、独立监听端口。不会随线程数增加总连接数或总工作量。
- payload 为 64 B、1 KiB；单 part copy，生产合批策略，peer pool 关闭。
  每连接 burst=128，发送 HWM=128；每组计时消息数为 `8 × 128 × 2048 = 2,097,152`。
- 每个 lane 每轮先向其所有连接各提交一个 burst，再依次接收这些 burst。
  would-block 时 `flowmq_poll(..., 0)` 推进 lane 上的全部 sockets；每轮结束再推进两次
  credit。单 lane 对照也允许八条连接同时有在途消息，不人为限制为一个连接的窗口。
  不同 lane 之间没有逐轮 barrier；快 lane 可以先进入下一轮。
- 每条消息含连接 identity 和递增序号，逐条检查长度、全部内容和 FIFO；发送缓冲区复用，
  校验同时检查 copy ownership。每个 lane 的最终消息数必须与其分配份额一致。
- 初始化、连接建立、八轮预热在计时外。全部 worker ready 后由控制线程同时放行；
  wall interval 从放行到最后一个 worker 完成，包含唤醒与调度。CPU interval 从放行前
  到控制线程确认全部完成，包含进程内所有线程以及少量控制开销。
- worker 完成后等待 cleanup 门，不在其他 worker 测量期间释放 socket；统一结束计时
  后才关闭、join、排序和打印。错误通过 atomic 首错记录传播；队列满返回既有错误并由
  owner 推进后重试，单轮 timeout=30 秒。线程 join 失败不能证明资源静止，测试进程终止。
- 每个连接每个 burst 记录一次延迟：从首条 send 前到最后一条 receive 内容校验后。
  它包含等待同 lane 其他连接被调度的时间；尾部两次 credit progress 计入总吞吐时间，
  不计入该 burst 延迟。这不是单消息延迟，也不是跨进程网络 RTT。
- 汇总 P95/P99 是该组 16,384 个 burst 的 nearest-rank 分位数，不是各 lane P99 的平均。
  CSV 同时保存每个 lane 的启动偏移、完成偏移、P99、poll 和 admission retries。
- 每个 payload/线程数组合八轮；线程数轮换，使每个组合在四个执行位置各出现两次。
  两种 payload 的执行顺序交替。总吞吐取实际完成条数 / wall 秒；CPU/条为进程 CPU ns /
  实际完成条数。RSS 是进程生命周期峰值，不能解释为每组独立内存增量。

此负载是闭环有限 burst、均匀静态分片。增加 lane 会减少一个 lane 内串行调度的连接数；
尾延迟改善包含这个效果，不应全部归因于网络 I/O 加速。虽然每组工作量相同，改变 lane
数也改变各连接在时间上的交错；这是调度拓扑对照，不是只改变 CPU 核数的孤立实验。

## 结果（事实与计算）

[汇总 CSV](flowmq-lane-scaling-windows-20261010.csv) 包含 64 组，
[逐 lane CSV](flowmq-lane-workers-windows-20261010.csv) 包含 240 条记录。
全部 134,217,728 条计时消息完成内容及 FIFO 校验，send admission retries 均为零。
下表吞吐、CPU/条、P99 均为八组中位数。配对加速比先将每组吞吐除以同 payload、同
repeat 的 1-lane 吞吐，再取中位数，因此不等于表中吞吐中位数之比。

| payload | lanes | 吞吐 M条/s | 配对加速比 | CPU ns/条 | 每连接每批 P99 µs | 进程 CPU/wall |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 B | 1 | 1.538 | 1.000 | 644.5 | 1,084.5 | 0.99 |
| 64 B | 2 | 2.526 | 1.706 | 782.3 | 505.5 | 1.97 |
| 64 B | 4 | 4.618 | 3.269 | 838.2 | 279.1 | 3.94 |
| 64 B | 8 | 7.734 | **5.032** | 998.4 | 169.6 | **7.80** |
| 1 KiB | 1 | 0.561 | 1.000 | 1,780.7 | 2,783.8 | 0.99 |
| 1 KiB | 2 | 1.120 | 1.883 | 1,765.8 | 1,339.2 | 1.97 |
| 1 KiB | 4 | 2.011 | 3.634 | 1,914.8 | 725.4 | 3.92 |
| 1 KiB | 8 | 3.736 | **6.576** | 2,086.2 | 360.8 | **7.77** |

进程 CPU/wall 逐组计算为 `cpu_ns_per_message × messages / wall_ns` 再取中位数；
约 7.8 表示测量期间进程平均消耗约 7.8 个逻辑 CPU 的执行时间，不代表绑在八个物理核。
这比只看线程数量更直接地证明多个 owner 正在并行工作。

| payload | lanes | 同轮加速比最小–最大 | 提高轮数 |
| --- | ---: | ---: | ---: |
| 64 B | 2 | 1.602–1.945 | 8/8 |
| 64 B | 4 | 2.558–3.623 | 8/8 |
| 64 B | 8 | 4.264–6.261 | 8/8 |
| 1 KiB | 2 | 1.712–2.534 | 8/8 |
| 1 KiB | 4 | 3.198–4.583 | 8/8 |
| 1 KiB | 8 | 5.681–9.166 | 8/8 |

这些范围是观测极值，不是置信区间。个别 8-lane 组出现超过 8 倍，不能解释为稳定的
超线性扩展；相邻分时运行仍受频率、缓存和系统负载变化影响，且调度拓扑发生了改变。

### CPU、尾延迟与 lane 分布

**计算：**64 B 的 8-lane 配对扩展效率为 `5.032 / 8 = 62.9%`，1 KiB 为
`6.576 / 8 = 82.2%`。相对 1 lane，8 lane 的 CPU/条中位数分别提高约 **54.9%** 和
**17.2%**。多核提高了总吞吐，但小消息并发的资源成本上升更明显；还不能断定是缓存、
分配器、调度、时钟频率还是内核竞争造成，需 CPU stack/硬件计数证据。

每批 P99 中位数分别下降约 84.4% 和 87.0%。其中包含一个 lane 从调度八条连接缩减为
调度一条连接的收益，不能称为单条 TCP 消息时延下降同样比例，也不能与前一轮单 PAIR
benchmark 的 P99 直接拼接比较。

所有 lane 均完成等额工作。8 lane 下，逐组计算 `最慢 lane 执行时间 / 最快 lane 执行时间`，
64 B 的中位数为 1.095、最大 1.140；1 KiB 的中位数为 1.056、最大 1.096。
执行时间扣除各自启动偏移；全体测量最大启动偏移为 59.4 µs，且放行开销已包含在吞吐
计时中。均匀连接负载下未见严重单 lane 拖尾，这不证明热点 key/不均匀业务负载也会均衡。

64 B 的 poll 调用数随 lane 数为 8,192 / 16,384 / 32,768 / 65,536；1 KiB 分别为
22,528 / 45,056 / 90,112 / 180,224。每次 poll 的 socket 数为 `16 / lanes`，故每组
`poll_calls × sockets_per_poll` 分别恒为 131,072 和 360,448。吞吐扩展没有依赖减少总
socket progress 访问数；但该计算不是 native I/O submit、完成事件或 syscall 计数。

进程峰值 RSS 从 14,790,656 到 30,928,896 bytes；这是整个 benchmark 进程的累积高水位，
其中也含线程和 benchmark storage，不能归因成 lane 数的独立内存成本或无泄漏证明。

## 结论与后续边界

**事实：**无需改动 CNet 单 owner 契约、无需引入 Leader/Followers，现有固定 owner 分片
已能在本机多连接负载下利用多核；2/4/8 lane 在两种 payload 的八轮配对中全部提高吞吐。
**推论：**下一阶段可以沿用固定 lane，在单 lane 内优化 shared wait，并独立接入服务端
Acceptor–Connector，而不必先重写为多线程共同推进同一个 client。

**MED：**本次未测试 `flowmq_owner_t` 多 lane shared backend、集中 accept 分配、Windows
同端口服务、跨 lane 有界 handoff/wake、TLS 性能或 Linux/macOS。连接两端在同 lane，且为
同进程 loopback，不能外推到外部客户端、跨机、多 NIC 或单热点连接。一个既有连接不会因
增开 lane 自动拆分到多个 owner。本轮也没有复测此前 SG/新策略的 1 KiB 回归；其性能疑点
仍保留，不能被本次多核扩展结果覆盖。

下一轮有意义的对照是同样固定总连接数下的 ordinary poll 与 explicit owner shared wait，
并分别测服务端 accept 接入及连接不均衡场景；不能把本轮收益算作未实现方案的收益。

## 验证

Release 构建成功，无新增编译警告。`test_flowmq_socket_owners` 通过（5.98 秒）：既有
TCP/TLS、multipart、HWM、pool 开关和 owner shutdown 场景继续通过，新增 1/2/4/8 lane
的 64 B / 1 KiB copy 校验通过。正式 `bench_flowmq_lane_scaling` 通过（85.24 秒），64 组
均成功。CSV 核对了行数、位置平衡、等额消息/连接分配、worker poll/retries 汇总及最后
完成时间；未发现缺组或短跑。

日志：`build/lane-scaling-build.log`、`build/lane-scaling-correctness.log`、
`build/lane-scaling.log`。本次只修改 benchmark 和注册，未改变生产 runtime；未重复整套
transport 回归，也未运行 sanitizer。此前 23/23 回归结果属于上一提交，不能算成本次重跑。
