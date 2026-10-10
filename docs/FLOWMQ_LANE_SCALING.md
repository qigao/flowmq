# FlowMQ 固定连接数的多 lane 扩展性与 libzmq 对照（2026-10-10）

前半部分保存 `794ec02` 的 FlowMQ 独立扩展性结果；末尾的 libzmq 对照是另一轮双方
相邻运行的配对实验。两轮数据独立保存，不跨轮相减推导性能收益。

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


## FlowMQ / libzmq 多 lane 配对对照

### 对照方法

本轮沿用八条 TCP PAIR 连接、每连接 batch=128、64 B / 1 KiB、1/2/4/8 个应用 worker，
每组 2,097,152 条消息、每组合八轮。两套实现共用 `owner_lane_round` 的发送/接收顺序、
每条消息校验、burst 延迟采集、启动/cleanup gate 和进程 CPU 采集；每个组重新创建资源，
预热八轮再测量。没有用前一轮 FlowMQ 的吞吐与本轮 libzmq 的吞吐作对照。

本轮为双方明确设置发送和接收 HWM=128。上一轮 FlowMQ 只显式设置发送 HWM；接收
HWM 此次在 benchmark 中也设为 128，不改变生产默认值。虽然应用每连接仍限制一个
128 条 burst，也不应忽略这个配置差异而把两轮绝对值直接比较。

每个应用 lane 一个独立 context。libzmq 来自已有 vcpkg 依赖，运行时版本为 4.3.5，
每个 context 明确设置并校验 `ZMQ_IO_THREADS=1`；FlowMQ 不创建后台 I/O 线程。
因此 N lane 比较的是 FlowMQ 的 N 个应用 progress 线程，与 libzmq 的 N 个应用线程
加 N 个 I/O 线程（另有库的管理线程），**不是相同总线程预算或相同物理核预算**。
未测试共享单个 ZMQ context 或其他 I/O 线程配置，不能宣称是 libzmq 的最优调参结果。

FlowMQ 使用 DONTWAIT + 全部 lane-local sockets 的非阻塞 poll，每轮结束两次 credit
progress；libzmq 使用阻塞 send/recv，由后台线程推进，收发分别设 30 秒超时，关闭时
linger=0。内容已在测量结束前逐条确认接收，linger 不用来丢弃未完成计时消息。
相同数字的 HWM 不意味着两种协议的内部 credit/watermark 实现完全相同。

CSV 中 `send_retries` 只计应用显式重试。libzmq 的阻塞等待不会增加这个计数，不能把
它的零重试解释为没有背压或等待；其 `poll_calls=0` 也不代表后台没有 I/O 或系统调用。
进程 CPU 覆盖双方的应用、I/O 和管理线程；计时结束时尚未开始资源关闭，故不含关闭
CPU。RSS 仍是整个混合实验进程的高水位，不能拿后运行一方的数值作独立内存比较。

每个 payload 的四种 lane 数轮换执行。对于相同 payload、lane、repeat，两种引擎相邻
运行；八轮中各先运行四次，且每个 lane-order 位置上各先运行一次。`order` 是 lane 组合
的位置，`engine_order` 是组内引擎的先后位置。吞吐和 CPU 采用各自中位数，配对比例则
先计算同轮 FlowMQ/libzmq 再取中位数，两个统计量不混用。

复现（开启现有 `FLOWMQ_BUILD_ZMQ_BENCHMARK` 配置，MSVC 开发环境）：

```powershell
cmake --build --preset win-release-user
ctest --preset win-release-user -R "^test_flowmq_socket_owners$" --output-on-failure
ctest --preset bench-win-release-user -R "^bench_flowmq_lane_zmq$" -V --output-log build/lane-zmq.log
```


### 配对对照结果（事实）

[对照汇总 CSV](flowmq-lane-vs-zmq-windows-20261010.csv) 为 128 组，
[逐 lane CSV](flowmq-lane-vs-zmq-workers-windows-20261010.csv) 为 480 条记录。
全部 268,435,456 条计时消息完成长度、内容和 FIFO 校验。下面为八组中位数，吞吐比为
双方吞吐中位数相除；计时范围包含内容验证，不能当作纯网络链路吞吐。

| payload | 应用 lanes | FlowMQ M条/s | libzmq M条/s | FMQ/ZMQ | FMQ CPU ns/条 | ZMQ CPU ns/条 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 B | 1 | 1.375 | 1.933 | 0.711 | 722.7 | 745.1 |
| 64 B | 2 | 2.499 | 2.965 | 0.843 | 797.2 | 972.3 |
| 64 B | 4 | 4.404 | 5.028 | 0.876 | 879.2 | 1,199.5 |
| 64 B | 8 | 7.342 | 7.686 | **0.955** | **1,035.6** | **1,587.0** |
| 1 KiB | 1 | 0.563 | 0.578 | 0.974 | 1,754.6 | 2,227.7 |
| 1 KiB | 2 | 1.047 | 0.922 | 1.136 | 1,885.0 | 2,875.9 |
| 1 KiB | 4 | 1.944 | 1.517 | 1.282 | 2,015.4 | 3,751.4 |
| 1 KiB | 8 | 3.666 | 2.100 | **1.746** | **2,116.0** | **5,755.6** |

| payload | 应用 lanes | FMQ 每批 P99 µs | ZMQ 每批 P99 µs | FMQ 进程 CPU/wall | ZMQ 进程 CPU/wall |
| --- | ---: | ---: | ---: | ---: | ---: |
| 64 B | 1 | 1,129.9 | 945.1 | 0.99 | 1.55 |
| 64 B | 2 | 538.0 | 640.4 | 2.00 | 2.93 |
| 64 B | 4 | 304.2 | 398.2 | 3.88 | 6.10 |
| 64 B | 8 | **210.6** | **271.2** | **7.66** | **11.75** |
| 1 KiB | 1 | 2,569.3 | 2,674.9 | 0.99 | 1.29 |
| 1 KiB | 2 | 1,489.4 | 1,720.8 | 1.98 | 2.68 |
| 1 KiB | 4 | 726.5 | 1,210.3 | 3.92 | 5.59 |
| 1 KiB | 8 | **381.7** | **901.4** | **7.71** | **12.11** |

P99 是每组实际 burst 样本的分位数，再取八组中位数，不是由吞吐倒数计算。CPU/wall
也是先逐组计算再取中位数；不可将表中不同列的中位数直接相乘来要求严格相等。

### 配对稳定性与解释

| payload | 应用 lanes | 同轮 FMQ/ZMQ 比最小–最大 | 配对比中位数 | FMQ 吞吐领先轮数 |
| --- | ---: | ---: | ---: | ---: |
| 64 B | 1 | 0.602–0.978 | 0.719 | 0/8 |
| 64 B | 2 | 0.742–0.861 | 0.821 | 0/8 |
| 64 B | 4 | 0.775–1.030 | 0.910 | 1/8 |
| 64 B | 8 | 0.918–1.113 | 0.985 | 3/8 |
| 1 KiB | 1 | 0.937–1.157 | 0.991 | 4/8 |
| 1 KiB | 2 | 1.043–1.336 | 1.168 | 8/8 |
| 1 KiB | 4 | 1.255–1.551 | 1.373 | 8/8 |
| 1 KiB | 8 | 1.643–1.867 | 1.713 | 8/8 |

**计算与判断（MED）：**64 B 在 1/2 lane 下 libzmq 八轮均领先；4 lane 也主要由
libzmq 领先。8 lane 接近且有交叉，FlowMQ 吞吐中位数低约 4.5%，CPU/条中位数低
34.7%，每批 P99 中位数低 22.4%。不能把节省 CPU 等同于吞吐已领先。

1 KiB 在单 lane 时接近，2/4/8 lane 则 FlowMQ 八轮均领先。8 lane 的吞吐中位数高
74.6%，CPU/条中位数低 63.2%，每批 P99 中位数低 57.7%。在本次 context/线程/HWM
配置下，FlowMQ 的多 lane 扩展与 CPU 效率更好；这不证明所有 libzmq 配置都会如此。

8 lane 下，两种 payload 的 FlowMQ CPU/条和每批 P99 都在八轮配对中全部低于 libzmq；
64 B 的吞吐则仍有交叉。效率与尾延迟的优势不能替代吞吐比较。

libzmq 8 lane 时 CPU/wall 约 11.8–12.1，FlowMQ 约 7.7，确认两者实际动用了不同的
并行 CPU 资源。不能将本对照描述为“同样八个核”；也没有采集 CPU stack、锁竞争、
分配或 cache-miss 计数，不能凭 CPU 增长就把差距归因为某个锁或复制路径。

进程峰值 RSS 为 15,040,512–32,256,000 bytes，最大 worker 启动偏移为 86.3 µs。
均为辅助诊断，不能用生命周期峰值给两种引擎排内存效率名次。两者都没有应用显式发送
重试，但 libzmq 阻塞调用内部的等待仍包含在 wall time 中。未测试相同总 CPU 预算、
共享 ZMQ context、更多 I/O threads 的调参、单端远程服务、TLS、Linux/macOS、热点
连接或开放环长时间饱和负载；原有 1 KiB 合批策略回归疑点也没有由本轮解决。

### 本轮验证

Release 构建成功，没有新增编译警告。`test_flowmq_socket_owners` 通过（6.72 秒），
包含既有 FlowMQ TCP/TLS、multipart、HWM、pool/关闭校验，以及双方 1/2/4/8 lane 的
copy workload 校验。`bench_flowmq_lane_zmq` 通过（177.46 秒），128 组均完成。
CSV 另核对总消息数、等额连接分配、worker 汇总、引擎/位置平衡和组结束时间，未发现
缺组或短跑。本次没有修改生产 runtime，未重复整套 transport 回归或运行 sanitizer；
ZMQ 关闭配置的构建未另行重跑。

完整日志：`build/lane-zmq-build.log`、`build/lane-zmq-correctness.log`、`build/lane-zmq.log`。


## 每线程独立 shared owner（2026-10-10）

### 所有权与测量协议

复用 `bench_flowmq_socket_owners.c` 的固定总量 workload，加入 explicit shared owner
变体。每个 worker 线程独立创建一个 context、一个 `flowmq_owner_t` 和其全部 sockets；
创建、bind/connect、warmup、收发、poll、关闭均在该线程完成，控制线程只负责启动屏障、
结果汇总和 join。线程间不共享 socket/owner/backend，不迁移连接或 payload，也不引入
逐消息队列。多个线程并行时，每线程仍只有一条 lane。

总量固定为八条 TCP PAIR 连接，每连接两端在同一个 lane，平均分到 1/2/4/8 个线程；
每个 owner 的硬 socket capacity 为 `2 * pairs_per_lane`，分别为 16/8/4/2，全局合计
始终为 16。listener 使用内部 `flowmq_socket_internal_bind_external()`，不是公开 bind
契约扩展，也不是统一端口的 accept 分配实验。

ordinary 与 shared 复用同一 128-message copy burst、序号/连接 ID/完整内容检查、
收发消息 HWM=128、背压重试、30 s deadline 和每轮末两次 credit progress；区别只有
socket 所属 execution domain 及对应的创建、bind、poll、关闭入口。接纳后源 buffer
可立即复用，接收必须按每连接 FIFO 校验；busy/full 由所属 lane 推进后重试，不丢消息。

任何 worker 错误通过已有 atomic first-error 通知其他 worker；所有已启动线程必须
抵达完成屏障并 join。完成较早的线程休眠，直到控制线程完成 CPU/时间采样，再在
原线程逐一 `flowmq_owner_close_socket()`，最后 `flowmq_owner_term()`、context term。
部分初始化也在原线程清理；join 失败沿用现有测试进程 fail-fast，不能释放仍在使用的
栈上 worker/gate。benchmark 计时循环不新增动态分配或跨线程可变状态，latency 存储仍按固定总量预分配。


### 八轮配对结果

环境沿用本文 Windows 11 10.0.26200、Ryzen 9 7940HX、MSVC 14.44.35207 Release、
Salts 2.3.0-rc.1 SDK（源码 `58ff08fc95b4aa1dc493c0b7080426b2c11d4959`）。
生产 runtime 仍为 `05287e2`；本轮在 `a578359` 基础上只改 benchmark/CTest 和报告。
线程不固定 CPU 亲和，测试进程内无其他并行 benchmark/build；未控制机器上其他进程
或 CPU 频率，因此报告完整八轮，保留波动与反转。

每组 2,048 轮 × 8 连接 × 128 消息 = **2,097,152 条消息**；共 128 组、
**268,435,456 条计时内消息**。64 B/1 KiB 交替领先，lane 数轮转，两种执行模式在每个
lane-order 位置各领先一次。原始数据见
[128 组结果](flowmq-shared-lanes-windows-20261010.csv)和
[480 个 worker 结果](flowmq-shared-lanes-workers-windows-20261010.csv)。

下表吞吐为各自八轮中位数，单位 **Mmsg/s**。配对变化逐轮计算 `shared/ordinary−1`
再取中位数，因此不等于吞吐中位数直接相除；“胜出”只指吞吐，不代表 P99 同样胜出。

| payload | 线程/lane 数 | ordinary | shared owner | 配对吞吐变化 | shared 胜出 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 64 B | 1 | 1.560 | 1.661 | +11.9% | 6/8 |
| 64 B | 2 | 2.413 | 2.725 | +18.0% | 7/8 |
| 64 B | 4 | 3.959 | 4.657 | +15.2% | 8/8 |
| 64 B | 8 | 6.884 | 7.329 | +7.9% | 7/8 |
| 1 KiB | 1 | 0.582 | 0.719 | +24.0% | 6/8 |
| 1 KiB | 2 | 0.908 | 1.142 | +21.4% | 8/8 |
| 1 KiB | 4 | 1.845 | 2.249 | +20.9% | 8/8 |
| 1 KiB | 8 | 3.363 | 3.982 | +19.3% | 8/8 |

CPU 成本和 burst P99 同样列各自八轮中位数；P99 的样本是**一个连接的 128 条消息
从首次发送到最后一条接收完成的时间**，包含同 lane 其他连接的排队与内容校验，不能
称为单消息 P99 或直接除以 128。CPU 数据为测试进程 user+kernel CPU 增量。

| payload | lanes | ordinary CPU ns/条 | shared CPU ns/条 | ordinary burst P99 µs | shared burst P99 µs |
| --- | ---: | ---: | ---: | ---: | ---: |
| 64 B | 1 | 640.75 | 599.77 | 1019.90 | 914.25 |
| 64 B | 2 | 812.11 | 737.61 | 576.30 | 542.90 |
| 64 B | 4 | 976.03 | 834.46 | 371.50 | 295.40 |
| 64 B | 8 | 1121.31 | 1054.26 | 238.50 | 218.85 |
| 1 KiB | 1 | 1713.63 | 1370.91 | 2612.50 | 2178.35 |
| 1 KiB | 2 | 2171.84 | 1728.53 | 1724.80 | 1335.90 |
| 1 KiB | 4 | 2115.97 | 1717.36 | 858.80 | 681.25 |
| 1 KiB | 8 | 2257.53 | 1903.62 | 426.10 | 367.05 |

**事实：**shared 的 CPU/wall 中位数随 1/2/4/8 lanes 分别为：
64 B **0.994 / 1.975 / 3.895 / 7.648**；1 KiB **0.990 / 1.975 / 3.859 / 7.656**。
每组所有 worker 的运行区间均有共同交集，等额完成自己的连接和消息，说明多个 owner
在并行运行，而非多个线程轮流操作一个 owner。

**计算：**按上表吞吐中位数相除，shared 从 1→8 lanes 提高约 **4.41× / 5.54×**
（64 B / 1 KiB）。若逐轮计算 8-lane/1-lane 再取中位数则为 4.59× / 7.18×；
两个统计量不同，尤其 1 KiB 各组波动较大，不将任何一个值声称为固定加速比。
八 lane 相对同批 ordinary 的配对 CPU/条下降 **5.0% / 16.1%**，burst P99 下降
**5.2% / 16.7%**。但增加 lane 并非免费：shared 自身 8/1 的配对 CPU/条中位数仍增加
66.5% / 7.3%，吞吐、尾延迟和 CPU 效率需要分别看。

**事实与推论：**同 payload/lane 下两种模式的 poll 调用数完全一致，发送接纳重试
均为零。64 B 总 poll 数依次为 8,192/16,384/32,768/65,536，1 KiB 为
22,528/45,056/90,112/180,224。本轮没有靠减少调用者的 credit progress 获得收益；
差别来自 execution domain 的推进/等待/完成分发实现。但这不是只替换 listener 的
单变量实验，不能把全部收益归因于某一个系统调用，也不能从零发送重试推断无背压。

**MED：**收益存在反转和明显波动。八 lane shared 的范围为 64 B **6.642–8.496 Mmsg/s**、
1 KiB **3.645–5.806 Mmsg/s**；相应 ordinary 为 **6.403–7.519 / 3.049–5.264 Mmsg/s**。
小消息八 lane 有一轮不胜 ordinary，单 lane 两种 payload 各有两轮反转。八轮配对支持
继续使用固定 lane 的 shared wait 方向，但不构成统计显著性、所有负载或线性扩展保证。
进程生命周期 peak RSS 最大约 30.40 MiB；这是整个进程的历史高水位，不能归因到某组
或据此比较单个 owner 的内存峰值。

### 验证、边界与复现

- 完整 Release build 通过；`test_flowmq_shared_lanes` 连续十轮通过，覆盖两种 payload、
  四种 lane 数和 pool 开关，共 160 个配置运行。关闭错误会传播为失败，不能将未关闭的
  owner 当作成功测量。现有 owner、batch、fault、listener 和 `test_flowmq_socket_owners`
  九个相邻 CTest 全部通过（8.97 s），后者还回归旧 ordinary/TLS/multipart/ZMQ 路径。
- `bench_flowmq_shared_lanes` 完成全部 128 组；每条计时消息均核对长度、完整内容、
  连接 ID 和 FIFO 序号。CSV 核对了唯一组、等额分配、位置平衡、worker 消息/poll/retry
  汇总、共同运行区间和组结束时间，无缺组或短跑。
- 未修改生产 runtime 或公开接口；未重复完整 core/transport suite，未运行 sanitizer、
  Linux/macOS。listener 仍使用内部 TCP 入口，不能把该 benchmark 当作公开 owner bind
  已完成发布门槛。
- 本轮只验证固定连接、每连接两端同 lane 的 TCP PAIR。尚未测跨 lane 两端、远程端点、
  统一监听端口分配、非 PAIR 多 lane、TLS shared listener、动态负载迁移或跨线程投递。
  未在本批次重测 ZMQ 性能；不能拿本轮 shared 数值直接除以上一批 ZMQ 数值。

在 VS developer environment 使用已有 Release user preset：

```powershell
cmake --build --preset win-release-user
ctest --preset win-release-user -R "^test_flowmq_shared_lanes$" --repeat until-fail:10 -V
ctest --preset win-release-user -R "^test_flowmq_(socket_owners|owner|owner_batch|owner_fault|owner_fault_pool|owner_listener|owner_listener_pool|owner_listener_fault|owner_listener_fault_pool)$" --output-on-failure
ctest --preset bench-win-release-user -R "^bench_flowmq_shared_lanes$" -V
```

benchmark 默认 2,048 rounds、8 repeats；沿用 `FLOWMQ_LANE_BENCH_ROUNDS`（1–4096）和
`FLOWMQ_LANE_BENCH_REPEATS`（1–9），完整位置平衡需要 8 repeats。日志为
`build/shared-lanes-build-final.log`、`build/shared-lanes-correctness.log`、
`build/shared-lanes-regression.log`、`build/shared-lanes-comparison.log`。
