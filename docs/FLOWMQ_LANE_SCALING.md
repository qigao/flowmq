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

## 低负载 CPU 与等待策略（2026-10-10）

### 问题与候选方式

**事实：**饱和测试持续使用 `owner_poll(..., 0)`，shared backend 不会让调用方的
忙循环自动休眠。当前 owner poll 先执行零等待推进，再按剩余调用超时等待；各个
socket 的 CNet/FlowMQ deadline 可进一步缩短 backend wait。一次 owner wait 仍
推进所有 owner-local sockets，items 只决定哪些 readiness 可以让调用提前返回。

| 方式 | CPU 与延迟取舍 | 本轮处理 |
| --- | --- | --- |
| 全程零超时 poll | 空闲仍扫描；避免等待超时引入的定时迟到 | 保留对照和调用方选择 |
| 有工作时批量处理，空闲时事件等待 | 减少空转；须核实平台等待和调度迟到 | 测量现有 API，不改变默认语义 |
| 有限自旋后等待 | 可能兼顾短间隔到达与空闲 CPU；不能自动解决定时等待过冲 | 尚未测量，不指定猜测的自旋阈值 |
| 减少独立 lanes | 减少扫描、线程和唤醒；单 lane 容量/延迟可能成为约束 | 同批比较 1/8 lanes，连接静态归属 |
| 多线程竞争同一 owner / Leader-Follower | 需要新的同步、移交和关闭协议，不直接消除空转 | 当前 owner 契约不支持，不采用 |

**选择：**延续每线程独立 context + owner + sockets，调用方按负载和延迟目标选择
lane 数与 poll timeout。等待时监听接收侧 POLLIN，仅在确实有待发送工作时监听对应
POLLOUT；常驻可写事件会让 poll 立即返回。`events=0` 可以推进，但不能靠普通数据
readiness 提前结束正超时调用。owner socket 使用 DONTWAIT 收发，并由 owner poll
推进；initialized owner socket 需要普通阻塞 progress 时返回 ENOTSUP。头文件此次
补充这些已有契约以及 context 生命周期串行化要求，没有改变签名、ABI 或协议。

跨线程应用命令不能只排入队列后假定 native wait 自动醒来。未来若需要此能力，须
先明确有界队列、payload 移交、空转为非空时合并唤醒、取消/关闭和 drain 协议。
当前 owner poll 不提供这种应用命令唤醒契约，不引入动态连接迁移或共享 owner 锁。

### 测量协议

基于 `583e66d` 的正式 TinyTest harness 增加 `bench_flowmq_paced_lanes`，沿用上述
Windows/SDK/编译器配置。固定八条 TCP PAIR 连接、每连接 batch=128、收发 HWM=128，
两端同 lane，listener 仍走私有 qualification 入口。比较 64 B/1 KiB、1/8 lanes、
spin/wait；每组 256 轮、六次配对重复，共 48 组，每组 **262,144 条**，合计
**12,582,912 条**计时消息。两种策略各在三次重复中领先；payload 和 lane 顺序轮转。
每条消息核对长度、连接 ID、FIFO 和完整内容，全部发送重试计数为零。

每个 worker 的固定释放计划为第 k 轮在 `start + k * 5 ms` 释放下一批，全局计划
速率为 `8 * 128 / 0.005 = 204,800 条/秒`。迟到后补齐所有批次，不丢弃工作，也不在
每轮完成后重新计算完整的 5 ms sleep。有数据时两种策略执行相同零超时推进/credit
循环；等待下一次释放时，spin 在接收侧 POLLIN items 上反复 poll(0)，wait 在同一
owner 上等待距下一次释放的剩余时间，向上取整到毫秒。控制/credit 仍正常推进；
已有数据全部接收后若出现意外 readiness/error 则测试失败，不继续空转掩盖问题。

这是**本地定时突发测试**，不是远端报文或外部线程随机到达时的唤醒延迟测试。
burst P99 是首次实际发送到该连接最后接收；scheduled completion P99 是计划释放到
最后接收，包含迟到和同 lane 排队；max release lag 是全组最坏释放迟到。平均完成
速率包含全部等待。setup/warmup/cleanup 不计入测量；先采样，再允许 worker 清理，
所有线程 join 后汇总。scheduled latency 数组按固定总量预分配，每线程写自己的
区间，热循环不新增分配或跨线程消息共享。

CPU 时间沿用进程 user+kernel 增量。本轮部分短促执行的 64 B 单 lane wait 样本
出现零 CPU 时间增量，不能解释为零成本。因此 Windows metrics 适配层补充
[QueryProcessCycleTime](https://learn.microsoft.com/en-us/windows/win32/api/realtimeapiset/nf-realtimeapiset-queryprocesscycletime)
作为独立工作量计数，覆盖所有线程的 user/kernel cycles。本轮所有 cycle 增量均为正；
保留原单位，不折算时间、频率、能耗或跨机器性能。非 Windows 明确标为
`cpu_cycles_available=0`，仍保留原 getrusage CPU 时间，不伪造 cycle 数。

### 六轮结果

原始 [48 组 CSV](flowmq-paced-lanes-windows-20261010.csv) 和
[216 个 worker CSV](flowmq-paced-lanes-workers-windows-20261010.csv) 保留全部重复。
表中分别取各自六轮中位数；配对变化逐轮计算 `wait/spin - 1` 再取中位数。
CPU/wall=1 表示一个逻辑 CPU 的执行时间，不是整机 100%。

| payload | lanes | spin CPU/wall | wait CPU/wall | spin cycles/条 | wait cycles/条 | 配对 cycles 下降 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 B | 1 | 0.994 | 0.097 | 11,636 | 1,570 | 86.5% |
| 64 B | 8 | 7.958 | 0.327 | 92,933 | 3,975 | 95.7% |
| 1 KiB | 1 | 0.994 | 0.353 | 11,653 | 3,508 | 69.9% |
| 1 KiB | 8 | 7.939 | 0.419 | 92,638 | 5,648 | 93.9% |

CPU/wall 来自上述存在零读数/离散变化的 OS 时间计数，尤其单 lane wait 的精确比例
不应过度解读。cycles 也受平台计数实现与频率条件影响，只比较同机配对结果。

| payload | lanes | 完成速率 spin→wait（万条/s） | burst P99 spin→wait（ms） | scheduled completion P99 spin→wait（ms） |
| --- | ---: | ---: | ---: | ---: |
| 64 B | 1 | 20.472 → 20.349 | 0.679 → 1.210 | 0.828 → 18.331 |
| 64 B | 8 | 20.477 → 20.344 | 0.251 → 0.485 | 0.259 → 16.796 |
| 1 KiB | 1 | 20.458 → 20.382 | 1.629 → 2.387 | 1.794 → 16.939 |
| 1 KiB | 8 | 20.474 → 20.406 | 0.371 → 0.691 | 0.389 → 16.570 |

配对平均完成速率下降依次为 **0.60% / 0.65% / 0.37% / 0.34%**。但它们会补发迟到
批次，不能由平均速率接近就声称满足 5 ms 实时周期。wait 的组内最坏 release lag
中位数约 18.65–21.83 ms，全批最大 **27.48 ms**。尚未分离 native wait 超时精度、
OS 调度、CPU 频率与其他进程的贡献，不能把所有迟到直接归因于 CNet。

poll 次数中位数从 64 B 单/八 lane 的 **281,979 / 12,231,431** 降至
**1,107 / 8,880**；1 KiB 从 **244,882 / 11,326,686** 降至 **2,898 / 23,208**。
这直接支持低负载存在大量可避免的空转。这是应用 owner_poll 次数，不是内部
observe/system-call 次数；未收集硬件能耗或上下文切换计数。

**MED：**等待省 CPU，但本配置显著增加定时迟到和尾延迟，不宜作为所有低延迟场景
的默认策略。严格周期应先验证平台 timer/native wait，再评估有限自旋；简单给每次
poll 加固定 sleep 或将 timeout 一律改为 1 ms 没有本轮依据。

**推论：**此约 20 万条/秒目标下，单 lane 已能完成同样消息量，八 lane 不会提高受
发送计划限制的平均速率，反而增加 CPU 工作量；八 lane busy 模式有更低 burst 和
scheduled P99。应按目标吞吐与 P99 选最少足够的静态 lanes，不能只按核数配置。
结论不覆盖单热点连接、非 PAIR、跨机、TLS 或更高目标负载。

### 验证与迁移边界

Release 完整 build 通过。`test_flowmq_paced_lanes` 与 `test_flowmq_owner` 各连续
十轮通过；前者覆盖 80 个定时配置，后者验证空接收的 DONTWAIT 返回 EBUSY、flags=0
返回 ENOTSUP，以及正超时无接收 readiness 的结果。48 组 benchmark 全部通过
（63.77 s），随后 14 个相邻 owner/lane/batch CTest 全部通过（8.59 s），包括旧
ordinary/TLS/multipart/ZMQ 正确性路径。CSV 审核了唯一组、策略位置、worker
消息/poll/retry 汇总和时间区间，无缺组或短跑。

没有修改生产 runtime、公开签名、协议、默认等待策略或 listener 发布门槛，无需
数据迁移。调用方可在既有 API 范围内逐 lane 选择等待，P99 不满足则恢复原 poll
策略，不在运行中迁移 socket。未运行 Linux/macOS、sanitizer、完整 core/transport
suite，也未在本批次比较 ZMQ CPU 或外部到达唤醒延迟。

在 VS developer environment 复现：

```powershell
cmake --build --preset win-release-user
ctest --preset win-release-user -R "^test_flowmq_(paced_lanes|owner)$" --repeat until-fail:10 --output-on-failure
ctest --preset bench-win-release-user -R "^bench_flowmq_paced_lanes$" -V
ctest --preset win-release-user -R "^test_(flowmq_(socket_owners|shared_lanes|paced_lanes|owner|owner_batch|owner_fault|owner_fault_pool|owner_listener|owner_listener_pool|owner_listener_fault|owner_listener_fault_pool)|bench_flowmq_batch.*)$" --output-on-failure
```

本对照固定 256 rounds、6 repeats、5 ms period，不受饱和 lane benchmark 环境变量
影响。最终日志为 `build/paced-lanes-build-final.log`、`build/paced-lanes-repeat.log`、
`build/paced-lanes-cycles.log`、`build/paced-lanes-regression.log`。早期
`build/paced-lanes-comparison.log` 没有 cycle/计划完成 P99，不与最终 CSV 混合统计。

## Disruptor retained mailbox 与 CNet SG（2026-10-10）

### 路径、所有权与边界

本轮内部验证将应用提交与 owner 的 I/O wait 接通：

```text
producer owned buffer → Disruptor slice descriptor → lane owner
    → flowmq_send_slice → FMQ framing + payload ranges
    → cnet_send_slicev → NativeIO SG → flowmq_recv_slicev
```

复用 Salts 的有界 Disruptor，不新增队列库或隐藏线程。每 lane 一个 owner 和一条
TCP PAIR 连接，两端同 lane；两个独立 producer 向该 lane 的 MPSC mailbox 提交，
唯一 consumer 是 owner。比较 1/8 lanes，因此对应 3/24 个工作线程，另有一个等待
汇总的控制线程；不绑核，不等同于八个物理核或此前只有八个 owner 线程的 workload。
连接数和总消息数随 lane 数增加，本轮只比较同 lane 数/负载下的策略，不声称固定
总连接数下的多核加速比。

每 mailbox 128 个槽位，entry 仅含一个拥有引用的 `mem_slice_t`；每 producer
预分配 64 个 buffer，源 backing 预算为 `lanes * 2 * 64 * payload_bytes`，最大
64 MiB。这个上限不包含 FlowMQ/CNet 的内部缓存。producer 永久保留一个基础引用，
仅在引用数回到 1 且没有其他线程能从裸指针重新 retain 时允许重写。此限制是协议
的一部分，不能将引用数查询推广为任意共享对象的并发写锁。

producer 先准备最多 16 个 slice，再 claim/publish 一个范围，成功后所有权移入
mailbox；满时在 30 s 总 deadline 内 yield 重试，不丢弃、不扩容。发布后每批最多
一次 native wake；失败不能重投已经提交的批次。consumer 持有当前 slot，只有发送
接纳成功才释放 slice 和 slot；busy/full 时继续推进 I/O。retained 发送由 CNet
继续保活原 buffer，基础引用不能因此提前允许重写。copy 对照也使用同一 retained
mailbox，仅 consumer 调用 `flowmq_send()`，隔离队列交接方式的差异。

接收统一使用 `flowmq_recv_slicev()`，直接遍历多段，校验跨段 header、lane/producer
ID、每 producer FIFO、长度和完整内容，再逐段 release；没有接收 coalesce 或复制
到第二个完整 payload。关闭必须先 join 所有 producer/wake 调用者，再清理 owner
和剩余 queue 引用，检查所有源 buffer 回到基础引用数，最终释放回调恰好执行一次。
取消路径同样检查引用归还。资格验证覆盖固定连接，不支持生产者动态注册或连接迁移。

内部新增 `flowmq_owner_internal_step/wake`，复用既有 CNet advance/observe/route
顺序。step 是单次推进，纯控制 wake 后返回给 mailbox 循环；空 owner 也观察 backend，
不调用不可唤醒的 sleep。已有公开 owner poll 和 close 继续走原入口和行为；这两个
内部入口不安装，不构成公开线程安全 admission/shutdown API。只有队列确认暂无
可消费项、无 pending slot、所有已接纳消息已接收时，owner 才允许最长 1 s 的 wait；
队列或网络还有工作时仍零等待推进。NativeIO 的持久 wake 衔接检查队列与进入等待
的竞态，不用近似 empty 查询省略通知。

### 测量口径

比较 send=`copy`/`sg`（后者指 retained admission）、wait=`spin`/`wake`、
64 B/64 KiB、1/8 lanes，以及 producer 连续提交和每批前 sleep(5 ms) 两种负载。
每个配置四轮，四种策略循环换位，使每种策略各在每个位置运行一次。小消息连续
提交用 2,048 rounds，其他用 32 rounds；每 producer 每 round 发布 16 条。CPU
采样前等待所有 producer 和 owner 到达启动屏障，采样后 join producer，再允许
owner 关闭。源 buffer/queue 预分配不计入计时，payload 构造、逐字节校验、引用操作、
背压和所有工作线程的 CPU 均计入，不能当成纯 CNet 网络带宽。

`prepare_receive_p99_ns` 从取得可重写 buffer 后、填充 payload 前计时，直到接收
校验完成；包含随后准备同批消息、入队与网络排队，不包含取得该 buffer 之前的等待，
也不包含 producer 的 sleep。buffer 等待次数另列，吞吐和 CPU 包含这部分成本。
低负载 sleep 的实际间隔受 Windows 调度影响，不声明固定 offered rate，也不与
前一节“按 5 ms 计划释放”的 P99 混用。CPU cycles 是测试进程全部工作线程的原始
计数，CPU 时间的零/离散读数继续保留，不据此宣称零成本。

**MED：**高负载 producer 在 buffer/queue 满时使用有界 yield 重试，仍会消耗 CPU；
原型没有验证 producer 侧空间/引用归还的阻塞通知。copy 与 retained 的可接纳队列、
实际在途数据量和收包分段也不同，因此收益不能全部归因于少一次 memcpy。此实验
证明的是完整调用路径的差异，不是等窗口、等内存预算下的单个 SG 系统调用 A/B。

**事实：**FlowMQ 的 retained PAIR 选择 peer 时先检查 write-idle 和 outbound queue，
已有写入时返回 busy；复制发送可以排队并在后续推进中合批。这里的 `sg` 标签不表示
copy 对照完全不用 SG：其内部复制出的 buffer 后续也会进入 CNet retained/SG 路径。
首要优化边界因此在 FlowMQ admission 与跨消息 batching，而非再引入一层 ring。
不得用 SNDMORE 合并独立消息来获得不等价的“batch 吞吐”。

### 四轮完整结果

环境沿用 Windows 11、Ryzen 9 7940HX、MSVC Release 和 pinned Salts 2.3 SDK。
[128 组原始 CSV](flowmq-mailbox-sg-windows-20261010.csv) 对应最后一次完整运行，合计
**9,879,552 条消息、19,940,769,792 字节 payload**。每组持续约 67.6–1,131.5 ms。
四种策略位置、配置唯一性、消息总数、每批一次 wake 数和非零 cycle 计数均核对通过。
下面吞吐列为各自四轮中位数，单位 **kmsg/s**；变化是逐轮计算 `retained/copy - 1`
后取中位数，不能用两个吞吐中位数相除替代配对计算。

| payload | lanes | 等待策略 | copy 吞吐 | retained 吞吐 | 配对吞吐变化 | 配对 CPU cycles/条变化 |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| 64 B | 1 | spin | 678.817 | 93.281 | −85.2% | +570.4% |
| 64 B | 1 | wake | 741.005 | 98.954 | −87.1% | +681.3% |
| 64 B | 8 | spin | 3277.874 | 504.696 | −84.2% | +544.9% |
| 64 B | 8 | wake | 3859.755 | 495.789 | −87.2% | +679.7% |
| 64 KiB | 1 | spin | 6.328 | 8.229 | +23.7% | −13.7% |
| 64 KiB | 1 | wake | 6.151 | 6.899 | +11.5% | −2.9% |
| 64 KiB | 8 | spin | 20.866 | 41.064 | +101.4% | −47.2% |
| 64 KiB | 8 | wake | 22.443 | 40.013 | +79.0% | −40.2% |

大消息八 lane 的两种等待模式均四轮吞吐胜出；单 lane spin 四轮胜出，wake 三轮
胜出。小消息所有配置 retained 均零轮胜出。大消息八 lane wake 的准备到接收完成
P99 中位数从 **88.349 ms → 26.593 ms**，配对下降 **69.7%**；spin 从
**101.556 ms → 27.709 ms**，配对下降 **73.1%**。这些是带 producer 构造和消费者
完整校验的队列 workload，不是单包网络延迟或 SDK 的极限吞吐。

**事实与推论：**小消息八 lane wake 每组 524,288 条消息时，retained 的 send busy
和 owner step 次数中位数分别为 **560,971 / 560,984**，copy 对照为
**20,444 / 53,231**。结合 write-idle admission 检查，主要可见限制是逐消息发送与
推进，Disruptor 的 16 条批量发布并未自动成为网络 batch。此处未采集 native syscall
或硬件 cache-miss，不能把所有 CPU 差异归因到某一系统调用或引用计数。

在每批 sleep(5 ms) 的低负载下，比较**同一种发送方式**的 wake 相对 spin：

| payload | lanes | copy 配对 cycles/条下降 | retained 配对 cycles/条下降 | retained P99 spin→wake（ms） |
| --- | ---: | ---: | ---: | ---: |
| 64 B | 1 | 97.9% | 96.1% | 0.582 → 0.804 |
| 64 B | 8 | 97.8% | 94.8% | 0.886 → 0.890 |
| 64 KiB | 1 | 59.6% | 71.8% | 5.165 → 4.144 |
| 64 KiB | 8 | 18.7% | 58.2% | 6.880 → 5.887 |

这里的 wake 由真正发布的 mailbox 命令触发，owner 的最大 wait 虽为 1 s，也不要求
等超时才发现数据。它与上节依赖定时 timeout 释放数据的实验不同。小消息单 lane 的
P99 仍有上升，不能声称唤醒零成本或所有尾延迟都会改善。低负载组 buffer 等待计数
均为零；高负载 producer 的 yield 重试仍需单独优化。

### 选择、验证与剩余工作

本轮选择为**大消息优先保留源 buffer、接收保留分段、小消息保留复制合批**，不加入
未经测量的自动阈值，也不将所有应用调用强制改成 send_slice。下一项有证据支撑的
优化是利用既有有界 outbound 结构合并多个 retained FMQ frame，再提交 CNet vector：
必须同时限定消息数、总字节和 range 数，保留每条消息的 wire 边界、credit/FIFO、
背压和失败释放。第一条 idle 消息不应为凑批而等待固定 timer。只有完成同语义 A/B
和取消/部分完成测试后，才能考虑扩大公开 admission 契约。

当前只交付内部 qualification seam 和正式 benchmark/test，不发布 mailbox API。
现有公开 owner poll、send/recv、listener bind 和默认等待策略均保持原行为；回滚可
移除内部测试接入及 step/wake 入口，无协议或持久数据迁移。

验证结果：

- 完整 Release build 通过，新增 benchmark 编译无 warning。
- 四个 mailbox 正式测试连续十轮通过：满队列引用归属/发布缺口，空 owner 的等待前
  与等待中 wake，1/8 lanes 和两种 payload 的 MPSC 复用收发，以及 producer 并发时
  取消并回收所有 retained 引用。最终版本每条发送还核对 retained admission 持有源
  引用、copy admission 不额外持有源引用；所有 backing 最终回调恰好一次。
- 33 个 `flowmq-transport` CTest 全部通过（210.21 s），覆盖普通/池化 socket、owner、
  fault/listener、public C/C++ 和现有多核示例。随后最终 benchmark/test-only 的启动
  屏障、引用断言和测量时长调整经重新完整 build、十轮 mailbox 测试和全部 128 组
  benchmark 验证（57.12 s）；runtime 在两批验证间未再修改。
- 尚未覆盖原生 wake 失败注入、跨机、Linux/macOS、sanitizer、TLS mailbox 路径或
  retained 跨消息 batching。未与 ZMQ 同批比较；无 8 个物理核 affinity 结论。

在 VS developer environment 复现：

```powershell
cmake --build --preset win-release-user
ctest --preset win-release-user -R "^test_flowmq_owner_mailbox$" --repeat until-fail:10 -V
ctest --preset bench-win-release-user -R "^bench_flowmq_owner_mailbox$" -V
ctest --preset win-release-user -L "flowmq-transport" --output-on-failure
```

最终日志：`build/mailbox-sg-build-qualified.log`、`build/mailbox-sg-qualified.log`、
`build/mailbox-sg-qualified-comparison.log`、`build/mailbox-sg-transport-regression.log`。
早期短时长的 `build/mailbox-sg-comparison.log` 不进入最终 CSV，也不与本表混合统计。

## 有界 retained SG 跨消息合批（2026-10-10）

### 实现与兼容边界

本轮复用 FlowMQ 的 outbound publication/credit/HWM 和 CNet retained vector，
增加不安装的 TCP PAIR 单部消息排队入口；公开 `flowmq_send_slice()` 仍然是原来的
immediate admission，已有 fanout 仍一条 publication 对应一条 CNet 逻辑写入。
私有配置只允许 owner 创建的 PAIR 在 connect 前指定每次 1–16 条消息。

首条 idle 消息直接提交。后续消息进入既有有界 outbound queue，每个 slot 保有
完整 framing 与 payload slices；flush 只合并连续 retained entries，最多 16 条、
`CNET_RETAINED_VECTOR_MAX=32` 个 ranges，且不超过既有 `max_encoded_size`。
遇到复制项停止当前 retained batch，按原队列 FIFO 继续。没有 payload flatten、
SNDMORE 合并或固定凑批 timer。队列成功接纳后，调用方可以释放自己的引用，但
backing 在 FlowMQ queued ownership 或 CNet 在途 ownership 结束前保持不可变。

CNet 接纳失败不弹出任何 slot；成功后才释放 FlowMQ publication，由 CNet 保活到
真实 terminal。FlowMQ 记录这一逻辑写入的消息数与 payload 总字节，在 on_send
完成时归还相应 HWM 占用；发送 credit 仍在各消息首次接纳时提交一次。取消和关闭
复用原先的 queued release、native drain，slot 释放不意味着 backing 可立即复用。
每个 queued publication 仍会分配固定上限的 descriptor 对象并克隆引用；本轮没有
消除这部分分配或引用成本。复杂度为每批 O(messages + ranges)，临时范围数组有界。

按 `$cmeta`/`$cnet`/`$ace` 核对后，既有 CMeta Schema 继续描述静态 pattern/peer
状态，运行时队列与批次仍由 FlowMQ owner 管理；NativeIO/CNet 仍是完成事实源。
没有引入第二套 ACT 终态记录、Leader/Followers、Component registry 或 scheduler。
本轮 SDK 实际仍为 `2.3.0-rc.1` / `58ff08fc95b4aa1dc493c0b7080426b2c11d4959`；
使用 RC2 skill 指引不代表 SDK 或 CMeta ABI 已升级。

### 同批四种路径对照

继续使用上节的相同 mailbox、producer/source buffer 数量、TCP PAIR、完整内容校验、
1/8 lanes（3/24 工作线程）、无 affinity、64 B/64 KiB、连续/每批 sleep(5 ms) 负载。
四种 send 策略分别为 `copy`、公开立即 `sg`、私有 `queued_sg`（最多一消息/写入）、
私有 `batch_sg`（最多 16 消息/写入）。后两者拥有相同 admission/queue 预算，用于
隔离跨消息 batching 的效果。每组固定 wait 策略下四种 send 循环换位四轮；wait、
payload 和 lane 顺序也轮换。只在本轮配对，不能将上节不同运行的绝对值当作回归比。

[256 组原始 CSV](flowmq-retained-batch-windows-20261010.csv)：共 **19,759,104 条消息、
39,881,539,584 字节 payload**，每组约 74.6–1,400.7 ms。新增 `sg_*` 字段记录写入数、
消息数、ranges 数及每批最大消息/range 数，只统计私有 queued-send 的成功 CNet admission，
包含首条直接发送；不统计 native submission、peer receipt、复制或公开立即发送。
`copy`/`sg` 的这些字段为零代表未采样，不能据此解释为没有网络写入。

以下为满载四轮吞吐中位数，单位 kmsg/s；百分比是每轮配对比值变化的中位数：

| payload | lanes | wait | copy | immediate SG | queued SG/1 | batch SG/16 | batch 相对 queued 吞吐 | cycles/条变化 |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 B | 1 | spin | 746.667 | 113.178 | 107.809 | 449.408 | +314.3% | −75.8% |
| 64 B | 1 | wake | 823.575 | 107.387 | 106.472 | 419.520 | +293.9% | −74.6% |
| 64 B | 8 | spin | 3203.831 | 476.376 | 462.354 | 1851.664 | +300.7% | −75.0% |
| 64 B | 8 | wake | 3805.072 | 500.765 | 470.270 | 1755.129 | +270.5% | −73.3% |
| 64 KiB | 1 | spin | 5.458 | 6.568 | 6.109 | 5.884 | −0.3% | −5.1% |
| 64 KiB | 1 | wake | 5.008 | 6.054 | 6.330 | 5.876 | −6.3% | +5.6% |
| 64 KiB | 8 | spin | 14.328 | 31.992 | 37.002 | 30.018 | −9.5% | +17.7% |
| 64 KiB | 8 | wake | 16.377 | 30.211 | 34.869 | 30.509 | −8.0% | +4.6% |

**事实：小 retained 消息有稳定的合批收益。**64 B 所有 lane/wait 配置均四轮胜过
queued SG/1 和 immediate SG，但均零轮胜过 copy。八 lane wake 的 CNet 逻辑写入
从每组 **524,288 → 36,864.5**（四轮中位数），降约 93%；平均约 14.2 消息/写入，
最大 16 消息/32 ranges。其 P99 中位数 **2.699 → 0.810 ms**，配对下降 70.0%；
send busy 中位数 **493,890 → 73,414**，owner step **561,006 → 106,319.5**。
这证明单纯排队不足，跨消息合批才减少了可见的逐消息推进成本。

**MED / 事实：大 retained 消息没有稳定的最大批收益。**64 KiB 八 lane wake 中，batch
相对 queued 吞吐只赢一轮，P99 中位数 **31.882 → 41.113 ms**、配对增加 25.5%。
每组写入 **8,192 → 520**，并未带来相应 CPU 或吞吐改善。大消息样本波动明显，
例如 queued wake 的一轮 P99 达 241.84 ms；四轮结果不足以推导最优 batch 大小，
也不能把所有差异归因于 SG 系统调用。

**推论：**大批次会将多个 source buffer 的释放绑定到同一逻辑终态，可能拉长占用和
尾延迟；producer 填充、逐字节接收校验及 yield 等待仍在计时范围内。需以 1/2/4/8/16
消息与批次字节上限的后续对照和 profiling 区分这些因素，不能仅凭写入次数越少就
选择越大的 batch。小帧需要交错的 framing/payload，32-range 逻辑上限只能容纳
16 个两段消息；复制路径能够将更多小帧装入连续 buffer。这里没有测量 native syscall 数。

同一 batch SG 策略的低负载 wake 相对 spin，cycles/条配对下降为：64 B 单/八 lane
**97.5% / 97.2%**，64 KiB 单/八 lane **61.1% / 33.2%**。仍是实际 publish 唤醒，
不是固定定时 tick。低负载 sleep 不是固定 offered rate；高负载 producer 仍 yield
重试，不能把 cycles/条下降描述为整机 CPU 利用率同比下降。

### 验证与选择

采用内部 opt-in 验证，保留生产默认路径。小 borrowed 消息继续适合复制合批；小
retained 消息已验证可用跨消息 SG 显著改善。大 retained 消息继续保留立即发送和
较小批次候选，不设置未经扫描验证的自动大小阈值。回滚只涉及私有接入、计数与
batch qualification；无公开接口、协议、持久格式或依赖升级。

正式测试增加配置/owner 拒绝、首条立即提交、HWM 消息与字节独立饱和、拒绝不
增加引用、copy/retained 混合 FIFO、64 B/65,537 B/1 MiB 的 16/8/1 消息 range 上限、
queued 与 native 同时持有引用时关闭。原有 MPSC wrap 和取消测试扩展到全部四种
发送策略。所有 backing 最终归还基础引用，release callback 恰好一次。

完整 Release build 通过；最终 **7 个 mailbox 测试连续十轮通过**（17.36 s）。
**33/33 transport CTest 通过**（211.25 s），包括普通/池化 socket、retained
multipart/fanout、owner/listener/fault、公开 C11/C++17 和多核示例。

最终验证日志：`build/retained-batch-final-build.log`、
`build/retained-batch-final-correctness.log`、`build/retained-batch-comparison.log`、
`build/retained-batch-regression.log`。benchmark 为 256 组、124.93 s；之后只增加
配置拒绝的正式测试，运行路径和测量实现未改。复现沿用上节 CMake/CTest 命令。
未覆盖新合批路径的 CNet admission 失败注入、native 部分失败注入、TLS、跨机、
Linux/macOS 或 sanitizer；32-range 实测包含多次 native 提交，但不等同于故障注入。
