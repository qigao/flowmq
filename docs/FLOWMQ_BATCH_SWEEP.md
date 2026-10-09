# FlowMQ / libzmq 批量扫描（2026-10-10）

本机 TCP PAIR、64 B copy 消息：增大 batch 对两者都有帮助。FlowMQ 在 batch
1/8/16 时吞吐中位数更高，约在 32 条附近被 libzmq 超过；到 64–128 条时
FlowMQ 的提升趋缓。后续受控实验确认：分散小 buffer 与 32-entry flush 上限导致
重复的有序写入/progress 成本，是该负载慢于 libzmq 的重要原因。
下文原始扫描及干预数据对应 `954f87e` 诊断阶段，当时尚未修改生产策略。
后续有界生产策略的设计与验证单列在文末，不与历史采样混合比较。

## 方法与边界

- Windows 11 `10.0.26200`，Ryzen 9 7940HX（16 核 / 32 逻辑处理器），MSVC
  `14.44.35207` Release。FlowMQ 基于 `ef3234f`；Salts `2.3.0-rc.1`、
  SaltsUtils `4.3.0-rc.1`、libzmq `4.3.5#2`，与
  [前次对照](FLOWMQ_ZMQ_COMPARISON.md) 使用同一发布 SDK。
- 每组固定 **262,144 条**消息，batch 为 **1/8/16/32/64/128**。
  每批全部接收后再开始下一批；每条包含递增序号，验证全部 64 B 内容及 FIFO 顺序。
- 共六轮，轮换 batch 顺序；每个 batch 下双方各有三轮先执行、三轮后执行。
  共 72 组正常库测量，18,874,368 条计时消息，不含预热。
- 每组重新创建两个 socket、预热八批，再开始计时。setup、close、输出排除在测量区间外。
  FlowMQ peer pool 关闭；HWM 默认；单个应用线程，没有额外 benchmark worker。
- 所有 FlowMQ batch 均使用非阻塞 `flowmq_poll(..., 0)`；每批结束保留原 queued
  benchmark 的两次 credit progress。libzmq 使用默认后台 I/O 线程及阻塞 send/recv，
  设置十秒操作超时，避免故障时无限阻塞。
- 每组 wall time 包括发送、接收、内容验证、progress 和批次准备；进程 CPU 时间覆盖
  用户态与内核态的全部线程，包括 libzmq 后台线程。CPU 时间不等于 wall latency。
  Windows CPU 计时存在量化误差，尤其是短组；没有控制 CPU 亲和、频率或系统其他负载。
- TinyTest 用一个完整 workload 作为 sample。CSV 使用 workload 自身的起止时钟；
  不逐消息调用计时器，也不从整批平均值推导 P99。

本扫描的 batch=1 仍执行批次结束的两个 progress pass，因此**不能与前次单条
immediate benchmark 直接作回归比较**。本次采用相同 burst 驱动策略扫描六个 batch。

## 吞吐与 CPU（六轮中位数）

| 每批条数 | FlowMQ 条/s | libzmq 条/s | 吞吐比 FMQ/ZMQ | FlowMQ CPU µs/条 | libzmq CPU µs/条 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 66,156 | 29,758 | 2.223× | 15.080 | 37.193 |
| 8 | 300,746 | 227,480 | 1.322× | 3.338 | 4.947 |
| 16 | 531,522 | 465,151 | 1.143× | 1.878 | 2.474 |
| 32 | 724,273 | 783,677 | 0.924× | 1.341 | 1.401 |
| 64 | 855,452 | 1,142,516 | 0.749× | 1.192 | 1.013 |
| 128 | 932,799 | 1,828,645 | 0.510× | 1.013 | 0.715 |

中位数分别计算；吞吐比是双方吞吐中位数相除。CPU 每条成本为
`(after.cpu_ns - before.cpu_ns) / completed_messages`，不是单个应用线程的 CPU 时间。
FlowMQ 全部计时组的 send admission 重试次数均为零。

| 每批条数 | FlowMQ 条/s 最小–最大 | libzmq 条/s 最小–最大 |
| ---: | ---: | ---: |
| 1 | 50,465–73,477 | 20,784–39,385 |
| 8 | 256,250–319,845 | 193,720–265,737 |
| 16 | 512,378–548,254 | 413,758–511,294 |
| 32 | 663,075–786,655 | 662,181–920,178 |
| 64 | 719,175–976,744 | 1,090,906–1,602,428 |
| 128 | 669,364–1,017,226 | 1,308,044–1,974,464 |

原始 72 组数据：[CSV](flowmq-batch-sweep-windows-20261010.csv)。
这些区间是观察到的最小/最大值，不是置信区间，也不支持跨平台性能承诺。

## 实际合批与观测缺口

诊断构建复用同一份 transport 源文件，baseline 模式在 copy DATA 成功提交 CNet 后更新
socket 内的固定计数和 histogram。计数器不分配、不持有 payload、不增加 callback。
普通静态库和共享发布库不定义 `FLOWMQ_BATCH_PROBE`，不包含这些字段或统计指令。
上面的 FlowMQ/libzmq 吞吐/CPU 表来自未启用计数器的正常库。
后面的受控 A/B 则只在同一种诊断构建内比较，不混用两组绝对吞吐。

独立诊断每个 batch 同样发送 262,144 条，预热结束后按测量前后 snapshot 差值统计。
验证收到的消息数、payload 字节数、逻辑写计数及 histogram 加权和一致。
计数限于本实验的单 part PAIR copy 路径，不宣称覆盖 retained/multipart/fanout。

| 应用突发 | DATA 逻辑写形状 | 平均消息/逻辑写 | 计时组 poll 调用/批 |
| ---: | --- | ---: | ---: |
| 1 | 1 | 1.00 | 3 |
| 8 | 1 + 7 | 4.00 | 4 |
| 16 | 1 + 15 | 8.00 | 4 |
| 32 | 1 + 31 | 16.00 | 5 |
| 64 | 1 + 32 + 31 | 21.33 | 7 |
| 128 | 1 + 32 + 32 + 32 + 31 | 25.60 | 11 |

第一个 `1` 是空闲时的直接提交，其余为 copied queue flush。histogram
记录的是每次 queued write 的 range 数；本负载每个 range 恰好对应一条完整消息。
原始计数与 histogram 见 [诊断 CSV](flowmq-batch-probe-windows-20261010.csv) 和
[histogram CSV](flowmq-batch-histogram-windows-20261010.csv)。

**事实：实际 NativeIO 写提交次数未采集。** 当前 CNet SDK 没有公开 standalone client
按操作类型拆分的提交计数；NativeIO 的 backend stats 是总提交数，而且此处 backend
由 CNet 私有持有。不能把 CNet 逻辑写、poll 次数、TCP packet 数或总提交数冒充 native write 数。
诊断 CSV 将 `native_write_count` 明确标为 `unavailable`。

**计算：**根据该 SDK 每次 native vector 最多 16 spans 的契约，上述 DATA 写形状在
无 partial write 时至少需要 `1/2/2/3/5/9` 次 native 提交/批；这是按
`sum(ceil(ranges / 16))` 得到的下界，不是实测，也不含控制帧。

## 原因定位：阶段计时与受控 A/B

### 阶段计时（事实，MED）

Windows WPR 的 CPU 采样启动被系统性能分析策略拒绝，错误 `0xc5585011`；确认没有
遗留录制。于是使用独立 `flowmq_transport_batch_profile` 构建启用阶段计时，
普通库及仅计数的 `flowmq_transport_batch_probe` 不包含这些阶段时钟调用。

三个完整诊断进程中，batch=128 的阶段耗时占总 wall time 比例中位数如下：

| 阶段 | 占比 | 嵌套关系 |
| --- | ---: | --- |
| 应用调用 `flowmq_send` | 12.78% | 不含 poll |
| 应用调用 `flowmq_recv` | 4.44% | 不含 poll |
| 应用调用 `flowmq_poll` | 77.97% | 包含以下三个 drive 阶段 |
| listener progress | 12.26% | poll 内；包括已连接 listener 的非阻塞 accept readiness 检查 |
| CNet client poll | 59.92% | poll 内；包含 NativeIO、CNet 推进和接收 callback |
| FlowMQ local progress | 4.36% | poll 内；包括 queued flush |
| 接收 slice callback | 12.03% | 嵌套于 CNet poll，不能重复相加 |

原始 [阶段 CSV](flowmq-batch-phases-windows-20261010.csv) 共 18 组。
这些是带计时开销的 wall 区间比例，不是 CPU 栈采样；不能直接从 59.92% 推出
“60% 都花在系统调用”。本扫描 poll timeout 为零，没有故意插入毫秒级 sleep。

### A/B 隔离两种成本（事实，MED）

使用相同源码、SDK、payload、消息数、验证、HWM/credit 和 progress 策略，只在私有诊断
构建中对 copied outbound queue 施加两个启动时选定的干预：

1. **SG/32**：原有路径，每个 queued flush 最多 32 个 encoded buffer，提交 retained vector。
2. **coalesce/32**：保持相同 flush 消息数，将 encoded buffer 额外复制到一个
   canonical buffer，再用 `cnet_send_buffer` 提交。buffer 由现有 message pool 分配，
   CNet 接纳后 retain，调用方释放引用；拒绝时保留原队列，取消 write-begin 状态。
3. **coalesce/128**：在相同连续 buffer 路径下，诊断 flush 上限提高到 128 entries。
   aggregate encoded bytes 仍受原有 `max_encoded_size` 限制，CNet range 上限没有改变。

这些干预只覆盖 copied single-part PAIR 测量，不改变 retained application payload 契约。
所有变体验证每条 payload、序号及完整收发数量；统计验证 queued histogram 与消息/字节
总量一致。每个 batch 测六轮，三种变体轮换顺序，每种均有两次处于第一/第二/第三位置。

最终使用**关闭阶段计时、保留相同计数器**的构建排除阶段时钟开销干扰，共 72 组通过。
batch=128 的六轮中位数：

| 变体 | 吞吐 条/s | CPU µs/条 | DATA 逻辑写/批 | poll/批 |
| --- | ---: | ---: | ---: | ---: |
| SG/32 | 790,176 | 1.252 | 5 | 11 |
| coalesce/32 | 1,182,265 | 0.834 | 5 | 7 |
| coalesce/128 | 1,730,175 | 0.536 | 2 | 4 |

计算：只合并 buffer、保持 32-entry 上限，吞吐中位数提高 **49.6%**；
进一步减少逻辑写，coalesce/128 比 SG/32 提高 **118.96%**，CPU/条降低约 **57.1%**。
六轮按同一 repetition 配对的 coalesce/128 / SG/32 比率为 **1.760–2.510×**，
中位数 **2.037×**；它不同于双方吞吐中位数相除得到的 **2.190×**。

| 应用 batch | SG/32 条/s | coalesce/32 条/s | coalesce/128 条/s |
| ---: | ---: | ---: | ---: |
| 16 | 377,389 | 406,549 | 410,272 |
| 32 | 645,388 | 827,156 | 801,467 |
| 64 | 873,211 | 1,016,741 | 1,337,029 |
| 128 | 790,176 | 1,182,265 | 1,730,175 |

batch=16 时两种 coalesce 实际都只有 15 条 queued entry，未越过 native 16-span 窗口；
其配对增益并非每轮都为正，不能推广成“复制总比 SG 快”。batch=32 的两种 coalesce
同样没有触及各自上限，二者数值差异反映测量噪声，不能归因于上限本身。

原始 [关闭阶段计时的 A/B CSV](flowmq-batch-ablation-windows-20261010.csv) 中
`*_ns` 阶段列为零表示没有采集，不能解读为耗时为零；wall、进程 CPU 与计数有效。
此前 [启用阶段计时的 A/B CSV](flowmq-batch-ablation-timed-windows-20261010.csv)
同样 72 组通过，batch=128 下 coalesce/128 比 SG/32 吞吐中位数高 70.6%。两组
绝对吞吐不同，所以报告分开保存，不选择某组绝对数值与 libzmq 作跨实验比较。

### 可以确认的原因与剩余范围

**已确认（MED）：**当前小消息 burst 的成本集中在重复 progress。第一条直接发、
剩余消息按 32-entry 排队 flush；每个大于 16 ranges 的向量再跨多个 native window。
FlowMQ 等待当前逻辑写完成后才提交下一次 flush。受控干预在额外复制 payload 的情况下
仍显著减少 poll/逻辑写及 CPU/条，证明这条调度/批处理路径是可复现的性能限制。
发送接纳重试始终为零，没有证据将本结果归因于 HWM 满或应用发送背压。
源码入口是 [flowmq_socket.c](../flowmq/src/runtime/flowmq_socket.c) 的
`flowmq_socket_peer_admit`、`flowmq_socket_peer_flush` 和 `flowmq_socket_drive`；
实验及完整 payload 验证见 [bench_flowmq_batch.c](../flowmq/benchmarks/runtime/bench_flowmq_batch.c)。

**尚未确认：**NativeIO 实际提交次数和 CNet poll 内的 CPU 栈分布；也未完成大包、TLS、
多 peer 公平性及 tail latency 的实验。因此不能把全部差距归因于某个具体 syscall，
不能直接将诊断 coalescing 变体作为默认策略交付。

后续生产候选应限定在 copy 小消息路径，明确字节/条数上限及额外复制成本，保留大包
retained-SG 所有权契约，并验证跨 peer 公平性、失败回滚、HWM/credit、TLS 和尾延迟。
本次诊断没有更改公开配置、生产 CNet 上限或增加延时 flush。

## 复测

使用配置好发布 SDK 与 vcpkg cache 的 MSVC 开发环境：

```powershell
cmake --preset win-release-user -DBUILD_TESTS=ON -DFLOWMQ_BUILD_ZMQ_BENCHMARK=ON
cmake --build --preset win-release-user
ctest --preset win-release-user -R "^test_bench_flowmq_batch(_probe|_profile)?$" --output-on-failure
ctest --preset bench-win-release-user -R "^bench_flowmq_batch_diagnostic$" -V --output-log build/batch-diagnostic.log
ctest --preset bench-win-release-user -R "^bench_flowmq_batch_sweep$" -V --output-log build/batch-sweep.log
ctest --preset bench-win-release-user -R "^bench_flowmq_batch_phases$" --repeat until-fail:3 -V --output-log build/batch-phases-final.log
ctest --preset bench-win-release-user -R "^bench_flowmq_batch_ablation$" -V --output-log build/batch-ablation-untimed.log
```

正常库与诊断库均运行相同的正式 FIFO/payload correctness case。性能扫描通过，
72 组均完整完成，总用时约 108.85 秒；没有将诊断耗时混入该结果。
最终阶段计时 CTest 连续三次通过；关闭阶段计时的三变体 A/B 共 72 组通过（27.61 秒）。
最终 Release 构建成功，`ctest --preset win-release-user -L flowmq-transport`
的 23 项回归全部通过（128.11 秒），包含普通、计数、阶段计时三个构建的 burst correctness。
本次没有跨进程、TLS、64 KiB 或 Linux/macOS 的批量扫描。

## 有界生产合批（诊断之后）

### 选择与兼容边界

在 `954f87e` 的诊断基础上，把已排队的小 copy 帧合并为一次 CNet retained-buffer
提交。选择以下内部上限，不增加公开选项、协议字段或延迟等待：

- 至少 **17 条相邻小 copy entry**，且每条 **encoded frame ≤256 B**；PAIR 的 32 B
  header 下对应 payload ≤224 B。身份/topic 等编码开销也计入判断。
- 每次最多 **128 帧、16 KiB encoded bytes**，同时保留已有 `max_encoded_size` 上限。
  128 帧来自诊断有效区间；17 帧下限避开已能装入单个 native 16-span 窗口的小批次。
  256 B 和 16 KiB 是保守的复制工作预算，并非全局最优阈值。
- 首条 idle send 仍立即提交。单条排队、大帧及 retained publication 使用原有 SG 路径。
  遇到 retained entry 或大帧时结束小帧前缀，不重排数据，不改变 multipart 边界。
- 每个 peer 每轮至多提交一次 flush，继续由原有 owner/progress 循环驱动 TCP/TLS。
  未增加线程、后台定时器或无界队列。

候选方案包括保持 SG/32、诊断阶段的任意大小 coalesce/128，以及有界小帧复制。
选择最后一种，是为了减少已测得的小帧 progress 成本，同时限制新增复制与存储需求；
大 payload 没有资格进入复制分支。时间复杂度为 O(选中帧数 + encoded bytes)，
新增一个 aggregate buffer 的有效数据最多 16 KiB/在途 peer。它来自现有 message pool，
实际容量受 pool 分配级别及缓存复用影响，不能把 16 KiB 当作进程 RSS 的严格增量。

最初候选允许两条小帧合批，其
[六轮原始数据](flowmq-bounded-policy-min2-windows-20261010.csv) 中，64 B / batch=8
吞吐中位数为 SG 319,454、新策略 290,161 条/s，没有支持额外复制的稳定收益。
batch=1 这个不经过 queue flush 的对照同样出现约 10% 差异，说明不能把上述全部差值
归因于复制成本。最终仍采取保守选择：未跨过已验证的 16-span native 窗口时不复制，
把触发下限提高到 17 个相邻小帧；下方正式结果使用这个最终策略，单独重新测量。

### 所有权、错误与回滚

队列在单一 socket owner 上持有原始 canonical buffers。临时 slices 保活至提交结束；
aggregate buffer 由现有 pool 分配。CNet 成功接纳后持有发送引用，FlowMQ 释放其本地
aggregate 引用，再移除已选队列项。只有原有 send terminal 才结算在途消息/字节；
credit 在 enqueue 时已提交，flush 不重复扣减。

分配或 CNet admission 失败时，释放临时引用、取消 write-begin，队列内容和计数保持
原样。EBUSY/ENOBUFS 按现有 progress 规则重试；ENOMEM 等终态错误仍由现有 peer
失败处理关闭并释放资源，不改走另一条路径掩盖错误。retained publication 的逻辑
vector 和完成生命周期保持原样。回滚仅需恢复内部 SG 策略，不涉及数据/配置迁移。

### 验证方法

私有 probe mode 0 保留历史 SG/32；新增 mode 3 执行与生产完全相同的策略选择。
mode 1/2 继续保留历史干预语义。新旧路径使用同一可执行文件、相同计数器、关闭阶段
计时，每个场景六轮且先后顺序各三次；新增 `policy:` CTest case。

测试覆盖 64 B 下 batch=1/8/32/128，以及 batch=128 下的 224 B、225 B、1 KiB；
64 KiB 使用 batch=16。≤1 KiB 的场景每组 262,144 条；64 KiB 每组 16,384 条（1 GiB），
每条验证长度、内容及 FIFO。试跑发现小组 CPU 时间量化误差较大，因此增大消息数；
正式结果单独保存，不与试跑混合。每批计两次 wall clock，记录完整发送/接收/验证/
credit progress 的耗时，报告 nearest-rank P95/P99；这是**每批端到端延迟**，不是
单条网络时延，也没有用平均值推导分位数。setup、预热、分位排序和关闭不计入测量。
进程 peak RSS 是进程生命周期高水位，不能当作独立场景的内存增量。

正式 socket 测试覆盖 0/1 B、单个 queued frame、17/128 帧边界、16 KiB 边界、224/225 B
分支边界、混合 copy/SG、消息 HWM 满及恢复、超过 1,024 entries 的环形队列回绕、
multipart 和经证书验证的 TLS burst。普通 socket 与 opt-in peer pool 运行同一套测试。
生命周期测试分别使用 64 B 合批和 1 KiB SG，在 CNet 接纳后释放原队列引用，再从
同一 pool 发送下一条，验证在途数据不会被复用覆盖。

### 最终策略 A/B 结果（事实，MED）

[最终策略 CSV](flowmq-bounded-policy-windows-20261010.csv) 共 96 组，全部完成内容及
FIFO 校验，无 send admission 重试。下表为六轮中位数；P99 列是六个独立组的每批
P99 的中位数，没有合并样本或换算成单条延迟。计数器开启、阶段计时关闭。

| payload / batch | SG 条/s | 新策略 条/s | SG CPU µs/条 | 新策略 CPU µs/条 | SG 每批 P99 µs | 新策略每批 P99 µs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 B / 1 | 71,050 | 67,771 | 14.037 | 14.812 | 23.65 | 24.60 |
| 64 B / 8 | 268,126 | 280,317 | 3.695 | 3.576 | 51.30 | 48.25 |
| 64 B / 32 | 780,496 | 931,773 | 1.282 | 1.073 | 65.50 | 54.70 |
| 64 B / 128 | 1,065,101 | 1,953,931 | 0.954 | 0.507 | 206.10 | 113.75 |
| 224 B / 128 | 988,570 | 1,561,851 | 1.013 | 0.656 | 201.85 | 121.40 |
| 225 B / 128 | 889,264 | 903,003 | 1.103 | 1.103 | 253.90 | 262.05 |
| 1 KiB / 128 | 894,705 | 821,484 | 1.132 | 1.222 | 219.60 | 294.40 |
| 64 KiB / 16 | 26,145 | 28,037 | 37.193 | 36.240 | 2,709.25 | 2,401.60 |

**计算：**64 B / batch=128 吞吐中位数提高 **83.45%**，CPU/条降低 **46.88%**，每批
P99 降低 **44.81%**。同一 repetition 配对的吞吐比为 **1.318–2.587×**，中位数
**1.911×**，六轮均为正。64 B / batch=32 吞吐提高 19.38%；224 B / batch=128
提高 57.99%，这两组同样六轮配对均为正。

64 B / 128 的 DATA 逻辑写每批从 **5 → 2**，submitted ranges 从 **128 → 2**，
poll 从 **11 → 4**；224 B / 128 因 16 KiB 上限拆成两次 queued flush，分别为
**5 → 3** 次逻辑写、**11 → 5** 次 poll。batch=8 两边均为 2 次逻辑写、8 ranges、
4 次 poll，确认短批次没有启用额外复制。这些仍然不是 NativeIO syscall 计数。

**限制（MED）：**1 KiB 对照虽然保持相同 SG 提交形状，吞吐中位数仍低 8.18%，每批
P99 高 34.06%；batch=1 完全没有 queued flush，也出现 4.61% 吞吐差异。重复组的
变化以及控制路径一致表明存在主机/测量波动，但不足以排除所有额外成本。因此只确认
已测小帧大批次收益，不宣称所有消息大小都提高或整体性能无回归。1 KiB 尾延迟需要在
更稳定的环境下进一步复测。64 KiB 也没有启用合批，其结果不能算作合批收益。

进程 peak RSS 在本轮从 11,128,832 到 12,206,080 bytes（约 10.61–11.64 MiB）；
这是跨案例累积高水位，不能证明每个策略独立的峰值或无泄漏。有限 burst、单 peer
结果不能外推为饱和流、多 peer 公平性、TLS 性能或跨平台结论。

### 最终生产库与 libzmq 对照

[生产对照 CSV](flowmq-bounded-vs-zmq-windows-20261010.csv) 为另外一轮 72 组扫描，
链接普通生产库，未编入 probe 计数、阶段计时或每批分位数采集。双方每组 262,144 条
64 B 消息，六轮且平衡执行顺序，全部校验通过；FlowMQ admission 重试为零。
本轮绝对吞吐不能与上面的 probe A/B 数据跨时间直接相减。

| batch | FlowMQ 条/s | libzmq 条/s | FMQ/ZMQ | FlowMQ CPU µs/条 | libzmq CPU µs/条 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 63,659 | 27,932 | 2.279 | 15.616 | 39.697 |
| 8 | 227,956 | 213,008 | 1.070 | 4.321 | 5.245 |
| 16 | 397,797 | 412,099 | 0.965 | 2.503 | 2.682 |
| 32 | 849,858 | 840,980 | 1.011 | 1.132 | 1.431 |
| 64 | 1,349,373 | 1,234,613 | 1.093 | 0.745 | 1.103 |
| 128 | 1,566,602 | 1,695,212 | 0.924 | 0.656 | 0.775 |

batch=128 下生产库仍比 libzmq 的吞吐中位数低 **7.59%**。这不是“全面超过 ZMQ”
的证据；本轮只覆盖相同应用 burst 策略下的单进程 TCP PAIR，线程模型差异仍然存在。
正常库与探针库结果分开呈现，也不把新旧两个生产扫描的差值当作严格配对收益。

复测入口（MSVC 开发环境）：

```powershell
cmake --build --preset win-release-user
ctest --preset win-release-user -L flowmq-transport --output-on-failure
ctest --preset bench-win-release-user -R "^bench_flowmq_batch_policy$" -V --output-log build/bounded-min17-policy.log
ctest --preset bench-win-release-user -R "^bench_flowmq_batch_sweep$" -V --output-log build/bounded-min17-sweep.log
```

最终 Release 构建成功；三个 burst correctness CTest 均通过。最终策略 A/B 的 96 组
通过（84.12 秒），普通生产库与 libzmq 的 72 组通过（116.56 秒）。最终
`flowmq-transport` 的 **23/23 项全部通过**（198.26 秒），包含 TCP/TLS、peer pool、
owner fault、multipart、HWM、在途 buffer 生命周期及公开 C11/C++17 consumer。
本地完整日志分别在 `build/bounded-min17-build.log`、`build/bounded-min17-policy.log`、
`build/bounded-min17-sweep.log` 和 `build/bounded-min17-regression.log`。

本次没有新增分配失败注入、CNet busy 注入或 sanitizer 配置；对应 flush 失败回滚
依据代码审查，不能标为故障注入测试已通过。TLS 与多 peer 的功能回归不等同于 TLS
吞吐或多 peer 尾延迟/公平性测量；Linux/macOS、跨进程及长期稳态负载仍未验证。
