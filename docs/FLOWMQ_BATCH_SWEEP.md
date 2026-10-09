# FlowMQ / libzmq 批量扫描（2026-10-10）

本机 TCP PAIR、64 B copy 消息：增大 batch 对两者都有帮助。FlowMQ 在 batch
1/8/16 时吞吐中位数更高，约在 32 条附近被 libzmq 超过；到 64–128 条时
FlowMQ 的提升趋缓。后续受控实验确认：分散小 buffer 与 32-entry flush 上限导致
重复的有序写入/progress 成本，是该负载慢于 libzmq 的重要原因。
仅诊断构建包含实验策略，生产发送策略和发布库运行时行为保持不变。

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
