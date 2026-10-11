# rc.10 性能归因：wait、listener 与 SG（2026-10-11）

本轮确认了三个局部成本：macOS 单条 exchange 的正超时轮询等待、普通 socket
progress 中重复检查空闲 listener，以及小消息 retained SG 的底层向量窗口限制。
这些实验解释了部分已测慢点，尚未解释 FlowMQ/ZMQ 的全部吞吐和尾延迟差距。
初轮归因只增加 benchmark、CI 与证据，不修改生产 transport、poll 或 batch 默认值；
随后验证的 poll 候选与结果独立记录在文末。

## 证据与复现

测量源码：`0b02737e824af2e22223eb77935affee2b55e504`；
[完整 CI、日志和 artifacts](https://github.com/qigao/flowmq/actions/runs/38074337640)。
三个 host 均使用 Salts.Native `2.3.0-rc.10`、SaltsUtils.Native `4.3.0-rc.7`。
Linux x64 / Windows x64 各 4 个逻辑 CPU，macOS arm64 为 3 个，均未绑定亲和。
不得把本轮绝对吞吐与另一 CI runner 的旧值相减来声称代码优化收益。

手动运行 `.github/workflows/native-sdk-release.yml`，设置
`run_diagnostics=true`。正式 build/test/install 后运行：

```sh
ctest --preset bench-host-release-ci --no-tests=error -R '^bench_flowmq_(wait_policy|progress_phases|progress_ablation|lane_sg_sweep)$' -V --output-log build/flowmq-diagnostics.log
```

macOS 使用 `bench-mac-release-ci`；Windows 使用 MSVC 开发环境和双引号表达式。
`flowmq-diagnostics-<platform>` artifact 保留原始日志与 SDK/runner provenance。

| 数据 | 每配置重复 | 三平台记录数 | 内容 |
| --- | ---: | ---: | --- |
| [wait policy](flowmq-rc10-wait-policy-20261011.csv) | 4 | 48 | 64 B / 64 KiB，poll 0 / 1 ms |
| [SG sweep](flowmq-rc10-sg-sweep-20261011.csv) | 10 | 840 | 2 尺寸 × 1/8 lane × 7 种发送策略 |
| [progress](flowmq-rc10-progress-20261011.csv) | phase 3 / ablation 6 | 189 | 3 负载，阶段计时与逐项消融 |

本轮 artifact 的 provenance 仍含旧的 `paired_runs=5`、
`lane_runs_per_configuration=5` 字段，不能把它们当作上述诊断的重复次数；
实际次数来自 benchmark 循环和原始结果行。后续提交修正这些元数据字段。
progress CSV 的 `instrumentation=phases/counters` 区分两种仪器开销，不混合基线。

## MED：正超时 poll 的等待成本

**事实：**[flowmq_poll](../flowmq/src/runtime/flowmq_socket.c) 每轮以零超时驱动
socket；未找到应用 readiness 时，正超时路径调用 `cmeta_sleep_ms()`，单次最多
1 ms。`events` 没有用于决定是否继续 drain。它不是在该函数中等待网络 completion。

新实验保持同一个 `bench_exchange()`、payload 校验和消息数，只改变
`bench_progress_wait()` 传入的 0/1 ms。每组预热 16 条，64 B 测 32,768 条，
64 KiB 测 4,096 条，连接建立/关闭不计时；每配置四次，交替尺寸与策略顺序。

| 平台 | payload | 1 ms：条/s | 0 ms：条/s | 吞吐倍率 | CPU ns/条：1 ms → 0 ms |
| --- | --- | ---: | ---: | ---: | ---: |
| Linux | 64 B | 66,225 | 74,456 | 1.124× | 13,515 → 13,429 |
| Linux | 64 KiB | 19,248 | 28,183 | 1.464× | 36,062 → 35,484 |
| Windows | 64 B | 105,379 | 103,876 | 0.986× | 9,060 → 9,537 |
| Windows | 64 KiB | 48,612 | 49,042 | 1.009× | 19,073 → 19,073 |
| macOS | 64 B | 4,952 | 29,375 | 5.931× | 21,178 → 18,147 |
| macOS | 64 KiB | 4,159 | 19,163 | 4.608× | 36,222 → 34,733 |

数值为各配置中位数，倍率为吞吐中位数之比。CPU 为整个进程的 CPU 时间/消息数。
Windows 短组受 CPU 计时粒度影响，不能解读微小差异。

**推论：**macOS 这个 exchange 协议的主要可避免成本来自等待策略；Windows
没有同样收益。这不能证明 CNet 普遍慢，也不能将此倍率套用到已用 external owner
的饱和 lane 测试。

CPU/条减少不等于 CPU 占用下降：按 `条/s × CPU ns/条 / 1e9` 估算，macOS
64 B 从约 0.105 增至 0.533 个 CPU 核的占用，因为单位时间完成了更多消息。
该估算使用各自中位数，不是逐组占用中位数。本轮没有 idle 测试，不能把永久忙轮询
作为默认策略。最小改进方向是有进展时有界 drain、无进展时等待 completion，保留
总 deadline 和多 socket 公平性；还需验证 idle CPU、唤醒和 P99。

## MED：空闲 listener 被重复查询

**事实：**普通 `flowmq_socket_listener_progress()` 调用
`cnet_listener_wait(..., 0, ...)`。rc.10 的
[cnet_listener_wait 实现](https://github.com/qigao/salts/blob/6ba9868a965f3273cf87ab243dab1884072eb7a3/cnet/src/cnet_listener.c#L1027)
每次进入 POSIX `poll` 或 Windows `WSAPoll`。

固定连接预热后，本轮所有 progress 组的 `listener_ready` 和 `manager_work` 均为零。
现有私有诊断开关临时跳过 listener 检查，得到以下吞吐倍率：

| payload / batch | Linux | Windows | macOS |
| --- | ---: | ---: | ---: |
| 64 B / 1 | 1.108× | 1.371× | 1.521× |
| 64 B / 128 | 1.030× | 1.081× | 1.506× |
| 1 KiB / 128 | 1.025× | 1.114× | 1.597× |

这里取六个同 repeat 的 `baseline wall_ns / intervention wall_ns` 的中位数，
与上一表的两个中位数之比不同。macOS CPU/条降至基线的 0.746/0.806/0.852。
独立阶段计时中，macOS listener 占总 wall time 的 30.37%–38.26%，其中
listener wait 占 30.05%–37.89%。后者嵌套于前者，不能相加；阶段时间也不等于
CPU profiler 的栈占比。

**限制：**跳过检查会停止接纳新连接，只能用于固定拓扑归因，不能用于生产。
最小改进方向是减少 owner 循环中的重复 readiness 查询，评估接入既有 external
accept/completion 路径；必须保留新连接、重连、关闭、错误和公平性测试。
此结果与 wait-policy 来自不同实验，不能把两个倍率相乘当作联合优化收益。

## MED：SG 逻辑 batch 增大没有同等减少 NativeIO 操作

**事实：**本负载每条 retained 消息包含 framing 和 payload 两个 range。
CNet 逻辑上限为 32 ranges，但
[NativeIO 上限为 16 spans](https://github.com/qigao/salts/blob/6ba9868a965f3273cf87ab243dab1884072eb7a3/native-io/include/salts/native_io.h#L69)。
[cnet_write_queue_build_vector](https://github.com/qigao/salts/blob/6ba9868a965f3273cf87ab243dab1884072eb7a3/cnet/src/cnet_write_queue.c#L502)
达到该上限就结束当前窗口，剩余数据需继续提交。因此 batch 16 的 32 ranges
不会变成一次 32-span native 提交。

64 B、单 lane 的十轮中位数：

| 平台 | copy：M条/s | SG batch 8：M条/s | SG batch 16：M条/s | native/条：copy → batch 8 → batch 16 |
| --- | ---: | ---: | ---: | --- |
| Linux | 0.6378 | 0.3909 | 0.3949 | 0.09390 → 0.15654 → 0.15654 |
| Windows | 0.9335 | 0.5788 | 0.5835 | 0.15628 → 0.28128 → 0.28128 |
| macOS | 0.3324 | 0.2627 | 0.2376 | 0.09658 → 0.15937 → 0.15841 |

三个平台的逻辑 SG writes/条均约从 0.13283 降至 0.07033，native/条基本不变。
8 lane 同样观察到近乎不变的 native/条。macOS 的 batch 8/16 吞吐波动较大，不能
从一次矩阵选择稳定赢家。`native_submitted` 是共享 owner backend 的总操作数，
包含发送、接收和 credit，不能叫作纯发送 syscall 数。

copy 已在满足条件时将小帧复制到连续 buffer：至少 17 帧触发，单帧 encoded size
≤256 B，上限 128 帧 / 16 KiB，见 `flowmq_socket_peer_can_coalesce()` 和
`flowmq_socket_peer_flush()`。SG 保留多 range 的生命周期和继续提交成本，所以
“少一次 payload copy”不能保证总成本更低。

**推论与边界：**当前证据支持保留小消息 copy/coalesce，按真实 native 窗口评估 SG
批次。它还不能证明把 NativeIO 上限改大就一定更快；该修改涉及跨平台存储、栈成本
及提交能力，需单独对照。retained 路径不能为了分数偷偷变成 copy，必须保留已有
ownership 契约。固定 32-slice publication 的分配仍是候选，本轮没有 allocator
或 CPU stack 证据证明它是主因。

## 未证实的原因与验证范围

- 跳过前置 manager poll 的配对倍率：Linux 约 1.000，Windows 1.003–1.007，
  macOS 0.954–1.042；不支持它是稳定的主要瓶颈。
- `client_poll_ns` 包含 NativeIO、callbacks 和协议推进，不能据此说 CNet 自身
  消耗了同等比例的 CPU。上述 direct 路径没有 Actor/mailbox。
- 尚未解释 Linux 64 KiB 相对 ZMQ 的差距；没有同轮 ZMQ 归因对照、CPU stack、
  allocation profile、固定物理核、TLS、跨机和低负载验证，不宣称整体反超 ZMQ。

CI 的 Linux/Windows 各 47 项、macOS 46 项正式 CTest 通过；三个 host 的四项诊断
CTest 全部通过。三个 native host 的安装后 C/C++ consumer 各两项通过，SDK 打包
通过；Android 仅编译/链接资格验证。CSV 核对配置/重复次数、吞吐公式、消息数和
在途窗口上限 128；正式 benchmark 校验 payload、FIFO 和保活释放等适用契约。
本次未重跑 sanitizer，未发布新的 FlowMQ release。

## 后续候选：有进展时有界重扫

基于上述等待证据，后续候选在 `flowmq_poll()` 中使用此前丢弃的 CNet 回调
进展计数：有进展且尚无请求的 readiness 时，立即再扫描完整 socket 列表。
最多连续 32 个有进展的扫描，然后恢复既有休眠；无进展也立即恢复休眠。
每轮仍检查同一个调用 deadline；零超时仍只有一轮；错误和 readiness 返回不变。
每轮为 O(item_count) 加各 socket 的既有推进成本，额外空间 O(1)，无分配或线程。

选择这一小改动，是为了先验证 completion 后的重复休眠是否值得消除；没有将
永久 spin、listener 禁用或整个 external-owner 迁移混入同一次实验。预算 32 是
有界候选，尚无证据证明它是最优值。现有 heartbeat、重连和 socket 测试继续覆盖
协议进展；增加 handshake 不误报 readiness、列表后部接收和连接空闲 CPU 对照。
空闲组每配置四次、每次 250 ms，对比 poll 0/1 ms，输出进程 CPU/墙钟之比。
最终是否保留由三平台功能和性能结果决定，回滚只需撤销本节对应候选改动。

### 三平台结果与保留决定

测量源码 `cbe122d52ea246df8a9d52ea83a76cda1772c603`，
[CI 与原始 artifacts](https://github.com/qigao/flowmq/actions/runs/38096396399)。
Salts.Native 仍为 `2.3.0-rc.10`；浮动恢复的 SaltsUtils.Native 已变为
`4.3.0-rc.8`，与初轮 rc.7 不同。CPU 数仍为 4/4/3、无亲和绑定，但不是同一
runner 的前后对照；不将两个 run 的绝对吞吐之比标为本补丁的因果收益。

[Exchange CSV](flowmq-rc10-poll-drain-wait-policy-20261011.csv) 有 48 行，
[连接空闲 CSV](flowmq-rc10-poll-drain-wait-idle-20261011.csv) 有 24 行。
每配置均四次。下面只比较同一 run、同平台的 1 ms 与 0 ms 策略；吞吐是各自
中位数，比例是中位数之比。两个策略都使用候选实现，0 ms 仍只扫描一次。

| 平台 | payload | 0 ms：条/s | 1 ms：条/s | 1 ms / 0 ms | CPU ns/条：0 ms → 1 ms |
| --- | --- | ---: | ---: | ---: | ---: |
| Linux | 64 B | 72,326 | 72,185 | 0.998× | 13,824 → 13,852 |
| Linux | 64 KiB | 27,490 | 27,575 | 1.003× | 36,377 → 36,265 |
| Windows | 64 B | 77,175 | 76,225 | 0.988× | 12,875 → 13,113 |
| Windows | 64 KiB | 33,511 | 33,790 | 1.008× | 30,518 → 30,518 |
| macOS | 64 B | 30,612 | 29,702 | 0.970× | 15,885 → 16,952 |
| macOS | 64 KiB | 22,076 | 20,335 | 0.921× | 29,662 → 31,012 |

| 平台 | 空闲 0 ms：单核占用 | 空闲 1 ms：单核占用 | 空闲 1 ms：poll 次数/s |
| --- | ---: | ---: | ---: |
| Linux | 100.00% | 1.41% | 935.3 |
| Windows | 100.00% | 低于本次 CPU 计时分辨率 | 65.0 |
| macOS | 22.11% | 0.89% | 125.7 |

空闲占用为每组 `cpu_ns / wall_ns × 100%` 后取中位数，100% 代表占用一个核，
不是全机 CPU 百分比。Windows 四组 CPU delta 均为零；250 ms 窗口不足以把这种
粗粒度读数解释成零成本。macOS 零超时轮询在本机也没有占满一个核，不外推平台
CPU 时间与 wall time 的差异为用户态算法成本。

**决定：保留有界重扫。** 当前等待模式在 64 B 达到同轮零超时吞吐的 97%–100%，
并保留低空闲 CPU。没有引入新线程、Actor、mailbox、payload 复制或 listener
跳过策略。预算 32 未做完整最优值扫描，不宣称是通用最优配置。

**MED：空闲唤醒仍未解决。** Windows/macOS 的 1 ms 空闲调用分别平均约
15.4/8.0 ms 一次（由 `wall_ns / poll_calls` 观察）；调用耗时包含整个扫描和等待，
不能全部归为 sleep syscall。忙时吞吐接近 spin，不代表低负载首条消息或 P99
也接近。下一步应比较 completion wait 与周期休眠，保留多 socket 进展、公平性
和 listener 接纳；现有 `flowmq_owner_poll()` 已有 shared-backend wait，普通
socket 的多个独立 backend 不能未经设计就塞进一个阻塞等待。

候选的 Linux/Windows 各 47 项、macOS 46 项正式 CTest，以及每 host 四项诊断
CTest 均通过；打包和安装后 consumer 资格验证全部通过（Android 仅编译/链接）。
新增两个正式 socket 用例验证 handshake 进展不误报 readiness、
后部 socket 接收与零超时；原 heartbeat、重连、TLS、pool 等回归同时运行。
当前没有旧/新实现同进程 A/B、低负载消息 P99 或 sanitizer 新结果；也没有新的
FlowMQ/ZMQ 同轮比较，因此本节不声称已经反超 ZMQ。

## 独立发送线程的低负载唤醒协议

既有 paced-lane 测试由同一个 owner 等到计划时间后发送，不能直接验证另一个
线程发来数据能否唤醒已经阻塞的 receiver。新增正式 benchmark 使用两个线程、
一条 loopback TCP PAIR 连接；发送线程绑定普通 socket，接收线程通过公开 API
连接普通或 owner socket，不借用私有 owner listener 接口。

- 每个线程创建、推进和关闭自己的 context/socket；controller 只管理同步和结果。
  每个 socket 的 HWM 固定 256 条，每轮最多 256 条 64 B 消息，拒绝时在原发送
  线程内重试，整体操作有 30 s deadline；不丢弃或无界排队。
- 使用 copy send，栈 payload 在成功 admission 后可复用；接收端独立 buffer，
  验证完整字节、序号和 FIFO。每条携带计划释放时间与首次发送尝试时间；后者不因
  busy 重试而重置。保留发送调度延误、实际发送至接收及计划释放至接收三种指标。
- 固定 5/20 ms 计划周期，发送线程用既有 sleep 原语等到 release；延误后赶上
  计划、不跳过样本。OS 调度可能造成突发，因此同时记录 release lag，不能声称
  实际到达严格等间隔。16 条预热与建立连接不计时。
- 接收端四种策略：ordinary poll 0/1 ms，owner poll 0/20 ms。POLLIN 为唯一
  数据 readiness；非阻塞 receive 校验成功才完成样本。测量包括实际排队、接收
  和校验，不称为纯内核唤醒时间。
- 共享控制区固定大小：mutex/condition 只发布 endpoint、启动和清理屏障；原子
  error/received 传递停止与完成进度。每个线程独占统计数组的写入；controller
  join 后读取，先结束测量，再允许各 owner 销毁 socket/context。
- 正常发送者推进到所有消息被接收；首个错误原子发布，使两个线程在有限 poll /
  sleep 后退出，join 完成前不回收控制区。正式 correctness 用例覆盖正常 FIFO
  和中途接收失败；join 失败时测试进程终止，不能假装已经 quiescent。
- 进程 CPU 包含 sender、receiver 与 controller；不是 receiver 单线程 CPU。
  每配置五次，共 40 组，轮换 period 和策略顺序；五次不能完全平衡四种顺序。
  暂不修改生产等待策略，以该对照决定 completion-wait 的后续边界。

### 单 socket 的 completion-wait 候选

Linux 对照中，两个周期的 ordinary sleep P99 都约 1.09 ms，owner wait 约
0.10 ms，且 owner wait 的 CPU 更低。后续候选只改变已初始化的单 socket
`flowmq_poll()`：无进展时下一轮用 CNet `poll` 等待最多 1 ms，让 incoming
completion 提前唤醒；复用 `flowmq_socket_drive()` 原有阻塞进度能力。

首轮与零超时仍非阻塞；正超时使用同一个 caller deadline。1 ms 上限继续保证
普通 listener 与 FlowMQ 本地 deadline 得到周期推进；有进展重扫达到预算后仍
执行既有 sleep。未初始化 socket 及多个独立 socket 保留原等待策略；后者没有
统一 backend，不能任选一个阻塞。多个 socket 的统一等待仍归显式 owner lane。
额外状态为一个局部 timeout，空间 O(1)；没有新线程、分配、API 参数或 payload
生命周期变化。增加单连接 idle deadline / 后续可读回归，重跑同一 wake 矩阵。
CSV 中 `ordinary_sleep` 名称为跨提交可比较的历史策略标签；候选实现下它表示
公开 `flowmq_poll(..., 1)`，不再表示一定执行了 sleep。
