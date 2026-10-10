# rc.10 性能归因：wait、listener 与 SG（2026-10-11）

本轮确认了三个局部成本：macOS 单条 exchange 的正超时轮询等待、普通 socket
progress 中重复检查空闲 listener，以及小消息 retained SG 的底层向量窗口限制。
这些实验解释了部分已测慢点，尚未解释 FlowMQ/ZMQ 的全部吞吐和尾延迟差距。
本次只增加 benchmark、CI 与证据，不修改生产 transport、poll 或 batch 默认值。

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
