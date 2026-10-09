# FlowMQ / libzmq TCP 对照（2026-10-10）

当前 ACE+CNet 2.3 迁移分支的本机实测：单条发送后立即接收时 FlowMQ 更快；
每批发送 64 条再接收时 libzmq 更快。结果只覆盖下述负载，不代表峰值吞吐或跨平台结论。

## 环境与口径

- Windows 11，10.0.26200；AMD Ryzen 9 7940HX，16 核 / 32 逻辑处理器。
- MSVC 14.44.35207，Release；FlowMQ 基于 `5409aad`，本次只改 benchmark 和文档。
- 发布 SDK：Salts `2.3.0-rc.1`（`58ff08fc95b4aa1dc493c0b7080426b2c11d4959`），
  SaltsUtils `4.3.0-rc.1`（`049f8e39e1d19ff21e9825df7c497e40b75a8ddf`）。
- libzmq `4.3.5`，vcpkg port-version `2`，Release 产物。
- 同一进程内两个 PAIR socket，TCP `127.0.0.1`，copy send/recv，逐消息验证长度和内容。
  FlowMQ peer pool 保持默认关闭。
- FlowMQ 由应用线程驱动 progress；libzmq 使用默认后台 I/O 线程。
  这是各自现有 API 使用方式的对照，没有统一 CPU/线程预算，也没有测 CPU 利用率。
- 单条负载：64 B 各 100,000 条，64 KiB 各 5,000 条；批量负载：
  各 10,000 批，每批 64 条、每条 64 B。每批全部接收后才开始下一批。
- 初始化、连接预热、关闭不计时；两者在批量测试前均重建连接并预热。
  计时包含进度驱动、复制、payload 比较和状态断言。
- 每次运行顺序固定为 FlowMQ、libzmq，未绑定 CPU；三个独立进程依次执行。
  下表是三次运行各自平均值/吞吐率的中位数，不是把全部消息合并后的分位数。

## 完整样本结果

| 负载 | FlowMQ | libzmq | FlowMQ / libzmq 吞吐 |
| --- | ---: | ---: | ---: |
| 64 B，单条发送后接收 | 101,012 条/s | 28,640 条/s | 3.53× |
| 64 KiB，单条发送后接收 | 756.37 MiB/s | 498.22 MiB/s | 1.52× |
| 64 B × 64 条/批 | 899,288 条/s | 1,102,927 条/s | 0.815×（低 18.5%） |

64 B 单条平均完成耗时的中位数为 FlowMQ **9.900 µs**、libzmq **34.917 µs**；
64 KiB 为 **82.632 µs**、**125.446 µs**。这些是应用发送到本机接收完成的平均耗时，
不是网络单向传输延迟、RTT 或 P99。

三次吞吐范围如下，反映本机运行波动：

| 负载 | FlowMQ 最小–最大 | libzmq 最小–最大 |
| --- | ---: | ---: |
| 64 B 单条，条/s | 95,562–107,798 | 24,489–34,508 |
| 64 KiB 单条，MiB/s | 688.21–813.61 | 388.11–656.02 |
| 64 B × 64 条/批，条/s | 855,831–915,968 | 992,900–1,115,268 |

原始表格列保存在 [CSV](flowmq-vs-zmq-windows-20261010.csv)：`profile=full` 为上述结果。
`avg_operation_ns` 在批量项目中是每条消息的摊销时间；`avg_sample_us` 是整批完成时间。
吞吐比值按双方中位数相除，展示精度受 TinyTest 原始输出的小数位限制。

## 探测与限制

先执行了三次较短探测（64 B 10,000 条、64 KiB 1,000 条、批量 1,000 批），
每个进程不到一秒。其吞吐比中位数之比为 2.44×、3.30×、0.596×，
与完整样本差异明显，因此不作为主结论；数据以 `profile=short` 保留。

最初调用原有全部 TCP case 时还包含 owned、retained 和 multipart 项目，
约四分钟后手动中止，未取得完整结果；本次新增配对 case 仅隔离双方共有的三种 copy 负载，
复用原有发送、接收和 progress helpers。未修改生产代码，也未据此判断其他 case 正确性。

本次没有测跨进程/跨机、TLS、多连接、持续灌流量、CPU 成本或尾延迟。
大包项目出现毫秒级最大样本，未进行调度或系统噪声归因。
固定测试顺序、短运行时间及样本数敏感性限制了结果的推广范围；不能宣称所有场景领先。

## 复测与验证

在配置好发布 SDK、vcpkg cache 和 MSVC 开发环境后：

```powershell
cmake --preset win-release-user -DBUILD_TESTS=ON -DFLOWMQ_BUILD_ZMQ_BENCHMARK=ON
cmake --build --preset win-release-user
ctest --preset bench-win-release-user -R "^bench_flowmq_zmq_comparison$" --repeat until-fail:3 -V --output-log build/flowmq-vs-zmq.log
```

正式入口 `bench_flowmq_zmq_comparison` 只在启用 libzmq 对照时注册，使用 benchmark label、
串行执行及 600 秒超时。两个 case 共用既有完整样本常量，不受 smoke/CI 开关缩减。
fixture 在断言失败后也执行 socket/context 清理。

实际验证：Release 构建成功；完整样本 CTest 连续三次通过，每次两个 case、
230,015 个断言，无失败，总耗时约 20.86 秒。此前三次短样本也通过。
本次未修改 runtime，未重跑完整 runtime 回归；未执行 Linux/macOS 测量。
