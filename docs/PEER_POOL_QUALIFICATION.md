# CNet 2.3 peer pool 验收记录（#123）

## 范围与环境

2026-10-10 Windows Release，MSVC 19.44，AMD Ryzen 9 7940HX（16 核 / 32 逻辑处理器），
已发布 Salts 2.3.0-rc.1 / Salts Utils 4.3.0-rc.1。运行时为 `5ef54e8` 的可选
peer pool，本次只扩展正式 benchmark、安装 consumer 和验收流程，没有性能路径修改。

这里的 1/2/4 owners 是独立 progress 线程，每线程独占 context 和 standalone
sockets，不是显式 `flowmq_owner_t` 的共享 wait。后者当前仅支持 TCP；本次 TLS
结果不表示新增了共享 Owner TLS 支持。线程未绑核，运行环境未隔离，不能从单机
样本推出普遍性能收益。

## 工作量与测量协议

- 固定总计 4 对真实 loopback PAIR 连接、8 个 socket。1/2/4 线程只改变连接分配。
- 每批每连接 16 条消息，每条 2 个等长 part；总 payload 为 64 B 或 64 KiB。
  检查每条消息的内容、连接身份、序号、长度和 `RCVMORE`；预热检查发送 HWM
  满时拒绝及恢复。启用 pool 时每 socket 的 physical/connecting 上限均为 1。
- 预热后和关闭前读取真实 pool snapshot，必须有且只有一个 READY peer 和 lease。
  lease 检查、建连、TLS 握手、预热和关闭在计时外；关闭/context 释放失败仍使整个
  用例失败。发送 buffer 在当前调用完成后复用，接收内容必须保持正确。
- 两组使用相同数据量、HWM 和代码路径。每轮交替 pool 关闭/启用的先后顺序，
  并轮换 1/2/4 线程的顺序。吞吐按完整应用 payload 计一次，不包含协议/TLS 头。
- P99 从每条消息首次发送尝试计到收齐最后一 part，包含排队。CPU 为进程测量阶段
  CPU 秒数；RSS 是进程生命周期峰值，不能据相邻行相减宣称 pool 内存开销。
  本测量也没有统计 allocator 调用次数或启动成本。

普通回归的 `test_flowmq_socket_owners` 覆盖两种模式的 24 个 TCP/TLS、payload、
owner 组合，通过。性能用例单独由 `bench-*` test presets 选择，默认测试排除
`benchmark` label，包含保留原有 source-contract 排除规则的 macOS preset。

## 首轮结果

四个 CTest 性能用例全部成功，共 72 行原始结果：
[peer-pool-windows-20261010.csv](peer-pool-windows-20261010.csv)。
每配置 3 轮；TCP 64 B 每连接 4096 批，TCP 64 KiB 和 TLS 64 B 为 256 批，
TLS 64 KiB 为 32 批。下表为各组吞吐中位数；变化率为
`100 × (启用中位数 / 关闭中位数 - 1)`。

| 负载 | Owners | 关闭 pool | 启用 pool | 变化 |
| --- | ---: | ---: | ---: | ---: |
| TCP 64 B（消息/s） | 1 | 436,995 | 497,574 | +13.86% |
| TCP 64 B（消息/s） | 2 | 829,924 | 975,082 | +17.49% |
| TCP 64 B（消息/s） | 4 | 1,529,647 | 1,891,223 | +23.64% |
| TCP 64 KiB（MiB/s） | 1 | 2,118.66 | 1,584.44 | -25.21% |
| TCP 64 KiB（MiB/s） | 2 | 2,479.20 | 2,589.76 | +4.46% |
| TCP 64 KiB（MiB/s） | 4 | 3,130.61 | 3,837.01 | +22.56% |
| TLS 64 B（消息/s） | 1 | 9,052 | 9,127 | +0.83% |
| TLS 64 B（消息/s） | 2 | 12,093 | 11,252 | -6.95% |
| TLS 64 B（消息/s） | 4 | 19,810 | 20,671 | +4.35% |
| TLS 64 KiB（MiB/s） | 1 | 13.71 | 13.36 | -2.55% |
| TLS 64 KiB（MiB/s） | 2 | 21.59 | 21.36 | -1.07% |
| TLS 64 KiB（MiB/s） | 4 | 38.65 | 37.97 | -1.76% |

MED，事实：单 owner TCP 64 KiB 首轮中位数下降 25.21%，但关闭组范围为
1075.39–2386.87 MiB/s，启用组为 1366.76–2490.64 MiB/s，不能只取中位数
确认稳定回归。为检查这项偏差，将每连接批数增至 1024，每配置重复 9 轮，
得到 [54 行复测数据](peer-pool-windows-20261010-tcp-large.csv)：

| TCP 64 KiB | 关闭 MiB/s | 启用 MiB/s | 变化 | P99 中位数关闭/启用（µs） |
| --- | ---: | ---: | ---: | ---: |
| 1 owner | 2,228.00 | 2,417.51 | +8.51% | 869.8 / 713.6 |
| 2 owners | 3,660.74 | 3,809.50 | +4.06% | 1020.7 / 1018.6 |
| 4 owners | 4,916.24 | 5,665.86 | +15.25% | 1678.2 / 1604.9 |

推论：本次较长复测没有复现该项持续降速，仍不足以把正向差值归因于 pool。
单 owner 复测范围仍达 1178.51–2464.20 / 1543.36–2676.19 MiB/s。
pool 默认关闭不变；没有以这些数据作为修改热路径或默认配置的依据。

TLS 64 B 首轮的 2-owner 降幅也做了较长复测：每连接 512 批、每配置 9 轮，
[54 行原始数据](peer-pool-windows-20261010-tls-small.csv) 对应结果如下。

| TLS 64 B | 关闭消息/s | 启用消息/s | 变化 | P99 中位数关闭/启用（µs） |
| --- | ---: | ---: | ---: | ---: |
| 1 owner | 7,281 | 6,651 | -8.65% | 4114.0 / 3868.0 |
| 2 owners | 10,871 | 10,932 | +0.56% | 5024.4 / 4446.7 |
| 4 owners | 15,718 | 16,243 | +3.34% | 7108.0 / 6593.8 |

MED，事实：复测没有保留首轮 2-owner 的下降方向，但 1-owner 中位数低 8.65%；
该组关闭/启用范围为 5297–8040 / 5207–8383 消息/s。两次运行的绝对吞吐也有
明显变化。推论：目前不能确认统一的加速或无回归结论；若要改变默认配置，需在
受控负载和线程亲和下采集 profiling 证据，区分 listener admission、TLS 处理与
系统调度成本。本次只记录观察，不据此修改运行时。

计算：同一 repeat 内 `100 × (启用 / 关闭 - 1)` 再取中位数时，TLS 小消息
1/2/4 owners 分别为 -0.94% / -1.15% / +3.69%。这与“两个中位数的比值”不是
同一统计量；1-owner 有 5/9 轮启用组较慢，配对变化范围为 -28.04% 至 +29.26%。

## 复现

Windows 先进入 VsDevCmd，使用正式 user presets 和已选择的 SDK：

```powershell
cmake --preset win-release-user -DBUILD_TESTS=ON
cmake --build --preset win-release-user
ctest --preset win-release-user -R '^test_flowmq_socket_owners$' --output-on-failure
ctest --preset bench-win-release-user -R '^bench_flowmq_peer_pool_' -V --output-log build/peer-pool-comparison.log

$env:FLOWMQ_OWNER_BENCH_REPEATS = '9'
$env:FLOWMQ_OWNER_BENCH_ROUNDS = '1024'
ctest --preset bench-win-release-user -R '^bench_flowmq_peer_pool_tcp_65536$' -V --output-log build/peer-pool-tcp-large.log
$env:FLOWMQ_OWNER_BENCH_ROUNDS = '512'
ctest --preset bench-win-release-user -R '^bench_flowmq_peer_pool_tls_64$' -V --output-log build/peer-pool-tls-small.log
Remove-Item Env:FLOWMQ_OWNER_BENCH_REPEATS
Remove-Item Env:FLOWMQ_OWNER_BENCH_ROUNDS
```

CSV 保留 `OWNER_COLUMNS` 对应的字段及每轮结果，末列 `pool_enabled` 为 0/1。
性能报告只输出完成全部消息、所有 worker 成功关闭的运行。

## 安装 SDK 与 CI

Windows 已通过 `install-win-release-user` 安装到 worktree 内独立目录
`stage/qualification/flowmq/windows-x64`，然后从该目录的 exported targets
配置、链接并运行既有 package C11/C++17 consumer，两项 CTest 均通过。
这次是安装树验证，不只是 build-tree shared-library 测试。

`tests/package/CMakeUserPresets.json` 复用根 `vcpkg.json`，使用共享工具链、
manifest 和 cache-only restore。父环境必须提供 `FLOWMQ_ROOT`、`SALTS_ROOT`、
`SALTS_UTILS_ROOT`、`VCPKG_ROOT`、`VCPKG_CACHE_REPOSITORY_ROOT`、`FLOWMQ_TRIPLET`
及既有缓存环境。前三者指向对应已安装的目标 SDK；只从指定根解析一方依赖。
Windows 在 VsDevCmd 后保留原先选定的 `VCPKG_ROOT`。

要复现本地安装，在仓库根目录将 `FLOWMQ_ROOT` 设为要生成的独立安装目录，
再执行（不要指向依赖 SDK）：

```powershell
cmake --preset win-release-user -DBUILD_TESTS=ON "-DCMAKE_INSTALL_PREFIX=$env:FLOWMQ_ROOT"
cmake --build --preset install-win-release-user
```

在 `tests/package` 内执行：

```sh
cmake --preset package-host-release
cmake --build --preset package-host-release
ctest --preset package-host-release
```

发布 workflow 的 Linux/Windows/macOS qualification 均调用以上入口；Android
使用 `package-android-release` configure/build，仅编译链接，不冒充宿主运行。
手动 `run_benchmarks` 同时为三个宿主启用 pool CTest 对照并上传日志。
workflow 已通过 actionlint；本机未执行 Linux/macOS/Android，也未远程触发 CI。

#123 保持打开：跨平台实跑以及 retained-byte/分配成本的
独立证据仍不能由上述单机 PAIR 吞吐结果替代。
