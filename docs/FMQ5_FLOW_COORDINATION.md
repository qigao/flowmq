# FMQ/5 流量协商与协调设计

## 背景与目标

FlowMQ 运行在 CNet TCP/TLS 有序字节流之上，由调用 `send`、`recv` 或 `poll` 的
单一 owner 推进，不创建后台线程。本次把 FMQ 控制协议明确分成两类：

- 协商：连接建立后交换能力、session generation、frame 上限和初始接收 credit。
- 协调：接收方以累计消费量推进发送方可发送的累计字节上限。

FMQ/5 不兼容 FMQ/4，也不协商或回退到旧 wire version。目标是让发送速率随接收方
消费能力自然收敛，同时保持控制流量远低于数据流量。

## 候选方案

### TCP/TLS 流内 FEC

拒绝。TCP 已提供可靠、有序传输和拥塞控制；同一有序流内的 repair symbol 不能绕过
丢包造成的 head-of-line blocking，只会增加带宽、CPU 和 framing 状态。FEC 留给未来
UDP、KCP、QUIC datagram 或 multicast transport，不进入本次 TCP/TLS wire。

### 发送方按 RTT 或固定速率 pacing

暂不采用。CNet 当前没有稳定公开的 RTT、loss、cwnd 或 delivery-rate 指标；仅用本地
时钟推算会把应用暂停误判为网络容量，并可能压低突发吞吐。固定速率也不能自适应。

### 接收方累计 credit

采用。接收方只公布单调递增的 `max_data`；发送方满足
`sent_data <= max_data`。应用释放消息时，接收方增加 `consumed_data`，并把新窗口
`consumed_data + receive_window` 通过 `FLOW_UPDATE` 公布。该方式同时表达接收队列
容量和真实消费速度，不需要线程、锁或网络指标。

## Wire 协议

固定 FMQ header 不变，version 从 `4` 直接改为 `5`。新增两个 core frame kind，避开
既有 ESB 保留段 `7..31`：

| Kind | Value | Payload | 作用 |
| --- | ---: | ---: | --- |
| `SETTINGS` | 32 | 32 bytes | 协商能力和初始 credit |
| `FLOW_UPDATE` | 33 | 24 bytes | 推进累计消费量和发送上限 |

`SETTINGS` payload 全部使用 big-endian：

| Offset | Size | Field | 约束 |
| ---: | ---: | --- | --- |
| 0 | 4 | capabilities | 当前必须等于 `FLOW_CREDIT` |
| 4 | 4 | max_frame_size | 非零；发送方使用本地与远端限制中的较小值 |
| 8 | 8 | session_generation | 非零 |
| 16 | 8 | initial_max_data | 非零 |
| 24 | 4 | flow_update_quantum | `1..initial_max_data` |
| 28 | 4 | flow_update_interval_ms | 非零 |

`FLOW_UPDATE` payload：

| Offset | Size | Field | 约束 |
| ---: | ---: | --- | --- |
| 0 | 8 | session_generation | 必须等于本方向 SETTINGS generation |
| 8 | 8 | consumed_data | 单调不减 |
| 16 | 8 | max_data | 单调不减且不小于 consumed_data |

两个控制帧的 identity、topic、message ID 和 `MORE` 必须为空或为零，且都必须是单
packet。控制帧不消耗 data credit，并优先于 queued DATA 发送。

## 连接状态机

每个方向固定执行：

```text
TCP/TLS connected -> HELLO -> SETTINGS -> READY -> DATA/FLOW_UPDATE
```

收到 SETTINGS 前的 DATA、重复 SETTINGS、generation 不匹配、累计值倒退或越过本地
公布上限都返回 `TURBO_EPROTO` 并关闭失败连接。FMQ/5 没有 legacy READY 分支。

## 状态所有权与算法

每个 `flowmq_socket_peer_t` 是双向流控状态的唯一事实源，且只由 socket owner lane
访问：

- 发送方向：`remote_generation`、`sent_data`、`remote_consumed_data`、
  `remote_max_data`。
- 接收方向：`local_generation`、`received_data`、`consumed_data`、
  `advertised_max_data`。

发送 admission 在复制或引用 payload 前检查完整 application part/message 是否满足
`payload_bytes <= remote_max_data - sent_data`。multipart 在最后一 part 提交时一次性
检查并记账，保持现有 transactional HWM 语义。

收到 DATA 时增加 `received_data`，若超过 `advertised_max_data` 则协议失败。应用成功
取走完整消息后才增加 `consumed_data`；ROUTER 合成的 identity part 和 XPUB 合成的
subscription event 不计入 wire data credit。

当未公布的消费字节达到 `flow_update_quantum`，或最早未公布消费已等待
`flow_update_interval_ms`，owner 的下一次进度调用发送一个累计 `FLOW_UPDATE`。窗口
目标为 `consumed_data + receive_hwm_bytes`，溢出返回 `TURBO_ERANGE`。默认 quantum
由 receive HWM 推导：`min(window, max(window / 4, 64 KiB))`；默认 interval 为 10 ms。

## CMeta 与 CFlow 边界

CMeta `Schema/Replay` 作为 SETTINGS/FLOW_UPDATE 字段 offset、width、编码和解码的
单一机械事实源，避免手写两套布局。公开 header 不暴露 CMeta 类型。

CFlow 不进入 protocol codec 或 per-message admission 热路径。本次状态迁移是一个
单 owner、常数复杂度的同步判定；把它包装成 graph 会引入调度和间接调用而不增加
并发能力。后续多 socket executor 可用 CFlow 组合低频 `poll`、deadline 和策略事件，
但不得改变 class-ZMQ 的同步公开 API。

## 复杂度、带宽与资源边界

- 每帧 admission、DATA/FLOW_UPDATE 处理均为 O(1) 时间、O(1) peer 状态。
- `FLOW_UPDATE` wire 大小为 32-byte header + 24-byte payload = 56 bytes。
- 每 64 KiB 更新一次的控制开销为 `56 / 65536 = 0.0854%`；默认 16 MiB HWM 下每
  4 MiB 更新一次为 `0.00134%`。
- credit 不替代本地 message/byte HWM；两者都必须通过才可 admission。

## 迁移、回滚与验证

迁移直接切换 wire version 和强制 SETTINGS，不保留 FMQ/4 decoder 或 fallback。
回滚只能整体恢复 FMQ/4 代码和双方部署版本，不能在运行时降级。

验证范围：协议 codec 正反例、连接握手乱序、累计值单调性、credit 耗尽与释放、
multipart 原子 admission、ROUTER synthetic part 记账、TCP/TLS loopback、既有 pattern
回归、Debug/ASan、Release 和 libzmq 对比 benchmark。
