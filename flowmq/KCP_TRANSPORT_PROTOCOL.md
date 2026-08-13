# FlowMQ Secure KCP Transport Protocol

本文定义唯一 KCP transport wire protocol：KCP 会话认证、AEAD record、
Reed-Solomon FEC、重放边界和公开调优参数。不存在 raw KCP、未认证 FEC、旧 frame、
自动 transport fallback 或版本协商。

## 1. 分层

```text
TFMP/1、TFCW/1 或应用 payload
                    |
                  FMQ/3
                    |
                   KCP
                    |
        TKSR/1 authenticated AEAD record
                    |
        TKF1/1 authenticated RS-FEC shard
                    |
                   UDP
```

发送顺序固定为 KCP packet → AEAD → FEC → UDP；接收顺序固定为有界 header 校验 →
FEC MAC 校验 → 缺片恢复 → AEAD tag 校验 → replay window → `ikcp_input()`。
GF256/Reed-Solomon 只负责丢包恢复，不能替代认证、完整性或重放防护。

## 2. 会话建立 TKSH/1

客户端发出 `HELLO`，服务端验证 PSK MAC 后才创建会话并锁定 UDP peer，再返回
`HELLO_ACK`。双方从 PSK、client nonce、server nonce 和随机 session epoch 派生
client→server key、server→client key 与 FEC MAC key。重复的同 nonce `HELLO`
返回同一会话确认；已建立会话收到不同 nonce 必须拒绝。

TKSH/1 固定 64 bytes，整数为 network byte order：

| Offset | Size | Field | Constraint |
| ---: | ---: | --- | --- |
| 0 | 4 | magic | `TKSH` |
| 4 | 1 | protocol | `1` |
| 5 | 1 | type | `HELLO=1`, `HELLO_ACK=2` |
| 6 | 2 | frame size | `64` |
| 8 | 16 | client nonce | 随机且非复用 |
| 24 | 16 | server nonce | HELLO 为零；ACK 为随机值 |
| 40 | 8 | session epoch | HELLO 为零；ACK 为随机非零值 |
| 48 | 16 | MAC | keyed BLAKE2b-128，覆盖 bytes 0..47 |

PSK 必须是 32 bytes，并通过 secret provider、受保护配置或平台密钥存储注入，不得
写入日志。该握手提供双向 PSK 认证，但不提供 forward secrecy；需要 forward secrecy
的部署应在受控网络隧道内运行，而不是在运行时切换协议。

## 3. AEAD record TKSR/1

每个 KCP packet 独立封装为一个 TKSR/1 record。AEAD 使用 Monocypher
XChaCha20-Poly1305；32-byte header 是 associated data，不加密但受认证。

| Offset | Size | Field | Constraint |
| ---: | ---: | --- | --- |
| 0 | 4 | magic | `TKSR` |
| 4 | 1 | protocol | `1` |
| 5 | 1 | direction | client=`1`, server=`2` |
| 6 | 2 | header size | `32` |
| 8 | 8 | session epoch | 必须等于当前会话 |
| 16 | 8 | packet number | 从 1 单调递增，不得回绕 |
| 24 | 2 | plaintext size | `1..65535` |
| 26 | 6 | reserved | 必须为零 |
| 32 | variable | ciphertext | 加密后的 KCP packet |
| 32+N | 16 | AEAD tag | 必须验证成功 |

24-byte AEAD nonce 由 `epoch[8] || packet_number[8] || direction[1] || zero[7]`
构成。接收端维护 64-packet replay bitmap；认证失败、重复、窗口外 packet 或错误方向
不得推进 replay state，也不得改变 peer/session 状态。

## 4. FEC shard TKF1/1

每个 TKSR record 成为一个 data shard。每收集 `data_shards` 个 data shard，编码器使用
`vendor/reed/gf256.h` 的 Reed-Solomon 实现生成 `parity_shards` 个 parity shard。

| Offset | Size | Field | Constraint |
| ---: | ---: | --- | --- |
| 0 | 4 | magic | `TKF1` |
| 4 | 1 | protocol | `1` |
| 5 | 1 | type | data=`1`, parity=`2` |
| 6 | 2 | header size | `30` |
| 8 | 8 | session epoch | 必须等于当前会话 |
| 16 | 4 | group ID | 组内一致 |
| 20 | 2 | shard ID | data 在前，parity 在后 |
| 22 | 2 | data shards | 必须等于本地配置 |
| 24 | 2 | parity shards | 必须等于本地配置 |
| 26 | 2 | payload size | data 为 record 大小；parity 为固定 shard 大小 |
| 28 | 2 | reserved | 必须为零 |
| 30 | variable | shard payload | data 或 parity |
| 30+N | 16 | MAC | session FEC key 的 keyed BLAKE2b-128 |

data/parity shard 必须先通过 MAC 才能进入 receive group。恢复出的 data record 仍必须
通过 TKSR AEAD tag；因此被篡改的 parity 不能伪造 KCP plaintext。

receive group 使用 session-owned 固定 ring。新 group 覆盖仍未完成的旧 slot 时，旧组
被明确丢弃；不得无界增长或静默把不同 epoch/config 的 shard 合并。

## 5. KCP 与确认语义

KCP 的 ACK、重传、窗口和拥塞算法属于 transport 内部状态，不是 FMQ delivery ACK。
应用请求不等待逐条管理结果；TFMP mutation 先返回 correlated ACCEPT，终态由
`OPERATION_GET` 或 event channel 获取。KCP conv 从 session epoch 派生，不使用固定值。

peer 只有在 TKSH MAC 验证成功后才能锁定。shutdown 顺序为停止 admission、取消或
drain FEC group/KCP packet、停止 timer/UDP callback、清零 session keys，再释放
KCP/FEC state。

## 6. 公开配置

FlowMQ/YAML 的 PSK 使用恰好 64 个十六进制字符。其他字段为启动期配置，不支持热切换：

| Parameter | Default | Constraint / effect |
| --- | ---: | --- |
| `kcp_pre_shared_key` | 无 | KCP 必填，32-byte PSK |
| `kcp_mtu` | 1200 | `576..65535` |
| `kcp_send_window` | 256 | `1..65535` |
| `kcp_receive_window` | 256 | `1..65535` |
| `kcp_interval_ms` | 5 | `1..100` |
| `kcp_handshake_retry_ms` | 200 | `1..65535` |
| `kcp_fast_resend` | 2 | `0..255`；零值配置使用默认值 |
| `kcp_congestion_control` | false | true 启用 KCP congestion window |
| `kcp_fec_data_shards` | 8 | `1..255` |
| `kcp_fec_parity_shards` | 2 | `1..255` |
| `kcp_fec_max_payload_size` | 1248 | 至少 `mtu + 48`，最大 65535 |
| `kcp_fec_receive_groups` | 16 | `1..64` |

`data_shards + parity_shards` 不得超过 255。receive ring 的 shard storage 不得超过
64 MiB；超限配置在 bind/connect 前失败。非 KCP transport 出现任意 KCP 参数也必须
失败，避免配置被静默忽略。

## 7. 验证入口

实现位于 CoroNet `src/turbo_kcp.c`、`src/turbo_kcp_secure.c` 和
`src/turbo_kcp_fec.c`。`tests/test_kcp.c` 覆盖错误 PSK、篡改、重放、parity corruption、
丢片恢复、配置边界、认证后 peer 建立与端到端收发；FlowMQ 的
`tests/test_fmq.c`、`io/socket/tests/test_socket.c` 覆盖上层配置转发与 transport 组合。
