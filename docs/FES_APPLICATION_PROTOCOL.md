# FES/1 Application Protocol

FES/1 是 FlowMQ 的 ESB application envelope。它只作为 FMQ/6 `DATA` payload 出现，不声明
socket pattern、peer pairing、连接状态或 transport credit。调用方可通过任何语义合适的
ZeroMQ-style socket pattern 传送其 bytes。

## Envelope

所有整数使用 network byte order（big-endian）。decoder 只接受一个完整 envelope，不接受未知
field、重复/乱序 field、保留位、长度不一致或 trailing bytes。

| Offset | Size | Field | Constraint |
| ---: | ---: | --- | --- |
| 0 | 4 | magic | `FES1` |
| 4 | 1 | message kind | `1..13` |
| 5 | 1 | flags | 必须为 `0` |
| 6 | 2 | reserved | 必须为 `0` |
| 8 | 4 | metadata length | 当前 kind 的完整 TLV byte count |
| 12 | 4 | payload length | raw binary payload byte count |
| 16 | variable | metadata | kind-specific strict TLV sequence |
| following | variable | payload | binary-safe borrowed view |

TLV header 为 `type:u8 + length:u32`，随后是 value。FES/1 不提供 relaxed compatibility mode；
新增字段或改变字段顺序需要发布新的 FES version。

## Message kind 与 metadata

| Kind | Value | Required metadata order |
| --- | ---: | --- |
| SCATTER_REQUEST | 1 | expected responses `u32`、aggregation policy `u8` |
| GATHER_RESPONSE | 2 | 无 |
| PARTIAL_RESPONSE | 3 | partial index `u32` |
| SAGA_EXECUTE / COMMIT / COMPENSATE / ABORT | 4..7 | saga id `u64`、step `u32`、state `u8` |
| STREAM_PUBLISH | 8 | partition `u32`、offset `u64` |
| STREAM_SUBSCRIBE | 9 | consumer group bytes |
| STREAM_COMMIT | 10 | partition `u32`、offset `u64`、consumer group bytes |
| STREAM_REBALANCE | 11 | consumer group bytes |
| PRIORITY_PUBLISH | 12 | priority `u8` |
| CIRCUIT_STATUS | 13 | state `u8`、failure count `u32` |

fanout 最大 1024；saga step 小于 256；partition 小于 256；consumer group 为 1..255 bytes。
SCATTER_REQUEST 的 expected responses 必须非零，SAGA message 的 saga id 必须非零。

## API 与所有权

`flowmq_esb_encode()` 返回 owned `tstr`；调用者使用 `tstr_free()` 释放。`flowmq_esb_decode()`
不分配，`consumer_group` 和 `payload` 都借用 encoded input。输入在借用 view 使用结束前必须存活
且不可修改。`max_message_size` 同时约束 encode 与 decode，超限返回 `SALTS_EMSGSIZE`。

```c
#include "flowmq_esb.h"
#include "salts_error.h"

int main(void) {
  flowmq_esb_message_t message = {
      .kind = FLOWMQ_ESB_PRIORITY_PUBLISH,
      .priority = 7u,
      .payload = vstr_from_cstr("work")};
  tstr encoded = NULL;

  if (flowmq_esb_encode(&message, 1024u, &encoded) != SALTS_OK) {
    return 1;
  }
  /* encoded 可直接作为一个普通 FMQ DATA frame 的 payload。 */
  tstr_free(encoded);
  return 0;
}
```

旧 `flowmq_protocol_esb_*` API 把 transport frame 与 ESB metadata 混合，并把 TLV 追加到完整
FMQ frame 后；该格式及 relaxed decoder 已删除，不提供兼容路径。
