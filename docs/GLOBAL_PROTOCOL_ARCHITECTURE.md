# FlowMQ 全局协议架构

## 决策背景

重构前，FMQ/5 transport codec 同时接受 ZeroMQ socket pattern、ESB pattern 和 ESB frame kind；
FMS 又同时被文档用于 security 与 media-provider 两种含义。结果是 transport、security、
application 三层共享编号空间，但 socket runtime 实际只实现 ZeroMQ 风格 API。ESB encoder 还会把
TLV 追加在一个已经完成编码的 FMQ frame 之后，迫使 FMQ decoder 识别 ESB trailing bytes。

本次不保留旧 wire decoder 或名称别名。公开 socket 行为仍是 caller-driven、单 owner、类 ZeroMQ
模式；协议目录是只读元数据，不引入全局可变状态、线程或隐藏 executor。

## 全局协议目录

`flowmq_protocol_catalog.h` 是协议 family、当前版本、层次、magic 与能力的唯一公开事实源。
目录由进程内不可变表实现，查询不分配内存、不加锁，也不参与逐消息 dispatch。

| Family | 当前版本 | 层次 | Magic / 标识 | 责任 |
| --- | ---: | --- | --- | --- |
| FMQ | 6 | transport | `TFMQ` + version byte | framing、ZMQ pattern、HELLO/SETTINGS、credit、heartbeat、multipart |
| FMS | 3 | security | `FMS3` | HELLO 内的认证/接受 envelope 语法 |
| FES | 1 | application | `FES1` | ESB application message metadata 与 payload envelope |
| FMP | 1 | application schema | TBE schema id `22001` | media-provider typed payload contract |

`FMS/1` 这一旧称被删除：FMS 只表示 FlowMQ Security，media-provider 统一称为 FMP/1。

## 分层与状态归属

```text
FMP/1 typed message 或 FES/1 application envelope
                    ↓ bytes
              FMQ/6 DATA payload
                    ↓ frame
           CNet TCP 或 verified TLS

FMS/3 只允许位于 FMQ/6 HELLO payload
```

- FMQ/6 拥有连接协商、传输 credit 和消息边界；只接受 PUB/SUB、PUSH/PULL、
  ROUTER/DEALER、PAIR、REQ/REP、XPUB/XSUB。
- FMS/3 只拥有 security envelope 的 wire 语法。credential verifier 尚未接入时，socket
  boundary 继续 fail fast 拒绝非空 security payload。
- FES/1 拥有 ESB message kind 与 kind-specific metadata。它是普通 DATA payload，不能声明新
  socket pattern，也不能改变 FMQ flow-control 状态。
- FMP/1 拥有 media-provider schema version 与 typed validation，不拥有 delivery、receipt 或
  durable storage 状态。

## 候选方案与选择

| 方案 | 性能 | 复杂度 | 接口一致性 | 结论 |
| --- | --- | --- | --- | --- |
| 保留 ESB reserved pattern/frame kind | core hot path 多分支，decoder 必须识别 trailing TLV | 高 | socket API 无对应类型 | 拒绝 |
| 为每个应用协议增加 FMQ frame kind | header 编码快 | 每个应用都修改 transport | 跨层耦合持续增长 | 拒绝 |
| 应用协议统一装入 FMQ DATA | core 路径最短；应用按需解码 | 分层清楚 | 与类 ZMQ bytes API 一致 | 采用 |

全局目录采用 immutable catalog，而不是 mutable registry/singleton。原因是协议集合在编译期确定，
运行期注册会引入初始化顺序、同步和第三方状态注入问题，却不能改善 wire dispatch。

## Wire 与错误语义

- FMQ/6 不接受 FMQ/5，也不协商或 fallback。
- FES/1 采用固定 header、network byte order 和严格 TLV：未知、重复、乱序、长度错误以及不属于
  当前 message kind 的字段都返回 `SALTS_EPROTO`。
- decode 返回的 `vstr` 都借用输入；encode 返回的 `tstr` 归调用者所有。
- 所有可增长 payload 受调用方 `max_message_size` 与协议常量双重约束。
- catalog 的未知 family/index 返回 `NULL`，不猜测默认协议。

## CMeta/CFlow 边界

catalog 与 fixed-width control field 使用 CMeta Schema/Replay 生成静态表和重复编解码语句；CMeta
保持为实现依赖，不泄露到公开头文件。CFlow/executor 仍用于控制面组合、测试编排或未来连接级流程，
不进入 FMQ/FES 每帧 codec，也不改变 CNet 的 caller-driven 执行模型。

## 迁移与回滚

迁移是一次性替换：升级双方到同一 build，应用把原 ESB frame 构造改为 `flowmq_esb_message_t`，
编码后的 FES/1 bytes 通过任意合法 FMQ socket 的 DATA payload 发送。旧
`flowmq_protocol_esb.h`、relaxed decoder、ESB pattern/frame kind 和 from-base API 全部删除。

回滚只能回到本变更前的完整二进制与协议文档；不能让 FMQ/5 与 FMQ/6 peer 混连。验证必须覆盖
版本拒绝、FMQ pattern 边界、FMS/3 round-trip、FES/1 strict round-trip、FMP/1 version routing、
TCP/TLS 回归与 release benchmark。
