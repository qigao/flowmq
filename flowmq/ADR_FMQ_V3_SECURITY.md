# ADR: FMQ v3-only optional security binding

本 ADR 是 [协议索引](PROTOCOL_SPEC.md) 指定的安全决策记录；FMS/3 字段和 wire 校验的
唯一正文见 [FMQ_WIRE_PROTOCOL.md](FMQ_WIRE_PROTOCOL.md)。本文记录决策理由、owner、
安全边界和验证要求，不重复维护 envelope layout。

状态：已采用（2026-07-18）

## 背景

FMQ 需要在 peer admission 前完成身份认证和 domain-aware authorization，同时保留由宿主可信边界
保护的轻量 endpoint。wire 版本必须只有一个事实源，认证失败不得回退匿名模式，否则部署者无法判断
实际安全边界。

本决策影响 `FlowMQ::Protocol`、private connect endpoint runtime、`TurboFlow::FMQ` adapter、公开 C API、
测试与部署文档，因此需要明确记录协议与状态所有权。

## 决策

1. FMQ decoder 只接受 wire v3。version byte 不是 negotiation 字段；任何其他值统一返回
     `TURBO_EPROTO`。
2. v3 HELLO security envelope 是可选的。未配置 security binding 时，双方必须发送空 envelope，形成
   trusted v3 endpoint；任何一方发送 secure envelope 都会被拒绝。
3. 配置 security binding 后，安全要求是强制且不可降级的：
   - TCP/TLS/UDP/KCP/Pipe/WS/WSS 都执行相同 authentication 和 default-deny ACL；
   - TLS/WSS 必须经过 client peer verification，并强制 RFC 9266 TLS exporter channel binding；
   - KCP 强制使用 TKSH/1 PSK 认证、TKSR/1 AEAD 和 authenticated RS-FEC；其 wire 见
     [KCP_TRANSPORT_PROTOCOL.md](KCP_TRANSPORT_PROTOCOL.md)；
   - TCP/UDP/Pipe/WS 不提供 credential confidentiality，生产环境必须由可信网络或额外安全隧道保护；
   - client 通过 key provider/reference 临时取得 credential；
   - AUTH envelope 绑定 claimed identity 和 method；TLS/WSS 还绑定 exporter；其他 transport 的
     binding 字段必须为空，不能伪装成 channel-bound session；
   - server 在创建 live peer、加入 selector/route registry 或发布 graph message之前完成 authentication；
   - provider principal ID 必须与 wire claimed identity 完全一致；
   - CONNECT 与后续 SUBSCRIBE/READ/WRITE/EXECUTE 均通过 immutable realm 做 default-deny authorization；
   - credential 不进入 graph、event、log 或 session，发送/decoder owned bytes 在边界结束前清零。
4. BIND endpoint 是认证与 ACL 决策 owner。它复制成功认证的 pointer-free principal 到 peer session；
   auth/key provider 和 realm 保持 host-owned borrowed lifetime。realm policy version 与 principal expiry 在
   每次 authorization 时重新校验。
5. 安全失败只通过统一的 connection failure 暴露给远端；host event 可以区分 authentication failure 与
   authorization denial，但不得携带 credential。
6. resolved product composition 使用 `turbo_flow_fmq_security_owner_t`：
   - BIND adapter 的 YAML 只保存 `security_realm`、`auth_provider` 和 `auth_method` 引用；realm 再通过
     `policy_source` 选择精确 SQLite/HTTPS ACL provider；
   - CONNECT adapter 只保存 `auth_method` 与 `secret_reference`，credential 继续来自 key provider；
   - security metadata 交给非 secure registration API 时必须失败，不能静默创建 trusted endpoint；
   - Flowie 与 FlowMQ 共享 provider ABI 和 immutable snapshot 实现，但 policy namespace 必须分离，例如
     `flowie.mqtt` 与 `flowmq.fmq3`。

## 权限映射

| FMQ 行为 | Realm action | Resource |
| --- | --- | --- |
| secure HELLO admission | `CONNECT` | `fmq:<adapter>:connection` |
| PUB/XPUB subscription | `SUBSCRIBE` | topic/prefix，空前缀规范化为 `*` |
| PUB/XPUB/PUSH 向 peer 交付 | `READ` | frame topic；空 topic 使用 connection resource |
| ROUTER/PAIR ingress | `WRITE` | frame topic 或 connection resource |
| REP request ingress | `EXECUTE` | frame topic 或 connection resource |
| ROUTER/REP routed reply | `READ` | frame topic 或 connection resource |

当前使用 Core `GENERIC` resource 的 exact/prefix matcher，避免把 FMQ 规则语义扩散到 security core。
如未来需要通配符语法，必须通过显式 FMQ matcher 和新的协议测试引入，不能改变现有 exact/prefix 解释。

## 候选方案

| 方案 | 结果 | 主要权衡 |
| --- | --- | --- |
| 多 wire version 协商 | 不采用 | 存在 downgrade 与状态组合爆炸，部署实际安全级别不确定 |
| 新增独立 AUTH frame 往返 | 不采用 | 增加握手乱序、timeout 和 partial state；HELLO 已是唯一 admission boundary |
| credential 写入 endpoint config/YAML | 不采用 | secret 生命周期扩散到 config snapshot，难以轮换和清零 |
| v3 HELLO + provider lease + realm | 采用 | 一次握手、状态单一；不同 wire version 不能混合直连 |

## 当前边界

- 只支持 wire v3，不提供 fallback、version negotiation、旧 decoder 或内置 gateway。
- C endpoint config 只有当前完整布局。普通 registration 创建 trusted v3 endpoint；secure
  registration 注入 FMS provider/realm；KCP 无论是否配置 FMS 都必须先通过 TKSH/1。

## 验证与性能边界

- protocol tests：v3 encode/decode、AUTH/ACCEPTED envelope、malformed envelope、unknown version rejection；
- live transport tests：TCP/TLS/UDP/KCP/Pipe/WS/WSS authentication 与 authorization；TLS/WSS 另外覆盖
  exporter binding、错误 credential、default-deny subscription 和 peer admission count；
- regression：所有 pattern、transport、reconnect、shutdown、backpressure 与 release label；
- benchmark：trusted v3 DATA hot path不解析 security envelope；secure authorization 只发生在有安全绑定的
  peer 上。相同 runner 下 throughput 下降超过 10% 或 P99 上升超过 10% 时必须重新 profile。

## 残余边界

- trusted v3 仍依赖宿主网络/进程隔离，不能宣称跨 Domain 安全隔离。
- TLS/WSS 具备 exporter channel binding；KCP 具备 PSK transport 认证与 AEAD；TCP/UDP/Pipe/WS
  只提供应用层认证和 ACL，不提供链路保密。
- secure identity、auth method 和 ACL topic/resource 必须是无内嵌 NUL 的有界文本；credential 仍是
  binary-safe bytes。trusted v3 的既有 binary topic 行为不受此约束。
- realm 与 provider 的配置装配由 host 注册 factory、由 FlowMQ security owner 执行；resolved YAML 不
  存放 ACL body、进程内 provider pointer 或 secret。
