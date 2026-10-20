# 独立 guardian 接口

`tickwire-guardian` 在独立进程内持有自己的凭据、TLS 连接、校准时钟、配额消费者和审计 WAL。它不调用故障 exec 的 RPC，也不持有账户释放或新 exec 授权接口。

`Guardian::submit` 只接受规范命令 `Query`、`Cancel`、`CancelList`、`CancelAlgo`，复用 Binance 已有请求编码。普通单查询可核查触发后子单；父结构取消结果保留原文，不能代替子单成交对账。`CancelReplace`、改单、普通或条件新单、SOR、平仓均拒绝。输入命令仍绑定原账户、产品、标的及请求期限。

权限有两个入口：

- `Authorization::Emergency`：健康期预提交的、由独立 emergency 公钥验证的授权，明确允许人工触发，限定 guardian、账户、标的、动作、请求数量和期限。它只能支持尽力查询和取消。
- `Authorization::Fenced`：由独立隔离控制器签名的证据，绑定预期旧主完整身份、控制提交位置、全部出口集合、deny tombstone、设备动作及版本、撤销既有会话、凭据访问撤销，以及读回和探测摘要。签名验证信任控制器实际完成这些动作；guardian 不把 PID、超时或租约过期转换成隔离证据。

轻量 `tickwire-fencing` 提供共享的 `ExecutionIdentity`、`FenceClaims`、`SignedDocument`、`Verifier` 和私有构造的 `VerifiedFence`。签名覆盖完整原始 JSON 字节。隔离和 emergency 使用不同固定 Ed25519 公钥；声明经过验证后才成为能力。

```rust,ignore
Guardian::create(store, config, verifier, clock, quota, network)
Guardian::recover(store, config, verifier, clock, quota, network, required)
guardian.submit(command, authorization, credentials)
guardian.poll(IoBudget)
guardian.status(command_digest)
guardian.audit_position()
```

提交先将规范命令、授权原文及签名、实际请求摘要、连接代次持久化，再启动输出。每次零接受重试都重验权限和共享额度；所有权限与配额检查之后，最后用原始 `SignedTime` 调用校准时钟检查，使用其返回的最新单调时间核对请求和授权期限。排队请求不会被 `drive` 自动送入 TLS。

完整原始 HTTP 响应和分类先同步进审计 WAL，再交给调用者。只有零接受证据可以记录 `NoBytes`；TLS 已接受部分明文后，超时或断连保留 `Unknown`。HTTP 完成不等于账户安全，guardian 不减少 Unknown 预留。恢复时所有未完成请求保留未决，精确重试读取原结果，不自动重新发送。

单次请求和响应有界；较大响应按小记录分批持久化，最终完成记录绑定整个响应摘要。副本未接入时显式保留 `audit_degraded`；本地审计失败停止后续输出。已持久化的配额运行标识不能在重启后重新发放同一份本地额度。

独立可执行程序读取自己的部署文件、授权文档和规范命令文件；凭据只在该进程内加载，日志不输出 API key、签名或密钥。校时使用真实 HTTPS `/time`，业务请求使用正常验证主机名的 TLS。`Endpoint` 对校时、guardian 连接和实际请求分别计费；外部配额控制存储可由部署适配器提供，内存控制存储只代表显式选用的控制后端。

设计依据：[guardian 独立撤单能力](../design/binance-ultra-low-latency.md#91-guardian-的独立撤单能力)、[实际隔离与执行权状态机](../design/replication-and-failover.md#8-实际隔离与执行权状态机)。本模块只构造和解释这两个协议已有的查询、取消请求，不新增交易所参数。
