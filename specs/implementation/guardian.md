# 独立 guardian

实现位于 `tickwire-guardian`，隔离证明的公共验签能力位于 `tickwire-fencing`。运行入口：

```sh
cargo run -p tickwire-guardian -- --config /absolute/guardian.json
```

部署文件采用 `cli::Deployment` 的严格 JSON 结构。它包含独立 guardian 身份、旧 exec 的预期完整身份、账户/产品/标的范围、审计目录、隔离与 emergency 的不同 Ed25519 公钥、独立 Binance 凭据文件、DER 信任根、校时和 REST 地址、校准误差预算、共享配额的配置输入、授权文件及规范命令文件列表。凭据文件要求没有组和其他用户的权限。

调用者通过 `Guardian::create/recover` 注入 `Store`、真实 `BinanceTimeClient` 和 `Endpoint`。CLI 使用真实 `FileStore` 和 TLS，仅外部配额分配控制存储采用显式 `MockQuotaStore`；这不是已连接分布式配额控制服务的声明。实际计费使用生产 `Endpoint`，CLI 只接受 Recovery 类规则和授权。`start_quota_run` 在任何校时或交易所连接前持久记录配额消费者身份；重启不能重新加载同一份额度。

输入只准查询、普通撤单、列表撤销和 algo 撤销。软件白名单不等于交易所提供 cancel-only API key。隔离证明必须由可信外部控制器提供；本模块不会执行交换机、电源或密钥撤销动作，也不把签名输入中的设备摘要当成本机已经执行过的动作。

每条命令先保存规范字节、原始签名授权、请求摘要、连接代次和本地时间域，再允许应用数据进入 TLS。审计逻辑事件按小块写入实际 WAL，最后用长度及 SHA-256 提交；中途失败后保留已写物理尾部，未完成的逻辑事件不发布。发送和审计结果的含义如下：

| 结果 | 依据 |
|---|---|
| `NoBytes` | TLS/socket 尚未接受请求应用字节 |
| `Unknown` | 已接纳部分请求但没有完整可解释回复，或恢复后只有请求记录 |
| `Response` | 单次请求完整接纳，收到完整有效的成功 HTTP/JSON 响应；原文已持久 |
| `Rejected/NotFound/RateLimited/Authentication` | 复用 Binance response 分类，并保存原响应 |

这些结果不减少账户预留、不授予新 exec、不声明账户已经安全。当前审计没有复制确认，所以 `audit_degraded` 固定为真。已有命令键恢复后只返回保存结果或 Unknown，不自动重新发送。

当前必要验证为两个轻量 crate 的纯单元测试和可执行目标编译：覆盖增险命令拒绝、跨账户拒绝、签名和期限、撤销存量连接的证据字段、请求计数、分块审计、同步失败后禁止继续、配额运行身份不可重用、连接重试计费和时间回退。

后续统一验证需覆盖真实独立进程、协议服务器、故障 exec 不可响应、部分 TLS 接纳、EOF 与完整响应、外部隔离设备回读、分布式配额及审计复制。当前功能不以这些尚未执行的场景作为完成证据。
