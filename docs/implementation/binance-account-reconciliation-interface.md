# Binance 订单响应与账户对账

`RestOwner` 持有账户 Gateway 和私有输入 WAL。一次最多保留一个交易 REST 请求；发送完成只表示本机输出完成。完整 HTTP 响应必须与已完全接纳的原请求对应，原文和解析决定先同步，再按独立输入键更新账户。ACK、RESULT、FULL 中的累计量用于订单观察，实际成交只来自带 trade ID、数量、价格和费用的成交事实。

`ReadOnlyOrderReader` 根据已持久化的原订单查询 `/api/v3/order` 或 `/fapi/v1/order`。有 venue order ID 时使用它，否则使用原 client ID。`ReadOnlyReconciler` 查询真实成交页，`AccountSnapshotReader` 顺序读取账户、普通开放订单和条件订单。三个读取器都使用真实 TLS 和原签名时间，在首次应用字节接纳前重新检查时钟、请求期限和共享额度。`cancel()` 释放当前只读请求，但保留连接代次计数和时钟故障状态。

控制器通过 `RestOwner::begin_reconciliation(&GapToken)` 持久保存当前私有连接和缺口代次。`reconciliation_orders()` 返回待核订单及本地责任；逐订单查询后，从 `from_id=0` 开始读取成交页。`complete_trade_reconciliation(query)` 从 WAL 核对同一连接/缺口下的完整页序列，累计实际成交必须等于订单查询累计量和账户已入账量。终态订单随后同步 `FeesReconciled`；活动订单继续保留费用责任。

`reactivate([SnapshotEvidence; 3], ActivationCut, Option<ConnectionConfig>)` 核验三份持久快照、完整输入前缀、基线、账户历史摘要、账本序号和时钟域，再同步账户 `Reactivated` 记录并发布新状态。快照年龄不能超过本次有效期长度。健康发送器可以保留 writer 和连接；已停止或恢复的发送器使用新连接，恢复 writer 高于物理日志中的历史 writer。旧意图保留原状态，不能重新发送。

USD-M 的激活记录包含真实账户响应中的可用余额。该余额已包括已核对订单与现金流的交易所占用，因此替换旧影子余额并清除旧请求的影子扣减；后续请求和费用从新起点计算。本地压力资本、资产责任和订单事实继续保留。

恢复先打开输入 WAL，再将 `activation_verifier()` 传给 `AccountGateway::recover_with_reactivation`。它按账户历史前缀重建状态，重新核验原始快照和激活绑定。普通 `recover` 拒绝没有对应核验器的激活历史。恢复完成的账户始终冻结，重新取得当前对账证据后才能发送新命令。

输入 WAL 新记录包括交易响应、成交页、订单查询、三类快照和对账起点；账户 codec 的 tag 10 保存完整激活决定，旧标签编码保持原样。复杂条件父单需要独立的责任核对能力，当前激活要求条件订单列表为空。
