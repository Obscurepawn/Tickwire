# run7 的账户与复制边界

run7 完成了实际成交入账和普通镜像复制。它的费用对账尚未结束；离线报告中的恢复 Unknown 不能解释为交易所返回了 Unknown。完整运行记录见 [run7 证据](../run7/README.md)。

只读提取结果在 [observed-prefix.json](observed-prefix.json)。该文件保留输入 WAL 中的实际 HTTP 响应和 executionReport 原文、记录哈希以及镜像前缀。检查没有重新打开 Journal、修改场景文件或启动网络请求。

账户 WAL 共四条：Reactivated、Admit、REST NEW 观察、私有 TRADE/FILLED。后两条分别绑定输入序号 8、9。成交原文为 trade 33、数量 0.001 BTC、报价金额 60 USDT、手续费 0 USDT。账户报告中的 BTC 增加 0.001、USDT 减少 60 与此一致，剩余数量为零；没有遗漏这笔现金流或重复扣费的证据。

这四条记录不含 FeesReconciled。`Account::recover` 对费用未闭合的终态订单设置恢复 Unknown，并始终冻结返回账户；运行停止时 `Gateway::stop_sending` 也会冻结账户。因此离线 `unknown=true`、`frozen=true` 不能用来判断成交当时是否发生拒绝。费用闭合需要独立记录，不能由 FILLED 或一笔零手续费自动推导。

run7 未发起订单状态和 myTrades 查询，虽然 fixture 已提供这两个端点。原控制器对健康账户直接返回，成功后还保留上一轮 `orders_loaded`；同一私有连接随后需要再次对账时可能沿用旧空集合。这两项控制器行为的实际单元 Red 和修复见 [费用维护验证](../../reconciliation-maintenance/README.md)。账户的费用释放与恢复规则没有放宽。

镜像 WAL 有 bootstrap 和四个 append 批次，逻辑账户前缀依次为 1、2、3、4。各账户 payload 与源 WAL 逐字节一致，关联输入前缀依次为 5、6、8、9；最后一个覆盖源输入 WAL 的全部九条记录。所有引用输入 payload 的长度、SHA256 和原文均匹配；镜像 31 个 object 文件与源文件匹配。三份 WAL 的 CRC32C、帧 SHA256 链及最后 BatchSeal 均完整。

这些是普通批次的封口。镜像没有 bootstrap/append 之外的关闭记录，本次也没有产生 SourceRecoverySeal 或故障接管资格；不能据此声称旧发送端已被隔离。下一阶段应在真实费用查询及快照对账完成后再观察增长后的账户和输入前缀。
