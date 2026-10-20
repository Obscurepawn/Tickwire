# Binance 请求额度适配器

`tickwire-binance-quota::Endpoint` 独占一个 `ExecLimiter`，时间查询、私有控制、公共行情和交易命令顺序借用同一个 Endpoint。各网络所有者不再分别领取完整共享限额。模块不修改网络核心，也不提供默认放行实现。

构造输入包括 supervisor 唯一分配的 producer ID、单调域，以及具有代次和有效期的不可变 Policy。规则精确匹配产品、环境和动作；每条规则列出完整、多桶、已排序的费用表及类别。网络 Charge 的 `api_weight` 和 WebSocket 消息数必须与规则一致。普通新单、撤单、查询、改单、普通列表、算法单、peg 与 SOR 使用不同动作；列表动作还包含种类和腿数。未配置的组合直接拒绝。

Policy 不含默认交易所限额，也不在发送时查询数据库。共享桶、REST/WS 重叠和保留类别由可信配置提供。Policy 构造后不可修改；换配置须建立新消费者代次和新的 Endpoint，不能清空旧尝试记录后继续使用原授权。

三种短借用对象分别实现 `TimePermit`、`ControlPermit` 和 `PublicPermit`。Time 绑定唯一 owner、Source 和 ClockDomain；Private 额外绑定完整 AccountScope；Public 核对 Charge 中的 owner、Source 和域。命令入口核对 CanonicalCommand 的风险域、交易所、账户、产品和环境，并绑定 REST/WS 通道。`consume_command` 在网关首次接受字节之前的当前门禁内调用，不在准备命令时提前缓存许可。

去重保留完整请求身份及确切内容摘要。时间连接与查询区分 kind；私有请求保留连接代次和 attempt；公共请求保留 lane、连接代次、operation 与 kind；命令保留原意图代次、ID 和通道。Endpoint 将这些身份映射成唯一递增的 `AttemptId`，同一请求精确重试重新执行本地期限和冷却检查。同一身份的内容或费用变化冻结额度消费者。

最多保存4096个完整身份，最多256条规则，每条规则最多16个桶。达到上限拒绝，不淘汰去重记录。producer/owner ID 没有默认值，其唯一性由 supervisor 分配与控制连接身份验证保证。

公共 HTTP 反馈中的429/418立即冻结共享 Endpoint。该回调没有当前 MonoTime 参数，适配器不会用上次许可时间推算 retryAfter；有当前可信时间和保守截止的调用方可以显式设置 cooldown。解除永久冻结需要新的消费者生命周期。其他权威计数的解析、全局传播和窗口更新由控制面继续接线。

对账查询通过 `Endpoint::read(ReadBinding)` 借用同一个额度 owner，同时实现 ReadPermit、SnapshotPermit 和 OrderPermit。规则区分 Connect、TradePage、OrderQuery、AccountSnapshot、OpenOrders 和 ConditionalOrders；首接受回调没有独立 wire weight，规则中的 api_weight/messages 配为0，实际扣除量来自显式 costs。去重绑定完整账户与 credential 代次、唯一 reader owner、请求连接代次、原查询内容及连接/请求阶段；同一 pending 重试幂等，新连接重新扣除。

`bootstrap::Config::build(store,now)` 从命名窗口、发放记录和费用规则建立 coordinator 与 Endpoint。调用方显式提供权威 QuotaStore，同一 namespace 的重启不能换成空存储。JSON 没有隐含限额、默认授予或丢失规则时放行。
