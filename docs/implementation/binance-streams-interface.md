# Binance 行情与私有事件解码

解码器消费真实 HTTP 响应体或 WebSocket 消息。网络分帧、TLS 和首次输出门禁由 `tickwire-transport` 负责；解码成功只表示报文和绑定有效，不代表盘口已经 Live 或账户已经完成对账。

## 行情

`feed::decode_exchange_info::<N>(market, bytes, max_bytes)` 返回有界元数据，保留未知交易状态和过滤器。价格档距来自 `PRICE_FILTER.tickSize`，数量步长来自 `LOT_SIZE.stepSize`，不使用 `pricePrecision` 或 `quantityPrecision` 代替。`PRICE_FILTER`、`LOT_SIZE`、`MARKET_LOT_SIZE` 的上下限和步长有精确十进制类型；其他过滤器、权限、限流、资产信息与完整 symbol 对象保留为借用的 `RawValue`。目录服务继续核查这些约束，并分配内部资产 ID、标的版本和目录摘要。

`DepthContext` 绑定产品、完整 `InstrumentBinding`、`BookKey`、来源代次和时间单位。`decode_snapshot::<N>` 与 `decode_diff::<N>` 一次返回完整结果；格式、容量或精度错误不交付半条更新。价格和数量按目录网格精确转成 `i64`，拒绝余数、溢出和负数量，增量零数量保留为删除。返回对象可借用为 `tickwire-book` 的输入，序列衔接仍由独立 `SequencePolicy` 判断。

普通深度与 RPI 深度使用不同来源，当前入口只接普通深度。USD-M 的 `st=2` 或未知值被拒绝；旧格式缺少 `st` 时使用已绑定的 USD-M 来源和目录。元数据中的 CM/未知产品仍保留原值，但 `product()` 返回 `None`。该方法只识别产品，不批准交易。

原始流依赖已绑定的订阅上下文；组合流还核对 stream 名称，拒绝 partial depth 和 RPI。REST 快照本身没有 symbol，其 HTTP 请求与响应的关联由调用者维持。返回的 `DepthSnapshot` 与 `DepthUpdate` 是两种类型，分别借用为 `Snapshot` 和 `DepthEvent`。快照零数量、重复价格、错误排序和交叉盘口被拒绝；增量内重复价格保留给 Book 的原子校验。快照的双侧 `complete` 始终为 `false`，外侧继续是 Unknown。

深度固定数组容量 `N` 为 1–5000；输入上限至多 16 MiB。元数据 `N` 为 1–16384，每 symbol 最多 64 个过滤器。价格和数量只接受普通十进制字符串；JSON 数字、指数记法、无法整除网格或超过 `i64` 的值均失败。实现先约去公因数，再补十进制因子，避免把原本可表示的比值变成不必要的中间溢出。字符串转义、目录对象和元数据集合可以分配内存；本接口没有声明整条解码链零分配。

## 私有事件

`events::SessionContext::new` 固定风险域、venue、环境、账户、连接代次、产品和时间单位。Spot 使用 `PrivateStream::SpotSubscription { subscription_id }`，只接收匹配的 `subscriptionId/event` 包装；USD-M 使用 `UsdmListenKey`，接收该连接上的裸事件。Spot 支持毫秒或微秒，USD-M 为毫秒，转纳秒时检查溢出。

`decode_private::<N>(&session, bytes, max_bytes, received_mono_ns)` 返回私有构造、只读的 `PrivateEvent`。每条消息最多 1 MiB，`N` 为 1–4096；账户余额与仓位的条目数合计受 `N` 限制。函数只在全部字段检查完成后交付结果。完整原始事件保留为借用的 `RawValue`；调用者如需跨输入缓冲区寿命保存它，应在释放缓冲区前复制到自己的持久记录。

`bind_order(&message, &original_command, &persisted_identity, &assets)` 关联原始创建命令、已持久化的订单映射和同一版本目录中的额外资产，再构造 `CanonicalEvent`。它核对风险域、venue、环境、账户、产品、symbol、方向、positionSide、原意图/执行代次，以及已有的原生订单 ID。首次绑定普通订单时，报文客户 ID 或撤单报文的原客户 ID 必须匹配原命令；已知原生订单 ID 允许后续修改客户 ID。List 腿还核对本地 List 身份，Algo 子订单要求已登记的原生子订单 ID。

原命令的数量、价格、类型和 TIF 不用于否决后续真实修改后的报告。报告中的数量和价格进入 `OrderObservation`；类型、TIF 和条件字段继续保存在 `OrderUpdate` 及原文。账户 owner 据已持久化的修改流程判断矛盾，不能因报告与旧参数不同就丢弃真实成交。

事件没有可靠的全局序号，`source_sequence` 为 `None`。`event_id` 是原始事件 JSON 的 SHA-256；它标识收到的内容，不代替成交去重。成交 ID 以原生十进制文本保留，消费者结合账户、标的和订单身份去重。连接代次由 SessionContext 传递；当前有效代次由会话 owner 检查。

Spot `Z/Y` 的累计和单次报价金额、手续费资产、USD-M `rp` 的已实现盈亏分别保留。手续费转为带符号的资产现金流：正 commission 变为负数，负 commission 返佣变为正数；PnL 单独绑定目录中的结算资产。费用资产不在目录或金额细于资产单位时，绑定失败。USD-M 缺少报价金额就返回 `None`，不以价格乘数量补造交易所原值。`rp` 可以缺省并保留 `None`，但 USD-M 的 TRADE/CALCULATED 绑定要求存在该字段；缺少 commission 的成交也拒绝绑定。

未知订单状态，以及没有成交数量的未知执行类型，绑定为 `OrderStatus::Unknown`。未知执行类型同时携带成交数量时保留原始事件、拒绝绑定，交给对账处理。找不到本地归属、资产或当前会话的资金记录同样保留，owner 关闭对应准入后补齐关联；纯解码器不直接修改账户门禁。

余额、仓位和配置变更返回独立有界类型。`outboundAccountPosition`、`balanceUpdate`、`externalLockUpdate` 各自保留余额、差额和外部锁定含义。USD-M `ACCOUNT_UPDATE` 中的 `B` 或 `P` 可以省略，表示这一类没有报告更新；未出现的资产或仓位保持未说明。`bc` 保留其“不含 PnL 和 commission 的余额变化”含义。跨仓资金费只含相关余额和 symbol 的消息可以直接解码。

`listStatus` 保留 List 状态和成员原生身份；`ALGO_UPDATE` 保留 Algo 状态、触发结果和实际订单 ID。`ALL_DONE`、`FINISHED` 不生成子订单 Filled，也不合成手续费。`ACCOUNT_CONFIG_UPDATE`、流终止和 listenKey 过期有独立类型；其他事件（包括尚未展开的 `TRADE_LITE`、`MARGIN_CALL`）进入 `Other` 并保留完整原文。

## 来源与接线

Spot 使用官方固定提交中的 [行情流](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/web-socket-streams.md)、[REST](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/rest-api.md) 和 [私有流](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/user-data-stream.md)。USD-M 的字段与稀疏规则来自官方 [行情 REST](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)、[公开流](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public)、[私有流](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/user-data-streams)，嵌套字段再与 [官方 SDK 固定版本](https://github.com/binance/binance-connector-python/tree/a54ec0b312c36706dc1d0b2cb33c658d55b72e89/clients/derivatives_trading_usds_futures) 核对。[字段清单和源哈希](../validation/binance-streams/official-futures-fields.json) 可用同目录脚本重新取得。

本阶段交付可直接消费真实传输报文的解码和身份绑定。自动登录/订阅、listenKey 续期与轮换、重连对账、快照期间的增量缓存及溢出重建、目录过滤器准入、账户日志接线仍由后续连接器 owner 实现。[验证记录](../validation/binance-streams/report.md) 说明单元与实际协议服务器覆盖。
