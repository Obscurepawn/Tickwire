# USD-M 账户交易结果与收入接口

状态：规范输入、资金费及跨来源佣金/盈亏去重、稀疏账户观察、输入 WAL、真实 `IncomeReader` 和专用审批基线导入已经实现。app 已接初始收入扫描和显式审批文件；强平/外部成交入账、交割闭合和新观察/资金费的对账解除尚待完成。实际完成边界见 [实现规格](../../specs/implementation/usdm-account-facts.md)。下文保留完整目标，尚未实现的接口草案不作为现有 API。依据为 [risk-calculations.md](../design/risk-calculations.md) §3.5、§5–7。

## 官方协议与身份

`ACCOUNT_UPDATE` 只报告变化项，`B`、`P` 可分别缺失；全仓资金费可以没有 `P`。`bc` 排除已实现盈亏与佣金。`E/T` 表达时间及顺序，没有被文档定义为跨重连唯一账务 ID。故余额、仓位更新保存为观察，不按余额差推断资金费，也不直接重复累计 `bc`。[官方私有流说明](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/user-data-streams)

`GET /fapi/v1/income` 返回带符号的 `income`、`asset`、`incomeType`、`tranId`，以及可空的 `symbol/tradeId`。文档限定同一用户、同一收入类型内交易 ID 唯一；文档说明处拼写为 `trandId`，响应字段为 `tranId`。当前查询参数有包含端点的时间区间和最多 1000 条限制，无 `fromId/page`；保留期为三个月。[官方收入接口](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/account)

强平和自动减仓可由私有回报的明确标识以及 `/fapi/v1/forceOrders` 核对；后者返回订单累计结果，逐笔数量、盈亏、佣金仍由成交记录提供。`userTrades` 返回原生订单和成交 ID、原始数量、报价金额、佣金币种及已实现盈亏。[官方交易接口](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade)

交割价接口提供 `deliveryTime/deliveryPrice`，作为结算核对参考；USDT 交割合约按线性方式结算，交割费用适用 taker 费率。实际账本仍取交易所收入与成交记录，不从参考价再生成一笔现金流。[交割价接口](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)、[USDT 季度合约规则](https://www.binance.com/en-AE/support/faq/detail/3ae441db4ae740e19af3fe9228eb6619)

## 规范记录

在 `tickwire-venue` 增加独立、封闭构造的 `CanonicalAccountFact` 和 `CanonicalAccountObservation`。二者具有有界规范编码、`bytes()/digest()/decode()`；不要求本地 intent、订单 ID 或单一 instrument。账户事实的市场域为 `UsdM`，合约引用保留 `LinearPerpetual/LinearFuture` 和目录版本，避免把同一 USD-M 钱包拆成两份收入账。

拟定的主要类型如下；本地接收信息由外层持久输入保存，不进入交易所去重键。

```rust
struct AccountScopeRef {
    risk_domain_id: [u8; 16],
    venue: VenueId,
    environment: Environment,
    account: AccountId,
    market: AccountMarket, // 当前支持 UsdM
}

struct IncomeKey {
    scope: AccountScopeRef,
    income_type: BoundedText<64>, // 原始类型，不把陌生类型改名
    transaction_id: BoundedText<32>, // 原始整数的规范十进制表示
}

struct NativeTradeKey {
    scope: AccountScopeRef,
    symbol: Symbol,
    order_id: VenueOrderId,
    trade_id: BoundedText<64>,
}

enum AccountFactBody {
    Income(IncomeRecord),
    ExternalTrade(ExternalTradeRecord),
}

struct IncomeRecord {
    key: IncomeKey,
    amount: AssetDelta, // 收入直接保留正负号；佣金支出转换仅在适配器进行
    symbol: Option<Symbol>,
    trade_id: Option<BoundedText<64>>,
    venue_time_ms: u64,
    info: BoundedText<256>,
}

struct ExternalTradeRecord {
    key: NativeTradeKey,
    instrument: InstrumentRef,
    side: Side,
    position_side: PositionSide,
    quantity: Decimal,
    price: Decimal,
    quote: Option<Decimal>,
    realized_pnl: AssetDelta,
    fees: BoundedList<AssetDelta, 8>,
    origin: ExternalOrigin, // 未登记交易 / 强平 / ADL / 已证实交割
}
```

`IncomeKey` 不含资产、金额、时间、symbol、writer epoch 或连接代次。同一键更换这些内容是冲突，而不是新收入。经济内容摘要包含原始类型、身份、资产版本和精确金额；本地接收时间、HTTP 请求代次、input 位置另有来源摘要。目录更新不会为同一交易所 ID 创造第二次入账资格，关联表按原生身份查重，再核对冻结的资产单位。

`AccountObservation` 保存原始原因、可选资金费 symbol、全部已报告的钱包项和仓位项、`E/T`。余额与仓位项分别有资产/合约上限，缺项表示未报告。未知资产、模式、类型或超限输入保存原文并冻结。金融值沿用精确 `Decimal → Fixed` 转换；资产单位转换有余数时返回明确错误。

各集合受部署上限约束：每个观察最多 `MAX_ASSETS` 个资产、`2 * MAX_INSTRUMENTS` 个仓位；交易所去重及关联项纳入 `MAX_FACTS` 总预算，不在运行中淘汰。单个规范记录保持账户 WAL 的 60 KiB 上限。原始响应单独落输入 WAL，保留现有有界网络读取；解析器在分配前检查数量和长度。

## 钱包、仓位与跨来源去重

收入接口、WebSocket 和成交历史会报告同一笔佣金或盈亏。新增账户级原生交易索引，把本地订单成交与外部成交的数量、盈亏、各币种费用组件统一登记。现有 `OrderKey + trade_id` 继续用于订单归属，但不再单独承担跨接口去重。

| 输入 | 处理 |
|---|---|
| 资金费及已识别、独立的划转/调整收入 | 按 `IncomeKey` 一次入账；未归属部分记 owner 0，扣款立即降低资本，未闭合正金额不增加可用额度 |
| `REALIZED_PNL/COMMISSION` 收入 | 先通过 symbol、tradeId 与真实成交查询建立唯一关联，再核对资产和精确金额，作为同一组件的第二个来源 |
| 收入先到、关联未建立 | 保存待关联收入并冻结；负金额建立保守扣款占用，正金额不可用。关联成立后原子地把占用换成唯一现金流或已有组件，不再扣第二次 |
| 已登记订单的成交 | 仍按原命令、原意图代次及原生订单身份绑定；同时登记组件，供收入关联核对 |
| 强平、ADL 或未登记订单的真实成交 | 无本地 intent 也可记真实数量、盈亏和费用；缺省归属 owner 0，冻结共享增险并检查减仓分配是否超过实际仓位 |
| 相同交易所键、相同内容 | 资金与仓位不变；新来源证据可持久保存 |
| 相同键、内容冲突或关联不唯一 | 冻结，保留冲突原文；不覆盖旧账，也不把冲突金额当成第二笔收入 |

关联由适配器根据持久的原文和成交查询建立，返回私有构造的关联结果。账户 reducer 再核对两个组件的全部绑定；公开接口只接收该能力与其持久来源引用。REST 与私有回报缺字段的差异按组件合并，例如 WS 没有 quote 时保留未知，后到 REST 原值可补充；同一个已报告字段发生变化仍为冲突。

外部订单的 client ID 前缀只用于启动对应查询；强平/ADL 分类再与 `forceOrders` 绑定，其他未登记成交归为未解释外部交易。陌生 incomeType 进入待解释集合，负额形成保守占用、正额不可用，保留原始类型供适配器升级后重放。

对无唯一 ID 的账户观察，采用独立的保守约束：已报告钱包低于本地总额时记录绝对钱包上限，随后按两者较小值计算资本；重复观察不会再次扣款。此上限与尚未关联收入的负额占用取保守组合，允许暂时多占、不增加可用额度。实际收入识别后只消除精确关联的临时占用；绝对钱包上限由完整对账解除。仓位差异作为未解释责任保留，冻结并查询成交，不先覆盖仓位再重复应用迟到成交。

资金费预留继续按原风险公式保留。只有结算时点与对应预留期间的关联成立，才消耗该次缓冲；长寿命订单未来期间的预留继续存在。第一步将缺少历史策略持仓覆盖的资金费全额记入 `unattributed_funding`；已有完整覆盖时，再按风险契约的向零舍入和有界残差规则转归属，转归属不改变总钱包。

交割进入 `AwaitingSettlement` 后保留未解决订单与费用责任。原生成交负责仓位变化，实际收入负责未在成交中覆盖的现金流；终态累计数量、全部成交、费用、钱包和实际仓位共同闭合后才释放。没有逐笔明细或可证明的结算关联时维持等待，不按计时器、公开交割价或 `position=0` 生成虚拟成交。

## 输入覆盖、持久顺序与重新激活

首次历史查询先处理基线覆盖问题。已实现的 `IncomeBaseline` 保存基线包含的收入键及完整内容、截止、原文输入位置与基线摘要。这些行标记为 `Baseline`，后续重放不再加钱。普通 REST 未提供跨 API 原子收入截止，覆盖断言采用[专用 risk-admin 基线导入](income-baseline-import-interface.md)：真实查询清单与明确签名审批缺一不可。未批准的历史收入按未知记录处理，负额占用、正额不可用。

`IncomeReader` 使用有界时间区间、队列和共享额度，响应达到 limit 时继续切分区间；同一毫秒仍饱和、超出保留期、查询失败或未知类型无法解释余额时，返回不完整覆盖。每次查询保留端点与返回键集合，跨查询重叠由交易所键去重。短页只说明该次请求未饱和，解除冻结还需要账户/仓位/普通及条件订单核对、当前私有 gap 和连续已应用输入前缀一致。

持久调用顺序沿用现有单所有者模式：

1. 原始私有报文或签名 REST 响应与规范化决定先同步到输入 WAL。
2. `AccountGateway::record_account_input(InputKey, CanonicalAccountInput)` 在候选账户上核验并计算，再同步新的账户 `RecoveryRecord::IngressAccountInput`，成功后发布。
3. 精确输入重试返回旧结果；同一输入键内容不同则停止。未识别原文有持久隔离决定，恢复不会跳过它。
4. 恢复先验证原始输入与规范记录绑定，再按账户 WAL 顺序重放组件、去重表、临时占用及观察；默认仍冻结。

`CanonicalAccountInput` 包含事实、观察和已验证关联决定三个有界分支。账户 codec 增加新标签，保留旧标签含义；新收入覆盖信息也进入恢复记录。完整重复来源可以增加来源历史，但资金与仓位不变。ledger_sequence 的变化仅取决于持久输入与持久状态，本地恢复冻结标记不参与去重。

重新激活证据扩展为版本化的账户查询覆盖对象，绑定基线、前序账户历史、收入覆盖、未关联记录集合为空、私有连接/gap、输入 `through_sequence`、实际账户/普通单/条件单查询。沿用私有构造的 `ActivationProof`，收入缺口关闭后才准许激活。复制的 `TWIPFX01` 包含所有相关原文及查询；新恢复标签、覆盖对象及递归依赖同时接入 source、mirror 和离线恢复验证。

网络接线沿用 `ReadOnlyTradeReader` 的所有权和轮询接口：`IncomeReader::new(config)`、`start(owner, query, credentials, clock, permit)`、`poll(owner, clock, permit, budget)`、`register/reregister/cancel`。`IncomeQuery` 显式绑定账户、市场、起止时间、limit、当前私有连接与 gap；`IncomePage` 只在原文和所有规范决定同步后返回输入前缀、返回键集合及后续查询工作。首次接受前复验原签名时间与共享额度；连接、收入页、强平查询各有明确费用动作。

## 实现顺序与首批行为用例

先完成规范类型、身份和有界 codec，再接账户 reducer、输入 WAL 与真实 IncomeReader，最后扩展对账控制器及激活覆盖。预留、正常下单和原有 Spot 记录保持原编码。需要协调的新增接线是 gateway/journal 新记录入口、quota 的收入/强平查询动作、app 对账调度和 replica 恢复标签，实施前通知相应文件所有者。

完整目标的用例包括：同 ID 跨重连重复、同 ID 改金额冲突、不同收入类型/相同时间的独立键、基线覆盖、资金费负额与未归属正额、收入和成交双向到达、强平无本地订单仍减仓、稀疏观察重复、交割缺明细、持久输入重放及饱和查询。已运行的具体测试和命令以[验证记录](../validation/usdm-account-facts/README.md)为准。
