# Binance 成交、最优报价与参考价格接口

本阶段拟补齐 Trade、独立 bookTicker、USD-M markPrice/funding 的真实 WebSocket owner，并接入 MD 发布、JOIN 快照和策略视图。优先完成行情输入；交易 WS 继续复用现有请求编码，随后补请求关联、响应持久化与发送状态管理。本文是待审接口，以下新增 API 尚未实现。

## 官方协议与现有接缝

| 输入 | 路由与编码 | 保留的业务信息 |
|---|---|---|
| Spot Trade | `stream.binance.com/ws/<symbol>@trade`，JSON；或 SBE endpoint 的同名流 | 原成交 ID、价格/数量、买方是否 maker、事件时间和成交时间 |
| Spot 最优报价 | JSON `@bookTicker`；SBE `@bestBidAsk` | 原 book update ID、两侧价格/数量。JSON 无交易所时间；SBE 有事件时间并允许主动丢弃过时更新 |
| USD-M 成交 | `fstream.binance.com/market/ws/<symbol>@aggTrade` | 聚合 ID、原成交 ID 范围、总数量，以及可选的非 RPI 数量；明确标为聚合成交 |
| USD-M 最优报价 | `fstream.binance.com/public/ws/<symbol>@bookTicker` | update ID、两侧价格/数量、事件时间和交易时间 |
| USD-M 参考价格 | `/market/ws/<symbol>@markPrice` 或 `@markPrice@1s` | 精确 mark/index price、预计结算价、可选移动均价、资金费率和下次资金费时间 |

Spot 固定来源为官方仓库 commit `f4a9c0be68235f758b46f571f7313c63d1306b25` 的 [JSON 行情](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/web-socket-streams.md)及 [SBE 行情](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/sbe-market-data-streams.md)。SBE 继续使用已经固定的 schema 1/version 0，Trade/BBA 分别为模板 10000/10001；接入密钥只放 Upgrade header，不发送签名或时间戳。

USD-M 以官方 [Market API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market)、[Public API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public)和[路由迁移说明](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice)为依据。实现时保存使用的字段样本及来源内容摘要。报文若含 `st`，USD-M 只接受 1，拒绝把 CM 数据混入同名标的；`nq` 与 `q` 分别保存。资金费数据是公开参考值，账户实际资金费仍由私有事实/对账入账。

现有 [PublicDepthClient](../../crates/tickwire-binance-public/src/client.rs)负责深度连接和快照，SBE codec 已能读取成交与 BBA 模板，但尚无对应 owner。另有三个实际接缝：

- [TopicPublisher](../../crates/tickwire-market-publisher/src/lib.rs)每次深度更新替换 `states=[Quote,FeedState]`，需要改为按事件类别保存最新状态。
- [MarketReplica](../../crates/tickwire-market-source/src/replica.rs)目前对 258/260 不更新视图，需要加载快照中的 Trade/MarkFunding 并应用增量。
- [MD 注册](../../crates/tickwire-md/src/engine.rs)固定 `required_states=QUOTE`，需要由明确的 topic 能力集合决定是否同时要求成交或参考价格。

## 业务类型和网络 owner

在 `tickwire-binance::market` 新增纯解码层，身份由可信目录与已登记流配置提供，不从收到的 symbol 自建目录。所有价格/数量转换使用检查溢出的整数；Trade/BBA 必须是目录 tick/step 的整倍数，参考价格保留十进制精确值。

```rust
pub enum EventKind { Trade, BestBidAsk, MarkFunding }
pub enum EventEncoding { Json, SpotSbeV1 }
pub struct StreamIdentity {
    pub source_id: u64,
    pub connection_epoch: u64,
    pub instrument_id: u64,
    pub instrument_version: u64,
    pub catalog_version: u64,
    pub catalog_hash: [u8; 32],
}
pub struct EventTimes {
    pub received_mono_ns: u64,
    pub exchange_event_ns: Option<u64>,
    pub transaction_ns: Option<u64>,
}
pub struct Trade {
    pub trade_id: u64,
    pub raw_trade_range: Option<(u64, u64)>,
    pub price_ticks: i64,
    pub quantity_lots: i64,
    pub normal_quantity_lots: Option<i64>,
    pub buyer_is_maker: bool,
    pub aggregate: bool,
}
pub struct BestBidAsk {
    pub update_id: u64,
    pub bid_price_ticks: i64, pub bid_quantity_lots: i64,
    pub ask_price_ticks: i64, pub ask_quantity_lots: i64,
}
pub struct MarkFunding {
    pub mark_price: Decimal, pub index_price: Decimal,
    pub estimated_settlement_price: Option<Decimal>,
    pub moving_average: Option<Decimal>,
    pub funding_rate: Option<Decimal>,
    pub next_funding_utc_ns: Option<u64>,
}
```

`Decimal` 复用 venue 的受校验整数尾数/指数类型。空资金费字段与数值零分开表示，交割合约不会自动获得永续资金费字段。JSON `E/T` 按明确的毫秒/微秒配置检查乘法；Spot bookTicker 保留 `None`，绝不以接收时间填充交易所时间。SBE 成交整帧先完整校验，再逐行交付，超出单次回调预算时保存帧内游标，禁止处理半帧后丢掉剩余成交。

在 `tickwire-binance-public` 新增 `PublicEventClient<C>`，使用独立 `EventConfig` 与单连接 `EventNetworkConfig`，保留现有深度构造接口。一个 owner 固定一个产品、标的和事件类别；构造器校验产品/编码/流名组合，TLS 名称由产品和环境选择。

```rust
pub fn new(config: EventConfig, network: EventNetworkConfig, clock: C)
    -> Result<PublicEventClient<C>>;
pub fn new_sbe(config: EventConfig, network: EventNetworkConfig,
               clock: C, access: SbeAccess) -> Result<PublicEventClient<C>>;
// 与现有 owner 一致：显式 start/poll/register/reregister/stop。
pub fn poll(&mut self, budget: PollBudget,
            permit: &mut impl PublicPermit, sink: &mut impl EventSink)
    -> Result<PollReport>;
```

配置包括固定身份、首次 connection/operation 计数、订阅方式、消息/批次容量和时间预算。`EventNotice` 交付已验证的类型、身份、时间以及仅回调期间有效的原始 Text/Binary；`Gap` 带精确类别和旧代次。连接、Upgrade、Subscribe、Pong 都沿现有共享额度接口逐次检查，零接受重试仍重新授权。不给外部暴露可绕过 owner 的传输 writer。

订阅 ACK 只改变订阅状态。首条合法数据才建立该类别的可用状态，旧连接数据不可推进新代次。成交和 BBA 可以合法静默：连接心跳与业务数据年龄独立记录，策略按自身最大年龄判断；markPrice 按配置的 1 秒/3 秒周期监视缺失。EOF/轮换先撤销当前可用状态，再排空旧连接已完整收到的记录；后者保留旧身份，不能恢复新连接健康。每次 poll 同时约束 I/O、帧解析、SBE 成交行数和通知数。

## 发布合同与兼容方案

现 ABI 的价格单位由 InstrumentCatalog 的订单 tick 决定。官方参考价格样本中的指数 `11784.62659091` 无法在一般订单 tick 上精确表示；直接舍入会改变风险输入。旧 Trade 还缺少聚合原成交范围，旧 Quote 的字段白名单不允许保存原 update ID。[现 ABI 定义](../design/ipc-abi.md)保持原义和原字节。

建议追加三种明确的新消息：`TradeExact`、`QuoteExact`、`MarkFundingExact`，仍为 128 字节公共头加 384 字节固定消息体。新消息保存上述业务字段、source/connection 身份和字段有效位；十进制按规范化 `i128 mantissa + i8 exponent` 编码，剩余字节全零。为新消息分配一个必需协商 feature；旧 reader 在资源/HELLO 阶段拒绝，避免读到一半才误解。消息号建议追加 264/265/266，必需 feature 建议使用当前未占用的 bit 9；这些编号在 contract 写入窗口确认后分配，避开正在新增的控制 opcode 38–40。旧消息 257–263、旧 codec 样本和旧快照解释全部保留。

拟新增共用的 `MarketExactBody`，以下偏移从消息体起点计算，小端编码。每种消息有独立字段白名单；不适用字段和值无效的字段全零。现 MarketBody 的 192 字节内联深度区已是有定义字段，旧消息不能直接占用它来偷换含义。新消息复用 512 字节槽位容量，采用独立布局即可容纳完整字段，无需改变 arena 或 record 大小。

| 偏移 | 字节 | 字段 |
|---:|---:|---|
| 0、8 | 各 8 | `source_id`、`source_connection_epoch` |
| 16、20、22 | 4、2、2 | `source_schema_id`、`source_encoding`、`source_schema_version` |
| 24 | 8 | `value_validity` |
| 32、40 | 各 8 | `exchange_event_ns`、`transaction_time_ns` |
| 48、56 | 各 8 | `venue_seq_first`、`venue_seq_last`：聚合成交原始 ID 范围；报价两项均为原 update ID |
| 64 | 8 | `trade_id`：原成交 ID 或原聚合 ID，由 Aggregate 标志区分 |
| 72、80、88 | 各 8 | `price_ticks`、`quantity_lots`、`normal_quantity_lots` |
| 96、104、112、120 | 各 8 | bid price/quantity、ask price/quantity |
| 128、152、176、200、224 | 各 24 | mark、index、预计结算、移动均价、funding rate 的精确十进制 |
| 248 | 8 | `next_funding_utc_ns` |
| 256、258、260、261 | 2、2、1、1 | `feed_state`、`reason`、主动成交 `side`、`trade_flags` |
| 262 | 122 | 保留，全部零 |

每个十进制占 24 字节：有符号 i128 尾数、i8 指数、7 字节零填充，值为 `mantissa × 10^exponent`；指数范围沿 venue Decimal 的 −38…38，零规范为 `(0,0)`，可继续约去的十进制尾零拒绝。价格用报价资产单位；资金费率无量纲。因此 SDK 读取 mark/index 时不再乘 instrument trade tick。

有效位 0/1 为事件/交易时间，2 为非 RPI 数量，3/4 为预计结算/移动均价，5/6 为资金费率/下次资金费时间，7 为聚合原始成交范围。Trade 的价格/数量和 ID、Quote 的原 update ID/两侧价格数量、MarkFunding 的 mark/index 为各自必需字段。Aggregate 成交必须同时带有效范围；Spot JSON Quote 的两个时间有效位为零。旧 `MARK_FUNDING` 注册能力可由新精确消息满足，但同一标的快照只选旧/新中的一种；新消息的快照校验及 reader 同样受新 feature 约束。

发布器按类别保存源代次、最新状态与观测时间。深度生成的 Quote 和独立 BBA 不竞争同一个字段：topic 配置显式选择 `quote_source=Depth|BestBidAsk`；深度层及其 coverage 始终来自 DepthSync。BBA 只提供最优报价，不生成虚构的深度连续性或覆盖区间。Trade 基线保存最后一笔，逐笔回放仍使用策略实际消费日志。

JOIN 对一个 topic 原子捕获深度、成交、参考价、各状态时间和共同发布序号。SDK `MarketView` 新增 `last_trade()`、`best_quote()`、`mark_funding()` 与按类别查询年龄/代次的只读入口；加载完整快照后再应用 S+1。断流使相应必需状态失效，FeedState 表示 topic 已配置必需状态的合取。热路径不新增同步原始网络日志；策略输入 WAL 保存实际消费的新规范记录，能够完整重放精度、来源和顺序。

## 实施范围与行为测试

先实现独立解码器和 event owner，再完成 IPC 扩展、Publisher、快照、MarketReplica 与 MD 配置接线。既有 Feed 配置默认只启用 Depth；新增事件流使用显式配置，不根据 symbol 或收到的帧自动订阅。最终以真实 TLS/WSS→typed event→发布槽位→策略视图验证完整字段，不只测 parser。

首批 Red 用例覆盖：SBE 多成交行预算续读、未来模板/截断拒绝；JSON 重复键与错误 symbol/product 拒绝；Spot BBA 无源时间；聚合 ID 与原成交范围分别保留；比订单 tick 更细的 mark/index 精确往返；负费率与无资金费字段；零接受多次 permit、错误 ACK/旧代次/ACK 与 EOF 同批；深度更新不抹掉 Trade/Mark；JOIN 完整状态恢复；Quote 源互斥及 BBA 不扩大深度 coverage。

交易 WS 的下一接口复用 `request::prepare(Transport::WebSocket)`，另建单连接请求 owner：request ID 精确关联已持久 Admission、首次 TLS 接受沿 fresh gate、ACK/RESULT/FULL 原文进入同一 durable inbox、部分接受或失联保持 Unknown、重连只查询不自动重发。该项涉及 account-gateway/账户响应协调，独立于本阶段行情状态；现有编码支持不计作会话已实现。
