# Binance Spot 列表、挂钩价格和 SOR 请求

`tickwire-binance::request::prepare` 将经过 `CanonicalCommand` 校验的命令转换为有界、不可变的 REST 或 WebSocket 请求。账户、环境、产品、签名密钥类型、字节预算和命令摘要继续沿用[原请求接口](binance-interface.md)。

## 命令与历史编码

旧 `OrderSpec`、`ListOrder` 及 schema 1 的命令标签 1–10 保持原编码。新增标签 11：`CommandBody::SpotAdvanced(SpotAdvancedCommand)`。旧解码器对标签 11 返回 `Error::Enum`；部署双方须先确认能力，再接受此命令。

```rust
pub enum PegPriceType { Primary, Market }
pub struct Peg { pub price_type: PegPriceType, pub offset_levels: Option<u16> }
pub struct SpotOrder { pub order: OrderSpec, pub peg: Option<Peg> }
pub struct SpotAdvancedList {
    pub list_id: [u8; 16], pub client_id: ClientId, pub kind: ListKind,
    pub legs: BoundedList<SpotOrder, 3>,
}
pub enum SpotAdvancedCommand {
    New(SpotOrder),
    CancelReplace { target: OrderTarget, replacement: SpotOrder, mode: CancelReplaceMode },
    List(SpotAdvancedList),
    SorNew(OrderSpec),
}
```

新标签内的子标签依次为 1–4。除 SOR 外，新命令至少有一条腿携带 peg；普通订单继续使用旧标签，避免同一含义出现两个编码。`Peg` 编码为 `[price_type, offset_levels]`，类型值为 1/2；其余结构按上述字段顺序编码为定长 CBOR 数组。可选值延续既有空数组或单元素数组编码。

## 请求映射

| 命令 | REST POST | WebSocket 方法 |
|---|---|---|
| OCO | `/api/v3/orderList/oco` | `orderList.place.oco` |
| OTO | `/api/v3/orderList/oto` | `orderList.place.oto` |
| OTOCO | `/api/v3/orderList/otoco` | `orderList.place.otoco` |
| 挂钩新订单 | `/api/v3/order` | `order.place` |
| 挂钩撤换 | `/api/v3/order/cancelReplace` | `order.cancelReplace` |
| SOR | `/api/v3/sor/order` | `sor.order.place` |

列表腿顺序固定为 OCO `[above, below]`、OTO `[working, pending]`、OTOCO `[working, pendingAbove, pendingBelow]`。OCO 两腿及 OTOCO 两条 pending 腿共享方向和 base quantity，存在差异即拒绝；所有腿的自成交处理模式也须相同，因为请求只提供一个公共字段。OTO/OTOCO 的 working 腿只允许 LIMIT 或 LIMIT_MAKER。OCO 配对由止损腿与止盈或 maker 腿组成，below 不能使用 maker。OTO pending 不支持 quote quantity。每条腿的价格、触发条件、追踪距离、冰山数量和客户订单标识均按官方前缀输出。

Peg 仅用于 LIMIT、LIMIT_MAKER、STOP_LOSS_LIMIT、TAKE_PROFIT_LIMIT。指定 peg 后可以省略价格；显式价格仍保留。PRIMARY 的普通 LIMIT 只允许 GTC，maker 只允许 PRIMARY。偏移是可选的 0–100 档，出现时同时输出 `PRICE_LEVEL` 和档数。SOR 只允许 base quantity 的 LIMIT/MARKET；不能叠加 peg、条件订单或 maker。符号是否开通 peg/SOR，以及根据实时价格执行的过滤条件，由调用方的最新准入信息校验。

## 完成边界

此接口完成确定性编码和签名，不推进列表腿状态，也不将未触发订单视为已成交。真实发送继续经过 `JournaledSender` 的持久化与首次输出门禁；账户 reducer 尚未实现的新命令风险界返回 Unsupported。现有 gateway 模型验证该发送顺序，不把本次字段映射和交易所行为算作已证明内容。

OPO/OPOCO 属于后续能力，本接口当前只接受上述三种列表结构。

## 官方依据

固定版本为 Binance Spot 文档提交 `f4a9c0be68235f758b46f571f7313c63d1306b25`。端点与字段取自 [REST](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/rest-api.md#new-order-list---oco-trade)、[WebSocket](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/web-socket-api.md#order-lists)；组合约束见[挂钩订单](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/faqs/pegged_orders.md)、[SOR](https://github.com/binance/binance-spot-api-docs/blob/f4a9c0be68235f758b46f571f7313c63d1306b25/faqs/sor_faq.md)。
