# Binance 行情与私有事件验证记录

本阶段实现有界行情/元数据 JSON 解码、私有订单与稀疏账户事件解码，以及原始命令到规范订单事件的显式关联。可调用接口和剩余接线见 [接口文档](../../implementation/binance-streams-interface.md)，状态契约见 [实现规格](../../../specs/implementation/binance-streams.md)。

## 失败与修复

初始 Red 都是可编译代码的断言失败，保留了失败时的模块源文件。

| 日志 | 实际结果 | 对应实现 |
|---|---:|---|
| [depth-red.log](depth-red.log) | 2 项失败 | Spot/USD-M 快照与深度更新 |
| [metadata-red.log](metadata-red.log) | 1 项失败 | 元数据网格及未知字段保留 |
| [private-red.log](private-red.log) | 2 项失败 | Spot executionReport / USD-M ORDER_TRADE_UPDATE |
| [binding-red.log](binding-red.log) | 1 项失败 | 原始命令、订单映射和资产目录关联 |
| [sparse-private-red.log](sparse-private-red.log) | 1 项失败 | 缺省 B/P 的稀疏账户更新 |
| [pnl-private-red.log](pnl-private-red.log) | 1 项失败 | 缺省 rp 的原始消息保留与成交绑定规则 |

独立审阅发现 USD-M 跨仓资金费只报告相关余额、没有 `P` 的合法情况。初版测试只包含 `P: []`，把 `B/P` 都设为必填，真实断言复现了拒绝。修复后每个缺省集合表示“这类没有报告更新”，两类条目总数仍受调用者容量限制。PnL 的 None 保留规则另有单独失败测试；绑定 USD-M 成交仍拒绝缺少 PnL 的记录。

## 实际覆盖

新增测试共 33 项：

- `feed` 11 项：精确整数与网格、容量、重复字段、组合流、Spot/USD-M 序列输入、CM 识别、Unknown 保留。
- `private_events` 18 项：订阅/环境/账户/原意图与原生 ID 绑定，原始金额、费用币种、返佣符号、资产单位、状态未知、稀疏更新、List/Algo、终止消息。
- `market_server` 2 项：真实 HTTPS 分块快照和 WSS 分片增量，经相同解码器送入真实 Book。
- `private_server` 2 项：真实 WSS 分片订单消息，经相同解码器与原命令/映射送入 CanonicalEvent。

行情服务器核对实际 HTTP 路径、WS 握手和客户端掩码帧。私有服务器从已认证 Spot 会话/已持有 listenKey 的边界开始；此组不实现登录和续期。私有升级处理保留 HTTP decoder 中已读到的 WS 字节，完整消息交付前没有订单事件。身份、时钟、输入数据均为测试配置，网络分帧、TLS 与解码实现未被 Mock 替换。

最终日志、命令和计数见 [results.json](results.json)。[SOURCE.sha256](SOURCE.sha256) 固定本阶段源码、测试及两个最小接入文件；[SHA256SUMS](SHA256SUMS) 覆盖证据文件。没有给出性能或压力结论。

## 官方来源与复查

[capture-sources.py](capture-sources.py) 下载固定官方输入并保存文件哈希，不归档整站。

- Spot 使用 `binance/binance-spot-api-docs` 的 `f4a9c0be68235f758b46f571f7313c63d1306b25`；三个相关文件及 SHA-256 见 [official-spot-sources.json](official-spot-sources.json)。
- USD-M 当前官方 [私有流页面](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/user-data-streams) 说明稀疏余额/持仓、资金费与 Algo 语义；[行情 REST](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data) 与 [公开流](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public) 规定元数据、深度字段与 `st`。
- Futures 页面中的嵌套 schema 未在读取结果中展开，因此字段名和类型另与官方 `binance/binance-connector-python` 的 `a54ec0b312c36706dc1d0b2cb33c658d55b72e89` 核对；[official-futures-fields.json](official-futures-fields.json) 保存 8 个模型的字段和完整源文件哈希。下载为 HTTP 202 空内容的网页没有冒充已取得的源文件。

独立审阅者再次下载了这 8 个官方文件，哈希全部一致。具体源代码复核结论记录在 [review.md](review.md)。
