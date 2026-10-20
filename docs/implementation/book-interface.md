# 有界订单簿接口设计

`tickwire-book` 将标准化价位更新写入单所有者订单簿。`Book<P, LEVELS, MAX_UPDATES>` 的存储和有效状态与交易所无关；`SequencePolicy` 决定快照桥接、旧事件和连续性。`BinanceSpot` 与 `BinanceUsdM` 是两种策略。网络、JSON/SBE 解码、增量缓存和跨进程发布由调用方提供。

## 数据与完成边界

`BookKey { venue, instrument, source }` 标识唯一行情域；`generation` 标识一次连续性建立过程。价格和数量分别使用 `PriceTicks(i64)`、`QtyLots(i64)`；调用方按照已验证的 instrument 元数据转换，价格严格为正、数量非负，数量零表示删除。

输入是借用的 `Snapshot` 或 `DepthEvent`，bids/asks 使用 `&[Level]`。数组每边最多 `LEVELS` 个存活价位；每个增量最多 `MAX_UPDATES` 个更新，双边合计。所有更新在提交前完成数值、重复价格、最终容量、最终最优买卖价和序号验证。买方更新成功而卖方失败时，买卖两边都保持原值，簿进入 `Invalid`。同一事件同侧重复价格被识别为无效输入；适配器若接收到有重复价格的协议，应先按该协议确定最终值。

每边采用连续定长数组，买价降序、卖价升序。更新先删除再插入，允许满容量批次先新增、后删除在最终容量内成功。没有扩容、哈希退化或价格窗口外静默丢弃；超出容量返回明确错误，停止对外提供有效 view。数组搬移的最坏成本为每个更新 `O(LEVELS)`；这是可测量的正确性基线，尚无端到端延迟结论。

```rust
Book::<BinanceSpot, 8192, 2048>::new(key, generation);
book.install_snapshot(snapshot)?;
book.apply(event)?;
let view = book.view(now_ns, maximum_age_ns)?;
book.begin_rebuild(new_generation)?;
```

`install_snapshot` 完成后状态为 `Bridging`，尚不能供策略使用；首个合法增量提交后才进入 `Live`。`apply` 返回 `Applied { last_update_id, became_live }` 或 `IgnoredStale`。不匹配的 key/generation 返回错误且不改变现有簿；当前 generation 的缺口、非法字段、容量耗尽使簿失效。失效后只有更大的 generation 可以开始重建，旧 REST 响应和旧连接的迟到包不会重新激活簿。

`view` 只提供满足 freshness 阈值的 `Live` 数据；时间来自事件携带的本地单调接收时间。重复包不刷新时间。快照之后的首个缓存增量可以早于快照接收时间；后续已应用增量的本地时间不得倒退。错误时原价位和 ID 保留供诊断，但有效 view 不再开放。

## 快照覆盖与同步策略

快照携带每边 `complete` 标志。默认的有深度上限快照在最差买价以上、最差卖价以下建立完整价格覆盖；边界外未出现的价位查询返回 `Unknown`。已收到更新的边界外价位可以返回已知数量。若覆盖区内的价格都删除，`best_bid/best_ask` 返回 `Unknown`，不会把未知区域中的某个已知价位当作真正最优价。`complete=true` 由适配器根据快照协议确认，空完整一侧返回 `Empty`。

| 策略 | 桥接前忽略 | 首个可应用事件 | Live 后续 |
| --- | --- | --- | --- |
| Binance Spot | `u <= L` | `U <= L + 1 <= u` | 忽略 `u <= local_id`；`U > local_id + 1` 失效 |
| Binance USDⓈ-M | `u < L` | `U <= L <= u` | 忽略 `u <= local_id`；其余要求 `pu == local_id` |

Spot 官方初始化文字要求首缓存包包含 `L`，而应用步骤允许 `U=L+1`。本实现遵循主设计 §6 的下一更新号判定；订阅缓存和快照请求方仍负责官方“快照早于第一缓存包 U 时重取”的流程。Futures 在桥接时允许 `u=L`，并对下一包开始检查 `pu`。同一 generation 的全量重放保持事件到达顺序；跨连接使用独立实例建立 shadow book，所有者切换实例。

来源：[Spot 官方原始文档](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#how-to-manage-a-local-order-book-correctly)、[USDⓈ-M 官方同步规则](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)。本次核验为 2026-10-20，具体边界同时固化为测试。

## TDD 验收范围

测试先于实现：Spot 的 `U=L`、`L+1`、`L+2` 和 `u=L`；Futures 的 `u<L`、`u=L`、初次 `pu`、后续 `pu`；重复、重叠、缺口、旧 generation、错误 key、失败后重建、零删除、重复价格、负值、双边原子提交、批次最终容量、未知深度、时间倒退和过期 view。另用独立 `BTreeMap` oracle 重放大量有效批次，检查排序、绝对数量与查询结果；分配计数测试检查构造、快照、更新、读取、重建不调用堆分配。

这份接口用于 reducer 实现。REST/WS transport、录包解码、首次缓存快照重试、shadow 切换所有权和策略执行门禁仍属于完整行情管线；它们需要单独的集成测试与模型。
