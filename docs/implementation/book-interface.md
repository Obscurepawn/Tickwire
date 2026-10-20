# 有界订单簿接口设计

`tickwire-book` 将标准化价位更新写入单所有者订单簿。`Book<P, LEVELS, MAX_UPDATES>` 的存储和有效状态与交易所无关；`SequencePolicy` 决定快照桥接、旧事件和连续性。`BinanceSpot` 与 `BinanceUsdM` 是两种策略。网络、JSON/SBE 解码、增量缓存和跨进程发布由调用方提供。

## 数据与完成边界

`BookKey { venue, instrument, source }` 标识唯一行情域；`generation` 标识一次连续性建立过程。价格和数量分别使用 `PriceTicks(i64)`、`QtyLots(i64)`；调用方按照已验证的 instrument 元数据转换，价格严格为正、数量非负，数量零表示删除。

输入是借用的 `Snapshot` 或 `DepthEvent`，bids/asks 使用 `&[Level]`。数组每边最多 `LEVELS` 个存活价位；每个增量最多 `MAX_UPDATES` 个更新，双边合计。所有更新在提交前完成数值、重复价格、最终容量、最终最优买卖价和序号验证。买方更新成功而卖方失败时，买卖两边都保持原值，簿进入 `Invalid`。同一事件同侧重复价格被识别为无效输入；适配器若接收到有重复价格的协议，应先按该协议确定最终值。

每边采用连续定长数组，买价降序、卖价升序。增量先复制到实例内的固定工作区，检查价格唯一性并缓存每个更新对应的旧位置。一般路径将更新按侧排序；每侧最多 32 项时先识别顺序，已正序的输入直接保留、反序输入直接反转。全部为正数量的循环有序批次，按唯一断点旋转工作区，必要时反转，再复用一般路径的相邻定位。全部验证通过后，纯改量直接写已有位置；存在插删时，只合并受影响的连续区间，最终长度改变才搬移一次后方数据。因此满容量批次先新增、后删除仍按最终容量成功，最佳价的等数量替换也不再搬移整簿。

工作区由 `Book` 持有并重复使用，更新过程不申请内存。当前 64 位实现中，除原有双侧 `32N` 字节价位载荷外，增加 `16N + 32M` 字节工作区，`N=LEVELS`、`M=MAX_UPDATES`。这是每个实例的固定成本；容量配置同时决定订单簿和工作区内存。超出容量仍返回明确错误并停止对外提供有效 view，覆盖外价位不会静默丢弃。

对于每侧 `L` 个已有价位、`K` 项更新，一般路径纯改量最坏为 `O(K log K + K log L)`；插删最坏再加 `O(L+K)`。相邻更新优先复用上次定位的下一个位置；价格跳跃时，二分查找使用固定的全簿边界，避免后一次查找边界依赖前一次结果。二分不会线性扫描全簿。两侧始终先验证、后提交，工作区变化不构成有效盘口发布。批量合并的首次实现与对照保留在[批量优化证据](../validation/book-optimization/report.md)。

每侧最多 32 项、输入既非正序、反序，也非循环有序，且全部数量为正时，尝试直接定位已有价位。只有每项都存在且价格唯一，才使用不改变价位集合的计划，保留原长度、最优价格和覆盖边界；任一价位不存在便回到排序与完整结构计划，并重新计算全部旧位置。64 位局部标记记录旧位置的低 6 位；命中已有标记时，逐项比较实际价格。因此不同位置的标记碰撞不会误报重复，真正重复也不会被接受。每侧最坏额外比较数为 `32×31/2=496`，无需增加实例工作区。全部价格/数量先验证，两侧计划全部成功前不写有效盘口。[小批优化证据](../validation/book-small-batch/report.md)记录阈值选择、工作量回归和相邻、散布、大批、插删的完整对照。

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
