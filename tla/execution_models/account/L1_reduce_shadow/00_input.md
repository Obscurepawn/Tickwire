# 输入与范围

本级在 L0 的订单生命周期上加入 USD-M 实际仓位、独占减仓额度和交易所可用资金影子账。源码仍为单个 `Account` 的独占可变调用；模型以一次公开方法为一个原子动作，没有引入并发写账户。

选定一套合法的整数输入：OneWay、单结算资产、数量和价格单位均为 1、买入和减仓卖出限价及压力价格均为 1、有效杠杆 1、维持保证金率与资金费率为 0、执行费率 1/2、最大成交次数 2。每单原始数量 1，最多一笔实际成交和一条独立费用记录。所有平仓成交明确带已报告的 PnL=0，不从价格推导损益。策略限额取足够大且不发生归属转移。费用取值包含 0、正常值和超预算值。

对应源码：`crates/tickwire-account/src/admission.rs:139-192`（独占减仓分配）、`crates/tickwire-account/src/admission.rs:268-334`（完整计算与新增 H）、`crates/tickwire-account/src/admission.rs:384-430`（影子账与中止）、`crates/tickwire-account/src/calculation.rs:275-400`（资本）、`crates/tickwire-account/src/events.rs:161-269`（成交）、`crates/tickwire-account/src/events.rs:288-342`（费用）、`crates/tickwire-account/src/events.rs:473-493`（实际仓位与额度一致性）、`crates/tickwire-account/src/external.rs:70-122`（外部仓位变化）。

外部仓位更新最多一次，真实数量可替换为 0、1、2，差额记入未归属桶，并保持原预留、立即冻结。模型不覆盖 Hedge 双侧、跨标的组合、外币估值、目录变更、i128 溢出或恢复 WAL；这些范围分别留给 Rust 测试和后续层级。
