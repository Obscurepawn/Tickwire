# 行情与私有事件独立审阅

审阅者 `risk_core` 只读核对了 [feed](../../../crates/tickwire-binance/src/feed.rs)、[metadata](../../../crates/tickwire-binance/src/feed/metadata.rs)、[events](../../../crates/tickwire-binance/src/events.rs)、[decode](../../../crates/tickwire-binance/src/events/decode.rs)、[binding](../../../crates/tickwire-binance/src/events/binding.rs) 和四个新增测试目标。

发现一项需要修正的协议边界：USD-M 跨仓资金费更新可以只有 `B` 和 symbol，没有 `P`。初版 decoder 强制两类集合同时出现，测试只使用了 `P: []`。作者补充可编译的失败断言后修复：每个缺省集合表示这一类没有报告更新，两类都缺省仍拒绝；容量按实际余额和仓位条目合计计算。实际 Red 和修复后的 Green 均已保存。

另一项讨论涉及 `rp`。官方模型将该字段标为 Optional，并说明其属于成交，没有“所有非成交事件都必须带 rp”的明确依据。最终规则允许原始解码保留 None，USD-M TRADE/CALCULATED 在绑定时要求该值，非成交订单允许缺省。没有生成默认 PnL。

最终增量二审通过，未发现剩余阻断项。审阅确认：

- 原始创建命令、持久订单映射、会话与目录共同确定本地身份；数量/价格/type/TIF 的后续实际变化保留，由账户 owner 与修改记录核对。
- 稀疏账户集合不表示全量资产或仓位；List/Algo 状态不替代子订单交易结果。
- 费用按资产现金流反号转换；PnL 独立绑定结算资产；报价金额保持交易所原值或 None。
- 未知字段和状态保留，未绑定事件可以继续交由对账处理。

审阅者实际运行：

```sh
cargo test -p tickwire-binance --test feed --test market_server --test private_events --test private_server
```

33 项全部通过，原始输出见 [independent-review.log](independent-review.log)。审阅者另外下载了 [official-futures-fields.json](official-futures-fields.json) 中的 8 个固定官方 URL，完整文件 SHA-256 全部一致。这里记录的是源码、协议与实际测试复查，没有新增 TLC 结论。
