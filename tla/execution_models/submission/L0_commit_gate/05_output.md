# L0 输出（已固定）

结果为 PASS。primary 穷举 2,236 个不同状态，scaling 穷举 9,076 个不同状态，两者最大深度均为 15。准确命令、全部计数和逐动作覆盖见 [运行记录](03_run_log.md)。本级没有 TLC 反例或开放 candidate。

在选定的无经济事实提交状态机中，检查通过的契约是：资金和订单槽不会超额准入；一次 intent 至多成功返回一次 begin_send；成功进入发送之前存在符合固定 Local/Replica 提交模式的可信完成证据；MayHaveSent 保留全部预算；不存在订单的槽没有占用。显式 Unknown 冻结共享池，NoBytesRejected 冻结但不释放，成功 NoBytes 与未发送撤销释放预算。

后一级可使用这些条件：固定订单身份、可信 CommitReceipt、可信 NoBytesAccepted、单所有者方法调用。L0 把完整 DurableRecord 的内容身份固定，因此不覆盖错误 request/sequence 回执；L1 进一步保留 profile 与 sequence，Rust 回归检查全结构体相等。global gateOpen 是实际 Gate::permits 的合取谓词，不证明真实时间刷新、行情新鲜度和账户恢复事实本身。

## Fidelity & coverage

- Fidelity: 12 transitions modeled, all cited file:line; 0 UNCLEAR/TODO within the stated scope; abstractions listed in [01_reasoning.md](01_reasoning.md). 两个 Persist 动作是可信回执来源的环境边界，L1 替换成实际 mock 提交流程。
- Code coverage: modeled 10 of 16 public state-mutating Engine APIs, excluding constructors/accessors. 覆盖 reserve、accept_commit、begin_send、abort_unsent、transport_rejected_without_bytes、uncertain、not_found、ack、update_gate、freeze；recover_intent 在 L1，request_cancel/cancel_rejected/terminal/fill/fee 留给 L2。这是设计接口覆盖，不是 Rust 行覆盖率。
- Model coverage: TLC --coverage — all 12 Next actions fired in both configurations, plus Init. scaling 中 Freeze 的 distinct 为 0、generated 为 4,538，仍是有效覆盖。

`ipc_codec` 独立复核了源码 guard、可信回执前提和 Gate 抽象，并指出错误 NoBytes 的冻结副作用；已增加独立 NoBytesRejected 动作并重新运行。经济事实后的 held 变化、实际磁盘持久化、并行 sender、恢复后激活和活性没有在本级证明。
