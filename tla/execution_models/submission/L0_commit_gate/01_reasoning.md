# L0 推导

模式为 faithful transcription。实际 Engine 使用 `&mut self`，订单状态迁移不会被另一个 reducer 调用插入；模型以一次公共方法成功作为一个原子动作。环境可以在两次调用间改变 gate、完成本地写入或完成复制。

模型具体保留订单槽存在性、固定提交模式、发送状态、终态、durable、资金占用、local/replica 完成证据。`sendCount` 是附加历史见证，计数成功的 begin_send 返回，防止仅凭最终标签遗漏重复提交。L0 的 NoBytes 分支只允许传输层真实没有接受输出字节时调用。

| 抽象 | 对本级性质的影响 |
| --- | --- |
| 数量、单位资金与费用合并成固定 Budget | 保留 reserve 的总额检查和未发送撤销释放；成交、费用分录明确留给 L2 |
| 只枚举有效 request、同 domain、唯一 intent | reserve 的无效输入返回且不修改状态，对本级状态不变量是 stutter；错误分支已由 Rust 测试覆盖 |
| Gate 的多个布尔/版本/期限条件合并为 gateOpen | begin_send 与 reserve 检查同一谓词；不证明上层按真实时间刷新 gate |
| local/replica 使用集合 | 每个 intent 的完成记录由可信适配器提供；记录内容身份在本级固定，L1 增加 request profile 与 sequence 的持久匹配 |
| 省略不会改变状态的错误回执/重复 reserve 调用 | 错误返回等价于 stutter，不会隐藏状态安全违规 |
| 有限 Orders、Slots、Budget、Capital | primary 让资金限制生效，scaling 让订单槽限制生效；没有用状态截断删除故障路径 |

UnknownKeepsReservation 只针对没有交易结果记录的本级状态机；有成交后剩余预留的真实公式在 L2 才能验证。L0 不检查 live-lock 或最终成交，环境允许永久不返回 ACK。

`NoBytesRejected` 单独表示矛盾或不存在订单的 NoBytes 回调：实现返回错误并冻结准入，保留资金和订单状态。本级排除了交易结果记录，因此 `filled=observed_filled=0`；包含费用或成交后的矛盾回调由 L2 继续检查。
