# Mock 未发送中止与顺序恢复

## 接口与决定

`ExecutionFact::AbortedBeforeSend { record: DurableRecord }` 绑定原始请求（包含 epoch）及 journal sequence。可信传输层的 `NoBytesAccepted` 才产生此事实。事实入日志失败时保留占用并冻结；写入成功后调用现有 Engine 的无字节拒绝转换。Engine 核心不改。

`ExecutionFact::apply` 先精确比较当前 `durable_record`，不匹配或缺失时冻结。重复应用同一中止记录只在订单已是无成交、无费用、已释放的 `NotSubmitted/Rejected` 状态时返回成功；存在费用、成交或其他终态的矛盾由现有转换拒绝并冻结。

`MockJournal` 增加只读 `events()` 统一流，以 `Intent(DurableRecord)` 和 `Fact(ExecutionFact)` 保存实际写入顺序。旧 `committed()`、`facts()` 接口保留，属于分类视图。幂等 commit 不追加重复 intent；回执丢失发生在 intent 事件保存后。恢复先冻结原 Engine，再向新的冻结 Engine 逐事件重放；失败时不替换旧 Engine。

## 实际 Red

使用现有可编译 API 的 16 项 services 测试，12 项通过、4 项断言失败，见 [red.log](../../docs/validation/mock-abort/red.log)。失败分别是：普通无字节中止后重新占用 102、事实存储失败仍成功、已释放本金重新占用导致溢出、终态释放与后续 intent 被分组重排导致溢出。失败恢复后的授权冻结另有独立回归保留。

## 验证状态

实现已完成：services 21 项和 execution 16 项全部通过，fmt、Clippy 通过。三组有限配置分别穷举 2,051 / 457 / 102,149 个不同状态，15 个动作均触发，全部 PASS。详见 [验证报告](../../docs/validation/mock-abort/report.md)。旧 TLA 层固定旧源码语义，当前修复建立独立 `mock_abort` 层，保留历史结果。risk_core 已独立对照代码与模型，无阻断项；本次证据已冻结。
