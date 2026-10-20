# Mock 持久中止修复证据

`NoBytesAccepted` 现在先保存绑定完整 `DurableRecord` 的中止事实，再释放资金。`MockJournal.events()` 保存 Intent 和 Fact 的实际顺序；恢复不再先重放全部 intents。旧 `committed()` 和 `facts()` 仍作为分类查询接口保留。

## 回归

[首次 Red](red.log) 编译成功，16 项 services 中 4 项真实断言失败：无字节中止后重新占用 102、中止写入失败仍成功，以及两种分组恢复导致的溢出。[首次 Green](green.log) 16/16 通过。

后续 [tests.log](tests.log) 包含 execution 16 项和 services 21 项，全部通过；两个 crate 的 doctest 均为 0。新增边界覆盖完整 record 的 sequence/epoch/request/domain 错配、缺失 intent、重复中止、零/正费用与中止矛盾、事实写入后尚未应用就重启、回执丢失后的幂等事件流，以及损坏事实令恢复失败后旧 Engine 仍冻结。[fmt](fmt.log) 与 [Clippy](clippy.log) 通过，Clippy 使用 `--all-targets -- -D warnings`。

顺序回归使用真实 `i128::MAX`：先释放小单，再提交最大预算订单。旧恢复把两份 intent 放到释放事实前，发生溢出；新恢复按日志顺序先释放再预留，成功恢复相同资金状态。另一回归包含大额成交，确认中止持久化修复后不再错误占用原订单预算。

## 有限模型

[Mock L0](../../../tla/execution_models/mock_abort/L0_ordered_abort/05_output.md) 三配置 PASS：生成/不同状态分别为 3,013/2,051、637/457、158,415/102,149，深度 26/22/40。每组 15 个动作均触发。模型验证零成交、零费用、固定请求的持久中止和事件顺序；完整字段冲突、重复中止、费用交错由上面的 Rust 测试验证。

日志持久性是 Mock 的跨模拟重启契约。这里没有真实文件同步、掉电或业务 WAL 测试。已确认旧问题来自源码审查和 Rust Red，TLC 没有发现新的代码反例。首次模型赋值错误另保留在模型目录，并标明作者错误。

所有本次代码、测试、模型和日志的 SHA-256 由本目录及模型目录的 `SHA256SUMS` 记录。risk_core 独立对照源码与模型无阻断项，见 [审阅记录](review.md)。
