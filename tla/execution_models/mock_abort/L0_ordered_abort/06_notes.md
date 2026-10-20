# 当前源码与旧层的关系

旧 `submission/L1_crash_receipt` 对 NoBytes 的结论允许重启后重新保留预算，且不包含交易结果记录；旧 `L2_economic_facts` 按 intents 后 facts 两组恢复，并记录了保留冻结旧 Engine 的回归。这些是其冻结源码下的历史结果。

本次 `MockJournal.events` 和持久中止改变了这两处语义，不能只平移行号后把旧模型称为当前实现验证。旧 00/05、module、日志与 anchors 保持原样；新 `mock_abort/L0_ordered_abort` 绑定当前源码。顶层索引采用 [INDEX_ENTRY](../INDEX_ENTRY.md) 标记语义变化及后续适用范围。
