# 顶层索引增量

`execution_models/mock_abort/L0_ordered_abort`：当前 Mock durable abort / ordered replay，三配置 PASS：primary 3,013 generated / 2,051 distinct / depth 26；boundary 637 / 457 / depth 22；scaling 158,415 / 102,149 / depth 40。Init 与 15 个 Next 动作全有生成状态。

旧 submission L1/L2 模型继续保留历史结果，其“没有持久 NoBytes 中止”和“intents 后 facts”的恢复语义已被当前实现替换。新层只验证零成交、零费用子集，不能替代旧 L2 的交易结果记录覆盖；现行全部交易结果记录仍由 Rust 回归及另行开发的业务 runtime 模型承担。

源码锚点和 SHA 位于 L0 的 anchors.lock 与 SHA256SUMS。顶层 PROFILE/MEMORY/ROADMAP 由 root 合并，本文件不自动修改旧冻结模型。
