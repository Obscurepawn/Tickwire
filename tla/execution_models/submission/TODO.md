# 提交阶梯待办

当前 Mock 已将 intent/fact 保存到同一事件序列，并持久记录 NoBytes 中止。旧 L1/L2 的“先恢复全部 intent，再应用全部 fact”与当前实现存在语义差异，其固定模型和锚点保留为历史证据。现行零成交、零费用子集已由 [mock_abort](../mock_abort/ROADMAP.md) 验证；下一交易结果记录层应按新顺序组合 Fill/Fee/Terminal 及 append/apply 崩溃，不能只更新旧模型的行号。

L0、L1、L2 v2 已完成源码映射、全部配置 TLC、逐动作覆盖和独立审阅；没有开放 candidate。L2-001 的原模型与失败日志保存在 `L2_economic_facts/04_evidence/pre_fix`，当前验证模型是 `02_models/L2_EconomicFacts_v2.tla`，固定结论见各级 05_output.md。

L2 已检查成交早于 ACK、重复/冲突回报、终态早于明细、迟到费用、零费用后的 NoBytes 拒绝、事件槽耗尽、所选金额上界、deficit 冻结与 Mock 有序重放。恢复重放失败关闭旧 Engine 权限的 Rust Red/Green 与 TLC 前后对照已经归档。

下一计划层为 L3_activation：先实现可检验的恢复授权、账户对账和旧 exec 隔离，再建模新 epoch 激活；当前恢复后始终冻结。交易结果记录后续扩展包括 append/apply 之间的进程崩溃、真实 journal/复制提交边界、费用尾账释放与完整多资产账本。

复验入口：`./scripts/check-tla.sh`。L1 因 restart 新增两行作非语义漂移同步，仅更新 anchors.lock 与 06_notes，固定 00/05 和原模型保持不变。后续源码变化先核对完整函数锚点；改变已固定结论使用新版本模型。
