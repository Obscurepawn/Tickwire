# 账户恢复依赖规范

1. kind 14 保存完整账户基线及目录，kind 15 保存该次准入之前的账户日志前缀。二者各出现一次且成对出现，并要求一个 kind 13 完整命令。
2. kind 1 的完整准入记录摘要等于 `ClosureBinding.business_digest`。记录内的原计算时刻、完整命令与 AdmissionImage 不能由恢复时的当前值替换。
3. `Persistence::publish/verify` 完成对象读取、长度与摘要检查及同步后，才能产生带账户摘要的 `VerifiedClosure`。旧 manifest 返回两个 `None`，账户协调器必须拒绝。
4. 协调器比较自己持有的完整基线和实际日志前缀，不接受仅来自调用方的摘要声明。
5. 前缀对象包含范围、日志流、基线、记录数量和历史链摘要。完整记录仍由 WAL 保存；缺失 WAL 记录不能由前缀对象补足。
6. kind 1～12 的既有依赖约束、不可变 manifest 绑定与完整输入覆盖规则保持有效。新增种类不改变旧 manifest 的字节编码。

接口见[账户恢复依赖绑定接口](../../docs/implementation/account-recovery-binding-interface.md)，行为测试位于 `tickwire-recovery/tests/closure.rs`，真实文件组合测试位于 `tickwire-account-gateway/tests/binance_rest.rs`。
