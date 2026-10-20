# L2 审查与修复记录

L2-001 由 root 独立审查提出，risk_core 用原模型的上界配置复现。ipc_codec 随后独立检查公开 API 可达性，并确认恢复入口冻结旧 Engine 的修复方向。root 编写 `failed_recovery_cannot_leave_the_old_engine_authorized`，先在旧源码得到真实失败，再加入 `self.engine.freeze()` 通过回归。Red 输出见 `04_evidence/pre_fix/rust-recovery-red.log`；当前回归在 `crates/tickwire-mock/tests/services.rs:227-262`。

该回归使用 capital=MAX、第二笔成交 cost=MAX-3，明确保留 available=2；恢复因多保留第一笔订单预算而溢出。它检查失败后的金额不变、frozen=true、第三笔预算为 1 的请求被拒绝，而且 exchange.submit 调用仍为 2。原 TLC 最短反例使用 MAX-1，仅证明失败后权限标志错误；没有把它表述成第三笔已成功提交。

原模型与配置保存在 `04_evidence/pre_fix`，该目录用于复现失败历史，不进入统一 PASS 检查。`02_models` 只保留当前 `L2_EconomicFacts_v2.tla`，避免统一脚本对多个模型产生歧义。v2 对完整同步 Restart 的可见差异是：失败也将原 Engine 冻结；金额、订单事实、原始日志仍保留。

修复后的 primary、long、boundary、scaling 四配置全部 PASS，18 个 Next 动作在每个配置中都实际生成后继。root 最终只读审阅确认 v2 的 freeze、去重维度、Fill/Fee/Terminal/Replace 顺序、有限数值嵌入和失败见证与源码匹配；05_output.md 现已固定。Finding 状态为 retired(fixed, validated)。
