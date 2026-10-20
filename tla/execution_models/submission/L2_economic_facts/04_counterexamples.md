# L2-001：恢复重放失败后保留旧准入权限

独立审查首先提出上界场景，随后原始 L2 模型的 boundary 配置产生反例。TLC 实际运行生成 38,635 状态、25,367 个不同状态，在第 6 个状态违反 `TransitionChecks.recovery`。原模型、配置和完整输出保存在 [pre_fix](04_evidence/pre_fix/)。

| 状态 | 动作 | 当前资金与权限 |
| --- | --- | --- |
| 1 | Init | spent=0、held=0、frozen=false |
| 2 | Admit(o1) | held=3；提交记录存在 |
| 3 | Admit(o2) | held=6；两个提交记录存在 |
| 4 | NoBytesAccepted(o1) | o1 预算释放，held=3；o1 提交记录仍存在 |
| 5 | Fill(o2, quantity=2, cost=999) | spent=999、held=1、capital=1000、frozen=false；事实已进入日志 |
| 6 | RestartFailed | 局部恢复先给 o1/o2 重新预留 6；重放 o2 成交需要 spent=999、held=4，超过 ArithMax=1000；函数返回错误，旧 Engine 的 frozen 仍为 false |

数值 999 对应真实 `i128::MAX-1`；这是同一 checked-add 分支的上界代表值。反例没有把普通金额缩小后误判为溢出。更强的 Rust 准入见证可用 `MAX-3`：旧 Engine 仍剩 2，局部重放仍溢出，随后预算 1 的新 intent 能否进入由实际回归测试确认。

根因在原 `MockRuntime::restart`：仅新建的局部 Engine 被冻结，所有恢复动作成功后才写入 `self.engine`。重放提前 `?` 返回时，旧 Engine 未被关闭。资金字段保留旧值符合失败原子性，但恢复失败同时保留了旧的准入权限。

反驳检查：`held` 是未结预算，NoBytes 释放并没有删除已提交记录；`recover_intent` 必须重新保守预留，不能跳过该记录。`&mut self` 保证串行，并不能改变提前返回的状态。此场景只使用公开 Mock/Engine API 与现有 checked-add，未引入磁盘损坏或并发写。原反例仅证明失败后 frozen=false；第三笔小预算准入由 Rust 测试补充。

Reviewed 2026-10-20：root 已用 MAX-3 的实际 Rust 回归复现，ipc_codec 独立检查公开 API 可达性及冻结修复。分类：retired(fixed, validated by L2_EconomicFacts_v2)；修复后的四配置均 PASS，root 已独立审阅最终 v2。候选来自独立源码审阅后由 TLC 验证，不表述为无提示盲搜发现。
