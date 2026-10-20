# L0 输入

目标是实际 `tickwire-gateway::JournaledSender`，并非平行的 Mock 发送器。本级按冻结源码建模，源码摘要保存在 [source.sha256](04_evidence/source.sha256)。模型以单实例的 `&mut self` 所有权为并发边界。

- 协调器：`crates/tickwire-gateway/src/lib.rs`，入口 create、submit、poll、cancel、recover。
- 日志：`crates/tickwire-business-journal/src/lib.rs` 的 commit_intent、append_fact、append；成功返回意味着物理批同步已完成。
- 传输：`crates/tickwire-transport/src/connection.rs` 的 queue、drive、try_send、cancel 和终态记录。
- 已有有限模型：[首次输出](../../transport/L0_first_output/05_output.md)、[业务日志](../../business_journal/L0_commit_abort_recovery/05_output.md)。本级使用其源码契约，重新检查组合顺序；不将两份 PASS 当作自动的组合证明。
- 实际回归：`crates/tickwire-gateway/tests/lifecycle.rs` 和后续真实 TLS/业务 WAL 集成；最终验证日志归 root 维护的 `docs/validation/gateway/`。

可信 `Authorization` 负责真实权限、账户、风险和恢复依赖；`Encoder` 负责规范命令与签名报文的准确映射。模型只检查协调器是否验证和使用这些结果，不证明这些提供者自身的算法。
