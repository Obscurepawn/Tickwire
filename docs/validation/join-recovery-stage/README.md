# JOIN 与业务恢复阶段验证

本阶段的 macOS 和 Linux aarch64 全仓检查均通过。两次运行包含格式检查、全部测试目标、文档测试和 Clippy；源码对应的新增有限模型共 13 组配置全部通过。

| 检查 | 命令或范围 | 记录 |
| --- | --- | --- |
| macOS | `./scripts/check-rust.sh`，退出码 0 | [macos.log](macos.log) |
| Linux aarch64 | `./scripts/check-linux.sh`，退出码 0 | [linux.log](linux.log) |
| 运行源码 | 102 个源码、Cargo 配置和检查脚本摘要 | [source.sha256](source.sha256) |
| Mock 中止与恢复 | 未发送中止先保存再释放；按实际事件顺序重放 | [测试与审阅](../mock-abort/report.md) |
| 业务日志 | 23 项测试；真实文件、同步错误、SIGKILL 和目录固定 | [测试与审阅](../business-journal/README.md) |
| 恢复依赖 | 12 项测试；独立基准样本、对象与输入验证、同步重试 | [测试与审阅](../recovery-closure/README.md) |
| JOIN | owner、Supervisor 与 consumer 三进程；真实 FD 交付、追平和退出 | [测试与审阅](../join/README.md) |

Mock 中止模型三配置、恢复依赖模型三配置、业务日志模型三配置和 JOIN 模型四配置全部通过，每组要求的迁移动作均生成过后继状态。详细有限域和原始日志分别位于 [mock_abort](../../../tla/execution_models/mock_abort/ROADMAP.md)、[recovery_closure](../../../tla/execution_models/recovery_closure/ROADMAP.md)、[business_journal](../../../tla/execution_models/business_journal/ROADMAP.md) 和 [join_owner](../../../tla/execution_models/join_owner/ROADMAP.md)。

旧 submission L1/L2 的分组重放语义已经与当前 Mock 不同，其结果保留为历史证据。新增 Mock 模型验证当前零成交、零费用场景；当前成交、费用与订单终态的组合验证仍需扩展。旧 ring 模型涉及的发布、借用与退出行为保持不变，只更新源码行号及锚点；本轮没有为这些未变迁移重复运行 TLC。
