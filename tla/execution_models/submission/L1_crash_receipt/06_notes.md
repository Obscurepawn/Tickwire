# L1 独立审阅与源码发现

`risk_core` 直接阅读 L1 和 MockRuntime/Engine，确认同步 &mut 调用支持 Idle 才能插入其他 owner 操作；journal 内容与丢回执分开；BeginSend 先写 Unknown；恢复按 journal 保守重建 Unknown 并冻结。审阅要求明确每 intent 独立 recovery_object 和无经济事实范围，已写入 01/05。

最终增量审阅再次核对 PendingResponse：真实 runtime 的空分支对应 Respond 保持 send 与 frozen；只有 Unknown 结果主动冻结。独立审阅确认 Capital=4、Budget=2 的双 intent 配置可以容纳正常等待订单并存，没有发现该增量的源码映射偏差。后续异步 ACK 与经济事实仍在本级之外。

## 源码审查发现及修复证据

以下问题在建模取证时由独立源码审查发现，之后由主线程用失败测试确认并修复；没有把它们称为 TLC 反例。

| 发现 | 修复前证据 | 修复与复验 | 状态 |
| --- | --- | --- | --- |
| 已收到的经济事实因事件槽耗尽无法入账，仍可准入新风险 | `unaccountable_external_fact_freezes_new_risk` 失败，断言 `e.frozen()` 为假 | fill/fee/terminal 处理错误冻结；相同回归测试通过 | retired: fixed by Rust regression；经济事实形式模型待 L2 |
| 空 journal 重启恢复了原 gate 权限 | `even_empty_journal_restart_requires_recovery_authorization` 失败，断言 `r.engine.frozen()` 为假 | restart 新建 Engine 后立即 freeze；相同回归通过，当前 L1 RecoveryStaysFrozen 通过 | retired: fixed |

本次独立复验 `cargo test -p tickwire-execution -p tickwire-mock`：16 个执行测试、11 个 mock 测试通过，含 PendingResponse 正常等待后超时冻结的回归。原 Red 日志由本次工具读取核实，其中两个断言均真实失败；复验针对修复后的当前工作树。费用事实后迟到 NoBytes、显式 Unknown 冻结共享池也有独立回归测试，但前者的经济交错没有包含在 L1。
