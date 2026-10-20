# 账户、日志与首次输出的组合检查

最终 `L0_AccountGateway_v2` 的四个配置均完成穷举，20 项不变量通过。模型保留单所有者同步调用的真实顺序，检查候选账户、同步 WAL、首次传输接受、零接受中止，以及崩溃后的完整重放。

| 配置 | generated | distinct | 深度 |
|---|---:|---:|---:|
| L0_AccountGateway_v2 | 5967655 | 961560 | 50 |
| L0_AccountGateway_v2_boundary | 5456397 | 892832 | 50 |
| L0_AccountGateway_v2_two | 12639561 | 2024848 | 50 |
| L0_AccountGateway_v2_crashes | 9600215 | 1793656 | 50 |

`AccountPrefix` 比较全部持久订单状态和账本序号；`SuccessfulPrefixReplays` 要求线上逐条应用成功的公开日志前缀能够完整重放。资金及剩余预留由该有限 Spot 输入的订单和成交集合计算。恢复附加 Unknown 与本地冻结不进入持久状态比较，公开风险检查继续使用它们。

本级还检查同步日志和完整闭包先于入队及首次接受、中止同步先于释放、正数接受后保持发送责任、错误交易结果留在日志、恢复后无连接与发送 capability。最终门禁对应传输写入前的授权及 Instant 采样；网卡实际发出和交易所收到报文不属于这个完成点。

## 发现与修复

Finding: retired(fixed, validated by L0_AccountGateway_v2)。初模发现恢复前后账本序号不一致；真实文件回归和独立审阅确认，MAX−2 基线下曾出现线上返回 Duplicate、再次恢复却溢出的日志。另一个回归确认主动冻结会遮住新的终态异常。

账户修复区分持久交易所 Unknown 与恢复附加 Unknown，并为终态异常保留单调字段。普通、MAX、溢出失败原子性及本地冻结四项回归通过。root 和 IPC 已独立审阅源码、模型、反例归因及 `allApplied` 增量；原模型、反例和此前各轮完整输出均保留。详细证据见 `04_counterexamples.md` 与 `03_run_log.md`。

## Fidelity & coverage

- Fidelity: 57 个 Next 转换及 Init，全部有源码或明确故障完成点映射；0 项 UNCLEAR/TODO。变量映射在模型首部，有限实例和不可交错的调用边界见 `00_input.md`、`01_reasoning.md`。
- Code coverage: 按接口流程统计，选定的 8 个流程均有转录：`recover`、`submit`、`poll`、`cancel_pending`、`record_fact`、`apply_report`、`abort`、`halt`。这是设计流程覆盖，不是 Rust 执行行覆盖；创建基线与物理存储事务作为已验证接口输入。
- Model coverage: 四配置合计覆盖 Init 与全部 57 个 Next 动作。primary、two、crashes 的 `AdmissionOverflow` 为零，由 boundary 覆盖；boundary 的 `QueueFail` 为零，由其他配置覆盖。完整 generated 数量逐项列于 `03_run_log.md`。

1576 行源码锚点检查零漂移。最终源码、模型、配置、TLC 工具和日志摘要保存在 `04_evidence/results.json`。

## 后续模型的输入

后续接入回报关联、连接重建或恢复激活时，继续保留 `DurableBeforeOutput`、`ReleaseAfterSync`、`RecoveredNoCapability`、`AccountPrefix` 和 `SuccessfulPrefixReplays`。本级账户实例是固定数量、限价、零费用的 Spot 买入；多资产、USD-M、字节编码、TLS 算法与物理 segment/manifest 的保证由对应模块提供，不从该实例推导。
