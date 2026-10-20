# Tickwire TLA+ 工作索引

## execution / submission / L0_commit_gate — PASS（2026-10-20）

- Mode: transcription
- Property: 固定 profile 下发送前完成证据；无重复 begin_send；Unknown 预算；有界准入。
- Code anchors: `crates/tickwire-execution/src/lib.rs:60-71` gate；`:138-181` 订单与预算；`:262-392` reserve/commit/send/abort/unknown/ack。完整路径和逐行哈希见该级 anchors.lock。
- TLC: primary 2,236 distinct / depth 15；scaling 9,076 distinct / depth 15；全部 12 个 Next 动作触发。
- Finding: none
- Reviewed: 2026-10-20，ipc_codec 独立审阅，补 NoBytesRejected 后重跑 PASS。
- Output: [L0 输出](execution_models/submission/L0_commit_gate/05_output.md)
- Last synced: 2026-10-20，完整函数范围重新锚定。

## execution / submission / L1_crash_receipt — PASS（2026-10-20）

- Mode: fault-injection（日志失败、记录已存但回执丢失、未知发送、exec 崩溃与重启）
- Property: send 前保存记录、无重复 exchange.submit、依赖完整性、Unknown 预算、空/非空 journal 重启冻结。
- Code anchors: `crates/tickwire-mock/src/lib.rs:54-73` objects；`:116-145` journal；`:163-170` exchange；`:210-235` runtime submit；`:265-278` restart；`crates/tickwire-execution/src/lib.rs:504-528` recover；`crates/tickwire-execution/src/ports.rs:21-27` 传输结果。
- TLC: primary 1,744 distinct / depth 14；scaling 96,828 distinct / depth 22；pending 186,148 distinct / depth 22；全部 19 个 Next 动作触发。
- Finding: none
- Reviewed: 2026-10-20，risk_core 独立源码/模型审阅通过，并复核 PendingResponse 最终增量。
- Output: [L1 输出](execution_models/submission/L1_crash_receipt/05_output.md)
- Last synced: 2026-10-20，完整函数范围重新锚定。

两个本轮独立源码审查发现已由 Rust Red/Green 确认并修复，记录于 [L1 notes](execution_models/submission/L1_crash_receipt/06_notes.md)；不是 TLC 发现。L2 已完成经济事实与恢复失败分支验证，见下方条目；既有 PASS 不覆盖实盘传输/真实持久化或完整生产系统。

## execution / submission / L2_economic_facts — PASS v2（2026-10-20）

- Mode: transcription，完整同步 record/restart；单结算资产经济事实与一次有序重放。
- Property: 唯一事件账本、池合计、重复幂等、失败原子性、缺失成交预留、费用证据阻止 NoBytes 释放、deficit/恢复失败冻结。
- Code anchors: `crates/tickwire-execution/src/lib.rs:138-245`、`:262-339`、`:352-528`；`crates/tickwire-mock/src/lib.rs:104-114`、`:236-278`；`crates/tickwire-execution/src/ports.rs:51-80`；444 行 anchors 检查无漂移。
- TLC: primary 24,036 / long 280,458 / boundary 1,495,032 / scaling 1,485,288 distinct；完整深度分别 8/9/11/11；每 cfg 全部 18 个 Next 动作生成。
- Finding: retired(fixed, validated by L2_EconomicFacts_v2)：L2-001 恢复重放失败后旧 Engine 未冻结；原 boundary 25,367 distinct、6 状态反例，真实 Rust Red/Green 与 v2 四配置验证完成。
- Reviewed: root 独立源码/模型审查；ipc_codec 独立确认实际 API 可达性与冻结修复；risk_core 独立复跑 Rust 回归。
- Output: [L2 输出](execution_models/submission/L2_economic_facts/05_output.md)
- Last synced: 2026-10-20，源文件/model/cfg SHA-256 已归档；历史失败源码匹配提交 cd8bde270ac257b296100979621c60c64f965e42。

## broadcast_ring / L0_publish_borrow_reclaim — PASS（2026-10-20）

- Mode: transcription，固定身份的单 bank；容量 64、有限序号/窗口和任意停顿。
- Property: 完整发布、连续释放/应用、保守最小水位、活跃借用及未读槽位不被覆盖、实际 cursor 退出证明、Freeze。
- Code anchors: `crates/tickwire-shm/src/lib.rs` 的 Producer/Reader 与 `src/mapped.rs` 的发布、借用、Quiesce/detach；638 行检查无漂移，完整源码哈希见 results.json。
- TLC: primary 29,889 / two_readers 930,364 / wrap 11,324 / pending_wrap 12,834 / wrap_two 40,796 distinct；各配置所列动作全部生成后继。
- Finding: 模型无新候选。实际 cursor 拒绝、Quiesce 幂等和 guard 的 NonNull 表示修复来自独立源码审查；分别保留 Rust 回归或语言契约依据，未归因为 TLC 发现。
- Reviewed: root 与 risk_core 独立审查模型；market_core 复核映射生命周期与 NonNull 修复。
- Output: [L0 输出](execution_models/broadcast_ring/L0_publish_borrow_reclaim/05_output.md)
- Last synced: 2026-10-20，NonNull 表示不改变协议迁移，源码哈希/anchors 更新后沿用相同模型结果。

## control_requests / L0_registry + L1_session — PASS（2026-10-20）

- L0: 370 / 1,570 / 13,442 distinct，depth 6/8/10；L1: 1,566 / 3,926 / 3,804 distinct，depth 8/8/9。
- Source: tickwire-control 的 Session/MockRequestStore；独立 ipc_codec 源码/模型审核通过。每个配置全部 Next 动作生成后继。
- 范围：已认证请求、串行调用、一次出票、重试先于版本、终态不覆盖、Mock 失败；L1 增加同时间域租约及永久关闭。字节解析、实际 OS 身份、业务副作用和持久 owner 恢复另验。
- 源码审查完善响应 opcode/Status version 关联和期限耗尽错误分类，保留 Rust Red/Green；没有将其归因为 TLC 反例。
- Output: [L0](execution_models/control_requests/L0_registry/05_output.md)、[L1](execution_models/control_requests/L1_session/05_output.md)。

## physical_journal / L0_batch_sync_recovery — PASS（2026-10-20）

- 1,816 / 2,695 / 45,883 distinct；depth 18/20/28；三配置 16 个 Next 动作均生成后继。
- 固定单段、帧粒度物理内容见证；写入/同步/收据分离、poison、崩溃、末尾修复与可选外部可信前缀。root 独立源码/模型审阅，ipc_codec 独立帧 codec 审阅。
- 段/manifest 目录事务的形式模型待 L1；实际文件、短写、同步失败与四个 SIGKILL 边界有 Rust 回归。设备掉电与跨主机复制不在当前结论内。
- Output: [L0](execution_models/physical_journal/L0_batch_sync_recovery/05_output.md)。

## book_state / L0_lifecycle + L1_coverage — PASS（2026-10-20）

- L0 Spot 主/扩展：1,392 / 8,499 distinct；USD-M 主/扩展：1,880 / 10,524 distinct；四配置 depth 6，各 19 个 Next 动作生成后继。
- L1 Bid 主/扩展：1,471 / 7,050 distinct；Ask 主/扩展：1,471 / 8,244 distinct；四配置 depth 6，各 15 个 Next 动作生成后继。
- Source: `crates/tickwire-book/src/lib.rs` SHA-256 `37beb8dcd515b76a8c3bec0a2cd46a1c3c2431375276810b45818b8b35fd04a3`，与已提交性能候选相同。
- L0 转录单所有者 API、Spot/USD-M 判定顺序、Bridge/Live/Invalid/rebuild、调用时 freshness、双侧失败原子性。L1 使用真实完整侧的隐藏价位见证，检查 Known 数量/最优价正确，Unknown 不伪装成零，增量保持 coverage。
- 限定：有限 generation/id/time/price 域；L1 假定适配器提供真实快照及完整已接受变化，没有证明 L0→L1 形式细化；不验证数组排序/合并、性能或未实现的自动风险门禁。
- Finding: 无源码缺陷候选；模型初次语法修正单独归档，未当作系统反例。root 独立源码/模型审阅通过。
- Output: [L0](execution_models/book_state/L0_lifecycle/05_output.md)、[L1](execution_models/book_state/L1_coverage/05_output.md)。
