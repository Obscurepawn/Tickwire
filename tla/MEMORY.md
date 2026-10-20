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
- Code anchors: `crates/tickwire-mock/src/lib.rs:54-73` objects；`:116-145` journal；`:163-170` exchange；`:210-235` runtime submit；`:264-276` restart；`crates/tickwire-execution/src/lib.rs:504-528` recover；`crates/tickwire-execution/src/ports.rs:21-27` 传输结果。
- TLC: primary 1,744 distinct / depth 14；scaling 96,828 distinct / depth 22；pending 186,148 distinct / depth 22；全部 19 个 Next 动作触发。
- Finding: none
- Reviewed: 2026-10-20，risk_core 独立源码/模型审阅通过，并复核 PendingResponse 最终增量。
- Output: [L1 输出](execution_models/submission/L1_crash_receipt/05_output.md)
- Last synced: 2026-10-20，完整函数范围重新锚定。

两个本轮独立源码审查发现已由 Rust Red/Green 确认并修复，记录于 [L1 notes](execution_models/submission/L1_crash_receipt/06_notes.md)；不是 TLC 发现。没有 STAGED 或开放 candidate。下一层为经济事实重放；当前 PASS 不覆盖实盘传输/真实持久化或完整生产系统。
