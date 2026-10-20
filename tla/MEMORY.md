# Tickwire TLA+ 工作索引

当前源码提示：submission L1/L2 的 Mock 分组重放语义已被统一事件流替换，下方旧条目保留其历史结论；现行中止重放见 mock_abort，真实业务日志见 business_journal。

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

两个本轮独立源码审查发现已由 Rust Red/Green 确认并修复，记录于 [L1 notes](execution_models/submission/L1_crash_receipt/06_notes.md)；不是 TLC 发现。L2 已完成交易结果记录与恢复失败分支验证，见下方条目；既有 PASS 不覆盖实盘传输/真实持久化或完整生产系统。

## execution / submission / L2_economic_facts — PASS v2（2026-10-20）

- Mode: transcription，完整同步 record/restart；单结算资产交易结果记录与一次有序重放。
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

## mock_abort / L0_ordered_abort — PASS（2026-10-20）

- 当前 Mock 中止先保存再释放，intent/fact 按实际事件顺序重放；零成交、零费用、LocalDurable 范围。
- 三配置不同状态 2,051 / 457 / 102,149，深度 26/22/40；各 15 个 Next 动作均触发。
- root、risk 独立源码及模型审阅通过；运行结果及 426 行锚点见 [输出](execution_models/mock_abort/L0_ordered_abort/05_output.md)。
- 旧 submission L1/L2 保留历史源码与结果；当前完整成交、费用、终态组合仍需新版本模型。

## recovery_closure / L0_publish_verify — PASS（2026-10-20）

- 区分对象与 manifest、不可变绑定、重命名可见、目录同步和结果交付；多个未完成目录项可分别保留或丢失。
- 三配置不同状态 2,345 / 15,097 / 3,898，深度 51/56/66；每组 Init 和 22 个 Next 动作均触发。
- 独立审阅指出首版故障域遗漏，修正后保持原规模重跑；初版保存在 pre-review，没有归为 Rust 缺陷。
- 原始运行保留 203 行锚点，范围与记录见 [输出](execution_models/recovery_closure/L0_publish_verify/05_output.md)。字节解析、输入采集和业务授权不由该模型证明。
- 本次同步更新为 214 行锚点，持久化状态公式与三配置不变，沿用原始 TLC 结果；kind 13～15 的形状检查和账户摘要不在旧模型覆盖内。旧源码、锁及日志保留于 `docs/validation/account-recovery-binding/pre-sync/`。

## business_journal / L0_commit_abort_recovery — PASS（2026-10-20）

- 串行 runtime：授权和恢复依赖完成、WAL 写入与同步、最终发送检查、中止保存后释放，以及冻结重放。
- 三配置不同状态 72 / 345 / 879，深度 14/26/28；每组 23 个 Next 动作均触发。
- root、ipc 独立审阅通过；332 行锚点及运行记录见 [输出](execution_models/business_journal/L0_commit_abort_recovery/05_output.md)。
- 固定身份、无成交与费用的有限范围；后续纳入交易结果组合、writer 变化、镜像与恢复后激活。最终检查边界不表示网卡实际发包期限。

## join_owner / L0_pending_activation — PASS（2026-10-20）

- 保留 S、构造与交付失败保留门禁、双资源就绪、真实 cursor 追平、原登记版本、幂等激活与协作退出。
- 四配置不同状态 2,181 / 321,256 / 172 / 7,652，深度 23/35/14/25；主配置各 22 个 Next 动作、两组 wrap 各 16 个动作全部触发。
- risk 独立源码、模型及最终日志审阅通过，1221 行锚点；[输出](execution_models/join_owner/L0_pending_activation/05_output.md)。
- 共用截止时间、固定认证身份与有限借用窗口；持久登记、独立截止、bank 轮换和死亡回收待后续。旧 ring 模型只发生行号变化，720 行锚点重审通过，状态公式未变。

## business_journal / L1_command_binding — PASS（2026-10-20）

- 源码：execution command_digest；codec schema 2；recovery kind 13；BusinessJournal 提交及 Runtime 恢复的精确命令依赖检查。
- 三配置：376 / 2664 / 7326 个不同状态；10 个不变量、全部 29 个 Next 动作可达。
- 单所有者、完整日志前缀、同步失败与最多两次崩溃；same-intent 变更冻结，恢复保持原摘要。
- 精确同请求幂等返回由 Rust 回归覆盖；完整命令语义和网络字节仍待网关组合。下层四个当前模型只更新源码锚点，公式不变，旧锁与映射见 docs/validation/command-binding。

## transport / L0_first_output — PASS（2026-10-20）

- 对应真实非阻塞 TCP/TLS 的首次应用接受、即时门禁、等待、部分接受、冲刷与零接受终态。
- 四配置不同状态 688 / 3,928 / 5,944 / 2,832；TLS 配置 19 个、明文配置 16 个 Next 动作均生成后继。
- 源码、模型与真实网络回归经独立审查；231 行锚点无漂移，完整记录见 [L0 输出](execution_models/transport/L0_first_output/05_output.md)。
- 应用接受与本机冲刷分开记录；交易所业务确认和会话恢复由上层模块处理。

## gateway / L0_journaled_sender — PASS（2026-10-20）

- 按单所有者 API 转录闭包验证、WAL 同步、入队、drive/settle/try_send、首次门禁、中止发布、冲突冻结与恢复。
- 四配置不同状态 7,572 / 57,624 / 11,188 / 8,610，深度 42/60/42/42；11 个不变量通过，每组 41 个 Next 动作全部生成后继。
- root、risk_core、market_core 独立审查通过；零 I/O 预算错误与连接错误分别建模。源码摘要、模型摘要及实际日志见 [L0 输出](execution_models/gateway/L0_journaled_sender/05_output.md)。
- 授权提供者和持久记录按接口契约建模；首次接受与本机冲刷不代替业务 ACK，持久 Aborted 的发布不代替账户资金释放。完整账户和恢复后激活继续由后续模型覆盖。

## account / L0_spot_facts + L1_reduce_shadow — PASS（2026-10-20）

- L0 最终 v3 三配置不同状态 176,596 / 192,984 / 180,131；L1 三配置 136,551 / 24,510 / 99,367。每个配置的全部 21 或 22 个 Next 动作均生成后继。
- 按账户同步 API 转录准入、成交与费用、重复事件后的状态观察、终态累计、未发送中止和异常冻结。L1 增加 USD-M 资金预留、费用额度与减仓一致性。
- root 与 ipc_codec 独立审查模型和源码；market_core 独立复核 ACK/中止、重复成交后的新观察、终态后的迟到成交三项真实 Rust 修复。问题来自源码审查及实际回归，未归因为 TLC 发现。
- 固定版本、有限账户和订单域；无跨币种保证金、在线基线切换或与真实发送协调的组合证明。完整源码锚点、配置及逐项范围见[账户路线](execution_models/account/ROADMAP.md)。
- 撤单/查询允许使用已过期的账户基线，新增显式 freeze 接口；旧模型没有这些动作。本次只同步锚点和范围，六组历史结果仍限于原有固定有效基线实例。

## account_journal / L0_ordered_recovery — PASS（2026-10-20）

- 三配置不同状态 52,700 / 748,060 / 125,574，深度 52/52/66；16 项不变量、Init 与 42 个 Next 动作全部通过并触达。
- 创建依赖顺序、写入/同步/收据分离、poison、未回执完整尾、required 精确前缀、结构与历史闭包校验、账户重放和冻结返回。
- root 与 ipc_codec 独立源码/模型审阅通过；582 个源码行锚点零漂移。初次目录同步反例为模型遗漏，已据源码修正并保留记录，无开放源码候选。
- 有限范围只包含一代原意图的 Admit/Aborted、writer 接管及 Spot 数量 1/价格 1/零费实例；物理字节、分段目录事务和网络发送不在本级证明内。见[输出](execution_models/account_journal/L0_ordered_recovery/05_output.md)。
