# L1 输出（已固定）

结果为 PASS。primary 为 1 个 intent、最多 1 次崩溃，共 1,744 个不同状态、最大深度 14；scaling 为 2 个 intent、最多 2 次崩溃，共 96,828 个不同状态、最大深度 22；pending 为 2 个 intent、最多 1 次崩溃，资本足以容纳两笔正常等待订单，共 186,148 个不同状态、最大深度 22。三个配置都穷举结束；全部 19 个 Next 动作触发，具体计数见 [运行记录](03_run_log.md)。没有 TLC 反例或开放 candidate。

本级检查了：实际调用交易所前已有符合 Local/Replica 画像的保存记录；每个已接受请求都能在 journal 找到；同一 intent 至多调用一次 exchange.submit；恢复依赖对象与记录保持一致，记录 sequence 唯一；Unknown 保留预算；进程重启后始终冻结准入，包括空 journal。写入成功但回执丢失、日志提交失败、发送前后崩溃均由独立动作覆盖。

恢复允许重新预留此前本地撤销但未留下终态事实的 durable intent，导致占用大于初始资本；此时 frozen 为真。因此不变量检查的是“未冻结的活动引擎不会超额准入”，没有把恢复期间的保守超额占用误判为超发订单。

## Fidelity & coverage

- Fidelity: 19 transitions modeled, all cited file:line; 0 UNCLEAR/TODO within the stated scope; abstractions listed in [01_reasoning.md](01_reasoning.md). 两个历史计数分别保留 dispatch 与 accepted 行为，避免只检查状态标签。
- Code coverage: modeled all 6 selected mock API surfaces — MockKv.put_immutable、MockJournal.commit、MockExchange.submit、MockRuntime.submit/resume_submit/restart — and 11 of the 16 Engine mutating APIs from L0's denominator, adding recover_intent. 经济事实重放循环固定为空；服务错误有抽象，具体字段与算术边界由 Rust 测试覆盖。这是设计接口覆盖，不是行覆盖率。
- Model coverage: TLC --coverage — all 19 Next actions fired in all three configurations, plus Init. scaling 中 CommitAndLoseReceipt 产生 65,424 个后继状态；Crash 产生 76,328 个，Restart 产生 5,944 个；均来自实际运行输出。

`risk_core` 独立复核了同步调用栈、持久内容与回执区别、恢复预算和冻结前提，结论为在提交/回执丢失/崩溃范围内忠实。每个 intent 具有独立 recovery_object；跨 intent 共用 key 冲突、真实 fsync/复制、fill/fee/terminal、事实日志重放、多 exec fencing 和恢复后新 epoch 激活未建模。PendingResponse 保留 MayHaveSent 且不主动冻结；本级未加入其后异步 ACK 调用，独立 ack 迁移已在 L0 覆盖。

现有代码没有解冻 API，所以本级没有“恢复后最终能继续交易”的活性结论。下一步为 L2 经济事实和重复回报，再为已实现的激活逻辑建立 L3。
