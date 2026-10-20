# 执行发送与恢复阶梯

本计划在 L0 开始时固定。目标是检查真实 `Engine` 与 `MockRuntime` 的发送完成边界：未持久化不得进入发送，发送结果未知继续保留资金，同一 intent 不重复提交，exec 重启不自动恢复发送权限。

现有 API 通过 Rust `&mut self` 串行修改 Engine。MockRuntime 的提交操作也同步独占自身；模型把 reducer 方法作为原子动作，把 runtime 内部 commit、接收回执、begin_send、exchange.submit 之间作为允许进程崩溃的位置。没有把源码中的一段更新拆成两个并发写线程。

| 级别 | 新增关注点 | 模式 | 检查对象 |
| --- | --- | --- | --- |
| L0_commit_gate | 单所有者订单提交状态机 | faithful transcription | reserve、accept_commit、begin_send、ack、uncertain/not_found、abort_unsent、NoBytesAccepted、gate 更新 |
| L1_crash_receipt | 外部持久记录与 exec 崩溃恢复 | faithful fault injection | dependency 写入、journal commit/失败/丢回执、发送前后崩溃、restart/recover_intent |
| L2_economic_facts | 成交/费用/终态的重放与未知累计成交 | 计划 | fill、fee、terminal、去重容量与冲突冻结 |
| L3_activation | 恢复闭锁后的新 epoch 激活 | 计划 | 尚缺少完整 runtime 激活代码，先不编造 |

L0 将可信回执来源抽象成独立 local/replica 完成集合；L1 用真实 MockJournal 的调用顺序替换这个边界。只有正向“接受可信完成信息”的逻辑被验证；真实磁盘、远端复制、账户对账、旧进程 fencing 继续留在后续验证范围。

`Gate::permits` 的布尔门控在模型中合并为一个谓词，其值由调用者更新。模型不证明调用者按真实时间刷新 `Gate.now`；配置版本、行情 freshness、账户恢复、epoch/generation 等每个实参的含义已经由实际代码和单元测试固定。
