# 形式验证路线

| 阶梯 | 当前范围 | 状态 | 后续 |
| --- | --- | --- | --- |
| execution/submission L0 | 预留、回执模式、发送、未知结果、未发送撤销 | PASS | L1 |
| execution/submission L1 | 日志回执丢失、提交失败、正常等待与未知结果、发送边界崩溃、恢复 | 历史版本 PASS；Mock 已改为有序事件重放 | 现行中止与重放见 mock_abort |
| execution/submission L2 | 成交/费用/终态、重复/冲突、容量/上界、Mock 事实重放 | 历史版本 PASS v2；L2-001 已修复验证 | 当前事件顺序下的交易结果记录组合模型与 L3 激活 |
| mock_abort L0 | 未发送中止先保存再释放，按事件顺序恢复，失败保持冻结 | 三配置 PASS | 成交、费用与终态的完整组合 |
| recovery_closure L0 | 对象、manifest、绑定、同步结果与验证结果交付 | 三配置 PASS | 依赖释放、多持有者及回收 |
| business_journal L0 | 恢复依赖验证、WAL 同步、最终发送检查、中止和冻结恢复 | 三配置 PASS | 交易结果记录、同步副本及恢复后激活 |
| book_state L0/L1 | Spot/Futures 桥接、旧 generation、失败原子性与有限深度知识 | 八配置 PASS | 真实适配器、快照/增量接线与 exec 行情门禁组合 |
| risk_reservation | 多资产预留与费用清算 | 未建模 | 衔接 execution 的资金分录 |
| broadcast_ring L0 | 固定 bank 发布、慢消费者、借用和协作回收 | 五配置 PASS | L1 身份控制与 bank 轮换，L2 进程退出回收 |
| join_owner L0 | 保留快照基线、双资源交付、真实游标追平、原登记版本和协作退出 | 四配置 PASS | 持久请求登记、独立截止时间、bank 轮换及死亡回收 |
| control_requests L0/L1 | 请求登记、终态重放、期限与续租 | 六配置 PASS | L2 真实持久 owner 恢复 |
| physical_journal L0 | 物理批写入、同步、收据、崩溃与恢复 | 三配置 PASS | L1 分段/manifest 目录事务 |
| budget_lease / strategy_handoff / multi_exec_activation | 授权、旧发送者隔离与多进程恢复 | 未建模 | 先实现可检验的控制面与运行时 |

下一命令和开放问题位于 [submission TODO](execution_models/submission/TODO.md)。
