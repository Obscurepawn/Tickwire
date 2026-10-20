# 形式验证路线

| 阶梯 | 当前范围 | 状态 | 后续 |
| --- | --- | --- | --- |
| execution/submission L0 | 预留、回执画像、发送、未知结果、未发送撤销 | PASS | L1 |
| execution/submission L1 | 日志回执丢失、提交失败、正常等待与未知结果、发送边界崩溃、恢复 | PASS | L2 |
| execution/submission L2 | 成交/费用/终态、重复/冲突、容量/上界、Mock 事实重放 | PASS v2；L2-001 已修复验证 | 真实日志崩溃边界与 L3 激活 |
| book_generation | Spot/Futures 桥接、旧 generation、容量失效 | 未建模 | 接口与单元测试已实现 |
| risk_reservation | 多资产预留与费用清算 | 未建模 | 衔接 execution 的资金分录 |
| broadcast_ring L0 | 固定 bank 发布、慢消费者、借用和协作回收 | 五配置 PASS | L1 身份控制与 bank 轮换，L2 进程退出回收 |
| budget_lease / strategy_handoff / multi_exec_activation | 授权、旧发送者隔离与多进程恢复 | 未建模 | 先实现可检验的控制面与运行时 |

下一命令和开放问题位于 [submission TODO](execution_models/submission/TODO.md)。
