# 终态费用维护验证

run7 已完成成交入账，但没有进入终态费用查询。只读核对发现原控制器在账户健康时直接返回；一次成功对账后也没有清除订单已加载标记。账户恢复 Unknown 与费用尾额保留符合原契约，修复目标是控制器的下一轮工作调度。

先提取原控制器的订单工作状态与健康短路条件，再运行以下两个行为测试。初版保留原有分支，编译成功后两项断言均失败；随后增加终态费用维护条件并在成功后清除订单工作状态。

```sh
cargo test -p tickwire-app --lib reconciliation::tests -- --nocapture
```

- `healthy_terminal_fee_maintenance_is_bounded_and_idle_only`：健康、空闲、到达间隔且确有终态费用责任时必须启动；无责任、发送忙或间隔未到时不启动；已冻结的恢复流程继续推进。
- `successful_attempt_reloads_new_orders_in_the_same_private_cut`：初次空订单对账完成后，即使连接切点不变，也须加载后来出现的订单；重试清空当前订单、页游标和观察状态。

[red.log](red.log) 为实际 0 passed / 2 failed，[green.log](green.log) 为实际 2 passed / 0 failed。[results.json](results.json) 保存测试名和本次源码摘要。此次只运行这两个纯单元；真实 order/myTrades/FeesReconciled/第二次激活的运行场景由后续场景记录报告。
