# 实际策略进程交接

动态加载、认证 READY 与退出条件见[策略运行接口](../../docs/implementation/strategy-runtime-interface.md#动态加载与交接)。账户管理权仅由同步的 `StrategyHandover` 改变，原订单归属、现金、费用、预留和未知发送状态继续按账户日志处理。

本次修复针对 Host 原先忽略 `Registry::terminate` 错误却返回交接成功的行为。实际空 Registry 的构造测试先确认该错误被吞掉，再改为必须取得真实 `ExitObservation`。Multi 的针对性测试检查旧实例退休返回失败时，不调用新实例激活，两个实例继续暂停。

对应 Red、Green 和源码摘要保存于[策略退出记录](../../docs/validation/strategy-retirement/README.md)。执行端对历史交接的停止操作比较完整 gate binding；连续交接中，op1 的失效检查或迟到 Halt 不关闭 op2 的 gate。无 gate 时，只能按精确旧/新实例授权停止。真实 ControlPlane 上的 5 项针对性结果见[历史操作停止记录](../../docs/validation/lifecycle-operation-halt/README.md)。

实际进程场景由 `tickwire-lab` 负责：旧 Active 订单 ACK 后暂扣成交，动态启动新 Shadow，核对两者消费同一账户前缀；真实生命周期 CLI 生成签名 Prepare、Commit、Release。测试传输保持原始签名字节，先发布新授权文件再转送 Commit；Release 只能在真实 Committed 回执之后生成，执行端处理下一轮请求前先应用控制 inbox。随后核对旧 PID 退出、管理权记录恰好一次，以及旧单迟到成交由新管理者接收且金额只入账一次。新意图放行和重启行为另设明确阶段，不用固定等待时间代表完成。

上述进程场景尚待本轮实际运行；本文件不将构造单元测试视为进程交接验收。
