# 历史交接操作的停止范围

`Inbox::halt_state` 使用生产 helper `halt_operation`。旧实现先按逻辑策略 ID 无条件撤销授权，导致 op1 Released 后，op2 已 Prepared 的下一轮旧状态失效检查关闭 op2；迟到的 op1 Halt 也有同样后果。

[red.log](red.log) 是真实 ControlPlane 上执行两次连续交接后的单元结果：两条后继 gate 保留断言失败，当前操作故障关闭测试通过。修复先比较完整 gate binding；无 gate 时只核精确旧/新实例授权，并删除新 Halt 状态构造时的无条件关闭。[green.log](green.log) 的 5 项测试通过，包含完整成员绑定不符和无 gate 的首次 Halt。

实际命令：`cargo test -p tickwire-app --lib operation_halt_tests`。测试直接调用生产停止 helper 与真实内存 ControlPlane，未运行完整文件 inbox 或进程场景。源码摘要见 [source-hashes.json](source-hashes.json)。
