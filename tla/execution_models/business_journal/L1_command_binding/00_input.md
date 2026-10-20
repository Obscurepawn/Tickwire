# L1：命令摘要与恢复依赖

本层沿用 L0 的单所有者执行、物理完整前缀及失败后冻结，增加实际命令摘要、闭包中的命令依赖和同意图参数冲突。源码包括 `tickwire-execution`、`tickwire-journal-codec`、`tickwire-business-journal` 和 `tickwire-recovery`。

验证问题：不同命令能否复用同一意图的提交权限；缺少完整命令对象的闭包能否进入 WAL；崩溃后恢复的摘要是否仍等于原摘要。

模式为 faithful fault injection。故障包括授权/闭包校验失败、完整日志写入或同步返回失败、一次或两次进程崩溃，以及恢复校验失败。字段编码的具体字节、schema 1 拒绝和摘要计算通过 Rust/Python 独立样本检查；本模型不把哈希碰撞、密码学算法或网络编码当作已证明性质。
