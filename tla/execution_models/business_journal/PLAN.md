# 业务 WAL 验证阶梯

源码范围是 `tickwire-business-journal` 与它调用的 execution、physical journal、codec、recovery 接口。`Runtime` 私有拥有 engine/journal，通过 `&mut self` 同步调用；FileBackend 持有排他目录锁。模型允许进程在持久化步骤之间崩溃，不引入源码没有的并发写者。

| 层级 | 新增关注点 |
| --- | --- |
| L0_commit_abort_recovery | 闭包先于意图、真实批同步先于发送、等待期间时效门变化、零字节中止先同步后释放、顺序恢复与冻结 |
| L1_economic_replay | 将成交、费用、终态重复和冲突接入同一持久流，检查恢复经济状态 |
| L2_writer_identity | 接管代次、原订单身份、幂等位置与受信恢复起点 |

L0 采用完整事件粒度的持久前缀抽象；物理帧损坏与目录事务由各自阶梯和 Rust 文件测试覆盖。闭包被表示为按订单绑定的持久对象集合，内部逐对象同步在 recovery_closure L0 检查。各层结果不合并为完整生产授权或签名证明。
