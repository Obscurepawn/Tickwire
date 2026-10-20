# 输入与范围

目标为 `crates/tickwire-account-gateway`，采用 faithful fault injection。源代码按单个 `&mut self` 所有者串行执行；调用中间只有外部时间变化、I/O 完成、失败或崩溃，不插入另一个 submit、record_fact 或 cancel_pending。

主要入口：`crates/tickwire-account-gateway/src/lib.rs:212-325` 提交；`:326-399` poll 与取消；`:400-418` 交易结果记录；`:460-525` 发送报告、中止和停止；`crates/tickwire-account-gateway/src/binding.rs:57-119` 依赖绑定与最终门禁；`crates/tickwire-account-gateway/src/recovery.rs:5-115` 恢复核验与 entry 重建。网络首次接受见 `crates/tickwire-transport/src/connection.rs:178-237`。

有限账户实例为 Spot 买入、每单数量 2、限价 1、零费用；每个订单至多一笔数量 1、实际 quote 为 1 的成交。成交可重复报告，并带正常、Unknown、取消且累计为 1、取消但缺累计量四种订单观察。每次 Fill 及其观察是同一次账户方法调用，不拆开交错。Cancel/Query 引用已有订单，各自有独立意图身份、零资源预算；它们仍经过完整持久化与发送流程。

记录类型选择 Admit、Aborted、ACK、Unknown、Fill，以及一个可编码但 reducer 拒绝的交易所 Rejected 事件。事件可以在首字节之前到达，也可在中止或协调器停止后到达。Unknown 只冻结账户；健康协调器仍可发送合法撤单或查询。错误记录保留在日志，恢复不得跳过。

每个意图代表已固定原始代次、命令摘要、策略和目录的完整身份。恢复进程可用较新 writer 代次追加旧订单结果，但不重新发送，也不生成新 CommittedAdmission。模型不枚举字节级身份变体；提交闭包失败、最终完整门禁失败和同意图冲突分别有独立动作，实际字段逐项比较由对应 Rust 接口完成。

存储注入未完成追加、同步错误和同步后丢失结果；网络注入未就绪、阻塞、短写、断连、截止和零 I/O budget。成功同步的前缀不会丢失。恢复成功时没有连接、队列或活跃发送 capability；账户中的持久 Admission 与新进程的 CommittedAdmission 分开表示。

`InputRecordBound` 限制外部输入记录数，内部中止可再追加一条，以保证已开始的调用能够完成。它不表示生产日志容量，也不制造小容量写满错误。`SequenceBase/SequenceLimit` 保留检查加法和失败原子性；边界配置把 `u64::MAX-2..MAX` 平移为有限域 `1..3`，不改变剩余可增加次数。

本级不重做全部多资产公式、USD-M 费用与减仓细节、业务字节编码、TLS 密码协议、物理分段或 manifest 事务。它检查选定真实输入实例的组合顺序；权威门禁、编码器和恢复依赖提供接口所要求的完整证据。
