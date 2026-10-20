# 反例与审阅

四组最终运行未出现不变量反例。草案曾因无待发项时读取 `fresh[0]` 而发生 TLC 求值错误，改为明确的条件分支后继续；该错误属于模型自身，没有对应的源码缺陷。二次逐行核对还补上 `TryFailure` 的 `accepted < Bytes` 条件：报文已全部接受时，真实 transport 提前返回，不会再调用 writer.write。

root、risk_core 与 market_core 独立对照 `JournaledSender`、transport 和模型，未发现阻断项。审阅确认保留：失败同步的完整尾记录可能存活、单实例公开调用不可重入、多个已冲刷待回包意图、零接受 write 错误当次尚未冻结、关闭连接上的后继 queue 失败、中止先同步后发布，以及恢复不重建队列。

根任务测试中的 fixture 参数顺序和测试服务器 nonblocking 设置修正属于测试输入与服务器设置，没有作为本级的实现反例。实际 Rust 回归由根任务单独归档。

market_core 的补充审阅指出，零 IoBudget 错误会保留连接和发送报告，不能归并成断连。最终模型加入 DriveBudgetError，再运行全部配置；它保持源码已有的冻结行为，不涉及 Rust 修复。
