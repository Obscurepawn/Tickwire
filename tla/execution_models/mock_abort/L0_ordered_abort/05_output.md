# L0 结果

三组有限配置均 PASS。持久中止先于实际释放；完整恢复后，持久中止或零成交终态的资金不重新占用，没有释放事实的已提交订单仍持有预算。按实际事件顺序重建不发生本级算术上界错误；恢复始终冻结且没有重复发送。

## 忠实度与覆盖

15 个状态动作全部有源码锚点，三组 generated 计数均大于 0。所选路径覆盖 17 个函数入口：MockRuntime 的 submit/resume_submit/record/record_terminal/restart，MockJournal 的 commit/append_fact，MockExchange.submit，Engine 的 reserve/accept_commit/begin_send/transport_rejected_without_bytes/uncertain/ack/terminal/recover_intent，以及 ExecutionFact.apply。这个分母指本级选取的函数入口；逐字段输入校验及所有错误分支没有全部建模。

模型保留提交失败、回执丢失、四种传输结果、事实保存失败、持久与应用之间崩溃、局部恢复和最终替换。具体抽象见 [01_reasoning](01_reasoning.md)：固定完整请求、零成交零费用、LocalDurable、至多一次终态及一次中止、无重新授权。公开 Engine/Journal 的直接外部修改不属于本级调用者；错误持久事实的失败恢复由 Rust 测试验证。

没有新 TLC candidate。源码/Rust 已确认的旧缺陷由本次实现修复；risk_core 已独立对照源码与模型，未发现阻断项：确认完整记录绑定、受限重复中止、统一事件恢复及本级零成交零费用范围。审阅者没有重复运行三组 TLC。此文件随本级关闭冻结。
