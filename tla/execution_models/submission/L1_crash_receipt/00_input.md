# L1 输入（已固定）

本级在 L0 已通过的单所有者提交门禁上加入外部记录存活与进程崩溃。源码中的显式 Unknown 冻结共享池，正常 PendingResponse 保持等待状态；两种行为均纳入本级。

实际调用链：`crates/tickwire-mock/src/lib.rs:210` submit 先 reserve、写恢复依赖，再 resume；`:220` resume 先 journal.commit、accept_commit，再 begin_send、exchange.submit 和回调。`:116` commit 在检查依赖字节和 identity/sequence 冲突后保存记录，之后才可能丢回执。`:163` MockExchange 记录真实调用次数，除 NoBytesAccepted 外保存 accepted request。`:265` restart 创建新 Engine、立即 freeze，重放所有 durable intents 和事实。

Engine 恢复 `crates/tickwire-execution/src/lib.rs:504-528` 的 recover_intent 将订单置为 durable、MayHaveSent，并恢复全部预算。此级不调用交易结果记录 API，所以 restart 的事实循环为空。生产 journal 与外部交易所协议尚未实现，结论针对现有 mock 服务契约。
