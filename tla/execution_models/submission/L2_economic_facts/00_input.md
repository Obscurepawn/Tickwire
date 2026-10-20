# L2 输入：订单交易结果记录

L0/L1 已分别检查提交资格与外部提交回执、崩溃恢复。L2 继承“同一 intent 经合法 reserve、commit、begin_send 后开始接收事实”的入口，新增成交、手续费、终态、重复内容与日志重放。入口可在余额允许时加入第二笔订单，检查共享资金账本。

权威源码是当前工作树：

- `crates/tickwire-execution/src/lib.rs:138-245`：订单字段、held、事件槽去重、Engine 初始化与冻结。
- `crates/tickwire-execution/src/lib.rs:262-339`：预算与合法 reserve/commit/begin_send 入口。
- `crates/tickwire-execution/src/lib.rs:352-405`：NoBytes、未知发送、ACK、取消请求/拒绝。
- `crates/tickwire-execution/src/lib.rs:407-501`：terminal/fill/fee、原子 replace、超预算冻结。
- `crates/tickwire-execution/src/lib.rs:504-528`：从提交记录恢复预留。
- `crates/tickwire-execution/src/ports.rs:51-80`：交易结果记录分派。
- `crates/tickwire-mock/src/lib.rs:104-114`：事实追加，包含重复与随后 apply 失败的事实。
- `crates/tickwire-mock/src/lib.rs:236-278`：先保存事实再 apply；重放全部成功后才替换旧 Engine。

相关 Rust 测试位于 `crates/tickwire-execution/tests/lifecycle.rs` 与 `crates/tickwire-mock/tests/services.rs`。零费用也确认发送、矛盾 NoBytes 冻结、显式 uncertain 冻结已经由源码确认。

本级不重证真实 journal 持久化、交易所真实性、完整保证金、费用尾账释放或多实例 fencing。Mock 的同步 `record` 和 `restart` 各作为完整调用；append/apply 之间的进程崩溃属于后续扩展，本级保留 append 失败、apply 失败和有序重放的结果。所有源码锚点在关闭前按完整函数范围锁定。
