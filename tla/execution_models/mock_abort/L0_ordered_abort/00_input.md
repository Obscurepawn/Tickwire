# L0 输入

本级验证当前 Mock 中止修复，独立于旧 submission 模型。实际 Red 已在 `docs/validation/mock-abort/red.log` 确认四项断言失败；这些缺陷来自源码审查和 Rust 回归，不归因于 TLC。

- `crates/tickwire-mock/src/lib.rs:76-160`：统一持久事件流、commit 幂等和失败/回执丢失边界。
- `crates/tickwire-mock/src/lib.rs:225-259`：reserve、提交、begin_send、传输结果及先持久事实后应用。
- `crates/tickwire-mock/src/lib.rs:267-292`：终态记录及按事件流恢复，重建失败保留冻结旧 Engine。
- `crates/tickwire-execution/src/ports.rs:52-108`：完整记录绑定、中止重复应用、事实分发。
- `crates/tickwire-execution/src/lib.rs:276-339`：准入、资金预留、发送前 durable 与禁止重复发送。
- `crates/tickwire-execution/src/lib.rs:351-390`：NoBytes、Unknown、Ack。
- `crates/tickwire-execution/src/lib.rs:407-436`：零成交终态的本金释放。
- `crates/tickwire-execution/src/lib.rs:504-528`：恢复 intent 为 MayHaveSent、算术和容量检查、冻结。

输入身份固定为每个有限 order 唯一且完整的合法 DurableRecord；不是只按 intent ID 验证任意输入。门控与对象依赖采用合法成功场景；本级关注持久记录顺序，不重新验证旧 L0 的期限和配置门控。

资金内容映射：`crates/tickwire-execution/src/lib.rs:169-181` 的订单占用与 `crates/tickwire-execution/src/lib.rs:483-501` 的总占用替换，限定零成交零费用时由状态求和。
