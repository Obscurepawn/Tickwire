# L0 输入（已固定）

用户要求按接口、TDD、Specs、TLA+ 顺序推进实现。选择 execution 的提交门禁作为首级，因为其已经有可执行 reducer 和 mock 调用方，且直接决定未确认订单是否继续占用资金。

源码已逐函数读取：`crates/tickwire-execution/src/lib.rs:62` Gate 判定；`crates/tickwire-execution/src/lib.rs:171` held；`crates/tickwire-execution/src/lib.rs:279` reserve；`crates/tickwire-execution/src/lib.rs:311` accept_commit；`crates/tickwire-execution/src/lib.rs:325` begin_send；`crates/tickwire-execution/src/lib.rs:344` abort_unsent；`crates/tickwire-execution/src/lib.rs:355` transport_rejected_without_bytes；`crates/tickwire-execution/src/lib.rs:377` uncertain/not_found；`crates/tickwire-execution/src/lib.rs:387` ack；`crates/tickwire-execution/src/lib.rs:236` update_gate；`crates/tickwire-execution/src/lib.rs:240` freeze。调用方 `crates/tickwire-mock/src/lib.rs:241-243` 在接受回执后调用 begin_send，返回后才调用 exchange。

L0 没有前级输入。独立代码审查已发现且经 Rust 回归修复的费用事件容量冻结、空日志恢复冻结，不作为本级 TLC 发现。生产 transport 和 journal 尚未实现，可信回执与 NoBytesAccepted 由适配器契约提供。
