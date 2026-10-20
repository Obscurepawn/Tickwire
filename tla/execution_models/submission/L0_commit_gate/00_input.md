# L0 输入（已固定）

用户要求按接口、TDD、Specs、TLA+ 顺序推进实现。选择 execution 的提交门禁作为首级，因为其已经有可执行 reducer 和 mock 调用方，且直接决定未确认订单是否继续占用资金。

源码已逐函数读取：`crates/tickwire-execution/src/lib.rs:60` Gate 判定；`:169` held；`:276` reserve；`:308` accept_commit；`:322` begin_send；`:341` abort_unsent；`:352` transport_rejected_without_bytes；`:374` uncertain/not_found；`:384` ack；`:234` update_gate；`:238` freeze。调用方 `crates/tickwire-mock/src/lib.rs:220` 在接受回执后调用 begin_send，返回后才调用 exchange。

L0 没有前级输入。独立代码审查已发现且经 Rust 回归修复的费用事件容量冻结、空日志恢复冻结，不作为本级 TLC 发现。生产 transport 和 journal 尚未实现，可信回执与 NoBytesAccepted 由适配器契约提供。
