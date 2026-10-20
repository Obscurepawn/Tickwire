# L0 反例记录

当前五个配置均为 PASS，没有 TLC 安全反例，也没有开放 candidate。

此前独立源码审查发现的伪造 `Quiesced` 问题由 Rust/Linux red 测试复现：独立纯 Reader 可以产生合法 token，而真实消费者仍持有共享字节引用。修复后的 mapped detach 检查真实已登记 cursor 的 lifecycle=5、三水位和 quiesced_seq。测试见 `crates/tickwire-shm/tests/mapped.rs` 中 `pure_state_quiescence_cannot_detach_a_real_live_borrow`。本级穷举修复后的协议，不将该历史 Rust 测试描述为 TLC 发现。
