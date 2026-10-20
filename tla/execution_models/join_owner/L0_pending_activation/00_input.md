# 输入

验证目标是 `JoinOwner` 与真实 mapped Publisher 的接缝。源码范围：`crates/tickwire-join/src/lib.rs:90-664`、`crates/tickwire-join/src/mapped.rs:9-309`、`crates/tickwire-shm/src/mapped.rs:205-407`；底层 Reader 的连续水位与借用规则见 `crates/tickwire-shm/src/lib.rs:417-558`。

root 初审 SDK 新接口，risk_core 独立审完整 JOIN owner/consumer 和真实三进程测试，未发现阻断项。编译通过的真实 Red 用例捕获了新增实现中的重复 READY 重新追平、冲突重复 Quiesce ACK 被接受两个问题；修复和 Linux Green 在 `docs/validation/join/`，不归因于 TLC。

前置结果：broadcast_ring L0 已检查容量 64、保守缓存最小水位、独立 released/applied、实际 cursor Quiesced 证据；control_requests L0/L1 已检查请求重放及单调租约。本级固定通过认证的唯一 owner/subscriber 身份和只读 FD，错误控制绑定归约为不相等符号，期限归约为 JOIN 未到期/已到期。OS syscall、摘要算法、字节布局、unsafe peer 合同及 CPU 弱内存另由源码/真实测试约束。
