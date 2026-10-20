# L0 输入

本级从已实现的 `Producer`、`Reader` 和映射 SDK 建立第一个广播 ring 模型。调用者提供合法、固定且互异的 bank/subscription 身份，cursor 由 `Subscriber::attach` 创建，再通过同一身份登记；这些是 `unsafe attach/register_cursor` 的真实前置条件。

- `crates/tickwire-shm/src/lib.rs:130-153`：容量至少 64，且为 2 的幂；本级固定 64。
- `crates/tickwire-shm/src/lib.rs:194-233`：JOIN 从当前 published 建立 pending gate。
- `crates/tickwire-shm/src/lib.rs:235-312`：ReadReady、释放观察、保守缓存与 claim。
- `crates/tickwire-shm/src/lib.rs:322-388`：publish、abort、Freeze、detach。
- `crates/tickwire-shm/src/lib.rs:433-557`：借用窗口、独立 applied/released 连续前缀与 Quiesce。
- `crates/tickwire-shm/src/mapped.rs:209-341`：真实 cursor 登记、Acquire 释放观察、逐槽写入与 Release 发布。
- `crates/tickwire-shm/src/mapped.rs:360-402`：detach 读取已登记 cursor 的 lifecycle=5 和精确水位。
- `crates/tickwire-shm/src/mapped.rs:405-547`：独立消费者映射、借用 guard、释放、应用和 Quiesce。
- `crates/tickwire-shm/src/mapped.rs:550-581`：guard 保存 `NonNull<[u8]>`，仅在 `bytes(&self)` 中构造短借用，Drop 推进 released。本级转录其借用生命周期，不验证 Rust 语言别名规则。

此前的伪造 `Quiesced` 问题由 Rust/Linux 测试复现并由父任务修复。本级针对修复后的源码，不把私有 token 字段视为认证来源。TLC 不执行 mmap，也不提供操作系统或 CPU 内存模型证明。
