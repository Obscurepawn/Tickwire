# Recovery closure 独立源码审阅

结论：两处文件存储问题已修复，当前审阅范围内没有未解决的阻断项。此次工作为源码审阅；下面两条失败序列没有由审阅者执行文件系统故障注入或进程工作目录切换实验。

1. `mkdir` 成功、父目录同步失败后，下一次 `open` 走 `AlreadyExists` 分支，旧实现没有再次确认根目录项持久化。当前 [FileBlobStore::open](../../../crates/tickwire-recovery/src/files.rs#L15) 在创建和重开两种分支后都同步根目录及父目录（第 47–55 行）。这关闭了失败重试绕过父目录同步的入口。
2. 旧实现保存调用者的相对目录。若进程随后改变工作目录，后续对象访问可能指向另一目录，而锁仍属于原目录。当前 `open` 在第 32 行调用 `canonicalize`，保存绝对路径。此问题以进程改变工作目录为触发条件；未在当前业务运行中观察到该条件。

其余逐项检查覆盖 [manifest 验证及闭包发布](../../../crates/tickwire-recovery/src/lib.rs#L160)、[规范 CBOR 解码](../../../crates/tickwire-recovery/src/codec.rs#L44)、[输入块连续性](../../../crates/tickwire-recovery/src/inputs.rs) 和 [文件完成点](../../../crates/tickwire-recovery/src/files.rs#L98)：对象集合和 DAG 形状、引用范围及去重、长度/hash、非最短和不定长编码拒绝、输入流身份/序号/前驱 hash、对象与 manifest/binding 再同步均有对应检查。

`VerifiedClosure` 的结论依赖 `BlobStore` 成功同步的契约。kind 1–11 的业务内容及授权语义由可信 authorizer 检查；此处验证对象完整性和引用闭包。内存 Mock 的成功同步只代表模拟持久域。`verify(&mut self)` 的重新同步修复由实现者先行完成，本次确认其调用顺序，没有将它记作本次独审发现。
