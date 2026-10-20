# L0 后续使用

当 `Producer` 的 claim/membership、`Reader` 的连续完成算法或 mapped 的 cursor evidence 发生改变时，先运行 anchors check，再逐项复查语义；相同行号哈希只能检测文本漂移。

L1 等待实际身份控制面和 bank 轮换实现。模型应加入新旧 generation、实际 cursor capability、Freeze cutover 和读者迁移；固定身份的 L0 PASS 不替代这些规则。退出回收另在 L2 增加真实进程身份及所有映射持有者退出证据。

并行源码审阅发现 `RecordBorrow` 内部长期保存 `&'a [u8]` 与 Rust 函数入参嵌套引用的生存规则存在冲突：guard 按值进入函数后可以提前 Drop，released 随之允许槽位复用，但函数尚未返回。root 与 market_core 对照 Rust Reference 和标准库 guard 实现后，将字段改为 `NonNull<[u8]>`，仅在 `bytes(&self)` 中构造受 guard 当前借用限制的引用。这是语言别名契约的源码审阅修复，没有实际误编译复现，也不是 TLC 发现。变更不改变 L0 的借用、释放或回收状态迁移，源码哈希和 anchors 已重新锁定；native/Linux 回归由父任务统一归档。
