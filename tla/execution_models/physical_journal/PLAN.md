# Physical journal 验证阶梯

目标是核对 `tickwire-journal` 的实际物理提交和恢复行为。独立于 `submission` 的 Mock 对象模型；下层结论不能替代恢复依赖闭包和交易授权。

并发来自源码：`Journal::append_batch(&mut self)` 独占 writer，`FileStore` 持有目录锁。单调用内部会分段写入、等待同步、发布收据；故障可以发生在这些边界。恢复持有同一个目录锁，验证整个目录后才截断最后未封口尾部。外部程序绕过 advisory lock 或修改可信目录不在并发保证内。

| 层 | 增量 | 状态 |
| --- | --- | --- |
| L0_batch_sync_recovery | 固定段内写入/同步/收据、失败、崩溃尾部、外部可信前缀 | 三配置 PASS |
| L1_segment_manifest | 建段、目录同步、临时 manifest、rename、孤儿段接纳与缺段 | 后续；当前 Rust 已实现并测试 |
| L2_dependency_closure | 内容寻址闭包、输入覆盖与完整 LocalCommitProof | 先实现上层闭包验证 |

L0 的文件内容保留字节顺序的 frame token 抽象，hash/CRC 运算由 Rust 测试另行核对。默认采用成功同步字节不丢、未同步末尾可撕裂的故障模型；另外注入完整已确认尾部丢失来检查 `required_frontier` 的检出能力。没有外部前缀时不把这种不可判定的媒体丢失纳入无丢失承诺。正式掉电测试尚未执行。
