# Physical journal 路线

| Level | 范围 | 状态 | 下一步 |
| --- | --- | --- | --- |
| L0_batch_sync_recovery | 同步、收据、尾部恢复、可信前缀 | 三配置 PASS；独审完成 | L1 |
| L1_segment_manifest | 目录事务与分段恢复 | planned；Rust 已实现/测试 | 从实际 Store 调用顺序转录 |
| L2_dependency_closure | 依赖闭包与交易提交证明 | planned | 上层实现后再建模 |
