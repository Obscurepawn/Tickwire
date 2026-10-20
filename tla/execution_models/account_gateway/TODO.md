# 账户发送协调待办

已关闭：恢复附加 Unknown 与主动冻结遮住持久观察变化，导致在线账本序号和重放不同。四项 Rust 回归及 v2 四配置通过，finding 为 retired(fixed, validated by L0_AccountGateway_v2)。

当前没有开放候选。后续范围为实际响应关联、连接重建、恢复激活和基线切换，等待对应实现后再安排层级。

保留观察项：`pre-metadata-review/` 的旧模型故意表示修复前代码；`pre-success-prefix/` 表示新增成功前缀性质之前的一轮。当前源码同步只针对 `02_models/L0_AccountGateway_v2.tla`。账户冻结与协调器停止仍是不同状态；恢复账户保留 Admission，但没有活跃 CommittedAdmission。
