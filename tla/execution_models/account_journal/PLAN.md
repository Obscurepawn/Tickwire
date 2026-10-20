# 账户日志验证计划

目标源码是 `crates/tickwire-account-journal`，按实际单所有者 API 验证记录持久化和冻结恢复。`AccountJournal` 的可变调用由一个所有者串行执行，FileBackend 持有同目录的排他文件锁；调用间不引入另一个写者，进程可以在 I/O 和结果发布之间退出。

| 层级 | 范围 |
|---|---|
| L0_ordered_recovery | 基线与 bootstrap 创建顺序、同步追加收据、poison、完整尾恢复、精确前缀检查、历史闭包顺序与冻结账户 |


物理 WAL 的帧、段和 manifest 格式不在本阶梯另建。L0 使用现有物理层接口完成点；历史前缀摘要只绑定 WAL 记录，闭包对象服务不作为完整日志副本。账户费用及资本公式已有独立阶梯，本级只选择可以明确重放的有界 Admit/Aborted 输入。

AccountGateway 的候选账户、输出与中止顺序由独立 `account_gateway` 阶梯检查。本阶梯下一层的范围待 L0 结果和新的日志职责明确后再制定。
