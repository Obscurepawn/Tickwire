# 交接输入缺失的实际 Red / Green

旧 runtime 和 SDK 只验证 `TWINP001` 外层格式及独立的账户摘要对象，没有检查输入对象是否真正保存了账户 callback；replica 已有更严格的检查。此次先修正 runtime 正例夹具：账户基线改用生产 `TWAI0001` envelope，随后新增两个行为拒绝测试。

`red.log` 来自真实编译后的运行：原有两项通过；删除基线 callback、删除已持久现金流 callback 的两项拒绝断言失败。两种变更均重新编码输入并更新 input、完整 seal、账户交接记录中的摘要，因此失败不是损坏 hash 的简单拒绝路径。修改前的三处实现保存在 `pre-fix/`。

修复新增 `tickwire-handover`，runtime、SDK、replica 共用同一个严格解析和账户重放校验器。runtime 还比对实际 prior WAL 的规范字节。SDK 的直接 `objects()` 回归确认：完整闭包可读，重新计算摘要但移除账户 callback 的对象集被拒绝。

定向结果：

- shared：6 项通过，包含缺失、重复、乱序、裸 payload、跨基线、序号、ledger/orders 以及实际历史比对。
- runtime：4 项通过，包含两个实际 Red 的修复回归。
- SDK handover：3 项通过，包含直接对象读取的拒绝回归。
- replica：8 项库测试通过。

命令、日志及最终源码摘要记录在 `results.json`。`shared-green.log`、`replica-green.log` 为首次 Green；`final-core-green.log`、`runtime-green.log`、`sdk-green.log` 对应最终格式化后的文本。本轮未运行全仓、Linux 场景或形式验证。

该校验确认账户恢复数据完整且可重放。它不证明策略程序已执行这些 callback；认证会话、报告游标完成情况和 release 授权仍由调用者各自验证。
