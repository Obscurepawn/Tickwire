# 账户对账与终态费用维护

实现位于 `crates/tickwire-app/src/reconciliation.rs`，调用边界见[控制器接口](../../docs/implementation/reconciliation-controller-interface.md)。

正常已对账账户仍会检查终态费用责任。只有存在 `terminal && !aborted && !fee_reconciled` 的订单、REST 发送器空闲且达到 `fee_maintenance_interval_ns` 时，才启动下一轮维护。该字段省略时为 1 秒，零值拒绝；失败仍按 `retry_ns` 等待。未对账或已冻结账户继续原有恢复流程，不受健康账户维护间隔阻挡。

每轮重新从当前账户取得订单集合，按订单查询、从零开始的成交分页、费用闭合、三类远端快照的顺序推进。原有共享额度、分页与 I/O 上限以及同步完成点不变。成功后清除本轮订单扫描状态并设置下次维护时间；失败或连接切点变化也清除本轮状态。相同连接和缺口代次不允许复用上轮空订单集合。

账户在完整对账期间保持现有冻结规则，私有回报继续持久入账。FeesReconciled 仍由完整成交页覆盖证明驱动，Reactivated 仍要求实际快照与当前账户匹配。此变更没有增加直接清除 Unknown、释放费用或绕过快照校验的入口。

两项纯单元分别验证维护触发/限频/发送忙边界，以及同一私有切点完成后的订单重新加载。实际 Red/Green 命令与结果见[验证记录](../../docs/validation/reconciliation-maintenance/README.md)。
