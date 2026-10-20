# 账户日志形式验证

| 层级 | 状态 | 结论 |
|---|---|---|
| L0_ordered_recovery | PASS | 3 cfg；52,700 / 748,060 / 125,574 distinct；16 项不变量；42 个 Next 全覆盖 |

AccountGateway 的资金候选、首次输出和持久中止由独立 `account_gateway` 阶梯继续检查。L0 没有发送授权结论。
