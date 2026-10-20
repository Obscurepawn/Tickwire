# 账户验证进度

| 层级 | 状态 | 已完成范围 |
|---|---|---|
| L0_spot_facts | PASS，v3 | 3 配置，10 不变量，21 Next 动作全覆盖；Spot 双资产、费用与订单观察 |
| L1_reduce_shadow | PASS | 3 配置，12 不变量，22 Next 动作全覆盖；USD-M 减仓、H/费用覆盖/D |
| L2_closed_baseline | 等待实现 | 存活旧单 coverage 导入、在线闭合基线替换 |

最终六配置均完成独立源码映射审阅、真实 TLC、动作覆盖和源码锚点。历史模型草稿保留在 L0 的 pre-review 目录；最终结论只引用各级 05_output。
