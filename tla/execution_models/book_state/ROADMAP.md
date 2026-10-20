# Book 状态模型路线

| 层级 | 关注点 | 状态 | 证据 |
|---|---|---|---|
| L0_lifecycle | generation、snapshot→bridge→live、错误拒绝与重建、view 创建时 freshness | PASS，四组，19 动作全部覆盖 | [输出](L0_lifecycle/05_output.md) |
| L1_coverage | 隐藏价格见证、Complete/Through/Unknown、Known quantity/best 正确性 | PASS，四组，15 动作全部覆盖 | [输出](L1_coverage/05_output.md) |

没有开放源码候选。后续范围为真实行情/策略适配器接入后的 generation 消息来源、快照完整性与风险读取契约；当前阶段不启动新模型。
