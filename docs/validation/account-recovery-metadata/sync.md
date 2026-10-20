# 既有账户模型的源码同步

账户恢复元数据修复把交易所观察到的 Unknown、恢复附加的 Unknown 和终态观察异常分别保存在订单中。重复成交是否包含新观察，改由订单的持久观察字段判断；本地冻结位不再参与这个判断。普通序号漂移、序号上限和冻结后的终态观察由新增 Rust 回归及 AccountGateway 组合模型检查。

这次检查涉及三个既有模型：

| 模型 | 原来保留的状态 | 修复对其转移的影响 |
| --- | --- | --- |
| Account L0 Spot facts | 成交/费用、资金和归属、终态、Unknown、冻结 | 没有恢复入口与账本序号。`unknown` 对应 `observed_unknown`；新增观察字段仅改变本层未保存的序号及返回分类。资金、归属、预留和冻结转移保持相同 |
| Account L1 reduce shadow | L0 状态及 USD-M 资金、费用额度、减仓分配 | 同样没有恢复入口与账本序号；资金与减仓公式未改，重复观察继续执行相同的 Unknown/终态处理 |
| AccountJournal L0 ordered recovery | Admit/Aborted 重放、账本序号、返回冻结和有效 Unknown | 有账本序号，但输入域没有成交、费用或终态观察。恢复附加 Unknown 的存储字段改变，公开有效 Unknown 与 Admit/Aborted 的序号变化相同 |

三个模型的动作、常量、配置和不变量均保持原文，只更新源码行号注释。没有重跑 TLC，原统计仍表示各自原有限输入域的结果；它们不承担新增的“恢复后重复观察与完整重放具有相同账本序号”性质。

`pre-sync/` 保留提交 `2ab7b1a` 中三个模型、配置、锚点锁和统计文件；`pre-fix-source/` 保留同一提交的四个账户源文件。原级别的 `00_input.md` 与 `05_output.md` 没有修改。更新锚点时，在临时工作目录中换算历史输入文档的行号，再用 `anchors.sh update/check` 生成并核对当前锁。逐文件摘要、注释外文本相等检查和锚点结果在 [sync-results.json](sync-results.json)。
