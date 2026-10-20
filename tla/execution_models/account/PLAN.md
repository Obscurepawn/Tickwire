# 账户预留验证计划

源码范围为 `crates/tickwire-account`。每个 `Account` 只有一个可变所有者；公开方法通过候选副本计算再整体替换，模型以一次公开调用为原子动作。调用方负责先同步事实 WAL，模型不替调用方建立持久化完成点。

| 层级 | 内容 |
|---|---|
| L0_spot_facts | 两种资产、有界买单、费用、终态、迟到和重复事实、中止与未知状态 |
| L1_reduce_shadow | USD-M 独占减仓分配、实际仓位、费用消耗与 venue shadow |
| L2_closed_baseline | 已存在订单 coverage、闭合基线替换；须先完成对应实现 |
