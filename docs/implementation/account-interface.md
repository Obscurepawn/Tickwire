# 多资产账户与订单预留接口

`tickwire-account` 读取完整 `CanonicalCommand`、账户快照和版本化目录，计算并原子占用资源。`Account` 由一个所有者调用；方法先计算候选副本，再整体替换。`Admission` 字段私有，返回命令摘要、意图代次、账户版本、逐资产预算、资本占用和订单键。预算由真实风险公式计算，复用 `tickwire-risk` 定点运算、Spot 覆盖公式、USD-M 压力资本与减仓分配。

## 输入与准入

`Account::from_snapshot(Snapshot, Catalog)` 建立钱包、资产、仓位和初始归属。钱包 ID 区分 Spot、USD-M 全仓及真实逐仓钱包；金额同时绑定钱包、资产 ID、资产版本和小数位。目录保存完整 `InstrumentBinding`，命令逐字段匹配。风险、费用、保证金档位、抵押和估值版本由快照固定，缺失参数拒绝构造或准入。

当前基线要求 `locked == nontrade_locked`，即没有尚未导入的存活订单锁定。已存在仓位可以输入。资产和仓位归属总和不足部分记入策略 0；相互抵消的策略仓位仍计入总毛敞口。支持的集合上限见 `src/types.rs`：64 个资产、64 个标的、1024 个意图/订单、16384 个成交与费用键、1024 个策略及每标的 128 档保证金参数。

`admit(&CanonicalCommand, now)` 计算所有资产、钱包资本和交易所可用资金影子账，全部通过才占用。相同意图与摘要返回原 Admission；修改已使用意图的字段则冻结。重试返回的是原结果，发送前仍调用 `check_admission(&Admission, now)` 重验冻结、账户版本、订单状态和截止时间。

| 命令 | 当前账户处理 |
|---|---|
| Spot/线性 USD-M 的 Limit、StopLimit、TakeProfitLimit，明确 Base 数量 | 逐币种及资本计算 |
| 普通 SpotList | 所有腿分别预留，不依靠互斥关系少留资金 |
| 普通 CancelReplace | 保留旧单责任，再为新单预留 |
| Cancel、Query | 校验身份、目录、原订单及策略归属和请求期限；账户快照过期或冻结后仍可接纳，不释放旧单 |
| 原生 Market、Quote 数量、动态 ClosePosition | `UnboundedExecution` |
| Modify、Amend、CancelList、CancelAlgo、SpotAdvanced | `Unsupported` |

原生 Market 按风险设计拒绝，没有使用最新价乘滑点构造虚假硬界。受限主动成交可用明确 IOC Limit 表达。USD-M OneWay reduceOnly 和 Hedge 指定侧减仓只分配实际仓位中尚未被占用的额度；不会把潜在开仓、虚拟净仓或别的订单额度重复分给新单。

账户快照时效和风险策略版本只约束新增风险；`Cancel/Query` 不依赖这两项条件。发送前的 `check_admission` 对所有动作核对原 Admission、账户代次和请求期限，对新增风险再核对快照时效、冻结、订单状态与额度。`freeze()` 单向关闭新增风险，保留账目、预留和序号，重复调用不产生额外变化；日志或传输故障的所有者可用它关闭准入。

## 交易结果与恢复

`apply_event(&CanonicalEvent)` 用原始意图代次、订单和 native trade/fee ID 去重，连接代次变化不产生新成交。相同成交金额只入账一次，但新附带的订单观察仍检查：Unknown 关闭准入；终态更新累计量；身份与原始数量冲突拒绝并冻结。已确认 ACK 会阻止中止释放，中止后迟到 ACK 会恢复责任并冻结。实际累计成交超过已保存终态累计量时照实记账并冻结。

实际成交、费用和返佣写入原策略归属。超预算和可表示的负余额保留，账户冻结。身份、单位或算术校验失败保持原财务状态，保存首条拒绝的规范事件；完整输入历史由外层 WAL 保留。缺少成交详情的终态不释放剩余本金或减仓额度；手续费由独立闭合记录释放。

| 方法 | 调用者提供的有序输入 |
|---|---|
| `apply_abort(&Admission)` | 精确请求的 AbortedBeforeSend 已同步；订单尚无 ACK、成交、费用、终态或 Unknown |
| `reconcile_fees(&OrderKey, cumulative)` | 终态累计与逐笔成交一致，且费用明细已闭合 |
| `apply_unattributed_cashflow` / `apply_external_position` | 外部对账变化，记入策略 0 并冻结；不删除原预留 |
| `transfer_asset_attribution` / `transfer_position_attribution` | 授权的归属转移，不改变真实总量 |
| `recover(Snapshot, Catalog, &[RecoveryRecord])` | 同一基线和版本下完整、已验证顺序的持久记录 |

调用者先同步记录再更新账户；这些方法本身不验证 WAL 或签发交易授权。`AdmissionImage` 是公开的持久化数据，不能据此构造活跃 Admission。恢复重算每条 Admit 的资源并精确比较 image，按顺序重放其余七类记录；成功结果始终冻结，未完全闭合订单标为 Unknown。恢复不使用当前时间替换历史 `evaluated_at`。

`ledger_sequence()` 从基线序号开始，只统计实际应用的账户变化；相同基线与有序记录重放后得到相同序号。恢复附加的 Unknown 和本地 `freeze()` 不属于持久订单观察，不遮住随后首次收到的交易所 Unknown。重复成交附带新的终态异常时，账户保存该异常并增加一次序号；再次收到相同异常不增加。公开订单状态仍把交易所 Unknown 与恢复 Unknown 合并，准入与中止校验也使用合并结果。

序号达到 `u64::MAX` 后，需要新序号的记录返回 `ArithmeticOverflow`，原账目与责任保持并冻结。外层 WAL 若已保存这条被拒记录，恢复同样报错，不能跳过记录后返回成功；精确重复且没有新观察的输入仍可返回 `Duplicate`。

Spot 分别核对本地可用余额和交易所 free 上限。第三资产专用费用池整体隔离，并保留接收币收费回退预留。USD-M 按真实结算钱包计算压力资本；新增 H 只有可信中止才移除，成交、普通终态和手续费对账不增加影子额度。实际结算币费用先消耗已预留费用覆盖，超出部分计入额外扣款。无法估值的外币支出使影子账返回 `Unreconciled`，不把不同币种的原子数直接相减。

后续组合层需要把 AdmissionImage、完整命令/事件和基线目录写入同一有序 WAL 与依赖闭包，并与实际输出状态协调。当前账户没有在线闭合基线替换或解冻接口；存活旧单导入、复杂撤改及条件子单替换、动态 closePosition、跨币抵押和目录切换仍待实现。
