# USD-M 收入与账户观察

当前实现以 `CanonicalAccountInput` 为规范入口，订单 `CanonicalEvent` 和旧恢复标签保持原义。该入口的字段与校验见 `tickwire-venue/src/account.rs`；单条规范记录上限为 60 KiB，观察最多 64 个资产、128 个仓位。

## 身份与资金

- 收入以当前账户的 `(incomeType, tranId)` 去重。金额、资产版本、钱包、symbol、tradeId、时间和 info 属于完整正文；同键正文不同是冲突。
- 已批准基线保存完整 covered 集合。集合内收入重放不改变钱包；截止前出现的未知键仍待解释，不按时间自动纳入基线。
- 基线后的 `FUNDING_FEE` 记录实际有符号现金流。负额降低资本；未归属正额计入钱包但等额隔离，不增加可用资本。新资金费先保持冻结；完整查询和快照可通过 tag14 标为已核对，仍保留原资金与隔离责任。
- `REALIZED_PNL` 和 `COMMISSION` 根据 symbol、原生 tradeId、资产及精确金额与本地 Fill 组件关联。两种到达顺序都只记一笔资金变化；未关联负额先占用，正额不可用。
- `ACCOUNT_UPDATE` 保存稀疏 `B/P`、`E/T`、`bc` 及仓位原值。`bc` 不生成现金流；较低的已报告钱包形成绝对资本上限。观察重复不重复扣款，未解释观察保持冻结。

账户恢复标签 12 保存 `IngressAccountInput`，标签 13 保存 `IncomeBaseline`。输入 WAL 保存认证原文和规范决定，先同步输入，再在候选账户上验证并同步账户记录，最后发布状态。恢复逐条重验原文和规范正文。失败不把未同步候选发布为现有账本。

## 查询与审批

`IncomeReader` 使用现有 TLS 请求实现、原 `SignedTime` 与共享额度首接受检查。每页最多 16 行；`IncomeScan` 最多 128 次查询，按包含端点的毫秒区间拆分饱和页。同一毫秒仍饱和时返回不完整结果。`BaselineCoverage` 只保存查询证据，`Account` 模式才写账户输入效果；HTTP 原文、规范行及所有效果在同一输入批次持久化。

`IncomeBaselineAuthority` 由输入配置固定专用控制者和 Ed25519 公钥。未配置时旧配置字节不增加字段。导入提案含实际收入叶节点、账户/普通单/条件单三个快照及其输入记录摘要，最多 54 KiB、128 条收入、128 个引用。真实远端快照与固定基线逐项一致，且初始导入期间不跨越账户/订单变动。

`RestOwner::import_income_baseline` 校验专用 risk-admin 签名，绑定请求 ID、完整基线、账户/输入流、writer、账本位置、时钟域和有效期。清单引用审批前输入前缀；账户记录的 `through_sequence` 包含随后同步的审批记录。重启后用原接受时刻和原 writer 重验，已使用请求 ID 不因重启或审批到期重新开放。精确重试返回原结果，改正文拒绝。

签名承载控制者批准的基线覆盖断言。Binance 的资产 `updateTime`、相同快照和短收入页都不提供这种权限。查询完整性和审批覆盖分别验证。

离线 `input_facts_not_in_account` 包含已同步但尚无账户效果的 `IncomeImport`。配对比较完整 `IncomeBaseline`，包括原输入流、审批位置、清单摘要与 covered 正文；同钱包或同截止时间不能替代该比较。文件级中断测试在两份 WAL 重新打开后检查未安装时为 1、精确账户记录同步并再次打开后为 0，账户保持冻结。

## 自动核对与后续范围

app 通过可选 `reconciliation.income_baseline_import` 调度真实 `IncomeReader`、三个远端快照和有界审批文件轮询；等待审批期间不重复请求，批准后重新查询绑定新账本位置的快照。该功能当前用于未跨越账户变动的初始基线导入。

标签 14 `AccountInputsReconciled` 处理全部已经入账、基线后且类型确定的资金费。它保存原 scope/account epoch/ledger/baseline/history/input cut、状态摘要和覆盖对象摘要，先核对候选再同步账户 WAL；只将 Booked 改为 Reconciled、增加一次 ledger，现金流、credit hold、资产上限、订单责任不变。账户仍冻结。

覆盖对象由实际 `IncomeUse::Account` 完整叶节点和三份快照形成，按固定 CBOR 编码同步到恢复对象存储。验证原响应重解析结果、全部已选收入正文、原 input prefix、writer/clock/private cut，以及远端钱包/仓位/订单与当前账户相等。恢复用原物理 writer 和原评估时间，已存在精确 tag14 重试不重新计账。对象缺失、未知观察、Pending、冲突、旧截止前未知键或快照账本序号不符均拒绝。

app 的可选 `income_account_scan` 定期查询显式回查区间，共用原 reader 的连接代次、签名和共享额度。tag14 成功后丢弃三份旧快照，重新取得绑定新 ledger 的完整三快照再走 Reactivated。持续查询不会增加旧批准基线的覆盖集合；未知余额/仓位观察仍阻止重新激活。`ExternalTrade` 具有规范编码，但 reducer 明确拒绝；强平、ADL、交割、外部调整分类及资金费策略归属尚未完成。未知 incomeType 保留并冻结。当前没有把旧普通三快照激活扩成自动收入覆盖。

编码细节见收入闭合接口，阶段测试与源摘要见自动核对验证。


## 明确观察关联（tag15）

`IncomeObservationsAssociated` 在单个 Cross 钱包内关联当前全部未解释 `FUNDING_FEE` 观察和互不重用的明确收入键。实际 anchor 到 cut 只允许选中资金费/观察及其精确重复、既有 tag14 和已核验 Reactivated；订单、管理、持仓和其他现金记录即使金额净变化为零也不属于允许表。原基线 covered 或截止前迟到未知键不能被选中。

验证器从真实 canonical baseline 和按序账户记录重放 W₀，绑定独立 canonical prefix 摘要及当前真实 HistoryFrontier。逐资产核对 bc、wb、cw；不同原私有连接或 P 非空拒绝。实际 Account 用途完整查询和三快照必须覆盖原 E/T、全部收入及当前 ledger/input cut。三份相等快照不产生关联授权。

审批采用固定部署根签叶证书、当前部署 active digest/revision、叶签 manifest。实际策略先进入输入 WAL；首次接受时间和物理 writer 随批准输入同步。账户候选通过后才同步 tag15，再发布。普通 `record_fact` 拒绝此标签。tag15 只清被批准观察的解释状态、写一次消费索引并增加一次 ledger；逐字段保留 cash、caps、credit holds、reserve、owner0、订单责任和原 baseline，账户仍冻结。

原 request 精确重试先于当前时间和授权策略判断。输入已同步而账户未同步时，新实际 owner 可在相同前态上按原接受上下文补写；不同账户前缀拒绝。恢复重验固定根、旧叶、原策略、原物理 writer、全部原文及依赖对象；当前撤销或叶轮换不否定原合法结果。缺失对象拒绝。根轮换和真实批准签发服务不在当前实现中。

app 可选关联文件调度使用独立显式配置。关联选择、批准与旧基线批准共用只接受普通文件的读取器；Unix 使用 O_NOFOLLOW|O_NONBLOCK 并在打开后核对类型和长度，拒绝 FIFO/目录/符号链接，缺文件保持等待。tag15 后收新三快照；存在 Booked 时再执行 tag14，再收新三快照才 Reactivated。源与镜像共同检查六个审批依赖对象，输入完整前缀与账户历史仍是恢复必需材料。接口和实际测试见关联审批、验证记录。
