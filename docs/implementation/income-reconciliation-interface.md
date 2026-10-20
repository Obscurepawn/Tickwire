# 收入与账户观察的对账闭合

状态：tag14 自动闭合方向已批准，以下编码和接线细则待审，尚未修改生产实现。当前账户仍保留新资金费、未知收入和未解释观察的冻结状态。本接口拟增加账户恢复标签 14 `AccountInputsReconciled`；不会改变已批准基线的 covered 集合或截止。未知观察与收入的关联批准不在本阶段实施。

## 能核对的关系

`IncomeKey = (account, incomeType, tranId)` 标识一条真实收入。输入 WAL 保存认证收入响应及规范正文，账户按该键记账；相同键的重复查询可以核对原金额、资产、symbol、tradeId 和时间。`REALIZED_PNL/COMMISSION` 还需与实际 Fill 的原生 tradeId、symbol、资产和完整金额一致。未知类型、冲突或未关联组件继续阻断。

`ACCOUNT_UPDATE` 没有上述收入 ID。`reason=FUNDING_FEE`、钱包/资产、可选 symbol、`T/E` 和 `bc` 可用于寻找资金费候选，不能单独确立唯一关联。即使当前只查到一条同金额、同时间收入，也可能尚未看到另一条记录；短页和资产 updateTime 没有消除这个问题。当前不以这些字段自动清除未解释观察。

因此，自动闭合只处理已经确定入账的资金费收入，并要求没有待解释的账户观察。若实际链上还存在未解释观察，拟由专用 risk-admin 对**明确的 observation digest → 完整 IncomeKey 列表**作关联批准，保留原签名及查询证据。该批准声明关联关系，不声明交易所提供全量收入截止，也不增加基线 covered 集合。此权限使用独立用途 `tickwire.usdm.income-association.v1` 和显式配置的验证者；现有仅允许基线导入的 key 不自动获得关联权限。此关联分支须单独获批后实施。

## 记录与完成顺序

拟定的持久数据如下；发布前的验证由封闭构造的能力负责。

```rust
struct AccountInputsReconciled {
    scope: Scope,
    account_epoch: u64,
    ledger_sequence: u64,
    baseline_digest: Hash,
    history_digest: Hash,
    input_stream: [u8; 16],
    through_sequence: u64,
    state_digest: Hash,
    coverage_digest: Hash,
}

// 封闭构造：核对真实 input WAL、当前账本和所有依赖后生成。
struct VerifiedAccountInputsClosure;
```

### `state_digest` 的规范原像

摘要定义为 `SHA256(state_bytes)`，用于固定本次转换之前的收入状态，不作为完整账户序列化或独立授权。原像依次为 ASCII `tickwire.account-input-state.v1` 的原始字节（无 NUL）、`u16LE(1)`、scope、`account_epoch`、`ledger_sequence`、`baseline_digest`、`history_digest`，再接下表的十个区段。scope 按 `risk_domain_id[16] / venue:u32 / environment:u8 / account:u64` 编码，environment 固定 Production=1、Test=2。其余标量遵循以下规则：

- 未另注宽度的整数均为 u64LE；计数包括 covered_count 均为 u32LE。哈希固定 32 字节，stream 固定 16 字节。
- `Fixed` 为 `atoms:i128LE / scale:u8`，保留原单位，不约分或重标度。`Decimal` 为规范值的 `coefficient:i128LE / exponent:i8`；有符号整数均用二进制补码。
- 文本为 `u16LE(UTF-8 字节数) / 原字节`，沿用对应 venue 类型的长度和字符约束，不大小写折叠、不做 Unicode 归一化。布尔仅 0/1；可选项为 `0`，或 `1 / 值`。
- 每个集合区段为 `tag:u8 / count:u32 / entries`，tag 固定按表中顺序出现，即使集合为空也保留。单值可选区段为 `tag:u8 / option`。不允许重复键、未知 tag 或尾随字节。
- 集合按业务键的元组顺序排序：整数按数值，文本按 UTF-8 字节序，哈希按字节序。排序比较的是键值，长度前缀仅用于编码。
- `CanonicalRef` 为 `byte_length:u32 / SHA256(canonical_bytes)`。验证时从实际日志中的 `CanonicalAccountInput` 或 `CanonicalEvent` 重新取得规范字节，核对长度和哈希，正文与引用一并作为验证材料。

| tag | 集合键及条目字段 |
|---|---|
| 1 | 基线，按 `wallet_id`。条目依次为 wallet、原导入 ledger、through_time_ms、coverage_digest、baseline_digest、input_stream、through_sequence、covered_count，再接按 IncomeKey 排序的 `(IncomeKey, CanonicalRef)`。各基线 scope/account_epoch 必须与头部相同。 |
| 2 | 全部收入，按 `IncomeKey=(income_type, tranId)`。条目为 key、status:u8、CanonicalRef；状态固定 Baseline=1、Booked=2、Pending=3、Trade=4、Reconciled=5。已闭合收入也保留在集合中。 |
| 3 | 成交组件，按 `(symbol, tradeId, kind:u8, asset_id:u64)`；kind 为 PnL=0、Fee=1。值依次为 OrderKey、原生订单 ID、wallet_id、AssetRef、Decimal。OrderKey 为 intent epoch、intent_id、client_id；AssetRef 为 id、version、symbol、exponent。 |
| 4 | 组件别名，按第 3 项的组件键排序，值为 IncomeKey。所有别名必须指向已存在且已核对的组件和收入。 |
| 5 | 全部账户观察，按 CanonicalAccountInput digest；值为 unresolved:bool。验证者必须能从实际输入前缀取得对应完整观察，并重新确认该状态。 |
| 6 | 资产绝对上限，按 `AssetKey=(wallet_id, asset_id)`；值为 Fixed。 |
| 7 | 未归属正额隔离，按 AssetKey；值为 Fixed。 |
| 8 | 首条被拒的账户输入：可选 CanonicalRef。存在时拒绝自动闭合。 |
| 9 | 持久订单观察责任，按 OrderKey。值依次为 observed_unknown、observation_unreconciled、confirmed、economic、terminal、fees_closed、aborted 七个 bool，original、filled、reduce 三个 Fixed，以及可选 terminal_cumulative:Fixed。 |
| 10 | 首条被拒的订单回报：可选 CanonicalEvent 引用。存在时拒绝自动闭合。 |

IncomeKey 的账户由头部 scope 固定；具体 wallet、资产版本和正文由 CanonicalRef 绑定。组件摘要同时绑定 `AssetRef`、完整订单身份和金额。

规范原像最多 16 MiB，可逐段送入 SHA256，不要求分配整块缓冲。income、component、observation 三个集合的合计条目数仍受 `MAX_FACTS=16384` 限制；alias 不多于 component；baseline/caps/credit_holds 分别最多 64 项，单个基线 covered 最多 128 项；订单最多 1024 项；单个规范输入/事件引用仍按各自 codec 上限检查。任一长度、计数或运算越界均返回 Bounds/Capacity，保留完整旧状态。

恢复在精确的 `baseline_digest + history_digest` 下重放 tag14 之前的全部规范账户记录，再计算上述摘要。该过程保留每条记录造成的 Unknown、缺失成交明细和矛盾终态责任。

`Account::frozen` 和订单 `recovery_unknown` 不进入摘要：前者还会由运维 freeze 或恢复入口合成，后者在恢复末尾根据未闭合订单加上。两者在恢复账户中照常保留，tag14 不把它们清零。持久记录造成的 `observed_unknown`、`observation_unreconciled`、成交数量、终态累计和费用责任则明确进入第 9 区段，因此该区段保留它们各自的值，不使用 effective unknown 的合成布尔。全部成交、资产与仓位、资本预留和其他订单字段仍由精确历史前缀绑定并按原 reducer 重放；验证器还要执行实际快照核对及候选账户校验。

在线账户与恢复账户的恢复标记可以不同，持久状态原像及 ledger 则须相同。tag14 只修改本次已验证资金费的 Booked 状态，不重置任何订单 Unknown、冻结原因或余额上限；后续 Reactivated 仍须独立满足已有完整对账要求。

### 覆盖对象和上限

`coverage_digest` 为独立规范覆盖对象的内容哈希。对象使用确定性 CBOR（最短整数/长度、有序固定字段、拒绝未知字段和非规范输入），域为 `tickwire.binance.account-input-closure.v1`，schema=1；绑定本条记录的全部字段（不含 coverage_digest 自身）、当前 writer、private connection epoch/gap、ClockDomain、原评估时刻/有效期，以及下列有序引用：

1. 完整 `IncomeUse::Account` 扫描的叶节点，逐项保存 input sequence、原规范输入记录的长度/哈希、查询区间及本次选中的 IncomeKey/CanonicalRef。验证者从真实响应重新解析，不接受调用者单独提供的键列表。
2. 按 Account、OpenOrders、ConditionalOrders 顺序排列的三个实际快照引用，核对每项的 query、baseline、account stream、ledger、接收时刻和原文哈希。

对象最多 64 KiB，引用合计最多 128 项（包括三份快照），本次待关闭的资金费最多 128 个键。当前自动接口要求所有尚未闭合的 Booked 键都被本次材料覆盖；超限保持冻结并返回 Capacity。Pending、未解释观察、正文冲突或被拒输入均使自动接口失败，即使所有金额相加为零。关联批准字段在 schema1 中不存在，未知字段直接拒绝；后续关联分支需另行审定版本。

这里的扫描完成仅指给定查询范围的所有叶节点已返回且不饱和。baseline covered 集合保持原批准内容；以后出现的迟到收入仍按原截止规则处理。

### 输入前缀和对象持久化

`through_sequence` 必须等于 owner 当前**连续已应用**的真实输入前缀；它还须等于已持久输入的 position；存在尚未应用的记录时拒绝生成能力。覆盖对象的所有引用都位于该前缀内，验证者同时核对最新 BeginReconciliation/gap 和输入 stream。生成候选后若输入前缀、账户 ledger/history 或 private cut 改变，旧候选失效。

顺序为：输入查询和快照先同步并应用 → 固定该输入 cut → 生成并同步覆盖对象 → 预校验候选账户 → 同步账户 tag14 → 发布候选。tag14 引用覆盖对象及其之前的输入 cut；覆盖对象不含 tag14 的哈希、后序账户 history 或最终闭包根。自动分支无需向输入 WAL 再写一份“批准”，也就不会要求输入前缀包含引用它自身的记录。

复制导出必须同时携带覆盖对象和覆盖 through_sequence 的真实 InputPrefix 及全部输入正文。InputPrefix 可覆盖更长前缀，但与已绑定前缀的逐项哈希必须相同；恢复只重验已有对象和日志；材料缺失即拒绝恢复。

闭合按单 owner 顺序执行：

1. 全部查询原文已同步输入 WAL，并应用到连续输入前缀；初始基线批准仍由既有 tag13 保存和验证。
2. 验证者核对当前 baseline/history/ledger、输入 cut、查询原文及所有规范金额；存在 `Pending`、未知类型、冲突、基线截止前未知键即拒绝。
3. 实际远端钱包与当前本地钱包逐资产相等，仓位/模式、普通订单和条件订单按现有严格规则相符；遗漏或仅净额相等不替代逐项核对。
4. 在候选账户上把本次全部已确定资金费从 Booked 标为 Reconciled；自动分支不清除任何观察约束。现金流、归属、原扣款、`credit_holds`、资产上限、未来资金费预留及现有订单费用责任保持。
5. 同步账户 WAL 的标签 14 后发布候选，并仅增加一次 ledger。账户仍冻结；controller 丢弃本轮三份旧快照，重新按 Account、OpenOrders、ConditionalOrders 顺序获取绑定新 ledger 的三份快照，再走已有持久 `Reactivated` 流程。仅更改旧快照的 ledger 字段或复用原 evidence 都无效。新一轮 private gap、账户变动或过期继续使该轮材料失效。崩溃恢复默认冻结。

### 精确重试与重放

已成功同步的 tag14 按 `(account stream, 前序 history_digest)` 定位首次提交记录。收到重试时先查询该历史位置，完整规范 binding 相同才返回该次已有结果，不再写 WAL、不再增加 ledger；同一前序 history 上的任何字段差异都拒绝。即使后续已有新记录，旧重试也只返回原结果。首次调用没有已提交记录时，才检查当前 ledger/history/state_digest/input cut 以及当时的时效。

同步报错或回执丢失时维持既有 journal poison/账户冻结，不能在原 owner 上假定“没写入”后继续追加。重新打开只接受实际完整批，若 tag14 确已留下，按其真实历史位置恢复一次；若没有留下，仍未闭合。索引由完整 WAL 重建，整个保留历史中的精确重试始终命中原记录。

恢复先在原前序历史下验证覆盖对象、全部查询和快照、原接受时刻与期限，再重算 state_digest 并执行相同转换。当前 wall/monotonic 时间不参与历史接受判定，已经过期的原材料不会因此抹掉已持久化的账本；新调用仍需检查当前有效期。自动分支没有新审批对象，原基线批准仍由 tag13 的既有规则重验。

## 接受与拒绝序列

| 序列 | 结果 |
|---|---|
| 已批准基线；新 `FUNDING_FEE/key=A/-2 USDT` 已记账；重查 A 完整正文相同；无未解释观察；真实钱包比基线少 2、仓位和全部订单相符 | 可以关闭 A 的待对账状态；本金不再扣第二次，账户待后续正常激活 |
| 新资金费 `+2 USDT` 已记账，其他核对同上 | 可以标为已核对，但未归属正额仍隔离；闭合不增加该笔可用额度 |
| 未知类型 A=-2、B=+2，最终钱包不变 | 拒绝，两个 `Pending` 均保留；不按净额零消除 |
| `ACCOUNT_UPDATE/FUNDING_FEE/bc=-2`，查询只见一条同时间、同金额 A | 暂不自动关闭该观察；保留冻结，待明确关联批准和全部实际查询核对 |
| 上述观察经专用批准明确关联 A；A、观察、审批、所有查询和快照均匹配 | 关联分支获批实现后可关闭；批准原文入 WAL，不按 eventTime 去重 |
| 观察对应 A、B 两条收入，其中一条尚未入账或正文冲突 | 拒绝，即使 A+B 恰好等于 bc |
| 已闭合 A 后重复收到 A | 仅保留新来源记录，资金和已闭合状态不变 |
| 已闭合后收到基线截止前的新键 C | C 保持未知，负额占用、正额不可用并冻结；不会扩展旧批准 |
| 已闭合后收到新观察或新收入 D | 按新输入处理并重新建立相应责任；旧标签 14 不能覆盖它 |
| 三快照一致，但收入查询缺页、同毫秒饱和、关联不唯一或当前 private gap 改变 | 拒绝 |

app 后续工作分两类：已配置初始导入时执行现有审批流程；已有基线后的收入扫描采用 `IncomeUse::Account`，每页仍受共享额度和请求预算限制。只在完整材料满足上述条件时调用闭合入口。外部调整、强平、ADL、交割与策略资金费归属继续按各自明确记录补齐；本入口遇到未知类型仍拒绝。
