# tickwire-account

## 1. 职责（架构位置：账户风险域 —— 单所有者多资产账户状态机）

固定账户基线与目录版本内的单所有者账户状态机：订单预留、成交与费用入账、策略归属、冻结与确定性恢复。Spot 多资产余额/费用/归属，USD-M（OneWay/Hedge）保证金档位、压力资本、未实现亏损、资金费与减仓；纯检查型定点 `Fixed` 算术，无 I/O、无浮点、无交易所依赖。lib 根文档：`Single-owner multi-asset reservations and economic attribution.`

## 2. 公共 API 入口

- `Account`（`src/lib.rs`）：`from_snapshot(snapshot, catalog)`、`freeze()`、`frozen()`、`valid_until()`、`scope() -> &Scope`（只读、零分配）、`account_epoch()`、`ledger_sequence()`、`rejected_event()`、`order(key) -> Option<OrderView>`、`position(...)`、`position_entry_bound(...)`、`transfer_asset_attribution(...)`、`order_keys()`。
- 同 crate 内跨模块 `impl Account` 公开方法：
  - 准入/中止：`admit(command, now) -> Admission`、`check_admission(admission, now)`、`venue_shadow(wallet)`、`apply_abort(admission)`、`reconcile_fees(order, cumulative)`（`admission.rs`）。
  - 事件/事实：`apply_event(event) -> Applied`（`events.rs`）。
  - 恢复：`Account::recover(snapshot, catalog, records) -> Account`、`admission(intent)`、`apply_reactivation(binding)`（`recovery.rs`）。
  - 交接：`strategy_orders_digest(strategy)`、`handover(operation, revision)`、`apply_strategy_handover(h)`（`handover.rs`）。
  - 外部事实：`apply_unattributed_cashflow(...)`、`apply_external_position(...)`、`transfer_position_attribution(...)`（`external.rs`）。
  - 修订视图：`amendments()`（`amendments.rs`）。
- `pub use tickwire_risk::Fixed`；`pub use types::*`：`Scope/Revisions/AssetSnapshot/PositionSnapshot/Snapshot/FeePolicy/FuturesPolicy/InstrumentPolicy/Catalog/AssetKey/AssetAmount/IntentKey/OrderKey/CapitalHold/Admission/AssetView/OrderView/AmendmentView/Applied/Error/Result/AdmissionImage/RecoveryRecord/IncomeBaseline/AccountInputsReconciled/IncomeIdentity/ObservationAssociation/IncomeObservationsAssociated/ManagerIdentity/StrategyHandover/Reactivation/UncreatedAlgoClosed/StructureView`。
- 容量常量（`types.rs`）：`MAX_ASSETS=64`、`MAX_INSTRUMENTS=64`、`MAX_ORDERS=1024`、`MAX_FACTS=16384`、`MAX_STRATEGIES=1024`、`MAX_BRACKETS=128`，以及关联观察 `MAX_ASSOCIATION_OBSERVATIONS=16/MAX_ASSOCIATION_INCOMES=128/MAX_ASSOCIATION_REQUESTS=1024/MAX_BASELINE_INCOMES=128`。
- 内部模块（非 pub）：`account_inputs(association, closure)`、`admission`、`amendments`、`calculation`、`events`、`external`、`handover`、`recovery`、`structures`、`types`。无 bin/example。

## 3. workspace 位置

上游（`Cargo.toml` 实际声明）：`tickwire-risk`、`tickwire-venue`；第三方 `sha2`。

下游（grep 命中）：`tickwire-account-codec`、`tickwire-account-gateway`、`tickwire-account-journal`、`tickwire-handover`、`tickwire-strategy-sdk`、`tickwire-strategy-host`、`tickwire-binance-account`、`tickwire-binance-execution`、`tickwire-binance-quota`、`tickwire-runtime`、`tickwire-runtime-policy`、`tickwire-replica`、`tickwire-app`、`tickwire-lab`。

## 4. 对应规格

- [../../specs/implementation/account.md](../../specs/implementation/account.md)（金额/预留/资本、状态与顺序、持久输入与验收、Unknown/冻结语义）
- [../../specs/implementation/conditional-orders.md](../../specs/implementation/conditional-orders.md)（SpotList/Algo 条件父子单、结构摘要 v1→v2→v3）
- 背景事实：[../../specs/implementation/usdm-account-facts.md](../../specs/implementation/usdm-account-facts.md)（CanonicalAccountInput 收入去重/关联，由上游入口产生）、[../../specs/implementation/spot-amend.md](../../specs/implementation/spot-amend.md)（REST 结果/历史查询续做背景）

## 5. 构建与测试

```sh
cargo build -p tickwire-account
cargo test -p tickwire-account
```

无 bin/example。`tests/`：`account_inputs.rs`、`admission.rs`、`amend.rs`、`conditional.rs`、`edge_cases.rs`、`facts.rs`、`futures.rs`、`handover.rs`、`nonincreasing.rs`、`recovery.rs`，公共 fixture 在 `tests/common/`。

## 6. 关键不变量与注意事项

- **候选克隆提交**：增险命令先在 `candidate = self.clone()` 上计算，成功才 `*self = candidate`；算术溢出/冲突（`RiskError::ArithmeticOverflow|Conflict`）直接冻结，不提交半成品财务状态（`admission.rs`）。
- **恢复按原顺序重算准入并保持冻结**：`Account::recover` 逐条重放 `RecoveryRecord`；`Admit` 用同一命令与 `evaluated_at` 重新 `admit` 并逐字段比对 `AdmissionImage`，不符返回 `Conflict`；结束后 `frozen = true`，未闭合/非中止订单置 `recovery_unknown`（`recovery.rs`）。
- **未知意图持续占额**：订单私有 `observed_unknown/recovery_unknown/observation_unreconciled`，公开 `unknown()` 是前两者或值，继续阻止新增风险与矛盾中止；正向 ACK/零费用/成交/终态/Unknown 都能阻止矛盾中止。
- **冻结只置状态**：`freeze()` 不改账目/预留/序号，无解冻方法；冻结账户仍处理真实成交结果，并允许身份/期限有效的 Cancel/Query。
- **精确幂等**：同意图同摘要命令返回既有 `Admission`；同意图不同内容直接 `Conflict` 并冻结。
- **Cancel/Query 不依赖风险策略版本**：旧责任保持，快照过期/冻结/资金不足时仍可接纳只读撤单。
- **中止证据要求**：本地 Abort 是决策而非外部观察；已有确认/成交/矛盾状态时中止失败且不追加 Aborted，不提前释放预留；中止后迟到 ACK 重建责任并冻结。
- **已保存未应用的记录导致恢复失败**；重复成交不重复现金/仓位，不同金额同 ID 保留旧账并冻结。
- 未实现/明确拒绝：跨币种保证金、在线换基线、动态 closePosition、SOR/peg 风险界、完整撤改单生命周期。

## 7. 改动检查清单

1. `cargo build -p tickwire-account && cargo test -p tickwire-account` 通过。
2. 改预留/费用/归属联动 [account.md](../../specs/implementation/account.md)；条件单联动 [conditional-orders.md](../../specs/implementation/conditional-orders.md) 与结构摘要版本。
3. 形式模型：`tla/execution_models/account/`（L0_spot_facts、L1_reduce_shadow），改动状态机先核对模型映射。
4. 新增恢复记录类型时同步 `tickwire-account-codec`（CBOR）、`tickwire-account-journal`（持久化）与 `tickwire-handover`（交接校验）。
5. 本 crate 不做网络、授权或浮点计算。
