# tickwire-risk 接手说明

## 1. 职责（架构位置：风控决策）

检查型定点风险计算库：`Fixed` 定点数、Spot 现货预留/可用/费用、USD-M 单向/对冲持仓包络、保证金档位、压力亏损、资金费、结算对账。**无 I/O、无浮点、无交易所依赖、无运行时第三方依赖**（Cargo.toml 的 `[dependencies]` 为空，`serde_json` 仅 dev-dependencies 用于读合成向量）；`#![forbid(unsafe_code)]`。所有运算 checked，失败返回 `RiskError` 而不是 panic。

架构位置：纯计算风控决策层，被账户状态机、网关、策略 SDK、运行时门禁等作为准入/预留/结算的数值裁判调用。

## 2. 公共 API 入口

- `RiskError`（含 `code()` 机器码：ARITHMETIC_OVERFLOW / INVALID_DECIMAL / SCALE_MISMATCH / DIVISION_BY_ZERO / ASSET_MISMATCH / INSUFFICIENT_ASSET / INSUFFICIENT_COLLATERAL / VENUE_AVAILABLE_CAP / UNKNOWN_EXPOSURE / BRACKET_SEMANTICS_UNVERIFIED / INVALID_BRACKETS / UNRECONCILED / DUPLICATE_CONFLICT / GATE_CLOSED 等）、`Result<T>`。
- `fixed` 模块：`Fixed { atoms, scale }`（`from_atoms`/`parse`/`zero`/`one`/`ensure_same_scale`/`nonnegative`/`positive`/`checked_add/sub/neg/abs/mul_int`/`max`/`min`/`positive_part`/`rescale`/`mul_to`/`div_to`/`quantize`）、`Rounding::{Floor, Ceil}`、`floor_div`/`ceil_div`（带符号向外舍入）。
- `spot` 模块：`partial_buy_fill`、`received_fees`（标准/税/特殊三费率）、`third_asset_fee`（BNB 第三方资产费用估算）、`available`（`SpotBaseline`/`OrderCoverage`/`Availability`）、`fee_rounding_tail`。
- `reservation` 模块：`Money`、`ReservationAccount`（`reserve`/`reserve_computed`/`on_timeout`）、`PotentialOrder`/`potential_order_reserve`/`potential_order_sum`、`unknown_exposure_gate`、`allocate_reduce`、`VenueShadow`（`calculate`/`admit`）。
- `futures` 模块：`one_way_envelope`、`hedge_envelope`、`qualify_brackets`、`Maintenance`、`collateral_component`、`StressPrices`、`position_loss`、`FundingMode`/`funding_reserve`、`effective_leverage`、`one_way_capital`/`hedge_capital`、`AccountNetting`。
- `settlement` 模块：`SettlementState`、`IncomeKind`、`Income`、`ReferenceSettlement`、`settlement_fee_reserve`、`ReconcileFacts`、`SettlementReport`、`Settlement<N>`（`new`/`on_expiry_timer`/`record_income`/`report`/`reconcile`）。

## 3. workspace 位置

- 上游依赖：无 workspace crate、无运行时第三方依赖（仅 dev serde_json）。
- 下游（grep `Cargo.toml` 核对）：`tickwire-account`、`tickwire-account-codec`、`tickwire-account-gateway`、`tickwire-account-journal`、`tickwire-app`、`tickwire-binance-account`、`tickwire-binance-execution`、`tickwire-handover`、`tickwire-mock`、`tickwire-runtime`、`tickwire-runtime-policy`、`tickwire-strategy-sdk`。

## 4. 对应规格

- [../../specs/implementation/risk-core.md](../../specs/implementation/risk-core.md) — Risk Core 实现契约（计算与提交边界、ID 表、验证位置）。
- 数值向量契约：[../../specs/contracts/risk-vectors-v1.json](../../specs/contracts/risk-vectors-v1.json)（21 个案例 / 131 个期望字段）。

## 5. 构建与测试

```sh
cargo test -p tickwire-risk
python3 scripts/verify-risk-vectors.py
```

- `tests/boundaries.rs`：17 个边界测试（带符号除法向外舍入、i128::MIN 溢出、除零、scale 校验等）。
- `tests/contract_vectors.rs`：读取 `specs/contracts/risk-vectors-v1.json`，用生产计算器重算每个 formula 的 input 并与 `expected` 逐字段断言，末尾硬断言 `(cases, fields) == (21, 131)`。
- `scripts/verify-risk-vectors.py`：用 Python `Decimal`（100 位精度、Ceil/Floor 量化）独立重算同一批向量并与 `expected` 比对——它是设计参考实现，**不是生产风控引擎**；Rust 测试与 Python 脚本构成双向交叉校验，改任一方向都要同时跑。

## 6. 关键不变量与注意事项

- **定点显式 scale**：`Fixed` 携带 scale，加法/减法必须 `ensure_same_scale`，scale 不一致返回 `ScaleMismatch`，绝不静默换算；乘法/除法显式 `mul_to`/`div_to` 到目标 scale 并指定 `Rounding`。
- **带符号除法向外舍入**：`floor_div`/`ceil_div` 对负数向远离零方向处理（如 -5/2 → floor -3 / ceil -2），`i128::MIN / -1` 是 `ArithmeticOverflow`，除零是 `DivisionByZero`。
- **未知意图占额与准入门**：`unknown_exposure_gate(has_unknown)` 在存在未解释敞口时返回 `UnknownExposure`，准入不得放行；`VenueShadow::admit` 用场所可用上限（shadow cap）拦截超额 `capital_delta`，超出返回 `VenueAvailableCap`。
- **现货可用必须对账**：`available` 要求 `coverage + nontrade_locked == locked`，否则 `Unreconciled`；`available = max(0, min(local_free, venue_cap))`。
- **费用尾款**：`fee_rounding_tail` 在聚合费用向上取整之外，再预留 `maximum_remaining_fills × fee_asset_quantum` 的舍入尾差；`received_fees` 三费率分别向上取整，净收入向下取整。
- **结算状态机**：`Settlement` 经 Active → AWAITING_SETTLEMENT（到期定时器只释放费用预留，不自动移仓）→ SETTLED_RECONCILED；`reconcile` 用实际收入组件对账参考 PnL/费用，差异非零不得进入已结算。
- **包络保守取界**：单向 `one_way_envelope` 给出 position_min/max/max_abs_position；对冲 `hedge_envelope` 分别给 long_max/short_max/net_min/net_max；资本数量按最坏方向累计，notional 用压力高价向上取整。
- **档位语义未验证即拒绝**：`qualify_brackets` 对 `BracketInterpretation` 未验证语义返回 `BracketSemanticsUnverified`；维护保证金按档累加、档内上限取历史最大费率。
- 所有金额路径 checked_add/checked_mul，任何溢出走 `ArithmeticOverflow`，禁止饱和包装后继续授信。

## 7. 改动检查清单

1. `cargo test -p tickwire-risk` 全绿。
2. 改动公式或新增案例后，同步更新 `specs/contracts/risk-vectors-v1.json`，并同时跑 `cargo test -p tickwire-risk` 与 `python3 scripts/verify-risk-vectors.py`；案例数/字段数变化时更新 contract_vectors.rs 末尾的 `(21, 131)` 硬断言。
3. 新增 `RiskError` 码需在 `code()` 机器码表与 risk-core.md 的 ID 表登记。
4. 保持无浮点、无 I/O、无第三方运行时依赖；舍入方向（Ceil 预留 / Floor 净收入）不得反转。
5. 改动 scale/舍入语义后，检查下游 `tickwire-account` 预留与 `tickwire-runtime-policy` 准入是否依赖既有边界值。
