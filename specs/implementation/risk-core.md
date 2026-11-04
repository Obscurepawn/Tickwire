# Risk arithmetic

`tickwire-risk` provides deterministic fixed-point calculations and bounded local reservation and settlement state. It has no external service dependency and forbids unsafe code. The caller binds inputs to the same account, asset, product mode, contract instance, configuration revision and observation time. Missing or expired inputs cannot be substituted with zero to admit an order. [Source](../../crates/tickwire-risk/src/lib.rs)

## Arithmetic and exposure

| Operation | Contract |
| --- | --- |
| Decimal | Parse directly into signed `i128` with scale 0–38. Reject extra nonzero fractional digits. Addition/subtraction requires an explicit scale conversion. Cross-asset transfers fail. Signed floor and ceiling follow mathematical division. |
| Products and quotients | Cancel decimal factors before multiplication. Round at the requested output scale. Prefer one rounding for a multi-factor charge or collateral value. Reject an unrepresentable checked intermediate, including in `div_to`. Preserve negative balances wherever the formula permits them. |
| Reservations | Compute the candidate and check both amounts before committing. Insufficient funds leave money unchanged. An arithmetic fault also closes the increase gate. A timer alone releases zero. |
| Spot | Require complete coverage of baseline locks. Do not subtract a baseline open-order lock from free funds twice. If the fee charged in the received asset exceeds the amount received, reserve the remaining liability externally. |
| Futures | Include unconfirmed orders in capital. One-way mode checks both position endpoints. Hedge mode checks `BUY == open_long + close_short` and `SELL == open_short + close_long` and each side’s potential exposure. |
| Tiers and collateral | Tiers begin at zero, join without gaps and have continuous cumulative maintenance amounts. A boundary uses the higher tier. Require approved coefficients. Give invalid positive collateral zero value while still subtracting liabilities. |
| Reduction | Subtract existing reduce allocations and round down to the quantity step. Require an actual position of sufficient size. Validate direction and reduce-only flags under the correct position mode. |

## Delivery settlement

`Settlement<N>` holds bounded income IDs for one account/contract/delivery instance. A timer retains position and fee reserves. Deduplicate each real income event. A theoretical reference calculation is for reconciliation and never posts a second wallet credit.

Release once after complete account facts, all orders terminal, actual position zero, and zero wallet and reference discrepancies. New real income reopens a previously reconciled instance. An exact duplicate is idempotent. Conflicting data, exhausted capacity or integer overflow makes no partial posting, reopens reconciliation and latches `unresolved_evidence`. Only a recovery process with complete evidence can rebuild the instance.

The reducer covers one asset reservation at a time. Execution supplies order IDs, serialization, durable deduplication, submission, cross-instrument budgets and account reconciliation. Cash/spot public quote normalization alone establishes none of these predicates.

```sh
cargo test --locked -p tickwire-risk
cargo test --locked --release -p tickwire-risk
```

The numeric fixtures exercise 21 cases and 131 expected fields. Boundary tests include signed division, fee shortfalls, incorrect BUY/SELL hedge allocation and late unrecordable income after settlement. [Numeric contract](../contracts/risk-vectors-v1.json) · [Boundary tests](../../crates/tickwire-risk/tests/boundaries.rs)
