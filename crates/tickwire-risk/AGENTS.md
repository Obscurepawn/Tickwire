# tickwire-risk

`tickwire-risk` (`#![forbid(unsafe_code)]`) is Tickwire's dependency-free, deterministic fixed-point risk kernel.
`tickwire-account` and margin/spot admission paths call it to evaluate decimal arithmetic, spot buy/sell holds, linear
futures initial margin and maintenance brackets, per-asset `ReservationAccount` transitions, and fill/fee settlements.
It performs zero I/O, zero floating-point math, and carries no exchange wire types, clocks, or order-identity
state—timeouts and replay belong to the caller.

## Invariants and Change Guidelines

* **Explicit scales and checked `i128` arithmetic**: `Fixed` in [src/fixed.rs](src/fixed.rs) supports scales `0..=38`
  without implicit unit rescaling on addition or subtraction; every intermediate product, quotient, and remainder must
  remain checked and surface `RiskError` codes rather than wrapping or saturating.
* **Conservative outward rounding**: Round required reserves, margin requirements, worst-case taker/maker fees, and
  open-loss haircuts upward (`Rounding::Ceil`), and round releasable credits, collateral haircuts, and realized equity
  downward (`Rounding::Floor`) across [src/spot.rs](src/spot.rs), [src/futures.rs](src/futures.rs), and
  [src/settlement.rs](src/settlement.rs).
* **Atomic reservation and overflow gate**: `ReservationAccount::reserve_computed` in
  [src/reservation.rs](src/reservation.rs) must compute both `available` and `reserved` before mutating state, close
  `increase_open` permanently if `ArithmeticOverflow` occurs while preserving existing balances, and return zero
  release on `on_timeout`.
* **Golden vector parity**: Any change to rounding, bracket tiering, collateral haircuts, or `RiskError::code()`
  strings is a cross-component contract change; update
  [risk-vectors-v1.json](../../specs/contracts/risk-vectors-v1.json) and run its Python verifier alongside Rust tests.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/fixed.rs](src/fixed.rs) · [src/spot.rs](src/spot.rs) ·
  [src/futures.rs](src/futures.rs) · [src/reservation.rs](src/reservation.rs) · [src/settlement.rs](src/settlement.rs)
* **Integration tests**: [tests/boundaries.rs](tests/boundaries.rs) ·
  [tests/contract_vectors.rs](tests/contract_vectors.rs)
* **Specifications, contracts, and models**: [risk-core.md](../../specs/implementation/risk-core.md) ·
  [risk-vectors-v1.json](../../specs/contracts/risk-vectors-v1.json) ·
  [risk_limits](../../tla/execution_models/risk_limits/ROADMAP.md)

```sh
cargo test --locked -p tickwire-risk --all-targets
python3 scripts/verify-risk-vectors.py
```
