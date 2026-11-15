# tickwire-risk

`tickwire-risk` (`#![forbid(unsafe_code)]`) is Tickwire's dependency-free, deterministic fixed-point risk kernel and
flat lock-free per-symbol Risk Management System (`RmsEngine` / `SymbolLimits`). `tickwire-account` and ORS `REQ`
admission paths call it to evaluate decimal arithmetic, spot buy/sell holds, linear futures initial margin and
maintenance brackets, per-symbol `1s / 5s / 10s` rolling order-rate and quantity windows, exact fixed-point fat-finger
price cages, per-asset `ReservationAccount` transitions, and fill/fee settlements. It performs zero I/O, zero
floating-point math, and carries no exchange wire types or OS clock syscalls.

## Invariants and Change Guidelines

* **Explicit scales and checked `i128` arithmetic**: `Fixed` in [src/fixed.rs](src/fixed.rs) supports scales `0..=38`
  without implicit unit rescaling on addition or subtraction; every intermediate product, quotient, and remainder must
  remain checked and surface `RiskError` codes rather than wrapping or saturating.
* **Conservative outward rounding**: Round required reserves, margin requirements, worst-case taker/maker fees, sell
  fat-finger floors, and open-loss haircuts upward (`Rounding::Ceil`), and round releasable credits, collateral
  haircuts, buy fat-finger ceilings, and realized equity downward (`Rounding::Floor`) across [src/spot.rs](src/spot.rs),
  [src/futures.rs](src/futures.rs), [src/rms.rs](src/rms.rs), and [src/settlement.rs](src/settlement.rs).
* **Flat lock-free `REQ` RMS & transactional `SIGUSR1` reload**: `RmsEngine` in [src/rms.rs](src/rms.rs) indexes
  preallocated `SymbolLimits` slots directly by `symbol_id: usize` (zero hash/tree lookup, zero locks, zero heap
  allocation on `REQ`). `FatFingerRatio` uses exact parts-per-billion (`10^-9`) integer arithmetic (zero floats).
  `RmsReloadHandle::reload_limits` parses and validates the entire static configuration table before mutating sequence-locked
  atomic cells so malformed reloads preserve active limits, rolling counters, and generation untouched.
* **Atomic reservation and overflow gate**: `ReservationAccount::reserve_computed` in
  [src/reservation.rs](src/reservation.rs) must compute both `available` and `reserved` before mutating state, close
  `increase_open` permanently if `ArithmeticOverflow` occurs while preserving existing balances, and return zero
  release on `on_timeout`.
* **Golden vector parity**: Any change to rounding, bracket tiering, collateral haircuts, or `RiskError::code()`
  strings is a cross-component contract change; update
  [risk-vectors-v1.json](../../specs/contracts/risk-vectors-v1.json) and run its Python verifier alongside Rust tests.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/fixed.rs](src/fixed.rs) · [src/rms.rs](src/rms.rs) ·
  [src/spot.rs](src/spot.rs) · [src/futures.rs](src/futures.rs) · [src/reservation.rs](src/reservation.rs) ·
  [src/settlement.rs](src/settlement.rs)
* **Integration tests**: [tests/boundaries.rs](tests/boundaries.rs) ·
  [tests/contract_vectors.rs](tests/contract_vectors.rs) · [tests/rms.rs](tests/rms.rs)
* **Specifications, contracts, and models**: [risk-core.md](../../specs/implementation/risk-core.md) ·
  [risk-vectors-v1.json](../../specs/contracts/risk-vectors-v1.json) ·
  [risk_limits](../../tla/execution_models/risk_limits/ROADMAP.md)

```sh
cargo test --locked -p tickwire-risk --all-targets
python3 scripts/verify-risk-vectors.py
```
