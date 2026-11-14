# Counterexamples and Rust fixes

Code-faithful modeling of `L0_PerOrderIsolation` against `crates/tickwire-account` uncovered two Futures `shadow_holds` accounting bugs on contradicted reopening and pre-`Reactivated` order eviction. Both were reproduced with failing Rust tests, fixed in `crates/tickwire-account`, and re-verified to exhaustion under TLC.

## Bug A1: `Order::reopen_aborted` failed to restore Futures `shadow_holds` (`ContradictedUncreatedFreezes`)

- **Scenario**:
  1. Strategy 1 submits a Futures order `o1` of notional hold `WalletCap`; `Account::admit` inserts `shadow_holds[o1] = WalletCap` (`admitted_sequence = seq_1`).
  2. `o1` enters `PlaceAmbiguous`, `not_found_grace_ns` elapses without venue visibility, and `QueryNotFoundAfterGrace` records `VenueEventBody::Rejected`, setting `o1.uncreated = true` and removing `shadow_holds[o1]`.
  3. Before any `Reactivated` snapshot refreshes the Futures wallet balance, an adversarial late venue `Order(Open)` or `Fill` arrives for `o1` (`AdversarialLateFactAfterUncreated`).
- **Pre-fix behavior**: `Order::reopen_aborted(&mut self)` restored `reduce` (`o.original - o.filled`), cleared `uncreated`/`aborted`/`fees_closed`, and latched `observation_unreconciled = true` + `self.frozen = true`, **but could not restore `Account.shadow_holds[o1]`** because `reopen_aborted` took `&mut Order` rather than `&mut Account`. As a result, `shadow_holds[o1]` remained `0` (`ContradictedUncreatedFreezes` violated) and `venue_shadow(wallet)` overstated available Futures margin by `WalletCap`.
- **Fix**: Added `Order.admitted_sequence`, `Order.initial_shadow_hold: Option<(u16, Fixed)>`, and `Account.last_reactivation_sequence` (`crates/tickwire-account/src/lib.rs:55-68`, `crates/tickwire-account/src/structures.rs:23-25`), plus `Account::reopen_order(&mut self, key: &OrderKey) -> Result<bool>` (`crates/tickwire-account/src/lib.rs:957-967`), restoring `shadow_holds[key] = (wallet_id, hold)` whenever `admitted_sequence > last_reactivation_sequence`. Wired across `mark_economic`, `observe_order`, and `observe_amendment` (`crates/tickwire-account/src/events.rs:219-227`, `crates/tickwire-account/src/events.rs:468-473`, `crates/tickwire-account/src/amendments.rs:202-207`).
- **Regression test**: `contradicted_uncreated_or_aborted_futures_order_restores_shadow_holds_and_eviction_preserves_shadow` in `crates/tickwire-account/tests/account.rs`.

## Bug A2: `Account::evict_expired` dropped unrefreshed `shadow_holds` without folding into `uncovered_debits` (`SolvencyNeverViolated` & `ReplayEquivalence`)

- **Scenario**:
  1. Futures order `o1` fills and reconciles fees (`terminal = true, fees_closed = true, is_retirable = true`), keeping `shadow_holds[o1] = WalletCap` because no `Reactivated` wallet refresh has occurred yet.
  2. `order_retention_ns` elapses (`clock >= retired_at[o1] + RetWindow`) and `Account::evict_expired(clock)` reclaims `o1`.
- **Pre-fix behavior**: `Account::evict_expired` called `self.shadow_holds.remove(&key)` and discarded the returned `(wallet_id, hold)` instead of accumulating `hold` into `self.uncovered_debits[wallet_id]`. Evicting `o1` refunded `WalletCap` of phantom Futures margin (`venue_shadow` jumped back to `WalletCap`), allowing a sibling Futures order `o2` to admit and double-spend margin (`SolvencyNeverViolated` violated) while diverging from an un-evicted WAL replay (`ReplayEquivalence` violated).
- **Fix**: Updated `Account::evict_expired` (`crates/tickwire-account/src/lib.rs:924-928`) so `if let Some((wallet_id, hold)) = self.shadow_holds.remove(&key) { let _ = self.add_uncovered(wallet_id, hold); }`, preserving `venue_shadow(wallet)` identically across eviction until the next authenticated `Reactivated` clears `uncovered_debits`.
- **Regression test**: `contradicted_uncreated_or_aborted_futures_order_restores_shadow_holds_and_eviction_preserves_shadow` in `crates/tickwire-account/tests/account.rs`.
