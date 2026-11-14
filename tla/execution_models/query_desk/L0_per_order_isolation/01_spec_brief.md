# Specification brief: L0 per-order doubt isolation and non-freezing rejection

## State ownership and scope

`L0_PerOrderIsolation` (`02_models/L0_PerOrderIsolation.tla`) models the cross-layer boundary between:

1. **`Account` (`crates/tickwire-account/src/{lib.rs, admission.rs, events.rs, recovery.rs}`)**:
   - Spot principal reservation (`Order::remaining`), Futures `shadow_holds` + `uncovered_debits` (`venue_shadow`), and derivative `reduce` holds across multiple concurrent strategy orders.
   - Zero-byte unsent `abort` (`Aborted`) vs. non-freezing deterministic `VenueEventBody::Rejected` (`observe_rejected` -> `uncreated = true, fees_closed = true`).
   - `Account::reopen_order` (`Order::reopen_aborted` + `shadow_holds` restoration + `observation_unreconciled = true` + `frozen = true`) when a contradicted venue event arrives after `uncreated = true` or `aborted = true`.
   - `Order::is_retirable` and `Account::evict_expired` (preserving unrefreshed `shadow_holds` inside `uncovered_debits` upon time-window eviction).
   - Pure durable WAL recovery reducer (`Account::recover`) replaying `Admit`, `Aborted`, `VenueEvent`, and `FeesReconciled` without transient `in_doubt` state.
2. **`AccountGateway` (`crates/tickwire-account-gateway/src/lib.rs`)**:
   - `per_order_doubt_isolation = true` transitions: `Queued` -> `NoBytes` abort vs. `MayHaveSent` -> `Flushed`, `drop_broken_transport_isolated` (socket drop isolating only the in-flight `MayHaveSent` entry into `Status::Unknown` + `DoubtReason::PlaceAmbiguous` while keeping `halted = false` and `Flushed` sibling entries intact), Category A response ambiguity (`isolate_intent_in_doubt`), and `install_transport_connection` replacement link handoff.
3. **`QueryDesk` grace boundary & venue matching engine (`crates/tickwire-account/src/query_desk.rs`)**:
   - `first_doubt_mono_ns + not_found_grace_ns` (`GraceTicks`) guarding `CloseUncreated` against late matching-engine indexing (`"PendingIndex" -> "Open"` within `recvWindow`).

## Finite configurations

| Configuration | Constants | Purpose |
| --- | --- | --- |
| [`L0_PerOrderIsolation.cfg`](02_models/L0_PerOrderIsolation.cfg) | `Orders = {1, 2}, WalletCap = 2, GraceTicks = 2, RetWindow = 1, MaxClock = 3, AllowContradictedUncreated = FALSE` | Primary multi-order / multi-strategy solvency, grace-window indexing, sibling liveness, eviction, and replay verification under lawful venue semantics. |
| [`L0_PerOrderIsolation_adversarial.cfg`](02_models/L0_PerOrderIsolation_adversarial.cfg) | `Orders = {1, 2}, WalletCap = 2, GraceTicks = 1, RetWindow = 1, MaxClock = 3, AllowContradictedUncreated = TRUE` | Adversarial fault injection delivering late venue observations after post-grace `uncreated = TRUE` to verify `reopen_order` liability restoration, non-eviction, and immediate account freeze. |

## Invariants checked

- **`TypeOK`**: Every state variable remains within its finite domain.
- **`ReservationLockedWhileInDoubt`**: Whenever `in_doubt[o] != "None"` and `~uncreated[o] /\ ~aborted[o]`, 100% of remaining Spot principal, Futures `shadow_holds`, and derivative `reduce` remains locked, `IsRetirable(o) = FALSE`, `retired_at[o] = -1`, and `~evicted[o]`.
- **`SolvencyNeverViolated`**: Absent an adversarial post-grace venue contradiction (`~observation_unreconciled`), committed exposure across in-doubt orders + active sibling orders + settled spend never exceeds `WalletCap` or drives `venue_shadow` negative; any `observation_unreconciled` latches `frozen = TRUE`.
- **`HealthySiblingUnblocked`**: Single-order transport drops, Category A response ambiguity, and deterministic `VenueEventBody::Rejected` closures never set `halted = TRUE`, never freeze the account (absent contradicted `uncreated`), and never clobber `Flushed` sibling entries.
- **`NoPrematureUncreatedBeforeGrace`**: Missing status queries (`"Absent"` / `"PendingIndex"`) before `first_doubt_at[o] + GraceTicks` never set `uncreated = TRUE` or release reservations.
- **`ContradictedUncreatedFreezes`**: Any late venue observation arriving after `uncreated = TRUE` clears `uncreated`/`aborted`/`fees_closed`, sets `observation_unreconciled = TRUE` and `frozen = TRUE`, blocks retirement/eviction, and restores full Spot principal, `reduce`, and `shadow_holds`.
- **`ReplayEquivalence`**: Durable WAL replay (`rep_*`) matches live order states (`PartialEq for Order`), settled spend, and total shadow deductions across all transient `in_doubt` transitions, socket drops, duplicate venue deliveries, and `WindowedTable` evictions.
