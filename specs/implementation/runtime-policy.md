# Stateless ORS Runtime Control Policy (`tickwire-runtime-policy`)

## Scope and Single-Writer Ownership

[`tickwire-runtime-policy`](../../crates/tickwire-runtime-policy/src/lib.rs) defines the venue-agnostic, lock-free control policy for a unified Order Routing System (ORS) execution process. In Tickwire's pure stateless 3-process shared-memory topology:

- **Zero local leases or WAL verifiers:** The ORS owns its venue accounts directly at process startup from immutable static configuration and cold Exchange REST snapshots (`Account + OpenOrders`). Distributed owner-lease renewal, lease preemption, reconciliation WAL digest verification, and shared-memory market freshness watermarks do not exist inside the ORS process.
- **Zero market-data coupling:** The ORS holds zero public market-data connections and zero SHM quote subscriptions. Market-data staleness is enforced per strategy worker inside `tickwire-strategy-sdk` and per outbound order on the ORS `REQ` hot path via the microsecond `tick_mono_ns` stale-order gate (`max_order_age_ns`, default `23_000` ns).
- **Venue agnosticism:** `tickwire-runtime-policy` names zero exchange venues and depends only on [`tickwire-venue`](../../crates/tickwire-venue/AGENTS.md), [`tickwire-clock`](../../crates/tickwire-clock/AGENTS.md), [`tickwire-clock-sys`](../../crates/tickwire-clock-sys/AGENTS.md), and `serde`.

## Process Operating Modes

`Mode` governs outbound command admission across the entire ORS process:

1. `Mode::Trading`: Admits authorized new-order, modify/amend, RFQ quote, cancel, and status-query intents when the target strategy is both `active` and `enabled` on the resolved `account_ref`.
2. `Mode::ManageOnly` (aliased as `Mode::RestrictedCancelOnly` for `OrsRestricted` wire semantics): Refuses risk-increasing `NewOrder`, `Modify`, and `NewRfqQuote`/`ModifyRfqQuote` intents while continuing to admit `Cancel`, `CancelAll`, `CancelRfqQuote`, `CancelAllRfqQuotes`, and read-only status queries so operators and strategies can flatten exposure cleanly.
3. `Mode::Stopped`: Refuses all new risk and modification intents while shutdown or emergency drain completes.

Transitions between `Trading`, `ManageOnly`, and `Stopped` execute through atomic `Ordering::AcqRel` stores on `OrsRuntimePolicy` (`ControlPlane`) so DOP control handlers and `REQ` hot-path checks never contend on a mutex.

## Strategy Whitelist, Account Authorization, and `(account_ref, strat_id)` Gating

Strategy identifiers span `0..=254` (`255` is reserved for `ALL_STRATS` administrative `CancelAll` and broadcast reports). Static startup configuration (`PolicyConfig` in [`bootstrap.rs`](../../crates/tickwire-runtime-policy/src/bootstrap.rs)) registers:

- **Process-wide strategy whitelist (`strategies`):** The set of `strat_id` values (`0..=254`) permitted to complete `DopLogin` and submit intents to this ORS process. Any `strat_id` outside the whitelist is rejected immediately with `RefusalReason::InvalidStratId` (`INVALID_STRAT_ID`).
- **Ordered `AccountAuthSpec` table (`account_ref = 1..=255`):** Each enabled account session declares its `1`-based `account_ref`, human-readable `name`, and per-account `strategies` allow-list in `enabled_accounts` configuration order:
  - `account_ref == 0` on an inbound strategy intent resolves in `O(1)` to the **first** enabled `account_ref` (in `enabled_accounts` configuration order) whose allow-list authorizes `strat_id`. If no enabled account block lists `strat_id`, resolution fails with `RefusalReason::InvalidStratId` (`INVALID_STRAT_ID`).
  - An explicit `account_ref` (`1..=255`) not present among enabled accounts fails with `RefusalReason::InvalidAccountId` (`INVALID_ACCOUNT_ID`).
  - An explicit `account_ref` (`1..=255`) whose allow-list omits `strat_id` fails with `RefusalReason::RejectRoutingIdMismatch` (`REJECT_ROUTING_ID_MISMATCH`).
- **Strategy `active` vs. global/per-account `enabled` flags:**
  - Every configured `strat_id` starts `active = true` and `enabled = true` on `configure`. An administrative `set_active(strat_id, false)` leaves any DOP TCP/Unix session in place while refusing new risk intents with `RefusalReason::InvalidStratId` (`INVALID_STRAT_ID`).
  - `connected[strat_id]` tracks whether `strat_id` currently holds a live DOP session (`DopLogin` refuses duplicate concurrent logins for an already-connected `strat_id` with `RefusalReason::InvalidStratId`; `DopLogout` clears `connected`).
  - `strat_enabled[strat_id]` (strategy-wide) and `account_strat_enabled[account_ref][strat_id]` (per `(account_ref, strat_id)` pair, packed into lock-free `AtomicU64` bitsets) track operator enablement. Disabling a strategy globally or on one `(account_ref, strat_id)` pair (`set_strat_enabled(account_ref, strat_id, false)`) atomically refuses subsequent `NewOrder` / `Modify` / `RfqQuote` intents on that scope with `RefusalReason::AdminAction` (`ADMIN_ACTION`) while keeping `Cancel` / `CancelAll` open so the ORS `REQ` thread can sweep and cancel open orders on that account.

## Command Evaluation Matrix

| Process `Mode` | `strat_id` Configured & `active` | `account_ref` Valid & Authorized | `enabled` (`strat` & `(account_ref, strat_id)`) | `PolicyAction::NewOrder` / `Modify` / `Quote` | `PolicyAction::Cancel` | `PolicyAction::AdminCancel` |
| --- | --- | --- | --- | --- | --- | --- |
| Any | No | — | — | `InvalidStratId` | `InvalidStratId` | Allowed (`ALL_STRATS` / valid) |
| Any | Yes | Unknown `account_ref` | — | `InvalidAccountId` | `InvalidAccountId` | `InvalidAccountId` |
| Any | Yes | Unauthorized `strat_id` | — | `RejectRoutingIdMismatch` | `RejectRoutingIdMismatch` | Allowed if admin |
| `Stopped` | Yes | Yes | Any | `MarketRejectMarketDown` | `MarketRejectMarketDown` | Allowed |
| `ManageOnly` | Yes | Yes | Any | `RestrictedCancelOnly` | Allowed | Allowed |
| `Trading` | Yes | Yes | `false` | `AdminAction` | Allowed | Allowed |
| `Trading` | Yes | Yes | `true` | Allowed (resolved `account_ref`) | Allowed | Allowed |

## Invariants and Verification

1. `#![forbid(unsafe_code)]`, zero floating-point arithmetic, and zero heap allocation on `evaluate`, `resolve_account_ref`, `set_mode`, `set_active`, and `set_strat_enabled`.
2. `PolicyConfig::validate` rejects duplicate `account_ref`s, `account_ref == 0`, empty `enabled_accounts` when accounts are required, reserved `strat_id == 255` in strategy allow-lists, and per-account allow-list entries that are not members of the process-wide `strategies` set (unless `strategies` is omitted, in which case the union of per-account allow-lists forms the process whitelist).
3. Verified by `cargo test --locked -p tickwire-runtime-policy --all-targets` alongside [`account-gateway.md`](account-gateway.md) and [`single-host-trading.md`](single-host-trading.md).
