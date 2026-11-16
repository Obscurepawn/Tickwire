# Stateless Execution Gateway and Multi-Account Configuration (`tickwire-account-gateway`)

## Scope and Responsibilities

[`tickwire-account-gateway`](../../crates/tickwire-account-gateway/src/lib.rs) provides two venue-agnostic building blocks for the unified ORS execution process:

1. **Multi-Account Configuration & Routing Loader ([`account_manager.rs`](../../crates/tickwire-account-gateway/src/account_manager.rs)):** Parses and validates `AccountManagerConfig`, `AccountSpec` (`account_ref = 1..=255`, `name`, `exchange_account_id`, `strategies` allow-list, redacted `Credentials`, `local_ip`, `nic`, `[local_ip, count]` `UrlBindSpec` pools, and per-account `AccountRateLimitConfig` / `AccountRateOverrides`), and deterministic `account_ref = 0` resolution in `enabled_accounts` order.
2. **Stateless Execution Gateway ([`lib.rs`](../../crates/tickwire-account-gateway/src/lib.rs), [`pool.rs`](../../crates/tickwire-account-gateway/src/pool.rs), [`binding.rs`](../../crates/tickwire-account-gateway/src/binding.rs), [`websocket.rs`](../../crates/tickwire-account-gateway/src/websocket.rs)):** Couples an in-memory [`tickwire_account::Account`](../../crates/tickwire-account/AGENTS.md) ledger, per-symbol self-trade prevention (`StpBook`), cold Exchange REST bootstrap (`VerifiedLiveBootstrap`), per-order transport-doubt isolation, multi-connection `[local_ip, count]` `OrderConnectionPool` routing, and process-wide [`SharedRateLimiter`](../../crates/tickwire-quota/AGENTS.md) admission with **zero local WAL, journal, or disk recovery state**.

`tickwire-account-gateway` compiles under `#![forbid(unsafe_code)]`, performs zero floating-point math, and names zero exchange-specific execution crates.

## `AccountSpec` and `AccountManager` (`account_manager.rs`)

An ORS process serves one venue and `1..=255` exchange UIDs (`1 AccountSession = 1 Exchange UID`):

- **Credentials redaction:** `Credentials { key, secret, passphrase }` implements custom `fmt::Debug` that always renders `"[REDACTED]"` for every secret field so API keys, HMAC/Ed25519 secrets, and passphrases never leak into diagnostic logs or panic traces.
- **Connection pool descriptors (`UrlBindSpec`):** Supports both compact `[local_ip, count]` tuple syntax (e.g. `["10.0.1.11", 2]` or `["", 2]`) and structured `{ local_ip, nic, count, colo }` configuration. When a session omits `urls`, `AccountSpec::effective_url_binds` synthesizes a default 2-connection pool bound to the account's `nic` / `local_ip`. `count == 0` or total connections exceeding `MAX_ORDER_CONNECTIONS` (`16`) per URL is rejected at startup.
- **`enabled_accounts` deterministic ordering & `account_ref = 0`:**
  - `AccountManager::from_config` loads only accounts listed in `enabled_accounts`, preserving exact declaration order, and assigns or verifies unique `account_ref` values in `1..=255` and unique non-zero `exchange_account_id`s.
  - `AccountManager::resolve_account_ref(requested_account_ref, strat_id)`:
    * Rejects `strat_id == 255` (`ALL_ORS_STRATS`) or any `strat_id` absent from the ORS whitelist with `AccountRoutingError::InvalidStratId` (`RefusalReason::InvalidStratId`).
    * When `requested_account_ref == 0`, returns the **first** enabled `AccountSpec` in `enabled_accounts` order whose `strategies` list contains `strat_id`, or `AccountRoutingError::InvalidStratId` (`RefusalReason::InvalidStratId`) if no enabled block lists `strat_id`.
    * When `requested_account_ref` is `1..=255`, looks up that exact `AccountSpec`—returning `AccountRoutingError::InvalidAccountId` (`RefusalReason::InvalidAccountId`) if absent, or `AccountRoutingError::RoutingIdMismatch` (`RefusalReason::RejectRoutingIdMismatch`) if its `strategies` list omits `strat_id`.

## Cold-Start Exchange REST Bootstrap (`VerifiedLiveBootstrap`)

Cold boot and restart reconstruct open-order holds and `SelfDescribingClientId` (`tw1...`) attribution exclusively from authenticated Exchange REST snapshots:

- `AccountGateway::create` and `AccountGateway::create_unconnected` initialize an empty reconciled account from a static `CanonicalBaseline` asset balance snapshot (for unit tests and clean accounts with zero open orders).
- `AccountGateway::create_from_live_exchange` and `AccountGateway::create_from_live_exchange_unconnected` consume a `VerifiedLiveBootstrap` (`VerifiedLiveBootstrap::new(snapshot, catalog, open_orders, freeze_on_external)` plus optional `with_imported_commands`), verify `SelfDescribingClientId` domain-keyed MAC tags on every live exchange order, reconstruct collateral reservations, populate `StpBook` resting entries, and enforce monotonic `writer_epoch` / `next_order_seqnum` floors without reading any local file.

## Stateless Submission, `SharedRateLimiter` Admission, and Doubt Isolation

1. **Pre-send admission & single-pick rate limiting:**
   - Outbound `NewOrder`, `Cancel`, and `AmendKeepPriority` / `CancelReplace` / `Modify` commands validate `Mode` ([`runtime-policy.md`](runtime-policy.md)), `SelfDescribingClientId` binding ([`command-binding.md`](command-binding.md)), self-trade prevention (`StpBook`), and fixed-point balance reservation in memory.
   - `OrderConnectionPool` selects at most **one** ready connection matching `product_mask` (`OrderRouter::pick`). When paired with `OrsAccountQuota` / `SharedRateLimiter`, rate-limit tokens are spent against the picked connection's `local_ip` (`pick_and_spend_or_nack`). If either account or `local_ip` budget refuses, the gateway rolls back the in-memory reservation and returns `Error::RateLimited(RateReject)` immediately **without retrying a sibling connection**.
2. **Per-order transport-doubt isolation:**
   - If a socket disconnects or retires after bytes may have reached the kernel (`SendState::MayHaveSent` / `Queued`), **only** the affected in-flight order transitions to `tickwire_account::DoubtReason` (`PlaceAmbiguous`, `CancelAmbiguous`, or `AmendAmbiguous`) while sibling orders on healthy connections continue trading uninterrupted.
   - Ambiguous orders retain their conservative worst-case collateral hold until an authoritative user-data stream frame or `QueryDesk` REST `StatusAsk` reconciles the order (`ProjectedFill` / `Ack` / `Canceled` / `Rejected`).
3. **Distinct execution facts & windowed eviction:**
   - `FrontAck` / `Ack`, order-status cumulative fill projections (`ProjectedFill`), and individual trade fills (`Fill`, including post-trade fee/rebate amendments) remain distinct facts deduplicated by trade ID.
   - Terminal entries and processed client command records are bounded in memory via `AccountGateway::evict_retired_entries`.

## Verification

- [`tests/account_manager.rs`](../../crates/tickwire-account-gateway/tests/account_manager.rs): `AccountSpec` validation, `Credentials` redaction, `enabled_accounts` ordering, and `account_ref = 0` routing.
- [`tests/live_bootstrap.rs`](../../crates/tickwire-account-gateway/tests/live_bootstrap.rs): cold Exchange REST `VerifiedLiveBootstrap` reconstruction and `writer_epoch` floor checks.
- [`tests/submission.rs`](../../crates/tickwire-account-gateway/tests/submission.rs): stateless order placement, cancel, STP check, and `SharedRateLimiter` single-pick NACK rollback.
- [`tests/per_order_isolation.rs`](../../crates/tickwire-account-gateway/tests/per_order_isolation.rs): per-order transport doubt isolation without freezing healthy sibling orders.
- [`tests/multi_slot_pool.rs`](../../crates/tickwire-account-gateway/tests/multi_slot_pool.rs): multi-connection `[local_ip, count]` routing, EWMA hysteresis, and reconnect slot recovery.
- [`tests/windowed_eviction.rs`](../../crates/tickwire-account-gateway/tests/windowed_eviction.rs): bounded in-memory eviction of terminal order and command entries.
- [`tests/lifecycle.rs`](../../crates/tickwire-account-gateway/tests/lifecycle.rs), [`tests/ingress.rs`](../../crates/tickwire-account-gateway/tests/ingress.rs), [`tests/websocket.rs`](../../crates/tickwire-account-gateway/tests/websocket.rs): end-to-end command lifecycle, ingress deduplication, and WebSocket framing.
