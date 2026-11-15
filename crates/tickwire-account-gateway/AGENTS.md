# `tickwire-account-gateway` Engineering Guide (`AGENTS.md`)

Governing specifications: [`specs/implementation/account-gateway.md`](../../specs/implementation/account-gateway.md) and [`specs/implementation/command-binding.md`](../../specs/implementation/command-binding.md).

## Role and Ownership

`tickwire-account-gateway` owns two venue-agnostic execution building blocks:

1. **Multi-Account Configuration & Routing (`src/account_manager.rs`):**
   - `AccountSpec` (`account_ref = 1..=255`, `name`, `exchange_account_id`, `strategies` allow-list `0..=254`, redacted-`Debug` `Credentials`, `local_ip`, `nic`, `[local_ip, count]` `UrlBindSpec` pools, and per-account `AccountRateOverrides`).
   - `AccountManager` (`enabled_accounts` declaration-order loader, `O(1)` `account_ref = 0` resolution to the first authorized account, and exact `InvalidStratId` / `InvalidAccountId` / `RejectRoutingIdMismatch` refusal mapping).
2. **Stateless Execution Gateway (`src/lib.rs`, `src/binding.rs`, `src/pool.rs`, `src/websocket.rs`):**
   - Pure in-memory `Account` state machine + `StpBook` + `VerifiedLiveBootstrap` cold Exchange REST recovery.
   - Multi-slot `OrderConnectionPool` (`OrderRouter`) + `SharedRateLimiter::pick_and_spend_or_nack` admission (single route pick, immediate NACK on account or `local_ip` budget exhaustion without retrying sibling connections).
   - Per-order transport-doubt isolation (`OrderDoubt`) and bounded windowed eviction (`evict_retired_entries`) with **zero local WAL, journal, or recovery manifest**.

## Non-Negotiable Invariants

1. `#![forbid(unsafe_code)]` and exact fixed-point / integer arithmetic only (zero floating-point types).
2. **Secret Hygiene:** `Credentials` `Debug` output must always redact `key`, `secret`, and `passphrase` (`"[REDACTED]"`).
3. **Venue Agnosticism:** Zero dependencies on exchange-specific adapter crates or disk-backed journal/recovery crates.
4. **Code-Doc Lockstep:** Keep [`specs/implementation/account-gateway.md`](../../specs/implementation/account-gateway.md), [`specs/implementation/command-binding.md`](../../specs/implementation/command-binding.md), this `AGENTS.md`, and [`src/lib.rs`](src/lib.rs) `//!` rustdoc synchronized.
