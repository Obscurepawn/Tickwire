# `tickwire-runtime-policy` Engineering Guide (`AGENTS.md`)

Governing specification: [`specs/implementation/runtime-policy.md`](../../specs/implementation/runtime-policy.md).

## Role and Ownership

`tickwire-runtime-policy` provides the stateless, venue-agnostic control policy (`OrsRuntimePolicy` / `ControlPlane`) for the unified Order Routing System (ORS) execution process:

- **Stateless process `Mode`:** Lock-free atomic mode transitions across `Mode::Trading`, `Mode::ManageOnly` (`Mode::RestrictedCancelOnly`), and `Mode::Stopped`.
- **Strategy whitelist & account routing:** `O(1)` validation of `strat_id` (`0..=254`; `255` reserved for `ALL_ORS_STRATS`), `account_ref = 0` resolution to the first authorized account in `enabled_accounts` order, and explicit `account_ref = 1..=255` authorization checks (`InvalidStratId`, `InvalidAccountId`, `RejectRoutingIdMismatch`).
- **Granular `active`, `connected`, and `(account_ref, strat_id)` `enabled` bitsets:** Atomic `Ordering::AcqRel` bitsets supporting DOP single-login gating, strategy-wide enable/disable, and per-`(account_ref, strat_id)` enable/disable (`AdminAction`) without touching sibling accounts or holding any mutex.

## Non-Negotiable Invariants

1. `#![forbid(unsafe_code)]` and exact integer/bitset arithmetic only (zero floating-point types).
2. **Venue agnosticism:** Zero dependencies on exchange-specific adapter crates, local WAL/journal crates, lease renewal/preemption, or SHM quote freshness watermarks.
3. **Zero hot-path allocation:** `evaluate`, `resolve_account_ref`, `mode`, `set_mode`, `set_active`, `try_connect`, `disconnect`, `set_global_strat_enabled`, and `set_strat_enabled` execute in `O(1)` stack/atomic instructions with zero heap allocation.
4. **Code-Doc Lockstep:** Keep [`specs/implementation/runtime-policy.md`](../../specs/implementation/runtime-policy.md), this `AGENTS.md`, and [`src/lib.rs`](src/lib.rs) `//!` rustdoc synchronized on any API or behavioral change.
