# TLA+ Model Family Roadmap

Each directory under `tla/execution_models/` links its TLA+ specifications and finite `.cfg` bounds to concrete Rust source anchors, modeling assumptions, and recorded TLC outputs. Run any individual family from the repository root with `./scripts/check-tla.sh <family>` (for example, `./scripts/check-tla.sh account_gateway`).

## Verified Subsystem Boundaries

| Subsystem Boundary | Model Families & Scope |
| :--- | :--- |
| **Order Intake & Submission** | [Submission](execution_models/submission/ROADMAP.md) models local commit receipts, lost receipts, and frozen restarts. [Ordered Abort](execution_models/mock_abort/ROADMAP.md) verifies zero-byte unsent abort ordering and replay against the mock execution journal. |
| **Account State & First-Byte Gate** | [Account](execution_models/account/ROADMAP.md) checks multi-asset holds, late fills, reduce-only limits, and fee closure. [Account Gateway](execution_models/account_gateway/ROADMAP.md) composes the account reducer, local WAL, and synchronous authorization at the first TLS byte. |
| **Durable Journals & Closures** | [Physical Journal](execution_models/physical_journal/ROADMAP.md) models batch `fsync` and torn-tail truncation. [Business Journal](execution_models/business_journal/ROADMAP.md), [Account Journal](execution_models/account_journal/ROADMAP.md), and [Recovery Closure](execution_models/recovery_closure/ROADMAP.md) verify hash-chained records, atomic rename + directory sync durability, and dependency verification. |
| **Guarded Network Transport** | [Gateway](execution_models/gateway/ROADMAP.md) traces durable admissions into outbound sessions. [Transport](execution_models/transport/ROADMAP.md) models the exact transition from `0` accepted bytes (`NoBytes`) to `MayHaveSent`, partial writes, and TLS buffer flush. |
| **Market Data & Venue Recovery** | [Order Book](execution_models/book_state/ROADMAP.md) checks sequence continuity and partial snapshot coverage. [Binance Depth Sync](execution_models/binance_depth_sync/ROADMAP.md) models WebSocket delta buffering across HTTPS snapshots and epochs. [Binance Private Session](execution_models/binance_private_session/ROADMAP.md) tracks user-stream gaps, and [Spot Account Recovery](execution_models/spot_account_recovery/README.md) checks Bybit/OKX dual REST scans against private WAL cuts. |
| **Shared Memory & Local Control** | [Broadcast Ring](execution_models/broadcast_ring/ROADMAP.md) verifies SPMC batch claims, `RecordBorrow` guards, wraparound, and bank freeze under acquire/release semantics. [JOIN Owner](execution_models/join_owner/ROADMAP.md) models late-reader catch-up and unforgeable detachment, and [Control Requests](execution_models/control_requests/ROADMAP.md) covers authenticated sessions and idempotent request stores. |
| **Monotonic Time & Calibration** | [Clock](execution_models/clock/ROADMAP.md) and [System Clock](execution_models/system_clock/ROADMAP.md) verify server-time interval extrapolation under drift bounds and permanent fault latching on backward readings. |

---

## Audit History and Coverage Extensions

* **[November 2026 Single-Host Audit](audits/2026-11-05/README.md)** — Added dedicated models for multi-venue spot execution, multi-strategy shared-cash isolation, terminal fee/cash evidence, worker market handover, quota budgets, and local configuration activation.
* **[Follow-Up Regressions & Fixes](audits/2026-11-05/fixes.md)** — Documents the four boundary counterexamples uncovered during modeling and the corresponding Rust fixes and native regression tests.
* **[Crate Coverage Inventory](audits/2026-11-05/inventory.md)** — Maps each workspace crate to its current source anchors and remaining unmodeled boundaries (see also the [archived pre-single-host inventory](https://github.com/Obscurepawn/Tickwire/blob/dc50649ebdcada26f7740181c16139cd3b32064a/tla/ROADMAP.md)).
