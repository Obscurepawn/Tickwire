# `tickwire-runtime` Engineering Guide and Invariants

Governing specification: [`specs/implementation/single-host-trading.md`](../../specs/implementation/single-host-trading.md).

## 1. Purpose and Venue-Agnostic Boundary

`tickwire-runtime` implements the generic, venue-agnostic Unified Order Routing System (`ORS`) process framework for Tickwire's stateless 3-process shared-memory topology (`1 ORS process = 1 Venue`, `1 AccountSession = 1 Exchange UID`):

- **Generic code never names a venue:** Zero dependencies on exchange-specific adapter crates. All venue wire framing, login/subscription handshakes, ping/pong framing, inbound event decoding, and REST reconciliation queries plug into [`VenueSession`](src/session.rs).
- **Zero market-data ownership:** Holds zero public market-data sockets and zero SHM quote subscriptions. Quote freshness on `REQ` is enforced strictly against `IntentBody::tick_mono_ns` (`max_order_age_ns`, default `23_000 ns` = `23 us`), refusing only the stale order (`RefusalReason::RejectStaleOrder`) without freezing any account or strategy.
- **Pure stateless memory & cold REST bootstrap:** Zero WAL, journal, blob-store, owner-lease, or handover files. Cold startup seeds `OrderManager` exclusively via `VenueSession::bootstrap_snapshots` (`Account + OpenOrders`) paired with `SelfDescribingClientId` (`tw1...`).

## 2. Thread / CPU Isolation & Spinlock Hierarchy

- **`#![forbid(unsafe_code)]` and zero floating-point arithmetic** across the entire crate; all Linux CPU pinning and signal syscalls delegate to [`tickwire-control-sys`](../tickwire-control-sys/AGENTS.md).
- **Disjoint hot vs. housekeeping cores (`OrsCpuConfig`):**
  - Validated at startup via `validate_cpu_isolation(&[("req_cpu", req_cpu), ("resp_cpu", resp_cpu)], housekeeping_cpus.as_ref())` (`req_cpu != resp_cpu`, `hot_cpus ∩ housekeeping_cpus == ∅`).
  - `REQ` (`tw-ors-req`) busy-spins with zero `sleep` on `req_cpu`, polling strategy `IntentReader`s (`1,024B`) and draining `control -> REQ` `AdminCommands` when idle.
  - `RESP` (`tw-ors-resp`) drives `mio::Poll` (`epoll_wait_ms`, default `0`) on `resp_cpu`, owning all `ReportWriter`s (`512B`) as sole writer.
  - Every cold thread—`Dialer` (`tw-dialer`), per-account `AsyncRestQuerier` (`tw-rest-query`), `LatencyTracer` (`tw-latency-sink`), DOP `ControlServer` (`tw-ors-ctrl`), and the `main` `SignalWaiter` loop—is explicitly pinned to `housekeeping_cpus`.
- **Spinlock hierarchy (non-negotiable):**
  - Never overlap `Order` and `Connection` spinlocks (`OrderManager` releases `LockedOrder` before `REQ` calls `LockedConnection::send_frame`; `RESP` releases `LockedConnection` before calling `OrderManager`).
  - Acquire `OrsStpBook` only beneath a held `LockedOrder` (`OrderLockProof`); send-path STP reads atomic opposite-side BBO lock-free.
  - Zero logging, zero heap allocation, and zero protocol parsing while holding any spinlock.

## 3. Single-Pick Rate Limiting & `dry_run` Discipline

- Outbound route admission picks one ready connection matching `OrderRoute.product_mask` and spends against `SharedRateLimiter` using that picked connection's `local_ip`. If account or IP quota refuses, `REQ` **NACKs immediately (`RefusalReason::RateLimited`) without retrying another connection**.
- `dry_run` (`IntentBody::intent_flags & DRY_RUN != 0`) uses `OrderManager`'s dedicated per-strategy scratch slot, releases holds immediately, and skips route selection, rate-limiter spend, and socket writes.

## 4. Verification

```sh
cargo fmt --all -- --check
cargo test --locked -p tickwire-runtime --all-targets
cargo clippy --locked -p tickwire-runtime --all-targets -- -D warnings
python3 scripts/verify-documentation.py
```
