# Tickwire Formal Verification Profile (`PROFILE.md`)

## 1. System Under Verification

Tickwire is a pure stateless 3-process Linux shared-memory (`memfd`-sealed) ultra-low-latency trading system anchored to `main` (`438551c`) ([November 2026 audit](audits/2026-11-16/README.md)):

- **Market-Data Publisher (`tickwire-md`):** Sole owner of public exchange WebSocket/REST depth feeds, sequence-continuity state machines, audited SIMD BBO parsers, bounded in-memory JOIN idempotency state, and SPMC `memfd` broadcast rings (`crates/tickwire-md`, `crates/tickwire-market-publisher`, `crates/tickwire-market-source`, `crates/tickwire-book`, `crates/tickwire-binance`, `crates/tickwire-shm`, `crates/tickwire-join`).
- **Strategy Workers (`tickwire-strategy-host` / `tickwire-strategy-sdk`):** Independent SHM ring readers exchanging fixed-size `IntentRecord` (`1024B`) and `ReportRecord` (`512B`) messages with the ORS execution process (`crates/tickwire-strategy-sdk`, `crates/tickwire-strategy-host`, `crates/tickwire-control`).
- **Unified Multi-UID ORS Execution Process (`1 ORS = 1 Venue`):** Manages `1..N` `AccountSession`s (`account_ref = 1..255`, with `0` resolving to the strategy's default authorized account) across dedicated `REQ` (`req_cpu`) and `RESP` (`resp_cpu`) hot threads isolated from cold `housekeeping_cpus` (`Dialer`, `OrsQueryDesk`, `LatencyTracer`, `ControlServer`). Recovers cold startup state exclusively from authenticated Exchange REST `Account + OpenOrders` snapshots paired with 32-character `[0-9a-z]` `tw1...` `SelfDescribingClientId` attribution (`crates/tickwire-runtime`, `crates/tickwire-account`, `crates/tickwire-account-gateway`, `crates/tickwire-runtime-policy`, `crates/tickwire-risk`, `crates/tickwire-transport`, `crates/tickwire-quota`, `crates/tickwire-binance-account`, `crates/tickwire-binance-execution`, `crates/tickwire-binance-private`, `crates/tickwire-binance-quota`, `crates/tickwire-spot-execution`, `crates/tickwire-clock`, `crates/tickwire-clock-sys`, `crates/tickwire-venue`).

## 2. Concurrency & Lock-Hierarchy Boundaries

- **Non-overlapping fine-grained spinlocks:** `LockedOrder` and `LockedConnection` spinlocks never overlap (`REQ` releases `LockedOrder` before acquiring `LockedConnection` to transmit a frame; `RESP` releases `LockedConnection` before acquiring `LockedOrder` to apply a venue event).
- **Leaf-only `StpBook` lock:** `StpBook` is acquired strictly while holding a single `LockedOrder` spinlock during resting-order insert, modify, or terminal unlink; send-path self-trade prevention (`self_cross`) reads published atomic best bid/ask ticks without locking `StpBook`.
- **Cold-thread handoffs:** `Dialer` hands freshly authenticated sockets to hot-path `OrderRouter` pools via lock-free `DialBaton` slots; `OrsQueryDesk` resolves uncertain orders (`-1007`, `-1000`, `MKT_DELAYED`, socket drops) asynchronously without blocking `REQ` or `RESP`.

## 3. Toolchain & Verification Gates

- **Finite-State Model Checking (`TLC 2.19`):** Pinned `tla/tools/tla2tools.jar` (`SHA-256: 936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`), orchestrated by `./scripts/check-tla.sh` with LPT multi-core scheduling, zero-touch `@TLAPlusOperator` bytecode overrides for large arithmetic/replay state spaces, and mandatory 100% nonzero action coverage (`scripts/tlc_coverage.py`).
- **Symbolic 64-Bit / 128-Bit Induction & BMC (`Apalache 0.63.0 + Z3`):** Snowcat-typed symbolic specifications (`L*_Sym*.tla`) verify inductive invariants (`IndInv`) and overflow-free `u32`/`u64`/`u128` linear integer arithmetic (`QF_LIA`) across unbounded execution horizons (`\forall k >= 0`).
- **Zero-Drift Source Anchors (`anchors.lock`):** Every modeled level under `tla/execution_models/*/L*/02_models/` tracks SHA-256 digests of its cited core-crate Rust source lines in `anchors.lock`, verified automatically by `anchors.sh check tla` and `./scripts/check-tla.sh`.
