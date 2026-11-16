# Tickwire TLA+ & Apalache Verification Ledger (`MEMORY.md`)

[November 2026 Stateless Multi-UID ORS Audit](audits/2026-11-16/README.md) · [Model Family Roadmap](ROADMAP.md) · [Verification Profile](PROFILE.md) · [Case Law](LESSONS.md)

Aggregate inventory across `tla/execution_models/`: **`21` model families, `40` finite-state TLC 2.19 models, `11` Apalache 0.63.0 + Z3 Snowcat symbolic `IndInv` models, and `44,618` zero-drift SHA-256 Rust source line anchors** verified on `main` (`4c52ff7`).

---

## Stateless Multi-UID ORS Families (2026-11-16, `main` @ `4c52ff7`)

### `request_budget / L0_shared_ors_limiter + L1_sym_shared_limiter` — PASS

- **Mode:** Exhaustive TLC (`L0_SharedOrsLimiter`, 2 configs) + Dual-engine Symbolic Apalache `0.63.0` + Z3 (`L1_SymSharedLimiter`, `IndInv` + `ConstInit` over `0 .. 2^64 - 1` & `u128`) + TLC bounded action-coverage smoke (2 configs).
- **Property:** Cross-account shared IP rolling windows (`RateRule::ip`, `account = 0`) + per-account `ORDER`/`CANCEL`/`REQUEST` windows, exact reverse-order `put_back_undo` rollback across coalesced active sub-buckets, strict single-pick `PublishedRoutes::pick -> spend(local_ip) -> NACK` without sibling connection retry, per-account `418` freeze isolation vs. per-IP `429` cooldown, and rising-edge `ExchRateLimit` (`80%` warning / `100%` ceiling) reconciliation (`SharedAndAccountCapsRespected`, `TransactionalRollbackExact`, `SinglePickNeverRetriesSibling`, `CrossAccountFreezeAndCancelIsolation`, `RisingEdgeExchLatchExact`, `U64AndU128OverflowSafe`).
- **Apalache/Z3 & TLC:** `L0` TLC `93,324` / `6,786` distinct (`1,333,208` generated), `14/14` actions; `L1` `IndInv` inductive proof `PASS` (`112` base VCs `5s` + `24` step transitions `11s` + entailment `5s`, `NoError`), BMC `k=2` `PASS`, TLC `1,394` / `1,337` distinct, `14/14` actions; `2,136` line anchors (`0` drift). [L0 run log](execution_models/request_budget/L0_shared_ors_limiter/03_run_log.md) · [L1 run log](execution_models/request_budget/L1_sym_shared_limiter/03_run_log.md) · [L1 output](execution_models/request_budget/L1_sym_shared_limiter/05_output.md)

### `connection_router_quota / L0_pool_router + L1_multi_ip_quota + L2_sym_rate_limiter` — PASS

- **Mode:** Exhaustive TLC (`L0_PoolRouter`, `L1_MultiIpQuota`) + Dual-engine Symbolic Apalache `0.63.0` + Z3 (`L2_SymRateLimiter`, `IndInv` + `ConstInit`) + TLC smoke.
- **Property:** Pooled `OrderRouter` (`Exclusive` & `Shared` slots, warmup round-robin, `9/8` EWMA hysteresis, epoch fencing), `DialBaton` reconnect guarding, pre-EOF socket draining, multi-window `FixedWindow` / `RollingWindow` caps, reverse-order `put_back` rollback, `can_afford == allow` parity across duplicate `RateLimitId` spends, and `u64`/`u128` overflow freedom.
- **Apalache/Z3 & TLC:** `L0` TLC `27,599` / `18,129` distinct (`100%` action coverage); `L1` TLC `20,428` / `2,349` distinct (`100%` action coverage); `L2` `IndInv` inductive proof `PASS` (`NoError`), TLC `104,416` / `110,407` distinct (`13/13` actions); `3,292` line anchors (`0` drift). [L0 output](execution_models/connection_router_quota/L0_pool_router/05_output.md) · [L1 output](execution_models/connection_router_quota/L1_multi_ip_quota/05_output.md) · [L2 output](execution_models/connection_router_quota/L2_sym_rate_limiter/05_output.md)

### `strategy_intake / L0_ors_session_routing + L1_sym_ors_session_routing` — PASS

- **Mode:** Exhaustive TLC (`L0`, 2 configs) + Dual-engine Symbolic Apalache `0.63.0` + Z3 (`L1`, `IndInv` + `ConstInit` over 64-bit seqnums and timestamps) + TLC action-coverage smoke.
- **Property:** Multi-UID `account_ref = 0..255` resolution (`0` -> strategy's default authorized UID), granular `(account_ref, strat_id)` `set_strat_enabled` gating with automatic open-order cancellation, `DopState` monotonic `max(login_seq, next_order_seqnum)` across worker restarts, `last_recv_seqnum` gap detection, and `max_order_age_ns` (`23,000 ns`) `RejectStaleOrder` pre-send rejection.
- **Apalache/Z3 & TLC:** `L0` TLC `21,488` / `73,856` distinct (`230,651` / `891,287` generated), `15/15` actions; `L1` `IndInv` inductive proof `PASS` (`NoError`, `4s`), BMC `k=8` `PASS` (`28.14s`), TLC `15,936` distinct (`172,114` generated), `15/15` actions; `1,592` line anchors (`0` drift). [Roadmap](execution_models/strategy_intake/ROADMAP.md)

### `transport`, `hotpath_decoupling` & `hotpath_ultralow_latency` — PASS

- **Mode:** Exhaustive TLC (`transport/L0_first_output`, `hotpath_decoupling/{L0_dial_querier,L1_windowed_pool}`, `hotpath_ultralow_latency/{L0,L1,L2}`) + Dual-engine Symbolic Apalache `0.63.0` + Z3 (`transport/L1_sym_dual_thread_switchover` and `hotpath_ultralow_latency/{L3,L4,L5}`, `IndInv` + `ConstInit`).
- **Property:** Lock-free `DialBaton` connection handoff, strict non-overlapping `LockedOrder` vs. `LockedConnection` spinlock hierarchy (`LockedOrder -> OrsStpBook` leaf-only acquisition, lock-free atomic BBO `self_cross`), TLS short-write connection self-retirement, server maintenance `notice` (`64008`) smooth drain and switchover without dropping in-flight replies, lifetime-pinned zero-copy SHM borrows, isolated `TlsLoopbackWarmer` AEAD nonces, and SIMD BBO oracle parity.
- **Apalache/Z3 & TLC:** `transport/L0` `688` / `3,928` / `5,944` / `2,832` distinct (`16/16` actions); `transport/L1_SymDualThreadSwitchover` `IndInv` `PASS` (`NoError`, `4s`), BMC `k=8` `PASS` (`5.35s`), TLC `2,056` distinct (`26/26` actions); `hotpath_decoupling/L0` `42,272` / `283,734` distinct (`21/21` actions); `hotpath_decoupling/L1` `51,469` / `340,142` distinct (`17/17` actions); `hotpath_ultralow_latency` `L0..L5` TLC + `L3/L4/L5` Apalache `IndInv` `PASS` (`NoError`, `100%` action coverage); `11,393` line anchors (`0` drift). [Transport roadmap](execution_models/transport/ROADMAP.md) · [Decoupling roadmap](execution_models/hotpath_decoupling/ROADMAP.md) · [Ultra-low-latency roadmap](execution_models/hotpath_ultralow_latency/ROADMAP.md)

### `account`, `binance_roundtrip` & `spot_roundtrip` — PASS

- **Mode:** Exhaustive TLC (`account/L0_ors_slot_fill_recycle`, `binance_roundtrip/L0_binance_ors_bootstrap`, `spot_roundtrip/L0_uta_ors_bootstrap`) + Dual-engine Symbolic Apalache `0.63.0` + Z3 (`account/L1_sym_ors_order_lifecycle`, `IndInv` + `ConstInit`).
- **Property:** Power-of-two `OrderManager` 4-state (`EMPTY -> CLAIMED -> LIVE -> RETIRED` / `SlotState::Retired`) slot recycle with post-lock `(strat_id, seqnum)` identity recheck, immediate retirement + `OrsStpBook` unlink on `Canceled`/`Rejected`/`Filled` without waiting on `fee_reconciled`, `FrontAck`/`Ack` separation, `cxl_qty = order_qty - projected_fill_qty`, `FillRecordPool` trade-ID deduplication with `amendment = true` fee/rebate updates, unified multi-wallet `AssetKey` solvency, and cold Exchange REST `Account + OpenOrders` + `tw1...` `SelfDescribingClientId` recovery.
- **Apalache/Z3 & TLC:** `account/L0` `97,889` / `220` distinct (`14/14` actions); `account/L1_SymOrsOrderLifecycle` `IndInv` `PASS` (`101` VCs, `NoError`, `4s`), BMC `k=5` `PASS` (`7s`), TLC `870` / `1,382` distinct (`12/12` actions); `binance_roundtrip/L0` `211` / `211` distinct (`8/8` actions); `spot_roundtrip/L0` `2,773` / `2,773` distinct (`10/10` actions); `9,876` line anchors (`0` drift). [Account roadmap](execution_models/account/ROADMAP.md) · [Binance roundtrip roadmap](execution_models/binance_roundtrip/ROADMAP.md) · [Spot roundtrip roadmap](execution_models/spot_roundtrip/ROADMAP.md)

### `query_desk / L0_ors_query_desk + L1_sym_ors_query_desk` — PASS

- **Mode:** Exhaustive TLC (`L0`, 2 configs) + Dual-engine Symbolic Apalache `0.63.0` + Z3 (`L1`, `IndInv` + `ConstInit`) + TLC smoke.
- **Property:** Per-account `OrsQueryDesk` doubt resolution across `-1007` / `-1000` / `MKT_DELAYED` / socket-disconnect `StatusAsk` enqueue, `Place` + `Cancel` `cancel_after` coalescing, bounded `Cancel` exponential backoff (`<= 8` retries), snapshot fill projection prior to remainder cancel, non-`LIVE`-only slot eviction on full table, and `max_query_count` `InternalCancel` release.
- **Apalache/Z3 & TLC:** `L0` TLC `672,496` / `479,654` distinct (`2,404,480` / `1,791,828` generated, `21/21` actions); `L1_SymOrsQueryDesk` `IndInv` `PASS` (`72` VCs, `NoError`, `4s`), BMC `k=5` `PASS` (`4s`), TLC `6,069` / `4,669` distinct (`15/15` actions); `3,613` line anchors (`0` drift). [Roadmap](execution_models/query_desk/ROADMAP.md)

---

## Retained Core Market-Data, SHM, Microstructure, Risk & Clock Families (`main` @ `4c52ff7`)

- **`binance_depth_sync` (`L0_coordinator` + `L1_sym_coordinator`):** TLC (`4` configs + `2` symbolic smoke configs, `23/23` actions) + Apalache/Z3 64-bit `IndInv` (`NoError`, `1,267` anchors, `0` drift). [Roadmap](execution_models/binance_depth_sync/ROADMAP.md)
- **`clock` (`L0_calibration` + `L1_sym_calibration`) & `system_clock` (`L0_native_owner`):** TLC (`4` + `2` + `4` configs, `16/16` and `21/21` actions) + Apalache/Z3 64-bit/`u128` `IndInv` (`NoError`, `956` anchors, `0` drift). [Clock roadmap](execution_models/clock/ROADMAP.md) · [System clock roadmap](execution_models/system_clock/ROADMAP.md)
- **`market_making_microstructure` (`L0_stp_modify` + `L1_projected_fill`) & `risk_limits` (`L0_cash_envelope`):** TLC (`2` + `2` + `2` configs, `14/14`, `11/11`, and `5/5` actions, `4,877` anchors, `0` drift). [Microstructure roadmap](execution_models/market_making_microstructure/ROADMAP.md) · [Risk roadmap](execution_models/risk_limits/ROADMAP.md)
- **`broadcast_ring` (`L0_publish_borrow_reclaim`), `join_owner` (`L0_pending_activation`) & `control_requests` (`L0_registry` + `L1_session`):** TLC (`5` + `4` + `3` + `3` configs, `18/18`, `24/24`, `5/5`, and `8/8` actions, `2,423` anchors, `0` drift). [Broadcast ring roadmap](execution_models/broadcast_ring/ROADMAP.md) · [JOIN roadmap](execution_models/join_owner/ROADMAP.md) · [Control roadmap](execution_models/control_requests/ROADMAP.md)
- **`book_state` (`L0_lifecycle` + `L1_coverage`), `quote_delivery` (`L0_pair` + `L0_pair_v2`) & `binance_private_session` (`L0_session_lifecycle`):** TLC (`4` + `4` + `2` + `2` + `4` configs, `20/20`, `16/16`, `7/7`, and `34/34` actions, `3,193` anchors, `0` drift). [Book roadmap](execution_models/book_state/ROADMAP.md) · [Quote delivery roadmap](execution_models/quote_delivery/ROADMAP.md) · [Private session roadmap](execution_models/binance_private_session/ROADMAP.md)
