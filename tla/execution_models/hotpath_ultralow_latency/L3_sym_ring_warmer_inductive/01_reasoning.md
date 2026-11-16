# Modeling reasoning — `L3_SymRingWarmerInductive`

## Scope and symbolic abstraction
1. **Unbounded SPMC `RecordBorrow` + Skolem witness consumer `c_star` (`crates/tickwire-shm/src/lib.rs:12-20,269-336,437-547`, `crates/tickwire-shm/src/mapped.rs:460-556,734-952`)**:
   - Replaces finite `RingCapacity = 2` and `Consumers = {1, 2}` with symbolic relational `ConstInit` (`RingCapacity >= 2`, `1 <= MaxBatch <= RingCapacity`, `WarmupInterval >= 0`) and a universal Skolem consumer projection `c_star` (`c_read`, `c_rel`, `c_active_borrow`) paired with aggregate minimum release watermark `env_min_rel <= c_rel`.
   - Proves `ZeroNoAliasWhileBorrowed` (`c_active_borrow > 0 => c_active_borrow > prod_pub - RingCapacity /\ ~(prod_claim_lo <= prod_claim_hi /\ c_active_borrow + RingCapacity \in prod_claim_lo .. prod_claim_hi)`) and `WatermarkMonotonicOrder` across `0 .. SEQUENCE_LIMIT`.
2. **Full 64-bit `u64` `WarmupTracker` + stateless ORS / `TlsLoopbackWarmer` (`crates/tickwire-transport/src/warmup.rs:57-422`, `crates/tickwire-runtime/src/ors.rs:2340-2380`, `crates/tickwire-runtime/src/session.rs:663-690`)**:
   - Models `0 .. 2^64 - 1` monotonic advances and backward clock jitter, live ORS balance/open-hold/sequence-floor/rate-limiter invariance against `shadow_*` variables, live vs. loopback `rustls` AEAD nonces, and unready-node step tracking.
3. **Inductive invariant `IndInv` (`02_models/L3_SymRingWarmerInductive.tla:628-664`)**:
   - Strengthens `SafetyConjunction` with 4 CTI lemmas (`CTI_Ring_CachedMinBound`, `CTI_Ring_ActiveBorrowPinsRelease`, `CTI_Ring_ActiveClaimWindow`, `CTI_Warmer_ControlCoupling`) so `Init => IndInv`, `IndInv /\ Next => IndInv'`, and `IndInv => SafetyConjunction` close in Z3 `QF_LIA`.
