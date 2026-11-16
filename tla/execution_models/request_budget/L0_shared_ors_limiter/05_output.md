# Verification output — `L0_SharedOrsLimiter`

- **Model**: [`02_models/L0_SharedOrsLimiter.tla`](02_models/L0_SharedOrsLimiter.tla)
- **Configs**: [`02_models/L0_SharedOrsLimiter.cfg`](02_models/L0_SharedOrsLimiter.cfg), [`02_models/L0_SharedOrsLimiter_multi_ip.cfg`](02_models/L0_SharedOrsLimiter_multi_ip.cfg)
- **Anchor lock**: [`02_models/anchors.lock`](02_models/anchors.lock) (`0` drifted lines)
- **Action coverage**: `14/14` (`100%` nonzero across `Init` and all 13 `Next` transitions)
- **Verified properties**:
  1. `SharedAndAccountCapsRespected`: Cross-account `RateRule::ip` (`account = 0`) sub-bucket sums and per-account `ORDER`, `CANCEL`, and `REQUEST` windows never exceed configured caps.
  2. `AtomicMultiBucketRollbackExact`: Partial-failure multi-bucket spends (`[AcctOrd -> SharedIp]`, `[AcctCxl -> SharedIp]`, `[SharedIp -> AcctOrd]`) unwind in reverse order via `put_back_undo` without leaking debits or disturbing sibling account units in shared rolling sub-buckets.
  3. `SinglePickNeverRetriesSibling`: `PublishedRoutes::pick -> SharedRateLimiter::pick_and_spend_or_nack` evaluates at most one route per request and returns `RateLimitedNack` immediately without retrying sibling connections.
  4. `CancelAndQueryImmuneToOrderCap` & `CrossAccountFreezeAndIpCooldownIsolation`: `ORDER` exhaustion never blocks `Cancel` or `Query`; Account 1 `418` freeze / cooldown never blocks Account 2 on a shared IP; IP 1 `429` cooldown never blocks IP 2.
  5. `RisingEdgeExchLatchSound`: `reconcile_venue_usage` emits `WarnThreshold` (`>= 80%`) and `CeilingReached` (`>= 100%`) strictly on upward crossings without mutating local spend buckets, re-arming only when reported usage drops strictly below the respective threshold.
