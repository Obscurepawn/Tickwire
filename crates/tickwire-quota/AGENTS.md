# tickwire-quota

`tickwire-quota` (`#![forbid(unsafe_code)]`) owns supervisor-side rate-limit budget coordination
(`Coordinator<S>`, `QuotaStore`, `FileQuotaStore`), single-owner grant-based consumption
(`ExecLimiter`), single-writer multi-IP windows ([`rate_limiter`](src/rate_limiter.rs):
`RateLimiter`, `VenueRateLimiter`, `VenuePreset`), and the process-wide cross-account lock-free
atomic ORS rate limiter ([`shared`](src/shared.rs): `SharedRateLimiter`, `OrsAccountQuota`,
`OrsVenueProfile`, `IpBudgetSpec`, `RouteAdmissionReject`, `ExchRateLimitEvent`). Hot-path
execution owners enforce orthogonal `ORDER`, `CANCEL`, and `REQUEST | WEIGHT` windows per
`(local_ip, interval_s, account, flags, symbol)` with zero heap allocation on the send path and
exact integer arithmetic (zero floating point); venue-specific endpoint adapters live in crates
such as `tickwire-binance-quota`.

## Invariants and Change Guidelines

* **Two-phase commit and CAS poisoning**: Keep `prepare_windows`, `prepare_issue`, and
  `prepare_renew_consumer` side-effect-free in [src/lib.rs](src/lib.rs); only `commit_*` persists
  incremented revisions via `QuotaStore::compare_exchange` and mints non-cloneable `Grant` or
  `LeaseRenewal` values. Any store failure poisons `Coordinator` (`Error::Store` /
  `Error::Poisoned`) so callers reopen durable state instead of retrying an ambiguous commit
  blindly.
* **Reserve protection and reclamation**: Enforce `accounted <= limit` and `Normal` allocations
  `<= limit - recovery_reserve` across non-overlapping `WindowConfig` intervals per `BucketId`.
  Unspent grant capacity returns to `Coordinator::reclaim` only through a `StoppedReceipt`
  produced by consuming `ExecLimiter::stop(self)`.
* **Idempotent local consumption**: `ExecLimiter::consume` in [src/lib.rs](src/lib.rs) deduplicates
  retries by `AttemptId`, freezing the limiter (`Error::Conflict`) if a repeated `AttemptId` changes
  `digest`, `class`, or `costs`, and checking consumer lease, cooldown, grant expiry, and window
  `[starts_ns, ends_ns)` bounds before debiting.
* **Lock-free cross-account `SharedRateLimiter` & single-pick NACK**: In [src/shared.rs](src/shared.rs),
  `RateRule::ip` (`account = 0`, `ips = [{ ip, ip_weight_min }]`) shares one atomic per-IP weight
  window across all `AccountSession`s, `REQ` sends, and `AsyncRestQuerier` requests bound to that
  `local_ip`. Fixed windows pack `(epoch: u32, used: u32)` in `AtomicU64`; sliding windows
  (`RateFlags::ROLLING`) maintain `20` atomic sub-buckets plus a conserved `in_window_sum` where
  only the CAS winner evicting an expired sub-bucket subtracts its count. Multi-bucket `allow`
  rolls back earlier debits via atomic delta subtraction on rejection, `pick_and_spend_or_nack`
  invokes the route picker `FnOnce` at most once (returning `RouteAdmissionReject::RateLimitedNack`
  immediately without retrying another connection), and `reconcile_venue_usage` emits rising-edge
  `ExchRateLimitEvent` notifications at `exch_warn_pct` (`80%`) and `100%` ceiling (re-arming only
  after usage drops strictly below threshold).
* **Compiled venue defaults & action-class isolation**: Keep built-in defaults accurate across
  Binance (`Spot`, `USD-M`, `COIN-M`, `Options`), `OKX UTA`, and `Bybit UTA` in both
  [src/shared.rs](src/shared.rs) and [src/rate_limiter.rs](src/rate_limiter.rs). Pure `allow_cancel`
  and `allow_query` calls must never touch `ORDER` windows, and `note_ip_cooldown` must isolate
  HTTP `429` throttling strictly to that `IpKey`.
* **Exclusive durable file store**: Maintain `0o700` directory permissions, `O_NOFOLLOW | O_CLOEXEC`
  opens, explicit `WriterLock::drop` unlock, `TWQUOTA1` + SHA-256 framing, and `STATE.pending-*`
  `sync_all` + rename + directory `sync_all` in [src/files.rs](src/files.rs).

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/files.rs](src/files.rs) ·
  [src/rate_limiter.rs](src/rate_limiter.rs) · [src/shared.rs](src/shared.rs) ·
  [tests/quota.rs](tests/quota.rs) · [tests/multi_ip_limiter.rs](tests/multi_ip_limiter.rs) ·
  [tests/shared_rate_limiter.rs](tests/shared_rate_limiter.rs)
* **Specifications and models**: [quota.md](../../specs/implementation/quota.md) ·
  [binance-quota.md](../../specs/implementation/binance-quota.md) ·
  [request_budget](../../tla/execution_models/request_budget/ROADMAP.md)

```sh
cargo test --locked -p tickwire-quota --all-targets
./scripts/check-tla.sh request_budget
```
