# tickwire-quota

`tickwire-quota` (`#![forbid(unsafe_code)]`) owns supervisor-side rate-limit budget coordination
(`Coordinator<S>`, `QuotaStore`, `FileQuotaStore`), single-owner grant-based consumption
(`ExecLimiter`), and the preallocated multi-source-IP, class-isolated hot-path rate limiter
([`rate_limiter`](src/rate_limiter.rs): `RateLimiter`, `VenueRateLimiter`, `VenuePreset`). A
supervisor issues `Grant` and `LeaseRenewal` capabilities across `Normal` and `Recovery` classes,
while hot-path execution owners enforce orthogonal `ORDER`, `CANCEL`, and `REQUEST | WEIGHT`
windows per `(local_ip, interval_s, account, flags, symbol)` without heap allocation or floating-point
arithmetic; venue-specific endpoint rules belong in exchange quota adapters such as
`tickwire-binance-quota`.

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
* **Zero-allocation multi-IP and class-isolated `RateLimiter`**: Keep `IpKey`, `RateLimitId`,
  `FixedWindow`, and preallocated `RollingWindow` (`Box<[RollingEntry]>`) free of hot-path heap
  allocations and floating-point math in [src/rate_limiter.rs](src/rate_limiter.rs). `RateLimiter::allow`
  must pre-check monotonic time plus per-account and per-IP cooldowns before mutating any counter,
  and reverse-roll back every earlier debit via `put_back` if a later `Spend` rejects. Pure
  `allow_cancel` and `allow_query` calls in `VenueRateLimiter` must never touch `ORDER` windows,
  and `note_ip_cooldown` must isolate `429` throttling strictly to that `IpKey`.
* **Exclusive durable file store**: Maintain `0o700` directory permissions, `O_NOFOLLOW | O_CLOEXEC`
  opens, explicit `WriterLock::drop` unlock (preventing inherited pre-exec child descriptors from
  retaining ownership), `TWQUOTA1` + SHA-256 framing, and `STATE.pending-*` `sync_all` + rename +
  directory `sync_all` in [src/files.rs](src/files.rs).

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/files.rs](src/files.rs) ·
  [src/rate_limiter.rs](src/rate_limiter.rs) · [tests/quota.rs](tests/quota.rs) ·
  [tests/multi_ip_limiter.rs](tests/multi_ip_limiter.rs)
* **Specifications and models**: [quota.md](../../specs/implementation/quota.md) ·
  [binance-quota.md](../../specs/implementation/binance-quota.md) ·
  [request_budget](../../tla/execution_models/request_budget/ROADMAP.md)

```sh
cargo test --locked -p tickwire-quota --all-targets
./scripts/check-tla.sh request_budget
```
