# Durable Shared Request Quotas and Lock-Free Cross-Account Rate Limiting

`tickwire-quota` partitions shared venue rate-limit budgets across independent local processes without permitting split-brain over-issuance across crashes or lost acknowledgements, and provides both a single-writer preallocated `RateLimiter` (`VenueRateLimiter`) and a process-wide cross-account lock-free atomic `SharedRateLimiter` (`shared`) for multi-session ORS execution. In the durable supervisor plane, a `BucketId` identifies `(namespace, ScopeKind [Ip | Uid | Account | Connection], subject, pool)`, and all consumers drawing from the same `WindowKey { bucket, window }` share a single immutable `WindowConfig` (`starts_ns < ends_ns`, `limit > 0`, `recovery_reserve <= limit`). Every subsequent window for a given `BucketId` requires a strictly larger `window` ID and non-overlapping interval (`new.starts_ns >= previous.ends_ns`), evaluated within a single trusted local `ClockDomain`.

Built in safe Rust (`#![forbid(unsafe_code)]`) with exact integer arithmetic (`u64`/`u128`, zero floating point), the crate separates durable supervisor allocation from hot-path spending: `Coordinator<S>` owns atomic store persistence of supervisor epochs, windows, consumer registrations, and `Grant` allocations, `ExecLimiter` owns non-`Clone` `Grant` capabilities and local attempt deduplication, `rate_limiter` (`RateLimiter`, `VenueRateLimiter`, `VenuePreset`) enforces single-owner multi-IP windows, and `shared` (`SharedRateLimiter`, `IpBudgetSpec`, `OrsVenueProfile`, `ExchRateLimitEvent`) provides lock-free atomic fixed and sub-bucket sliding (`RateFlags::ROLLING`) counters shared across `1..=255` `AccountSession`s, `REQ`, `RESP`, and `AsyncRestQuerier` threads. Endpoint-specific cost tables, `Normal` vs `Recovery` classification, and HTTP `429`/`418` feedback belong to venue adapters ([binance-quota.md](binance-quota.md)).

## State Model and Bounds

`Coordinator` snapshots (`TWQUOTA1` file image `<= 16 MiB`), `ExecLimiter`, `rate_limiter::RateLimiter`, and `shared::SharedRateLimiter` enforce fixed structural capacities:

| Resource | Hard Limit | Enforcement Rule |
| --- | --- | --- |
| Tracked windows (`Coordinator`) | `256` (`MAX_WINDOWS`) | Immutable once committed; non-overlapping per `BucketId`. |
| Registered consumers | `256` (`MAX_CONSUMERS`) | Unique `ConsumerId { instance, generation }` with strictly increasing `generation`. |
| Issued / local grants | `4,096` (`MAX_GRANTS`) | Non-`Clone`, no public deserialization; `expires_ns <= owner.lease_until_ns`. |
| Costs per grant/attempt | `16` (`MAX_COSTS`) | Non-empty, nonzero `units`, strictly ascending `WindowKey` order. |
| Local attempts | `4,096` (`MAX_ATTEMPTS`) | Keyed by `AttemptId { producer, sequence }` with 32-byte request `digest`. |
| Hot-path `Spend`s per `allow` | `8` (`MAX_SPENDS_PER_CALL`) | Stack-allocated transactional undo log; nonzero `units` per entry (zero-unit entries skipped in `SharedSpend`). |
| Rolling timestamp ring (`RateLimiter`) | `64..=4,096` (`default_rolling_ring_capacity`) | Preallocated `Box<[RollingEntry]>` at setup; same-nanosecond bursts coalesce into tail. |
| Lock-free shared table (`SharedRateLimiter`) | Power-of-two `64..=65,536` slots (default `4,096`, `<= 50%` load factor) | Preallocated open-addressed atomic slot array; `20` atomic sub-buckets (`ROLLING_SUB_BUCKETS`) per sliding window. |
| Venue egress IPs / symbols | `16` (`MAX_VENUE_IPS`) / `256` (`MAX_VENUE_SYMBOLS`) | Distinct non-`NONE` `IpKey`s and preallocated symbol rolling-ring pool. |

Opening `Coordinator::open(store, generation, now)` requires `generation > state.supervisor_generation`, matching `ClockDomain`, and non-regressing `now >= state.last_ns`. Consumer registration requires a strictly higher consumer `generation`, ensuring a lost registration reply can never spawn a duplicate consumer instance in the same epoch.

## Core Invariants

* **Pessimistic durable accounting across windows**: Every newly issued grant retains its full maximum `cost.units` in `Coordinator` state until reclaimed via an explicit `StoppedReceipt` from the consumed `ExecLimiter`. For every `WindowKey`, durable state preserves:
  ```text
  accounted        = sum of retained quantities across all grants for this window
  accounted       <= window.limit
  normal_accounted <= window.limit - window.recovery_reserve
  ```
  Request timeouts, network errors, zero-byte `NoBytes` outcomes, consumer freezes, uninstalled grants, or missing stop receipts never refund debited or unreturned units.
* **Compare-and-swap commit before grant delivery**: Grant issuance, window publication, and lease renewal validate candidate invariants first (`PreparedGrant`, `PreparedWindows`, `PreparedRenewal`) and atomically `compare_exchange(expected_revision, next)` the backing `QuotaStore` before returning a capability. Any store error poisons that `Coordinator` instance against further writes (`Error::Poisoned`); if a crash or error occurs *after* the store commits, reopening under a higher supervisor generation accounts for the **full** grant allocation, and reusing `request_id` cannot mint a duplicate grant.
* **All-or-nothing multi-bucket debit and original-grant retry**: Before charging an `AttemptId`, `ExecLimiter::consume` verifies `ClockDomain`, monotonic non-regression, `!frozen`, `now >= cooldown_until_ns`, active consumer lease (`now < lease_until_ns`), and non-expired grants/windows, computing the complete multi-window debit plan before mutating any balance—if any bucket is short, zero buckets are debited. Retrying an `AttemptId` (`Consumed::Duplicate`) must match the original `digest`, `Class`, and `costs` slice byte-for-byte and re-verify the *exact original grants* (a newly installed grant cannot mask an expired original grant); any conflicting reuse permanently freezes the consumer (`Error::Conflict`).
* **Lease-only renewal and terminal stop**: Applying a committed `LeaseRenewal` advances only `owner.lease_until_ns` while the consumer's current lease is still active—it never extends `Grant.expires_ns` or revives an already-expired consumer. Calling `ExecLimiter::stop(self)` consumes the owner by value, preventing any further charges and producing the `StoppedReceipt` needed to reduce coordinator retention to actual usage.

## Process-Wide Cross-Account Lock-Free `SharedRateLimiter` and Single-Writer `RateLimiter`

`tickwire_quota::rate_limiter` and `tickwire_quota::shared` key every counter by `RateLimitId { local_ip: IpKey, interval_s: u32, account: u64, flags: RateFlags, symbol: u64 }`:

* **Lossless zero-allocation `IpKey` and cross-account IP sharing**: `IpKey` distinguishes `IpKey::NONE` (account-wide or symbol-scoped windows) from all $2^{32}$ IPv4 addresses (including `0.0.0.0`) and all $2^{128}$ IPv6 addresses (including `::`) without heap allocation. A source-IP rule `RateRule::ip(local_ip, interval_s, REQUEST | WEIGHT, ip_weight_min)` (`ips = [{ ip, ip_weight_min }]`) sets `account = 0`, so every `AccountSession` (`account_ref = 1..255`), `REQ` order send, and `AsyncRestQuerier` query bound to that `local_ip` contends on and shares the exact same atomic IP weight counter.
* **Lock-free atomic fixed and `kRolling` sub-bucket windows with delta rollback**:
  * In `SharedRateLimiter`, fixed windows pack `(epoch: u32, used: u32)` into an `AtomicU64` updated via `compare_exchange_weak`; rolling windows (`RateFlags::ROLLING`) partition `interval_s` into `20` atomic sub-buckets (`[AtomicU64; 20]`, each packing `36`-bit `sub_tick` and `28`-bit `count`) paired with a conserved `in_window_sum: AtomicU64` where only the single thread winning an expired sub-bucket's eviction CAS subtracts `old_count` from `in_window_sum`.
  * If step `k` in a multi-bucket `SharedRateLimiter::allow` chain rejects, steps `0..k` roll back in reverse order via atomic delta subtraction (`used - units` gated on matching `epoch` or `booked_tick`), preserving concurrent increments applied by sibling threads without snapshot clobbering.
  * Unseen per-symbol or per-family rules (`RateRule::symbol`, `RateRule::family`) resolve on first send into the preallocated atomic open-addressed slot table without heap allocation; the first insert retains its `limit`, and subsequent `add` calls for the same `RateLimitId` return the existing slot handle.
* **Single-pick route admission (`pick -> spend(conn.local_ip) -> NACK immediately`)**: `SharedRateLimiter::pick_and_spend_or_nack` accepts a `FnOnce` route picker, invokes it at most once, and spends the chosen connection's `local_ip` alongside the account/product/symbol windows. Any limit exhaustion or IP/account cooldown returns `Err(RouteAdmissionReject::RateLimitedNack(_))` immediately without retrying another connection.
* **Compiled venue default tables (`VenuePreset` / `OrsVenueProfile`)**: Built-in defaults cover:
  * **Binance Spot** (`100/10s` + `200,000/86,400s` account `ORDER`; `6,000/60s` shared per-IP `REQUEST | WEIGHT`),
  * **Binance USD-M** (`300/10s` + `1,200/60s` account `ORDER`; `2,400/60s` shared per-IP `REQUEST | WEIGHT`),
  * **Binance COIN-M** (`1,200/60s` account `ORDER`; `2,400/60s` shared per-IP `REQUEST | WEIGHT`; new orders spend only `ORDER` on WS/coin-margined rules while cancels/queries spend IP weight),
  * **Binance Options** (`100/10s` + `1,200/60s` account `ORDER`; `2,400/60s` shared per-IP `REQUEST | WEIGHT`),
  * **OKX UTA** (`1,000/2s` account `ORDER`; `60/2s` per-`(account, symbol)` `ORDER` & `CANCEL` or `60/2s` per-`(account, family)` Options `ORDER` & `CANCEL`; `20/2s` account `REQUEST`; `600/2s` shared per-IP `REQUEST | WEIGHT`),
  * **Bybit UTA** (`20/1s` rolling per-`(account, product)` `ORDER` and `CANCEL`; `50/1s` rolling account `REQUEST`; `600/5s` rolling shared per-IP `REQUEST | WEIGHT`).
  Across every venue profile, pure cancellations and status/REST queries never touch `ORDER` windows.
* **Rising-edge `reconcile_venue_usage` (`ExchRateLimit`)**: `SharedRateLimiter::reconcile_venue_usage` evaluates parsed `rateLimits[]` `(count, limit)` rows in exact integer arithmetic (`u128::from(count) * 100 >= u128::from(limit) * u128::from(exch_warn_pct)` with default `80%`, and `count >= limit` for the `100%` ceiling) without mutating local spend counters. Each threshold is tracked by an atomic latch bit that emits `ExchRateLimitEvent` (`WarnThreshold` or `CeilingReached`) strictly on the upward crossing and re-arms only after venue-reported usage drops strictly below that threshold.

## Exclusive File Store Semantics

`FileQuotaStore::open` enforces a `0700` canonical directory, opens `LOCK` (`0600`, `O_NOFOLLOW | O_CLOEXEC`), and acquires an exclusive nonblocking file lock wrapped in a private `WriterLock` guard *before* reading, SHA-256-verifying, and `fsync`ing `STATE` (`TWQUOTA1` magic + 32-byte SHA-256 + JSON image). Updates write and `fsync` `STATE.pending-<pid>-<rev>` (`create_new`, `0600`) before atomic rename and directory `fsync`, poisoning the handle on any I/O failure.

On `Drop` (including early construction failure), `WriterLock` explicitly calls `unlock()` before closing the file descriptor: an inherited descriptor held briefly by a forked child before `exec` cannot prolong a retired owner's lock, a live owner continues to exclude concurrent openers, and closing a stale descriptor later cannot release an independent lock acquired by a successor.

## Verification and References

```sh
cargo test --locked -p tickwire-quota --all-targets
```

Test suites exercise `MockQuotaStore` `BeforeCommit` and `AfterCommit` fault injection, recovery-reserve isolation, multi-bucket atomic rollback, original-grant expiry on duplicate retries, conflicting-digest freeze, `FileQuotaStore` lock/inheritance invariants, single-writer `VenueRateLimiter` presets, and multithreaded cross-account `SharedRateLimiter` IP contention, `kRolling` conservation, single-pick NACK enforcement, and rising-edge `ExchRateLimit` re-arming.

* [Quota coordinator, `ExecLimiter`, and `Grant` implementation](../../crates/tickwire-quota/src/lib.rs)
* [Single-writer `RateLimiter` and `VenueRateLimiter`](../../crates/tickwire-quota/src/rate_limiter.rs)
* [Lock-free cross-account `SharedRateLimiter` and `ExchRateLimit` reconciliation](../../crates/tickwire-quota/src/shared.rs)
* [Exclusive `TWQUOTA1` file-backed quota store](../../crates/tickwire-quota/src/files.rs)
* [Grant issuance, CAS failure, and local consumption tests](../../crates/tickwire-quota/tests/quota.rs)
* [Multi-IP, sliding window, and venue preset tests](../../crates/tickwire-quota/tests/multi_ip_limiter.rs)
* [Cross-account lock-free `SharedRateLimiter` tests](../../crates/tickwire-quota/tests/shared_rate_limiter.rs)
* [Binance endpoint quota owner](../../crates/tickwire-binance-quota/src/lib.rs)
* [Binance request quota specification](binance-quota.md)
