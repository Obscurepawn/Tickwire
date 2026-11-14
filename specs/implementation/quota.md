# Durable Shared Request Quotas

`tickwire-quota` partitions shared venue rate-limit budgets across independent local processes without permitting split-brain over-issuance across crashes or lost acknowledgements, and provides a zero-allocation, multi-source-IP, class-isolated hot-path `RateLimiter` for single-writer order routing. A `BucketId` identifies `(namespace, ScopeKind [Ip | Uid | Account | Connection], subject, pool)`, and all consumers drawing from the same `WindowKey { bucket, window }` share a single immutable `WindowConfig` (`starts_ns < ends_ns`, `limit > 0`, `recovery_reserve <= limit`). Every subsequent window for a given `BucketId` requires a strictly larger `window` ID and non-overlapping interval (`new.starts_ns >= previous.ends_ns`), evaluated within a single trusted local `ClockDomain`.

Built in safe Rust (`#![forbid(unsafe_code)]`) with exact integer arithmetic (`u64`/`u128`, zero floating point), the crate separates durable supervisor allocation from hot-path spending: `Coordinator<S>` owns atomic store persistence of supervisor epochs, windows, consumer registrations, and `Grant` allocations, `ExecLimiter` owns non-`Clone` `Grant` capabilities and local attempt deduplication, and `rate_limiter` (`RateLimiter`, `VenueRateLimiter`, `VenuePreset`) enforces orthogonal `ORDER`, `CANCEL`, and `REQUEST | WEIGHT` counters across multiple bound local source IPs (`IpKey`). Endpoint-specific cost tables, `Normal` vs `Recovery` classification, and HTTP `429`/`418` feedback belong to venue adapters ([binance-quota.md](binance-quota.md)).

## State Model and Bounds

Both `Coordinator` snapshots (`TWQUOTA1` file image `<= 16 MiB`), `ExecLimiter`, and `rate_limiter::RateLimiter` enforce fixed structural capacities:

| Resource | Hard Limit | Enforcement Rule |
| --- | --- | --- |
| Tracked windows (`Coordinator`) | `256` (`MAX_WINDOWS`) | Immutable once committed; non-overlapping per `BucketId`. |
| Registered consumers | `256` (`MAX_CONSUMERS`) | Unique `ConsumerId { instance, generation }` with strictly increasing `generation`. |
| Issued / local grants | `4,096` (`MAX_GRANTS`) | Non-`Clone`, no public deserialization; `expires_ns <= owner.lease_until_ns`. |
| Costs per grant/attempt | `16` (`MAX_COSTS`) | Non-empty, nonzero `units`, strictly ascending `WindowKey` order. |
| Local attempts | `4,096` (`MAX_ATTEMPTS`) | Keyed by `AttemptId { producer, sequence }` with 32-byte request `digest`. |
| Hot-path `Spend`s per `allow` | `8` (`MAX_SPENDS_PER_CALL`) | Stack-allocated transactional undo log; nonzero `units` per entry. |
| Rolling timestamp ring | `64..=4,096` (`default_rolling_ring_capacity`) | Preallocated `Box<[RollingEntry]>` at setup; same-nanosecond bursts coalesce into tail. |
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

## Multi-Source-IP and Class-Isolated `RateLimiter`

`tickwire_quota::rate_limiter` keys every counter by `RateLimitId { local_ip: IpKey, interval_s: u32, account: u64, flags: RateFlags, symbol: u64 }`:

* **Lossless zero-allocation `IpKey`**: `IpKey` distinguishes `IpKey::NONE` (account-wide or symbol-scoped windows) from all $2^{32}$ IPv4 addresses (including `0.0.0.0`) and all $2^{128}$ IPv6 addresses (including `::`) without heap allocation.
* **Fixed and rolling windows with exact `put_back` rollback**: Fixed windows align to `epoch_idx = now_ns / (interval_s * 1_000_000_000)` and record `WindowUndo::Fixed { prev_used }`. Rolling windows (`RateFlags::ROLLING`) preallocate a fixed `Box<[RollingEntry]>` once at registration, evict entries with `now_ns - ts_ns >= interval_ns`, coalesce same-nanosecond bursts into the tail slot (`WindowUndo::RollingCoalesced` vs `WindowUndo::RollingPushed`), and compute exact `retry_after_ns` from prefix expirations. `RateLimiter::allow` validates monotonic time and account/IP cooldowns upfront and reverses `0..k` via `put_back` in reverse order if step `k` rejects; `can_afford` evaluates headroom without mutation.
* **Per-IP cooldown isolation and exchange usage reconciliation**: `note_ip_cooldown(local_ip, until_ns)` isolates HTTP `429` throttling strictly to `local_ip`, allowing healthy sibling source IPs to continue serving requests, whereas `note_account_cooldown` and `freeze_account` gate all IPs for `account`. `reconcile_exchange_usage` raises fixed-window `used = used.max(reported_used.min(limit))`, tracks rolling peaks without mutating ring timestamps, and sets `warn` via exact integer arithmetic (`reported_used * 100 >= limit * exch_warn_pct`, default `80%`).
* **Compiled `VenuePreset` isolation**: `VenueRateLimiter` compiles `BinanceSpot` (`100/10s` + `200,000/86,400s` `ORDER`, `6,000/60s` per-IP `WEIGHT`), `BinanceUsdm` (`300/10s` + `1,200/60s` `ORDER`, `2,400/60s` per-IP `WEIGHT` with `weight == 0` WS support), `BybitSpot` (`20/1s` `ORDER`, `20/1s` `CANCEL`, `50/1s` `REQUEST`, `600/5s` per-IP `REQUEST` rolling), and `OkxSpot` (`1,000/2s` account `ORDER`, `60/2s` per-symbol `ORDER` and `CANCEL`, `20/2s` account `REQUEST`, `600/2s` per-IP `REQUEST` rolling). Across every preset, `allow_cancel` and `allow_query` never debit `ORDER` windows.

## Exclusive File Store Semantics

`FileQuotaStore::open` enforces a `0700` canonical directory, opens `LOCK` (`0600`, `O_NOFOLLOW | O_CLOEXEC`), and acquires an exclusive nonblocking file lock wrapped in a private `WriterLock` guard *before* reading, SHA-256-verifying, and `fsync`ing `STATE` (`TWQUOTA1` magic + 32-byte SHA-256 + JSON image). Updates write and `fsync` `STATE.pending-<pid>-<rev>` (`create_new`, `0600`) before atomic rename and directory `fsync`, poisoning the handle on any I/O failure.

On `Drop` (including early construction failure), `WriterLock` explicitly calls `unlock()` before closing the file descriptor: an inherited descriptor held briefly by a forked child before `exec` cannot prolong a retired owner's lock, a live owner continues to exclude concurrent openers, and closing a stale descriptor later cannot release an independent lock acquired by a successor.

## Verification and References

```sh
cargo test --locked -p tickwire-quota --all-targets
```

Test suites exercise `MockQuotaStore` `BeforeCommit` and `AfterCommit` fault injection, recovery-reserve isolation, multi-bucket atomic rollback, original-grant expiry on duplicate retries, conflicting-digest freeze, `FileQuotaStore` lock/inheritance invariants, and multi-IP fixed/rolling window `VenuePreset` isolation.

* [Quota coordinator, `ExecLimiter`, and `Grant` implementation](../../crates/tickwire-quota/src/lib.rs)
* [Multi-source-IP and class-isolated `RateLimiter` and `VenueRateLimiter`](../../crates/tickwire-quota/src/rate_limiter.rs)
* [Exclusive `TWQUOTA1` file-backed quota store](../../crates/tickwire-quota/src/files.rs)
* [Grant issuance, CAS failure, and local consumption tests](../../crates/tickwire-quota/tests/quota.rs)
* [Multi-IP, sliding window, and venue preset tests](../../crates/tickwire-quota/tests/multi_ip_limiter.rs)
* [Binance endpoint quota owner](../../crates/tickwire-binance-quota/src/lib.rs)
* [Binance request quota specification](binance-quota.md)
