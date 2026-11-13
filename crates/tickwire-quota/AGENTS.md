# tickwire-quota

`tickwire-quota` (`#![forbid(unsafe_code)]`) owns supervisor-side rate-limit budget coordination
(`Coordinator<S>`, `QuotaStore`, `FileQuotaStore`) and single-owner local request consumption
(`ExecLimiter`) bound to one `ClockDomain`. A supervisor issues `Grant` and `LeaseRenewal`
capabilities across `Normal` and `Recovery` classes, and an execution owner debits costs per
`AttemptId` before socket transmission; venue-specific endpoint weights and retry headers belong in
exchange quota adapters such as `tickwire-binance-quota`.

## Invariants and Change Guidelines

* **Two-phase commit and CAS poisoning**: Keep `prepare_windows`, `prepare_issue`, and
  `prepare_renew_consumer` side-effect-free in [src/lib.rs](src/lib.rs); only `commit_*` persists
  incremented revisions via `QuotaStore::compare_exchange` and mints non-cloneable `Grant` or
  `LeaseRenewal` values. Any store failure poisons `Coordinator` (`Error::Store` /
  `Error::Poisoned`) so callers reopen durable state instead of retrying an ambiguous commit
  blindly.
* **Reserve protection and reclamation**: Enforce `accounted <= limit` and `Normal` allocations
  `<= limit - recovery_reserve` across non-overlapping `WindowConfig` intervals per `BucketId`.
  Unspent grant capacity returns to `Coordinator::reclaim_stopped` only through a `StoppedReceipt`
  produced by consuming `ExecLimiter::stop(self)`.
* **Idempotent local consumption**: `ExecLimiter::consume` in [src/lib.rs](src/lib.rs) deduplicates
  retries by `AttemptId`, freezing the limiter (`Error::Conflict`) if a repeated `AttemptId` changes
  `digest`, `class`, or `costs`, and checking consumer lease, cooldown, grant expiry, and window
  `[starts_ns, ends_ns)` bounds before debiting.
* **Exclusive durable file store**: Maintain `0o700` directory permissions, `O_NOFOLLOW | O_CLOEXEC`
  opens, explicit `WriterLock::drop` unlock (preventing inherited pre-exec child descriptors from
  retaining ownership), `TWQUOTA1` + SHA-256 framing, and `STATE.pending-*` `sync_all` + rename +
  directory `sync_all` in [src/files.rs](src/files.rs).

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/files.rs](src/files.rs) ·
  [tests/quota.rs](tests/quota.rs)
* **Specifications and models**: [quota.md](../../specs/implementation/quota.md) ·
  [binance-quota.md](../../specs/implementation/binance-quota.md) ·
  [request_budget](../../tla/execution_models/request_budget/ROADMAP.md)

```sh
cargo test --locked -p tickwire-quota --all-targets
./scripts/check-tla.sh request_budget
```
