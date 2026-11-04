# Shared request quotas

A `BucketId` identifies the authority namespace, type and ID of the shared principal, and pool. All consumers charged to the same `WindowKey` share a single limit. A committed window is immutable. A subsequent window for the bucket needs a larger ID and cannot start before the previous window ends. Windows use conservative send intervals in a trusted local monotonic clock domain. [Quota coordinator](../../crates/tickwire-quota/src/lib.rs)

## Durable grants

The coordinator persists supervisor epoch, clock domain, committed time lower bound, windows, newest consumer epochs and issued grants. Reopening requires a higher supervisor epoch, the same domain and a non-regressing time. Consumer registration requires a higher execution epoch. A lost registration acknowledgement does not allow a second consumer instance in that epoch.

Grant issuance validates all affected windows and balances, then atomically compare-and-swaps the candidate store state. Only a confirmed successful commit can return a grant. A failed compare-and-swap stops that coordinator from writing again. If commit succeeds but the acknowledgement is lost, recovery still accounts for the *full* allocation. Reusing a request ID cannot mint another grant. For each window the following relations hold.

```text
accounted = sum of retained quantities in all grants for this window
accounted <= window limit
normal_accounted <= window limit - recovery reserve
```

A new grant retains its maximum amount. Only a final usage record from the stopped consumer may reduce that quantity to actual use. Timeouts, failed requests, a local no-bytes outcome and freezing do not refund previously used units. A missing stop receipt or an uninstalled grant leaves its full allocation outstanding. [Grant and consumption tests](../../crates/tickwire-quota/tests/quota.rs)

## Local consumption

A single consumer owns each non-copyable grant. Before charging an attempt it verifies clock domain, time ordering, lease, cooldown, window and grant expiration. It computes the complete multi-bucket debit plan first. If any bucket is short, no bucket is charged. Exact attempt retries must match the original digest, charge class and entire cost table, and revalidate the *original* grants. A newer grant cannot bypass the earlier grant's expiration. Conflicting reuse permanently freezes that consumer. The bounded owner tracks up to 256 windows, 256 consumers, 4,096 grants, 16 costs per attempt and 4,096 attempts. [Quota limits and transitions](../../crates/tickwire-quota/src/lib.rs)

A durable lease renewal affects only the consumer lease. It cannot extend an existing grant or revive a consumer whose old lease has already expired. `stop(self)` consumes the owner, reports usage of installed grants and leaves no subsequent charge entry point. Endpoint adapters supply the actual costs, recovery classification, venue limit configuration and 429/418 feedback. The quota core cannot infer venue rate limits or translate a clock between hosts. [Binance endpoint quota owner](../../crates/tickwire-binance-quota/src/lib.rs)

`FileQuotaStore` takes an exclusive file lock before reading and validating state. A private guard releases it on subsequent construction failure or owner destruction. Explicit unlock before close prevents a temporarily inherited descriptor from extending the retired owner's lock across fork/exec. A still-live owner continues to exclude another opener. A stale descriptor closing later cannot release a successor's independent lock. [File-backed store](../../crates/tickwire-quota/src/files.rs)
