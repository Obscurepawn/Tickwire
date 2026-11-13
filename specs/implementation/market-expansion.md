# Live Binance Market-Data Topic Expansion

A running Binance `tickwire-md` process can append pre-approved catalog instruments to an active shared-memory market-data topic without restarting existing venue sockets or disturbing unexpanded topics. Existing instruments retain their dense local slots, a successor shared-memory bank publishes the enlarged instrument set, and every approved consumer executes a coordinated quiesce-and-rejoin transition.

This protocol governs market-data topic membership and replica handoff exclusively. Account assets, cash balances, order-entry routes, and trading permissions maintain independent lifecycles, and the single-instrument Bybit/OKX spot BBO service does not implement topic expansion.

## State Model and Bounds

Expansion must be explicitly enabled in the `tickwire-md` configuration. A launcher stages a schema-`1` JSON `Plan` inside the configured private control directory, binding the expected base state to the target configuration and warmup budgets:

| Resource or Parameter | Bound | Enforcement Rule |
| --- | --- | --- |
| `Plan` payload | `<= 4 MiB` (`MAX_PLAN_BYTES`) | Nonzero 16-byte `operation_id`; canonical digest ignores JSON whitespace. |
| Approval / Commit control files | `<= 2 MiB` each | Content-addressed per-process decisions persisted before WAL references. |
| Operation history | `128` operation IDs | Retains canonical digests (including rejected plans); reusing an ID with different bytes fails. |
| Warmup & freshness budgets | `<= 60 s` (`MAX_WARMUP_NS`) | `now < warmup_deadline_mono_ns` and `0 < maximum_age_ns <= 60 s`. |
| Feed & admission fanout | `<= 32` feeds in `Plan`, `<= 128` server participants | Target feeds must be a strict superset of current feeds (`<= 3` event streams/feed). |
| Peak memory budgets | `old_cache + new_cache <= peak_cache_bytes` | Shared-memory budget covers `retained + old_bank + new_bank <= maximum_retained_bank_bytes`. |
| Strategy `MarketTransition` | `<= 16 MiB` typed-input log record | Captures old cursor, new baseline, catch-up inputs, and timestamps. |
| Recovery store | `<= 1,024` snapshots, `16 MiB` each, `512 MiB` total | Tracks 63-bit sequence, predecessor hash, config, dense IDs, banks, and `<= 16,384` owner IDs. |

The target configuration must leave every existing instrument's economics, catalog hash/version, source bindings, book keys, connection settings, directory identities, and sibling topics bit-for-bit unchanged. Only appended feeds, `target_revision = base_revision + 1`, `instrument_set_version` (`< 2^63`), and non-decreasing topic cache/snapshot capacities may advance. The successor bank template preserves producer identity, channel, catalog, ring geometry, and `host_boot_id`, advances `ring_epoch` and `feed_generation`, and initializes `created_mono_ns = 0`, `cutover_seq = 0`, `first_seq = 1`, and an unset `descriptor_hash` (unless supplied matching its exact wire bytes) to be finalized at cutover.

## Core Invariants

* **Slot and live-socket preservation**: `FeedIndex::expanded` clones a private copy of the current mapping, keeping every pre-existing instrument at its original `DenseId` and appending new instruments in first-seen order. Active upstream depth and event sockets retain their network tokens uninterrupted, while retired candidate tokens and 16-byte `owner_id` values are permanently burned and never silently reused.
* **Reversible pre-revocation, fail-closed post-revocation**: Any validation, warmup, budget, or candidate write failure *before* old-bank revocation aborts cleanly and leaves the existing bank and publisher running. Once old-bank revocation begins (or if bank freeze fails), any error invokes the fault sink and keeps the topic closed rather than serving split state.
* **Evidence-backed quiescence**: The old bank cannot be cut over on timeouts, broken control sockets, incomplete handshakes, or unverified text messages; every approved participant must supply mapped-cursor `QUIESCED` evidence and a detached registration.
* **Zero synthetic orders on transition**: Replaying or applying a `MarketTransition` reconstructs and verifies the consumer's enlarged market replica and advances its durable `HEAD`, generating zero strategy order callbacks.

## Prepare and Warm

Before touching live publication, a cold worker can prepare DNS resolution, TLS root stores, and client structures off the owner thread, while the single `tickwire-md` owner retains quota consumption, nonblocking socket polling, and ring publication. New Binance depth and event owners warm real HTTPS snapshots and WebSocket streams inside a private candidate publisher (`prepare_expansion`) with prebound topic slots. Warmup succeeds only when every required depth book and configured event category on the newly added instruments is continuous, usable, and within `maximum_age_ns`; any sequence gap, stale observation, disconnect, or book rebuild clears readiness immediately.

## Quiesce and cut over

1. **Arm participants**: Bind each participant's current client admission, successor template, plan approval digest, target revision, monotonic clock, and deadline. In the execution control inbox, validate per-process `Arm`/`Commit` files from a private directory and sync the content-addressed object before appending its control-WAL reference. Unknown or unfinished participant handshakes block confirmation.
2. **Stop new JOINs and quiesce**: Once **all** current participants return `Armed` receipts, stop admitting new JOINs to the target topic and send `QUIESCE` packets carrying operation- and subscription-bound request IDs (preserving exact packet bytes across nonblocking send retries). Wait until every peer proves mapped-cursor `QUIESCED` state and detaches its active registration.
3. **Finalize and freeze at `cutover_seq`**: Copy current live state for pre-existing rows into the candidate, re-verify readiness, freshness, snapshot size, and peak cache/bank budgets, and sample `cutover_seq = old.published`. Finalize the successor `ResourceHeader` with `created_mono_ns = now`, `cutover_seq`, `first_seq = cutover_seq + 1`, `predecessor_ring_epoch = old.ring_epoch`, and its SHA-256 `descriptor_hash`. Write the successor bank, republish all member states, and freeze the predecessor bank while retaining its mappings for retirement accounting.
4. **Rejoin and persist consumer state**: Every consumer performs a [JOIN](join.md) against the finalized successor descriptor, maps the enlarged state, and drains catch-up events. The account executor suspends market observations until its updated effective state and receipts are durable. Each registered strategy writes a `MarketTransition` record, verifies its reconstructed replica against the new client, and syncs its input log and `HEAD` before re-enabling `on_market` callbacks.
5. **Confirm activation**: Mark the expansion complete only after `tickwire-md`'s effective revision/digest and all live `Activated` receipts agree. For every control packet, the strategy host samples its monotonic clock once after the socket read to check both session deadline and receipt timestamps, rejecting receipts from the future or arriving at/after expiry. Historical receipts never qualify a later failed candidate or substitute for a live JOIN.

## Durable Recovery Across Process Restart

When optional `ExpansionControl.recovery` is configured, `tickwire-md` maintains an exclusive `0700` recovery directory containing `0600` single-link regular files (rejecting symlinks, unexpected files, orphaned phases, or hash/chain mismatches). Every checkpoint writes and `fsync`s an immutable snapshot object before atomically replacing and syncing `HEAD`; any I/O failure after a write begins poisons the store handle for the remainder of the process. When catalogs are enabled, up to 32 raw catalog objects (`<= 2 MiB` total) are persisted and re-verified so economics can never be downgraded or substituted across restarts.

Restarting with `restart_from = <HEAD digest>` succeeds only from a committed `Ready` checkpoint (incomplete irreversible transitions remain closed). Before allocating runtime resources, the new process persists `Starting` and requires a fresh producer instance, fresh channels/resources, and unused source `owner_id` values while preserving logical configuration, dense ID assignments, consumed operation digests, quota directory state, and grant history. Upstream feeds warm from scratch starting at independent sequence `1`, publish a new active resource header, and require fresh consumer JOIN admissions rather than resuming a retired producer's ring.

## Verification and References

```sh
cargo test --locked -p tickwire-market-publisher -p tickwire-md --lib
```

An archived Linux multi-process run (`spot-market-expansion`) started `BTCUSDT` against a local TLS Binance fixture, appended pre-approved `ETHUSDT` (`instrument_id = u64::MAX`), and verified continuous execution-subscriber and dual-strategy-worker handoff across the cutover while cold-start tests enforce fresh producer identities, live warmup, and fresh admissions.

* [Cold plan validation and bank finalization](../../crates/tickwire-md/src/expansion.rs)
* [Market-data runtime expansion coordinator](../../crates/tickwire-md/src/engine/expansion)
* [Topic publisher candidate warmup and cutover](../../crates/tickwire-market-publisher/src/expansion.rs)
* [Consumer market transition state machine](../../crates/tickwire-market-source/src/transition.rs)
* [JOIN protocol specification](join.md)
* [Restart verification tests](../../crates/tickwire-md/src/server/runtime_tests/restart.rs)
* [Restart failure and poison tests](../../crates/tickwire-md/src/server/runtime_tests/restart_failure.rs)
* [Archived Linux multi-process expansion evidence (`20261028`)](../evidence/20261028/market-expansion-runtime.json)
