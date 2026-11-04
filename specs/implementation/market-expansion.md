# Add instruments to a live Binance topic

A running Binance MD process can append instruments from its approved catalog to an existing topic. Existing slots keep their meaning, a successor bank carries the enlarged member set, and every approved reader completes a fresh JOIN. The operation changes market-data membership. Account assets, balances, order routes and trading permissions have separate lifecycles. The single-instrument Bybit/OKX BBO service does not implement this expansion protocol. [Runtime](../../crates/tickwire-md/src/engine/expansion) · [Publisher](../../crates/tickwire-market-publisher/src/expansion.rs)

## Prepare and warm

Enable expansion explicitly in the MD configuration. The launcher places a plan in the configured private control path. `Plan` binds a nonzero operation ID, expected revision and configuration digest, exact old resource, beneficiary topic, target revision/configuration, new resource template, approved old/new subscribers and the warmup/freshness/cache budgets. Limit a plan to 4 MiB and warmup to 60 seconds. Retain at most 128 operation IDs and their canonical digests, including rejected plans. Reformatting JSON does not change the canonical digest. Reusing an ID for different content fails.

The target is a strict superset. Preserve all old economics, feeds, connection settings, source bindings, keys, directory identities and other topics. Increase the instrument-set version, ring epoch and feed generation. Preserve producer, channel, catalog, geometry and host boot. The new template uses zero creation/cutover time, `first_seq=1` and an unset descriptor hash. Finalize those values at the actual cut. A nonzero supplied descriptor hash must match its bytes.

`FeedIndex::expanded` takes a private copy, leaves old IDs at their original positions and appends new IDs in first-seen order. New Binance owners warm actual REST and WebSocket inputs into a private candidate. Warmup completes when every required event category and depth observation is usable and recent. A gap, stale event, disconnect or rebuild clears the corresponding readiness. A cold worker can prepare DNS, certificates and client construction. The MD owner retains quota, sockets, event polling and publication. Live sources keep their existing network tokens. Retired candidate tokens and owner identities cannot be silently reused.

Budget for candidate and old caches at peak, including exact retained capacity. Check the final complete snapshot size, and account for active, candidate and retired banks under the shared-memory budget. Prebind new topic slots for all affected feeds before routing the successor.

## Quiesce and cut over

1. Bind each participant’s approved current client, successor template, plan digest, revision, clock and deadline. Approval and Commit objects each fit 2 MiB. The MD control server can bind up to 128 participants to one operation. Unknown or unfinished handshakes prevent it from confirming the set.
2. Wait for **all** current Armed receipts, stop new JOINs to the target, and send QUIESCE using operation- and subscription-bound request IDs. A pending nonblocking send retains its original packet. Require actual mapped-cursor QUIESCED evidence and a detached registration for each peer. A timeout, broken socket, failed registration or text receipt cannot release the old bank.
3. Refresh old rows from current state and recheck readiness, freshness and budgets. Take `cutover_seq = old.published` at commit. Set `new.first_seq = cutover_seq + 1` and `predecessor_ring_epoch = old.ring_epoch`. Write the successor state, freeze the predecessor and retain its mappings. Before revocation a failed candidate leaves the old path running. After revocation, a failure keeps the path closed.
4. Each actual consumer joins the same finalized descriptor, maps the full old/new state and catches up. Execution pauses observations until its effective state and receipts are durable. A registered strategy records a `MarketTransition` with the old cursor, new authenticated baseline, catch-up inputs and timestamps (16 MiB typed-input bound). It checks the reconstructed replica against its new client and persists its input and HEAD before allowing further strategy callbacks. Applying this transition generates no order.
5. Record success only after the MD effective revision/digest and all real Activated receipts agree. An earlier successful receipt is historical evidence. It cannot qualify a currently failed successor or replace a live JOIN.

The execution control inbox accepts approved per-process Arm/Commit files in a private directory. It checks the current binding and persists the content-addressed decision before its reference in the control WAL. Consumers publish receipts for Armed, Quiesced and Activated. File reads, durable writes and verification are bounded cold work and can still occupy the owner thread. [Consumer transition](../../crates/tickwire-market-source/src/transition.rs) · [JOIN protocol](join.md)

## Recover a completed expansion

`ExpansionControl.recovery` is optional. Supply a private recovery directory on first start and the exact current HEAD digest as `restart_from` on restart. The writer exclusively holds a real 0700 directory with 0600, single-link ordinary files. Validate the entire object chain, reference lengths and hashes. Reject symlinks, unexpected objects, orphaned state and uncommitted phases. Write and sync the immutable object before atomically replacing and syncing HEAD. An I/O error after a write starts poisons that store.

Keep at most 1024 snapshots, 16 MiB per snapshot and 512 MiB total. Each snapshot retains a 63-bit increasing sequence, predecessor digest, full logical configuration and revision, dense ID order, exact bank descriptors, consumed operations and up to 16384 reserved owner IDs. Old slots and reservations survive candidate failure. When catalogs are enabled, keep the original approved objects, up to 32 catalogs and 2 MiB in total. Their presence and economics cannot be downgraded or substituted on recovery.

Only a stable `Ready` checkpoint can start a new incarnation. Persist `Starting` before allocation. Require a fresh approved producer instance, channels/resources and source owners. Preserve the logical configuration, used IDs, quota directory and grant history. A new independent stream starts at sequence one. Rewarm the real feeds, write the actual active resource and require fresh reader admissions. Logical recovery does not reconnect old consumers or continue an old producer's strategy stream. Incomplete irreversible operations remain closed.

## Verify

```sh
cargo test --locked -p tickwire-market-publisher -p tickwire-md --lib
```

The `spot-market-expansion` lab case starts BTCUSDT on a local Binance TLS exchange, adds a preapproved ETHUSDT instrument with ID `u64::MAX`, and verifies the execution subscriber and two independent strategy workers. MD, execution and worker identities stay stable across the in-process cut. The replay cut must extend the actual retained input chain and cover every consumer's activation watermark. New cold-start tests require a different MD identity, real source warmup and new admissions. [Recorded Linux process run](../evidence/20261028/market-expansion-runtime.json) · [Restart tests](../../crates/tickwire-md/src/server/runtime_tests/restart.rs) · [Failure cases](../../crates/tickwire-md/src/server/runtime_tests/restart_failure.rs)
