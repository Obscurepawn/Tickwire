# L0 modeling rationale

The model transcribes the `DepthSync` state machine alongside all supported failure inputs: HTTP failures and timeouts, duplicate or stale callbacks, delta buffer overflow, malformed payloads, sequence gaps, and monotonic clock regressions. Network calls are never assumed to succeed, and `DepthSync` holds purely in-memory ephemeral state with zero persistent queues.

Each transition corresponds to one completed public `&mut self` API call. Inside `on_snapshot`, buffered deltas are replayed atomically via a recursive pure helper; if an error occurs after installing the snapshot or applying an earlier buffered delta, `reset` executes within the same call before returning, matching Rust's exclusive `&mut self` borrow discipline so callers can never observe a transient mid-replay `Live` view.

`BookKey`, symbol, catalog, and product are fixed per configuration, so tokens retain the varying `(connection, generation, request_id)` triple. `issued` records every `SnapshotToken` handed out to the network owner so late or duplicate callbacks can replay arbitrary historical tokens. Connection epochs increment sequentially without loss of generality because stale/equal epoch checks depend only on order. `clock` and event `stamp` share one bounded monotonic domain, while `view(now)` supplies an independent observation timestamp without mutating coordinator time.

Buffered events retain abstract `(U, u, pu, stamp, qty, ok)` fields. `qty` witnesses the latest sequence-accepted quantity assignment at a covered price level (`0` = delete) so any stale-token bug would visibly corrupt book content; tick/lot sorting, crossed-book checks, and level capacity collapse into `ok`, preserving sequence-before-payload check order.

Bounded domains for `U`/`u`/`pu`, buffer capacity (`1` primary, `2` scaled so prefix-success + suffix-failure replay occurs), `MaxGenerations`, and `MaxRequests` exercise every `checked_add` saturation and stale/bridging/gap branch. Time `0..2` separates arrival at `0`, snapshot completion at `1`, and deadline `2`, allowing `SnapshotDelayed` and `SnapshotRetainsArrivalStamp` to witness non-equal arrival vs. completion timestamps.

When mid-replay failure triggers `reset`, normal generation advance clears the `Book`, whereas generation counter exhaustion invalidates the `Book` without clearing diagnostic `last_update_id()`; the model preserves both branches while keeping `view` closed in both cases.

| API | Model actions | Source |
| --- | --- | --- |
| `connect` | `Connect`, `ConnectRejected` | `crates/tickwire-binance/src/depth_sync.rs:202-218` |
| `subscription_ready` | `Subscribe`, `OldConnection` | `crates/tickwire-binance/src/depth_sync.rs:220-226` |
| `request_snapshot` | `Request`, `RequestBadState`, `RequestBadDeadline`, `RequestPending`, `ClockRegression` | `crates/tickwire-binance/src/depth_sync.rs:228-263` |
| `on_delta` | `OldConnection`, `DeltaInvalid`, `DeltaBuffer`, `DeltaApply`, `ClockRegression` | `crates/tickwire-binance/src/depth_sync.rs:265-317` |
| `on_snapshot` | `OldSnapshot`, `SnapshotExpired`, `SnapshotHttpFailure`, `SnapshotInvalid`, `SnapshotValid`, `SnapshotDelayed`, `ClockRegression` | `crates/tickwire-binance/src/depth_sync.rs:318-371` |
| `snapshot_failed` | `OldSnapshot`, `SnapshotTransportFailure`, `ClockRegression` | `crates/tickwire-binance/src/depth_sync.rs:372-389` |
| `expire_snapshot` | `Expire`, `ClockRegression` | `crates/tickwire-binance/src/depth_sync.rs:390-402` |
| `disconnect` | `Disconnect`, `OldConnection` | `crates/tickwire-binance/src/depth_sync.rs:403-414` |
| `view` | `View` | `crates/tickwire-binance/src/depth_sync.rs:428-433` |

Expired and non-expired `snapshot_failed` return codes collapse into `SnapshotTransportFailure` because both clear `pending` without mutating the buffer or book. Stale connection callbacks share `OldConnection`, and stale snapshot callbacks share `OldSnapshot`, matching early token guards prior to clock or payload checks.

In `DeltaBuffer`, the full-buffer overflow branch computes `Reset` directly and quantifies `e \in Events(t, s.gen)` only on the non-full branch; because the overflow transition ignores `e` and `Events` is non-empty, moving the existential quantifier preserves the exact transition relation without redundant successor enumeration. Zero state constraints are applied to the four safety configurations.
