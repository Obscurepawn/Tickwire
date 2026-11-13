# Binance Depth Snapshot and Delta Synchronization

`DepthSync<P, LEVELS, UPDATES, BUFFER>` coordinates an asynchronous WebSocket diff stream with an HTTPS `/depth` snapshot for a single catalog-bound Binance instrument (`BinanceSpot` or `BinanceUsdM`). It owns a heap-boxed `Book<P, LEVELS, UPDATES>` (boxed once at construction so large inline level arrays do not bloat caller stack frames), a pre-reserved bounded delta FIFO, and monotonic connection, book-generation, and snapshot-request tokens.

`DepthSync` owns the local book state machine and sequence-bridging buffer, while its caller (`PublicDepthClient`) owns sockets, TLS, timers, rate-limit permits, and backoff scheduling. Readers can borrow a `BookView` only after the snapshot and every buffered delta have passed sequence and structural validation.

## State Model and Bounds

Construction validates all compile-time and runtime capacities up front (`SyncError::InvalidConfig` on violation) and pre-encodes the exact REST path (`/api/v3/depth?symbol=...&limit=...` for Spot, `/fapi/v1/depth?symbol=...&limit=...` for USD-M):

| Parameter | Permitted Range | Rationale |
| --- | --- | --- |
| `LEVELS` (per side) | `1..=5,000` (`MAX_DEPTH_LEVELS`) | Bounds resting price levels per side inside `Book`. |
| `UPDATES` (per delta) | `1..=5,000` | Bounds total level mutations per decoded `DepthUpdate`. |
| `BUFFER` (delta FIFO) | `1..=4,096` events and `<= 64 MiB` (`size_of::<DepthUpdate<U>>() * B`) | Prevents unbounded buffering while awaiting a REST snapshot. |
| `max_message_bytes` | `1..=16 MiB` (`MAX_MARKET_MESSAGE_BYTES`) | Caps individual REST snapshot and WebSocket frame payloads. |
| `snapshot_limit` | `1..=LEVELS`; USD-M restricts to `{5, 10, 20, 50, 100, 500, 1000}` | Matches venue REST constraints; oversized snapshots fail atomically. |

The coordinator advances through five states (`State`):

1. **`Disconnected`**: Initial state and post-disconnect state; no active `ConnectionToken`, subscription unconfirmed, book invalidated. Calling `connect(epoch)` with `epoch > last_connection_epoch` mints a `ConnectionToken` and enters `Buffering`.
2. **`Buffering`**: Independently records `subscription_ready(token)` and the earliest buffered delta's `first_update_id` (`first_u`)—which may arrive in either order on the wire. Only after *both* are present may the caller invoke `request_snapshot(now, deadline)`, which mints a `SnapshotToken { connection, generation, request }` while continuing to append live deltas to the FIFO.
3. **`Bridging`**: Entered when an installed REST snapshot's `lastUpdateId` (`L`) is newer than every buffered delta, draining the FIFO and waiting for the first live delta on the same connection that satisfies the product's bridge rule.
4. **`Live`**: Every continuous delta applies directly to `Book` using that delta's original monotonic `received_at_ns`.
5. **`Exhausted`**: Entered permanently if `book_generation` or `request_id` overflows `u64::MAX`; the caller must retire the owner and allocate a fresh external generation range.

## Core Invariants

* **Token-first stale discard**: Every `ConnectionToken` binds `(BookKey, epoch)` and every `SnapshotToken` binds `(ConnectionToken, book_generation, request_id)`. Events or completed HTTP responses carrying a stale token are discarded *before* inspecting their payload bytes or timestamps.
* **Product-exact snapshot bridging**:
  * **Spot (`BinanceSpot`)**: If the snapshot's `L` trails the earliest buffered delta (`L + 1 < first_buffered_U`), `DepthSync` rejects the snapshot with `Outcome::RetrySnapshot(RetryReason::TooOld)`, *retains* the buffered deltas, and requests a newer snapshot. Otherwise it drops obsolete deltas (`u <= L`) and bridges at `U <= L + 1`.
  * **USD-M (`BinanceUsdM`)**: Retains a bridging delta ending at `L` (`U <= L <= u`) and requires `pu == L` on every subsequent `Live` update.
* **Atomic replay and clock preservation**: If any buffered delta fails sequence or structural validation during snapshot replay, the entire synchronization attempt fails (`Outcome::Rebuild`), invalidating any prefix applied earlier in that batch. Obsolete deltas and REST snapshots never advance the observation freshness clock.
* **Permanent partial-snapshot boundary**: Because Binance REST snapshots return bounded depth (`bids_complete = false`, `asks_complete = false`), outer book coverage remains `Unknown`—including when all covered top-of-book levels are deleted while deeper levels remain stored.

## Failure and Recovery Semantics

Transient snapshot failures—HTTP status errors (`RetryReason::Http(status)`), transport errors (`RetryReason::Transport`), or timeouts at `now >= deadline` (`RetryReason::Expired`)—clear only the pending `SnapshotToken` and preserve the buffered delta stream so the network owner can schedule another snapshot request under its quota and backoff policy.

By contrast, malformed frames, sequence gaps, FIFO overflow (`RebuildReason::BufferOverflow`), book validation errors, or regressed timestamps abort the current synchronization attempt, advance `book_generation`, clear the FIFO, and return `Outcome::Rebuild`. When this occurs on an otherwise healthy socket, `subscribed` remains `true`, but at least one new delta must arrive on the new generation before `request_snapshot` is permitted again. Calling `disconnect` additionally clears `subscribed` and requires a strictly larger `epoch` on the next `connect`.

Keep the full `(venue, local_slot, source)` `BookKey` stable across reconnects and unique within a publishing directory; `BookKey.instrument` is an owner-local `u32` dense slot (which may be `0`), whereas external identity remains the nonzero `u64` catalog `instrument_id`.

## Verification and References

```sh
cargo test --locked -p tickwire-binance --test depth_sync --test depth_sync_server
```

Deterministic unit fixtures test out-of-order subscription ACK vs first delta arrival, Spot `TooOld` retention vs USD-M `pu` bridging, stale token rejection, deadline boundaries, FIFO capacity limits, mid-batch replay rollback, and partial coverage. Local TLS server tests exercise chunked HTTPS snapshots concurrent with fragmented WebSocket deltas, live sequence-gap rebuilds on an open socket, and epoch advancement across disconnects.

* [Single-owner depth synchronization coordinator](../../crates/tickwire-binance/src/depth_sync.rs)
* [Bounded order book specification](book-core.md)
* [Deterministic synchronization state-machine tests](../../crates/tickwire-binance/tests/depth_sync.rs)
* [Local TLS server and fragmented-stream tests](../../crates/tickwire-binance/tests/depth_sync_server.rs)
