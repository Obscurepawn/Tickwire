# Binance depth synchronization

`DepthSync<P, LEVELS, UPDATES, BUFFER>` combines WebSocket deltas with a REST snapshot for one catalog instrument. Its `Book` applies the Binance Spot or USD-M sequence policy. The coordinator owns the mutable book. A reader sees a fresh live view only after all buffered updates have passed validation. [Coordinator](../../crates/tickwire-binance/src/depth_sync.rs) · [Book contract](book-core.md)

## Connect and bridge

| State | Operation |
| --- | --- |
| `Disconnected` | Call `connect` with a larger connection epoch. |
| `Buffering` | Record subscription acknowledgement and the first delta independently. Once both arrive, request a snapshot and continue buffering deltas. |
| `Bridging` | The snapshot is newer than every buffered event. Wait for the first applicable delta on this connection. |
| `Live` | Apply each continuous delta, using its original receive time for freshness. |
| `Exhausted` | A generation or request ID can no longer increase. Replace the owner with a fresh external generation range. |

A snapshot token binds the full book key, connection epoch, book generation and request ID. Another connection requires a greater epoch. A gap on the same connection advances the book generation. The coordinator discards a stale connection event or completed request before inspecting its bytes or time. A failed replay invalidates the whole attempt, including any successful prefix.

The stable instrument ID is a nonzero `u64`. `BookKey.instrument` is an owner-local `u32` slot and may be zero. Keep the full `(venue, local slot, source)` key across reconnects and keep it unique within a publishing directory. The catalog binding supplies the symbol, product and price/quantity increments.

For Spot, a snapshot older than the earliest buffered `U` yields `TooOld`. Retain the buffer and request a newer snapshot. The book drops obsolete deltas and bridges at `L+1`. USD-M retains a first delta ending at snapshot sequence `L` if it spans `L`, then checks `pu` on each new update. An obsolete delta or REST response never refreshes the observation clock.

## Recovery and capacity

An HTTP error, transport error, or deadline at `now >= deadline` cancels the snapshot request and keeps the buffer. The network owner schedules the next attempt under its quota and backoff policy. A bad message, gap, full FIFO or rejected book update clears the current synchronization attempt. On the same connection the subscription remains confirmed, but another delta must arrive before a new snapshot request. Disconnect also clears the subscription.

Each side permits 1–5000 levels. One delta may contain 1–5000 configured update slots across both sides. The FIFO holds up to 4096 deltas with at most 64 MiB of reserved objects. Each input is limited to 16 MiB. The REST limit fits one side. USD-M permits 5, 10, 20, 50, 100, 500 or 1000. An oversized snapshot fails as a whole. Limited REST snapshots always leave outer coverage unknown, including after a known best level disappears. Configuration, JSON decoding and rebuilds can allocate.

## Verify

```sh
cargo test --locked -p tickwire-binance --test depth_sync --test depth_sync_server
```

The behavior fixtures cover the first-delta/ack gate, both sequence policies, stale responses, timeout boundaries, bounds, partial replay failure and unknown coverage. The network tests use local TLS peers, chunked HTTP and fragmented WebSocket frames. They inject deltas during the snapshot request, a gap on a live socket, and a disconnect followed by a new epoch. [Behavior tests](../../crates/tickwire-binance/tests/depth_sync.rs) · [Transport tests](../../crates/tickwire-binance/tests/depth_sync_server.rs)
