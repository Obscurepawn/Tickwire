# Binance public depth session

`PublicDepthClient` runs one WebSocket depth subscription and one HTTPS snapshot client for a fixed product, environment and catalog instrument. It connects and waits for both a confirmed subscription and the first increment, which may arrive in either order. It then requests depth and replays the buffered stream through `DepthSync`. It publishes borrowed `BookView` observations, services ping and close frames, backs off after disconnect and rotates the stream before its 24-hour life expires. [Public owner](../../crates/tickwire-binance-public/src/lib.rs) · [Synchronization rules](binance-depth-sync.md)

A gap on a healthy WebSocket starts a new book generation and snapshot attempt while keeping that subscription. A disconnect invalidates the book before creating the next connection epoch and book generation. Exhausted counters, backward time or a changed clock domain stop the owner.

## Preserve receive order

Each batch of decrypted bytes has a monotonic receive time. A frame takes the timestamp of the batch containing its final byte. When the decoder yields with buffered work remaining, resume it before advancing the coordinator clock for any snapshot request, response, failure or timeout. `NeedInput` signals that no complete message remains in the existing buffer. Temporarily suspend new reads while draining so a busy stream cannot starve the snapshot.

Both connections spend one poll’s I/O budget. Before the first application byte, obtain the permit in the transport callback, resample local time and check the operation and connection deadlines. A zero-byte retry repeats the current checks. A partially accepted frame continues from its saved offset. Close first invalidates the book, then uses a bounded closing phase to finish any started frame and echo the close payload. A sink returning false stops the source.

## Read the result

A sync notice lends the source increment and, when available, the validated view. A limited snapshot only proves prices within its coverage. The publisher writes normalized depth and feed state under its own single-writer discipline. JSON depth covers Binance Spot and USD-M, production and test. Spot production also has an explicitly configured SBE diff input. Bybit and OKX use a separate snapshot-only owner. [SBE contract](binance-sbe.md) · [Spot BBO contract](spot-quote-input.md)

```sh
cargo test --locked -p tickwire-binance-public
```

Real TCP/TLS fixtures cover the four product/environment paths, same-socket snapshot rebuild, disconnect/reconnect, Close echo, buffered timing and bounded scheduling. A quiet socket must wake for its retry deadline, even if its heartbeat is later. [Network](../../crates/tickwire-binance-public/tests/network.rs) · [Scheduling](../../crates/tickwire-binance-public/tests/scheduling.rs) · [Rebuild](../../crates/tickwire-binance-public/tests/rebuild.rs)
