# tickwire-binance-public

`tickwire-binance-public` (`#![forbid(unsafe_code)]`) drives nonblocking Binance Spot (JSON and SBE v1) and USD-M
public market-data sessions (`DepthStreamClient`, `PublicEventClient`) over `tickwire-transport`, pairing live WSS incremental streams (`Lane::Stream`) with
HTTPS depth snapshots (`Lane::Snapshot`) to maintain a synchronized `tickwire-book` replica. Market-source publishers
poll the client with bounded `PollBudget` quotas and consume `Notice` callbacks (`Sync`, `Gap`, `Http`); shared-memory
ring publication and strategy fan-out belong upstream.

## Invariants and Change Guidelines

* **Generation-fenced snapshot bridging**: Advance `book_generation` on every synchronization reset in
  [src/client.rs](src/client.rs) and [src/protocol.rs](src/protocol.rs) so `BookView` borrows delivered in
  `Notice::Sync` only reflect a bridged, sequence-contiguous order book (`BinanceSpot` or `BinanceUsdM`), emitting
  `Notice::Gap` immediately on sequence gaps, disconnects, or stale heartbeats.
* **Idempotent `PublicPermit` charging per lane**: Check `PublicPermit::allow` before every `Connect`, `Upgrade`,
  `Subscribe`, `Pong`, `Close`, and `Snapshot` step in [src/client.rs](src/client.rs), charging
  `(owner_id, domain, source, lane, connection_epoch, operation, kind)` at most once and forwarding HTTP
  status/headers via `observe_http`.
* **Spot SBE credential confinement**: `SbeAccess` in [src/lib.rs](src/lib.rs) validates alphanumeric Ed25519 API keys
  without implementing `Debug`, attaches `X-MBX-APIKEY` solely to the Spot SBE WebSocket upgrade, and never sends it
  to the REST snapshot endpoint.
* **Bounded poll work and zero-escape borrows**: Respect `PollBudget` (`io`, `max_ws_messages`, `max_decoder_turns`,
  `max_notifications`) across [src/client.rs](src/client.rs) and [src/event.rs](src/event.rs), and halt permanently
  (`Phase::Stopped`) if `PublicSink::on_notice` returns `false`.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/client.rs](src/client.rs) ·
  [src/protocol.rs](src/protocol.rs) · [src/event.rs](src/event.rs)
* **Integration tests**: [tests/network.rs](tests/network.rs) · [tests/sbe_network.rs](tests/sbe_network.rs) ·
  [tests/sbe_config.rs](tests/sbe_config.rs) · [tests/rebuild.rs](tests/rebuild.rs) ·
  [tests/events.rs](tests/events.rs) · [tests/control.rs](tests/control.rs) ·
  [tests/scheduling.rs](tests/scheduling.rs) · [tests/timer.rs](tests/timer.rs) ·
  [tests/constructor_stack.rs](tests/constructor_stack.rs)
* **Specifications**: [binance-public-session.md](../../specs/implementation/binance-public-session.md) ·
  [binance-depth-sync.md](../../specs/implementation/binance-depth-sync.md) ·
  [binance-sbe.md](../../specs/implementation/binance-sbe.md)

```sh
cargo test --locked -p tickwire-binance-public --all-targets
```
