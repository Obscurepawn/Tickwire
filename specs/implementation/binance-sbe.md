# Binance Spot SBE market data

Tickwire pins `stream_1_0.xml` at upstream commit `f4a9c0be68235f758b46f571f7313c63d1306b25`, schema ID 1, version 0. Four templates represent trades, best bid/ask, a depth-20 snapshot and incremental depth. The generator produces compile-time constants. The runtime does not read XML. [Schema and original digest](../../crates/tickwire-binance-sbe/schema/source.json)

The decoder validates the entire frame and its capacity before returning a borrowed view. It checks template, schema/version, group width, enum/null fields, symbol, lengths, trailing bytes and numeric ranges. Convert integer mantissa and exponent to approved instrument ticks/lots exactly. Refuse overflow or an off-grid observation. Exchange times are microseconds. Multiply by 1000 with overflow checking to publish nanoseconds. Bind observations to the configured instrument, catalog, source and generation.

## Diff, depth-20 and independent events

`PublicDepthClient::new_sbe` supports Spot production, microseconds and default stream speed. Connect to `stream-sbe.binance.com` with hostname verification and the explicit Ed25519 API key in the Upgrade. The client needs no private signing key for this operation. REST initialization still uses `api.binance.com` without that key. Both a raw stream URL and explicit text subscription are supported. A binary delta may arrive before an explicit subscription ACK. Buffer it and wait for the exact ACK before requesting the REST snapshot.

The diff decoder sends checked `U/u` and zero-quantity removals to the existing Spot `DepthSync`. Reject old tokens before decoding. JSON and SBE share generation, replay, capacity and coverage rules. A depth-20 frame proves coverage only through the last observed price on each side, or unknown for an empty side. It cannot initialize the diff book in place of the REST baseline.

A sync notice identifies SBE and lends the binary input. The publisher emits normalized levels and Quote/FeedState records through SHM. Set `sbe_api_key_file` on the Binance feed to enable the SBE option. Separate SBE trade and BBO event owners are connected to the exact-event publishing path. The strategy logs the records and payloads it consumed. [Exact event publication](binance-market-events.md)

```sh
cargo test --locked -p tickwire-binance-sbe
cargo test --locked -p tickwire-binance-public --test sbe_config --test sbe_network
cargo test --locked -p tickwire-md --test sbe_publication --test market_events
```

Independent Python fixtures use literal little-endian field layouts. The Linux round trip drives a TLS WebSocket, REST snapshot, subsequent binary update, real memfd and reader. It checks integer units, sequence, nanoseconds and depth payload. [Fixture generator](../../crates/tickwire-binance-sbe/tools/fixtures.py) · [SHM integration](../../crates/tickwire-md/tests/sbe_publication.rs)
