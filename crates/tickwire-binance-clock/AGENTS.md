# tickwire-binance-clock

The time client samples Binance server time over TLS and bounds the relationship to the local monotonic clock.

Keep each network sample bound to its authenticated endpoint, product and connection. Exercise expiry, an excessive round trip and an incoming response after the original deadline.

[Crate API](src/lib.rs) · [https](tests/https.rs) · [system clock](tests/system_clock.rs)

Specifications include [clock](../../specs/implementation/clock.md).

```sh
cargo test --locked -p tickwire-binance-clock --all-targets
```
