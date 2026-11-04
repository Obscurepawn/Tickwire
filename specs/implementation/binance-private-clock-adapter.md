# Clock for a Binance private session

`ClockAdapter` borrows one `BinanceTimeClient` and implements the private session’s `Clock` interface. The HTTPS time calibration and private request share the same source interval. The adapter holds neither an independent clock estimate nor a send queue. Construct it with the exact account, risk domain, credential generation, product and environment. [Adapter](../../crates/tickwire-binance-session/src/lib.rs)

`sample()` returns the time client’s original `Instant`, domain-bound `MonoTime` and optional `SignedTime`. Before calibration, the stamp is absent. For a prepared signed request, `check_signed` performs these checks in order.

1. Match the account scope, source and clock domain against the original stamp. Convert the nonzero receive window from microseconds to nanoseconds using checked multiplication.
2. Sample the clock again. Pass that sample and **the original stamp** to the caller's remaining-delivery-budget provider. A new sample cannot replace the request’s signed timestamp.
3. Call `TimeClient::check_send`, which samples again after the provider returns. Check the current calibration interval, original stamp, receive window and delivery budget.
4. The private session samples its local clock once more after the final adapter check. It rejects a domain change, backward time or expiry before the transport can accept the first byte.

Domain change, backward time, a latched clock fault, clock jump, arithmetic overflow in the clock core, or exhausted time-client generation maps to `ClockDomain` and stops the private owner. Other calibration, network and protocol errors deny the current request. The delivery budget is explicit.

```sh
cargo test --locked -p tickwire-binance-session --test integration
```

A local HTTPS-to-WSS test gives the request a 1-second deadline, a 5-second signature window and a 10-second calibration lifetime. If the budget callback advances local time to 2 seconds, the transport must accept zero bytes. [Integration tests](../../crates/tickwire-binance-session/tests/integration.rs)
