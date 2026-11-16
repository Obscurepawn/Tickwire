# `v2` review notes

In the native Red test, advancing `MonoTime` from `0 s` to `2 s` inside the second `DeliveryBudget` callback previously allowed a `1 s`-deadline signed request to pass using the pre-callback `second` sample and transmit `219` wire bytes. Sampling `MonoTime` once more immediately after the final `check_clock` callback — validating clock domain, monotonic order, request freshness, and local deadline while preserving the original `SignedTime` — ensures `0` bytes (`NoBytes`) are written across both regression tests.

Monotonic regression after the callback maps to `Error::Clock` and domain mismatch maps to `Error::ClockDomain`; see the [Binance private clock adapter specification](../../../../specs/implementation/binance-private-clock-adapter.md) and [Binance private session specification](../../../../specs/implementation/binance-private-session.md).
