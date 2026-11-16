# L0 input (archived `v1`)

Verified against the pre-`v2` `crates/tickwire-binance-private/src/{lib,request,client,session}.rs` revision alongside 41 native/Linux behavioral tests. The model fixes a single owner, at most one pending management request, one account, and a static authentication mode per configuration.

The network adversary permits disconnects, lost or delayed replies, error replies, and early event frames. `PrivateClient` owns fail-closed teardown and reconnect backoff, while the caller supplies authenticated account/API-key binding, initial connection epoch, and the `Clock` adapter.

Time uses discrete milliseconds (`source_ns = model_ms * 1_000_000`) matching default configuration constants: `5,000 ms` request/signature window, `3,600,000 ms` `listenKey` validity, `1,800,000 ms` renewal interval, and `100 ms` initial backoff.
