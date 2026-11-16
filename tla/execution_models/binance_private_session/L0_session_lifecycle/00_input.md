# L0 input

Verified against `crates/tickwire-binance-private/src/{lib,request,client,session}.rs` and `crates/tickwire-transport/src/connection.rs:380-441` alongside 41 private-stream and 12 clock-adapter native tests. The model fixes a single owner, at most one in-flight management request, one account, and a static authentication mode per configuration.

The network adversary permits disconnects, lost or delayed replies, error replies, and events arriving ahead of subscription replies. `PrivateClient` owns fail-closed teardown and reconnect backoff, while the caller supplies authenticated account/API-key binding, initial connection epoch, and the mandatory `Clock` adapter.

Time uses discrete milliseconds (`source_ns = model_ms * 1_000_000`) matching production defaults: `5,000 ms` request/signature window, `3,600,000 ms` `listenKey` validity, `1,800,000 ms` renewal interval, and `100 ms` initial reconnect backoff. Quantified time steps sample creation, renewal, deadline, and post-callback deadline-crossing points; `u64` counter exhaustion is covered by Rust unit tests.

Revision `v2` separates the pre-budget-callback `second` sample from the final post-callback `MonoTime` sample: if a synchronous `DeliveryBudget` callback advances monotonic time past the local request deadline (`5,000 ms` in the model; `1 s` deadline vs. `2 s` callback advance in the native regression test), the request aborts with `0` wire bytes (`NoBytes`).
