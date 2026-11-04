# Order and reservation reducer

`tickwire-execution` models the lifetime of an intent and its claim on a bounded single-settlement-asset pool. `tickwire-mock` supplies test services for configuration, immutable objects, journaling and the exchange. The reducer accounts an order from observed fills, fees and terminal facts. Creating an order request only reserves funds. [Reducer](../../crates/tickwire-execution/src/lib.rs)

## Order lifecycle

* Reserve `quantity × unit_budget + fee_budget` as one checked change. Bind the account domain, intent, recovery object, authorization version, deadline and commit mode.
* Accept a receipt only for the exact durable record. Local mode requires local durability. Explicit synchronous mode also requires the replica's durable receipt for that record. A receipt cannot downgrade the request.
* Set the order to `MayHaveSent` **before** calling an exchange adapter. The transport can then prove that no bytes were accepted, remain pending, acknowledge the request or report uncertainty. Pending responses allow other orders within budget. A timeout cannot authorize automatic resubmission. Explicit uncertainty freezes new risk.
* Record fills and charges by their separate fact IDs. A later smaller fill ID can still be new. `spent` includes actual expenditure and fees. `held` covers remaining executable quantity, cumulative terminal fills still missing details, and outstanding fee allowance. Releasing an unsent order’s fee budget increments `fee_released`, not `fees_paid`. A fee report contradicts a later no-bytes claim even if the fee was zero.
* Keep real facts even when actual spend exceeds capital. Available funds can become negative, and new risk closes. Conflicting events, exhausted deduplication capacity or unrepresentable facts freeze the pool without partially updating order or balance fields.

The mock stores an input fact before applying it. An unaccountable stored fact prevents a clean recovery. If persistence fails, freeze and retain the original input for retry. `restart()` reconstructs execution from existing service state and begins frozen. A previous authorization does not survive as proof of current readiness.

## Service contracts

`ConfigStore` updates by revision CAS. An immutable `ObjectStore` accepts the same bytes at an existing key and rejects conflicting bytes. The journal receipt binds the request, sequence and recovery object. Tests distinguish losing a commit receipt from losing an exchange ACK. Venue IDs 7 and 42 exercise the generic adapter. Production multi-asset accounting, physical persistence and reconciliation have their own owners. [Account contract](account.md) · [Physical journal](journal.md)

```sh
cargo test --locked -p tickwire-execution -p tickwire-mock
cargo run --locked -p tickwire-mock --example spot-roundtrip
```

The round trip places a simulated order, applies a fill and cancellation and recovers its journal. The expected terminal summary includes `submit_calls=1 spent=101 held=1 recovered_frozen=true`. The example proves this lifecycle with local services. [Lifecycle tests](../../crates/tickwire-execution/tests/lifecycle.rs) · [Service tests](../../crates/tickwire-mock/tests/services.rs)
