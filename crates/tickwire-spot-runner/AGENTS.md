# Cash strategy runner

The [account runner](src/lib.rs) hands one strategy submission to an existing cash account gateway. Keep one account owner, one recovery queue and one outstanding native request. [Order origins](src/origins.rs) bind immutable local and exchange aliases to the original admitted intent. Handle incorrect identity before encoding an order. The owner of the authenticated account inputs must stop output when its qualification expires.

The `live` and `reconcile` commands use the [private account owner](src/live/account.rs). Check the last durable private cursor and account WAL position before installing a recovered grant. Exercise a new epoch, duplicate fill, mismatched balances and restart when changing this boundary. The `paper` command runs two file-queue producers in a local simulation. Keep its TLS peer, credentials and grants local to the process. The [routing specification](../../specs/implementation/spot-strategy-routing.md) defines what the simulation proves.

Run `cargo test --locked -p tickwire-spot-runner --all-targets`. The integration tests cover competing orders, the last grant check and the native receipts that return to each producer's file-queue owner.
