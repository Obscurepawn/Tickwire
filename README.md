# Tickwire

Tickwire is a Rust trading engine for Linux. A market-data process publishes quotes, each strategy runs in its own worker, and an account process owns orders and cash for one risk domain. Start with one host. Add workers to run more strategies against the same market data and account limits.

## Run a spot order

Use the Rust version in `rust-toolchain.toml`. The example supplies a book, a strategy, a simulated exchange and a temporary journal. It places one order, processes a fill and cancellation, then reopens the journal.

```sh
cargo run --locked -p tickwire-mock --example spot-roundtrip
```

The last line reports `submit_calls=1 spent=101 held=1 recovered_frozen=true`. The last field tells you the restarted account is waiting for reconciliation before accepting new risk.

## Exchange support

| Venue | Public market data | Spot order execution |
| --- | --- | --- |
| Binance | Spot and USD-M depth, trades and best bid/ask. Spot also has a fixed-schema SBE decoder. | The file-configured runtime drives orders, private reports, reconciliation and journal recovery. The Linux spot process suite uses a local TLS exchange. |
| Bybit | V5 public spot `orderbook.1`, with one replacement best bid/ask per update. | The adapter signs V5 limit and market orders, cancellations and status queries. The shared-account paper runner uses this API over TLS. |
| OKX | V5 public spot `bbo-tbt`, with prices and sizes converted to integer ticks and lots. | The adapter signs V5 cash limit and market orders, cancellations and status queries. It distinguishes the request acknowledgement from a final order status. |

Run a public quote feed with the example for your venue. It uses the public endpoint and prints each quote with its venue, instrument, session and timestamps.

```sh
cargo run --locked -p tickwire-md -- --spot-quotes crates/tickwire-md/examples/bybit-spot-quote.json
cargo run --locked -p tickwire-md -- --spot-quotes crates/tickwire-md/examples/okx-spot-quote.json
```

For a shared feed, start one `tickwire-md --spot-market` publisher and give each worker its own JOIN membership. Both workers receive the same quote sequence and keep independent cursors. A top-of-book feed reports known bid/ask and leaves depth coverage unknown. The [Linux example](crates/tickwire-md/examples/spot-market.md) creates the source configuration, socket and catalog.

The read-only account CLI supports native order status, open orders, balances and fills. Configure the exchange credentials listed in the [execution guide](specs/implementation/spot-execution.md). This command reads an OKX demo balance.

```sh
cargo run --locked -p tickwire-spot-execution --bin tickwire-spot-observe -- okx test balance
```

To exercise two strategy orders against the Bybit or OKX wire protocol, run the paper account with a fresh directory. Replace `bybit` with `okx` to select the other adapter. Two independent producers read a synthetic quote and submit one limit order each. The account sends both signed requests to a local TLS exchange and reports each acknowledgement to the originating member.

```sh
cargo run --locked -p tickwire-spot-runner -- paper bybit /tmp/tickwire-bybit-paper
```

The file-configured live account currently grants new risk through the Binance private reconciler. Bybit and OKX support public strategy callbacks, read-only private observation and native order execution through a qualified gateway. Their private-stream recovery and production account activation are still open. The [public quote contract](specs/implementation/spot-quote-input.md) and [spot execution contract](specs/implementation/spot-execution.md) specify the supported messages and the recovery boundary for each adapter.

## Follow one order

1. The market-data owner applies the venue's sequence rules and publishes a normalized record. Each subscribed worker consumes the stream at its own cursor.
2. A strategy reads its current market view and returns an intent. The worker records the originating input, instance and generation before publishing the command reference.
3. The account owner checks the command and its recovery dependencies, reserves exposure and synchronizes the admission record. The sender checks the current execution grant at the first write to TLS.
4. The venue returns acknowledgements, order updates and fills. The account persists the input and applies the economic facts. Strategies receive ordered account reports.

The [account gateway](crates/tickwire-account-gateway/src/lib.rs) is the place to trace a new order. Start at `submit` for admission and `poll` for the final send check. For multiple workers, start with the [strategy host](crates/tickwire-strategy-host/AGENTS.md) and [example strategy](crates/tickwire-strategy-example/README.md). The [system design](specs/implementation/single-host-trading.md) explains the state and performance cost at each boundary.

## Deploy two strategies

Run the launcher from the repository root on Linux with Python 3.11+, OpenSSL, binutils and the pinned Rust toolchain.

```sh
python3 scripts/run-two-strategies.py
```

The launcher creates the market catalog and both worker profiles. It starts one `tickwire-md --spot-market` process against a local TLS exchange and launches two registered counting workers through the App's multi-strategy host. After both JOIN, the exchange publishes a final quote. Look for `two_strategies_observed`, two different worker PIDs, `quote_sequence: 1000000` in both checkpoints, positive callback counts and `strategy_effects: 0`. This observation-only example needs no exchange account. The Linux process harness hosts the group directly and never starts an order gateway.

Use `--venue okx` for the OKX feed. Add `--live` to observe the actual public venue, `--proxy ADDRESS:PORT` for an HTTP CONNECT proxy, or `--keep` to retain the generated group, bootstraps and checkpoint evidence. The two live checkpoints can capture different quote sequences as the market moves. The [shared-market guide](crates/tickwire-md/examples/spot-market.md) describes the generated files and the production deployment fields. One account process can host multiple strategies sharing a risk domain. Independent risk domains run separate account owners.

## Development

```sh
cargo test --locked -p tickwire-strategy-example --all-targets
cargo test --locked -p tickwire-account-gateway --test submission
cargo bench -p tickwire-book --bench depth_apply
python3 scripts/verify-documentation.py
```

For the Linux process chain, run `./scripts/run-core-chain-linux.sh spot-fill` with Docker. The [test guide](scripts/README.md) lists the native tests, process scenarios and model checks. The [specification index](specs/INDEX.md) links the contracts to their owners. Cross-host replication and failover have their own [implementation notes](specs/implementation/replica.md).

Tickwire is a private workspace. Packages use `publish = false` and the repository declares `UNLICENSED`.
