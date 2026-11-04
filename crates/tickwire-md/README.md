# Market data

`tickwire-md` owns the exchange connection and publishes a local topic. Each admitted reader gets a complete baseline, its own cursor, and a read-only mapping of the same ring. A strategy can join while other strategies are running.

## Run a public spot feed

From the repository root, use either command to print top-of-book snapshots. Neither venue requires a key for public data.

```sh
cargo run --locked -p tickwire-md -- --spot-quotes crates/tickwire-md/examples/bybit-spot-quote.json
cargo run --locked -p tickwire-md -- --spot-quotes crates/tickwire-md/examples/okx-spot-quote.json
```

Bybit uses `orderbook.1`. OKX uses `bbo-tbt`. A quote carries native sequence, connection epoch, exchange timestamp, local receive timestamp, and prices and sizes in ticks and lots. A disconnect emits an unavailable notice. After reconnect, the source must receive a fresh subscription acknowledgement and snapshot.

For one feed shared by several strategies, use `--spot-market FILE`. This mode checks the instrument, venue, symbol and increments against the approved production catalog. It publishes a BBO-only topic and writes the current resource header to `spot-market.active` under the control directory. Strategies read `MarketView::best_quote()` and check quote age and feed state. Full depth is unknown. The [single-host launch example](examples/spot-market.md) prepares the source, catalog, resource and JOIN server files. See the [spot input contract](../../specs/implementation/spot-quote-input.md).

## Run Binance depth and event topics

`examples/md.json` defines two independent Binance Spot feeds. Topic 101 includes BTCUSDT and ETHUSDT. Topic 102 includes BTCUSDT. One network feed can publish to both topics.

Supply the approved process identity and artifact hashes, host namespace and boot ID, catalog and instruments, CA files, unique resource IDs and actual quota grants. The example budgets are placeholders. Owners sharing an IP must receive non-overlapping grants from the same quota authority.

```sh
cargo build --locked -p tickwire-md --bin tickwire-md
chmod 700 /opt/tickwire/md /opt/tickwire/md/admissions
target/debug/tickwire-md --resource /opt/tickwire/md/topic-101.json --output /opt/tickwire/md/topic-101.bin
target/debug/tickwire-md --resource /opt/tickwire/md/topic-102.json --output /opt/tickwire/md/topic-102.bin
target/debug/tickwire-md --config /opt/tickwire/md/md.json
```

| Configuration | Purpose |
| --- | --- |
| `identity`, `clock_generation` | Bind this producer and its shared monotonic clock. Architecture 1 is x86-64, 2 is AArch64. A different executable needs its own approved digest. |
| `feeds[].owner_id`, `source` | Give each network owner an independent quota request identity. Choose spot or USD-M, production or test. |
| `stream_address`, `snapshot_address`, `trust_anchors` | Select TCP addresses and trusted DER roots. TLS verifies the official hostname for the selected product. |
| `instrument`, `topics`, `snapshot_limit` | Bind a catalog instrument and its topic memberships. USD-M accepts perpetual or dated delivery contracts. Levels outside the snapshot’s proven coverage remain unknown. |
| `topics[].resource` | Select a 4096-byte wire header with arena and depth coverage features. Members of a topic share one catalog version and hash. |
| `quota_directory`, `quota` | Persist finite grants and their consumption across restarts. Expired grants cannot fund another request. |
| `control_directory`, `admission_directory` | Persist JOIN request IDs and process approvals. The admission directory must be private (0700). |
| `maximum_idle_ns` | Limit the poll wait. An earlier owner deadline takes precedence. |

## Admit a reader

The Binance process writes the current bank descriptor beside the configured resource with suffix `.active`. Point the client template at the active descriptor. The template identifies the running MD PID/UID/GID, the reader, its cursor, and its request ID.

```sh
target/debug/tickwire-md --client /opt/tickwire/md/exec-client.json --output /opt/tickwire/md/exec-connect.json
target/debug/tickwire-md --admit /opt/tickwire/md/exec-admission.json --directory /opt/tickwire/md/admissions
```

The launcher creates the admission after it knows the child's PID. Include the generated `ConnectConfig`, PID/UID/GID, the approved role-1 supervisor and PID, and a fresh snapshot ID. The MD server compares the approval with Unix peer credentials. The library entry point is `server::register_admission`.

For strategies, put the role-3 reader configuration in `Bootstrap.external_market` and the private directory in `HostConfig.md_admission_directory`. Each worker joins a complete topic directly. Its intents and reports use separate execution channels. Recovery records retain the JOIN baseline, consumed ring records, arena data, and decision inputs.

A Binance gap or capacity fault invalidates the old bank through the control channel. A successor needs a new resource ID and larger feed and ring generations. The process retains up to 32 old owners per topic. On exhaustion, restart with a newly approved identity. A lease expiry closes the reader. Rejoining requires new epoch, cursor, request and snapshot IDs.
