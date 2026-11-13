# Market-Data Process (`tickwire-md`)

`tickwire-md` is Tickwire's standalone public market-data publisher. A single producer process owns venue network connections, validates sequence continuity against the approved instrument catalog, and publishes normalized integer records into `memfd`-sealed shared-memory rings. Admitted consumers attach dynamically via the [JOIN protocol](../../specs/implementation/join.md), receiving an immutable baseline snapshot and advancing independent read-only cursors without interrupting running peers.

---

## 1. Inspect Public Bybit and OKX Spot Feeds

Both Bybit V5 (`orderbook.1`) and OKX V5 (`bbo-tbt`) publish top-of-book quotes without trading credentials. From the workspace root, stream newline-delimited JSON diagnostics from either venue:

```sh
cargo run --locked -p tickwire-md -- --spot-quotes crates/tickwire-md/examples/bybit-spot-quote.json
cargo run --locked -p tickwire-md -- --spot-quotes crates/tickwire-md/examples/okx-spot-quote.json
```

Each emitted notice records the venue ID, native symbol, connection epoch, native sequence, exchange timestamp, monotonic receive timestamp, and integer bid/ask ticks and lots. On disconnect or frame validation failure, the source emits an unavailable notice and requires a fresh subscription acknowledgement and uncrossed snapshot on a new connection epoch before resuming live status.

### Publish a Shared Spot BBO Topic

To broadcast one public feed across multiple strategy workers and an account freshness observer, run `--spot-market FILE`:

* Validates the configured venue, symbol, and price/quantity increments against the production [instrument catalog](../../specs/implementation/instrument-catalog.md).
* Publishes paired exact-BBO and feed-state records into a shared-memory ring and writes the active resource descriptor to `spot-market.active` inside the control directory.
* Strategies read `MarketView::best_quote()` after checking `continuous()` and quote age (`state_age_ns`), while full order-book depth remains intentionally marked unknown on BBO-only topics.

See the [shared spot market walkthrough](examples/spot-market.md) and the [public spot quote specification](../../specs/implementation/spot-quote-input.md) for full configuration details.

---

## 2. Run Full Binance Depth and Event Topics

`examples/md.json` configures two independent Binance Spot shared-memory topics—Topic `101` (`BTCUSDT`, `ETHUSDT`) and Topic `102` (`BTCUSDT`)—demonstrating how a single decoded venue feed can fan out to multiple topic rings.

Before launching in a deployment directory, supply approved process identities, executable digests, host boot/namespace bindings, DER trust anchors, unique resource IDs, and non-overlapping quota grants:

```sh
cargo build --locked -p tickwire-md --bin tickwire-md
chmod 700 /opt/tickwire/md /opt/tickwire/md/admissions
target/debug/tickwire-md --resource /opt/tickwire/md/topic-101.json --output /opt/tickwire/md/topic-101.bin
target/debug/tickwire-md --resource /opt/tickwire/md/topic-102.json --output /opt/tickwire/md/topic-102.bin
target/debug/tickwire-md --config /opt/tickwire/md/md.json
```

### Configuration Reference

| Field | Responsibility |
| :--- | :--- |
| `identity`, `clock_generation` | Binds the producer's approved executable digest (`1` = `x86_64`, `2` = `aarch64`) and shared monotonic clock domain. |
| `feeds[].owner_id`, `source` | Assigns an isolated quota consumer ID per network feed and selects product (`Spot` or `USD-M`) and environment (`Production` or `Test`). |
| `stream_address`, `snapshot_address`, `trust_anchors` | Configures TCP endpoints and DER root certificates; `rustls` still verifies the official exchange hostname. |
| `instrument`, `topics`, `snapshot_limit` | Maps a catalog instrument into target topics and bounds REST snapshot depth (levels outside snapshot coverage remain explicitly unknown). |
| `topics[].resource` | Points to a 4,096-byte wire resource header enabling arena allocation and depth-coverage metadata across a uniform catalog hash. |
| `quota_directory`, `quota` | Persists rate-limit grants and consumed weight across process restarts until grant expiry. |
| `control_directory`, `admission_directory` | Stores JOIN request state and per-reader credential approvals (`admission_directory` requires mode `0700`). |
| `maximum_idle_ns` | Caps reactor poll sleep unless an earlier calibration or heartbeat deadline takes precedence. |

---

## 3. Admit Consumers and Handle Bank Rollover

When `tickwire-md` activates a Binance topic bank, it writes the live descriptor alongside the configured resource path with a `.active` suffix. Once the supervisor spawns a consumer and knows its OS `PID`, `UID`, and `GID`, generate the connector configuration and register its admission:

```sh
target/debug/tickwire-md --client /opt/tickwire/md/exec-client.json --output /opt/tickwire/md/exec-connect.json
target/debug/tickwire-md --admit /opt/tickwire/md/exec-admission.json --directory /opt/tickwire/md/admissions
```

* **Credential verification:** During JOIN, `tickwire-md` cross-checks the registered `ConnectConfig`, reader `PID`/`UID`/`GID`, approved role-1 supervisor identity, and fresh snapshot ID against kernel `SO_PEERCRED` peer credentials (programmatic entry point: `tickwire_md::server::register_admission`).
* **Hosted strategies:** Supply the role-3 reader configuration in `Bootstrap.external_market` and set `HostConfig.md_admission_directory` to the private admission directory. Workers join the market topic directly while keeping intents and execution reports on separate SPSC channels.
* **Fault invalidation and rollover:** A sequence gap or ring capacity fault immediately invalidates the active bank via the control plane. Publishing resumes only on a new resource ID with strictly higher feed and ring generations (retaining up to 32 historical owners per topic before requiring a process restart). Expired leases close the reader and require fresh epoch, cursor, request, and snapshot identifiers to rejoin.
