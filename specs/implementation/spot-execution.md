# Bybit and OKX spot execution

A strategy submits a canonical order intent. The account gateway reserves cash, journals the admission and checks its trading grant immediately before the first write to TLS. The spot encoder translates the admitted command into the venue's native request. One encoder binds a venue, environment, account, credential scope and trusted UTC clock.

| Operation | Request | What a successful reply establishes |
| --- | --- | --- |
| Create | A limit or market cash order in base units, with the original client order ID. | The venue acknowledged the request. An order-status report or trade can follow. |
| Cancel | A single existing order, identified within the bound account and instrument. | The venue acknowledged the cancellation request. The order can still fill in a race. |
| Query | One order on its native instrument. | A matching order ID and instrument establish an observed status. A cache miss or interrupted connection leaves the outcome unknown. |
| Book a fill | An authenticated execution record with trade ID, side, size, price, fee and fee asset. | The account records the individual trade once. A status snapshot supplies no per-trade fee. |

For each fill, the caller supplies the original intent, internal order ID, source scope, connection epoch and durable input stream position. `record_fill` resolves the command from the same gateway journal, checks the native IDs, instrument and side, then appends through `record_ingress_event`. An OKX fill uses the fee for that execution. An unrecognized fee asset or an unsupported Bybit regional fee stops booking until the decoder can represent it. The feed owner retains the authenticated input envelope for replay.

## Inspect an account

`tickwire-spot-observe` reads a private account using the venue's global test or production endpoint. Use an API key with read access. Supply `TICKWIRE_SPOT_KEY` and `TICKWIRE_SPOT_SECRET`, plus `TICKWIRE_SPOT_PASSPHRASE` for OKX. Each accepts a mounted secret through its `_FILE` variable. Choose one source per credential.

```sh
cargo build --locked -p tickwire-spot-execution --bin tickwire-spot-observe
target/debug/tickwire-spot-observe bybit test open-orders BTCUSDT
target/debug/tickwire-spot-observe okx test balance
```

Use `order SYMBOL CLIENT_ID` to look up one order and `fills SYMBOL [CURSOR]` to read a page of trades. Each invocation takes a public exchange-clock sample, signs a private GET and verifies the TLS certificate on both connections. The JSON result includes the HTTP status, venue code, selected fields, sample time and continuation token if the exchange provides one. Transport errors, malformed replies and venue denials produce a failing exit status. Each page allocates in proportion to its size, with a 2 MiB body limit. Balances describe the Bybit Unified wallet or the OKX trading account. A recent-order lookup is bounded by the exchange's retention window.

### Compare account state over a time range

`scan EXPECTED_UID START_UTC_MS` fetches the native account identity, balance, standard spot open orders across instruments, and spot fills through the sampled exchange time. Supply a numeric UID and a start time within the past three days. The scan follows each cursor and verifies the UID again after the final page. It checks the Bybit execution time or OKX transaction timestamp on each fill. Repeated IDs, repeated cursors and out-of-range records fail the scan. Each collection allows 128 pages and 8 MiB of selected row data. A clock sample expires after 120 seconds.

A scan combines pages collected at different instants. On OKX the separate algo-order service and non-order wallet transfers fall outside its scope. The result carries `fresh_risk_qualified: false`. A strategy needs a separate account trading grant.

### Watch a private stream

`watch EXPECTED_UID SECONDS` observes a connection for 1–60 seconds. The tool verifies the UID via REST with the same key, authenticates the WebSocket and emits one JSON line per event. Bybit sends `order.spot`, `execution.spot` and `wallet`. OKX sends SPOT `orders` and trading-account `account`. Ordinary-tier OKX users receive individual fills on the orders channel. The client tracks subscription acknowledgements and heartbeat deadlines. A venue control error or unexpected product or connection ends the session. A new observation starts its own message count. Embedders can read the original channel-validated data through `PrivateFeed::on_text` and `Batch::records`.

## Recover a trading account

After a disconnect, the account owner needs to establish which private events its journal has already applied. A Bybit wallet subscription and an OKX orders subscription both begin without an initial portfolio snapshot. Bybit can reuse the same execution sequence across symbols. OKX can repeat an order update, or report a filled market order with a zero last-fill size. A balance observed before and after a gap can look identical even if an intervening order was placed and canceled. [Bybit wallet stream](https://bybit-exchange.github.io/docs/v5/websocket/private/wallet) · [Bybit executions](https://bybit-exchange.github.io/docs/v5/websocket/private/execution) · [OKX orders channel](https://www.okx.com/docs-v5/en/#order-book-trading-trade-ws-order-channel)

The file-configured trading runtime currently obtains its new-risk grant from the Binance private reconciler. A live Bybit or OKX owner still needs to persist its private feed, identify the overlap between applied messages and query coverage, account for transfers and other enabled modes, and establish exclusive account ownership. The observer provides evidence for inspection. `RestSession::submit` accepts a gateway with an existing qualified baseline and authority. TLS integration tests exercise both venues' signed order, cancel, query and fill-accounting paths. Private-stream state tests cover both dialects, with an OKX login-to-report flow over loopback TLS.

## Cost of one order

The encoder formats decimals and IDs into owned strings, builds a JSON body or query string and constructs an HTTP byte buffer. HMAC-SHA256 reads the exact signed payload once. Bybit uses a hexadecimal digest and OKX uses Base64. A valid clock sample avoids a network time query per attempt. The gateway checks the command and destination, copies bytes into a bounded `PreparedSend` and clones the account state used for risk admission. Allocation and CPU cost depend on command size and account state. [Spot encoder](../../crates/tickwire-spot-execution/src/lib.rs) · [Prepared send](../../crates/tickwire-transport/src/lib.rs)

The gateway synchronizes the recovery dependencies and admission in the WAL. A pending commit keeps the reservation. At first write, the sender checks the current grant against the same admission. A matching retry returns the stored status. A conflicting intent halts the writer. Order reports and trades produce separate journal writes. Storage synchronization, TLS readiness and queue wait are separate intervals to measure when profiling order latency. [Account gateway](../../crates/tickwire-account-gateway/src/lib.rs) · [Account journal](../../crates/tickwire-account-journal/src/lib.rs)

## Protocol references

Bybit documents the signed payload, create and cancel acknowledgement and the recent-order lookup. [Authentication](https://bybit-exchange.github.io/docs/v5/guide) · [Place Order](https://bybit-exchange.github.io/docs/v5/order/create-order) · [Cancel Order](https://bybit-exchange.github.io/docs/v5/order/cancel-order) · [Get Open and Closed Orders](https://bybit-exchange.github.io/docs/v5/order/open-order)

Bybit supplies individual trade amounts, fees, opaque continuation cursors and wallet account modes. [Private execution stream](https://bybit-exchange.github.io/docs/v5/websocket/private/execution) · [Trade History](https://bybit-exchange.github.io/docs/v5/order/execution) · [Wallet Balance](https://bybit-exchange.github.io/docs/v5/account/wallet-balance)

OKX documents the exact query path in the signature, demo mode, the synchronous order and cancel response, and REST versus private stream fill fields. [REST Authentication](https://www.okx.com/docs-v5/en/#overview-rest-authentication) · [Place Order](https://www.okx.com/docs-v5/en/#order-book-trading-trade-post-place-order) · [Get Order Details](https://www.okx.com/docs-v5/en/#order-book-trading-trade-get-order-details) · [Orders Channel](https://www.okx.com/docs-v5/en/#order-book-trading-trade-ws-order-channel) · [Get Transaction Details](https://www.okx.com/docs-v5/en/#order-book-trading-trade-get-transaction-details-last-3-days)
