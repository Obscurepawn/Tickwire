# Decode Binance depth and private events

`tickwire-binance::feed` and `events` decode complete HTTP bodies or WebSocket messages. The caller supplies the product, catalog and source binding. Decoding a market or private message does not alter an account, grant risk or establish continuity. [Feed decoder](../../crates/tickwire-binance/src/feed.rs) · [Private events](../../crates/tickwire-binance/src/events.rs)

| Input | Output and rejection conditions |
| --- | --- |
| Spot/USD-M exchangeInfo | Retain symbol, product, trading status and exact grid filters. Preserve unrecognized statuses/filters. Reject duplicate symbols or filters and excessive input. A coin-margined instrument cannot be classified as USD-M. |
| REST depth | Bind a symbol-free response to the request context and retain `lastUpdateId`. Both sides have limited coverage. Reject off-grid, oversized, crossed or improperly sorted input as a whole. |
| depthUpdate | Decode `U/u`, and `pu` for USD-M. Quantity zero deletes a level. Check combined-stream identity and reject partial, RPI or coin-margined streams. The book owns the product-specific sequence and duplicate-price rules. |
| executionReport / ORDER_TRADE_UPDATE | Retain native IDs, order values, quoted amounts, commissions and PnL. Preserve unknown statuses and distinguish omitted fields from zero. |
| `bind_order` | Match the original canonical command, persistent order mapping, account, side and asset versions. Modified order quantity/price remains observed data. An unresolved order or status requires correlation or reconciliation. |
| Account, list and algo events | Balances and positions are sparse patches. Missing B or P leaves that field unspecified. A list/algo terminal event does not by itself prove a child fill. Preserve unsupported event kinds as other observations. |

Parse ordinary decimal strings without floating point. An amount must fit the asset's scale. A positive commission becomes a negative cash flow. Negative commission is a rebate. Keep USD-M PnL separate in the settlement asset. A missing quoted amount cannot be synthesized from price and quantity.

Use the full external nonzero `u64` instrument ID for IPC, logs and economic identity. A separate local `BookKey` binds nonzero venue/source and an owner-assigned `u32` slot, which may be zero. Never derive that slot by truncating the external ID. [Instrument index](instrument-index.md)

## Reusable depth storage

`DepthWorkspace::new(max_total_levels)` reserves capacity for **both sides combined**, from 1 to 5000 updates. `decode_diff_into` and `decode_sbe_diff_into` return a view borrowing that workspace until the next exclusive decode. A failed decode has no observable partial result. The older `decode_diff::<N>` and `decode_sbe_diff::<N>` retain their per-side bounds. The shared-memory publisher uses a combined limit of 4096.

The JSON scanner handles escaped known and unknown fields and nested ignored values. Limit nested containers to 128 levels and decoded decimal text to 80 bytes. Reject malformed UTF-8, escapes, numbers or structure. On healthy JSON/SBE workspace decodes, allocation tests cover a previously reserved workspace and a 2 MiB stack. Metadata and private events may use bounded vectors and owned strings. [Workspace regression tests](../../crates/tickwire-binance/tests/depth_workspace.rs)

```sh
cargo test --locked -p tickwire-binance --test feed --test depth_workspace --test market_server
cargo test --locked -p tickwire-binance --test private_events --test private_structures --test private_server
```

The TLS fixtures pass framed market data to the real book and private facts to canonical event binding. Network subscription, snapshot coordination, durable accounting and reconciliation are separate state owners.
