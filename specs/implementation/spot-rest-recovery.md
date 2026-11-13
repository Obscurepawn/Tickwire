# Native REST Evidence for Cash Account Recovery

When a Bybit V5 or OKX V5 private WebSocket disconnects or encounters an ambiguous frame, `RestReader` (`tickwire-spot-execution`) and the transcript proof verifier (`tickwire-spot-runner`) reconstruct a complete, auditable view of the venue's cash trading wallet, standard spot orders, individual fills, and cash-flow bills over HTTPS. Every HTTP call connects to a pinned official venue authority using bounded body decoding and an exchange-calibrated UTC clock sample (refreshed automatically once older than 30 seconds), and every scan begins and ends with an authenticated `verify_uid` check. While the local journal's numeric `account` ID and the exchange's `expected_uid` string may differ in format, deployment genesis binds them permanently to the state directory.

## Venue Cash-Account Coverage Matrix

| Audit Dimension | Bybit V5 Contract | OKX V5 Contract |
| --- | --- | --- |
| Account profile | UTA 2.0 or 2.0 Pro with regular margin mode and spot borrowing disabled. Queries all linear settlement currencies from standard and pre-market instrument endpoints plus inverse and options; any open order or position across linear, inverse, or options halts recovery. | Spot account mode (`acctLv=1`). Queries pending spot algorithms, active spot grids, and recurring-buy bots; any order carrying a contingent leg or active bot halts recovery. |
| Wallet invariants | Per returned currency requires non-negative cash, explained spot locks, zero borrowing, and `equity == walletBalance`; derives spendable cash as `walletBalance - spot_orders_locked`. | Requires `availBal + frozenBal == cashBal` and `eq == cashBal` (accepting an explicitly empty liability string alongside `"0"` in verified spot mode); nonzero debt or non-cash encumbrances halt recovery. |
| Order pagination | Paginates account-wide spot open orders and order history following every native continuation cursor; conditional spot orders surfaced by the default filter are rejected under the cash profile. | Paginates all standard pending spot orders and order history (`instType=SPOT`) following the last order ID until an explicitly empty page—even after a short page (conditional algos reside on separate endpoints). |
| Retention bounds | Scan `start_ms` must be `< 24 hours` before sampled venue time; canceled, rejected, and deactivated orders expire sooner than filled orders. | Scan `start_ms` must be `< 2 hours` before sampled venue time; unfilled canceled orders expire after 2 hours, and history filters by order **creation** time. |
| Fill economics | Execution ID, order ID, client ID, quantity, price, reported quote amount, execution time, signed fee, and fee currency. Positive execution fees debit cash; unsupported regional fees halt recovery. | Trade ID, bill ID, order ID, client ID, quantity, price, `fillTime`, and per-trade fee/currency. Negative native fees debit cash; pagination and window bounds use bill ID and processing timestamp `ts`, whereas `Account` fill booking records `fillTime`. |
| Cash-bill parity | Paginates the authenticated transaction log; every trade bill must match a returned managed fill via unique composite key `(symbol, orderId, tradeId, currency, id)`. | Paginates trading-account bills and requires matching spot trades; currency transfers and unattributed balance movements—including net-zero offsetting flows—halt recovery. |

### Targeted Lookups, Execution Terms, and Composite Bill Keys

Every known unresolved local order (`OrderLookup`) is queried individually by symbol and `ClientId` even if it was created prior to the scan's historical window (with Bybit falling back from open orders to order history when realtime lookup returns empty). Missing, ambiguous, or mismatched order rows prevent recovery, and request/cancel acknowledgements never substitute for observed order state. Every managed limit order must report the exact execution terms (`GTC`, `IOC`, `FOK`, or post-only) admitted in its `CanonicalCommand`—carried in `timeInForce` on Bybit and `ordType` on OKX.

On Bybit spot, multiple transaction-log rows can correspond to different currency legs of a single trade and share one native log `id` (Bybit's own documentation labels `id` unique yet illustrates one `id` shared between a linear settlement and a linear trade without addressing spot legs). Keying rows by `(symbol, orderId, tradeId, currency, id)` accepts distinct base and quote currency legs of one fill while halting recovery if the identical five-tuple repeats within or across pages.

### Operational Exclusions: Exclusive Ownership and Event Contracts

Point-in-time REST pagination cannot prevent an external credential holder from enabling a trading bot after a scan completes; deployments must dedicate an exclusive UID with no external order writers or non-spot products. In particular, **event contracts lie outside the supported cash profile** on both venues:

* **Bybit**: Event-order and event-position endpoints are restricted to Event Contract maker accounts, and neither `/v5/user/apikey-info` nor `/v5/account/info` exposes an account-wide event-exposure flag. While an event cash transaction inside the scanned window halts recovery, an older unsettled event position would not appear in recent logs.
* **OKX**: `acctLv=1` establishes spot mode, and the scanner inventories `instType=SPOT` rather than `EVENTS`, where pre-existing event contracts could settle cash later.

Operators must verify at genesis that the dedicated UID holds zero event orders or positions and keep event trading disabled.

## Core Invariants, Transcript Replay, and Resource Bounds

* **Credential-scrubbed, deterministic transcript replay**: Each scan serializes its exact HTTP request paths, status codes, and response bytes—first reducing UID and account-config responses to identity fields so Bybit's `apikey-info` response never persists raw API keys into recovery artifacts. Offline and startup replay reconstructs every expected request path and continuation cursor from the stored responses and re-verifies windows, account profile, target order terms, asset totals, per-trade charges, and cash-bill parity, rejecting missing, duplicate, or surplus pages.
* **Bracketed dual-scan reactivation**: `tickwire-spot-runner` requires two consecutive transcripts sharing `start_ms`, having strictly increasing `end_ms` timestamps, producing identical 32-byte economic `fingerprint`s, and spanning an unmoved private-stream cut. On restart, `AccountJournal` re-verifies both archived transcripts and requires a newly approved `writer_epoch`.

| Scanner Resource | Hard Bound | Failure Behavior |
| --- | --- | --- |
| Total HTTP responses per scan | `512` responses | Exceeding any response, byte, page, timeout, retention, or rate-limit bound aborts the scan and leaves `Account` frozen. |
| Single native response body | `2 MiB` | Checked during bounded HTTP read prior to JSON parsing. |
| Retained raw response bytes | `12 MiB` (`24 MiB` serialized transcript) | Bounds peak heap footprint on the recovery worker thread. |
| Standard order / fill pagination | `128` pages per collection | Prevents runaway cursor loops. |
| Explicit local `OrderLookup`s | `128` lookups per scan | Caps active unresolved order count recoverable in one pass. |

## Performance and Cost

Scanning runs off the trading thread on a dedicated recovery worker. Each HTTP `GET` currently performs DNS resolution, a fresh TCP and TLS handshake, JSON parsing, and bounded transcript buffering within the caps above; those limits govern the maximum number of simultaneously active orders and recent fills a deployment can recover within venue retention windows.

## Verification and References

```sh
cargo test --locked -p tickwire-spot-execution --test recovery
cargo test --locked -p tickwire-spot-runner --test reconcile_process
```

On Linux hosts with `glibc`, a C compiler, and Python 3, `reconcile_process` intercepts the official Bybit and OKX REST and WebSocket hostnames via a per-process resolver against a loopback TLS exchange—exercising journal reopen, private connection epoch rollover, signed limit-buy submission, fill-and-fee accounting, stream disconnect, and full dual-scan re-qualification.

* Implementation modules: [RestReader and RestEvidence](../../crates/tickwire-spot-execution/src/recovery.rs), [collection and transcript replay](../../crates/tickwire-spot-execution/src/recovery/collect.rs), [order and fill row parsers](../../crates/tickwire-spot-execution/src/recovery/rows.rs), [account profile checks](../../crates/tickwire-spot-execution/src/recovery/profile.rs), [cash-bill matching](../../crates/tickwire-spot-execution/src/recovery/cash_flows.rs), [dual-scan proof verifier](../../crates/tickwire-spot-runner/src/reconcile/proof.rs), [background scan worker](../../crates/tickwire-spot-runner/src/live/scan.rs)
* Test suites and related specs: [recovery transcript tests](../../crates/tickwire-spot-execution/tests/recovery.rs), [loopback process test](../../crates/tickwire-spot-runner/tests/reconcile_process.rs), [Bybit and OKX spot execution](spot-execution.md)
* Bybit V5 references: [API Key Info](https://bybit-exchange.github.io/docs/v5/user/apikey-info), [Account Info](https://bybit-exchange.github.io/docs/v5/account/account-info), [Open Orders](https://bybit-exchange.github.io/docs/v5/order/open-order), [Order History](https://bybit-exchange.github.io/docs/v5/order/order-list), [Trade History](https://bybit-exchange.github.io/docs/v5/order/execution), [Wallet Balance](https://bybit-exchange.github.io/docs/v5/account/wallet-balance), [Transaction Log](https://bybit-exchange.github.io/docs/v5/account/transaction-log), [Event Open Orders](https://bybit-exchange.github.io/docs/v5/event/trade/open-order), [Event Positions](https://bybit-exchange.github.io/docs/v5/event/trade/position)
* OKX V5 references: [Account Configuration](https://www.okx.com/docs-v5/en/#trading-account-rest-api-get-account-configuration), [Order History (Last 7 Days)](https://www.okx.com/docs-v5/en/#order-book-trading-trade-get-order-history-last-7-days), [Transaction Details (Last 3 Days)](https://www.okx.com/docs-v5/en/#order-book-trading-trade-get-transaction-details-last-3-days), [Account Bills (Last 7 Days)](https://www.okx.com/docs-v5/en/#trading-account-rest-api-get-bills-details-last-7-days), [Event Contract Settlement](https://www.okx.com/en-us/help/how-to-trade-event-contracts-and-settlement-logic)
