# `tickwire-binance-execution` Engineering Guide (`AGENTS.md`)

Governing specification: [`specs/implementation/binance-execution.md`](../../specs/implementation/binance-execution.md)

## Ownership and Invariants

1. **Pure Stateless ORS Execution Assembly (Zero Local WAL, Thread-Safe `Send + Sync`)**:
   - `tickwire-binance-execution` builds directly on `tickwire-binance-account`, `tickwire-account`, `tickwire-transport`, and `tickwire-binance-quota` with zero local disk persistence or legacy gateway dependencies.
   - Coordinates [`tickwire_binance_account::BinanceSession`] (`Send + Sync`), [`tickwire_binance_account::BinanceConnectionSet`], [`tickwire_account::OrderManager`], [`tickwire_binance_quota::SharedBinanceOrsQuota`], [`tickwire_transport::AsyncRestQuerier`] (`BinanceRestRequestBuilder`), and [`warmup::BinanceHotPathWarmer`] across Spot, USD-M, Coin-M, and European Options.
2. **Thread & Lock Discipline**:
   - `#![forbid(unsafe_code)]`; zero floating-point arithmetic (`f32`/`f64`) anywhere in execution, warmup, or reconciliation logic.
   - `REQ` calls `&BinanceSession` directly (`submit_new`, `submit_cancel`, `submit_modify`) without touching `BinanceConnectionSet`.
   - `RESP` owns `BinanceExecutionEngine` (`BinanceConnectionSet`), reaps disconnected links into `BinanceSession::on_disconnected`, pumps due `OrsQueryDesk` tasks into `AsyncRestQuerier`, and applies `RestReply` completions via `BinanceExecutionEngine::apply_rest_reply`.
   - `BinanceRestRequestBuilder` signs `GET /{api,fapi,dapi,eapi}/v*/order` (`recvWindow=60000`), `Account + OpenOrders`, and `POST`/`PUT /{fapi,dapi,eapi}/v1/listenKey` requests on `"tw-rest-query"` (`housekeeping_cpus`).
3. **Side-Effect-Free Hot-Path Warmup (`src/warmup.rs`)**:
   - `BinanceHotPathWarmer` warms instruction/data caches, `OrsStpBook::self_cross`, `BinanceMsgParser`, and loopback TLS framing across `Product::{Spot, UsdM, CoinM, Options}` without mutating live order slots or touching exchange sockets.
