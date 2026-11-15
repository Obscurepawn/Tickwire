//! Unified `Send + Sync` full-market Binance ORS account session (`BinanceSession`) across Spot
//! (`/api/v3`, `ws-api`), USD-M (`/fapi/v1`, `ws-fapi`), Coin-M (`/dapi/v1`, `ws-dapi`), and
//! European Options (`/eapi/v1`, `fstream/private/ws`) (`#![forbid(unsafe_code)]`).
//!
//! # Architecture & State Ownership
//! - **Zero Local WAL / Thread-Safe `Send + Sync` State**: Built directly on `tickwire-account` (`OrderManager`,
//!   `OrsQueryDesk`, `FillRecordPool`, `UtaWalletLedger`), `tickwire-transport` (`JsonScan`,
//!   `LinkOps`, `UrlConnections`, `PublishedRoutes`, `GuardedValue`, `Dialer`,
//!   `AsyncRestQuerier`), and `tickwire-binance-quota` (`SharedBinanceOrsQuota`) with zero coarse
//!   process-wide mutexes and zero local disk journals.
//! - **Disjoint Thread Isolation**: `BinanceSession` is `Send + Sync` so `REQ` (`submit_new`,
//!   `submit_cancel`, `submit_modify`), `RESP` (`on_message`, `on_disconnected`, `apply_status`),
//!   `Dialer` (`connection_url`, `encode_login` with its dedicated `Ed25519` signing copy and
//!   `GuardedValue` `listenKey`), and `AsyncRestQuerier` (`order_status_query`,
//!   `stream_token_query`, cold bootstrap) run concurrently across isolated cores.
//! - **Zero-Allocation Single-Pass `BinanceMsgParser`**: Scans synonymous WebSocket API,
//!   user-data stream (`executionReport`, `ORDER_TRADE_UPDATE`, `TRADE_LITE`, `listenKeyExpired`),
//!   and Options HTTP payloads in one pass using `JsonScan`, extracting order fields, up to 4
//!   `rateLimits[]` rows, and error `code`/`msg` without heap allocation.

#![forbid(unsafe_code)]

pub mod bootstrap;
pub mod msg_parser;
pub mod session;
pub mod urls;

pub use bootstrap::{
    BinanceAssetBinding, BinanceBootstrapCatalog, BinanceBootstrapError, BinanceSymbolBinding,
    bootstrap_market_into_order_manager, build_market_bootstrap_snapshot, parse_account_snapshot,
    parse_open_orders_snapshot, parse_scaled_i128,
};
pub use msg_parser::{
    BinanceMsgParser, BinanceParsedFrame, BinanceRateLimitRow, BinanceRequestKind,
    MAX_BINANCE_RATE_LIMITS, decode_ws_request_id, encode_ws_request_id, pack_ws_reply_id,
    parse_step_i64, unpack_ws_reply_id,
};
pub use session::{
    BinanceAccountSpec, BinanceCancelOutcome, BinanceConnectionSet, BinanceEventSink,
    BinanceLoginReply, BinanceMarketAuth, BinanceMarketConfig, BinanceModifyOutcome,
    BinanceNewOrderIntent, BinanceRequestRef, BinanceRestCall, BinanceSession, BinanceSessionError,
    BinanceStatusApplyOutcome, BinanceSubmitOutcome, DEFAULT_ORDER_RECV_WINDOW_MS,
    DEFAULT_STATUS_QUERY_DELAY_MS, DEFAULT_WS_PING_PERIOD_S, ExchRateLimitNotice,
    LISTEN_KEY_REFRESH_S, ReapedOrderLink, STATUS_QUERY_RECV_WINDOW_MS,
    unmask_ws_client_text_frame,
};
pub use tickwire_binance::full_market::BinanceVenueMarket;
pub use urls::{
    BinanceConfigError, BinanceResolvedMarketUrls, BinanceUrlId, UrlRole, resolve_market_urls,
};
