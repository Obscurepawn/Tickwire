//! Unified stateless full-market Binance ORS execution runtime (`tickwire-binance-execution`)
//! across Spot, USD-M, Coin-M, and European Options (`#![forbid(unsafe_code)]`).
//!
//! # Architecture & State Ownership
//! - **Pure Stateless 3-Process Topology (Zero Local WAL / Thread-Safe `Send + Sync`)**: Built directly on
//!   [`tickwire_binance_account::BinanceSession`], [`tickwire_account::OrderManager`],
//!   [`tickwire_transport::rest_querier::AsyncRestQuerier`], and [`tickwire_binance_quota::SharedBinanceOrsQuota`]
//!   with zero local disk journals or legacy gateway dependencies.
//! - **Disjoint Thread Isolation**:
//!   * `REQ` (`req_cpu`): calls `&BinanceSession::submit_new` / `submit_cancel` / `submit_modify`.
//!   * `RESP` (`resp_cpu`): owns [`BinanceExecutionEngine`] (`BinanceConnectionSet`), drives socket
//!     reads into `BinanceSession::on_message`, reaps disconnects into `on_disconnected`, drains
//!     due `OrsQueryDesk` tasks into `AsyncRestQuerier`, and applies `RestReply`s via
//!     `apply_rest_reply`.
//!   * `housekeeping_cpus`: runs `Dialer` and `AsyncRestQuerier` (`BinanceRestRequestBuilder`).

#![forbid(unsafe_code)]

pub mod warmup;

use std::sync::Arc;

use tickwire_account::{DueQueryAction, OrderManager};
use tickwire_binance_quota::SharedBinanceOrsQuota;
use tickwire_transport::mio::Waker;
use tickwire_transport::{
    Error as TransportError,
    latency_tracer::LatencyTracer,
    rest_querier::{
        AsyncRestQuerier, BuiltRequest, RestReply, RestRequestBuilder, RestStatus, RestTask,
        query_kind,
    },
};

pub use tickwire_binance_account::{
    BinanceAccountSpec, BinanceAssetBinding, BinanceBootstrapCatalog, BinanceBootstrapError,
    BinanceCancelOutcome, BinanceConfigError, BinanceConnectionSet, BinanceEventSink,
    BinanceLoginReply, BinanceMarketAuth, BinanceMarketConfig, BinanceModifyOutcome,
    BinanceMsgParser, BinanceNewOrderIntent, BinanceParsedFrame, BinanceRateLimitRow,
    BinanceRequestKind, BinanceRequestRef, BinanceResolvedMarketUrls, BinanceRestCall,
    BinanceSession, BinanceSessionError, BinanceStatusApplyOutcome, BinanceSubmitOutcome,
    BinanceSymbolBinding, BinanceUrlId, BinanceVenueMarket, DEFAULT_ORDER_RECV_WINDOW_MS,
    DEFAULT_STATUS_QUERY_DELAY_MS, DEFAULT_WS_PING_PERIOD_S, ExchRateLimitNotice,
    LISTEN_KEY_REFRESH_S, ReapedOrderLink, STATUS_QUERY_RECV_WINDOW_MS, UrlRole,
    resolve_market_urls,
};
pub use warmup::{
    BinanceHotPathWarmer, DecodedBinanceSampleBbo, decode_sample_binance_book_ticker,
};

const CANCEL_FLAG_BIT: u64 = 1u64 << 63;

/// Encodes `(ors_order_id, BinanceRequestKind)` into a `RestTask::key` for `STATUS_ASK`.
#[inline]
pub const fn pack_status_task_key(ors_order_id: u64, cancel_origin: bool) -> u64 {
    if cancel_origin {
        ors_order_id | CANCEL_FLAG_BIT
    } else {
        ors_order_id & !CANCEL_FLAG_BIT
    }
}

/// Unpacks a `STATUS_ASK` `RestTask::key` into `(ors_order_id, BinanceRequestKind)`.
#[inline]
pub const fn unpack_status_task_key(task_key: u64) -> (u64, BinanceRequestKind) {
    let ors_order_id = task_key & !CANCEL_FLAG_BIT;
    let kind = if (task_key & CANCEL_FLAG_BIT) != 0 {
        BinanceRequestKind::Cancel
    } else {
        BinanceRequestKind::Place
    };
    (ors_order_id, kind)
}

/// `Send + 'static` [`RestRequestBuilder`] adapter signing `STATUS_ASK` (`recvWindow=60000`) and
/// `listenKey` (`POST` / `PUT`) requests on `"tw-rest-query"` (`housekeeping_cpus`) using
/// [`BinanceSession`]'s dedicated `rest_query_signer`.
pub struct BinanceRestRequestBuilder<const CHECK_STP: bool = true> {
    session: Arc<BinanceSession<CHECK_STP>>,
    market: BinanceVenueMarket,
    wall_clock_ms: Box<dyn FnMut() -> u64 + Send>,
}

impl<const CHECK_STP: bool> BinanceRestRequestBuilder<CHECK_STP> {
    pub fn new(
        session: Arc<BinanceSession<CHECK_STP>>,
        market: BinanceVenueMarket,
        wall_clock_ms: impl FnMut() -> u64 + Send + 'static,
    ) -> Self {
        Self {
            session,
            market,
            wall_clock_ms: Box::new(wall_clock_ms),
        }
    }
}

impl<const CHECK_STP: bool> RestRequestBuilder for BinanceRestRequestBuilder<CHECK_STP> {
    fn build_request(
        &mut self,
        task: &RestTask,
        _host: &str,
        _scratch: &mut Vec<u8>,
    ) -> Result<Option<BuiltRequest>, TransportError> {
        let wall_ms = (self.wall_clock_ms)();
        let call_opt = match task.kind {
            query_kind::LISTEN_KEY_CREATE => self
                .session
                .stream_token_query(self.market, false)
                .map_err(|_| TransportError::Protocol("stream_token_query create"))?,
            query_kind::LISTEN_KEY_KEEPALIVE => self
                .session
                .stream_token_query(self.market, true)
                .map_err(|_| TransportError::Protocol("stream_token_query keepalive"))?,
            query_kind::STATUS_ASK => {
                let (ors_order_id, kind) = unpack_status_task_key(task.key);
                match self.session.order_status_query(ors_order_id, kind, wall_ms) {
                    Ok(call) => Some(call),
                    Err(BinanceSessionError::OrderNotFound(_)) => None,
                    Err(_) => return Err(TransportError::Protocol("order_status_query failed")),
                }
            }
            query_kind::ACCOUNT_SNAPSHOT => self
                .session
                .account_bootstrap_queries(wall_ms)
                .map_err(|_| TransportError::Protocol("account_bootstrap_queries"))?
                .into_iter()
                .find(|c| c.market == self.market),
            query_kind::OPEN_ORDERS_SNAPSHOT => self
                .session
                .open_orders_queries(wall_ms)
                .map_err(|_| TransportError::Protocol("open_orders_queries"))?
                .into_iter()
                .find(|c| c.market == self.market),
            _ => None,
        };

        let Some(call) = call_opt else {
            return Ok(None);
        };
        Ok(Some(BuiltRequest {
            method: String::from(call.method),
            path: call.target,
            headers: vec![(String::from("X-MBX-APIKEY"), call.api_key)],
            body: Vec::new(),
        }))
    }
}

/// `RESP`-thread execution coordinator pairing an `Arc<BinanceSession<CHECK_STP>>` (shared with
/// `REQ`, `Dialer`, and `AsyncRestQuerier`) with its per-market [`BinanceConnectionSet`].
#[derive(Debug)]
pub struct BinanceExecutionEngine<const CHECK_STP: bool = true> {
    session: Arc<BinanceSession<CHECK_STP>>,
    connections: BinanceConnectionSet,
}

impl<const CHECK_STP: bool> BinanceExecutionEngine<CHECK_STP> {
    pub fn configure(
        spec: BinanceAccountSpec,
        order_manager: Arc<OrderManager<CHECK_STP>>,
        quota: SharedBinanceOrsQuota,
        catalog: Arc<BinanceBootstrapCatalog>,
        resp_waker: Option<Arc<Waker>>,
        tracer: Option<Arc<LatencyTracer>>,
    ) -> Result<Self, BinanceSessionError> {
        let (session, connections) =
            BinanceSession::configure(spec, order_manager, quota, catalog, resp_waker, tracer)?;
        Ok(Self {
            session,
            connections,
        })
    }

    #[inline]
    pub fn session(&self) -> &Arc<BinanceSession<CHECK_STP>> {
        &self.session
    }

    #[inline]
    pub fn connections(&self) -> &BinanceConnectionSet {
        &self.connections
    }

    #[inline]
    pub fn connections_mut(&mut self) -> &mut BinanceConnectionSet {
        &mut self.connections
    }

    /// Reaps any disconnected order/stream links and routes in-flight Options `HttpsLink` requests
    /// into [`BinanceSession::on_disconnected`].
    pub fn reap_disconnects(
        &mut self,
        now_mono_ns: u64,
        sink: &mut impl BinanceEventSink,
    ) -> Result<usize, BinanceSessionError> {
        let mut reaped = Vec::new();
        let n = self
            .connections
            .reap_close_pending(now_mono_ns, &mut reaped)?;
        for link in reaped {
            self.session
                .on_disconnected(link.conn_id, link.inflight, now_mono_ns, sink);
        }
        Ok(n)
    }

    /// Enqueues a `listenKey` create (`keepalive == false`) or keepalive (`keepalive == true`)
    /// task onto `querier` (`13.9`).
    pub fn enqueue_listen_key_task(
        &self,
        market: BinanceVenueMarket,
        keepalive: bool,
        querier: &AsyncRestQuerier,
    ) -> bool {
        if !self.session.needs_stream_token(market) {
            return false;
        }
        let kind = if keepalive {
            query_kind::LISTEN_KEY_KEEPALIVE
        } else {
            query_kind::LISTEN_KEY_CREATE
        };
        querier.enqueue(RestTask::cold_snapshot(kind, market as u64))
    }

    /// Polls due `OrsQueryDesk` actions on `RESP`, enqueuing `StatusAsk` tasks (both `Place` and
    /// exponential-backoff `Cancel` asks) onto the matching market's `AsyncRestQuerier`.
    pub fn pump_due_queries<'a>(
        &self,
        now_mono_ns: u64,
        mut querier_for_market: impl FnMut(BinanceVenueMarket) -> Option<&'a AsyncRestQuerier>,
    ) -> usize {
        let mut due_buf = [DueQueryAction::StatusAsk {
            ors_order_id: 0,
            account_ref: 0,
            symbol_id: 0,
            attempt: 0,
            cancel_after: false,
        }; 32];
        let om = self.session.order_manager();
        let n = om.query_desk().poll_due(
            self.session.account_ref(),
            now_mono_ns,
            &mut due_buf,
            |s_idx, oid| om.is_slot_live(s_idx, oid),
        );

        let mut handled = 0usize;
        for action in due_buf.iter().copied().take(n) {
            let (ors_order_id, symbol_id, cancel_origin) = match action {
                DueQueryAction::StatusAsk {
                    ors_order_id,
                    symbol_id,
                    cancel_after,
                    ..
                } => (ors_order_id, symbol_id, cancel_after),
                DueQueryAction::RetryCancel {
                    ors_order_id,
                    symbol_id,
                    ..
                } => (ors_order_id, symbol_id, true),
            };
            let Some(binding) = self.session.catalog().symbol(symbol_id) else {
                continue;
            };
            if let Some(q) = querier_for_market(binding.market) {
                let key = pack_status_task_key(ors_order_id, cancel_origin);
                if q.enqueue(RestTask::status_ask(key, 1, 0, 0)) {
                    handled += 1;
                }
            }
        }
        handled
    }

    /// Applies one completed [`RestReply`] drained from `AsyncRestQuerier` on `RESP`.
    pub fn apply_rest_reply(
        &self,
        market: BinanceVenueMarket,
        reply: &RestReply,
        now_mono_ns: u64,
        sink: &mut impl BinanceEventSink,
    ) -> Result<(), BinanceSessionError> {
        match reply.task.kind {
            query_kind::LISTEN_KEY_CREATE | query_kind::LISTEN_KEY_KEEPALIVE => {
                let _ = self
                    .session
                    .on_stream_token_reply(market, reply.http_code, &reply.body)?;
                Ok(())
            }
            query_kind::STATUS_ASK => {
                let (ors_order_id, kind) = unpack_status_task_key(reply.task.key);
                let answered = matches!(reply.status, RestStatus::Ok | RestStatus::HttpError)
                    && reply.http_code > 0;
                let _ = self.session.apply_status(
                    ors_order_id,
                    kind,
                    answered,
                    reply.http_code,
                    &reply.body,
                    now_mono_ns,
                    sink,
                )?;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
