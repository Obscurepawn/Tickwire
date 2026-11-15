//! Venue-agnostic `VenueSession` protocol trait, `UrlRole`, `SessionMessageEffects`, and default
//! `REQ` outbound orchestration (`OrderManager` + `dry_run` + `PublishedRoutes::pick` +
//! single-pick `SharedRateLimiter` NACK admission + `LockedConnection::send_frame` with zero
//! spinlock overlap).

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use tickwire_account::{
    InlineAscii, OrderManager, OrsCancelDispatch, OrsCancelObservation, OrsCancelRequest,
    OrsExchangeBootstrapSnapshot, OrsFillObservation, OrsModifyDispatch, OrsModifyRequest,
    OrsNewOrderRequest, OrsOrderQuerySnapshot, OrsPlaceOutcome, OrsQueryOrigin, OrsVenueEvent,
};
use tickwire_account_gateway::AccountSpec;
use tickwire_ipc::ReportBody;
use tickwire_quota::rate_limiter::IpKey;
use tickwire_quota::shared::{
    InstrumentScope, OrsAccountQuota, OrsVenueProfile, RouteAdmissionReject, SharedRateLimiter,
    VenueRateLimitRow,
};
use tickwire_transport::links::{LinkKind, LockedConnection, RequestRef, SendOutcome};
use tickwire_transport::order_router::PublishedRoutes;
use tickwire_transport::rest_querier::{BuiltRequest, DialSecurity, RestReply, RestTask};
use tickwire_transport::{FixSessionConfig, WsLoginChallenge};
use tickwire_venue::{OrderRoute, Product, RefusalReason, Side, TerminalReason};

/// Role bitmask of a venue URL pool (`8.4 protocol.md`: `Order`, `Stream`, or combined).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct UrlRole(pub u8);

impl UrlRole {
    /// Outbound order / cancel / modify execution connection.
    pub const ORDER: Self = Self(1 << 0);
    /// Private execution-report / user-data push stream connection.
    pub const STREAM: Self = Self(1 << 1);
    /// Combined duplex order + execution stream connection (e.g., OKX/Bybit/Deribit WS).
    pub const ORDER_AND_STREAM: Self = Self(Self::ORDER.0 | Self::STREAM.0);

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    #[inline]
    pub const fn is_order(self) -> bool {
        (self.0 & Self::ORDER.0) != 0
    }

    #[inline]
    pub const fn is_stream(self) -> bool {
        (self.0 & Self::STREAM.0) != 0
    }
}

/// Kind of in-flight or queried request (`Place`, `Cancel`, `Modify`, `Other`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SessionRequestKind {
    Place,
    Cancel,
    Modify,
    Other,
}

/// Result of inspecting an inbound WebSocket authentication frame (`8.4 protocol.md`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoginReply {
    /// Authentication succeeded; connection transitions to `Authenticated` and replays early frames.
    Ok,
    /// Authentication rejected; retire connection and retry after backoff.
    Fail,
    /// Multi-step challenge frame; pass payload to [`VenueSession::login_challenge`].
    Other,
}

/// Static URL pool descriptor declared by a [`VenueSession`] for an [`AccountSpec`].
#[derive(Clone, Debug)]
pub struct SessionUrlPoolSpec {
    pub url_id: String,
    pub role: UrlRole,
    pub kind: LinkKind,
    /// `0` for shared UTA pools (OKX/Bybit); product bitmask (`Product::mask_bit()`) for multi-endpoint venues.
    pub product_mask: u16,
    pub host: String,
    pub port: u16,
    pub path: String,
    pub security: DialSecurity,
    pub initial_peers: Vec<SocketAddr>,
}

/// Resolved connection target for a single `Dialer` attempt (`VenueSession::connection_url`).
#[derive(Clone, Debug)]
pub struct SessionEndpoint {
    pub host: String,
    pub port: u16,
    pub path: String,
    pub security: DialSecurity,
    pub initial_peers: Vec<SocketAddr>,
}

/// Optional REST query host configuration for `AsyncRestQuerier` on an `AccountSession`.
#[derive(Clone, Debug)]
pub struct SessionRestEndpoint {
    pub url_id: Option<String>,
    pub host: String,
    pub port: u16,
    pub security: DialSecurity,
    pub initial_peers: Vec<SocketAddr>,
}

/// Owned inbound venue event emitted by [`VenueSession::on_message`] / [`VenueSession::on_disconnected`]
/// without borrowing the transient socket read buffer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionVenueEvent {
    FrontAck {
        ors_order_id: u64,
        venue_order_id: Option<InlineAscii<64>>,
        received_mono_ns: u64,
    },
    Ack {
        ors_order_id: u64,
        venue_order_id: Option<InlineAscii<64>>,
        ack_price_ticks: Option<i64>,
        ack_qty_lots: Option<i64>,
        received_mono_ns: u64,
    },
    ProjectedFill {
        ors_order_id: u64,
        cum_qty_lots: i64,
        price_ticks: i64,
        received_mono_ns: u64,
    },
    Fill(OrsFillObservation),
    Modified {
        ors_order_id: u64,
        price_ticks: i64,
        qty_lots: i64,
        venue_order_id: Option<InlineAscii<64>>,
        received_mono_ns: u64,
    },
    ModifyRejected {
        ors_order_id: u64,
        reason: TerminalReason,
        received_mono_ns: u64,
    },
    Canceled {
        ors_order_id: u64,
        cum_qty_lots: Option<i64>,
        error_code: Option<i64>,
        status_or_msg: InlineAscii<64>,
        stp_mode_or_reason: Option<InlineAscii<32>>,
        explicit_reason: Option<TerminalReason>,
        received_mono_ns: u64,
    },
    CancelRejected {
        ors_order_id: u64,
        error_code: Option<i64>,
        reason: TerminalReason,
        received_mono_ns: u64,
    },
    Rejected {
        ors_order_id: u64,
        error_code: Option<i64>,
        status_or_msg: InlineAscii<64>,
        explicit_reason: Option<TerminalReason>,
        received_mono_ns: u64,
    },
}

impl SessionVenueEvent {
    /// Borrows `self` as a zero-allocation [`OrsVenueEvent`] for [`OrderManager::apply_venue_event`].
    #[inline]
    pub fn as_ors_event(&self) -> OrsVenueEvent<'_> {
        match self {
            Self::FrontAck {
                ors_order_id,
                venue_order_id,
                received_mono_ns,
            } => OrsVenueEvent::FrontAck {
                ors_order_id: *ors_order_id,
                venue_order_id: venue_order_id.as_ref().map(InlineAscii::as_str),
                received_mono_ns: *received_mono_ns,
            },
            Self::Ack {
                ors_order_id,
                venue_order_id,
                ack_price_ticks,
                ack_qty_lots,
                received_mono_ns,
            } => OrsVenueEvent::Ack {
                ors_order_id: *ors_order_id,
                venue_order_id: venue_order_id.as_ref().map(InlineAscii::as_str),
                ack_price_ticks: *ack_price_ticks,
                ack_qty_lots: *ack_qty_lots,
                received_mono_ns: *received_mono_ns,
            },
            Self::ProjectedFill {
                ors_order_id,
                cum_qty_lots,
                price_ticks,
                received_mono_ns,
            } => OrsVenueEvent::ProjectedFill {
                ors_order_id: *ors_order_id,
                cum_qty_lots: *cum_qty_lots,
                price_ticks: *price_ticks,
                received_mono_ns: *received_mono_ns,
            },
            Self::Fill(obs) => OrsVenueEvent::Fill(*obs),
            Self::Modified {
                ors_order_id,
                price_ticks,
                qty_lots,
                venue_order_id,
                received_mono_ns,
            } => OrsVenueEvent::Modified {
                ors_order_id: *ors_order_id,
                price_ticks: *price_ticks,
                qty_lots: *qty_lots,
                venue_order_id: venue_order_id.as_ref().map(InlineAscii::as_str),
                received_mono_ns: *received_mono_ns,
            },
            Self::ModifyRejected {
                ors_order_id,
                reason,
                received_mono_ns,
            } => OrsVenueEvent::ModifyRejected {
                ors_order_id: *ors_order_id,
                reason: *reason,
                received_mono_ns: *received_mono_ns,
            },
            Self::Canceled {
                ors_order_id,
                cum_qty_lots,
                error_code,
                status_or_msg,
                stp_mode_or_reason,
                explicit_reason,
                received_mono_ns,
            } => OrsVenueEvent::Canceled(OrsCancelObservation {
                ors_order_id: *ors_order_id,
                cum_qty_lots: *cum_qty_lots,
                error_code: *error_code,
                status_or_msg: status_or_msg.as_str(),
                stp_mode_or_reason: stp_mode_or_reason.as_ref().map(InlineAscii::as_str),
                explicit_reason: *explicit_reason,
                received_mono_ns: *received_mono_ns,
            }),
            Self::CancelRejected {
                ors_order_id,
                error_code,
                reason,
                received_mono_ns,
            } => OrsVenueEvent::CancelRejected {
                ors_order_id: *ors_order_id,
                error_code: *error_code,
                reason: *reason,
                received_mono_ns: *received_mono_ns,
            },
            Self::Rejected {
                ors_order_id,
                error_code,
                status_or_msg,
                explicit_reason,
                received_mono_ns,
            } => OrsVenueEvent::Rejected {
                ors_order_id: *ors_order_id,
                error_code: *error_code,
                status_or_msg: status_or_msg.as_str(),
                explicit_reason: *explicit_reason,
                received_mono_ns: *received_mono_ns,
            },
        }
    }
}

/// Request emitted by `on_message` / `on_disconnected` to schedule per-order `OrsQueryDesk`
/// status reconciliation (`13.8`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryStatusRequest {
    pub ors_order_id: u64,
    pub origin: OrsQueryOrigin,
}

/// Batch of side-effects emitted by [`VenueSession::on_message`] or [`VenueSession::on_disconnected`]
/// on the `RESP` thread (`8.4`, `8.8`).
#[derive(Clone, Debug, Default)]
pub struct SessionMessageEffects {
    /// Order lifecycle updates applied through `OrderManager`.
    pub events: Vec<SessionVenueEvent>,
    /// Parsed venue `rateLimits[]` rows fed into `SharedRateLimiter::reconcile_venue_usage_batch`.
    pub rate_limits: Vec<VenueRateLimitRow>,
    /// Ambiguous orders requiring `OrsQueryDesk::schedule_status_ask`.
    pub query_status: Vec<QueryStatusRequest>,
    /// Direct strategy reports emitted by venue-managed sub-protocols (e.g. RFQ quote acks/nacks).
    pub direct_reports: Vec<ReportBody>,
    /// Set `true` when `listenKeyExpired` / `urlToken` expiry requires immediate stream-token rotation.
    pub stream_token_expired: bool,
    /// Set `true` on OKX/Bybit server maintenance `notice` (`8.8`) to unpublish this connection from
    /// active routing immediately, dial a replacement connection, and close once in-flight count hits `0`.
    pub graceful_drain_connection: bool,
}

impl SessionMessageEffects {
    pub fn clear(&mut self) {
        self.events.clear();
        self.rate_limits.clear();
        self.query_status.clear();
        self.direct_reports.clear();
        self.stream_token_expired = false;
        self.graceful_drain_connection = false;
    }
}

/// Outcome of [`VenueSession::apply_status`] for a single-order `StatusAsk` REST reply.
#[derive(Clone, Debug, Default)]
pub struct StatusReplyOutcome {
    /// Structured order snapshot when the venue answered conclusively.
    pub snapshot: Option<OwnedOrderQuerySnapshot>,
    /// Set `true` when the HTTP/REST call failed or remained inconclusive and should retry.
    pub retry_query: bool,
    /// Optional venue rate-limit rows parsed from HTTP response headers.
    pub rate_limits: Vec<VenueRateLimitRow>,
}

/// Owned counterpart of [`OrsOrderQuerySnapshot`] returned by [`VenueSession::apply_status`].
#[derive(Clone, Debug)]
pub struct OwnedOrderQuerySnapshot {
    pub ors_order_id: u64,
    pub account_ref: u8,
    pub symbol_id: u32,
    pub status: tickwire_account::OrsQueriedOrderStatus,
    pub venue_order_id: Option<InlineAscii<64>>,
    pub price_ticks: Option<i64>,
    pub orig_qty_lots: Option<i64>,
    pub cum_filled_lots: i64,
    pub avg_fill_price_ticks: Option<i64>,
    pub trades: Vec<OrsFillObservation>,
    pub received_mono_ns: u64,
}

impl OwnedOrderQuerySnapshot {
    pub fn as_borrowed(&self) -> OrsOrderQuerySnapshot<'_> {
        OrsOrderQuerySnapshot {
            ors_order_id: self.ors_order_id,
            account_ref: self.account_ref,
            symbol_id: self.symbol_id,
            status: self.status,
            venue_order_id: self.venue_order_id.as_ref().map(InlineAscii::as_str),
            price_ticks: self.price_ticks,
            orig_qty_lots: self.orig_qty_lots,
            cum_filled_lots: self.cum_filled_lots,
            avg_fill_price_ticks: self.avg_fill_price_ticks,
            trades: &self.trades,
            received_mono_ns: self.received_mono_ns,
        }
    }
}

/// Outbound RFQ quote intent passed to [`VenueSession::submit_quote`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RfqQuoteRequest {
    pub strat_id: u8,
    pub account_ref: u8,
    pub msg_seqnum: u64,
    pub order_seqnum: u64,
    pub ors_order_id: u64,
    pub symbol_id: u32,
    pub side: Side,
    pub price_ticks: i64,
    pub qty_lots: i64,
    pub is_modify: bool,
    pub now_mono_ns: u64,
}

/// RFQ quote cancellation intent passed to [`VenueSession::cancel_quotes`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RfqQuoteCancelRequest {
    /// `None` cancels across all strategies on this account (shutdown / `ALL_STRATS`).
    pub strat_id: Option<u8>,
    pub ors_order_id: Option<u64>,
    pub symbol_id: Option<u32>,
    pub now_mono_ns: u64,
}

/// Outcome of [`VenueSession::submit_new`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmitNewOutcome {
    /// Order validated and immediately released on the `dry_run` scratch slot with zero wire I/O.
    DryRunPassed {
        ors_order_id: u64,
        account_ref: u8,
        client_id: InlineAscii<64>,
    },
    /// Order frame written to `connection_id`.
    Dispatched {
        ors_order_id: u64,
        account_ref: u8,
        connection_id: u16,
        client_id: InlineAscii<64>,
    },
    /// Socket/TLS short write retired `connection_id` and scheduled `OrsQueryDesk` reconciliation.
    InDoubtScheduled {
        ors_order_id: u64,
        account_ref: u8,
        connection_id: u16,
        client_id: InlineAscii<64>,
    },
}

/// Outcome of [`VenueSession::submit_cancel`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmitCancelOutcome {
    /// Queued behind `PendingNew` (`cancel_on_ack`) or coalesced into `OrsQueryDesk` (`cancel_after`).
    Deferred(OrsCancelDispatch),
    /// Cancel frame written to `connection_id`.
    Dispatched {
        ors_order_id: u64,
        account_ref: u8,
        connection_id: u16,
    },
    /// Socket/TLS short write retired `connection_id` and scheduled `OrsQueryDesk` reconciliation.
    InDoubtScheduled {
        ors_order_id: u64,
        account_ref: u8,
        connection_id: u16,
    },
}

/// Outcome of [`VenueSession::submit_modify`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmitModifyOutcome {
    Dispatched {
        ors_order_id: u64,
        account_ref: u8,
        connection_id: u16,
    },
    InDoubtScheduled {
        ors_order_id: u64,
        account_ref: u8,
        connection_id: u16,
    },
}

/// Encoded wire frame metadata returned by `encode_new_order` / `encode_cancel_order` / `encode_modify_order`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EncodedOrderFrame {
    /// Number of bytes written into caller's `scratch` slice.
    pub frame_len: usize,
    /// WebSocket `reply_id` or FIX `MsgSeqNum (34)` (`0` for HTTP/1.1 `HttpsLink`).
    pub reply_or_seq_id: u64,
}

/// Read-only execution context for one `AccountSession` passed into [`VenueSession`] `REQ` hooks.
pub struct AccountSessionContext<'a> {
    pub spec: &'a AccountSpec,
    pub order_manager: &'a OrderManager<true>,
    pub published_routes: &'a PublishedRoutes,
    pub rate_limiter: &'a SharedRateLimiter,
    pub account_quota: OrsAccountQuota<'a>,
    connections: &'a [(u16, Option<IpAddr>, Arc<LockedConnection>)],
    last_conn_id: Option<u16>,
}

impl<'a> AccountSessionContext<'a> {
    #[inline]
    pub fn new(
        spec: &'a AccountSpec,
        order_manager: &'a OrderManager<true>,
        published_routes: &'a PublishedRoutes,
        rate_limiter: &'a SharedRateLimiter,
        account_quota: OrsAccountQuota<'a>,
        connections: &'a [(u16, Option<IpAddr>, Arc<LockedConnection>)],
        last_conn_id: Option<u16>,
    ) -> Self {
        Self {
            spec,
            order_manager,
            published_routes,
            rate_limiter,
            account_quota,
            connections,
            last_conn_id,
        }
    }

    #[inline]
    pub fn connection_handle(&self, conn_id: u16) -> Option<&Arc<LockedConnection>> {
        self.connections
            .iter()
            .find(|(id, _, _)| *id == conn_id)
            .map(|(_, _, handle)| handle)
    }

    #[inline]
    pub fn slot_local_ip(&self, conn_id: u16) -> Option<IpAddr> {
        self.connections
            .iter()
            .find(|(id, _, _)| *id == conn_id)
            .and_then(|(_, ip, _)| *ip)
    }

    #[inline]
    fn resolve_route_ip_key(&self, route: &OrderRoute) -> IpKey {
        if let Some(ip) = self.slot_local_ip(route.connection_id) {
            return IpKey::from_ip(ip);
        }
        if route.local_ip != 0 {
            return IpKey::from_v4(Ipv4Addr::from_bits(route.local_ip));
        }
        if let Some(ip) = self.spec.local_ip {
            return IpKey::from_ip(ip);
        }
        IpKey::NONE
    }

    /// Single-pick outbound order route + rate-limiter admission:
    /// picks **one** ready route matching `product`, spends against `SharedRateLimiter` using that
    /// picked connection's `local_ip`, and **never** retries another sibling connection on limit!
    pub fn pick_and_place_or_nack(
        &self,
        product: Product,
        scope: InstrumentScope,
        order_units: u32,
        ip_weight: u32,
        now_mono_ns: u64,
    ) -> Result<OrderRoute, RouteAdmissionReject> {
        let (route, _permit) = self.rate_limiter.pick_and_spend_or_nack(
            || {
                self.published_routes.pick(
                    self.spec.account_ref,
                    Some(product),
                    self.last_conn_id,
                    false,
                )
            },
            |route, buf| {
                let ip_key = self.resolve_route_ip_key(route);
                if ip_weight > 0 && ip_key.is_none() {
                    return Err(RouteAdmissionReject::MissingLocalIp {
                        conn_id: route.connection_id,
                    });
                }
                self.account_quota
                    .build_place_spends(ip_key, scope, order_units, ip_weight, buf)
                    .map_err(RouteAdmissionReject::RateLimitedNack)
            },
            now_mono_ns,
        )?;
        Ok(route)
    }

    /// Single-pick outbound cancel route + rate-limiter admission (strictly isolated from `ORDER`
    /// windows; never retries another connection on limit).
    pub fn pick_and_cancel_or_nack(
        &self,
        product: Option<Product>,
        scope: InstrumentScope,
        ip_weight: u32,
        now_mono_ns: u64,
    ) -> Result<OrderRoute, RouteAdmissionReject> {
        let (route, _permit) = self.rate_limiter.pick_and_spend_or_nack(
            || {
                self.published_routes
                    .pick(self.spec.account_ref, product, self.last_conn_id, false)
            },
            |route, buf| {
                let ip_key = self.resolve_route_ip_key(route);
                if ip_weight > 0 && ip_key.is_none() {
                    return Err(RouteAdmissionReject::MissingLocalIp {
                        conn_id: route.connection_id,
                    });
                }
                self.account_quota
                    .build_cancel_spends(ip_key, scope, ip_weight, buf)
                    .map_err(RouteAdmissionReject::RateLimitedNack)
            },
            now_mono_ns,
        )?;
        Ok(route)
    }
}

#[inline]
fn map_admission_reject(err: RouteAdmissionReject) -> RefusalReason {
    match err {
        RouteAdmissionReject::NoReadyConnection | RouteAdmissionReject::MissingLocalIp { .. } => {
            RefusalReason::MarketRejectMarketDown
        }
        RouteAdmissionReject::RateLimitedNack(_) => RefusalReason::RateLimited,
    }
}

/// Venue-agnostic session driver (`1 AccountSession = 1 Exchange UID`, `8.4 protocol.md`).
///
/// Generic ORS runtime code (`OrderRoutingSystem`, `OrsProcess`, `ReqWorker`, `RespWorker`)
/// interacts with exchanges exclusively through this trait (`Send + Sync`, zero coarse session
/// mutexes) and **never** names a concrete venue.
pub trait VenueSession: Send + Sync + 'static {
    /// Compiled rate-limiter profile for this session's venue.
    fn venue_profile(&self) -> OrsVenueProfile;

    /// Declares the persistent WS/HTTPS/FIX URL pools opened for `spec` (`[local_ip, count]`).
    fn url_pools(&self, spec: &AccountSpec) -> Result<Vec<SessionUrlPoolSpec>, String>;

    /// Optional REST query endpoint for cold/live `AsyncRestQuerier` (`OrsQueryDesk`, `listenKey`).
    fn rest_endpoint(&self, _spec: &AccountSpec) -> Option<SessionRestEndpoint> {
        None
    }

    /// Per-symbol / per-product rate-limit scope for `symbol_id`.
    fn instrument_scope(&self, symbol_id: u32, product: Product) -> InstrumentScope {
        let defs = self.venue_profile().defaults();
        if defs.per_symbol_order_cancel.is_some() {
            InstrumentScope::Symbol(u64::from(symbol_id).saturating_add(1))
        } else if defs.per_product_order_cancel.is_some() {
            InstrumentScope::Product(u64::from(product.code()))
        } else {
            InstrumentScope::None
        }
    }

    /// Outbound IP weight debited when placing/modifying an order (`defaults().default_ws_order_ip_weight`).
    fn place_ip_weight(&self, _product: Product) -> u32 {
        self.venue_profile().defaults().default_ws_order_ip_weight
    }

    /// Outbound IP weight debited when cancelling an order.
    fn cancel_ip_weight(&self, _product: Option<Product>) -> u32 {
        self.venue_profile().defaults().default_ws_order_ip_weight
    }

    /// Encodes a `NewOrder` wire frame into `scratch` without touching any spinlock.
    fn encode_new_order(
        &self,
        spec: &AccountSpec,
        req: &OrsNewOrderRequest,
        placed: &OrsPlaceOutcome,
        conn_id: u16,
        scratch: &mut [u8],
    ) -> Result<EncodedOrderFrame, RefusalReason>;

    /// Encodes a `Cancel` wire frame into `scratch` without touching any spinlock.
    fn encode_cancel_order(
        &self,
        spec: &AccountSpec,
        cancel: &OrsCancelDispatch,
        conn_id: u16,
        scratch: &mut [u8],
    ) -> Result<EncodedOrderFrame, RefusalReason>;

    /// Encodes an in-place `Modify` wire frame into `scratch` without touching any spinlock.
    fn encode_modify_order(
        &self,
        _spec: &AccountSpec,
        _modify: &OrsModifyDispatch,
        _client_id: &str,
        _venue_order_id: &str,
        _conn_id: u16,
        _scratch: &mut [u8],
    ) -> Result<EncodedOrderFrame, RefusalReason> {
        Err(RefusalReason::AbortedBeforeSend)
    }

    /// Default `REQ` `NewOrder` orchestration:
    /// 1. `OrderManager::place_order` (atomic slot claim + wallet hold + lock-free atomic BBO
    ///    `self_cross`; releases `LockedOrder` before returning).
    /// 2. `dry_run` short-circuit (`!outcome.should_send_to_venue`): warms up `encode_new_order`
    ///    into `scratch` and skips route rate-limiter spend and socket write (`10 §2.4`).
    /// 3. `AccountSessionContext::pick_and_place_or_nack`: single-picks one ready route matching
    ///    `OrderRoute.product_mask` and spends `SharedRateLimiter` on that socket's `local_ip`—
    ///    NACKing immediately without retrying another connection on limit!
    /// 4. `encode_new_order` + `LockedConnection::send_frame` with zero `Order` spinlock overlap.
    fn submit_new(
        &self,
        ctx: &AccountSessionContext<'_>,
        req: &OrsNewOrderRequest,
        scratch: &mut [u8],
    ) -> Result<SubmitNewOutcome, RefusalReason> {
        let outcome = ctx
            .order_manager
            .place_order(req)
            .map_err(|refusal| refusal.reason)?;

        if !outcome.should_send_to_venue {
            let _ = self.encode_new_order(ctx.spec, req, &outcome, 0, scratch);
            return Ok(SubmitNewOutcome::DryRunPassed {
                ors_order_id: outcome.ors_order_id,
                account_ref: outcome.account_ref,
                client_id: outcome.client_id,
            });
        }

        let scope = self.instrument_scope(req.symbol_id, outcome.product);
        let ip_weight = self.place_ip_weight(outcome.product);
        let route =
            match ctx.pick_and_place_or_nack(outcome.product, scope, 1, ip_weight, req.now_mono_ns)
            {
                Ok(r) => r,
                Err(err) => {
                    let reason = map_admission_reject(err);
                    let _ = ctx.order_manager.on_rejected(
                        outcome.ors_order_id,
                        None,
                        "LOCAL_ROUTE_OR_RATE_REJECT",
                        Some(reason),
                        req.now_mono_ns,
                    );
                    return Err(reason);
                }
            };

        let encoded =
            match self.encode_new_order(ctx.spec, req, &outcome, route.connection_id, scratch) {
                Ok(enc) if enc.frame_len <= scratch.len() => enc,
                Ok(_) => {
                    let _ = ctx.order_manager.on_rejected(
                        outcome.ors_order_id,
                        None,
                        "ENCODE_BOUNDS",
                        Some(TerminalReason::AbortedBeforeSend),
                        req.now_mono_ns,
                    );
                    return Err(RefusalReason::AbortedBeforeSend);
                }
                Err(reason) => {
                    let _ = ctx.order_manager.on_rejected(
                        outcome.ors_order_id,
                        None,
                        "ENCODE_REFUSED",
                        Some(reason),
                        req.now_mono_ns,
                    );
                    return Err(reason);
                }
            };

        let Some(conn) = ctx.connection_handle(route.connection_id) else {
            let _ = ctx.order_manager.on_rejected(
                outcome.ors_order_id,
                None,
                "MISSING_CONN",
                Some(TerminalReason::MarketRejectMarketDown),
                req.now_mono_ns,
            );
            return Err(RefusalReason::MarketRejectMarketDown);
        };

        let req_ref = RequestRef {
            order_key: outcome.ors_order_id,
            client_seq: req.order_seqnum,
            sent_tsc: 0,
            sent_mono_ns: req.now_mono_ns,
        };
        match conn.send_frame(
            &scratch[..encoded.frame_len],
            encoded.reply_or_seq_id,
            req_ref,
        ) {
            SendOutcome::Sent => Ok(SubmitNewOutcome::Dispatched {
                ors_order_id: outcome.ors_order_id,
                account_ref: outcome.account_ref,
                connection_id: route.connection_id,
                client_id: outcome.client_id,
            }),
            SendOutcome::ShortWriteRetired => {
                let due_ns = req
                    .now_mono_ns
                    .saturating_add(self.status_query_delay_ms().saturating_mul(1_000_000));
                let _ = ctx.order_manager.query_desk().schedule_status_ask(
                    outcome.account_ref,
                    outcome.ors_order_id,
                    outcome.slot_idx,
                    req.strat_id,
                    req.symbol_id,
                    req.order_seqnum,
                    OrsQueryOrigin::Place,
                    due_ns,
                    |_, oid| ctx.order_manager.lock_order(oid).is_some(),
                );
                Ok(SubmitNewOutcome::InDoubtScheduled {
                    ors_order_id: outcome.ors_order_id,
                    account_ref: outcome.account_ref,
                    connection_id: route.connection_id,
                    client_id: outcome.client_id,
                })
            }
            SendOutcome::Busy | SendOutcome::Disconnected | SendOutcome::Bounds => {
                let _ = ctx.order_manager.on_rejected(
                    outcome.ors_order_id,
                    None,
                    "SEND_FAILED_BEFORE_WIRE",
                    Some(TerminalReason::MarketRejectMarketDown),
                    req.now_mono_ns,
                );
                Err(RefusalReason::MarketRejectMarketDown)
            }
        }
    }

    /// Default `REQ` `Cancel` orchestration (`OrderManager::request_cancel` + single-pick cancel
    /// rate-limiter admission + `LockedConnection::send_frame` without overlapping `LockedOrder`).
    fn submit_cancel(
        &self,
        ctx: &AccountSessionContext<'_>,
        req: &OrsCancelRequest,
        scratch: &mut [u8],
    ) -> Result<SubmitCancelOutcome, RefusalReason> {
        let dispatch = ctx
            .order_manager
            .request_cancel(req)
            .map_err(|refusal| refusal.reason)?;

        if !dispatch.should_send_now {
            return Ok(SubmitCancelOutcome::Deferred(dispatch));
        }

        self.dispatch_cancel_wire(ctx, &dispatch, req.now_mono_ns, scratch)
    }

    /// Transmits an already-approved [`OrsCancelDispatch`] (used by `submit_cancel`,
    /// `cancel-before-ack` follow-up on `RESP`, admin cancel sweeps, and `QueryDesk` retries).
    fn dispatch_cancel_wire(
        &self,
        ctx: &AccountSessionContext<'_>,
        dispatch: &OrsCancelDispatch,
        now_mono_ns: u64,
        scratch: &mut [u8],
    ) -> Result<SubmitCancelOutcome, RefusalReason> {
        let product = ctx
            .order_manager
            .symbol_spec(dispatch.symbol_id as usize)
            .map(|s| s.product);
        let scope = match product {
            Some(p) => self.instrument_scope(dispatch.symbol_id, p),
            None => InstrumentScope::None,
        };
        let ip_weight = self.cancel_ip_weight(product);
        let route = match ctx.pick_and_cancel_or_nack(product, scope, ip_weight, now_mono_ns) {
            Ok(r) => r,
            Err(err) => {
                let reason = map_admission_reject(err);
                let _ = ctx.order_manager.on_cancel_rejected(
                    dispatch.ors_order_id,
                    None,
                    reason,
                    now_mono_ns,
                );
                return Err(reason);
            }
        };

        let encoded = self.encode_cancel_order(ctx.spec, dispatch, route.connection_id, scratch)?;
        if encoded.frame_len > scratch.len() {
            let _ = ctx.order_manager.on_cancel_rejected(
                dispatch.ors_order_id,
                None,
                TerminalReason::AbortedBeforeSend,
                now_mono_ns,
            );
            return Err(RefusalReason::AbortedBeforeSend);
        }

        let Some(conn) = ctx.connection_handle(route.connection_id) else {
            let _ = ctx.order_manager.on_cancel_rejected(
                dispatch.ors_order_id,
                None,
                TerminalReason::MarketRejectMarketDown,
                now_mono_ns,
            );
            return Err(RefusalReason::MarketRejectMarketDown);
        };

        let (order_seqnum, strat_id) = tickwire_account::split_ors_order_id(dispatch.ors_order_id);
        let req_ref = RequestRef {
            order_key: dispatch.ors_order_id,
            client_seq: order_seqnum,
            sent_tsc: 0,
            sent_mono_ns: now_mono_ns,
        };
        match conn.send_frame(
            &scratch[..encoded.frame_len],
            encoded.reply_or_seq_id,
            req_ref,
        ) {
            SendOutcome::Sent => Ok(SubmitCancelOutcome::Dispatched {
                ors_order_id: dispatch.ors_order_id,
                account_ref: dispatch.account_ref,
                connection_id: route.connection_id,
            }),
            SendOutcome::ShortWriteRetired => {
                let slot_idx = ctx
                    .order_manager
                    .lock_order(dispatch.ors_order_id)
                    .map(|l| l.order().slot_idx)
                    .unwrap_or(0);
                let due_ns = now_mono_ns
                    .saturating_add(self.status_query_delay_ms().saturating_mul(1_000_000));
                let _ = ctx.order_manager.query_desk().schedule_status_ask(
                    dispatch.account_ref,
                    dispatch.ors_order_id,
                    slot_idx,
                    strat_id,
                    dispatch.symbol_id,
                    order_seqnum,
                    OrsQueryOrigin::Cancel,
                    due_ns,
                    |_, oid| ctx.order_manager.lock_order(oid).is_some(),
                );
                Ok(SubmitCancelOutcome::InDoubtScheduled {
                    ors_order_id: dispatch.ors_order_id,
                    account_ref: dispatch.account_ref,
                    connection_id: route.connection_id,
                })
            }
            SendOutcome::Busy | SendOutcome::Disconnected | SendOutcome::Bounds => {
                let _ = ctx.order_manager.on_cancel_rejected(
                    dispatch.ors_order_id,
                    None,
                    TerminalReason::MarketRejectMarketDown,
                    now_mono_ns,
                );
                Err(RefusalReason::MarketRejectMarketDown)
            }
        }
    }

    /// Default `REQ` `Modify` orchestration (`OrderManager::request_modify` + single-pick order
    /// rate-limiter admission + `LockedConnection::send_frame` without overlapping `LockedOrder`).
    fn submit_modify(
        &self,
        ctx: &AccountSessionContext<'_>,
        req: &OrsModifyRequest,
        scratch: &mut [u8],
    ) -> Result<SubmitModifyOutcome, RefusalReason> {
        let dispatch = ctx
            .order_manager
            .request_modify(req)
            .map_err(|refusal| refusal.reason)?;

        let (client_id, venue_order_id, slot_idx) =
            match ctx.order_manager.lock_order(req.ors_order_id) {
                Some(locked) => {
                    let o = locked.order();
                    (o.client_id, o.venue_order_id, o.slot_idx)
                }
                None => return Err(RefusalReason::OrderNotOpen),
            };

        let product = ctx
            .order_manager
            .symbol_spec(dispatch.symbol_id as usize)
            .map(|s| s.product)
            .ok_or_else(|| {
                let _ = ctx.order_manager.on_modify_rejected(
                    dispatch.ors_order_id,
                    TerminalReason::InvalidQuantity,
                    req.now_mono_ns,
                );
                RefusalReason::InvalidQuantity
            })?;

        let scope = self.instrument_scope(dispatch.symbol_id, product);
        let ip_weight = self.place_ip_weight(product);
        let route = match ctx.pick_and_place_or_nack(product, scope, 1, ip_weight, req.now_mono_ns)
        {
            Ok(r) => r,
            Err(err) => {
                let reason = map_admission_reject(err);
                let _ = ctx.order_manager.on_modify_rejected(
                    dispatch.ors_order_id,
                    reason,
                    req.now_mono_ns,
                );
                return Err(reason);
            }
        };

        let encoded = match self.encode_modify_order(
            ctx.spec,
            &dispatch,
            client_id.as_str(),
            venue_order_id.as_str(),
            route.connection_id,
            scratch,
        ) {
            Ok(enc) if enc.frame_len <= scratch.len() => enc,
            Ok(_) => {
                let _ = ctx.order_manager.on_modify_rejected(
                    dispatch.ors_order_id,
                    TerminalReason::AbortedBeforeSend,
                    req.now_mono_ns,
                );
                return Err(RefusalReason::AbortedBeforeSend);
            }
            Err(reason) => {
                let _ = ctx.order_manager.on_modify_rejected(
                    dispatch.ors_order_id,
                    reason,
                    req.now_mono_ns,
                );
                return Err(reason);
            }
        };

        let Some(conn) = ctx.connection_handle(route.connection_id) else {
            let _ = ctx.order_manager.on_modify_rejected(
                dispatch.ors_order_id,
                TerminalReason::MarketRejectMarketDown,
                req.now_mono_ns,
            );
            return Err(RefusalReason::MarketRejectMarketDown);
        };

        let (order_seqnum, strat_id) = tickwire_account::split_ors_order_id(dispatch.ors_order_id);
        let req_ref = RequestRef {
            order_key: dispatch.ors_order_id,
            client_seq: order_seqnum,
            sent_tsc: 0,
            sent_mono_ns: req.now_mono_ns,
        };
        match conn.send_frame(
            &scratch[..encoded.frame_len],
            encoded.reply_or_seq_id,
            req_ref,
        ) {
            SendOutcome::Sent => Ok(SubmitModifyOutcome::Dispatched {
                ors_order_id: dispatch.ors_order_id,
                account_ref: dispatch.account_ref,
                connection_id: route.connection_id,
            }),
            SendOutcome::ShortWriteRetired => {
                let due_ns = req
                    .now_mono_ns
                    .saturating_add(self.status_query_delay_ms().saturating_mul(1_000_000));
                let _ = ctx.order_manager.query_desk().schedule_status_ask(
                    dispatch.account_ref,
                    dispatch.ors_order_id,
                    slot_idx,
                    strat_id,
                    dispatch.symbol_id,
                    order_seqnum,
                    OrsQueryOrigin::Modify,
                    due_ns,
                    |_, oid| ctx.order_manager.lock_order(oid).is_some(),
                );
                Ok(SubmitModifyOutcome::InDoubtScheduled {
                    ors_order_id: dispatch.ors_order_id,
                    account_ref: dispatch.account_ref,
                    connection_id: route.connection_id,
                })
            }
            SendOutcome::Busy | SendOutcome::Disconnected | SendOutcome::Bounds => {
                let _ = ctx.order_manager.on_modify_rejected(
                    dispatch.ors_order_id,
                    TerminalReason::MarketRejectMarketDown,
                    req.now_mono_ns,
                );
                Err(RefusalReason::MarketRejectMarketDown)
            }
        }
    }

    // --- Optional RFQ Quote Hooks (`8.4 protocol.md`) ---

    /// Returns `true` when this session currently holds RFQ quote `ors_order_id`.
    fn has_quote(&self, _ors_order_id: u64) -> bool {
        false
    }

    /// Number of open RFQ maker quotes on this session (checked during 2s graceful shutdown drain).
    fn open_quotes(&self) -> usize {
        0
    }

    /// Submits or modifies an RFQ maker quote (`RefusalReason::RfqQuoteNotSupported` by default).
    fn submit_quote(
        &self,
        _ctx: &AccountSessionContext<'_>,
        _quote: &RfqQuoteRequest,
        _scratch: &mut [u8],
    ) -> Result<ReportBody, RefusalReason> {
        Err(RefusalReason::RfqQuoteNotSupported)
    }

    /// Cancels open RFQ quotes (`RefusalReason::RfqQuoteNotSupported` by default).
    fn cancel_quotes(
        &self,
        _ctx: &AccountSessionContext<'_>,
        _cancel: &RfqQuoteCancelRequest,
        _scratch: &mut [u8],
    ) -> Result<usize, RefusalReason> {
        Err(RefusalReason::RfqQuoteNotSupported)
    }

    // --- Inbound & Session Bring-Up Hooks (`RESP` & `Dialer`) ---

    /// Resolves the dial endpoint for `url_id` / `role` (injecting `stream_token` when needed).
    fn connection_url(
        &self,
        spec: &AccountSpec,
        pool: &SessionUrlPoolSpec,
        stream_token: Option<&[u8]>,
    ) -> Result<SessionEndpoint, String> {
        let path = if pool.role.is_stream() && self.has_stream_token() {
            let tok = stream_token
                .and_then(|b| core::str::from_utf8(b).ok())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| String::from("stream token not ready"))?;
            format!("{}/{}", pool.path.trim_end_matches('/'), tok)
        } else {
            pool.path.clone()
        };
        let _ = spec;
        Ok(SessionEndpoint {
            host: pool.host.clone(),
            port: pool.port,
            path,
            security: pool.security.clone(),
            initial_peers: pool.initial_peers.clone(),
        })
    }

    /// Writes an optional post-`101` WebSocket login frame (`0` bytes when URL-authenticated).
    fn encode_login(
        &self,
        _spec: &AccountSpec,
        _role: UrlRole,
        _seq: u32,
        _out: &mut [u8],
    ) -> Result<usize, String> {
        Ok(0)
    }

    /// Returns `true` when the link must await [`LoginReply::Ok`] before becoming order-ready.
    fn awaits_login_reply(&self, _role: UrlRole) -> bool {
        false
    }

    /// Classifies an authentication frame while `awaits_login_reply` is active.
    fn login_reply(&self, _payload: &[u8], _role: UrlRole) -> LoginReply {
        LoginReply::Ok
    }

    /// Optional signed challenge frame writer when [`Self::login_reply`] returns [`LoginReply::Other`].
    fn login_challenge(
        &self,
        _spec: &AccountSpec,
        _frame: &[u8],
        _role: UrlRole,
        _out: &mut [u8],
    ) -> Result<usize, String> {
        Ok(0)
    }

    /// Optional multi-step WebSocket login challenge configuration for `Dialer`.
    fn ws_login_challenge(&self, _spec: &AccountSpec, _role: UrlRole) -> WsLoginChallenge {
        WsLoginChallenge::None
    }

    /// Encodes subscription step `step` (`0, 1, ...`); returning `0` finishes subscription setup.
    fn encode_subscribe(
        &self,
        _spec: &AccountSpec,
        _role: UrlRole,
        _step: usize,
        _out: &mut [u8],
    ) -> Result<usize, String> {
        Ok(0)
    }

    /// Optional FIX session framing parameters (`BeginString`, `SenderCompID`, `TargetCompID`, `HeartBtInt`).
    fn fix_session(&self, _spec: &AccountSpec) -> Option<FixSessionConfig> {
        None
    }

    /// Optional extra FIX `Logon (35=A)` authentication tags signed over `sending_time`.
    fn fix_logon(&self, _spec: &AccountSpec, _sending_time: &str) -> Option<String> {
        None
    }

    /// Returns `true` when `payload` is an application-level pong frame to be dropped before `on_message`.
    fn is_pong(&self, _payload: &[u8]) -> bool {
        false
    }

    /// Encodes an immediate pong frame into `out` when `payload` is a venue application ping.
    fn reply_to_ping(&self, _payload: &[u8], _role: UrlRole, _out: &mut [u8]) -> usize {
        0
    }

    /// Extracts the correlated `reply_id` (`ors_order_id` or sequence key) from a multiplexed WS reply.
    fn reply_id(&self, _payload: &[u8]) -> Option<u64> {
        None
    }

    /// Encodes a periodic application-level WebSocket ping frame (`0` when WS opcode Ping is used).
    fn encode_ping(&self, _role: UrlRole, _now_ms: u64, _out: &mut [u8]) -> usize {
        0
    }

    /// Processes one complete decoded inbound WS/HTTPS/FIX payload on `RESP`.
    #[allow(clippy::too_many_arguments)]
    fn on_message(
        &self,
        account_ref: u8,
        url_id: &str,
        role: UrlRole,
        conn_id: u16,
        hint: Option<RequestRef>,
        payload: &[u8],
        now_mono_ns: u64,
        effects: &mut SessionMessageEffects,
    );

    /// Invoked on `RESP` when connection `conn_id` drops (`pending` carries any in-flight HTTPS request).
    #[allow(clippy::too_many_arguments)]
    fn on_disconnected(
        &self,
        _account_ref: u8,
        _url_id: &str,
        _role: UrlRole,
        _conn_id: u16,
        pending: Option<RequestRef>,
        _now_mono_ns: u64,
        effects: &mut SessionMessageEffects,
    ) {
        if let Some(req) = pending
            && req.order_key != 0
        {
            effects.query_status.push(QueryStatusRequest {
                ors_order_id: req.order_key,
                origin: OrsQueryOrigin::SocketDrop,
            });
        }
    }

    // --- Off-Hot-Path REST Hooks (`AsyncRestQuerier` & Cold Bootstrap) ---

    /// Returns `true` when this venue requires a dynamic `listenKey` / `urlToken` for private streams.
    fn has_stream_token(&self) -> bool {
        false
    }

    /// Stream token refresh period in seconds (`0` disables periodic refresh).
    fn stream_token_refresh_s(&self) -> u64 {
        1_800
    }

    /// Builds the `POST` (`refresh == false`) or `PUT` (`refresh == true`) stream-token REST request.
    fn stream_token_query(
        &self,
        _spec: &AccountSpec,
        _refresh: bool,
        _current_token: Option<&[u8]>,
    ) -> Option<BuiltRequest> {
        None
    }

    /// Parses a stream-token REST response, returning `Ok(Some(token_bytes))` on new token creation,
    /// `Ok(None)` on keep-alive `PUT` acknowledgement, or `Err` to schedule retry.
    fn on_stream_token_reply(
        &self,
        _body: &[u8],
        _refresh: bool,
    ) -> Result<Option<Vec<u8>>, String> {
        Ok(None)
    }

    /// Builds diagnostic `OpenOrders` REST `GET` queries (`SIGUSR1` / `QueryOpen` admin command).
    fn open_orders_queries(&self, _spec: &AccountSpec) -> Vec<BuiltRequest> {
        Vec::new()
    }

    /// Builds a single-order `StatusAsk` REST `GET` query for `OrsQueryDesk` (`None` ends the ask).
    fn order_status_query(
        &self,
        _spec: &AccountSpec,
        _ors_order_id: u64,
        _symbol_id: u32,
        _client_id: &str,
        _venue_order_id: &str,
        _kind: SessionRequestKind,
    ) -> Option<BuiltRequest> {
        None
    }

    /// Initial delay (`ms`) between scheduling a `StatusAsk` and dispatching its REST query.
    fn status_query_delay_ms(&self) -> u64 {
        1_000
    }

    /// Decodes a `StatusAsk` REST response into a structured [`StatusReplyOutcome`].
    #[allow(clippy::too_many_arguments)]
    fn apply_status(
        &self,
        _account_ref: u8,
        _ors_order_id: u64,
        _symbol_id: u32,
        _kind: SessionRequestKind,
        _task: &RestTask,
        reply: &RestReply,
        _now_mono_ns: u64,
    ) -> StatusReplyOutcome {
        StatusReplyOutcome {
            snapshot: None,
            retry_query: reply.status != tickwire_transport::rest_querier::RestStatus::Ok,
            rate_limits: Vec::new(),
        }
    }

    /// Cold-startup hook fetching authenticated Exchange REST `Account + OpenOrders` snapshots.
    fn bootstrap_snapshots(
        &self,
        _spec: &AccountSpec,
    ) -> Result<OrsExchangeBootstrapSnapshot, String> {
        Ok(OrsExchangeBootstrapSnapshot {
            balances: Vec::new(),
            positions: Vec::new(),
            open_orders: Vec::new(),
            balances_include_open_order_locks: true,
        })
    }
}
