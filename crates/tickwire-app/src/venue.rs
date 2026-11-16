//! Unified single-venue session selector (`1 ORS process = 1 Venue`) dispatching cleanly between
//! [`BinanceVenueSession`] (Spot, USD-M, Coin-M, Options) and [`UtaVenueSession`] (`Bybit V5` and
//! `OKX V5` 6-product Unified Trading Accounts) over [`tickwire_runtime::VenueSession`].

#![forbid(unsafe_code)]

use tickwire_account::{
    OrsCancelDispatch, OrsExchangeBootstrapSnapshot, OrsModifyDispatch, OrsNewOrderRequest,
    OrsPlaceOutcome,
};
use tickwire_account_gateway::AccountSpec;
use tickwire_ipc::ReportBody;
use tickwire_quota::shared::{InstrumentScope, OrsVenueProfile};
use tickwire_runtime::{
    AccountSessionContext, EncodedOrderFrame, LoginReply, RfqQuoteCancelRequest, RfqQuoteRequest,
    SessionEndpoint, SessionMessageEffects, SessionRequestKind, SessionRestEndpoint,
    SessionUrlPoolSpec, StatusReplyOutcome, UrlRole, VenueSession,
};
use tickwire_spot_runner::UtaVenueSession;
use tickwire_transport::FixSessionConfig;
use tickwire_transport::WsLoginChallenge;
use tickwire_transport::links::RequestRef;
use tickwire_transport::rest_querier::{BuiltRequest, RestReply, RestTask};
use tickwire_venue::{Product, RefusalReason};

use crate::binance_session::BinanceVenueSession;

/// Concrete venue driver instantiated inside a single-venue ORS process (`1 ORS process = 1 Venue`).
pub enum UnifiedVenueSession {
    Binance(BinanceVenueSession),
    BybitUta(UtaVenueSession),
    OkxUta(UtaVenueSession),
}

impl UnifiedVenueSession {
    pub fn warm_hot_path(&self, iterations: u32) -> Result<(), String> {
        match self {
            Self::Binance(s) => {
                let _ = s.warm_hot_path(iterations)?;
                Ok(())
            }
            Self::BybitUta(s) | Self::OkxUta(s) => {
                let _ = s.warm_hot_path(iterations)?;
                Ok(())
            }
        }
    }
}

impl VenueSession for UnifiedVenueSession {
    #[inline]
    fn venue_profile(&self) -> OrsVenueProfile {
        match self {
            Self::Binance(s) => s.venue_profile(),
            Self::BybitUta(s) | Self::OkxUta(s) => s.venue_profile(),
        }
    }

    fn url_pools(&self, spec: &AccountSpec) -> Result<Vec<SessionUrlPoolSpec>, String> {
        match self {
            Self::Binance(s) => s.url_pools(spec),
            Self::BybitUta(s) | Self::OkxUta(s) => s.url_pools(spec),
        }
    }

    fn rest_endpoint(&self, spec: &AccountSpec) -> Option<SessionRestEndpoint> {
        match self {
            Self::Binance(s) => s.rest_endpoint(spec),
            Self::BybitUta(s) | Self::OkxUta(s) => s.rest_endpoint(spec),
        }
    }

    #[inline]
    fn instrument_scope(&self, symbol_id: u32, product: Product) -> InstrumentScope {
        match self {
            Self::Binance(s) => s.instrument_scope(symbol_id, product),
            Self::BybitUta(s) | Self::OkxUta(s) => s.instrument_scope(symbol_id, product),
        }
    }

    #[inline]
    fn place_ip_weight(&self, product: Product) -> u32 {
        match self {
            Self::Binance(s) => s.place_ip_weight(product),
            Self::BybitUta(s) | Self::OkxUta(s) => s.place_ip_weight(product),
        }
    }

    #[inline]
    fn cancel_ip_weight(&self, product: Option<Product>) -> u32 {
        match self {
            Self::Binance(s) => s.cancel_ip_weight(product),
            Self::BybitUta(s) | Self::OkxUta(s) => s.cancel_ip_weight(product),
        }
    }

    #[inline]
    fn encode_new_order(
        &self,
        spec: &AccountSpec,
        req: &OrsNewOrderRequest,
        placed: &OrsPlaceOutcome,
        conn_id: u16,
        scratch: &mut [u8],
    ) -> Result<EncodedOrderFrame, RefusalReason> {
        match self {
            Self::Binance(s) => s.encode_new_order(spec, req, placed, conn_id, scratch),
            Self::BybitUta(s) | Self::OkxUta(s) => {
                s.encode_new_order(spec, req, placed, conn_id, scratch)
            }
        }
    }

    #[inline]
    fn encode_cancel_order(
        &self,
        spec: &AccountSpec,
        cancel: &OrsCancelDispatch,
        conn_id: u16,
        scratch: &mut [u8],
    ) -> Result<EncodedOrderFrame, RefusalReason> {
        match self {
            Self::Binance(s) => s.encode_cancel_order(spec, cancel, conn_id, scratch),
            Self::BybitUta(s) | Self::OkxUta(s) => {
                s.encode_cancel_order(spec, cancel, conn_id, scratch)
            }
        }
    }

    #[inline]
    fn encode_modify_order(
        &self,
        spec: &AccountSpec,
        modify: &OrsModifyDispatch,
        client_id: &str,
        venue_order_id: &str,
        conn_id: u16,
        scratch: &mut [u8],
    ) -> Result<EncodedOrderFrame, RefusalReason> {
        match self {
            Self::Binance(s) => {
                s.encode_modify_order(spec, modify, client_id, venue_order_id, conn_id, scratch)
            }
            Self::BybitUta(s) | Self::OkxUta(s) => {
                s.encode_modify_order(spec, modify, client_id, venue_order_id, conn_id, scratch)
            }
        }
    }

    #[inline]
    fn has_quote(&self, ors_order_id: u64) -> bool {
        match self {
            Self::Binance(s) => s.has_quote(ors_order_id),
            Self::BybitUta(s) | Self::OkxUta(s) => s.has_quote(ors_order_id),
        }
    }

    #[inline]
    fn open_quotes(&self) -> usize {
        match self {
            Self::Binance(s) => s.open_quotes(),
            Self::BybitUta(s) | Self::OkxUta(s) => s.open_quotes(),
        }
    }

    fn submit_quote(
        &self,
        ctx: &AccountSessionContext<'_>,
        quote: &RfqQuoteRequest,
        scratch: &mut [u8],
    ) -> Result<ReportBody, RefusalReason> {
        match self {
            Self::Binance(s) => s.submit_quote(ctx, quote, scratch),
            Self::BybitUta(s) | Self::OkxUta(s) => s.submit_quote(ctx, quote, scratch),
        }
    }

    fn cancel_quotes(
        &self,
        ctx: &AccountSessionContext<'_>,
        cancel: &RfqQuoteCancelRequest,
        scratch: &mut [u8],
    ) -> Result<usize, RefusalReason> {
        match self {
            Self::Binance(s) => s.cancel_quotes(ctx, cancel, scratch),
            Self::BybitUta(s) | Self::OkxUta(s) => s.cancel_quotes(ctx, cancel, scratch),
        }
    }

    fn connection_url(
        &self,
        spec: &AccountSpec,
        pool: &SessionUrlPoolSpec,
        stream_token: Option<&[u8]>,
    ) -> Result<SessionEndpoint, String> {
        match self {
            Self::Binance(s) => s.connection_url(spec, pool, stream_token),
            Self::BybitUta(s) | Self::OkxUta(s) => s.connection_url(spec, pool, stream_token),
        }
    }

    fn encode_login(
        &self,
        spec: &AccountSpec,
        role: UrlRole,
        seq: u32,
        out: &mut [u8],
    ) -> Result<usize, String> {
        match self {
            Self::Binance(s) => s.encode_login(spec, role, seq, out),
            Self::BybitUta(s) | Self::OkxUta(s) => s.encode_login(spec, role, seq, out),
        }
    }

    fn awaits_login_reply(&self, role: UrlRole) -> bool {
        match self {
            Self::Binance(s) => s.awaits_login_reply(role),
            Self::BybitUta(s) | Self::OkxUta(s) => s.awaits_login_reply(role),
        }
    }

    fn login_reply(&self, payload: &[u8], role: UrlRole) -> LoginReply {
        match self {
            Self::Binance(s) => s.login_reply(payload, role),
            Self::BybitUta(s) | Self::OkxUta(s) => s.login_reply(payload, role),
        }
    }

    fn login_challenge(
        &self,
        spec: &AccountSpec,
        frame: &[u8],
        role: UrlRole,
        out: &mut [u8],
    ) -> Result<usize, String> {
        match self {
            Self::Binance(s) => s.login_challenge(spec, frame, role, out),
            Self::BybitUta(s) | Self::OkxUta(s) => s.login_challenge(spec, frame, role, out),
        }
    }

    fn ws_login_challenge(&self, spec: &AccountSpec, role: UrlRole) -> WsLoginChallenge {
        match self {
            Self::Binance(s) => s.ws_login_challenge(spec, role),
            Self::BybitUta(s) | Self::OkxUta(s) => s.ws_login_challenge(spec, role),
        }
    }

    fn encode_subscribe(
        &self,
        spec: &AccountSpec,
        role: UrlRole,
        step: usize,
        out: &mut [u8],
    ) -> Result<usize, String> {
        match self {
            Self::Binance(s) => s.encode_subscribe(spec, role, step, out),
            Self::BybitUta(s) | Self::OkxUta(s) => s.encode_subscribe(spec, role, step, out),
        }
    }

    fn fix_session(&self, spec: &AccountSpec) -> Option<FixSessionConfig> {
        match self {
            Self::Binance(s) => s.fix_session(spec),
            Self::BybitUta(s) | Self::OkxUta(s) => s.fix_session(spec),
        }
    }

    fn fix_logon(&self, spec: &AccountSpec, sending_time: &str) -> Option<String> {
        match self {
            Self::Binance(s) => s.fix_logon(spec, sending_time),
            Self::BybitUta(s) | Self::OkxUta(s) => s.fix_logon(spec, sending_time),
        }
    }

    #[inline]
    fn is_pong(&self, payload: &[u8]) -> bool {
        match self {
            Self::Binance(s) => s.is_pong(payload),
            Self::BybitUta(s) | Self::OkxUta(s) => s.is_pong(payload),
        }
    }

    #[inline]
    fn reply_to_ping(&self, payload: &[u8], role: UrlRole, out: &mut [u8]) -> usize {
        match self {
            Self::Binance(s) => s.reply_to_ping(payload, role, out),
            Self::BybitUta(s) | Self::OkxUta(s) => s.reply_to_ping(payload, role, out),
        }
    }

    #[inline]
    fn reply_id(&self, payload: &[u8]) -> Option<u64> {
        match self {
            Self::Binance(s) => s.reply_id(payload),
            Self::BybitUta(s) | Self::OkxUta(s) => s.reply_id(payload),
        }
    }

    #[inline]
    fn encode_ping(&self, role: UrlRole, now_ms: u64, out: &mut [u8]) -> usize {
        match self {
            Self::Binance(s) => s.encode_ping(role, now_ms, out),
            Self::BybitUta(s) | Self::OkxUta(s) => s.encode_ping(role, now_ms, out),
        }
    }

    #[inline]
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
    ) {
        match self {
            Self::Binance(s) => s.on_message(
                account_ref,
                url_id,
                role,
                conn_id,
                hint,
                payload,
                now_mono_ns,
                effects,
            ),
            Self::BybitUta(s) | Self::OkxUta(s) => s.on_message(
                account_ref,
                url_id,
                role,
                conn_id,
                hint,
                payload,
                now_mono_ns,
                effects,
            ),
        }
    }

    fn on_disconnected(
        &self,
        account_ref: u8,
        url_id: &str,
        role: UrlRole,
        conn_id: u16,
        pending: Option<RequestRef>,
        now_mono_ns: u64,
        effects: &mut SessionMessageEffects,
    ) {
        match self {
            Self::Binance(s) => s.on_disconnected(
                account_ref,
                url_id,
                role,
                conn_id,
                pending,
                now_mono_ns,
                effects,
            ),
            Self::BybitUta(s) | Self::OkxUta(s) => s.on_disconnected(
                account_ref,
                url_id,
                role,
                conn_id,
                pending,
                now_mono_ns,
                effects,
            ),
        }
    }

    fn has_stream_token(&self) -> bool {
        match self {
            Self::Binance(s) => s.has_stream_token(),
            Self::BybitUta(s) | Self::OkxUta(s) => s.has_stream_token(),
        }
    }

    fn stream_token_refresh_s(&self) -> u64 {
        match self {
            Self::Binance(s) => s.stream_token_refresh_s(),
            Self::BybitUta(s) | Self::OkxUta(s) => s.stream_token_refresh_s(),
        }
    }

    fn stream_token_query(
        &self,
        spec: &AccountSpec,
        refresh: bool,
        current_token: Option<&[u8]>,
    ) -> Option<BuiltRequest> {
        match self {
            Self::Binance(s) => s.stream_token_query(spec, refresh, current_token),
            Self::BybitUta(s) | Self::OkxUta(s) => {
                s.stream_token_query(spec, refresh, current_token)
            }
        }
    }

    fn on_stream_token_reply(&self, body: &[u8], refresh: bool) -> Result<Option<Vec<u8>>, String> {
        match self {
            Self::Binance(s) => s.on_stream_token_reply(body, refresh),
            Self::BybitUta(s) | Self::OkxUta(s) => s.on_stream_token_reply(body, refresh),
        }
    }

    fn open_orders_queries(&self, spec: &AccountSpec) -> Vec<BuiltRequest> {
        match self {
            Self::Binance(s) => s.open_orders_queries(spec),
            Self::BybitUta(s) | Self::OkxUta(s) => s.open_orders_queries(spec),
        }
    }

    fn order_status_query(
        &self,
        spec: &AccountSpec,
        ors_order_id: u64,
        symbol_id: u32,
        client_id: &str,
        venue_order_id: &str,
        kind: SessionRequestKind,
    ) -> Option<BuiltRequest> {
        match self {
            Self::Binance(s) => s.order_status_query(
                spec,
                ors_order_id,
                symbol_id,
                client_id,
                venue_order_id,
                kind,
            ),
            Self::BybitUta(s) | Self::OkxUta(s) => s.order_status_query(
                spec,
                ors_order_id,
                symbol_id,
                client_id,
                venue_order_id,
                kind,
            ),
        }
    }

    fn status_query_delay_ms(&self) -> u64 {
        match self {
            Self::Binance(s) => s.status_query_delay_ms(),
            Self::BybitUta(s) | Self::OkxUta(s) => s.status_query_delay_ms(),
        }
    }

    fn apply_status(
        &self,
        account_ref: u8,
        ors_order_id: u64,
        symbol_id: u32,
        kind: SessionRequestKind,
        task: &RestTask,
        reply: &RestReply,
        now_mono_ns: u64,
    ) -> StatusReplyOutcome {
        match self {
            Self::Binance(s) => s.apply_status(
                account_ref,
                ors_order_id,
                symbol_id,
                kind,
                task,
                reply,
                now_mono_ns,
            ),
            Self::BybitUta(s) | Self::OkxUta(s) => s.apply_status(
                account_ref,
                ors_order_id,
                symbol_id,
                kind,
                task,
                reply,
                now_mono_ns,
            ),
        }
    }

    fn bootstrap_snapshots(
        &self,
        spec: &AccountSpec,
    ) -> Result<OrsExchangeBootstrapSnapshot, String> {
        match self {
            Self::Binance(s) => s.bootstrap_snapshots(spec),
            Self::BybitUta(s) | Self::OkxUta(s) => s.bootstrap_snapshots(spec),
        }
    }
}
