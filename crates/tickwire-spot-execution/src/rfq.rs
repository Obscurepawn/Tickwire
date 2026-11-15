//! Optional UTA Options and institutional Block `RFQ` Quote Extension Hook (`RfqQuoteHook`)
//! supporting Bybit V5 (`/v5/rfq/*`) and OKX V5 (`/api/v5/rfq/*`) with zero-allocation stack
//! wire builders and a default `NoopRfqQuoteHook` returning `TerminalReason::RFQ_QUOTE_NOT_SUPPORTED`.

use crate::{SpotVenue, UtaVenueError, client_ok, symbol_ok, wire_encode::BufWriter};
use tickwire_account::InlineAscii;
use tickwire_transport::latency_tracer::{LockDomain, LockId, ThreadTag, TimedSpinLock};
use tickwire_venue::{Product, Side, TerminalReason};

/// Maximum legs inside one multi-leg UTA Options/Block RFQ quote.
pub const MAX_RFQ_LEGS: usize = 8;
/// Default preallocated quote slot capacity in [`UtaRfqQuoteBook`].
pub const DEFAULT_MAX_OPEN_RFQ_QUOTES: usize = 64;

/// Single leg inside a multi-leg Options or Block RFQ quote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RfqQuoteLeg {
    pub symbol: InlineAscii<32>,
    pub product: Product,
    pub side: Side,
    pub price_atoms: i128,
    pub price_scale: u8,
    pub qty_atoms: i128,
    pub qty_scale: u8,
    pub ratio: u32,
}

impl Default for RfqQuoteLeg {
    fn default() -> Self {
        Self {
            symbol: InlineAscii::empty(),
            product: Product::Options,
            side: Side::Buy,
            price_atoms: 0,
            price_scale: 2,
            qty_atoms: 0,
            qty_scale: 2,
            ratio: 1,
        }
    }
}

/// Outbound RFQ quote submission request (`Bybit` `/v5/rfq/create-quote` or `OKX` `/api/v5/rfq/create-quote`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RfqQuoteRequest {
    pub ors_order_id: u64,
    pub strat_id: u8,
    pub rfq_id: InlineAscii<40>,
    pub client_quote_id: InlineAscii<36>,
    pub quote_side: Side,
    pub valid_until_ms: u64,
    pub anonymous: bool,
    pub legs: [RfqQuoteLeg; MAX_RFQ_LEGS],
    pub leg_count: u8,
}

impl RfqQuoteRequest {
    /// Constructs a single-leg Options/Block RFQ quote request.
    pub fn single_leg(
        ors_order_id: u64,
        strat_id: u8,
        rfq_id: &str,
        client_quote_id: &str,
        quote_side: Side,
        leg: RfqQuoteLeg,
        valid_until_ms: u64,
    ) -> Result<Self, UtaVenueError> {
        if ors_order_id == 0 || rfq_id.is_empty() || client_quote_id.is_empty() {
            return Err(UtaVenueError::Encoding);
        }
        let rfq_inline = InlineAscii::try_from_str(rfq_id).ok_or(UtaVenueError::Bounds)?;
        let cid_inline = InlineAscii::try_from_str(client_quote_id).ok_or(UtaVenueError::Bounds)?;
        let mut legs = [RfqQuoteLeg::default(); MAX_RFQ_LEGS];
        legs[0] = leg;
        Ok(Self {
            ors_order_id,
            strat_id,
            rfq_id: rfq_inline,
            client_quote_id: cid_inline,
            quote_side,
            valid_until_ms,
            anonymous: false,
            legs,
            leg_count: 1,
        })
    }
}

/// Outbound RFQ quote cancellation request (`/v5/rfq/{cancel-quote,cancel-all-quotes}` or `/api/v5/rfq/{cancel-quote,cancel-all-quotes}`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RfqCancelQuotesRequest {
    /// Specific `ors_order_id` to cancel, or `None` for cancel-all-quotes / `rfq_id` scope.
    pub ors_order_id: Option<u64>,
    pub rfq_id: Option<InlineAscii<40>>,
    pub client_quote_id: Option<InlineAscii<36>>,
    pub venue_quote_id: Option<InlineAscii<40>>,
}

/// Built RFQ HTTP endpoint path and body length written into caller's stack buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BuiltRfqPayload {
    pub path: &'static str,
    pub body_len: usize,
}

/// Lifecycle state of an active RFQ quote tracked in [`UtaRfqQuoteBook`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RfqQuoteState {
    PendingNew,
    Open,
    PendingCancel,
}

/// Snapshot of a tracked RFQ quote slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RfqQuoteSnapshot {
    pub ors_order_id: u64,
    pub strat_id: u8,
    pub state: RfqQuoteState,
    pub rfq_id: InlineAscii<40>,
    pub client_quote_id: InlineAscii<36>,
    pub venue_quote_id: InlineAscii<40>,
}

/// Extension hook attached to `BybitUtaSession` / `OkxUtaSession` for UTA Options & Block RFQ quoting.
///
/// Implementations are `Send + Sync` so `REQ` (`submit_quote`, `cancel_quotes`) and `RESP`
/// (`on_quote_ack`, `on_quote_terminal`) operate concurrently via `&self` without coarse session locks.
pub trait RfqQuoteHook: Send + Sync + std::fmt::Debug {
    /// Returns `true` when RFQ quoting is enabled on this session.
    fn is_enabled(&self) -> bool;

    /// Returns `true` when `ors_order_id` currently owns a live (`PendingNew`, `Open`, or `PendingCancel`) RFQ quote.
    fn has_quote(&self, ors_order_id: u64) -> bool;

    /// Returns the count of currently live RFQ quotes on this session.
    fn open_quotes(&self) -> usize;

    /// Admits and encodes an outbound RFQ quote creation payload into `out_body`.
    fn submit_quote(
        &self,
        venue: SpotVenue,
        req: &RfqQuoteRequest,
        out_body: &mut [u8],
    ) -> Result<BuiltRfqPayload, TerminalReason>;

    /// Marks matching RFQ quote(s) `PendingCancel` and encodes the cancellation payload into `out_body`.
    fn cancel_quotes(
        &self,
        venue: SpotVenue,
        req: &RfqCancelQuotesRequest,
        out_body: &mut [u8],
    ) -> Result<BuiltRfqPayload, TerminalReason>;

    /// Transitions `ors_order_id` from `PendingNew` to `Open`, recording `venue_quote_id` when supplied.
    fn on_quote_ack(&self, ors_order_id: u64, venue_quote_id: Option<&str>) -> bool;

    /// Releases the RFQ quote slot for `ors_order_id` upon fill, expiry, rejection, or cancellation.
    fn on_quote_terminal(&self, ors_order_id: u64) -> bool;
}

/// Default disabled RFQ hook returning `TerminalReason::RFQ_QUOTE_NOT_SUPPORTED` (`RfqQuoteNotSupported`).
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopRfqQuoteHook;

impl RfqQuoteHook for NoopRfqQuoteHook {
    #[inline]
    fn is_enabled(&self) -> bool {
        false
    }

    #[inline]
    fn has_quote(&self, _ors_order_id: u64) -> bool {
        false
    }

    #[inline]
    fn open_quotes(&self) -> usize {
        0
    }

    #[inline]
    fn submit_quote(
        &self,
        _venue: SpotVenue,
        _req: &RfqQuoteRequest,
        _out_body: &mut [u8],
    ) -> Result<BuiltRfqPayload, TerminalReason> {
        Err(TerminalReason::RFQ_QUOTE_NOT_SUPPORTED)
    }

    #[inline]
    fn cancel_quotes(
        &self,
        _venue: SpotVenue,
        _req: &RfqCancelQuotesRequest,
        _out_body: &mut [u8],
    ) -> Result<BuiltRfqPayload, TerminalReason> {
        Err(TerminalReason::RFQ_QUOTE_NOT_SUPPORTED)
    }

    #[inline]
    fn on_quote_ack(&self, _ors_order_id: u64, _venue_quote_id: Option<&str>) -> bool {
        false
    }

    #[inline]
    fn on_quote_terminal(&self, _ors_order_id: u64) -> bool {
        false
    }
}

#[derive(Debug)]
struct RfqBookInner {
    slots: Box<[Option<RfqQuoteSnapshot>]>,
    live_count: usize,
}

/// Preallocated, zero-hot-path-allocation RFQ quote book (`Send + Sync`, fine-grained
/// `TimedSpinLock`) for Bybit V5 (`/v5/rfq/*`) and OKX V5 (`/api/v5/rfq/*`).
#[derive(Debug)]
pub struct UtaRfqQuoteBook {
    inner: TimedSpinLock<RfqBookInner>,
}

impl Default for UtaRfqQuoteBook {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_OPEN_RFQ_QUOTES)
    }
}

impl UtaRfqQuoteBook {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(1);
        Self {
            inner: TimedSpinLock::new(
                RfqBookInner {
                    slots: vec![None; cap].into_boxed_slice(),
                    live_count: 0,
                },
                LockId::new(LockDomain::GuardedValue, 0x0052_4651),
            ),
        }
    }

    pub fn snapshot(&self, ors_order_id: u64) -> Option<RfqQuoteSnapshot> {
        let guard = self.inner.lock(ThreadTag::UNKNOWN);
        guard
            .slots
            .iter()
            .flatten()
            .find(|s| s.ors_order_id == ors_order_id)
            .copied()
    }
}

/// Encodes a Bybit V5 `/v5/rfq/create-quote` or OKX V5 `/api/v5/rfq/create-quote` JSON body into `out_body`.
pub fn encode_rfq_create_quote_body_into(
    venue: SpotVenue,
    req: &RfqQuoteRequest,
    out_body: &mut [u8],
) -> Result<BuiltRfqPayload, UtaVenueError> {
    let leg_n = usize::from(req.leg_count);
    if req.ors_order_id == 0
        || leg_n == 0
        || leg_n > MAX_RFQ_LEGS
        || req.rfq_id.is_empty()
        || !symbol_ok(req.rfq_id.as_str())
        || !client_ok(venue, req.client_quote_id.as_str())
    {
        return Err(UtaVenueError::Encoding);
    }

    let mut w = BufWriter::new(out_body);
    match venue {
        SpotVenue::Bybit => {
            w.put_bytes(b"{\"rfqId\":\"")?;
            w.put_bytes(req.rfq_id.as_bytes())?;
            w.put_bytes(b"\",\"quoteLinkId\":\"")?;
            w.put_bytes(req.client_quote_id.as_bytes())?;
            w.put_bytes(b"\",\"quoteSide\":\"")?;
            w.put_bytes(match req.quote_side {
                Side::Buy => b"Buy",
                Side::Sell => b"Sell",
            })?;
            w.put_bytes(b"\",\"anonymous\":")?;
            w.put_bytes(if req.anonymous { b"true" } else { b"false" })?;
            if req.valid_until_ms > 0 {
                w.put_bytes(b",\"expiresAt\":")?;
                w.put_u64(req.valid_until_ms)?;
            }
            w.put_bytes(b",\"quoteBuyList\":[")?;
            for (idx, leg) in req.legs[..leg_n].iter().enumerate() {
                if !symbol_ok(leg.symbol.as_str()) || leg.price_atoms <= 0 || leg.qty_atoms <= 0 {
                    return Err(UtaVenueError::Encoding);
                }
                let cat = crate::BybitCategory::from_product(leg.product)
                    .ok_or(UtaVenueError::Encoding)?;
                if idx > 0 {
                    w.put_byte(b',')?;
                }
                w.put_bytes(b"{\"category\":\"")?;
                w.put_bytes(cat.as_str().as_bytes())?;
                w.put_bytes(b"\",\"symbol\":\"")?;
                w.put_bytes(leg.symbol.as_bytes())?;
                w.put_bytes(b"\",\"side\":\"")?;
                w.put_bytes(match leg.side {
                    Side::Buy => b"Buy",
                    Side::Sell => b"Sell",
                })?;
                w.put_bytes(b"\",\"price\":\"")?;
                w.put_scaled_atoms(leg.price_atoms, leg.price_scale)?;
                w.put_bytes(b"\",\"qty\":\"")?;
                w.put_scaled_atoms(leg.qty_atoms, leg.qty_scale)?;
                w.put_bytes(b"\"}")?;
            }
            w.put_bytes(b"]}")?;
            Ok(BuiltRfqPayload {
                path: "/v5/rfq/create-quote",
                body_len: w.written(),
            })
        }
        SpotVenue::Okx => {
            w.put_bytes(b"{\"rfqId\":\"")?;
            w.put_bytes(req.rfq_id.as_bytes())?;
            w.put_bytes(b"\",\"clQuoteId\":\"")?;
            w.put_bytes(req.client_quote_id.as_bytes())?;
            w.put_bytes(b"\",\"quoteSide\":\"")?;
            w.put_bytes(match req.quote_side {
                Side::Buy => b"buy",
                Side::Sell => b"sell",
            })?;
            w.put_bytes(b"\",\"anonymous\":")?;
            w.put_bytes(if req.anonymous { b"true" } else { b"false" })?;
            if req.valid_until_ms > 0 {
                w.put_bytes(b",\"validUntil\":\"")?;
                w.put_u64(req.valid_until_ms)?;
                w.put_byte(b'"')?;
            }
            w.put_bytes(b",\"legs\":[")?;
            for (idx, leg) in req.legs[..leg_n].iter().enumerate() {
                if !symbol_ok(leg.symbol.as_str()) || leg.price_atoms <= 0 || leg.qty_atoms <= 0 {
                    return Err(UtaVenueError::Encoding);
                }
                let _ =
                    crate::OkxInstType::from_product(leg.product).ok_or(UtaVenueError::Encoding)?;
                if idx > 0 {
                    w.put_byte(b',')?;
                }
                w.put_bytes(b"{\"instId\":\"")?;
                w.put_bytes(leg.symbol.as_bytes())?;
                w.put_bytes(b"\",\"side\":\"")?;
                w.put_bytes(match leg.side {
                    Side::Buy => b"buy",
                    Side::Sell => b"sell",
                })?;
                w.put_bytes(b"\",\"px\":\"")?;
                w.put_scaled_atoms(leg.price_atoms, leg.price_scale)?;
                w.put_bytes(b"\",\"sz\":\"")?;
                w.put_scaled_atoms(leg.qty_atoms, leg.qty_scale)?;
                w.put_bytes(b"\",\"ratio\":\"")?;
                w.put_u64(u64::from(leg.ratio.max(1)))?;
                w.put_bytes(b"\"}")?;
            }
            w.put_bytes(b"]}")?;
            Ok(BuiltRfqPayload {
                path: "/api/v5/rfq/create-quote",
                body_len: w.written(),
            })
        }
    }
}

/// Encodes a Bybit V5 `/v5/rfq/execute-quote` JSON body into `out_body`.
pub fn encode_bybit_rfq_execute_quote_into(
    rfq_id: &str,
    quote_id: &str,
    quote_side: Side,
    out_body: &mut [u8],
) -> Result<BuiltRfqPayload, UtaVenueError> {
    if !symbol_ok(rfq_id) || !symbol_ok(quote_id) {
        return Err(UtaVenueError::Encoding);
    }
    let mut w = BufWriter::new(out_body);
    w.put_bytes(b"{\"rfqId\":\"")?;
    w.put_bytes(rfq_id.as_bytes())?;
    w.put_bytes(b"\",\"quoteId\":\"")?;
    w.put_bytes(quote_id.as_bytes())?;
    w.put_bytes(b"\",\"quoteSide\":\"")?;
    w.put_bytes(match quote_side {
        Side::Buy => b"Buy",
        Side::Sell => b"Sell",
    })?;
    w.put_bytes(b"\"}")?;
    Ok(BuiltRfqPayload {
        path: "/v5/rfq/execute-quote",
        body_len: w.written(),
    })
}

/// Encodes a Bybit V5 or OKX V5 RFQ single-quote or cancel-all-quotes JSON body into `out_body`.
pub fn encode_rfq_cancel_quotes_body_into(
    venue: SpotVenue,
    req: &RfqCancelQuotesRequest,
    out_body: &mut [u8],
) -> Result<BuiltRfqPayload, UtaVenueError> {
    let has_specific = req.venue_quote_id.as_ref().is_some_and(|s| !s.is_empty())
        || req.client_quote_id.as_ref().is_some_and(|s| !s.is_empty());
    let mut w = BufWriter::new(out_body);
    if !has_specific {
        match venue {
            SpotVenue::Bybit => {
                if let Some(rfq_id) = req.rfq_id.as_ref().filter(|s| !s.is_empty()) {
                    if !symbol_ok(rfq_id.as_str()) {
                        return Err(UtaVenueError::Encoding);
                    }
                    w.put_bytes(b"{\"rfqId\":\"")?;
                    w.put_bytes(rfq_id.as_bytes())?;
                    w.put_bytes(b"\"}")?;
                } else {
                    w.put_bytes(b"{}")?;
                }
                return Ok(BuiltRfqPayload {
                    path: "/v5/rfq/cancel-all-quotes",
                    body_len: w.written(),
                });
            }
            SpotVenue::Okx => {
                if let Some(rfq_id) = req.rfq_id.as_ref().filter(|s| !s.is_empty()) {
                    if !symbol_ok(rfq_id.as_str()) {
                        return Err(UtaVenueError::Encoding);
                    }
                    w.put_bytes(b"{\"rfqId\":\"")?;
                    w.put_bytes(rfq_id.as_bytes())?;
                    w.put_bytes(b"\"}")?;
                } else {
                    w.put_bytes(b"{}")?;
                }
                return Ok(BuiltRfqPayload {
                    path: "/api/v5/rfq/cancel-all-quotes",
                    body_len: w.written(),
                });
            }
        }
    }

    w.put_byte(b'{')?;
    let mut need_comma = false;
    if let Some(rfq_id) = req.rfq_id.as_ref().filter(|s| !s.is_empty()) {
        if !symbol_ok(rfq_id.as_str()) {
            return Err(UtaVenueError::Encoding);
        }
        w.put_bytes(b"\"rfqId\":\"")?;
        w.put_bytes(rfq_id.as_bytes())?;
        w.put_byte(b'"')?;
        need_comma = true;
    }
    if let Some(vqid) = req.venue_quote_id.as_ref().filter(|s| !s.is_empty()) {
        if !symbol_ok(vqid.as_str()) {
            return Err(UtaVenueError::Encoding);
        }
        if need_comma {
            w.put_byte(b',')?;
        }
        w.put_bytes(b"\"quoteId\":\"")?;
        w.put_bytes(vqid.as_bytes())?;
        w.put_byte(b'"')?;
        need_comma = true;
    }
    if let Some(cqid) = req.client_quote_id.as_ref().filter(|s| !s.is_empty()) {
        if !client_ok(venue, cqid.as_str()) {
            return Err(UtaVenueError::Encoding);
        }
        if need_comma {
            w.put_byte(b',')?;
        }
        w.put_bytes(match venue {
            SpotVenue::Bybit => b"\"quoteLinkId\":\"",
            SpotVenue::Okx => b"\"clQuoteId\":\"",
        })?;
        w.put_bytes(cqid.as_bytes())?;
        w.put_byte(b'"')?;
    }
    w.put_byte(b'}')?;
    Ok(BuiltRfqPayload {
        path: match venue {
            SpotVenue::Bybit => "/v5/rfq/cancel-quote",
            SpotVenue::Okx => "/api/v5/rfq/cancel-quote",
        },
        body_len: w.written(),
    })
}

impl RfqQuoteHook for UtaRfqQuoteBook {
    #[inline]
    fn is_enabled(&self) -> bool {
        true
    }

    #[inline]
    fn has_quote(&self, ors_order_id: u64) -> bool {
        if ors_order_id == 0 {
            return false;
        }
        let guard = self.inner.lock(ThreadTag::UNKNOWN);
        guard
            .slots
            .iter()
            .flatten()
            .any(|s| s.ors_order_id == ors_order_id)
    }

    #[inline]
    fn open_quotes(&self) -> usize {
        self.inner.lock(ThreadTag::UNKNOWN).live_count
    }

    fn submit_quote(
        &self,
        venue: SpotVenue,
        req: &RfqQuoteRequest,
        out_body: &mut [u8],
    ) -> Result<BuiltRfqPayload, TerminalReason> {
        let built = encode_rfq_create_quote_body_into(venue, req, out_body)
            .map_err(|_| TerminalReason::MKT_REJECT)?;
        let snapshot = RfqQuoteSnapshot {
            ors_order_id: req.ors_order_id,
            strat_id: req.strat_id,
            state: RfqQuoteState::PendingNew,
            rfq_id: req.rfq_id,
            client_quote_id: req.client_quote_id,
            venue_quote_id: InlineAscii::empty(),
        };
        let mut guard = self.inner.lock(ThreadTag::REQ);
        if guard
            .slots
            .iter()
            .flatten()
            .any(|s| s.ors_order_id == req.ors_order_id)
        {
            return Err(TerminalReason::ALLOCATION_ERROR);
        }
        let free_idx = guard
            .slots
            .iter()
            .position(Option::is_none)
            .ok_or(TerminalReason::ALLOCATION_ERROR)?;
        guard.slots[free_idx] = Some(snapshot);
        guard.live_count += 1;
        drop(guard);
        Ok(built)
    }

    fn cancel_quotes(
        &self,
        venue: SpotVenue,
        req: &RfqCancelQuotesRequest,
        out_body: &mut [u8],
    ) -> Result<BuiltRfqPayload, TerminalReason> {
        let mut effective = *req;
        {
            let mut guard = self.inner.lock(ThreadTag::REQ);
            if let Some(target_id) = req.ors_order_id {
                let Some(slot) = guard
                    .slots
                    .iter_mut()
                    .flatten()
                    .find(|s| s.ors_order_id == target_id)
                else {
                    return Err(TerminalReason::MKT_REJECT_NO_SUCH_ORDER);
                };
                slot.state = RfqQuoteState::PendingCancel;
                if effective.rfq_id.is_none() {
                    effective.rfq_id = Some(slot.rfq_id);
                }
                if effective.client_quote_id.is_none() {
                    effective.client_quote_id = Some(slot.client_quote_id);
                }
                if effective.venue_quote_id.is_none() && !slot.venue_quote_id.is_empty() {
                    effective.venue_quote_id = Some(slot.venue_quote_id);
                }
            } else {
                for slot in guard.slots.iter_mut().flatten() {
                    if effective.rfq_id.is_none_or(|id| id == slot.rfq_id) {
                        slot.state = RfqQuoteState::PendingCancel;
                    }
                }
            }
        }
        encode_rfq_cancel_quotes_body_into(venue, &effective, out_body)
            .map_err(|_| TerminalReason::MKT_REJECT)
    }

    fn on_quote_ack(&self, ors_order_id: u64, venue_quote_id: Option<&str>) -> bool {
        let vqid_inline = venue_quote_id.and_then(InlineAscii::try_from_str);
        let mut guard = self.inner.lock(ThreadTag::RESP);
        let Some(slot) = guard
            .slots
            .iter_mut()
            .flatten()
            .find(|s| s.ors_order_id == ors_order_id)
        else {
            return false;
        };
        if slot.state == RfqQuoteState::PendingNew {
            slot.state = RfqQuoteState::Open;
        }
        if let Some(inline) = vqid_inline {
            slot.venue_quote_id = inline;
        }
        true
    }

    fn on_quote_terminal(&self, ors_order_id: u64) -> bool {
        let mut guard = self.inner.lock(ThreadTag::RESP);
        for slot_opt in guard.slots.iter_mut() {
            if slot_opt
                .as_ref()
                .is_some_and(|s| s.ors_order_id == ors_order_id)
            {
                *slot_opt = None;
                guard.live_count = guard.live_count.saturating_sub(1);
                return true;
            }
        }
        false
    }
}
