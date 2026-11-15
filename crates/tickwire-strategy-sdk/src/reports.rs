//! Zero-copy 512-byte `ReportRecord` shared-memory decoding (`FrontAck`, `Ack`, `Nack`,
//! `CancelNack`, `Fill` with `Execution` / `FeeAmendment` / `Projected` distinction, `Canceled`,
//! `Modified`, `ModifyRejected`, `ExchRateLimit`, `OrsShutDown`, `OrsRestricted`, and RFQ replies).
#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]

use std::os::fd::OwnedFd;

use tickwire_ipc::{
    ALL_STRATS, RecordHeader, ReportBody, ReportRecord, ResourceHeader, Wire, report_kind,
};
use tickwire_shm::{Publisher, Subscriber, SubscriptionKey};
use tickwire_venue::{
    ClientId, FrontAck, Liquidity, OrderStatus, PositionSide, Side, TerminalReason, VenueOrderId,
    VenueTradeId,
};

use crate::{
    Error, Result,
    arena::{ShmArenaReader, ShmArenaWriter, in_process_arena_pair},
};

const SEQUENCE_LIMIT: u64 = 1 << 63;

/// Required `ResourceHeader::required_features` bit for the 512-byte SPSC report ring (`resource_kind = 3`).
pub const FEATURE: u64 = 1 << 8;

/// Strategy worker identity bound to a 512-byte execution report ring (`resource_kind = 3`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReportIdentity {
    pub strat_id: u8,
    pub instance_id: [u8; 16],
    pub strategy_generation: u64,
}

impl ReportIdentity {
    #[forbid(unsafe_code)]
    pub fn validate(&self, bank: &ResourceHeader) -> Result<()> {
        if self.strat_id == ALL_STRATS
            || self.instance_id == [0; 16]
            || self.strategy_generation == 0
            || self.strategy_generation >= SEQUENCE_LIMIT
            || bank.resource_kind != 3
            || bank.record_bytes != ReportRecord::SIZE as u32
            || bank.required_features & FEATURE == 0
            || bank.risk_scope == 0
            || bank.consumer_instance != self.instance_id
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}

/// Explicit semantic classification of `ReportRecord` `Fill` (`773`) messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FillKind {
    /// Authoritative per-trade execution (`amendment == false && projected == false`).
    Execution,
    /// Post-trade commission or maker-rebate adjustment (`amendment == true`); never changes fill quantities.
    FeeAmendment,
    /// Early cumulative order-status fill projection (`projected == true`).
    Projected,
}

/// Signed asset fee/rebate attached to a [`FillReport`] (`has_fee == 1`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FillFee {
    pub fee_units: i64,
    pub fee_asset_id: u64,
    pub asset_exponent: i8,
}

/// Decoded payload of a `ReportRecord` `Fill` (`773`, `report_kind = 5`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FillReport {
    pub kind: FillKind,
    pub exec_ref: u64,
    pub trade_id: Option<VenueTradeId>,
    pub liquidity: Liquidity,
    pub last_qty_lots: i64,
    pub last_price_ticks: i64,
    pub cumulative_qty_lots: i64,
    pub projected_qty_lots: i64,
    pub leaves_qty_lots: i64,
    pub realized_pnl_units: i64,
    pub fee: Option<FillFee>,
}

/// Decoded payload of a `ReportRecord` `ExchRateLimit` (`777`, `report_kind = 9`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimitReport {
    pub rate_limit_type: u8,
    pub interval_unit: u8,
    pub interval_num: u16,
    pub limit: u32,
    pub count: u32,
    pub hard_limit_hit: bool,
}

/// Typed variant carried by an inbound [`ExecutionReport`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionReportKind {
    Ack,
    FrontAck,
    Nack,
    CancelNack,
    Fill(FillReport),
    Canceled,
    Modified,
    ModifyRejected,
    ExchRateLimit(RateLimitReport),
    OrsShutDown,
    OrsRestricted,
    RfqQuoteAck,
    RfqQuoteNack,
}

/// Validated strategy-facing view of a 512-byte `ReportRecord` SHM slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionReport {
    pub ring_sequence: u64,
    pub report_seq: u64,
    pub message_type: u16,
    pub strat_id: u8,
    pub account_ref: u8,
    pub ors_order_id: u64,
    pub order_seqnum: u64,
    pub orig_seqnum: u64,
    pub order_id: [u8; 16],
    pub instrument_id: u64,
    pub instrument_version: u64,
    pub strategy_generation: u64,
    pub exec_generation: u64,
    pub event_mono_ns: u64,
    pub exchange_time_ns: u64,
    pub order_status: OrderStatus,
    pub reason: TerminalReason,
    pub venue_errno: i32,
    pub side: Option<Side>,
    pub position_side: Option<PositionSide>,
    pub quantity_lots: i64,
    pub cumulative_qty_lots: i64,
    pub projected_qty_lots: i64,
    pub leaves_qty_lots: i64,
    pub price_ticks: i64,
    pub report_flags: u8,
    pub client_id: Option<ClientId>,
    pub venue_order_id: Option<VenueOrderId>,
    pub kind: ExecutionReportKind,
    pub arena_payload: Option<Vec<u8>>,
}

#[forbid(unsafe_code)]
impl ExecutionReport {
    /// Construct an [`ExecutionReport`] from a zero-copy validated [`ReportRecord`] and optional
    /// retained SHM Arena payload slice.
    pub fn from_wire(record: &ReportRecord, arena_payload: Option<&[u8]>) -> Result<Self> {
        let h = &record.header;
        let b = &record.body;
        if (b.arena_len > 0) != arena_payload.is_some() {
            return Err(Error::Bounds);
        }
        let order_status = decode_order_status(b.order_status)?;
        let reason = TerminalReason::from_code(b.reason)
            .ok_or(Error::Wire(tickwire_ipc::Error::InvalidEnum))?;
        let side = decode_side(b.side)?;
        let position_side = decode_position_side(b.position_side)?;
        let client_id = decode_optional_text::<40, 64>(&b.client_id, b.client_id_len)?;
        let venue_order_id =
            decode_optional_text::<64, 64>(&b.venue_order_id, b.venue_order_id_len)?;

        let kind = match h.message_type {
            report_kind::MSG_ACK => ExecutionReportKind::Ack,
            report_kind::MSG_FRONT_ACK => ExecutionReportKind::FrontAck,
            report_kind::MSG_NACK => ExecutionReportKind::Nack,
            report_kind::MSG_CANCEL_NACK => ExecutionReportKind::CancelNack,
            report_kind::MSG_FILL => {
                if b.amendment == 1 && b.projected == 1 {
                    return Err(Error::Wire(tickwire_ipc::Error::Malformed));
                }
                let fill_kind = if b.amendment == 1 {
                    FillKind::FeeAmendment
                } else if b.projected == 1 {
                    FillKind::Projected
                } else {
                    FillKind::Execution
                };
                let liquidity = decode_liquidity(b.liquidity)?;
                let trade_id =
                    decode_optional_text::<32, 32>(&b.venue_trade_id, b.venue_trade_id_len)?;
                let fee = if b.has_fee == 1 {
                    Some(FillFee {
                        fee_units: b.fee_units,
                        fee_asset_id: b.fee_asset_id,
                        asset_exponent: b.asset_exponent,
                    })
                } else {
                    None
                };
                if fill_kind == FillKind::FeeAmendment && fee.is_none() {
                    return Err(Error::Wire(tickwire_ipc::Error::Malformed));
                }
                ExecutionReportKind::Fill(FillReport {
                    kind: fill_kind,
                    exec_ref: b.exec_ref,
                    trade_id,
                    liquidity,
                    last_qty_lots: b.last_qty_lots,
                    last_price_ticks: b.last_price_ticks,
                    cumulative_qty_lots: b.cumulative_qty_lots,
                    projected_qty_lots: b.projected_qty_lots,
                    leaves_qty_lots: b.leaves_qty_lots,
                    realized_pnl_units: b.realized_pnl_units,
                    fee,
                })
            }
            report_kind::MSG_CANCELED => ExecutionReportKind::Canceled,
            report_kind::MSG_MODIFIED => ExecutionReportKind::Modified,
            report_kind::MSG_MODIFY_REJECTED => ExecutionReportKind::ModifyRejected,
            report_kind::MSG_EXCH_RATE_LIMIT => {
                ExecutionReportKind::ExchRateLimit(RateLimitReport {
                    rate_limit_type: b.rate_limit_type,
                    interval_unit: b.rate_limit_interval_unit,
                    interval_num: b.rate_limit_interval_num,
                    limit: b.rate_limit_limit,
                    count: b.rate_limit_count,
                    hard_limit_hit: b.hard_limit_hit == 1,
                })
            }
            report_kind::MSG_ORS_SHUT_DOWN => ExecutionReportKind::OrsShutDown,
            report_kind::MSG_ORS_RESTRICTED => ExecutionReportKind::OrsRestricted,
            report_kind::MSG_RFQ_QUOTE_ACK => ExecutionReportKind::RfqQuoteAck,
            report_kind::MSG_RFQ_QUOTE_NACK => ExecutionReportKind::RfqQuoteNack,
            _ => return Err(Error::Wire(tickwire_ipc::Error::UnknownMessage)),
        };

        Ok(Self {
            ring_sequence: h.sequence,
            report_seq: b.report_seq,
            message_type: h.message_type,
            strat_id: b.strat_id,
            account_ref: b.account_ref,
            ors_order_id: b.ors_order_id,
            order_seqnum: b.order_seqnum,
            orig_seqnum: b.orig_seqnum,
            order_id: b.order_id,
            instrument_id: b.instrument_id,
            instrument_version: h.instrument_version,
            strategy_generation: b.strategy_generation,
            exec_generation: b.exec_generation,
            event_mono_ns: b.event_mono_ns,
            exchange_time_ns: b.exchange_time_ns,
            order_status,
            reason,
            venue_errno: b.venue_errno,
            side,
            position_side,
            quantity_lots: b.quantity_lots,
            cumulative_qty_lots: b.cumulative_qty_lots,
            projected_qty_lots: b.projected_qty_lots,
            leaves_qty_lots: b.leaves_qty_lots,
            price_ticks: b.price_ticks,
            report_flags: b.report_flags,
            client_id,
            venue_order_id,
            kind,
            arena_payload: arena_payload.map(<[u8]>::to_vec),
        })
    }

    #[inline]
    pub fn fill(&self) -> Option<&FillReport> {
        match &self.kind {
            ExecutionReportKind::Fill(f) => Some(f),
            _ => None,
        }
    }

    #[inline]
    pub fn is_execution_fill(&self) -> bool {
        matches!(
            &self.kind,
            ExecutionReportKind::Fill(f) if f.kind == FillKind::Execution
        )
    }

    #[inline]
    pub fn is_amendment_fill(&self) -> bool {
        matches!(
            &self.kind,
            ExecutionReportKind::Fill(f) if f.kind == FillKind::FeeAmendment
        )
    }

    #[inline]
    pub fn is_projected_fill(&self) -> bool {
        matches!(
            &self.kind,
            ExecutionReportKind::Fill(f) if f.kind == FillKind::Projected
        )
    }

    /// Convert a `FrontAck` (`770`) report into a validated [`tickwire_venue::FrontAck`].
    pub fn to_venue_front_ack(&self) -> Result<Option<FrontAck>> {
        if self.kind != ExecutionReportKind::FrontAck {
            return Ok(None);
        }
        let ack = FrontAck {
            ors_order_id: self.ors_order_id,
            strat_id: self.strat_id,
            account_ref: self.account_ref,
            order_seqnum: self.order_seqnum,
            orig_seqnum: self.orig_seqnum,
            client_id: self.client_id.clone(),
            venue_order_id: self.venue_order_id.clone(),
            received_mono_ns: self.event_mono_ns,
        };
        ack.validate()?;
        Ok(Some(ack))
    }

    /// Classified [`TerminalReason`] when non-`Unknown` or implied by a terminal [`OrderStatus`].
    #[inline]
    pub fn terminal_reason(&self) -> Option<TerminalReason> {
        if self.reason != TerminalReason::Unknown {
            Some(self.reason)
        } else if self.order_status == OrderStatus::Filled {
            Some(TerminalReason::Filled)
        } else {
            None
        }
    }

    /// Returns `true` when this report closes an order's resting lifecycle (`Nack`, `Canceled`,
    /// or `OrderStatus::Filled | Canceled | Expired | Rejected` on non-rejection management events).
    #[inline]
    pub fn is_order_lifecycle_terminal(&self) -> bool {
        match &self.kind {
            ExecutionReportKind::Nack | ExecutionReportKind::Canceled => true,
            ExecutionReportKind::CancelNack
            | ExecutionReportKind::ModifyRejected
            | ExecutionReportKind::ExchRateLimit(_)
            | ExecutionReportKind::OrsShutDown
            | ExecutionReportKind::OrsRestricted
            | ExecutionReportKind::RfqQuoteAck
            | ExecutionReportKind::RfqQuoteNack => false,
            ExecutionReportKind::Fill(f) if f.kind == FillKind::FeeAmendment => false,
            ExecutionReportKind::Ack
            | ExecutionReportKind::FrontAck
            | ExecutionReportKind::Fill(_)
            | ExecutionReportKind::Modified => matches!(
                self.order_status,
                OrderStatus::Filled
                    | OrderStatus::Canceled
                    | OrderStatus::Expired
                    | OrderStatus::Rejected
            ),
        }
    }
}

#[forbid(unsafe_code)]
fn decode_optional_text<const RAW_N: usize, const BOUND_N: usize>(
    raw: &[u8; RAW_N],
    len: u8,
) -> Result<Option<tickwire_venue::BoundedText<BOUND_N>>> {
    let len_usize = usize::from(len);
    if len_usize == 0 {
        if raw.iter().any(|&b| b != 0) {
            return Err(Error::Wire(tickwire_ipc::Error::ReservedNonzero));
        }
        return Ok(None);
    }
    if len_usize > RAW_N || len_usize > BOUND_N || raw[len_usize..].iter().any(|&b| b != 0) {
        return Err(Error::Wire(tickwire_ipc::Error::Bounds));
    }
    let text = core::str::from_utf8(&raw[..len_usize])
        .map_err(|_| Error::Wire(tickwire_ipc::Error::Malformed))?;
    Ok(Some(tickwire_venue::BoundedText::<BOUND_N>::new(text)?))
}

#[forbid(unsafe_code)]
const fn decode_side(code: u8) -> Result<Option<Side>> {
    match code {
        0 => Ok(None),
        1 => Ok(Some(Side::Buy)),
        2 => Ok(Some(Side::Sell)),
        _ => Err(Error::Wire(tickwire_ipc::Error::InvalidEnum)),
    }
}

#[forbid(unsafe_code)]
const fn decode_position_side(code: u8) -> Result<Option<PositionSide>> {
    match code {
        0 => Ok(None),
        1 => Ok(Some(PositionSide::Both)),
        2 => Ok(Some(PositionSide::Long)),
        3 => Ok(Some(PositionSide::Short)),
        _ => Err(Error::Wire(tickwire_ipc::Error::InvalidEnum)),
    }
}

#[forbid(unsafe_code)]
const fn decode_liquidity(code: u8) -> Result<Liquidity> {
    match code {
        0 => Ok(Liquidity::Unknown),
        1 => Ok(Liquidity::Maker),
        2 => Ok(Liquidity::Taker),
        _ => Err(Error::Wire(tickwire_ipc::Error::InvalidEnum)),
    }
}

#[forbid(unsafe_code)]
const fn decode_order_status(code: u16) -> Result<OrderStatus> {
    match code {
        0 => Ok(OrderStatus::Unknown),
        1 => Ok(OrderStatus::Pending),
        2 => Ok(OrderStatus::New),
        3 => Ok(OrderStatus::PartiallyFilled),
        4 => Ok(OrderStatus::Filled),
        5 => Ok(OrderStatus::Canceled),
        6 => Ok(OrderStatus::Expired),
        7 => Ok(OrderStatus::Rejected),
        8 => Ok(OrderStatus::Unreported),
        9 => Ok(OrderStatus::Unknown),
        _ => Err(Error::Wire(tickwire_ipc::Error::InvalidEnum)),
    }
}

/// ORS-side SPSC writer encoding 512-byte `ReportRecord` replies onto a strategy's report ring
/// (with optional in-memory `ShmArenaWriter` strictly for oversize payloads).
pub struct ReportWriter {
    publisher: Publisher<1>,
    subscription: Option<SubscriptionKey>,
    arena: Option<ShmArenaWriter>,
    identity: ReportIdentity,
    next_report_seq: u64,
}

impl ReportWriter {
    #[forbid(unsafe_code)]
    pub fn new(publisher: Publisher<1>, identity: ReportIdentity) -> Result<Self> {
        Self::with_arena(publisher, None, None, identity)
    }

    #[forbid(unsafe_code)]
    pub fn with_arena(
        publisher: Publisher<1>,
        subscription: Option<SubscriptionKey>,
        arena: Option<ShmArenaWriter>,
        identity: ReportIdentity,
    ) -> Result<Self> {
        identity.validate(publisher.header())?;
        Ok(Self {
            publisher,
            subscription,
            arena,
            identity,
            next_report_seq: 1,
        })
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn publisher(&mut self) -> &mut Publisher<1> {
        &mut self.publisher
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn header(&self) -> &ResourceHeader {
        self.publisher.header()
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn identity(&self) -> &ReportIdentity {
        &self.identity
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn next_report_seq(&self) -> u64 {
        self.next_report_seq
    }

    /// Encode and publish a 512-byte `ReportRecord` (`message_type = 768 + body.report_kind`).
    #[forbid(unsafe_code)]
    pub fn publish_report(
        &mut self,
        mut body: ReportBody,
        instrument_version: u64,
        arena_payload: Option<&[u8]>,
        now_mono_ns: u64,
    ) -> Result<u64> {
        if !(1..=13).contains(&body.report_kind) {
            return Err(Error::Wire(tickwire_ipc::Error::InvalidEnum));
        }
        let message_type = 768 + body.report_kind;
        if body.strat_id != self.identity.strat_id
            && !(body.strat_id == ALL_STRATS && (777..=779).contains(&message_type))
        {
            return Err(Error::Identity);
        }

        let sequence = self
            .publisher
            .published()
            .checked_add(1)
            .filter(|&v| v < SEQUENCE_LIMIT)
            .ok_or(Error::Sequence)?;
        let report_seq = self.next_report_seq;
        let next_rep = report_seq
            .checked_add(1)
            .filter(|&v| v < SEQUENCE_LIMIT)
            .ok_or(Error::Sequence)?;

        let arena_meta = match arena_payload {
            None => None,
            Some(payload) => {
                let released = match self.subscription {
                    Some(sub) => self.publisher.cursor_watermarks(sub)?.released,
                    None => 0,
                };
                let arena = self.arena.as_mut().ok_or(Error::NotReady)?;
                Some(arena.write_payload(sequence, released, payload)?)
            }
        };

        let (
            payload_kind,
            payload_offset,
            payload_length,
            arena_class,
            allocation_seq,
            payload_crc32c,
        ) = match arena_meta {
            None => (0u16, 0u64, 0u32, 0u16, 0u64, 0u32),
            Some(meta) => (
                2u16,
                meta.payload_offset,
                meta.payload_length,
                meta.class_id,
                meta.allocation_seq,
                meta.payload_crc32c,
            ),
        };

        let bank = self.publisher.header();
        body.report_seq = report_seq;
        body.strategy_generation = self.identity.strategy_generation;
        body.exec_generation = bank.producer_generation;
        if body.event_mono_ns == 0 {
            body.event_mono_ns = now_mono_ns;
        }
        body.arena_offset = payload_offset;
        body.arena_len = payload_length;
        sanitize_report_body(&mut body, message_type);

        let flags = {
            let mut f = if now_mono_ns != 0 { 4u32 } else { 0u32 };
            if cfg!(feature = "crc32") {
                f |= 8;
            }
            f
        };

        #[cfg_attr(not(feature = "crc32"), allow(unused_mut))]
        let mut record = ReportRecord {
            header: RecordHeader {
                sequence,
                message_type,
                schema_major: 1,
                schema_minor: 0,
                header_bytes: 128,
                flags,
                payload_kind,
                body_bytes: 384,
                payload_offset,
                payload_length,
                arena_class,
                _pad_38: [0; 2],
                allocation_seq,
                topic_or_scope_id: bank.risk_scope,
                feed_generation: 0,
                producer_generation: bank.producer_generation,
                instrument_id: body.instrument_id,
                instrument_version,
                source_timestamp_ns: 0,
                ingress_mono_ns: 0,
                publish_mono_ns: now_mono_ns,
                record_crc32c: 0,
                payload_crc32c: if cfg!(feature = "crc32") {
                    payload_crc32c
                } else {
                    0
                },
                _pad_120: [0; 8],
            },
            body,
        };

        let mut image = [0u8; ReportRecord::SIZE];
        record.encode(&mut image)?;
        #[cfg(feature = "crc32")]
        {
            record.header.record_crc32c = tickwire_ipc::crc32c_masked(&image, 112..120);
            image[112..116].copy_from_slice(&record.header.record_crc32c.to_le_bytes());
        }
        tickwire_ipc::decode_record(&image)?;
        let published = self.publisher.publish(&[&image])?;
        self.next_report_seq = next_rep;
        Ok(published)
    }
}

#[forbid(unsafe_code)]
fn sanitize_report_body(b: &mut ReportBody, message_type: u16) {
    if message_type != report_kind::MSG_FILL {
        b.last_qty_lots = 0;
        b.last_price_ticks = 0;
        b.fee_units = 0;
        b.realized_pnl_units = 0;
        b.fee_asset_id = 0;
        b.liquidity = 0;
        b.amendment = 0;
        b.projected = 0;
        b.has_fee = 0;
        b.asset_exponent = 0;
        b.venue_trade_id_len = 0;
        b.venue_trade_id = [0; 32];
    } else if b.has_fee == 0 {
        b.fee_units = 0;
        b.fee_asset_id = 0;
        b.asset_exponent = 0;
    }
    if message_type != report_kind::MSG_EXCH_RATE_LIMIT {
        b.rate_limit_limit = 0;
        b.rate_limit_count = 0;
        b.rate_limit_interval_num = 0;
        b.rate_limit_type = 0;
        b.rate_limit_interval_unit = 0;
        b.hard_limit_hit = 0;
    }
    match message_type {
        report_kind::MSG_FRONT_ACK => {
            b.cumulative_qty_lots = 0;
            b.projected_qty_lots = 0;
        }
        report_kind::MSG_NACK => {
            b.cumulative_qty_lots = 0;
            b.projected_qty_lots = 0;
            b.leaves_qty_lots = 0;
        }
        report_kind::MSG_CANCEL_NACK => {
            b.projected_qty_lots = 0;
        }
        report_kind::MSG_EXCH_RATE_LIMIT
        | report_kind::MSG_ORS_SHUT_DOWN
        | report_kind::MSG_ORS_RESTRICTED => {
            b.quantity_lots = 0;
            b.cumulative_qty_lots = 0;
            b.projected_qty_lots = 0;
            b.leaves_qty_lots = 0;
            b.price_ticks = 0;
            b.side = 0;
            b.position_side = 0;
        }
        report_kind::MSG_RFQ_QUOTE_ACK => {
            b.cumulative_qty_lots = 0;
            b.projected_qty_lots = 0;
            b.position_side = 0;
        }
        report_kind::MSG_RFQ_QUOTE_NACK => {
            b.cumulative_qty_lots = 0;
            b.projected_qty_lots = 0;
            b.leaves_qty_lots = 0;
            b.position_side = 0;
        }
        _ => {}
    }
}

/// Strategy-side zero-copy SPSC consumer for 512-byte `ReportRecord` SHM slots.
pub struct ReportReceiver {
    reader: Subscriber<4>,
    bank: ResourceHeader,
    arena: Option<ShmArenaReader>,
    identity: ReportIdentity,
    last_report_seq: u64,
}

impl ReportReceiver {
    /// Attach to an authenticated `F_SEAL_FUTURE_WRITE` report ring `memfd`.
    ///
    /// # Safety
    /// The ORS execution process must be the sole writer obeying the `Publisher<1>` release protocol.
    #[allow(unsafe_code)]
    pub unsafe fn attach(
        fd: OwnedFd,
        bank: ResourceHeader,
        subscription: SubscriptionKey,
        arena: Option<ShmArenaReader>,
        identity: ReportIdentity,
    ) -> Result<Self> {
        identity.validate(&bank)?;
        if subscription.instance.to_le_bytes() != identity.instance_id {
            return Err(Error::Identity);
        }
        let first = bank.first_seq.checked_sub(1).ok_or(Error::Bounds)?;
        // SAFETY: caller guarantees the sole writer and exact registered cursor gate.
        let reader = unsafe { Subscriber::attach(fd, bank.clone(), subscription, first)? };
        Ok(Self {
            reader,
            bank,
            arena,
            identity,
            last_report_seq: 0,
        })
    }

    #[forbid(unsafe_code)]
    pub fn cursor_fd(&self) -> Result<OwnedFd> {
        Ok(self.reader.cursor_fd()?)
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn header(&self) -> &ResourceHeader {
        &self.bank
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn identity(&self) -> &ReportIdentity {
        &self.identity
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn last_report_seq(&self) -> u64 {
        self.last_report_seq
    }

    #[forbid(unsafe_code)]
    pub fn start_quiesce(&self) {
        self.reader.start_quiesce();
    }

    #[forbid(unsafe_code)]
    pub fn quiesced(&self) -> Result<tickwire_shm::Quiesced> {
        Ok(self.reader.quiesced()?)
    }

    /// Borrow and validate the next 512-byte `ReportRecord` in place (`ReportRecord::from_aligned_bytes`)
    /// without copying onto the stack, passing an optional retained arena slice while the slot guard is held.
    #[allow(unsafe_code)]
    pub fn poll_zero_copy<F>(&mut self, mut f: F) -> Result<bool>
    where
        F: FnMut(&ReportRecord, Option<&[u8]>) -> Result<()>,
    {
        let Some(guard) = self.reader.borrow()? else {
            return Ok(false);
        };
        let raw_arr: &[u8; ReportRecord::SIZE] =
            guard.bytes().try_into().map_err(|_| Error::Bounds)?;
        let record = ReportRecord::from_aligned_bytes(raw_arr)?;
        let h = &record.header;
        let b = &record.body;
        if !(report_kind::MSG_ACK..=report_kind::MSG_RFQ_QUOTE_NACK).contains(&h.message_type)
            || h.topic_or_scope_id != self.bank.risk_scope
            || h.producer_generation != self.bank.producer_generation
            || b.strategy_generation != self.identity.strategy_generation
            || b.exec_generation != self.bank.producer_generation
            || (b.strat_id != self.identity.strat_id
                && !(b.strat_id == ALL_STRATS && (777..=779).contains(&h.message_type)))
        {
            return Err(Error::Identity);
        }
        if b.report_seq != self.last_report_seq.checked_add(1).ok_or(Error::Sequence)? {
            return Err(Error::Sequence);
        }
        let payload_slice = if b.arena_len > 0 {
            let arena = self.arena.as_ref().ok_or(Error::NotReady)?;
            // SAFETY: `guard` holds the live ring borrow for `h.sequence`, preventing
            // `ShmArenaWriter` from reclaiming or mutating this arena block until `guard` drops.
            Some(unsafe { arena.read_retained_payload(h)? })
        } else {
            None
        };
        f(record, payload_slice)?;
        let seq = guard.sequence();
        let rep_seq = b.report_seq;
        drop(guard);
        self.reader.apply(seq)?;
        self.last_report_seq = rep_seq;
        Ok(true)
    }

    /// Poll and decode the next [`ExecutionReport`].
    #[forbid(unsafe_code)]
    pub fn poll(&mut self) -> Result<Option<ExecutionReport>> {
        let mut out = None;
        self.poll_zero_copy(|record, payload| {
            out = Some(ExecutionReport::from_wire(record, payload)?);
            Ok(())
        })?;
        Ok(out)
    }

    // Fallible polling distinguishes an empty live ring from an exhausted iterator.
    #[allow(clippy::should_implement_trait)]
    #[forbid(unsafe_code)]
    pub fn next(&mut self) -> Result<Option<ExecutionReport>> {
        self.poll()
    }
}

/// Create an in-process `(ReportWriter, ReportReceiver)` pair over a sealed `memfd` report ring.
#[allow(unsafe_code)]
pub fn in_process_report_pair(
    header: ResourceHeader,
    subscription_generation: u64,
    cursor_id: [u8; 16],
    identity: ReportIdentity,
) -> Result<(ReportWriter, ReportReceiver)> {
    in_process_report_pair_with_arena(header, None, subscription_generation, cursor_id, identity)
}

/// Create an in-process `(ReportWriter, ReportReceiver)` pair with an optional sealed `ShmArena`.
#[allow(unsafe_code)]
pub fn in_process_report_pair_with_arena(
    header: ResourceHeader,
    arena_header: Option<ResourceHeader>,
    subscription_generation: u64,
    cursor_id: [u8; 16],
    identity: ReportIdentity,
) -> Result<(ReportWriter, ReportReceiver)> {
    if cursor_id == [0; 16] || subscription_generation == 0 {
        return Err(Error::Identity);
    }
    let mut publisher = Publisher::<1>::create(header)?;
    let sub = SubscriptionKey {
        bank: publisher.key(),
        instance: u128::from_le_bytes(identity.instance_id),
        generation: subscription_generation,
        cursor: u128::from_le_bytes(cursor_id),
    };
    publisher.join(sub)?;
    let (arena_writer, arena_reader) = match arena_header {
        None => (None, None),
        Some(ah) => {
            let (w, r) = in_process_arena_pair(ah)?;
            (Some(w), Some(r))
        }
    };
    let bank = publisher.header().clone();
    let bank_fd = publisher.reader_fd()?;
    // SAFETY: `publisher` is freshly created and exclusively owned by `ReportWriter`.
    let receiver =
        unsafe { ReportReceiver::attach(bank_fd, bank, sub, arena_reader, identity.clone())? };
    let cursor_fd = receiver.cursor_fd()?;
    // SAFETY: `cursor_fd` is the exact `Subscriber` cursor descriptor for `sub`.
    unsafe {
        publisher.register_cursor(sub, cursor_fd)?;
    }
    let writer = ReportWriter::with_arena(publisher, Some(sub), arena_writer, identity)?;
    Ok((writer, receiver))
}
