//! Strategy-side terminal-reason classification and monotonic pure-`Fixed` fill watermark
//! tracking across `Execution`, `Projected`, and post-trade `FeeAmendment` reports.
//!
//! All watermark and incremental price calculations operate strictly over checked
//! `tickwire_risk::Fixed` integers with zero floating-point arithmetic and zero `unsafe` code.
#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use tickwire_account::{OrderKey, ProjectedFill, ProjectedPriceInput};
use tickwire_risk::{Fixed, RiskError, Rounding};
use tickwire_venue as v;

use crate::{
    Error, Result,
    reports::{ExecutionReport, ExecutionReportKind, FillFee, FillKind, FillReport},
};

const MAX_FIXED_SCALE: u8 = 38;

/// Default preallocated active/recent order capacity in [`FillWatermarkTracker`].
pub const DEFAULT_MAX_TRACKED_ORDERS: usize = 1024;
/// Default bounded trade-ID deduplication window in [`FillWatermarkTracker`].
pub const DEFAULT_MAX_TRACKED_TRADES: usize = 4096;

/// Optional order-parameter hints used to disambiguate generic venue status or rejection events
/// (for example, distinguishing post-only `GTX` expiry from `IOC`/`FOK` expiry).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OrderClassificationHints<'a> {
    pub time_in_force: Option<v::TimeInForce>,
    pub post_only: bool,
    pub stp_mode_or_reason: Option<&'a str>,
}

impl<'a> OrderClassificationHints<'a> {
    #[inline]
    pub const fn new() -> Self {
        Self {
            time_in_force: None,
            post_only: false,
            stp_mode_or_reason: None,
        }
    }

    #[inline]
    pub const fn with_time_in_force(mut self, tif: v::TimeInForce) -> Self {
        self.time_in_force = Some(tif);
        self
    }

    #[inline]
    pub const fn with_post_only(mut self, post_only: bool) -> Self {
        self.post_only = post_only;
        self
    }

    #[inline]
    pub const fn with_stp_mode_or_reason(mut self, token: &'a str) -> Self {
        self.stp_mode_or_reason = Some(token);
        self
    }
}

/// Classify a terminal [`v::OrderStatus`] (returning `None` for non-terminal statuses).
pub fn classify_order_status(
    venue: u32,
    status: v::OrderStatus,
    hints: OrderClassificationHints<'_>,
) -> Option<v::TerminalReason> {
    match status {
        v::OrderStatus::Filled => Some(v::TerminalReason::Filled),
        v::OrderStatus::Canceled => {
            if let Some(internal) = hints
                .stp_mode_or_reason
                .and_then(classify_internal_guard_token)
            {
                return Some(internal);
            }
            if venue == v::venue_id::BINANCE {
                Some(v::classify_binance_reason(
                    None,
                    "CANCELED",
                    hints.stp_mode_or_reason,
                    hints.time_in_force,
                    hints.post_only,
                ))
            } else {
                Some(v::classify_spot_venue_reason(
                    None,
                    "CANCELED",
                    hints.stp_mode_or_reason.unwrap_or(""),
                    hints.post_only,
                    hints.time_in_force,
                ))
            }
        }
        v::OrderStatus::Expired => {
            if let Some(internal) = hints
                .stp_mode_or_reason
                .and_then(classify_internal_guard_token)
            {
                return Some(internal);
            }
            Some(v::classify_binance_reason(
                None,
                "EXPIRED",
                hints.stp_mode_or_reason,
                hints.time_in_force,
                hints.post_only,
            ))
        }
        v::OrderStatus::Rejected => {
            if let Some(internal) = hints
                .stp_mode_or_reason
                .and_then(classify_internal_guard_token)
            {
                return Some(internal);
            }
            if venue == v::venue_id::BINANCE {
                Some(v::classify_binance_reason(
                    None,
                    "REJECTED",
                    hints.stp_mode_or_reason,
                    hints.time_in_force,
                    hints.post_only,
                ))
            } else {
                Some(v::classify_spot_venue_reason(
                    None,
                    "REJECTED",
                    hints.stp_mode_or_reason.unwrap_or(""),
                    hints.post_only,
                    hints.time_in_force,
                ))
            }
        }
        v::OrderStatus::Pending
        | v::OrderStatus::New
        | v::OrderStatus::PartiallyFilled
        | v::OrderStatus::Unknown
        | v::OrderStatus::Unreported => None,
    }
}

/// Zero-allocation classifier for venue and gateway rejection codes/messages across
/// Binance (`venue_id::BINANCE`) and spot venues (`venue_id::BYBIT`, `venue_id::OKX`).
pub fn classify_venue_rejection(
    venue: u32,
    venue_code: &str,
    message: &str,
    hints: OrderClassificationHints<'_>,
) -> v::TerminalReason {
    if let Some(reason) = classify_internal_guard_token(venue_code)
        .or_else(|| classify_internal_guard_token(message))
        .or_else(|| {
            hints
                .stp_mode_or_reason
                .and_then(classify_internal_guard_token)
        })
    {
        return reason;
    }

    let code_trimmed = venue_code.trim();
    let parsed_code = code_trimmed.parse::<i64>().ok();

    if venue == v::venue_id::BINANCE {
        let primary = v::classify_binance_reason(
            parsed_code,
            message,
            hints.stp_mode_or_reason.or(Some(code_trimmed)),
            hints.time_in_force,
            hints.post_only,
        );
        if !matches!(
            primary,
            v::TerminalReason::Unknown | v::TerminalReason::VenueRejected
        ) {
            return primary;
        }
        if parsed_code.is_none() {
            let from_code = v::classify_binance_reason(
                None,
                code_trimmed,
                hints.stp_mode_or_reason,
                hints.time_in_force,
                hints.post_only,
            );
            if !matches!(
                from_code,
                v::TerminalReason::Unknown | v::TerminalReason::VenueRejected
            ) {
                return from_code;
            }
        }
        if primary == v::TerminalReason::Unknown && !code_trimmed.is_empty() && code_trimmed != "0"
        {
            return v::TerminalReason::VenueRejected;
        }
        primary
    } else {
        v::classify_spot_venue_reason(
            parsed_code,
            code_trimmed,
            message,
            hints.post_only,
            hints.time_in_force,
        )
    }
}

#[inline]
fn contains_ascii_ignore_case(haystack: &str, needle: &str) -> bool {
    let h = haystack.as_bytes();
    let n = needle.as_bytes();
    !n.is_empty() && h.len() >= n.len() && h.windows(n.len()).any(|w| w.eq_ignore_ascii_case(n))
}

fn classify_internal_guard_token(text: &str) -> Option<v::TerminalReason> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    if contains_ascii_ignore_case(trimmed, "aborted_before_send")
        || contains_ascii_ignore_case(trimmed, "aborted before send")
    {
        return Some(v::TerminalReason::AbortedBeforeSend);
    }
    if contains_ascii_ignore_case(trimmed, "crosses_book")
        || contains_ascii_ignore_case(trimmed, "crosses book")
        || contains_ascii_ignore_case(trimmed, "cross_book")
    {
        return Some(v::TerminalReason::CrossesBook);
    }
    if contains_ascii_ignore_case(trimmed, "too_many_pending")
        || contains_ascii_ignore_case(trimmed, "too many pending")
        || contains_ascii_ignore_case(trimmed, "modify_in_flight")
    {
        return Some(v::TerminalReason::TooManyPendingActions);
    }
    None
}

/// Snapshot of settled and projected fill watermarks for a single tracked order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderFillWatermarks {
    pub confirmed_filled: Fixed,
    pub projected_filled: Fixed,
    pub cumulative_observed: Fixed,
    pub unsettled_projected: Fixed,
}

/// Incremental inventory-skew delta emitted when an order's effective fill watermark advances.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectiveFillDelta {
    pub delta_quantity: Fixed,
    pub estimated_price: Option<Fixed>,
    pub confirmed_filled: Fixed,
    pub projected_filled: Fixed,
    pub is_projected: bool,
}

/// Settled accounting and remaining unprojected skew delta returned by [`FillWatermarkTracker::on_fill`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettledFillDelta {
    pub fill_quantity: Fixed,
    pub fill_price: Fixed,
    pub effective_delta_quantity: Fixed,
    pub already_projected: Fixed,
    pub unprojected_delta: Fixed,
    pub confirmed_filled: Fixed,
    pub projected_filled: Fixed,
    pub duplicate: bool,
}

impl SettledFillDelta {
    /// Converts any positive unprojected fill portion into a unified [`EffectiveFillDelta`]
    /// for quote-skew consumers; returns `None` when the entire fill was already projected
    /// or was a duplicate `trade_id`.
    #[inline]
    pub fn into_effective_delta(self) -> Option<EffectiveFillDelta> {
        (self.unprojected_delta.atoms() > 0).then_some(EffectiveFillDelta {
            delta_quantity: self.unprojected_delta,
            estimated_price: Some(self.fill_price),
            confirmed_filled: self.confirmed_filled,
            projected_filled: self.projected_filled,
            is_projected: false,
        })
    }
}

/// Post-trade fee/rebate adjustment observed via `FillKind::FeeAmendment` (`amendment == true`);
/// fill quantity watermarks remain unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeeAmendmentOutcome {
    pub exec_ref: u64,
    pub trade_id: Option<v::VenueTradeId>,
    pub fee: FillFee,
    pub confirmed_filled: Fixed,
    pub projected_filled: Fixed,
}

/// Unified outcome returned by [`FillWatermarkTracker::on_fill_report`] and
/// [`FillWatermarkTracker::on_execution_report`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReportFillOutcome {
    Execution(SettledFillDelta),
    Projected(Option<EffectiveFillDelta>),
    FeeAmendment(FeeAmendmentOutcome),
    NonFill(Option<EffectiveFillDelta>),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TradeDedupKey {
    order: OrderKey,
    trade_id: Box<str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OrderWatermarkEntry {
    order: OrderKey,
    created_seq: u64,
    base_scale: u8,
    price_scale: u8,
    limit_price: Option<Fixed>,
    confirmed_filled: Fixed,
    exch_cum_qty: Fixed,
    exch_cum_quote: Option<Fixed>,
    exch_quote_qty: Fixed,
    exch_avg_price: Option<Fixed>,
    exch_avg_qty: Fixed,
    projected_cum_qty: Fixed,
    retired: bool,
}

/// Bounded, pure-`Fixed` strategy-side fill watermark tracker.
///
/// Reconciles early volatile cumulative-quantity observations (`FillKind::Projected`,
/// `ProjectedFill`, `OrderObservation`, or `Fixed` cumulative quantities) with authoritative
/// deduplicated `FillKind::Execution` fills while keeping post-trade `FillKind::FeeAmendment`
/// fee/rebate updates strictly decoupled from quantity watermarks (`0` double-counting).
pub struct FillWatermarkTracker {
    max_orders: usize,
    max_trades: usize,
    next_seq: u64,
    entries: Vec<OrderWatermarkEntry>,
    index: BTreeMap<OrderKey, usize>,
    seen_trades_ring: VecDeque<TradeDedupKey>,
    seen_trades_set: BTreeSet<TradeDedupKey>,
}

impl Default for FillWatermarkTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl FillWatermarkTracker {
    #[inline]
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_MAX_TRACKED_ORDERS, DEFAULT_MAX_TRACKED_TRADES)
            .expect("default FillWatermarkTracker capacities are non-zero")
    }

    pub fn with_capacity(max_orders: usize, max_trades: usize) -> Result<Self> {
        if max_orders == 0 || max_trades == 0 {
            return Err(Error::Bounds);
        }
        Ok(Self {
            max_orders,
            max_trades,
            next_seq: 0,
            entries: Vec::with_capacity(max_orders),
            index: BTreeMap::new(),
            seen_trades_ring: VecDeque::with_capacity(max_trades),
            seen_trades_set: BTreeSet::new(),
        })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[inline]
    pub fn max_orders(&self) -> usize {
        self.max_orders
    }

    #[inline]
    pub fn max_trades(&self) -> usize {
        self.max_trades
    }

    /// Pre-registers an order's base/price scales and optional fallback limit price.
    pub fn register_order(
        &mut self,
        order: OrderKey,
        base_scale: u8,
        price_scale: u8,
        limit_price: Option<Fixed>,
    ) -> Result<()> {
        if base_scale > MAX_FIXED_SCALE || price_scale > MAX_FIXED_SCALE {
            return Err(Error::Risk(RiskError::InvalidScale));
        }
        if let Some(px) = limit_price {
            px.positive()?;
            if px.scale() != price_scale {
                return Err(Error::Risk(RiskError::ScaleMismatch));
            }
        }
        let idx = self.ensure_entry(&order, base_scale, price_scale, limit_price)?;
        if let Some(px) = limit_price {
            self.entries[idx].limit_price = Some(px);
        }
        Ok(())
    }

    /// Sum of deduplicated settled `Execution` fill quantities for `order`.
    #[inline]
    pub fn confirmed_filled(&self, order: &OrderKey) -> Option<Fixed> {
        let idx = *self.index.get(order)?;
        Some(self.entries[idx].confirmed_filled)
    }

    /// Current monotonic fill watermark `max(confirmed_filled, cumulative_observed)` for `order`.
    #[inline]
    pub fn projected_filled(&self, order: &OrderKey) -> Option<Fixed> {
        let idx = *self.index.get(order)?;
        let e = &self.entries[idx];
        e.projected_cum_qty.max(e.confirmed_filled).ok()
    }

    /// Currently projected quantity not yet backed by settled `Execution` fills
    /// (`projected_filled - confirmed_filled >= 0`).
    #[inline]
    pub fn unsettled_projected(&self, order: &OrderKey) -> Option<Fixed> {
        let idx = *self.index.get(order)?;
        let e = &self.entries[idx];
        let proj = e.projected_cum_qty.max(e.confirmed_filled).ok()?;
        proj.checked_sub(e.confirmed_filled).ok()
    }

    /// Full watermark snapshot for `order`.
    pub fn watermarks(&self, order: &OrderKey) -> Option<OrderFillWatermarks> {
        let idx = *self.index.get(order)?;
        let e = &self.entries[idx];
        let projected_filled = e.projected_cum_qty.max(e.confirmed_filled).ok()?;
        let unsettled_projected = projected_filled.checked_sub(e.confirmed_filled).ok()?;
        Some(OrderFillWatermarks {
            confirmed_filled: e.confirmed_filled,
            projected_filled,
            cumulative_observed: e.exch_cum_qty,
            unsettled_projected,
        })
    }

    /// Marks an order slot as retired so it becomes eligible for FIFO eviction when
    /// `max_orders` is reached while keeping its watermarks readable in the meantime.
    pub fn mark_retired(&mut self, order: &OrderKey) -> bool {
        let Some(&idx) = self.index.get(order) else {
            return false;
        };
        self.entries[idx].retired = true;
        true
    }

    /// Removes `order` and purges any retained trade-ID deduplication entries for it.
    pub fn remove_order(&mut self, order: &OrderKey) -> bool {
        let Some(idx) = self.index.remove(order) else {
            return false;
        };
        let evicted = self.entries.swap_remove(idx);
        if idx < self.entries.len() {
            let moved = self.entries[idx].order.clone();
            self.index.insert(moved, idx);
        }
        self.purge_order_trades(&evicted.order);
        true
    }

    /// Feeds a decoded SHM [`FillReport`] (`Execution`, `Projected`, or `FeeAmendment`) into the
    /// watermark tracker using exact integer lot/tick scaling.
    pub fn on_fill_report(
        &mut self,
        order: &OrderKey,
        fill: &FillReport,
        lot_step: v::Decimal,
        price_tick: v::Decimal,
        base_scale: u8,
        price_scale: u8,
    ) -> Result<ReportFillOutcome> {
        match fill.kind {
            FillKind::FeeAmendment => {
                let fee = fill.fee.ok_or(Error::Input)?;
                let idx = self.ensure_entry(order, base_scale, price_scale, None)?;
                let entry = &self.entries[idx];
                let projected_filled = entry.projected_cum_qty.max(entry.confirmed_filled)?;
                Ok(ReportFillOutcome::FeeAmendment(FeeAmendmentOutcome {
                    exec_ref: fill.exec_ref,
                    trade_id: fill.trade_id.clone(),
                    fee,
                    confirmed_filled: entry.confirmed_filled,
                    projected_filled,
                }))
            }
            FillKind::Projected => {
                let lots = fill.projected_qty_lots.max(fill.cumulative_qty_lots);
                if lots < 0 {
                    return Err(Error::Risk(RiskError::InvalidInput));
                }
                let cum_qty = lots_to_fixed_exact(lots, lot_step, base_scale)?;
                let est_px = if fill.last_price_ticks > 0 {
                    Some(lots_to_fixed_exact(
                        fill.last_price_ticks,
                        price_tick,
                        price_scale,
                    )?)
                } else {
                    None
                };
                let delta = self.on_cumulative_quantity(
                    order,
                    cum_qty,
                    ProjectedPriceInput::LimitFallback,
                    est_px,
                    price_scale,
                )?;
                Ok(ReportFillOutcome::Projected(delta))
            }
            FillKind::Execution => {
                if fill.last_qty_lots <= 0 || fill.last_price_ticks <= 0 {
                    return Err(Error::Risk(RiskError::InvalidInput));
                }
                let last_qty = lots_to_fixed_exact(fill.last_qty_lots, lot_step, base_scale)?;
                let last_px = lots_to_fixed_exact(fill.last_price_ticks, price_tick, price_scale)?;
                let tid = fill.trade_id.as_ref().map(v::VenueTradeId::as_str);
                let settled = self.on_fill_fixed(order, tid, last_qty, last_px)?;
                Ok(ReportFillOutcome::Execution(settled))
            }
        }
    }

    /// Feeds an inbound SHM [`ExecutionReport`] into the tracker, handling `Fill` (`Execution`,
    /// `Projected`, `FeeAmendment`), cumulative status advancements on `Ack`/`Modified`/`Canceled`,
    /// and automatic slot retirement on terminal lifecycle states.
    pub fn on_execution_report(
        &mut self,
        order: &OrderKey,
        report: &ExecutionReport,
        lot_step: v::Decimal,
        price_tick: v::Decimal,
        base_scale: u8,
        price_scale: u8,
    ) -> Result<ReportFillOutcome> {
        let outcome = match &report.kind {
            ExecutionReportKind::Fill(fill) => {
                self.on_fill_report(order, fill, lot_step, price_tick, base_scale, price_scale)?
            }
            ExecutionReportKind::Ack
            | ExecutionReportKind::Modified
            | ExecutionReportKind::Canceled => {
                let limit_px = if report.price_ticks > 0 {
                    Some(lots_to_fixed_exact(
                        report.price_ticks,
                        price_tick,
                        price_scale,
                    )?)
                } else {
                    None
                };
                let cum_lots = report.projected_qty_lots.max(report.cumulative_qty_lots);
                if cum_lots > 0 {
                    let cum_qty = lots_to_fixed_exact(cum_lots, lot_step, base_scale)?;
                    let delta = self.on_cumulative_quantity(
                        order,
                        cum_qty,
                        ProjectedPriceInput::LimitFallback,
                        limit_px,
                        price_scale,
                    )?;
                    ReportFillOutcome::NonFill(delta)
                } else {
                    if limit_px.is_some() {
                        self.ensure_entry(order, base_scale, price_scale, limit_px)?;
                    }
                    ReportFillOutcome::NonFill(None)
                }
            }
            _ => ReportFillOutcome::NonFill(None),
        };
        if report.is_order_lifecycle_terminal() {
            self.mark_retired(order);
        }
        Ok(outcome)
    }

    /// Feeds an [`Account::on_cum_qty`](tickwire_account::Account::on_cum_qty) [`ProjectedFill`]
    /// into the strategy watermark tracker.
    pub fn on_projected_fill(
        &mut self,
        fill: &ProjectedFill,
    ) -> Result<Option<EffectiveFillDelta>> {
        fill.gap_quantity.nonnegative()?;
        fill.order_projected.nonnegative()?;
        fill.order_filled.nonnegative()?;
        fill.gap_price.nonnegative()?;
        let base_scale = fill.order_projected.scale();
        if fill.gap_quantity.scale() != base_scale || fill.order_filled.scale() != base_scale {
            return Err(Error::Risk(RiskError::ScaleMismatch));
        }
        let price_scale = fill.gap_price.scale();
        let fallback_px = (fill.gap_price.atoms() > 0).then_some(fill.gap_price);
        let idx = self.ensure_entry(&fill.order, base_scale, price_scale, fallback_px)?;
        let entry = &mut self.entries[idx];

        if fill.order_projected.atoms() < entry.exch_cum_qty.atoms() {
            return Ok(None);
        }
        let watermark = entry.projected_cum_qty.max(entry.confirmed_filled)?;
        let gap = fill.order_projected.checked_sub(watermark)?;
        if gap.atoms() <= 0 {
            if fill.order_projected.atoms() > entry.exch_cum_qty.atoms() {
                entry.exch_cum_qty = fill.order_projected;
            }
            return Ok(None);
        }

        entry.exch_cum_qty = fill.order_projected;
        entry.projected_cum_qty = fill.order_projected;
        let estimated_price = fallback_px.or(entry.limit_price);
        Ok(Some(EffectiveFillDelta {
            delta_quantity: gap,
            estimated_price,
            confirmed_filled: entry.confirmed_filled,
            projected_filled: fill.order_projected,
            is_projected: true,
        }))
    }

    /// Feeds an [`v::OrderObservation`] (`cumulative_quantity`, `cumulative_quote`, `order_price`)
    /// into the strategy watermark tracker.
    pub fn on_order_observation(
        &mut self,
        order: &OrderKey,
        observation: &v::OrderObservation,
        base_scale: u8,
        price_scale: u8,
    ) -> Result<Option<EffectiveFillDelta>> {
        let limit_price = match observation.order_price {
            Some(px_dec) if px_dec.is_positive() => {
                let px = decimal_to_fixed_half_even(px_dec, price_scale)?;
                (px.atoms() > 0).then_some(px)
            }
            _ => None,
        };
        self.ensure_entry(order, base_scale, price_scale, limit_price)?;

        let Some(cum_dec) = observation.cumulative_quantity else {
            return Ok(None);
        };
        let cum_qty = decimal_to_fixed_exact(cum_dec, base_scale)?;

        let price_input = match observation.cumulative_quote {
            Some(q_dec) if q_dec.is_positive() => {
                let quote_scale = (-q_dec.exponent()).clamp(0, MAX_FIXED_SCALE as i8) as u8;
                let cum_quote = decimal_to_fixed_half_even(q_dec, quote_scale)?;
                if cum_quote.atoms() > 0 {
                    ProjectedPriceInput::CumulativeQuote { cum_quote }
                } else {
                    ProjectedPriceInput::LimitFallback
                }
            }
            _ => ProjectedPriceInput::LimitFallback,
        };

        self.on_cumulative_quantity(order, cum_qty, price_input, limit_price, price_scale)
    }

    /// Evaluates a `Fixed` cumulative exchange quantity and [`ProjectedPriceInput`] directly.
    pub fn on_cumulative_quantity(
        &mut self,
        order: &OrderKey,
        cum_qty: Fixed,
        price_input: ProjectedPriceInput,
        fallback_limit_price: Option<Fixed>,
        price_scale: u8,
    ) -> Result<Option<EffectiveFillDelta>> {
        cum_qty.nonnegative()?;
        if price_scale > MAX_FIXED_SCALE {
            return Err(Error::Risk(RiskError::InvalidScale));
        }
        if let Some(px) = fallback_limit_price {
            px.positive()?;
            if px.scale() != price_scale {
                return Err(Error::Risk(RiskError::ScaleMismatch));
            }
        }
        match price_input {
            ProjectedPriceInput::CumulativeQuote { cum_quote } => {
                cum_quote.nonnegative()?;
            }
            ProjectedPriceInput::AveragePrice { avg_price } => {
                avg_price.nonnegative()?;
            }
            ProjectedPriceInput::LimitFallback => {}
        }

        let idx = self.ensure_entry(order, cum_qty.scale(), price_scale, fallback_limit_price)?;
        let entry = &mut self.entries[idx];
        if let Some(px) = fallback_limit_price {
            entry.limit_price = Some(px);
        }

        if cum_qty.atoms() < entry.exch_cum_qty.atoms() {
            return Ok(None);
        }

        let watermark = entry.projected_cum_qty.max(entry.confirmed_filled)?;
        let gap = cum_qty.checked_sub(watermark)?;
        if gap.atoms() <= 0 {
            if cum_qty.atoms() >= entry.exch_cum_qty.atoms() {
                if cum_qty.atoms() > entry.exch_cum_qty.atoms() {
                    entry.exch_cum_qty = cum_qty;
                }
                if cum_qty.atoms() > 0 {
                    match price_input {
                        ProjectedPriceInput::CumulativeQuote { cum_quote }
                            if cum_quote.atoms() > 0
                                && cum_qty.atoms() >= entry.exch_quote_qty.atoms() =>
                        {
                            entry.exch_cum_quote = Some(cum_quote);
                            entry.exch_quote_qty = cum_qty;
                        }
                        ProjectedPriceInput::AveragePrice { avg_price }
                            if avg_price.atoms() > 0
                                && cum_qty.atoms() >= entry.exch_avg_qty.atoms() =>
                        {
                            entry.exch_avg_price = Some(avg_price);
                            entry.exch_avg_qty = cum_qty;
                        }
                        _ => {}
                    }
                }
            }
            return Ok(None);
        }

        let fallback_px =
            resolve_fallback_price(entry.limit_price, entry.exch_avg_price, price_scale)?;
        let estimated_price = estimate_gap_price(
            cum_qty,
            entry.exch_cum_quote,
            entry.exch_quote_qty,
            entry.exch_avg_price,
            entry.exch_avg_qty,
            price_input,
            fallback_px,
            price_scale,
        )?;

        entry.exch_cum_qty = cum_qty;
        entry.projected_cum_qty = cum_qty;
        match price_input {
            ProjectedPriceInput::CumulativeQuote { cum_quote } if cum_quote.atoms() > 0 => {
                entry.exch_cum_quote = Some(cum_quote);
                entry.exch_quote_qty = cum_qty;
            }
            ProjectedPriceInput::AveragePrice { avg_price } if avg_price.atoms() > 0 => {
                entry.exch_avg_price = Some(avg_price);
                entry.exch_avg_qty = cum_qty;
            }
            _ => {}
        }

        Ok(Some(EffectiveFillDelta {
            delta_quantity: gap,
            estimated_price,
            confirmed_filled: entry.confirmed_filled,
            projected_filled: cum_qty,
            is_projected: true,
        }))
    }

    /// Applies an authoritative [`v::FillEvent`], deduplicating by `trade_id` and returning
    /// both settled accounting and the remaining unprojected skew delta.
    pub fn on_fill(
        &mut self,
        order: &OrderKey,
        fill: &v::FillEvent,
        base_scale: u8,
        price_scale: u8,
    ) -> Result<SettledFillDelta> {
        let last_qty = decimal_to_fixed_exact(fill.last_quantity, base_scale)?;
        let last_px = decimal_to_fixed_exact(fill.last_price, price_scale)?;
        self.on_fill_fixed(order, Some(fill.trade_id.as_str()), last_qty, last_px)
    }

    /// Applies an authoritative `Fixed` trade execution directly.
    pub fn on_fill_fixed(
        &mut self,
        order: &OrderKey,
        trade_id: Option<&str>,
        last_quantity: Fixed,
        last_price: Fixed,
    ) -> Result<SettledFillDelta> {
        last_quantity.positive()?;
        last_price.positive()?;

        let idx = self.ensure_entry(
            order,
            last_quantity.scale(),
            last_price.scale(),
            Some(last_price),
        )?;

        let normalized_tid = trade_id
            .map(str::trim)
            .filter(|tid| !tid.is_empty() && *tid != "0");

        if let Some(tid) = normalized_tid {
            let dedup_key = TradeDedupKey {
                order: order.clone(),
                trade_id: tid.into(),
            };
            if self.seen_trades_set.contains(&dedup_key) {
                let entry = &self.entries[idx];
                let zero = Fixed::zero(entry.base_scale)?;
                let projected_filled = entry.projected_cum_qty.max(entry.confirmed_filled)?;
                return Ok(SettledFillDelta {
                    fill_quantity: zero,
                    fill_price: last_price,
                    effective_delta_quantity: zero,
                    already_projected: zero,
                    unprojected_delta: zero,
                    confirmed_filled: entry.confirmed_filled,
                    projected_filled,
                    duplicate: true,
                });
            }
            if self.seen_trades_ring.len() == self.max_trades
                && let Some(oldest) = self.seen_trades_ring.pop_front()
            {
                self.seen_trades_set.remove(&oldest);
            }
            self.seen_trades_ring.push_back(dedup_key.clone());
            self.seen_trades_set.insert(dedup_key);
        }

        let entry = &mut self.entries[idx];
        let watermark_before = entry.projected_cum_qty.max(entry.confirmed_filled)?;
        let confirmed_after = entry.confirmed_filled.checked_add(last_quantity)?;
        let watermark_after = watermark_before.max(confirmed_after)?;
        let unprojected_delta = watermark_after.checked_sub(watermark_before)?;
        let already_projected = last_quantity.checked_sub(unprojected_delta)?;

        entry.confirmed_filled = confirmed_after;
        entry.projected_cum_qty = watermark_after;

        Ok(SettledFillDelta {
            fill_quantity: last_quantity,
            fill_price: last_price,
            effective_delta_quantity: unprojected_delta,
            already_projected,
            unprojected_delta,
            confirmed_filled: confirmed_after,
            projected_filled: watermark_after,
            duplicate: false,
        })
    }

    fn ensure_entry(
        &mut self,
        order: &OrderKey,
        base_scale: u8,
        price_scale: u8,
        limit_price: Option<Fixed>,
    ) -> Result<usize> {
        if let Some(&idx) = self.index.get(order) {
            let existing = &self.entries[idx];
            if existing.base_scale != base_scale || existing.price_scale != price_scale {
                return Err(Error::Risk(RiskError::ScaleMismatch));
            }
            return Ok(idx);
        }

        if self.entries.len() == self.max_orders {
            let Some(evict_idx) = self
                .entries
                .iter()
                .enumerate()
                .filter(|(_, e)| e.retired)
                .min_by_key(|(_, e)| e.created_seq)
                .map(|(i, _)| i)
            else {
                return Err(Error::Bounds);
            };
            let evicted = self.entries.swap_remove(evict_idx);
            self.index.remove(&evicted.order);
            if evict_idx < self.entries.len() {
                let moved = self.entries[evict_idx].order.clone();
                self.index.insert(moved, evict_idx);
            }
            self.purge_order_trades(&evicted.order);
        }

        let zero = Fixed::zero(base_scale)?;
        let created_seq = self.next_seq;
        self.next_seq = self.next_seq.checked_add(1).ok_or(Error::Bounds)?;
        let idx = self.entries.len();
        self.entries.push(OrderWatermarkEntry {
            order: order.clone(),
            created_seq,
            base_scale,
            price_scale,
            limit_price,
            confirmed_filled: zero,
            exch_cum_qty: zero,
            exch_cum_quote: None,
            exch_quote_qty: zero,
            exch_avg_price: None,
            exch_avg_qty: zero,
            projected_cum_qty: zero,
            retired: false,
        });
        self.index.insert(order.clone(), idx);
        Ok(idx)
    }

    fn purge_order_trades(&mut self, order: &OrderKey) {
        self.seen_trades_ring.retain(|k| &k.order != order);
        self.seen_trades_set.retain(|k| &k.order != order);
    }
}

fn lots_to_fixed_exact(units: i64, step: v::Decimal, target_scale: u8) -> Result<Fixed> {
    if !step.is_positive() {
        return Err(Error::Risk(RiskError::InvalidInput));
    }
    let coeff = i128::from(units)
        .checked_mul(step.coefficient())
        .ok_or(Error::Risk(RiskError::ArithmeticOverflow))?;
    let dec = v::Decimal::new(coeff, step.exponent())?;
    decimal_to_fixed_exact(dec, target_scale)
}

/// Converts a [`v::Decimal`] to [`Fixed`] at `target_scale` without loss of precision.
pub fn decimal_to_fixed_exact(value: v::Decimal, target_scale: u8) -> Result<Fixed> {
    if target_scale > MAX_FIXED_SCALE {
        return Err(Error::Risk(RiskError::InvalidScale));
    }
    if value.coefficient() == 0 {
        return Ok(Fixed::zero(target_scale)?);
    }
    let shift = i32::from(value.exponent()) + i32::from(target_scale);
    if shift < 0 {
        return Err(Error::Bounds);
    }
    let factor = 10i128
        .checked_pow(shift as u32)
        .ok_or(Error::Risk(RiskError::ArithmeticOverflow))?;
    let atoms = value
        .coefficient()
        .checked_mul(factor)
        .ok_or(Error::Risk(RiskError::ArithmeticOverflow))?;
    Ok(Fixed::from_atoms(atoms, target_scale)?)
}

/// Converts a nonnegative [`v::Decimal`] to [`Fixed`] at `target_scale`, using exact
/// integer `round_half_even` when `value` carries extra decimal digits beyond `target_scale`.
pub fn decimal_to_fixed_half_even(value: v::Decimal, target_scale: u8) -> Result<Fixed> {
    if target_scale > MAX_FIXED_SCALE {
        return Err(Error::Risk(RiskError::InvalidScale));
    }
    if value.coefficient() < 0 {
        return Err(Error::Risk(RiskError::InvalidInput));
    }
    if value.coefficient() == 0 {
        return Ok(Fixed::zero(target_scale)?);
    }
    let shift = i32::from(value.exponent()) + i32::from(target_scale);
    let coeff = value.coefficient() as u128;
    let atoms_u128 = if shift >= 0 {
        let factor = 10u128
            .checked_pow(shift as u32)
            .ok_or(Error::Risk(RiskError::ArithmeticOverflow))?;
        coeff
            .checked_mul(factor)
            .ok_or(Error::Risk(RiskError::ArithmeticOverflow))?
    } else {
        let den = 10u128
            .checked_pow((-shift) as u32)
            .ok_or(Error::Risk(RiskError::ArithmeticOverflow))?;
        div_round_half_even_u256((0, coeff), den)?
    };
    let atoms =
        i128::try_from(atoms_u128).map_err(|_| Error::Risk(RiskError::ArithmeticOverflow))?;
    Ok(Fixed::from_atoms(atoms, target_scale)?)
}

fn resolve_fallback_price(
    limit_price: Option<Fixed>,
    prev_avg: Option<Fixed>,
    price_scale: u8,
) -> Result<Option<Fixed>> {
    if let Some(px) = limit_price
        && px.atoms() > 0
    {
        return Ok(Some(rescale_half_even(px, price_scale)?));
    }
    if let Some(avg) = prev_avg
        && avg.atoms() > 0
    {
        return Ok(Some(rescale_half_even(avg, price_scale)?));
    }
    Ok(None)
}

#[allow(clippy::too_many_arguments)]
fn estimate_gap_price(
    cum_qty: Fixed,
    prev_cum_quote: Option<Fixed>,
    prev_quote_qty: Fixed,
    prev_avg_price: Option<Fixed>,
    prev_avg_qty: Fixed,
    price_input: ProjectedPriceInput,
    fallback_px: Option<Fixed>,
    price_scale: u8,
) -> Result<Option<Fixed>> {
    match price_input {
        ProjectedPriceInput::CumulativeQuote { cum_quote } => {
            if cum_quote.atoms() == 0 {
                return Ok(fallback_px);
            }
            if let Some(prev_q) = prev_cum_quote
                && prev_quote_qty.atoms() > 0
                && cum_qty.atoms() > prev_quote_qty.atoms()
            {
                let quote_delta_qty = cum_qty.checked_sub(prev_quote_qty)?;
                let s = cum_quote.scale().max(prev_q.scale());
                let cur_q = cum_quote.rescale(s, Rounding::Floor)?;
                let old_q = prev_q.rescale(s, Rounding::Floor)?;
                let delta_q = cur_q.checked_sub(old_q)?;
                if delta_q.atoms() <= 0 {
                    Ok(fallback_px)
                } else {
                    let px = div_half_even(delta_q, quote_delta_qty, price_scale)?;
                    Ok((px.atoms() > 0).then_some(px).or(fallback_px))
                }
            } else if prev_quote_qty.atoms() == 0 && cum_qty.atoms() > 0 {
                let px = div_half_even(cum_quote, cum_qty, price_scale)?;
                Ok((px.atoms() > 0).then_some(px).or(fallback_px))
            } else {
                Ok(fallback_px)
            }
        }
        ProjectedPriceInput::AveragePrice { avg_price } => {
            if avg_price.atoms() == 0 {
                return Ok(fallback_px);
            }
            if prev_avg_qty.atoms() == 0 || prev_avg_price.is_none_or(|p| p.atoms() == 0) {
                let px = rescale_half_even(avg_price, price_scale)?;
                Ok((px.atoms() > 0).then_some(px).or(fallback_px))
            } else if cum_qty.atoms() > prev_avg_qty.atoms() {
                let avg_delta_qty = cum_qty.checked_sub(prev_avg_qty)?;
                let prev_avg = prev_avg_price.unwrap();
                let px_opt = delta_notional_div_half_even(
                    cum_qty,
                    avg_price,
                    prev_avg_qty,
                    prev_avg,
                    avg_delta_qty,
                    price_scale,
                )?;
                match px_opt {
                    Some(px) if px.atoms() > 0 => Ok(Some(px)),
                    _ => {
                        let avg_rescaled = rescale_half_even(avg_price, price_scale)?;
                        Ok((avg_rescaled.atoms() > 0)
                            .then_some(avg_rescaled)
                            .or(fallback_px))
                    }
                }
            } else {
                let px = rescale_half_even(avg_price, price_scale)?;
                Ok((px.atoms() > 0).then_some(px).or(fallback_px))
            }
        }
        ProjectedPriceInput::LimitFallback => Ok(fallback_px),
    }
}

fn rescale_half_even(val: Fixed, target_scale: u8) -> Result<Fixed> {
    div_half_even(val, Fixed::one(0)?, target_scale)
}

fn delta_notional_div_half_even(
    cum_qty: Fixed,
    avg_price: Fixed,
    prev_qty: Fixed,
    prev_avg: Fixed,
    delta_qty: Fixed,
    target_scale: u8,
) -> Result<Option<Fixed>> {
    let common_px_scale = avg_price.scale().max(prev_avg.scale());
    let cur_avg = avg_price.rescale(common_px_scale, Rounding::Floor)?;
    let old_avg = prev_avg.rescale(common_px_scale, Rounding::Floor)?;

    let prod_cur = mul_u128_wide(cum_qty.atoms() as u128, cur_avg.atoms() as u128);
    let prod_old = mul_u128_wide(prev_qty.atoms() as u128, old_avg.atoms() as u128);

    let Some(diff_notional) = sub_u256_checked(prod_cur, prod_old) else {
        return Ok(None);
    };
    if diff_notional == (0, 0) {
        return Ok(None);
    }

    let exp_shift = i32::from(target_scale) - i32::from(common_px_scale);
    let den_atoms = delta_qty.atoms() as u128;
    if den_atoms == 0 {
        return Err(Error::Risk(RiskError::DivisionByZero));
    }

    let (num, den) = if exp_shift >= 0 {
        let pow = 10u128
            .checked_pow(exp_shift as u32)
            .ok_or(RiskError::ArithmeticOverflow)?;
        let scaled =
            mul_u256_u128_checked(diff_notional, pow).ok_or(RiskError::ArithmeticOverflow)?;
        (scaled, den_atoms)
    } else {
        let pow = 10u128
            .checked_pow((-exp_shift) as u32)
            .ok_or(RiskError::ArithmeticOverflow)?;
        let scaled_den = den_atoms
            .checked_mul(pow)
            .ok_or(RiskError::ArithmeticOverflow)?;
        (diff_notional, scaled_den)
    };

    let q = div_round_half_even_u256(num, den)?;
    let atoms = i128::try_from(q).map_err(|_| RiskError::ArithmeticOverflow)?;
    Ok(Some(Fixed::from_atoms(atoms, target_scale)?))
}

fn div_half_even(num: Fixed, den: Fixed, target_scale: u8) -> Result<Fixed> {
    num.nonnegative()?;
    den.positive()?;
    if num.atoms() == 0 {
        return Ok(Fixed::zero(target_scale)?);
    }
    let exp_shift = i32::from(target_scale) + i32::from(den.scale()) - i32::from(num.scale());
    let u_num = num.atoms() as u128;
    let u_den = den.atoms() as u128;

    let (wide_num, eff_den) = if exp_shift >= 0 {
        let pow = 10u128
            .checked_pow(exp_shift as u32)
            .ok_or(RiskError::ArithmeticOverflow)?;
        (mul_u128_wide(u_num, pow), u_den)
    } else {
        let pow = 10u128
            .checked_pow((-exp_shift) as u32)
            .ok_or(RiskError::ArithmeticOverflow)?;
        let scaled_den = u_den
            .checked_mul(pow)
            .ok_or(RiskError::ArithmeticOverflow)?;
        ((0u128, u_num), scaled_den)
    };

    let q = div_round_half_even_u256(wide_num, eff_den)?;
    let atoms = i128::try_from(q).map_err(|_| RiskError::ArithmeticOverflow)?;
    Ok(Fixed::from_atoms(atoms, target_scale)?)
}

#[inline]
fn mul_u128_wide(a: u128, b: u128) -> (u128, u128) {
    let a_lo = a & 0xffff_ffff_ffff_ffff;
    let a_hi = a >> 64;
    let b_lo = b & 0xffff_ffff_ffff_ffff;
    let b_hi = b >> 64;

    let p0 = a_lo * b_lo;
    let p1 = a_lo * b_hi;
    let p2 = a_hi * b_lo;
    let p3 = a_hi * b_hi;

    let mid1 = (p0 >> 64) + (p1 & 0xffff_ffff_ffff_ffff) + (p2 & 0xffff_ffff_ffff_ffff);
    let hi = p3 + (p1 >> 64) + (p2 >> 64) + (mid1 >> 64);
    let lo = (p0 & 0xffff_ffff_ffff_ffff) | (mid1 << 64);
    (hi, lo)
}

#[inline]
fn sub_u256_checked(a: (u128, u128), b: (u128, u128)) -> Option<(u128, u128)> {
    if a.0 < b.0 || (a.0 == b.0 && a.1 < b.1) {
        return None;
    }
    let (lo, borrow) = a.1.overflowing_sub(b.1);
    let hi = a.0 - b.0 - u128::from(borrow);
    Some((hi, lo))
}

#[inline]
fn mul_u256_u128_checked(a: (u128, u128), m: u128) -> Option<(u128, u128)> {
    if m == 0 || a == (0, 0) {
        return Some((0, 0));
    }
    if m == 1 {
        return Some(a);
    }
    let (lo_hi, lo_lo) = mul_u128_wide(a.1, m);
    let (hi_hi, hi_lo) = mul_u128_wide(a.0, m);
    if hi_hi != 0 {
        return None;
    }
    let hi = hi_lo.checked_add(lo_hi)?;
    Some((hi, lo_lo))
}

fn div_round_half_even_u256(num: (u128, u128), den: u128) -> Result<u128> {
    if den == 0 {
        return Err(Error::Risk(RiskError::DivisionByZero));
    }
    if num.0 >= den {
        return Err(Error::Risk(RiskError::ArithmeticOverflow));
    }
    let (q, rem) = if num.0 == 0 {
        (num.1 / den, num.1 % den)
    } else {
        div_rem_u256_by_u128(num.0, num.1, den)
    };
    let half = den / 2;
    let round_up = if den & 1 == 0 {
        rem > half || (rem == half && (q & 1) == 1)
    } else {
        rem > half
    };
    if round_up {
        q.checked_add(1)
            .ok_or(Error::Risk(RiskError::ArithmeticOverflow))
    } else {
        Ok(q)
    }
}

fn div_rem_u256_by_u128(hi: u128, lo: u128, den: u128) -> (u128, u128) {
    debug_assert!(hi < den && den > 0);
    let mut rem = hi;
    let mut q = 0u128;
    for bit in (0..128).rev() {
        let carry = (rem >> 127) != 0;
        rem = (rem << 1) | ((lo >> bit) & 1);
        if carry || rem >= den {
            rem = rem.wrapping_sub(den);
            q |= 1u128 << bit;
        }
    }
    (q, rem)
}
