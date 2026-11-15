//! Fine-grained venue terminal and stateless ORS refusal reason classification.
//!
//! All classifiers operate over borrowed ASCII slices and integer error codes
//! with zero heap allocations.

use crate::TimeInForce;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TerminalReason {
    Filled,
    UserCanceled,
    CrossesBook,
    SelfTradePreventedByVenue,
    PostOnlyWouldTake,
    ExpiredIocOrFok,
    ReduceOnlyClippedOrCanceled,
    InsufficientFunds,
    RateLimited,
    OrderNotOpen,
    TooManyPendingActions,
    InvalidPrice,
    InvalidQuantity,
    NoChange,
    AbortedBeforeSend,
    VenueRejected,
    Unknown,
    /// `NO_REASON`: strategy-initiated cancellation confirmed cleanly (`pending_cxl == true`).
    NoReason,
    /// `MKT_CANCELED`: unsolicited exchange cancellation when no strategy cancel was pending.
    MarketCanceled,
    /// `MKT_DELAYED`: ambiguous send/execution status (`-1007`, `-1000`, timeout) requiring status query.
    MarketDelayed,
    /// `REJECT_STALE_ORDER`: `now_mono_ns - tick_mono_ns > max_order_age_ns`.
    RejectStaleOrder,
    /// `ALLOCATION_ERROR`: ORS order-table slot CAS collision.
    AllocationError,
    /// `UNEXPECTED_MSG_SEQNUM`: non-monotonic outbound `msg_seqnum` / `order_seqnum`.
    UnexpectedMsgSeqnum,
    /// `INVALID_STRAT_ID`: `strat_id == 255` on login/order, unconfigured strategy, or duplicate login.
    InvalidStratId,
    /// `INVALID_ACCOUNT_ID`: unknown `account_ref`.
    InvalidAccountId,
    /// `REJECT_ROUTING_ID_MISMATCH`: strategy not authorized to trade on `account_ref`.
    RejectRoutingIdMismatch,
    /// `ADMIN_ACTION`: operator, logout, or supervisor cancellation/disablement.
    AdminAction,
    /// `RESTRICTED_CANCEL_ONLY`: ORS suspended into cancel-only mode.
    RestrictedCancelOnly,
    /// `MKT_REJECT_MARKET_DOWN`: no ready venue session or exchange market closed/halted.
    MarketRejectMarketDown,
    /// `RFQ_QUOTE_NOT_SUPPORTED`: venue session does not support RFQ quote operations.
    RfqQuoteNotSupported,
    /// `INTERNAL_CANCEL`: ORS status-query reconciliation exhausted `max_query_count`.
    InternalCancel,
}

impl TerminalReason {
    #[allow(non_upper_case_globals)]
    pub const NO_REASON: Self = Self::NoReason;
    #[allow(non_upper_case_globals)]
    pub const MKT_CANCELED: Self = Self::MarketCanceled;
    #[allow(non_upper_case_globals)]
    pub const MKT_IOC_CANCELED: Self = Self::ExpiredIocOrFok;
    #[allow(non_upper_case_globals)]
    pub const MKT_REJECT_POST_ONLY: Self = Self::PostOnlyWouldTake;
    #[allow(non_upper_case_globals)]
    pub const MKT_REDUCE_ONLY_CANCELED: Self = Self::ReduceOnlyClippedOrCanceled;
    #[allow(non_upper_case_globals)]
    pub const MKT_CANCELED_SELFTRADE: Self = Self::SelfTradePreventedByVenue;
    #[allow(non_upper_case_globals)]
    pub const MKT_DELAYED: Self = Self::MarketDelayed;
    #[allow(non_upper_case_globals)]
    pub const REJECT_STALE_ORDER: Self = Self::RejectStaleOrder;
    #[allow(non_upper_case_globals)]
    pub const ALLOCATION_ERROR: Self = Self::AllocationError;
    #[allow(non_upper_case_globals)]
    pub const UNEXPECTED_MSG_SEQNUM: Self = Self::UnexpectedMsgSeqnum;
    #[allow(non_upper_case_globals)]
    pub const INVALID_STRAT_ID: Self = Self::InvalidStratId;
    #[allow(non_upper_case_globals)]
    pub const INVALID_ACCOUNT_ID: Self = Self::InvalidAccountId;
    #[allow(non_upper_case_globals)]
    pub const REJECT_ROUTING_ID_MISMATCH: Self = Self::RejectRoutingIdMismatch;
    #[allow(non_upper_case_globals)]
    pub const ADMIN_ACTION: Self = Self::AdminAction;
    #[allow(non_upper_case_globals)]
    pub const RESTRICTED_CANCEL_ONLY: Self = Self::RestrictedCancelOnly;
    #[allow(non_upper_case_globals)]
    pub const MKT_REJECT_MARKET_DOWN: Self = Self::MarketRejectMarketDown;
    #[allow(non_upper_case_globals)]
    pub const RFQ_QUOTE_NOT_SUPPORTED: Self = Self::RfqQuoteNotSupported;
    #[allow(non_upper_case_globals)]
    pub const INTERNAL_CANCEL: Self = Self::InternalCancel;
    #[allow(non_upper_case_globals)]
    pub const MKT_REJECT_NO_SUCH_ORDER: Self = Self::OrderNotOpen;
    #[allow(non_upper_case_globals)]
    pub const MKT_REJECT: Self = Self::VenueRejected;

    pub const ALL: [Self; 31] = [
        Self::Filled,
        Self::UserCanceled,
        Self::CrossesBook,
        Self::SelfTradePreventedByVenue,
        Self::PostOnlyWouldTake,
        Self::ExpiredIocOrFok,
        Self::ReduceOnlyClippedOrCanceled,
        Self::InsufficientFunds,
        Self::RateLimited,
        Self::OrderNotOpen,
        Self::TooManyPendingActions,
        Self::InvalidPrice,
        Self::InvalidQuantity,
        Self::NoChange,
        Self::AbortedBeforeSend,
        Self::VenueRejected,
        Self::Unknown,
        Self::NoReason,
        Self::MarketCanceled,
        Self::MarketDelayed,
        Self::RejectStaleOrder,
        Self::AllocationError,
        Self::UnexpectedMsgSeqnum,
        Self::InvalidStratId,
        Self::InvalidAccountId,
        Self::RejectRoutingIdMismatch,
        Self::AdminAction,
        Self::RestrictedCancelOnly,
        Self::MarketRejectMarketDown,
        Self::RfqQuoteNotSupported,
        Self::InternalCancel,
    ];

    #[inline]
    pub const fn code(self) -> u16 {
        match self {
            Self::Filled => 0,
            Self::UserCanceled => 1,
            Self::CrossesBook => 2,
            Self::SelfTradePreventedByVenue => 3,
            Self::PostOnlyWouldTake => 4,
            Self::ExpiredIocOrFok => 5,
            Self::ReduceOnlyClippedOrCanceled => 6,
            Self::InsufficientFunds => 7,
            Self::RateLimited => 8,
            Self::OrderNotOpen => 9,
            Self::TooManyPendingActions => 10,
            Self::InvalidPrice => 11,
            Self::InvalidQuantity => 12,
            Self::NoChange => 13,
            Self::AbortedBeforeSend => 14,
            Self::VenueRejected => 15,
            Self::Unknown => 16,
            Self::NoReason => 17,
            Self::MarketCanceled => 18,
            Self::MarketDelayed => 19,
            Self::RejectStaleOrder => 20,
            Self::AllocationError => 21,
            Self::UnexpectedMsgSeqnum => 22,
            Self::InvalidStratId => 23,
            Self::InvalidAccountId => 24,
            Self::RejectRoutingIdMismatch => 25,
            Self::AdminAction => 26,
            Self::RestrictedCancelOnly => 27,
            Self::MarketRejectMarketDown => 28,
            Self::RfqQuoteNotSupported => 29,
            Self::InternalCancel => 30,
        }
    }

    #[inline]
    pub const fn from_code(code: u16) -> Option<Self> {
        match code {
            0 => Some(Self::Filled),
            1 => Some(Self::UserCanceled),
            2 => Some(Self::CrossesBook),
            3 => Some(Self::SelfTradePreventedByVenue),
            4 => Some(Self::PostOnlyWouldTake),
            5 => Some(Self::ExpiredIocOrFok),
            6 => Some(Self::ReduceOnlyClippedOrCanceled),
            7 => Some(Self::InsufficientFunds),
            8 => Some(Self::RateLimited),
            9 => Some(Self::OrderNotOpen),
            10 => Some(Self::TooManyPendingActions),
            11 => Some(Self::InvalidPrice),
            12 => Some(Self::InvalidQuantity),
            13 => Some(Self::NoChange),
            14 => Some(Self::AbortedBeforeSend),
            15 => Some(Self::VenueRejected),
            16 => Some(Self::Unknown),
            17 => Some(Self::NoReason),
            18 => Some(Self::MarketCanceled),
            19 => Some(Self::MarketDelayed),
            20 => Some(Self::RejectStaleOrder),
            21 => Some(Self::AllocationError),
            22 => Some(Self::UnexpectedMsgSeqnum),
            23 => Some(Self::InvalidStratId),
            24 => Some(Self::InvalidAccountId),
            25 => Some(Self::RejectRoutingIdMismatch),
            26 => Some(Self::AdminAction),
            27 => Some(Self::RestrictedCancelOnly),
            28 => Some(Self::MarketRejectMarketDown),
            29 => Some(Self::RfqQuoteNotSupported),
            30 => Some(Self::InternalCancel),
            _ => None,
        }
    }

    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Filled => "filled",
            Self::UserCanceled => "user_canceled",
            Self::CrossesBook => "crosses_book",
            Self::SelfTradePreventedByVenue => "self_trade_prevented_by_venue",
            Self::PostOnlyWouldTake => "post_only_would_take",
            Self::ExpiredIocOrFok => "expired_ioc_or_fok",
            Self::ReduceOnlyClippedOrCanceled => "reduce_only_clipped_or_canceled",
            Self::InsufficientFunds => "insufficient_funds",
            Self::RateLimited => "rate_limited",
            Self::OrderNotOpen => "order_not_open",
            Self::TooManyPendingActions => "too_many_pending_actions",
            Self::InvalidPrice => "invalid_price",
            Self::InvalidQuantity => "invalid_quantity",
            Self::NoChange => "no_change",
            Self::AbortedBeforeSend => "aborted_before_send",
            Self::VenueRejected => "venue_rejected",
            Self::Unknown => "unknown",
            Self::NoReason => "no_reason",
            Self::MarketCanceled => "mkt_canceled",
            Self::MarketDelayed => "mkt_delayed",
            Self::RejectStaleOrder => "reject_stale_order",
            Self::AllocationError => "allocation_error",
            Self::UnexpectedMsgSeqnum => "unexpected_msg_seqnum",
            Self::InvalidStratId => "invalid_strat_id",
            Self::InvalidAccountId => "invalid_account_id",
            Self::RejectRoutingIdMismatch => "reject_routing_id_mismatch",
            Self::AdminAction => "admin_action",
            Self::RestrictedCancelOnly => "restricted_cancel_only",
            Self::MarketRejectMarketDown => "mkt_reject_market_down",
            Self::RfqQuoteNotSupported => "rfq_quote_not_supported",
            Self::InternalCancel => "internal_cancel",
        }
    }

    #[inline]
    pub const fn ors_name(self) -> &'static str {
        match self {
            Self::Filled => "FILLED",
            Self::UserCanceled | Self::NoReason => "NO_REASON",
            Self::CrossesBook | Self::PostOnlyWouldTake => "MKT_REJECT_POST_ONLY",
            Self::SelfTradePreventedByVenue => "MKT_CANCELED_SELFTRADE",
            Self::ExpiredIocOrFok => "MKT_IOC_CANCELED",
            Self::ReduceOnlyClippedOrCanceled => "MKT_REDUCE_ONLY_CANCELED",
            Self::InsufficientFunds => "INSUFFICIENT_FUNDS",
            Self::RateLimited => "RATE_LIMITED",
            Self::OrderNotOpen => "MKT_REJECT_NO_SUCH_ORDER",
            Self::TooManyPendingActions => "TOO_MANY_PENDING_ACTIONS",
            Self::InvalidPrice => "INVALID_PRICE",
            Self::InvalidQuantity => "INVALID_QUANTITY",
            Self::NoChange => "NO_CHANGE",
            Self::AbortedBeforeSend => "ABORTED_BEFORE_SEND",
            Self::VenueRejected => "MKT_REJECT",
            Self::Unknown => "UNKNOWN",
            Self::MarketCanceled => "MKT_CANCELED",
            Self::MarketDelayed => "MKT_DELAYED",
            Self::RejectStaleOrder => "REJECT_STALE_ORDER",
            Self::AllocationError => "ALLOCATION_ERROR",
            Self::UnexpectedMsgSeqnum => "UNEXPECTED_MSG_SEQNUM",
            Self::InvalidStratId => "INVALID_STRAT_ID",
            Self::InvalidAccountId => "INVALID_ACCOUNT_ID",
            Self::RejectRoutingIdMismatch => "REJECT_ROUTING_ID_MISMATCH",
            Self::AdminAction => "ADMIN_ACTION",
            Self::RestrictedCancelOnly => "RESTRICTED_CANCEL_ONLY",
            Self::MarketRejectMarketDown => "MKT_REJECT_MARKET_DOWN",
            Self::RfqQuoteNotSupported => "RFQ_QUOTE_NOT_SUPPORTED",
            Self::InternalCancel => "INTERNAL_CANCEL",
        }
    }

    /// Returns `true` when the refusal represents an expected market, order-state,
    /// or strategy-level guard outcome rather than protocol/parameter corruption.
    #[inline]
    pub const fn is_benign_business_refusal(self) -> bool {
        matches!(
            self,
            Self::CrossesBook
                | Self::SelfTradePreventedByVenue
                | Self::PostOnlyWouldTake
                | Self::ExpiredIocOrFok
                | Self::ReduceOnlyClippedOrCanceled
                | Self::OrderNotOpen
                | Self::TooManyPendingActions
                | Self::NoChange
                | Self::AbortedBeforeSend
                | Self::NoReason
                | Self::MarketCanceled
                | Self::RejectStaleOrder
                | Self::AdminAction
                | Self::RestrictedCancelOnly
        )
    }
}

impl core::fmt::Display for TerminalReason {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.as_str())
    }
}

pub type RefusalReason = TerminalReason;

#[inline]
fn contains_ascii_ignore_case(haystack: &str, needle: &str) -> bool {
    let h = haystack.as_bytes();
    let n = needle.as_bytes();
    !n.is_empty() && h.len() >= n.len() && h.windows(n.len()).any(|w| w.eq_ignore_ascii_case(n))
}

#[inline]
fn is_stp_token(s: &str) -> bool {
    let trimmed = s.trim();
    !trimmed.is_empty()
        && !trimmed.eq_ignore_ascii_case("NONE")
        && (contains_ascii_ignore_case(trimmed, "EXPIRE_MAKER")
            || contains_ascii_ignore_case(trimmed, "EXPIRE_TAKER")
            || contains_ascii_ignore_case(trimmed, "EXPIRE_BOTH")
            || contains_ascii_ignore_case(trimmed, "EXPIRED_IN_MATCH")
            || contains_ascii_ignore_case(trimmed, "SELF_TRADE")
            || contains_ascii_ignore_case(trimmed, "SELF-TRADE")
            || contains_ascii_ignore_case(trimmed, "SELFMATCH")
            || contains_ascii_ignore_case(trimmed, "DECREMENT")
            || trimmed.eq_ignore_ascii_case("STP")
            || trimmed.eq_ignore_ascii_case("SMP")
            || contains_ascii_ignore_case(trimmed, "STP_"))
}

fn classify_filter_or_text(
    msg: &str,
    post_only: bool,
    tif: Option<TimeInForce>,
) -> Option<TerminalReason> {
    if contains_ascii_ignore_case(msg, "no need to modify")
        || contains_ascii_ignore_case(msg, "not modified")
        || contains_ascii_ignore_case(msg, "no change")
    {
        return Some(TerminalReason::NoChange);
    }
    if contains_ascii_ignore_case(msg, "post only")
        || contains_ascii_ignore_case(msg, "post-only")
        || contains_ascii_ignore_case(msg, "post_only")
        || contains_ascii_ignore_case(msg, "postonly")
        || contains_ascii_ignore_case(msg, "limit_maker")
        || contains_ascii_ignore_case(msg, "immediately match and take")
        || contains_ascii_ignore_case(msg, "would take liquidity")
        || contains_ascii_ignore_case(msg, "willtake")
        || contains_ascii_ignore_case(msg, "gtx")
        || contains_ascii_ignore_case(msg, "rpi")
    {
        return Some(TerminalReason::PostOnlyWouldTake);
    }
    if is_stp_token(msg) || contains_ascii_ignore_case(msg, "self trade") {
        return Some(TerminalReason::SelfTradePreventedByVenue);
    }
    if contains_ascii_ignore_case(msg, "reduceonly")
        || contains_ascii_ignore_case(msg, "reduce-only")
        || contains_ascii_ignore_case(msg, "reduce only")
        || contains_ascii_ignore_case(msg, "reduce_only")
    {
        return Some(TerminalReason::ReduceOnlyClippedOrCanceled);
    }
    if contains_ascii_ignore_case(msg, "too many requests")
        || contains_ascii_ignore_case(msg, "rate limit")
        || contains_ascii_ignore_case(msg, "ratelimit")
        || contains_ascii_ignore_case(msg, "too many new orders")
    {
        return Some(TerminalReason::RateLimited);
    }
    if contains_ascii_ignore_case(msg, "insufficient")
        || contains_ascii_ignore_case(msg, "not enough")
        || contains_ascii_ignore_case(msg, "margin is insufficient")
    {
        return Some(TerminalReason::InsufficientFunds);
    }
    if contains_ascii_ignore_case(msg, "unknown order")
        || contains_ascii_ignore_case(msg, "order does not exist")
        || contains_ascii_ignore_case(msg, "order not found")
        || contains_ascii_ignore_case(msg, "already canceled")
        || contains_ascii_ignore_case(msg, "already cancelled")
        || contains_ascii_ignore_case(msg, "already filled")
        || contains_ascii_ignore_case(msg, "order_not_open")
    {
        return Some(TerminalReason::OrderNotOpen);
    }
    if contains_ascii_ignore_case(msg, "price_filter")
        || contains_ascii_ignore_case(msg, "percent_price")
        || contains_ascii_ignore_case(msg, "tick size")
        || contains_ascii_ignore_case(msg, "price band")
        || contains_ascii_ignore_case(msg, "invalid price")
        || contains_ascii_ignore_case(msg, "price is higher")
        || contains_ascii_ignore_case(msg, "price is lower")
    {
        return Some(TerminalReason::InvalidPrice);
    }
    if contains_ascii_ignore_case(msg, "lot_size")
        || contains_ascii_ignore_case(msg, "min_notional")
        || contains_ascii_ignore_case(msg, "notional")
        || contains_ascii_ignore_case(msg, "step size")
        || contains_ascii_ignore_case(msg, "invalid quantity")
        || contains_ascii_ignore_case(msg, "qty")
        || contains_ascii_ignore_case(msg, "quantity")
    {
        return Some(TerminalReason::InvalidQuantity);
    }
    if (post_only || matches!(tif, Some(TimeInForce::PostOnly | TimeInForce::Rpi)))
        && (contains_ascii_ignore_case(msg, "expire")
            || contains_ascii_ignore_case(msg, "reject")
            || contains_ascii_ignore_case(msg, "maker"))
    {
        return Some(TerminalReason::PostOnlyWouldTake);
    }
    if matches!(tif, Some(TimeInForce::Ioc | TimeInForce::Fok))
        && (contains_ascii_ignore_case(msg, "expire")
            || contains_ascii_ignore_case(msg, "fok")
            || contains_ascii_ignore_case(msg, "ioc"))
    {
        return Some(TerminalReason::ExpiredIocOrFok);
    }
    None
}

/// Zero-allocation classifier for Binance Spot, USD-M, and COIN-M error codes, execution statuses,
/// STP modes, and time-in-force / post-only / RPI combinations.
pub fn classify_binance_reason(
    error_code: Option<i64>,
    status_or_msg: &str,
    stp_mode_or_reason: Option<&str>,
    tif: Option<TimeInForce>,
    post_only: bool,
) -> TerminalReason {
    let effective_post_only =
        post_only || matches!(tif, Some(TimeInForce::PostOnly | TimeInForce::Rpi));

    if let Some(code) = error_code {
        match code {
            0 => {}
            429 | 418 | -1003 | -1015 => return TerminalReason::RateLimited,
            -5022 | -5021 => return TerminalReason::PostOnlyWouldTake,
            -2022 => return TerminalReason::ReduceOnlyClippedOrCanceled,
            -2019 => return TerminalReason::InsufficientFunds,
            -2011 | -2013 => return TerminalReason::OrderNotOpen,
            -5027 => return TerminalReason::NoChange,
            -4014 | -4016 | -4024 | -4131 => return TerminalReason::InvalidPrice,
            -4003 | -4164 => return TerminalReason::InvalidQuantity,
            -2010 => {
                if let Some(reason) =
                    classify_filter_or_text(status_or_msg, effective_post_only, tif)
                {
                    return reason;
                }
                if stp_mode_or_reason.is_some_and(is_stp_token) {
                    return TerminalReason::SelfTradePreventedByVenue;
                }
                return TerminalReason::VenueRejected;
            }
            -1013 | -1111 => {
                if let Some(reason) =
                    classify_filter_or_text(status_or_msg, effective_post_only, tif)
                {
                    return reason;
                }
                return TerminalReason::VenueRejected;
            }
            _ => {
                if let Some(reason) =
                    classify_filter_or_text(status_or_msg, effective_post_only, tif)
                {
                    return reason;
                }
                return TerminalReason::VenueRejected;
            }
        }
    }

    let trimmed = status_or_msg.trim();
    if trimmed.eq_ignore_ascii_case("FILLED") {
        return TerminalReason::Filled;
    }
    if trimmed.eq_ignore_ascii_case("EXPIRED_IN_MATCH") {
        return TerminalReason::SelfTradePreventedByVenue;
    }
    if trimmed.eq_ignore_ascii_case("CANCELED") || trimmed.eq_ignore_ascii_case("CANCELLED") {
        if let Some(stp) = stp_mode_or_reason {
            if contains_ascii_ignore_case(stp, "EXPIRED_IN_MATCH")
                || contains_ascii_ignore_case(stp, "SELF_TRADE")
                || contains_ascii_ignore_case(stp, "SELFMATCH")
                || stp.trim().eq_ignore_ascii_case("STP")
            {
                return TerminalReason::SelfTradePreventedByVenue;
            }
            if contains_ascii_ignore_case(stp, "POST_ONLY")
                || contains_ascii_ignore_case(stp, "GTX")
                || contains_ascii_ignore_case(stp, "RPI")
            {
                return TerminalReason::PostOnlyWouldTake;
            }
            if contains_ascii_ignore_case(stp, "REDUCE_ONLY") {
                return TerminalReason::ReduceOnlyClippedOrCanceled;
            }
        }
        return TerminalReason::UserCanceled;
    }
    if trimmed.eq_ignore_ascii_case("EXPIRED") {
        if stp_mode_or_reason.is_some_and(is_stp_token) {
            return TerminalReason::SelfTradePreventedByVenue;
        }
        if effective_post_only {
            return TerminalReason::PostOnlyWouldTake;
        }
        if matches!(tif, Some(TimeInForce::Ioc | TimeInForce::Fok)) {
            return TerminalReason::ExpiredIocOrFok;
        }
        if let Some(r) = stp_mode_or_reason
            && let Some(classified) = classify_filter_or_text(r, effective_post_only, tif)
        {
            return classified;
        }
        return TerminalReason::ExpiredIocOrFok;
    }
    if trimmed.eq_ignore_ascii_case("REJECTED") {
        if let Some(r) = stp_mode_or_reason
            && let Some(classified) = classify_filter_or_text(r, effective_post_only, tif)
        {
            return classified;
        }
        if effective_post_only {
            return TerminalReason::PostOnlyWouldTake;
        }
        return TerminalReason::VenueRejected;
    }

    if let Some(r) = stp_mode_or_reason
        && is_stp_token(r)
    {
        return TerminalReason::SelfTradePreventedByVenue;
    }
    if let Some(reason) = classify_filter_or_text(status_or_msg, effective_post_only, tif) {
        return reason;
    }
    TerminalReason::Unknown
}

/// Stateless ORS cancel/reject classifier distinguishing strategy-initiated cancels (`NO_REASON`
/// when `pending_strategy_cancel == true`) from unsolicited exchange cancels (`MKT_CANCELED` when
/// `pending_strategy_cancel == false`), ambiguous timeouts (`MKT_DELAYED`), and market-down errors.
pub fn classify_ors_cancel_reason(
    error_code: Option<i64>,
    status_or_msg: &str,
    stp_mode_or_reason: Option<&str>,
    tif: Option<TimeInForce>,
    post_only: bool,
    pending_strategy_cancel: bool,
) -> TerminalReason {
    if matches!(error_code, Some(-1000 | -1001 | -1006 | -1007 | 408)) {
        return TerminalReason::MarketDelayed;
    }
    if matches!(error_code, Some(-1008 | -4165))
        || contains_ascii_ignore_case(status_or_msg, "market is closed")
        || contains_ascii_ignore_case(status_or_msg, "market_down")
        || contains_ascii_ignore_case(status_or_msg, "market down")
        || contains_ascii_ignore_case(status_or_msg, "trading is disabled")
    {
        return TerminalReason::MarketRejectMarketDown;
    }
    let base = classify_binance_reason(
        error_code,
        status_or_msg,
        stp_mode_or_reason,
        tif,
        post_only,
    );
    match base {
        TerminalReason::UserCanceled => {
            if pending_strategy_cancel {
                TerminalReason::NoReason
            } else {
                TerminalReason::MarketCanceled
            }
        }
        other => other,
    }
}

/// Zero-allocation classifier for Bybit v5, OKX v5, and generic spot venue error codes,
/// string codes, messages, and time-in-force / post-only / RPI combinations.
pub fn classify_spot_venue_reason(
    venue_code: Option<i64>,
    code_str: &str,
    msg: &str,
    post_only: bool,
    tif: Option<TimeInForce>,
) -> TerminalReason {
    let effective_post_only =
        post_only || matches!(tif, Some(TimeInForce::PostOnly | TimeInForce::Rpi));
    let code_trimmed = code_str.trim();

    if let Some(code) = venue_code {
        match code {
            0 => {}
            // HTTP / Bybit / OKX rate limit codes
            429 | 10006 | 10018 | 50011 => return TerminalReason::RateLimited,
            // Bybit & OKX Post-Only rejection/cancellation codes
            140001 | 170193 | 110079 | 51019 => return TerminalReason::PostOnlyWouldTake,
            // Bybit & OKX insufficient funds / balance codes
            110007 | 170131 | 170033 | 51008 | 51119 | 51502 => {
                return TerminalReason::InsufficientFunds;
            }
            // Bybit & OKX order-not-open / unknown-order codes
            110001 | 170213 | 170145 | 51400 | 51401 | 51402 | 51501 | 51503 => {
                return TerminalReason::OrderNotOpen;
            }
            // Bybit & OKX price filter / tick / price-band codes
            110003 | 170134 | 170137 | 170194 | 51006 | 51030 | 51137 => {
                return TerminalReason::InvalidPrice;
            }
            // Bybit & OKX lot-size / min-notional / qty <= filled codes
            110004 | 170135 | 170136 | 170140 | 51004 | 51020 | 51506 => {
                return TerminalReason::InvalidQuantity;
            }
            // Bybit & OKX reduce-only codes
            110017 | 51116 | 51059 => return TerminalReason::ReduceOnlyClippedOrCanceled,
            // Bybit & OKX unmodified order codes
            110025 | 51509 => return TerminalReason::NoChange,
            _ => {
                if let Some(r) = classify_filter_or_text(code_trimmed, effective_post_only, tif)
                    .or_else(|| classify_filter_or_text(msg, effective_post_only, tif))
                {
                    return r;
                }
                return TerminalReason::VenueRejected;
            }
        }
    }

    if code_trimmed.eq_ignore_ascii_case("FILLED") {
        return TerminalReason::Filled;
    }
    if code_trimmed.eq_ignore_ascii_case("EC_PostOnlyWillTakeLiquidity") {
        return TerminalReason::PostOnlyWouldTake;
    }
    if is_stp_token(code_trimmed) || is_stp_token(msg) {
        return TerminalReason::SelfTradePreventedByVenue;
    }
    if let Some(r) = classify_filter_or_text(code_trimmed, effective_post_only, tif)
        .or_else(|| classify_filter_or_text(msg, effective_post_only, tif))
    {
        return r;
    }
    if code_trimmed.eq_ignore_ascii_case("CANCELED")
        || code_trimmed.eq_ignore_ascii_case("CANCELLED")
        || code_trimmed.eq_ignore_ascii_case("USER_CANCELED")
    {
        if effective_post_only
            && (contains_ascii_ignore_case(msg, "liquidity")
                || contains_ascii_ignore_case(msg, "maker"))
        {
            return TerminalReason::PostOnlyWouldTake;
        }
        if matches!(tif, Some(TimeInForce::Ioc | TimeInForce::Fok)) {
            return TerminalReason::ExpiredIocOrFok;
        }
        return TerminalReason::UserCanceled;
    }
    if code_trimmed.eq_ignore_ascii_case("REJECTED") {
        return if effective_post_only {
            TerminalReason::PostOnlyWouldTake
        } else {
            TerminalReason::VenueRejected
        };
    }
    if !code_trimmed.is_empty() && code_trimmed != "0" {
        return TerminalReason::VenueRejected;
    }
    TerminalReason::Unknown
}
