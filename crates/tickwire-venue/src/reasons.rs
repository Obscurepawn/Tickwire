//! Fine-grained terminal and modification refusal reason classification.
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
}

impl TerminalReason {
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
        )
    }
}

impl core::fmt::Display for TerminalReason {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.as_str())
    }
}

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
    if (post_only || tif == Some(TimeInForce::PostOnly))
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

/// Zero-allocation classifier for Binance Spot and USDⓈ-M error codes, execution statuses,
/// STP modes, and time-in-force / post-only combinations.
pub fn classify_binance_reason(
    error_code: Option<i64>,
    status_or_msg: &str,
    stp_mode_or_reason: Option<&str>,
    tif: Option<TimeInForce>,
    post_only: bool,
) -> TerminalReason {
    let effective_post_only = post_only || tif == Some(TimeInForce::PostOnly);

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

/// Zero-allocation classifier for Bybit v5, OKX v5, and generic spot venue error codes,
/// string codes, messages, and time-in-force / post-only combinations.
pub fn classify_spot_venue_reason(
    venue_code: Option<i64>,
    code_str: &str,
    msg: &str,
    post_only: bool,
    tif: Option<TimeInForce>,
) -> TerminalReason {
    let effective_post_only = post_only || tif == Some(TimeInForce::PostOnly);
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
