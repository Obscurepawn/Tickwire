//! Zero-allocation, single-pass `JsonScan` parsers for Bybit V5 (`BybitUtaParser`) and OKX V5
//! (`OkxUtaParser`) Unified Trading Account WebSocket and HTTPS execution payloads.
//!
//! Every function in this module scans raw UTF-8/ASCII JSON bytes in a single forward pass using
//! [`tickwire_transport::json_scan::JsonScan`] without constructing `serde_json::Value`, `String`,
//! or `Vec` on the hot path.

use crate::SpotVenue;
use tickwire_transport::{
    Error as TransportError,
    json_scan::{JsonScan, ScannedExecFrame, ScannedRateLimit, parse_i64_ascii, parse_u64_ascii},
};
use tickwire_venue::{Liquidity, OrderStatus, Side, TerminalReason, TimeInForce};

/// Classifies Bybit V5 and OKX V5 synchronous/query error codes into per-order vs. account-wide actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SpotIssueKind {
    /// Definitive business rejection proving a `New`, `Modify`, or `Cancel` request was rejected on the venue.
    DeterministicReject,
    /// Timeout, clock-window, or internal error whose remote execution outcome requires inquiry.
    Ambiguous,
    /// Target order absent or already closed on a query, cancel, or modify endpoint.
    NotFound,
    /// Credential, permission, IP/WAF ban, or rate-limit hazard.
    AccountHazard,
}

/// Pure zero-allocation venue error-code classifier. Unrecognized codes default conservatively to
/// [`SpotIssueKind::Ambiguous`].
pub fn classify_issue(venue: SpotVenue, code: &str) -> SpotIssueKind {
    match venue {
        SpotVenue::Bybit => match code {
            "10003" | "10004" | "10005" | "10006" | "10007" | "10009" | "10010" | "10018"
            | "33004" | "401" | "403" | "418" | "429" => SpotIssueKind::AccountHazard,
            "110001" | "170145" | "170213" => SpotIssueKind::NotFound,
            "10000" | "10002" | "10016" | "170007" => SpotIssueKind::Ambiguous,
            "110003" | "110004" | "110007" | "110017" | "110025" | "110079" | "140001"
            | "170033" | "170121" | "170124" | "170130" | "170131" | "170132" | "170133"
            | "170134" | "170135" | "170136" | "170137" | "170139" | "170140" | "170192"
            | "170193" | "170194" | "170210" => SpotIssueKind::DeterministicReject,
            _ => SpotIssueKind::Ambiguous,
        },
        SpotVenue::Okx => match code {
            "50011" | "50101" | "50103" | "50105" | "50111" | "50113" | "50119" | "50120"
            | "401" | "403" | "418" | "429" => SpotIssueKind::AccountHazard,
            "51400" | "51401" | "51402" | "51501" | "51503" | "51603" => SpotIssueKind::NotFound,
            "50000" | "50001" | "50004" | "50013" | "50026" | "50102" => SpotIssueKind::Ambiguous,
            "51000" | "51001" | "51002" | "51004" | "51006" | "51008" | "51010" | "51019"
            | "51020" | "51030" | "51059" | "51080" | "51116" | "51119" | "51137" | "51502"
            | "51506" | "51509" => SpotIssueKind::DeterministicReject,
            _ => SpotIssueKind::Ambiguous,
        },
    }
}

/// Returns `true` when an HTTP status code signals an authentication, permission, WAF, or rate-limit hazard.
#[inline]
pub const fn is_http_account_hazard(http_status: u16) -> bool {
    matches!(http_status, 401 | 403 | 418 | 429)
}

/// Returns `true` when `code` is a venue rate-limit error code (`Bybit` `10006`/`429`, `OKX` `50011`/`429`).
#[inline]
pub fn is_venue_rate_limit_code(venue: SpotVenue, code: &str) -> bool {
    matches!(
        (venue, code),
        (SpotVenue::Bybit, "10006" | "429") | (SpotVenue::Okx, "50011" | "429")
    )
}

#[inline]
fn ascii_contains_ignore_case(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack.len() >= needle.len()
        && haystack
            .windows(needle.len())
            .any(|w| w.eq_ignore_ascii_case(needle))
}

/// Zero-allocation signed decimal ASCII parser into scaled integer atoms (`"50000.5"` at scale `2` -> `5_000_050`).
pub fn parse_signed_scaled_i128(raw: &[u8], target_scale: u8) -> Option<i128> {
    if raw.is_empty() || raw.len() > 40 || target_scale > 18 {
        return None;
    }
    let (neg, body) = match raw[0] {
        b'-' => (true, &raw[1..]),
        b'+' => (false, &raw[1..]),
        _ => (false, raw),
    };
    if body.is_empty() {
        return None;
    }
    let mut whole: i128 = 0;
    let mut frac: i128 = 0;
    let mut seen_dot = false;
    let mut seen_digit = false;
    let mut frac_seen: u8 = 0;

    for &b in body {
        if b == b'.' {
            if seen_dot {
                return None;
            }
            seen_dot = true;
            continue;
        }
        if !b.is_ascii_digit() {
            return None;
        }
        seen_digit = true;
        let digit = i128::from(b - b'0');
        if !seen_dot {
            whole = whole.checked_mul(10)?.checked_add(digit)?;
        } else if frac_seen < target_scale {
            frac = frac.checked_mul(10)?.checked_add(digit)?;
            frac_seen += 1;
        } else if digit != 0 {
            return None;
        }
    }
    if !seen_digit {
        return None;
    }
    while frac_seen < target_scale {
        frac = frac.checked_mul(10)?;
        frac_seen += 1;
    }
    let scale_pow = 10i128.checked_pow(u32::from(target_scale))?;
    let abs_atoms = whole.checked_mul(scale_pow)?.checked_add(frac)?;
    if neg {
        abs_atoms.checked_neg()
    } else {
        Some(abs_atoms)
    }
}

/// Zero-allocation unsigned decimal ASCII parser into integer ticks/lots (`atoms / step_atoms`).
pub fn parse_positive_step_i64(raw: &[u8], scale: u8, step_atoms: i128) -> Option<i64> {
    if step_atoms <= 0 {
        return None;
    }
    let atoms = parse_signed_scaled_i128(raw, scale)?;
    if atoms < 0 || atoms % step_atoms != 0 {
        return None;
    }
    i64::try_from(atoms / step_atoms).ok()
}

/// Trade command operation identified in a WS or HTTPS reply.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum UtaReplyOp {
    #[default]
    Unknown,
    Create,
    Amend,
    Cancel,
}

/// Borrowed synchronous WS/HTTPS trade command reply decoded in a single `JsonScan` pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UtaCommandReply<'a> {
    pub reply_id: Option<u64>,
    pub op: UtaReplyOp,
    pub code: &'a str,
    pub sub_code: &'a str,
    pub message: &'a str,
    pub order_id: &'a str,
    pub client_id: &'a str,
    pub amend_or_req_id: &'a str,
    pub clamped_qty: &'a str,
    pub clamped_price: &'a str,
    pub cum_exec_qty: &'a str,
    pub bapi_limit: Option<u64>,
    pub bapi_limit_status: Option<u64>,
    pub rate_limited: bool,
}

impl<'a> UtaCommandReply<'a> {
    #[inline]
    pub fn is_ok(&self) -> bool {
        self.code == "0" && (self.sub_code.is_empty() || self.sub_code == "0")
    }

    #[inline]
    pub fn effective_error_code(&self) -> &'a str {
        if !self.sub_code.is_empty() && self.sub_code != "0" {
            self.sub_code
        } else {
            self.code
        }
    }
}

/// Borrowed private `order` / `orders` projection row decoded in a single `JsonScan` pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UtaOrderRow<'a> {
    pub scope: &'a str,
    pub symbol: &'a str,
    pub order_id: &'a str,
    pub client_id: &'a str,
    pub status_raw: &'a str,
    pub side_raw: &'a str,
    pub ord_type_raw: &'a str,
    pub tif_raw: &'a str,
    pub reduce_only: bool,
    pub price_raw: &'a str,
    pub qty_raw: &'a str,
    pub cum_exec_qty_raw: &'a str,
    pub leaves_qty_raw: &'a str,
    pub updated_ms: Option<u64>,
    pub reject_reason: &'a str,
    pub cancel_type: &'a str,
    pub smp_type: &'a str,
    pub cancel_source: &'a str,
    pub cancel_source_reason: &'a str,
    pub amend_req_id: &'a str,
    // Optional inline execution fields carried on OKX `orders` channel rows:
    pub inline_trade_id: &'a str,
    pub inline_fill_px: &'a str,
    pub inline_fill_sz: &'a str,
    pub inline_fill_fee: &'a str,
    pub inline_fill_fee_ccy: &'a str,
    pub inline_exec_type: &'a str,
    pub inline_fill_time_ms: Option<u64>,
}

impl<'a> UtaOrderRow<'a> {
    pub fn normalized_status(&self, venue: SpotVenue) -> Option<OrderStatus> {
        match (venue, self.status_raw) {
            (SpotVenue::Bybit, "New" | "Created" | "Untriggered") | (SpotVenue::Okx, "live") => {
                Some(OrderStatus::New)
            }
            (SpotVenue::Bybit, "PartiallyFilled") | (SpotVenue::Okx, "partially_filled") => {
                Some(OrderStatus::PartiallyFilled)
            }
            (SpotVenue::Bybit, "Filled") | (SpotVenue::Okx, "filled") => Some(OrderStatus::Filled),
            (SpotVenue::Bybit, "Cancelled" | "PartiallyFilledCanceled" | "Deactivated")
            | (SpotVenue::Okx, "canceled" | "mmp_canceled") => Some(OrderStatus::Canceled),
            (SpotVenue::Bybit, "Rejected") => Some(OrderStatus::Rejected),
            _ => None,
        }
    }

    pub fn normalized_side(&self, venue: SpotVenue) -> Option<Side> {
        match (venue, self.side_raw) {
            (SpotVenue::Bybit, "Buy") | (SpotVenue::Okx, "buy") => Some(Side::Buy),
            (SpotVenue::Bybit, "Sell") | (SpotVenue::Okx, "sell") => Some(Side::Sell),
            _ => None,
        }
    }

    pub fn normalized_tif(&self, venue: SpotVenue) -> Option<TimeInForce> {
        match venue {
            SpotVenue::Bybit => match self.tif_raw {
                "GTC" => Some(TimeInForce::Gtc),
                "IOC" => Some(TimeInForce::Ioc),
                "FOK" => Some(TimeInForce::Fok),
                "PostOnly" => Some(TimeInForce::PostOnly),
                "RPI" => Some(TimeInForce::Rpi),
                _ => None,
            },
            SpotVenue::Okx => match self.ord_type_raw {
                "limit" => Some(TimeInForce::Gtc),
                "ioc" | "optimal_limit_ioc" | "market" => Some(TimeInForce::Ioc),
                "fok" => Some(TimeInForce::Fok),
                "post_only" => Some(TimeInForce::PostOnly),
                "rpi" => Some(TimeInForce::Rpi),
                _ => None,
            },
        }
    }

    /// Classifies an explicit venue-driven [`TerminalReason`] for a terminal order row.
    ///
    /// Returns `None` for normal cancel completions (`EC_NoError` / `EC_PerCancelRequest` / user cancel)
    /// so [`tickwire_account::OrderManager::on_canceled`] maps `pending_cxl > 0` -> `NO_REASON`
    /// (or `ADMIN_ACTION`) and `pending_cxl == 0` -> `MKT_CANCELED`.
    pub fn classify_explicit_terminal_reason(
        &self,
        venue: SpotVenue,
        order_post_only: bool,
        order_tif: TimeInForce,
    ) -> Option<TerminalReason> {
        let eff_tif = self.normalized_tif(venue).unwrap_or(order_tif);
        let eff_post_only =
            order_post_only || matches!(eff_tif, TimeInForce::PostOnly | TimeInForce::Rpi);

        match venue {
            SpotVenue::Bybit => {
                if (!self.smp_type.is_empty() && !self.smp_type.eq_ignore_ascii_case("None"))
                    || ascii_contains_ignore_case(self.cancel_type.as_bytes(), b"Smp")
                    || ascii_contains_ignore_case(self.reject_reason.as_bytes(), b"Smp")
                {
                    return Some(TerminalReason::MKT_CANCELED_SELFTRADE);
                }
                if ascii_contains_ignore_case(self.cancel_type.as_bytes(), b"ReduceOnly")
                    || ascii_contains_ignore_case(self.reject_reason.as_bytes(), b"ReduceOnly")
                    || self.reject_reason == "110017"
                {
                    return Some(TerminalReason::MKT_REDUCE_ONLY_CANCELED);
                }
                if ascii_contains_ignore_case(self.reject_reason.as_bytes(), b"PostOnly")
                    || matches!(self.reject_reason, "110079" | "170139" | "140001")
                    || (eff_post_only
                        && ascii_contains_ignore_case(
                            self.reject_reason.as_bytes(),
                            b"TakeLiquidity",
                        ))
                {
                    return Some(TerminalReason::MKT_REJECT_POST_ONLY);
                }
                if matches!(eff_tif, TimeInForce::Ioc | TimeInForce::Fok)
                    && (self.cancel_type == "CancelByCannotAffordOrderCost"
                        || self.reject_reason == "EC_IOCNoImmediateFill"
                        || self.reject_reason == "EC_FOKNotFill"
                        || self.reject_reason == "EC_NoImmediateQtyToFill"
                        || self.cancel_type.is_empty()
                        || self.cancel_type == "UNKNOWN"
                        || self.reject_reason.is_empty()
                        || self.reject_reason == "EC_NoError")
                {
                    return Some(TerminalReason::MKT_IOC_CANCELED);
                }
                None
            }
            SpotVenue::Okx => {
                if self.cancel_source == "32"
                    || ascii_contains_ignore_case(self.cancel_source_reason.as_bytes(), b"smp")
                    || ascii_contains_ignore_case(self.cancel_source_reason.as_bytes(), b"self")
                {
                    return Some(TerminalReason::MKT_CANCELED_SELFTRADE);
                }
                if matches!(self.cancel_source, "14" | "15")
                    || matches!(self.reject_reason, "51116" | "51059")
                    || ascii_contains_ignore_case(self.cancel_source_reason.as_bytes(), b"reduce")
                {
                    return Some(TerminalReason::MKT_REDUCE_ONLY_CANCELED);
                }
                if self.cancel_source == "20"
                    || self.reject_reason == "51019"
                    || ascii_contains_ignore_case(
                        self.cancel_source_reason.as_bytes(),
                        b"post_only",
                    )
                    || ascii_contains_ignore_case(
                        self.cancel_source_reason.as_bytes(),
                        b"post only",
                    )
                    || (eff_post_only && self.cancel_source == "31")
                {
                    return Some(TerminalReason::MKT_REJECT_POST_ONLY);
                }
                if matches!(eff_tif, TimeInForce::Ioc | TimeInForce::Fok)
                    && !matches!(self.cancel_source, "1" | "7")
                {
                    return Some(TerminalReason::MKT_IOC_CANCELED);
                }
                None
            }
        }
    }
}

/// Borrowed private `execution` / `execution.fast` trade row decoded in a single `JsonScan` pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UtaExecRow<'a> {
    pub is_fast: bool,
    pub scope: &'a str,
    pub symbol: &'a str,
    pub exec_id: &'a str,
    pub order_id: &'a str,
    pub client_id: &'a str,
    pub exec_type: &'a str,
    pub side_raw: &'a str,
    pub exec_price_raw: &'a str,
    pub exec_qty_raw: &'a str,
    /// `None` on `execution.fast` (`TRADE_LITE` fast fill); `Some(...)` on full `execution`.
    pub exec_fee_raw: Option<&'a str>,
    pub fee_currency: Option<&'a str>,
    pub liquidity: Liquidity,
    pub exec_time_ms: Option<u64>,
}

impl Default for UtaExecRow<'_> {
    #[inline]
    fn default() -> Self {
        Self {
            is_fast: false,
            scope: "",
            symbol: "",
            exec_id: "",
            order_id: "",
            client_id: "",
            exec_type: "",
            side_raw: "",
            exec_price_raw: "",
            exec_qty_raw: "",
            exec_fee_raw: None,
            fee_currency: None,
            liquidity: Liquidity::Unknown,
            exec_time_ms: None,
        }
    }
}

/// Borrowed multi-wallet balance row from Bybit `wallet` or OKX `balance_and_position` / `account`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UtaWalletCoinRow<'a> {
    pub coin: &'a str,
    pub wallet_balance_raw: &'a str,
    pub available_balance_raw: &'a str,
    pub locked_raw: &'a str,
}

/// Borrowed single-pass event emitted by [`scan_bybit_frame`] and [`scan_okx_frame`].
///
/// Kept unboxed (`Copy`) on the stack so `RESP` single-pass `JsonScan` performs zero heap allocation.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UtaFrameEvent<'a> {
    Control { op: &'a str, success: bool },
    MaintenanceNotice { code: &'a str, message: &'a str },
    CommandReply(UtaCommandReply<'a>),
    OrderRow(UtaOrderRow<'a>),
    ExecRow(UtaExecRow<'a>),
    WalletCoin(UtaWalletCoinRow<'a>),
}

/// Summary returned after scanning a single venue frame.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UtaScanSummary {
    pub reply_id: Option<u64>,
    pub graceful_drain_connection: bool,
    pub rate_limited: bool,
    pub bapi_limit: Option<(u64, u64)>,
    pub events_emitted: u32,
}

#[inline]
fn read_str_opt<'a>(scan: &mut JsonScan<'a>) -> Result<&'a str, TransportError> {
    Ok(scan.read_string_or_scalar()?.unwrap_or(""))
}

fn scan_bybit_order_obj<'a>(scan: &mut JsonScan<'a>) -> Result<UtaOrderRow<'a>, TransportError> {
    scan.eat_byte(b'{')?;
    let mut row = UtaOrderRow::default();
    let mut first = true;
    loop {
        if scan.eat_byte(b'}').is_ok() {
            break;
        }
        if !first {
            scan.eat_byte(b',')?;
        }
        first = false;
        let key = scan.read_string_raw()?;
        scan.eat_byte(b':')?;
        match key {
            "category" => row.scope = read_str_opt(scan)?,
            "symbol" => row.symbol = read_str_opt(scan)?,
            "orderId" => row.order_id = read_str_opt(scan)?,
            "orderLinkId" => row.client_id = read_str_opt(scan)?,
            "orderStatus" => row.status_raw = read_str_opt(scan)?,
            "side" => row.side_raw = read_str_opt(scan)?,
            "orderType" => row.ord_type_raw = read_str_opt(scan)?,
            "timeInForce" => row.tif_raw = read_str_opt(scan)?,
            "reduceOnly" => {
                let p = read_str_opt(scan)?;
                row.reduce_only = p == "true";
            }
            "price" => row.price_raw = read_str_opt(scan)?,
            "qty" => row.qty_raw = read_str_opt(scan)?,
            "cumExecQty" => row.cum_exec_qty_raw = read_str_opt(scan)?,
            "leavesQty" => row.leaves_qty_raw = read_str_opt(scan)?,
            "updatedTime" => {
                let raw = read_str_opt(scan)?;
                row.updated_ms = parse_u64_ascii(raw.as_bytes());
            }
            "rejectReason" => row.reject_reason = read_str_opt(scan)?,
            "cancelType" => row.cancel_type = read_str_opt(scan)?,
            "smpType" => row.smp_type = read_str_opt(scan)?,
            _ => {
                let _ = scan.skip_value()?;
            }
        }
    }
    Ok(row)
}

fn scan_bybit_exec_obj<'a>(
    scan: &mut JsonScan<'a>,
    is_fast: bool,
) -> Result<UtaExecRow<'a>, TransportError> {
    scan.eat_byte(b'{')?;
    let mut row = UtaExecRow {
        is_fast,
        liquidity: Liquidity::Unknown,
        ..UtaExecRow::default()
    };
    let mut first = true;
    loop {
        if scan.eat_byte(b'}').is_ok() {
            break;
        }
        if !first {
            scan.eat_byte(b',')?;
        }
        first = false;
        let key = scan.read_string_raw()?;
        scan.eat_byte(b':')?;
        match key {
            "category" => row.scope = read_str_opt(scan)?,
            "symbol" => row.symbol = read_str_opt(scan)?,
            "execId" => row.exec_id = read_str_opt(scan)?,
            "orderId" => row.order_id = read_str_opt(scan)?,
            "orderLinkId" => row.client_id = read_str_opt(scan)?,
            "execType" => row.exec_type = read_str_opt(scan)?,
            "side" => row.side_raw = read_str_opt(scan)?,
            "execPrice" => row.exec_price_raw = read_str_opt(scan)?,
            "execQty" => row.exec_qty_raw = read_str_opt(scan)?,
            "execFee" if !is_fast => {
                let f = read_str_opt(scan)?;
                if !f.is_empty() {
                    row.exec_fee_raw = Some(f);
                }
            }
            "feeCurrency" if !is_fast => {
                let c = read_str_opt(scan)?;
                if !c.is_empty() {
                    row.fee_currency = Some(c);
                }
            }
            "isMaker" => {
                let p = read_str_opt(scan)?;
                row.liquidity = match p {
                    "true" => Liquidity::Maker,
                    "false" => Liquidity::Taker,
                    _ => Liquidity::Unknown,
                };
            }
            "execTime" => {
                let raw = read_str_opt(scan)?;
                row.exec_time_ms = parse_u64_ascii(raw.as_bytes());
            }
            _ => {
                let _ = scan.skip_value()?;
            }
        }
    }
    Ok(row)
}

fn scan_bybit_wallet_account_obj<'a>(
    scan: &mut JsonScan<'a>,
    on_event: &mut impl FnMut(UtaFrameEvent<'a>),
    emitted: &mut u32,
) -> Result<(), TransportError> {
    scan.eat_byte(b'{')?;
    let mut first = true;
    loop {
        if scan.eat_byte(b'}').is_ok() {
            break;
        }
        if !first {
            scan.eat_byte(b',')?;
        }
        first = false;
        let key = scan.read_string_raw()?;
        scan.eat_byte(b':')?;
        if key == "coin" && scan.eat_byte(b'[').is_ok() {
            let mut first_coin = true;
            loop {
                if scan.eat_byte(b']').is_ok() {
                    break;
                }
                if !first_coin {
                    scan.eat_byte(b',')?;
                }
                first_coin = false;
                scan.eat_byte(b'{')?;
                let mut crow = UtaWalletCoinRow::default();
                let mut f2 = true;
                loop {
                    if scan.eat_byte(b'}').is_ok() {
                        break;
                    }
                    if !f2 {
                        scan.eat_byte(b',')?;
                    }
                    f2 = false;
                    let ck = scan.read_string_raw()?;
                    scan.eat_byte(b':')?;
                    match ck {
                        "coin" => crow.coin = read_str_opt(scan)?,
                        "walletBalance" => crow.wallet_balance_raw = read_str_opt(scan)?,
                        "availableToWithdraw" | "equity" => {
                            let v = read_str_opt(scan)?;
                            if crow.available_balance_raw.is_empty() {
                                crow.available_balance_raw = v;
                            }
                        }
                        "locked" => crow.locked_raw = read_str_opt(scan)?,
                        _ => {
                            let _ = scan.skip_value()?;
                        }
                    }
                }
                if !crow.coin.is_empty() {
                    *emitted = emitted.saturating_add(1);
                    on_event(UtaFrameEvent::WalletCoin(crow));
                }
            }
        } else {
            let _ = scan.skip_value()?;
        }
    }
    Ok(())
}

/// Scans a single Bybit V5 WebSocket (`trade` or `private`) or HTTPS JSON frame in one pass with zero heap allocation.
pub fn scan_bybit_frame<'a>(
    payload: &'a [u8],
    mut on_event: impl FnMut(UtaFrameEvent<'a>),
) -> Result<UtaScanSummary, TransportError> {
    let mut scan = JsonScan::new(payload);
    scan.eat_byte(b'{')?;

    let mut summary = UtaScanSummary::default();
    let mut topic: &'a str = "";
    let mut op: &'a str = "";
    let mut event_tag: &'a str = "";
    let mut ret_code_str: &'a str = "";
    let mut ret_msg: &'a str = "";
    let mut success_opt: Option<bool> = None;
    let mut reply = UtaCommandReply::default();
    let mut saw_result_or_data_reply = false;
    let mut deferred_data_slice: Option<&'a [u8]> = None;

    let mut first = true;
    loop {
        if scan.eat_byte(b'}').is_ok() {
            break;
        }
        if !first {
            scan.eat_byte(b',')?;
        }
        first = false;
        let key = scan.read_string_raw()?;
        scan.eat_byte(b':')?;
        match key {
            "reqId" | "req_id" => {
                let raw = read_str_opt(&mut scan)?;
                if let Some(id_num) = parse_u64_ascii(raw.as_bytes()) {
                    summary.reply_id = Some(id_num);
                    reply.reply_id = Some(id_num);
                }
            }
            "topic" => {
                topic = scan.read_string_raw()?;
            }
            "op" => {
                op = scan.read_string_raw()?;
                reply.op = match op {
                    "order.create" => UtaReplyOp::Create,
                    "order.amend" => UtaReplyOp::Amend,
                    "order.cancel" => UtaReplyOp::Cancel,
                    _ => UtaReplyOp::Unknown,
                };
            }
            "event" => {
                event_tag = scan.read_string_raw()?;
            }
            "retCode" | "code" => {
                ret_code_str = read_str_opt(&mut scan)?;
                reply.code = ret_code_str;
                if ret_code_str == "10006" || ret_code_str == "429" {
                    summary.rate_limited = true;
                    reply.rate_limited = true;
                }
            }
            "retMsg" | "ret_msg" | "msg" => {
                ret_msg = read_str_opt(&mut scan)?;
                reply.message = ret_msg;
            }
            "success" => {
                let raw = scan.read_primitive_bytes()?;
                if raw == b"true" {
                    success_opt = Some(true);
                } else if raw == b"false" {
                    success_opt = Some(false);
                }
            }
            "header" => {
                if scan.eat_byte(b'{').is_ok() {
                    let mut hf = true;
                    loop {
                        if scan.eat_byte(b'}').is_ok() {
                            break;
                        }
                        if !hf {
                            scan.eat_byte(b',')?;
                        }
                        hf = false;
                        let hk = scan.read_string_raw()?;
                        scan.eat_byte(b':')?;
                        if hk.eq_ignore_ascii_case("X-Bapi-Limit") {
                            let v = read_str_opt(&mut scan)?;
                            reply.bapi_limit = parse_u64_ascii(v.as_bytes());
                        } else if hk.eq_ignore_ascii_case("X-Bapi-Limit-Status") {
                            let v = read_str_opt(&mut scan)?;
                            reply.bapi_limit_status = parse_u64_ascii(v.as_bytes());
                        } else {
                            let _ = scan.skip_value()?;
                        }
                    }
                } else {
                    let _ = scan.skip_value()?;
                }
            }
            "result" => {
                saw_result_or_data_reply = true;
                if scan.eat_byte(b'{').is_ok() {
                    let mut rf = true;
                    loop {
                        if scan.eat_byte(b'}').is_ok() {
                            break;
                        }
                        if !rf {
                            scan.eat_byte(b',')?;
                        }
                        rf = false;
                        let rk = scan.read_string_raw()?;
                        scan.eat_byte(b':')?;
                        match rk {
                            "orderId" => reply.order_id = read_str_opt(&mut scan)?,
                            "orderLinkId" => reply.client_id = read_str_opt(&mut scan)?,
                            "amendId" => reply.amend_or_req_id = read_str_opt(&mut scan)?,
                            "qty" => reply.clamped_qty = read_str_opt(&mut scan)?,
                            "price" => reply.clamped_price = read_str_opt(&mut scan)?,
                            "cumExecQty" => reply.cum_exec_qty = read_str_opt(&mut scan)?,
                            _ => {
                                let _ = scan.skip_value()?;
                            }
                        }
                    }
                } else {
                    let _ = scan.skip_value()?;
                }
            }
            "data" => {
                deferred_data_slice = Some(scan.skip_value()?);
            }
            _ => {
                let _ = scan.skip_value()?;
            }
        }
    }

    if let (Some(limit), Some(rem)) = (reply.bapi_limit, reply.bapi_limit_status) {
        let used = limit.saturating_sub(rem);
        summary.bapi_limit = Some((used, limit));
    }

    // 1. Server maintenance / connection-retire notice detection:
    if event_tag == "notice"
        || matches!(op, "retire" | "disconnect" | "notice")
        || topic == "system.maintenance"
    {
        summary.graceful_drain_connection = true;
        summary.events_emitted = summary.events_emitted.saturating_add(1);
        on_event(UtaFrameEvent::MaintenanceNotice {
            code: ret_code_str,
            message: ret_msg,
        });
        return Ok(summary);
    }

    // 2. Private stream topics (`order`, `execution.fast`, `execution`, `wallet`):
    if !topic.is_empty() {
        let Some(data_bytes) = deferred_data_slice else {
            return Ok(summary);
        };
        let mut dscan = JsonScan::new(data_bytes);
        dscan.eat_byte(b'[')?;
        if topic == "order" || topic.starts_with("order.") {
            let mut df = true;
            loop {
                if dscan.eat_byte(b']').is_ok() {
                    break;
                }
                if !df {
                    dscan.eat_byte(b',')?;
                }
                df = false;
                let row = scan_bybit_order_obj(&mut dscan)?;
                summary.events_emitted = summary.events_emitted.saturating_add(1);
                on_event(UtaFrameEvent::OrderRow(row));
            }
        } else if topic == "execution.fast" || topic.starts_with("execution.fast.") {
            let mut df = true;
            loop {
                if dscan.eat_byte(b']').is_ok() {
                    break;
                }
                if !df {
                    dscan.eat_byte(b',')?;
                }
                df = false;
                let row = scan_bybit_exec_obj(&mut dscan, true)?;
                summary.events_emitted = summary.events_emitted.saturating_add(1);
                on_event(UtaFrameEvent::ExecRow(row));
            }
        } else if topic == "execution" || topic.starts_with("execution.") {
            let mut df = true;
            loop {
                if dscan.eat_byte(b']').is_ok() {
                    break;
                }
                if !df {
                    dscan.eat_byte(b',')?;
                }
                df = false;
                let row = scan_bybit_exec_obj(&mut dscan, false)?;
                if row.exec_type.is_empty() || row.exec_type == "Trade" {
                    summary.events_emitted = summary.events_emitted.saturating_add(1);
                    on_event(UtaFrameEvent::ExecRow(row));
                }
            }
        } else if topic == "wallet" {
            let mut df = true;
            loop {
                if dscan.eat_byte(b']').is_ok() {
                    break;
                }
                if !df {
                    dscan.eat_byte(b',')?;
                }
                df = false;
                scan_bybit_wallet_account_obj(
                    &mut dscan,
                    &mut on_event,
                    &mut summary.events_emitted,
                )?;
            }
        }
        return Ok(summary);
    }

    // 3. Control frames (`auth`, `subscribe`, `pong`, `ping`):
    if matches!(op, "auth" | "subscribe" | "pong" | "ping") {
        let ok = success_opt.unwrap_or(ret_code_str == "0" || ret_msg == "pong");
        summary.events_emitted = summary.events_emitted.saturating_add(1);
        on_event(UtaFrameEvent::Control { op, success: ok });
        return Ok(summary);
    }

    // 4. WS trade `data: {...}` reply object (`/v5/trade` returns `data: {"orderId":...,"orderLinkId":...}`):
    if let Some(data_bytes) = deferred_data_slice {
        saw_result_or_data_reply = true;
        let mut dscan = JsonScan::new(data_bytes);
        if dscan.eat_byte(b'{').is_ok() {
            let mut df = true;
            loop {
                if dscan.eat_byte(b'}').is_ok() {
                    break;
                }
                if !df && dscan.eat_byte(b',').is_err() {
                    break;
                }
                df = false;
                let Ok(dk) = dscan.read_string_raw() else {
                    break;
                };
                if dscan.eat_byte(b':').is_err() {
                    break;
                }
                match dk {
                    "orderId" => {
                        if let Ok(v) = read_str_opt(&mut dscan) {
                            reply.order_id = v;
                        }
                    }
                    "orderLinkId" => {
                        if let Ok(v) = read_str_opt(&mut dscan) {
                            reply.client_id = v;
                        }
                    }
                    "amendId" => {
                        if let Ok(v) = read_str_opt(&mut dscan) {
                            reply.amend_or_req_id = v;
                        }
                    }
                    "qty" => {
                        if let Ok(v) = read_str_opt(&mut dscan) {
                            reply.clamped_qty = v;
                        }
                    }
                    "price" => {
                        if let Ok(v) = read_str_opt(&mut dscan) {
                            reply.clamped_price = v;
                        }
                    }
                    "cumExecQty" => {
                        if let Ok(v) = read_str_opt(&mut dscan) {
                            reply.cum_exec_qty = v;
                        }
                    }
                    _ => {
                        let _ = dscan.skip_value();
                    }
                }
            }
        }
    }

    if !ret_code_str.is_empty() || saw_result_or_data_reply || reply.op != UtaReplyOp::Unknown {
        summary.events_emitted = summary.events_emitted.saturating_add(1);
        on_event(UtaFrameEvent::CommandReply(reply));
    }
    Ok(summary)
}

fn scan_okx_order_obj<'a>(scan: &mut JsonScan<'a>) -> Result<UtaOrderRow<'a>, TransportError> {
    scan.eat_byte(b'{')?;
    let mut row = UtaOrderRow::default();
    let mut first = true;
    loop {
        if scan.eat_byte(b'}').is_ok() {
            break;
        }
        if !first {
            scan.eat_byte(b',')?;
        }
        first = false;
        let key = scan.read_string_raw()?;
        scan.eat_byte(b':')?;
        match key {
            "instType" => row.scope = read_str_opt(scan)?,
            "instId" => row.symbol = read_str_opt(scan)?,
            "ordId" => row.order_id = read_str_opt(scan)?,
            "clOrdId" => row.client_id = read_str_opt(scan)?,
            "reqId" => row.amend_req_id = read_str_opt(scan)?,
            "state" => row.status_raw = read_str_opt(scan)?,
            "side" => row.side_raw = read_str_opt(scan)?,
            "ordType" => row.ord_type_raw = read_str_opt(scan)?,
            "reduceOnly" => {
                let p = read_str_opt(scan)?;
                row.reduce_only = p == "true";
            }
            "px" => row.price_raw = read_str_opt(scan)?,
            "sz" => row.qty_raw = read_str_opt(scan)?,
            "accFillSz" => row.cum_exec_qty_raw = read_str_opt(scan)?,
            "uTime" => {
                let raw = read_str_opt(scan)?;
                row.updated_ms = parse_u64_ascii(raw.as_bytes());
            }
            "code" => row.reject_reason = read_str_opt(scan)?,
            "cancelSource" => row.cancel_source = read_str_opt(scan)?,
            "cancelSourceReason" => row.cancel_source_reason = read_str_opt(scan)?,
            "tradeId" => row.inline_trade_id = read_str_opt(scan)?,
            "fillPx" => row.inline_fill_px = read_str_opt(scan)?,
            "fillSz" => row.inline_fill_sz = read_str_opt(scan)?,
            "fillFee" | "fee" => {
                let f = read_str_opt(scan)?;
                if row.inline_fill_fee.is_empty() && !f.is_empty() {
                    row.inline_fill_fee = f;
                }
            }
            "fillFeeCcy" | "feeCcy" => {
                let c = read_str_opt(scan)?;
                if row.inline_fill_fee_ccy.is_empty() && !c.is_empty() {
                    row.inline_fill_fee_ccy = c;
                }
            }
            "execType" => row.inline_exec_type = read_str_opt(scan)?,
            "fillTime" => {
                let raw = read_str_opt(scan)?;
                row.inline_fill_time_ms = parse_u64_ascii(raw.as_bytes());
            }
            _ => {
                let _ = scan.skip_value()?;
            }
        }
    }
    Ok(row)
}

fn scan_okx_wallet_obj<'a>(
    scan: &mut JsonScan<'a>,
    on_event: &mut impl FnMut(UtaFrameEvent<'a>),
    emitted: &mut u32,
) -> Result<(), TransportError> {
    scan.eat_byte(b'{')?;
    let mut first = true;
    loop {
        if scan.eat_byte(b'}').is_ok() {
            break;
        }
        if !first {
            scan.eat_byte(b',')?;
        }
        first = false;
        let key = scan.read_string_raw()?;
        scan.eat_byte(b':')?;
        if matches!(key, "balData" | "details") && scan.eat_byte(b'[').is_ok() {
            let mut fc = true;
            loop {
                if scan.eat_byte(b']').is_ok() {
                    break;
                }
                if !fc {
                    scan.eat_byte(b',')?;
                }
                fc = false;
                scan.eat_byte(b'{')?;
                let mut crow = UtaWalletCoinRow::default();
                let mut f2 = true;
                loop {
                    if scan.eat_byte(b'}').is_ok() {
                        break;
                    }
                    if !f2 {
                        scan.eat_byte(b',')?;
                    }
                    f2 = false;
                    let ck = scan.read_string_raw()?;
                    scan.eat_byte(b':')?;
                    match ck {
                        "ccy" => crow.coin = read_str_opt(scan)?,
                        "cashBal" | "eq" => {
                            let val = read_str_opt(scan)?;
                            if crow.wallet_balance_raw.is_empty() {
                                crow.wallet_balance_raw = val;
                            }
                        }
                        "availBal" | "availEq" => {
                            let val = read_str_opt(scan)?;
                            if crow.available_balance_raw.is_empty() {
                                crow.available_balance_raw = val;
                            }
                        }
                        "ordFrozen" | "frozenBal" => {
                            let val = read_str_opt(scan)?;
                            if crow.locked_raw.is_empty() {
                                crow.locked_raw = val;
                            }
                        }
                        _ => {
                            let _ = scan.skip_value()?;
                        }
                    }
                }
                if !crow.coin.is_empty() {
                    *emitted = emitted.saturating_add(1);
                    on_event(UtaFrameEvent::WalletCoin(crow));
                }
            }
        } else {
            let _ = scan.skip_value()?;
        }
    }
    Ok(())
}

/// Scans a single OKX V5 WebSocket (`/ws/v5/private`) or HTTPS JSON frame in one pass with zero heap allocation.
pub fn scan_okx_frame<'a>(
    payload: &'a [u8],
    mut on_event: impl FnMut(UtaFrameEvent<'a>),
) -> Result<UtaScanSummary, TransportError> {
    let mut summary = UtaScanSummary::default();
    if payload == b"pong" {
        summary.events_emitted = 1;
        on_event(UtaFrameEvent::Control {
            op: "pong",
            success: true,
        });
        return Ok(summary);
    }
    if payload == b"ping" {
        summary.events_emitted = 1;
        on_event(UtaFrameEvent::Control {
            op: "ping",
            success: true,
        });
        return Ok(summary);
    }

    let mut scan = JsonScan::new(payload);
    scan.eat_byte(b'{')?;

    let mut event_tag: &'a str = "";
    let mut op: &'a str = "";
    let mut channel: &'a str = "";
    let mut code: &'a str = "";
    let mut msg: &'a str = "";
    let mut reply = UtaCommandReply::default();
    let mut deferred_data_slice: Option<&'a [u8]> = None;

    let mut first = true;
    loop {
        if scan.eat_byte(b'}').is_ok() {
            break;
        }
        if !first {
            scan.eat_byte(b',')?;
        }
        first = false;
        let key = scan.read_string_raw()?;
        scan.eat_byte(b':')?;
        match key {
            "id" => {
                let raw = read_str_opt(&mut scan)?;
                if let Some(id_num) = parse_u64_ascii(raw.as_bytes()) {
                    summary.reply_id = Some(id_num);
                    reply.reply_id = Some(id_num);
                }
            }
            "event" => {
                event_tag = scan.read_string_raw()?;
            }
            "op" => {
                op = scan.read_string_raw()?;
                reply.op = match op {
                    "order" => UtaReplyOp::Create,
                    "amend-order" => UtaReplyOp::Amend,
                    "cancel-order" => UtaReplyOp::Cancel,
                    _ => UtaReplyOp::Unknown,
                };
            }
            "code" => {
                code = read_str_opt(&mut scan)?;
                reply.code = code;
                if code == "50011" || code == "429" {
                    summary.rate_limited = true;
                    reply.rate_limited = true;
                }
            }
            "msg" => {
                msg = read_str_opt(&mut scan)?;
                reply.message = msg;
            }
            "arg" => {
                if scan.eat_byte(b'{').is_ok() {
                    let mut af = true;
                    loop {
                        if scan.eat_byte(b'}').is_ok() {
                            break;
                        }
                        if !af {
                            scan.eat_byte(b',')?;
                        }
                        af = false;
                        let ak = scan.read_string_raw()?;
                        scan.eat_byte(b':')?;
                        if ak == "channel" {
                            channel = scan.read_string_raw()?;
                        } else {
                            let _ = scan.skip_value()?;
                        }
                    }
                } else {
                    let _ = scan.skip_value()?;
                }
            }
            "data" => {
                deferred_data_slice = Some(scan.skip_value()?);
            }
            _ => {
                let _ = scan.skip_value()?;
            }
        }
    }

    // 1. Server maintenance notice (`{"event":"notice","code":"64008",...}`):
    if event_tag == "notice" {
        summary.graceful_drain_connection = true;
        summary.events_emitted = summary.events_emitted.saturating_add(1);
        on_event(UtaFrameEvent::MaintenanceNotice { code, message: msg });
        return Ok(summary);
    }

    // 2. Control frames (`login`, `subscribe`, `error` without trade op):
    if !event_tag.is_empty() && op.is_empty() {
        let ok = event_tag != "error" && (code.is_empty() || code == "0");
        summary.events_emitted = summary.events_emitted.saturating_add(1);
        on_event(UtaFrameEvent::Control {
            op: event_tag,
            success: ok,
        });
        return Ok(summary);
    }

    // 3. Private push channels (`orders`, `balance_and_position`, `account`):
    if !channel.is_empty() {
        let Some(data_bytes) = deferred_data_slice else {
            return Ok(summary);
        };
        let mut dscan = JsonScan::new(data_bytes);
        dscan.eat_byte(b'[')?;
        if channel == "orders" {
            let mut df = true;
            loop {
                if dscan.eat_byte(b']').is_ok() {
                    break;
                }
                if !df {
                    dscan.eat_byte(b',')?;
                }
                df = false;
                let row = scan_okx_order_obj(&mut dscan)?;
                // Emit inline individual trade execution BEFORE terminal order status when `tradeId` is non-empty!
                if !row.inline_trade_id.is_empty()
                    && row.inline_trade_id != "0"
                    && !row.inline_fill_sz.is_empty()
                    && row.inline_fill_sz != "0"
                {
                    let liq = match row.inline_exec_type {
                        "M" => Liquidity::Maker,
                        "T" => Liquidity::Taker,
                        _ => Liquidity::Unknown,
                    };
                    let fee_opt = (!row.inline_fill_fee.is_empty()).then_some(row.inline_fill_fee);
                    let ccy_opt =
                        (!row.inline_fill_fee_ccy.is_empty()).then_some(row.inline_fill_fee_ccy);
                    summary.events_emitted = summary.events_emitted.saturating_add(1);
                    on_event(UtaFrameEvent::ExecRow(UtaExecRow {
                        is_fast: fee_opt.is_none(),
                        scope: row.scope,
                        symbol: row.symbol,
                        exec_id: row.inline_trade_id,
                        order_id: row.order_id,
                        client_id: row.client_id,
                        exec_type: row.inline_exec_type,
                        side_raw: row.side_raw,
                        exec_price_raw: row.inline_fill_px,
                        exec_qty_raw: row.inline_fill_sz,
                        exec_fee_raw: fee_opt,
                        fee_currency: ccy_opt,
                        liquidity: liq,
                        exec_time_ms: row.inline_fill_time_ms.or(row.updated_ms),
                    }));
                }
                summary.events_emitted = summary.events_emitted.saturating_add(1);
                on_event(UtaFrameEvent::OrderRow(row));
            }
        } else if channel == "balance_and_position" || channel == "account" {
            let mut df = true;
            loop {
                if dscan.eat_byte(b']').is_ok() {
                    break;
                }
                if !df {
                    dscan.eat_byte(b',')?;
                }
                df = false;
                scan_okx_wallet_obj(&mut dscan, &mut on_event, &mut summary.events_emitted)?;
            }
        }
        return Ok(summary);
    }

    // 4. WS trade (`op: "order" | "amend-order" | "cancel-order"`) or HTTPS `POST /api/v5/trade/*` reply:
    if let Some(data_bytes) = deferred_data_slice {
        let mut dscan = JsonScan::new(data_bytes);
        if dscan.eat_byte(b'[').is_ok() && dscan.eat_byte(b'{').is_ok() {
            let mut df = true;
            loop {
                if dscan.eat_byte(b'}').is_ok() {
                    break;
                }
                if !df && dscan.eat_byte(b',').is_err() {
                    break;
                }
                df = false;
                let Ok(dk) = dscan.read_string_raw() else {
                    break;
                };
                if dscan.eat_byte(b':').is_err() {
                    break;
                }
                match dk {
                    "ordId" => {
                        if let Ok(v) = read_str_opt(&mut dscan) {
                            reply.order_id = v;
                        }
                    }
                    "clOrdId" => {
                        if let Ok(v) = read_str_opt(&mut dscan) {
                            reply.client_id = v;
                        }
                    }
                    "reqId" => {
                        if let Ok(v) = read_str_opt(&mut dscan) {
                            reply.amend_or_req_id = v;
                        }
                    }
                    "newSz" | "sz" => {
                        if let Ok(v) = read_str_opt(&mut dscan)
                            && reply.clamped_qty.is_empty()
                        {
                            reply.clamped_qty = v;
                        }
                    }
                    "newPx" | "px" => {
                        if let Ok(v) = read_str_opt(&mut dscan)
                            && reply.clamped_price.is_empty()
                        {
                            reply.clamped_price = v;
                        }
                    }
                    "accFillSz" => {
                        if let Ok(v) = read_str_opt(&mut dscan) {
                            reply.cum_exec_qty = v;
                        }
                    }
                    "sCode" => {
                        if let Ok(sc) = read_str_opt(&mut dscan) {
                            reply.sub_code = sc;
                            if sc == "50011" {
                                summary.rate_limited = true;
                                reply.rate_limited = true;
                            }
                        }
                    }
                    "sMsg" => {
                        if let Ok(sm) = read_str_opt(&mut dscan)
                            && !sm.is_empty()
                        {
                            reply.message = sm;
                        }
                    }
                    _ => {
                        let _ = dscan.skip_value();
                    }
                }
            }
        }
    }

    if !code.is_empty() || reply.op != UtaReplyOp::Unknown {
        summary.events_emitted = summary.events_emitted.saturating_add(1);
        on_event(UtaFrameEvent::CommandReply(reply));
    }
    Ok(summary)
}

/// Zero-allocation Bybit V5 scanner projection into [`ScannedExecFrame`] + graceful drain flag.
#[derive(Clone, Copy, Debug, Default)]
pub struct BybitUtaParser;

impl BybitUtaParser {
    pub fn scan<'a>(payload: &'a [u8]) -> Result<(ScannedExecFrame<'a>, bool), TransportError> {
        let mut out = ScannedExecFrame::default();
        let summary = scan_bybit_frame(payload, |ev| match ev {
            UtaFrameEvent::CommandReply(r) => {
                out.reply_id_u64 = r.reply_id;
                out.error_code = parse_i64_ascii(r.effective_error_code().as_bytes());
                if !r.message.is_empty() {
                    out.error_msg = Some(r.message);
                }
                if !r.order_id.is_empty() {
                    out.venue_order_id = Some(r.order_id);
                }
                if !r.client_id.is_empty() {
                    out.client_order_id = Some(r.client_id);
                }
            }
            UtaFrameEvent::MaintenanceNotice { code, message } => {
                out.event_kind = Some("notice");
                out.error_code = parse_i64_ascii(code.as_bytes());
                if !message.is_empty() {
                    out.error_msg = Some(message);
                }
            }
            _ => {}
        })?;
        out.reply_id_u64 = summary.reply_id.or(out.reply_id_u64);
        if let Some((used, limit)) = summary.bapi_limit
            && usize::from(out.rate_limit_count) < out.rate_limits.len()
        {
            out.rate_limits[usize::from(out.rate_limit_count)] = ScannedRateLimit {
                rate_limit_type: Some("ORDERS"),
                interval: Some("SECOND"),
                interval_num: Some(1),
                limit: Some(limit),
                count: Some(used),
            };
            out.rate_limit_count += 1;
        }
        Ok((out, summary.graceful_drain_connection))
    }
}

/// Zero-allocation OKX V5 scanner projection into [`ScannedExecFrame`] + graceful drain flag.
#[derive(Clone, Copy, Debug, Default)]
pub struct OkxUtaParser;

impl OkxUtaParser {
    pub fn scan<'a>(payload: &'a [u8]) -> Result<(ScannedExecFrame<'a>, bool), TransportError> {
        let mut out = ScannedExecFrame::default();
        let summary = scan_okx_frame(payload, |ev| match ev {
            UtaFrameEvent::CommandReply(r) => {
                out.reply_id_u64 = r.reply_id;
                out.error_code = parse_i64_ascii(r.effective_error_code().as_bytes());
                if !r.message.is_empty() {
                    out.error_msg = Some(r.message);
                }
                if !r.order_id.is_empty() {
                    out.venue_order_id = Some(r.order_id);
                }
                if !r.client_id.is_empty() {
                    out.client_order_id = Some(r.client_id);
                }
            }
            UtaFrameEvent::MaintenanceNotice { code, message } => {
                out.event_kind = Some("notice");
                out.error_code = parse_i64_ascii(code.as_bytes());
                if !message.is_empty() {
                    out.error_msg = Some(message);
                }
            }
            _ => {}
        })?;
        out.reply_id_u64 = summary.reply_id.or(out.reply_id_u64);
        Ok((out, summary.graceful_drain_connection))
    }
}
