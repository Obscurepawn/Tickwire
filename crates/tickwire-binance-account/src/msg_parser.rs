//! Zero-allocation single-pass `BinanceMsgParser` built on `tickwire_transport::json_scan::JsonScan`
//! (`human-docs/8.5`).
//!
//! Scans any Binance WebSocket API reply (`order.place`, `order.cancel`, `order.modify`,
//! `order.cancelReplace`, `session.logon`, `userDataStream.subscribe`), REST order/status/listenKey
//! payload, or private user-data stream push (`ORDER_TRADE_UPDATE`, `executionReport`,
//! `listenKeyExpired`, `MARGIN_CALL`) in **one single forward pass** with **zero heap allocation**,
//! **zero `serde_json`**, and **zero floating-point operations**.

use tickwire_transport::{
    Error, Result,
    json_scan::{JsonScan, MAX_JSON_DEPTH, parse_i64_ascii, parse_scaled_i64, parse_u64_ascii},
};
use tickwire_venue::Liquidity;

/// Maximum `rateLimits[]` entries captured per Binance reply (`human-docs/8.5`).
pub const MAX_BINANCE_RATE_LIMITS: usize = 4;

const WS_REPLY_ID_MASK: u64 = (1u64 << 62) - 1;

/// Request kind encoded in WebSocket `"id"` prefixes (`""`, `"c"`, `"m"`) and attached to
/// [`tickwire_transport::RequestRef`] hints on European Options `HttpsLink`s (`human-docs/8.4`, `8.5`).
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum BinanceRequestKind {
    #[default]
    Other = 0,
    Place = 1,
    Cancel = 2,
    Modify = 3,
}

impl BinanceRequestKind {
    #[inline]
    pub const fn from_u64(raw: u64) -> Self {
        match raw {
            1 => Self::Place,
            2 => Self::Cancel,
            3 => Self::Modify,
            _ => Self::Other,
        }
    }
}

/// Encodes `(ors_order_id, kind)` into a stack ASCII WebSocket `"id"` (`"<id>"` for `Place`,
/// `"c<id>"` for `Cancel`, `"m<id>"` for `Modify`, `"o<id>"` for `Other`).
pub fn encode_ws_request_id(
    ors_order_id: u64,
    kind: BinanceRequestKind,
    out: &mut [u8; 24],
) -> &str {
    let prefix: Option<u8> = match kind {
        BinanceRequestKind::Place => None,
        BinanceRequestKind::Cancel => Some(b'c'),
        BinanceRequestKind::Modify => Some(b'm'),
        BinanceRequestKind::Other => Some(b'o'),
    };
    let mut digits = [0u8; 20];
    let mut rem = ors_order_id;
    let mut idx = 20usize;
    if rem == 0 {
        idx -= 1;
        digits[idx] = b'0';
    } else {
        while rem > 0 {
            idx -= 1;
            digits[idx] = b'0' + (rem % 10) as u8;
            rem /= 10;
        }
    }
    let digit_slice = &digits[idx..];
    let mut len = 0usize;
    if let Some(p) = prefix {
        out[0] = p;
        len = 1;
    }
    out[len..len + digit_slice.len()].copy_from_slice(digit_slice);
    len += digit_slice.len();
    core::str::from_utf8(&out[..len]).unwrap_or("")
}

/// Decodes a Binance WebSocket reply `"id"` (`"<ors_id>"`, `"c<ors_id>"`, `"m<ors_id>"`) into
/// `(ors_order_id, BinanceRequestKind)` in `O(len)` without allocation.
#[inline]
pub fn decode_ws_request_id(raw: &str) -> Option<(u64, BinanceRequestKind)> {
    let bytes = raw.as_bytes();
    let (&first, rest) = bytes.split_first()?;
    let (kind, digits) = match first {
        b'c' => (BinanceRequestKind::Cancel, rest),
        b'm' => (BinanceRequestKind::Modify, rest),
        b'o' => (BinanceRequestKind::Other, rest),
        b'0'..=b'9' => (BinanceRequestKind::Place, bytes),
        _ => return None,
    };
    let id = parse_u64_ascii(digits)?;
    if id == 0 {
        return None;
    }
    Some((id, kind))
}

/// Packs `(ors_order_id, kind)` into a non-zero `u64` `reply_id` for `WsLink` 16-slot tracking so
/// concurrent `Place`, `Cancel`, and `Modify` frames for the same order never collide.
#[inline]
pub const fn pack_ws_reply_id(ors_order_id: u64, kind: BinanceRequestKind) -> u64 {
    let tag = match kind {
        BinanceRequestKind::Place => 1u64,
        BinanceRequestKind::Cancel => 2u64,
        BinanceRequestKind::Modify => 3u64,
        BinanceRequestKind::Other => 0u64,
    };
    (tag << 62) | (ors_order_id & WS_REPLY_ID_MASK)
}

/// Unpacks a `u64` `WsLink` `reply_id` produced by [`pack_ws_reply_id`].
#[inline]
pub const fn unpack_ws_reply_id(packed: u64) -> Option<(u64, BinanceRequestKind)> {
    if packed == 0 {
        return None;
    }
    let kind = BinanceRequestKind::from_u64(packed >> 62);
    let id_low = packed & WS_REPLY_ID_MASK;
    if id_low == 0 {
        return None;
    }
    Some((id_low, kind))
}

/// Borrowed `rateLimits[]` row extracted in the single `BinanceMsgParser` pass (`human-docs/8.5`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BinanceRateLimitRow<'a> {
    pub rate_limit_type: Option<&'a str>,
    pub interval: Option<&'a str>,
    pub interval_num: Option<u32>,
    pub limit: Option<u64>,
    pub count: Option<u64>,
}

impl<'a> BinanceRateLimitRow<'a> {
    /// Returns `(interval_initial_char, interval_seconds)` for Binance `SECOND`, `MINUTE`, `HOUR`, `DAY`.
    #[inline]
    pub fn interval_spec(&self) -> Option<(u8, u32)> {
        let name = self.interval?;
        let num = self.interval_num.unwrap_or(1);
        if num == 0 {
            return None;
        }
        if name.eq_ignore_ascii_case("SECOND") {
            Some((b'S', num))
        } else if name.eq_ignore_ascii_case("MINUTE") {
            Some((b'M', num.checked_mul(60)?))
        } else if name.eq_ignore_ascii_case("HOUR") {
            Some((b'H', num.checked_mul(3_600)?))
        } else if name.eq_ignore_ascii_case("DAY") {
            Some((b'D', num.checked_mul(86_400)?))
        } else {
            None
        }
    }
}

/// Flat zero-allocation projection of any Binance order reply or user-stream event (`human-docs/8.5`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BinanceParsedFrame<'a> {
    pub request_id: Option<&'a str>,
    pub http_status: Option<u16>,
    pub error_code: Option<i64>,
    pub error_msg: Option<&'a str>,
    pub authorized_since: Option<u64>,
    pub subscription_id: Option<u64>,
    pub listen_key: Option<&'a str>,
    pub event_type: Option<&'a str>,
    pub symbol: Option<&'a str>,
    pub side: Option<&'a str>,
    pub order_type: Option<&'a str>,
    pub time_in_force: Option<&'a str>,
    pub venue_order_id: Option<&'a str>,
    pub client_order_id: Option<&'a str>,
    pub orig_client_order_id: Option<&'a str>,
    pub price: Option<&'a str>,
    pub orig_qty: Option<&'a str>,
    pub executed_qty: Option<&'a str>,
    pub cum_quote_qty: Option<&'a str>,
    pub avg_price: Option<&'a str>,
    pub last_executed_qty: Option<&'a str>,
    pub last_executed_price: Option<&'a str>,
    pub order_status: Option<&'a str>,
    pub execution_type: Option<&'a str>,
    pub trade_id: Option<&'a str>,
    pub commission: Option<&'a str>,
    pub commission_asset: Option<&'a str>,
    pub is_maker: Option<bool>,
    pub reduce_only: Option<bool>,
    pub stp_mode: Option<&'a str>,
    pub expired_reason: Option<&'a str>,
    pub update_time_ms: Option<u64>,
    pub rate_limits: [BinanceRateLimitRow<'a>; MAX_BINANCE_RATE_LIMITS],
    pub rate_limit_count: u8,
}

impl<'a> BinanceParsedFrame<'a> {
    #[inline]
    pub fn rate_limits_slice(&self) -> &[BinanceRateLimitRow<'a>] {
        &self.rate_limits[..usize::from(self.rate_limit_count)]
    }

    #[inline]
    pub const fn has_error(&self) -> bool {
        if let Some(code) = self.error_code
            && code != 0
        {
            return true;
        }
        matches!(self.http_status, Some(s) if s >= 400)
    }

    /// Returns candidate client order IDs in priority order:
    /// on Spot `CANCELED` `executionReport`s, `"C"` (`orig_client_order_id`) names the canceled
    /// resting order while `"c"` names the cancel request itself (`human-docs/8.5`).
    #[inline]
    pub fn client_id_candidates(&self) -> [Option<&'a str>; 2] {
        let is_cancel_report = self
            .execution_type
            .is_some_and(|x| x.eq_ignore_ascii_case("CANCELED"))
            || self
                .order_status
                .is_some_and(|s| s.eq_ignore_ascii_case("CANCELED"));
        if is_cancel_report && self.orig_client_order_id.is_some() {
            [self.orig_client_order_id, self.client_order_id]
        } else {
            [self.client_order_id, self.orig_client_order_id]
        }
    }

    #[inline]
    pub fn valid_trade_id(&self) -> Option<&'a str> {
        let tid = self.trade_id?;
        if tid.is_empty() || tid == "-1" || tid == "0" {
            None
        } else {
            Some(tid)
        }
    }

    #[inline]
    pub fn liquidity(&self) -> Liquidity {
        match self.is_maker {
            Some(true) => Liquidity::Maker,
            Some(false) => Liquidity::Taker,
            None => Liquidity::Unknown,
        }
    }

    #[inline]
    pub fn parse_price_ticks(&self, step_atoms: i128, scale: u8) -> Option<i64> {
        parse_step_i64(self.price?, step_atoms, scale)
    }

    #[inline]
    pub fn parse_orig_qty_lots(&self, step_atoms: i128, scale: u8) -> Option<i64> {
        parse_step_i64(self.orig_qty?, step_atoms, scale)
    }

    #[inline]
    pub fn parse_executed_qty_lots(&self, step_atoms: i128, scale: u8) -> Option<i64> {
        parse_step_i64(self.executed_qty?, step_atoms, scale)
    }

    #[inline]
    pub fn parse_last_qty_lots(&self, step_atoms: i128, scale: u8) -> Option<i64> {
        parse_step_i64(self.last_executed_qty?, step_atoms, scale)
    }

    #[inline]
    pub fn parse_last_price_ticks(&self, step_atoms: i128, scale: u8) -> Option<i64> {
        parse_step_i64(self.last_executed_price?, step_atoms, scale)
    }
}

/// Parses a decimal string into exact integer steps (`atoms / step_atoms`) without floating point.
#[inline]
pub fn parse_step_i64(raw: &str, step_atoms: i128, scale: u8) -> Option<i64> {
    if step_atoms <= 0 {
        return None;
    }
    let scaled = i128::from(parse_scaled_i64(raw, scale)?);
    if scaled % step_atoms != 0 {
        return None;
    }
    i64::try_from(scaled / step_atoms).ok()
}

/// Zero-allocation single-pass Binance execution frame scanner (`human-docs/8.5`).
pub struct BinanceMsgParser;

impl BinanceMsgParser {
    #[inline]
    pub fn parse(bytes: &[u8]) -> Result<BinanceParsedFrame<'_>> {
        let mut cursor = JsonScan::new(bytes);
        let mut out = BinanceParsedFrame::default();
        let Some(first) = cursor.peek() else {
            return Err(Error::Protocol("empty binance json payload"));
        };
        if first != b'{' {
            return Err(Error::Protocol("expected binance json object"));
        }
        scan_binance_object(&mut cursor, &mut out, 0)?;
        cursor.skip_ws();
        if !cursor.is_eof() {
            return Err(Error::Protocol("trailing bytes after binance json"));
        }
        Ok(out)
    }
}

#[inline]
fn set_nonempty<'a>(slot: &mut Option<&'a str>, candidate: Option<&'a str>) {
    if slot.is_none()
        && let Some(v) = candidate
        && !v.is_empty()
    {
        *slot = Some(v);
    }
}

#[inline]
fn parse_bool_str(raw: &str) -> Option<bool> {
    if raw.eq_ignore_ascii_case("true") {
        Some(true)
    } else if raw.eq_ignore_ascii_case("false") {
        Some(false)
    } else {
        None
    }
}

fn scan_binance_object<'a>(
    cursor: &mut JsonScan<'a>,
    out: &mut BinanceParsedFrame<'a>,
    depth: usize,
) -> Result<()> {
    if depth >= MAX_JSON_DEPTH {
        return Err(Error::Bounds);
    }
    cursor.eat_byte(b'{')?;
    if cursor.peek() == Some(b'}') {
        cursor.eat_byte(b'}')?;
        return Ok(());
    }

    loop {
        let key = cursor.read_string_raw()?;
        cursor.eat_byte(b':')?;

        match key {
            "result" | "error" | "data" | "order" | "newOrderResponse" => {
                if cursor.peek() == Some(b'{') {
                    scan_binance_object(cursor, out, depth + 1)?;
                } else {
                    let _ = cursor.skip_value()?;
                }
            }
            "event" => {
                if cursor.peek() == Some(b'{') {
                    scan_binance_object(cursor, out, depth + 1)?;
                } else {
                    let v = cursor.read_string_or_scalar()?;
                    set_nonempty(&mut out.event_type, v);
                }
            }
            "o" => {
                if cursor.peek() == Some(b'{') {
                    scan_binance_object(cursor, out, depth + 1)?;
                } else {
                    let v = cursor.read_string_or_scalar()?;
                    set_nonempty(&mut out.order_type, v);
                }
            }
            // Skip Spot `fills[]` and Spot `cancelReplace` `cancelResponse` in one jump (`8.5`).
            "fills" | "cancelResponse" => {
                let _ = cursor.skip_value()?;
            }
            "rateLimits" => {
                if cursor.peek() == Some(b'[') {
                    scan_rate_limits(cursor, out, depth + 1)?;
                } else {
                    let _ = cursor.skip_value()?;
                }
            }
            "id" if depth == 0 => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.request_id, v);
            }
            "status" => {
                if cursor.peek() == Some(b'"') {
                    let s = cursor.read_string_raw()?;
                    if !s.is_empty() {
                        out.order_status = Some(s);
                    }
                } else if let Some(s) = cursor.read_string_or_scalar()?
                    && let Some(num) = parse_u64_ascii(s.as_bytes())
                    && let Ok(code) = u16::try_from(num)
                {
                    out.http_status = Some(code);
                }
            }
            "X" => {
                let v = cursor.read_string_or_scalar()?;
                if let Some(s) = v
                    && !s.is_empty()
                {
                    out.order_status = Some(s);
                }
            }
            "x" | "executionType" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.execution_type, v);
            }
            "code" => {
                if let Some(s) = cursor.read_string_or_scalar()?
                    && let Some(code) = parse_i64_ascii(s.as_bytes())
                    && (out.error_code.is_none() || out.error_code == Some(0))
                {
                    out.error_code = Some(code);
                }
            }
            "msg" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.error_msg, v);
            }
            "authorizedSince" => {
                if let Some(s) = cursor.read_string_or_scalar()? {
                    out.authorized_since = parse_u64_ascii(s.as_bytes());
                }
            }
            "subscriptionId" => {
                if let Some(s) = cursor.read_string_or_scalar()? {
                    out.subscription_id = parse_u64_ascii(s.as_bytes());
                }
            }
            "listenKey" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.listen_key, v);
            }
            "e" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.event_type, v);
            }
            "symbol" | "s" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.symbol, v);
            }
            "side" | "S" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.side, v);
            }
            "type" | "origType" | "ot" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.order_type, v);
            }
            "timeInForce" | "f" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.time_in_force, v);
            }
            "orderId" | "i" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.venue_order_id, v);
            }
            "clientOrderId" | "newClientOrderId" | "c" => {
                let v = cursor.read_string_or_scalar()?;
                if let Some(val) = v
                    && !val.is_empty()
                    && val != "null"
                {
                    out.client_order_id = Some(val);
                }
            }
            "origClientOrderId" | "C" => {
                let v = cursor.read_string_or_scalar()?;
                if let Some(val) = v
                    && !val.is_empty()
                    && val != "null"
                {
                    out.orig_client_order_id = Some(val);
                }
            }
            "price" | "p" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.price, v);
            }
            "origQty" | "quantity" | "q" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.orig_qty, v);
            }
            "executedQty" | "z" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.executed_qty, v);
            }
            "cummulativeQuoteQty" | "cumQuote" | "Z" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.cum_quote_qty, v);
            }
            "avgPrice" | "ap" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.avg_price, v);
            }
            "lastExecutedQty" | "lastQty" | "l" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.last_executed_qty, v);
            }
            "lastExecutedPrice" | "lastPrice" | "L" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.last_executed_price, v);
            }
            "tradeId" | "t" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.trade_id, v);
            }
            "commission" | "fee" | "n" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.commission, v);
            }
            "commissionAsset" | "N" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.commission_asset, v);
            }
            "m" | "isMaker" => {
                if let Some(raw) = cursor.read_string_or_scalar()?
                    && out.is_maker.is_none()
                {
                    out.is_maker = parse_bool_str(raw);
                }
            }
            "R" | "reduceOnly" => {
                if let Some(raw) = cursor.read_string_or_scalar()?
                    && out.reduce_only.is_none()
                {
                    out.reduce_only = parse_bool_str(raw);
                }
            }
            "selfTradePreventionMode" | "V" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.stp_mode, v);
            }
            "expiredReason" | "er" => {
                let v = cursor.read_string_or_scalar()?;
                set_nonempty(&mut out.expired_reason, v);
            }
            "updateTime" | "transactTime" | "T" | "E" => {
                if let Some(raw) = cursor.read_string_or_scalar()?
                    && (out.update_time_ms.is_none() || key != "E")
                    && let Some(ts) = parse_u64_ascii(raw.as_bytes())
                {
                    out.update_time_ms = Some(ts);
                }
            }
            _ => {
                let _ = cursor.skip_value()?;
            }
        }

        match cursor.peek() {
            Some(b',') => cursor.eat_byte(b',')?,
            Some(b'}') => {
                cursor.eat_byte(b'}')?;
                break;
            }
            _ => return Err(Error::Protocol("malformed binance json object")),
        }
    }

    Ok(())
}

fn scan_rate_limits<'a>(
    cursor: &mut JsonScan<'a>,
    out: &mut BinanceParsedFrame<'a>,
    depth: usize,
) -> Result<()> {
    if depth >= MAX_JSON_DEPTH {
        return Err(Error::Bounds);
    }
    cursor.eat_byte(b'[')?;
    if cursor.peek() == Some(b']') {
        cursor.eat_byte(b']')?;
        return Ok(());
    }

    loop {
        if cursor.peek() == Some(b'{')
            && usize::from(out.rate_limit_count) < MAX_BINANCE_RATE_LIMITS
        {
            let row = scan_rate_limit_row(cursor, depth + 1)?;
            out.rate_limits[usize::from(out.rate_limit_count)] = row;
            out.rate_limit_count += 1;
        } else {
            let _ = cursor.skip_value()?;
        }

        match cursor.peek() {
            Some(b',') => cursor.eat_byte(b',')?,
            Some(b']') => {
                cursor.eat_byte(b']')?;
                break;
            }
            _ => return Err(Error::Protocol("malformed binance rateLimits array")),
        }
    }

    Ok(())
}

fn scan_rate_limit_row<'a>(
    cursor: &mut JsonScan<'a>,
    depth: usize,
) -> Result<BinanceRateLimitRow<'a>> {
    if depth >= MAX_JSON_DEPTH {
        return Err(Error::Bounds);
    }
    cursor.eat_byte(b'{')?;
    let mut row = BinanceRateLimitRow::default();
    if cursor.peek() == Some(b'}') {
        cursor.eat_byte(b'}')?;
        return Ok(row);
    }

    loop {
        let key = cursor.read_string_raw()?;
        cursor.eat_byte(b':')?;
        match key {
            "rateLimitType" => {
                row.rate_limit_type = cursor.read_string_or_scalar()?;
            }
            "interval" => {
                row.interval = cursor.read_string_or_scalar()?;
            }
            "intervalNum" => {
                if let Some(raw) = cursor.read_string_or_scalar()? {
                    row.interval_num =
                        parse_u64_ascii(raw.as_bytes()).and_then(|v| u32::try_from(v).ok());
                }
            }
            "limit" => {
                if let Some(raw) = cursor.read_string_or_scalar()? {
                    row.limit = parse_u64_ascii(raw.as_bytes());
                }
            }
            "count" | "used" => {
                if let Some(raw) = cursor.read_string_or_scalar()? {
                    row.count = parse_u64_ascii(raw.as_bytes());
                }
            }
            _ => {
                let _ = cursor.skip_value()?;
            }
        }

        match cursor.peek() {
            Some(b',') => cursor.eat_byte(b',')?,
            Some(b'}') => {
                cursor.eat_byte(b'}')?;
                break;
            }
            _ => return Err(Error::Protocol("malformed binance rateLimit row")),
        }
    }

    Ok(row)
}
