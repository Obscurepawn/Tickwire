//! Zero-allocation single-pass byte-slice JSON scanner (`JsonScan` / `MsgParser`) for venue
//! execution replies, private stream frames, `rateLimits[]` counters, and error envelopes.
#![forbid(unsafe_code)]

use crate::{Error, Result};

pub const MAX_JSON_DEPTH: usize = 16;
pub const MAX_SCANNED_RATE_LIMITS: usize = 8;

/// Borrowed `rateLimits[]` entry extracted without heap allocation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScannedRateLimit<'a> {
    pub rate_limit_type: Option<&'a str>,
    pub interval: Option<&'a str>,
    pub interval_num: Option<u32>,
    pub limit: Option<u64>,
    pub count: Option<u64>,
}

/// Zero-allocation single-pass projection of a Binance, OKX, or Bybit execution reply or private
/// stream frame.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScannedExecFrame<'a> {
    pub request_id: Option<&'a str>,
    pub reply_id_u64: Option<u64>,
    pub http_status: Option<u16>,
    pub event_kind: Option<&'a str>,
    pub symbol: Option<&'a str>,
    pub side: Option<&'a str>,
    pub venue_order_id: Option<&'a str>,
    pub client_order_id: Option<&'a str>,
    pub orig_client_order_id: Option<&'a str>,
    pub trade_id: Option<&'a str>,
    pub status: Option<&'a str>,
    pub exec_type: Option<&'a str>,
    pub price_raw: Option<&'a str>,
    pub price_ticks: Option<i64>,
    pub orig_qty_raw: Option<&'a str>,
    pub orig_lots: Option<i64>,
    pub cum_fill_qty_raw: Option<&'a str>,
    pub cum_fill_lots: Option<i64>,
    pub last_fill_qty_raw: Option<&'a str>,
    pub last_fill_lots: Option<i64>,
    pub last_fill_price_raw: Option<&'a str>,
    pub last_fill_ticks: Option<i64>,
    pub fee_raw: Option<&'a str>,
    pub fee_scaled: Option<i64>,
    pub fee_asset: Option<&'a str>,
    pub event_time_ms: Option<u64>,
    pub error_code: Option<i64>,
    pub error_msg: Option<&'a str>,
    pub listen_key: Option<&'a str>,
    pub challenge: Option<&'a str>,
    pub rate_limits: [ScannedRateLimit<'a>; MAX_SCANNED_RATE_LIMITS],
    pub rate_limit_count: u8,
}

impl<'a> ScannedExecFrame<'a> {
    #[inline]
    pub fn rate_limits_slice(&self) -> &[ScannedRateLimit<'a>] {
        &self.rate_limits[..usize::from(self.rate_limit_count)]
    }

    /// Returns `true` when `error_code` indicates a non-zero venue error (e.g. Binance `< 0` or
    /// OKX/Bybit `> 0`).
    #[inline]
    pub const fn has_venue_error(&self) -> bool {
        matches!(self.error_code, Some(code) if code != 0)
    }
}

/// Low-level zero-allocation forward cursor over a JSON byte slice.
#[derive(Clone, Copy, Debug)]
pub struct JsonScan<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> JsonScan<'a> {
    #[inline]
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    #[inline]
    pub const fn position(&self) -> usize {
        self.pos
    }

    #[inline]
    pub fn remaining(&self) -> &'a [u8] {
        &self.bytes[self.pos..]
    }

    #[inline]
    pub fn is_eof(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    #[inline]
    pub fn skip_ws(&mut self) {
        while self.pos < self.bytes.len() {
            match self.bytes[self.pos] {
                b' ' | b'\t' | b'\r' | b'\n' => self.pos += 1,
                _ => break,
            }
        }
    }

    #[inline]
    pub fn peek(&mut self) -> Option<u8> {
        self.skip_ws();
        self.bytes.get(self.pos).copied()
    }

    #[inline]
    pub fn eat_byte(&mut self, expected: u8) -> Result<()> {
        self.skip_ws();
        if self.bytes.get(self.pos) == Some(&expected) {
            self.pos += 1;
            Ok(())
        } else {
            Err(Error::Protocol("unexpected json token"))
        }
    }

    /// Reads a JSON string literal `"..."` and returns the borrowed raw interior UTF-8 `&'a str`
    /// without allocating.
    pub fn read_string_raw(&mut self) -> Result<&'a str> {
        self.eat_byte(b'"')?;
        let start = self.pos;
        while self.pos < self.bytes.len() {
            let b = self.bytes[self.pos];
            if b == b'"' {
                let end = self.pos;
                self.pos += 1;
                return core::str::from_utf8(&self.bytes[start..end])
                    .map_err(|_| Error::Protocol("invalid json utf8"));
            }
            if b == b'\\' {
                self.pos += 1;
                if self.pos >= self.bytes.len() {
                    return Err(Error::Protocol("truncated json escape"));
                }
                if self.bytes[self.pos] == b'u' {
                    if self.pos + 4 >= self.bytes.len() {
                        return Err(Error::Protocol("truncated unicode escape"));
                    }
                    for hex in &self.bytes[self.pos + 1..=self.pos + 4] {
                        if !hex.is_ascii_hexdigit() {
                            return Err(Error::Protocol("invalid unicode escape"));
                        }
                    }
                    self.pos += 5;
                    continue;
                }
                self.pos += 1;
                continue;
            }
            if b < 0x20 {
                return Err(Error::Protocol("control char in json string"));
            }
            self.pos += 1;
        }
        Err(Error::Protocol("unterminated json string"))
    }

    /// Reads a JSON string OR scalar token (number / `true` / `false` / `null`) as `&'a str`,
    /// returning `Ok(None)` if the token is literal `null`.
    pub fn read_string_or_scalar(&mut self) -> Result<Option<&'a str>> {
        let Some(first) = self.peek() else {
            return Err(Error::Protocol("unexpected json eof"));
        };
        if first == b'"' {
            return self.read_string_raw().map(Some);
        }
        let raw = self.read_primitive_bytes()?;
        if raw == b"null" {
            return Ok(None);
        }
        let s = core::str::from_utf8(raw).map_err(|_| Error::Protocol("invalid scalar utf8"))?;
        Ok(Some(s))
    }

    /// Reads a non-string scalar token (`number`, `true`, `false`, `null`).
    pub fn read_primitive_bytes(&mut self) -> Result<&'a [u8]> {
        self.skip_ws();
        let start = self.pos;
        while self.pos < self.bytes.len() {
            match self.bytes[self.pos] {
                b' ' | b'\t' | b'\r' | b'\n' | b',' | b'}' | b']' => break,
                b'"' | b'{' | b'[' | b':' => {
                    return Err(Error::Protocol("unexpected delimiter in json scalar"));
                }
                _ => self.pos += 1,
            }
        }
        if self.pos == start {
            return Err(Error::Protocol("empty json scalar"));
        }
        Ok(&self.bytes[start..self.pos])
    }

    /// Skips one complete JSON value (string, primitive, object, or array) with bounded nesting
    /// `<= MAX_JSON_DEPTH` and zero heap allocation, returning the raw slice of that value.
    pub fn skip_value(&mut self) -> Result<&'a [u8]> {
        self.skip_ws();
        let start = self.pos;
        let Some(&first) = self.bytes.get(self.pos) else {
            return Err(Error::Protocol("unexpected json eof"));
        };
        if first == b'"' {
            let _ = self.read_string_raw()?;
            return Ok(&self.bytes[start..self.pos]);
        }
        if first != b'{' && first != b'[' {
            let _ = self.read_primitive_bytes()?;
            return Ok(&self.bytes[start..self.pos]);
        }

        let mut stack = [0u8; MAX_JSON_DEPTH];
        let mut depth = 0usize;
        stack[0] = if first == b'{' { b'}' } else { b']' };
        depth += 1;
        self.pos += 1;

        while depth > 0 {
            self.skip_ws();
            let Some(&b) = self.bytes.get(self.pos) else {
                return Err(Error::Protocol("unterminated json container"));
            };
            if b == b'"' {
                let _ = self.read_string_raw()?;
                continue;
            }
            if b == b'{' || b == b'[' {
                if depth >= MAX_JSON_DEPTH {
                    return Err(Error::Bounds);
                }
                stack[depth] = if b == b'{' { b'}' } else { b']' };
                depth += 1;
                self.pos += 1;
                continue;
            }
            if b == b'}' || b == b']' {
                if stack[depth - 1] != b {
                    return Err(Error::Protocol("mismatched json bracket"));
                }
                depth -= 1;
                self.pos += 1;
                continue;
            }
            self.pos += 1;
        }
        Ok(&self.bytes[start..self.pos])
    }
}

/// Parses an unsigned 64-bit integer from ASCII digits without allocation.
#[inline]
pub fn parse_u64_ascii(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() {
        return None;
    }
    let mut acc = 0u64;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        acc = acc.checked_mul(10)?.checked_add(u64::from(b - b'0'))?;
    }
    Some(acc)
}

/// Parses a signed 64-bit integer from ASCII digits without allocation.
#[inline]
pub fn parse_i64_ascii(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let (neg, digits) = if let Some(rest) = bytes.strip_prefix(b"-") {
        (true, rest)
    } else {
        (false, bytes)
    };
    if digits.is_empty() {
        return None;
    }
    let mut acc = 0u128;
    for &b in digits {
        if !b.is_ascii_digit() {
            return None;
        }
        acc = acc.checked_mul(10)?.checked_add(u128::from(b - b'0'))?;
    }
    if neg {
        if acc == (1u128 << 63) {
            Some(i64::MIN)
        } else {
            i64::try_from(acc).ok()?.checked_neg()
        }
    } else {
        i64::try_from(acc).ok()
    }
}

/// Parses an exact decimal string into a signed integer scaled by `10^decimals` using `i128`
/// integer arithmetic (zero floating-point operations).
///
/// Extra fractional digits beyond `decimals` are accepted if and only if they are all `'0'`;
/// non-zero excess precision returns `None` rather than rounding silently.
pub fn parse_scaled_i64(raw: &str, decimals: u8) -> Option<i64> {
    if raw.is_empty() || decimals > 18 {
        return None;
    }
    let bytes = raw.as_bytes();
    let (neg, s) = if let Some(rest) = bytes.strip_prefix(b"-") {
        (true, rest)
    } else {
        (false, bytes)
    };
    if s.is_empty() || s.starts_with(b".") || s.ends_with(b".") {
        return None;
    }

    let mut mag = 0u128;
    let mut in_frac = false;
    let mut frac_digits = 0u8;
    let mut digits_seen = 0usize;

    for &b in s {
        if b == b'.' {
            if in_frac {
                return None;
            }
            in_frac = true;
            continue;
        }
        if !b.is_ascii_digit() {
            return None;
        }
        digits_seen += 1;
        if in_frac {
            if frac_digits < decimals {
                mag = mag.checked_mul(10)?.checked_add(u128::from(b - b'0'))?;
                frac_digits += 1;
            } else if b != b'0' {
                return None;
            }
        } else {
            mag = mag.checked_mul(10)?.checked_add(u128::from(b - b'0'))?;
        }
    }

    if digits_seen == 0 {
        return None;
    }
    while frac_digits < decimals {
        mag = mag.checked_mul(10)?;
        frac_digits += 1;
    }

    if neg {
        if mag == 0 {
            Some(0)
        } else if mag == (1u128 << 63) {
            Some(i64::MIN)
        } else {
            i64::try_from(mag).ok()?.checked_neg()
        }
    } else {
        i64::try_from(mag).ok()
    }
}

/// Converts a decimal string into exact integer ticks or lots given `step = step_mantissa * 10^-step_decimals`.
#[inline]
pub fn parse_step_units_i64(raw: &str, step_mantissa: i64, step_decimals: u8) -> Option<i64> {
    if step_mantissa <= 0 {
        return None;
    }
    let scaled = parse_scaled_i64(raw, step_decimals)?;
    if scaled % step_mantissa != 0 {
        return None;
    }
    Some(scaled / step_mantissa)
}

/// Zero-allocation single-pass execution scanner across Binance, OKX, and Bybit JSON frames.
pub struct MsgParser;

impl MsgParser {
    #[inline]
    pub fn scan(bytes: &[u8]) -> Result<ScannedExecFrame<'_>> {
        Self::scan_inner(bytes, None, None)
    }

    #[inline]
    pub fn scan_with_steps(
        bytes: &[u8],
        price_decimals: u8,
        qty_decimals: u8,
    ) -> Result<ScannedExecFrame<'_>> {
        Self::scan_inner(bytes, Some(price_decimals), Some(qty_decimals))
    }

    fn scan_inner(
        bytes: &[u8],
        price_decimals: Option<u8>,
        qty_decimals: Option<u8>,
    ) -> Result<ScannedExecFrame<'_>> {
        let mut cursor = JsonScan::new(bytes);
        let mut out = ScannedExecFrame::default();
        let Some(first) = cursor.peek() else {
            return Err(Error::Protocol("empty json payload"));
        };
        if first == b'{' {
            scan_object(&mut cursor, &mut out, price_decimals, qty_decimals, 0)?;
        } else if first == b'[' {
            scan_payload_array(&mut cursor, &mut out, price_decimals, qty_decimals, 0)?;
        } else {
            return Err(Error::Protocol("expected json object or array"));
        }
        cursor.skip_ws();
        if !cursor.is_eof() {
            return Err(Error::Protocol("trailing json bytes"));
        }
        Ok(out)
    }
}

fn scan_payload_array<'a>(
    cursor: &mut JsonScan<'a>,
    out: &mut ScannedExecFrame<'a>,
    price_decimals: Option<u8>,
    qty_decimals: Option<u8>,
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
    let mut index = 0usize;
    loop {
        if index == 0 && cursor.peek() == Some(b'{') {
            scan_object(cursor, out, price_decimals, qty_decimals, depth + 1)?;
        } else {
            let _ = cursor.skip_value()?;
        }
        index += 1;
        match cursor.peek() {
            Some(b',') => {
                cursor.eat_byte(b',')?;
            }
            Some(b']') => {
                cursor.eat_byte(b']')?;
                break;
            }
            _ => return Err(Error::Protocol("malformed json array")),
        }
    }
    Ok(())
}

fn scan_rate_limits_array<'a>(
    cursor: &mut JsonScan<'a>,
    out: &mut ScannedExecFrame<'a>,
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
            && usize::from(out.rate_limit_count) < MAX_SCANNED_RATE_LIMITS
        {
            let entry = scan_rate_limit_item(cursor, depth + 1)?;
            out.rate_limits[usize::from(out.rate_limit_count)] = entry;
            out.rate_limit_count += 1;
        } else {
            let _ = cursor.skip_value()?;
        }
        match cursor.peek() {
            Some(b',') => {
                cursor.eat_byte(b',')?;
            }
            Some(b']') => {
                cursor.eat_byte(b']')?;
                break;
            }
            _ => return Err(Error::Protocol("malformed rateLimits array")),
        }
    }
    Ok(())
}

fn scan_rate_limit_item<'a>(
    cursor: &mut JsonScan<'a>,
    depth: usize,
) -> Result<ScannedRateLimit<'a>> {
    if depth >= MAX_JSON_DEPTH {
        return Err(Error::Bounds);
    }
    cursor.eat_byte(b'{')?;
    let mut item = ScannedRateLimit::default();
    if cursor.peek() == Some(b'}') {
        cursor.eat_byte(b'}')?;
        return Ok(item);
    }
    loop {
        let key = cursor.read_string_raw()?;
        cursor.eat_byte(b':')?;
        match key {
            "rateLimitType" => {
                item.rate_limit_type = cursor.read_string_or_scalar()?;
            }
            "interval" => {
                item.interval = cursor.read_string_or_scalar()?;
            }
            "intervalNum" => {
                if let Some(s) = cursor.read_string_or_scalar()? {
                    item.interval_num =
                        parse_u64_ascii(s.as_bytes()).and_then(|v| u32::try_from(v).ok());
                }
            }
            "limit" => {
                if let Some(s) = cursor.read_string_or_scalar()? {
                    item.limit = parse_u64_ascii(s.as_bytes());
                }
            }
            "count" | "used" => {
                if let Some(s) = cursor.read_string_or_scalar()? {
                    item.count = parse_u64_ascii(s.as_bytes());
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
            _ => return Err(Error::Protocol("malformed rateLimits object")),
        }
    }
    Ok(item)
}

fn set_if_empty_nonempty<'a>(slot: &mut Option<&'a str>, candidate: Option<&'a str>) {
    if slot.is_none()
        && let Some(val) = candidate
        && !val.is_empty()
    {
        *slot = Some(val);
    }
}

fn scan_object<'a>(
    cursor: &mut JsonScan<'a>,
    out: &mut ScannedExecFrame<'a>,
    price_decimals: Option<u8>,
    qty_decimals: Option<u8>,
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
            "result" | "data" | "order" | "o" | "error" => match cursor.peek() {
                Some(b'{') => {
                    scan_object(cursor, out, price_decimals, qty_decimals, depth + 1)?;
                }
                Some(b'[') => {
                    scan_payload_array(cursor, out, price_decimals, qty_decimals, depth + 1)?;
                }
                _ => {
                    let _ = cursor.skip_value()?;
                }
            },
            "rateLimits" => {
                if cursor.peek() == Some(b'[') {
                    scan_rate_limits_array(cursor, out, depth + 1)?;
                } else {
                    let _ = cursor.skip_value()?;
                }
            }
            "id" | "reqId" if depth == 0 => {
                if let Some(s) = cursor.read_string_or_scalar()? {
                    set_if_empty_nonempty(&mut out.request_id, Some(s));
                    if out.reply_id_u64.is_none() {
                        out.reply_id_u64 = parse_u64_ascii(s.as_bytes());
                    }
                }
            }
            "e" | "topic" | "channel" | "op" | "event" => {
                let v = cursor.read_string_or_scalar()?;
                set_if_empty_nonempty(&mut out.event_kind, v);
            }
            "orderId" | "ordId" | "i" => {
                let v = cursor.read_string_or_scalar()?;
                set_if_empty_nonempty(&mut out.venue_order_id, v);
            }
            "origClientOrderId" => {
                let v = cursor.read_string_or_scalar()?;
                set_if_empty_nonempty(&mut out.orig_client_order_id, v);
                set_if_empty_nonempty(&mut out.client_order_id, v);
            }
            "clientOrderId" | "newClientOrderId" | "clOrdId" | "orderLinkId" | "c" => {
                let v = cursor.read_string_or_scalar()?;
                if (out.client_order_id.is_none() || key == "c" || key == "clientOrderId")
                    && let Some(val) = v
                    && !val.is_empty()
                {
                    out.client_order_id = Some(val);
                }
            }
            "tradeId" | "t" | "execId" | "fillId" => {
                let v = cursor.read_string_or_scalar()?;
                set_if_empty_nonempty(&mut out.trade_id, v);
            }
            "symbol" | "s" | "instId" => {
                let v = cursor.read_string_or_scalar()?;
                set_if_empty_nonempty(&mut out.symbol, v);
            }
            "side" | "S" => {
                let v = cursor.read_string_or_scalar()?;
                set_if_empty_nonempty(&mut out.side, v);
            }
            "status" => {
                if cursor.peek() == Some(b'"') {
                    let s = cursor.read_string_raw()?;
                    if !s.is_empty() {
                        out.status = Some(s);
                    }
                } else if let Some(s) = cursor.read_string_or_scalar()?
                    && let Some(code) = parse_u64_ascii(s.as_bytes())
                    && let Ok(http_code) = u16::try_from(code)
                {
                    out.http_status = Some(http_code);
                }
            }
            "X" | "state" | "orderStatus" => {
                let v = cursor.read_string_or_scalar()?;
                if let Some(val) = v
                    && !val.is_empty()
                {
                    out.status = Some(val);
                }
            }
            "x" | "execType" => {
                let v = cursor.read_string_or_scalar()?;
                set_if_empty_nonempty(&mut out.exec_type, v);
            }
            "price" | "p" | "px" => {
                if let Some(raw) = cursor.read_string_or_scalar()?
                    && !raw.is_empty()
                    && out.price_raw.is_none()
                {
                    out.price_raw = Some(raw);
                    if let Some(dec) = price_decimals {
                        out.price_ticks = parse_scaled_i64(raw, dec);
                    }
                }
            }
            "origQty" | "q" | "sz" | "qty" => {
                if let Some(raw) = cursor.read_string_or_scalar()?
                    && !raw.is_empty()
                    && out.orig_qty_raw.is_none()
                {
                    out.orig_qty_raw = Some(raw);
                    if let Some(dec) = qty_decimals {
                        out.orig_lots = parse_scaled_i64(raw, dec);
                    }
                }
            }
            "executedQty" | "z" | "accFillSz" | "cumExecQty" => {
                if let Some(raw) = cursor.read_string_or_scalar()?
                    && !raw.is_empty()
                    && out.cum_fill_qty_raw.is_none()
                {
                    out.cum_fill_qty_raw = Some(raw);
                    if let Some(dec) = qty_decimals {
                        out.cum_fill_lots = parse_scaled_i64(raw, dec);
                    }
                }
            }
            "l" | "lastQty" | "fillSz" | "execQty" => {
                if let Some(raw) = cursor.read_string_or_scalar()?
                    && !raw.is_empty()
                    && out.last_fill_qty_raw.is_none()
                {
                    out.last_fill_qty_raw = Some(raw);
                    if let Some(dec) = qty_decimals {
                        out.last_fill_lots = parse_scaled_i64(raw, dec);
                    }
                }
            }
            "L" | "lastPrice" | "fillPx" | "execPrice" => {
                if let Some(raw) = cursor.read_string_or_scalar()?
                    && !raw.is_empty()
                    && out.last_fill_price_raw.is_none()
                {
                    out.last_fill_price_raw = Some(raw);
                    if let Some(dec) = price_decimals {
                        out.last_fill_ticks = parse_scaled_i64(raw, dec);
                    }
                }
            }
            "n" | "commission" | "fee" | "execFee" => {
                if let Some(raw) = cursor.read_string_or_scalar()?
                    && !raw.is_empty()
                    && out.fee_raw.is_none()
                {
                    out.fee_raw = Some(raw);
                    if let Some(dec) = price_decimals {
                        out.fee_scaled = parse_scaled_i64(raw, dec);
                    }
                }
            }
            "N" | "commissionAsset" | "feeCcy" | "feeCurrency" => {
                let v = cursor.read_string_or_scalar()?;
                set_if_empty_nonempty(&mut out.fee_asset, v);
            }
            "E" | "T" | "transactTime" | "uTime" | "cTime" | "updatedTime" => {
                if let Some(raw) = cursor.read_string_or_scalar()?
                    && out.event_time_ms.is_none()
                {
                    out.event_time_ms = parse_u64_ascii(raw.as_bytes());
                }
            }
            "code" | "sCode" | "retCode" => {
                if let Some(raw) = cursor.read_string_or_scalar()?
                    && let Some(parsed) = parse_i64_ascii(raw.as_bytes())
                    && (out.error_code.is_none()
                        || out.error_code == Some(0)
                        || (key == "sCode" && parsed != 0))
                {
                    out.error_code = Some(parsed);
                }
            }
            "msg" | "sMsg" | "retMsg" | "message" => {
                if let Some(raw) = cursor.read_string_or_scalar()?
                    && !raw.is_empty()
                    && (out.error_msg.is_none() || key == "sMsg")
                {
                    out.error_msg = Some(raw);
                }
            }
            "listenKey" | "urlToken" | "token" => {
                let v = cursor.read_string_or_scalar()?;
                set_if_empty_nonempty(&mut out.listen_key, v);
            }
            "challenge" | "nonce" => {
                let v = cursor.read_string_or_scalar()?;
                set_if_empty_nonempty(&mut out.challenge, v);
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
            _ => return Err(Error::Protocol("malformed json object")),
        }
    }

    Ok(())
}
