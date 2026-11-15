//! Zero-allocation stack wire encoders for Bybit V5 and OKX V5 Unified Trading Account (UTA)
//! WebSocket trade frames (`WsLink` `Shared` 16-in-flight) and pooled HTTP/1.1 keep-alive `POST`
//! requests (`HttpsLink` `Exclusive`), covering `Spot`, `LinearPerp`, `InversePerp`,
//! `LinearFutures`, `InverseFutures`, `Options`, and `TimeInForce::Rpi` (`RPI`).

use crate::{
    BybitCategory, BybitPositionIdx, Credentials, OkxInstType, OkxMarginMode, SpotVenue,
    UtaVenueError, authority, client_ok, symbol_ok,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use ring::hmac;
use tickwire_account::OrsSymbolSpec;
use tickwire_venue::{
    Environment, OrderType, PositionSide, Product, SelfTradePrevention, Side, TimeInForce,
};

/// Bounded zero-allocation byte-slice writer over a caller-provided stack buffer.
pub struct BufWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl<'a> BufWriter<'a> {
    #[inline]
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    #[inline]
    pub const fn written(&self) -> usize {
        self.pos
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.buf[..self.pos]
    }

    #[inline]
    pub fn put_byte(&mut self, b: u8) -> Result<(), UtaVenueError> {
        if self.pos >= self.buf.len() {
            return Err(UtaVenueError::Bounds);
        }
        self.buf[self.pos] = b;
        self.pos += 1;
        Ok(())
    }

    #[inline]
    pub fn put_bytes(&mut self, bytes: &[u8]) -> Result<(), UtaVenueError> {
        let end = self
            .pos
            .checked_add(bytes.len())
            .ok_or(UtaVenueError::Bounds)?;
        if end > self.buf.len() {
            return Err(UtaVenueError::Bounds);
        }
        self.buf[self.pos..end].copy_from_slice(bytes);
        self.pos = end;
        Ok(())
    }

    #[inline]
    pub fn put_u64(&mut self, val: u64) -> Result<(), UtaVenueError> {
        let mut tmp = [0u8; 20];
        let n = write_u64_into(&mut tmp, val)?;
        self.put_bytes(&tmp[..n])
    }

    #[inline]
    pub fn put_scaled_atoms(&mut self, atoms: i128, scale: u8) -> Result<(), UtaVenueError> {
        let mut tmp = [0u8; 48];
        let n = write_scaled_atoms_into(&mut tmp, atoms, scale)?;
        self.put_bytes(&tmp[..n])
    }
}

/// Formats an unsigned `u64` as decimal ASCII into `out`, returning the byte count written.
#[inline]
pub fn write_u64_into(out: &mut [u8; 20], mut val: u64) -> Result<usize, UtaVenueError> {
    if val == 0 {
        out[0] = b'0';
        return Ok(1);
    }
    let mut rev = [0u8; 20];
    let mut len = 0usize;
    while val > 0 {
        rev[len] = b'0' + (val % 10) as u8;
        val /= 10;
        len += 1;
    }
    for i in 0..len {
        out[i] = rev[len - 1 - i];
    }
    Ok(len)
}

/// Formats a signed fixed-point integer `atoms` at decimal `scale` (`0..=18`) into plain decimal ASCII
/// (`"50000.5"`, `"0.0025"`, `"10"`), trimming trailing fractional zeros while preserving exact precision.
pub fn write_scaled_atoms_into(
    out: &mut [u8; 48],
    atoms: i128,
    scale: u8,
) -> Result<usize, UtaVenueError> {
    if scale > 18 || atoms == i128::MIN {
        return Err(UtaVenueError::Encoding);
    }
    let negative = atoms < 0;
    let mag: u128 = atoms.unsigned_abs();
    let mut digits = [0u8; 40];
    let mut dlen = 0usize;
    let mut rem = mag;
    if rem == 0 {
        digits[0] = b'0';
        dlen = 1;
    } else {
        while rem > 0 {
            if dlen >= digits.len() {
                return Err(UtaVenueError::Bounds);
            }
            digits[dlen] = b'0' + (rem % 10) as u8;
            rem /= 10;
            dlen += 1;
        }
    }
    let scale_usize = usize::from(scale);
    let mut pos = 0usize;
    if negative && mag > 0 {
        out[pos] = b'-';
        pos += 1;
    }
    if scale_usize == 0 {
        for i in 0..dlen {
            out[pos] = digits[dlen - 1 - i];
            pos += 1;
        }
        return Ok(pos);
    }

    if dlen <= scale_usize {
        out[pos] = b'0';
        pos += 1;
        // Pad leading fractional zeros if any non-zero fractional digits exist.
        let mut first_nonzero = 0usize;
        while first_nonzero < dlen && digits[first_nonzero] == b'0' {
            first_nonzero += 1;
        }
        if first_nonzero == dlen {
            return Ok(pos);
        }
        out[pos] = b'.';
        pos += 1;
        for _ in 0..(scale_usize - dlen) {
            out[pos] = b'0';
            pos += 1;
        }
        for i in (first_nonzero..dlen).rev() {
            out[pos] = digits[i];
            pos += 1;
        }
        return Ok(pos);
    }

    let whole_digits = dlen - scale_usize;
    for i in 0..whole_digits {
        out[pos] = digits[dlen - 1 - i];
        pos += 1;
    }
    let mut first_nonzero = 0usize;
    while first_nonzero < scale_usize && digits[first_nonzero] == b'0' {
        first_nonzero += 1;
    }
    if first_nonzero < scale_usize {
        out[pos] = b'.';
        pos += 1;
        for i in (first_nonzero..scale_usize).rev() {
            out[pos] = digits[i];
            pos += 1;
        }
    }
    Ok(pos)
}

/// Formats a 32-byte digest into 64 lowercase hexadecimal ASCII bytes on the stack.
#[inline]
pub fn write_hex_lower_32(digest: &[u8], out: &mut [u8; 64]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for (idx, &byte) in digest.iter().take(32).enumerate() {
        out[idx * 2] = HEX[usize::from(byte >> 4)];
        out[idx * 2 + 1] = HEX[usize::from(byte & 0x0f)];
    }
}

/// Pure integer civil-from-days UTC timestamp formatter (`YYYY-MM-DDTHH:MM:SS.mmmZ`, 24 bytes)
/// for OKX `OK-ACCESS-TIMESTAMP` headers with zero heap allocation.
pub fn format_okx_iso8601_into(utc_ms: u64, out: &mut [u8; 24]) -> Result<(), UtaVenueError> {
    if utc_ms == 0 {
        return Err(UtaVenueError::Encoding);
    }
    const MS_PER_DAY: u64 = 86_400_000;
    let days = (utc_ms / MS_PER_DAY) as i64;
    let day_ms = utc_ms % MS_PER_DAY;

    // Howard Hinnant's exact integer civil_from_days algorithm (proleptic Gregorian).
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let mut y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    if m <= 2 {
        y += 1;
    }
    if !(1970..=9999).contains(&y) {
        return Err(UtaVenueError::Encoding);
    }

    let year = y as u32;
    let month = m as u8;
    let day = d as u8;
    let hour = (day_ms / 3_600_000) as u8;
    let minute = ((day_ms % 3_600_000) / 60_000) as u8;
    let second = ((day_ms % 60_000) / 1_000) as u8;
    let millis = (day_ms % 1_000) as u16;

    out[0] = b'0' + ((year / 1000) % 10) as u8;
    out[1] = b'0' + ((year / 100) % 10) as u8;
    out[2] = b'0' + ((year / 10) % 10) as u8;
    out[3] = b'0' + (year % 10) as u8;
    out[4] = b'-';
    out[5] = b'0' + (month / 10);
    out[6] = b'0' + (month % 10);
    out[7] = b'-';
    out[8] = b'0' + (day / 10);
    out[9] = b'0' + (day % 10);
    out[10] = b'T';
    out[11] = b'0' + (hour / 10);
    out[12] = b'0' + (hour % 10);
    out[13] = b':';
    out[14] = b'0' + (minute / 10);
    out[15] = b'0' + (minute % 10);
    out[16] = b':';
    out[17] = b'0' + (second / 10);
    out[18] = b'0' + (second % 10);
    out[19] = b'.';
    out[20] = b'0' + (millis / 100) as u8;
    out[21] = b'0' + ((millis / 10) % 10) as u8;
    out[22] = b'0' + (millis % 10) as u8;
    out[23] = b'Z';
    Ok(())
}

/// Venue-specific UTA parameters accompanying an `OrsNewOrderRequest` on Bybit V5 or OKX V5.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UtaOrderParams {
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub post_only: bool,
    pub reduce_only: bool,
    pub position_side: PositionSide,
    pub bybit_position_idx: BybitPositionIdx,
    pub okx_td_mode: OkxMarginMode,
    /// When `true` on OKX with `OrderType::Market` + `TimeInForce::Ioc`, encodes `ordType: "optimal_limit_ioc"`.
    pub okx_optimal_limit_ioc: bool,
    pub smp_mode: SelfTradePrevention,
}

impl Default for UtaOrderParams {
    fn default() -> Self {
        Self {
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            position_side: PositionSide::Both,
            bybit_position_idx: BybitPositionIdx::OneWay,
            okx_td_mode: OkxMarginMode::Cross,
            okx_optimal_limit_ioc: false,
            smp_mode: SelfTradePrevention::Default,
        }
    }
}

impl UtaOrderParams {
    /// Default parameters for cash `Spot` limit orders (`tdMode = "cash"`, one-way).
    #[inline]
    pub const fn cash_spot_limit(tif: TimeInForce) -> Self {
        Self {
            order_type: OrderType::Limit,
            time_in_force: tif,
            post_only: matches!(tif, TimeInForce::PostOnly),
            reduce_only: false,
            position_side: PositionSide::Both,
            bybit_position_idx: BybitPositionIdx::OneWay,
            okx_td_mode: OkxMarginMode::Cash,
            okx_optimal_limit_ioc: false,
            smp_mode: SelfTradePrevention::Default,
        }
    }

    /// Default parameters for cross-margin UTA derivatives/options limit orders.
    #[inline]
    pub const fn cross_derivative_limit(tif: TimeInForce, reduce_only: bool) -> Self {
        Self {
            order_type: OrderType::Limit,
            time_in_force: tif,
            post_only: matches!(tif, TimeInForce::PostOnly),
            reduce_only,
            position_side: PositionSide::Both,
            bybit_position_idx: BybitPositionIdx::OneWay,
            okx_td_mode: OkxMarginMode::Cross,
            okx_optimal_limit_ioc: false,
            smp_mode: SelfTradePrevention::Default,
        }
    }
}

/// Stack input for encoding a UTA `NewOrder` JSON object (`Bybit` `order.create` or `OKX` `order`).
#[derive(Clone, Copy, Debug)]
pub struct UtaNewOrderWireInput<'a> {
    pub symbol: &'a str,
    pub spec: &'a OrsSymbolSpec,
    pub side: Side,
    pub price_ticks: i64,
    pub qty_lots: i64,
    pub client_id: &'a str,
    pub params: UtaOrderParams,
}

/// Stack input for encoding a UTA `Modify` (`amend`) JSON object.
#[derive(Clone, Copy, Debug)]
pub struct UtaModifyWireInput<'a> {
    pub symbol: &'a str,
    pub spec: &'a OrsSymbolSpec,
    pub client_id: Option<&'a str>,
    pub venue_order_id: Option<&'a str>,
    pub req_id: Option<&'a str>,
    pub new_price_ticks: i64,
    pub new_qty_lots: i64,
}

/// Stack input for encoding a UTA `Cancel` JSON object.
#[derive(Clone, Copy, Debug)]
pub struct UtaCancelWireInput<'a> {
    pub symbol: &'a str,
    pub product: Product,
    pub client_id: Option<&'a str>,
    pub venue_order_id: Option<&'a str>,
}

fn bybit_smp_str(smp: SelfTradePrevention) -> Option<&'static [u8]> {
    match smp {
        SelfTradePrevention::Default | SelfTradePrevention::None => None,
        SelfTradePrevention::ExpireMaker => Some(b"CancelMaker"),
        SelfTradePrevention::ExpireTaker => Some(b"CancelTaker"),
        SelfTradePrevention::ExpireBoth | SelfTradePrevention::Decrement => Some(b"CancelBoth"),
    }
}

/// Encodes the inner Bybit V5 `order.create` / `/v5/order/create` JSON object into `w`.
pub fn encode_bybit_new_order_body_into(
    input: &UtaNewOrderWireInput<'_>,
    w: &mut BufWriter<'_>,
) -> Result<(), UtaVenueError> {
    if !symbol_ok(input.symbol)
        || !client_ok(SpotVenue::Bybit, input.client_id)
        || input.qty_lots <= 0
    {
        return Err(UtaVenueError::Encoding);
    }
    let category =
        BybitCategory::from_product(input.spec.product).ok_or(UtaVenueError::Encoding)?;
    if category == BybitCategory::Spot && input.params.reduce_only {
        return Err(UtaVenueError::Encoding);
    }

    let eff_tif = if input.params.post_only && input.params.time_in_force == TimeInForce::Gtc {
        TimeInForce::PostOnly
    } else {
        input.params.time_in_force
    };

    let (order_type_str, tif_str): (&[u8], Option<&[u8]>) = match (input.params.order_type, eff_tif)
    {
        (OrderType::Market, TimeInForce::Ioc | TimeInForce::Gtc) => (b"Market", Some(b"IOC")),
        (OrderType::Market, TimeInForce::Fok) => (b"Market", Some(b"FOK")),
        (OrderType::Market, _) => return Err(UtaVenueError::Encoding),
        (OrderType::Limit, TimeInForce::Gtc) => (b"Limit", Some(b"GTC")),
        (OrderType::Limit, TimeInForce::Ioc) => (b"Limit", Some(b"IOC")),
        (OrderType::Limit, TimeInForce::Fok) => (b"Limit", Some(b"FOK")),
        (OrderType::Limit, TimeInForce::PostOnly) => (b"Limit", Some(b"PostOnly")),
        (OrderType::Limit, TimeInForce::Rpi) => (b"Limit", Some(b"RPI")),
        _ => return Err(UtaVenueError::Encoding),
    };
    if input.params.order_type == OrderType::Limit && input.price_ticks <= 0 {
        return Err(UtaVenueError::Encoding);
    }

    let qty_atoms = i128::from(input.qty_lots)
        .checked_mul(input.spec.qty_step_atoms)
        .ok_or(UtaVenueError::Encoding)?;

    w.put_bytes(b"{\"category\":\"")?;
    w.put_bytes(category.as_str().as_bytes())?;
    w.put_bytes(b"\",\"symbol\":\"")?;
    w.put_bytes(input.symbol.as_bytes())?;
    w.put_bytes(b"\",\"side\":\"")?;
    w.put_bytes(match input.side {
        Side::Buy => b"Buy",
        Side::Sell => b"Sell",
    })?;
    w.put_bytes(b"\",\"orderType\":\"")?;
    w.put_bytes(order_type_str)?;
    w.put_bytes(b"\",\"qty\":\"")?;
    w.put_scaled_atoms(qty_atoms, input.spec.qty_scale)?;
    w.put_byte(b'"')?;

    if input.params.order_type == OrderType::Limit {
        let px_atoms = i128::from(input.price_ticks)
            .checked_mul(input.spec.price_tick_atoms)
            .ok_or(UtaVenueError::Encoding)?;
        w.put_bytes(b",\"price\":\"")?;
        w.put_scaled_atoms(px_atoms, input.spec.price_scale)?;
        w.put_byte(b'"')?;
    } else if category == BybitCategory::Spot {
        w.put_bytes(b",\"marketUnit\":\"baseCoin\"")?;
    }

    if let Some(tif) = tif_str {
        w.put_bytes(b",\"timeInForce\":\"")?;
        w.put_bytes(tif)?;
        w.put_byte(b'"')?;
    }

    w.put_bytes(b",\"orderLinkId\":\"")?;
    w.put_bytes(input.client_id.as_bytes())?;
    w.put_byte(b'"')?;

    if matches!(category, BybitCategory::Linear | BybitCategory::Inverse) {
        let pos_idx = if input.params.bybit_position_idx != BybitPositionIdx::OneWay {
            input.params.bybit_position_idx
        } else {
            match input.params.position_side {
                PositionSide::Both => BybitPositionIdx::OneWay,
                PositionSide::Long => BybitPositionIdx::BuyHedge,
                PositionSide::Short => BybitPositionIdx::SellHedge,
            }
        };
        w.put_bytes(b",\"positionIdx\":")?;
        w.put_u64(pos_idx as u64)?;
    }

    if input.params.reduce_only {
        w.put_bytes(b",\"reduceOnly\":true")?;
    }

    if let Some(smp) = bybit_smp_str(input.params.smp_mode) {
        w.put_bytes(b",\"smpType\":\"")?;
        w.put_bytes(smp)?;
        w.put_byte(b'"')?;
    }

    w.put_byte(b'}')?;
    Ok(())
}

/// Encodes the inner OKX V5 `op: "order"` / `/api/v5/trade/order` JSON object into `w`.
pub fn encode_okx_new_order_body_into(
    input: &UtaNewOrderWireInput<'_>,
    w: &mut BufWriter<'_>,
) -> Result<(), UtaVenueError> {
    if !symbol_ok(input.symbol)
        || !client_ok(SpotVenue::Okx, input.client_id)
        || input.qty_lots <= 0
    {
        return Err(UtaVenueError::Encoding);
    }
    let inst_type = OkxInstType::from_product(input.spec.product).ok_or(UtaVenueError::Encoding)?;

    let td_mode =
        if inst_type != OkxInstType::Spot && input.params.okx_td_mode == OkxMarginMode::Cash {
            // Derivatives and options on OKX UTA always execute in `cross` or `isolated` margin mode.
            OkxMarginMode::Cross
        } else {
            input.params.okx_td_mode
        };
    if td_mode == OkxMarginMode::Cash && input.params.reduce_only {
        return Err(UtaVenueError::Encoding);
    }

    let eff_tif = if input.params.post_only && input.params.time_in_force == TimeInForce::Gtc {
        TimeInForce::PostOnly
    } else {
        input.params.time_in_force
    };

    let ord_type_str: &[u8] = match (input.params.order_type, eff_tif) {
        (OrderType::Market, TimeInForce::Ioc) if input.params.okx_optimal_limit_ioc => {
            b"optimal_limit_ioc"
        }
        (OrderType::Market, TimeInForce::Gtc | TimeInForce::Ioc) => b"market",
        (OrderType::Market, _) => return Err(UtaVenueError::Encoding),
        (OrderType::Limit, TimeInForce::Gtc) => b"limit",
        (OrderType::Limit, TimeInForce::PostOnly) => b"post_only",
        (OrderType::Limit, TimeInForce::Fok) => b"fok",
        (OrderType::Limit, TimeInForce::Ioc) => b"ioc",
        (OrderType::Limit, TimeInForce::Rpi) => b"rpi",
        _ => return Err(UtaVenueError::Encoding),
    };
    if input.params.order_type == OrderType::Limit && input.price_ticks <= 0 {
        return Err(UtaVenueError::Encoding);
    }

    let qty_atoms = i128::from(input.qty_lots)
        .checked_mul(input.spec.qty_step_atoms)
        .ok_or(UtaVenueError::Encoding)?;

    w.put_bytes(b"{\"instId\":\"")?;
    w.put_bytes(input.symbol.as_bytes())?;
    w.put_bytes(b"\",\"tdMode\":\"")?;
    w.put_bytes(td_mode.as_str().as_bytes())?;
    w.put_bytes(b"\",\"side\":\"")?;
    w.put_bytes(match input.side {
        Side::Buy => b"buy",
        Side::Sell => b"sell",
    })?;
    w.put_bytes(b"\",\"ordType\":\"")?;
    w.put_bytes(ord_type_str)?;
    w.put_bytes(b"\",\"sz\":\"")?;
    w.put_scaled_atoms(qty_atoms, input.spec.qty_scale)?;
    w.put_byte(b'"')?;

    if input.params.order_type == OrderType::Limit {
        let px_atoms = i128::from(input.price_ticks)
            .checked_mul(input.spec.price_tick_atoms)
            .ok_or(UtaVenueError::Encoding)?;
        w.put_bytes(b",\"px\":\"")?;
        w.put_scaled_atoms(px_atoms, input.spec.price_scale)?;
        w.put_byte(b'"')?;
    } else if inst_type == OkxInstType::Spot && td_mode == OkxMarginMode::Cash {
        w.put_bytes(b",\"tgtCcy\":\"base_ccy\",\"banAmend\":true")?;
    }

    w.put_bytes(b",\"clOrdId\":\"")?;
    w.put_bytes(input.client_id.as_bytes())?;
    w.put_byte(b'"')?;

    if matches!(inst_type, OkxInstType::Swap | OkxInstType::Futures) {
        let pos_side: &[u8] = match input.params.position_side {
            PositionSide::Both => b"net",
            PositionSide::Long => b"long",
            PositionSide::Short => b"short",
        };
        w.put_bytes(b",\"posSide\":\"")?;
        w.put_bytes(pos_side)?;
        w.put_byte(b'"')?;
    }

    if input.params.reduce_only {
        w.put_bytes(b",\"reduceOnly\":true")?;
    }

    w.put_byte(b'}')?;
    Ok(())
}

/// Encodes the inner Bybit V5 `order.amend` / `/v5/order/amend` JSON object into `w`.
pub fn encode_bybit_modify_body_into(
    input: &UtaModifyWireInput<'_>,
    w: &mut BufWriter<'_>,
) -> Result<(), UtaVenueError> {
    if !symbol_ok(input.symbol) || input.new_price_ticks <= 0 || input.new_qty_lots <= 0 {
        return Err(UtaVenueError::Encoding);
    }
    if input.client_id.is_none() && input.venue_order_id.is_none() {
        return Err(UtaVenueError::Encoding);
    }
    let category =
        BybitCategory::from_product(input.spec.product).ok_or(UtaVenueError::Encoding)?;
    let qty_atoms = i128::from(input.new_qty_lots)
        .checked_mul(input.spec.qty_step_atoms)
        .ok_or(UtaVenueError::Encoding)?;
    let px_atoms = i128::from(input.new_price_ticks)
        .checked_mul(input.spec.price_tick_atoms)
        .ok_or(UtaVenueError::Encoding)?;

    w.put_bytes(b"{\"category\":\"")?;
    w.put_bytes(category.as_str().as_bytes())?;
    w.put_bytes(b"\",\"symbol\":\"")?;
    w.put_bytes(input.symbol.as_bytes())?;
    w.put_byte(b'"')?;
    if let Some(oid) = input.venue_order_id
        && !oid.is_empty()
    {
        if !symbol_ok(oid) {
            return Err(UtaVenueError::Encoding);
        }
        w.put_bytes(b",\"orderId\":\"")?;
        w.put_bytes(oid.as_bytes())?;
        w.put_byte(b'"')?;
    }
    if let Some(cid) = input.client_id
        && !cid.is_empty()
    {
        if !client_ok(SpotVenue::Bybit, cid) {
            return Err(UtaVenueError::Encoding);
        }
        w.put_bytes(b",\"orderLinkId\":\"")?;
        w.put_bytes(cid.as_bytes())?;
        w.put_byte(b'"')?;
    }
    w.put_bytes(b",\"qty\":\"")?;
    w.put_scaled_atoms(qty_atoms, input.spec.qty_scale)?;
    w.put_bytes(b"\",\"price\":\"")?;
    w.put_scaled_atoms(px_atoms, input.spec.price_scale)?;
    w.put_bytes(b"\"}")?;
    Ok(())
}

/// Encodes the inner OKX V5 `op: "amend-order"` / `/api/v5/trade/amend-order` JSON object into `w`.
pub fn encode_okx_modify_body_into(
    input: &UtaModifyWireInput<'_>,
    w: &mut BufWriter<'_>,
) -> Result<(), UtaVenueError> {
    if !symbol_ok(input.symbol) || input.new_price_ticks <= 0 || input.new_qty_lots <= 0 {
        return Err(UtaVenueError::Encoding);
    }
    if input.client_id.is_none() && input.venue_order_id.is_none() {
        return Err(UtaVenueError::Encoding);
    }
    let _ = OkxInstType::from_product(input.spec.product).ok_or(UtaVenueError::Encoding)?;
    let qty_atoms = i128::from(input.new_qty_lots)
        .checked_mul(input.spec.qty_step_atoms)
        .ok_or(UtaVenueError::Encoding)?;
    let px_atoms = i128::from(input.new_price_ticks)
        .checked_mul(input.spec.price_tick_atoms)
        .ok_or(UtaVenueError::Encoding)?;

    w.put_bytes(b"{\"instId\":\"")?;
    w.put_bytes(input.symbol.as_bytes())?;
    w.put_byte(b'"')?;
    if let Some(oid) = input.venue_order_id
        && !oid.is_empty()
    {
        if !symbol_ok(oid) {
            return Err(UtaVenueError::Encoding);
        }
        w.put_bytes(b",\"ordId\":\"")?;
        w.put_bytes(oid.as_bytes())?;
        w.put_byte(b'"')?;
    }
    if let Some(cid) = input.client_id
        && !cid.is_empty()
    {
        if !client_ok(SpotVenue::Okx, cid) {
            return Err(UtaVenueError::Encoding);
        }
        w.put_bytes(b",\"clOrdId\":\"")?;
        w.put_bytes(cid.as_bytes())?;
        w.put_byte(b'"')?;
    }
    if let Some(rid) = input.req_id
        && !rid.is_empty()
    {
        if !client_ok(SpotVenue::Okx, rid) {
            return Err(UtaVenueError::Encoding);
        }
        w.put_bytes(b",\"reqId\":\"")?;
        w.put_bytes(rid.as_bytes())?;
        w.put_byte(b'"')?;
    }
    w.put_bytes(b",\"newSz\":\"")?;
    w.put_scaled_atoms(qty_atoms, input.spec.qty_scale)?;
    w.put_bytes(b"\",\"newPx\":\"")?;
    w.put_scaled_atoms(px_atoms, input.spec.price_scale)?;
    w.put_bytes(b"\"}")?;
    Ok(())
}

/// Encodes the inner Bybit V5 `order.cancel` / `/v5/order/cancel` JSON object into `w`.
pub fn encode_bybit_cancel_body_into(
    input: &UtaCancelWireInput<'_>,
    w: &mut BufWriter<'_>,
) -> Result<(), UtaVenueError> {
    if !symbol_ok(input.symbol) {
        return Err(UtaVenueError::Encoding);
    }
    if input.client_id.is_none_or(str::is_empty) && input.venue_order_id.is_none_or(str::is_empty) {
        return Err(UtaVenueError::Encoding);
    }
    let category = BybitCategory::from_product(input.product).ok_or(UtaVenueError::Encoding)?;
    w.put_bytes(b"{\"category\":\"")?;
    w.put_bytes(category.as_str().as_bytes())?;
    w.put_bytes(b"\",\"symbol\":\"")?;
    w.put_bytes(input.symbol.as_bytes())?;
    w.put_byte(b'"')?;
    if let Some(oid) = input.venue_order_id
        && !oid.is_empty()
    {
        if !symbol_ok(oid) {
            return Err(UtaVenueError::Encoding);
        }
        w.put_bytes(b",\"orderId\":\"")?;
        w.put_bytes(oid.as_bytes())?;
        w.put_byte(b'"')?;
    }
    if let Some(cid) = input.client_id
        && !cid.is_empty()
    {
        if !client_ok(SpotVenue::Bybit, cid) {
            return Err(UtaVenueError::Encoding);
        }
        w.put_bytes(b",\"orderLinkId\":\"")?;
        w.put_bytes(cid.as_bytes())?;
        w.put_byte(b'"')?;
    }
    w.put_byte(b'}')?;
    Ok(())
}

/// Encodes the inner OKX V5 `op: "cancel-order"` / `/api/v5/trade/cancel-order` JSON object into `w`.
pub fn encode_okx_cancel_body_into(
    input: &UtaCancelWireInput<'_>,
    w: &mut BufWriter<'_>,
) -> Result<(), UtaVenueError> {
    if !symbol_ok(input.symbol) {
        return Err(UtaVenueError::Encoding);
    }
    if input.client_id.is_none_or(str::is_empty) && input.venue_order_id.is_none_or(str::is_empty) {
        return Err(UtaVenueError::Encoding);
    }
    let _ = OkxInstType::from_product(input.product).ok_or(UtaVenueError::Encoding)?;
    w.put_bytes(b"{\"instId\":\"")?;
    w.put_bytes(input.symbol.as_bytes())?;
    w.put_byte(b'"')?;
    if let Some(oid) = input.venue_order_id
        && !oid.is_empty()
    {
        if !symbol_ok(oid) {
            return Err(UtaVenueError::Encoding);
        }
        w.put_bytes(b",\"ordId\":\"")?;
        w.put_bytes(oid.as_bytes())?;
        w.put_byte(b'"')?;
    }
    if let Some(cid) = input.client_id
        && !cid.is_empty()
    {
        if !client_ok(SpotVenue::Okx, cid) {
            return Err(UtaVenueError::Encoding);
        }
        w.put_bytes(b",\"clOrdId\":\"")?;
        w.put_bytes(cid.as_bytes())?;
        w.put_byte(b'"')?;
    }
    w.put_byte(b'}')?;
    Ok(())
}

/// Wraps a Bybit V5 trade JSON argument object inside a `/v5/trade` WebSocket envelope
/// (`op: "order.create" | "order.amend" | "order.cancel"`, numeric `reqId == reply_id`).
pub fn encode_bybit_ws_trade_envelope_into(
    reply_id: u64,
    utc_ms: u64,
    op: &str,
    arg_json: &[u8],
    out: &mut [u8],
) -> Result<usize, UtaVenueError> {
    if reply_id == 0 || utc_ms == 0 {
        return Err(UtaVenueError::Encoding);
    }
    let mut w = BufWriter::new(out);
    w.put_bytes(b"{\"reqId\":\"")?;
    w.put_u64(reply_id)?;
    w.put_bytes(b"\",\"header\":{\"X-BAPI-TIMESTAMP\":\"")?;
    w.put_u64(utc_ms)?;
    w.put_bytes(b"\",\"X-BAPI-RECV-WINDOW\":\"5000\"},\"op\":\"")?;
    w.put_bytes(op.as_bytes())?;
    w.put_bytes(b"\",\"args\":[")?;
    w.put_bytes(arg_json)?;
    w.put_bytes(b"]}")?;
    Ok(w.written())
}

/// Wraps an OKX V5 trade JSON argument object inside a `/ws/v5/private` WebSocket envelope
/// (`op: "order" | "amend-order" | "cancel-order"`, decimal `id == reply_id`).
pub fn encode_okx_ws_trade_envelope_into(
    reply_id: u64,
    op: &str,
    arg_json: &[u8],
    out: &mut [u8],
) -> Result<usize, UtaVenueError> {
    if reply_id == 0 {
        return Err(UtaVenueError::Encoding);
    }
    let mut w = BufWriter::new(out);
    w.put_bytes(b"{\"id\":\"")?;
    w.put_u64(reply_id)?;
    w.put_bytes(b"\",\"op\":\"")?;
    w.put_bytes(op.as_bytes())?;
    w.put_bytes(b"\",\"args\":[")?;
    w.put_bytes(arg_json)?;
    w.put_bytes(b"]}")?;
    Ok(w.written())
}

/// Signs and encodes a complete HTTP/1.1 keep-alive `POST` request for Bybit V5 or OKX V5 into `out`
/// with zero heap allocation.
pub fn encode_signed_https_post_into(
    credentials: &Credentials<'_>,
    environment: Environment,
    utc_ms: u64,
    path: &str,
    body: &[u8],
    out: &mut [u8],
) -> Result<usize, UtaVenueError> {
    if !credentials.valid() {
        return Err(UtaVenueError::Identity);
    }
    if utc_ms == 0 || !path.starts_with('/') {
        return Err(UtaVenueError::Encoding);
    }
    let host = authority(credentials.venue(), environment);
    match credentials.venue() {
        SpotVenue::Bybit => {
            let mut ts_buf = [0u8; 20];
            let ts_len = write_u64_into(&mut ts_buf, utc_ms)?;
            let ts_str =
                core::str::from_utf8(&ts_buf[..ts_len]).map_err(|_| UtaVenueError::Encoding)?;

            let mut mac = hmac::Context::with_key(credentials.hmac_key());
            mac.update(&ts_buf[..ts_len]);
            mac.update(credentials.key().as_bytes());
            mac.update(b"5000");
            mac.update(body);
            let tag = mac.sign();
            let mut sig_hex = [0u8; 64];
            write_hex_lower_32(tag.as_ref(), &mut sig_hex);
            let sig_str = core::str::from_utf8(&sig_hex).map_err(|_| UtaVenueError::Encoding)?;

            let headers = [
                ("Host", host),
                ("Content-Type", "application/json"),
                ("X-BAPI-API-KEY", credentials.key()),
                ("X-BAPI-TIMESTAMP", ts_str),
                ("X-BAPI-RECV-WINDOW", "5000"),
                ("X-BAPI-SIGN", sig_str),
            ];
            tickwire_transport::warmup::encode_http_request_into(out, "POST", path, &headers, body)
                .map_err(UtaVenueError::Transport)
        }
        SpotVenue::Okx => {
            let passphrase = credentials.passphrase().ok_or(UtaVenueError::Identity)?;
            let mut iso_buf = [0u8; 24];
            format_okx_iso8601_into(utc_ms, &mut iso_buf)?;
            let iso_str = core::str::from_utf8(&iso_buf).map_err(|_| UtaVenueError::Encoding)?;

            let mut mac = hmac::Context::with_key(credentials.hmac_key());
            mac.update(&iso_buf);
            mac.update(b"POST");
            mac.update(path.as_bytes());
            mac.update(body);
            let tag = mac.sign();
            let mut sig_b64 = [0u8; 44];
            let sig_len = STANDARD
                .encode_slice(tag.as_ref(), &mut sig_b64)
                .map_err(|_| UtaVenueError::Encoding)?;
            let sig_str =
                core::str::from_utf8(&sig_b64[..sig_len]).map_err(|_| UtaVenueError::Encoding)?;

            if environment == Environment::Test {
                let headers = [
                    ("Host", host),
                    ("Content-Type", "application/json"),
                    ("OK-ACCESS-KEY", credentials.key()),
                    ("OK-ACCESS-SIGN", sig_str),
                    ("OK-ACCESS-TIMESTAMP", iso_str),
                    ("OK-ACCESS-PASSPHRASE", passphrase),
                    ("x-simulated-trading", "1"),
                ];
                tickwire_transport::warmup::encode_http_request_into(
                    out, "POST", path, &headers, body,
                )
                .map_err(UtaVenueError::Transport)
            } else {
                let headers = [
                    ("Host", host),
                    ("Content-Type", "application/json"),
                    ("OK-ACCESS-KEY", credentials.key()),
                    ("OK-ACCESS-SIGN", sig_str),
                    ("OK-ACCESS-TIMESTAMP", iso_str),
                    ("OK-ACCESS-PASSPHRASE", passphrase),
                ];
                tickwire_transport::warmup::encode_http_request_into(
                    out, "POST", path, &headers, body,
                )
                .map_err(UtaVenueError::Transport)
            }
        }
    }
}

pub use encode_bybit_cancel_body_into as encode_bybit_cancel_arg;
pub use encode_bybit_modify_body_into as encode_bybit_modify_arg;
pub use encode_bybit_new_order_body_into as encode_bybit_new_order_arg;
pub use encode_okx_cancel_body_into as encode_okx_cancel_arg;
pub use encode_okx_modify_body_into as encode_okx_modify_arg;
pub use encode_okx_new_order_body_into as encode_okx_new_order_arg;
