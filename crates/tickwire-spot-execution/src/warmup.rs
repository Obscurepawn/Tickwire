//! Side-effect-free hot-path cache and loopback TLS warmer for Bybit V5 and OKX V5 spot sessions.
#![forbid(unsafe_code)]

use crate::{SpotVenue, authority};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use ring::hmac;
use tickwire_account::{Account, Fixed};
use tickwire_transport::{
    TlsLoopbackWarmer,
    warmup::{
        WARMUP_SCRATCH_BYTES, decode_http_response_slice, encode_http_request_into,
        find_json_field, write_u64_ascii,
    },
};
use tickwire_venue::{Environment, Side};

const SYNTHETIC_API_KEY: &str = "tw-warmup-synthetic-key";
const SYNTHETIC_PASSPHRASE: &str = "tw-warmup-synthetic-pass";
const SYNTHETIC_HMAC_SECRET: &[u8] = b"tickwire-spot-hotpath-warmup-synthetic-secret-v1";

const SAMPLE_BYBIT_BBO: &[u8] = br#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000123,"data":{"s":"BTCUSDT","b":[["50000.00","0.2500"]],"a":[["50000.50","0.4000"]],"u":1042,"seq":9001}}"#;
const SAMPLE_OKX_BBO: &[u8] = br#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"data":[{"asks":[["50000.50","0.4000","0","2"]],"bids":[["50000.00","0.2500","0","3"]],"ts":"1700000000123","seqId":9001}]}"#;

const BYBIT_DUMMY_HTTP_ACK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 81\r\n\r\n{\"retCode\":0,\"retMsg\":\"OK\",\"result\":{\"orderId\":\"10000001\",\"orderLinkId\":\"warm1\"}}";
const OKX_DUMMY_HTTP_ACK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 81\r\n\r\n{\"code\":\"0\",\"msg\":\"\",\"data\":[{\"ordId\":\"10000001\",\"clOrdId\":\"warm1\",\"sCode\":\"0\"}]}";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodedSampleBbo {
    pub bid_ticks: i64,
    pub bid_lots: i64,
    pub ask_ticks: i64,
    pub ask_lots: i64,
    pub sequence: u64,
}

/// Parses an unescaped decimal ASCII string (`"50000.50"`) into integer units scaled by `frac_digits`.
#[inline]
pub fn parse_decimal_scaled(raw: &[u8], frac_digits: u8) -> Option<i64> {
    if raw.is_empty() || raw.len() > 24 {
        return None;
    }
    let mut whole: i64 = 0;
    let mut frac: i64 = 0;
    let mut seen_dot = false;
    let mut seen_digit = false;
    let mut frac_seen: u8 = 0;

    for &b in raw {
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
        let digit = i64::from(b - b'0');
        if !seen_dot {
            whole = whole.checked_mul(10)?.checked_add(digit)?;
        } else if frac_seen < frac_digits {
            frac = frac.checked_mul(10)?.checked_add(digit)?;
            frac_seen += 1;
        } else if digit != 0 {
            return None;
        }
    }
    if !seen_digit {
        return None;
    }
    while frac_seen < frac_digits {
        frac = frac.checked_mul(10)?;
        frac_seen += 1;
    }
    let mut scale_mul: i64 = 1;
    let mut rem = frac_digits;
    while rem > 0 {
        scale_mul = scale_mul.checked_mul(10)?;
        rem -= 1;
    }
    whole.checked_mul(scale_mul)?.checked_add(frac)
}

#[inline]
fn parse_u64_ascii(raw: &[u8]) -> Option<u64> {
    if raw.is_empty() || raw.len() > 20 {
        return None;
    }
    let mut out = 0u64;
    for &b in raw {
        if !b.is_ascii_digit() {
            return None;
        }
        out = out.checked_mul(10)?.checked_add(u64::from(b - b'0'))?;
    }
    Some(out)
}

#[inline]
fn extract_first_pair_after<'a>(json: &'a [u8], marker: &[u8]) -> Option<(&'a [u8], &'a [u8])> {
    let mut pos = 0usize;
    while pos + marker.len() < json.len() {
        if &json[pos..pos + marker.len()] == marker {
            let mut i = pos + marker.len();
            while i < json.len() && json[i] != b'"' {
                i += 1;
            }
            i = i.checked_add(1)?;
            let px_start = i;
            while i < json.len() && json[i] != b'"' {
                i += 1;
            }
            if i >= json.len() {
                return None;
            }
            let px = &json[px_start..i];
            i += 1;
            while i < json.len() && json[i] != b'"' {
                i += 1;
            }
            i = i.checked_add(1)?;
            let qty_start = i;
            while i < json.len() && json[i] != b'"' {
                i += 1;
            }
            if i >= json.len() {
                return None;
            }
            let qty = &json[qty_start..i];
            return Some((px, qty));
        }
        pos += 1;
    }
    None
}

/// Allocation-free stack parser for representative Bybit V5 or OKX V5 top-of-book JSON frames.
pub fn decode_sample_spot_bbo(venue: SpotVenue, frame: &[u8]) -> Option<DecodedSampleBbo> {
    let (bid_marker, ask_marker, seq_key): (&[u8], &[u8], &[u8]) = match venue {
        SpotVenue::Bybit => (b"\"b\":[[", b"\"a\":[[", b"\"seq\""),
        SpotVenue::Okx => (b"\"bids\":[[", b"\"asks\":[[", b"\"seqId\""),
    };
    let (bid_px_raw, bid_qty_raw) = extract_first_pair_after(frame, bid_marker)?;
    let (ask_px_raw, ask_qty_raw) = extract_first_pair_after(frame, ask_marker)?;
    let seq_raw = find_json_field(frame, seq_key)?;

    let bid_ticks = parse_decimal_scaled(bid_px_raw, 2)?;
    let ask_ticks = parse_decimal_scaled(ask_px_raw, 2)?;
    let bid_lots = parse_decimal_scaled(bid_qty_raw, 4)?;
    let ask_lots = parse_decimal_scaled(ask_qty_raw, 4)?;
    let sequence = parse_u64_ascii(seq_raw)?;

    if bid_ticks <= 0 || ask_ticks <= bid_ticks || bid_lots <= 0 || ask_lots <= 0 {
        return None;
    }
    Some(DecodedSampleBbo {
        bid_ticks,
        bid_lots,
        ask_ticks,
        ask_lots,
        sequence,
    })
}

#[inline]
fn write_hex_lower_32(digest: &[u8], out: &mut [u8; 64]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for (idx, &byte) in digest.iter().take(32).enumerate() {
        out[idx * 2] = HEX[usize::from(byte >> 4)];
        out[idx * 2 + 1] = HEX[usize::from(byte & 0x0f)];
    }
}

#[inline]
fn format_okx_iso_timestamp(now_ns: u64, out: &mut [u8; 24]) {
    // Fixed valid ISO-8601 prefix `2026-11-14T15:30:SS.MMMZ` with deterministic SS.MMM digits.
    let template = b"2026-11-14T15:30:00.000Z";
    out.copy_from_slice(template);
    let total_ms = (now_ns / 1_000_000) % 60_000;
    let secs = (total_ms / 1_000) as u8;
    let millis = (total_ms % 1_000) as u16;
    out[17] = b'0' + (secs / 10);
    out[18] = b'0' + (secs % 10);
    out[20] = b'0' + ((millis / 100) as u8);
    out[21] = b'0' + (((millis / 10) % 10) as u8);
    out[22] = b'0' + ((millis % 10) as u8);
}

#[inline]
fn push_slice(
    dst: &mut [u8],
    cursor: &mut usize,
    bytes: &[u8],
) -> Result<(), tickwire_transport::Error> {
    let end = cursor
        .checked_add(bytes.len())
        .ok_or(tickwire_transport::Error::Bounds)?;
    if end > dst.len() {
        return Err(tickwire_transport::Error::Bounds);
    }
    dst[*cursor..end].copy_from_slice(bytes);
    *cursor = end;
    Ok(())
}

/// Preallocated hot-path dry-run engine owned by `RestSession`.
pub struct SpotHotPathWarmer {
    venue: SpotVenue,
    environment: Environment,
    host: &'static str,
    instrument_id: u64,
    symbol_buf: [u8; 32],
    symbol_len: usize,
    sample_price: Fixed,
    sample_notional_atoms: i128,
    sample_fee_atoms: i128,
    money_scale: u8,
    synthetic_key: hmac::Key,
    scratch_account: Account,
    tls_warmer: TlsLoopbackWarmer,
    req_scratch: [u8; WARMUP_SCRATCH_BYTES],
    ack_scratch: [u8; 512],
}

impl SpotHotPathWarmer {
    pub fn new(
        venue: SpotVenue,
        environment: Environment,
        instrument_id: u64,
        symbol: &str,
        sample_price: Fixed,
        money_scale: u8,
        scratch_account: Account,
    ) -> Result<Self, tickwire_transport::Error> {
        let sym_bytes = symbol.as_bytes();
        if sym_bytes.is_empty() || sym_bytes.len() > 32 {
            return Err(tickwire_transport::Error::Bounds);
        }
        let mut symbol_buf = [0u8; 32];
        symbol_buf[..sym_bytes.len()].copy_from_slice(sym_bytes);

        let ack_payload = match venue {
            SpotVenue::Bybit => BYBIT_DUMMY_HTTP_ACK,
            SpotVenue::Okx => OKX_DUMMY_HTTP_ACK,
        };
        let tls_warmer = TlsLoopbackWarmer::new()?.with_ack_payload(ack_payload);

        let scale_clamped = money_scale.min(18);
        let mut unit_atoms: i128 = 1;
        let mut rem = scale_clamped;
        while rem > 0 {
            unit_atoms = unit_atoms.saturating_mul(10);
            rem -= 1;
        }
        let sample_notional_atoms = unit_atoms.saturating_mul(50);
        let sample_fee_atoms = (unit_atoms / 10).max(1);

        Ok(Self {
            venue,
            environment,
            host: authority(venue, environment),
            instrument_id,
            symbol_buf,
            symbol_len: sym_bytes.len(),
            sample_price,
            sample_notional_atoms,
            sample_fee_atoms,
            money_scale: scale_clamped,
            synthetic_key: hmac::Key::new(hmac::HMAC_SHA256, SYNTHETIC_HMAC_SECRET),
            scratch_account,
            tls_warmer,
            req_scratch: [0u8; WARMUP_SCRATCH_BYTES],
            ack_scratch: [0u8; 512],
        })
    }

    /// Refreshes the isolated scratch `Account` snapshot outside the active order path.
    pub fn refresh_scratch_account(&mut self, account: &Account) {
        self.scratch_account = account.clone();
    }

    #[inline]
    pub fn scratch_addr(&self) -> usize {
        self.req_scratch.as_ptr() as usize
    }

    /// Runs all 4 hot-path stages without mutating any live `Account`, `AccountJournal`, `OrderRouter`,
    /// `VenueRateLimiter`, or live `rustls::ClientConnection`.
    pub fn warm_once(
        &mut self,
        live_account: &Account,
        now_mono_ns: u64,
    ) -> Result<usize, tickwire_transport::Error> {
        // 1. Stack-decode representative venue BBO frame.
        let sample_frame = match self.venue {
            SpotVenue::Bybit => SAMPLE_BYBIT_BBO,
            SpotVenue::Okx => SAMPLE_OKX_BBO,
        };
        let bbo = decode_sample_spot_bbo(self.venue, sample_frame).ok_or(
            tickwire_transport::Error::Protocol("warmup sample bbo decode"),
        )?;

        // 2. Read-only risk, lease, STP self-cross, and exact Fixed arithmetic dry-run.
        let _live_gate = !live_account.frozen()
            && live_account.account_epoch() > 0
            && live_account.valid_until() > 0
            && !self.scratch_account.frozen();
        if self.instrument_id != 0 {
            let _ = live_account.stp_book().self_cross(
                self.instrument_id,
                Side::Buy,
                self.sample_price,
            );
            let _ = self.scratch_account.stp_book().self_cross(
                self.instrument_id,
                Side::Buy,
                self.sample_price,
            );
        }
        let notional = Fixed::from_atoms(self.sample_notional_atoms, self.money_scale)
            .map_err(|_| tickwire_transport::Error::Protocol("warmup fixed notional"))?;
        let fee = Fixed::from_atoms(self.sample_fee_atoms, self.money_scale)
            .map_err(|_| tickwire_transport::Error::Protocol("warmup fixed fee"))?;
        let total_reserve = notional
            .checked_add(fee)
            .and_then(|v| v.checked_mul_int(1))
            .map_err(|_| tickwire_transport::Error::Protocol("warmup fixed reserve"))?;
        let _ = total_reserve.positive();

        // 3. Synthetic HMAC-SHA256 signing + stack HTTP/1.1 POST request framing.
        let sym = &self.symbol_buf[..self.symbol_len];
        let mut body_buf = [0u8; 256];
        let mut body_len = 0usize;
        let seq_tag = bbo.sequence.wrapping_add(now_mono_ns / 1_000_000) % 1_000_000;
        let mut seq_digits = [0u8; 20];
        let seq_digits_len = write_u64_ascii(&mut seq_digits, seq_tag)?;

        let req_len = match self.venue {
            SpotVenue::Bybit => {
                push_slice(
                    &mut body_buf,
                    &mut body_len,
                    b"{\"category\":\"spot\",\"symbol\":\"",
                )?;
                push_slice(&mut body_buf, &mut body_len, sym)?;
                push_slice(
                    &mut body_buf,
                    &mut body_len,
                    b"\",\"side\":\"Buy\",\"orderType\":\"Limit\",\"qty\":\"0.001\",\"price\":\"50000.00\",\"timeInForce\":\"GTC\",\"orderLinkId\":\"tw1warm",
                )?;
                push_slice(&mut body_buf, &mut body_len, &seq_digits[..seq_digits_len])?;
                push_slice(&mut body_buf, &mut body_len, b"\"}")?;

                let ts_ms = 1_700_000_000_000u64.wrapping_add(now_mono_ns / 1_000_000);
                let mut ts_buf = [0u8; 20];
                let ts_len = write_u64_ascii(&mut ts_buf, ts_ms)?;
                let ts_str = std::str::from_utf8(&ts_buf[..ts_len])
                    .map_err(|_| tickwire_transport::Error::Protocol("warmup ts utf8"))?;

                let mut ctx = hmac::Context::with_key(&self.synthetic_key);
                ctx.update(&ts_buf[..ts_len]);
                ctx.update(SYNTHETIC_API_KEY.as_bytes());
                ctx.update(b"5000");
                ctx.update(&body_buf[..body_len]);
                let tag = ctx.sign();
                let mut sig_hex = [0u8; 64];
                write_hex_lower_32(tag.as_ref(), &mut sig_hex);
                let sig_str = std::str::from_utf8(&sig_hex)
                    .map_err(|_| tickwire_transport::Error::Protocol("warmup sig utf8"))?;

                let headers = [
                    ("Host", self.host),
                    ("Content-Type", "application/json"),
                    ("X-BAPI-API-KEY", SYNTHETIC_API_KEY),
                    ("X-BAPI-TIMESTAMP", ts_str),
                    ("X-BAPI-RECV-WINDOW", "5000"),
                    ("X-BAPI-SIGN", sig_str),
                ];
                encode_http_request_into(
                    &mut self.req_scratch,
                    "POST",
                    "/v5/order/create",
                    &headers,
                    &body_buf[..body_len],
                )?
            }
            SpotVenue::Okx => {
                push_slice(&mut body_buf, &mut body_len, b"{\"instId\":\"")?;
                push_slice(&mut body_buf, &mut body_len, sym)?;
                push_slice(
                    &mut body_buf,
                    &mut body_len,
                    b"\",\"tdMode\":\"cash\",\"side\":\"buy\",\"ordType\":\"limit\",\"sz\":\"0.001\",\"px\":\"50000.00\",\"clOrdId\":\"tw1warm",
                )?;
                push_slice(&mut body_buf, &mut body_len, &seq_digits[..seq_digits_len])?;
                push_slice(&mut body_buf, &mut body_len, b"\"}")?;

                let mut iso_buf = [0u8; 24];
                format_okx_iso_timestamp(now_mono_ns, &mut iso_buf);
                let iso_str = std::str::from_utf8(&iso_buf)
                    .map_err(|_| tickwire_transport::Error::Protocol("warmup iso utf8"))?;

                let mut ctx = hmac::Context::with_key(&self.synthetic_key);
                ctx.update(&iso_buf);
                ctx.update(b"POST/api/v5/trade/order");
                ctx.update(&body_buf[..body_len]);
                let tag = ctx.sign();
                let mut sig_b64 = [0u8; 44];
                let written = STANDARD
                    .encode_slice(tag.as_ref(), &mut sig_b64)
                    .map_err(|_| tickwire_transport::Error::Protocol("warmup b64 encode"))?;
                let sig_str = std::str::from_utf8(&sig_b64[..written])
                    .map_err(|_| tickwire_transport::Error::Protocol("warmup b64 utf8"))?;

                if self.environment == Environment::Test {
                    let headers = [
                        ("Host", self.host),
                        ("Content-Type", "application/json"),
                        ("OK-ACCESS-KEY", SYNTHETIC_API_KEY),
                        ("OK-ACCESS-SIGN", sig_str),
                        ("OK-ACCESS-TIMESTAMP", iso_str),
                        ("OK-ACCESS-PASSPHRASE", SYNTHETIC_PASSPHRASE),
                        ("x-simulated-trading", "1"),
                    ];
                    encode_http_request_into(
                        &mut self.req_scratch,
                        "POST",
                        "/api/v5/trade/order",
                        &headers,
                        &body_buf[..body_len],
                    )?
                } else {
                    let headers = [
                        ("Host", self.host),
                        ("Content-Type", "application/json"),
                        ("OK-ACCESS-KEY", SYNTHETIC_API_KEY),
                        ("OK-ACCESS-SIGN", sig_str),
                        ("OK-ACCESS-TIMESTAMP", iso_str),
                        ("OK-ACCESS-PASSPHRASE", SYNTHETIC_PASSPHRASE),
                    ];
                    encode_http_request_into(
                        &mut self.req_scratch,
                        "POST",
                        "/api/v5/trade/order",
                        &headers,
                        &body_buf[..body_len],
                    )?
                }
            }
        };

        // 4. Isolated in-memory TLS 1.3 encrypt + decrypt roundtrip + 200 OK ack parse.
        let ack_template = match self.venue {
            SpotVenue::Bybit => BYBIT_DUMMY_HTTP_ACK,
            SpotVenue::Okx => OKX_DUMMY_HTTP_ACK,
        };
        let (req_dec, ack_dec) = self.tls_warmer.warm_step_exchange(
            &self.req_scratch[..req_len],
            ack_template,
            &mut self.ack_scratch,
        )?;
        let (status, ack_body) = decode_http_response_slice(&self.ack_scratch[..ack_dec])?;
        if status != 200 {
            return Err(tickwire_transport::Error::Protocol("warmup ack status"));
        }
        match self.venue {
            SpotVenue::Bybit => {
                if find_json_field(ack_body, b"\"retCode\"") != Some(b"0")
                    || find_json_field(ack_body, b"\"orderId\"").is_none()
                {
                    return Err(tickwire_transport::Error::Protocol("warmup bybit ack body"));
                }
            }
            SpotVenue::Okx => {
                if find_json_field(ack_body, b"\"code\"") != Some(b"0")
                    || find_json_field(ack_body, b"\"ordId\"").is_none()
                {
                    return Err(tickwire_transport::Error::Protocol("warmup okx ack body"));
                }
            }
        }
        Ok(req_dec.saturating_add(ack_dec))
    }
}
