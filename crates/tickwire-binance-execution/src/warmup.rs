//! Side-effect-free hot-path cache and loopback TLS warmer for Binance Spot, USD-M, Coin-M, and
//! European Options REST/WebSocket execution (`#![forbid(unsafe_code)]`).
#![forbid(unsafe_code)]

use sha2::{Digest, Sha256};
use tickwire_account::{Account, Fixed, OrderManager};
use tickwire_binance_account::BinanceMsgParser;
use tickwire_transport::{
    TlsLoopbackWarmer,
    warmup::{
        WARMUP_SCRATCH_BYTES, decode_http_response_slice, decode_ws_server_frame_slice,
        encode_http_request_into, encode_ws_client_frame_into, encode_ws_server_frame_into,
        find_json_field, write_u64_ascii,
    },
    ws::Opcode,
};
use tickwire_venue::{Environment, Product, Side};

const SYNTHETIC_BINANCE_API_KEY: &str = "tw-binance-warmup-synthetic-key";
const SYNTHETIC_BINANCE_SECRET: &[u8] = b"tickwire-binance-hotpath-warmup-synthetic-secret-v1";

const SAMPLE_BINANCE_BOOK_TICKER: &[u8] =
    br#"{"u":400900217,"s":"BTCUSDT","b":"50000.00","B":"0.2500","a":"50000.50","A":"0.4000"}"#;

const BINANCE_DUMMY_HTTP_ACK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 74\r\n\r\n{\"symbol\":\"BTCUSDT\",\"orderId\":28,\"clientOrderId\":\"tw1warm\",\"status\":\"NEW\"}";
const BINANCE_DUMMY_WS_ACK_JSON: &[u8] =
    br#"{"id":"tw1warm","status":200,"result":{"symbol":"BTCUSDT","orderId":28,"status":"NEW"}}"#;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodedBinanceSampleBbo {
    pub bid_ticks: i64,
    pub bid_lots: i64,
    pub ask_ticks: i64,
    pub ask_lots: i64,
    pub update_id: u64,
}

#[inline]
fn parse_decimal_scaled(raw: &[u8], frac_digits: u8) -> Option<i64> {
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
    let mut mul: i64 = 1;
    let mut rem = frac_digits;
    while rem > 0 {
        mul = mul.checked_mul(10)?;
        rem -= 1;
    }
    whole.checked_mul(mul)?.checked_add(frac)
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

/// Allocation-free stack parser for a representative Binance `bookTicker` JSON frame.
pub fn decode_sample_binance_book_ticker(frame: &[u8]) -> Option<DecodedBinanceSampleBbo> {
    let u_raw = find_json_field(frame, b"\"u\"")?;
    let b_raw = find_json_field(frame, b"\"b\"")?;
    let qty_b_raw = find_json_field(frame, b"\"B\"")?;
    let a_raw = find_json_field(frame, b"\"a\"")?;
    let qty_a_raw = find_json_field(frame, b"\"A\"")?;

    let update_id = parse_u64_ascii(u_raw)?;
    let bid_ticks = parse_decimal_scaled(b_raw, 2)?;
    let ask_ticks = parse_decimal_scaled(a_raw, 2)?;
    let bid_lots = parse_decimal_scaled(qty_b_raw, 4)?;
    let ask_lots = parse_decimal_scaled(qty_a_raw, 4)?;
    if bid_ticks <= 0 || ask_ticks <= bid_ticks || bid_lots <= 0 || ask_lots <= 0 {
        return None;
    }
    Some(DecodedBinanceSampleBbo {
        bid_ticks,
        bid_lots,
        ask_ticks,
        ask_lots,
        update_id,
    })
}

/// Stack-allocated RFC 2104 HMAC-SHA256 key pads (`ipad` and `opad`) requiring zero heap allocations.
#[derive(Clone, Copy)]
struct StackHmacSha256 {
    ipad: [u8; 64],
    opad: [u8; 64],
}

impl StackHmacSha256 {
    fn new(key: &[u8]) -> Self {
        let mut key_block = [0u8; 64];
        if key.len() <= 64 {
            key_block[..key.len()].copy_from_slice(key);
        } else {
            let hashed = Sha256::digest(key);
            key_block[..32].copy_from_slice(&hashed);
        }
        let mut ipad = [0x36u8; 64];
        let mut opad = [0x5cu8; 64];
        for i in 0..64 {
            ipad[i] ^= key_block[i];
            opad[i] ^= key_block[i];
        }
        Self { ipad, opad }
    }

    #[inline]
    fn sign(&self, message: &[u8]) -> [u8; 32] {
        let mut inner = Sha256::new();
        inner.update(self.ipad);
        inner.update(message);
        let inner_digest = inner.finalize();

        let mut outer = Sha256::new();
        outer.update(self.opad);
        outer.update(inner_digest);
        outer.finalize().into()
    }
}

#[inline]
fn write_hex_lower_32(digest: &[u8; 32], out: &mut [u8; 64]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for (idx, &byte) in digest.iter().enumerate() {
        out[idx * 2] = HEX[usize::from(byte >> 4)];
        out[idx * 2 + 1] = HEX[usize::from(byte & 0x0f)];
    }
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

/// Preallocated side-effect-free hot-path dry-run engine warming instruction/data caches across
/// Spot, USD-M, Coin-M, and European Options without touching live venue sockets.
pub struct BinanceHotPathWarmer {
    rest_host: &'static str,
    rest_path_prefix: &'static [u8],
    hmac: StackHmacSha256,
    sample_price: Fixed,
    scratch_account: Option<Account>,
    tls_warmer: TlsLoopbackWarmer,
    req_scratch: [u8; WARMUP_SCRATCH_BYTES],
    ws_ack_frame: [u8; 192],
    ws_ack_frame_len: usize,
    ack_scratch: [u8; 512],
}

impl BinanceHotPathWarmer {
    pub fn new(
        product: Product,
        environment: Environment,
    ) -> Result<Self, tickwire_transport::Error> {
        let (rest_host, rest_path_prefix): (&'static str, &'static [u8]) =
            match (product, environment) {
                (Product::Spot, Environment::Production) => ("api.binance.com", b"/api/v3/order?"),
                (Product::Spot, Environment::Test) => ("testnet.binance.vision", b"/api/v3/order?"),
                (Product::UsdM, Environment::Production) => {
                    ("fapi.binance.com", b"/fapi/v1/order?")
                }
                (Product::UsdM, Environment::Test) => {
                    ("testnet.binancefuture.com", b"/fapi/v1/order?")
                }
                (Product::CoinM, Environment::Production) => {
                    ("dapi.binance.com", b"/dapi/v1/order?")
                }
                (Product::CoinM, Environment::Test) => {
                    ("testnet.binancefuture.com", b"/dapi/v1/order?")
                }
                (Product::Option, _) => ("eapi.binance.com", b"/eapi/v1/order?"),
                (_, Environment::Production) => ("fapi.binance.com", b"/fapi/v1/order?"),
                (_, Environment::Test) => ("testnet.binancefuture.com", b"/fapi/v1/order?"),
            };
        let tls_warmer = TlsLoopbackWarmer::new()?.with_ack_payload(BINANCE_DUMMY_HTTP_ACK);
        let mut ws_ack_frame = [0u8; 192];
        let ws_ack_frame_len = encode_ws_server_frame_into(
            &mut ws_ack_frame,
            Opcode::Text,
            BINANCE_DUMMY_WS_ACK_JSON,
        )?;
        let sample_price = Fixed::from_atoms(5_000_000, 2)
            .map_err(|_| tickwire_transport::Error::Protocol("warmup sample price"))?;

        Ok(Self {
            rest_host,
            rest_path_prefix,
            hmac: StackHmacSha256::new(SYNTHETIC_BINANCE_SECRET),
            sample_price,
            scratch_account: None,
            tls_warmer,
            req_scratch: [0u8; WARMUP_SCRATCH_BYTES],
            ws_ack_frame,
            ws_ack_frame_len,
            ack_scratch: [0u8; 512],
        })
    }

    pub fn refresh_scratch_account(&mut self, account: &Account) {
        self.scratch_account = Some(account.clone());
    }

    /// Warms `OrderManager`'s atomic-BBO `OrsStpBook::self_cross` path and single-pass
    /// `BinanceMsgParser` without mutating order slots or sending to any venue socket.
    pub fn warm_order_manager<const CHECK_STP: bool>(
        &mut self,
        order_manager: &OrderManager<CHECK_STP>,
        symbol_id: usize,
        use_websocket: bool,
        now_mono_ns: u64,
    ) -> Result<usize, tickwire_transport::Error> {
        let _ = order_manager
            .stp_book()
            .self_cross(symbol_id, Side::Buy, 5_000_000);
        let parsed = BinanceMsgParser::parse(BINANCE_DUMMY_WS_ACK_JSON)
            .map_err(|_| tickwire_transport::Error::Protocol("binance msg parser warmup"))?;
        if parsed.http_status != Some(200) {
            return Err(tickwire_transport::Error::Protocol(
                "binance msg parser status",
            ));
        }
        self.warm_once(use_websocket, None, now_mono_ns)
    }

    /// Executes one full side-effect-free Binance hot-path dry-run iteration.
    pub fn warm_once(
        &mut self,
        use_websocket: bool,
        live_account: Option<&Account>,
        now_mono_ns: u64,
    ) -> Result<usize, tickwire_transport::Error> {
        // 1. Decode sample Binance bookTicker frame onto the stack.
        let bbo = decode_sample_binance_book_ticker(SAMPLE_BINANCE_BOOK_TICKER)
            .ok_or(tickwire_transport::Error::Protocol("binance sample bbo"))?;

        // 2. Read-only & scratch Account / StpBook / Fixed arithmetic dry-run.
        if let Some(acc) = live_account {
            let _gate = !acc.frozen() && acc.account_epoch() > 0 && acc.valid_until() > 0;
            let _ = acc.stp_book().self_cross(1, Side::Buy, self.sample_price);
        }
        if let Some(scratch) = self.scratch_account.as_ref() {
            let _gate = !scratch.frozen() && scratch.account_epoch() > 0;
            let _ = scratch
                .stp_book()
                .self_cross(1, Side::Buy, self.sample_price);
        }
        let notional = Fixed::from_atoms(i128::from(bbo.ask_ticks) * 10, 8)
            .map_err(|_| tickwire_transport::Error::Protocol("binance fixed notional"))?;
        let fee = Fixed::from_atoms(5_000, 8)
            .map_err(|_| tickwire_transport::Error::Protocol("binance fixed fee"))?;
        let reserve = notional
            .checked_add(fee)
            .and_then(|v| v.checked_mul_int(1))
            .map_err(|_| tickwire_transport::Error::Protocol("binance fixed reserve"))?;
        let _ = reserve.positive();

        // 3. Synthetic HMAC-SHA256 signing + REST HTTP/1.1 or RFC 6455 WebSocket framing.
        let ts_ms = 1_700_000_000_000u64.wrapping_add(now_mono_ns / 1_000_000);
        let mut ts_digits = [0u8; 20];
        let ts_len = write_u64_ascii(&mut ts_digits, ts_ms)?;
        let seq_tag = bbo.update_id.wrapping_add(now_mono_ns / 1_000_000) % 1_000_000;
        let mut seq_digits = [0u8; 20];
        let seq_len = write_u64_ascii(&mut seq_digits, seq_tag)?;

        if !use_websocket {
            let mut query_buf = [0u8; 256];
            let mut query_len = 0usize;
            push_slice(
                &mut query_buf,
                &mut query_len,
                b"symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=GTC&quantity=0.001&price=50000.00&newClientOrderId=tw1warm",
            )?;
            push_slice(&mut query_buf, &mut query_len, &seq_digits[..seq_len])?;
            push_slice(
                &mut query_buf,
                &mut query_len,
                b"&recvWindow=5000&timestamp=",
            )?;
            push_slice(&mut query_buf, &mut query_len, &ts_digits[..ts_len])?;

            let tag = self.hmac.sign(&query_buf[..query_len]);
            let mut sig_hex = [0u8; 64];
            write_hex_lower_32(&tag, &mut sig_hex);

            let mut path_buf = [0u8; 384];
            let mut path_len = 0usize;
            push_slice(&mut path_buf, &mut path_len, self.rest_path_prefix)?;
            push_slice(&mut path_buf, &mut path_len, &query_buf[..query_len])?;
            push_slice(&mut path_buf, &mut path_len, b"&signature=")?;
            push_slice(&mut path_buf, &mut path_len, &sig_hex)?;
            let path_str = std::str::from_utf8(&path_buf[..path_len])
                .map_err(|_| tickwire_transport::Error::Protocol("binance path utf8"))?;

            let headers = [
                ("Host", self.rest_host),
                ("X-MBX-APIKEY", SYNTHETIC_BINANCE_API_KEY),
            ];
            let req_len =
                encode_http_request_into(&mut self.req_scratch, "POST", path_str, &headers, b"")?;

            // 4a. Isolated loopback TLS roundtrip + HTTP 200 OK JSON ack parse.
            let (req_dec, ack_dec) = self.tls_warmer.warm_step_exchange(
                &self.req_scratch[..req_len],
                BINANCE_DUMMY_HTTP_ACK,
                &mut self.ack_scratch,
            )?;
            let (status, body) = decode_http_response_slice(&self.ack_scratch[..ack_dec])?;
            if status != 200
                || find_json_field(body, b"\"orderId\"") != Some(b"28")
                || find_json_field(body, b"\"status\"") != Some(b"NEW")
            {
                return Err(tickwire_transport::Error::Protocol("binance rest ack"));
            }
            Ok(req_dec.saturating_add(ack_dec))
        } else {
            let mut canonical_buf = [0u8; 256];
            let mut canonical_len = 0usize;
            push_slice(&mut canonical_buf, &mut canonical_len, b"apiKey=")?;
            push_slice(
                &mut canonical_buf,
                &mut canonical_len,
                SYNTHETIC_BINANCE_API_KEY.as_bytes(),
            )?;
            push_slice(
                &mut canonical_buf,
                &mut canonical_len,
                b"&newClientOrderId=tw1warm",
            )?;
            push_slice(
                &mut canonical_buf,
                &mut canonical_len,
                &seq_digits[..seq_len],
            )?;
            push_slice(
                &mut canonical_buf,
                &mut canonical_len,
                b"&price=50000.00&quantity=0.001&recvWindow=5000&side=BUY&symbol=BTCUSDT&timeInForce=GTC&timestamp=",
            )?;
            push_slice(&mut canonical_buf, &mut canonical_len, &ts_digits[..ts_len])?;
            push_slice(&mut canonical_buf, &mut canonical_len, b"&type=LIMIT")?;

            let tag = self.hmac.sign(&canonical_buf[..canonical_len]);
            let mut sig_hex = [0u8; 64];
            write_hex_lower_32(&tag, &mut sig_hex);

            let mut ws_json = [0u8; 512];
            let mut ws_json_len = 0usize;
            push_slice(&mut ws_json, &mut ws_json_len, b"{\"id\":\"tw1warm")?;
            push_slice(&mut ws_json, &mut ws_json_len, &seq_digits[..seq_len])?;
            push_slice(
                &mut ws_json,
                &mut ws_json_len,
                b"\",\"method\":\"order.place\",\"params\":{\"apiKey\":\"",
            )?;
            push_slice(
                &mut ws_json,
                &mut ws_json_len,
                SYNTHETIC_BINANCE_API_KEY.as_bytes(),
            )?;
            push_slice(
                &mut ws_json,
                &mut ws_json_len,
                b"\",\"newClientOrderId\":\"tw1warm",
            )?;
            push_slice(&mut ws_json, &mut ws_json_len, &seq_digits[..seq_len])?;
            push_slice(
                &mut ws_json,
                &mut ws_json_len,
                b"\",\"price\":\"50000.00\",\"quantity\":\"0.001\",\"recvWindow\":5000,\"side\":\"BUY\",\"symbol\":\"BTCUSDT\",\"timeInForce\":\"GTC\",\"timestamp\":",
            )?;
            push_slice(&mut ws_json, &mut ws_json_len, &ts_digits[..ts_len])?;
            push_slice(
                &mut ws_json,
                &mut ws_json_len,
                b",\"type\":\"LIMIT\",\"signature\":\"",
            )?;
            push_slice(&mut ws_json, &mut ws_json_len, &sig_hex)?;
            push_slice(&mut ws_json, &mut ws_json_len, b"\"}}")?;

            let mask = [
                0x5au8 ^ (seq_digits[0]),
                0xa5u8,
                0x3cu8,
                0xc3u8 ^ (ts_digits[ts_len - 1]),
            ];
            let req_len = encode_ws_client_frame_into(
                &mut self.req_scratch,
                Opcode::Text,
                mask,
                &ws_json[..ws_json_len],
            )?;

            // 4b. Isolated loopback TLS roundtrip + RFC 6455 server frame & JSON status=200 parse.
            let (req_dec, ack_dec) = self.tls_warmer.warm_step_exchange(
                &self.req_scratch[..req_len],
                &self.ws_ack_frame[..self.ws_ack_frame_len],
                &mut self.ack_scratch,
            )?;
            let (opcode, payload) = decode_ws_server_frame_slice(&self.ack_scratch[..ack_dec])?;
            if opcode != Opcode::Text
                || find_json_field(payload, b"\"status\"") != Some(b"200")
                || find_json_field(payload, b"\"orderId\"") != Some(b"28")
            {
                return Err(tickwire_transport::Error::Protocol("binance ws ack"));
            }
            Ok(req_dec.saturating_add(ack_dec))
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn stack_hmac_sha256_matches_rfc4231_test_vector_2() {
        let mac = StackHmacSha256::new(b"Jefe");
        let tag = mac.sign(b"what do ya want for nothing?");
        let mut hex = [0u8; 64];
        write_hex_lower_32(&tag, &mut hex);
        assert_eq!(
            std::str::from_utf8(&hex).unwrap(),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn binance_warmer_executes_both_rest_and_websocket_dry_runs() {
        for product in [
            Product::Spot,
            Product::UsdM,
            Product::CoinM,
            Product::Options,
        ] {
            let mut warmer = BinanceHotPathWarmer::new(product, Environment::Production).unwrap();
            for i in 0..8u64 {
                let rest_bytes = warmer.warm_once(false, None, i * 1_000_000).unwrap();
                assert!(rest_bytes > BINANCE_DUMMY_HTTP_ACK.len());
                let ws_bytes = warmer.warm_once(true, None, i * 1_000_000).unwrap();
                assert!(ws_bytes > BINANCE_DUMMY_WS_ACK_JSON.len());
            }
        }
    }
}
