//! Side-effect-free hot-path instruction/data-cache and loopback TLS warmer (`UtaHotPathWarmer`)
//! for Bybit V5 and OKX V5 Unified Trading Account sessions.
//!
//! Exercises the exact `REQ` and `RESP` hot-path stages — BBO JSON scan, `OrderManager` `dry_run`
//! admission + `OrsStpBook` check, stack WS/HTTPS wire encoding, in-memory `TlsLoopbackWarmer`
//! AES-GCM record encryption/decryption, and single-pass `JsonScan` reply parsing — with zero
//! persistent state mutation, zero network I/O, and zero rate-limiter consumption.

#![forbid(unsafe_code)]

use crate::{
    BybitUtaParser, Credentials, Environment, OkxUtaParser, SpotVenue, UtaNewOrderWireInput,
    UtaOrderParams, encode_bybit_new_order_arg, encode_bybit_ws_trade_envelope_into,
    encode_okx_new_order_arg, encode_okx_ws_trade_envelope_into, encode_signed_https_post_into,
    encode_ws_client_frame_into, try_decode_http_response, wire_encode::BufWriter,
};
use tickwire_account::{
    OrderManager, OrderManagerConfig, OrsAccountDomainConfig, OrsNewOrderRequest, OrsSymbolSpec,
};
use tickwire_transport::{TlsLoopbackWarmer, warmup::find_json_field, ws::Opcode};
use tickwire_venue::{AssetKey, Product, Side, TimeInForce};

const SYNTHETIC_API_KEY: &str = "tw-warmup-synthetic-key";
const SYNTHETIC_PASSPHRASE: &str = "tw-warmup-synthetic-pass";
const SYNTHETIC_HMAC_SECRET: &[u8] = b"tickwire-uta-hotpath-warmup-synthetic-secret-v2";

const SAMPLE_BYBIT_BBO: &[u8] = br#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000123,"data":{"s":"BTCUSDT","b":[["50000.00","0.2500"]],"a":[["50000.50","0.4000"]],"u":1042,"seq":9001}}"#;
const SAMPLE_OKX_BBO: &[u8] = br#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT-SWAP"},"data":[{"asks":[["50000.50","0.4000","0","2"]],"bids":[["50000.00","0.2500","0","3"]],"ts":"1700000000123","seqId":9001}]}"#;

const BYBIT_DUMMY_HTTP_ACK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Bapi-Limit: 100\r\nX-Bapi-Limit-Status: 99\r\nContent-Length: 81\r\n\r\n{\"retCode\":0,\"retMsg\":\"OK\",\"result\":{\"orderId\":\"10000001\",\"orderLinkId\":\"warm1\"}}";
const OKX_DUMMY_HTTP_ACK: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 81\r\n\r\n{\"code\":\"0\",\"msg\":\"\",\"data\":[{\"ordId\":\"10000001\",\"clOrdId\":\"warm1\",\"sCode\":\"0\"}]}";

/// Decoded top-of-book ticks and lots used during cache warmup.
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
    let sequence = tickwire_transport::json_scan::parse_u64_ascii(seq_raw)?;

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

/// Periodic hot-path warmup schedule parameters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UtaWarmupConfig {
    pub period_ns: u64,
    pub venue: SpotVenue,
    pub environment: Environment,
}

impl UtaWarmupConfig {
    pub const DEFAULT_PERIOD_NS: u64 = 100_000_000;

    pub const fn new(venue: SpotVenue, environment: Environment) -> Self {
        Self {
            period_ns: Self::DEFAULT_PERIOD_NS,
            venue,
            environment,
        }
    }

    pub const fn with_period_ns(mut self, period_ns: u64) -> Self {
        self.period_ns = period_ns;
        self
    }
}

/// Compatibility alias for [`UtaWarmupConfig`].
pub type SpotWarmupConfig = UtaWarmupConfig;

/// Metrics returned by one side-effect-free [`UtaHotPathWarmer`] cycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UtaWarmupReport {
    pub http_status: u16,
    pub request_bytes: usize,
    pub ws_frame_bytes: usize,
    pub response_bytes: usize,
    pub runs: u64,
}

/// Compatibility alias for [`UtaWarmupReport`].
pub type SpotWarmupReport = UtaWarmupReport;

/// Preallocated side-effect-free hot-path warmer for Bybit V5 and OKX V5 UTA execution.
pub struct UtaHotPathWarmer {
    cfg: UtaWarmupConfig,
    order_manager: OrderManager<true>,
    spec: OrsSymbolSpec,
    symbol_name: &'static str,
    tls_loopback: TlsLoopbackWarmer,
    next_seq: u64,
    last_warm_ns: Option<u64>,
    runs: u64,
}

/// Compatibility alias for [`UtaHotPathWarmer`].
pub type SpotHotPathWarmer = UtaHotPathWarmer;

impl UtaHotPathWarmer {
    pub fn new(cfg: UtaWarmupConfig) -> Result<Self, String> {
        let tls_loopback =
            TlsLoopbackWarmer::new().map_err(|e| format!("TLS loopback warmer init: {e}"))?;
        let spec = OrsSymbolSpec {
            symbol_id: 0,
            instrument_id: 1,
            product: Product::LinearPerpetual,
            wallet_id: 1,
            base_asset_id: 1,
            base_scale: 8,
            quote_asset_id: 2,
            quote_scale: 6,
            settlement_asset_id: 2,
            settlement_scale: 6,
            price_tick_atoms: 1,
            price_scale: 2,
            qty_step_atoms: 1,
            qty_scale: 4,
            contract_size_atoms: 1,
            contract_scale: 0,
            initial_margin_bps: 1_000,
        };
        let domain = OrsAccountDomainConfig {
            account_ref: 1,
            risk_domain_id: [0x55; 16],
            venue: cfg.venue.venue_id(),
            environment: cfg.environment,
            exchange_account_id: 99_001,
            epoch: 1,
        };
        let mut om_cfg = OrderManagerConfig::single_account_default(domain, vec![spec]);
        om_cfg.slots_per_strat = 64;
        let order_manager = OrderManager::new(om_cfg)?;
        order_manager
            .wallet_ledger()
            .set_balance(1, AssetKey::new(1, 2), 6, 1_000_000_000_000, 0)
            .map_err(|e| format!("warmup ledger init: {e}"))?;
        order_manager
            .dop_state()
            .configure_strategy(0, &[1])
            .map_err(|e| format!("warmup dop configure: {e:?}"))?;
        order_manager
            .dop_state()
            .login(0, 1)
            .map_err(|e| format!("warmup dop login: {e:?}"))?;
        let symbol_name = match cfg.venue {
            SpotVenue::Bybit => "BTCUSDT",
            SpotVenue::Okx => "BTC-USDT-SWAP",
        };

        Ok(Self {
            cfg,
            order_manager,
            spec,
            symbol_name,
            tls_loopback,
            next_seq: 1,
            last_warm_ns: None,
            runs: 0,
        })
    }

    #[inline]
    pub const fn runs(&self) -> u64 {
        self.runs
    }

    /// Runs a warmup cycle only when `now_mono_ns - last_warm_ns >= cfg.period_ns`.
    pub fn maybe_warm(&mut self, now_mono_ns: u64, utc_ms: u64) -> Option<UtaWarmupReport> {
        if let Some(prev) = self.last_warm_ns
            && now_mono_ns.saturating_sub(prev) < self.cfg.period_ns
        {
            return None;
        }
        let rep = self.warm_once(now_mono_ns, utc_ms).ok()?;
        self.last_warm_ns = Some(now_mono_ns);
        Some(rep)
    }

    /// Executes one full side-effect-free `REQ` + TLS + `RESP` warmup pass.
    pub fn warm_once(&mut self, now_mono_ns: u64, utc_ms: u64) -> Result<UtaWarmupReport, String> {
        let sample_bbo = match self.cfg.venue {
            SpotVenue::Bybit => SAMPLE_BYBIT_BBO,
            SpotVenue::Okx => SAMPLE_OKX_BBO,
        };
        let bbo = decode_sample_spot_bbo(self.cfg.venue, sample_bbo)
            .ok_or_else(|| "invalid warmup sample BBO".to_owned())?;

        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1).max(1);
        let req = OrsNewOrderRequest {
            strat_id: 0,
            account_ref: 1,
            msg_seqnum: seq,
            order_seqnum: seq,
            symbol_id: 0,
            side: Side::Buy,
            price_ticks: bbo.bid_ticks,
            qty_lots: bbo.bid_lots,
            tif: TimeInForce::Rpi,
            post_only: true,
            reduce_only: false,
            check_stp: true,
            dry_run: true,
            now_mono_ns,
        };
        let placed = self
            .order_manager
            .place_order(&req)
            .map_err(|e| format!("warmup dry_run place_order failed: {:?}", e.reason))?;
        debug_assert!(!placed.should_send_to_venue);

        let params = UtaOrderParams::cross_derivative_limit(TimeInForce::Rpi, false);
        let input = UtaNewOrderWireInput {
            symbol: self.symbol_name,
            spec: &self.spec,
            side: req.side,
            price_ticks: req.price_ticks,
            qty_lots: req.qty_lots,
            client_id: placed.client_id.as_str(),
            params,
        };

        let mut arg_buf = [0u8; 768];
        let mut w = BufWriter::new(&mut arg_buf);
        match self.cfg.venue {
            SpotVenue::Bybit => encode_bybit_new_order_arg(&input, &mut w)
                .map_err(|e| format!("bybit arg encode: {e}"))?,
            SpotVenue::Okx => encode_okx_new_order_arg(&input, &mut w)
                .map_err(|e| format!("okx arg encode: {e}"))?,
        }
        let arg_len = w.written();

        // 1. Warm WS trade envelope + RFC 6455 masked client framing.
        let mut ws_env = [0u8; 1024];
        let env_len = match self.cfg.venue {
            SpotVenue::Bybit => encode_bybit_ws_trade_envelope_into(
                seq,
                utc_ms.max(1_700_000_000_000),
                "order.create",
                &arg_buf[..arg_len],
                &mut ws_env,
            )
            .map_err(|e| format!("bybit ws env: {e}"))?,
            SpotVenue::Okx => {
                encode_okx_ws_trade_envelope_into(seq, "order", &arg_buf[..arg_len], &mut ws_env)
                    .map_err(|e| format!("okx ws env: {e}"))?
            }
        };
        let mut ws_frame = [0u8; 1280];
        let ws_frame_bytes = encode_ws_client_frame_into(
            Opcode::Text,
            &ws_env[..env_len],
            seq ^ utc_ms,
            &mut ws_frame,
        )
        .map_err(|e| format!("ws mask encode: {e}"))?;

        // 2. Warm signed HTTPS POST + rustls AES-GCM loopback + zero-alloc HTTP/JsonScan decode.
        let creds = match self.cfg.venue {
            SpotVenue::Bybit => Credentials::bybit(SYNTHETIC_API_KEY, SYNTHETIC_HMAC_SECRET),
            SpotVenue::Okx => Credentials::okx(
                SYNTHETIC_API_KEY,
                SYNTHETIC_HMAC_SECRET,
                SYNTHETIC_PASSPHRASE,
            ),
        };
        let path = match self.cfg.venue {
            SpotVenue::Bybit => "/v5/order/create",
            SpotVenue::Okx => "/api/v5/trade/order",
        };
        let mut http_req = [0u8; 2048];
        let request_bytes = encode_signed_https_post_into(
            &creds,
            self.cfg.environment,
            utc_ms.max(1_700_000_000_000),
            path,
            &arg_buf[..arg_len],
            &mut http_req,
        )
        .map_err(|e| format!("https post encode: {e}"))?;

        let raw_ack = match self.cfg.venue {
            SpotVenue::Bybit => BYBIT_DUMMY_HTTP_ACK,
            SpotVenue::Okx => OKX_DUMMY_HTTP_ACK,
        };
        let mut plain_rx = [0u8; 2048];
        let (_req_n, rx_n) = self
            .tls_loopback
            .warm_step_exchange(&http_req[..request_bytes], raw_ack, &mut plain_rx)
            .map_err(|e| format!("tls loopback roundtrip: {e}"))?;
        let http_resp = try_decode_http_response(&plain_rx[..rx_n])
            .map_err(|e| format!("http decode: {e}"))?
            .ok_or_else(|| "incomplete warmup HTTP response".to_owned())?;
        match self.cfg.venue {
            SpotVenue::Bybit => {
                let _ =
                    BybitUtaParser::scan(http_resp.body).map_err(|e| format!("bybit scan: {e}"))?;
            }
            SpotVenue::Okx => {
                let _ = OkxUtaParser::scan(http_resp.body).map_err(|e| format!("okx scan: {e}"))?;
            }
        }

        self.runs += 1;
        Ok(UtaWarmupReport {
            http_status: http_resp.status,
            request_bytes,
            ws_frame_bytes,
            response_bytes: rx_n,
            runs: self.runs,
        })
    }
}
