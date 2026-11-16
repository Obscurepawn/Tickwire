//! End-to-end integration tests for `tickwire-app`'s `BinanceVenueSession` and
//! `UnifiedVenueSession` wired onto `tickwire-runtime`'s stateless `OrderRoutingSystem<S>` and
//! `OrsProcess<S>` across all four Binance trading markets (`Spot`, `Usdm`, `Coinm`, `Options`).
//!
//! Verifies:
//! 1. Zero-heap-allocation hot-path `encode_new_order` + `on_message` across Binance Spot/USD-M/
//!    Coin-M/Options (`allocation_counter::measure`).
//! 2. Cold Exchange REST `Account + OpenOrders` + `tw1...` `SelfDescribingClientId` recovery
//!    advancing `DopState::next_order_seqnum`, Ed25519 `session.logon` + `userDataStream.subscribe`
//!    on Spot, `listenKey` lifecycle on USD-M & Coin-M, `HttpsLink` `/eapi/v1/order` on Options,
//!    `TRADE_LITE` -> `ORDER_TRADE_UPDATE` `FillKind::FeeAmendment` (`amendment = true`),
//!    `23,000 ns` stale-order gate (`TerminalReason::RejectStaleOrder`), `dry_run` synthetic cancel,
//!    and `SIGUSR1` / `SIGUSR2` / `SIGTERM` process lifecycle.

#![forbid(unsafe_code)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ring::rand::SystemRandom;
use ring::signature::Ed25519KeyPair;
use serde_json::json;
use sha1::{Digest as Sha1Digest, Sha1};
use tickwire_account::{
    InlineAscii, OrderManagerConfig, OrsNewOrderRequest, OrsPlaceOutcome, OrsSymbolSpec,
    make_ors_order_id,
};
use tickwire_account_gateway::AccountManager;
use tickwire_app::{
    BinanceAssetConfig, BinanceMarketEndpoint, BinanceMarketKind, BinanceSymbolConfig,
    BinanceVenueSession, BinanceVenueSessionConfig, UnifiedVenueSession,
};
use tickwire_binance::auth::SigningKey;
use tickwire_binance::full_market::BinanceVenueMarket;
use tickwire_binance_account::{
    BinanceAssetBinding, BinanceBootstrapCatalog, BinanceSymbolBinding,
};
use tickwire_control_sys::{OrsSignal, PrivateDirectory};
use tickwire_ipc::{ResourceHeader, intent_flags};
use tickwire_risk::RmsEngine;
use tickwire_runtime::{
    ControlServerConfig, DEFAULT_MAX_ORDER_AGE_NS, OrderRoutingSystem, OrsCpuConfig, OrsProcess,
    OrsProcessConfig, OrsRuntimeConfig, OrsSignalOutcome, SessionMessageEffects,
    SessionRestEndpoint, UrlRole, VenueSession,
};
use tickwire_spot_runner::{UtaEndpointConfig, UtaEndpointSpec};
use tickwire_strategy_host::{prepare_cross_thread_intent_lane, prepare_cross_thread_report_lane};
use tickwire_strategy_sdk::{
    DopConfig, DopSession, ExecutionReport, ExecutionReportKind, FillKind, IntentWriter,
    OutboundIntent, ReportIdentity, ReportReceiver,
};
use tickwire_venue::{
    ClientIdDomain, Environment, OrderType, Product, SelfDescribingClientId, Side, TerminalReason,
    TimeInForce, venue_id,
};

const TEST_DOMAIN: [u8; 16] = [0x5A; 16];
const TEST_UID: u64 = 777_001;
const TEST_STRAT_ID: u8 = 1;
const TEST_ACCOUNT_REF: u8 = 1;
const STRAT_INST: [u8; 16] = [0x21; 16];
const ORS_INST: [u8; 16] = [0x81; 16];
const BOOT_ID: [u8; 16] = [0x12; 16];
const NS_ID: [u8; 16] = [0x34; 16];

fn deterministic_clock(initial_ns: u64) -> (Arc<AtomicU64>, tickwire_transport::ClockFn) {
    let cell = Arc::new(AtomicU64::new(initial_ns));
    let reader = Arc::clone(&cell);
    let clock_fn: tickwire_transport::ClockFn = Arc::new(move || reader.load(Ordering::Acquire));
    (cell, clock_fn)
}

fn target_arch_code() -> u16 {
    if cfg!(target_arch = "x86_64") {
        1
    } else if cfg!(target_arch = "aarch64") {
        2
    } else {
        panic!("unsupported architecture")
    }
}

fn intent_header() -> ResourceHeader {
    let capacity = 64u32;
    let record_bytes = 1024u32;
    ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
        abi_minor: 0,
        header_bytes: 4096,
        resource_kind: 2,
        endian: 1,
        atomic_width: 8,
        page_shift: 12,
        isolation_shift: 7,
        target_arch: target_arch_code(),
        resource_id: [0x11; 16],
        channel_id: [0x12; 16],
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        producer_instance: STRAT_INST,
        producer_generation: 1,
        consumer_instance: ORS_INST,
        host_boot_id: BOOT_ID,
        namespace_id: NS_ID,
        risk_scope: TEST_UID,
        producer_page_offset: 4096,
        slot_offset: 8192,
        capacity,
        record_bytes,
        resource_bytes: 8192 + u64::from(capacity) * u64::from(record_bytes),
        first_seq: 1,
        initial_membership_version: 1,
        max_consumers: 1,
        required_features: 0x1c | tickwire_strategy_sdk::outbound::FEATURE,
        schema_major: 1,
        clock_domain: 1,
        qos: 1,
        event_class: 8,
        ..Default::default()
    }
}

fn report_header() -> ResourceHeader {
    let capacity = 64u32;
    let record_bytes = 512u32;
    ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
        abi_minor: 0,
        header_bytes: 4096,
        resource_kind: 3,
        endian: 1,
        atomic_width: 8,
        page_shift: 12,
        isolation_shift: 7,
        target_arch: target_arch_code(),
        resource_id: [0x21; 16],
        channel_id: [0x22; 16],
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        producer_instance: ORS_INST,
        producer_generation: 1,
        consumer_instance: STRAT_INST,
        host_boot_id: BOOT_ID,
        namespace_id: NS_ID,
        risk_scope: TEST_UID,
        producer_page_offset: 4096,
        slot_offset: 8192,
        capacity,
        record_bytes,
        resource_bytes: 8192 + u64::from(capacity) * u64::from(record_bytes),
        first_seq: 1,
        initial_membership_version: 1,
        max_consumers: 1,
        required_features: 0x1c | tickwire_strategy_sdk::reports::FEATURE,
        schema_major: 1,
        clock_domain: 1,
        qos: 1,
        event_class: 9,
        ..Default::default()
    }
}

fn generate_ed25519_pem() -> String {
    let rng = SystemRandom::new();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).expect("Ed25519 PKCS#8 generation");
    let b64 = BASE64.encode(pkcs8.as_ref());
    format!("-----BEGIN PRIVATE KEY-----\n{b64}\n-----END PRIVATE KEY-----\n")
}

fn read_http_request(stream: &mut TcpStream) -> Result<(String, String), String> {
    let mut raw = Vec::with_capacity(2048);
    let mut byte = [0u8; 1];
    while !raw.ends_with(b"\r\n\r\n") {
        let n = stream.read(&mut byte).map_err(|e| e.to_string())?;
        if n == 0 {
            return Err(String::from("EOF reading HTTP headers"));
        }
        raw.push(byte[0]);
    }
    let hdr_str = String::from_utf8_lossy(&raw).into_owned();
    let content_len = hdr_str
        .lines()
        .find_map(|line| {
            let (k, v) = line.split_once(':')?;
            if k.trim().eq_ignore_ascii_case("content-length") {
                v.trim().parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0);
    let mut body = vec![0u8; content_len];
    if content_len > 0 {
        stream.read_exact(&mut body).map_err(|e| e.to_string())?;
    }
    Ok((hdr_str, String::from_utf8_lossy(&body).into_owned()))
}

fn upgrade_ws(stream: &mut TcpStream) -> Result<String, String> {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| e.to_string())?;
    let (hdr_str, _) = read_http_request(stream)?;
    let first_line = hdr_str.lines().next().unwrap_or_default().to_owned();
    let key = hdr_str
        .lines()
        .find_map(|line| {
            let (k, v) = line.split_once(':')?;
            if k.trim().eq_ignore_ascii_case("sec-websocket-key") {
                Some(v.trim().to_owned())
            } else {
                None
            }
        })
        .ok_or_else(|| format!("missing Sec-WebSocket-Key in {hdr_str}"))?;
    let mut sha = Sha1::new();
    sha.update(key.as_bytes());
    sha.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    let accept = BASE64.encode(sha.finalize());
    let resp = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    stream
        .write_all(resp.as_bytes())
        .map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())?;
    Ok(first_line)
}

fn send_server_ws_text(stream: &mut TcpStream, text: &str) -> Result<(), String> {
    let bytes = text.as_bytes();
    let mut frame = Vec::with_capacity(10 + bytes.len());
    frame.push(0x81);
    if bytes.len() < 126 {
        frame.push(bytes.len() as u8);
    } else {
        frame.push(126);
        frame.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    }
    frame.extend_from_slice(bytes);
    stream.write_all(&frame).map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())
}

fn try_read_ws_client_text(stream: &mut TcpStream, raw_buf: &mut Vec<u8>) -> Option<String> {
    let mut tmp = [0u8; 4096];
    loop {
        match stream.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => raw_buf.extend_from_slice(&tmp[..n]),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                break;
            }
            Err(_) => break,
        }
    }
    loop {
        if raw_buf.len() < 6 {
            return None;
        }
        let opcode = raw_buf[0] & 0x0F;
        let b1 = raw_buf[1];
        if (b1 & 0x80) == 0 {
            return None;
        }
        let len_code = usize::from(b1 & 0x7F);
        let (hdr_len, payload_len) = if len_code < 126 {
            (2usize, len_code)
        } else if len_code == 126 {
            if raw_buf.len() < 8 {
                return None;
            }
            (
                4usize,
                usize::from(u16::from_be_bytes([raw_buf[2], raw_buf[3]])),
            )
        } else {
            return None;
        };
        let total_len = hdr_len + 4 + payload_len;
        if raw_buf.len() < total_len {
            return None;
        }
        let mask = [
            raw_buf[hdr_len],
            raw_buf[hdr_len + 1],
            raw_buf[hdr_len + 2],
            raw_buf[hdr_len + 3],
        ];
        let mut unmasked = Vec::with_capacity(payload_len);
        for (idx, &b) in raw_buf[hdr_len + 4..total_len].iter().enumerate() {
            unmasked.push(b ^ mask[idx % 4]);
        }
        raw_buf.drain(..total_len);
        if opcode == 0x9 {
            // Reply Pong to Ping.
            let mut pong = Vec::with_capacity(2 + unmasked.len());
            pong.push(0x8A);
            pong.push(unmasked.len() as u8);
            pong.extend_from_slice(&unmasked);
            let _ = stream.write_all(&pong);
            let _ = stream.flush();
            continue;
        }
        if opcode == 0x1 {
            return String::from_utf8(unmasked).ok();
        }
    }
}

fn read_client_ws_json(
    stream: &mut TcpStream,
    raw_buf: &mut Vec<u8>,
    timeout: Duration,
) -> Result<serde_json::Value, String> {
    let _ = stream.set_nonblocking(true);
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(txt) = try_read_ws_client_text(stream, raw_buf) {
            return serde_json::from_str(&txt).map_err(|e| format!("invalid client WS JSON: {e}"));
        }
        if Instant::now() >= deadline {
            return Err(String::from("timed out waiting for client WS JSON"));
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn build_four_market_symbols_and_catalog() -> (Vec<OrsSymbolSpec>, Arc<BinanceBootstrapCatalog>) {
    let symbol_cfgs = [
        BinanceSymbolConfig {
            symbol_id: 0,
            instrument_id: Some(0),
            market: BinanceMarketKind::Spot,
            venue_symbol: String::from("BTCUSDT"),
            wallet_id: 1,
            base_asset_id: 1,
            base_scale: 8,
            quote_asset_id: 2,
            quote_scale: 8,
            settlement_asset_id: 2,
            settlement_scale: 8,
            price_tick_atoms: 1_000_000,
            price_scale: 8,
            qty_step_atoms: 100_000,
            qty_scale: 8,
            contract_size_atoms: 1,
            contract_scale: 0,
            initial_margin_bps: 10_000,
            default_fee_asset_code: Some(String::from("USDT")),
        },
        BinanceSymbolConfig {
            symbol_id: 1,
            instrument_id: Some(1),
            market: BinanceMarketKind::Usdm,
            venue_symbol: String::from("BTCUSDT"),
            wallet_id: 2,
            base_asset_id: 1,
            base_scale: 8,
            quote_asset_id: 2,
            quote_scale: 8,
            settlement_asset_id: 2,
            settlement_scale: 8,
            price_tick_atoms: 10_000_000,
            price_scale: 8,
            qty_step_atoms: 100_000,
            qty_scale: 8,
            contract_size_atoms: 1,
            contract_scale: 0,
            initial_margin_bps: 1_000,
            default_fee_asset_code: Some(String::from("USDT")),
        },
        BinanceSymbolConfig {
            symbol_id: 2,
            instrument_id: Some(2),
            market: BinanceMarketKind::Coinm,
            venue_symbol: String::from("BTCUSD_PERP"),
            wallet_id: 3,
            base_asset_id: 1,
            base_scale: 8,
            quote_asset_id: 3,
            quote_scale: 8,
            settlement_asset_id: 1,
            settlement_scale: 8,
            price_tick_atoms: 10_000_000,
            price_scale: 8,
            qty_step_atoms: 100_000_000,
            qty_scale: 8,
            contract_size_atoms: 100,
            contract_scale: 0,
            initial_margin_bps: 2_000,
            default_fee_asset_code: Some(String::from("BTC")),
        },
        BinanceSymbolConfig {
            symbol_id: 3,
            instrument_id: Some(3),
            market: BinanceMarketKind::Options,
            venue_symbol: String::from("BTC-261225-80000-C"),
            wallet_id: 4,
            base_asset_id: 1,
            base_scale: 8,
            quote_asset_id: 2,
            quote_scale: 8,
            settlement_asset_id: 2,
            settlement_scale: 8,
            price_tick_atoms: 1_000_000,
            price_scale: 8,
            qty_step_atoms: 1_000_000,
            qty_scale: 8,
            contract_size_atoms: 1,
            contract_scale: 0,
            initial_margin_bps: 10_000,
            default_fee_asset_code: Some(String::from("USDT")),
        },
    ];

    let sym_bindings: Vec<BinanceSymbolBinding> = symbol_cfgs
        .iter()
        .map(BinanceSymbolConfig::to_binding)
        .collect::<Result<_, _>>()
        .unwrap();
    let specs: Vec<OrsSymbolSpec> = sym_bindings.iter().map(|b| b.spec).collect();

    let mut asset_bindings: Vec<BinanceAssetBinding> = Vec::new();
    for (mk, wid) in [
        (BinanceMarketKind::Spot, 1u16),
        (BinanceMarketKind::Usdm, 2u16),
        (BinanceMarketKind::Coinm, 3u16),
        (BinanceMarketKind::Options, 4u16),
    ] {
        for (code, aid) in [("BTC", 1u64), ("USDT", 2u64), ("USD", 3u64)] {
            asset_bindings.push(
                BinanceAssetConfig {
                    market: mk,
                    asset_code: String::from(code),
                    wallet_id: wid,
                    asset_id: aid,
                    scale: 8,
                }
                .to_binding(),
            );
        }
    }

    let catalog = Arc::new(BinanceBootstrapCatalog::new(sym_bindings, asset_bindings).unwrap());
    (specs, catalog)
}

fn build_four_account_manager() -> AccountManager {
    let doc = json!({
        "enabled_accounts": ["spot_main", "usdm_main", "coinm_main", "opt_main"],
        "spot_main": {
            "account_ref": 1,
            "exchange_account_id": TEST_UID,
            "strategies": [TEST_STRAT_ID],
            "local_ip": "127.0.0.1",
            "epoch": 1,
            "risk_domain_id": TEST_DOMAIN,
            "credentials": { "key": "binance-key", "secret": "binance-secret" },
            "urls": {
                "binance-spot-order": { "local_ip/connection_count": [["127.0.0.1", 1]] }
            }
        },
        "usdm_main": {
            "account_ref": 2,
            "exchange_account_id": TEST_UID + 1,
            "strategies": [TEST_STRAT_ID],
            "local_ip": "127.0.0.1",
            "epoch": 1,
            "risk_domain_id": TEST_DOMAIN,
            "credentials": { "key": "binance-key", "secret": "binance-secret" },
            "urls": {
                "binance-usdm-order": { "local_ip/connection_count": [["127.0.0.1", 1]] },
                "binance-usdm-stream": { "local_ip/connection_count": [["127.0.0.1", 1]] }
            }
        },
        "coinm_main": {
            "account_ref": 3,
            "exchange_account_id": TEST_UID + 2,
            "strategies": [TEST_STRAT_ID],
            "local_ip": "127.0.0.1",
            "epoch": 1,
            "risk_domain_id": TEST_DOMAIN,
            "credentials": { "key": "binance-key", "secret": "binance-secret" },
            "urls": {
                "binance-coinm-order": { "local_ip/connection_count": [["127.0.0.1", 1]] },
                "binance-coinm-stream": { "local_ip/connection_count": [["127.0.0.1", 1]] }
            }
        },
        "opt_main": {
            "account_ref": 4,
            "exchange_account_id": TEST_UID + 3,
            "strategies": [TEST_STRAT_ID],
            "local_ip": "127.0.0.1",
            "epoch": 1,
            "risk_domain_id": TEST_DOMAIN,
            "credentials": { "key": "binance-key", "secret": "binance-secret" },
            "urls": {
                "binance-options-order": { "local_ip/connection_count": [["127.0.0.1", 1]] },
                "binance-options-stream": { "local_ip/connection_count": [["127.0.0.1", 1]] }
            }
        }
    });
    AccountManager::from_json_str(&doc.to_string()).unwrap()
}

fn plain_endpoint(addr: std::net::SocketAddr, path: &str) -> UtaEndpointSpec {
    UtaEndpointConfig {
        host: String::from("127.0.0.1"),
        port: addr.port(),
        path: path.to_owned(),
        tls: false,
        ca_pem_path: None,
        initial_peers: vec![addr],
    }
    .to_endpoint_spec(path)
    .unwrap()
}

fn plain_rest_endpoint(addr: std::net::SocketAddr) -> SessionRestEndpoint {
    UtaEndpointConfig {
        host: String::from("127.0.0.1"),
        port: addr.port(),
        path: String::new(),
        tls: false,
        ca_pem_path: None,
        initial_peers: vec![addr],
    }
    .to_rest_endpoint()
    .unwrap()
}

#[test]
fn binance_hotpath_encode_and_scan_perform_zero_heap_allocations() {
    let (_symbols, catalog) = build_four_market_symbols_and_catalog();
    let pem = generate_ed25519_pem();
    let mut markets = Vec::new();
    for m in [
        BinanceVenueMarket::Spot,
        BinanceVenueMarket::Usdm,
        BinanceVenueMarket::Coinm,
        BinanceVenueMarket::Options,
    ] {
        markets.push(BinanceMarketEndpoint {
            market: m,
            api_key: String::from("test-key"),
            ed25519_signer: (m != BinanceVenueMarket::Options)
                .then(|| SigningKey::ed25519_pem(&pem).unwrap()),
            hmac_signer: SigningKey::hmac(b"test-hmac-secret").unwrap(),
            order_endpoint: UtaEndpointSpec::manual(m.ws_order_path().unwrap_or("/eapi/v1/order")),
            stream_endpoint: None,
            rest_endpoint: None,
            stream_colo: false,
        });
    }

    let mut cfg =
        BinanceVenueSessionConfig::new(Environment::Test, TEST_UID, TEST_DOMAIN, catalog, markets);
    cfg.clock_utc_ms = Some(Arc::new(|| 1_700_000_000_000));
    let session = BinanceVenueSession::new(cfg).expect("construct BinanceVenueSession");

    let acct_mgr = build_four_account_manager();
    let spec = &acct_mgr.specs()[0];
    let domain = ClientIdDomain {
        risk_domain_id: TEST_DOMAIN,
        venue: venue_id::BINANCE,
        environment: Environment::Test,
        account: TEST_UID,
        product: Product::Spot,
    };
    let cid = SelfDescribingClientId::from_ors_order(1, 0, 1, 9)
        .unwrap()
        .encode(&domain)
        .unwrap();
    let mut out = [0u8; 2048];
    let trade_lite = format!(
        r#"{{"e":"TRADE_LITE","E":1700000000100,"T":1700000000099,"s":"BTCUSDT","q":"0.001","p":"50000.0","m":false,"c":"{}","S":"BUY","L":"50000.0","l":"0.001","t":901,"i":55001}}"#,
        cid.as_str()
    );
    let mut effects = SessionMessageEffects {
        events: Vec::with_capacity(8),
        rate_limits: Vec::with_capacity(4),
        query_status: Vec::with_capacity(4),
        direct_reports: Vec::with_capacity(2),
        stream_token_expired: false,
        graceful_drain_connection: false,
    };

    // Warm up once before measuring.
    let warm_req = OrsNewOrderRequest {
        strat_id: TEST_STRAT_ID,
        account_ref: TEST_ACCOUNT_REF,
        msg_seqnum: 9,
        order_seqnum: 9,
        symbol_id: 0,
        side: Side::Buy,
        price_ticks: 5_000_000,
        qty_lots: 1,
        tif: TimeInForce::Gtc,
        post_only: false,
        reduce_only: false,
        check_stp: true,
        dry_run: false,
        now_mono_ns: 1_000_000_000,
    };
    let warm_placed = OrsPlaceOutcome {
        ors_order_id: make_ors_order_id(9, TEST_STRAT_ID),
        slot_idx: 0,
        account_ref: TEST_ACCOUNT_REF,
        product: Product::Spot,
        client_id: InlineAscii::lossy_ascii(cid.as_str()),
        should_send_to_venue: true,
    };
    let _ = session
        .encode_new_order(spec, &warm_req, &warm_placed, 1, &mut out)
        .unwrap();
    effects.clear();
    session.on_message(
        TEST_ACCOUNT_REF,
        "binance-usdm-stream",
        UrlRole::STREAM,
        1,
        None,
        trade_lite.as_bytes(),
        1_000_000_000,
        &mut effects,
    );

    let info = allocation_counter::measure(|| {
        for (seq, product, sym_id) in [
            (10u64, Product::Spot, 0u32),
            (11u64, Product::UsdM, 1u32),
            (12u64, Product::CoinM, 2u32),
            (13u64, Product::Option, 3u32),
        ] {
            let req = OrsNewOrderRequest {
                strat_id: TEST_STRAT_ID,
                account_ref: TEST_ACCOUNT_REF,
                msg_seqnum: seq,
                order_seqnum: seq,
                symbol_id: sym_id,
                side: Side::Buy,
                price_ticks: 500_000,
                qty_lots: 1,
                tif: TimeInForce::Gtc,
                post_only: false,
                reduce_only: false,
                check_stp: true,
                dry_run: false,
                now_mono_ns: 1_000_000_100 + seq,
            };
            let placed = OrsPlaceOutcome {
                ors_order_id: make_ors_order_id(seq, TEST_STRAT_ID),
                slot_idx: 0,
                account_ref: TEST_ACCOUNT_REF,
                product,
                client_id: InlineAscii::lossy_ascii(cid.as_str()),
                should_send_to_venue: true,
            };
            let enc = session
                .encode_new_order(spec, &req, &placed, 1, &mut out)
                .unwrap();
            assert!(enc.frame_len > 0);
        }
        effects.clear();
        session.on_message(
            TEST_ACCOUNT_REF,
            "binance-usdm-stream",
            UrlRole::STREAM,
            1,
            None,
            trade_lite.as_bytes(),
            1_000_000_200,
            &mut effects,
        );
        assert_eq!(effects.events.len(), 2);
    });

    assert_eq!(
        info.count_total, 0,
        "Binance hot-path encode + scan must perform zero heap allocations, got {}",
        info.count_total
    );
}

#[test]
fn unified_binance_four_market_cold_recovery_amendment_stale_gate_and_signals() {
    let dir = PrivateDirectory::create_in(Path::new("/tmp"), "tw-b4").unwrap();
    let dop_sock = dir.path().join("dop.sock");
    let rms_path = dir.path().join("rms.cfg");
    let mut rms_rules = String::new();
    for sid in 0..4 {
        rms_rules.push_str(&format!(
            "symbol_id={sid} enabled=true max_qty=1000000 max_notional=1000000000000 new_count=1000/5000/10000 order_qty=1000000/5000000/10000000\n"
        ));
    }
    std::fs::write(&rms_path, &rms_rules).unwrap();

    let spot_domain = ClientIdDomain {
        risk_domain_id: TEST_DOMAIN,
        venue: venue_id::BINANCE,
        environment: Environment::Test,
        account: TEST_UID,
        product: Product::Spot,
    };
    let recovered_cid = SelfDescribingClientId::from_ors_order(TEST_STRAT_ID, 0, 1, 7)
        .unwrap()
        .encode(&spot_domain)
        .unwrap();

    let rest_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let rest_addr = rest_listener.local_addr().unwrap();
    let spot_order_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let spot_order_addr = spot_order_listener.local_addr().unwrap();
    let usdm_order_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let usdm_order_addr = usdm_order_listener.local_addr().unwrap();
    let usdm_stream_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let usdm_stream_addr = usdm_stream_listener.local_addr().unwrap();
    let coinm_order_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let coinm_order_addr = coinm_order_listener.local_addr().unwrap();
    let coinm_stream_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let coinm_stream_addr = coinm_stream_listener.local_addr().unwrap();
    let opt_order_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let opt_order_addr = opt_order_listener.local_addr().unwrap();
    let opt_stream_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let opt_stream_addr = opt_stream_listener.local_addr().unwrap();

    let stop = Arc::new(AtomicBool::new(false));

    // 1. REST server handling cold Account + OpenOrders + listenKey POST across all 4 markets.
    let stop_rest = Arc::clone(&stop);
    let rec_cid_str = recovered_cid.as_str().to_owned();
    let rest_thread = thread::spawn(move || {
        let _ = rest_listener.set_nonblocking(true);
        while !stop_rest.load(Ordering::Acquire) {
            match rest_listener.accept() {
                Ok((mut s, _)) => {
                    let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
                    let Ok((hdr, _)) = read_http_request(&mut s) else {
                        continue;
                    };
                    let first = hdr.lines().next().unwrap_or_default();
                    let body = if first.starts_with("GET /api/v3/account") {
                        String::from(
                            r#"{"balances":[{"asset":"BTC","free":"2.00000000","locked":"0.00000000"},{"asset":"USDT","free":"250000.00000000","locked":"50.00000000"}]}"#,
                        )
                    } else if first.starts_with("GET /api/v3/openOrders") {
                        format!(
                            r#"[{{"symbol":"BTCUSDT","orderId":70007,"clientOrderId":"{rec_cid_str}","price":"50000.00","origQty":"0.001","executedQty":"0.000","status":"NEW","timeInForce":"GTC","type":"LIMIT","side":"BUY"}}]"#
                        )
                    } else if first.starts_with("GET /fapi/v2/account") {
                        String::from(
                            r#"{"assets":[{"asset":"USDT","walletBalance":"100000.00000000","availableBalance":"100000.00000000"}],"positions":[]}"#,
                        )
                    } else if first.starts_with("GET /fapi/v1/openOrders") {
                        String::from("[]")
                    } else if first.starts_with("POST /fapi/v1/listenKey")
                        || first.starts_with("PUT /fapi/v1/listenKey")
                    {
                        String::from(r#"{"listenKey":"lk-usdm-01"}"#)
                    } else if first.starts_with("GET /dapi/v1/account") {
                        String::from(
                            r#"{"assets":[{"asset":"BTC","walletBalance":"5.00000000","availableBalance":"5.00000000"}],"positions":[]}"#,
                        )
                    } else if first.starts_with("GET /dapi/v1/openOrders") {
                        String::from("[]")
                    } else if first.starts_with("POST /dapi/v1/listenKey")
                        || first.starts_with("PUT /dapi/v1/listenKey")
                    {
                        String::from(r#"{"listenKey":"lk-coinm-01"}"#)
                    } else if first.starts_with("GET /eapi/v1/account") {
                        String::from(
                            r#"{"asset":[{"asset":"USDT","equity":"50000.00000000","available":"50000.00000000"}],"greek":[]}"#,
                        )
                    } else if first.starts_with("GET /eapi/v1/openOrders") {
                        String::from("[]")
                    } else if first.starts_with("POST /eapi/v1/listenKey")
                        || first.starts_with("PUT /eapi/v1/listenKey")
                    {
                        String::from(r#"{"listenKey":"lk-opt-01"}"#)
                    } else {
                        String::from("{}")
                    };
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = s.write_all(resp.as_bytes());
                    let _ = s.flush();
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(2));
                }
                Err(_) => break,
            }
        }
    });

    // 2. Spot WS API server (`session.logon` Ed25519 + `userDataStream.subscribe` + `order.place` + `order.cancel`).
    let stop_spot = Arc::clone(&stop);
    let spot_thread = thread::spawn(move || {
        let run = || -> Result<(), String> {
            let (mut ws, _) = spot_order_listener.accept().map_err(|e| e.to_string())?;
            upgrade_ws(&mut ws)?;
            let mut buf = Vec::new();
            let logon = read_client_ws_json(&mut ws, &mut buf, Duration::from_secs(5))?;
            assert_eq!(logon["method"].as_str(), Some("session.logon"));
            send_server_ws_text(
                &mut ws,
                &json!({
                    "id": logon["id"],
                    "status": 200,
                    "result": { "apiKey": "binance-key", "authorizedSince": 1700000000000u64 }
                })
                .to_string(),
            )?;

            let sub = read_client_ws_json(&mut ws, &mut buf, Duration::from_secs(5))?;
            assert_eq!(sub["method"].as_str(), Some("userDataStream.subscribe"));
            send_server_ws_text(
                &mut ws,
                &json!({
                    "id": sub["id"],
                    "status": 200,
                    "result": { "subscriptionId": 0 }
                })
                .to_string(),
            )?;

            while !stop_spot.load(Ordering::Acquire) {
                if let Ok(req) = read_client_ws_json(&mut ws, &mut buf, Duration::from_millis(30)) {
                    match req["method"].as_str() {
                        Some("order.place") => {
                            let cid = req["params"]["newClientOrderId"]
                                .as_str()
                                .unwrap_or_default()
                                .to_owned();
                            assert!(cid.starts_with("tw1") && cid.len() == 32);
                            send_server_ws_text(
                                &mut ws,
                                &json!({
                                    "id": req["id"],
                                    "status": 200,
                                    "result": {
                                        "symbol": "BTCUSDT",
                                        "orderId": 80008u64,
                                        "clientOrderId": cid,
                                        "transactTime": 1700000000100u64,
                                        "price": "50000.00",
                                        "origQty": "0.001",
                                        "executedQty": "0.000",
                                        "status": "NEW",
                                        "type": "LIMIT",
                                        "side": "BUY"
                                    }
                                })
                                .to_string(),
                            )?;
                        }
                        Some("order.cancel") => {
                            let orig = req["params"]["origClientOrderId"]
                                .as_str()
                                .unwrap_or_default()
                                .to_owned();
                            send_server_ws_text(
                                &mut ws,
                                &json!({
                                    "id": req["id"],
                                    "status": 200,
                                    "result": {
                                        "symbol": "BTCUSDT",
                                        "origClientOrderId": orig,
                                        "orderId": 70007u64,
                                        "clientOrderId": "cancel-1",
                                        "status": "CANCELED"
                                    }
                                })
                                .to_string(),
                            )?;
                        }
                        _ => {}
                    }
                }
            }
            Ok(())
        };
        run().expect("spot_thread failed");
    });

    // 3. USD-M WS API + listenKey stream (`TRADE_LITE` fast fill -> `ORDER_TRADE_UPDATE` fee amendment!).
    let stop_usdm = Arc::clone(&stop);
    let usdm_thread = thread::spawn(move || {
        let run = || -> Result<(), String> {
            let (mut order_ws, _) = usdm_order_listener.accept().map_err(|e| e.to_string())?;
            upgrade_ws(&mut order_ws)?;
            let mut buf = Vec::new();
            let logon = read_client_ws_json(&mut order_ws, &mut buf, Duration::from_secs(5))?;
            assert_eq!(logon["method"].as_str(), Some("session.logon"));
            send_server_ws_text(
                &mut order_ws,
                &json!({
                    "id": logon["id"],
                    "status": 200,
                    "result": { "apiKey": "binance-key" }
                })
                .to_string(),
            )?;

            let (mut stream_ws, _) = usdm_stream_listener.accept().map_err(|e| e.to_string())?;
            let req_line = upgrade_ws(&mut stream_ws)?;
            assert!(req_line.contains("lk-usdm-01"), "got {req_line}");

            while !stop_usdm.load(Ordering::Acquire) {
                if let Ok(req) =
                    read_client_ws_json(&mut order_ws, &mut buf, Duration::from_millis(30))
                    && req["method"].as_str() == Some("order.place")
                {
                    let cid = req["params"]["newClientOrderId"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned();
                    send_server_ws_text(
                        &mut order_ws,
                        &json!({
                            "id": req["id"],
                            "status": 200,
                            "result": {
                                "orderId": 81001u64,
                                "symbol": "BTCUSDT",
                                "status": "NEW",
                                "clientOrderId": cid,
                                "price": "50000.0",
                                "origQty": "0.001",
                                "executedQty": "0.000"
                            }
                        })
                        .to_string(),
                    )?;
                    send_server_ws_text(
                        &mut stream_ws,
                        &json!({
                            "e": "TRADE_LITE",
                            "E": 1700000000200u64,
                            "T": 1700000000199u64,
                            "s": "BTCUSDT",
                            "q": "0.001",
                            "p": "50000.0",
                            "m": false,
                            "c": cid,
                            "S": "BUY",
                            "L": "50000.0",
                            "l": "0.001",
                            "t": 9901u64,
                            "i": 81001u64
                        })
                        .to_string(),
                    )?;
                    send_server_ws_text(
                        &mut stream_ws,
                        &json!({
                            "e": "ORDER_TRADE_UPDATE",
                            "E": 1700000000205u64,
                            "T": 1700000000204u64,
                            "o": {
                                "s": "BTCUSDT",
                                "c": cid,
                                "S": "BUY",
                                "o": "LIMIT",
                                "f": "GTC",
                                "q": "0.001",
                                "p": "50000.0",
                                "X": "FILLED",
                                "x": "TRADE",
                                "i": 81001u64,
                                "l": "0.001",
                                "z": "0.001",
                                "L": "50000.0",
                                "n": "0.025",
                                "N": "USDT",
                                "t": 9901u64,
                                "m": false,
                                "ps": "BOTH"
                            }
                        })
                        .to_string(),
                    )?;
                }
            }
            Ok(())
        };
        run().expect("usdm_thread failed");
    });

    // 4. Coin-M WS API + listenKey stream.
    let stop_coinm = Arc::clone(&stop);
    let coinm_thread = thread::spawn(move || {
        let run = || -> Result<(), String> {
            let (mut order_ws, _) = coinm_order_listener.accept().map_err(|e| e.to_string())?;
            upgrade_ws(&mut order_ws)?;
            let mut buf = Vec::new();
            let logon = read_client_ws_json(&mut order_ws, &mut buf, Duration::from_secs(5))?;
            assert_eq!(logon["method"].as_str(), Some("session.logon"));
            send_server_ws_text(
                &mut order_ws,
                &json!({
                    "id": logon["id"],
                    "status": 200,
                    "result": { "apiKey": "binance-key" }
                })
                .to_string(),
            )?;

            let (mut stream_ws, _) = coinm_stream_listener.accept().map_err(|e| e.to_string())?;
            let req_line = upgrade_ws(&mut stream_ws)?;
            assert!(req_line.contains("lk-coinm-01"));

            while !stop_coinm.load(Ordering::Acquire) {
                if let Ok(req) =
                    read_client_ws_json(&mut order_ws, &mut buf, Duration::from_millis(30))
                {
                    match req["method"].as_str() {
                        Some("order.place") => {
                            let cid = req["params"]["newClientOrderId"]
                                .as_str()
                                .unwrap_or_default()
                                .to_owned();
                            send_server_ws_text(
                                &mut order_ws,
                                &json!({
                                    "id": req["id"],
                                    "status": 200,
                                    "result": {
                                        "orderId": 82001u64,
                                        "symbol": "BTCUSD_PERP",
                                        "status": "NEW",
                                        "clientOrderId": cid,
                                        "price": "50000.0",
                                        "origQty": "1",
                                        "executedQty": "0"
                                    }
                                })
                                .to_string(),
                            )?;
                        }
                        Some("order.cancel") => {
                            let orig = req["params"]["origClientOrderId"]
                                .as_str()
                                .unwrap_or_default()
                                .to_owned();
                            send_server_ws_text(
                                &mut order_ws,
                                &json!({
                                    "id": req["id"],
                                    "status": 200,
                                    "result": {
                                        "orderId": 82001u64,
                                        "symbol": "BTCUSD_PERP",
                                        "origClientOrderId": orig,
                                        "status": "CANCELED"
                                    }
                                })
                                .to_string(),
                            )?;
                        }
                        _ => {}
                    }
                }
            }
            let _ = stream_ws;
            Ok(())
        };
        run().expect("coinm_thread failed");
    });

    // 5. Options HTTP order link (`/eapi/v1/order`) + listenKey WS stream.
    let stop_opt = Arc::clone(&stop);
    let opt_thread = thread::spawn(move || -> Result<(), String> {
        let (mut order_http, _) = opt_order_listener.accept().map_err(|e| e.to_string())?;
        let (mut stream_ws, _) = opt_stream_listener.accept().map_err(|e| e.to_string())?;
        let req_line = upgrade_ws(&mut stream_ws)?;
        assert!(req_line.contains("lk-opt-01"));

        order_http
            .set_read_timeout(Some(Duration::from_millis(30)))
            .map_err(|e| e.to_string())?;
        while !stop_opt.load(Ordering::Acquire) {
            if let Ok((hdr, _)) = read_http_request(&mut order_http) {
                let first = hdr.lines().next().unwrap_or_default();
                let cid = first
                    .split(['?', '&', ' '])
                    .find_map(|kv| kv.strip_prefix("clientOrderId="))
                    .unwrap_or_default()
                    .to_owned();
                let body = if first.starts_with("POST /eapi/v1/order") {
                    json!({
                        "orderId": 83001u64,
                        "symbol": "BTC-261225-80000-C",
                        "price": "1200.00",
                        "quantity": "0.01",
                        "executedQty": "0.00",
                        "side": "BUY",
                        "type": "LIMIT",
                        "status": "ACCEPTED",
                        "clientOrderId": cid
                    })
                    .to_string()
                } else {
                    json!({
                        "orderId": 83001u64,
                        "symbol": "BTC-261225-80000-C",
                        "status": "CANCELLED",
                        "clientOrderId": cid
                    })
                    .to_string()
                };
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
                    body.len(),
                    body
                );
                order_http
                    .write_all(resp.as_bytes())
                    .map_err(|e| e.to_string())?;
                order_http.flush().map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    });

    let (symbols, catalog) = build_four_market_symbols_and_catalog();
    let pem = generate_ed25519_pem();
    let spot_session = Arc::new(UnifiedVenueSession::Binance(
        BinanceVenueSession::new(BinanceVenueSessionConfig::new(
            Environment::Test,
            TEST_UID,
            TEST_DOMAIN,
            Arc::clone(&catalog),
            vec![BinanceMarketEndpoint {
                market: BinanceVenueMarket::Spot,
                api_key: String::from("binance-key"),
                ed25519_signer: Some(SigningKey::ed25519_pem(&pem).unwrap()),
                hmac_signer: SigningKey::hmac(b"binance-secret").unwrap(),
                order_endpoint: plain_endpoint(spot_order_addr, "/ws-api/v3"),
                stream_endpoint: None,
                rest_endpoint: Some(plain_rest_endpoint(rest_addr)),
                stream_colo: true,
            }],
        ))
        .unwrap(),
    ));
    let usdm_session = Arc::new(UnifiedVenueSession::Binance(
        BinanceVenueSession::new(BinanceVenueSessionConfig::new(
            Environment::Test,
            TEST_UID + 1,
            TEST_DOMAIN,
            Arc::clone(&catalog),
            vec![BinanceMarketEndpoint {
                market: BinanceVenueMarket::Usdm,
                api_key: String::from("binance-key"),
                ed25519_signer: Some(SigningKey::ed25519_pem(&pem).unwrap()),
                hmac_signer: SigningKey::hmac(b"binance-secret").unwrap(),
                order_endpoint: plain_endpoint(usdm_order_addr, "/ws-fapi/v1"),
                stream_endpoint: Some(plain_endpoint(usdm_stream_addr, "/ws")),
                rest_endpoint: Some(plain_rest_endpoint(rest_addr)),
                stream_colo: false,
            }],
        ))
        .unwrap(),
    ));
    let coinm_session = Arc::new(UnifiedVenueSession::Binance(
        BinanceVenueSession::new(BinanceVenueSessionConfig::new(
            Environment::Test,
            TEST_UID + 2,
            TEST_DOMAIN,
            Arc::clone(&catalog),
            vec![BinanceMarketEndpoint {
                market: BinanceVenueMarket::Coinm,
                api_key: String::from("binance-key"),
                ed25519_signer: Some(SigningKey::ed25519_pem(&pem).unwrap()),
                hmac_signer: SigningKey::hmac(b"binance-secret").unwrap(),
                order_endpoint: plain_endpoint(coinm_order_addr, "/ws-dapi/v1"),
                stream_endpoint: Some(plain_endpoint(coinm_stream_addr, "/ws")),
                rest_endpoint: Some(plain_rest_endpoint(rest_addr)),
                stream_colo: false,
            }],
        ))
        .unwrap(),
    ));
    let opt_session = Arc::new(UnifiedVenueSession::Binance(
        BinanceVenueSession::new(BinanceVenueSessionConfig::new(
            Environment::Test,
            TEST_UID + 3,
            TEST_DOMAIN,
            catalog,
            vec![BinanceMarketEndpoint {
                market: BinanceVenueMarket::Options,
                api_key: String::from("binance-key"),
                ed25519_signer: None,
                hmac_signer: SigningKey::hmac(b"binance-secret").unwrap(),
                order_endpoint: plain_endpoint(opt_order_addr, "/eapi/v1/order"),
                stream_endpoint: Some(plain_endpoint(opt_stream_addr, "/ws")),
                rest_endpoint: Some(plain_rest_endpoint(rest_addr)),
                stream_colo: false,
            }],
        ))
        .unwrap(),
    ));

    let acct_mgr = build_four_account_manager();
    let domain_cfgs: Vec<_> = acct_mgr
        .specs()
        .iter()
        .map(|s| s.to_domain_config(venue_id::BINANCE, Environment::Test))
        .collect();
    let mut om_cfg = OrderManagerConfig::single_account_default(domain_cfgs[0], symbols);
    om_cfg.accounts = domain_cfgs;
    om_cfg.slots_per_strat = 64;
    let rms = RmsEngine::new(4);
    rms.reload_handle().reload_limits(&rms_rules).unwrap();

    let (intent_starter, intent_factory) =
        prepare_cross_thread_intent_lane(intent_header(), 1, [0x41; 16], TEST_STRAT_ID, 1).unwrap();
    let (report_starter, report_factory) = prepare_cross_thread_report_lane(
        report_header(),
        1,
        [0x42; 16],
        ReportIdentity {
            strat_id: TEST_STRAT_ID,
            instance_id: STRAT_INST,
            strategy_generation: 1,
        },
    )
    .unwrap();

    let (clock_cell, clock_fn) = deterministic_clock(10_000_000_000);
    let rt_cfg = OrsRuntimeConfig {
        cpu: OrsCpuConfig {
            req_cpu: None,
            resp_cpu: None,
            housekeeping_cpus: None,
            epoll_wait_ms: 1,
        },
        max_order_age_ns: DEFAULT_MAX_ORDER_AGE_NS,
        clock: clock_fn,
        ..OrsRuntimeConfig::default()
    };
    let proc_cfg = OrsProcessConfig {
        control_socket_path: Some(dop_sock.clone()),
        control_config: ControlServerConfig {
            ors_instance: ORS_INST,
            ..ControlServerConfig::default()
        },
        rms_config_path: Some(rms_path),
        shutdown_drain_timeout: Duration::from_millis(300),
        block_signals_on_spawn: false,
    };

    let process = OrsProcess::build_and_spawn(
        rt_cfg,
        &acct_mgr,
        &[TEST_STRAT_ID],
        om_cfg,
        rms,
        vec![
            (1, spot_session),
            (2, usdm_session),
            (3, coinm_session),
            (4, opt_session),
        ],
        proc_cfg,
        move |ors: &mut OrderRoutingSystem<UnifiedVenueSession>| {
            ors.attach_strategy_lane_factories(TEST_STRAT_ID, intent_factory, report_factory)
        },
    )
    .unwrap();

    let mut intent_writer: IntentWriter = intent_starter.finish(Duration::from_secs(5)).unwrap();
    let mut report_rx: ReportReceiver = report_starter.finish(Duration::from_secs(5)).unwrap();

    let mut dop = DopSession::connect_and_login(
        &dop_sock,
        DopConfig {
            strat_id: TEST_STRAT_ID,
            client_id: 101,
            local_instance: STRAT_INST,
            peer_instance: ORS_INST,
            control_epoch: 1,
            expected_peer_uid: None,
            max_order_age_ns: DEFAULT_MAX_ORDER_AGE_NS,
        },
        1,
        clock_cell.load(Ordering::Acquire),
        Duration::from_secs(5),
    )
    .unwrap();

    // Cold REST recovery found `order_seqnum = 7`, so DOP login MUST report `next_order_seqnum = 8`!
    assert_eq!(dop.next_order_seqnum(), 8);
    intent_writer.sync_from_dop(&dop).unwrap();

    let wait_ready = Instant::now() + Duration::from_secs(5);
    while !process.accounts().iter().all(|a| a.is_ready()) {
        let states: Vec<(u8, bool)> = process
            .accounts()
            .iter()
            .map(|a| (a.account_ref(), a.is_ready()))
            .collect();
        assert!(
            Instant::now() < wait_ready,
            "timed out waiting for 4-account / 4-market Binance session readiness: {states:?}"
        );
        clock_cell.fetch_add(100_000_000, Ordering::AcqRel);
        thread::sleep(Duration::from_millis(5));
    }

    let next_report = |rx: &mut ReportReceiver| -> ExecutionReport {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(rep) = rx.poll().unwrap() {
                return rep;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for ReportRecord"
            );
            thread::sleep(Duration::from_millis(2));
        }
    };

    // 1) Stale order (`tick_mono_ns` 50us behind `clock` > 23us `DEFAULT_MAX_ORDER_AGE_NS`) -> `RejectStaleOrder`.
    let now = clock_cell.load(Ordering::Acquire);
    let stale = OutboundIntent::new_order(
        TEST_STRAT_ID,
        TEST_ACCOUNT_REF,
        0,
        1,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        5_000_000,
        1,
        "stale-hint",
        now - 50_000,
    )
    .unwrap();
    intent_writer.publish(&stale, now).unwrap();
    dop.observe_published_sequences(
        intent_writer.next_msg_seqnum(),
        intent_writer.next_order_seqnum(),
    );
    let rep_stale = next_report(&mut report_rx);
    assert_eq!(rep_stale.kind, ExecutionReportKind::Nack);
    assert_eq!(rep_stale.reason, TerminalReason::RejectStaleOrder);

    // 2) `dry_run` order -> synthetic `Canceled(AbortedBeforeSend)` with zero wire dispatch.
    let mut dry = OutboundIntent::new_order(
        TEST_STRAT_ID,
        TEST_ACCOUNT_REF,
        0,
        1,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        5_000_000,
        1,
        "dry-hint",
        now,
    )
    .unwrap();
    dry.intent_flags |= intent_flags::DRY_RUN;
    intent_writer.publish(&dry, now).unwrap();
    dop.observe_published_sequences(
        intent_writer.next_msg_seqnum(),
        intent_writer.next_order_seqnum(),
    );
    let rep_dry = next_report(&mut report_rx);
    assert_eq!(rep_dry.kind, ExecutionReportKind::Canceled);
    assert_eq!(rep_dry.reason, TerminalReason::AbortedBeforeSend);

    // 3) Live Spot order (`symbol_id = 0`) -> `Ack` (`80008`).
    let spot_order = OutboundIntent::new_order(
        TEST_STRAT_ID,
        TEST_ACCOUNT_REF,
        0,
        1,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        5_000_000,
        1,
        "spot-live",
        now,
    )
    .unwrap();
    intent_writer.publish(&spot_order, now).unwrap();
    dop.observe_published_sequences(
        intent_writer.next_msg_seqnum(),
        intent_writer.next_order_seqnum(),
    );
    loop {
        let rep = next_report(&mut report_rx);
        if matches!(rep.kind, ExecutionReportKind::Ack) {
            break;
        }
    }

    // 4) Live USD-M order (`account_ref = 2`, `symbol_id = 1`) -> `Ack` + `TRADE_LITE` `Fill(Execution)` + `ORDER_TRADE_UPDATE` `Fill(FeeAmendment)`!
    let usdm_order = OutboundIntent::new_order(
        TEST_STRAT_ID,
        2,
        1,
        1,
        Product::UsdM,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        500_000,
        1,
        "usdm-live",
        now,
    )
    .unwrap();
    intent_writer.publish(&usdm_order, now).unwrap();
    dop.observe_published_sequences(
        intent_writer.next_msg_seqnum(),
        intent_writer.next_order_seqnum(),
    );
    let mut saw_exec_fill = false;
    let mut saw_fee_amend = false;
    while !(saw_exec_fill && saw_fee_amend) {
        let rep = next_report(&mut report_rx);
        if let ExecutionReportKind::Fill(fill) = rep.kind {
            match fill.kind {
                FillKind::Execution => saw_exec_fill = true,
                FillKind::FeeAmendment => {
                    let fee = fill.fee.expect("expected attached fee on FeeAmendment");
                    assert_eq!(fee.fee_units, -2_500_000);
                    saw_fee_amend = true;
                }
                _ => {}
            }
        }
    }

    // 5) Live Coin-M (`account_ref = 3`, `symbol_id = 2`) and Options (`account_ref = 4`, `symbol_id = 3`) orders -> both `Ack`ed!
    let coinm_order = OutboundIntent::new_order(
        TEST_STRAT_ID,
        3,
        2,
        1,
        Product::CoinM,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        500_000,
        1,
        "coinm-live",
        now,
    )
    .unwrap();
    intent_writer.publish(&coinm_order, now).unwrap();
    dop.observe_published_sequences(
        intent_writer.next_msg_seqnum(),
        intent_writer.next_order_seqnum(),
    );
    loop {
        let rep = next_report(&mut report_rx);
        if matches!(rep.kind, ExecutionReportKind::Ack) && rep.instrument_id == 2 {
            break;
        }
    }

    let opt_order = OutboundIntent::new_order(
        TEST_STRAT_ID,
        4,
        3,
        1,
        Product::Option,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        120_000,
        1,
        "opt-live",
        now,
    )
    .unwrap();
    intent_writer.publish(&opt_order, now).unwrap();
    dop.observe_published_sequences(
        intent_writer.next_msg_seqnum(),
        intent_writer.next_order_seqnum(),
    );
    loop {
        let rep = next_report(&mut report_rx);
        if matches!(rep.kind, ExecutionReportKind::Ack) && rep.instrument_id == 3 {
            break;
        }
    }

    // 6) Signals: `SIGUSR1` reloads static RMS + schedules `QueryOpen`; `SIGUSR2` triggers `CancelAll` + `OrsRestricted`; `SIGTERM` broadcasts `OrsShutDown`.
    assert!(matches!(
        process.handle_signal(OrsSignal::SigUsr1),
        OrsSignalOutcome::ReloadedRms { generation: Ok(_) }
    ));
    assert_eq!(
        process.handle_signal(OrsSignal::SigUsr2),
        OrsSignalOutcome::EmergencyCancelAll
    );
    loop {
        let rep = next_report(&mut report_rx);
        if matches!(rep.kind, ExecutionReportKind::OrsRestricted) {
            break;
        }
    }

    let _ = dop.logout(
        false,
        TerminalReason::AdminAction,
        Instant::now() + Duration::from_secs(1),
    );
    assert_eq!(
        process.handle_signal(OrsSignal::SigTerm),
        OrsSignalOutcome::ShutdownInitiated
    );
    loop {
        let rep = next_report(&mut report_rx);
        if matches!(rep.kind, ExecutionReportKind::OrsShutDown) {
            break;
        }
    }

    stop.store(true, Ordering::Release);
    let _ = spot_thread.join();
    let _ = usdm_thread.join();
    let _ = coinm_thread.join();
    let _ = opt_thread.join();
    let _ = rest_thread.join();
}
