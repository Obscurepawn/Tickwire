//! Ephemeral loopback TLS exchange peer and end-to-end stateless `OrsProcess<UtaVenueSession>`
//! verification driver for Bybit V5 and OKX V5 Unified Trading Accounts.
//!
//! Generates an in-memory self-signed TLS certificate, serves authenticated cold REST
//! `Account + OpenOrders` snapshots, accepts a `Dialer`-managed TLS `HttpsLink` order connection,
//! verifies HMAC-SHA256 headers and the 32-character `[0-9a-z]` `SelfDescribingClientId` (`tw1...`)
//! MAC on every submitted order, and exercises `DopLogin` + inline `1,024B` `IntentRecord` /
//! `512B` `ReportRecord` SHM rings + `SIGTERM` `OrsShutDown` with zero local disk files.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use ring::hmac;
use serde::Serialize;
use serde_json::{Value, json};
use tickwire_account::{OrderManagerConfig, OrsSymbolSpec};
use tickwire_account_gateway::AccountManager;
use tickwire_control_sys::{OrsSignal, PrivateDirectory};
use tickwire_ipc::ResourceHeader;
use tickwire_risk::RmsEngine;
use tickwire_runtime::{
    ControlServerConfig, OrderRoutingSystem, OrsCpuConfig, OrsProcess, OrsProcessConfig,
    OrsRuntimeConfig, OrsSignalOutcome, SessionRestEndpoint,
};
use tickwire_spot_execution::network::OwnedCredentials;
use tickwire_spot_execution::rest_bootstrap::{UtaAssetBinding, UtaSymbolBinding};
use tickwire_spot_execution::{Environment, SpotVenue, UtaTransportMode, authority};
use tickwire_strategy_host::{prepare_cross_thread_intent_lane, prepare_cross_thread_report_lane};
use tickwire_strategy_sdk::{
    DopConfig, DopSession, ExecutionReportKind, OutboundIntent, ReportIdentity,
};
use tickwire_transport::rest_querier::DialSecurity;
use tickwire_transport::{mono_now_ns, rustls};
use tickwire_venue::{
    AssetKey, ClientIdDomain, DecodedClientId, OrderType, Product, SelfDescribingClientId, Side,
    TimeInForce,
};

use crate::session::{UtaEndpointSpec, UtaVenueSession, UtaVenueSessionConfig};

const PAPER_KEY: &str = "paper-key";
const PAPER_SECRET: &str = "local-paper-secret";
const PAPER_PASSPHRASE: &str = "paper-pass";
const PAPER_UID: u64 = 900_001;
const PAPER_RISK_DOMAIN: [u8; 16] = [0x42; 16];
const PAPER_STRAT_ID: u8 = 1;
const PAPER_ACCOUNT_REF: u8 = 1;
const PAPER_STRAT_INSTANCE: [u8; 16] = [0x31; 16];
const PAPER_ORS_INSTANCE: [u8; 16] = [0x77; 16];

/// Summary produced by [`run_paper_demo`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PaperRunSummary {
    pub venue: String,
    pub account_ref: u8,
    pub strat_id: u8,
    pub orders_acknowledged: usize,
    pub native_acknowledgements: usize,
    pub open_orders: usize,
    pub client_ids: Vec<String>,
    pub venue_order_ids: Vec<String>,
}

struct EphemeralTlsPair {
    server_cfg: Arc<rustls::ServerConfig>,
    client_cfg: Arc<rustls::ClientConfig>,
}

fn generate_ephemeral_tls(host: &str) -> Result<EphemeralTlsPair, String> {
    let cert = rcgen::generate_simple_self_signed(vec![host.to_owned()])
        .map_err(|e| format!("rcgen self-signed cert failed: {e}"))?;
    let key_der = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let cert_der = cert.cert.der().clone();

    let server_cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der.into())
        .map_err(|e| format!("rustls ServerConfig failed: {e}"))?;

    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(cert_der)
        .map_err(|e| format!("RootCertStore::add failed: {e}"))?;
    let client_cfg = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    Ok(EphemeralTlsPair {
        server_cfg: Arc::new(server_cfg),
        client_cfg: Arc::new(client_cfg),
    })
}

type ParsedHttpRequest = (String, String, BTreeMap<String, String>, Vec<u8>);

fn read_http_request<R: Read>(
    wire: &mut BufReader<R>,
) -> Result<Option<ParsedHttpRequest>, String> {
    let mut req_line = String::new();
    let n = wire
        .read_line(&mut req_line)
        .map_err(|e| format!("HTTP request line read error: {e}"))?;
    if n == 0 {
        return Ok(None);
    }
    let mut parts = req_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| String::from("missing HTTP method"))?
        .to_owned();
    let path = parts
        .next()
        .ok_or_else(|| String::from("missing HTTP path"))?
        .to_owned();

    let mut headers = BTreeMap::new();
    let mut header_bytes = 0usize;
    loop {
        let mut line = String::new();
        let read = wire
            .read_line(&mut line)
            .map_err(|e| format!("HTTP header read error: {e}"))?;
        header_bytes += read;
        if read == 0 || header_bytes > 16 * 1024 {
            return Err(String::from("malformed or oversized HTTP headers"));
        }
        if line == "\r\n" || line == "\n" {
            break;
        }
        let (k, v) = line
            .trim_end()
            .split_once(':')
            .ok_or_else(|| format!("malformed HTTP header line: {line:?}"))?;
        headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_owned());
    }

    let content_len = headers
        .get("content-length")
        .map(|s| s.parse::<usize>())
        .transpose()
        .map_err(|e| format!("invalid Content-Length: {e}"))?
        .unwrap_or(0);
    if content_len > 16 * 1024 {
        return Err(String::from("oversized HTTP body"));
    }
    let mut body = vec![0u8; content_len];
    if content_len > 0 {
        wire.read_exact(&mut body)
            .map_err(|e| format!("HTTP body read_exact failed: {e}"))?;
    }
    Ok(Some((method, path, headers, body)))
}

fn verify_hmac_signature(
    venue: SpotVenue,
    method: &str,
    path: &str,
    headers: &BTreeMap<String, String>,
    body: &[u8],
) -> Result<(), String> {
    let key = hmac::Key::new(hmac::HMAC_SHA256, PAPER_SECRET.as_bytes());
    let mut mac = hmac::Context::with_key(&key);
    match venue {
        SpotVenue::Bybit => {
            let api_key = headers
                .get("x-bapi-api-key")
                .ok_or_else(|| String::from("missing x-bapi-api-key"))?;
            if api_key != PAPER_KEY {
                return Err(format!("unexpected Bybit api key: {api_key}"));
            }
            let ts = headers
                .get("x-bapi-timestamp")
                .ok_or_else(|| String::from("missing x-bapi-timestamp"))?;
            let rw = headers
                .get("x-bapi-recv-window")
                .ok_or_else(|| String::from("missing x-bapi-recv-window"))?;
            let sign = headers
                .get("x-bapi-sign")
                .ok_or_else(|| String::from("missing x-bapi-sign"))?;
            mac.update(ts.as_bytes());
            mac.update(api_key.as_bytes());
            mac.update(rw.as_bytes());
            if method == "GET" {
                if let Some((_, query)) = path.split_once('?') {
                    mac.update(query.as_bytes());
                }
            } else {
                mac.update(body);
            }
            let expected: String = mac
                .sign()
                .as_ref()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            if sign != &expected {
                return Err(String::from("Bybit HMAC signature mismatch"));
            }
        }
        SpotVenue::Okx => {
            let api_key = headers
                .get("ok-access-key")
                .ok_or_else(|| String::from("missing ok-access-key"))?;
            let pass = headers
                .get("ok-access-passphrase")
                .ok_or_else(|| String::from("missing ok-access-passphrase"))?;
            let sim = headers
                .get("x-simulated-trading")
                .ok_or_else(|| String::from("missing x-simulated-trading"))?;
            let ts = headers
                .get("ok-access-timestamp")
                .ok_or_else(|| String::from("missing ok-access-timestamp"))?;
            let sign = headers
                .get("ok-access-sign")
                .ok_or_else(|| String::from("missing ok-access-sign"))?;
            if api_key != PAPER_KEY || pass != PAPER_PASSPHRASE || sim != "1" {
                return Err(String::from(
                    "unexpected OKX credentials or simulated header",
                ));
            }
            mac.update(ts.as_bytes());
            mac.update(method.as_bytes());
            mac.update(path.as_bytes());
            mac.update(body);
            let expected = STANDARD.encode(mac.sign().as_ref());
            if sign != &expected {
                return Err(String::from("OKX HMAC signature mismatch"));
            }
        }
    }
    Ok(())
}

fn serve_cold_rest_peer(
    venue: SpotVenue,
    server_cfg: Arc<rustls::ServerConfig>,
    listener: TcpListener,
) -> Result<(), String> {
    for _ in 0..2 {
        let (sock, _) = listener
            .accept()
            .map_err(|e| format!("REST peer accept failed: {e}"))?;
        sock.set_read_timeout(Some(Duration::from_secs(5)))
            .map_err(|e| format!("REST peer set_read_timeout failed: {e}"))?;
        let tls_conn = rustls::ServerConnection::new(Arc::clone(&server_cfg))
            .map_err(|e| format!("REST TLS ServerConnection failed: {e}"))?;
        let mut wire = BufReader::new(rustls::StreamOwned::new(tls_conn, sock));
        let Some((method, path, headers, body)) = read_http_request(&mut wire)? else {
            return Err(String::from("empty cold REST connection"));
        };
        verify_hmac_signature(venue, &method, &path, &headers, &body)?;

        let payload = match venue {
            SpotVenue::Bybit if path.starts_with("/v5/account/wallet-balance") => json!({
                "retCode": 0,
                "result": {
                    "list": [{
                        "accountType": "UNIFIED",
                        "coin": [
                            { "coin": "BTC", "walletBalance": "2.0", "locked": "0", "totalOrderIM": "0" },
                            { "coin": "USDT", "walletBalance": "250000.0", "locked": "0", "totalOrderIM": "0" }
                        ]
                    }]
                }
            }),
            SpotVenue::Bybit if path.starts_with("/v5/order/realtime") => json!({
                "retCode": 0,
                "result": { "list": [], "nextPageCursor": "" }
            }),
            SpotVenue::Okx if path.starts_with("/api/v5/account/balance") => json!({
                "code": "0",
                "data": [{
                    "details": [
                        { "ccy": "BTC", "availBal": "2.0", "ordFrozen": "0" },
                        { "ccy": "USDT", "availBal": "250000.0", "ordFrozen": "0" }
                    ]
                }]
            }),
            SpotVenue::Okx if path.starts_with("/api/v5/trade/orders-pending") => json!({
                "code": "0",
                "data": []
            }),
            _ => return Err(format!("unexpected cold REST path: {path}")),
        };
        let bytes = serde_json::to_vec(&payload).map_err(|e| e.to_string())?;
        write!(
            wire.get_mut(),
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            bytes.len()
        )
        .map_err(|e| format!("REST write header failed: {e}"))?;
        wire.get_mut()
            .write_all(&bytes)
            .map_err(|e| format!("REST write body failed: {e}"))?;
        wire.get_mut()
            .flush()
            .map_err(|e| format!("REST flush failed: {e}"))?;
    }
    Ok(())
}

fn serve_order_https_peer(
    venue: SpotVenue,
    server_cfg: Arc<rustls::ServerConfig>,
    listener: TcpListener,
    expected_orders: usize,
) -> Result<Vec<String>, String> {
    let (sock, _) = listener
        .accept()
        .map_err(|e| format!("order peer accept failed: {e}"))?;
    sock.set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| format!("order peer set_read_timeout failed: {e}"))?;
    let tls_conn = rustls::ServerConnection::new(server_cfg)
        .map_err(|e| format!("order TLS ServerConnection failed: {e}"))?;
    let mut wire = BufReader::new(rustls::StreamOwned::new(tls_conn, sock));

    let domain = ClientIdDomain {
        risk_domain_id: PAPER_RISK_DOMAIN,
        venue: venue.venue_id(),
        environment: Environment::Test,
        account: PAPER_UID,
        product: Product::Spot,
    };

    let mut seen_cids = Vec::with_capacity(expected_orders);
    for idx in 1..=expected_orders {
        let Some((method, path, headers, body)) = read_http_request(&mut wire)? else {
            return Err(String::from(
                "order connection closed before expected orders arrived",
            ));
        };
        verify_hmac_signature(venue, &method, &path, &headers, &body)?;
        let parsed: Value =
            serde_json::from_slice(&body).map_err(|e| format!("invalid order JSON: {e}"))?;

        let cid = match venue {
            SpotVenue::Bybit => {
                if path != "/v5/order/create" || parsed["category"] != "spot" {
                    return Err(format!(
                        "unexpected Bybit order request on {path}: {parsed}"
                    ));
                }
                parsed["orderLinkId"]
                    .as_str()
                    .ok_or_else(|| String::from("missing Bybit orderLinkId"))?
            }
            SpotVenue::Okx => {
                if path != "/api/v5/trade/order" || parsed["tdMode"] != "cash" {
                    return Err(format!("unexpected OKX order request on {path}: {parsed}"));
                }
                parsed["clOrdId"]
                    .as_str()
                    .ok_or_else(|| String::from("missing OKX clOrdId"))?
            }
        };

        let DecodedClientId::Managed(decoded) = SelfDescribingClientId::decode_str(cid, &domain)
        else {
            return Err(format!(
                "order client_id '{cid}' failed tw1... MAC verification"
            ));
        };
        if decoded.ors_strat_id() != Some(PAPER_STRAT_ID) {
            return Err(format!(
                "unexpected decoded strat_id {:?} in '{cid}'",
                decoded.ors_strat_id()
            ));
        }
        seen_cids.push(cid.to_owned());

        let oid_str = format!("paper-venue-{idx}");
        let resp_json = match venue {
            SpotVenue::Bybit => json!({
                "retCode": 0,
                "result": {
                    "orderId": oid_str,
                    "orderLinkId": cid
                }
            }),
            SpotVenue::Okx => json!({
                "code": "0",
                "data": [{
                    "sCode": "0",
                    "ordId": oid_str,
                    "clOrdId": cid
                }]
            }),
        };
        let resp_bytes = serde_json::to_vec(&resp_json).map_err(|e| e.to_string())?;
        write!(
            wire.get_mut(),
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            resp_bytes.len()
        )
        .map_err(|e| format!("order response header write failed: {e}"))?;
        wire.get_mut()
            .write_all(&resp_bytes)
            .map_err(|e| format!("order response body write failed: {e}"))?;
        wire.get_mut()
            .flush()
            .map_err(|e| format!("order response flush failed: {e}"))?;
    }

    while let Ok(Some((method, path, headers, body))) = read_http_request(&mut wire) {
        verify_hmac_signature(venue, &method, &path, &headers, &body)?;
        let parsed: Value =
            serde_json::from_slice(&body).map_err(|e| format!("invalid cancel JSON: {e}"))?;
        let cid = match venue {
            SpotVenue::Bybit => parsed["orderLinkId"].as_str().unwrap_or(""),
            SpotVenue::Okx => parsed["clOrdId"].as_str().unwrap_or(""),
        };
        let resp_json = match venue {
            SpotVenue::Bybit => json!({
                "retCode": 0,
                "result": { "orderId": "paper-cxl", "orderLinkId": cid }
            }),
            SpotVenue::Okx => json!({
                "code": "0",
                "data": [{ "sCode": "0", "ordId": "paper-cxl", "clOrdId": cid }]
            }),
        };
        let resp_bytes = serde_json::to_vec(&resp_json).map_err(|e| e.to_string())?;
        let _ = write!(
            wire.get_mut(),
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            resp_bytes.len()
        );
        let _ = wire.get_mut().write_all(&resp_bytes);
        let _ = wire.get_mut().flush();
    }

    Ok(seen_cids)
}

fn target_arch_code() -> u16 {
    if cfg!(target_arch = "x86_64") {
        1
    } else if cfg!(target_arch = "aarch64") {
        2
    } else {
        0
    }
}

fn intent_ring_header(strat_instance: [u8; 16], ors_instance: [u8; 16]) -> ResourceHeader {
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
        resource_id: [0x33; 16],
        channel_id: [0x44; 16],
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        producer_instance: strat_instance,
        producer_generation: 1,
        consumer_instance: ors_instance,
        host_boot_id: [0x11; 16],
        namespace_id: [0x22; 16],
        risk_scope: 1,
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

fn report_ring_header(ors_instance: [u8; 16], strat_instance: [u8; 16]) -> ResourceHeader {
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
        resource_id: [0x55; 16],
        channel_id: [0x66; 16],
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        producer_instance: ors_instance,
        producer_generation: 1,
        consumer_instance: strat_instance,
        host_boot_id: [0x11; 16],
        namespace_id: [0x22; 16],
        risk_scope: 1,
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

/// Executes a complete stateless loopback TLS run for `venue` (`Bybit` or `Okx`) over
/// `OrsProcess<UtaVenueSession>`, `DopSession`, and inline SHM `IntentWriter`/`ReportReceiver`.
pub fn run_paper_demo(venue: SpotVenue) -> Result<PaperRunSummary, String> {
    let host = authority(venue, Environment::Test);
    let tls = generate_ephemeral_tls(host)?;

    let rest_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .map_err(|e| format!("bind rest_listener failed: {e}"))?;
    let rest_addr: SocketAddr = rest_listener
        .local_addr()
        .map_err(|e| format!("rest local_addr failed: {e}"))?;

    let order_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .map_err(|e| format!("bind order_listener failed: {e}"))?;
    let order_addr: SocketAddr = order_listener
        .local_addr()
        .map_err(|e| format!("order local_addr failed: {e}"))?;

    let rest_srv = Arc::clone(&tls.server_cfg);
    let rest_job = thread::spawn(move || serve_cold_rest_peer(venue, rest_srv, rest_listener));

    let order_srv = Arc::clone(&tls.server_cfg);
    let order_job =
        thread::spawn(move || serve_order_https_peer(venue, order_srv, order_listener, 2));

    let sym_str = match venue {
        SpotVenue::Bybit => "BTCUSDT",
        SpotVenue::Okx => "BTC-USDT",
    };
    let symbol_spec = OrsSymbolSpec {
        symbol_id: 0,
        instrument_id: 1001,
        product: Product::Spot,
        wallet_id: 0,
        base_asset_id: 1,
        base_scale: 8,
        quote_asset_id: 2,
        quote_scale: 8,
        settlement_asset_id: 2,
        settlement_scale: 8,
        price_tick_atoms: 1_000_000, // 0.01 USDT
        price_scale: 8,
        qty_step_atoms: 100_000, // 0.001 BTC
        qty_scale: 8,
        contract_size_atoms: 1,
        contract_scale: 0,
        initial_margin_bps: 10_000,
    };
    let sym_binding = UtaSymbolBinding::new(0, sym_str, symbol_spec)
        .map_err(|e| format!("UtaSymbolBinding::new failed: {e}"))?;
    let assets = vec![
        UtaAssetBinding::new(
            "BTC",
            AssetKey {
                wallet_id: 0,
                asset_id: 1,
            },
            8,
        )
        .map_err(|e| e.to_string())?,
        UtaAssetBinding::new(
            "USDT",
            AssetKey {
                wallet_id: 0,
                asset_id: 2,
            },
            8,
        )
        .map_err(|e| e.to_string())?,
    ];

    let owned_creds = match venue {
        SpotVenue::Bybit => OwnedCredentials::bybit(PAPER_KEY, PAPER_SECRET),
        SpotVenue::Okx => OwnedCredentials::okx(PAPER_KEY, PAPER_SECRET, PAPER_PASSPHRASE),
    }
    .map_err(|e| format!("OwnedCredentials failed: {e}"))?;
    let order_path = match venue {
        SpotVenue::Bybit => "/v5/order/create",
        SpotVenue::Okx => "/api/v5/trade/order",
    };

    let server_name = rustls::pki_types::ServerName::try_from(host.to_owned())
        .map_err(|e| format!("invalid TLS ServerName '{host}': {e}"))?;

    let session = Arc::new(UtaVenueSession::new(UtaVenueSessionConfig {
        venue,
        environment: Environment::Test,
        exchange_uid: PAPER_UID,
        risk_domain_id: PAPER_RISK_DOMAIN,
        credentials: owned_creds,
        transport_mode: UtaTransportMode::PooledHttps,
        order_endpoint: UtaEndpointSpec::tls(
            host,
            order_addr.port(),
            order_path,
            Arc::clone(&tls.client_cfg),
            vec![order_addr],
        )?,
        stream_endpoint: None,
        rest_endpoint: Some(SessionRestEndpoint {
            url_id: None,
            host: host.to_owned(),
            port: rest_addr.port(),
            security: DialSecurity::Tls {
                server_name,
                config: Arc::clone(&tls.client_cfg),
            },
            initial_peers: vec![rest_addr],
        }),
        symbols: vec![sym_binding],
        assets,
        ip_order_weight: 1,
        status_query_delay_ms: 500,
        okx_ws_auto_subscribe_private: false,
        rfq_hook: None,
        clock_utc_ms: None,
        bootstrap_override: None,
    })?);

    let url_id = format!("{}-uta-order", venue.as_str());
    let acct_json = json!({
        "enabled_accounts": ["paper_uta"],
        "paper_uta": {
            "account_ref": PAPER_ACCOUNT_REF,
            "exchange_account_id": PAPER_UID,
            "strategies": [PAPER_STRAT_ID],
            "local_ip": "127.0.0.1",
            "epoch": 1,
            "risk_domain_id": PAPER_RISK_DOMAIN,
            "credentials": {
                "key": PAPER_KEY,
                "secret": PAPER_SECRET,
                "passphrase": PAPER_PASSPHRASE
            },
            "urls": {
                url_id: { "local_ip/connection_count": [["127.0.0.1", 1]] }
            }
        }
    });
    let account_manager = AccountManager::from_json_str(&acct_json.to_string())
        .map_err(|e| format!("AccountManager::from_json_str failed: {e}"))?;

    let mut om_cfg = OrderManagerConfig::single_account_default(
        account_manager.specs()[0].to_domain_config(venue.venue_id(), Environment::Test),
        vec![symbol_spec],
    );
    om_cfg.slots_per_strat = 256;

    let rms_engine = RmsEngine::new(1);
    rms_engine
        .reload_handle()
        .reload_limits(
            "symbol_id=0 enabled=true max_qty=100000 max_notional=100000000000 new_count=100/500/1000 order_qty=100000/500000/1000000\n",
        )
        .map_err(|e| format!("RmsEngine reload_limits failed: {e}"))?;

    let (intent_starter, intent_factory) = prepare_cross_thread_intent_lane(
        intent_ring_header(PAPER_STRAT_INSTANCE, PAPER_ORS_INSTANCE),
        1,
        [0x88; 16],
        PAPER_STRAT_ID,
        1,
    )
    .map_err(|e| format!("prepare_cross_thread_intent_lane failed: {e}"))?;
    let (report_starter, report_factory) = prepare_cross_thread_report_lane(
        report_ring_header(PAPER_ORS_INSTANCE, PAPER_STRAT_INSTANCE),
        1,
        [0x99; 16],
        ReportIdentity {
            strat_id: PAPER_STRAT_ID,
            instance_id: PAPER_STRAT_INSTANCE,
            strategy_generation: 1,
        },
    )
    .map_err(|e| format!("prepare_cross_thread_report_lane failed: {e}"))?;

    let sock_dir = PrivateDirectory::create_in(&std::env::temp_dir(), "tw-paper")
        .map_err(|e| format!("PrivateDirectory::create_in failed: {e}"))?;
    let dop_sock = sock_dir.path().join("dop.sock");

    let runtime_cfg = OrsRuntimeConfig {
        cpu: OrsCpuConfig::default(),
        max_order_age_ns: 2_000_000_000,
        ..OrsRuntimeConfig::default()
    };
    let proc_cfg = OrsProcessConfig {
        control_socket_path: Some(dop_sock.clone()),
        control_config: ControlServerConfig {
            ors_instance: PAPER_ORS_INSTANCE,
            ..ControlServerConfig::default()
        },
        shutdown_drain_timeout: Duration::from_millis(500),
        block_signals_on_spawn: false,
        ..OrsProcessConfig::default()
    };

    let process = OrsProcess::build_and_spawn(
        runtime_cfg,
        &account_manager,
        &[PAPER_STRAT_ID],
        om_cfg,
        rms_engine,
        vec![(PAPER_ACCOUNT_REF, session)],
        proc_cfg,
        move |ors: &mut OrderRoutingSystem<UtaVenueSession>| {
            ors.attach_strategy_lane_factories(PAPER_STRAT_ID, intent_factory, report_factory)
        },
    )?;

    rest_job
        .join()
        .map_err(|_| String::from("cold REST peer thread panicked"))??;

    let mut intent_writer = intent_starter
        .finish(Duration::from_secs(5))
        .map_err(|e| format!("CrossThreadIntentStarter::finish failed: {e}"))?;
    let mut report_receiver = report_starter
        .finish(Duration::from_secs(5))
        .map_err(|e| format!("CrossThreadReportStarter::finish failed: {e}"))?;

    let ready_deadline = Instant::now() + Duration::from_secs(5);
    while !process.accounts()[0].is_ready() {
        if Instant::now() >= ready_deadline {
            process.shutdown();
            return Err(String::from(
                "timed out waiting for Dialer TLS HttpsLink readiness",
            ));
        }
        thread::sleep(Duration::from_millis(5));
    }

    let mut dop = DopSession::connect_and_login(
        &dop_sock,
        DopConfig {
            strat_id: PAPER_STRAT_ID,
            client_id: 1,
            local_instance: PAPER_STRAT_INSTANCE,
            peer_instance: PAPER_ORS_INSTANCE,
            control_epoch: 1,
            expected_peer_uid: None,
            max_order_age_ns: 2_000_000_000,
        },
        1,
        mono_now_ns(),
        Duration::from_secs(3),
    )
    .map_err(|e| format!("DopSession::connect_and_login failed: {e}"))?;
    intent_writer
        .sync_from_dop(&dop)
        .map_err(|e| format!("IntentWriter::sync_from_dop failed: {e}"))?;

    let mut venue_ids = Vec::new();
    for idx in 1..=2u64 {
        let now_ns = mono_now_ns();
        let intent = OutboundIntent::new_order(
            PAPER_STRAT_ID,
            0, // default route -> `account_ref = 1`
            0, // `symbol_id = 0` (`OrderManager::symbol_spec(body.instrument_id as usize)`)
            1,
            Product::Spot,
            Side::Buy,
            OrderType::Limit,
            Some(TimeInForce::Gtc),
            6_000_000, // 60,000.00 USDT
            1,         // 0.001 BTC
            &format!("paper-seq-{idx}"),
            now_ns,
        )
        .map_err(|e| format!("OutboundIntent::new_order failed: {e}"))?;

        intent_writer
            .publish(&intent, now_ns)
            .map_err(|e| format!("IntentWriter::publish failed: {e}"))?;
        dop.observe_published_sequences(
            intent_writer.next_msg_seqnum(),
            intent_writer.next_order_seqnum(),
        );

        let ack_deadline = Instant::now() + Duration::from_secs(5);
        let mut got_ack = false;
        while !got_ack {
            if Instant::now() >= ack_deadline {
                process.shutdown();
                return Err(String::from("timed out waiting for ReportRecord Ack"));
            }
            if let Some(rep) = report_receiver
                .poll()
                .map_err(|e| format!("report_receiver.poll failed: {e}"))?
            {
                match rep.kind {
                    ExecutionReportKind::Ack => {
                        if let Some(vid) = rep.venue_order_id {
                            venue_ids.push(vid.as_str().to_owned());
                        }
                        got_ack = true;
                    }
                    ExecutionReportKind::Nack => {
                        process.shutdown();
                        return Err(format!("paper order unexpectedly NACKed: {:?}", rep.reason));
                    }
                    _ => {}
                }
            } else {
                thread::sleep(Duration::from_millis(1));
            }
        }
    }

    let sig_out = process.handle_signal(OrsSignal::SigTerm);
    if sig_out != OrsSignalOutcome::ShutdownInitiated {
        return Err(String::from("expected ShutdownInitiated on SigTerm"));
    }

    let client_ids = order_job
        .join()
        .map_err(|_| String::from("order peer thread panicked"))??;

    let ack_count = venue_ids.len();
    Ok(PaperRunSummary {
        venue: venue.as_str().to_owned(),
        account_ref: PAPER_ACCOUNT_REF,
        strat_id: PAPER_STRAT_ID,
        orders_acknowledged: ack_count,
        native_acknowledgements: ack_count,
        open_orders: ack_count,
        client_ids,
        venue_order_ids: venue_ids,
    })
}
