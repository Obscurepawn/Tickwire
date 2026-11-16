//! Deterministic loopback TLS Binance Spot exchange counterpart (`tickwire-lab`).
//!
//! Serves:
//! - Public Spot depth snapshot (`GET /api/v3/depth`) and WebSocket depth stream (`/ws/...`) for
//!   `tickwire-md`.
//! - Authenticated cold Exchange REST `GET /api/v3/account`, `GET /api/v3/openOrders` (returning a
//!   pre-existing `tw1...` open order at `order_seqnum = 3`), and `GET /api/v3/order` for
//!   `tickwire-app` stateless startup recovery and `OrsQueryDesk`.
//! - Spot WebSocket API v3 (`/ws-api/v3`) verifying Ed25519 `session.logon`,
//!   `userDataStream.subscribe`, `order.place` (verifying 32-char `tw1...`
//!   `SelfDescribingClientId` at `order_seqnum = 4`), `order.cancel`, and private
//!   `executionReport` `TRADE`/`FILLED` + `CANCELED` frames.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ring::hmac;
use ring::signature::{ED25519, UnparsedPublicKey};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::{ServerConfig, ServerConnection, StreamOwned};
use serde_json::{Value, json};
use sha1::{Digest as Sha1Digest, Sha1};
use tickwire_venue::SelfDescribingClientId;

use crate::model::{
    LAB_COLD_OPEN_SEQNUM, LAB_STRAT_ID, cold_open_order_client_id, spot_client_id_domain,
};
use crate::{Result, Scenario};

const MAX_REQUEST: usize = 32_768;
type TlsStream = StreamOwned<ServerConnection, TcpStream>;

struct State {
    scenario: Scenario,
    api_key: String,
    hmac_secret: Vec<u8>,
    ed25519_pub: Vec<u8>,
    cold_cid: String,
    cold_canceled: AtomicBool,
    live_cid: Mutex<Option<String>>,
    live_placed_at: Mutex<Option<Instant>>,
    live_filled: AtomicBool,
    sequence: AtomicU64,
    first_eligible_quote: OnceLock<Instant>,
    audit: Mutex<std::fs::File>,
    connections: AtomicUsize,
}

/// Runs the loopback TLS exchange fixture until `scenario.root.join("STOP")` appears.
pub fn serve(s: &Scenario) -> Result<()> {
    s.validate()?;
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(fs::read(&s.private_key)?));
    let tls_cfg = Arc::new(
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()?
            .with_no_client_auth()
            .with_single_cert(vec![CertificateDer::from(fs::read(&s.certificate)?)], key)?,
    );
    let listener = TcpListener::bind(s.listen)?;
    listener.set_nonblocking(true)?;

    let state = Arc::new(State {
        scenario: s.clone(),
        api_key: String::from_utf8(fs::read(&s.api_key)?)?,
        hmac_secret: fs::read(&s.api_secret)?,
        ed25519_pub: fs::read(&s.ed25519_public_key)?,
        cold_cid: cold_open_order_client_id()?,
        cold_canceled: AtomicBool::new(false),
        live_cid: Mutex::new(None),
        live_placed_at: Mutex::new(None),
        live_filled: AtomicBool::new(false),
        sequence: AtomicU64::new(100),
        first_eligible_quote: OnceLock::new(),
        audit: Mutex::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(s.root.join("fixture.jsonl"))?,
        ),
        connections: AtomicUsize::new(0),
    });

    crate::generate::write_private(&s.root.join("fixture.ready"), b"bound\n")?;
    while !s.root.join("STOP").exists() {
        match listener.accept() {
            Ok((socket, peer)) => {
                if !peer.ip().is_loopback()
                    || state.connections.fetch_add(1, Ordering::AcqRel) >= 128
                {
                    state.connections.fetch_sub(1, Ordering::AcqRel);
                    drop(socket);
                    continue;
                }
                let st = Arc::clone(&state);
                let cfg = Arc::clone(&tls_cfg);
                std::thread::spawn(move || {
                    if let Err(err) = handle_connection(socket, cfg, &st) {
                        let _ = write_audit(
                            &st,
                            json!({ "event": "connection_closed", "error": err.to_string() }),
                        );
                    }
                    st.connections.fetch_sub(1, Ordering::AcqRel);
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

fn write_audit(s: &State, value: Value) -> Result<()> {
    let mut f = s.audit.lock().map_err(|_| "audit lock poisoned")?;
    if f.metadata()?.len() > 16 * 1024 * 1024 {
        return Err("fixture audit log exceeded 16 MiB".into());
    }
    serde_json::to_writer(&mut *f, &value)?;
    f.write_all(b"\n")?;
    f.flush()?;
    Ok(())
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(1_700_000_000_000)
}

fn handle_connection(socket: TcpStream, config: Arc<ServerConfig>, s: &State) -> Result<()> {
    socket.set_read_timeout(Some(Duration::from_millis(50)))?;
    socket.set_write_timeout(Some(Duration::from_secs(2)))?;
    socket.set_nodelay(true)?;
    let mut tls = StreamOwned::new(ServerConnection::new(config)?, socket);
    let mut input = Vec::new();
    loop {
        let request = read_request(&mut tls, &mut input, s)?;
        if request
            .headers
            .get("upgrade")
            .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
        {
            let key = request
                .headers
                .get("sec-websocket-key")
                .ok_or("missing Sec-WebSocket-Key")?;
            let accept = BASE64.encode(Sha1::digest(
                format!("{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11").as_bytes(),
            ));
            write!(
                tls,
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
            )?;
            tls.flush()?;
            return serve_websocket(tls, input, &request.path, s);
        }

        let (status, body) = handle_http(&request, s)?;
        write_audit(
            s,
            json!({
                "event": "http",
                "method": request.method,
                "path": request.path.split('?').next(),
                "status": status
            }),
        )?;
        let bytes = serde_json::to_vec(&body)?;
        write!(
            tls,
            "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
            bytes.len()
        )?;
        tls.write_all(&bytes)?;
        tls.flush()?;
    }
}

#[derive(Debug)]
pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

pub fn decode_http_request(input: &[u8]) -> Result<Option<(HttpRequest, usize)>> {
    let Some(end) = input.windows(4).position(|x| x == b"\r\n\r\n") else {
        return Ok(None);
    };
    let text = std::str::from_utf8(&input[..end])?;
    let mut lines = text.split("\r\n");
    let mut first = lines.next().ok_or("HTTP first line")?.split(' ');
    let method = first.next().ok_or("HTTP method")?.to_owned();
    let path = first.next().ok_or("HTTP path")?.to_owned();
    if first.next() != Some("HTTP/1.1") || first.next().is_some() {
        return Err("unsupported HTTP version".into());
    }
    let mut headers = BTreeMap::new();
    for line in lines {
        let (k, v) = line.split_once(':').ok_or("malformed HTTP header")?;
        if headers
            .insert(k.to_ascii_lowercase(), v.trim().to_owned())
            .is_some()
        {
            return Err("duplicate HTTP header".into());
        }
    }
    if headers.contains_key("transfer-encoding") {
        return Err("fixture rejects transfer-encoding".into());
    }
    let length = headers
        .get("content-length")
        .map(|s| s.parse::<usize>())
        .transpose()?
        .unwrap_or(0);
    let total = (end + 4)
        .checked_add(length)
        .ok_or("HTTP length overflow")?;
    if total > MAX_REQUEST {
        return Err("HTTP request exceeds capacity".into());
    }
    if input.len() < total {
        return Ok(None);
    }
    Ok(Some((
        HttpRequest {
            method,
            path,
            headers,
            body: input[end + 4..total].to_vec(),
        },
        total,
    )))
}

fn read_request(tls: &mut TlsStream, input: &mut Vec<u8>, s: &State) -> Result<HttpRequest> {
    let deadline = Instant::now() + Duration::from_secs(65);
    loop {
        if let Some((r, consumed)) = decode_http_request(input)? {
            input.drain(..consumed);
            return Ok(r);
        }
        if s.scenario.root.join("STOP").exists() || Instant::now() >= deadline {
            return Err("fixture stopped or HTTP request timed out".into());
        }
        read_more(tls, input)?;
    }
}

fn read_more(tls: &mut TlsStream, input: &mut Vec<u8>) -> Result<()> {
    let mut buf = [0u8; 8192];
    match tls.read(&mut buf) {
        Ok(0) => return Err("peer EOF".into()),
        Ok(n) => {
            if input.len() + n > MAX_REQUEST {
                return Err("TLS input capacity exceeded".into());
            }
            input.extend_from_slice(&buf[..n]);
        }
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::WouldBlock
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::Interrupted
            ) => {}
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

fn verify_signed_http(req: &HttpRequest, s: &State) -> Result<BTreeMap<String, String>> {
    if req.headers.get("x-mbx-apikey") != Some(&s.api_key) {
        return Err("REST API key mismatch".into());
    }
    let query = req.path.split_once('?').map(|(_, q)| q).unwrap_or("");
    let body = std::str::from_utf8(&req.body)?;
    let combined = format!("{query}{body}");
    let (preimage, sig_hex) = combined
        .rsplit_once("&signature=")
        .ok_or("missing REST HMAC signature")?;
    verify_hmac_hex(&s.hmac_secret, preimage.as_bytes(), sig_hex)?;
    let params = parse_query_params(preimage)?;
    let ts = params
        .get("timestamp")
        .ok_or("missing timestamp")?
        .parse::<u64>()?;
    let recv_window = params
        .get("recvWindow")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(5_000);
    if wall_ms().abs_diff(ts) > recv_window.max(5_000) {
        return Err("expired REST timestamp".into());
    }
    Ok(params)
}

fn parse_query_params(text: &str) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for part in text.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = part.split_once('=').ok_or("malformed query parameter")?;
        if out.insert(k.to_owned(), v.to_owned()).is_some() {
            return Err("duplicate query parameter".into());
        }
    }
    Ok(out)
}

pub fn verify_hmac_hex(secret: &[u8], preimage: &[u8], sig_hex: &str) -> Result<()> {
    if sig_hex.len() != 64 {
        return Err("invalid HMAC hex length".into());
    }
    let mut sig_bytes = [0u8; 32];
    for (i, chunk) in sig_hex.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let hex_pair = core::str::from_utf8(chunk)?;
        sig_bytes[i] = u8::from_str_radix(hex_pair, 16).map_err(|_| "non-hex HMAC byte")?;
    }
    hmac::verify(
        &hmac::Key::new(hmac::HMAC_SHA256, secret),
        preimage,
        &sig_bytes,
    )
    .map_err(|_| "HMAC signature mismatch".into())
}

pub fn verify_ed25519_ws_logon(
    frame: &Value,
    expected_api_key: &str,
    ed25519_pub: &[u8],
) -> Result<()> {
    let params = frame["params"]
        .as_object()
        .ok_or("missing WS logon params")?;
    if params.get("apiKey").and_then(Value::as_str) != Some(expected_api_key) {
        return Err("WS logon apiKey mismatch".into());
    }
    let sig_b64 = params
        .get("signature")
        .and_then(Value::as_str)
        .ok_or("missing Ed25519 signature")?;
    let sig_bytes = BASE64
        .decode(sig_b64)
        .map_err(|_| "invalid base64 Ed25519 signature")?;

    let mut sorted = BTreeMap::new();
    for (k, v) in params {
        if k != "signature" {
            let text = match v {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                _ => return Err("unsupported WS logon param value".into()),
            };
            sorted.insert(k.as_str(), text);
        }
    }
    let preimage = sorted
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    UnparsedPublicKey::new(&ED25519, ed25519_pub)
        .verify(preimage.as_bytes(), &sig_bytes)
        .map_err(|_| "Ed25519 session.logon signature verification failed".into())
}

fn handle_http(req: &HttpRequest, s: &State) -> Result<(u16, Value)> {
    let path = req.path.split('?').next().ok_or("missing HTTP path")?;
    if path == "/api/v3/time" && req.method == "GET" {
        return Ok((200, json!({ "serverTime": wall_ms() })));
    }
    if path == "/api/v3/depth" && req.method == "GET" {
        let armed = s.scenario.root.join("ARM").exists();
        let bid = if armed { "60000" } else { "59999" };
        if armed {
            s.first_eligible_quote.get_or_init(Instant::now);
        }
        return Ok((
            200,
            json!({
                "lastUpdateId": s.sequence.load(Ordering::Acquire),
                "bids": [[bid, "1"]],
                "asks": [["60001", "1"]]
            }),
        ));
    }

    let params = verify_signed_http(req, s)?;
    match (req.method.as_str(), path) {
        ("GET", "/api/v3/account") => {
            let filled = s.live_filled.load(Ordering::Acquire);
            let canceled = s.cold_canceled.load(Ordering::Acquire);
            let btc_free = if filled { "1.00100000" } else { "1.00000000" };
            let (usdt_free, usdt_locked) = match (filled, canceled) {
                (false, false) => ("940.00000000", "60.00000000"),
                (true, false) => ("879.94000000", "60.00000000"),
                (false, true) => ("1000.00000000", "0.00000000"),
                (true, true) => ("939.94000000", "0.00000000"),
            };
            Ok((
                200,
                json!({
                    "canTrade": true,
                    "accountType": "SPOT",
                    "balances": [
                        { "asset": "BTC", "free": btc_free, "locked": "0.00000000" },
                        { "asset": "USDT", "free": usdt_free, "locked": usdt_locked }
                    ]
                }),
            ))
        }
        ("GET", "/api/v3/openOrders") => {
            let mut open = Vec::new();
            if !s.cold_canceled.load(Ordering::Acquire) {
                open.push(json!({
                    "symbol": "BTCUSDT",
                    "orderId": 19u64,
                    "clientOrderId": s.cold_cid,
                    "price": "60000.00",
                    "origQty": "0.00100000",
                    "executedQty": "0.00000000",
                    "status": "NEW",
                    "timeInForce": "GTC",
                    "type": "LIMIT",
                    "side": "BUY"
                }));
            }
            write_audit(
                s,
                json!({
                    "event": "cold_bootstrap_served",
                    "open_orders": open.len(),
                    "cold_client_id": s.cold_cid,
                    "cold_order_seqnum": LAB_COLD_OPEN_SEQNUM
                }),
            )?;
            Ok((200, Value::Array(open)))
        }
        ("GET", "/api/v3/order") => {
            let cid = params
                .get("origClientOrderId")
                .map(String::as_str)
                .unwrap_or("");
            if cid == s.cold_cid {
                let status = if s.cold_canceled.load(Ordering::Acquire) {
                    "CANCELED"
                } else {
                    "NEW"
                };
                return Ok((
                    200,
                    json!({
                        "symbol": "BTCUSDT",
                        "orderId": 19u64,
                        "clientOrderId": s.cold_cid,
                        "price": "60000.00",
                        "origQty": "0.00100000",
                        "executedQty": "0.00000000",
                        "cummulativeQuoteQty": "0.00000000",
                        "status": status,
                        "timeInForce": "GTC",
                        "type": "LIMIT",
                        "side": "BUY"
                    }),
                ));
            }
            let live_guard = s.live_cid.lock().map_err(|_| "live_cid lock")?;
            if let Some(live) = live_guard.as_deref()
                && (cid.is_empty() || cid == live)
            {
                let filled = s.live_filled.load(Ordering::Acquire);
                return Ok((
                    200,
                    json!({
                        "symbol": "BTCUSDT",
                        "orderId": 22u64,
                        "clientOrderId": live,
                        "price": "60000.00",
                        "origQty": "0.00100000",
                        "executedQty": if filled { "0.00100000" } else { "0.00000000" },
                        "cummulativeQuoteQty": if filled { "60.00000000" } else { "0.00000000" },
                        "status": if filled { "FILLED" } else { "NEW" },
                        "timeInForce": "GTC",
                        "type": "LIMIT",
                        "side": "BUY"
                    }),
                ));
            }
            Ok((
                400,
                json!({ "code": -2013, "msg": "Order does not exist." }),
            ))
        }
        _ => Ok((
            404,
            json!({ "code": -1, "msg": "unsupported laboratory endpoint" }),
        )),
    }
}

pub fn encode_server_ws_frame(opcode: u8, payload: &[u8]) -> Result<Vec<u8>> {
    if payload.len() > MAX_REQUEST {
        return Err("WS payload exceeds capacity".into());
    }
    let mut out = Vec::with_capacity(4 + payload.len());
    out.push(0x80 | opcode);
    if payload.len() < 126 {
        out.push(payload.len() as u8);
    } else {
        out.push(126);
        out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    }
    out.extend_from_slice(payload);
    Ok(out)
}

pub fn decode_client_ws_frame(input: &[u8]) -> Result<Option<(u8, Vec<u8>, usize)>> {
    if input.len() < 2 {
        return Ok(None);
    }
    if (input[0] & 0xf0) != 0x80 || (input[1] & 0x80) == 0 {
        return Err("fixture requires final masked client WebSocket frame".into());
    }
    let mut payload_len = usize::from(input[1] & 0x7f);
    let mut hdr_len = 2usize;
    if payload_len == 126 {
        if input.len() < 4 {
            return Ok(None);
        }
        payload_len = usize::from(u16::from_be_bytes([input[2], input[3]]));
        hdr_len = 4;
    } else if payload_len == 127 {
        return Err("64-bit WebSocket frames rejected in fixture".into());
    }
    if payload_len > MAX_REQUEST {
        return Err("WebSocket payload exceeds capacity".into());
    }
    let total = hdr_len + 4 + payload_len;
    if input.len() < total {
        return Ok(None);
    }
    let mask = &input[hdr_len..hdr_len + 4];
    let payload = input[hdr_len + 4..total]
        .iter()
        .enumerate()
        .map(|(i, b)| b ^ mask[i % 4])
        .collect();
    Ok(Some((input[0] & 0x0f, payload, total)))
}

fn ws_send_json(tls: &mut TlsStream, value: &Value) -> Result<()> {
    let raw = serde_json::to_vec(value)?;
    tls.write_all(&encode_server_ws_frame(1, &raw)?)?;
    tls.flush()?;
    Ok(())
}

fn serve_websocket(mut tls: TlsStream, mut input: Vec<u8>, path: &str, s: &State) -> Result<()> {
    let private_ws = path.starts_with("/ws-api/v3");
    let mut logged_on = false;
    let mut subscribed = !private_ws && path.starts_with("/ws/");
    let mut sent_fill = false;
    let mut previous_bid = "59999";
    let market_interval = Duration::from_millis(50);
    let mut last_market_push = Instant::now() - Duration::from_secs(1);

    while !s.scenario.root.join("STOP").exists() {
        while let Some((opcode, body, consumed)) = decode_client_ws_frame(&input)? {
            input.drain(..consumed);
            match opcode {
                8 => {
                    tls.write_all(&encode_server_ws_frame(8, &body)?)?;
                    tls.flush()?;
                    return Ok(());
                }
                9 => {
                    tls.write_all(&encode_server_ws_frame(10, &body)?)?;
                    tls.flush()?;
                }
                10 => {}
                1 => {
                    let frame: Value = serde_json::from_slice(&body)?;
                    let id = frame.get("id").ok_or("missing WS id")?.clone();
                    let method = frame["method"].as_str().ok_or("missing WS method")?;
                    if private_ws {
                        match method {
                            "session.logon" => {
                                verify_ed25519_ws_logon(&frame, &s.api_key, &s.ed25519_pub)?;
                                logged_on = true;
                                write_audit(s, json!({ "event": "ws_session_logon_verified" }))?;
                                ws_send_json(
                                    &mut tls,
                                    &json!({
                                        "id": id,
                                        "status": 200,
                                        "result": {
                                            "apiKey": s.api_key,
                                            "authorizedSince": wall_ms(),
                                            "connectedSince": wall_ms(),
                                            "returnRateLimits": true,
                                            "serverTime": wall_ms()
                                        }
                                    }),
                                )?;
                            }
                            "userDataStream.subscribe" => {
                                if !logged_on {
                                    return Err(
                                        "userDataStream.subscribe before session.logon".into()
                                    );
                                }
                                subscribed = true;
                                let marker = s.scenario.root.join("PRIVATE_SUBSCRIBED");
                                if !marker.exists() {
                                    let _ = crate::generate::write_private(&marker, b"ready\n");
                                }
                                ws_send_json(
                                    &mut tls,
                                    &json!({
                                        "id": id,
                                        "status": 200,
                                        "result": { "subscriptionId": 0 }
                                    }),
                                )?;
                            }
                            "order.place" => {
                                if !logged_on {
                                    return Err("order.place before session.logon".into());
                                }
                                let received = Instant::now();
                                let p = frame["params"]
                                    .as_object()
                                    .ok_or("missing order.place params")?;
                                if p.get("symbol").and_then(Value::as_str) != Some("BTCUSDT")
                                    || p.get("side").and_then(Value::as_str) != Some("BUY")
                                    || p.get("type").and_then(Value::as_str) != Some("LIMIT")
                                {
                                    return Err("unexpected order.place parameters".into());
                                }
                                let cid = p
                                    .get("newClientOrderId")
                                    .and_then(Value::as_str)
                                    .ok_or("missing newClientOrderId")?;
                                let tickwire_venue::DecodedClientId::Managed(decoded) =
                                    SelfDescribingClientId::decode_str(
                                        cid,
                                        &spot_client_id_domain(),
                                    )
                                else {
                                    return Err(format!("invalid tw1 client id '{cid}'").into());
                                };
                                if decoded.ors_strat_id() != Some(LAB_STRAT_ID)
                                    || decoded.intent_id != LAB_COLD_OPEN_SEQNUM + 1
                                {
                                    return Err(format!(
                                        "expected strat={} order_seqnum={}, got strat={:?} seq={}",
                                        LAB_STRAT_ID,
                                        LAB_COLD_OPEN_SEQNUM + 1,
                                        decoded.ors_strat_id(),
                                        decoded.intent_id
                                    )
                                    .into());
                                }

                                let elapsed_ns = s
                                    .first_eligible_quote
                                    .get()
                                    .and_then(|t0| received.checked_duration_since(*t0))
                                    .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
                                    .ok_or("order arrived before eligible 60000 quote")?;

                                *s.live_cid.lock().map_err(|_| "live_cid lock")? =
                                    Some(cid.to_owned());
                                *s.live_placed_at.lock().map_err(|_| "live_placed_at lock")? =
                                    Some(received);

                                write_audit(
                                    s,
                                    json!({
                                        "event": "eligible_quote_to_order",
                                        "elapsed_ns": elapsed_ns
                                    }),
                                )?;
                                write_audit(
                                    s,
                                    json!({
                                        "event": "order_accepted",
                                        "client_id": cid,
                                        "order_seqnum": decoded.intent_id,
                                        "venue_order_id": 22u64
                                    }),
                                )?;

                                ws_send_json(
                                    &mut tls,
                                    &json!({
                                        "id": id,
                                        "status": 200,
                                        "result": {
                                            "symbol": "BTCUSDT",
                                            "orderId": 22u64,
                                            "orderListId": -1,
                                            "clientOrderId": cid,
                                            "transactTime": wall_ms(),
                                            "price": "60000.00",
                                            "origQty": "0.00100000",
                                            "executedQty": "0.00000000",
                                            "cummulativeQuoteQty": "0.00000000",
                                            "status": "NEW",
                                            "timeInForce": "GTC",
                                            "type": "LIMIT",
                                            "side": "BUY"
                                        }
                                    }),
                                )?;
                            }
                            "order.cancel" => {
                                if !logged_on {
                                    return Err("order.cancel before session.logon".into());
                                }
                                let orig = frame["params"]["origClientOrderId"]
                                    .as_str()
                                    .ok_or("missing origClientOrderId")?
                                    .to_owned();
                                s.cold_canceled.store(true, Ordering::Release);
                                ws_send_json(
                                    &mut tls,
                                    &json!({
                                        "id": id,
                                        "status": 200,
                                        "result": {
                                            "symbol": "BTCUSDT",
                                            "origClientOrderId": orig,
                                            "orderId": 19u64,
                                            "clientOrderId": "cancel-19",
                                            "status": "CANCELED"
                                        }
                                    }),
                                )?;
                                if subscribed {
                                    ws_send_json(
                                        &mut tls,
                                        &json!({
                                            "subscriptionId": 0,
                                            "event": {
                                                "e": "executionReport",
                                                "E": wall_ms(),
                                                "s": "BTCUSDT",
                                                "c": "cancel-19",
                                                "C": orig,
                                                "S": "BUY",
                                                "o": "LIMIT",
                                                "f": "GTC",
                                                "q": "0.00100000",
                                                "p": "60000.00",
                                                "x": "CANCELED",
                                                "X": "CANCELED",
                                                "i": 19u64,
                                                "l": "0.00000000",
                                                "z": "0.00000000",
                                                "L": "0.00",
                                                "n": "0.00000000",
                                                "N": "USDT",
                                                "T": wall_ms(),
                                                "t": -1,
                                                "m": true
                                            }
                                        }),
                                    )?;
                                }
                                write_audit(
                                    s,
                                    json!({
                                        "event": "cold_order_canceled",
                                        "orig_client_id": orig,
                                        "venue_order_id": 19u64
                                    }),
                                )?;
                            }
                            _ => {
                                return Err(
                                    format!("unsupported private WS method: {method}").into()
                                );
                            }
                        }
                    } else if method == "SUBSCRIBE" {
                        subscribed = true;
                        ws_send_json(&mut tls, &json!({ "id": id, "result": null }))?;
                    } else {
                        return Err(format!("unsupported public WS method: {method}").into());
                    }
                }
                other => return Err(format!("unsupported WS opcode: {other}").into()),
            }
        }

        if private_ws
            && subscribed
            && !sent_fill
            && s.live_placed_at
                .lock()
                .map_err(|_| "live_placed_at lock")?
                .is_some_and(|t| t.elapsed() >= Duration::from_millis(25))
        {
            let cid = s
                .live_cid
                .lock()
                .map_err(|_| "live_cid lock")?
                .clone()
                .ok_or("missing live_cid when emitting fill")?;
            s.live_filled.store(true, Ordering::Release);
            let fill_push = json!({
                "subscriptionId": 0,
                "event": {
                    "e": "executionReport",
                    "E": wall_ms(),
                    "s": "BTCUSDT",
                    "c": cid,
                    "C": "",
                    "S": "BUY",
                    "o": "LIMIT",
                    "f": "GTC",
                    "q": "0.00100000",
                    "p": "60000.00",
                    "x": "TRADE",
                    "X": "FILLED",
                    "i": 22u64,
                    "l": "0.00100000",
                    "z": "0.00100000",
                    "L": "60000.00",
                    "n": "0.06000000",
                    "N": "USDT",
                    "T": wall_ms(),
                    "t": 33u64,
                    "m": true,
                    "Z": "60.00000000",
                    "Y": "60.00000000"
                }
            });
            ws_send_json(&mut tls, &fill_push)?;
            write_audit(
                s,
                json!({
                    "event": "private_fill_sent",
                    "client_id": cid,
                    "trade_id": 33u64,
                    "commission_usdt": "0.06000000"
                }),
            )?;
            sent_fill = true;
        }

        if !private_ws && subscribed && last_market_push.elapsed() >= market_interval {
            let seq = s.sequence.fetch_add(1, Ordering::AcqRel) + 1;
            let armed = s.scenario.root.join("ARM").exists();
            let bid = if armed { "60000" } else { "59999" };
            let mut bids = vec![json!([bid, "1"])];
            if previous_bid != bid {
                bids.push(json!([previous_bid, "0"]));
            }
            if armed {
                s.first_eligible_quote.get_or_init(Instant::now);
            }
            ws_send_json(
                &mut tls,
                &json!({
                    "e": "depthUpdate",
                    "E": wall_ms(),
                    "s": "BTCUSDT",
                    "U": seq,
                    "u": seq,
                    "b": bids,
                    "a": [["60001", "1"]]
                }),
            )?;
            if previous_bid != bid {
                write_audit(
                    s,
                    json!({
                        "event": "market_arm_sent",
                        "sequence": seq,
                        "bid": bid,
                        "wall_ms": wall_ms()
                    }),
                )?;
            }
            previous_bid = bid;
            last_market_push = Instant::now();
        }

        read_more(&mut tls, &mut input)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::rand::SystemRandom;
    use ring::signature::{Ed25519KeyPair, KeyPair};

    #[test]
    fn partial_http_and_duplicate_headers_are_rejected() {
        assert!(
            decode_http_request(b"GET / HTTP/1.1\r\nHost: x\r\n")
                .unwrap()
                .is_none()
        );
        let raw = b"POST /x HTTP/1.1\r\nContent-Length: 3\r\n\r\nabcnext";
        let (req, n) = decode_http_request(raw).unwrap().unwrap();
        assert_eq!(req.body, b"abc");
        assert_eq!(&raw[n..], b"next");
        assert!(
            decode_http_request(
                b"GET / HTTP/1.1\r\nContent-Length: 0\r\nContent-Length: 1\r\n\r\n"
            )
            .is_err()
        );
    }

    #[test]
    fn masked_ws_frame_and_ed25519_session_logon_verify() {
        let masked = [0x81, 0x82, 1, 2, 3, 4, b'o' ^ 1, b'k' ^ 2];
        let (op, body, len) = decode_client_ws_frame(&masked).unwrap().unwrap();
        assert_eq!((op, body, len), (1, b"ok".to_vec(), 8));
        assert!(decode_client_ws_frame(&encode_server_ws_frame(1, b"ok").unwrap()).is_err());

        let rng = SystemRandom::new();
        let pk8 = Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let kp = Ed25519KeyPair::from_pkcs8(pk8.as_ref()).unwrap();
        let preimage = b"apiKey=k1&recvWindow=5000&timestamp=1700000000000";
        let sig = BASE64.encode(kp.sign(preimage).as_ref());
        let logon = json!({
            "id": "twlogon1",
            "method": "session.logon",
            "params": {
                "apiKey": "k1",
                "recvWindow": 5000,
                "timestamp": 1700000000000u64,
                "signature": sig
            }
        });
        verify_ed25519_ws_logon(&logon, "k1", kp.public_key().as_ref()).unwrap();

        let mut tampered = logon;
        tampered["params"]["timestamp"] = json!(1700000000001u64);
        assert!(verify_ed25519_ws_logon(&tampered, "k1", kp.public_key().as_ref()).is_err());
    }
}
