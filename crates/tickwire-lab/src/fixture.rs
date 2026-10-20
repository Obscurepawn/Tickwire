//! Bounded local exchange counterpart. Real clients still use their TLS/auth codecs.
use crate::{Result, Scenario};
use base64::{Engine, engine::general_purpose::STANDARD};
use ring::hmac;
use rustls::{
    ServerConfig, ServerConnection, StreamOwned,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
};
use serde_json::{Value, json};
use sha1::{Digest, Sha1};
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
const MAX_REQUEST: usize = 32768;
type Tls = StreamOwned<ServerConnection, TcpStream>;
struct State {
    scenario: Scenario,
    key: String,
    secret: Vec<u8>,
    sequence: AtomicU64,
    order: Mutex<Option<Value>>,
    output_at: Mutex<Option<Instant>>,
    audit: Mutex<std::fs::File>,
    connections: AtomicUsize,
}
pub fn serve(s: &Scenario) -> Result<()> {
    if !s.fixture_only || !s.listen.ip().is_loopback() {
        return Err("fixture requires loopback".into());
    }
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(fs::read(&s.private_key)?));
    let config = Arc::new(
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()?
            .with_no_client_auth()
            .with_single_cert(vec![CertificateDer::from(fs::read(&s.certificate)?)], key)?,
    );
    let listener = TcpListener::bind(s.listen)?;
    listener.set_nonblocking(true)?;
    let state = Arc::new(State {
        scenario: s.clone(),
        key: String::from_utf8(fs::read(&s.api_key)?)?,
        secret: fs::read(&s.api_secret)?,
        sequence: AtomicU64::new(100),
        order: Mutex::new(None),
        output_at: Mutex::new(None),
        audit: Mutex::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(s.root.join("fixture.jsonl"))?,
        ),
        connections: AtomicUsize::new(0),
    });
    crate::generate::write(&s.root.join("fixture.ready"), b"bound\n")?;
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
                let state = Arc::clone(&state);
                let config = Arc::clone(&config);
                std::thread::spawn(move || {
                    let result = handle(socket, config, &state);
                    if let Err(e) = result {
                        let _ = audit(
                            &state,
                            json!({"event":"connection_closed","error":e.to_string()}),
                        );
                    }
                    state.connections.fetch_sub(1, Ordering::AcqRel);
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1))
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
fn audit(s: &State, value: Value) -> Result<()> {
    let mut f = s.audit.lock().map_err(|_| "audit lock")?;
    if f.metadata()?.len() > 16 * 1024 * 1024 {
        return Err("fixture audit capacity".into());
    }
    serde_json::to_writer(&mut *f, &value)?;
    f.write_all(b"\n")?;
    f.flush()?;
    Ok(())
}
fn wall() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("wall epoch")
        .as_millis() as u64
}
fn handle(socket: TcpStream, config: Arc<ServerConfig>, s: &State) -> Result<()> {
    socket.set_read_timeout(Some(Duration::from_millis(100)))?;
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
                .ok_or("websocket key")?;
            let accept = STANDARD.encode(Sha1::digest(
                format!("{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11").as_bytes(),
            ));
            write!(
                tls,
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
            )?;
            tls.flush()?;
            return websocket(tls, input, &request.path, s);
        }
        let (status, body) = http(&request, s)?;
        audit(
            s,
            json!({"event":"http","method":request.method,"path":request.path.split('?').next(),"status":status}),
        )?;
        let bytes = serde_json::to_vec(&body)?;
        write!(
            tls,
            "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
            bytes.len()
        )?;
        tls.write_all(&bytes)?;
        tls.flush()?;
        if request.method == "POST"
            && request.path.split('?').next() == Some("/api/v3/order")
            && status == 200
        {
            *s.output_at.lock().map_err(|_| "output lock")? = Some(Instant::now());
        }
    }
}
#[derive(Debug)]
struct Request {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}
fn decode_request(input: &[u8]) -> Result<Option<(Request, usize)>> {
    let Some(end) = input.windows(4).position(|x| x == b"\r\n\r\n") else {
        return Ok(None);
    };
    let text = std::str::from_utf8(&input[..end])?;
    let mut lines = text.split("\r\n");
    let mut first = lines.next().ok_or("HTTP first line")?.split(' ');
    let method = first.next().ok_or("method")?.to_owned();
    let path = first.next().ok_or("path")?.to_owned();
    if first.next() != Some("HTTP/1.1") || first.next().is_some() {
        return Err("HTTP version".into());
    }
    let mut headers = BTreeMap::new();
    for line in lines {
        let (k, v) = line.split_once(':').ok_or("header")?;
        if headers
            .insert(k.to_ascii_lowercase(), v.trim().to_owned())
            .is_some()
        {
            return Err("duplicate header".into());
        }
    }
    if headers.contains_key("transfer-encoding") {
        return Err("fixture rejects transfer encoding".into());
    }
    let length = headers
        .get("content-length")
        .map(|s| s.parse::<usize>())
        .transpose()?
        .unwrap_or(0);
    let total = (end + 4).checked_add(length).ok_or("HTTP bounds")?;
    if total > MAX_REQUEST {
        return Err("HTTP capacity".into());
    }
    if input.len() < total {
        return Ok(None);
    }
    Ok(Some((
        Request {
            method,
            path,
            headers,
            body: input[end + 4..total].to_vec(),
        },
        total,
    )))
}
fn read_request(tls: &mut Tls, input: &mut Vec<u8>, s: &State) -> Result<Request> {
    let end = Instant::now() + Duration::from_secs(65);
    loop {
        if let Some((r, n)) = decode_request(input)? {
            input.drain(..n);
            return Ok(r);
        }
        if s.scenario.root.join("STOP").exists() || Instant::now() >= end {
            return Err("fixture stopped or request timeout".into());
        }
        read_more(tls, input)?;
    }
}
fn read_more(tls: &mut Tls, input: &mut Vec<u8>) -> Result<()> {
    let mut b = [0; 8192];
    match tls.read(&mut b) {
        Ok(0) => return Err("peer EOF".into()),
        Ok(n) => {
            if input.len() + n > MAX_REQUEST {
                return Err("input capacity".into());
            }
            input.extend_from_slice(&b[..n]);
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
fn signed(request: &Request, s: &State) -> Result<BTreeMap<String, String>> {
    if request.headers.get("x-mbx-apikey") != Some(&s.key) {
        return Err("API key mismatch".into());
    }
    let query = request.path.split_once('?').map(|(_, q)| q).unwrap_or("");
    let body = std::str::from_utf8(&request.body)?;
    let text = format!("{query}{body}");
    let (preimage, signature) = text.rsplit_once("&signature=").ok_or("signature missing")?;
    verify_signature(&s.secret, preimage.as_bytes(), signature)?;
    let params = parse_params(preimage)?;
    let timestamp = params.get("timestamp").ok_or("timestamp")?.parse::<u64>()?;
    if wall().abs_diff(timestamp) > 5000 {
        return Err("expired signed request".into());
    }
    Ok(params)
}
fn parse_params(text: &str) -> Result<BTreeMap<String, String>> {
    let mut p = BTreeMap::new();
    for part in text.split('&') {
        let (k, v) = part.split_once('=').ok_or("parameter")?;
        if p.insert(k.into(), v.into()).is_some() {
            return Err("duplicate parameter".into());
        }
    }
    Ok(p)
}
fn verify_signature(secret: &[u8], preimage: &[u8], sig: &str) -> Result<()> {
    if sig.len() != 64 {
        return Err("signature length".into());
    }
    let mut bytes = [0u8; 32];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = u8::from_str_radix(&sig[2 * i..2 * i + 2], 16).map_err(|_| "signature encoding")?;
    }
    hmac::verify(&hmac::Key::new(hmac::HMAC_SHA256, secret), preimage, &bytes)
        .map_err(|_| "signature mismatch".into())
}
fn order_value(client: &str) -> Value {
    json!({"symbol":"BTCUSDT","orderId":22,"orderListId":-1,"clientOrderId":client,"transactTime":wall(),"price":"60000","origQty":"0.001","executedQty":"0.001","cummulativeQuoteQty":"60","status":"FILLED","timeInForce":"GTC","type":"LIMIT","side":"BUY","workingTime":wall(),"selfTradePreventionMode":"NONE"})
}
fn http(r: &Request, s: &State) -> Result<(u16, Value)> {
    let path = r.path.split('?').next().ok_or("path")?;
    if path == "/api/v3/time" && r.method == "GET" {
        return Ok((200, json!({"serverTime":wall()})));
    }
    if path == "/api/v3/depth" && r.method == "GET" {
        let bid = if s.scenario.root.join("ARM").exists() {
            "60000"
        } else {
            "59999"
        };
        return Ok((
            200,
            json!({"lastUpdateId":s.sequence.load(Ordering::Acquire),"bids":[[bid,"1"]],"asks":[["60001","1"]]}),
        ));
    }
    let p = signed(r, s)?;
    match (r.method.as_str(), path) {
        ("GET", "/api/v3/account") => {
            let order = s.order.lock().map_err(|_| "order lock")?;
            let filled = order.as_ref().is_some_and(|o| o["status"] == "FILLED");
            Ok((
                200,
                json!({"canTrade":true,"accountType":"SPOT","balances":[{"asset":"BTC","free":if filled{"1.001"}else{"1"},"locked":"0"},{"asset":"USDT","free":if order.is_some(){"940"}else{"1000"},"locked":if order.is_some()&&!filled{"60"}else{"0"}}]}),
            ))
        }
        ("GET", "/api/v3/openOrders") => {
            let order = s.order.lock().map_err(|_| "order lock")?;
            Ok((
                200,
                match order.as_ref() {
                    Some(o) if o["status"] == "NEW" => json!([o]),
                    _ => json!([]),
                },
            ))
        }
        ("GET", "/api/v3/openOrderList") => Ok((200, json!([]))),
        ("POST", "/api/v3/order") => {
            for (k, v) in [
                ("symbol", "BTCUSDT"),
                ("side", "BUY"),
                ("type", "LIMIT"),
                ("quantity", "0.001"),
                ("price", "60000"),
                ("timeInForce", "GTC"),
            ] {
                if p.get(k).map(String::as_str) != Some(v) {
                    return Err(format!("fixture only supports exact {k}={v}").into());
                }
            }
            let client = p.get("newClientOrderId").ok_or("client id")?;
            if client != "lab-spot-1" {
                return Err("unexpected client id".into());
            }
            let mut order = s.order.lock().map_err(|_| "order lock")?;
            if order.is_some() {
                return Ok((400, json!({"code":-2010,"msg":"Duplicate order sent."})));
            }
            let mut value = order_value(client);
            value["status"] = json!("NEW");
            value["executedQty"] = json!("0");
            value["cummulativeQuoteQty"] = json!("0");
            *order = Some(value.clone());
            audit(
                s,
                json!({"event":"order_accepted","client_id":client,"venue_order_id":22}),
            )?;
            if p.get("newOrderRespType").is_some_and(|v| v == "ACK") {
                Ok((
                    200,
                    json!({"symbol":"BTCUSDT","orderId":22,"orderListId":-1,"clientOrderId":client,"transactTime":wall()}),
                ))
            } else {
                let mut reply = value;
                reply["status"] = json!("NEW");
                reply["executedQty"] = json!("0");
                reply["cummulativeQuoteQty"] = json!("0");
                reply["fills"] = json!([]);
                Ok((200, reply))
            }
        }
        ("GET", "/api/v3/order") => {
            let order = s.order.lock().map_err(|_| "order lock")?;
            match order.as_ref() {
                Some(o) => Ok((200, o.clone())),
                None => Ok((400, json!({"code":-2013,"msg":"Order does not exist."}))),
            }
        }
        ("GET", "/api/v3/myTrades") => {
            let order = s.order.lock().map_err(|_| "order lock")?;
            let filled = order.as_ref().is_some_and(|o| o["status"] == "FILLED");
            let from = p
                .get("fromId")
                .map(|v| v.parse::<u64>())
                .transpose()?
                .unwrap_or(0);
            Ok((
                200,
                if filled && from <= 33 {
                    json!([{"symbol":"BTCUSDT","id":33,"orderId":22,"price":"60000","qty":"0.001","quoteQty":"60","commission":"0","commissionAsset":"USDT","isBuyer":true,"isMaker":true,"time":wall()}])
                } else {
                    json!([])
                },
            ))
        }
        _ => Ok((
            404,
            json!({"code":-1,"msg":"unsupported laboratory endpoint"}),
        )),
    }
}
fn frame(op: u8, bytes: &[u8]) -> Result<Vec<u8>> {
    if bytes.len() > MAX_REQUEST {
        return Err("WS size".into());
    }
    let mut out = vec![0x80 | op];
    if bytes.len() < 126 {
        out.push(bytes.len() as u8)
    } else {
        out.push(126);
        out.extend((bytes.len() as u16).to_be_bytes());
    }
    out.extend_from_slice(bytes);
    Ok(out)
}
fn decode_frame(input: &[u8]) -> Result<Option<(u8, Vec<u8>, usize)>> {
    if input.len() < 2 {
        return Ok(None);
    }
    if input[0] & 0xf0 != 0x80 || input[1] & 0x80 == 0 {
        return Err("fixture requires final masked websocket frame".into());
    }
    let mut n = (input[1] & 127) as usize;
    let mut offset = 2;
    if n == 126 {
        if input.len() < 4 {
            return Ok(None);
        }
        n = u16::from_be_bytes([input[2], input[3]]) as usize;
        offset = 4;
    } else if n == 127 {
        return Err("WS frame too large".into());
    }
    if n > MAX_REQUEST {
        return Err("WS payload capacity".into());
    }
    if input.len() < offset + 4 + n {
        return Ok(None);
    }
    let mask = &input[offset..offset + 4];
    let bytes = input[offset + 4..offset + 4 + n]
        .iter()
        .enumerate()
        .map(|(i, b)| b ^ mask[i % 4])
        .collect();
    Ok(Some((input[0] & 15, bytes, offset + 4 + n)))
}
fn ws_send(tls: &mut Tls, value: &Value) -> Result<()> {
    tls.write_all(&frame(1, &serde_json::to_vec(value)?)?)?;
    tls.flush()?;
    Ok(())
}
fn websocket(mut tls: Tls, mut input: Vec<u8>, path: &str, s: &State) -> Result<()> {
    let private = path.starts_with("/ws-api/v3");
    let mut subscribed = !private && path.starts_with("/ws/");
    let mut sent_fill = false;
    let mut previous_bid = "59999";
    let mut last = Instant::now() - Duration::from_secs(1);
    while !s.scenario.root.join("STOP").exists() {
        while let Some((opcode, body, n)) = decode_frame(&input)? {
            input.drain(..n);
            match opcode {
                8 => {
                    tls.write_all(&frame(8, &body)?)?;
                    tls.flush()?;
                    return Ok(());
                }
                9 => {
                    tls.write_all(&frame(10, &body)?)?;
                    tls.flush()?;
                }
                10 => {}
                1 => {
                    let value: Value = serde_json::from_slice(&body)?;
                    let id = value.get("id").ok_or("WS id")?.clone();
                    let method = value["method"].as_str().ok_or("WS method")?;
                    if private {
                        let result = match method {
                            "userDataStream.subscribe.signature" => {
                                verify_ws(&value, &s.key, &s.secret)?;
                                subscribed = true;
                                json!({"subscriptionId":0})
                            }
                            "session.status" => json!({"apiKey":null,"userDataStream":subscribed}),
                            "session.subscriptions" => json!([{"subscriptionId":0}]),
                            "userDataStream.unsubscribe" => {
                                subscribed = false;
                                json!({})
                            }
                            _ => return Err("unsupported private WS method".into()),
                        };
                        ws_send(&mut tls, &json!({"id":id,"status":200,"result":result}))?;
                    } else if method == "SUBSCRIBE" {
                        subscribed = true;
                        ws_send(&mut tls, &json!({"id":id,"result":null}))?;
                    } else {
                        return Err("unsupported public method".into());
                    }
                }
                _ => return Err("websocket opcode".into()),
            }
        }
        if subscribed
            && private
            && !sent_fill
            && (s.scenario.kind != crate::ScenarioKind::SpotHandover
                || s.scenario.root.join("RELEASE_FILL").is_file())
            && s.output_at
                .lock()
                .map_err(|_| "output lock")?
                .is_some_and(|t| t.elapsed() >= Duration::from_millis(100))
        {
            {
                let mut order = s.order.lock().map_err(|_| "order lock")?;
                let current = order.as_mut().ok_or("fill without order")?;
                current["status"] = json!("FILLED");
                current["executedQty"] = json!("0.001");
                current["cummulativeQuoteQty"] = json!("60");
            }
            let event = json!({"subscriptionId":0,"event":{"e":"executionReport","E":wall(),"s":"BTCUSDT","c":"lab-spot-1","S":"BUY","o":"LIMIT","f":"GTC","q":"0.001","p":"60000","g":-1,"C":"","x":"TRADE","X":"FILLED","i":22,"l":"0.001","z":"0.001","L":"60000","n":"0","N":"USDT","T":wall(),"t":33,"I":44,"m":true,"Z":"60","Y":"60","r":"NONE"}});
            ws_send(&mut tls, &event)?;
            audit(s, json!({"event":"private_fill_sent","trade_id":33}))?;
            sent_fill = true;
        }
        if subscribed && !private && last.elapsed() >= Duration::from_millis(100) {
            let seq = s.sequence.fetch_add(1, Ordering::AcqRel) + 1;
            let bid = if s.scenario.root.join("ARM").exists() {
                "60000"
            } else {
                "59999"
            };
            let mut bids = vec![json!([bid, "1"])];
            if previous_bid != bid {
                bids.push(json!([previous_bid, "0"]));
            }
            ws_send(
                &mut tls,
                &json!({"e":"depthUpdate","E":wall(),"s":"BTCUSDT","U":seq,"u":seq,"b":bids,"a":[["60001","1"]]}),
            )?;
            previous_bid = bid;
            last = Instant::now();
        }
        read_more(&mut tls, &mut input)?;
    }
    Ok(())
}
fn verify_ws(value: &Value, key: &str, secret: &[u8]) -> Result<()> {
    let params = value["params"].as_object().ok_or("WS params")?;
    if params.get("apiKey").and_then(Value::as_str) != Some(key) {
        return Err("WS API key mismatch".into());
    }
    let sig = params
        .get("signature")
        .and_then(Value::as_str)
        .ok_or("WS signature")?;
    let mut sorted = BTreeMap::new();
    for (k, v) in params {
        if k != "signature" {
            let v = match v {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                _ => return Err("WS param type".into()),
            };
            sorted.insert(k, v);
        }
    }
    let text = sorted
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    verify_signature(secret, text.as_bytes(), sig)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn partial_http_and_smuggling_rejected() {
        assert!(
            decode_request(b"GET / HTTP/1.1\r\nHost: x\r\n")
                .unwrap()
                .is_none()
        );
        let r = b"POST /x HTTP/1.1\r\nContent-Length: 3\r\n\r\nabcnext";
        let (q, n) = decode_request(r).unwrap().unwrap();
        assert_eq!(q.body, b"abc");
        assert_eq!(&r[n..], b"next");
        assert!(
            decode_request(b"GET / HTTP/1.1\r\nContent-Length: 0\r\nContent-Length: 1\r\n\r\n")
                .is_err()
        );
    }
    #[test]
    fn masked_frame_and_unmasked_rejection() {
        let input = [0x81, 0x82, 1, 2, 3, 4, b'h' ^ 1, b'i' ^ 2];
        let (op, b, n) = decode_frame(&input).unwrap().unwrap();
        assert_eq!((op, b, n), (1, b"hi".to_vec(), 8));
        assert!(decode_frame(&frame(1, b"hi").unwrap()).is_err());
    }
    #[test]
    fn websocket_signature_covers_received_numeric_representation() {
        let secret = b"test";
        let p = b"apiKey=k&recvWindow=5000.12&timestamp=123";
        let signature = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, secret), p)
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let mut v = json!({"params":{"timestamp":123,"apiKey":"k","recvWindow":5000.12,"signature":signature}});
        verify_ws(&v, "k", secret).unwrap();
        v["params"]["timestamp"] = json!(124);
        assert!(verify_ws(&v, "k", secret).is_err());
    }
}
