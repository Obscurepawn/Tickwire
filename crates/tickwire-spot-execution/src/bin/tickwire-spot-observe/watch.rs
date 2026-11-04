//! Bounded, server-authenticated private WebSocket sampling for one credential and UID.
use super::*;
use serde_json::json;
use tickwire_spot_execution::private::{PrivateFeed, Reaction};
use tickwire_transport::ws::{ClientHandshake, Decoder, Opcode, PollMessage, encode_client};
const LIMIT: usize = 1024 * 1024;
fn write_frame(stream: &mut impl Write, op: Opcode, b: &[u8]) -> Result<()> {
    stream.write_all(&encode_client(op, b, LIMIT)?)?;
    stream.flush()?;
    Ok(())
}
fn poll_read(stream: &mut impl Read, buf: &mut [u8]) -> Result<usize> {
    match stream.read(buf) {
        Ok(0) => Err("private WebSocket peer closed".into()),
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
            ) =>
        {
            Ok(0)
        }
        v => Ok(v?),
    }
}
pub(super) fn capture(
    mut stream: impl Read + Write,
    host: &str,
    path: &str,
    credentials: &Credentials<'_>,
    stamp: impl Fn() -> Option<u64>,
    span: Duration,
    mut emit: impl FnMut(Value) -> Result<()>,
) -> Result<Value> {
    let handshake = ClientHandshake::new(host, path, &[], 4096)?;
    stream.write_all(handshake.request())?;
    stream.flush()?;
    let setup = Instant::now() + Duration::from_secs(10);
    let mut buf = [0; 8192];
    let mut http = ResponseDecoder::new(8192, 64)?;
    let mut ws = Decoder::new(LIMIT, LIMIT)?;
    loop {
        if Instant::now() >= setup {
            return Err("private WebSocket upgrade timeout".into());
        }
        let n = poll_read(&mut stream, &mut buf)?;
        http.push(&buf[..n])?;
        if let Some(response) = http.next_response()? {
            if response.status != 101 {
                return Err(format!("private WebSocket upgrade HTTP {}", response.status).into());
            }
            handshake.validate(&response)?;
            ws.push(&http.take_buffered())?;
            break;
        }
    }
    let mut state = PrivateFeed::new(credentials, stamp().ok_or("exchange timestamp overflow")?)?;
    let bybit = state.login()["op"] == "auth";
    write_frame(
        &mut stream,
        Opcode::Text,
        &serde_json::to_vec(state.login())?,
    )?;
    let mut sampled = None;
    let mut heartbeat = Instant::now();
    let mut last_frame = Instant::now();
    let mut message = 0u64;
    loop {
        let now = Instant::now();
        if sampled.is_some_and(|t: Instant| now.duration_since(t) >= span) {
            break;
        }
        if sampled.is_none() && now >= setup {
            return Err("private WebSocket login or subscription timed out".into());
        }
        if now.duration_since(last_frame) > Duration::from_secs(25) {
            return Err("private WebSocket became silent".into());
        }
        if sampled.is_some() && now.duration_since(heartbeat) >= Duration::from_secs(10) {
            write_frame(
                &mut stream,
                Opcode::Text,
                if bybit { br#"{"op":"ping"}"# } else { b"ping" },
            )?;
            heartbeat = now;
        }
        match ws.poll_message()? {
            PollMessage::NeedInput => {
                let n = poll_read(&mut stream, &mut buf)?;
                ws.push(&buf[..n])?;
            }
            PollMessage::Yielded => continue,
            PollMessage::Message(m) => {
                last_frame = now;
                match m.opcode {
                    Opcode::Text => match state.on_text(&m.payload)? {
                        Reaction::Subscribe(q) => {
                            write_frame(&mut stream, Opcode::Text, &serde_json::to_vec(&q)?)?
                        }
                        Reaction::Control => {}
                        Reaction::Data(d) => {
                            message += 1;
                            emit(
                                json!({"kind":"private_data","session_message":message,"channel":d.channel,"rows":d.rows}),
                            )?;
                        }
                    },
                    Opcode::Ping => write_frame(&mut stream, Opcode::Pong, &m.payload)?,
                    Opcode::Pong => {}
                    _ => return Err("private WebSocket ended or sent unexpected data".into()),
                }
                if sampled.is_none() && state.subscribed() {
                    sampled = Some(now);
                    emit(
                        json!({"kind":"subscribed","coverage":"current_connection_only","fresh_risk_qualified":false}),
                    )?;
                }
            }
        }
    }
    write_frame(&mut stream, Opcode::Close, &1000u16.to_be_bytes())?;
    Ok(
        json!({"success":true,"query":"watch","coverage":"current_connection_only","fresh_risk_qualified":false,"received_messages":message}),
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use ring::digest::{SHA1_FOR_LEGACY_USE_ONLY, digest};
    use std::{io::BufRead, net::TcpListener};
    fn read_frame(stream: &mut impl Read) -> Value {
        let mut h = [0; 2];
        stream.read_exact(&mut h).unwrap();
        assert_eq!(h[0], 0x81);
        assert!(h[1] & 0x80 != 0);
        let mut n = (h[1] & 0x7f) as usize;
        if n == 126 {
            stream.read_exact(&mut h).unwrap();
            n = u16::from_be_bytes(h) as usize;
        }
        let mut mask = [0; 4];
        stream.read_exact(&mut mask).unwrap();
        let mut b = vec![0; n];
        stream.read_exact(&mut b).unwrap();
        for (i, c) in b.iter_mut().enumerate() {
            *c ^= mask[i % 4];
        }
        serde_json::from_slice(&b).unwrap()
    }
    fn server_frame(s: &mut impl Write, v: Value) {
        let b = serde_json::to_vec(&v).unwrap();
        let mut h = vec![0x81];
        if b.len() < 126 {
            h.push(b.len() as u8)
        } else {
            h.push(126);
            h.extend((b.len() as u16).to_be_bytes())
        };
        s.write_all(&h).unwrap();
        s.write_all(&b).unwrap();
        s.flush().unwrap();
    }
    #[test]
    fn loopback_tls_confirms_private_login_and_subscriptions_and_delivers_only_selected_order_fields()
     {
        let (host, path) =
            tickwire_spot_execution::private::endpoint(SpotVenue::Okx, Environment::Test);
        let cert = rcgen::generate_simple_self_signed(vec![host.into()]).unwrap();
        let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
        let server = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.cert.der().clone()], key.into())
            .unwrap();
        let mut store = rustls::RootCertStore::empty();
        store.add(cert.cert.der().clone()).unwrap();
        let client = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(store)
                .with_no_client_auth(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let job = std::thread::spawn(move || {
            let (sock, _) = listener.accept().unwrap();
            sock.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            let mut stream = BufReader::new(rustls::StreamOwned::new(
                rustls::ServerConnection::new(Arc::new(server)).unwrap(),
                sock,
            ));
            let mut head = String::new();
            loop {
                assert!(stream.read_line(&mut head).unwrap() > 0);
                if head.ends_with("\r\n\r\n") {
                    break;
                }
            }
            assert!(head.starts_with("GET /ws/v5/private "));
            let k = head
                .lines()
                .find_map(|v| v.strip_prefix("Sec-WebSocket-Key: "))
                .unwrap();
            let accept = STANDARD.encode(
                digest(
                    &SHA1_FOR_LEGACY_USE_ONLY,
                    format!("{k}258EAFA5-E914-47DA-95CA-C5AB0DC85B11").as_bytes(),
                )
                .as_ref(),
            );
            write!(stream.get_mut(),"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").unwrap();
            let login = read_frame(&mut stream);
            assert_eq!(login["op"], "login");
            assert!(!login["args"][0]["sign"].as_str().unwrap().is_empty());
            server_frame(
                stream.get_mut(),
                json!({"event":"login","code":"0","connId":"c"}),
            );
            let sub = read_frame(&mut stream);
            assert_eq!(sub["op"], "subscribe");
            for arg in sub["args"].as_array().unwrap() {
                server_frame(
                    stream.get_mut(),
                    json!({"event":"subscribe","id":"twaccount","connId":"c","arg":arg}),
                );
            }
            server_frame(
                stream.get_mut(),
                json!({"arg":{"channel":"orders","instType":"SPOT"},"data":[{"instType":"SPOT","instId":"BTC-USDT","ordId":"42","tradeId":"17","fillFee":"-0.1","sensitive":"never-print"}]}),
            );
            let mut close = [0; 2];
            stream.read_exact(&mut close).unwrap();
            assert_eq!(close[0], 0x88);
        });
        let stream = tls_at(host, [addr], client).unwrap();
        stream
            .sock
            .set_read_timeout(Some(Duration::from_millis(10)))
            .unwrap();
        let mut out = vec![];
        let summary = capture(
            stream,
            host,
            path,
            &Credentials::okx("public", b"secret", "pass"),
            || Some(1700000000123),
            Duration::from_millis(150),
            |v| {
                out.push(v);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(summary["received_messages"], 1);
        assert_eq!(out[0]["kind"], "subscribed");
        assert_eq!(out[1]["rows"][0]["ordId"], "42");
        assert!(!serde_json::to_string(&out).unwrap().contains("never-print"));
        job.join().unwrap();
    }
}
