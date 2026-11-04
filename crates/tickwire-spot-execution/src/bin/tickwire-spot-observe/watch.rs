pub(super) use tickwire_spot_execution::private::sample as capture;
#[cfg(test)]
mod tests {
    use super::capture;
    use crate::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use ring::digest::{SHA1_FOR_LEGACY_USE_ONLY, digest};
    use serde_json::json;
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
