mod common;
use base64::{Engine, engine::general_purpose::STANDARD};
use common::*;
use sha1::{Digest, Sha1};
use std::{
    io::Write,
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};
use tickwire_book::{BookKey, PriceTicks, QuantityKnowledge, Side};
fn certs_sbe() -> (Arc<rustls::ClientConfig>, Arc<rustls::ServerConfig>) {
    let c = rcgen::generate_simple_self_signed(vec![
        "stream-sbe.binance.com".into(),
        "stream.binance.com".into(),
        "api.binance.com".into(),
    ])
    .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(c.cert.der().clone()).unwrap();
    let client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![c.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(c.signing_key.serialize_der()).into(),
        )
        .unwrap();
    (Arc::new(client), Arc::new(server))
}
fn literal() -> Vec<u8> {
    include_str!("../../tickwire-binance-sbe/tests/fixtures/diff.hex")
        .trim()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
        .collect()
}
#[allow(dead_code)] // Also consumed by the MD mapped-publication integration test.
pub fn instrument() -> tickwire_venue::InstrumentBinding {
    config::<BinanceSpot>(Source::SpotProduction, SubscribeMode::RawUrl)
        .depth
        .instrument
}
pub fn run_with(mode: SubscribeMode, callback: impl FnMut(&Notice<'_>)) {
    let cfg = config::<BinanceSpot>(Source::SpotProduction, mode);
    run_with_binding(
        mode,
        cfg.depth.instrument.instrument_id,
        cfg.depth.key,
        callback,
    );
}
pub fn run_with_binding(
    mode: SubscribeMode,
    instrument_id: u64,
    book_key: BookKey,
    callback: impl FnMut(&Notice<'_>),
) {
    struct Checked<F> {
        callback: F,
        ordinary: Sink,
    }
    impl<F: FnMut(&Notice<'_>)> PublicSink for Checked<F> {
        fn on_notice(&mut self, n: Notice<'_>) -> bool {
            if let Notice::Sync {
                encoding,
                raw_delta: Some(raw),
                ..
            } = &n
            {
                assert_eq!(*encoding, DepthEncoding::SpotSbeV1);
                assert_eq!(&raw[..8], &literal()[..8]);
            }
            (self.callback)(&n);
            self.ordinary.on_notice(n)
        }
    }
    let (tls, server_tls) = certs_sbe();
    let ws = listener();
    let http = listener();
    let network = NetworkConfig {
        stream_address: ws.local_addr().unwrap(),
        snapshot_address: http.local_addr().unwrap(),
        tls,
    };
    let (done, wait) = mpsc::channel();
    let (next, continuation) = mpsc::channel();
    let server = thread::spawn(move || {
        let mut s = accept(&ws, server_tls.clone());
        let h = headers(&mut s);
        let path = if mode == SubscribeMode::RawUrl {
            "/ws/btcusdt@depth"
        } else {
            "/ws"
        };
        assert!(h.starts_with(&format!("GET {path} HTTP/1.1\r\n")), "{h}");
        assert!(
            h.lines().any(|l| l == "Host: stream-sbe.binance.com"),
            "{h}"
        );
        assert!(h.lines().any(|l| l == "X-MBX-APIKEY: sbeKey"), "{h}");
        assert_eq!(s.conn.server_name(), Some("stream-sbe.binance.com"));
        let key = h
            .lines()
            .find_map(|l| {
                l.split_once(':')
                    .filter(|(k, _)| k.eq_ignore_ascii_case("sec-websocket-key"))
                    .map(|(_, v)| v.trim())
            })
            .unwrap();
        let accept = STANDARD.encode(Sha1::digest(format!(
            "{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
        )));
        write!(s,"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").unwrap();
        s.flush().unwrap();
        if mode == SubscribeMode::Explicit {
            let request: serde_json::Value = serde_json::from_slice(&client_frame(&mut s)).unwrap();
            assert_eq!(request["params"], serde_json::json!(["btcusdt@depth"]));
            // Binary input can arrive before its matching Text ACK; it must remain buffered.
            frame(&mut s, 2, &literal());
            frame(
                &mut s,
                1,
                &serde_json::to_vec(&serde_json::json!({"id":request["id"],"result":null}))
                    .unwrap(),
            );
        } else {
            frame(&mut s, 2, &literal());
        }
        let mut rest = common::accept(&http, server_tls);
        let h = headers(&mut rest);
        assert!(h.starts_with("GET /api/v3/depth?symbol=BTCUSDT&limit=5 HTTP/1.1\r\n"));
        assert!(h.lines().any(|l| l == "Host: api.binance.com"));
        assert!(!h.contains("sbeKey"));
        reply(&mut rest,br#"{"lastUpdateId":43,"bids":[["65000","0.1"],["64999","0.2"]],"asks":[["65001","0.3"]]}"#);
        continuation.recv_timeout(Duration::from_secs(5)).unwrap();
        let mut next = literal()[..54].to_vec(); // Fixed root + one zero-quantity bid.
        next[8..16].copy_from_slice(&2_000_000i64.to_le_bytes());
        next[16..24].copy_from_slice(&45i64.to_le_bytes());
        next[24..32].copy_from_slice(&45i64.to_le_bytes());
        next.extend(16u16.to_le_bytes());
        next.extend(14u16.to_le_bytes());
        for i in 0..14 {
            next.extend((6500100i64 + i * 100).to_le_bytes());
            next.extend(600i64.to_le_bytes());
        }
        next.push(7);
        next.extend(b"BTCUSDT");
        frame(&mut s, 2, &next);
        wait.recv_timeout(Duration::from_secs(5)).unwrap();
    });
    let mut cfg = config::<BinanceSpot>(Source::SpotProduction, mode);
    cfg.depth.instrument.instrument_id = instrument_id;
    cfg.depth.key = book_key;
    cfg.depth.time_unit = TimeUnit::Microseconds;
    let mut client = PublicDepthClient::<BinanceSpot, _, 32, 32, 8>::new_sbe(
        cfg,
        network,
        Clock(Instant::now()),
        SbeAccess::ed25519_api_key("sbeKey".into()).unwrap(),
    )
    .unwrap();
    let mut permit = Permit::default();
    let mut sink = Checked {
        callback,
        ordinary: Sink::default(),
    };
    client.start(&mut permit).unwrap();
    let end = Instant::now() + Duration::from_secs(3);
    let mut signalled = false;
    while client.status().last_update_id != Some(45) && Instant::now() < end {
        if !signalled && client.status().depth == DepthState::Live {
            next.send(()).unwrap();
            signalled = true;
        }
        client
            .poll(&mut permit, PollBudget::default(), &mut sink)
            .unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        client.status().depth,
        DepthState::Live,
        "{:#?}",
        sink.ordinary.gaps
    );
    assert_eq!(client.status().last_update_id, Some(45));
    assert_eq!(client.view(NS).unwrap().key(), book_key);
    assert_eq!(
        client
            .view(NS)
            .unwrap()
            .quantity(Side::Ask, PriceTicks(6500100)),
        QuantityKnowledge::Known(tickwire_book::QtyLots(60000))
    );
    assert!(permit.charges.iter().any(|c| c.kind == ChargeKind::Connect));
    assert!(permit.charges.iter().any(|c| c.kind == ChargeKind::Upgrade));
    assert_eq!(permit.feedback, vec![200]);
    done.send(()).unwrap();
    server.join().unwrap();
}
#[test]
fn raw_sbe_binary_wss_and_rest_snapshot_share_typed_depth_owner() {
    run_with(SubscribeMode::RawUrl, |_| {});
}
#[test]
fn explicit_sbe_buffers_binary_before_text_subscription_ack() {
    run_with(SubscribeMode::Explicit, |_| {});
}
