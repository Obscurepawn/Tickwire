use serde_json::json;
use tickwire_spot_execution::{
    Credentials,
    private::{PrivateFeed, Reaction},
};
#[test]
fn login_and_channel_acks_are_required_before_data() {
    let key = Credentials::okx("key", b"secret", "pass");
    let mut f = PrivateFeed::new(&key, 1700000000123).unwrap();
    assert_eq!(f.login()["op"], "login");
    let orders = json!({"arg":{"channel":"orders","instType":"SPOT"},"data":[{"instType":"SPOT","instId":"BTC-USDT","clOrdId":"a","tradeId":"3","fillSz":"1","fillFee":"-0.1","fillFeeCcy":"USDT","apiKey":"never-print"}]});
    assert!(f.on_text(&serde_json::to_vec(&orders).unwrap()).is_err());
    let mut f = PrivateFeed::new(&key, 1700000000123).unwrap();
    let Reaction::Subscribe(request) = f
        .on_text(br#"{"event":"login","code":"0","connId":"a"}"#)
        .unwrap()
    else {
        panic!("OKX login must subscribe to orders and account events");
    };
    assert_eq!(request["args"][1]["channel"], "account");
    assert_eq!(
        request["args"][1]["extraParams"],
        r#"{"updateInterval":"0"}"#
    );
    assert!(matches!(f.on_text(br#"{"id":"twaccount","event":"subscribe","connId":"a","arg":{"channel":"orders","instType":"SPOT"}}"#).unwrap(),Reaction::Control));
    let Reaction::Data(data) = f.on_text(&serde_json::to_vec(&orders).unwrap()).unwrap() else {
        panic!()
    };
    assert_eq!(data.rows[0]["fillFee"], "-0.1");
    assert!(!data.rows[0].to_string().contains("never-print"));
    assert!(!f.subscribed());
    assert!(matches!(
        f.on_text(
            br#"{"id":"twaccount","event":"subscribe","connId":"a","arg":{"channel":"account"}}"#
        )
        .unwrap(),
        Reaction::Control
    ));
    assert!(f.subscribed());
}
#[test]
fn bybit_private_topics_share_an_ack_but_keep_wallet_scope_separate() {
    let key = Credentials::bybit("key", b"secret");
    let mut f = PrivateFeed::new(&key, 1700000000123).unwrap();
    assert_eq!(f.login()["args"][1], 1700000005123u64);
    assert!(matches!(
        f.on_text(br#"{"op":"auth","success":true,"conn_id":"a"}"#)
            .unwrap(),
        Reaction::Subscribe(_)
    ));
    f.on_text(br#"{"req_id":"twaccount","op":"subscribe","success":true,"conn_id":"a"}"#)
        .unwrap();
    assert!(f.subscribed());
    let Reaction::Data(data)=f.on_text(br#"{"topic":"execution.spot","data":[{"category":"spot","symbol":"BTCUSDT","execId":"ab","execTime":"123","private":"never-print"}]}"#).unwrap()else{panic!()};
    assert_eq!(data.channel, "fills");
    assert!(!data.rows[0].to_string().contains("never-print"));
    assert!(
        f.on_text(
            br#"{"topic":"execution.spot","data":[{"category":"linear","symbol":"BTCUSDT"}]}"#
        )
        .is_err()
    );
}

#[test]
fn a_bybit_heartbeat_error_invalidates_the_authenticated_connection() {
    let mut f = PrivateFeed::new(&Credentials::bybit("k", b"s"), 1700000000010).unwrap();
    f.on_text(br#"{"success":true,"op":"auth","conn_id":"c"}"#)
        .unwrap();
    f.on_text(br#"{"success":true,"op":"subscribe","req_id":"twaccount","conn_id":"c"}"#)
        .unwrap();
    assert!(f.subscribed());
    assert!(
        f.on_text(br#"{"op":"ping","success":false,"ret_msg":"session closed","conn_id":"c"}"#)
            .is_err()
    );
    assert!(!f.subscribed());
}

#[test]
fn live_network_hands_off_background_dialer_prewarmed_tls_socket() {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };
    use tickwire_spot_execution::network::LiveNetwork;
    use tickwire_transport::{
        dialer::{DialState, Dialer},
        http, rustls,
    };

    let host = "stream-testnet.bybit.com";
    let cert = rcgen::generate_simple_self_signed(vec![host.to_owned()]).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let srv_cfg = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.cert.der().clone()], key.into())
            .unwrap(),
    );
    let client_tls = Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let accepted_srv = Arc::clone(&accepted);

    let server = thread::spawn(move || {
        for idx in 0..2usize {
            let (tcp, _) = listener.accept().unwrap();
            accepted_srv.fetch_add(1, Ordering::AcqRel);
            tcp.set_nodelay(true).unwrap();
            tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            let mut tls = rustls::StreamOwned::new(
                rustls::ServerConnection::new(srv_cfg.clone()).unwrap(),
                tcp,
            );
            // Complete the server TLS handshake while the background Dialer pre-warms.
            while tls.conn.is_handshaking() {
                tls.conn.complete_io(&mut tls.sock).unwrap();
            }

            let mut req = Vec::new();
            let mut buf = [0u8; 1024];
            while !req.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = tls.read(&mut buf).unwrap();
                assert!(n > 0);
                req.extend_from_slice(&buf[..n]);
            }
            let body = format!(r#"{{"attempt":{idx}}}"#);
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            tls.write_all(resp.as_bytes()).unwrap();
            tls.conn.send_close_notify();
            tls.flush().unwrap();
        }
    });

    let dialer = Arc::new(Dialer::new().unwrap());
    let mut net = LiveNetwork::new(host, addr.port(), client_tls).with_peers(vec![addr]);
    let baton = net.attach_dialer(Arc::clone(&dialer), 1).unwrap();
    assert!(net.request_prewarm(0));

    let until = Instant::now() + Duration::from_secs(5);
    while baton.state() != DialState::Ready {
        assert!(
            Instant::now() < until,
            "background Dialer did not become Ready"
        );
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(accepted.load(Ordering::Acquire), 1);

    // Hot-path handoff takes the pre-established TLS connection without opening a 2nd socket!
    let wire =
        http::encode_request("GET", "/v5/market/time", &[("Host", host)], &[], 1024).unwrap();
    let r0 = net.get(&wire).unwrap();
    assert_eq!(r0.status, 200);
    assert_eq!(r0.body, br#"{"attempt":0}"#);
    assert_eq!(accepted.load(Ordering::Acquire), 1);

    // Notify disconnect schedules background pre-warm of the replacement connection!
    net.notify_disconnected(0);
    let until = Instant::now() + Duration::from_secs(5);
    while baton.state() != DialState::Ready {
        assert!(
            Instant::now() < until,
            "replacement Dialer slot did not become Ready"
        );
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(accepted.load(Ordering::Acquire), 2);

    let r1 = net.get(&wire).unwrap();
    assert_eq!(r1.status, 200);
    assert_eq!(r1.body, br#"{"attempt":1}"#);
    assert_eq!(accepted.load(Ordering::Acquire), 2);

    server.join().unwrap();
}
