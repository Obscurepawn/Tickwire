mod common;
use common::*;
use std::{
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
use tickwire_book::{PriceTicks, QuantityKnowledge, Side};

fn live<P: DepthPolicy + Send + 'static>(source: Source, mode: SubscribeMode) {
    let (tls, server_tls) = certs();
    let ws = listener();
    let http = listener();
    let network = NetworkConfig {
        stream_address: ws.local_addr().unwrap(),
        snapshot_address: http.local_addr().unwrap(),
        tls,
    };
    let (finish, finished) = mpsc::channel();
    let handle = thread::spawn(move || {
        let (host, rest, prefix, route) = match source {
            Source::SpotProduction => ("stream.binance.com", "api.binance.com", "/api/v3", ""),
            Source::SpotTest => (
                "stream.testnet.binance.vision",
                "testnet.binance.vision",
                "/api/v3",
                "",
            ),
            Source::UsdmProduction => (
                "fstream.binance.com",
                "fapi.binance.com",
                "/fapi/v1",
                "/public",
            ),
            Source::UsdmTest => (
                "demo-fstream.binance.com",
                "demo-fapi.binance.com",
                "/fapi/v1",
                "/public",
            ),
        };
        let path = match mode {
            SubscribeMode::RawUrl => format!("{route}/ws/btcusdt@depth"),
            SubscribeMode::Explicit if route.is_empty() => "/ws".into(),
            SubscribeMode::Explicit => format!("{route}/stream"),
        };
        let mut stream = accept(&ws, server_tls.clone());
        upgrade(&mut stream, &path, host);
        if mode == SubscribeMode::Explicit {
            let request: serde_json::Value =
                serde_json::from_slice(&client_frame(&mut stream)).unwrap();
            assert_eq!(request["method"], "SUBSCRIBE");
            assert_eq!(request["params"], serde_json::json!(["btcusdt@depth"]));
            let event = delta::<P>(99, 101, 98);
            if route.is_empty() {
                frame(&mut stream, 1, &event);
            } else {
                frame(&mut stream,1,&serde_json::to_vec(&serde_json::json!({"stream":"btcusdt@depth","data":serde_json::from_slice::<serde_json::Value>(&event).unwrap()})).unwrap());
            }
            frame(
                &mut stream,
                1,
                &serde_json::to_vec(&serde_json::json!({"result":null,"id":request["id"]}))
                    .unwrap(),
            );
        } else {
            frame(&mut stream, 1, &delta::<P>(99, 101, 98));
        }
        let mut snapshot = accept(&http, server_tls);
        let h = headers(&mut snapshot);
        assert!(
            h.starts_with(&format!(
                "GET {prefix}/depth?symbol=BTCUSDT&limit=5 HTTP/1.1\r\n"
            )),
            "{h}"
        );
        assert!(h.lines().any(|l| l == format!("Host: {rest}")));
        reply(&mut snapshot, &common::snapshot::<P>(100));
        finished.recv_timeout(Duration::from_secs(5)).unwrap();
    });
    let mut client = PublicDepthClient::<P, _, 8, 8, 8>::new(
        config::<P>(source, mode),
        network,
        Clock(Instant::now()),
    )
    .unwrap();
    let mut permit = Permit::default();
    let mut sink = Sink::default();
    client.start(&mut permit).unwrap();
    let end = Instant::now() + Duration::from_secs(3);
    while client.status().depth != DepthState::Live && Instant::now() < end {
        client
            .poll(&mut permit, PollBudget::default(), &mut sink)
            .unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        client.status().depth,
        DepthState::Live,
        "real WSS and HTTPS must bridge the book"
    );
    assert_eq!(client.status().last_update_id, Some(101));
    assert_eq!(
        client
            .view(10 * NS)
            .unwrap()
            .quantity(Side::Bid, PriceTicks(9800)),
        QuantityKnowledge::Unknown
    );
    assert_eq!(permit.feedback, vec![200]);
    assert!(
        permit
            .charges
            .iter()
            .any(|c| c.kind == ChargeKind::Snapshot)
    );
    assert!(sink.gaps.is_empty());
    finish.send(()).unwrap();
    handle.join().unwrap();
    client.stop(&mut sink).unwrap();
    assert_eq!(client.status().phase, Phase::Stopped);
    assert!(client.view(10 * NS).is_err());
    client.stop(&mut sink).unwrap();
    assert_eq!(sink.gaps.len(), 1);
}
#[test]
fn spot_production_raw_owner_bridges_real_wss_and_https() {
    live::<BinanceSpot>(Source::SpotProduction, SubscribeMode::RawUrl);
}
#[test]
fn spot_test_explicit_buffers_data_before_ack() {
    live::<BinanceSpot>(Source::SpotTest, SubscribeMode::Explicit);
}
#[test]
fn usdm_production_raw_owner_bridges_real_wss_and_https() {
    live::<BinanceUsdM>(Source::UsdmProduction, SubscribeMode::RawUrl);
}
#[test]
fn usdm_test_explicit_combined_buffers_data_before_ack() {
    live::<BinanceUsdM>(Source::UsdmTest, SubscribeMode::Explicit);
}
