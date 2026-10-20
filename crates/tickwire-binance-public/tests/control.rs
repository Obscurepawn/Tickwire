mod common;
use common::*;
use std::{
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

#[test]
fn ping_and_close_echo_the_original_payload_then_rebuild() {
    let (tls, server_tls) = certs();
    let ws = listener();
    let http = listener();
    let network = NetworkConfig {
        stream_address: ws.local_addr().unwrap(),
        snapshot_address: http.local_addr().unwrap(),
        tls,
    };
    let (done, ended) = mpsc::channel();
    let server = thread::spawn(move || {
        let mut stream = accept(&ws, server_tls);
        upgrade(
            &mut stream,
            "/ws/btcusdt@depth",
            "stream.testnet.binance.vision",
        );
        frame(&mut stream, 9, b"heartbeat");
        assert_eq!(read_frame(&mut stream), (10, b"heartbeat".to_vec()));
        frame(&mut stream, 8, &[3, 232]);
        assert_eq!(read_frame(&mut stream), (8, vec![3, 232]));
        done.send(()).unwrap();
    });
    let mut client = PublicDepthClient::<BinanceSpot, _, 8, 8, 8>::new(
        config::<BinanceSpot>(Source::SpotTest, SubscribeMode::RawUrl),
        network,
        Clock(Instant::now()),
    )
    .unwrap();
    let mut permit = Permit::default();
    let mut sink = Sink::default();
    client.start(&mut permit).unwrap();
    let until = Instant::now() + Duration::from_secs(3);
    while Instant::now() < until {
        client
            .poll(&mut permit, PollBudget::default(), &mut sink)
            .unwrap();
        match ended.try_recv() {
            Ok(()) | Err(mpsc::TryRecvError::Disconnected) => break,
            Err(mpsc::TryRecvError::Empty) => {}
        }
        thread::sleep(Duration::from_millis(1));
    }
    server.join().unwrap();
    assert!(!sink.gaps.is_empty());
    assert!(client.view(NS).is_err());
    assert!(permit.charges.iter().any(|c| c.kind == ChargeKind::Pong));
    assert!(permit.charges.iter().any(|c| c.kind == ChargeKind::Close));
}
