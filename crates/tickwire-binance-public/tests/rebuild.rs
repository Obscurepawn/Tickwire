mod common;
use common::*;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};
struct ClockNow {
    now: Arc<AtomicU64>,
    instant: Instant,
}
impl MonotonicClock for ClockNow {
    fn sample(&mut self) -> tickwire_binance_clock::Result<LocalTime> {
        Ok(LocalTime {
            instant: self.instant,
            monotonic: MonoTime::new(domain(), self.now.load(Ordering::SeqCst)),
        })
    }
}
#[test]
fn a_late_gap_rebuilds_on_the_same_healthy_subscription() {
    let (tls, server_tls) = certs();
    let ws = listener();
    let http = listener();
    let network = NetworkConfig {
        stream_address: ws.local_addr().unwrap(),
        snapshot_address: http.local_addr().unwrap(),
        tls,
    };
    let (go, when) = mpsc::channel();
    let (done, ended) = mpsc::channel();
    let server = thread::spawn(move || {
        let mut stream = accept(&ws, server_tls.clone());
        upgrade(
            &mut stream,
            "/ws/btcusdt@depth",
            "stream.testnet.binance.vision",
        );
        frame(&mut stream, 1, &delta::<BinanceSpot>(99, 101, 98));
        let mut first = accept(&http, server_tls.clone());
        headers(&mut first);
        reply(&mut first, &snapshot::<BinanceSpot>(100));
        when.recv_timeout(Duration::from_secs(5)).unwrap();
        frame(&mut stream, 1, &delta::<BinanceSpot>(105, 106, 104));
        when.recv_timeout(Duration::from_secs(5)).unwrap();
        frame(&mut stream, 1, &delta::<BinanceSpot>(110, 112, 109));
        let mut second = accept(&http, server_tls);
        headers(&mut second);
        reply(&mut second, &snapshot::<BinanceSpot>(111));
        ended.recv_timeout(Duration::from_secs(5)).unwrap();
    });
    let now = Arc::new(AtomicU64::new(0));
    let mut c = PublicDepthClient::<BinanceSpot, _, 8, 8, 8>::new(
        config::<BinanceSpot>(Source::SpotTest, SubscribeMode::RawUrl),
        network,
        ClockNow {
            now: now.clone(),
            instant: Instant::now(),
        },
    )
    .unwrap();
    let mut permit = Permit::default();
    let mut sink = Sink::default();
    c.start(&mut permit).unwrap();
    let end = Instant::now() + Duration::from_secs(3);
    while c.status().depth != DepthState::Live && Instant::now() < end {
        c.poll(&mut permit, PollBudget::default(), &mut sink)
            .unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(c.status().depth, DepthState::Live);
    let old = c.status();
    now.store(10 * NS, Ordering::SeqCst);
    go.send(()).unwrap();
    while !sink
        .outcomes
        .iter()
        .any(|x| matches!(x, SyncOutcome::Rebuild(_)))
        && Instant::now() < end
    {
        c.poll(&mut permit, PollBudget::default(), &mut sink)
            .unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    assert!(
        sink.outcomes
            .iter()
            .any(|x| matches!(x, SyncOutcome::Rebuild(_)))
    );
    go.send(()).unwrap();
    while c.status().depth != DepthState::Live && Instant::now() < end {
        c.poll(&mut permit, PollBudget::default(), &mut sink)
            .unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(c.status().depth, DepthState::Live, "{:?}", sink.gaps);
    assert_eq!(c.status().connection_epoch, old.connection_epoch);
    assert_eq!(c.status().book_generation, old.book_generation + 1);
    assert_eq!(c.status().last_update_id, Some(112));
    assert!(sink.gaps.is_empty());
    done.send(()).unwrap();
    server.join().unwrap();
}
