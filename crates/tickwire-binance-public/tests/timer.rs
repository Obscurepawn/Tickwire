mod common;
use common::*;
use std::{
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};
struct ManualClock {
    now: Arc<AtomicU64>,
    instant: Instant,
}
impl MonotonicClock for ManualClock {
    fn sample(&mut self) -> tickwire_binance_clock::Result<LocalTime> {
        Ok(LocalTime {
            instant: self.instant,
            monotonic: MonoTime::new(domain(), self.now.load(Ordering::SeqCst)),
        })
    }
}
#[test]
fn a_quiet_stream_wakes_at_the_snapshot_retry_permission() {
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
        let mut stream = accept(&ws, server_tls.clone());
        upgrade(
            &mut stream,
            "/ws/btcusdt@depth",
            "stream.testnet.binance.vision",
        );
        frame(&mut stream, 1, &delta::<BinanceSpot>(99, 101, 98));
        let mut first = accept(&http, server_tls.clone());
        headers(&mut first);
        first
            .write_all(
                b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 2\r\nRetry-After: 2\r\n\r\n{}",
            )
            .unwrap();
        first.flush().unwrap();
        let mut retry = accept(&http, server_tls);
        headers(&mut retry);
        reply(&mut retry, &snapshot::<BinanceSpot>(100));
        ended.recv_timeout(Duration::from_secs(5)).unwrap();
    });
    let now = Arc::new(AtomicU64::new(0));
    let mut c = PublicDepthClient::<BinanceSpot, _, 8, 8, 8>::new(
        config::<BinanceSpot>(Source::SpotTest, SubscribeMode::RawUrl),
        network,
        ManualClock {
            now: now.clone(),
            instant: Instant::now(),
        },
    )
    .unwrap();
    let mut permit = Permit::default();
    let mut sink = Sink::default();
    c.start(&mut permit).unwrap();
    let end = Instant::now() + Duration::from_secs(3);
    let report = loop {
        let report = c
            .poll(&mut permit, PollBudget::default(), &mut sink)
            .unwrap();
        if sink
            .outcomes
            .iter()
            .any(|o| matches!(o, SyncOutcome::RetrySnapshot(_)))
        {
            break report;
        }
        assert!(Instant::now() < end);
        thread::sleep(Duration::from_millis(1));
    };
    assert!(!c.status().snapshot_pending);
    assert_eq!(report.next_deadline_ns, Some(2 * NS));
    now.store(2 * NS, Ordering::SeqCst);
    while c.status().depth != DepthState::Live && Instant::now() < end {
        c.poll(&mut permit, PollBudget::default(), &mut sink)
            .unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(c.status().depth, DepthState::Live);
    assert_eq!(c.status().connection_epoch, 1);
    done.send(()).unwrap();
    server.join().unwrap();
}
