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
struct TickClock {
    now: Arc<AtomicU64>,
    instant: Instant,
}
impl MonotonicClock for TickClock {
    fn sample(&mut self) -> tickwire_binance_clock::Result<LocalTime> {
        let n = self.now.fetch_add(1_000_000, Ordering::SeqCst);
        Ok(LocalTime {
            instant: self.instant,
            monotonic: MonoTime::new(domain(), n),
        })
    }
}
struct Notifier {
    inner: Sink,
    second: Option<mpsc::Sender<()>>,
}
impl PublicSink for Notifier {
    fn on_notice(&mut self, n: Notice<'_>) -> bool {
        self.inner.on_notice(n);
        if self.inner.stamps.len() == 2
            && let Some(tx) = self.second.take()
        {
            tx.send(()).unwrap();
        }
        true
    }
}
fn encoded(b: &[u8]) -> Vec<u8> {
    let mut out = vec![0x81];
    if b.len() < 126 {
        out.push(b.len() as u8);
    } else {
        out.push(126);
        out.extend_from_slice(&(b.len() as u16).to_be_bytes());
    }
    out.extend_from_slice(b);
    out
}
fn pending_response(failed: bool) {
    let (tls, server_tls) = certs();
    let ws = listener();
    let http = listener();
    let network = NetworkConfig {
        stream_address: ws.local_addr().unwrap(),
        snapshot_address: http.local_addr().unwrap(),
        tls,
    };
    let (second, second_seen) = mpsc::channel();
    let (done, ended) = mpsc::channel();
    let server = thread::spawn(move || {
        let mut stream = accept(&ws, server_tls.clone());
        upgrade(
            &mut stream,
            "/ws/btcusdt@depth",
            "stream.testnet.binance.vision",
        );
        frame(&mut stream, 1, &delta::<BinanceSpot>(99, 100, 98));
        let mut snapshot_conn = accept(&http, server_tls.clone());
        headers(&mut snapshot_conn);
        // Both complete frames belong to one TLS/application receive batch.
        let mut batch = encoded(&delta::<BinanceSpot>(101, 102, 100));
        batch.extend(encoded(&delta::<BinanceSpot>(103, 104, 102)));
        stream.write_all(&batch).unwrap();
        stream.flush().unwrap();
        second_seen.recv_timeout(Duration::from_secs(5)).unwrap();
        if failed {
            drop(snapshot_conn);
            let mut retry = accept(&http, server_tls);
            headers(&mut retry);
            frame(&mut stream, 1, &delta::<BinanceSpot>(105, 106, 104));
            reply(&mut retry, &snapshot::<BinanceSpot>(104));
            ended.recv_timeout(Duration::from_secs(5)).unwrap();
        } else {
            reply(&mut snapshot_conn, &snapshot::<BinanceSpot>(100));
            ended.recv_timeout(Duration::from_secs(5)).unwrap();
        }
    });
    let clock = Arc::new(AtomicU64::new(0));
    let mut c = PublicDepthClient::<BinanceSpot, _, 8, 8, 8>::new(
        config::<BinanceSpot>(Source::SpotTest, SubscribeMode::RawUrl),
        network,
        TickClock {
            now: clock.clone(),
            instant: Instant::now(),
        },
    )
    .unwrap();
    let mut permit = Permit::default();
    let mut sink = Notifier {
        inner: Sink::default(),
        second: Some(second),
    };
    c.start(&mut permit).unwrap();
    let until = Instant::now() + Duration::from_secs(3);
    while c.status().depth != DepthState::Live && Instant::now() < until {
        c.poll(
            &mut permit,
            PollBudget {
                max_ws_messages: 1,
                max_notifications: 1,
                ..PollBudget::default()
            },
            &mut sink,
        )
        .unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        c.status().depth,
        DepthState::Live,
        "{:?}",
        sink.inner.outcomes
    );
    assert_eq!(
        c.status().last_update_id,
        Some(if failed { 106 } else { 104 })
    );
    assert_eq!(
        sink.inner.stamps[1], sink.inner.stamps[2],
        "budget yield must keep the original receive batch"
    );
    assert_eq!(
        c.view(10 * NS).unwrap().received_at_ns(),
        *sink.inner.stamps.last().unwrap()
    );
    assert!(clock.load(Ordering::SeqCst) > *sink.inner.stamps.last().unwrap());
    assert!(
        !sink
            .inner
            .outcomes
            .iter()
            .any(|o| matches!(o, SyncOutcome::Rebuild(_)))
    );
    assert!(sink.inner.gaps.is_empty());
    if failed {
        assert!(
            sink.inner
                .outcomes
                .iter()
                .any(|o| matches!(o, SyncOutcome::RetrySnapshot(_)))
        );
    }
    done.send(()).unwrap();
    server.join().unwrap();
}
#[test]
fn a_completed_snapshot_waits_for_buffered_ws_messages_and_preserves_arrival_time() {
    pending_response(false);
}
#[test]
fn a_failed_snapshot_waits_for_buffered_ws_messages_before_advancing_the_clock() {
    pending_response(true);
}
