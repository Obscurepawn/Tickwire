//! A real MD publisher, snapshot JOIN and credential-bound executor cursor in the process lab.
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_binance_clock::SystemClock;
use tickwire_book::BookKey;
use tickwire_clock::ClockDomain;
use tickwire_join::{JoinOwner, TopicRegistry};
use tickwire_market_publisher::{Limits, SpotQuote};
use tickwire_market_source::{
    ConnectConfig,
    protocol::{Endpoint, Instrument, wire_bytes},
};
use tickwire_md::{
    config::ResourceSpec,
    server::{Admission, Server, ServerConfig, TopicOwner, register_admission},
};
use tickwire_shm::Publisher;
use tickwire_snapshot::{InstrumentSpec, QUOTE};
use tickwire_venue::TriggerRef;

pub(super) struct Fixture {
    client: ConnectConfig,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}
impl Fixture {
    pub(super) fn client(&self) -> serde_json::Value {
        serde_json::to_value(&self.client).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.worker.take().unwrap().join().unwrap();
    }
}
pub(super) fn start(root: &Path) -> Fixture {
    let (tx, rx) = mpsc::sync_channel(1);
    let root = root.to_owned();
    let stop = Arc::new(AtomicBool::new(false));
    let finished = stop.clone();
    let worker = thread::spawn(move || {
        let account =
            CanonicalBaseline::decode(&fs::read(root.join("baseline.cbor")).unwrap()).unwrap();
        let i = &account.catalog().entries[0].instrument;
        let key = BookKey {
            venue: account.snapshot().scope.venue,
            instrument: 0,
            source: 1,
        };
        let mut spec: ResourceSpec =
            serde_json::from_str(include_str!("../../../tickwire-md/examples/topic-101.json"))
                .unwrap();
        spec.identity.arch = if cfg!(target_arch = "aarch64") { 2 } else { 1 };
        spec.identity.instance = [7; 16];
        spec.identity.host_boot = [2; 16];
        spec.catalog_version = i.catalog_version;
        spec.capacity = 512;
        spec.arena_classes = vec![(65536, 32)];
        let header = spec.header_with_exact_events().unwrap();
        let registry = TopicRegistry {
            catalog_hash: i.catalog_hash,
            instrument_set_version: 1,
            instruments: vec![InstrumentSpec {
                instrument_id: i.instrument_id,
                instrument_version: i.instrument_version,
                required_states: QUOTE,
            }],
        };
        let owner =
            JoinOwner::new(Publisher::<32>::create(header.clone()).unwrap(), registry).unwrap();
        let mut topic = TopicOwner::new_spot_quotes(
            owner,
            vec![(i.clone(), key)],
            Limits {
                levels_per_side: 1,
                cache_bytes: 1 << 20,
                snapshot_bytes: 1 << 20,
                max_delta_bytes: 4096,
            },
        )
        .unwrap();
        let admissions = root.join("admissions");
        fs::create_dir(&admissions).unwrap();
        fs::set_permissions(&admissions, fs::Permissions::from_mode(0o700)).unwrap();
        let server_cfg = ServerConfig {
            socket: root.join("md.sock"),
            admission_directory: admissions.clone(),
            control_directory: root.join("md-journal"),
            control_stream: [17; 16],
            control_domain: [18; 16],
            control_writer_epoch: 1,
        };
        let mut server = Server::open(&spec.identity, &server_cfg).unwrap();
        let mut local = spec.identity.clone();
        local.instance = [2; 16];
        local.role = 4;
        let mut supervisor = spec.identity.clone();
        supervisor.instance = [1; 16];
        supervisor.role = 1;
        let meta = fs::metadata("/proc/self").unwrap();
        let client = ConnectConfig {
            endpoint: Endpoint {
                path: server_cfg.socket,
                local,
                peer: spec.identity,
                peer_pid: std::process::id(),
                peer_uid: meta.uid(),
                peer_gid: meta.gid(),
                required_features: 0x1c,
                supported_features: 1023,
                control_epoch: 7,
                lease_duration_ns: 120_000_000_000,
                max_frame_bytes: 65536,
                nonce: [11; 32],
            },
            bank_header: wire_bytes(&header).unwrap(),
            catalog_hash: i.catalog_hash,
            instrument_set_version: 1,
            instruments: vec![Instrument {
                id: i.instrument_id,
                version: i.instrument_version,
                required_states: QUOTE,
            }],
            cursor_id: [33; 16],
            subscription_generation: 1,
            request_id: [34; 16],
            maximum_levels: 1,
            startup_timeout_ms: 10000,
            clock_generation: 1,
        };
        let mut clock = SystemClock::new(ClockDomain::new([2; 16], 1).unwrap()).unwrap();
        let mut seq = 0;
        let mut publish = |topic: &mut TopicOwner, now| {
            seq += 1;
            topic
                .on_spot_quote(
                    &SpotQuote {
                        key,
                        instrument: i.instrument_id,
                        version: i.instrument_version,
                        epoch: 1,
                        sequence: seq,
                        exchange_ns: 1_790_000_000_000_000_000 + seq,
                        received_ns: now,
                        bid_ticks: 100,
                        ask_ticks: 101,
                        bid_lots: 2,
                        ask_lots: 2,
                    },
                    now,
                )
                .unwrap();
            TriggerRef {
                topic_id: header.topic_id,
                feed_generation: header.feed_generation,
                ring_epoch: header.ring_epoch,
                sequence: topic.split().0.published(),
                instrument_id: i.instrument_id,
                instrument_version: i.instrument_version,
                observed_mono_ns: now,
            }
        };
        publish(&mut topic, clock.sample().unwrap().monotonic.nanos());
        tx.send(client.clone()).unwrap();
        let mut registered = false;
        let mut issued = false;
        let mut next = Instant::now() + Duration::from_millis(100);
        while !finished.load(Ordering::Acquire) {
            if !registered && let Ok(pid) = fs::read_to_string(root.join("trader-pid")) {
                register_admission(
                    &admissions,
                    &Admission {
                        client: client.clone(),
                        pid: pid.trim().parse().unwrap(),
                        uid: meta.uid(),
                        gid: meta.gid(),
                        supervisor: supervisor.clone(),
                        supervisor_pid: std::process::id(),
                        snapshot_id: [35; 16],
                    },
                )
                .unwrap();
                registered = true;
            }
            let now = clock.sample().unwrap().monotonic.nanos();
            if registered {
                server.poll(std::slice::from_mut(&mut topic), now).unwrap();
            }
            if !topic.needs_bank() && Instant::now() >= next {
                let trigger = publish(&mut topic, now);
                if !issued && root.join("strategy-go").exists() {
                    super::publish_order(&root, trigger);
                    issued = true;
                }
                next = Instant::now() + Duration::from_millis(100);
            }
            thread::sleep(Duration::from_millis(1));
        }
    });
    Fixture {
        client: rx.recv().unwrap(),
        stop,
        worker: Some(worker),
    }
}

#[test]
fn execution_source_only_announces_complete_fresh_quote_pairs() {
    use tickwire_market_source::{FeedObservation, Invalidation, MarketSink, MarketSource};
    #[derive(Default)]
    struct Sink(Vec<u64>);
    impl MarketSink for Sink {
        fn observed(&mut self, v: FeedObservation) -> bool {
            assert!(v.live);
            self.0.push(v.sequence);
            true
        }
        fn invalidated(&mut self, _: Invalidation) -> bool {
            panic!("an unfinished BBO is not a feed gap")
        }
    }
    let root = std::env::temp_dir().join(format!("tw-bbo-pair-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    struct Cleanup<'a>(&'a Path);
    impl Drop for Cleanup<'_> {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(self.0);
        }
    }
    let _cleanup = Cleanup(&root);
    let sample =
        CanonicalBaseline::decode(include_bytes!("../../examples/baseline-template.cbor")).unwrap();
    let mut snap = sample.snapshot().clone();
    snap.scope.venue = 2;
    fs::write(
        root.join("baseline.cbor"),
        CanonicalBaseline::new(snap, sample.catalog().clone())
            .unwrap()
            .bytes(),
    )
    .unwrap();
    let fixture = start(&root);
    let cfg: ConnectConfig = serde_json::from_value(fixture.client()).unwrap();
    fs::write(root.join("trader-pid"), std::process::id().to_string()).unwrap();
    let mut clock = SystemClock::new(cfg.domain().unwrap()).unwrap();
    let mut now = || {
        clock
            .sample()
            .map(|s| s.monotonic)
            .map_err(|_| tickwire_market_source::Error::Identity)
    };
    let mut source = tickwire_app::market::Sources::connect(
        std::slice::from_ref(&cfg),
        cfg.domain().unwrap(),
        [2; 16],
        1,
    )
    .unwrap();
    let mut sink = Sink::default();
    let age = 1_000_000_000;
    let deadline = Instant::now() + Duration::from_secs(3);
    source.poll(now().unwrap(), age, 32, &mut sink).unwrap();
    assert!(
        source.is_live(now().unwrap(), age),
        "the joined BBO includes both quote and feed"
    );
    let before = sink.0.len();
    while source
        .poll(now().unwrap(), age, 1, &mut sink)
        .unwrap()
        .records
        == 0
    {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        sink.0.len(),
        before,
        "the standalone quote cannot become a trigger"
    );
    assert!(!source.is_live(now().unwrap(), age));
    source.poll(now().unwrap(), age, 1, &mut sink).unwrap();
    assert_eq!(sink.0.len(), before + 1);
    assert!(source.is_live(now().unwrap(), age));
    assert!(!source.is_live(now().unwrap(), 1));
}
