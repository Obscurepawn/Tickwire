use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::Path,
};
use tickwire_binance::{feed::TimeUnit, request::Market};
use tickwire_binance_public::{DepthEncoding, Notice, SyncOutcome};
use tickwire_book::{BinanceSpot, Book, BookKey, DepthEvent, Level, PriceTicks, QtyLots, Snapshot};
use tickwire_control::{Policy, ProcessIdentity};
use tickwire_control_sys::PrivateDirectory;
use tickwire_market_publisher::{InstrumentConfig, Limits};
use tickwire_market_source::{
    ConnectConfig,
    protocol::{Endpoint, Instrument, wire_bytes},
};
use tickwire_md::{
    config,
    server::{Admission, Server, TopicOwner},
};
use tickwire_shm::Publisher;
pub struct Fixture {
    _directory: PrivateDirectory,
    pub server: Server,
    pub config: config::Config,
    pub topics: Vec<TopicOwner>,
}
impl Fixture {
    pub fn new(now: u64) -> Self {
        let directory = PrivateDirectory::create_in(Path::new("/tmp"), "tw-md-expand").unwrap();
        let mut config: config::Config =
            serde_json::from_str(include_str!("../../../tickwire-md/examples/md.json")).unwrap();
        config.identity.arch = if cfg!(target_arch = "aarch64") { 2 } else { 1 };
        config.socket = directory.path().join("md.sock");
        config.control_directory = directory.path().join("control");
        config.admission_directory = directory.path().join("admissions");
        fs::create_dir(&config.admission_directory).unwrap();
        fs::set_permissions(
            &config.admission_directory,
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        let mut spec: config::ResourceSpec =
            serde_json::from_str(include_str!("../../../tickwire-md/examples/topic-101.json"))
                .unwrap();
        spec.identity = config.identity.clone();
        spec.capacity = 64;
        spec.arena_classes = vec![(65536, 32)];
        let instrument = config.feeds[0]
            .instrument
            .build(tickwire_binance_public::Source::SpotProduction)
            .unwrap();
        let id = instrument.instrument_id;
        let registry = tickwire_join::TopicRegistry {
            catalog_hash: instrument.catalog_hash,
            instrument_set_version: 1,
            instruments: vec![tickwire_snapshot::InstrumentSpec {
                instrument_id: id,
                instrument_version: instrument.instrument_version,
                required_states: 1,
            }],
        };
        let owner = tickwire_join::JoinOwner::new(
            Publisher::<32>::create(spec.header_with_exact_events().unwrap()).unwrap(),
            registry,
        )
        .unwrap();
        let key = BookKey {
            venue: 1,
            instrument: 0,
            source: 1,
        };
        let mut topic = TopicOwner::new(
            owner,
            vec![InstrumentConfig {
                instrument,
                key,
                market: Market::Spot,
                time_unit: TimeUnit::Milliseconds,
            }],
            Limits {
                levels_per_side: 8,
                cache_bytes: 1 << 20,
                snapshot_bytes: 1 << 20,
                max_delta_bytes: 4096,
            },
        )
        .unwrap();
        let mut book = Book::<BinanceSpot, 8, 8>::new(key, 1);
        book.install_snapshot(Snapshot {
            key,
            generation: 1,
            last_update_id: 1,
            bids: &[Level {
                price: PriceTicks(100),
                quantity: QtyLots(1),
            }],
            asks: &[Level {
                price: PriceTicks(101),
                quantity: QtyLots(1),
            }],
            bids_complete: true,
            asks_complete: true,
        })
        .unwrap();
        book.apply(DepthEvent {
            key,
            generation: 1,
            first_update_id: 2,
            last_update_id: 2,
            previous_update_id: None,
            received_at_ns: now,
            bids: &[],
            asks: &[],
        })
        .unwrap();
        struct Faults;
        impl tickwire_market_publisher::FaultSink for Faults {
            fn invalidate(&mut self, fault: tickwire_market_publisher::Fault) {
                panic!("{fault:?}")
            }
        }
        topic
            .on_notice(
                id,
                Notice::Sync {
                    connection_epoch: 1,
                    book_generation: 1,
                    received_mono_ns: Some(now),
                    raw_delta: None,
                    encoding: DepthEncoding::Json,
                    outcome: SyncOutcome::Applied(tickwire_book::ApplyOutcome::Applied {
                        last_update_id: 2,
                        became_live: true,
                    }),
                    view: Some(book.view(now, 1_000_000).unwrap()),
                },
                now,
                &mut Faults,
            )
            .unwrap();
        Self {
            server: Server::new(&config).unwrap(),
            _directory: directory,
            config,
            topics: vec![topic],
        }
    }
    pub fn update(&mut self, now: u64) {
        let id = self.config.feeds[0].instrument.id;
        let key = BookKey {
            venue: 1,
            instrument: 0,
            source: 1,
        };
        let mut book = Book::<BinanceSpot, 8, 8>::new(key, 1);
        book.install_snapshot(Snapshot {
            key,
            generation: 1,
            last_update_id: 2,
            bids: &[Level {
                price: PriceTicks(100),
                quantity: QtyLots(2),
            }],
            asks: &[Level {
                price: PriceTicks(101),
                quantity: QtyLots(2),
            }],
            bids_complete: true,
            asks_complete: true,
        })
        .unwrap();
        book.apply(DepthEvent {
            key,
            generation: 1,
            first_update_id: 3,
            last_update_id: 3,
            previous_update_id: None,
            received_at_ns: now,
            bids: &[],
            asks: &[],
        })
        .unwrap();
        struct Faults;
        impl tickwire_market_publisher::FaultSink for Faults {
            fn invalidate(&mut self, f: tickwire_market_publisher::Fault) {
                panic!("{f:?}")
            }
        }
        self.topics[0]
            .on_notice(
                id,
                Notice::Sync {
                    connection_epoch: 1,
                    book_generation: 1,
                    received_mono_ns: Some(now),
                    raw_delta: None,
                    encoding: DepthEncoding::Json,
                    outcome: SyncOutcome::Applied(tickwire_book::ApplyOutcome::Applied {
                        last_update_id: 3,
                        became_live: false,
                    }),
                    view: Some(book.view(now, 1_000_000).unwrap()),
                },
                now,
                &mut Faults,
            )
            .unwrap();
    }
    pub fn admission(&mut self, id: u8) -> Admission {
        let mut local = self.config.identity.clone();
        local.instance = [20 + id; 16];
        local.role = 4;
        local.logical_strategy = 0;
        let mut supervisor = self.config.identity.clone();
        supervisor.instance = [1; 16];
        supervisor.role = 1;
        let meta = fs::metadata("/proc/self").unwrap();
        let (owner, _) = self.topics[0].split();
        let policy = Policy {
            local: local.identity(),
            peer: self.config.identity.identity(),
            peer_process: ProcessIdentity {
                pid: std::process::id(),
                uid: meta.uid(),
                gid: meta.gid(),
            },
            required_features: 0x1c,
            supported_features: 1023,
            control_epoch: 7,
            lease_duration_ns: 60_000_000_000,
            max_frame_bytes: 65536,
            nonce: [id; 32],
        };
        Admission {
            client: ConnectConfig {
                endpoint: Endpoint::new(self.config.socket.clone(), &policy),
                bank_header: wire_bytes(owner.header()).unwrap(),
                catalog_hash: owner.registry().catalog_hash,
                instrument_set_version: owner.registry().instrument_set_version,
                instruments: owner
                    .registry()
                    .instruments
                    .iter()
                    .map(|i| Instrument {
                        id: i.instrument_id,
                        version: i.instrument_version,
                        required_states: i.required_states,
                    })
                    .collect(),
                cursor_id: [60 + id; 16],
                subscription_generation: 1,
                request_id: [40 + id; 16],
                maximum_levels: 8,
                startup_timeout_ms: 10000,
                clock_generation: 1,
            },
            pid: std::process::id(),
            uid: meta.uid(),
            gid: meta.gid(),
            supervisor,
            supervisor_pid: std::process::id(),
            snapshot_id: [80 + id; 16],
        }
    }
}
