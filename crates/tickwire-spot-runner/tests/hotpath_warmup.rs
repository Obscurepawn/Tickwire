#![forbid(unsafe_code)]

#[path = "../../tickwire-account-gateway/tests/common/mod.rs"]
mod common;
#[path = "../src/paper_peer.rs"]
mod paper_peer;

use common::fixture;
use std::{
    cell::RefCell, collections::VecDeque, fs, net::Ipv4Addr, path::Path, rc::Rc, time::Instant,
};
use tickwire_account::Fixed;
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::{AccountGateway, Status};
use tickwire_account_journal::FileBackend;
use tickwire_app::strategy::{AppStrategy, Config, FileStrategy, MultiStrategy};
use tickwire_binance_clock::LocalTime;
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_quota::rate_limiter::{IpKey, VenuePreset, VenueRateLimiter};
use tickwire_recovery::{InputRecord, MockBlobStore, RecoveryMode, SharedBlobStore, encode_inputs};
use tickwire_runtime::{
    authority::{Dependency, RecoveryBundle, RecoveryQueue},
    node::{Health, Strategy, Submission, SubmissionResult},
};
use tickwire_runtime_policy::{ControlPlane, Versions};
use tickwire_spot_execution::{Credentials, RestSession, SpotEncoder, SpotVenue, symbol_rate_key};
use tickwire_spot_runner::AccountRunner;
use tickwire_transport::IoBudget;
use tickwire_venue::{CanonicalCommand, ClientId, CommandBody, Environment};

fn submission(command: &CanonicalCommand, id: u8) -> Submission {
    Submission {
        command: command.clone(),
        order_ids: vec![[id; 16]],
        deadline: common::until(),
        recovery: RecoveryBundle {
            command_digest: command.digest(),
            dependencies: (2..=11)
                .map(|kind| Dependency {
                    kind,
                    schema_id: 1,
                    owner: [9; 16],
                    bytes: vec![kind; 4],
                })
                .collect(),
            inputs: encode_inputs(
                [9; 16],
                1,
                [0; 32],
                &[InputRecord {
                    kind: 1,
                    payload: b"warmup isolation quote",
                }],
            )
            .unwrap(),
            mode: RecoveryMode::DeterministicReplay,
            fault_model_id: [4; 32],
            pin_id: [9; 16],
            owner: [9; 16],
        },
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
struct Receipts {
    local: Vec<Status>,
    denials: Vec<tickwire_account::Error>,
    acknowledgements: usize,
}

struct Member {
    id: u64,
    queue: VecDeque<Submission>,
    receipts: Rc<RefCell<Receipts>>,
}

impl AppStrategy for Member {
    fn failed(&self) -> bool {
        false
    }
}

impl Strategy for Member {
    fn on_market(&mut self, _: &tickwire_binance_public::Notice<'_>) -> bool {
        true
    }
    fn next(
        &mut self,
        _: &tickwire_account::Account,
        _: Health,
        _: LocalTime,
    ) -> Option<Submission> {
        self.queue.pop_front()
    }
    fn on_submission(&mut self, c: &CanonicalCommand, r: &SubmissionResult<'_>) {
        assert_eq!(self.id, c.command().context.strategy.logical_strategy_id);
        match r {
            Ok(status) => self.receipts.borrow_mut().local.push(*status),
            Err(error) => {
                let Some(tickwire_spot_runner::Error::Session(
                    tickwire_spot_execution::SessionError::Gateway(
                        tickwire_account_gateway::Error::Account(refusal),
                    ),
                )) = error.downcast_ref()
                else {
                    panic!("unexpected submission result: {error}");
                };
                self.receipts.borrow_mut().denials.push(*refusal);
            }
        }
    }
    fn on_request_reply(&mut self, c: &CanonicalCommand, r: &tickwire_venue::RequestReply) {
        assert_eq!(self.id, c.command().context.strategy.logical_strategy_id);
        assert!(matches!(
            r,
            tickwire_venue::RequestReply::Acknowledged { .. }
        ));
        self.receipts.borrow_mut().acknowledgements += 1;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LimiterSnapshot {
    frozen: bool,
    account_windows: Vec<(Option<u64>, Option<u64>)>,
    ip_window: Option<(Option<u64>, Option<u64>)>,
    sym_order_window: Option<(Option<u64>, Option<u64>)>,
    sym_cancel_window: Option<(Option<u64>, Option<u64>)>,
}

fn snapshot_limiter(
    limiter: &VenueRateLimiter,
    ip_key: IpKey,
    sym_key: u64,
    now_ns: u64,
) -> LimiterSnapshot {
    let inner = limiter.limiter();
    let account_windows = limiter
        .account_order_window_ids()
        .iter()
        .map(|&id| (inner.window_limit(id), inner.window_used(id, now_ns)))
        .collect();
    let ip_window = limiter
        .ip_window_id(ip_key)
        .map(|id| (inner.window_limit(id), inner.window_used(id, now_ns)));
    let sym_order_window = limiter
        .symbol_order_window_id(sym_key)
        .map(|id| (inner.window_limit(id), inner.window_used(id, now_ns)));
    let sym_cancel_window = limiter
        .symbol_cancel_window_id(sym_key)
        .map(|id| (inner.window_limit(id), inner.window_used(id, now_ns)));
    LimiterSnapshot {
        frozen: limiter.is_account_frozen(),
        account_windows,
        ip_window,
        sym_order_window,
        sym_cancel_window,
    }
}

fn snapshot_dir_files(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            let name = entry.file_name().into_string().unwrap();
            let bytes = fs::read(&path).unwrap();
            entries.push((name, bytes));
        }
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RunnerStateSnapshot {
    base_asset: (Fixed, Fixed),
    quote_asset: (Fixed, Fixed),
    order_count: usize,
    account_epoch: u64,
    ledger_sequence: u64,
    account_frozen: bool,
    journal_position: u64,
    journal_files: Vec<(String, Vec<u8>)>,
    origin_counts: (usize, usize, usize),
    inflight_count: usize,
    limiter: LimiterSnapshot,
}

#[test]
fn warmup_interval_zero_disables_and_positive_respects_idle_and_activity_reset() {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let dir = common::Directory::new();
        let (mut snapshot, catalog, mut source) = fixture::fixture();
        snapshot.scope.venue = venue.id();
        source.context.request.venue = venue.id();
        let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
        let creds = match venue {
            SpotVenue::Bybit => Credentials::bybit("paper", paper_peer::SECRET),
            SpotVenue::Okx => Credentials::okx("paper", paper_peer::SECRET, "paper-pass"),
        };
        let encoder =
            SpotEncoder::new(creds, Environment::Test, 2, || Ok(1_700_000_000_000)).unwrap();
        let mut cfg = common::config(&baseline);
        cfg.route = encoder.route();
        let (connection, wire) = common::peer(&cfg.route.authority, 16384);
        let queue = RecoveryQueue::new(2, 1024 * 1024).unwrap();
        let objects = SharedBlobStore::new(MockBlobStore::default());
        let gateway = AccountGateway::create(
            FileBackend::open(&dir.0).unwrap(),
            cfg,
            baseline,
            common::MockAuthority::default(),
            encoder,
            connection,
        )
        .unwrap();
        let mut runner = AccountRunner::new(RestSession::new(gateway, 1).unwrap(), queue, objects);

        // 1. Default `warmup_interval_ns == 0`: never triggers across arbitrary timestamps.
        assert_eq!(runner.warmup_interval_ns(), 0);
        for t in [0u64, 1, 500_000, 10_000_000_000] {
            assert!(!runner.maybe_warm_hot_path(t).unwrap());
        }
        assert_eq!(runner.warmup_count(), 0);

        // 2. Positive interval (`500_000 ns`): fires only after idle >= 500_000 ns and resets on activity.
        runner.set_warmup_interval_ns(500_000).unwrap();
        assert_eq!(runner.warmup_interval_ns(), 500_000);
        runner.record_activity(1_000_000);
        assert!(!runner.maybe_warm_hot_path(1_000_000).unwrap());
        assert!(!runner.maybe_warm_hot_path(1_499_999).unwrap());
        assert_eq!(runner.warmup_count(), 0);

        // Real activity at 1_300_000 pushes next due window out to 1_800_000.
        runner.record_activity(1_300_000);
        assert!(!runner.maybe_warm_hot_path(1_500_000).unwrap());
        assert!(!runner.maybe_warm_hot_path(1_799_999).unwrap());
        assert_eq!(runner.warmup_count(), 0);

        assert!(runner.maybe_warm_hot_path(1_800_000).unwrap());
        assert_eq!(runner.warmup_count(), 1);
        assert_eq!(runner.last_warm_ns(), 1_800_000);

        // Next check before another 500_000 ns elapses does not re-fire.
        assert!(!runner.maybe_warm_hot_path(2_299_999).unwrap());
        assert!(runner.maybe_warm_hot_path(2_300_000).unwrap());
        assert_eq!(runner.warmup_count(), 2);

        drop(runner);
        assert!(wire.join().unwrap().is_empty());
    }
}

#[test]
fn hundred_hotpath_warmups_preserve_all_live_state_and_101st_real_tls_order_succeeds() {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let dir = common::Directory::new();
        let (mut snapshot, mut catalog, mut source) = fixture::fixture();
        snapshot.scope.venue = venue.id();
        snapshot.valid_until = 10_000_000_000;
        snapshot.assets[1].free = fixture::units(80);
        let cap = catalog.entries[0].max_gross_quantity;
        catalog.entries[0].strategy_limits = vec![(8, cap)];
        let sym_key = symbol_rate_key(catalog.entries[0].instrument.instrument_id);
        let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();

        let base_t = 1_000_000u64;
        let expected_send_t = base_t + 100 * 500_000 + 100_000;

        source.context.request.venue = venue.id();
        source.context.request.deadline = 10_000_000_000;
        source.context.request.intent_id = 8;
        source.context.request.recovery_object = 8;
        source.context.strategy.instance_id = [8; 16];
        source.context.strategy.logical_strategy_id = 8;
        source.context.strategy.economic_intent_id = [8; 16];
        source.context.strategy.strategy_intent_seq = 1;
        source.context.strategy.created_mono_ns = expected_send_t - 50;
        let CommandBody::New(order) = &mut source.body else {
            unreachable!()
        };
        order.client_id = ClientId::new("paper8").unwrap();
        let command = CanonicalCommand::new(source.clone()).unwrap();

        let domain = ClockDomain::new([4; 16], 1).unwrap();
        let control = ControlPlane::new(tickwire_runtime_policy::Config {
            scope: baseline.snapshot().scope.clone(),
            product: source.context.request.product,
            executor_id: [2; 16],
            owner_generation: 1,
            writer_epoch: source.context.request.intent_exec_epoch,
            domain,
            versions: Versions {
                config_revision: source.context.request.config_revision,
                risk_policy_version: source.context.strategy.risk_policy_version,
                order_config_version: source.context.strategy.order_config_version,
                catalog_version: source.context.instrument.catalog_version,
                catalog_hash: source.context.instrument.catalog_hash,
            },
            max_strategies: 1,
            max_feeds: 1,
            marks_per_feed: 1,
        })
        .unwrap();

        let receipts = Rc::new(RefCell::new(Receipts::default()));
        let queue_items = VecDeque::from([submission(&command, 8)]);
        let mut group = MultiStrategy::new(
            vec![(
                8,
                Box::new(Member {
                    id: 8,
                    queue: queue_items,
                    receipts: receipts.clone(),
                }),
            )],
            control,
        )
        .unwrap();

        let (connection, wire) = paper_peer::start(venue, 1).unwrap();
        let creds = match venue {
            SpotVenue::Bybit => Credentials::bybit("paper", paper_peer::SECRET),
            SpotVenue::Okx => Credentials::okx("paper", paper_peer::SECRET, "paper-pass"),
        };
        let encoder =
            SpotEncoder::new(creds, Environment::Test, 2, || Ok(1_700_000_000_000)).unwrap();
        let mut cfg = common::config(&baseline);
        cfg.route = encoder.route();

        let preset = match venue {
            SpotVenue::Bybit => VenuePreset::BybitSpot,
            SpotVenue::Okx => VenuePreset::OkxSpot,
        };
        let ip_key = IpKey::from_v4(Ipv4Addr::LOCALHOST);
        let mut limiter = VenueRateLimiter::new(preset, 2, &[ip_key]).unwrap();
        if preset == VenuePreset::OkxSpot {
            limiter.register_symbol(sym_key).unwrap();
        }

        let queue = RecoveryQueue::new(2, 1024 * 1024).unwrap();
        let objects = SharedBlobStore::new(MockBlobStore::default());
        let authority = common::MockAuthority::default();
        {
            let mut pol = authority.policy.lock().unwrap();
            pol.gate.now = expected_send_t + 1;
            pol.gate.lease_until = 10_000_000_000;
        }
        let gateway = AccountGateway::create(
            FileBackend::open(&dir.0).unwrap(),
            cfg,
            baseline,
            authority,
            encoder,
            connection,
        )
        .unwrap();
        let session = RestSession::new(gateway, 1)
            .unwrap()
            .with_venue_rate_limiter(limiter);
        let mut runner = AccountRunner::new(session, queue, objects)
            .with_warmup_interval_ns(500_000)
            .unwrap();

        // Complete the live TLS 1.3 handshake against `paper_peer` before warming so the
        // live `rustls::ClientConnection` is active and ready on the socket.
        let mut idle_strategy = FileStrategy::new(&Config::Observe).unwrap();
        let handshake_deadline = common::until();
        while !runner.available() {
            assert!(Instant::now() < handshake_deadline);
            runner
                .poll(
                    &mut idle_strategy,
                    LocalTime {
                        instant: Instant::now(),
                        monotonic: MonoTime::new(domain, 50),
                    },
                    IoBudget::default(),
                )
                .unwrap();
            std::thread::yield_now();
        }

        runner.record_activity(base_t);
        assert_eq!(runner.warmup_count(), 0);

        let capture_snapshot = |r: &AccountRunner<'_, _, _, _, _>, now_ns: u64| {
            let b = r.account().asset(fixture::key(1)).unwrap();
            let q = r.account().asset(fixture::key(2)).unwrap();
            RunnerStateSnapshot {
                base_asset: (b.available, b.reserved),
                quote_asset: (q.available, q.reserved),
                order_count: r.account().order_keys().count(),
                account_epoch: r.account().account_epoch(),
                ledger_sequence: r.account().ledger_sequence(),
                account_frozen: r.account().frozen(),
                journal_position: r.position(),
                journal_files: snapshot_dir_files(&dir.0),
                origin_counts: r.origin_counts(),
                inflight_count: r.inflight_count(),
                limiter: snapshot_limiter(
                    r.session().venue_rate_limiter().unwrap(),
                    ip_key,
                    sym_key,
                    now_ns,
                ),
            }
        };

        let before = capture_snapshot(&runner, base_t);
        assert_eq!(before.journal_position, 0);
        assert_eq!(before.origin_counts, (0, 0, 0));

        // Run 100 consecutive hot-path warmup iterations (50 idle-triggered + 50 explicit)
        // while the live TLS 1.3 connection to `paper_peer` sits open and idle.
        let mut t = base_t;
        for _ in 0..50 {
            t += 500_000;
            assert!(runner.maybe_warm_hot_path(t).unwrap());
        }
        for _ in 0..50 {
            t += 500_000;
            let bytes = runner.warm_hot_path(t).unwrap();
            assert!(bytes > 64);
        }
        assert_eq!(runner.warmup_count(), 100);

        // Verify 100% bit-for-bit invariance of Account, AccountJournal, Origins, Receipts, and VenueRateLimiter!
        let after_warm = capture_snapshot(&runner, t);
        assert_eq!(after_warm, before);
        assert_eq!(*receipts.borrow(), Receipts::default());

        // Step 101: Submit and flush a REAL signed order over the live TLS 1.3 connection.
        let send_t = t + 100_000;
        let send_time = || LocalTime {
            instant: Instant::now(),
            monotonic: MonoTime::new(domain, send_t),
        };
        let health = Health {
            calibrated: true,
            market_live: true,
            private_receiving: true,
            account_frozen: false,
            sender_halted: false,
        };
        assert!(
            runner
                .poll_strategy(&mut group, health, send_time())
                .unwrap()
        );
        // While the real order is in-flight, `maybe_warm_hot_path` refuses to warm.
        assert!(!runner.maybe_warm_hot_path(send_t + 1_000_000).unwrap());
        assert_eq!(runner.warmup_count(), 100);

        let reply_deadline = common::until();
        while receipts.borrow().acknowledgements == 0 {
            assert!(Instant::now() < reply_deadline);
            runner
                .poll(&mut group, send_time(), IoBudget::default())
                .unwrap();
            std::thread::yield_now();
        }
        assert_eq!(
            *receipts.borrow(),
            Receipts {
                local: vec![Status::Queued],
                denials: vec![],
                acknowledgements: 1,
            }
        );
        assert_eq!(runner.position(), 1);
        assert_eq!(
            runner.account().asset(fixture::key(2)).unwrap().reserved,
            fixture::units(60)
        );
        assert!(!runner.account().frozen());
        // Only the 101st real order debited 1 token from VenueRateLimiter!
        let limiter_after_real = snapshot_limiter(
            runner.session().venue_rate_limiter().unwrap(),
            ip_key,
            sym_key,
            send_t,
        );
        for (_, used) in &limiter_after_real.account_windows {
            assert_eq!(*used, Some(1));
        }

        drop(runner);
        // Proves live TLS 1.3 AEAD nonces remained 100% synchronized and zero extra bytes hit the socket.
        wire.join().unwrap().unwrap();
    }
}
