//! Live spot account owner. Only this thread can admit orders or publish account facts.
mod account;
mod config;
mod scan;
use crate::{Result, protocol_error};
use account::PrivateAccount;
use config::Deployment;
use scan::{Scan, Scanner};
use std::{
    cell::Cell,
    net::ToSocketAddrs,
    path::Path,
    rc::Rc,
    time::{Duration, Instant},
};
use tickwire_account_gateway::{AccountGateway, AuthorityError, ConnectionConfig};
use tickwire_account_journal::FileBackend;
use tickwire_app::{market::Sources, strategy::AppStrategy};
use tickwire_binance_clock::{LocalTime, SystemClock};
use tickwire_clock::MonoTime;
use tickwire_market_source::{FeedObservation, Invalidation, MarketSink, MarketSource};
use tickwire_recovery::{BlobStore, FileBlobStore, SharedBlobStore};
use tickwire_runtime::{
    authority::{GateProvider, LocalAuthority, RecoveryQueue},
    node::{Health, Observer},
};
use tickwire_runtime_policy::NodeObserver;
use tickwire_spot_execution::{
    RestSession, SpotEncoder, authority,
    network::{self, OwnedCredentials},
    private::stream::{Channel, EventKind, PrivateStream, StreamConfig},
};
use tickwire_spot_runner::{
    AccountRunner,
    reconcile::{CashUpdate, CashVerifier, PrivateCut},
};
use tickwire_transport::{IoBudget, Limits};

const MAX_PRIVATE_AGE_NS: u64 = 45_000_000_000;
const MAX_MARKET_AGE_NS: u64 = 1_000_000_000;
const GRANT_NS: u64 = 60_000_000_000;
const RETRY: Duration = Duration::from_secs(2);

struct Markets(NodeObserver);
impl MarketSink for Markets {
    fn observed(&mut self, update: FeedObservation) -> bool {
        self.0.market_observed(update)
    }
    fn invalidated(&mut self, update: Invalidation) -> bool {
        self.0.market_invalidated(update)
    }
}

fn signing_time(clock: Option<(u64, Instant)>) -> tickwire_gateway::Result<u64> {
    let (ms, at) = clock.ok_or(tickwire_gateway::Error::Frozen)?;
    let age = at.elapsed();
    if age >= Duration::from_secs(60) {
        return Err(tickwire_gateway::Error::Frozen);
    }
    ms.checked_add(age.as_millis() as u64)
        .ok_or(tickwire_gateway::Error::Invalid)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    Trading,
    Reconcile,
}

pub(super) fn run(path: &Path, mode: Mode) -> Result<()> {
    let cfg = Deployment::read(path)?;
    let trading = mode == Mode::Trading;
    let run_strategies = trading
        && !(cfg.strategies.is_empty()
            && cfg.policy.mode == tickwire_runtime_policy::Mode::Stopped);
    let (base, venue) = cfg.validate(run_strategies)?;
    let env = base.snapshot().scope.environment;
    let credentials = OwnedCredentials::load(venue)?;
    let tls = network::roots()?;
    let rest_host = authority(venue, env);
    let mut addresses = (rest_host, 443).to_socket_addrs()?;
    let address = addresses
        .next()
        .ok_or("no address for the spot REST host")?;
    let connection = || ConnectionConfig {
        address,
        tls: tls.clone(),
        limits: Limits::default(),
        connect_deadline: Instant::now() + Duration::from_secs(10),
    };
    let root = &cfg.state_directory;
    std::fs::create_dir_all(root)?;
    let account_dir = root.join("account");
    let backend = FileBackend::open(&account_dir)?;
    let existing = account_dir.join("ACCOUNT-BASELINE.cbor").try_exists()?;
    let mut objects = SharedBlobStore::new(FileBlobStore::open(root.join("objects"))?);
    cfg.bind(&mut objects, &base, existing)?;
    let mut verifier = CashVerifier::new(
        FileBlobStore::open(root.join("reconciliation"))?,
        &cfg.expected_uid,
        cfg.baseline_utc_ms,
    )?;
    let plane = cfg.policy.build().map_err(protocol_error)?;
    let domain = cfg.policy.domain().map_err(protocol_error)?;
    let mut clock = SystemClock::new(domain).map_err(protocol_error)?;
    let mut strategy = run_strategies
        .then(|| cfg.strategies(plane.clone()))
        .transpose()?;
    let mut market = run_strategies
        .then(|| {
            Sources::connect(
                &cfg.market_sources,
                domain,
                cfg.policy.executor_id,
                cfg.policy.owner_generation,
            )
        })
        .transpose()?;
    let mut sink = Markets(plane.observer());
    let private_clock = std::cell::RefCell::new(SystemClock::new(domain).map_err(protocol_error)?);
    let stream = PrivateStream::start(
        StreamConfig {
            directory: root.join("private"),
            scope: base.snapshot().scope.clone(),
            uid: cfg.expected_uid.clone(),
            stream_id: cfg.private_stream,
            boot_id: domain.boot_id(),
        },
        &credentials.credentials(),
        tls.clone(),
        move || {
            private_clock
                .borrow_mut()
                .sample()
                .map_or(0, |s| s.monotonic.nanos())
        },
    )?;
    let reader = stream.reader();
    verifier.bind_private(reader.clone());
    let permission = Rc::new(Cell::new(None::<PrivateCut>));
    let signing = Rc::new(Cell::new(None::<(u64, Instant)>));
    let gate = {
        let reader = reader.clone();
        let permission = permission.clone();
        let market = market.clone();
        let mut gate = cfg.policy.native_provider(&plane).map_err(protocol_error)?;
        move |command: &tickwire_venue::CanonicalCommand,
              admission: &tickwire_account::Admission| {
            let gate = gate.current(command, admission)?;
            if admission.increases_risk()
                && !market.as_ref().is_some_and(|source| {
                    source.is_live(MonoTime::new(domain, gate.now), MAX_MARKET_AGE_NS)
                })
            {
                return Err(AuthorityError::Denied);
            }
            let cut = permission.get().ok_or(AuthorityError::Denied)?;
            if !reader
                .status()
                .is_some_and(|s| s.current(cut.epoch, cut.through, gate.now, MAX_PRIVATE_AGE_NS))
            {
                return Err(AuthorityError::Denied);
            }
            Ok(gate)
        }
    };
    let queue = RecoveryQueue::new(32, 64 * 1024 * 1024).map_err(protocol_error)?;
    let auth = LocalAuthority::new(objects.clone(), queue.clone(), gate, cfg.policy.executor_id);
    let server_time = signing.clone();
    let encoder = SpotEncoder::new(
        credentials.credentials(),
        env,
        base.snapshot().scope.account,
        move || signing_time(server_time.get()),
    )
    .map_err(protocol_error)?;
    let settings = tickwire_account_gateway::Config {
        journal: tickwire_account_journal::Config {
            physical: tickwire_journal::Config::new(
                cfg.account_stream,
                base.snapshot().scope.risk_domain_id,
            ),
            baseline_digest: base.digest(),
            initial_writer_epoch: cfg.initial_writer_epoch,
            scope: base.snapshot().scope.clone(),
        },
        writer_epoch: cfg.policy.writer_epoch,
        route: encoder.route(),
        max_intents: tickwire_account::MAX_ORDERS,
    };
    let baseline_digest = base.digest();
    let gateway = if existing {
        AccountGateway::recover_with_reactivation(
            backend,
            settings,
            auth,
            encoder,
            None,
            &mut verifier,
        )?
    } else {
        AccountGateway::create(backend, settings, base, auth, encoder, connection())?
    };
    objects.put_sync(config::initialized(), &baseline_digest)?;
    let mut runner = AccountRunner::new(RestSession::new(gateway, 1)?, queue, objects);
    runner.freeze_cash();
    let scanner = Scanner::start(
        credentials.clone(),
        env,
        tls.clone(),
        cfg.expected_uid.clone(),
    )?;
    let mut owner = PrivateAccount {
        runner,
        verifier,
        reader,
        plane,
        permission,
        signing,
        writer_epoch: cfg.policy.writer_epoch,
    };
    let mut after = owner.resume()?;
    let mut outstanding: Option<(PrivateCut, u64)> = None;
    let mut retry_at = Instant::now();
    let mut renew_at = retry_at;
    let mut check_stop_at = retry_at;
    let timeout = retry_at + Duration::from_secs(180);
    loop {
        if Instant::now() >= check_stop_at {
            if cfg.stop_file.try_exists()? {
                if !trading {
                    return Err("spot recovery stopped before account qualification".into());
                }
                break;
            }
            check_stop_at = Instant::now() + Duration::from_millis(100);
        }
        if !trading && Instant::now() >= timeout {
            return Err("private recovery timed out".into());
        }
        if owner.reader.failed() {
            return Err("private capture worker stopped".into());
        }
        let time = clock.sample().map_err(protocol_error)?;
        let now = LocalTime {
            instant: time.instant,
            monotonic: time.monotonic,
        };
        let market_work = match market.as_mut() {
            Some(market) => market.poll(now.monotonic, MAX_MARKET_AGE_NS, 64, &mut sink)?,
            None => Default::default(),
        };
        after = owner.drain(after)?;
        let current = owner.current(after, now.monotonic);
        if owner.permission.get().is_some()
            && (owner.runner.account().frozen()
                || now.monotonic.nanos() >= owner.runner.account().valid_until())
        {
            owner.permission.set(None);
            owner.runner.freeze_cash();
        }
        if let Some(response) = scanner.poll()? {
            let (cut, position) = outstanding.take().ok_or("unexpected recovery response")?;
            retry_at = Instant::now() + RETRY;
            if let Some(observed) = current.filter(|observed| *observed == cut)
                && owner.runner.position() == position
                && !owner.runner.has_pending()
            {
                let activated = response.map_err(Into::into).and_then(|scan| {
                    owner.activate([cut, observed], scan, &mut clock, &connection)
                });
                match activated {
                    Ok(true) => {
                        renew_at = Instant::now() + Duration::from_secs(30);
                        println!(
                            "{}",
                            serde_json::json!({"event":"spot_account_reconciled","private_epoch":cut.epoch,"private_sequence":cut.through,"account_sequence":owner.runner.position()})
                        );
                        if !trading {
                            break;
                        }
                    }
                    Ok(false) => {}
                    Err(error) if !trading => return Err(error),
                    Err(error) => {
                        owner.permission.set(None);
                        owner.runner.freeze_cash();
                        owner
                            .plane
                            .private_uncertain(cut.epoch)
                            .map_err(protocol_error)?;
                        eprintln!("spot recovery: {error}");
                    }
                }
            }
        }
        if outstanding.is_none()
            && !owner.runner.has_pending()
            && now.instant >= retry_at
            && (owner.permission.get().is_none() || now.instant >= renew_at)
            && let Some(cut) = current
        {
            if owner.permission.get().is_none() {
                owner.runner.freeze_cash();
                owner
                    .plane
                    .private_uncertain(cut.epoch)
                    .map_err(protocol_error)?;
            }
            scanner.request(Scan {
                start_ms: owner.runner.coverage_start(&owner.verifier)?,
                orders: owner.runner.recovery_targets()?,
            })?;
            outstanding = Some((cut, owner.runner.position()));
        }
        let time = clock.sample().map_err(protocol_error)?;
        let now = LocalTime {
            instant: time.instant,
            monotonic: time.monotonic,
        };
        if owner.runner.transport_needs_poll()
            && let Some(strategy) = strategy.as_mut()
        {
            owner.runner.poll(strategy, now, IoBudget::default())?;
        }
        if owner.signing.get().is_some() && owner.runner.halted() {
            owner.permission.set(None);
            return Err("execution transport halted; restart with a newer approved writer_epoch to reconcile outstanding orders".into());
        }
        if owner.permission.get().is_some()
            && owner.permission.get() == current
            && !owner.runner.halted()
            && let (Some(strategy), Some(market)) = (strategy.as_mut(), market.as_ref())
        {
            let healthy = Health {
                calibrated: signing_time(owner.signing.get()).is_ok(),
                market_live: market.is_live(now.monotonic, MAX_MARKET_AGE_NS),
                private_receiving: true,
                account_frozen: owner.runner.account().frozen(),
                sender_halted: false,
            };
            owner.runner.poll_strategy(strategy, healthy, now)?;
        }
        if strategy.as_ref().is_some_and(AppStrategy::failed) {
            return Err("live strategy group failed".into());
        }
        if !market_work.runnable && !owner.runner.has_pending() {
            std::thread::sleep(Duration::from_micros(100));
        } else {
            std::thread::yield_now();
        }
    }
    owner.permission.set(None);
    stream.stop()?;
    Ok(())
}
