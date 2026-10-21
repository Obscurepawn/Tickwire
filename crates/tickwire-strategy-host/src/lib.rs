//! Parent-owned strategy process, real JOIN and SPSC command intake.
#![deny(unsafe_code)]
use std::{
    cell::RefCell,
    fs::{File, OpenOptions},
    io::Write,
    os::fd::AsFd,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::{Path, PathBuf},
    rc::Rc,
    time::{Duration, Instant},
};
use tickwire_clock_sys::SystemClock;
use tickwire_control::{ClientHandshake, Identity, Policy, ProcessIdentity, Session};
use tickwire_control_sys::{ControlListener, PrivateDirectory};
use tickwire_ipc::{ControlEnvelope, JoinBody, ResourceHeader, Wire};
use tickwire_join::{PrepareOutcome, SubscriberRegistration};
use tickwire_market_publisher::{Fault, FaultSink, TopicPublisher};
use tickwire_runtime_policy::{ControlPlane, FeedKey, FeedUpdate};
use tickwire_strategy_sdk::reports::{ReportIdentity, ReportWriter};
use tickwire_strategy_sdk::{
    bootstrap::{Bootstrap, JoinBootstrap, wire_bytes},
    runtime::{IntentReceiver, RecoveryResolver, RuntimeStrategy},
    worker,
};
use tickwire_supervisor::{AuthenticatedChild, LaunchSpec, Registry};
mod preflight;
mod readiness;
mod remote;
mod source_replay;
pub mod rebuild_probe;
pub use remote::{RemoteHostedStrategy, RemoteLaunch};
type Reports = ReportWriter<tickwire_recovery::FileBlobStore>;

fn retire_owned_child(registry: &mut Registry, instance: [u8; 16]) -> Result<()> {
    let exited = registry.terminate(instance)?;
    eprintln!(
        "{}",
        serde_json::json!({"event":"strategy_retired","instance":exited.instance(),
            "generation":exited.generation(),"worker_pid":exited.pid(),"exit_status":exited.status().to_string()})
    );
    Ok(())
}

#[cfg(test)]
mod retirement_tests {
    use super::*;

    #[test]
    fn retirement_requires_an_actual_owned_child_exit() {
        let mut registry = Registry::new(1).unwrap();
        assert!(matches!(
            retire_owned_child(&mut registry, [7; 16]),
            Err(Error::Supervisor(tickwire_supervisor::Error::Identity))
        ));
    }
}

#[derive(Debug)]
pub enum Error {
    Release(tickwire_release::Error),
    Source(tickwire_market_source::Error),
    Invalid,
    NotReady,
    Busy,
    Io(std::io::Error),
    Sdk(tickwire_strategy_sdk::Error),
    Supervisor(tickwire_supervisor::Error),
    Control(tickwire_control::Error),
    Transport(tickwire_control_sys::Error),
    Join(tickwire_join::Error),
    Wire(tickwire_ipc::Error),
    Clock(tickwire_clock_sys::Error),
    Policy(tickwire_runtime_policy::Error),
    Market(tickwire_market_publisher::Error),
    Ring(tickwire_shm::MappedError),
    Storage(tickwire_recovery::Error),
}
pub type Result<T> = std::result::Result<T, Error>;
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
macro_rules! from {
    ($ty:ty,$v:ident) => {
        impl From<$ty> for Error {
            fn from(e: $ty) -> Self {
                Self::$v(e)
            }
        }
    };
}
from!(tickwire_market_source::Error, Source);
from!(tickwire_release::Error, Release);
from!(std::io::Error, Io);
from!(tickwire_strategy_sdk::Error, Sdk);
from!(tickwire_supervisor::Error, Supervisor);
from!(tickwire_control::Error, Control);
from!(tickwire_control_sys::Error, Transport);
from!(tickwire_join::Error, Join);
from!(tickwire_ipc::Error, Wire);
from!(tickwire_clock_sys::Error, Clock);
from!(tickwire_runtime_policy::Error, Policy);
from!(tickwire_market_publisher::Error, Market);
from!(tickwire_shm::MappedError, Ring);
from!(tickwire_recovery::Error, Storage);

#[derive(Clone)]
pub struct SharedPublisher<const M: usize>(Rc<RefCell<TopicPublisher<M>>>);
impl<const M: usize> SharedPublisher<M> {
    pub fn new(publisher: TopicPublisher<M>) -> Self {
        Self(Rc::new(RefCell::new(publisher)))
    }
    pub fn with<T>(&self, f: impl FnOnce(&mut TopicPublisher<M>) -> Result<T>) -> Result<T> {
        {
            let mut p = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
            f(&mut p)
        }
    }
    pub fn live(&self) -> bool {
        self.0
            .try_borrow()
            .is_ok_and(|p| p.state().live() && !p.needs_bank())
    }
}
struct Invalidate {
    policy: ControlPlane,
    ring: u64,
    failed: bool,
}
impl FaultSink for Invalidate {
    fn invalidate(&mut self, f: Fault) {
        if self
            .policy
            .invalidate_feed(
                FeedKey {
                    topic_id: f.topic,
                    instrument_id: f.instrument,
                },
                f.feed_generation,
                self.ring,
            )
            .is_err()
        {
            self.failed = true;
        }
    }
}
/// Uses actual SHM sequence numbers, publishing time, and immediate independent gap invalidation.
pub struct PublicSink<const M: usize> {
    publisher: SharedPublisher<M>,
    policy: ControlPlane,
    instrument: u64,
    version: u64,
    clock: SystemClock,
    error: Option<Error>,
}
impl<const M: usize> PublicSink<M> {
    pub fn new(
        publisher: SharedPublisher<M>,
        policy: ControlPlane,
        instrument: u64,
        version: u64,
        clock: SystemClock,
    ) -> Self {
        Self {
            publisher,
            policy,
            instrument,
            version,
            clock,
            error: None,
        }
    }
    pub fn error(&self) -> Option<&Error> {
        self.error.as_ref()
    }
    pub fn notice(&mut self, n: &tickwire_binance_public::Notice<'_>) -> bool {
        if self.error.is_some() {
            return false;
        }
        match self.publish(n) {
            Ok(()) => true,
            Err(e) => {
                self.error = Some(e);
                false
            }
        }
    }
    fn publish(&mut self, n: &tickwire_binance_public::Notice<'_>) -> Result<()> {
        let sample = self.clock.sample()?;
        let now = sample.monotonic.nanos();
        let mut publisher = self.publisher.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        let (owner, _) = publisher.split();
        let h = owner.header().clone();
        let before = owner.published();
        let mut faults = Invalidate {
            policy: self.policy.clone(),
            ring: h.ring_epoch,
            failed: false,
        };
        let result = publisher.on_notice_ref(self.instrument, n, now, &mut faults);
        if result.is_err() || publisher.needs_bank() {
            let _ = self.policy.invalidate_feed(
                FeedKey {
                    topic_id: h.topic_id,
                    instrument_id: self.instrument,
                },
                h.feed_generation,
                h.ring_epoch,
            );
            // The source may continue collecting a fresh book while the invalidated bank is retained.
            return if matches!(result, Err(tickwire_market_publisher::Error::Clock)) {
                Err(Error::Invalid)
            } else {
                Ok(())
            };
        }
        result?;
        let after = publisher.state().sequence();
        if publisher.state().live() {
            for sequence in before + 1..=after {
                self.policy.update_feed(FeedUpdate {
                    key: FeedKey {
                        topic_id: h.topic_id,
                        instrument_id: self.instrument,
                    },
                    instrument_version: self.version,
                    feed_generation: h.feed_generation,
                    ring_epoch: h.ring_epoch,
                    sequence,
                    published_at: sample.monotonic,
                })?;
            }
        }
        Ok(())
    }
}
impl<const M: usize> tickwire_binance_public::PublicSink for PublicSink<M> {
    fn on_notice(&mut self, n: tickwire_binance_public::Notice<'_>) -> bool {
        self.notice(&n)
    }
}

/// All identities and resource templates come from the control plane. The worker
/// executable hash is verified by Registry before exec; child PID is verified after connect.
pub struct Launch {
    pub spec: LaunchSpec,
    pub bootstrap: Bootstrap,
    pub bootstrap_file: PathBuf,
    pub directory_parent: PathBuf,
    pub supervisor: Identity,
    pub parent_process: ProcessIdentity,
    pub registration: SubscriberRegistration,
    pub snapshot_id: [u8; 16],
    pub request_directory: PathBuf,
    pub requests: tickwire_control_store::Config,
}
pub struct ChildHandles {
    domain: tickwire_clock::ClockDomain,
    subscription: tickwire_shm::SubscriptionKey,
    instance: [u8; 16],
    registry: Registry,
    pub market: AuthenticatedChild,
    pub intent: AuthenticatedChild,
    pub report: AuthenticatedChild,
    _directory: PrivateDirectory,
    _market_listener: ControlListener,
    _intent_listener: ControlListener,
    _report_listener: ControlListener,
    _requests: tickwire_control_store::DurableRequests<tickwire_journal::store::FileStore>,
}
impl ChildHandles {
    pub fn registry(&mut self) -> &mut Registry {
        &mut self.registry
    }
    pub fn process(&self) -> ProcessIdentity {
        self.market.process
    }
}
pub type MarketCallback = Box<dyn FnMut(&tickwire_binance_public::Notice<'_>) -> bool>;
pub struct Started<R, const M: usize> {
    readiness: readiness::ReadyProof,
    report_sub: tickwire_shm::SubscriptionKey,
    reports: Reports,
    pub publisher: SharedPublisher<M>,
    pub strategy: RuntimeStrategy<R, MarketCallback>,
    pub child: ChildHandles,
}
fn write_json<T: serde::Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec(value).map_err(|_| Error::Invalid)?;
    if bytes.len() > 2 * 1024 * 1024 {
        return Err(Error::Invalid);
    }
    let temp = path.with_extension("pending");
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&temp)?;
    f.write_all(&bytes)?;
    f.sync_all()?;
    std::fs::rename(&temp, path)?;
    File::open(path.parent().ok_or(Error::Invalid)?)?.sync_all()?;
    Ok(())
}
fn server_policy(
    client: &tickwire_strategy_sdk::bootstrap::Endpoint,
    child: ProcessIdentity,
) -> Policy {
    let mut p = client.policy();
    std::mem::swap(&mut p.local, &mut p.peer);
    p.peer_process = child;
    p
}
fn supervisor_session(launch: &Launch, market: &Identity, now: u64) -> Result<(Session, Session)> {
    let mut client = launch.bootstrap.market.policy();
    client.local = launch.supervisor.clone();
    client.peer = market.clone();
    client.peer_process = launch.parent_process;
    client.nonce[0] ^= 0x80;
    let mut server = client.clone();
    server.local = market.clone();
    server.peer = launch.supervisor.clone();
    let hello = ClientHandshake::new(client, [10; 16])?;
    let (server, response) = Session::accept(&server, launch.parent_process, hello.hello(), now)?;
    let client = hello.finish(launch.parent_process, &response, now)?;
    Ok((client, server))
}
fn request_join(
    h: &ResourceHeader,
    registration: &SubscriberRegistration,
    registry: &tickwire_join::TopicRegistry,
    deadline: u64,
) -> JoinBody {
    JoinBody {
        subscriber_instance: registration.identity.instance,
        logical_strategy_id: registration.identity.logical_strategy,
        subscription_generation: registration.subscription.generation,
        topic_id: h.topic_id,
        channel_id: h.channel_id,
        channel_epoch: h.channel_epoch,
        feed_generation: h.feed_generation,
        ring_epoch: h.ring_epoch,
        cursor_resource_id: registration.subscription.cursor.to_le_bytes(),
        bank_resource_id: h.resource_id,
        readiness_deadline_mono_ns: deadline,
        expected_catalog_version: h.catalog_version,
        qos: h.qos,
        role: 3,
        join_flags: 3,
        topic_instrument_set_version: registry.instrument_set_version,
        topic_instrument_set_hash: tickwire_join::instrument_set_hash(&registry.instruments),
        ..Default::default()
    }
}
/// Launches only after the complete topic is Live. Caller retains the returned
/// handles for both connections and the owned Child throughout runtime execution.
///
/// # Safety
/// The approved build/qualification hashes identify qualified strategy and
/// publisher SDKs. No peer bypasses the SHM immutable header or cursor contracts.
#[allow(unsafe_code)]
pub unsafe fn launch<R: RecoveryResolver + 'static, const M: usize>(
    mut config: Launch,
    publisher: SharedPublisher<M>,
    resolver: R,
    policy: ControlPlane,
    instrument: u64,
    instrument_version: u64,
) -> Result<Started<R, M>> {
    if config.spec.uid != config.parent_process.uid
        || config.spec.gid != config.parent_process.gid
        || config.parent_process.pid != std::process::id()
        || config.supervisor.role != 1
        || config.spec.identity != config.bootstrap.market.local.identity()
        || config.spec.identity != config.bootstrap.intent.local.identity()
        || config.spec.identity != config.bootstrap.report.local.identity()
        || config.bootstrap.report.peer.identity() != config.bootstrap.intent.peer.identity()
        || !publisher.live()
    {
        return Err(Error::NotReady);
    }
    let domain = tickwire_clock::ClockDomain::new(
        config.spec.identity.host_boot,
        config.bootstrap.clock_generation,
    )
    .map_err(|_| Error::Invalid)?;
    let mut clock = SystemClock::new(domain)?;
    let now = clock.sample()?.monotonic.nanos();
    let timeout = Duration::from_millis(config.bootstrap.startup_timeout_ms);
    let deadline = Instant::now().checked_add(timeout).ok_or(Error::Invalid)?;
    let directory = PrivateDirectory::create_in(&config.directory_parent, "tickwire-strategy")?;
    config.bootstrap.market.path = directory.path().join("market.sock");
    config.bootstrap.intent.path = directory.path().join("intent.sock");
    config.bootstrap.report.path = directory.path().join("report.sock");
    config.bootstrap.join_file = directory.path().join("join.json");
    config.bootstrap_file = directory.path().join("worker.json");
    // The child must be able to read its own command directory; the execution owner
    // independently verifies file owner/mode/hash through FileBlobReader.
    match std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&config.bootstrap.command_directory)
    {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    for endpoint in [
        &mut config.bootstrap.market,
        &mut config.bootstrap.intent,
        &mut config.bootstrap.report,
    ] {
        endpoint.peer_pid = config.parent_process.pid;
        endpoint.peer_uid = config.parent_process.uid;
        endpoint.peer_gid = config.parent_process.gid;
    }
    let market_listener = ControlListener::bind(&config.bootstrap.market.path)?;
    let intent_listener = ControlListener::bind(&config.bootstrap.intent.path)?;
    let report_listener = ControlListener::bind(&config.bootstrap.report.path)?;
    let report_header = config.bootstrap.report_resource()?;
    if report_header.producer_instance != config.bootstrap.report.peer.instance
        || report_header.producer_generation != config.bootstrap.report.peer.generation
        || report_header.consumer_instance != config.spec.identity.instance
        || report_header.host_boot_id != config.spec.identity.host_boot
        || report_header.namespace_id != config.spec.identity.namespace
    {
        return Err(Error::Invalid);
    }
    let mut report_publisher = tickwire_shm::Publisher::<1>::create(report_header)?;
    config.bootstrap.report_header = wire_bytes(report_publisher.header())?;
    let report_objects =
        tickwire_recovery::FileBlobStore::open(&config.bootstrap.report_directory)?;
    let file_store = tickwire_journal::store::FileStore::open(&config.request_directory)?;
    let mut requests = if config.request_directory.join("MANIFEST.json").exists() {
        tickwire_control_store::DurableRequests::recover(file_store, config.requests.clone(), None)?
    } else {
        tickwire_control_store::DurableRequests::create(file_store, config.requests.clone())?
    };
    let market_identity = config.bootstrap.market.peer.identity();
    let (supervisor, mut market_control) = supervisor_session(&config, &market_identity, now)?;
    let j = publisher.with(|p| {
        let (owner, state) = p.split();
        let readiness = now
            .checked_add(u64::try_from(timeout.as_nanos()).map_err(|_| Error::Invalid)?)
            .ok_or(Error::Invalid)?
            .min(market_control.deadline());
        let request = request_join(
            owner.header(),
            &config.registration,
            owner.registry(),
            readiness,
        );
        let frame = supervisor.encode(
            ControlEnvelope {
                opcode: 16,
                flags: 1,
                request_id: [11; 16],
                expected_state_version: owner.membership_version(),
                ..Default::default()
            },
            &wire_bytes(&request)?,
        )?;
        let outcome = owner.prepare(
            &mut market_control,
            &mut requests,
            &frame,
            &config.registration,
            config.snapshot_id,
            now,
            |plan| state.snapshot(plan, now, readiness),
        )?;
        if !matches!(outcome, PrepareOutcome::Prepared { .. }) {
            return Err(Error::NotReady);
        }
        let j = owner.join(config.registration.subscription)?.clone();
        let bootstrap = JoinBootstrap::new(
            owner.header(),
            owner.snapshot_header(config.registration.subscription)?,
            &j,
            owner.registry(),
        )?;
        write_json(&config.bootstrap.join_file, &bootstrap)?;
        Ok(j)
    })?;
    preflight::prepare(&config.bootstrap, &config.spec)?;
    write_json(&config.bootstrap_file, &config.bootstrap)?;
    config.spec.arguments = vec![
        "--bootstrap".into(),
        config.bootstrap_file.to_str().ok_or(Error::Invalid)?.into(),
    ];
    let mut registry = Registry::new(1)?;
    let child = registry.spawn(config.spec.clone())?;
    let mut market = registry.accept(
        &market_listener,
        server_policy(&config.bootstrap.market, child),
        clock.sample()?.monotonic.nanos(),
        deadline,
    )?;
    publisher.with(|p| {
        p.split().0.deliver(
            config.registration.subscription,
            &mut market.session,
            &mut market.connection,
            clock.sample()?.monotonic.nanos(),
            deadline,
        )?;
        Ok(())
    })?;
    let cursor = market.connection.recv(deadline)?;
    publisher.with(|p| {
        // SAFETY: Registry authenticated the exact live child and verified its approved executable hash.
        unsafe {
            p.split().0.receive_cursor(
                config.registration.subscription,
                &mut market.session,
                cursor,
                clock.sample()?.monotonic.nanos(),
            )?;
        }
        Ok(())
    })?;
    for _ in 0..2 {
        let packet = market.connection.recv(deadline)?;
        if !packet.fds().is_empty() {
            return Err(Error::Invalid);
        }
        publisher.with(|p| {
            p.split().0.read_ready(
                config.registration.subscription,
                &mut market.session,
                packet.bytes(),
                clock.sample()?.monotonic.nanos(),
            )?;
            Ok(())
        })?;
    }
    let ready = market.connection.recv(deadline)?;
    if !ready.fds().is_empty() {
        return Err(Error::Invalid);
    }
    let published = publisher.with(|p| {
        let (owner, _) = p.split();
        owner.join_ready(
            config.registration.subscription,
            &mut market.session,
            ready.bytes(),
            clock.sample()?.monotonic.nanos(),
        )?;
        Ok(owner.published())
    })?;
    let envelope = ControlEnvelope::decode(&ready.bytes()[..128])?;
    let ack = worker::activation_ack(&market.session, &envelope, &j, published)?;
    market.connection.send(&ack, &[], deadline)?;
    // Registry owns this same child; a second lane does not manufacture another process identity.
    let expected = registry.process(child_instance(&config))?;
    let mut connection = intent_listener.accept(deadline)?;
    let actual = connection.peer_credentials();
    let actual = ProcessIdentity {
        pid: actual.pid,
        uid: actual.uid,
        gid: actual.gid,
    };
    if actual != expected {
        return Err(Error::Invalid);
    }
    let hello = connection.recv(deadline)?;
    if !hello.fds().is_empty() {
        return Err(Error::Invalid);
    }
    let (mut session, welcome) = Session::accept(
        &server_policy(&config.bootstrap.intent, child),
        actual,
        hello.bytes(),
        clock.sample()?.monotonic.nanos(),
    )?;
    connection.send(&welcome, &[], deadline)?;
    registry.process(child_instance(&config))?;
    let mut bank = config.bootstrap.intent_resource()?;
    bank.descriptor_hash = [0; 32];
    // Publisher computes this immutable digest before creating the actual mapping.
    bank.descriptor_hash = header_hash(&bank)?;
    let membership = bank
        .initial_membership_version
        .checked_add(1)
        .ok_or(Error::Invalid)?;
    let expires = session.deadline();
    let fd = worker::accept_offer(
        &mut session,
        connection.recv(deadline)?,
        &bank,
        [7; 16],
        expires,
        membership,
        false,
        clock.sample()?.monotonic.nanos(),
    )?;
    let sub = tickwire_shm::SubscriptionKey {
        bank: tickwire_shm::BankKey {
            resource: u128::from_le_bytes(bank.resource_id),
            channel: u128::from_le_bytes(bank.channel_id),
            channel_epoch: bank.channel_epoch,
            ring_epoch: bank.ring_epoch,
            feed_generation: bank.feed_generation,
        },
        instance: u128::from_le_bytes(config.bootstrap.intent.peer.instance),
        generation: config.bootstrap.intent_subscription_generation,
        cursor: u128::from_le_bytes(config.bootstrap.intent_cursor_id),
    };
    let objects = tickwire_recovery::FileBlobReader::open(
        &config.bootstrap.command_directory,
        config.spec.uid,
    )?;
    // SAFETY: the same actual authenticated child is the sole writer and uses the qualified worker SDK.
    let receiver = unsafe {
        IntentReceiver::attach(
            fd,
            bank.clone(),
            sub,
            objects,
            config.bootstrap.risk_domain,
            config.spec.identity.logical_strategy,
        )?
    };
    let cursor_header = worker::cursor_header(&bank, sub)?;
    let cursor = receiver.cursor_fd()?;
    let offered = worker::resource_offer(
        &session,
        &cursor_header,
        [8; 16],
        session.deadline(),
        membership,
        false,
    )?;
    connection.send(&offered, &[cursor.as_fd()], deadline)?;
    let expected = registry.process(child_instance(&config))?;
    let mut report_connection = report_listener.accept(deadline)?;
    let credentials = report_connection.peer_credentials();
    let report_process = ProcessIdentity {
        pid: credentials.pid,
        uid: credentials.uid,
        gid: credentials.gid,
    };
    if report_process != expected {
        return Err(Error::Invalid);
    }
    let hello = report_connection.recv(deadline)?;
    if !hello.fds().is_empty() {
        return Err(Error::Invalid);
    }
    let (mut report_session, welcome) = Session::accept(
        &server_policy(&config.bootstrap.report, child),
        report_process,
        hello.bytes(),
        clock.sample()?.monotonic.nanos(),
    )?;
    report_connection.send(&welcome, &[], deadline)?;
    registry.process(child_instance(&config))?;
    let report_sub = tickwire_shm::SubscriptionKey {
        bank: report_publisher.key(),
        instance: u128::from_le_bytes(config.spec.identity.instance),
        generation: config.bootstrap.report_subscription_generation,
        cursor: u128::from_le_bytes(config.bootstrap.report_cursor_id),
    };
    report_publisher.join(report_sub)?;
    let membership = report_publisher.membership_version();
    let expiry = report_session.deadline();
    let offer = worker::resource_offer(
        &report_session,
        report_publisher.header(),
        [14; 16],
        expiry,
        membership,
        false,
    )?;
    let fd = report_publisher.reader_fd()?;
    report_connection.send(&offer, &[fd.as_fd()], deadline)?;
    let ch = worker::cursor_header(report_publisher.header(), report_sub)?;
    let cursor = worker::accept_offer(
        &mut report_session,
        report_connection.recv(deadline)?,
        &ch,
        [15; 16],
        expiry,
        membership,
        false,
        clock.sample()?.monotonic.nanos(),
    )?;
    // SAFETY: this is the exact owned, qualified child and authenticated cursor.
    unsafe {
        report_publisher.register_cursor(report_sub, cursor)?;
    }
    let reports = ReportWriter::new(
        report_publisher,
        report_objects,
        ReportIdentity {
            logical_strategy_id: config.spec.identity.logical_strategy,
            instance_id: config.spec.identity.instance,
            strategy_generation: config.spec.identity.generation,
            risk_domain: config.bootstrap.risk_domain,
        },
    )?;
    let sink = PublicSink::new(
        publisher.clone(),
        policy,
        instrument,
        instrument_version,
        SystemClock::new(domain)?,
    );
    let mut sink = sink;
    let callback: Box<dyn FnMut(&tickwire_binance_public::Notice<'_>) -> bool> =
        Box::new(move |n| sink.notice(n));
    let strategy = RuntimeStrategy::new(receiver, resolver, callback);
    Ok(Started {
        readiness: readiness::ReadyProof::new(&config.bootstrap, &config.spec)?,
        report_sub,
        reports,
        publisher,
        strategy,
        child: ChildHandles {
            domain,
            subscription: config.registration.subscription,
            instance: config.spec.identity.instance,
            registry,
            market,
            intent: AuthenticatedChild {
                connection,
                session,
                process: actual,
            },
            report: AuthenticatedChild {
                connection: report_connection,
                session: report_session,
                process: report_process,
            },
            _directory: directory,
            _market_listener: market_listener,
            _intent_listener: intent_listener,
            _report_listener: report_listener,
            _requests: requests,
        },
    })
}
fn child_instance(config: &Launch) -> [u8; 16] {
    config.spec.identity.instance
}
fn header_hash(h: &ResourceHeader) -> Result<[u8; 32]> {
    use sha2::{Digest, Sha256};
    Ok(Sha256::digest(wire_bytes(h)?).into())
}
/// Lazy runtime assembly: market collection runs first; the child starts once a
/// complete Live snapshot exists. Subsequent errors stop this source and preserve
/// its old resources until the owned process has exited.
pub struct HostedStrategy<R, const M: usize> {
    publisher: SharedPublisher<M>,
    sink: PublicSink<M>,
    policy: ControlPlane,
    launch: Option<Launch>,
    resolver: Option<R>,
    started: Option<Started<R, M>>,
    instrument: u64,
    version: u64,
    error: Option<Error>,
    logical_strategy_id: u64,
    identity: ([u8; 16], u64),
    forwarding: bool,
    domain: tickwire_clock::ClockDomain,
    rotation: Option<Box<dyn RotationSource<R, M>>>,
    retired: Vec<tickwire_join::JoinOwner<M>>,
}
impl<R: RecoveryResolver + 'static, const M: usize> HostedStrategy<R, M> {
    /// # Safety
    /// The launcher identities and build qualifications satisfy `launch`'s SHM peer contracts.
    #[allow(unsafe_code)]
    pub unsafe fn new(
        config: Launch,
        publisher: SharedPublisher<M>,
        resolver: R,
        policy: ControlPlane,
        instrument: u64,
        version: u64,
    ) -> Result<Self> {
        let domain = tickwire_clock::ClockDomain::new(
            config.spec.identity.host_boot,
            config.bootstrap.clock_generation,
        )
        .map_err(|_| Error::Invalid)?;
        let sink = PublicSink::new(
            publisher.clone(),
            policy.clone(),
            instrument,
            version,
            SystemClock::new(domain)?,
        );
        Ok(Self {
            publisher,
            sink,
            policy,
            logical_strategy_id: config.spec.identity.logical_strategy,
            identity: (
                config.spec.identity.instance,
                config.spec.identity.generation,
            ),
            forwarding: true,
            domain,
            launch: Some(config),
            resolver: Some(resolver),
            started: None,
            instrument,
            version,
            error: None,
            rotation: None,
            retired: Vec::new(),
        })
    }
    pub fn error(&self) -> Option<&Error> {
        self.error.as_ref().or_else(|| self.sink.error())
    }
    pub fn failed(&self) -> bool {
        self.error().is_some()
            || self
                .started
                .as_ref()
                .is_some_and(|s| s.strategy.error().is_some())
    }
    pub fn set_shadow(&mut self) {
        self.forwarding = false;
    }
    pub fn instance(&self) -> Option<([u8; 16], u64)> {
        Some(self.identity)
    }
    pub fn child(&mut self) -> Option<&mut ChildHandles> {
        self.started.as_mut().map(|s| &mut s.child)
    }
    pub fn publisher(&self) -> SharedPublisher<M> {
        self.publisher.clone()
    }
    #[allow(unsafe_code)]
    fn start_if_ready(&mut self) -> Result<()> {
        if self.started.is_some() || !self.publisher.live() {
            return Ok(());
        }
        let config = self.launch.take().ok_or(Error::NotReady)?;
        let resolver = self.resolver.take().ok_or(Error::NotReady)?;
        // SAFETY: HostedStrategy::new requires the same trusted qualified-peer contract.
        self.started = Some(unsafe {
            launch(
                config,
                self.publisher.clone(),
                resolver,
                self.policy.clone(),
                self.instrument,
                self.version,
            )?
        });
        Ok(())
    }
}
impl<R: RecoveryResolver + 'static, const M: usize> tickwire_runtime::node::Strategy
    for HostedStrategy<R, M>
{
    fn handover_quiesce(&mut self, change: &tickwire_runtime::node::StrategySwitch) -> bool {
        if self.logical_strategy_id != change.logical_strategy_id {
            return false;
        }
        if self.instance().is_some_and(|i| {
            i == (change.old_instance, change.old_generation)
                || i == (change.new_instance, change.new_generation)
        }) {
            self.forwarding = false;
            if self.instance() == Some((change.old_instance, change.old_generation))
                && let Some(s) = &mut self.started
            {
                s.readiness.pause_on_capture();
            }
            true
        } else {
            false
        }
    }
    fn handover_activate(&mut self, change: &tickwire_runtime::node::StrategySwitch) -> bool {
        if self.logical_strategy_id != change.logical_strategy_id {
            return false;
        }
        match self.instance() {
            Some(i) if i == (change.new_instance, change.new_generation) => {
                if self.failed() {
                    return false;
                }
                let allowed = SystemClock::new(self.domain)
                    .and_then(|mut c| c.sample())
                    .ok()
                    .is_some_and(|n| {
                        self.policy
                            .strategy_instance_authorized(
                                self.logical_strategy_id,
                                i.0,
                                i.1,
                                n.monotonic,
                            )
                            .unwrap_or(false)
                    });
                if allowed {
                    self.forwarding = true;
                }
                allowed
            }
            Some(i) if i == (change.old_instance, change.old_generation) => {
                self.forwarding = false;
                let result = (|| {
                    self.policy
                        .stop_strategy_instance(self.logical_strategy_id, i.0, i.1)?;
                    if let Some(s) = &mut self.started {
                        // A missing QUIESCED reply may still end in a real child
                        // exit. Only the Registry's exit observation completes
                        // retirement; it does not invent released cursor watermarks.
                        let _ = s.stop(Instant::now() + Duration::from_millis(100));
                        retire_owned_child(&mut s.child.registry, i.0)?;
                    }
                    Ok(())
                })();
                self.launch = None;
                let retired = result.is_ok();
                if retired {
                    self.started.take();
                }
                self.error = Some(result.err().unwrap_or(Error::NotReady));
                retired
            }
            _ => false,
        }
    }
    fn handover_halt(&mut self, logical: u64) -> bool {
        if logical != self.logical_strategy_id {
            return false;
        }
        self.forwarding = false;
        true
    }
    fn release_preflight(
        &mut self,
        instance: [u8; 16],
        generation: u64,
        roots: [[u8; 32]; 3],
    ) -> Option<Vec<Vec<u8>>> {
        if self.identity != (instance, generation) || self.error.is_some() {
            return None;
        }
        let result = (|| -> Result<_> {
            let s = self.started.as_mut().ok_or(Error::NotReady)?;
            s.child.registry.process(s.child.instance)?;
            let now = SystemClock::new(s.child.domain)?
                .sample()?
                .monotonic
                .nanos();
            s.readiness.poll(&mut s.child.report, now)?;
            s.readiness.release_objects(roots)
        })();
        match result {
            Ok(v) => v,
            Err(e) => {
                self.error = Some(e);
                None
            }
        }
    }
    fn handover_checkpoint(
        &mut self,
        instance: [u8; 16],
        generation: u64,
    ) -> Option<(tickwire_runtime::node::HandoverReadiness, Vec<Vec<u8>>)> {
        if self.identity != (instance, generation) || self.error.is_some() {
            return None;
        }
        let s = self.started.as_mut()?;
        let refreshed = (|| -> Result<_> {
            s.child.registry.process(s.child.instance)?;
            let mut clock = SystemClock::new(s.child.domain)?;
            s.readiness
                .capture(&mut s.child.report, &s.reports, s.report_sub, &mut clock)
        })();
        let want = match refreshed {
            Ok(w) => w,
            Err(e) => {
                self.error = Some(e);
                return None;
            }
        };
        let objects = self.handover_objects(&want)?;
        Some((want, objects))
    }
    fn handover_replay_source(
        &mut self,
        operation: &tickwire_release::Operation,
        member: &tickwire_release::Member,
        fixed: &tickwire_runtime::node::HandoverReadiness,
    ) -> Option<tickwire_release::replay::source::VerifiedSourceReplay> {
        if self.identity != (member.old_instance, member.old_generation)
            || self.forwarding
            || self.error.is_some()
        {
            return None;
        }
        let fixed_objects = self.handover_objects(fixed)?;
        let (current, current_objects) =
            self.handover_checkpoint(member.old_instance, member.old_generation)?;
        let result = self.started.as_ref()?.readiness.source_replay(
            operation,
            member,
            fixed.clone(),
            fixed_objects,
            current,
            current_objects,
        );
        match result {
            Ok(v) => Some(v),
            Err(e) => {
                self.error = Some(e);
                None
            }
        }
    }
    fn handover_install_replay(
        &mut self,
        source: &tickwire_release::replay::source::VerifiedSourceReplay,
    ) -> Option<tickwire_runtime::node::InstalledReplay> {
        let member = &source.completion().member;
        if self.identity != (member.new_instance, member.new_generation)
            || self.forwarding
            || self.error.is_some()
        {
            return None;
        }
        let s = self.started.as_mut()?;
        let result = (|| {
            s.child.registry.process(s.child.instance)?;
            let mut clock = SystemClock::new(s.child.domain)?;
            s.readiness.install_replay(
                &mut s.child.report,
                &mut s.reports,
                s.report_sub,
                &mut clock,
                source,
            )
        })();
        match result {
            Ok(value) => Some(value),
            Err(e) => {
                self.error = Some(e);
                None
            }
        }
    }
    fn handover_finish_replay(
        &mut self,
        installed: tickwire_runtime::node::InstalledReplay,
    ) -> Option<tickwire_runtime::node::PreparedReplay> {
        let member = &installed.verified().request.member;
        if self.identity != (member.new_instance, member.new_generation)
            || self.forwarding
            || self.error.is_some()
        {
            return None;
        }
        let s = self.started.as_mut()?;
        let result = (|| {
            s.child.registry.process(s.child.instance)?;
            let mut clock = SystemClock::new(s.child.domain)?;
            s.readiness.finish_replay(
                &mut s.child.report,
                &mut s.reports,
                s.report_sub,
                &mut clock,
                installed,
            )
        })();
        match result {
            Ok(v) => Some(v),
            Err(e) => {
                self.error = Some(e);
                None
            }
        }
    }
    fn handover_objects(
        &mut self,
        want: &tickwire_runtime::node::HandoverReadiness,
    ) -> Option<Vec<Vec<u8>>> {
        if self.identity != (want.instance_id, want.strategy_generation) || self.error.is_some() {
            return None;
        }
        let s = self.started.as_mut()?;
        if s.child.registry.process(s.child.instance).is_err() {
            return None;
        }
        let now = SystemClock::new(s.child.domain)
            .ok()?
            .sample()
            .ok()?
            .monotonic
            .nanos();
        s.readiness
            .objects(&s.child.report, now, &s.reports, s.report_sub, want)
            .ok()
            .flatten()
    }
    fn handover_ready(&mut self, want: &tickwire_runtime::node::HandoverReadiness) -> bool {
        let result = (|| -> Result<bool> {
            let Some(s) = &mut self.started else {
                return Ok(false);
            };
            s.child.registry.process(s.child.instance)?;
            let now = SystemClock::new(s.child.domain)?
                .sample()?
                .monotonic
                .nanos();
            s.readiness.poll(&mut s.child.report, now)?;
            s.readiness
                .confirm(&s.reports, s.report_sub, &s.child.report, now)?;
            s.readiness.matches(&s.reports, s.report_sub, want)
        })();
        match result {
            Ok(v) => v,
            Err(e) => {
                self.error = Some(e);
                false
            }
        }
    }
    fn on_market(&mut self, n: &tickwire_binance_public::Notice<'_>) -> bool {
        self.error.is_none() && self.sink.notice(n)
    }
    fn next(
        &mut self,
        a: &tickwire_account::Account,
        h: tickwire_runtime::node::Health,
        now: tickwire_binance_clock::LocalTime,
    ) -> Option<tickwire_runtime::node::Submission> {
        if self.error().is_some() {
            return None;
        }
        if let Err(e) = self.maintain(now) {
            self.error = Some(e);
            return None;
        }
        if let Err(e) = self.start_if_ready() {
            self.error = Some(e);
            return None;
        }
        let started = self.started.as_mut()?;
        if !self.forwarding {
            if let Err(e) = started.strategy.drain_shadow(32) {
                self.error = Some(e.into());
            }
            return None;
        }
        let submission = tickwire_runtime::node::Strategy::next(&mut started.strategy, a, h, now);
        if started.strategy.error().is_some() {
            if let Err(e) = self.invalidate_generation() {
                self.error = Some(e);
            }
            return None;
        }
        submission
    }
    fn on_account_baseline(
        &mut self,
        baseline: &tickwire_account_codec::CanonicalBaseline,
        _: &tickwire_account::Account,
    ) -> bool {
        let Some(s) = self.started.as_mut() else {
            return false;
        };
        let result = (|| -> Result<()> {
            let now = SystemClock::new(s.child.domain)?
                .sample()?
                .monotonic
                .nanos();
            s.reports.publish_baseline(baseline, now)?;
            Ok(())
        })();
        self.report_result(result)
    }
    fn account_report_position(&self) -> Option<u64> {
        self.started.as_ref().map(|s| s.reports.last_position())
    }
    fn on_account_record(
        &mut self,
        position: u64,
        record: &tickwire_account_codec::CanonicalRecord,
        _: &tickwire_account_codec::CanonicalBaseline,
        _: &tickwire_account::Account,
    ) -> bool {
        let Some(s) = self.started.as_mut() else {
            return false;
        };
        let result = (|| -> Result<()> {
            let now = SystemClock::new(s.child.domain)?
                .sample()?
                .monotonic
                .nanos();
            s.reports.publish_record(position, record, now)?;
            Ok(())
        })();
        self.report_result(result)
    }
    fn on_submission(
        &mut self,
        c: &tickwire_venue::CanonicalCommand,
        r: &std::result::Result<tickwire_account_gateway::Status, tickwire_binance_account::Error>,
    ) {
        if let Some(started) = &mut self.started {
            tickwire_runtime::node::Strategy::on_submission(&mut started.strategy, c, r);
        }
    }
}
/// Fresh identities/epochs and approvals come from the external control owner;
/// the host fills no missing authorization or strategy state.
pub struct RotationPlan<R, const M: usize> {
    pub owner: tickwire_join::JoinOwner<M>,
    pub launch: Launch,
    pub resolver: R,
}
pub trait RotationSource<R, const M: usize> {
    fn next(
        &mut self,
        current: &ResourceHeader,
        published: u64,
    ) -> Result<Option<RotationPlan<R, M>>>;
}
impl<R: RecoveryResolver + 'static, const M: usize> Started<R, M> {
    pub fn stop(&mut self, deadline: Instant) -> Result<()> {
        self.strategy.start_quiesce();
        let result = (|| {
            let frame = self.publisher.with(|p| {
                Ok(p.split().0.quiesce(
                    self.child.subscription,
                    &self.child.market.session,
                    [12; 16],
                    1,
                )?)
            })?;
            self.child.market.connection.send(&frame, &[], deadline)?;
            let response = self.child.market.connection.recv(deadline)?;
            if !response.fds().is_empty() {
                return Err(Error::Invalid);
            }
            let now = SystemClock::new(self.child.domain)?
                .sample()?
                .monotonic
                .nanos();
            self.publisher.with(|p| {
                p.split().0.quiesced(
                    self.child.subscription,
                    &mut self.child.market.session,
                    response.bytes(),
                    now,
                )?;
                Ok(())
            })
        })();
        // Owned Child termination is process evidence only. On a missing QUIESCED
        // acknowledgement the old mapping is still retained; no cursor release is fabricated.
        self.child.registry.terminate(self.child.instance)?;
        result
    }
}
impl<R: RecoveryResolver + 'static, const M: usize> HostedStrategy<R, M> {
    fn report_result(&mut self, result: Result<()>) -> bool {
        match result {
            Ok(()) => true,
            Err(Error::Sdk(tickwire_strategy_sdk::Error::Ring(
                tickwire_shm::MappedError::Protocol(tickwire_shm::Error::Full),
            ))) => false,
            Err(e) => {
                let _ = self.invalidate_generation();
                self.error = Some(e);
                false
            }
        }
    }
    pub fn set_rotation_source(&mut self, source: Box<dyn RotationSource<R, M>>) {
        self.rotation = Some(source);
    }
    fn invalidate_generation(&mut self) -> Result<()> {
        let policy = self.policy.clone();
        self.publisher.with(|p| {
            let (owner, state) = p.split();
            let h = owner.header();
            for (instrument, _, _, _) in state.publications() {
                let _ = policy.invalidate_feed(
                    FeedKey {
                        topic_id: h.topic_id,
                        instrument_id: instrument,
                    },
                    h.feed_generation,
                    h.ring_epoch,
                );
            }
            p.require_rebuild();
            Ok(())
        })
    }
    fn maintain(&mut self, now: tickwire_binance_clock::LocalTime) -> Result<()> {
        if let Some(s) = &mut self.started {
            s.readiness
                .poll(&mut s.child.report, now.monotonic.nanos())?;
            s.child.registry.process(s.child.instance)?;
            s.readiness.confirm(
                &s.reports,
                s.report_sub,
                &s.child.report,
                now.monotonic.nanos(),
            )?;
        }
        let child_failed = if let Some(s) = &mut self.started {
            !s.child.registry.observe_exits()?.is_empty() || s.strategy.error().is_some()
        } else {
            false
        };
        if child_failed {
            self.invalidate_generation()?;
        }
        let rotating = self.publisher.with(|p| Ok(p.needs_bank()))?;
        if !rotating {
            return Ok(());
        }
        self.invalidate_generation()?;
        if self.started.is_some() {
            self.policy.stop_strategy_instance(
                self.logical_strategy_id,
                self.identity.0,
                self.identity.1,
            )?;
        }
        if let Some(mut old) = self.started.take() {
            let _ = old.stop(Instant::now() + Duration::from_secs(1));
        }
        let ready = self.publisher.with(|p| Ok(p.state().cached_live()))?;
        if !ready {
            return Ok(());
        }
        let Some(source) = &mut self.rotation else {
            return Err(Error::NotReady);
        };
        if self.retired.len() >= 8 {
            return Err(Error::Invalid);
        }
        let (h, sequence) = self.publisher.with(|p| {
            let (o, _) = p.split();
            Ok((o.header().clone(), o.published()))
        })?;
        let Some(plan) = source.next(&h, sequence)? else {
            return Ok(());
        };
        if plan.launch.spec.identity.logical_strategy != self.logical_strategy_id
            || plan.launch.spec.identity.instance == self.identity.0
            || plan.launch.spec.identity.generation <= self.identity.1
        {
            return Err(Error::Invalid);
        }
        let old = self
            .publisher
            .with(|p| Ok(p.replace_bank(plan.owner, now.monotonic.nanos())?))?;
        self.retired.push(old);
        let policy = self.policy.clone();
        self.publisher.with(|p| {
            let (o, state) = p.split();
            let h = o.header();
            for (instrument, version, sequence, published) in state.publications() {
                policy.update_feed(FeedUpdate {
                    key: FeedKey {
                        topic_id: h.topic_id,
                        instrument_id: instrument,
                    },
                    instrument_version: version,
                    feed_generation: h.feed_generation,
                    ring_epoch: h.ring_epoch,
                    sequence,
                    published_at: tickwire_clock::MonoTime::new(now.monotonic.domain(), published),
                })?;
            }
            Ok(())
        })?;
        self.identity = (
            plan.launch.spec.identity.instance,
            plan.launch.spec.identity.generation,
        );
        self.launch = Some(plan.launch);
        self.resolver = Some(plan.resolver);
        Ok(())
    }
}
