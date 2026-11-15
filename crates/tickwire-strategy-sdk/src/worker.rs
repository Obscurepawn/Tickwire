//! Stateless strategy worker runtime, trusted configuration reader (`std::fs` metadata checks), CPU affinity
//! isolation (`worker_cpu` vs. `strategy-cold` on `housekeeping_cpus`), and hot-loop driver.

use std::{
    fs::{self, File},
    io::Read,
    os::{fd::AsFd, unix::fs::MetadataExt},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use tickwire_clock_sys::SystemClock;
use tickwire_control::{ClientHandshake, ProcessIdentity, Session};
use tickwire_control_sys::{
    Connection, CpuSet, pin_current_thread, spawn_pinned_thread, validate_cpu_isolation,
};
use tickwire_ipc::ControlFrame;
use tickwire_join::JoinConsumer;
pub use tickwire_market_source::protocol::{
    accept_offer, activation_ack, cursor_header, descriptor, offer_id, resource_offer,
};
use tickwire_shm::{BankKey, Publisher, SubscriptionKey};
use tickwire_venue::TerminalReason;

use crate::{
    Error, FixedDemo, MarketReplica, ObserveOnly, Result, Strategy,
    bootstrap::{Bootstrap, Endpoint, MAX_PARAM_BYTES},
    dop::{DopControlEvent, DopSession},
    outbound::{IntentWriter, OutboundIntent},
    reports::{ExecutionReportKind, ReportIdentity, ReportReceiver},
};

/// Read an immutable trusted file (`<= maximum` bytes) using pure `std::fs`, rejecting symlinks,
/// verifying `(dev, ino)` continuity across `symlink_metadata` and `File::open` (retrying up to
/// 4 times on concurrent atomic rename), and rejecting group/world-writable permissions.
#[forbid(unsafe_code)]
pub fn read_trusted_bytes(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    let mut attempts = 0u8;
    let (file, meta) = loop {
        let sym = fs::symlink_metadata(path)?;
        if sym.file_type().is_symlink() || !sym.is_file() {
            return Err(Error::Identity);
        }
        let file = File::open(path)?;
        let meta = file.metadata()?;
        if !meta.is_file() {
            return Err(Error::Identity);
        }
        if (meta.dev(), meta.ino()) == (sym.dev(), sym.ino()) {
            break (file, meta);
        }
        attempts += 1;
        if attempts >= 4 {
            return Err(Error::Identity);
        }
    };
    if meta.mode() & 0o022 != 0 {
        return Err(Error::Identity);
    }
    if meta.len() > maximum as u64 {
        return Err(Error::Bounds);
    }
    let mut bytes = Vec::with_capacity(usize::try_from(meta.len()).unwrap_or(0));
    file.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        return Err(Error::Bounds);
    }
    Ok(bytes)
}

/// Read and strictly deserialize a JSON configuration document (`<= maximum` bytes).
#[forbid(unsafe_code)]
pub fn read_config<T: serde::de::DeserializeOwned>(path: &Path, maximum: usize) -> Result<T> {
    let bytes = read_trusted_bytes(path, maximum)?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// Validate `worker_cpu ∩ housekeeping_cpus == ∅` via `tickwire_control_sys::validate_cpu_isolation`.
#[forbid(unsafe_code)]
pub fn validate_affinity(
    worker_cpu: Option<u32>,
    housekeeping_cpus: Option<&CpuSet>,
) -> Result<()> {
    validate_cpu_isolation(&[("worker_cpu", worker_cpu)], housekeeping_cpus)?;
    Ok(())
}

/// Spawn a cold worker housekeeping thread (`"strategy-cold"`) onto `housekeeping_cpus`
/// via `tickwire_control_sys::spawn_pinned_thread` *before* running any cold closure body,
/// escaping any single-core `taskset` mask inherited by the parent process.
#[forbid(unsafe_code)]
pub fn spawn_housekeeping<F, T>(housekeeping_cpus: Option<&CpuSet>, f: F) -> Result<JoinHandle<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    Ok(spawn_pinned_thread("strategy-cold", housekeeping_cpus, f)?)
}

/// Validate CPU isolation and pin the calling hot strategy thread to `worker_cpu` when configured
/// (with mandatory `sched_getaffinity` read-back verification inside `tickwire-control-sys`).
#[forbid(unsafe_code)]
pub fn pin_worker_thread(
    worker_cpu: Option<u32>,
    housekeeping_cpus: Option<&CpuSet>,
) -> Result<()> {
    validate_affinity(worker_cpu, housekeeping_cpus)?;
    if let Some(cpu) = worker_cpu {
        let set = CpuSet::single(cpu)?;
        pin_current_thread(&set)?;
    }
    Ok(())
}

/// Summary of work performed in a single non-blocking [`WorkerDriver::step`] iteration.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkerStepOutcome {
    pub reports_processed: usize,
    pub market_records_processed: usize,
    pub intents_published: usize,
    pub dop_events_processed: usize,
    pub heartbeat_sent: bool,
    pub ors_shutdown: bool,
    pub peer_logged_out: bool,
}

/// Composable stateless strategy hot-loop driver consuming `tickwire-md` SHM updates and inbound
/// 512B `ReportRecord` replies while publishing inline 1024B `IntentRecord` commands.
pub struct WorkerDriver<S> {
    replica: MarketReplica,
    dop: Option<DopSession>,
    intent_writer: Option<IntentWriter>,
    report_receiver: Option<ReportReceiver>,
    strategy: S,
    default_account_ref: u8,
    maximum_market_age_ns: u64,
    poll_records: usize,
    ors_shutdown: bool,
    peer_logged_out: bool,
}

impl<S: Strategy> WorkerDriver<S> {
    #[forbid(unsafe_code)]
    pub fn new(
        replica: MarketReplica,
        dop: Option<DopSession>,
        mut intent_writer: Option<IntentWriter>,
        report_receiver: Option<ReportReceiver>,
        strategy: S,
        maximum_market_age_ns: u64,
        poll_records: usize,
    ) -> Result<Self> {
        if maximum_market_age_ns == 0 || poll_records == 0 || poll_records > 4096 {
            return Err(Error::Bounds);
        }
        if let (Some(d), Some(w)) = (&dop, &mut intent_writer) {
            w.sync_from_dop(d)?;
        }
        Ok(Self {
            replica,
            dop,
            intent_writer,
            report_receiver,
            strategy,
            default_account_ref: 0,
            maximum_market_age_ns,
            poll_records,
            ors_shutdown: false,
            peer_logged_out: false,
        })
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn with_default_account_ref(mut self, account_ref: u8) -> Self {
        self.default_account_ref = account_ref;
        self
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn strategy(&self) -> &S {
        &self.strategy
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn strategy_mut(&mut self) -> &mut S {
        &mut self.strategy
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn replica(&self) -> &MarketReplica {
        &self.replica
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn dop(&self) -> Option<&DopSession> {
        self.dop.as_ref()
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn dop_mut(&mut self) -> Option<&mut DopSession> {
        self.dop.as_mut()
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn intent_writer(&self) -> Option<&IntentWriter> {
        self.intent_writer.as_ref()
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn report_receiver(&self) -> Option<&ReportReceiver> {
        self.report_receiver.as_ref()
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn ors_shutdown(&self) -> bool {
        self.ors_shutdown
    }

    #[inline]
    #[forbid(unsafe_code)]
    pub fn peer_logged_out(&self) -> bool {
        self.peer_logged_out
    }

    /// Execute one non-blocking hot-path iteration: drain `ReportRecord` replies, drain
    /// `MarketRecord`/`MarketExactRecord` updates, emit resulting `IntentRecord`s, and service
    /// `DopSession` control events/heartbeats.
    #[forbid(unsafe_code)]
    pub fn step<const B: usize>(
        &mut self,
        consumer: &JoinConsumer<B>,
        now_mono_ns: u64,
    ) -> Result<WorkerStepOutcome> {
        let mut outcome = WorkerStepOutcome {
            ors_shutdown: self.ors_shutdown,
            peer_logged_out: self.peer_logged_out,
            ..Default::default()
        };

        for _ in 0..self.poll_records {
            let Some(rx) = &mut self.report_receiver else {
                break;
            };
            let Some(report) = rx.poll()? else {
                break;
            };
            outcome.reports_processed += 1;
            if matches!(report.kind, ExecutionReportKind::OrsShutDown) {
                self.ors_shutdown = true;
                outcome.ors_shutdown = true;
            }
            if let Some(intent) = self.strategy.on_report(&report, now_mono_ns)?
                && !self.ors_shutdown
                && !self.peer_logged_out
            {
                publish_outbound(
                    &mut self.dop,
                    &mut self.intent_writer,
                    self.default_account_ref,
                    intent,
                    now_mono_ns,
                )
                .map_err(|e| e.context("report intent"))?;
                outcome.intents_published += 1;
            }
        }

        let dop_ref = &mut self.dop;
        let writer_ref = &mut self.intent_writer;
        let default_account_ref = self.default_account_ref;
        let halted = self.ors_shutdown || self.peer_logged_out;
        let mut emitted_from_market = 0usize;
        let market_count = self.replica.drain(
            consumer,
            &mut self.strategy,
            now_mono_ns,
            self.maximum_market_age_ns,
            self.poll_records,
            |intent| {
                if halted {
                    return Ok(());
                }
                publish_outbound(
                    dop_ref,
                    writer_ref,
                    default_account_ref,
                    intent,
                    now_mono_ns,
                )
                .map_err(|e| e.context("market intent"))?;
                emitted_from_market += 1;
                Ok(())
            },
        )?;
        outcome.market_records_processed = market_count;
        outcome.intents_published += emitted_from_market;

        if let Some(dop) = &mut self.dop {
            for _ in 0..self.poll_records {
                let Some(ev) = dop.try_recv(now_mono_ns)? else {
                    break;
                };
                outcome.dop_events_processed += 1;
                if let DopControlEvent::PeerLogout { .. } = ev {
                    self.peer_logged_out = true;
                    outcome.peer_logged_out = true;
                    break;
                }
            }
            if !self.peer_logged_out {
                let hb_deadline = Instant::now() + Duration::from_millis(50);
                outcome.heartbeat_sent = dop.maybe_send_heartbeat(now_mono_ns, hb_deadline)?;
            }
        }

        Ok(outcome)
    }
}

#[forbid(unsafe_code)]
fn publish_outbound(
    dop: &mut Option<DopSession>,
    writer: &mut Option<IntentWriter>,
    default_account_ref: u8,
    mut intent: OutboundIntent,
    now_mono_ns: u64,
) -> Result<()> {
    let writer = writer.as_mut().ok_or(Error::NotReady)?;
    if intent.account_ref == 0 && default_account_ref != 0 {
        intent.account_ref = default_account_ref;
    }
    if let Some(dop_session) = dop {
        writer.publish_with_dop(dop_session, &intent, now_mono_ns)?;
    } else {
        writer.publish(&intent, now_mono_ns)?;
    }
    Ok(())
}

#[forbid(unsafe_code)]
fn now(clock: &mut SystemClock) -> Result<u64> {
    Ok(clock.sample()?.monotonic.nanos())
}

#[allow(unsafe_code)]
pub(crate) fn connect_market(
    config: &tickwire_market_source::ConnectConfig,
    clock: &mut SystemClock,
) -> Result<tickwire_market_source::Client> {
    // SAFETY: the qualified launcher/worker authenticates the `tickwire-md` peer identity and
    // immutable `F_SEAL_FUTURE_WRITE` topic descriptors before attaching.
    Ok(unsafe {
        tickwire_market_source::Client::connect(config, &mut || {
            clock
                .sample()
                .map(|s| s.monotonic)
                .map_err(|_| tickwire_market_source::Error::Identity)
        })?
    })
}

#[allow(unsafe_code)]
pub(crate) fn register_intent_cursor(
    publisher: &mut Publisher<1>,
    sub: SubscriptionKey,
    cursor_fd: std::os::fd::OwnedFd,
) -> Result<()> {
    // SAFETY: the authenticated ORS peer supplied the exact cursor descriptor bound to `sub`.
    unsafe {
        publisher.register_cursor(sub, cursor_fd)?;
    }
    Ok(())
}

#[allow(unsafe_code)]
pub(crate) fn attach_reports(
    fd: std::os::fd::OwnedFd,
    bank: tickwire_ipc::ResourceHeader,
    subscription: SubscriptionKey,
    identity: ReportIdentity,
) -> Result<ReportReceiver> {
    // SAFETY: the authenticated ORS execution owner is the sole writer and registered `subscription`
    // before offering the sealed read-only report ring descriptor.
    unsafe { ReportReceiver::attach(fd, bank, subscription, None, identity) }
}

#[forbid(unsafe_code)]
pub(crate) fn connect(
    ep: &Endpoint,
    clock: &mut SystemClock,
    deadline: Instant,
    id: [u8; 16],
) -> Result<(Connection, Session)> {
    let mut connection = Connection::connect(&ep.path, deadline)?;
    let hello = ClientHandshake::new(ep.policy(), id)?;
    connection.send(hello.hello(), &[], deadline)?;
    let reply = connection.recv(deadline)?;
    if !reply.fds().is_empty() {
        return Err(Error::Identity);
    }
    let peer = connection.peer_credentials();
    let session = hello.finish(
        ProcessIdentity {
            pid: peer.pid,
            uid: peer.uid,
            gid: peer.gid,
        },
        reply.bytes(),
        now(clock)?,
    )?;
    Ok((connection, session))
}

struct IntentLane {
    connection: Connection,
    session: Session,
    writer: IntentWriter,
}

#[forbid(unsafe_code)]
fn connect_intent(
    config: &Bootstrap,
    ep: &Endpoint,
    dop: Option<&DopSession>,
    clock: &mut SystemClock,
    deadline: Instant,
) -> Result<IntentLane> {
    let (mut intent, mut intent_session) = connect(ep, clock, deadline, [6; 16])?;
    let h = config.intent_resource()?.ok_or(Error::Identity)?;
    if h.consumer_instance != ep.peer.instance
        || h.producer_instance != config.instance_id
        || h.producer_generation != config.generation
        || h.host_boot_id != ep.local.host_boot
        || h.namespace_id != ep.local.namespace
    {
        return Err(Error::Identity);
    }
    let mut publisher = Publisher::<1>::create(h)?;
    let sub = SubscriptionKey {
        bank: publisher.key(),
        instance: u128::from_le_bytes(ep.peer.instance),
        generation: config.intent_subscription_generation,
        cursor: u128::from_le_bytes(config.intent_cursor_id),
    };
    publisher.join(sub)?;
    let membership = publisher.membership_version();
    let expires = intent_session.deadline();
    let offered = resource_offer(
        &intent_session,
        publisher.header(),
        [7; 16],
        expires,
        membership,
        false,
    )?;
    let bankfd = publisher.reader_fd()?;
    intent.send(&offered, &[bankfd.as_fd()], deadline)?;
    let ch = cursor_header(publisher.header(), sub)?;
    let cursor = accept_offer(
        &mut intent_session,
        intent.recv(deadline)?,
        &ch,
        [8; 16],
        expires,
        membership,
        false,
        now(clock)?,
    )?;
    register_intent_cursor(&mut publisher, sub, cursor)?;
    let next_order_seqnum = dop.map_or(1, |d| d.next_order_seqnum().max(1));
    let mut writer = IntentWriter::with_arena(
        publisher,
        Some(sub),
        None,
        config.strat_id,
        config.instance_id,
        config.generation,
        next_order_seqnum,
    )?;
    if let Some(d) = dop {
        writer.sync_from_dop(d)?;
    }
    Ok(IntentLane {
        connection: intent,
        session: intent_session,
        writer,
    })
}

struct ReportLane {
    connection: Connection,
    session: Session,
    receiver: ReportReceiver,
}

#[forbid(unsafe_code)]
fn connect_report(
    config: &Bootstrap,
    ep: &Endpoint,
    clock: &mut SystemClock,
    deadline: Instant,
) -> Result<ReportLane> {
    let (mut report, mut report_session) = connect(ep, clock, deadline, [13; 16])?;
    let report_bank = config.report_resource()?.ok_or(Error::Identity)?;
    if report_bank.consumer_instance != config.instance_id
        || report_bank.producer_instance != ep.peer.instance
        || report_bank.producer_generation != ep.peer.generation
        || report_bank.host_boot_id != ep.local.host_boot
        || report_bank.namespace_id != ep.local.namespace
    {
        return Err(Error::Identity);
    }
    let report_sub = SubscriptionKey {
        bank: BankKey {
            resource: u128::from_le_bytes(report_bank.resource_id),
            channel: u128::from_le_bytes(report_bank.channel_id),
            channel_epoch: report_bank.channel_epoch,
            ring_epoch: report_bank.ring_epoch,
            feed_generation: report_bank.feed_generation,
        },
        instance: u128::from_le_bytes(config.instance_id),
        generation: config.report_subscription_generation,
        cursor: u128::from_le_bytes(config.report_cursor_id),
    };
    let report_membership = report_bank
        .initial_membership_version
        .checked_add(1)
        .ok_or(Error::Bounds)?;
    let expires = report_session.deadline();
    let fd = accept_offer(
        &mut report_session,
        report.recv(deadline)?,
        &report_bank,
        [14; 16],
        expires,
        report_membership,
        false,
        now(clock)?,
    )?;
    let receiver = attach_reports(
        fd,
        report_bank.clone(),
        report_sub,
        ReportIdentity {
            strat_id: config.strat_id,
            instance_id: config.instance_id,
            strategy_generation: config.generation,
        },
    )?;
    let cursor = receiver.cursor_fd()?;
    let ch = cursor_header(&report_bank, report_sub)?;
    let offer = resource_offer(
        &report_session,
        &ch,
        [15; 16],
        expires,
        report_membership,
        false,
    )?;
    report.send(&offer, &[cursor.as_fd()], deadline)?;
    Ok(ReportLane {
        connection: report,
        session: report_session,
        receiver,
    })
}

/// Internal safe entrypoint invoked by [`crate::registered::run`] and [`run_builtin`] after
/// startup identity and digest checks pass.
#[forbid(unsafe_code)]
pub(crate) fn run_qualified<S: Strategy>(config: Bootstrap, strategy: S) -> Result<()> {
    config.validate()?;
    crate::catalog::validate_startup(&config)?;

    // Spawn `"strategy-cold"` housekeeping onto `housekeeping_cpus` BEFORE pinning the hot
    // thread to `worker_cpu` so cold housekeeping never inherits `worker_cpu` and escapes any
    // external single-core `taskset` parent mask.
    let stop_cold = Arc::new(AtomicBool::new(false));
    let cold_ticks = Arc::new(AtomicU64::new(0));
    let stop_flag = Arc::clone(&stop_cold);
    let ticks_flag = Arc::clone(&cold_ticks);
    let cold_handle = spawn_housekeeping(config.housekeeping_cpus.as_ref(), move || {
        while !stop_flag.load(Ordering::Acquire) {
            ticks_flag.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(10));
        }
    })?;

    let run_result = run_hot_worker(&config, strategy);
    stop_cold.store(true, Ordering::Release);
    let _ = cold_handle.join();
    run_result
}

#[forbid(unsafe_code)]
fn run_hot_worker<S: Strategy>(config: &Bootstrap, strategy: S) -> Result<()> {
    pin_worker_thread(config.worker_cpu, config.housekeeping_cpus.as_ref())?;

    let domain = config.market.domain()?;
    let mut clock = SystemClock::new(domain)?;
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(config.startup_timeout_ms))
        .ok_or(Error::Bounds)?;
    let startup_timeout = Duration::from_millis(config.startup_timeout_ms);

    let mut client = connect_market(&config.market, &mut clock)?;

    let dop = if let (Some(sock), Some(dop_cfg)) = (&config.dop_socket, &config.dop) {
        Some(DopSession::connect_and_login(
            sock,
            dop_cfg.clone(),
            0,
            now(&mut clock)?,
            startup_timeout,
        )?)
    } else {
        None
    };

    let (mut intent_ctrl, intent_writer) = match &config.intent {
        Some(ep) => {
            let lane = connect_intent(config, ep, dop.as_ref(), &mut clock, deadline)?;
            (Some((lane.connection, lane.session)), Some(lane.writer))
        }
        None => (None, None),
    };

    let (mut report_ctrl, report_receiver) = match &config.report {
        Some(ep) => {
            let lane = connect_report(config, ep, &mut clock, deadline)?;
            (Some((lane.connection, lane.session)), Some(lane.receiver))
        }
        None => (None, None),
    };

    let mut driver = WorkerDriver::new(
        MarketReplica::from_core(client.replica),
        dop,
        intent_writer,
        report_receiver,
        strategy,
        config.maximum_market_age_ns,
        config.poll_records,
    )?
    .with_default_account_ref(config.account_ref);

    loop {
        let now_ns = now(&mut clock)?;
        if now_ns >= client.session.deadline()
            || intent_ctrl
                .as_ref()
                .is_some_and(|(_, s)| now_ns >= s.deadline())
            || report_ctrl
                .as_ref()
                .is_some_and(|(_, s)| now_ns >= s.deadline())
        {
            return Err(Error::Deadline);
        }

        let outcome = driver.step(&client.consumer, now_ns)?;
        if outcome.ors_shutdown || outcome.peer_logged_out {
            break;
        }

        if poll_market_quiesce(
            &mut client.connection,
            &mut client.session,
            &client.consumer,
            now_ns,
        )? {
            break;
        }
        if let Some((conn, sess)) = &mut intent_ctrl
            && poll_control_quiesce(conn, sess, now_ns)?
        {
            break;
        }
        if let Some((conn, sess)) = &mut report_ctrl
            && poll_control_quiesce(conn, sess, now_ns)?
        {
            break;
        }

        if outcome.reports_processed == 0
            && outcome.market_records_processed == 0
            && outcome.dop_events_processed == 0
            && config.idle_sleep_us > 0
        {
            std::thread::sleep(Duration::from_micros(config.idle_sleep_us));
        }
    }

    if let Some(rx) = &driver.report_receiver {
        rx.start_quiesce();
    }
    if let Some(dop) = &mut driver.dop {
        let logout_deadline = Instant::now() + Duration::from_millis(250);
        let _ = dop.logout(false, TerminalReason::AdminAction, logout_deadline);
    }
    Ok(())
}

#[forbid(unsafe_code)]
fn poll_market_quiesce(
    conn: &mut Connection,
    session: &mut Session,
    consumer: &JoinConsumer<4>,
    now_ns: u64,
) -> Result<bool> {
    let Some(msg) = conn.try_recv()? else {
        return Ok(false);
    };
    if !msg.fds().is_empty() {
        return Err(Error::Identity);
    }
    let reply = consumer.quiesce(session, msg.bytes(), now_ns)?;
    let send_deadline = Instant::now() + Duration::from_millis(100);
    let _ = conn.send(&reply, &[], send_deadline);
    Ok(true)
}

#[forbid(unsafe_code)]
fn poll_control_quiesce(conn: &mut Connection, session: &mut Session, now_ns: u64) -> Result<bool> {
    let Some(msg) = conn.try_recv()? else {
        return Ok(false);
    };
    if !msg.fds().is_empty() {
        return Err(Error::Identity);
    }
    let frame: ControlFrame<'_> = session.validate(msg.bytes(), now_ns)?;
    match frame.envelope.opcode {
        20 => {
            let ack = session.encode(
                tickwire_ipc::ControlEnvelope {
                    opcode: 21,
                    flags: 10,
                    request_id: frame.envelope.request_id,
                    ..Default::default()
                },
                &[],
            )?;
            let send_deadline = Instant::now() + Duration::from_millis(100);
            let _ = conn.send(&ack, &[], send_deadline);
            Ok(true)
        }
        _ => Ok(false),
    }
}

/// Run the built-in stateless worker (`ObserveOnly` or `FixedDemo`) from a validated [`Bootstrap`].
#[forbid(unsafe_code)]
pub fn run_builtin(config: Bootstrap) -> Result<()> {
    config.validate()?;
    crate::catalog::validate_startup(&config)?;
    if config.factory.is_some() {
        return Err(Error::Identity);
    }
    if config.demo_enabled {
        let path = config.demo_command_file.as_ref().ok_or(Error::NotReady)?;
        let bytes = read_trusted_bytes(path, MAX_PARAM_BYTES)?;
        let command = tickwire_venue::CanonicalCommand::decode(&bytes)?;
        let strategy = FixedDemo::new(
            command,
            config.strat_id,
            config.account_ref,
            config.demo_minimum_bid_ticks,
            true,
        );
        run_qualified(config, strategy)
    } else {
        run_qualified(config, ObserveOnly)
    }
}

/// Run a qualified stateless worker until `QUIESCE`, `DopLogout`, `OrsShutDown`, or lease expiry.
///
/// # Safety
/// The caller/launcher must guarantee that peer `tickwire-md` and ORS processes obey the single-writer
/// `F_SEAL_FUTURE_WRITE` shared-memory ring contracts for all offered descriptors.
#[allow(unsafe_code)]
pub unsafe fn run<S: Strategy>(config: Bootstrap, strategy: S) -> Result<()> {
    run_qualified(config, strategy)
}

/// Complete an authenticated in-process `JOIN` handshake (`prepare` -> `deliver` -> `attach` ->
/// `load_baseline` -> `receive_cursor` -> `read_ready` -> `join_ready`) against an owned
/// [`tickwire_join::JoinOwner`], returning an active `(JoinConsumer<4>, MarketReplica)` at an
/// independent reader cursor (`0` disk writes).
#[allow(clippy::too_many_arguments)]
#[allow(unsafe_code)]
pub fn in_process_join_consumer<const M: usize, S, F>(
    owner: &mut tickwire_join::JoinOwner<M>,
    supervisor: &mut Session,
    store: &mut S,
    admin_request: &[u8],
    registration: &tickwire_join::SubscriberRegistration,
    snapshot_id: [u8; 16],
    maximum_levels: usize,
    now_mono_ns: u64,
    build: F,
) -> Result<(JoinConsumer<4>, MarketReplica)>
where
    S: tickwire_control::RequestStore,
    F: FnOnce(&tickwire_join::JoinPlan) -> tickwire_ipc::Result<tickwire_snapshot::BuiltTopic>,
{
    use sha2::{Digest, Sha256};
    use tickwire_control::{Identity, Policy};
    use tickwire_join::PrepareOutcome;

    let sub = registration.subscription;
    match owner.prepare(
        supervisor,
        store,
        admin_request,
        registration,
        snapshot_id,
        now_mono_ns,
        build,
    )? {
        PrepareOutcome::Prepared { subscription, .. } if subscription == sub => {}
        _ => return Err(Error::NotReady),
    }

    let deadline = Instant::now() + Duration::from_secs(2);
    let (mut server_conn, mut client_conn) = crate::dop::local_seqpacket_pair(deadline)?;
    let creds = server_conn.peer_credentials();
    let process = ProcessIdentity {
        pid: creds.pid,
        uid: creds.uid,
        gid: creds.gid,
    };
    let h = owner.header();
    let owner_id = Identity {
        instance: h.producer_instance,
        generation: h.producer_generation,
        role: 2,
        logical_strategy: 0,
        host_boot: h.host_boot_id,
        namespace: h.namespace_id,
        build_hash: registration.identity.build_hash,
        qualification_hash: registration.identity.qualification_hash,
        manifest_hash: registration.identity.manifest_hash,
        arch: h.target_arch,
        trust_profile: registration.identity.trust_profile,
    };
    let nonce: [u8; 32] = Sha256::digest(now_mono_ns.to_le_bytes()).into();
    let server_policy = Policy {
        local: owner_id.clone(),
        peer: registration.identity.clone(),
        peer_process: process,
        required_features: 0x1c,
        supported_features: 0x1f | 64 | 512,
        control_epoch: 1,
        lease_duration_ns: 60_000_000_000,
        max_frame_bytes: 65_536,
        nonce,
    };
    let client_policy = Policy {
        local: registration.identity.clone(),
        peer: owner_id,
        ..server_policy.clone()
    };
    let hello = ClientHandshake::new(client_policy, [1; 16])?;
    let (mut server_sess, welcome) =
        Session::accept(&server_policy, process, hello.hello(), now_mono_ns)?;
    let mut client_sess = hello.finish(process, &welcome, now_mono_ns)?;

    owner.deliver(
        sub,
        &mut server_sess,
        &mut server_conn,
        now_mono_ns,
        deadline,
    )?;

    let j = owner.join(sub)?.clone();
    let bank = owner.header().clone();
    let snapshot_hdr = owner.snapshot_header(sub)?.clone();
    let registry = owner.registry().clone();

    let fd = accept_offer(
        &mut client_sess,
        client_conn.recv(deadline)?,
        &bank,
        offer_id(&j, 1),
        j.readiness_deadline_mono_ns,
        j.membership_version,
        false,
        now_mono_ns,
    )?;
    let sf = accept_offer(
        &mut client_sess,
        client_conn.recv(deadline)?,
        &snapshot_hdr,
        offer_id(&j, 2),
        j.readiness_deadline_mono_ns,
        j.membership_version,
        true,
        now_mono_ns,
    )?;

    // SAFETY: `owner` is exclusively borrowed and owns the sealed bank/snapshot descriptors.
    let mut consumer = unsafe {
        JoinConsumer::<4>::attach(fd, sf, bank, snapshot_hdr, j.clone(), registry, now_mono_ns)?
    };
    let mut replica = consumer.load_baseline(now_mono_ns, |topic| {
        MarketReplica::load(j.topic_id, j.ring_epoch, topic, maximum_levels)
    })?;

    let (cursor_frame, cursor_fd) = consumer.cursor_offer(&client_sess, [2; 16])?;
    client_conn.send(&cursor_frame, &[cursor_fd.as_fd()], deadline)?;
    let cursor_packet = server_conn.recv(deadline)?;
    // SAFETY: `cursor_packet` carries the exact cursor descriptor produced by `consumer`.
    unsafe {
        owner.receive_cursor(sub, &mut server_sess, cursor_packet, now_mono_ns)?;
    }

    for frame in consumer.read_ready_frames(&client_sess, [[3; 16], [4; 16]])? {
        owner.read_ready(sub, &mut server_sess, &frame, now_mono_ns)?;
    }

    let mut obs = ObserveOnly;
    replica.drain(&consumer, &mut obs, now_mono_ns, u64::MAX, 4096, |_| Ok(()))?;
    let ready = consumer.join_ready_frame(&client_sess, [5; 16], now_mono_ns)?;
    owner.join_ready(sub, &mut server_sess, &ready, now_mono_ns)?;

    Ok((consumer, replica))
}
