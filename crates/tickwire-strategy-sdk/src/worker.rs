//! Qualified worker: authenticated launcher policies are the unsafe SHM admission boundary.
use crate::bootstrap::{Bootstrap, Endpoint, JoinBootstrap};
mod expansion;
mod rebuild;
use crate::market_transition as transition_control;
use crate::{CommandStore, Error, IntentWriter, MarketReplica, Result, Strategy};
use std::{
    fs::OpenOptions,
    io::Read,
    os::{
        fd::AsFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::Path,
    time::{Duration, Instant},
};
use tickwire_clock_sys::SystemClock;
use tickwire_control::{ClientHandshake, ProcessIdentity, Session};
use tickwire_control_sys::Connection;
use tickwire_ipc::{ControlEnvelope, Wire};
use tickwire_market_source::transition::{Approval, Commit, Phase, Receipt};
use tickwire_shm::{Publisher, SubscriptionKey};

pub fn read_config<T: serde::de::DeserializeOwned>(path: &Path, maximum: usize) -> Result<T> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)?;
    let m = file.metadata()?;
    if !m.is_file() || m.mode() & 0o022 != 0 || m.len() > maximum as u64 {
        return Err(Error::Identity);
    }
    let mut bytes = Vec::new();
    file.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        return Err(Error::Bounds);
    }
    Ok(serde_json::from_slice(&bytes)?)
}
fn now(clock: &mut SystemClock) -> Result<u64> {
    Ok(clock.sample()?.monotonic.nanos())
}
// Crate-private shared boundary. Only qualified run/registered entrypoints call
// this after validating the exact launch identities and approved resource.
pub(crate) fn connect_market(
    config: &tickwire_market_source::ConnectConfig,
    clock: &mut SystemClock,
) -> Result<tickwire_market_source::Client> {
    // SAFETY: the trusted launcher admits this exact qualified child and MD identity.
    Ok(unsafe {
        tickwire_market_source::Client::connect(config, &mut || {
            clock
                .sample()
                .map(|s| s.monotonic)
                .map_err(|_| tickwire_market_source::Error::Identity)
        })?
    })
}
// Same qualified execution-owned report boundary, shared by normal and capture
// workers. This is deliberately not a public safe SHM attachment API.
pub(crate) fn attach_reports(
    fd: std::os::fd::OwnedFd,
    bank: tickwire_ipc::ResourceHeader,
    subscription: SubscriptionKey,
    objects: tickwire_recovery::FileBlobReader,
    identity: crate::reports::ReportIdentity,
) -> Result<crate::reports::ReportReceiver> {
    // SAFETY: the authenticated execution owner is the qualified sole writer and
    // has registered this exact pending gate before offering the read-only bank.
    unsafe { crate::reports::ReportReceiver::attach(fd, bank, subscription, objects, identity) }
}
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
use tickwire_market_source::protocol::activated;
pub use tickwire_market_source::protocol::{
    accept_offer, activation_ack, cursor_header, descriptor, offer_id, resource_offer,
};
pub(crate) struct IntentLane {
    pub connection: Connection,
    pub session: Session,
    pub writer: IntentWriter,
    pub store: CommandStore<tickwire_recovery::FileBlobStore>,
}
/// Called by qualified startup modes only, preserving the original intent
/// mapping/cursor contract and the actual execution peer credentials.
pub(crate) fn connect_intent(
    config: &Bootstrap,
    clock: &mut SystemClock,
    deadline: Instant,
) -> Result<IntentLane> {
    let (mut intent, mut intent_session) = connect(&config.intent, clock, deadline, [6; 16])?;
    let h = config.intent_resource()?;
    if h.consumer_instance != config.intent.peer.instance
        || h.producer_instance != config.intent.local.instance
        || h.producer_generation != config.intent.local.generation
        || h.host_boot_id != config.intent.local.host_boot
        || h.namespace_id != config.intent.local.namespace
    {
        return Err(Error::Identity);
    }
    let mut publisher = Publisher::<1>::create(h)?;
    let sub = SubscriptionKey {
        bank: publisher.key(),
        instance: u128::from_le_bytes(config.intent.peer.instance),
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
    // SAFETY: the registered execution peer is authenticated, qualified, and its
    // cursor descriptor exactly binds the pending subscription and immutable header.
    unsafe {
        publisher.register_cursor(sub, cursor)?;
    }
    let writer = IntentWriter::new(
        publisher,
        config.intent.local.logical_strategy,
        config.intent.local.instance,
        config.intent.local.generation,
        config.risk_domain,
    )?;
    let store = CommandStore::new(tickwire_recovery::FileBlobStore::open(
        &config.command_directory,
    )?);
    Ok(IntentLane {
        connection: intent,
        session: intent_session,
        writer,
        store,
    })
}
/// Runs until authenticated QUIESCE, expired control lease, or a closed/invalid resource.
/// # Safety
/// The launcher authenticates qualified Publisher/Subscriber processes and enforces
/// the exact immutable resource and truthful cursor lifetime contracts for both lanes.
pub unsafe fn run<S: Strategy>(config: Bootstrap, strategy: S) -> Result<()> {
    if config.preflight_job.is_some() {
        return Err(Error::NotReady);
    }
    // SAFETY: the caller supplies the same qualified publisher/subscriber contract.
    unsafe { run_inner(config, strategy, None) }
}
/// Run only the object returned by a successful, synchronized startup restoration.
/// # Safety
/// The caller enforces the same qualified SHM peers and resource ownership as `run`.
pub unsafe fn run_restored<S: Strategy>(
    config: Bootstrap,
    strategy: S,
    completed: crate::preflight::Completed,
) -> Result<()> {
    if config.preflight_job.is_none()
        || completed.receipt().process.instance != config.market.local.instance
        || completed.receipt().process.generation != config.market.local.generation
    {
        return Err(Error::Identity);
    }
    completed.receipt().output.verify(
        &strategy.checkpoint()?,
        tickwire_recovery::MAX_OBJECT_BYTES as u64,
    )?;
    // SAFETY: this forwards the caller's qualified resource contract unchanged.
    unsafe { run_inner(config, strategy, Some(completed)) }
}
unsafe fn run_inner<S: Strategy>(
    config: Bootstrap,
    strategy: S,
    completed: Option<crate::preflight::Completed>,
) -> Result<()> {
    if config.version != 1
        || config.maximum_market_age_ns == 0
        || config.poll_records == 0
        || config.poll_records > 4096
        || config.startup_timeout_ms == 0
        || config.startup_timeout_ms > 60000
        || config.idle_sleep_us > 1_000_000
        || config.market.local.role != 3
        || config.intent.local.role != 3
        || config.market.local.instance != config.intent.local.instance
        || config.market.local.generation != config.intent.local.generation
        || config.market.peer.role != 2
        || config.intent.peer.role != 4
        || config.report.local.identity() != config.market.local.identity()
        || config.report.peer.identity() != config.intent.peer.identity()
    {
        return Err(Error::Identity);
    }
    let domain =
        tickwire_clock::ClockDomain::new(config.market.local.host_boot, config.clock_generation)
            .map_err(|_| Error::Identity)?;
    let mut clock = SystemClock::new(domain)?;
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(config.startup_timeout_ms))
        .ok_or(Error::Bounds)?;
    let remote = config.external_market.is_some();
    let (
        mut market,
        mut market_session,
        consumer,
        mut replica,
        snapshot_bytes,
        baseline_now,
        catchup,
        join_config,
        bank,
        j,
    ) = if let Some(external) = &config.external_market {
        if external.endpoint.local.identity() != config.market.local.identity()
            || external.endpoint.peer.identity() != config.market.peer.identity()
            || external.domain()? != domain
            || external.maximum_levels != config.maximum_levels
        {
            return Err(Error::Identity);
        }
        let ready = config
            .market_admission_ready
            .as_ref()
            .ok_or(Error::Identity)?;
        loop {
            match read_config::<bool>(ready, 16) {
                Ok(true) => break,
                Err(Error::Io(e))
                    if e.kind() == std::io::ErrorKind::NotFound && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(1))
                }
                _ => return Err(Error::NotReady),
            }
        }
        let client = connect_market(external, &mut clock)?;
        (
            client.connection,
            client.session,
            client.consumer,
            MarketReplica::from_core(client.replica),
            client.baseline_bytes,
            client.baseline_now,
            client.catchup,
            client.baseline,
            client.bank,
            client.join,
        )
    } else {
        let (mut market, mut market_session) =
            connect(&config.market, &mut clock, deadline, [1; 16])?;
        let join_config: JoinBootstrap = loop {
            match read_config(&config.join_file, 2 * 1024 * 1024) {
                Ok(v) => break v,
                Err(Error::Io(e))
                    if e.kind() == std::io::ErrorKind::NotFound && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(1))
                }
                Err(e) => return Err(e),
            }
        };
        let (bank, snapshot, j, registry) = join_config.decode()?;
        if j.subscriber_instance != config.market.local.instance
            || j.logical_strategy_id != config.market.local.logical_strategy
            || bank.producer_instance != config.market.peer.instance
            || bank.producer_generation != config.market.peer.generation
            || bank.host_boot_id != config.market.local.host_boot
            || bank.namespace_id != config.market.local.namespace
            || bank.required_features & 64 == 0
        {
            return Err(Error::Identity);
        }
        let fd = accept_offer(
            &mut market_session,
            market.recv(deadline)?,
            &bank,
            offer_id(&j, 1),
            j.readiness_deadline_mono_ns,
            j.membership_version,
            false,
            now(&mut clock)?,
        )?;
        let sf = accept_offer(
            &mut market_session,
            market.recv(deadline)?,
            &snapshot,
            offer_id(&j, 2),
            j.readiness_deadline_mono_ns,
            j.membership_version,
            true,
            now(&mut clock)?,
        )?;
        // SAFETY: exact trusted launcher identities and authenticated descriptors have
        // been checked. This entry point is only for qualified Publisher/Subscriber SDKs.
        let mut consumer = unsafe {
            tickwire_join::JoinConsumer::<4>::attach(
                fd,
                sf,
                bank.clone(),
                snapshot,
                j.clone(),
                registry,
                now(&mut clock)?,
            )?
        };
        let baseline_now = now(&mut clock)?;
        let (replica, snapshot_bytes) = consumer.load_baseline(baseline_now, |topic| {
            Ok((
                MarketReplica::load(j.topic_id, j.ring_epoch, topic, config.maximum_levels)?,
                topic.bytes().to_vec(),
            ))
        })?;
        (
            market,
            market_session,
            consumer,
            replica,
            snapshot_bytes,
            baseline_now,
            Vec::new(),
            join_config,
            bank,
            j,
        )
    };
    let mut recovery = if let Some(path) = &config.recovery_template {
        let template: crate::recovery::Template = read_config(path, 2 * 1024 * 1024)?;
        if template.owner != config.market.local.instance {
            return Err(Error::Identity);
        }
        let snapshot = crate::recovery::snapshot_payload(
            &join_config,
            &snapshot_bytes,
            baseline_now,
            config.maximum_levels,
        )?;
        Some(crate::recovery::RecoveryWriter::new(
            template,
            &strategy.checkpoint()?,
            &snapshot,
            config.maximum_market_age_ns,
        )?)
    } else {
        if config.demo_enabled {
            return Err(Error::NotReady);
        }
        None
    };
    for record in catchup {
        crate::recovery::InputJournal::market(&mut recovery, &record.record, &record.payload)?;
        // Recovery starts inactive. Record the actual pre-activation apply time;
        // replay applies the input but does not invoke strategy code at this cut.
        crate::recovery::InputJournal::decision(
            &mut recovery,
            record.instrument,
            record.applied_mono_ns,
        )?;
    }
    if !remote {
        let (cursor, fd) = consumer.cursor_offer(&market_session, [2; 16])?;
        market.send(&cursor, &[fd.as_fd()], deadline)?;
        for frame in consumer.read_ready_frames(&market_session, [[3; 16], [4; 16]])? {
            market.send(&frame, &[], deadline)?;
        }
        // Catch up without running strategy code before activation.
        replica.drain_recorded(
            &consumer,
            &mut crate::ObserveOnly,
            now(&mut clock)?,
            config.poll_records,
            &mut recovery,
            |_, _| Ok(()),
        )?;
        let ready = consumer.join_ready_frame(&market_session, [5; 16], now(&mut clock)?)?;
        market.send(&ready, &[], deadline)?;
        activated(
            &mut market_session,
            market.recv(deadline)?,
            &j,
            [5; 16],
            now(&mut clock)?,
        )?;
    }
    let IntentLane {
        connection: intent,
        session: intent_session,
        writer,
        store,
    } = connect_intent(&config, &mut clock, deadline)?;
    let (mut report, mut report_session) = connect(&config.report, &mut clock, deadline, [13; 16])?;
    let report_bank = config.report_resource()?;
    if report_bank.consumer_instance != config.report.local.instance
        || report_bank.producer_instance != config.report.peer.instance
        || report_bank.producer_generation != config.report.peer.generation
        || report_bank.host_boot_id != config.report.local.host_boot
        || report_bank.namespace_id != config.report.local.namespace
    {
        return Err(Error::Identity);
    }
    let report_sub = SubscriptionKey {
        bank: tickwire_shm::BankKey {
            resource: u128::from_le_bytes(report_bank.resource_id),
            channel: u128::from_le_bytes(report_bank.channel_id),
            channel_epoch: report_bank.channel_epoch,
            ring_epoch: report_bank.ring_epoch,
            feed_generation: report_bank.feed_generation,
        },
        instance: u128::from_le_bytes(config.report.local.instance),
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
        now(&mut clock)?,
    )?;
    let objects =
        tickwire_recovery::FileBlobReader::open(&config.report_directory, config.report.peer_uid)?;
    let reports = attach_reports(
        fd,
        report_bank.clone(),
        report_sub,
        objects,
        crate::reports::ReportIdentity {
            logical_strategy_id: config.report.local.logical_strategy,
            instance_id: config.report.local.instance,
            strategy_generation: config.report.local.generation,
            risk_domain: config.risk_domain,
        },
    )?;
    let cursor = reports.cursor_fd()?;
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
    if let Some(completed) = &completed {
        let r = completed.receipt();
        let body = tickwire_ipc::RestoreAppliedBody {
            instance_id: r.process.instance,
            logical_strategy_id: r.logical_strategy,
            strategy_generation: r.process.generation,
            receipt_digest: completed.digest(),
            receipt_bytes: completed.bytes().len() as u64,
            receipt_sequence: 1,
            release_manifest_digest: r.roots[0],
            schema: 1,
            completion_kind: 1,
        };
        let raw = report_session.encode(
            ControlEnvelope {
                opcode: 38,
                flags: 32,
                request_id: r.process.instance,
                ..Default::default()
            },
            &crate::bootstrap::wire_bytes(&body)?,
        )?;
        report.send(&raw, &[], deadline)?;
        let packet = report.recv(Instant::now() + Duration::from_secs(12))?;
        if !packet.fds().is_empty() {
            return Err(Error::Identity);
        }
        let frame = report_session.validate(packet.bytes(), now(&mut clock)?)?;
        let status = tickwire_ipc::StatusBody::decode(frame.body)?;
        let expected = tickwire_ipc::StatusBody {
            subject_id: body.instance_id,
            original_request_id: r.process.instance,
            original_control_epoch: report_session.epoch(),
            state_version: 1,
            state: 3,
            result_hash: status.result_hash,
            ..Default::default()
        };
        if frame.envelope.opcode != 49
            || frame.envelope.flags != 10
            || frame.envelope.request_id != r.process.instance
            || frame.envelope.expected_state_version != 0
            || status != expected
            || status.result_hash == [0; 32]
        {
            return Err(Error::Identity);
        }
        let job: crate::preflight::Job = read_config(
            config.preflight_job.as_ref().ok_or(Error::Identity)?,
            2 * 1024 * 1024,
        )?;
        if job.operation.schema == 3 {
            let verified =
                crate::preflight::startup_completion(&job, completed, status.result_hash)?;
            recovery
                .as_mut()
                .ok_or(Error::NotReady)?
                .install_replay_restore(status.result_hash, verified)?;
        } else {
            let verified = crate::preflight::completion(&job, completed, status.result_hash)?;
            recovery
                .as_mut()
                .ok_or(Error::NotReady)?
                .install_preflight(status.result_hash, verified)?;
        }
    }
    if let Some(w) = &mut recovery {
        w.set_active();
    }
    let strategy =
        crate::replay::live::Live::new(&config, strategy, bank, replica, now(&mut clock)?)?;
    if let Some(anchor) = strategy.anchor_bytes()? {
        recovery
            .as_mut()
            .ok_or(Error::NotReady)?
            .merged_anchor(&anchor)?;
    }
    run_attached(Attached {
        config,
        clock,
        market,
        market_session,
        consumer,
        j,
        intent,
        intent_session,
        writer,
        store,
        report,
        report_session,
        report_bank,
        report_sub,
        report_membership,
        reports,
        recovery,
        strategy,
        account_mirror: crate::handover::AccountMirror::default(),
        merged_journal_sequence: 0,
        ready_sequence: 0,
        rebuild_installation: None,
    })
}

/// Resources that have already completed their real authenticated attachments.
/// Moving this value preserves the original MD and report cursor lifetimes.
pub(crate) struct InstalledControl {
    pub request: tickwire_ipc::ReplayControlBody,
    pub receipt: tickwire_release::Object,
    pub verified: tickwire_release::rebuild::install::Verified,
    pub capture: crate::capture::Bootstrap,
    pub input: crate::capture::install::Request,
    pub rollback: Option<(tickwire_ipc::ReplayControlBody, tickwire_release::Object)>,
}
pub(crate) struct Attached<S> {
    pub config: Bootstrap,
    pub clock: SystemClock,
    pub market: Connection,
    pub market_session: Session,
    pub consumer: tickwire_join::JoinConsumer<4>,
    pub j: tickwire_ipc::JoinBody,
    pub intent: Connection,
    pub intent_session: Session,
    pub writer: IntentWriter,
    pub store: CommandStore<tickwire_recovery::FileBlobStore>,
    pub report: Connection,
    pub report_session: Session,
    pub report_bank: tickwire_ipc::ResourceHeader,
    pub report_sub: SubscriptionKey,
    pub report_membership: u64,
    pub reports: crate::reports::ReportReceiver,
    pub recovery: Option<crate::recovery::RecoveryWriter>,
    pub strategy: crate::replay::live::Live<S>,
    pub account_mirror: crate::handover::AccountMirror,
    pub merged_journal_sequence: u64,
    pub ready_sequence: u64,
    pub rebuild_installation: Option<InstalledControl>,
}
pub(crate) fn run_attached<S: Strategy>(state: Attached<S>) -> Result<()> {
    let Attached {
        config,
        mut clock,
        mut market,
        mut market_session,
        mut consumer,
        mut j,
        mut intent,
        mut intent_session,
        mut writer,
        mut store,
        mut report,
        mut report_session,
        report_bank,
        report_sub,
        report_membership,
        mut reports,
        mut recovery,
        mut strategy,
        mut account_mirror,
        mut merged_journal_sequence,
        mut ready_sequence,
        mut rebuild_installation,
    } = state;
    let domain = clock.domain();
    let remote = config.external_market.is_some();
    let mut pending_seal: Option<(ControlEnvelope, tickwire_ipc::CheckpointSealBody)> = None;
    let mut callbacks_paused = false;
    let mut pending_replay_install: Option<tickwire_ipc::ReplayControlBody> = None;
    let mut replay_completion_pending = false;
    let mut current_market = config.external_market.clone();
    let mut expansion: Option<expansion::Expansion> = None;
    let mut expansion_replies = std::collections::VecDeque::<Vec<u8>>::new();
    loop {
        let n = now(&mut clock)?;
        let transition_paused = expansion.as_ref().is_some_and(|x| x.paused());
        if expansion
            .as_ref()
            .is_some_and(|x| x.active() && n >= x.approval.deadline_mono_ns)
        {
            if transition_paused {
                return Err(Error::Deadline);
            }
            expansion = None;
            expansion_replies.clear();
        }
        if (!transition_paused && n >= market_session.deadline())
            || n >= intent_session.deadline()
            || n >= report_session.deadline()
        {
            return Err(Error::Deadline);
        }
        let mut report_count = 0;
        for _ in 0..config.poll_records {
            if callbacks_paused
                || transition_paused
                || pending_seal
                    .as_ref()
                    .is_some_and(|(_, s)| account_mirror.position() == Some(s.report_sequence))
            {
                break;
            }
            let Some(value) = reports.next()? else {
                break;
            };
            report_count += 1;
            let at = now(&mut clock)?;
            if let Some(w) = &mut recovery {
                w.account_report(&value, at)?;
            }
            let commands = strategy.report(&value, at)?;
            retain_merged(&strategy, &mut recovery, &mut merged_journal_sequence)?;
            for command in commands {
                prepare_and_publish(
                    || recovery.as_mut().ok_or(Error::NotReady)?.prepare(&command),
                    || now(&mut clock),
                    |published_at| {
                        writer
                            .publish(&command, &mut store, published_at)
                            .map(|_| ())
                    },
                )
                .map_err(|e| e.context("report intent"))?;
            }
            if config.handover_enabled {
                account_mirror.observe(&value)?;
            }
        }
        if config.handover_enabled && report_count > 0 {
            if let Some((cut, _)) = strategy.committed() {
                recovery
                    .as_mut()
                    .ok_or(Error::NotReady)?
                    .merged_checkpoint(cut)?;
            }
            ready_sequence = ready_sequence.checked_add(1).ok_or(Error::Bounds)?;
            let ready = account_mirror.ready(
                &config.market.local,
                reports.last_ring_sequence()?,
                ready_sequence,
                recovery.as_ref().ok_or(Error::NotReady)?,
                &strategy.checkpoint()?,
            )?;
            let mut request_id = config.market.local.instance;
            request_id[..8].copy_from_slice(&ready_sequence.to_le_bytes());
            let frame = report_session.encode(
                ControlEnvelope {
                    opcode: 37,
                    flags: 32,
                    request_id,
                    ..Default::default()
                },
                &crate::bootstrap::wire_bytes(&ready)?,
            )?;
            report.send(&frame, &[], Instant::now() + Duration::from_millis(100))?;
        }
        if let Some((request, seal)) = &pending_seal {
            if now(&mut clock)? >= seal.valid_until_mono_ns
                || account_mirror
                    .position()
                    .is_some_and(|p| p > seal.report_sequence)
            {
                return Err(Error::Stale);
            }
            if account_mirror.position() == Some(seal.report_sequence) {
                if let Some((cut, _)) = strategy.committed() {
                    recovery
                        .as_mut()
                        .ok_or(Error::NotReady)?
                        .merged_checkpoint(cut)?;
                }
                ready_sequence = ready_sequence.checked_add(1).ok_or(Error::Bounds)?;
                let ready = account_mirror.ready(
                    &config.market.local,
                    reports.last_ring_sequence()?,
                    ready_sequence,
                    recovery.as_ref().ok_or(Error::NotReady)?,
                    &strategy.checkpoint()?,
                )?;
                let reply = tickwire_ipc::CheckpointSealReplyBody {
                    challenge: seal.challenge,
                    report_sequence: seal.report_sequence,
                    seal_mode: seal.seal_mode,
                    schema: 1,
                    ready,
                };
                let raw = report_session.encode(
                    ControlEnvelope {
                        opcode: 40,
                        flags: 10,
                        ..request.clone()
                    },
                    &crate::bootstrap::wire_bytes(&reply)?,
                )?;
                if now(&mut clock)? >= seal.valid_until_mono_ns {
                    return Err(Error::Deadline);
                }
                report.send(&raw, &[], Instant::now() + Duration::from_millis(100))?;
                callbacks_paused |= seal.seal_mode == 1;
                pending_seal = None;
            }
        }
        let count = if callbacks_paused || transition_paused || pending_seal.is_some() {
            0
        } else {
            let mut count = 0;
            while count < config.poll_records {
                let Some(guard) = consumer.borrow()? else {
                    break;
                };
                let header = tickwire_ipc::RecordHeader::decode(&guard.bytes()[..128])?;
                let payload = if header.message_type == 259 {
                    guard.depth_payload()?.to_vec()
                } else {
                    vec![]
                };
                let raw = guard.bytes().to_vec();
                let seq = guard.sequence();
                drop(guard);
                crate::recovery::InputJournal::market(&mut recovery, &raw, &payload)?;
                let at = now(&mut clock)?;
                crate::recovery::InputJournal::decision(&mut recovery, header.instrument_id, at)?;
                // The actual callback and its input/checkpoint sync complete before
                // the consumer watermark or an intent is exposed to another owner.
                let commands = strategy.market(&raw, &payload, at)?;
                retain_merged(&strategy, &mut recovery, &mut merged_journal_sequence)?;
                consumer.apply(seq)?;
                for command in commands {
                    prepare_and_publish(
                        || recovery.as_mut().ok_or(Error::NotReady)?.prepare(&command),
                        || now(&mut clock),
                        |published_at| {
                            writer
                                .publish(&command, &mut store, published_at)
                                .map(|_| ())
                        },
                    )
                    .map_err(|e| e.context("market intent"))?;
                }
                count += 1;
            }
            let commands = strategy.timers(now(&mut clock)?, config.poll_records.min(32))?;
            retain_merged(&strategy, &mut recovery, &mut merged_journal_sequence)?;
            for command in commands {
                prepare_and_publish(
                    || recovery.as_mut().ok_or(Error::NotReady)?.prepare(&command),
                    || now(&mut clock),
                    |published_at| {
                        writer
                            .publish(&command, &mut store, published_at)
                            .map(|_| ())
                    },
                )
                .map_err(|e| e.context("timer intent"))?;
            }
            count
        };
        // Each complete drain has ended all payload references before control handling.
        if let Some(bytes) = expansion_replies.front()
            && transition_control::try_send(&mut report, bytes)?
        {
            expansion_replies.pop_front();
        }
        if let Some(x) = &mut expansion {
            if let Some(bytes) = &x.market_reply
                && transition_control::try_send(&mut market, bytes)?
            {
                x.market_reply = None;
            }
            if let Some(pending) = &mut x.pending {
                pending.poll(
                    tickwire_clock::MonoTime::new(domain, now(&mut clock)?),
                    config.poll_records,
                )?;
                if let Some(client) = pending.take_client() {
                    x.candidate = Some(client);
                    x.pending = None;
                }
            }
            let ready = if let Some(client) = &mut x.candidate {
                expansion::poll_frontier(
                    client,
                    tickwire_clock::MonoTime::new(domain, now(&mut clock)?),
                    x.approval.deadline_mono_ns,
                    config.maximum_market_age_ns,
                )?
            } else {
                false
            };
            if ready {
                let mut client = x.candidate.take().ok_or(Error::NotReady)?;
                let at = now(&mut clock)?;
                let previous_applied = x.previous_applied()?;
                let receipt = Receipt::activated(
                    &x.approval,
                    std::process::id(),
                    previous_applied,
                    &client,
                    tickwire_clock::MonoTime::new(domain, at),
                )?;
                strategy.transition_market(
                    crate::replay::live::MarketTransitionRequest {
                        operation_id: x.approval.operation_id,
                        approval_digest: x.approval.approval_digest,
                        previous_applied,
                        maximum_levels: config.maximum_levels,
                    },
                    &client,
                    at,
                )?;
                retain_merged(&strategy, &mut recovery, &mut merged_journal_sequence)?;
                let (checkpoint, _) = strategy.committed().ok_or(Error::NotReady)?;
                recovery
                    .as_mut()
                    .ok_or(Error::NotReady)?
                    .merged_checkpoint(checkpoint)?;
                let object = strategy.store_object(&serde_json::to_vec(&receipt)?)?;
                x.approval.validate_receipt(
                    &receipt,
                    std::process::id(),
                    Phase::Activated,
                    tickwire_clock::MonoTime::new(domain, now(&mut clock)?),
                )?;
                // Persistence may outlive the new session or receive QUIESCE.
                // Recheck the retained client before enabling any callback.
                if !expansion::poll_frontier(
                    &mut client,
                    tickwire_clock::MonoTime::new(domain, now(&mut clock)?),
                    x.approval.deadline_mono_ns,
                    config.maximum_market_age_ns,
                )? {
                    return Err(Error::NotReady);
                }
                let (commit, request) = x.commit.as_ref().ok_or(Error::NotReady)?;
                let reply = transition_control::encode_reply(&report_session, request, object)?;
                market = client.connection;
                market_session = client.session;
                consumer = client.consumer;
                j = client.join;
                current_market = Some(commit.config.clone());
                x.receipt = receipt;
                x.activated = Some(object);
                x.pending = None;
                expansion::queue(&mut expansion_replies, reply)?;
            }
        }
        if !expansion.as_ref().is_some_and(|x| x.paused()) {
            match market.try_recv() {
                Ok(Some(packet)) => {
                    if !packet.fds().is_empty() {
                        return Err(Error::Identity);
                    }
                    let envelope = ControlEnvelope::decode(&packet.bytes()[..128])?;
                    if envelope.opcode == 20 {
                        if let Some(x) = &mut expansion {
                            if x.active() {
                                if envelope.request_id
                                    != tickwire_market_source::transition::quiesce_request_id(
                                        x.approval.operation_id,
                                        transition_control::subscription(&x.approval.previous)?,
                                    )
                                {
                                    return Err(Error::Identity);
                                }
                                let at = now(&mut clock)?;
                                let response =
                                    consumer.quiesce(&mut market_session, packet.bytes(), at)?;
                                let receipt = Receipt::quiesced(
                                    &x.approval,
                                    std::process::id(),
                                    consumer.watermarks().applied,
                                    tickwire_clock::MonoTime::new(domain, at),
                                )?;
                                let object =
                                    strategy.store_object(&serde_json::to_vec(&receipt)?)?;
                                x.receipt = receipt;
                                x.quiesced = Some(object);
                                x.market_reply = Some(response);
                                expansion::queue(
                                    &mut expansion_replies,
                                    transition_control::encode_reply(
                                        &report_session,
                                        &x.arm,
                                        object,
                                    )?,
                                )?;
                            } else {
                                let response = consumer.quiesce(
                                    &mut market_session,
                                    packet.bytes(),
                                    now(&mut clock)?,
                                )?;
                                market.send(
                                    &response,
                                    &[],
                                    Instant::now() + Duration::from_secs(1),
                                )?;
                                return Ok(());
                            }
                        } else {
                            let response = consumer.quiesce(
                                &mut market_session,
                                packet.bytes(),
                                now(&mut clock)?,
                            )?;
                            market.send(&response, &[], Instant::now() + Duration::from_secs(1))?;
                            return Ok(());
                        }
                    } else {
                        market_session.validate(packet.bytes(), now(&mut clock)?)?;
                        return Err(Error::NotReady);
                    }
                    // The approved old consumer is detached; subsequent work remains in this process.
                }
                Ok(None) => {}
                Err(e) => return Err(e.into()),
            }
        }
        match intent.try_recv() {
            Ok(Some(packet)) => {
                intent_session.validate(packet.bytes(), now(&mut clock)?)?;
                return Err(Error::NotReady);
            }
            Ok(None) => {}
            Err(e) => return Err(e.into()),
        }
        match report.try_recv() {
            Ok(Some(packet)) => {
                if !packet.fds().is_empty() {
                    return Err(Error::Identity);
                }
                let received_at = now(&mut clock)?;
                let frame = report_session.validate(packet.bytes(), received_at)?;
                if frame.envelope.opcode == 49 {
                    let request = pending_replay_install.as_ref().ok_or(Error::Identity)?;
                    let status = tickwire_ipc::StatusBody::decode(frame.body)?;
                    let expected = tickwire_ipc::StatusBody {
                        subject_id: request.instance_id,
                        original_request_id: request.challenge,
                        original_control_epoch: frame.envelope.control_epoch,
                        state_version: request.expected_sequence,
                        state: 3,
                        result_hash: status.result_hash,
                        ..Default::default()
                    };
                    if frame.envelope.flags != 10
                        || frame.envelope.request_id != request.challenge
                        || frame.envelope.expected_state_version != request.expected_sequence
                        || status != expected
                        || status.result_hash == [0; 32]
                    {
                        return Err(Error::Identity);
                    }
                    let objects = tickwire_recovery::FileBlobReader::open(
                        &config.report_directory,
                        config.report.peer_uid,
                    )?;
                    let verified =
                        tickwire_release::replay::rollback::verify(status.result_hash, |h| {
                            objects
                                .get_object(h, tickwire_recovery::MAX_OBJECT_BYTES)
                                .map_err(|_| tickwire_release::Error::Missing)
                        })?;
                    if verified.installation.installation.request.digest != request.object_digest
                        || verified.installation.request.operation.operation_id
                            != request.operation_id
                        || verified.installation.request.operation.operation_revision
                            != request.operation_revision
                        || now(&mut clock)? >= verified.installation.request.member.valid_until_ns
                    {
                        return Err(Error::Identity);
                    }
                    recovery
                        .as_mut()
                        .ok_or(Error::NotReady)?
                        .install_replay_rollback(status.result_hash, verified)?;
                    let (cut, _) = strategy.committed().ok_or(Error::NotReady)?;
                    recovery
                        .as_mut()
                        .ok_or(Error::NotReady)?
                        .merged_checkpoint(cut)?;
                    ready_sequence = ready_sequence.checked_add(1).ok_or(Error::Bounds)?;
                    let ready = account_mirror.ready(
                        &config.market.local,
                        reports.last_ring_sequence()?,
                        ready_sequence,
                        recovery.as_ref().ok_or(Error::NotReady)?,
                        &strategy.checkpoint()?,
                    )?;
                    let raw = report_session.encode(
                        ControlEnvelope {
                            opcode: 37,
                            flags: 32,
                            request_id: request.challenge,
                            ..Default::default()
                        },
                        &crate::bootstrap::wire_bytes(&ready)?,
                    )?;
                    report.send(&raw, &[], Instant::now() + Duration::from_millis(100))?;
                    replay_completion_pending = false;
                    // Retain the original challenge for exact completion retries.
                    // RecoveryWriter rejects a different completion root.
                    continue;
                }
                if frame.envelope.opcode == 41 {
                    use tickwire_release::replay::{ControlInput, canonical};
                    let body = tickwire_ipc::ReplayControlBody::decode(frame.body)?;
                    if body.replay_kind == crate::capture::install::rollback::KIND {
                        let installed = rebuild_installation.as_mut().ok_or(Error::NotReady)?;
                        let object = rebuild::rollback(
                            installed,
                            &frame.envelope,
                            &body,
                            &config,
                            &mut clock,
                            &mut strategy,
                            recovery.as_mut().ok_or(Error::NotReady)?,
                            report_session.deadline(),
                        )?;
                        ready_sequence = ready_sequence.checked_add(1).ok_or(Error::Bounds)?;
                        let ready = account_mirror.ready(
                            &config.market.local,
                            reports.last_ring_sequence()?,
                            ready_sequence,
                            recovery.as_ref().ok_or(Error::NotReady)?,
                            &strategy.checkpoint()?,
                        )?;
                        let fresh = now(&mut clock)?;
                        if fresh >= body.valid_until_mono_ns
                            || fresh >= market_session.deadline()
                            || fresh >= intent_session.deadline()
                            || fresh >= report_session.deadline()
                        {
                            return Err(Error::Deadline);
                        }
                        expansion::queue(
                            &mut expansion_replies,
                            crate::capture::encode_reply(&report_session, &body, object)?,
                        )?;
                        expansion::queue(
                            &mut expansion_replies,
                            report_session.encode(
                                ControlEnvelope {
                                    opcode: 37,
                                    flags: 32,
                                    request_id: body.challenge,
                                    ..Default::default()
                                },
                                &crate::bootstrap::wire_bytes(&ready)?,
                            )?,
                        )?;
                        continue;
                    }
                    if body.replay_kind == crate::capture::install::KIND {
                        let installed = rebuild_installation.as_ref().ok_or(Error::NotReady)?;
                        let raw = if body == installed.request
                            && frame.envelope.flags == 1
                            && frame.envelope.status == 0
                            && frame.envelope.request_id == body.challenge
                            && frame.envelope.expected_state_version == 0
                            && now(&mut clock)? < body.valid_until_mono_ns
                        {
                            crate::capture::encode_reply(&report_session, &body, installed.receipt)?
                        } else {
                            crate::capture::encode_rejection(
                                &report_session,
                                &body,
                                crate::market_transition::INVALID,
                            )?
                        };
                        expansion::queue(&mut expansion_replies, raw)?;
                        continue;
                    }
                    let objects = tickwire_recovery::FileBlobReader::open(
                        &config.report_directory,
                        config.report.peer_uid,
                    )?;
                    let raw = objects
                        .get_object(body.object_digest, tickwire_release::replay::MAX_BYTES)?;
                    if matches!(
                        body.replay_kind,
                        transition_control::ARM | transition_control::COMMIT
                    ) {
                        if body.instance_id != config.report.local.instance
                            || body.strategy_generation != config.report.local.generation
                            || body.logical_strategy_id != config.report.local.logical_strategy
                            || body.object_bytes != raw.len() as u64
                            || raw.len() > transition_control::MAX_BYTES
                        {
                            return Err(Error::Identity);
                        }
                        let at = tickwire_clock::MonoTime::new(domain, now(&mut clock)?);
                        if body.replay_kind == transition_control::ARM {
                            let approval = match Approval::decode(&raw) {
                                Ok(value) => value,
                                Err(_) => {
                                    expansion::queue(
                                        &mut expansion_replies,
                                        transition_control::encode_rejection(
                                            &report_session,
                                            &body,
                                            transition_control::INVALID,
                                        )?,
                                    )?;
                                    continue;
                                }
                            };
                            transition_control::verify_request(
                                &frame.envelope,
                                &body,
                                &approval,
                                &raw,
                                at.nanos(),
                                report_session.deadline(),
                            )?;
                            if let Some(x) = &expansion
                                && x.approval.digest()? == approval.digest()?
                            {
                                expansion::queue(
                                    &mut expansion_replies,
                                    transition_control::encode_reply(
                                        &report_session,
                                        &body,
                                        x.quiesced.unwrap_or(x.armed),
                                    )?,
                                )?;
                                continue;
                            }
                            let rejection = expansion::arm_rejection(
                                &approval,
                                current_market.as_ref(),
                                strategy.supports_market_transition(),
                                callbacks_paused
                                    || pending_seal.is_some()
                                    || replay_completion_pending
                                    || expansion.as_ref().is_some_and(|x| x.active()),
                                at,
                            );
                            if let Some(status) = rejection {
                                expansion::queue(
                                    &mut expansion_replies,
                                    transition_control::encode_rejection(
                                        &report_session,
                                        &body,
                                        status,
                                    )?,
                                )?;
                                continue;
                            }
                            strategy.store_object(&raw)?;
                            let receipt = Receipt::armed(&approval, std::process::id(), at)?;
                            let armed = strategy.store_object(&serde_json::to_vec(&receipt)?)?;
                            expansion::queue(
                                &mut expansion_replies,
                                transition_control::encode_reply(&report_session, &body, armed)?,
                            )?;
                            expansion = Some(expansion::Expansion {
                                approval,
                                arm: body,
                                armed,
                                receipt,
                                quiesced: None,
                                commit: None,
                                activated: None,
                                pending: None,
                                candidate: None,
                                market_reply: None,
                            });
                        } else {
                            let Some(x) = &mut expansion else {
                                expansion::queue(
                                    &mut expansion_replies,
                                    transition_control::encode_rejection(
                                        &report_session,
                                        &body,
                                        transition_control::INVALID,
                                    )?,
                                )?;
                                continue;
                            };
                            transition_control::verify_request(
                                &frame.envelope,
                                &body,
                                &x.approval,
                                &raw,
                                at.nanos(),
                                report_session.deadline(),
                            )?;
                            let commit = Commit::decode(&raw)?;
                            if let Some((old, _)) = &x.commit {
                                if serde_json::to_vec(old)? != raw {
                                    return Err(Error::Identity);
                                }
                                if let Some(object) = x.activated {
                                    expansion::queue(
                                        &mut expansion_replies,
                                        transition_control::encode_reply(
                                            &report_session,
                                            &body,
                                            object,
                                        )?,
                                    )?;
                                }
                                continue;
                            }
                            if !x.paused() {
                                expansion::queue(
                                    &mut expansion_replies,
                                    transition_control::encode_rejection(
                                        &report_session,
                                        &body,
                                        transition_control::BUSY,
                                    )?,
                                )?;
                                continue;
                            }
                            commit.validate(&x.approval, x.previous_applied()?, at)?;
                            strategy.store_object(&raw)?;
                            // SAFETY: authenticated qualified Host approved this exact successor;
                            // the same immutable bank and cursor contracts as startup still apply.
                            let pending = unsafe {
                                tickwire_market_source::PendingJoin::new(&commit.config, at)?
                            };
                            x.commit = Some((commit, body));
                            x.pending = Some(pending);
                        }
                        continue;
                    }
                    if expansion.as_ref().is_some_and(|x| x.active()) {
                        return Err(Error::NotReady);
                    }
                    let input = ControlInput::decode(&raw)?;
                    let expected = strategy.control_checkpoint(&input)?;
                    crate::replay::control::verify_request(
                        &frame.envelope,
                        &body,
                        &input,
                        &expected,
                        now(&mut clock)?,
                        report_session.deadline(),
                    )?;
                    let mut installed_ready = false;
                    let (object, commands) = match &input {
                        ControlInput::Capture { pause, .. } => {
                            let object = strategy.capture(&input)?;
                            callbacks_paused |= *pause;
                            (object, vec![])
                        }
                        ControlInput::Auxiliary { input, .. } => {
                            if callbacks_paused || pending_seal.is_some() {
                                return Err(Error::NotReady);
                            }
                            let commands = strategy.auxiliary(input.clone(), now(&mut clock)?)?;
                            retain_merged(&strategy, &mut recovery, &mut merged_journal_sequence)?;
                            let (cut, commit) = strategy.committed().ok_or(Error::NotReady)?;
                            let bytes = canonical(&(body.object_digest, cut.cut.clone(), commit))?;
                            (strategy.store_object(&bytes)?, commands)
                        }
                        ControlInput::Install { request, .. } => {
                            if pending_seal.is_some() {
                                return Err(Error::NotReady);
                            }
                            let job: crate::preflight::Job = read_config(
                                config.preflight_job.as_ref().ok_or(Error::NotReady)?,
                                2 * 1024 * 1024,
                            )?;
                            if job.operation != request.operation || job.member != request.member {
                                return Err(Error::Identity);
                            }
                            let bundle = job.bundle.load(&job.operation)?;
                            let release = bundle.verify_operation(&job.operation, &job.member)?;
                            let files = tickwire_release::files::load(
                                &release.target.files,
                                &job.target_files,
                            )?;
                            let template: Bootstrap = serde_json::from_slice(
                                files
                                    .bytes(&tickwire_release::Role::Bootstrap)
                                    .ok_or(Error::Identity)?,
                            )?;
                            crate::preflight::verify_bootstrap(&template, &config)?;
                            let target = crate::preflight::target(&config, &files)?;
                            files.recheck()?;
                            let proof = request.source_proof(|o| {
                                objects
                                    .get_object(o.digest, tickwire_recovery::MAX_OBJECT_BYTES)
                                    .map_err(|_| tickwire_release::Error::Missing)
                            })?;
                            for bytes in bundle.object_bytes()? {
                                strategy.store_object(&bytes)?;
                            }
                            let root = strategy.install(
                                &input,
                                &release,
                                target,
                                &proof,
                                config.maximum_levels,
                                || {
                                    let at =
                                        now(&mut clock).map_err(|_| crate::replay::Error::Time)?;
                                    if at >= body.valid_until_mono_ns {
                                        return Err(crate::replay::Error::Time);
                                    }
                                    Ok(at)
                                },
                            )?;
                            let verified = strategy.installation_objects(root)?;
                            merged_journal_sequence = recovery
                                .as_mut()
                                .ok_or(Error::NotReady)?
                                .install_replayed(root.digest, verified)?;
                            callbacks_paused = false;
                            installed_ready = true;
                            (root, vec![])
                        }
                    };
                    if now(&mut clock)? >= body.valid_until_mono_ns {
                        return Err(Error::Deadline);
                    }
                    for command in commands {
                        prepare_and_publish(
                            || recovery.as_mut().ok_or(Error::NotReady)?.prepare(&command),
                            || now(&mut clock),
                            |at| writer.publish(&command, &mut store, at).map(|_| ()),
                        )?;
                    }
                    let reply = tickwire_ipc::ReplayAppliedBody {
                        instance_id: body.instance_id,
                        strategy_generation: body.strategy_generation,
                        logical_strategy_id: body.logical_strategy_id,
                        challenge: body.challenge,
                        valid_until_mono_ns: body.valid_until_mono_ns,
                        object_digest: object.digest,
                        object_bytes: object.bytes,
                        expected_sequence: body.expected_sequence,
                        expected_cut_digest: body.expected_cut_digest,
                        operation_id: body.operation_id,
                        operation_revision: body.operation_revision,
                        schema: 1,
                        replay_kind: body.replay_kind,
                    };
                    let raw = report_session.encode(
                        ControlEnvelope {
                            opcode: 42,
                            flags: 10,
                            ..frame.envelope.clone()
                        },
                        &crate::bootstrap::wire_bytes(&reply)?,
                    )?;
                    if now(&mut clock)? >= body.valid_until_mono_ns {
                        return Err(Error::Deadline);
                    }
                    report.send(&raw, &[], Instant::now() + Duration::from_millis(100))?;
                    if installed_ready {
                        pending_replay_install = Some(body.clone());
                        replay_completion_pending = true;
                        ready_sequence = ready_sequence.checked_add(1).ok_or(Error::Bounds)?;
                        let ready = account_mirror.ready(
                            &config.market.local,
                            reports.last_ring_sequence()?,
                            ready_sequence,
                            recovery.as_ref().ok_or(Error::NotReady)?,
                            &strategy.checkpoint()?,
                        )?;
                        let mut request_id = config.market.local.instance;
                        request_id[..8].copy_from_slice(&ready_sequence.to_le_bytes());
                        let ready_raw = report_session.encode(
                            ControlEnvelope {
                                opcode: 37,
                                flags: 32,
                                request_id,
                                ..Default::default()
                            },
                            &crate::bootstrap::wire_bytes(&ready)?,
                        )?;
                        report.send(
                            &ready_raw,
                            &[],
                            Instant::now() + Duration::from_millis(100),
                        )?;
                    }
                    continue;
                }
                if frame.envelope.opcode == 39 {
                    if !config.handover_enabled
                        || pending_seal.is_some()
                        || expansion.as_ref().is_some_and(|x| x.active())
                    {
                        return Err(Error::Identity);
                    }
                    let seal = crate::checkpoint::verify_request(
                        &frame.envelope,
                        frame.body,
                        &config.report.local,
                        received_at,
                        report_session.deadline(),
                    )?;
                    if account_mirror
                        .position()
                        .is_some_and(|p| p > seal.report_sequence)
                    {
                        return Err(Error::Stale);
                    }
                    pending_seal = Some((frame.envelope.clone(), seal));
                    continue;
                }
                if !remote || expansion.as_ref().is_some_and(|x| x.active()) {
                    return Err(Error::Identity);
                }
                let mut proof = crate::retirement::check_request(
                    &frame.envelope,
                    frame.body,
                    &report_bank,
                    report_sub,
                    report_membership,
                )?;
                let request = frame.envelope.clone();
                // No callback or borrowed payload remains. The report acknowledgement
                // is delayed until MD confirms the real mapped cursor detach.
                reports.start_quiesce();
                let released = reports.quiesced()?;
                proof.released_seq = released.watermarks().released;
                proof.applied_seq = released.watermarks().applied;
                proof.evidence_kind = 1;
                let until = Instant::now() + Duration::from_secs(3);
                crate::retirement::depart_market(
                    &mut market,
                    &mut market_session,
                    &consumer,
                    &j,
                    request.request_id,
                    &mut clock,
                    until,
                )?;
                let response = report_session.encode(
                    ControlEnvelope {
                        flags: 10,
                        ..request
                    },
                    &crate::bootstrap::wire_bytes(&proof)?,
                )?;
                report
                    .send(&response, &[], until)
                    .map_err(|e| Error::from(e).context("retirement: send Host quiesced"))?;
                return Ok(());
            }
            Ok(None) => {}
            Err(e) => return Err(e.into()),
        }
        if count == 0 && report_count == 0 {
            let sampled = now(&mut clock)?;
            let delay = Duration::from_micros(config.idle_sleep_us);
            let delay = strategy
                .next_deadline()
                .map(|at| delay.min(Duration::from_nanos(at.saturating_sub(sampled))))
                .unwrap_or(delay);
            std::thread::sleep(delay);
        }
    }
}

fn retain_merged<S: Strategy>(
    owner: &crate::replay::live::Live<S>,
    recovery: &mut Option<crate::recovery::RecoveryWriter>,
    sequence: &mut u64,
) -> Result<()> {
    for row in owner.recorded_after(*sequence) {
        recovery.as_mut().ok_or(Error::NotReady)?.merged_row(row)?;
        *sequence = row.sequence;
    }
    Ok(())
}

// Recovery preparation can block in fsync. Publication must observe the real
// clock after that work, rather than reuse the decision or loop-start timestamp.
fn prepare_and_publish(
    prepare: impl FnOnce() -> Result<()>,
    sample: impl FnOnce() -> Result<u64>,
    publish: impl FnOnce(u64) -> Result<()>,
) -> Result<()> {
    prepare().map_err(|e| e.context("recovery prepare"))?;
    let now = sample().map_err(|e| e.context("publication clock"))?;
    publish(now).map_err(|e| e.context("intent publish"))
}

#[cfg(test)]
mod publication_tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn publication_samples_after_preparation_and_checks_the_current_deadline() {
        // Decision at 50 is later than loop entry (40). Preparation may finish
        // either before or at the command's exclusive deadline (100).
        for finished in [60, 100] {
            let clock = Cell::new(40);
            let published = Cell::new(false);
            let result = prepare_and_publish(
                || {
                    clock.set(finished);
                    Ok(())
                },
                || Ok(clock.get()),
                |at| {
                    assert_eq!(at, finished);
                    if !(50..100).contains(&at) {
                        return Err(Error::Deadline);
                    }
                    published.set(true);
                    Ok(())
                },
            );
            assert_eq!(result.is_ok(), finished < 100);
            assert_eq!(published.get(), finished < 100);
            if let Err(e) = result {
                assert!(e.to_string().contains("intent publish"));
            }
        }
    }

    #[test]
    fn failed_preparation_never_samples_or_publishes() {
        let result = prepare_and_publish(
            || Err(Error::NotReady),
            || panic!("sample after failed preparation"),
            |_| panic!("publish after failed preparation"),
        );
        assert!(result.unwrap_err().to_string().contains("recovery prepare"));
    }
}
