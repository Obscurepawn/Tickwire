//! Qualified worker: authenticated launcher policies are the unsafe SHM admission boundary.
use crate::bootstrap::{Bootstrap, Endpoint, JoinBootstrap};
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
struct Fresh<S> {
    inner: S,
    maximum_age: u64,
}
impl<S: Strategy> Strategy for Fresh<S> {
    fn checkpoint(&self) -> Result<Vec<u8>> {
        self.inner.checkpoint()
    }
    fn on_report(
        &mut self,
        report: &crate::reports::AccountReport,
        now: u64,
    ) -> Result<Option<tickwire_venue::CanonicalCommand>> {
        self.inner.on_report(report, now)
    }
    fn on_market(
        &mut self,
        v: crate::MarketView<'_>,
        now: u64,
    ) -> Result<Option<tickwire_venue::CanonicalCommand>> {
        let received = v.received_mono_ns();
        if !v.live() || now < received || now - received > self.maximum_age {
            return Ok(None);
        }
        self.inner.on_market(v, now)
    }
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
pub unsafe fn run_restored(
    config: Bootstrap,
    strategy: crate::preflight::Running,
    completed: crate::preflight::Completed,
) -> Result<()> {
    if config.preflight_job.is_none()
        || completed.receipt().process.instance != config.market.local.instance
        || completed.receipt().process.generation != config.market.local.generation
    {
        return Err(Error::Identity);
    }
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
        _bank,
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
        // SAFETY: the trusted launcher admits this exact qualified child and MD identity.
        let client = unsafe {
            tickwire_market_source::Client::connect(external, &mut || {
                clock
                    .sample()
                    .map(|s| s.monotonic)
                    .map_err(|_| tickwire_market_source::Error::Identity)
            })?
        };
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
    let (mut intent, mut intent_session) = connect(&config.intent, &mut clock, deadline, [6; 16])?;
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
        now(&mut clock)?,
    )?;
    // SAFETY: the registered execution peer is authenticated, qualified, and its
    // cursor descriptor exactly binds the pending subscription and immutable header.
    unsafe {
        publisher.register_cursor(sub, cursor)?;
    }
    let mut writer = IntentWriter::new(
        publisher,
        config.intent.local.logical_strategy,
        config.intent.local.instance,
        config.intent.local.generation,
        config.risk_domain,
    )?;
    let mut store = CommandStore::new(tickwire_recovery::FileBlobStore::open(
        &config.command_directory,
    )?);
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
    // SAFETY: the authenticated execution owner is the qualified sole writer and
    // has registered this exact pending gate before offering the read-only bank.
    let mut reports = unsafe {
        crate::reports::ReportReceiver::attach(
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
        )?
    };
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
        let verified = crate::preflight::completion(&job, completed, status.result_hash)?;
        recovery
            .as_mut()
            .ok_or(Error::NotReady)?
            .install_preflight(status.result_hash, verified)?;
    }
    if let Some(w) = &mut recovery {
        w.set_active();
    }
    let mut strategy = Fresh {
        inner: strategy,
        maximum_age: config.maximum_market_age_ns,
    };
    let mut account_mirror = crate::handover::AccountMirror::default();
    let mut ready_sequence = 0u64;
    let mut pending_seal: Option<(ControlEnvelope, tickwire_ipc::CheckpointSealBody)> = None;
    let mut callbacks_paused = false;
    loop {
        let n = now(&mut clock)?;
        if n >= market_session.deadline()
            || n >= intent_session.deadline()
            || n >= report_session.deadline()
        {
            return Err(Error::Deadline);
        }
        let mut report_count = 0;
        for _ in 0..config.poll_records {
            if callbacks_paused
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
            if let Some(command) = strategy.on_report(&value, at)? {
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
        let count = if callbacks_paused || pending_seal.is_some() {
            0
        } else {
            replica.drain_recorded(
                &consumer,
                &mut strategy,
                now(&mut clock)?,
                config.poll_records,
                &mut recovery,
                |command, recovery| {
                    prepare_and_publish(
                        || recovery.as_mut().ok_or(Error::NotReady)?.prepare(&command),
                        || now(&mut clock),
                        |published_at| {
                            writer
                                .publish(&command, &mut store, published_at)
                                .map(|_| ())
                        },
                    )
                    .map_err(|e| e.context("market intent"))
                },
            )?
        };
        // Each complete drain has ended all payload references before control handling.
        match market.try_recv() {
            Ok(Some(packet)) => {
                if !packet.fds().is_empty() {
                    return Err(Error::Identity);
                }
                let envelope = ControlEnvelope::decode(&packet.bytes()[..128])?;
                if envelope.opcode == 20 {
                    replica.invalidate();
                    let response =
                        consumer.quiesce(&mut market_session, packet.bytes(), now(&mut clock)?)?;
                    market.send(&response, &[], Instant::now() + Duration::from_secs(1))?;
                    return Ok(());
                }
                market_session.validate(packet.bytes(), now(&mut clock)?)?;
                replica.invalidate();
                return Err(Error::NotReady);
            }
            Ok(None) => {}
            Err(e) => return Err(e.into()),
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
                if frame.envelope.opcode == 39 {
                    if !config.handover_enabled || pending_seal.is_some() {
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
                if !remote {
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
                let departure = tickwire_market_source::retirement::request(
                    &market_session,
                    &j,
                    request.request_id,
                )?;
                let departure_request = ControlEnvelope::decode(&departure[..128])?;
                market
                    .send(&departure, &[], until)
                    .map_err(|e| Error::from(e).context("retirement: send MD detach"))?;
                let command = market
                    .recv(until)
                    .map_err(|e| Error::from(e).context("retirement: await MD quiesce"))?;
                if !command.fds().is_empty() {
                    return Err(Error::Identity);
                }
                let response = consumer
                    .quiesce(&mut market_session, command.bytes(), now(&mut clock)?)
                    .map_err(|e| Error::from(e).context("retirement: release market cursor"))?;
                let market_proof = tickwire_ipc::QuiesceBody::decode(&response[128..])?;
                market
                    .send(&response, &[], until)
                    .map_err(|e| Error::from(e).context("retirement: send MD quiesced"))?;
                let ack = market
                    .recv(until)
                    .map_err(|e| Error::from(e).context("retirement: await MD detached"))?;
                if !ack.fds().is_empty() {
                    return Err(Error::Identity);
                }
                let ack = market_session.validate(ack.bytes(), now(&mut clock)?)?;
                tickwire_market_source::retirement::verify_applied(
                    &ack.envelope,
                    ack.body,
                    &departure_request,
                    &market_proof,
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
            std::thread::sleep(Duration::from_micros(config.idle_sleep_us));
        }
    }
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
