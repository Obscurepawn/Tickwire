//! Qualified worker: authenticated launcher policies are the unsafe SHM admission boundary.
use crate::bootstrap::{Bootstrap, Endpoint, JoinBootstrap, wire_bytes};
use crate::{CommandStore, Error, IntentWriter, MarketReplica, Result, Strategy};
use sha2::{Digest, Sha256};
use std::{
    fs::OpenOptions,
    io::Read,
    os::{
        fd::{AsFd, OwnedFd},
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::Path,
    time::{Duration, Instant},
};
use tickwire_clock_sys::SystemClock;
use tickwire_control::{ClientHandshake, ProcessIdentity, Session};
use tickwire_control_sys::{Connection, ReceivedFrame};
use tickwire_ipc::{
    ControlEnvelope, JoinBody, OfferPrefix, ResourceDescriptor, ResourceHeader, StatusBody, Wire,
};
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
fn connect(
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
pub fn descriptor(h: &ResourceHeader, profile: u16) -> ResourceDescriptor {
    ResourceDescriptor {
        resource_id: h.resource_id,
        channel_id: h.channel_id,
        parent_resource_id: h.parent_resource_id,
        resource_bytes: h.resource_bytes,
        channel_epoch: h.channel_epoch,
        ring_epoch: h.ring_epoch,
        resource_kind: h.resource_kind,
        access: 1,
        fd_index: 0,
        seal_profile: profile,
        header_hash: h.descriptor_hash,
        writer_instance: h.producer_instance,
    }
}
pub fn offer_id(join: &JoinBody, kind: u8) -> [u8; 16] {
    let mut h = Sha256::new();
    h.update(b"tickwire.join-offer.v1");
    h.update([kind]);
    h.update(join.snapshot_id);
    h.update(join.cursor_resource_id);
    h.update(join.subscription_generation.to_le_bytes());
    h.finalize()[..16].try_into().unwrap()
}
pub fn resource_offer(
    session: &Session,
    h: &ResourceHeader,
    id: [u8; 16],
    expires: u64,
    membership: u64,
    immutable: bool,
) -> Result<Vec<u8>> {
    let mut body = wire_bytes(&OfferPrefix {
        offer_id: id,
        descriptor_count: 1,
        descriptor_bytes: 128,
        required_seals: if immutable { 15 } else { 23 },
        expires_mono_ns: expires,
        membership_version: membership,
    })?;
    body.extend(wire_bytes(&descriptor(h, if immutable { 2 } else { 1 }))?);
    Ok(session.encode(
        ControlEnvelope {
            opcode: 3,
            flags: 32,
            fd_count: 1,
            request_id: id,
            expected_state_version: membership,
            ..Default::default()
        },
        &body,
    )?)
}
pub fn accept_offer(
    session: &mut Session,
    packet: ReceivedFrame,
    h: &ResourceHeader,
    id: [u8; 16],
    expires: u64,
    membership: u64,
    immutable: bool,
    now: u64,
) -> Result<OwnedFd> {
    let (bytes, mut fds) = packet.into_parts();
    let frame = session.validate(&bytes, now)?;
    if frame.envelope.opcode != 3
        || frame.envelope.flags != 32
        || frame.envelope.fd_count != 1
        || fds.len() != 1
        || frame.envelope.expected_state_version != membership
    {
        return Err(Error::Identity);
    }
    let p = OfferPrefix::decode(&frame.body[..64])?;
    if p.offer_id != id
        || p.descriptor_count != 1
        || p.descriptor_bytes != 128
        || p.expires_mono_ns != expires
        || now >= expires
        || p.membership_version != membership
        || p.required_seals != if immutable { 15 } else { 23 }
        || ResourceDescriptor::decode(&frame.body[64..])?
            != descriptor(h, if immutable { 2 } else { 1 })
    {
        return Err(Error::Identity);
    }
    fds.pop().ok_or(Error::Identity)
}
pub fn cursor_header(bank: &ResourceHeader, sub: SubscriptionKey) -> Result<ResourceHeader> {
    let mut h = bank.clone();
    h.resource_kind = 4;
    h.resource_id = sub.cursor.to_le_bytes();
    h.parent_resource_id = bank.resource_id;
    h.producer_instance = sub.instance.to_le_bytes();
    h.producer_generation = sub.generation;
    h.consumer_instance = bank.producer_instance;
    h.capacity = 0;
    h.record_bytes = 0;
    h.resource_bytes = 8192;
    h.slot_offset = 0;
    h.producer_page_offset = 0;
    h.cursor_page_offset = 4096;
    h.max_consumers = 1;
    h.arena_offset = 0;
    h.arena_bytes = 0;
    h.arena_class_count = 0;
    h.class_descriptors = Default::default();
    h.descriptor_hash = [0; 32];
    h.descriptor_hash = Sha256::digest(wire_bytes(&h)?).into();
    Ok(h)
}
/// Parent replies only after JoinOwner::join_ready succeeds; no response is an activation.
pub fn activation_ack(
    session: &Session,
    request: &ControlEnvelope,
    join: &JoinBody,
    published: u64,
) -> Result<Vec<u8>> {
    Ok(session.encode(
        ControlEnvelope {
            opcode: 49,
            flags: 10,
            request_id: request.request_id,
            expected_state_version: request.expected_state_version,
            ..Default::default()
        },
        &wire_bytes(&StatusBody {
            subject_id: join.bank_resource_id,
            original_request_id: request.request_id,
            original_control_epoch: session.epoch(),
            state_version: join.membership_version,
            membership_version: join.membership_version,
            ring_epoch: join.ring_epoch,
            published_seq: published,
            state: 3,
            ..Default::default()
        })?,
    )?)
}
fn activated(
    session: &mut Session,
    packet: ReceivedFrame,
    join: &JoinBody,
    id: [u8; 16],
    now: u64,
) -> Result<()> {
    if !packet.fds().is_empty() {
        return Err(Error::Identity);
    }
    let f = session.validate(packet.bytes(), now)?;
    let s = StatusBody::decode(f.body)?;
    if f.envelope.opcode != 49
        || f.envelope.flags != 10
        || f.envelope.request_id != id
        || f.envelope.expected_state_version != join.membership_version
        || s.original_request_id != id
        || s.original_control_epoch != session.epoch()
        || s.subject_id != join.bank_resource_id
        || s.state != 3
        || s.membership_version != join.membership_version
        || s.ring_epoch != join.ring_epoch
        || s.published_seq < join.snapshot_seq
    {
        return Err(Error::Identity);
    }
    Ok(())
}
struct Fresh<S> {
    inner: S,
    maximum_age: u64,
}
impl<S: Strategy> Strategy for Fresh<S> {
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
    let (mut market, mut market_session) = connect(&config.market, &mut clock, deadline, [1; 16])?;
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
    let (mut replica, snapshot_bytes) = consumer.load_baseline(baseline_now, |topic| {
        Ok((
            MarketReplica::load(j.topic_id, j.ring_epoch, topic, config.maximum_levels)?,
            topic.bytes().to_vec(),
        ))
    })?;
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
    if let Some(w) = &mut recovery {
        w.set_active();
    }
    let mut strategy = Fresh {
        inner: strategy,
        maximum_age: config.maximum_market_age_ns,
    };
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
            let Some(value) = reports.next()? else {
                break;
            };
            report_count += 1;
            let at = now(&mut clock)?;
            if let Some(w) = &mut recovery {
                w.account_report(&value, at)?;
            }
            if let Some(command) = strategy.on_report(&value, at)? {
                recovery
                    .as_mut()
                    .ok_or(Error::NotReady)?
                    .prepare(&command)?;
                writer.publish(&command, &mut store, at)?;
            }
        }
        let count = replica.drain_recorded(
            &consumer,
            &mut strategy,
            now(&mut clock)?,
            config.poll_records,
            &mut recovery,
            |command, recovery| {
                recovery
                    .as_mut()
                    .ok_or(Error::NotReady)?
                    .prepare(&command)?;
                writer.publish(&command, &mut store, n)?;
                Ok(())
            },
        )?;
        // Each complete drain has ended all payload references before control handling.
        match market.recv(Instant::now() + Duration::from_micros(1)) {
            Ok(packet) => {
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
            Err(tickwire_control_sys::Error::Timeout) => {}
            Err(e) => return Err(e.into()),
        }
        match intent.recv(Instant::now() + Duration::from_micros(1)) {
            Ok(packet) => {
                intent_session.validate(packet.bytes(), now(&mut clock)?)?;
                return Err(Error::NotReady);
            }
            Err(tickwire_control_sys::Error::Timeout) => {}
            Err(e) => return Err(e.into()),
        }
        match report.recv(Instant::now() + Duration::from_micros(1)) {
            Ok(packet) => {
                report_session.validate(packet.bytes(), now(&mut clock)?)?;
                return Err(Error::NotReady);
            }
            Err(tickwire_control_sys::Error::Timeout) => {}
            Err(e) => return Err(e.into()),
        }
        if count == 0 && report_count == 0 {
            std::thread::sleep(Duration::from_micros(config.idle_sleep_us));
        }
    }
}
