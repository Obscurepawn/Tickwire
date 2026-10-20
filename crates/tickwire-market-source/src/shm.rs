use crate::protocol::*;
use crate::replica::MarketReplica;
use crate::{
    Error, FeedKey, FeedObservation, Invalidation, MarketSink, MarketSource, PollReport, Result,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    os::{fd::AsFd, unix::fs::FileExt},
    time::{Duration, Instant},
};
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_control::{ClientHandshake, ProcessIdentity, Session};
use tickwire_control_sys::{Connection, ReceivedFrame};
use tickwire_ipc::{ControlEnvelope, JoinBody, RecordHeader, ResourceHeader, Wire};
use tickwire_join::{JoinConsumer, TopicRegistry};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectConfig {
    pub endpoint: Endpoint,
    /// Exact approved MD bank header, including full topic/catalog/arena identity.
    pub bank_header: Vec<u8>,
    pub catalog_hash: [u8; 32],
    pub instrument_set_version: u64,
    pub instruments: Vec<Instrument>,
    pub cursor_id: [u8; 16],
    pub subscription_generation: u64,
    pub request_id: [u8; 16],
    pub maximum_levels: usize,
    pub startup_timeout_ms: u64,
    pub clock_generation: u64,
}
impl ConnectConfig {
    pub fn domain(&self) -> Result<ClockDomain> {
        ClockDomain::new(self.endpoint.local.host_boot, self.clock_generation)
            .map_err(|_| Error::Identity)
    }
    pub fn registry(&self) -> Result<TopicRegistry> {
        if self.instruments.is_empty() || self.instruments.len() > 4096 {
            return Err(Error::Bounds);
        }
        Ok(TopicRegistry {
            catalog_hash: self.catalog_hash,
            instrument_set_version: self.instrument_set_version,
            instruments: self
                .instruments
                .iter()
                .map(|i| tickwire_snapshot::InstrumentSpec {
                    instrument_id: i.id,
                    instrument_version: i.version,
                    required_states: i.required_states,
                })
                .collect(),
        })
    }
}
/// A completed actual JOIN. The baseline is retained verbatim for strategy recovery.
pub struct Client {
    pub consumer: JoinConsumer<4>,
    pub replica: MarketReplica,
    pub connection: Connection,
    pub session: Session,
    pub baseline: JoinBootstrap,
    pub baseline_bytes: Vec<u8>,
    pub baseline_now: u64,
    /// Exact post-snapshot records consumed before activation, with the actual
    /// local apply time. A strategy must journal them before its first callback.
    pub catchup: Vec<CatchupRecord>,
    pub bank: ResourceHeader,
    pub join: JoinBody,
    pub domain: ClockDomain,
}
#[derive(Clone, Debug)]
pub struct CatchupRecord {
    pub record: Vec<u8>,
    pub payload: Vec<u8>,
    pub instrument: u64,
    pub applied_mono_ns: u64,
}
const MAX_CATCHUP_BYTES: usize = 64 * 1024 * 1024;

fn retain_catchup(
    records: &mut Vec<CatchupRecord>,
    bytes: &mut usize,
    capacity: usize,
    record: CatchupRecord,
) -> Result<()> {
    let next = bytes
        .checked_add(record.record.len())
        .and_then(|n| n.checked_add(record.payload.len()))
        .and_then(|n| n.checked_add(32))
        .ok_or(Error::Bounds)?;
    if records.len() >= capacity || next > MAX_CATCHUP_BYTES {
        return Err(Error::Bounds);
    }
    records.push(record);
    *bytes = next;
    Ok(())
}

// The drain binds cursor, clock domain, expiry and retained replay independently.
#[allow(clippy::too_many_arguments)]
fn drain_catchup(
    consumer: &JoinConsumer<4>,
    replica: &mut MarketReplica,
    records: &mut Vec<CatchupRecord>,
    retained_bytes: &mut usize,
    capacity: usize,
    clock: &mut impl FnMut() -> Result<MonoTime>,
    domain: ClockDomain,
    expires: u64,
    deadline: Instant,
) -> Result<()> {
    for _ in 0..64 {
        if current(clock, domain)? >= expires || Instant::now() >= deadline {
            return Err(Error::Deadline);
        }
        let Some(guard) = consumer.borrow()? else {
            break;
        };
        let header = RecordHeader::decode(&guard.bytes()[..128])?;
        let payload = if header.message_type == 259 {
            guard.depth_payload()?
        } else {
            &[]
        };
        let applied_mono_ns = current(clock, domain)?;
        if applied_mono_ns >= expires {
            return Err(Error::Deadline);
        }
        let record = CatchupRecord {
            record: guard.bytes().to_vec(),
            payload: payload.to_vec(),
            instrument: header.instrument_id,
            applied_mono_ns,
        };
        // Retain the exact input before acknowledging that it was applied.
        retain_catchup(records, retained_bytes, capacity, record)?;
        replica.apply(guard.bytes(), payload)?;
        let sequence = guard.sequence();
        drop(guard);
        consumer.apply(sequence)?;
    }
    Ok(())
}
fn current(clock: &mut impl FnMut() -> Result<MonoTime>, domain: ClockDomain) -> Result<u64> {
    let n = clock()?;
    if n.domain() != domain {
        return Err(Error::Identity);
    }
    Ok(n.nanos())
}
fn approved_header(packet: &ReceivedFrame) -> Result<ResourceHeader> {
    if packet.fds().len() != 1 {
        return Err(Error::Identity);
    }
    let file = File::from(packet.fds()[0].try_clone()?);
    let mut bytes = [0; 4096];
    file.read_exact_at(&mut bytes, 0)?;
    let header = ResourceHeader::decode(&bytes)?;
    if file.metadata()?.len() != header.resource_bytes {
        return Err(Error::Identity);
    }
    let mut zero = header.clone();
    zero.descriptor_hash = [0; 32];
    if header.descriptor_hash != <[u8; 32]>::from(Sha256::digest(wire_bytes(&zero)?)) {
        return Err(Error::Identity);
    }
    // The caller checks this exact hash against the authenticated offer descriptor
    // before using any field to admit the mapping.
    Ok(header)
}
impl Client {
    /// # Safety
    /// The endpoint policy names an authenticated qualified MD writer, whose
    /// immutable bank and cursor gating contracts hold for all registered readers.
    pub unsafe fn connect(
        config: &ConnectConfig,
        clock: &mut impl FnMut() -> Result<MonoTime>,
    ) -> Result<Self> {
        if config.startup_timeout_ms == 0
            || config.startup_timeout_ms > 60000
            || config.maximum_levels == 0
            || config.maximum_levels > 10000
            || config.cursor_id == [0; 16]
            || config.subscription_generation == 0
            || config.request_id == [0; 16]
            || !matches!(config.endpoint.local.role, 3 | 4)
            || config.endpoint.peer.role != 2
        {
            return Err(Error::Identity);
        }
        let domain = config.domain()?;
        let registry = config.registry()?;
        let mut bank = ResourceHeader::decode(&config.bank_header)?;
        if bank.producer_instance != config.endpoint.peer.instance
            || bank.producer_generation != config.endpoint.peer.generation
            || bank.host_boot_id != config.endpoint.local.host_boot
            || bank.namespace_id != config.endpoint.local.namespace
            || bank.required_features & 64 == 0
            || bank.resource_kind != 1
        {
            return Err(Error::Identity);
        }
        bank.descriptor_hash = [0; 32];
        bank.descriptor_hash = Sha256::digest(wire_bytes(&bank)?).into();
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(config.startup_timeout_ms))
            .ok_or(Error::Bounds)?;
        let mut connection = Connection::connect(&config.endpoint.path, deadline)?;
        let handshake = ClientHandshake::new(config.endpoint.policy(), config.request_id)?;
        connection.send(handshake.hello(), &[], deadline)?;
        let packet = connection.recv(deadline)?;
        if !packet.fds().is_empty() {
            return Err(Error::Identity);
        }
        let peer = connection.peer_credentials();
        let mut session = handshake.finish(
            ProcessIdentity {
                pid: peer.pid,
                uid: peer.uid,
                gid: peer.gid,
            },
            packet.bytes(),
            current(clock, domain)?,
        )?;
        let now = current(clock, domain)?;
        let expires = now
            .checked_add(
                config
                    .startup_timeout_ms
                    .checked_mul(1_000_000)
                    .ok_or(Error::Bounds)?,
            )
            .ok_or(Error::Bounds)?
            .min(session.deadline());
        let request = JoinBody {
            subscriber_instance: config.endpoint.local.instance,
            logical_strategy_id: config.endpoint.local.logical_strategy,
            subscription_generation: config.subscription_generation,
            topic_id: bank.topic_id,
            channel_id: bank.channel_id,
            channel_epoch: bank.channel_epoch,
            feed_generation: bank.feed_generation,
            ring_epoch: bank.ring_epoch,
            cursor_resource_id: config.cursor_id,
            bank_resource_id: bank.resource_id,
            readiness_deadline_mono_ns: expires,
            expected_catalog_version: bank.catalog_version,
            qos: bank.qos,
            role: config.endpoint.local.role,
            join_flags: 3,
            topic_instrument_set_version: registry.instrument_set_version,
            topic_instrument_set_hash: tickwire_join::instrument_set_hash(&registry.instruments),
            ..Default::default()
        };
        let frame = session.encode(
            ControlEnvelope {
                opcode: 16,
                flags: 1,
                request_id: config.request_id,
                ..Default::default()
            },
            &wire_bytes(&request)?,
        )?;
        connection.send(&frame, &[], deadline)?;
        let response = connection.recv(deadline)?;
        if !response.fds().is_empty() {
            return Err(Error::Identity);
        }
        let frame = session.validate(response.bytes(), current(clock, domain)?)?;
        if frame.envelope.opcode != 16
            || frame.envelope.flags != 10
            || frame.envelope.request_id != config.request_id
        {
            return Err(Error::Identity);
        }
        let join = JoinBody::decode(frame.body)?;
        let mut echo = join.clone();
        echo.snapshot_id = [0; 16];
        echo.snapshot_hash = [0; 32];
        echo.snapshot_seq = 0;
        echo.join_seq = 0;
        echo.membership_version = 0;
        if echo != request {
            return Err(Error::Identity);
        }
        let bankfd = accept_offer(
            &mut session,
            connection.recv(deadline)?,
            &bank,
            offer_id(&join, 1),
            expires,
            join.membership_version,
            false,
            current(clock, domain)?,
        )?;
        let packet = connection.recv(deadline)?;
        let snapshot = approved_header(&packet)?;
        let snapshotfd = accept_offer(
            &mut session,
            packet,
            &snapshot,
            offer_id(&join, 2),
            expires,
            join.membership_version,
            true,
            current(clock, domain)?,
        )?;
        if snapshot.resource_kind != 8 || snapshot.resource_id != join.snapshot_id {
            return Err(Error::Identity);
        }
        let baseline = JoinBootstrap::new(&bank, &snapshot, &join, &registry)?;
        // SAFETY: the trusted endpoint and authenticated exact descriptors were
        // checked above; the qualified owner registered the pending gate first.
        let mut consumer = unsafe {
            JoinConsumer::<4>::attach(
                bankfd,
                snapshotfd,
                bank.clone(),
                snapshot,
                join.clone(),
                registry,
                current(clock, domain)?,
            )?
        };
        let baseline_now = current(clock, domain)?;
        let (mut replica, baseline_bytes) = consumer.load_baseline(baseline_now, |topic| {
            Ok((
                MarketReplica::load(join.topic_id, join.ring_epoch, topic, config.maximum_levels)?,
                topic.bytes().to_vec(),
            ))
        })?;
        let (offer, fd) = consumer.cursor_offer(&session, config.request_id)?;
        connection.send(&offer, &[fd.as_fd()], deadline)?;
        for frame in consumer.read_ready_frames(&session, [config.request_id, config.request_id])? {
            connection.send(&frame, &[], deadline)?;
        }
        let mut catchup = Vec::new();
        let mut retained_bytes = 0;
        let mut target = join.snapshot_seq;
        loop {
            drain_catchup(
                &consumer,
                &mut replica,
                &mut catchup,
                &mut retained_bytes,
                bank.capacity as usize,
                clock,
                domain,
                expires,
                deadline,
            )?;
            if consumer.watermarks().applied < target {
                continue;
            }
            let ready =
                consumer.join_ready_frame(&session, config.request_id, current(clock, domain)?)?;
            connection.send(&ready, &[], deadline)?;
            let response = loop {
                match connection.recv((Instant::now() + Duration::from_millis(1)).min(deadline)) {
                    Ok(packet) => break packet,
                    Err(tickwire_control_sys::Error::Timeout) => {
                        drain_catchup(
                            &consumer,
                            &mut replica,
                            &mut catchup,
                            &mut retained_bytes,
                            bank.capacity as usize,
                            clock,
                            domain,
                            expires,
                            deadline,
                        )?;
                    }
                    Err(error) => return Err(error.into()),
                }
            };
            match activation_status(
                &mut session,
                response,
                &join,
                config.request_id,
                current(clock, domain)?,
            )? {
                ActivationStatus::Active { published } => {
                    if consumer.watermarks().applied < published {
                        return Err(Error::Identity);
                    }
                    break;
                }
                ActivationStatus::CatchUp { published } => {
                    if published < target {
                        return Err(Error::Identity);
                    }
                    target = published;
                }
            }
        }
        Ok(Self {
            consumer,
            replica,
            connection,
            session,
            baseline,
            baseline_bytes,
            baseline_now,
            catchup,
            bank,
            join,
            domain,
        })
    }
}
pub struct ShmSource {
    client: Client,
    stopped: bool,
    announced: bool,
    last_now: u64,
}
impl ShmSource {
    pub fn new(client: Client) -> Self {
        Self {
            last_now: client.baseline_now,
            client,
            stopped: false,
            announced: false,
        }
    }
    pub fn client(&self) -> &Client {
        &self.client
    }
    pub fn into_client(self) -> Client {
        self.client
    }
    fn observation(&self, id: u64, _now: MonoTime, _age: u64) -> Result<FeedObservation> {
        let v = self.client.replica.view(id).ok_or(Error::Identity)?;
        let mark = v.trigger();
        let received = v.received_mono_ns();
        let published = v.published_mono_ns();
        Ok(FeedObservation {
            key: FeedKey {
                topic_id: mark.topic_id,
                instrument_id: id,
            },
            instrument_version: v.version(),
            feed_generation: mark.feed_generation,
            ring_epoch: mark.ring_epoch,
            sequence: mark.sequence,
            received_at: MonoTime::new(self.client.domain, received),
            published_at: MonoTime::new(self.client.domain, published),
            live: !self.stopped && v.continuous(),
        })
    }
    fn invalidate(&mut self, sink: &mut dyn MarketSink) -> Result<()> {
        self.stopped = true;
        self.client.replica.invalidate();
        let mut accepted = true;
        for id in self.client.replica.instruments() {
            accepted &= sink.invalidated(Invalidation {
                key: FeedKey {
                    topic_id: self.client.bank.topic_id,
                    instrument_id: id,
                },
                feed_generation: self.client.bank.feed_generation,
                ring_epoch: self.client.bank.ring_epoch,
            });
        }
        if accepted { Ok(()) } else { Err(Error::Sink) }
    }
    fn drive(
        &mut self,
        now: MonoTime,
        age: u64,
        budget: usize,
        sink: &mut dyn MarketSink,
    ) -> Result<PollReport> {
        if self.stopped || now.domain() != self.client.domain || age == 0 || budget == 0 {
            return Err(Error::NotReady);
        }
        if now.nanos() < self.last_now {
            return Err(Error::Stale);
        }
        self.last_now = now.nanos();
        if now.nanos() >= self.client.session.deadline() {
            return Err(Error::Deadline);
        }
        match self.client.connection.try_recv() {
            Ok(Some(packet)) => {
                if !packet.fds().is_empty() {
                    return Err(Error::Identity);
                }
                let h = ControlEnvelope::decode(packet.bytes().get(..128).ok_or(Error::Bounds)?)?;
                if h.opcode == 20 {
                    let response = self.client.consumer.quiesce(
                        &mut self.client.session,
                        packet.bytes(),
                        now.nanos(),
                    )?;
                    self.client.connection.send(
                        &response,
                        &[],
                        Instant::now() + Duration::from_millis(10),
                    )?;
                    return Err(Error::NotReady);
                }
                self.client.session.validate(packet.bytes(), now.nanos())?;
                return Err(Error::NotReady);
            }
            Ok(None) => {}
            Err(e) => return Err(e.into()),
        }
        if !self.announced {
            for id in self.client.replica.instruments() {
                if !sink.observed(self.observation(id, now, age)?) {
                    return Err(Error::Sink);
                }
            }
            self.announced = true;
        }
        let mut out = PollReport {
            next_deadline_ns: Some(self.client.session.deadline()),
            ..Default::default()
        };
        while out.records < budget {
            let Some(guard) = self.client.consumer.borrow()? else {
                break;
            };
            let h = RecordHeader::decode(&guard.bytes()[..128])?;
            let payload = if h.message_type == 259 {
                guard.depth_payload()?
            } else {
                &[]
            };
            let id = self.client.replica.apply(guard.bytes(), payload)?;
            let seq = guard.sequence();
            drop(guard);
            self.client.consumer.apply(seq)?;
            if !sink.observed(self.observation(id, now, age)?) {
                return Err(Error::Sink);
            }
            out.records += 1;
        }
        out.runnable = out.records == budget;
        for id in self.client.replica.instruments() {
            let o = self.observation(id, now, age)?;
            if !o.live
                && !sink.invalidated(Invalidation {
                    key: o.key,
                    feed_generation: o.feed_generation,
                    ring_epoch: o.ring_epoch,
                })
            {
                return Err(Error::Sink);
            }
        }
        Ok(out)
    }
}
impl MarketSource for ShmSource {
    fn domain(&self) -> ClockDomain {
        self.client.domain
    }
    fn poll(
        &mut self,
        now: MonoTime,
        maximum_age_ns: u64,
        budget: usize,
        sink: &mut dyn MarketSink,
    ) -> Result<PollReport> {
        match self.drive(now, maximum_age_ns, budget, sink) {
            Ok(v) => Ok(v),
            Err(e) => {
                let _ = self.invalidate(sink);
                Err(e)
            }
        }
    }
    fn is_live(&self, now: MonoTime, maximum_age_ns: u64) -> bool {
        !self.stopped
            && now.nanos() < self.client.session.deadline()
            && self.client.replica.instruments().all(|id| {
                self.observation(id, now, maximum_age_ns).is_ok_and(|o| {
                    o.live
                        && self.client.replica.view(id).is_some_and(|v| v.live())
                        && now.domain() == self.client.domain
                        && now.nanos() >= o.received_at.nanos()
                        && now.nanos() >= o.published_at.nanos()
                        && now.nanos() - o.received_at.nanos() <= maximum_age_ns
                })
            })
    }
    fn stop(&mut self, sink: &mut dyn MarketSink) -> Result<()> {
        self.invalidate(sink)
    }
}

#[cfg(test)]
mod catchup_tests {
    use super::*;
    #[test]
    fn retained_prefix_keeps_exact_record_payload_and_apply_time_with_atomic_bounds() {
        let record = CatchupRecord {
            record: vec![7; 512],
            payload: vec![8; 1024],
            instrument: 12,
            applied_mono_ns: 55,
        };
        let mut records = vec![];
        let mut bytes = 0;
        retain_catchup(&mut records, &mut bytes, 1, record.clone()).unwrap();
        assert_eq!(records[0].record, record.record);
        assert_eq!(records[0].payload, record.payload);
        assert_eq!(records[0].applied_mono_ns, 55);
        let before = bytes;
        assert!(retain_catchup(&mut records, &mut bytes, 1, record).is_err());
        assert_eq!(records.len(), 1);
        assert_eq!(bytes, before);
    }
}
