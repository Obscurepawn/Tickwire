//! JOIN coordination over actual shared-memory publishers and immutable snapshots.
#![deny(unsafe_code)]

use sha2::{Digest, Sha256};
use std::os::fd::AsFd;
use std::os::fd::OwnedFd;
use std::time::Instant;
use tickwire_control::{Admission, Identity, RequestStore, Session};
use tickwire_control_sys::{Connection, ReceivedFrame};
use tickwire_ipc::{
    ControlEnvelope, JoinBody, OfferPrefix, QuiesceBody, ResourceAckBody, ResourceDescriptor,
    ResourceHeader, Wire,
};
use tickwire_shm::{Publisher, SubscriptionKey, Watermarks};
use tickwire_snapshot::{BuiltTopic, InstrumentSpec};

#[derive(Debug)]
pub enum Error {
    Control(tickwire_control::Error),
    Transport(tickwire_control_sys::Error),
    Mapping(tickwire_shm_sys::Error),
    Ring(tickwire_shm::MappedError),
    Wire(tickwire_ipc::Error),
    Identity,
    State,
    Capacity,
    Deadline,
    NotReady,
}
pub type Result<T> = std::result::Result<T, Error>;
macro_rules! from_error {
    ($variant:ident, $ty:ty) => {
        impl From<$ty> for Error {
            fn from(error: $ty) -> Self {
                Self::$variant(error)
            }
        }
    };
}
from_error!(Control, tickwire_control::Error);
from_error!(Transport, tickwire_control_sys::Error);
from_error!(Mapping, tickwire_shm_sys::Error);
from_error!(Ring, tickwire_shm::MappedError);
from_error!(Wire, tickwire_ipc::Error);
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Debug, Clone)]
pub struct TopicRegistry {
    pub catalog_hash: [u8; 32],
    pub instrument_set_version: u64,
    pub instruments: Vec<InstrumentSpec>,
}
#[derive(Debug, Clone)]
pub struct SubscriberRegistration {
    pub identity: Identity,
    pub subscription: SubscriptionKey,
    pub qos: u16,
}
#[derive(Debug, Clone)]
pub struct JoinPlan {
    pub resource: ResourceHeader,
    pub join: JoinBody,
    pub sequence: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    Reserved,
    Prepared,
    Delivered,
    Active,
    Failed,
    Expired,
    Quiescing,
    Detached,
}
#[derive(Debug)]
pub enum PrepareOutcome {
    Prepared {
        subscription: SubscriptionKey,
        response: Vec<u8>,
    },
    Completed(Vec<u8>),
    Pending,
}

pub struct JoinOwner<const M: usize> {
    publisher: Publisher<M>,
    registry: TopicRegistry,
    entries: Vec<Entry>,
    last_now: u64,
}
struct Entry {
    registration: SubscriberRegistration,
    join: JoinBody,
    stage: Stage,
    resource: Option<tickwire_shm_sys::ImmutableResource>,
    bank_ready: bool,
    snapshot_ready: bool,
    cursor_registered: bool,
    catchup_seen: Option<Watermarks>,
    activated: Option<Watermarks>,
    quiesce: Option<(ControlEnvelope, QuiesceBody)>,
    detached: Option<QuiesceBody>,
}
fn require(value: bool) -> Result<()> {
    if value { Ok(()) } else { Err(Error::Identity) }
}
fn encode<T: Wire>(value: &T) -> Result<Vec<u8>> {
    let mut bytes = vec![0; T::SIZE];
    value.encode(&mut bytes)?;
    Ok(bytes)
}
pub fn instrument_set_hash(instruments: &[InstrumentSpec]) -> [u8; 32] {
    let mut hash = Sha256::new();
    for i in instruments {
        hash.update(i.instrument_id.to_le_bytes());
        hash.update(i.instrument_version.to_le_bytes());
    }
    hash.finalize().into()
}
fn offer_id(join: &JoinBody, kind: u8) -> [u8; 16] {
    let mut h = Sha256::new();
    h.update(b"tickwire.join-offer.v1");
    h.update([kind]);
    h.update(join.snapshot_id);
    h.update(join.cursor_resource_id);
    h.update(join.subscription_generation.to_le_bytes());
    h.finalize()[..16].try_into().expect("fixed digest prefix")
}
fn descriptor(h: &ResourceHeader, profile: u16) -> ResourceDescriptor {
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
fn offer(session: &Session, h: &ResourceHeader, j: &JoinBody, kind: u8) -> Result<Vec<u8>> {
    let id = offer_id(j, kind);
    let mut body = encode(&OfferPrefix {
        offer_id: id,
        descriptor_count: 1,
        descriptor_bytes: 128,
        required_seals: if kind == 2 { 0x0f } else { 0x17 },
        expires_mono_ns: j.readiness_deadline_mono_ns,
        membership_version: j.membership_version,
    })?;
    body.extend_from_slice(&encode(&descriptor(h, if kind == 2 { 2 } else { 1 }))?);
    Ok(session.encode(
        ControlEnvelope {
            opcode: 3,
            flags: 32,
            request_id: id,
            fd_count: 1,
            expected_state_version: j.membership_version,
            ..Default::default()
        },
        &body,
    )?)
}
fn cursor_header(bank: &ResourceHeader, sub: SubscriptionKey) -> Result<ResourceHeader> {
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
    h.descriptor_hash = Sha256::digest(encode(&h)?).into();
    Ok(h)
}
impl<const M: usize> JoinOwner<M> {
    pub fn new(publisher: Publisher<M>, registry: TopicRegistry) -> Result<Self> {
        require(
            publisher.header().event_class == 7
                && publisher.header().resource_kind == 1
                && publisher.header().topic_id != 0
                && publisher.header().catalog_version != 0
                && registry.catalog_hash != [0; 32]
                && (1..1 << 63).contains(&registry.instrument_set_version)
                && (1..=4096).contains(&registry.instruments.len()),
        )?;
        let mut previous = 0;
        for i in &registry.instruments {
            require(
                i.instrument_id > previous
                    && (1..1 << 63).contains(&i.instrument_version)
                    && i.required_states & !7 == 0,
            )?;
            previous = i.instrument_id;
        }
        Ok(Self {
            publisher,
            registry,
            entries: Vec::new(),
            last_now: 0,
        })
    }
    pub fn header(&self) -> &ResourceHeader {
        self.publisher.header()
    }
    pub fn published(&self) -> u64 {
        self.publisher.published()
    }
    pub fn membership_version(&self) -> u64 {
        self.publisher.membership_version()
    }
    /// True only when every registered subscription has completed real cursor detachment.
    pub fn all_detached(&self) -> bool {
        self.entries
            .iter()
            .all(|entry| entry.stage == Stage::Detached)
    }
    pub fn publish(&mut self, records: &[&[u8]]) -> Result<u64> {
        Ok(self.publisher.publish(records)?)
    }
    pub fn publish_market(&mut self, messages: &[tickwire_shm::MarketMessage<'_>]) -> Result<u64> {
        Ok(self.publisher.publish_market(messages)?)
    }
    /// Freeze at the current complete prefix while retaining every reader gate.
    pub fn freeze_for_successor(&mut self, ring_epoch: u64) -> Result<tickwire_shm::Freeze> {
        self.publisher
            .prepare_freeze(ring_epoch, self.publisher.published())?;
        Ok(self.publisher.freeze()?)
    }
    pub fn registry(&self) -> &TopicRegistry {
        &self.registry
    }
    fn index(&self, sub: SubscriptionKey) -> Result<usize> {
        self.entries
            .iter()
            .position(|e| e.registration.subscription == sub)
            .ok_or(Error::Identity)
    }
    fn observe(&mut self, now: u64) -> Result<()> {
        if now < self.last_now {
            self.expire(0);
            return Err(Error::Deadline);
        }
        self.last_now = now;
        Ok(())
    }
    fn live(&mut self, index: usize, now: u64) -> Result<()> {
        self.observe(now)?;
        if now >= self.entries[index].join.readiness_deadline_mono_ns {
            if self.entries[index].stage != Stage::Active {
                self.entries[index].stage = Stage::Expired;
            }
            return Err(Error::Deadline);
        }
        Ok(())
    }
    fn peer(&self, index: usize, session: &Session) -> Result<()> {
        require(session.peer() == &self.entries[index].registration.identity)
    }
    pub fn stage(&self, sub: SubscriptionKey) -> Option<Stage> {
        self.index(sub).ok().map(|i| self.entries[i].stage)
    }
    pub fn join(&self, sub: SubscriptionKey) -> Result<&JoinBody> {
        Ok(&self.entries[self.index(sub)?].join)
    }
    pub fn snapshot_header(&self, sub: SubscriptionKey) -> Result<&ResourceHeader> {
        Ok(self.entries[self.index(sub)?]
            .resource
            .as_ref()
            .ok_or(Error::State)?
            .header())
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "keep authenticated session, request store, registry and build callback explicit"
    )]
    pub fn prepare<S, F>(
        &mut self,
        supervisor: &mut Session,
        store: &mut S,
        request: &[u8],
        registration: &SubscriberRegistration,
        snapshot_id: [u8; 16],
        now: u64,
        build: F,
    ) -> Result<PrepareOutcome>
    where
        S: RequestStore,
        F: FnOnce(&JoinPlan) -> tickwire_ipc::Result<BuiltTopic>,
    {
        self.observe(now)?;
        require(supervisor.peer().role == 1)?;
        let ticket = match supervisor.admit(store, request, now, self.membership_version())? {
            Admission::Completed(v) => return Ok(PrepareOutcome::Completed(v.response)),
            Admission::Pending => return Ok(PrepareOutcome::Pending),
            Admission::Execute(t) => t,
        };
        let frame = tickwire_ipc::ControlFrame::decode(request)?;
        require(
            frame.envelope.opcode == 16
                && frame.envelope.target_instance == self.header().producer_instance,
        )?;
        let mut j = JoinBody::decode(frame.body)?;
        let sub = registration.subscription;
        let h = self.header();
        require(
            matches!(registration.identity.role, 3 | 4)
                && registration.identity.instance == sub.instance.to_le_bytes()
                && registration.identity.host_boot == h.host_boot_id
                && registration.identity.namespace == h.namespace_id
                && registration.identity.arch == h.target_arch
                && u16::from(registration.identity.trust_profile) == h.trust_profile
                && sub.bank == self.publisher.key()
                && j.subscriber_instance == registration.identity.instance
                && j.logical_strategy_id == registration.identity.logical_strategy
                && j.subscription_generation == sub.generation
                && j.cursor_resource_id == sub.cursor.to_le_bytes()
                && j.bank_resource_id == h.resource_id
                && j.channel_id == h.channel_id
                && j.channel_epoch == h.channel_epoch
                && j.ring_epoch == h.ring_epoch
                && j.feed_generation == h.feed_generation
                && j.topic_id == h.topic_id
                && j.expected_catalog_version == h.catalog_version
                && j.qos == registration.qos
                && j.qos == h.qos
                && j.role == registration.identity.role
                && j.join_flags == 3
                && j.topic_instrument_set_version == self.registry.instrument_set_version
                && j.topic_instrument_set_hash == instrument_set_hash(&self.registry.instruments)
                && j.snapshot_id == [0; 16]
                && j.snapshot_hash == [0; 32]
                && j.join_seq == 0
                && j.snapshot_seq == 0
                && j.membership_version == 0
                && snapshot_id != [0; 16]
                && snapshot_id != h.resource_id
                && snapshot_id != j.cursor_resource_id,
        )?;
        if now >= j.readiness_deadline_mono_ns
            || j.readiness_deadline_mono_ns > supervisor.deadline()
        {
            return Err(Error::Deadline);
        }
        if self.entries.len() >= M {
            return Err(Error::Capacity);
        }
        require(!self.entries.iter().any(|e| {
            e.registration.subscription == sub
                || e.resource
                    .as_ref()
                    .is_some_and(|r| r.header().resource_id == snapshot_id)
        }))?;
        let first = self.publisher.join(sub)?;
        let sequence = first - 1;
        j.membership_version = self.publisher.membership_version();
        let mut resource = self.publisher.header().clone();
        resource.resource_id = snapshot_id;
        resource.parent_resource_id = self.header().resource_id;
        resource.consumer_instance = j.subscriber_instance;
        resource.initial_membership_version = j.membership_version;
        resource.created_mono_ns = now;
        let plan = JoinPlan {
            resource,
            join: j.clone(),
            sequence,
        };
        let index = self.entries.len();
        self.entries.push(Entry {
            registration: registration.clone(),
            join: j,
            stage: Stage::Reserved,
            resource: None,
            bank_ready: false,
            snapshot_ready: false,
            cursor_registered: false,
            catchup_seen: None,
            activated: None,
            quiesce: None,
            detached: None,
        });
        let build_result = (|| {
            let built = build(&plan)?;
            require(
                built.join().subscriber_instance == plan.join.subscriber_instance
                    && built.join().snapshot_seq == sequence
                    && built.join().membership_version == plan.join.membership_version,
            )?;
            let mut normalized = built.join().clone();
            normalized.snapshot_id = [0; 16];
            normalized.snapshot_hash = [0; 32];
            normalized.snapshot_seq = 0;
            normalized.join_seq = 0;
            require(normalized == plan.join && built.resource().resource_id == snapshot_id)?;
            let resource = tickwire_shm_sys::ImmutableMapping::create(
                "tickwire-join-snapshot",
                built.bytes(),
            )?
            .validate_resource(
                built.resource(),
                built.bytes().len() - 4096,
                built.file_hash(),
            )?;
            self.entries[index].join = built.join().clone();
            self.entries[index].resource = Some(resource);
            let response = supervisor.encode(
                ControlEnvelope {
                    opcode: 16,
                    flags: 10,
                    ..frame.envelope.clone()
                },
                &encode(built.join())?,
            )?;
            ticket.complete(store, &response, self.membership_version())?;
            self.entries[index].stage = Stage::Prepared;
            Ok(PrepareOutcome::Prepared {
                subscription: sub,
                response,
            })
        })();
        if build_result.is_err() {
            self.entries[index].stage = Stage::Failed;
        }
        build_result
    }
    pub fn deliver(
        &mut self,
        sub: SubscriptionKey,
        session: &mut Session,
        connection: &mut Connection,
        now: u64,
        deadline: Instant,
    ) -> Result<()> {
        let index = self.index(sub)?;
        self.peer(index, session)?;
        self.live(index, now)?;
        if now >= session.deadline() {
            return Err(Error::Deadline);
        }
        if !matches!(
            self.entries[index].stage,
            Stage::Prepared | Stage::Delivered
        ) {
            return Err(Error::State);
        }
        let e = &self.entries[index];
        let result = (|| {
            let bank = self.publisher.reader_fd()?;
            connection.send(
                &offer(session, self.header(), &e.join, 1)?,
                &[bank.as_fd()],
                deadline,
            )?;
            let snapshot = e.resource.as_ref().ok_or(Error::State)?;
            let fd = snapshot.duplicate_fd()?;
            connection.send(
                &offer(session, snapshot.header(), &e.join, 2)?,
                &[fd.as_fd()],
                deadline,
            )?;
            Ok(())
        })();
        self.entries[index].stage = if result.is_ok() {
            Stage::Delivered
        } else {
            Stage::Failed
        };
        result
    }
    pub fn read_ready(
        &mut self,
        sub: SubscriptionKey,
        session: &mut Session,
        raw: &[u8],
        now: u64,
    ) -> Result<()> {
        let index = self.index(sub)?;
        self.peer(index, session)?;
        self.live(index, now)?;
        let frame = session.validate(raw, now)?;
        require(frame.envelope.opcode == 5 && frame.envelope.flags == 1)?;
        let ack = ResourceAckBody::decode(frame.body)?;
        let e = &self.entries[index];
        if !matches!(e.stage, Stage::Delivered | Stage::Active) {
            return Err(Error::State);
        }
        let kind = if ack.resource_id == self.header().resource_id {
            1
        } else if ack.resource_id == e.join.snapshot_id {
            2
        } else {
            return Err(Error::Identity);
        };
        let h = if kind == 1 {
            self.header()
        } else {
            e.resource.as_ref().ok_or(Error::State)?.header()
        };
        require(
            ack.offer_id == offer_id(&e.join, kind)
                && ack.header_hash == h.descriptor_hash
                && ack.mapped_bytes == h.resource_bytes
                && ack.observed_seals == if kind == 1 { 0x17 } else { 0x0f }
                && ack.access == 1
                && ack.ack_kind == 2
                && ack.registration_generation == sub.generation
                && frame.envelope.expected_state_version == e.join.membership_version,
        )?;
        if kind == 1 {
            self.entries[index].bank_ready = true;
        } else {
            self.entries[index].snapshot_ready = true;
        }
        Ok(())
    }
    pub fn join_ready(
        &mut self,
        sub: SubscriptionKey,
        session: &mut Session,
        raw: &[u8],
        now: u64,
    ) -> Result<Watermarks> {
        let index = self.index(sub)?;
        self.peer(index, session)?;
        self.observe(now)?;
        let frame = session.validate(raw, now)?;
        let e = &self.entries[index];
        require(
            frame.envelope.opcode == 17
                && frame.envelope.flags == 1
                && JoinBody::decode(frame.body)? == e.join
                && frame.envelope.expected_state_version == e.join.membership_version,
        )?;
        if e.stage == Stage::Active {
            // A completed JOIN is a historical result, not a new catch-up check.
            return e.activated.ok_or(Error::State);
        }
        self.live(index, now)?;
        let e = &self.entries[index];
        if !matches!(e.stage, Stage::Delivered | Stage::Active)
            || !e.bank_ready
            || !e.snapshot_ready
            || !e.cursor_registered
        {
            return Err(Error::NotReady);
        }
        let resource = e.resource.as_ref().ok_or(Error::State)?;
        tickwire_snapshot::validate(
            resource.bytes(),
            &tickwire_snapshot::JoinContext {
                expected_resource: resource.header(),
                catalog_hash: self.registry.catalog_hash,
                instruments: &self.registry.instruments,
                join: &e.join,
                now_mono_ns: now,
            },
        )?
        .check_readiness(now)?;
        if self.catchup_target(sub)?.is_some() {
            return Err(Error::Ring(tickwire_shm::MappedError::Protocol(
                tickwire_shm::Error::Cursor,
            )));
        }
        // Current published is fixed during this exclusive owner call.
        let e = &self.entries[index];
        let watermarks =
            self.publisher
                .activate_join(sub, e.join.snapshot_seq, self.publisher.published())?;
        self.entries[index].stage = Stage::Active;
        self.entries[index].activated = Some(watermarks);
        Ok(watermarks)
    }
    /// Samples only the registered, qualified cursor. A lagging reader keeps its
    /// original pending gate; malformed or regressing watermarks remain errors.
    pub fn catchup_target(&mut self, sub: SubscriptionKey) -> Result<Option<u64>> {
        let index = self.index(sub)?;
        let e = &self.entries[index];
        if e.stage != Stage::Delivered || !e.cursor_registered {
            return Err(Error::NotReady);
        }
        let actual = self.publisher.cursor_watermarks(sub)?;
        let target = catchup_watermarks(
            e.join.snapshot_seq,
            e.catchup_seen,
            self.publisher.published(),
            actual,
        )?;
        self.entries[index].catchup_seen = Some(actual);
        Ok(target)
    }
    pub fn expire(&mut self, now: u64) {
        let regressed = now < self.last_now;
        self.last_now = self.last_now.max(now);
        for e in &mut self.entries {
            if !matches!(e.stage, Stage::Active | Stage::Detached | Stage::Quiescing)
                && (regressed || now >= e.join.readiness_deadline_mono_ns)
            {
                e.stage = Stage::Expired;
            }
        }
    }
    pub fn quiesce(
        &mut self,
        sub: SubscriptionKey,
        session: &Session,
        request_id: [u8; 16],
        reason: u16,
    ) -> Result<Vec<u8>> {
        let index = self.index(sub)?;
        self.peer(index, session)?;
        if self.entries[index].stage == Stage::Detached {
            return Err(Error::State);
        }
        if let Some((e, b)) = &self.entries[index].quiesce {
            return Ok(session.encode(e.clone(), &encode(b)?)?);
        }
        let j = &self.entries[index].join;
        let b = QuiesceBody {
            subscriber_instance: j.subscriber_instance,
            bank_resource_id: j.bank_resource_id,
            cursor_resource_id: j.cursor_resource_id,
            subscription_generation: j.subscription_generation,
            channel_epoch: j.channel_epoch,
            ring_epoch: j.ring_epoch,
            membership_version: self.membership_version(),
            detach_reason: reason,
            ..Default::default()
        };
        let e = ControlEnvelope {
            opcode: 20,
            flags: 1,
            request_id,
            expected_state_version: b.membership_version,
            ..Default::default()
        };
        let frame = session.encode(e.clone(), &encode(&b)?)?;
        self.entries[index].quiesce = Some((ControlEnvelope::decode(&frame[..128])?, b));
        self.entries[index].stage = Stage::Quiescing;
        Ok(frame)
    }
    pub fn quiesced(
        &mut self,
        sub: SubscriptionKey,
        session: &mut Session,
        raw: &[u8],
        now: u64,
    ) -> Result<()> {
        let index = self.index(sub)?;
        self.peer(index, session)?;
        let frame = session.validate(raw, now)?;
        let e = &self.entries[index];
        let (request, expected) = e.quiesce.as_ref().ok_or(Error::State)?;
        let body = QuiesceBody::decode(frame.body)?;
        require(
            frame.envelope.opcode == 20
                && frame.envelope.flags == 10
                && frame.envelope.request_id == request.request_id
                && frame.envelope.control_epoch == request.control_epoch
                && frame.envelope.expected_state_version == request.expected_state_version
                && body.evidence_kind == 1
                && body.exit_evidence_id == [0; 16],
        )?;
        let mut normalized = body.clone();
        normalized.released_seq = 0;
        normalized.applied_seq = 0;
        normalized.evidence_kind = 0;
        require(&normalized == expected)?;
        if e.stage == Stage::Detached {
            return require(e.detached.as_ref() == Some(&body));
        }
        self.publisher.detach_registered(
            sub,
            Watermarks {
                read: body.released_seq,
                released: body.released_seq,
                applied: body.applied_seq,
            },
        )?;
        self.entries[index].stage = Stage::Detached;
        self.entries[index].detached = Some(body);
        self.entries[index].resource = None;
        Ok(())
    }
}

#[allow(unsafe_code)]
mod mapped;
pub use mapped::JoinConsumer;

fn catchup_watermarks(
    baseline: u64,
    previous: Option<Watermarks>,
    published: u64,
    actual: Watermarks,
) -> Result<Option<u64>> {
    if actual.applied < baseline
        || actual.released < baseline
        || actual.applied > actual.read
        || actual.released > actual.read
        || actual.read > published
        || previous.is_some_and(|p| {
            actual.read < p.read || actual.applied < p.applied || actual.released < p.released
        })
    {
        return Err(Error::Ring(tickwire_shm::MappedError::Protocol(
            tickwire_shm::Error::Cursor,
        )));
    }
    Ok((actual.applied < published).then_some(published))
}

#[cfg(test)]
mod catchup_tests {
    use super::*;
    fn at(n: u64) -> Watermarks {
        Watermarks {
            read: n,
            applied: n,
            released: n,
        }
    }
    #[test]
    fn publication_advancing_between_ready_attempts_requires_another_catchup() {
        assert_eq!(catchup_watermarks(10, None, 13, at(10)).unwrap(), Some(13));
        assert_eq!(
            catchup_watermarks(10, Some(at(10)), 16, at(13)).unwrap(),
            Some(16)
        );
        assert_eq!(
            catchup_watermarks(10, Some(at(13)), 16, at(16)).unwrap(),
            None
        );
    }
    #[test]
    fn malformed_and_regressing_cursors_are_errors_not_retry_targets() {
        for actual in [
            at(9),
            at(17),
            Watermarks {
                read: 12,
                applied: 13,
                released: 12,
            },
            Watermarks {
                read: 12,
                applied: 12,
                released: 13,
            },
        ] {
            assert!(catchup_watermarks(10, None, 16, actual).is_err());
        }
        assert!(catchup_watermarks(10, Some(at(13)), 16, at(12)).is_err());
    }
}
