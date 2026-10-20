//! The only module that discharges shared-memory SDK peer-behavior contracts.
use super::*;
use std::cell::Cell;
use tickwire_shm::{BankKey, Subscriber};
use tickwire_shm_sys::{ImmutableMapping, ImmutableResource};
use tickwire_snapshot::{JoinContext, ValidatedTopic};

impl<const M: usize> JoinOwner<M> {
    /// # Safety
    /// The authenticated peer uses the qualified Subscriber SDK, never mutates
    /// its immutable cursor header, and never releases any outstanding reference.
    pub unsafe fn receive_cursor(
        &mut self,
        sub: SubscriptionKey,
        session: &mut Session,
        packet: ReceivedFrame,
        now: u64,
    ) -> Result<()> {
        let index = self.index(sub)?;
        self.peer(index, session)?;
        self.live(index, now)?;
        if !matches!(
            self.entries[index].stage,
            Stage::Prepared | Stage::Delivered
        ) || self.entries[index].cursor_registered
        {
            return Err(Error::State);
        }
        let (bytes, mut fds) = packet.into_parts();
        let frame = session.validate(&bytes, now)?;
        require(
            frame.envelope.opcode == 3
                && frame.envelope.flags == 32
                && frame.envelope.fd_count == 1
                && fds.len() == 1,
        )?;
        let prefix = OfferPrefix::decode(&frame.body[..64])?;
        let incoming = ResourceDescriptor::decode(&frame.body[64..])?;
        let e = &self.entries[index];
        let expected = cursor_header(self.header(), sub)?;
        require(
            prefix.offer_id == offer_id(&e.join, 3)
                && prefix.required_seals == 0x17
                && prefix.expires_mono_ns == e.join.readiness_deadline_mono_ns
                && prefix.membership_version == e.join.membership_version
                && incoming == descriptor(&expected, 1)
                && frame.envelope.expected_state_version == e.join.membership_version,
        )?;
        let fd = fds.pop().ok_or(Error::State)?;
        // SAFETY: caller establishes the qualified remote cursor writer contract;
        // exact authenticated peer and immutable descriptor are checked above.
        unsafe {
            self.publisher.register_pending_cursor(sub, fd)?;
        }
        self.entries[index].cursor_registered = true;
        Ok(())
    }
}

pub struct JoinConsumer<const B: usize> {
    reader: Subscriber<B>,
    resource: ImmutableResource,
    bank_header: ResourceHeader,
    join: JoinBody,
    registry: TopicRegistry,
    loaded: bool,
    quiescing: Cell<bool>,
}
impl<const B: usize> JoinConsumer<B> {
    /// # Safety
    /// The authenticated owner obeys Publisher's single-writer and gated-reuse
    /// protocol for this exact bank and subscription, including all holders.
    /// Headers and JOIN must come from the authenticated supervisor/offer registry,
    /// not from self-attestation by the received file.
    pub unsafe fn attach(
        bank: OwnedFd,
        snapshot: OwnedFd,
        bank_header: ResourceHeader,
        snapshot_header: ResourceHeader,
        join: JoinBody,
        registry: TopicRegistry,
        now: u64,
    ) -> Result<Self> {
        require(
            join.bank_resource_id == bank_header.resource_id
                && join.channel_id == bank_header.channel_id
                && join.channel_epoch == bank_header.channel_epoch
                && join.ring_epoch == bank_header.ring_epoch
                && join.feed_generation == bank_header.feed_generation
                && join.topic_id == bank_header.topic_id
                && join.expected_catalog_version == bank_header.catalog_version
                && join.qos == bank_header.qos
                && join.join_flags == 3
                && join.topic_instrument_set_version == registry.instrument_set_version
                && join.topic_instrument_set_hash == instrument_set_hash(&registry.instruments)
                && snapshot_header.producer_instance == bank_header.producer_instance
                && snapshot_header.producer_generation == bank_header.producer_generation
                && snapshot_header.host_boot_id == bank_header.host_boot_id
                && snapshot_header.namespace_id == bank_header.namespace_id,
        )?;
        let len = usize::try_from(snapshot_header.resource_bytes).map_err(|_| Error::Capacity)?;
        let resource = ImmutableMapping::map(snapshot, len)?.validate_resource(
            &snapshot_header,
            len.checked_sub(4096).ok_or(Error::Capacity)?,
            join.snapshot_hash,
        )?;
        tickwire_snapshot::validate(
            resource.bytes(),
            &JoinContext {
                expected_resource: resource.header(),
                catalog_hash: registry.catalog_hash,
                instruments: &registry.instruments,
                join: &join,
                now_mono_ns: now,
            },
        )?;
        let sub = SubscriptionKey {
            bank: BankKey {
                resource: u128::from_le_bytes(bank_header.resource_id),
                channel: u128::from_le_bytes(bank_header.channel_id),
                channel_epoch: bank_header.channel_epoch,
                ring_epoch: bank_header.ring_epoch,
                feed_generation: bank_header.feed_generation,
            },
            instance: u128::from_le_bytes(join.subscriber_instance),
            generation: join.subscription_generation,
            cursor: u128::from_le_bytes(join.cursor_resource_id),
        };
        // SAFETY: caller establishes the qualified publisher and persistent gate;
        // immutable resource and exact identity bindings were validated above.
        let reader =
            unsafe { Subscriber::attach(bank, bank_header.clone(), sub, join.snapshot_seq)? };
        Ok(Self {
            reader,
            resource,
            bank_header,
            join,
            registry,
            loaded: false,
            quiescing: Cell::new(false),
        })
    }
    fn baseline(&self, now: u64) -> Result<ValidatedTopic<'_>> {
        Ok(tickwire_snapshot::validate(
            self.resource.bytes(),
            &JoinContext {
                expected_resource: self.resource.header(),
                catalog_hash: self.registry.catalog_hash,
                instruments: &self.registry.instruments,
                join: &self.join,
                now_mono_ns: now,
            },
        )?)
    }
    fn peer(&self, session: &Session) -> Result<()> {
        require(
            session.peer().instance == self.bank_header.producer_instance
                && session.peer().generation == self.bank_header.producer_generation
                && session.peer().role == 2
                && session.peer().host_boot == self.bank_header.host_boot_id
                && session.peer().namespace == self.bank_header.namespace_id,
        )
    }
    pub fn load_baseline<T>(
        &mut self,
        now: u64,
        load: impl FnOnce(&ValidatedTopic<'_>) -> tickwire_ipc::Result<T>,
    ) -> Result<T> {
        if self.quiescing.get() || self.loaded {
            return Err(Error::State);
        }
        let result = load(&self.baseline(now)?)?;
        self.loaded = true;
        Ok(result)
    }
    pub fn cursor_offer(
        &self,
        session: &Session,
        request_id: [u8; 16],
    ) -> Result<(Vec<u8>, OwnedFd)> {
        self.peer(session)?;
        let sub = SubscriptionKey {
            bank: BankKey {
                resource: u128::from_le_bytes(self.bank_header.resource_id),
                channel: u128::from_le_bytes(self.bank_header.channel_id),
                channel_epoch: self.bank_header.channel_epoch,
                ring_epoch: self.bank_header.ring_epoch,
                feed_generation: self.bank_header.feed_generation,
            },
            instance: u128::from_le_bytes(self.join.subscriber_instance),
            generation: self.join.subscription_generation,
            cursor: u128::from_le_bytes(self.join.cursor_resource_id),
        };
        let h = cursor_header(&self.bank_header, sub)?;
        let bytes = offer(session, &h, &self.join, 3)?;
        let mut envelope = ControlEnvelope::decode(&bytes[..128])?;
        envelope.request_id = request_id;
        Ok((
            session.encode(envelope, &bytes[128..])?,
            self.reader.cursor_fd()?,
        ))
    }
    pub fn read_ready_frames(
        &self,
        session: &Session,
        request_ids: [[u8; 16]; 2],
    ) -> Result<[Vec<u8>; 2]> {
        self.peer(session)?;
        if !self.loaded || self.quiescing.get() {
            return Err(Error::NotReady);
        }
        let mut result = [Vec::new(), Vec::new()];
        for (i, h) in [&self.bank_header, self.resource.header()]
            .into_iter()
            .enumerate()
        {
            result[i] = session.encode(
                ControlEnvelope {
                    opcode: 5,
                    flags: 1,
                    request_id: request_ids[i],
                    expected_state_version: self.join.membership_version,
                    ..Default::default()
                },
                &encode(&ResourceAckBody {
                    offer_id: offer_id(&self.join, i as u8 + 1),
                    resource_id: h.resource_id,
                    header_hash: h.descriptor_hash,
                    mapped_bytes: h.resource_bytes,
                    observed_seals: if i == 0 { 0x17 } else { 0x0f },
                    access: 1,
                    ack_kind: 2,
                    registration_generation: self.join.subscription_generation,
                })?,
            )?;
        }
        Ok(result)
    }
    pub fn join_ready_frame(
        &self,
        session: &Session,
        request_id: [u8; 16],
        now: u64,
    ) -> Result<Vec<u8>> {
        self.peer(session)?;
        if !self.loaded || self.quiescing.get() {
            return Err(Error::NotReady);
        }
        self.baseline(now)?.check_readiness(now)?;
        Ok(session.encode(
            ControlEnvelope {
                opcode: 17,
                flags: 1,
                request_id,
                expected_state_version: self.join.membership_version,
                ..Default::default()
            },
            &encode(&self.join)?,
        )?)
    }
    pub fn borrow(&self) -> Result<Option<tickwire_shm::RecordBorrow<'_, B>>> {
        if !self.loaded {
            return Err(Error::NotReady);
        }
        Ok(self.reader.borrow()?)
    }
    pub fn apply(&self, sequence: u64) -> Result<u64> {
        if !self.loaded {
            return Err(Error::NotReady);
        }
        Ok(self.reader.apply(sequence)?)
    }
    pub fn watermarks(&self) -> Watermarks {
        self.reader.watermarks()
    }
    pub fn quiesce(&self, session: &mut Session, request: &[u8], now: u64) -> Result<Vec<u8>> {
        self.peer(session)?;
        let frame = session.validate(request, now)?;
        let mut body = QuiesceBody::decode(frame.body)?;
        require(
            frame.envelope.opcode == 20
                && frame.envelope.flags == 1
                && body.subscriber_instance == self.join.subscriber_instance
                && body.bank_resource_id == self.join.bank_resource_id
                && body.cursor_resource_id == self.join.cursor_resource_id
                && body.subscription_generation == self.join.subscription_generation
                && body.channel_epoch == self.join.channel_epoch
                && body.ring_epoch == self.join.ring_epoch
                && body.membership_version >= self.join.membership_version
                && body.released_seq == 0
                && body.applied_seq == 0
                && body.evidence_kind == 0
                && body.exit_evidence_id == [0; 16],
        )?;
        self.quiescing.set(true);
        self.reader.start_quiesce();
        let evidence = self.reader.quiesced()?;
        body.released_seq = evidence.watermarks().released;
        body.applied_seq = evidence.watermarks().applied;
        body.evidence_kind = 1;
        Ok(session.encode(
            ControlEnvelope {
                flags: 10,
                ..frame.envelope
            },
            &encode(&body)?,
        )?)
    }
}
