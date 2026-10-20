//! Authenticated-context validation for immutable normalized market snapshots.
#![forbid(unsafe_code)]
use tickwire_ipc::*;

pub const QUOTE: u8 = 1;
pub const TRADE: u8 = 2;
pub const MARK_FUNDING: u8 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstrumentSpec {
    pub instrument_id: u64,
    pub instrument_version: u64,
    pub required_states: u8,
}
pub struct JoinContext<'a> {
    pub expected_resource: &'a ResourceHeader,
    pub catalog_hash: [u8; 32],
    pub instruments: &'a [InstrumentSpec],
    pub join: &'a JoinBody,
    pub now_mono_ns: u64,
}
pub struct ValidatedTopic<'a> {
    bytes: &'a [u8],
    header: SnapshotHeader,
    validated_at: u64,
    deadline: u64,
    live_required_state: bool,
}
impl core::fmt::Debug for ValidatedTopic<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ValidatedTopic")
            .field("byte_len", &self.bytes.len())
            .field("snapshot_id", &self.header.snapshot_id)
            .field("members", &self.len())
            .field("deadline", &self.deadline)
            .field("live_required_state", &self.live_required_state)
            .finish()
    }
}
impl<'a> ValidatedTopic<'a> {
    pub fn bytes(&self) -> &'a [u8] {
        self.bytes
    }
    pub fn header(&self) -> &SnapshotHeader {
        &self.header
    }
    pub fn len(&self) -> usize {
        self.header.entry_count as usize
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn member(&self, index: usize) -> Result<Member<'a>> {
        if index >= self.len() {
            return Err(Error::Bounds);
        }
        member(self.bytes, index)
    }
    /// Snapshot baseline only: the owner must still check cursor/resource
    /// readiness, per-state freshness and the independent execution gate.
    pub fn check_readiness(&self, now_mono_ns: u64) -> Result<()> {
        if now_mono_ns < self.validated_at || now_mono_ns >= self.deadline {
            return Err(Error::Deadline);
        }
        if !self.live_required_state {
            return Err(Error::NotReady);
        }
        Ok(())
    }
}
#[derive(Debug)]
pub struct Member<'a> {
    pub entry: TopicSnapshotEntry,
    pub header: SnapshotHeader,
    pub state_records: &'a [u8],
    pub depth_levels: &'a [u8],
}
fn range(raw: &[u8], offset: u64, length: u64) -> Result<&[u8]> {
    let end = offset.checked_add(length).ok_or(Error::Bounds)?;
    raw.get(
        usize::try_from(offset).map_err(|_| Error::Bounds)?
            ..usize::try_from(end).map_err(|_| Error::Bounds)?,
    )
    .ok_or(Error::Bounds)
}
fn identity(condition: bool) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(Error::Identity)
    }
}
fn time_window(created: u64, expires: u64, now: u64) -> Result<()> {
    if created > now || now >= expires {
        return Err(Error::Deadline);
    }
    Ok(())
}
fn member(raw: &[u8], index: usize) -> Result<Member<'_>> {
    let entry = TopicSnapshotEntry::decode(range(raw, 4352 + index as u64 * 128, 128)?)?;
    let off = entry.snapshot_offset;
    let header = SnapshotHeader::decode(range(raw, off, 256)?)?;
    if header.snapshot_kind != 6 {
        return Err(Error::UnsupportedOperation);
    }
    let state =
        SnapshotSection::decode(range(raw, off.checked_add(256).ok_or(Error::Bounds)?, 64)?)?;
    let depth =
        SnapshotSection::decode(range(raw, off.checked_add(320).ok_or(Error::Bounds)?, 64)?)?;
    Ok(Member {
        entry,
        header,
        state_records: range(raw, state.offset, state.length)?,
        depth_levels: range(raw, depth.offset, depth.length)?,
    })
}

/// Context must come from the authenticated offer and local registry, never
/// reconstructed from the received bytes. This function does not inspect FDs.
pub fn validate<'a>(bytes: &'a [u8], context: &JoinContext<'_>) -> Result<ValidatedTopic<'a>> {
    if bytes.len() > 64 * 1024 * 1024 || !(1..=4096).contains(&context.instruments.len()) {
        return Err(Error::Bounds);
    }
    if context
        .instruments
        .iter()
        .any(|i| i.required_states & !7 != 0)
    {
        return Err(Error::UnsupportedFeature);
    }
    let r = ResourceHeader::decode(range(bytes, 0, 4096)?)?;
    identity(r == *context.expected_resource)?;
    let s = SnapshotHeader::decode(range(bytes, 4096, 256)?)?;
    let j = context.join;
    let instruments = context
        .instruments
        .iter()
        .map(|i| (i.instrument_id, i.instrument_version))
        .collect::<Vec<_>>();
    validate_topic_bundle(
        bytes,
        &TopicSnapshotContext {
            expected_instruments: &instruments,
            expected_instrument_set_hash: j.topic_instrument_set_hash,
            expected_instrument_set_version: j.topic_instrument_set_version,
            boundary_sequence: j.snapshot_seq,
            join: j,
        },
    )?;
    identity(
        r.event_class == 7
            && r.resource_id == s.snapshot_id
            && r.channel_id == s.channel_id
            && r.channel_id == j.channel_id
            && r.channel_epoch == j.channel_epoch
            && r.ring_epoch == j.ring_epoch
            && r.feed_generation == s.feed_generation
            && r.feed_generation == j.feed_generation
            && r.topic_id == j.topic_id
            && r.catalog_version == j.expected_catalog_version
            && r.initial_membership_version == j.membership_version
            && r.qos == j.qos
            && s.catalog_hash == context.catalog_hash
            && s.exec_generation == 0
            && (s.logical_strategy_id == 0 || s.logical_strategy_id == j.logical_strategy_id)
            && (r.consumer_instance == [0; 16] || r.consumer_instance == j.subscriber_instance),
    )?;
    identity(
        r.resource_id != [0; 16]
            && r.channel_id != [0; 16]
            && r.producer_instance != [0; 16]
            && r.host_boot_id != [0; 16]
            && r.namespace_id != [0; 16]
            && j.subscriber_instance != [0; 16]
            && j.cursor_resource_id != [0; 16]
            && j.bank_resource_id != [0; 16]
            && r.producer_generation != 0
            && r.feed_generation != 0
            && r.topic_id != 0
            && j.subscription_generation != 0
            && r.catalog_version != 0,
    )?;
    if r.required_features & 0x1c != 0x1c {
        return Err(Error::UnsupportedFeature);
    }
    if j.join_flags & !3 != 0 || j.join_flags & 1 == 0 || !(1..=2).contains(&r.trust_profile) {
        return Err(Error::InvalidEnum);
    }
    if [
        r.channel_epoch,
        r.ring_epoch,
        r.feed_generation,
        r.producer_generation,
        r.catalog_version,
        r.first_seq,
        r.predecessor_ring_epoch,
        r.cutover_seq,
        r.initial_membership_version,
        j.subscription_generation,
        j.join_seq,
        j.topic_instrument_set_version,
    ]
    .iter()
    .any(|&v| v >= 1 << 63)
    {
        return Err(Error::SequenceExhausted);
    }
    let now = context.now_mono_ns;
    time_window(s.created_mono_ns, s.expires_mono_ns, now)?;
    if r.created_mono_ns > s.created_mono_ns || now >= j.readiness_deadline_mono_ns {
        return Err(Error::Deadline);
    }
    let mut deadline = s.expires_mono_ns.min(j.readiness_deadline_mono_ns);
    let mut ready = true;
    let mut snapshot_ids = std::collections::HashSet::with_capacity(context.instruments.len() + 1);
    snapshot_ids.insert(s.snapshot_id);
    for (index, required) in context.instruments.iter().enumerate() {
        let m = member(bytes, index)?;
        identity(m.entry.snapshot_id != [0; 16] && snapshot_ids.insert(m.entry.snapshot_id))?;
        time_window(m.header.created_mono_ns, m.header.expires_mono_ns, now)?;
        identity(
            m.header.exec_generation == 0 && m.header.logical_strategy_id == s.logical_strategy_id,
        )?;
        if required.instrument_version >= 1 << 63 {
            return Err(Error::SequenceExhausted);
        }
        deadline = deadline.min(m.header.expires_mono_ns);
        let mut observed = 0;
        ready &= m.entry.feed_state == 2;
        for row in m.state_records.as_chunks::<512>().0 {
            // validate_topic_bundle already checked every record and row count.
            let record = MarketRecord::decode(row)?;
            let h = record.header;
            identity(
                h.topic_or_scope_id == r.topic_id && h.producer_generation == r.producer_generation,
            )?;
            if (h.flags & 2 != 0 && h.ingress_mono_ns > m.header.created_mono_ns)
                || (h.flags & 4 != 0 && h.publish_mono_ns > m.header.created_mono_ns)
            {
                return Err(Error::Deadline);
            }
            let bit = match h.message_type {
                257 => QUOTE,
                258 => TRADE,
                260 => MARK_FUNDING,
                261 => {
                    identity(record.body.feed_state == m.entry.feed_state)?;
                    0
                }
                _ => return Err(Error::UnknownMessage),
            };
            observed |= bit;
            if bit & required.required_states != 0 {
                ready &= record.body.feed_state == 2;
            }
        }
        ready &= observed & required.required_states == required.required_states;
    }
    Ok(ValidatedTopic {
        bytes,
        header: s,
        validated_at: now,
        deadline,
        live_required_state: ready,
    })
}
