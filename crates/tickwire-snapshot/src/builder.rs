//! Canonical cold-path construction of a complete topic snapshot.
use crate::{InstrumentSpec, JoinContext, validate};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use tickwire_ipc::*;

pub const MAX_TOPIC_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug)]
pub struct BuildRequest<'a> {
    pub resource: &'a ResourceHeader,
    pub join: &'a JoinBody,
    pub catalog_hash: [u8; 32],
    pub instrument_set_version: u64,
    pub sequence: u64,
    pub created_mono_ns: u64,
    pub expires_mono_ns: u64,
    pub now_mono_ns: u64,
    pub logical_strategy_id: u64,
    pub instruments: &'a [InstrumentSpec],
    pub members: &'a [InstrumentSnapshot<'a>],
    pub byte_limit: usize,
}
#[derive(Debug, Clone)]
pub struct InstrumentSnapshot<'a> {
    pub instrument_id: u64,
    pub instrument_version: u64,
    pub snapshot_id: [u8; 16],
    pub at_sequence: u64,
    pub last_event_sequence: u64,
    pub feed_generation: u64,
    pub catalog_version: u64,
    pub catalog_hash: [u8; 32],
    pub created_mono_ns: u64,
    pub expires_mono_ns: u64,
    pub state_version: u64,
    pub coverage_depth: u32,
    pub coverage_flags: u32,
    pub states: &'a [MarketRecord],
    pub bids: &'a [DepthLevel],
    pub asks: &'a [DepthLevel],
}
#[derive(Debug)]
pub struct BuiltTopic {
    bytes: Vec<u8>,
    resource: ResourceHeader,
    header: SnapshotHeader,
    join: JoinBody,
}
impl BuiltTopic {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn resource(&self) -> &ResourceHeader {
        &self.resource
    }
    pub fn header(&self) -> &SnapshotHeader {
        &self.header
    }
    pub fn join(&self) -> &JoinBody {
        &self.join
    }
    pub fn file_hash(&self) -> [u8; 32] {
        self.join.snapshot_hash
    }
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
fn same(condition: bool) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(Error::Identity)
    }
}
fn sequence(value: u64) -> Result<()> {
    if value >= 1 << 63 {
        Err(Error::SequenceExhausted)
    } else {
        Ok(())
    }
}
fn page(value: usize) -> Result<usize> {
    Ok(value.checked_add(4095).ok_or(Error::Bounds)? & !4095)
}
fn member_bytes(states: usize, bids: usize, asks: usize) -> Result<usize> {
    let levels = bids.checked_add(asks).ok_or(Error::Bounds)?;
    let depth = levels.checked_mul(16).ok_or(Error::Bounds)?;
    let state = states.checked_mul(512).ok_or(Error::Bounds)?;
    page(
        384usize
            .checked_add(state)
            .and_then(|n| n.checked_add(depth))
            .ok_or(Error::Bounds)?,
    )
}
fn binding<T: PartialEq>(input: &T, zero: &T, actual: &T) -> Result<()> {
    same(input == zero || input == actual)
}
fn normalize_record(record: &MarketRecord, row: u64, out: &mut [u8; 512]) -> Result<()> {
    let mut normalized = record.clone();
    normalized.header.sequence = row;
    normalized.header.record_crc32c = 0;
    normalized.encode(out)?;
    if normalized.header.flags & 8 != 0 {
        // All accepted snapshot records have no external payload and a zero payload CRC.
        normalized.header.record_crc32c = crc32c(out);
        normalized.encode(out)?;
    }
    decode_record(out)?;
    Ok(())
}
fn state_order(member: &InstrumentSnapshot<'_>) -> Result<([usize; 4], u16)> {
    if !(1..=4).contains(&member.states.len()) {
        return Err(Error::Bounds);
    }
    let mut order = [0, 1, 2, 3];
    order[..member.states.len()].sort_unstable_by_key(|&i| member.states[i].header.message_type);
    let mut prior = 0;
    let mut feed = None;
    for &i in &order[..member.states.len()] {
        let r = &member.states[i];
        if !matches!(r.header.message_type, 257 | 258 | 260 | 261) {
            return Err(Error::UnknownMessage);
        }
        if r.header.message_type == prior {
            return Err(Error::Malformed);
        }
        prior = r.header.message_type;
        if prior == 261 {
            feed = Some(r.body.feed_state);
        }
    }
    let feed = feed.ok_or(Error::Malformed)?;
    if !(1..=5).contains(&feed) {
        return Err(Error::InvalidEnum);
    }
    Ok((order, feed))
}
fn depth(levels: &[DepthLevel], bid: bool) -> Result<()> {
    let mut previous = None;
    for level in levels {
        if level.price_ticks <= 0 || level.qty_lots <= 0 {
            return Err(Error::Malformed);
        }
        if let Some(price) = previous
            && ((bid && price <= level.price_ticks) || (!bid && price >= level.price_ticks))
        {
            return Err(Error::Malformed);
        }
        previous = Some(level.price_ticks);
    }
    Ok(())
}
fn check_member(request: &BuildRequest<'_>, member: &InstrumentSnapshot<'_>) -> Result<()> {
    same(
        member.snapshot_id != [0; 16]
            && member.at_sequence == request.sequence
            && member.last_event_sequence <= request.sequence
            && member.feed_generation == request.resource.feed_generation
            && member.catalog_version == request.resource.catalog_version
            && member.catalog_hash == request.catalog_hash,
    )?;
    sequence(member.instrument_version)?;
    sequence(member.state_version)?;
    if member.coverage_flags & !3 != 0 {
        return Err(Error::InvalidEnum);
    }
    crate::time_window(
        member.created_mono_ns,
        member.expires_mono_ns,
        request.now_mono_ns,
    )?;
    let (order, feed) = state_order(member)?;
    for (row, &index) in order[..member.states.len()].iter().enumerate() {
        let r = &member.states[index];
        let h = &r.header;
        same(
            h.sequence <= member.last_event_sequence
                && h.instrument_id == member.instrument_id
                && h.instrument_version == member.instrument_version
                && h.feed_generation == member.feed_generation
                && h.topic_or_scope_id == request.resource.topic_id
                && h.producer_generation == request.resource.producer_generation,
        )?;
        if (h.flags & 2 != 0 && h.ingress_mono_ns > member.created_mono_ns)
            || (h.flags & 4 != 0 && h.publish_mono_ns > member.created_mono_ns)
        {
            return Err(Error::Deadline);
        }
        // A non-depth state cannot carry a payload CRC, even when record CRC is recomputed.
        if h.payload_crc32c != 0 {
            return Err(Error::Checksum);
        }
        normalize_record(r, row as u64 + 1, &mut [0; 512])?;
    }
    depth(member.bids, true)?;
    depth(member.asks, false)?;
    if feed == 2
        && let (Some(b), Some(a)) = (member.bids.first(), member.asks.first())
        && b.price_ticks >= a.price_ticks
    {
        return Err(Error::Malformed);
    }
    Ok(())
}
fn check_context(request: &BuildRequest<'_>, set_hash: [u8; 32]) -> Result<JoinBody> {
    let r = request.resource;
    let j = request.join;
    same(
        r.resource_id != [0; 16]
            && r.channel_id != [0; 16]
            && r.producer_instance != [0; 16]
            && r.host_boot_id != [0; 16]
            && r.namespace_id != [0; 16]
            && r.producer_generation != 0
            && r.feed_generation != 0
            && r.topic_id != 0
            && r.catalog_version != 0
            && request.instrument_set_version != 0
            && r.event_class == 7
            && r.channel_id == j.channel_id
            && r.channel_epoch == j.channel_epoch
            && r.ring_epoch == j.ring_epoch
            && r.feed_generation == j.feed_generation
            && r.topic_id == j.topic_id
            && r.catalog_version == j.expected_catalog_version
            && r.initial_membership_version == j.membership_version
            && r.qos == j.qos
            && j.subscriber_instance != [0; 16]
            && j.cursor_resource_id != [0; 16]
            && j.bank_resource_id != [0; 16]
            && j.subscription_generation != 0
            && (r.consumer_instance == [0; 16] || r.consumer_instance == j.subscriber_instance)
            && (request.logical_strategy_id == 0
                || request.logical_strategy_id == j.logical_strategy_id),
    )?;
    if !matches!(r.target_arch, 1 | 2)
        || r.endian != 1
        || r.atomic_width != 8
        || r.page_shift != 12
        || r.isolation_shift != 7
        || r.clock_domain != 1
    {
        return Err(Error::UnsupportedArchitecture);
    }
    if r.required_features & !63 != 0
        || r.optional_features & !63 != 0
        || r.required_features & 0x1c != 0x1c
    {
        return Err(Error::UnsupportedFeature);
    }
    if !(1..=2).contains(&r.trust_profile)
        || !(1..=3).contains(&r.qos)
        || !(1..=9).contains(&j.role)
        || j.join_flags & !3 != 0
        || j.join_flags & 1 == 0
    {
        return Err(Error::InvalidEnum);
    }
    for v in [
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
        request.instrument_set_version,
    ] {
        sequence(v)?;
    }
    crate::time_window(
        request.created_mono_ns,
        request.expires_mono_ns,
        request.now_mono_ns,
    )?;
    if r.created_mono_ns > request.created_mono_ns
        || request.now_mono_ns >= j.readiness_deadline_mono_ns
    {
        return Err(Error::Deadline);
    }
    binding(&j.snapshot_id, &[0; 16], &r.resource_id)?;
    binding(&j.snapshot_seq, &0, &request.sequence)?;
    binding(&j.join_seq, &0, &(request.sequence + 1))?;
    binding(&j.topic_instrument_set_hash, &[0; 32], &set_hash)?;
    binding(
        &j.topic_instrument_set_version,
        &0,
        &request.instrument_set_version,
    )?;
    let mut result = j.clone();
    result.snapshot_id = r.resource_id;
    result.snapshot_seq = request.sequence;
    result.join_seq = request.sequence + 1;
    result.topic_instrument_set_hash = set_hash;
    result.topic_instrument_set_version = request.instrument_set_version;
    Ok(result)
}

/// Build a complete immutable byte image. The caller owns the fixed publication
/// cut and its pending gate; neither is advanced or released by this function.
pub fn build_topic(request: &BuildRequest<'_>) -> Result<BuiltTopic> {
    let count = request.instruments.len();
    if !(1..=4096).contains(&count)
        || request.byte_limit == 0
        || request.byte_limit > MAX_TOPIC_SNAPSHOT_BYTES
    {
        return Err(Error::Bounds);
    }
    if request.members.len() != count {
        return Err(Error::ResyncRequired);
    }
    // J must also remain strictly below the wire sequence exhaustion boundary.
    if request.sequence >= (1 << 63) - 1 {
        return Err(Error::SequenceExhausted);
    }
    let member_start = page(
        4352usize
            .checked_add(count.checked_mul(128).ok_or(Error::Bounds)?)
            .ok_or(Error::Bounds)?,
    )?;
    let mut total = member_start;
    for m in request.members {
        let bytes = member_bytes(m.states.len(), m.bids.len(), m.asks.len())?;
        total = total.checked_add(bytes).ok_or(Error::Bounds)?;
        if total > request.byte_limit {
            return Err(Error::Bounds);
        }
    }
    let mut set = Sha256::new();
    let mut prior = 0;
    for (spec, m) in request.instruments.iter().zip(request.members) {
        if spec.required_states & !7 != 0 {
            return Err(Error::UnsupportedFeature);
        }
        if spec.instrument_id <= prior
            || spec.instrument_version == 0
            || (spec.instrument_id, spec.instrument_version)
                != (m.instrument_id, m.instrument_version)
        {
            return Err(Error::ResyncRequired);
        }
        prior = spec.instrument_id;
        set.update(spec.instrument_id.to_le_bytes());
        set.update(spec.instrument_version.to_le_bytes());
    }
    let set_hash: [u8; 32] = set.finalize().into();
    let mut join = check_context(request, set_hash)?;
    let mut ids = HashSet::new();
    ids.try_reserve(count + 1).map_err(|_| Error::Bounds)?;
    ids.insert(request.resource.resource_id);
    for m in request.members {
        same(ids.insert(m.snapshot_id))?;
        check_member(request, m)?;
    }
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(total).map_err(|_| Error::Bounds)?;
    bytes.resize(total, 0);
    let mut offset = member_start;
    for (index, m) in request.members.iter().enumerate() {
        let size = member_bytes(m.states.len(), m.bids.len(), m.asks.len())?;
        let (order, feed_state) = state_order(m)?;
        let state_offset = offset + 384;
        for (row, &i) in order[..m.states.len()].iter().enumerate() {
            let mut image = [0; 512];
            normalize_record(&m.states[i], row as u64 + 1, &mut image)?;
            bytes[state_offset + row * 512..state_offset + (row + 1) * 512].copy_from_slice(&image);
        }
        let state_length = m.states.len() * 512;
        let depth_offset = state_offset + state_length;
        for (i, level) in m.bids.iter().chain(m.asks).enumerate() {
            level.encode(&mut bytes[depth_offset + i * 16..depth_offset + (i + 1) * 16])?;
        }
        let depth_length = (m.bids.len() + m.asks.len()) * 16;
        let state = SnapshotSection {
            section_kind: 1,
            entry_bytes: 512,
            entry_count: m.states.len() as u32,
            offset: state_offset as u64,
            length: state_length as u64,
            entries_hash: hash(&bytes[state_offset..depth_offset]),
        };
        let depth = SnapshotSection {
            section_kind: 2,
            entry_bytes: 16,
            entry_count: (m.bids.len() + m.asks.len()) as u32,
            offset: depth_offset as u64,
            length: depth_length as u64,
            entries_hash: hash(&bytes[depth_offset..depth_offset + depth_length]),
        };
        state.encode(&mut bytes[offset + 256..offset + 320])?;
        depth.encode(&mut bytes[offset + 320..offset + 384])?;
        let header = SnapshotHeader {
            magic: *b"TWSNP001",
            snapshot_kind: 6,
            schema_major: 1,
            entry_bytes: 64,
            entry_count: 2,
            entry_offset: (offset + 256) as u32,
            snapshot_id: m.snapshot_id,
            channel_id: request.resource.channel_id,
            feed_generation: m.feed_generation,
            sequence: request.sequence,
            instrument_id: m.instrument_id,
            instrument_version: m.instrument_version,
            catalog_hash: request.catalog_hash,
            entries_hash: hash(&bytes[offset + 256..offset + 384]),
            bid_count: m.bids.len() as u32,
            ask_count: m.asks.len() as u32,
            coverage_depth: m.coverage_depth,
            coverage_flags: m.coverage_flags,
            created_mono_ns: m.created_mono_ns,
            expires_mono_ns: m.expires_mono_ns,
            logical_strategy_id: request.logical_strategy_id,
            state_version: m.state_version,
            object_bytes: size as u64,
            ..SnapshotHeader::default()
        };
        header.encode(&mut bytes[offset..offset + 256])?;
        let entry = TopicSnapshotEntry {
            instrument_id: m.instrument_id,
            instrument_version: m.instrument_version,
            snapshot_id: m.snapshot_id,
            snapshot_hash: hash(&bytes[offset..offset + size]),
            snapshot_bytes: size as u64,
            snapshot_seq: request.sequence,
            feed_generation: m.feed_generation,
            catalog_version: m.catalog_version,
            snapshot_kind: 6,
            feed_state,
            last_instrument_event_seq: m.last_event_sequence,
            snapshot_offset: offset as u64,
            entry_flags: 0,
        };
        entry.encode(&mut bytes[4352 + index * 128..4352 + (index + 1) * 128])?;
        offset += size;
    }
    let header = SnapshotHeader {
        magic: *b"TWSNP001",
        snapshot_kind: 4,
        schema_major: 1,
        entry_bytes: 128,
        entry_count: count as u32,
        entry_offset: 4352,
        snapshot_id: request.resource.resource_id,
        channel_id: request.resource.channel_id,
        feed_generation: request.resource.feed_generation,
        sequence: request.sequence,
        catalog_hash: request.catalog_hash,
        entries_hash: hash(&bytes[4352..4352 + count * 128]),
        created_mono_ns: request.created_mono_ns,
        expires_mono_ns: request.expires_mono_ns,
        logical_strategy_id: request.logical_strategy_id,
        state_version: request.instrument_set_version,
        object_bytes: total as u64,
        topic_instrument_set_hash: set_hash,
        manifest_member_bytes: (total - member_start) as u64,
        ..SnapshotHeader::default()
    };
    header.encode(&mut bytes[4096..4352])?;
    let mut resource = request.resource.clone();
    resource.magic = *b"TWIPC001";
    resource.abi_major = 1;
    resource.abi_minor = 0;
    resource.header_bytes = 4096;
    resource.resource_kind = 8;
    resource.record_bytes = 128;
    resource.capacity = count as u32;
    resource.resource_bytes = total as u64;
    resource.schema_major = 1;
    resource.schema_minor = 0;
    resource.slot_offset = 4096;
    resource.producer_page_offset = 0;
    resource.cursor_page_offset = 0;
    resource.arena_offset = 0;
    resource.arena_bytes = 0;
    resource.arena_class_count = 0;
    resource.class_descriptors = core::array::from_fn(|_| ArenaClass::default());
    resource.descriptor_hash = [0; 32];
    resource.encode(&mut bytes[..4096])?;
    resource.descriptor_hash = hash(&bytes[..4096]);
    resource.encode(&mut bytes[..4096])?;
    let file_hash = hash(&bytes);
    binding(&request.join.snapshot_hash, &[0; 32], &file_hash)?;
    join.snapshot_hash = file_hash;
    validate(
        &bytes,
        &JoinContext {
            expected_resource: &resource,
            catalog_hash: request.catalog_hash,
            instruments: request.instruments,
            join: &join,
            now_mono_ns: request.now_mono_ns,
        },
    )?;
    Ok(BuiltTopic {
        bytes,
        resource,
        header,
        join,
    })
}

#[cfg(test)]
mod arithmetic_tests {
    use super::*;
    #[test]
    fn unrepresentable_layouts_are_rejected_without_slices_or_allocation() {
        assert_eq!(member_bytes(usize::MAX, 0, 0), Err(Error::Bounds));
        assert_eq!(member_bytes(1, usize::MAX, 1), Err(Error::Bounds));
        assert_eq!(member_bytes(1, usize::MAX / 16, 0), Err(Error::Bounds));
        assert_eq!(page(usize::MAX), Err(Error::Bounds));
    }
}
