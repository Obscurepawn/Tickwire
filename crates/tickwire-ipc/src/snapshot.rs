use crate::*;
use sha2::{Digest, Sha256};

fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
fn hash_eq(bytes: &[u8], expected: &[u8; 32]) -> Result<()> {
    if hash(bytes) != *expected {
        Err(Error::Checksum)
    } else {
        Ok(())
    }
}
fn check(condition: bool) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(Error::Malformed)
    }
}
fn page_align(value: u64) -> Result<u64> {
    Ok(value.checked_add(4095).ok_or(Error::Bounds)? & !4095)
}

/// Resource shape validation is distinct from mapped FD identity/seals validation.
pub fn validate_resource_shape(
    kind: u16,
    capacity: u32,
    record_bytes: u32,
    resource_bytes: u64,
    snapshot: Option<&SnapshotHeader>,
) -> Result<()> {
    match kind {
        1..=3 => {
            if !(64..=1048576).contains(&capacity) || !capacity.is_power_of_two() {
                return Err(Error::Bounds);
            }
            check(record_bytes == if kind == 2 { 1024 } else { 512 })?;
        }
        4 => check(capacity == 0 && record_bytes == 0 && resource_bytes == 8192)?,
        9 => {
            if record_bytes != 1 || !(1..=4194304).contains(&capacity) {
                return Err(Error::Bounds);
            }
        }
        5..=8 => {
            let s = snapshot.ok_or(Error::Malformed)?;
            check(capacity == s.entry_count)?;
            let width = match (kind, s.snapshot_kind) {
                (5, 1) => 16,
                (5, 5) | (6, 2) => 512,
                (5, 6 | 7) => 64,
                (7, 3) => 256,
                (8, 4) => 128,
                _ => return Err(Error::Malformed),
            };
            check(record_bytes == width && u32::from(s.entry_bytes) == width)?;
            if kind == 8 && !(1..=4096).contains(&capacity) {
                return Err(Error::Bounds);
            }
        }
        _ => return Err(Error::InvalidEnum),
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct TopicSnapshotContext<'a> {
    pub expected_instruments: &'a [(u64, u64)],
    pub expected_instrument_set_hash: [u8; 32],
    pub expected_instrument_set_version: u64,
    pub boundary_sequence: u64,
    pub join: &'a JoinBody,
}

/// Checks a whole manifest and all members without allocating or returning borrowed mutable views.
pub fn validate_topic_bundle(raw: &[u8], context: &TopicSnapshotContext<'_>) -> Result<()> {
    if raw.len() > 64 * 1024 * 1024 {
        return Err(Error::Bounds);
    }
    let r = ResourceHeader::decode(bounded(raw, 0, 4096)?)?;
    let s = SnapshotHeader::decode(bounded(raw, 4096, 256)?)?;
    schema(r.abi_major, r.abi_minor)?;
    schema(r.schema_major, r.schema_minor)?;
    validate_snapshot_header(&s)?;
    check(
        r.magic == *b"TWIPC001"
            && r.header_bytes == 4096
            && r.resource_kind == 8
            && s.snapshot_kind == 4,
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
    if r.required_features & !511 != 0 || r.optional_features & !511 != 0 {
        return Err(Error::UnsupportedFeature);
    }
    check(
        r.resource_bytes == raw.len() as u64
            && s.object_bytes == r.resource_bytes
            && r.slot_offset == 4096
            && s.entry_offset == 4352,
    )?;
    check(
        r.producer_page_offset == 0
            && r.cursor_page_offset == 0
            && r.arena_offset == 0
            && r.arena_bytes == 0
            && r.arena_class_count == 0,
    )?;
    zero(&raw[384..640])?; // Unused class descriptors.
    validate_resource_shape(
        r.resource_kind,
        r.capacity,
        r.record_bytes,
        r.resource_bytes,
        Some(&s),
    )?;
    let mut hasher = Sha256::new();
    hasher.update(&raw[..272]);
    hasher.update([0u8; 32]);
    hasher.update(&raw[304..4096]);
    let digest: [u8; 32] = hasher.finalize().into();
    if digest != r.descriptor_hash {
        return Err(Error::Checksum);
    }
    let entries_len = u64::from(s.entry_count) * 128;
    let entries = bounded(raw, 4352, entries_len)?;
    hash_eq(entries, &s.entries_hash)?;
    if context.expected_instruments.len() != s.entry_count as usize {
        return Err(Error::ResyncRequired);
    }
    let mut set_hash = Sha256::new();
    for pair in context.expected_instruments {
        set_hash.update(pair.0.to_le_bytes());
        set_hash.update(pair.1.to_le_bytes());
    }
    let set_hash: [u8; 32] = set_hash.finalize().into();
    if set_hash != s.topic_instrument_set_hash
        || set_hash != context.expected_instrument_set_hash
        || s.state_version != context.expected_instrument_set_version
    {
        return Err(Error::ResyncRequired);
    }
    check(
        s.sequence == context.boundary_sequence
            && s.instrument_id == 0
            && s.instrument_version == 0,
    )?;
    let entry_end = 4352 + entries_len;
    let mut cursor = page_align(entry_end)?;
    zero(bounded(raw, entry_end, cursor - entry_end)?)?;
    let member_start = cursor;
    let mut prior = 0;
    for (index, row) in entries.as_chunks::<128>().0.iter().enumerate() {
        let e = TopicSnapshotEntry::decode(row)?;
        if (e.instrument_id, e.instrument_version) != context.expected_instruments[index]
            || e.instrument_id <= prior
        {
            return Err(Error::ResyncRequired);
        }
        prior = e.instrument_id;
        check(e.entry_flags == 0 && (1..=5).contains(&e.feed_state))?;
        check(
            e.snapshot_offset == cursor && e.snapshot_bytes >= 4096 && e.snapshot_bytes % 4096 == 0,
        )?;
        let member = bounded(raw, cursor, e.snapshot_bytes)?;
        hash_eq(member, &e.snapshot_hash)?;
        let h = SnapshotHeader::decode(&member[..256])?;
        validate_snapshot_header(&h)?;
        if (h.snapshot_kind == 7) != (r.required_features & 64 != 0) {
            return Err(Error::UnsupportedFeature);
        }
        check(
            h.snapshot_id == e.snapshot_id
                && h.object_bytes == e.snapshot_bytes
                && h.instrument_id == e.instrument_id
                && h.instrument_version == e.instrument_version
                && h.snapshot_kind == e.snapshot_kind,
        )?;
        check(
            h.sequence == e.snapshot_seq
                && h.sequence == s.sequence
                && e.last_instrument_event_seq <= s.sequence,
        )?;
        check(
            h.channel_id == s.channel_id
                && h.feed_generation == e.feed_generation
                && h.feed_generation == s.feed_generation
                && h.catalog_hash == s.catalog_hash
                && e.catalog_version == r.catalog_version,
        )?;
        check(
            u64::from(h.entry_offset) == cursor + 256
                && is_zero(&h.topic_instrument_set_hash)
                && h.manifest_member_bytes == 0,
        )?;
        let length = u64::from(h.entry_count) * u64::from(h.entry_bytes);
        let payload = bounded(member, 256, length)?;
        hash_eq(payload, &h.entries_hash)?;
        let used = match h.snapshot_kind {
            1 => {
                check(h.entry_bytes == 16)?;
                validate_levels(payload, h.bid_count, h.ask_count)?;
                256 + length
            }
            5 => {
                check(h.entry_bytes == 512)?;
                validate_state_records(payload, &h)?;
                256 + length
            }
            6 | 7 => {
                let exact = h.snapshot_kind == 7;
                check(h.entry_count == if exact { 3 } else { 2 } && h.entry_bytes == 64)?;
                let state = SnapshotSection::decode(&payload[..64])?;
                let depth = SnapshotSection::decode(&payload[64..128])?;
                let coverage = if exact { Some(SnapshotSection::decode(&payload[128..192])?) } else { None };
                if let Some(c) = &coverage {
                    check(c.section_kind == 3 && c.entry_bytes == 32 && c.entry_count == 1)?;
                }
                check(
                    state.section_kind == 1
                        && depth.section_kind == 2
                        && state.entry_bytes == 512
                        && depth.entry_bytes == 16,
                )?;
                let mut offset = cursor + 256 + length;
                for section in [&state, &depth].into_iter().chain(coverage.as_ref()) {
                    check(
                        section.offset == offset
                            && section.length
                                == u64::from(section.entry_count) * u64::from(section.entry_bytes),
                    )?;
                    let part = bounded(
                        member,
                        offset.checked_sub(cursor).ok_or(Error::Bounds)?,
                        section.length,
                    )?;
                    hash_eq(part, &section.entries_hash)?;
                    if section.section_kind == 1 {
                        validate_state_records(part, &h)?;
                    } else if section.section_kind == 2 {
                        validate_levels(part, h.bid_count, h.ask_count)?;
                        if exact {
                            for bytes in part.chunks_exact(16) {
                                let level = DepthLevel::decode(bytes)?;
                                check(level.price_ticks > 0 && level.qty_lots > 0)?;
                            }
                        }
                    } else {
                        check(section.section_kind == 3 && section.entry_bytes == 32 && section.entry_count == 1)?;
                        let c = DepthCoverage::decode(part)?;
                        validate_depth_coverage(&c)?;
                        check(h.coverage_flags == if c.bid_kind == 1 && c.ask_kind == 1 { 0 } else { 3 })?;
                    }
                    offset = offset.checked_add(section.length).ok_or(Error::Bounds)?;
                }
                offset.checked_sub(cursor).ok_or(Error::Bounds)?
            }
            _ => return Err(Error::InvalidEnum),
        };
        zero(bounded(
            member,
            used,
            e.snapshot_bytes.checked_sub(used).ok_or(Error::Bounds)?,
        )?)?;
        cursor = cursor.checked_add(e.snapshot_bytes).ok_or(Error::Bounds)?;
    }
    check(cursor == raw.len() as u64 && cursor - member_start == s.manifest_member_bytes)?;
    let join = context.join;
    if join.snapshot_id != s.snapshot_id
        || join.snapshot_hash != hash(raw)
        || join.topic_instrument_set_hash != set_hash
        || join.topic_instrument_set_version != s.state_version
        || join.snapshot_seq != s.sequence
        || join.join_seq != s.sequence.checked_add(1).ok_or(Error::Bounds)?
    {
        return Err(Error::Identity);
    }
    Ok(())
}

fn validate_snapshot_header(s: &SnapshotHeader) -> Result<()> {
    schema(s.schema_major, s.schema_minor)?;
    check(
        s.magic == *b"TWSNP001" && (1..=7).contains(&s.snapshot_kind) && s.coverage_flags & !3 == 0,
    )?;
    seq(s.sequence)?;
    Ok(())
}

fn validate_state_records(raw: &[u8], snapshot: &SnapshotHeader) -> Result<()> {
    if !raw.len().is_multiple_of(512) {
        return Err(Error::Bounds);
    }
    let mut prior = 0;
    let mut feed = false;
    for (i, chunk) in raw.as_chunks::<512>().0.iter().enumerate() {
        let Record::Market(record) = decode_record(chunk)? else {
            return Err(Error::Malformed);
        };
        let h = record.header;
        check(
            matches!(h.message_type, 257 | 258 | 260 | 261)
                && h.message_type > prior
                && h.sequence == i as u64 + 1
                && h.instrument_id == snapshot.instrument_id
                && h.instrument_version == snapshot.instrument_version
                && h.feed_generation == snapshot.feed_generation,
        )?;
        prior = h.message_type;
        feed |= h.message_type == 261;
    }
    if matches!(snapshot.snapshot_kind, 6 | 7) && !feed {
        return Err(Error::Malformed);
    }
    Ok(())
}

pub fn validate_depth_coverage(c: &DepthCoverage) -> Result<()> {
    if c.version != 1 { return Err(Error::UnsupportedFeature); }
    for (kind, boundary) in [(c.bid_kind, c.bid_boundary_ticks), (c.ask_kind, c.ask_boundary_ticks)] {
        match kind {
            0 | 1 if boundary == 0 => {}
            2 if boundary > 0 => {}
            _ => return Err(Error::Malformed),
        }
    }
    Ok(())
}
