use super::*;
use sha2::{Digest, Sha256};
use tickwire_ipc::{ResourceHeader, SnapshotHeader, Wire};

/// An immutable resource envelope matched to authenticated expected metadata.
/// No JOIN readiness or snapshot business-validity capability is granted.
pub struct ImmutableResource {
    mapping: ImmutableMapping,
    header: ResourceHeader,
    content_bytes: usize,
}
impl ImmutableResource {
    pub fn header(&self) -> &ResourceHeader {
        &self.header
    }
    pub fn bytes(&self) -> &[u8] {
        self.mapping.bytes()
    }
    pub fn content(&self) -> &[u8] {
        &self.mapping.bytes()[PAGE_BYTES..PAGE_BYTES + self.content_bytes]
    }
    pub fn duplicate_fd(&self) -> Result<OwnedFd> {
        self.mapping.duplicate_fd()
    }
}

fn require(condition: bool, reason: &'static str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(Error::InvalidLayout(reason))
    }
}
fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
pub(super) fn validate(
    mapping: ImmutableMapping,
    expected: &ResourceHeader,
    content_bytes: usize,
    object_hash: [u8; 32],
) -> Result<ImmutableResource> {
    let len = mapping.len();
    require(len <= 64 * 1024 * 1024, "immutable resource exceeds 64 MiB")?;
    let content_end = PAGE_BYTES
        .checked_add(content_bytes)
        .ok_or(Error::OutOfBounds)?;
    let padded_end = content_end
        .checked_add(PAGE_BYTES - 1)
        .ok_or(Error::OutOfBounds)?
        & !(PAGE_BYTES - 1);
    require(
        content_bytes > 0 && padded_end == len,
        "resource content range",
    )?;
    let raw = mapping.bytes();
    let header = ResourceHeader::decode(&raw[..PAGE_BYTES]).map_err(Error::Wire)?;
    if header != *expected {
        return Err(Error::Identity);
    }
    let mut header_hash = Sha256::new();
    header_hash.update(&raw[..272]);
    header_hash.update([0; 32]);
    header_hash.update(&raw[304..PAGE_BYTES]);
    let header_hash: [u8; 32] = header_hash.finalize().into();
    if header_hash != header.descriptor_hash {
        return Err(Error::Checksum);
    }
    let arch = if cfg!(target_arch = "x86_64") {
        1
    } else if cfg!(target_arch = "aarch64") {
        2
    } else {
        0
    };
    require(
        header.magic == *b"TWIPC001"
            && (header.abi_major, header.abi_minor, header.header_bytes) == (1, 0, 4096)
            && (header.schema_major, header.schema_minor) == (1, 0)
            && arch != 0
            && header.target_arch == arch
            && (
                header.endian,
                header.atomic_width,
                header.page_shift,
                header.isolation_shift,
            ) == (1, 8, 12, 7)
            && header.clock_domain == 1
            && (1..=2).contains(&header.trust_profile),
        "immutable resource ABI profile",
    )?;
    require(
        header.resource_id != [0; 16]
            && header.producer_instance != [0; 16]
            && header.host_boot_id != [0; 16]
            && header.namespace_id != [0; 16]
            && (1..1 << 63).contains(&header.producer_generation),
        "immutable resource identity",
    )?;
    require(
        [
            header.channel_epoch,
            header.ring_epoch,
            header.feed_generation,
            header.producer_generation,
            header.topic_id,
            header.risk_scope,
            header.catalog_version,
            header.first_seq,
            header.predecessor_ring_epoch,
            header.cutover_seq,
            header.initial_membership_version,
        ]
        .iter()
        .all(|&value| value < 1 << 63),
        "immutable resource identity counter exhausted",
    )?;
    require(
        header.required_features & 0x1c == 0x1c
            && header.required_features & !0x2ff == 0
            && header.optional_features & !0x2ff == 0
            && ((header.required_features | header.optional_features) & 512 == 0
                || matches!(header.resource_kind, 5 | 8)),
        "immutable resource features",
    )?;
    require(
        (5..=9).contains(&header.resource_kind)
            && header.resource_bytes == len as u64
            && header.slot_offset == PAGE_BYTES as u64
            && header.producer_page_offset == 0
            && header.cursor_page_offset == 0
            && header.arena_offset == 0
            && header.arena_bytes == 0
            && header.arena_class_count == 0
            && header
                .class_descriptors
                .iter()
                .all(|class| *class == Default::default()),
        "immutable resource regions",
    )?;
    if header.resource_kind == 9 {
        require(
            header.required_features & 0x20 != 0
                && header.consumer_instance != [0; 16]
                && content_bytes <= 4 * 1024 * 1024
                && header.capacity as usize == content_bytes
                && header.record_bytes == 1,
            "configuration projection range",
        )?;
    } else {
        require(
            content_bytes >= SnapshotHeader::SIZE,
            "snapshot header range",
        )?;
        let snapshot = SnapshotHeader::decode(&raw[PAGE_BYTES..PAGE_BYTES + SnapshotHeader::SIZE])
            .map_err(Error::Wire)?;
        require(
            snapshot.magic == *b"TWSNP001"
                && (snapshot.schema_major, snapshot.schema_minor) == (1, 0)
                && snapshot.object_bytes == len as u64
                && snapshot.entry_offset == 4352
                && snapshot.sequence < 1 << 63
                && snapshot.coverage_flags & !3 == 0,
            "snapshot envelope",
        )?;
        tickwire_ipc::validate_resource_shape(
            header.resource_kind,
            header.capacity,
            header.record_bytes,
            header.resource_bytes,
            Some(&snapshot),
        )
        .map_err(Error::Wire)?;
        let entries_bytes = u64::from(snapshot.entry_count) * u64::from(snapshot.entry_bytes);
        let entries_end = u64::from(snapshot.entry_offset)
            .checked_add(entries_bytes)
            .ok_or(Error::OutOfBounds)?;
        require(entries_end <= content_end as u64, "snapshot entries range")?;
        // Sections, embedded member objects, their internal zero padding and
        // business identities are checked by the relevant snapshot validator.
    }
    require(
        raw[content_end..].iter().all(|&byte| byte == 0),
        "nonzero resource page tail",
    )?;
    if hash(raw) != object_hash {
        return Err(Error::Checksum);
    }
    Ok(ImmutableResource {
        mapping,
        header,
        content_bytes,
    })
}
