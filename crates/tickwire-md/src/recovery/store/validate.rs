use super::*;
pub(super) fn snapshot(s: &Snapshot) -> Result<()> {
    if s.schema != 1
        || s.sequence == 0
        || s.sequence >= 1 << 63
        || s.revision == 0
        || s.revision >= 1 << 63
        || s.config.topics.is_empty()
        || s.config.topics.len() > 32
        || s.banks.len() != s.config.topics.len()
        || s.operations.len() > 128
        || s.reserved_owners.len() > 16384
        || s.config_hash != digest(&encode(&s.config, MAX_SNAPSHOT_BYTES)?)
    {
        return Err(invalid("MD recovery snapshot bounds/config"));
    }
    crate::config::FeedIndex::restore(&s.config.feeds, &s.instrument_ids)?;
    let mut operations = BTreeSet::new();
    for operation in &s.operations {
        if operation.operation_id == [0; 16]
            || operation.approval_digest == [0; 32]
            || !operations.insert(operation.operation_id)
        {
            return Err(invalid("MD recovery consumed operations"));
        }
    }
    let mut topics = BTreeSet::new();
    let mut resources = BTreeSet::new();
    let mut paths = BTreeSet::new();
    for (bytes, topic) in s.banks.iter().zip(&s.config.topics) {
        if bytes.len() != 4096
            || !paths.insert(&topic.resource)
            || topic.instrument_set_version == 0
            || topic.instrument_set_version >= 1 << 63
        {
            return Err(invalid("MD recovery bank/topic bounds"));
        }
        let bank = ResourceHeader::decode(bytes)?;
        tickwire_shm::validate_bank(&bank)?;
        if bank.descriptor_hash != [0; 32] {
            let mut normalized = bank.clone();
            normalized.descriptor_hash = [0; 32];
            let mut wire = [0; ResourceHeader::SIZE];
            normalized.encode(&mut wire)?;
            if digest(&wire) != bank.descriptor_hash {
                return Err(invalid("MD recovery descriptor digest"));
            }
        }
        if bank.resource_kind != 1
            || !topics.insert(bank.topic_id)
            || !resources.insert(bank.resource_id)
            || bank.producer_instance != s.config.identity.instance
            || bank.producer_generation != s.config.identity.generation
            || bank.host_boot_id != s.config.identity.host_boot
            || bank.namespace_id != s.config.identity.namespace
            || bank.target_arch != s.config.identity.arch
            || bank.trust_profile != u16::from(s.config.identity.trust_profile)
        {
            return Err(invalid("MD recovery bank identity"));
        }
        let mut ids = BTreeSet::new();
        let mut catalog = None;
        for feed in s
            .config
            .feeds
            .iter()
            .filter(|f| f.topics.contains(&bank.topic_id))
        {
            let instrument = feed.instrument.build(feed.source.build())?;
            if !ids.insert(instrument.instrument_id)
                || bank.catalog_version != instrument.catalog_version
                || catalog.is_some_and(|hash| hash != instrument.catalog_hash)
            {
                return Err(invalid("MD recovery bank catalog"));
            }
            catalog = Some(instrument.catalog_hash);
        }
        if ids.is_empty() {
            return Err(invalid("MD recovery empty bank membership"));
        }
    }
    let reserved: BTreeSet<_> = s.reserved_owners.iter().copied().collect();
    if reserved.len() != s.reserved_owners.len() || reserved.contains(&[0; 16]) {
        return Err(invalid("MD recovery reserved source owners"));
    }
    let mut owners = BTreeSet::new();
    for feed in &s.config.feeds {
        if feed.owner_id == [0; 16]
            || !owners.insert(feed.owner_id)
            || !reserved.contains(&feed.owner_id)
            || feed.topics.is_empty()
            || feed.topics.len() > 32
            || feed.events.len() > 3
            || feed.topics.iter().any(|id| !topics.contains(id))
            || feed.topics.iter().collect::<BTreeSet<_>>().len() != feed.topics.len()
        {
            return Err(invalid("MD recovery feed owner/topics"));
        }
        for event in &feed.events {
            if event.owner_id == [0; 16]
                || !owners.insert(event.owner_id)
                || !reserved.contains(&event.owner_id)
            {
                return Err(invalid("MD recovery event owner"));
            }
        }
    }
    Ok(())
}

pub(super) struct Link {
    phase: Phase,
    revision: u64,
    producer: ([u8; 16], u64, [u8; 16]),
    instruments: Vec<u64>,
    operations: BTreeMap<[u8; 16], Hash>,
    reserved_owners: BTreeSet<[u8; 16]>,
    banks: BTreeMap<u64, ResourceHeader>,
}
impl Link {
    pub(super) fn new(snapshot: &Snapshot) -> Result<Self> {
        let identity = &snapshot.config.identity;
        Ok(Self {
            phase: snapshot.phase,
            revision: snapshot.revision,
            producer: (identity.instance, identity.generation, identity.host_boot),
            instruments: snapshot.instrument_ids.clone(),
            operations: snapshot
                .operations
                .iter()
                .map(|op| (op.operation_id, op.approval_digest))
                .collect(),
            reserved_owners: snapshot.reserved_owners.iter().copied().collect(),
            banks: snapshot
                .banks
                .iter()
                .map(|bytes| {
                    let bank = ResourceHeader::decode(bytes)?;
                    Ok((bank.topic_id, bank))
                })
                .collect::<Result<_>>()?,
        })
    }
}
pub(super) fn continuation(previous: &Link, next: &Link) -> Result<()> {
    if next.revision < previous.revision
        || !next.instruments.starts_with(&previous.instruments)
        || !previous.reserved_owners.is_subset(&next.reserved_owners)
        || previous
            .operations
            .iter()
            .any(|(id, digest)| next.operations.get(id) != Some(digest))
    {
        return Err(invalid("MD recovery historical facts changed"));
    }
    if next.producer != previous.producer {
        if previous.phase != Phase::Ready
            || next.phase != Phase::Starting
            || next.revision != previous.revision
        {
            return Err(invalid("MD recovery incarnation transition"));
        }
    } else {
        for (topic, before) in &previous.banks {
            let bank = next
                .banks
                .get(topic)
                .ok_or_else(|| invalid("MD recovery bank removed"))?;
            if bank.ring_epoch < before.ring_epoch
                || bank.feed_generation < before.feed_generation
                || (bank.ring_epoch == before.ring_epoch && bank != before)
            {
                return Err(invalid("MD recovery bank frontier changed"));
            }
        }
    }
    Ok(())
}
