//! Cold validation of a launcher-approved, append-only market expansion.
//! Valid bytes describe an operation; they never authenticate the launcher.
use crate::{
    Result,
    config::{Config, FeedIndex},
    invalid,
    server::Admission,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_market_source::protocol::{Instrument, wire_bytes};

pub const MAX_PLAN_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_WARMUP_NS: u64 = 60_000_000_000;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Consumer {
    pub previous: Admission,
    pub next: Admission,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub schema: u16,
    pub operation_id: [u8; 16],
    pub base_revision: u64,
    pub base_config_hash: [u8; 32],
    pub base_bank: Vec<u8>,
    pub target_revision: u64,
    /// Cutover and descriptor fields are finalized only after actual QUIESCED.
    pub target_bank: Vec<u8>,
    pub target_config: Config,
    pub topic_id: u64,
    pub consumers: Vec<Consumer>,
    pub warmup_deadline_mono_ns: u64,
    pub maximum_age_ns: u64,
    pub peak_cache_bytes: usize,
    pub maximum_retained_bank_bytes: u64,
}

pub struct Prepared {
    plan: Plan,
    approval_digest: [u8; 32],
    target_config_hash: [u8; 32],
    index: FeedIndex,
    topic_index: usize,
    added_from: usize,
    bank_template: ResourceHeader,
    prepared_at: u64,
}

pub struct Finalized {
    pub bank: ResourceHeader,
    pub admissions: Vec<Admission>,
}

impl Prepared {
    pub fn plan(&self) -> &Plan {
        &self.plan
    }
    pub fn approval_digest(&self) -> [u8; 32] {
        self.approval_digest
    }
    pub fn target_config_hash(&self) -> [u8; 32] {
        self.target_config_hash
    }
    pub fn index(&self) -> &FeedIndex {
        &self.index
    }
    pub fn topic_index(&self) -> usize {
        self.topic_index
    }
    pub fn added_feeds(&self) -> &[crate::config::Feed] {
        &self.plan.target_config.feeds[self.added_from..]
    }
    pub fn bank_template(&self) -> &ResourceHeader {
        &self.bank_template
    }
    /// Bind the actual final cut only after the owner has verified QUIESCED.
    /// This constructs descriptors; it does not freeze or activate any bank.
    pub fn finalize(&self, published: u64, now: u64) -> Result<Finalized> {
        if now < self.prepared_at || now >= self.plan.warmup_deadline_mono_ns {
            return Err(invalid("market expansion cut deadline"));
        }
        let previous = ResourceHeader::decode(&self.plan.base_bank)?;
        if now < previous.created_mono_ns || published < previous.first_seq.saturating_sub(1) {
            return Err(invalid("market expansion cut/clock regression"));
        }
        let mut bank = self.bank_template.clone();
        bank.created_mono_ns = now;
        bank.cutover_seq = published;
        bank.first_seq = published
            .checked_add(1)
            .ok_or_else(|| invalid("market expansion sequence exhausted"))?;
        tickwire_shm::validate_bank(&bank)?;
        bank.descriptor_hash = [0; 32];
        bank.descriptor_hash = Sha256::digest(wire_bytes(&bank)?).into();
        let bytes = wire_bytes(&bank)?;
        let admissions = self
            .plan
            .consumers
            .iter()
            .map(|consumer| {
                let mut admission = consumer.next.clone();
                admission.client.bank_header.clone_from(&bytes);
                admission
            })
            .collect();
        Ok(Finalized { bank, admissions })
    }
}

pub fn config_hash(config: &Config) -> Result<[u8; 32]> {
    Ok(Sha256::digest(serde_json::to_vec(config)?).into())
}

impl Plan {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > MAX_PLAN_BYTES {
            return Err(invalid("market expansion plan size"));
        }
        Ok(serde_json::from_slice(bytes)?)
    }
    /// `bank` and `topic_index` must name the same runtime-owned topic;
    /// `admissions` must come from the MD server's actual current participants.
    /// `retained_bytes` includes already retired banks for this topic.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare(
        self,
        current: &Config,
        index: &FeedIndex,
        revision: u64,
        bank: &ResourceHeader,
        topic_index: usize,
        admissions: &[Admission],
        retained_bytes: u64,
        now: u64,
    ) -> Result<Prepared> {
        if self.schema != 1
            || self.operation_id == [0; 16]
            || revision == 0
            || self.base_revision != revision
            || self.base_config_hash != config_hash(current)?
            || Some(self.target_revision) != revision.checked_add(1)
            || self.base_bank != wire_bytes(bank)?
            || self.topic_id != bank.topic_id
            || bank.producer_instance != current.identity.instance
            || bank.producer_generation != current.identity.generation
            || bank.host_boot_id != current.identity.host_boot
            || bank.namespace_id != current.identity.namespace
            || bank.target_arch != current.identity.arch
            || bank.trust_profile != u16::from(current.identity.trust_profile)
            || self.warmup_deadline_mono_ns <= now
            || self.warmup_deadline_mono_ns - now > MAX_WARMUP_NS
            || self.maximum_age_ns == 0
            || self.maximum_age_ns > MAX_WARMUP_NS
            || self.target_config.feeds.len() <= current.feeds.len()
            || self.target_config.feeds.len() > 32
            || self.target_config.topics.len() != current.topics.len()
            || admissions.len() > 32
            || self.consumers.len() != admissions.len()
        {
            return Err(invalid("market expansion base/revision/deadline/bounds"));
        }
        let previous = current
            .topics
            .get(topic_index)
            .ok_or_else(|| invalid("market expansion topic"))?;
        let topic = &self.target_config.topics[topic_index];
        if topic.instrument_set_version <= previous.instrument_set_version
            || topic.instrument_set_version >= 1 << 63
            || topic.maximum_cache_bytes < previous.maximum_cache_bytes
            || topic.maximum_snapshot_bytes < previous.maximum_snapshot_bytes
            || previous
                .maximum_cache_bytes
                .checked_add(topic.maximum_cache_bytes)
                .is_none_or(|n| n > self.peak_cache_bytes)
        {
            return Err(invalid("market expansion topic version/cache budget"));
        }
        // Only the target topic's cold capacity and set version may change.
        let mut normalized = self.target_config.clone();
        normalized.feeds.truncate(current.feeds.len());
        normalized.topics[topic_index].instrument_set_version = previous.instrument_set_version;
        normalized.topics[topic_index].maximum_cache_bytes = previous.maximum_cache_bytes;
        normalized.topics[topic_index].maximum_snapshot_bytes = previous.maximum_snapshot_bytes;
        if config_hash(&normalized)? != config_hash(current)? {
            return Err(invalid(
                "market expansion changes existing runtime configuration",
            ));
        }
        let template = ResourceHeader::decode(&self.target_bank)?;
        validate_bank(bank, &template)?;
        if retained_bytes
            .checked_add(bank.resource_bytes)
            .and_then(|n| n.checked_add(template.resource_bytes))
            .is_none_or(|n| n > self.maximum_retained_bank_bytes)
        {
            return Err(invalid("market expansion retained bank budget"));
        }
        let old_members = instruments(current, self.topic_id)?;
        let members = instruments(&self.target_config, self.topic_id)?;
        let catalog = current
            .feeds
            .iter()
            .find(|f| f.topics.contains(&self.topic_id))
            .ok_or_else(|| invalid("market expansion old topic membership"))?
            .instrument
            .catalog_hash;
        let mut owners = BTreeSet::new();
        let mut ids = BTreeSet::new();
        for (position, feed) in self.target_config.feeds.iter().enumerate() {
            if feed.owner_id == [0; 16]
                || !owners.insert(feed.owner_id)
                || feed.first_connection_epoch == 0
                || feed.first_operation == 0
                || feed.initial_book_generation == 0
                || feed.events.len() > 3
            {
                return Err(invalid("market expansion duplicate/invalid source owner"));
            }
            if position >= current.feeds.len()
                && (feed.topics != [self.topic_id] || !ids.insert(feed.instrument.id))
            {
                return Err(invalid("market expansion addition membership"));
            }
            ids.insert(feed.instrument.id);
            let binding = feed.instrument.build(feed.source.build())?;
            if binding.instrument_version >= 1 << 63 {
                return Err(invalid("market expansion instrument version range"));
            }
            if feed.topics.contains(&self.topic_id)
                && (binding.catalog_version != bank.catalog_version
                    || binding.catalog_hash != catalog)
            {
                return Err(invalid("market expansion catalog identity"));
            }
            let mut kinds = BTreeSet::new();
            let mut sources = BTreeSet::new();
            for event in &feed.events {
                event.context(feed)?;
                if !owners.insert(event.owner_id)
                    || !kinds.insert(event.kind.state())
                    || !sources.insert(event.stream_id)
                {
                    return Err(invalid(
                        "market expansion duplicate event owner/kind/source",
                    ));
                }
            }
        }
        let candidate_index = index.expanded(&self.target_config.feeds)?;
        let mut keys = BTreeSet::new();
        for feed in &self.target_config.feeds {
            let key = candidate_index.book_key(feed)?;
            if !keys.insert((key.venue, key.instrument, key.source)) {
                return Err(invalid("market expansion duplicate complete book key"));
            }
        }
        self.validate_consumers(
            current,
            admissions,
            &old_members,
            &members,
            catalog,
            topic_index,
        )?;
        let encoded = serde_json::to_vec(&self)?;
        if encoded.len() > MAX_PLAN_BYTES {
            return Err(invalid("market expansion plan size"));
        }
        Ok(Prepared {
            approval_digest: Sha256::digest(encoded).into(),
            target_config_hash: config_hash(&self.target_config)?,
            index: candidate_index,
            topic_index,
            added_from: current.feeds.len(),
            bank_template: template,
            prepared_at: now,
            plan: self,
        })
    }

    fn validate_consumers(
        &self,
        current: &Config,
        admissions: &[Admission],
        old_members: &[Instrument],
        members: &[Instrument],
        catalog: [u8; 32],
        topic_index: usize,
    ) -> Result<()> {
        let mut previous_keys = BTreeSet::new();
        let mut next_requests = BTreeSet::new();
        let mut next_nonces = BTreeSet::new();
        let old_bank = ResourceHeader::decode(&self.base_bank)?.resource_id;
        let new_bank = ResourceHeader::decode(&self.target_bank)?.resource_id;
        let mut previous_resources = BTreeSet::from([old_bank]);
        for admission in admissions {
            previous_resources.insert(admission.snapshot_id);
            previous_resources.insert(admission.client.cursor_id);
        }
        if previous_resources.contains(&new_bank) {
            return Err(invalid("market expansion reused bank resource identity"));
        }
        let mut next_resources = BTreeSet::from([new_bank]);
        for entry in &self.consumers {
            let old = &entry.previous;
            let new = &entry.next;
            let before = &old.client;
            let after = &new.client;
            tickwire_control::ClientHandshake::new(after.endpoint.policy(), after.request_id)?;
            if !admissions
                .iter()
                .any(|actual| same_json(actual, old).unwrap_or(false))
                || !previous_keys.insert((old.pid, before.request_id))
                || !next_requests.insert(after.request_id)
                || !next_resources.insert(after.cursor_id)
                || !next_resources.insert(new.snapshot_id)
                || previous_resources.contains(&after.cursor_id)
                || previous_resources.contains(&new.snapshot_id)
                || !next_nonces.insert(after.endpoint.nonce)
                || admissions.iter().any(|a| {
                    a.client.request_id == after.request_id
                        || a.client.cursor_id == after.cursor_id
                        || a.snapshot_id == new.snapshot_id
                        || a.client.endpoint.nonce == after.endpoint.nonce
                })
                || old.pid == 0
                || old.pid != new.pid
                || old.uid != new.uid
                || old.gid != new.gid
                || old.supervisor_pid != new.supervisor_pid
                || !same_json(&old.supervisor, &new.supervisor)?
                || before.bank_header != self.base_bank
                || after.bank_header != self.target_bank
                || before.catalog_hash != catalog
                || after.catalog_hash != catalog
                || before.instrument_set_version
                    != current.topics[topic_index].instrument_set_version
                || after.instrument_set_version
                    != self.target_config.topics[topic_index].instrument_set_version
                || !same_json(&before.instruments, &old_members)?
                || !same_json(&after.instruments, &members)?
                || before.endpoint.path != current.socket
                || after.endpoint.path != current.socket
                || !same_json(&before.endpoint.local, &after.endpoint.local)?
                || !same_json(&before.endpoint.peer, &after.endpoint.peer)?
                || !same_json(&after.endpoint.peer, &current.identity)?
                || before.endpoint.peer_pid != after.endpoint.peer_pid
                || before.endpoint.peer_uid != after.endpoint.peer_uid
                || before.endpoint.peer_gid != after.endpoint.peer_gid
                || after.endpoint.control_epoch <= before.endpoint.control_epoch
                || after.endpoint.nonce == [0; 32]
                || after.endpoint.nonce == before.endpoint.nonce
                || after.subscription_generation <= before.subscription_generation
                || after.subscription_generation >= 1 << 63
                || after.cursor_id == [0; 16]
                || after.request_id == [0; 16]
                || new.snapshot_id == [0; 16]
                || new.snapshot_id == old.snapshot_id
                || after.clock_generation != current.clock_generation
                || after.domain()? != before.domain()?
                || after.maximum_levels != before.maximum_levels
                || after.maximum_levels < self.target_config.topics[topic_index].maximum_levels
                || after.maximum_levels > 10000
                || after.startup_timeout_ms == 0
                || after.startup_timeout_ms > 60000
            {
                return Err(invalid("market expansion consumer authorization/identity"));
            }
        }
        Ok(())
    }
}

fn same_json<A: Serialize + ?Sized, B: Serialize + ?Sized>(a: &A, b: &B) -> Result<bool> {
    Ok(serde_json::to_vec(a)? == serde_json::to_vec(b)?)
}

fn instruments(config: &Config, topic: u64) -> Result<Vec<Instrument>> {
    let mut members: Vec<_> = config
        .feeds
        .iter()
        .filter(|f| f.topics.contains(&topic))
        .map(|f| Instrument {
            id: f.instrument.id,
            version: f.instrument.version,
            required_states: f
                .events
                .iter()
                .fold(tickwire_snapshot::QUOTE, |state, e| state | e.kind.state()),
        })
        .collect();
    members.sort_by_key(|m| m.id);
    if members.is_empty() || members.windows(2).any(|p| p[0].id == p[1].id) {
        return Err(invalid("market expansion topic membership"));
    }
    Ok(members)
}

fn validate_bank(old: &ResourceHeader, next: &ResourceHeader) -> Result<()> {
    tickwire_shm::validate_bank(old)?;
    tickwire_shm::validate_bank(next)?;
    if next.resource_id == [0; 16]
        || next.resource_id == old.resource_id
        || next.ring_epoch <= old.ring_epoch
        || next.feed_generation <= old.feed_generation
        || next.predecessor_ring_epoch != old.ring_epoch
        || next.first_seq != 1
        || next.cutover_seq != 0
        || next.created_mono_ns != 0
        || next.descriptor_hash != [0; 32]
    {
        return Err(invalid("market expansion target bank generation/template"));
    }
    let mut normalized = next.clone();
    normalized.resource_id = old.resource_id;
    normalized.ring_epoch = old.ring_epoch;
    normalized.feed_generation = old.feed_generation;
    normalized.predecessor_ring_epoch = old.predecessor_ring_epoch;
    normalized.first_seq = old.first_seq;
    normalized.cutover_seq = old.cutover_seq;
    normalized.created_mono_ns = old.created_mono_ns;
    normalized.descriptor_hash = old.descriptor_hash;
    if wire_bytes(&normalized)? != wire_bytes(old)? {
        return Err(invalid("market expansion bank identity/layout changed"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
