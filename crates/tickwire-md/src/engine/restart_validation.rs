//! Content checks for an explicitly approved new producer, never launch authority.
use crate::{
    Result, config,
    expansion::config_hash,
    invalid,
    recovery::{Phase, Snapshot},
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    path::{Component, Path},
};
use tickwire_ipc::{ResourceHeader, Wire};

pub(super) fn validate(
    previous: &Snapshot,
    next: &config::Config,
    banks: &[ResourceHeader],
) -> Result<()> {
    let old = &previous.config;
    let before = old
        .expansion
        .as_ref()
        .ok_or_else(|| invalid("MD restart old control"))?;
    let after = next
        .expansion
        .as_ref()
        .ok_or_else(|| invalid("MD restart new control"))?;
    if previous.phase != Phase::Ready
        || previous.config_hash != config_hash(old)?
        || after.initial_revision != previous.revision
        || previous.revision == 0
        || old.feeds.len() != next.feeds.len()
        || old.topics.len() != next.topics.len()
        || old.topics.len() != banks.len()
        || banks.len() != previous.banks.len()
        || banks.is_empty()
        || banks.len() > 32
        || next.identity.instance == [0; 16]
        || next.identity.instance == old.identity.instance
        || next.identity.generation <= old.identity.generation
        || next.identity.host_boot == [0; 16]
        || next.clock_generation == 0
        || (next.identity.host_boot == old.identity.host_boot
            && next.clock_generation < old.clock_generation)
        || next.quota.consumer_instance != next.identity.instance
        || next.quota.consumer_generation != next.identity.generation
    {
        return Err(invalid("MD restart phase/revision/producer/clock/quota"));
    }
    let before_recovery = before
        .recovery
        .as_ref()
        .ok_or_else(|| invalid("MD restart old history"))?;
    let after_recovery = after
        .recovery
        .as_ref()
        .ok_or_else(|| invalid("MD restart new history"))?;
    if before_recovery.directory != after_recovery.directory {
        return Err(invalid("MD restart history directory"));
    }
    paths(old, next, &before_recovery.directory)?;
    source_owners(old, next)?;
    validate_banks(previous, next, banks)?;

    // Only these launch-specific fields may differ. Keeping the rest intact
    // makes newly added Config fields immutable until explicitly reviewed here.
    let mut normalized = next.clone();
    normalized.identity.instance = old.identity.instance;
    normalized.identity.generation = old.identity.generation;
    normalized.identity.host_boot = old.identity.host_boot;
    normalized.clock_generation = old.clock_generation;
    normalized.socket.clone_from(&old.socket);
    normalized
        .admission_directory
        .clone_from(&old.admission_directory);
    normalized
        .control_directory
        .clone_from(&old.control_directory);
    // The trusted launcher may renew grants; the unchanged durable quota owner
    // still checks its epochs, request IDs and all outstanding old reservations.
    normalized.quota.clone_from(&old.quota);
    for (feed, old_feed) in normalized.feeds.iter_mut().zip(&old.feeds) {
        feed.owner_id = old_feed.owner_id;
        feed.first_connection_epoch = old_feed.first_connection_epoch;
        feed.first_operation = old_feed.first_operation;
        feed.initial_book_generation = old_feed.initial_book_generation;
        for (event, old_event) in feed.events.iter_mut().zip(&old_feed.events) {
            event.owner_id = old_event.owner_id;
            event.first_connection_epoch = old_event.first_connection_epoch;
            event.first_operation = old_event.first_operation;
        }
    }
    for (topic, old_topic) in normalized.topics.iter_mut().zip(&old.topics) {
        topic.resource.clone_from(&old_topic.resource);
    }
    let control = normalized
        .expansion
        .as_mut()
        .ok_or_else(|| invalid("MD restart control"))?;
    control.plan_path.clone_from(&before.plan_path);
    control.result_path.clone_from(&before.result_path);
    control.effective_path.clone_from(&before.effective_path);
    control
        .consumer_directory
        .clone_from(&before.consumer_directory);
    control.initial_revision = before.initial_revision;
    control
        .recovery
        .as_mut()
        .ok_or_else(|| invalid("MD restart history"))?
        .restart_from = before_recovery.restart_from;
    if config_hash(&normalized)? != previous.config_hash {
        return Err(invalid("MD restart immutable configuration"));
    }
    Ok(())
}

fn source_owners(old: &config::Config, next: &config::Config) -> Result<()> {
    let old_ids: BTreeSet<_> = old
        .feeds
        .iter()
        .flat_map(|feed| {
            std::iter::once(feed.owner_id).chain(feed.events.iter().map(|event| event.owner_id))
        })
        .collect();
    let mut new_ids = BTreeSet::new();
    for feed in &next.feeds {
        for id in
            std::iter::once(feed.owner_id).chain(feed.events.iter().map(|event| event.owner_id))
        {
            if id == [0; 16] || old_ids.contains(&id) || !new_ids.insert(id) {
                return Err(invalid("MD restart source owner reuse"));
            }
        }
    }
    Ok(())
}

fn validate_banks(
    previous: &Snapshot,
    next: &config::Config,
    banks: &[ResourceHeader],
) -> Result<()> {
    let old: Vec<_> = previous
        .banks
        .iter()
        .map(|bytes| ResourceHeader::decode(bytes))
        .collect::<std::result::Result<_, _>>()?;
    let old_resources: BTreeSet<_> = old.iter().map(|bank| bank.resource_id).collect();
    let old_channels: BTreeSet<_> = old.iter().map(|bank| bank.channel_id).collect();
    let mut resources = BTreeSet::new();
    let mut channels = BTreeSet::new();
    let mut topics = BTreeSet::new();
    for (bank, before) in banks.iter().zip(&old) {
        tickwire_shm::validate_bank(bank)?;
        tickwire_shm::validate_bank(before)?;
        if !resources.insert(bank.resource_id)
            || !channels.insert(bank.channel_id)
            || !topics.insert(bank.topic_id)
            || old_resources.contains(&bank.resource_id)
            || old_channels.contains(&bank.channel_id)
            || bank.ring_epoch <= before.ring_epoch
            || bank.feed_generation <= before.feed_generation
            || bank.producer_instance != next.identity.instance
            || bank.producer_generation != next.identity.generation
            || bank.host_boot_id != next.identity.host_boot
            || bank.namespace_id != next.identity.namespace
            || bank.target_arch != next.identity.arch
            || bank.trust_profile != u16::from(next.identity.trust_profile)
            || bank.first_seq != 1
            || bank.cutover_seq != 0
            || bank.predecessor_ring_epoch != 0
        {
            return Err(invalid("MD restart bank identity/frontier"));
        }
        descriptor(bank)?;
        descriptor(before)?;
        let mut normalized = bank.clone();
        normalized.resource_id = before.resource_id;
        normalized.channel_id = before.channel_id;
        normalized.channel_epoch = before.channel_epoch;
        normalized.ring_epoch = before.ring_epoch;
        normalized.feed_generation = before.feed_generation;
        normalized.producer_instance = before.producer_instance;
        normalized.producer_generation = before.producer_generation;
        normalized.host_boot_id = before.host_boot_id;
        normalized.first_seq = before.first_seq;
        normalized.cutover_seq = before.cutover_seq;
        normalized.predecessor_ring_epoch = before.predecessor_ring_epoch;
        normalized.initial_membership_version = before.initial_membership_version;
        normalized.created_mono_ns = before.created_mono_ns;
        normalized.descriptor_hash = before.descriptor_hash;
        if normalized != *before {
            return Err(invalid("MD restart bank shape/stream"));
        }
    }
    Ok(())
}

fn descriptor(bank: &ResourceHeader) -> Result<()> {
    // Launcher templates use zero; actual persisted descriptors carry SHA-256.
    if bank.descriptor_hash != [0; 32] {
        let mut header = bank.clone();
        header.descriptor_hash = [0; 32];
        let mut bytes = [0; ResourceHeader::SIZE];
        header.encode(&mut bytes)?;
        if bank.descriptor_hash != <[u8; 32]>::from(Sha256::digest(bytes)) {
            return Err(invalid("MD restart descriptor hash"));
        }
    }
    Ok(())
}

fn launch_paths(config: &config::Config) -> Result<Vec<(&Path, bool)>> {
    let expansion = config
        .expansion
        .as_ref()
        .ok_or_else(|| invalid("MD restart control"))?;
    let mut paths = vec![
        (config.socket.as_path(), false),
        (config.admission_directory.as_path(), true),
        (config.control_directory.as_path(), true),
        (expansion.plan_path.as_path(), false),
        (expansion.result_path.as_path(), false),
        (expansion.effective_path.as_path(), false),
        (expansion.consumer_directory.as_path(), true),
    ];
    paths.extend(
        config
            .topics
            .iter()
            .map(|topic| (topic.resource.as_path(), false)),
    );
    Ok(paths)
}

fn paths(old: &config::Config, next: &config::Config, history: &Path) -> Result<()> {
    let old_paths = launch_paths(old)?;
    let next_paths = launch_paths(next)?;
    let mut unique = BTreeSet::new();
    for &(path, directory) in &next_paths {
        if !path.is_absolute()
            || path.file_name().is_none()
            || path
                .components()
                .any(|part| !matches!(part, Component::RootDir | Component::Normal(_)))
            || !unique.insert(path)
            || path.starts_with(history)
            || path.starts_with(&old.quota_directory)
            || (directory && (history.starts_with(path) || old.quota_directory.starts_with(path)))
            || old_paths.iter().any(|&(previous, old_directory)| {
                path == previous
                    || (old_directory && path.starts_with(previous))
                    || (directory && previous.starts_with(path))
            })
        {
            return Err(invalid("MD restart launch paths overlap"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
