//! Deterministic reconstruction of a recorded JOIN, without a live connection
//! or user callback. The authenticated runtime supplies the original context.
use super::*;

pub(super) fn replacement(old: &Market, input: &Input, now: u64) -> Result<Market> {
    let Input::MarketTransition {
        previous,
        next,
        baseline_sequence,
        activated_sequence,
        baseline,
        catchup,
        ..
    } = input
    else {
        return Err(Error::Identity);
    };
    if previous.stream != market_stream(&old.resource)?
        || previous.sequence != old.replica.sequence()
        || baseline.len() < 28
        || &baseline[..8] != b"TWSB0001"
    {
        return Err(Error::Identity);
    }
    let at = u64::from_le_bytes(baseline[8..16].try_into().unwrap());
    let levels = usize::try_from(u64::from_le_bytes(baseline[16..24].try_into().unwrap()))
        .map_err(|_| Error::Capacity)?;
    let len = u32::from_le_bytes(baseline[24..28].try_into().unwrap()) as usize;
    let boundary = 28usize.checked_add(len).ok_or(Error::Capacity)?;
    let bootstrap: crate::bootstrap::JoinBootstrap =
        serde_json::from_slice(baseline.get(28..boundary).ok_or(Error::Identity)?)
            .map_err(|_| Error::Identity)?;
    let (bank, resource, join, registry) = bootstrap.decode().map_err(|_| Error::Identity)?;
    let old_bank = &old.resource;
    if at > now
        || levels != old.maximum_levels
        || *next != market_stream(&bank)?
        || bank.resource_kind != 1
        || bank.resource_id == old_bank.resource_id
        || bank.channel_id != old_bank.channel_id
        || bank.channel_epoch != old_bank.channel_epoch
        || bank.topic_id != old_bank.topic_id
        || bank.catalog_version != old_bank.catalog_version
        || bank.producer_instance != old_bank.producer_instance
        || bank.producer_generation != old_bank.producer_generation
        || bank.host_boot_id != old_bank.host_boot_id
        || bank.namespace_id != old_bank.namespace_id
        || bank.feed_generation <= old_bank.feed_generation
        || bank.ring_epoch <= old_bank.ring_epoch
        || bank.predecessor_ring_epoch != old_bank.ring_epoch
        || bank.cutover_seq < previous.sequence
        || bank.cutover_seq.checked_add(1) != Some(bank.first_seq)
        || *baseline_sequence < bank.cutover_seq
        || join.snapshot_seq != *baseline_sequence
        || join.bank_resource_id != bank.resource_id
        || join.channel_id != bank.channel_id
        || join.channel_epoch != bank.channel_epoch
        || join.ring_epoch != bank.ring_epoch
        || join.feed_generation != bank.feed_generation
        || join.topic_id != bank.topic_id
        || join.expected_catalog_version != bank.catalog_version
        || join.qos != bank.qos
        || join.join_flags != 3
        || join.topic_instrument_set_version != registry.instrument_set_version
        || join.topic_instrument_set_hash
            != tickwire_join::instrument_set_hash(&registry.instruments)
        || resource.producer_instance != bank.producer_instance
        || resource.producer_generation != bank.producer_generation
        || resource.host_boot_id != bank.host_boot_id
        || resource.namespace_id != bank.namespace_id
    {
        return Err(Error::Identity);
    }
    let snapshot = baseline.get(boundary..).ok_or(Error::Identity)?;
    if tickwire_recovery::digest(snapshot) != join.snapshot_hash {
        return Err(Error::Identity);
    }
    let topic = tickwire_snapshot::validate(
        snapshot,
        &tickwire_snapshot::JoinContext {
            expected_resource: &resource,
            catalog_hash: registry.catalog_hash,
            instruments: &registry.instruments,
            join: &join,
            now_mono_ns: at,
        },
    )
    .map_err(|_| Error::Identity)?;
    topic.check_readiness(at).map_err(|_| Error::Identity)?;
    let mut replica = crate::MarketReplica::load(join.topic_id, join.ring_epoch, &topic, levels)
        .map_err(|_| Error::Identity)?;
    for id in old.replica.instruments() {
        if replica.view(id).is_none_or(|v| {
            v.trigger().instrument_version
                != old.replica.view(id).unwrap().trigger().instrument_version
        }) {
            return Err(Error::Identity);
        }
    }
    let mut last = at;
    for row in catchup {
        let header = RecordHeader::decode(row.record.get(..128).ok_or(Error::Identity)?)
            .map_err(|_| Error::Identity)?;
        if row.applied_mono_ns < last
            || row.applied_mono_ns > now
            || header.ingress_mono_ns > row.applied_mono_ns
            || header.instrument_id != row.instrument
        {
            return Err(Error::Identity);
        }
        let id = replica
            .apply(&row.record, &row.payload)
            .map_err(|_| Error::Identity)?;
        if id != row.instrument {
            return Err(Error::Identity);
        }
        last = row.applied_mono_ns;
    }
    if replica.sequence() != *activated_sequence {
        return Err(Error::Identity);
    }
    market(bank, replica, levels)
}

pub(super) fn replace(
    markets: &mut BTreeMap<StreamKey, Market>,
    input: &Input,
    now: u64,
) -> Result<()> {
    let Input::MarketTransition { previous, next, .. } = input else {
        return Err(Error::Identity);
    };
    if markets.contains_key(next) {
        return Err(Error::Identity);
    }
    let replacement = replacement(
        markets.get(&previous.stream).ok_or(Error::Identity)?,
        input,
        now,
    )?;
    markets.remove(&previous.stream);
    markets.insert(next.clone(), replacement);
    Ok(())
}
