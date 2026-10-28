//! The named original JOIN can describe only its own exact bank.
use super::*;

pub(super) fn extract(
    bytes: &[u8],
    current: &observed::MarketInput,
    now: u64,
) -> Result<Option<ObservedJoin>> {
    // Other recovery formats never acquire JOIN evidence from approval files.
    if !bytes.starts_with(b"TWSC0001") {
        return Ok(None);
    }
    if bytes.len() < 20 {
        return Err(Error::Invalid);
    }
    let n = u32::from_le_bytes(bytes[16..20].try_into().map_err(|_| Error::Invalid)?) as usize;
    let at = 20usize.checked_add(n).ok_or(Error::Invalid)?;
    let raw = bytes.get(at..).ok_or(Error::Invalid)?;
    if raw.len() < 28 || &raw[..8] != b"TWSB0001" {
        return Err(Error::Invalid);
    }
    let when = u64::from_le_bytes(raw[8..16].try_into().map_err(|_| Error::Invalid)?);
    let levels = usize::try_from(u64::from_le_bytes(
        raw[16..24].try_into().map_err(|_| Error::Invalid)?,
    ))
    .map_err(|_| Error::Invalid)?;
    let n = u32::from_le_bytes(raw[24..28].try_into().map_err(|_| Error::Invalid)?) as usize;
    let end = 28usize.checked_add(n).ok_or(Error::Invalid)?;
    let encoded = raw.get(28..end).ok_or(Error::Invalid)?;
    let bootstrap: tickwire_market_source::protocol::JoinBootstrap =
        serde_json::from_slice(encoded).map_err(|_| Error::Invalid)?;
    if serde_json::to_vec(&bootstrap).map_err(|_| Error::Invalid)? != encoded
        || when > now
        || levels == 0
        || levels > 10000
    {
        return Err(Error::Invalid);
    }
    let (bank, resource, join, registry) = bootstrap.decode().map_err(|_| Error::Invalid)?;
    // A current market may have legitimately transitioned after this baseline.
    // Its old JOIN is historical, never a substitute for the current input scope.
    if &bank != current.bank() {
        return Ok(None);
    }
    if bank.resource_kind != 1
        || bank.producer_instance == [0; 16]
        || join.bank_resource_id != bank.resource_id
        || join.channel_id != bank.channel_id
        || join.channel_epoch != bank.channel_epoch
        || join.ring_epoch != bank.ring_epoch
        || join.feed_generation != bank.feed_generation
        || join.topic_id != bank.topic_id
        || join.expected_catalog_version != bank.catalog_version
        || join.qos != bank.qos
        || join.topic_instrument_set_version != registry.instrument_set_version
        || join.topic_instrument_set_hash
            != tickwire_join::instrument_set_hash(&registry.instruments)
        || resource.producer_instance != bank.producer_instance
        || resource.producer_generation != bank.producer_generation
        || resource.host_boot_id != bank.host_boot_id
        || resource.namespace_id != bank.namespace_id
        || resource.catalog_version != bank.catalog_version
        || registry.catalog_hash == [0; 32]
        || registry.instrument_set_version == 0
        || registry.instrument_set_version >= 1 << 63
        || join.snapshot_seq > current.cursor().sequence
    {
        return Err(Error::Invalid);
    }
    let snapshot = raw.get(end..).ok_or(Error::Invalid)?;
    if Object::of(snapshot).digest != join.snapshot_hash {
        return Err(Error::Invalid);
    }
    let topic = tickwire_snapshot::validate(
        snapshot,
        &tickwire_snapshot::JoinContext {
            expected_resource: &resource,
            catalog_hash: registry.catalog_hash,
            instruments: &registry.instruments,
            join: &join,
            now_mono_ns: when,
        },
    )
    .map_err(|_| Error::Invalid)?;
    topic.check_readiness(when).map_err(|_| Error::Invalid)?;
    let replica =
        tickwire_strategy_sdk::MarketReplica::load(join.topic_id, join.ring_epoch, &topic, levels)
            .map_err(|_| Error::Invalid)?;
    let actual = replica
        .instruments()
        .map(|id| {
            Ok((
                id,
                replica
                    .view(id)
                    .ok_or(Error::Invalid)?
                    .trigger()
                    .instrument_version,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    if actual != current.instruments() {
        return Err(Error::Invalid);
    }
    // Canonical registry order is part of the saved, validated snapshot binding.
    let instruments = registry
        .instruments
        .iter()
        .map(|i| (i.instrument_id, i.instrument_version, i.required_states))
        .collect();
    Ok(Some(ObservedJoin {
        catalog_hash: registry.catalog_hash,
        instrument_set_version: registry.instrument_set_version,
        instruments,
        snapshot: Object::of(snapshot),
        baseline_sequence: join.snapshot_seq,
    }))
}
