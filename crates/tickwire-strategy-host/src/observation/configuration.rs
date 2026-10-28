//! Configuration at the authenticated current cut, never an arbitrary local file.
use super::*;
use tickwire_release::{
    Object,
    replay::{Cut, Source, VersionedBytes},
};

/// Content observed in the same current checkpoint as a fresh worker response.
/// Construction is private; this value grants no configuration or trading authority.
#[derive(Debug)]
pub struct ObservedConfiguration {
    source: Source,
    cut: Cut,
    checkpoint: Object,
    value: VersionedBytes,
}
impl ObservedConfiguration {
    pub fn revision(&self) -> u64 {
        self.value.revision
    }
    pub fn object(&self) -> Object {
        Object::of(&self.value.bytes)
    }
    pub fn bytes(&self) -> &[u8] {
        &self.value.bytes
    }
    pub fn checkpoint(&self) -> Object {
        self.checkpoint
    }
    pub fn cut(&self) -> &Cut {
        &self.cut
    }
    pub fn source(&self) -> &Source {
        &self.source
    }
}

#[cfg(test)]
fn extract(
    ready: &HandoverReadyBody,
    objects: &[Vec<u8>],
    identity: &Identity,
    image: [u8; 32],
    domain: tickwire_clock::ClockDomain,
) -> Result<Option<ObservedConfiguration>> {
    current(ready, objects, identity, image, domain)?
        .map(|c| configuration(&c.checkpoint))
        .transpose()
}

pub(crate) struct Current {
    pub checkpoint: tickwire_release::replay::Checkpoint,
    pub seal: tickwire_recovery::HandoverSeal,
}
pub(crate) fn configuration(
    current: &tickwire_release::replay::Checkpoint,
) -> Result<ObservedConfiguration> {
    Ok(ObservedConfiguration {
        source: current.source.clone(),
        cut: current.cut.clone(),
        checkpoint: Object::of(&current.bytes()?),
        value: current.runtime.config.clone(),
    })
}
pub(crate) fn current(
    ready: &HandoverReadyBody,
    objects: &[Vec<u8>],
    identity: &Identity,
    image: [u8; 32],
    domain: tickwire_clock::ClockDomain,
) -> Result<Option<Current>> {
    let seal = tickwire_recovery::HandoverSeal::decode(find(
        objects,
        Object {
            digest: ready.input_seal_digest,
            bytes: ready.input_seal_bytes,
        },
    )?)?;
    if seal.instance != ready.instance_id
        || seal.generation != ready.strategy_generation
        || seal.logical_strategy != ready.logical_strategy_id
        || seal.report_position != ready.exec_report_sequence
        || seal.ledger_sequence != ready.ledger_sequence
        || seal.orders_digest != ready.orders_digest
        || seal.checkpoint_digest != ready.checkpoint_digest
        || seal.checkpoint_bytes != ready.checkpoint_bytes
    {
        return Err(Error::Invalid);
    }
    let raw = find(
        objects,
        Object {
            digest: seal.checkpoint_digest,
            bytes: seal.checkpoint_bytes,
        },
    )?;
    seal.verify_checkpoint(raw)?;
    let inputs = find(
        objects,
        Object {
            digest: seal.input_digest,
            bytes: seal.input_bytes,
        },
    )?;
    if !has_merged(inputs)? {
        return Ok(None);
    }
    let retained = tickwire_release::replay::retained::decode(inputs)?;
    let current = retained.current();
    let source = &current.source;
    // Source.clock names the approved logical clock domain used by this query.
    // Kernel boot UUID is separately sampled before/after and challenge-bound;
    // the existing launcher does not equate these two namespaces.
    if source.instance != seal.instance
        || source.generation != seal.generation
        || source.logical_strategy != seal.logical_strategy
        || source.instance != identity.instance
        || source.generation != identity.generation
        || source.logical_strategy != identity.logical_strategy
        || source.binary_digest != image
        || identity.build_hash != image
        || source.clock.boot_id != identity.host_boot
        || source.clock.boot_id != domain.boot_id()
        || source.clock.generation != domain.generation()
    {
        return Err(Error::Invalid);
    }
    let business = tickwire_strategy_sdk::replay::application::business_bytes(&current.business)
        .map_err(|_| Error::Invalid)?;
    if raw.get(96..) != Some(business.as_slice()) {
        return Err(Error::Invalid);
    }
    Ok(Some(Current {
        checkpoint: current.clone(),
        seal,
    }))
}

pub(super) fn find(objects: &[Vec<u8>], object: Object) -> Result<&[u8]> {
    object.validate(tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
    objects
        .iter()
        .find(|bytes| bytes.len() as u64 == object.bytes && Object::of(bytes) == object)
        .map(Vec::as_slice)
        .ok_or(Error::Invalid)
}

/// Only a closure with no merged rows is legacy. A partial or malformed merged
/// history must reach the retained verifier and fail, never downgrade to None.
fn has_merged(bytes: &[u8]) -> Result<bool> {
    let inputs = tickwire_recovery::EncodedInputs::decode(bytes.to_vec())?;
    let mut position = 68;
    while position < inputs.bytes.len() {
        let row = &inputs.bytes[position..position + 13];
        if row[8] == 5 {
            return Ok(true);
        }
        let length =
            u32::from_be_bytes(row[9..13].try_into().map_err(|_| Error::Invalid)?) as usize;
        position += 13 + length;
    }
    Ok(false)
}

#[cfg(test)]
mod tests;
