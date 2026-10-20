//! Bounded persistent completion graph. This validates data and prior callbacks;
//! live process ownership is still checked by the Host before publication.
use crate::*;
use receipt::{Completion, Receipt};
use std::collections::BTreeMap;
pub struct Verified {
    pub completion: Completion,
    pub restored: Receipt,
    pub rollback: Receipt,
    pub bundle: ReleaseBundle,
    pub objects: Vec<Vec<u8>>,
}
struct Reader<F> {
    read: F,
    cache: BTreeMap<Hash, Vec<u8>>,
    bytes: usize,
}
impl<F: FnMut(Hash) -> Result<Vec<u8>>> Reader<F> {
    fn get(&mut self, h: Hash) -> Result<Vec<u8>> {
        if let Some(b) = self.cache.get(&h) {
            return Ok(b.clone());
        }
        if h == [0; 32] || self.cache.len() >= 256 {
            return Err(Error::Bounds);
        }
        let b = (self.read)(h)?;
        if b.is_empty()
            || b.len() > tickwire_recovery::MAX_OBJECT_BYTES
            || tickwire_recovery::digest(&b) != h
        {
            return Err(Error::Identity);
        }
        self.bytes = self
            .bytes
            .checked_add(b.len())
            .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES as usize)
            .ok_or(Error::Bounds)?;
        self.cache.insert(h, b.clone());
        Ok(b)
    }
    fn completion(
        &mut self,
        h: Hash,
        depth: usize,
    ) -> Result<(Completion, Receipt, Receipt, ReleaseBundle)> {
        if depth >= 16 {
            return Err(Error::Bounds);
        }
        let c = Completion::decode(&self.get(h)?)?;
        let bundle = ReleaseBundle::decode(
            &self.get(c.roots[0])?,
            &self.get(c.roots[1])?,
            &self.get(c.roots[2])?,
        )?;
        let release = bundle.verify_operation(&c.operation, &c.member)?;
        let (a, b) = c.verify(
            &release,
            &self.get(c.restored.digest)?,
            &self.get(c.rollback.digest)?,
        )?;
        a.output.verify(
            &self.get(a.output.digest)?,
            tickwire_recovery::MAX_OBJECT_BYTES as u64,
        )?;
        b.output.verify(
            &self.get(b.output.digest)?,
            tickwire_recovery::MAX_OBJECT_BYTES as u64,
        )?;
        let raw = self.get(release.migration.source_proof.digest)?;
        release
            .migration
            .source_proof
            .verify(&raw, tickwire_recovery::HandoverSeal::MAX_BYTES as u64)?;
        let source = tickwire_recovery::HandoverSeal::decode(&raw).map_err(|_| Error::Invalid)?;
        if source.instance != c.member.old_instance
            || source.generation != c.member.old_generation
            || source.logical_strategy != c.logical_strategy
            || source.checkpoint_digest != release.migration.source_checkpoint.digest
            || source.report_position != a.source_report_position
        {
            return Err(Error::Identity);
        }
        let cp = self.get(source.checkpoint_digest)?;
        release
            .migration
            .source_checkpoint
            .verify(&cp, tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
        let inputs = self.get(source.input_digest)?;
        let account = self.get(source.account_input_digest)?;
        let verified = tickwire_handover::verify(&source, &cp, &inputs, &account)
            .map_err(|_| Error::Invalid)?;
        if verified.baseline().snapshot().scope.risk_domain_id != c.member.risk_domain {
            return Err(Error::Identity);
        }
        if let Some(prefix) = &release.migration.inputs {
            prefix.verify(inputs)?;
        }
        for (_, h) in &source.dependencies {
            self.get(*h)?;
        }
        if let Some(parent) = source.preflight_receipt_digest {
            let (prior, _, _, _) = self.completion(parent, depth + 1)?;
            if prior.member.new_instance != source.instance
                || prior.member.new_generation != source.generation
                || prior.logical_strategy != source.logical_strategy
            {
                return Err(Error::Identity);
            }
        }
        Ok((c, a, b, bundle))
    }
}
pub fn verify<F: FnMut(Hash) -> Result<Vec<u8>>>(root: Hash, read: F) -> Result<Verified> {
    let mut r = Reader {
        read,
        cache: BTreeMap::new(),
        bytes: 0,
    };
    let (completion, restored, rollback, bundle) = r.completion(root, 0)?;
    Ok(Verified {
        completion,
        restored,
        rollback,
        bundle,
        objects: r.cache.into_values().collect(),
    })
}
