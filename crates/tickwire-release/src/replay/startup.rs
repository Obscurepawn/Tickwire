//! Schema-3 startup proves the S restore callback only. It carries no rollback
//! completion and cannot authorize ReplayPrepared or Commit.
use super::*;
use std::collections::BTreeMap;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    pub schema: u16,
    pub operation: crate::Operation,
    pub member: crate::Member,
    pub restored: Object,
}
impl Completion {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        self.operation.validate().map_err(|_| Error::Identity)?;
        if self.schema != 1
            || self.operation.schema != 3
            || !self.operation.members.contains(&self.member)
        {
            return Err(Error::Identity);
        }
        self.restored.validate(MAX_BYTES as u64)?;
        canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let c: Self = decode(bytes)?;
        c.bytes()?;
        Ok(c)
    }
}
pub struct Verified {
    pub completion: Completion,
    pub restored: crate::receipt::Receipt,
    pub bundle: crate::ReleaseBundle,
    pub objects: Vec<Vec<u8>>,
}
pub fn verify(root: Hash, mut read: impl FnMut(Hash) -> Result<Vec<u8>>) -> Result<Verified> {
    verify_at(root, &mut read, 0)
}
pub(crate) fn verify_at(
    root: Hash,
    read: &mut dyn FnMut(Hash) -> Result<Vec<u8>>,
    depth: usize,
) -> Result<Verified> {
    if depth >= 16 {
        return Err(Error::Bounds);
    }
    let mut map = BTreeMap::new();
    let mut total = 0usize;
    let mut get = |h| -> Result<Vec<u8>> {
        if let Some(b) = map.get(&h) {
            return Ok(Vec::clone(b));
        }
        if map.len() >= 512 {
            return Err(Error::Bounds);
        }
        let bytes = read(h)?;
        if bytes.is_empty() || bytes.len() > MAX_BYTES || Object::of(&bytes).digest != h {
            return Err(Error::Identity);
        }
        total = total
            .checked_add(bytes.len())
            .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES as usize)
            .ok_or(Error::Bounds)?;
        map.insert(h, bytes.clone());
        Ok(bytes)
    };
    let c = Completion::decode(&get(root)?)?;
    let op = &c.operation;
    let bundle = crate::ReleaseBundle::decode(
        &get(op.artifacts_digest)?,
        &get(op.checkpoint_digest)?,
        &get(op.rollback_digest)?,
    )?;
    let release = bundle.verify_operation(op, &c.member)?;
    let raw = get(c.restored.digest)?;
    c.restored.verify(&raw, MAX_BYTES as u64)?;
    let receipt = crate::receipt::Receipt::decode(&raw)?;
    receipt.verify(&release, None)?;
    receipt
        .output
        .verify(&get(receipt.output.digest)?, MAX_BYTES as u64)?;
    let raw = get(release.migration.source_proof.digest)?;
    release
        .migration
        .source_proof
        .verify(&raw, MAX_BYTES as u64)?;
    let seal = tickwire_recovery::HandoverSeal::decode(&raw).map_err(|_| Error::Identity)?;
    if seal.instance != c.member.old_instance
        || seal.generation != c.member.old_generation
        || seal.logical_strategy != op.logical_strategy_id
        || seal.checkpoint_digest != release.migration.source_checkpoint.digest
        || seal.report_position != receipt.source_report_position
    {
        return Err(Error::Identity);
    }
    let cp = get(seal.checkpoint_digest)?;
    release
        .migration
        .source_checkpoint
        .verify(&cp, MAX_BYTES as u64)?;
    let inputs = get(seal.input_digest)?;
    let account = get(seal.account_input_digest)?;
    let callbacks =
        tickwire_handover::verify(&seal, &cp, &inputs, &account).map_err(|_| Error::Identity)?;
    if callbacks.baseline().snapshot().scope.risk_domain_id != c.member.risk_domain {
        return Err(Error::Identity);
    }
    if let Some(prefix) = &release.migration.inputs {
        prefix.verify(inputs.clone())?;
    }
    for (_, h) in &seal.dependencies {
        get(*h)?;
    }
    if let Some(root) = seal.preflight_receipt_digest {
        let old = crate::closure::verify_at(root, &mut get, depth + 1)?;
        if old.completion.member.new_instance != seal.instance
            || old.completion.member.new_generation != seal.generation
            || old.completion.logical_strategy != seal.logical_strategy
        {
            return Err(Error::Identity);
        }
    }
    if seal.version == 3 {
        if seal.replay_rollback_digest.is_none() {
            return Err(Error::Identity);
        }
        verify_seal_at(&seal, &cp, &inputs, &mut get, depth + 1)?;
    }
    Ok(Verified {
        completion: c,
        restored: receipt,
        bundle,
        objects: map.into_values().collect(),
    })
}
/// Verifies all present stages without upgrading a restored/installed stage to
/// rollback-complete. Callers granting release also require rollback_digest.
pub fn verify_seal(
    seal: &tickwire_recovery::HandoverSeal,
    cp: &[u8],
    inputs: &[u8],
    mut read: impl FnMut(Hash) -> Result<Vec<u8>>,
) -> Result<Vec<Vec<u8>>> {
    verify_seal_at(seal, cp, inputs, &mut read, 0)
}
pub(crate) fn verify_seal_at(
    seal: &tickwire_recovery::HandoverSeal,
    cp: &[u8],
    inputs: &[u8],
    mut read: &mut dyn FnMut(Hash) -> Result<Vec<u8>>,
    depth: usize,
) -> Result<Vec<Vec<u8>>> {
    if depth >= 16 || seal.version != 3 {
        return Err(Error::Identity);
    }
    seal.verify_checkpoint(cp).map_err(|_| Error::Identity)?;
    let startup = verify_at(
        seal.replay_restore_digest.ok_or(Error::Missing)?,
        &mut read,
        depth + 1,
    )?;
    let member = &startup.completion.member;
    if member.new_instance != seal.instance
        || member.new_generation != seal.generation
        || startup.completion.operation.logical_strategy_id != seal.logical_strategy
    {
        return Err(Error::Identity);
    }
    let mut objects = startup.objects;
    if let Some(root) = seal.replay_installation_digest {
        let installed = install::verify_at(root, &mut read, depth + 1)?;
        if installed.request.operation != startup.completion.operation
            || installed.request.member != *member
        {
            return Err(Error::Identity);
        }
        installed.bind_seal(seal, cp, inputs)?;
        objects.extend(installed.objects);
    }
    if let Some(root) = seal.replay_rollback_digest {
        let done = rollback::verify_at(root, &mut read, depth + 1)?;
        if Some(Object::of(&done.installation.continued.bytes()?).digest)
            != seal.replay_installation_digest
        {
            return Err(Error::Identity);
        }
        objects.extend(done.objects);
    }
    let mut map = BTreeMap::new();
    for bytes in objects {
        map.insert(Object::of(&bytes).digest, bytes);
    }
    let sum: usize = map.values().map(Vec::len).sum();
    if map.len() > 512 || sum > tickwire_recovery::MAX_CLOSURE_BYTES as usize {
        return Err(Error::Bounds);
    }
    Ok(map.into_values().collect())
}
