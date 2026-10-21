//! Verify and enumerate the complete strategy handover closure.
use crate::*;
use tickwire_recovery::HandoverSeal;

pub(crate) fn dependencies<S: BlobStore>(
    p: &Persistence<S>,
    stream: &Stream,
    entry: &Entry,
) -> Result<Vec<ObjectRef>> {
    if entry.kind != 11 {
        return Ok(Vec::new());
    }
    let (objects, _, _) = inspect(p, stream, entry)?;
    Ok(objects)
}
pub(crate) fn verify_history<S: BlobStore>(
    p: &Persistence<S>,
    stream: &Stream,
    entry: &Entry,
    history: &[Entry],
) -> Result<()> {
    if entry.kind != 11 {
        return Ok(());
    }
    let (_, records, _) = inspect(p, stream, entry)?;
    for (index, record) in records.iter().enumerate() {
        if history.get(index).map(|e| (e.sequence, e.payload.hash))
            != Some((record.sequence(), record.digest()))
        {
            return Err(Error::Conflict);
        }
    }
    Ok(())
}
pub(crate) fn verify<S: BlobStore>(
    p: &mut Persistence<S>,
    stream: &Stream,
    entry: &Entry,
) -> Result<()> {
    let objects = dependencies(p, stream, entry)?;
    crate::source::verify_objects(p, &objects)
}
fn inspect<S: BlobStore>(
    p: &Persistence<S>,
    stream: &Stream,
    entry: &Entry,
) -> Result<(
    Vec<ObjectRef>,
    Vec<tickwire_account_codec::CanonicalRecord>,
    HandoverSeal,
)> {
    let Format::Account { baseline } = stream.format else {
        return Err(Error::Conflict);
    };
    let payload = p.read_object(entry.payload.hash, entry.payload.bytes as usize)?;
    let record = tickwire_account_codec::CanonicalRecord::decode(&payload)
        .map_err(|_| Error::Invalid("handover record"))?;
    let tickwire_account::RecoveryRecord::StrategyHandover(binding) = record.record() else {
        return Err(Error::Conflict);
    };
    if !entry
        .extra_objects
        .iter()
        .any(|r| r.hash == binding.input_seal_digest)
        || !entry
            .extra_objects
            .iter()
            .any(|r| r.hash == binding.checkpoint_digest)
    {
        return Err(Error::Missing);
    }
    let seal =
        HandoverSeal::decode(&p.read_object(binding.input_seal_digest, HandoverSeal::MAX_BYTES)?)?;
    if binding.scope.risk_domain_id != stream.domain
        || binding.writer_epoch != entry.writer_epoch
        || seal.instance != binding.to.instance
        || seal.generation != binding.to.generation
        || seal.logical_strategy != binding.logical_strategy_id
        || seal.ledger_sequence != binding.expected_ledger_sequence
        || seal.orders_digest != binding.expected_orders_digest
        || seal.checkpoint_digest != binding.checkpoint_digest
        || seal.report_position.checked_add(1) != Some(entry.sequence)
    {
        return Err(Error::Conflict);
    }
    let mut objects = Vec::new();
    let mut total = 0usize;
    for hash in seal.roots() {
        let bytes = p.read_object(hash, MAX_OBJECT_BYTES)?;
        total = total
            .checked_add(bytes.len())
            .filter(|n| *n <= MAX_OBJECT_BYTES * 2)
            .ok_or(Error::Bounds)?;
        objects.push(ObjectRef::new(&bytes)?);
    }
    let checkpoint = p.read_object(seal.checkpoint_digest, seal.checkpoint_bytes as usize)?;
    let inputs = p.read_object(seal.input_digest, seal.input_bytes as usize)?;
    let account_input =
        p.read_object(seal.account_input_digest, seal.account_input_bytes as usize)?;
    let verified = tickwire_handover::verify(&seal, &checkpoint, &inputs, &account_input)
        .map_err(|_| Error::Conflict)?;
    if verified.baseline().digest() != baseline
        || verified.baseline().snapshot().scope != binding.scope
    {
        return Err(Error::Conflict);
    }
    if let Some(root) = seal.preflight_receipt_digest {
        let release = tickwire_release::closure::verify(root, |hash| {
            p.read_object(hash, MAX_OBJECT_BYTES)
                .map_err(|_| tickwire_release::Error::Missing)
        })
        .map_err(|_| Error::Conflict)?;
        let c = &release.completion;
        if c.operation.schema != 2
            || c.operation.operation_id != binding.operation_id
            || c.operation.operation_revision != binding.operation_revision
            || c.logical_strategy != binding.logical_strategy_id
            || c.member.risk_domain != binding.scope.risk_domain_id
            || c.member.old_instance != binding.from.instance
            || c.member.old_generation != binding.from.generation
            || c.member.new_instance != binding.to.instance
            || c.member.new_generation != binding.to.generation
        {
            return Err(Error::Conflict);
        }
        for bytes in release.objects {
            let r = ObjectRef::new(&bytes)?;
            if !objects.contains(&r) {
                total = total
                    .checked_add(bytes.len())
                    .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES as usize)
                    .ok_or(Error::Bounds)?;
                if objects.len() >= 512 {
                    return Err(Error::Bounds);
                }
                objects.push(r);
            }
        }
    }
    if seal.version == 3 {
        let root = seal.replay_rollback_digest.ok_or(Error::Conflict)?;
        let stages =
            tickwire_release::replay::startup::verify_seal(&seal, &checkpoint, &inputs, |hash| {
                p.read_object(hash, MAX_OBJECT_BYTES)
                    .map_err(|_| tickwire_release::Error::Missing)
            })
            .map_err(|_| Error::Conflict)?;
        for bytes in stages {
            let object = ObjectRef::new(&bytes)?;
            if !objects.contains(&object) {
                total = total
                    .checked_add(bytes.len())
                    .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES as usize)
                    .ok_or(Error::Bounds)?;
                if objects.len() >= 512 {
                    return Err(Error::Bounds);
                }
                objects.push(object);
            }
        }

        let completed = tickwire_release::replay::rollback::verify(root, |hash| {
            p.read_object(hash, MAX_OBJECT_BYTES)
                .map_err(|_| tickwire_release::Error::Missing)
        })
        .map_err(|_| Error::Conflict)?;
        let installed = &completed.installation;
        installed
            .bind_seal(&seal, &checkpoint, &inputs)
            .map_err(|_| Error::Conflict)?;
        let op = &installed.request.operation;
        let member = &installed.request.member;
        if op.operation_id != binding.operation_id
            || op.operation_revision != binding.operation_revision
            || op.logical_strategy_id != binding.logical_strategy_id
            || member.risk_domain != binding.scope.risk_domain_id
            || member.old_instance != binding.from.instance
            || member.old_generation != binding.from.generation
            || member.new_instance != binding.to.instance
            || member.new_generation != binding.to.generation
        {
            return Err(Error::Conflict);
        }
        for bytes in completed.objects {
            let object = ObjectRef::new(&bytes)?;
            if !objects.contains(&object) {
                total = total
                    .checked_add(bytes.len())
                    .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES as usize)
                    .ok_or(Error::Bounds)?;
                if objects.len() >= 512 {
                    return Err(Error::Bounds);
                }
                objects.push(object);
            }
        }
    }
    Ok((objects, verified.records().to_vec(), seal))
}
