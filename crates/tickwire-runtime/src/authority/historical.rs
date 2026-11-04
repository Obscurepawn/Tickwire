//! Read compatibility for strategy transfer records in existing account journals.
use std::collections::BTreeMap;
use tickwire_account::RecoveryRecord;
use tickwire_account_gateway::AuthorityError;
use tickwire_recovery::{BlobStore, Persistence};

/// Validate historical strategy ownership against the retained strategy closure
/// and the exact canonical account prefix.
pub fn verify_handover_objects<S: BlobStore>(
    persistence: &Persistence<S>,
    baseline: &tickwire_account_codec::CanonicalBaseline,
    record: &tickwire_account_codec::CanonicalRecord,
    prior: &[tickwire_account_codec::CanonicalRecord],
) -> Result<(), AuthorityError> {
    let RecoveryRecord::StrategyHandover(h) = record.record() else {
        return Err(AuthorityError::Denied);
    };
    let invalid = || AuthorityError::Denied;
    let seal = tickwire_recovery::HandoverSeal::decode(
        &persistence
            .read_object(
                h.input_seal_digest,
                tickwire_recovery::HandoverSeal::MAX_BYTES,
            )
            .map_err(AuthorityError::Recovery)?,
    )
    .map_err(AuthorityError::Recovery)?;
    if record.baseline() != baseline.digest()
        || h.scope != baseline.snapshot().scope
        || seal.instance != h.to.instance
        || seal.generation != h.to.generation
        || seal.logical_strategy != h.logical_strategy_id
        || seal.ledger_sequence != h.expected_ledger_sequence
        || seal.orders_digest != h.expected_orders_digest
        || seal.checkpoint_digest != h.checkpoint_digest
        || seal.report_position.checked_add(1) != Some(record.sequence())
        || seal.report_position != prior.len() as u64
    {
        return Err(invalid());
    }
    let checkpoint = persistence
        .read_object(seal.checkpoint_digest, tickwire_recovery::MAX_OBJECT_BYTES)
        .map_err(AuthorityError::Recovery)?;
    let inputs = persistence
        .read_object(seal.input_digest, tickwire_recovery::MAX_OBJECT_BYTES)
        .map_err(AuthorityError::Recovery)?;
    let account_object = persistence
        .read_object(
            seal.account_input_digest,
            tickwire_recovery::MAX_OBJECT_BYTES,
        )
        .map_err(AuthorityError::Recovery)?;
    let verified =
        tickwire_release::account_inputs::verify(&seal, &checkpoint, &inputs, &account_object)
            .map_err(|_| invalid())?;
    verified
        .matches_history(baseline, prior)
        .map_err(|_| invalid())?;
    let mut sizes = BTreeMap::from([
        (seal.checkpoint_digest, checkpoint.len()),
        (seal.account_input_digest, account_object.len()),
        (seal.input_digest, inputs.len()),
        (
            h.input_seal_digest,
            seal.bytes().map_err(|_| invalid())?.len(),
        ),
    ]);
    if let Some(root) = seal.preflight_receipt_digest {
        let release = tickwire_release::closure::verify(root, |hash| {
            persistence
                .read_object(hash, tickwire_recovery::MAX_OBJECT_BYTES)
                .map_err(|_| tickwire_release::Error::Missing)
        })
        .map_err(|_| invalid())?;
        let c = &release.completion;
        if c.operation.schema != 2
            || c.operation.operation_id != h.operation_id
            || c.operation.operation_revision != h.operation_revision
            || c.logical_strategy != h.logical_strategy_id
            || c.member.risk_domain != h.scope.risk_domain_id
            || c.member.old_instance != h.from.instance
            || c.member.old_generation != h.from.generation
            || c.member.new_instance != h.to.instance
            || c.member.new_generation != h.to.generation
        {
            return Err(invalid());
        }
        for bytes in &release.objects {
            sizes.insert(tickwire_recovery::digest(bytes), bytes.len());
        }
    }
    if seal.version == 3 {
        let root = seal.replay_rollback_digest.ok_or_else(invalid)?;
        let objects =
            tickwire_release::replay::startup::verify_seal(&seal, &checkpoint, &inputs, |hash| {
                persistence
                    .read_object(hash, tickwire_recovery::MAX_OBJECT_BYTES)
                    .map_err(|_| tickwire_release::Error::Missing)
            })
            .map_err(|_| invalid())?;
        for bytes in objects {
            sizes.insert(tickwire_recovery::digest(&bytes), bytes.len());
        }
        let completed = tickwire_release::replay::rollback::verify(root, |hash| {
            persistence
                .read_object(hash, tickwire_recovery::MAX_OBJECT_BYTES)
                .map_err(|_| tickwire_release::Error::Missing)
        })
        .map_err(|_| invalid())?;
        let installed = &completed.installation;
        installed
            .bind_seal(&seal, &checkpoint, &inputs)
            .map_err(|_| invalid())?;
        let op = &installed.request.operation;
        let member = &installed.request.member;
        if op.operation_id != h.operation_id
            || op.operation_revision != h.operation_revision
            || op.logical_strategy_id != h.logical_strategy_id
            || member.risk_domain != h.scope.risk_domain_id
            || member.old_instance != h.from.instance
            || member.old_generation != h.from.generation
            || member.new_instance != h.to.instance
            || member.new_generation != h.to.generation
        {
            return Err(invalid());
        }
        for bytes in completed.objects {
            sizes.insert(tickwire_recovery::digest(&bytes), bytes.len());
        }
    }
    if seal.version == 4 {
        let root = seal.rebuild_installation_digest.ok_or_else(invalid)?;
        if seal.rebuild_rollback_digest.is_none() {
            return Err(invalid());
        }
        let objects =
            tickwire_release::rebuild::install::verify_seal(&seal, &checkpoint, &inputs, |hash| {
                persistence
                    .read_object(hash, tickwire_recovery::MAX_OBJECT_BYTES)
                    .map_err(|_| tickwire_release::Error::Missing)
            })
            .map_err(|_| invalid())?;
        for bytes in objects {
            sizes.insert(tickwire_recovery::digest(&bytes), bytes.len());
        }
        let bytes = persistence
            .read_object(root, tickwire_release::replay::MAX_BYTES)
            .map_err(|_| invalid())?;
        let installed = tickwire_release::rebuild::install::verify(
            tickwire_release::Object::of(&bytes),
            |hash| {
                persistence
                    .read_object(hash, tickwire_recovery::MAX_OBJECT_BYTES)
                    .map_err(|_| tickwire_release::Error::Missing)
            },
        )
        .map_err(|_| invalid())?;
        let request = installed.request();
        let op = &request.operation;
        let member = &request.member;
        if op.schema != 4
            || op.operation_id != h.operation_id
            || op.operation_revision != h.operation_revision
            || op.logical_strategy_id != h.logical_strategy_id
            || member.risk_domain != h.scope.risk_domain_id
            || member.old_instance != h.from.instance
            || member.old_generation != h.from.generation
            || member.new_instance != h.to.instance
            || member.new_generation != h.to.generation
        {
            return Err(invalid());
        }
    }
    for (_, hash) in &seal.dependencies {
        let bytes = persistence
            .read_object(*hash, tickwire_recovery::MAX_OBJECT_BYTES)
            .map_err(AuthorityError::Recovery)?;
        sizes.insert(*hash, bytes.len());
    }
    let total = sizes
        .values()
        .try_fold(0usize, |total, n| total.checked_add(*n))
        .ok_or_else(invalid)?;
    if sizes.len() > 512 || total > tickwire_recovery::MAX_CLOSURE_BYTES as usize {
        return Err(invalid());
    }
    Ok(())
}
