//! Content qualification for an actual old checkpoint and its complete tail.
//! The SDK subsequently executes the old factory and compares the materialized
//! views at the fixed cut before a probe completion can be produced.
use super::*;

pub(super) fn verify<R: FnMut(Hash) -> Result<Vec<u8>>>(
    plan: &MemberPlan,
    operation: &Operation,
    forward: &Forward<'_>,
    end: &Frontier,
    objects: &mut Objects<R>,
) -> Result<ExactInput> {
    let Recovery::ExactOldRestore {
        source,
        anchor,
        tail,
        checkpoint,
        source_proof,
    } = &plan.recovery
    else {
        return Err(Error::Identity);
    };
    let object = |role: &Role| {
        plan.files
            .iter()
            .find(|f| &f.role == role)
            .map(|f| f.object)
    };
    if source.instance != plan.member.old_instance
        || source.generation != plan.member.old_generation
        || source.logical_strategy != operation.logical_strategy_id
        || source.clock != forward.inputs.clock
        || object(&Role::Binary).map(|o| o.digest) != Some(source.binary_digest)
        || source.state_schema != plan.capabilities.state_schema
        || !plan.capabilities.methods.contains(&Method::Restore {
            schema: source.state_schema,
        })
    {
        return Err(Error::Identity);
    }
    let a = Checkpoint::decode(&objects.get(*anchor)?)?;
    let c = Checkpoint::decode(&objects.get(*checkpoint)?)?;
    if a.source != *source
        || c.source != *source
        || c.runtime.now_ns > end.now_ns
        || object(&Role::StrategyConfig) != Some(Object::of(&c.runtime.config.bytes))
    {
        return Err(Error::Identity);
    }
    for model in &c.runtime.models {
        if object(&Role::Model(model.id)) != Some(Object::of(&model.value.bytes)) {
            return Err(Error::Identity);
        }
    }
    if plan
        .files
        .iter()
        .filter(|f| matches!(f.role, Role::Model(_)))
        .count()
        != c.runtime.models.len()
    {
        return Err(Error::Identity);
    }
    let raw = objects.get(*source_proof)?;
    let seal = tickwire_recovery::HandoverSeal::decode(&raw).map_err(|_| Error::Identity)?;
    if seal.instance != source.instance
        || seal.generation != source.generation
        || seal.logical_strategy != source.logical_strategy
    {
        return Err(Error::Identity);
    }
    let business = objects.get(Object {
        digest: seal.checkpoint_digest,
        bytes: seal.checkpoint_bytes,
    })?;
    let inputs = objects.get(Object {
        digest: seal.input_digest,
        bytes: seal.input_bytes,
    })?;
    let account = objects.get(Object {
        digest: seal.account_input_digest,
        bytes: seal.account_input_bytes,
    })?;
    let callbacks = tickwire_handover::verify(&seal, &business, &inputs, &account)
        .map_err(|_| Error::Identity)?;
    if callbacks.baseline().snapshot().scope.risk_domain_id != plan.member.risk_domain {
        return Err(Error::Identity);
    }
    let retained = replay::retained::decode(&inputs)?;
    if retained.current() != &c
        || retained.anchor().checkpoint() != &a
        || retained.tail_from(&a)? != objects.get(*tail)?
    {
        return Err(Error::Identity);
    }
    let app: serde_json::Value =
        serde_json::from_slice(&c.business).map_err(|_| Error::Encoding)?;
    let state: Vec<u8> =
        serde_json::from_value(app.get("strategy").cloned().ok_or(Error::Identity)?)
            .map_err(|_| Error::Encoding)?;
    if business.get(96..) != Some(state.as_slice()) {
        return Err(Error::Identity);
    }
    for (_, hash) in &seal.dependencies {
        objects.by_hash(*hash)?;
    }
    if let Some(parent) = seal.preflight_receipt_digest {
        let prior = crate::closure::verify(parent, |h| objects.by_hash(h))?;
        if prior.completion.member.new_instance != seal.instance
            || prior.completion.member.new_generation != seal.generation
            || prior.completion.logical_strategy != seal.logical_strategy
        {
            return Err(Error::Identity);
        }
    }
    if seal.version == 3 {
        if seal.replay_rollback_digest.is_none() {
            return Err(Error::Identity);
        }
        replay::startup::verify_seal(&seal, &business, &inputs, |h| objects.by_hash(h))?;
    }
    if forward.inputs.accounts.len() != 1 {
        return Err(Error::Unsupported);
    }
    let prefix = &forward.inputs.accounts[0];
    if prefix.baseline != callbacks.baseline().bytes() {
        return Err(Error::Identity);
    }
    let stream = StreamKey {
        identity: callbacks.baseline().digest(),
        generation: callbacks.baseline().snapshot().account_epoch,
    };
    let mut expected = prefix.records.iter().map(Vec::as_slice).collect::<Vec<_>>();
    for row in &forward.inputs.warm {
        if let replay::Input::AccountCallback {
            stream: actual,
            position,
            canonical,
        } = &row.input
        {
            if *actual != stream || *position != expected.len() as u64 + 1 {
                return Err(Error::Identity);
            }
            expected.push(canonical);
        }
    }
    if expected.len() != callbacks.records().len()
        || expected
            .iter()
            .zip(callbacks.records())
            .any(|(a, b)| *a != b.bytes())
        || c.runtime.accounts.len() != 1
        || c.runtime.accounts[0].stream != stream
        || c.runtime.accounts[0].sequence != expected.len() as u64
    {
        return Err(Error::Identity);
    }
    // Account cursor hashes may use the canonical callback rather than the
    // raw record hash. Compare actual bytes above and complete stream positions.
    let positions = |v: &[Cursor]| {
        v.iter()
            .map(|c| (c.stream.clone(), c.sequence))
            .collect::<Vec<_>>()
    };
    if positions(&c.runtime.markets) != positions(&end.markets)
        || positions(&c.runtime.accounts) != positions(&end.accounts)
        || c.runtime.external != end.external
    {
        return Err(Error::Identity);
    }
    let models = a
        .runtime
        .models
        .iter()
        .map(|m| (m.id, Object::of(&m.value.bytes)))
        .collect::<Vec<_>>();
    let anchor = replay::verify_anchor(
        &a.bytes()?,
        source,
        Object::of(&a.runtime.config.bytes),
        &models,
    )?;
    let tail = replay::verify_tail(&anchor, &objects.get(*tail)?, &c.cut)?;
    Ok(ExactInput {
        anchor,
        tail,
        checkpoint: c,
    })
}
