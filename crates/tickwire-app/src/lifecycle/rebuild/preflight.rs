//! The fixed capture must belong to this account owner's actual history. A
//! later READY may advance existing responsibilities but cannot add an action
//! missing from the approved rebuild contract.
use super::*;
use tickwire_account_codec::{CanonicalBaseline, CanonicalRecord};
use tickwire_release::{
    Object,
    rebuild::{ActionBinding, Inputs},
};
use tickwire_runtime::node::RebuildInput;

pub(super) fn qualify<B: Backend, A: Authority, E: Encoder>(
    input: &RebuildInput,
    request: &protocol::ControlRequest,
    gateway: &AccountGateway<B, A, E>,
    now: MonoTime,
) -> Result<Fixed> {
    let install = input.request();
    if install.operation != request.operation
        || install.member != request.member
        || now.nanos() >= install.deadline_mono_ns
    {
        return Err(invalid("rebuild input differs from signed publication"));
    }
    let plan = install.verify(
        input.plans(),
        input.contract(),
        input.inputs(),
        input.manifest(),
        input.seal(),
    )?;
    let index = crate::lifecycle::preflight::ObjectIndex::new(input.objects())?;
    let read = |root: Object| -> Result<&[u8]> {
        let raw = index.get(root.digest)?;
        root.verify(raw, tickwire_release::replay::MAX_BYTES as u64)?;
        Ok(raw)
    };
    let completion = tickwire_strategy_sdk::replay::rebuild::capture::store::Completion::decode(
        read(install.capture_completion)?,
    )?;
    let seed = Inputs::decode(read(completion.seed)?)?;
    let window = tickwire_strategy_sdk::replay::rebuild::capture::VerifiedWindow::verify(
        &completion.identity,
        input.contract(),
        &seed,
        input.inputs(),
        input.seal(),
    )?;
    window.validate_plan(plan)?;
    let receipt = tickwire_strategy_sdk::capture::Receipt::decode(read(install.capture_receipt)?)?;
    let bootstrap = serde_json::from_slice(read(receipt.bootstrap)?)?;
    let seal_request = tickwire_strategy_sdk::capture::SealRequest::decode(read(receipt.request)?)?;
    if receipt.completion != completion {
        return Err(invalid("capture completion mismatch"));
    }
    receipt.verify(&bootstrap, &seal_request, input.worker_pid(), now, &window)?;
    current(
        input.contract(),
        input.inputs(),
        input.manifest(),
        input.rollback_plans(),
        request,
        gateway,
    )?;
    let object = |root: Object| protocol::ReplayObject {
        digest: root.digest,
        bytes: root.bytes,
    };
    let cursors = |v: &[tickwire_release::replay::Cursor]| {
        v.iter()
            .map(|c| protocol::RebuildCursor {
                identity: c.stream.identity,
                generation: c.stream.generation,
                sequence: c.sequence,
                content_digest: c.content_digest,
            })
            .collect()
    };
    Ok(Fixed {
        request: object(Object::of(&install.bytes()?)),
        plans: object(install.plans),
        contract: object(plan.contract),
        inputs: object(plan.inputs),
        rollback_plans: object(Object::of(&input.rollback_plans().bytes()?)),
        capture_completion: object(install.capture_completion),
        capture_seal: object(install.capture_seal),
        capture_receipt: object(install.capture_receipt),
        end: protocol::RebuildFrontier {
            now_ns: plan.end.now_ns,
            rows: plan.end.rows,
            markets: cursors(&plan.end.markets),
            accounts: cursors(&plan.end.accounts),
            external: cursors(&plan.end.external),
        },
    })
}

pub(super) fn current<B: Backend, A: Authority, E: Encoder>(
    contract: &tickwire_release::rebuild::Contract,
    inputs: &Inputs,
    manifest: &tickwire_release::ReleaseManifest,
    rollback: &tickwire_release::rebuild::rollback::Plans,
    request: &protocol::ControlRequest,
    gateway: &AccountGateway<B, A, E>,
) -> Result<()> {
    history(
        inputs,
        &contract.actions,
        request.operation.logical_strategy_id,
        gateway.baseline(),
        gateway
            .journal_records_after(0, tickwire_account_journal::MAX_RECORDS)?
            .map(|(_, r)| r),
    )?;
    let required = crate::lifecycle::preflight::requirements(
        gateway.account(),
        gateway.baseline().catalog(),
        request.operation.logical_strategy_id,
        gateway
            .journal_records_after(0, tickwire_account_journal::MAX_RECORDS)?
            .map(|(_, r)| r),
    )?;
    manifest
        .targets
        .iter()
        .find(|t| t.member == request.member)
        .ok_or_else(|| invalid("rebuild target missing"))?
        .capabilities
        .check_requirements(&required)?;
    rollback
        .members
        .iter()
        .find(|t| t.member == request.member)
        .ok_or_else(|| invalid("rebuild rollback target missing"))?
        .capabilities
        .check_requirements(&required)?;
    Ok(())
}

pub(super) fn history<'a>(
    inputs: &Inputs,
    actions: &[ActionBinding],
    logical: u64,
    baseline: &CanonicalBaseline,
    records: impl Iterator<Item = &'a CanonicalRecord>,
) -> Result<()> {
    use tickwire_release::replay::{Input, StreamKey};
    if inputs.accounts.len() != 1
        || logical == 0
        || actions.len() > tickwire_release::rebuild::MAX_ACTIONS
        || actions
            .windows(2)
            .any(|w| w[0].economic_intent >= w[1].economic_intent)
    {
        return Err(invalid("rebuild account or action shape"));
    }
    let prefix = &inputs.accounts[0];
    if prefix.baseline != baseline.bytes() {
        return Err(invalid("rebuild account baseline differs"));
    }
    let stream = StreamKey {
        identity: baseline.digest(),
        generation: baseline.snapshot().account_epoch,
    };
    let mut expected = prefix.records.iter().map(Vec::as_slice).collect::<Vec<_>>();
    for row in &inputs.warm {
        if let Input::AccountCallback {
            stream: actual,
            position,
            canonical,
        } = &row.input
        {
            if *actual != stream || *position != expected.len() as u64 + 1 {
                return Err(invalid("rebuild window account cursor differs"));
            }
            expected.push(canonical.as_slice());
        }
    }
    let mut position = 0u64;
    let mut seen = BTreeSet::new();
    for record in records {
        position = position
            .checked_add(1)
            .ok_or_else(|| invalid("rebuild account overflow"))?;
        if record.baseline() != baseline.digest() || record.sequence() != position {
            return Err(invalid("rebuild actual account history is incomplete"));
        }
        if expected
            .get((position - 1) as usize)
            .is_some_and(|raw| *raw != record.bytes())
        {
            return Err(invalid(
                "rebuild window differs from actual account history",
            ));
        }
        if let RecoveryRecord::Admit { command, .. } = record.record() {
            let identity = &command.command().context.strategy;
            if identity.logical_strategy_id == logical {
                let index = actions
                    .binary_search_by_key(&identity.economic_intent_id, |a| a.economic_intent)
                    .map_err(|_| invalid("new admitted action requires a new rebuild operation"))?;
                actions[index]
                    .admitted_command
                    .verify(command.bytes(), tickwire_release::replay::MAX_BYTES as u64)?;
                if !seen.insert(identity.economic_intent_id) {
                    return Err(invalid("rebuild action admitted more than once"));
                }
            }
        }
    }
    if position < expected.len() as u64 || seen.len() != actions.len() {
        return Err(invalid(
            "rebuild window or admitted action is absent from actual account history",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
