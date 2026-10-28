use super::*;
use crate::capture::install::rollback as protocol;
use tickwire_release::{
    Object,
    rebuild::{probe, rollback::Forward},
};
#[allow(clippy::too_many_arguments)]
pub(super) fn rollback<S: Strategy>(
    installed: &mut InstalledControl,
    envelope: &ControlEnvelope,
    body: &tickwire_ipc::ReplayControlBody,
    config: &Bootstrap,
    clock: &mut SystemClock,
    live: &mut crate::replay::live::Live<S>,
    recovery: &mut crate::recovery::RecoveryWriter,
    session_until: u64,
) -> Result<Object> {
    let reader =
        tickwire_recovery::FileBlobReader::open(&config.report_directory, config.report.peer_uid)?;
    let raw = reader.get_object(body.object_digest, crate::capture::MAX_BYTES)?;
    Object {
        digest: body.object_digest,
        bytes: body.object_bytes,
    }
    .verify(&raw, crate::capture::MAX_BYTES as u64)?;
    let request = protocol::Request::decode(&raw)?;
    let at = clock.sample()?.monotonic;
    request.validate(
        &installed.input.request,
        installed.verified.installation(),
        at,
    )?;
    protocol::verify_request(
        envelope,
        body,
        &installed.capture,
        &installed.input,
        &request,
        at,
        session_until,
    )?;
    if let Some((old, receipt)) = &installed.rollback {
        if old != body {
            return Err(Error::Identity);
        }
        return Ok(*receipt);
    }
    let mut map = std::collections::BTreeMap::new();
    for object in &request.objects {
        let bytes = reader.get_object(object.digest, tickwire_recovery::MAX_OBJECT_BYTES)?;
        object.verify(&bytes, tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
        map.insert(object.digest, bytes);
        request.validate(
            &installed.input.request,
            installed.verified.installation(),
            clock.sample()?.monotonic,
        )?;
    }
    let original = installed
        .verified
        .objects()
        .map(|b| (tickwire_recovery::digest(b), b))
        .collect::<std::collections::BTreeMap<_, _>>();
    let read = |object: Object| original.get(&object.digest).copied().ok_or(Error::Identity);
    let plans = tickwire_release::rebuild::Plans::decode(read(installed.input.request.plans)?)?;
    let manifest = serde_json::from_slice(read(installed.input.request.manifest)?)?;
    let plan = plans
        .members
        .iter()
        .find(|p| p.member == installed.input.request.member)
        .ok_or(Error::Identity)?;
    let contract = tickwire_release::rebuild::Contract::decode(read(plan.contract)?)?;
    let inputs = tickwire_release::rebuild::Inputs::decode(read(plan.inputs)?)?;
    let verified = probe::Verified::verify(
        request.completion,
        &installed.input.request.operation,
        &installed.input.request.member,
        Forward {
            plans: &plans,
            contract: &contract,
            inputs: &inputs,
            manifest: &manifest,
        },
        |h| map.get(&h).cloned().ok_or(tickwire_release::Error::Missing),
    )?;
    for bytes in verified.objects() {
        live.store_object(bytes)?;
    }
    request.validate(
        &installed.input.request,
        installed.verified.installation(),
        clock.sample()?.monotonic,
    )?;
    recovery.install_rebuild_rollback(&installed.verified, &verified)?;
    recovery.merged_checkpoint(live.committed().ok_or(Error::NotReady)?.0)?;
    live.store_object(&raw)?;
    let receipt = protocol::Receipt {
        schema: 1,
        request: Object::of(&raw),
        installation: request.installation,
        completion: request.completion,
        pid: std::process::id(),
        observed_mono_ns: clock.sample()?.monotonic.nanos(),
    };
    receipt.verify(&request, std::process::id(), clock.sample()?.monotonic)?;
    let root = live.store_object(&receipt.bytes()?)?;
    receipt.verify(&request, std::process::id(), clock.sample()?.monotonic)?;
    installed.rollback = Some((body.clone(), root));
    Ok(root)
}
