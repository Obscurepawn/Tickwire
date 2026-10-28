use super::*;
use tickwire_release::{
    rebuild::{Contract, Inputs, Plans, install, probe, rollback},
    replay,
};
fn object(value: &tickwire_lifecycle::ReplayObject) -> Object {
    Object {
        digest: value.digest,
        bytes: value.bytes,
    }
}
fn load(s: &Scenario, hash: [u8; 32]) -> tickwire_release::Result<Vec<u8>> {
    let bytes = fs::read(
        s.root
            .join("next/recovery")
            .join(format!("{}.bin", hex(&hash))),
    )
    .map_err(|_| tickwire_release::Error::Missing)?;
    if bytes.len() > tickwire_recovery::MAX_OBJECT_BYTES
        || tickwire_recovery::digest(&bytes) != hash
    {
        return Err(tickwire_release::Error::Identity);
    }
    Ok(bytes)
}
pub(super) fn verify(
    s: &Scenario,
    prepared: &tickwire_lifecycle::RebuildPrepared,
    pid: u32,
    final_cut: bool,
) -> Result<Value> {
    let ready = json(&s.root.join(format!(
        "next/reports/CONSUMED-{}-6.json",
        hex(&NEW_INSTANCE)
    )))?;
    let hash = serde_json::from_value(ready["input_seal_digest"].clone())?;
    let seal = tickwire_recovery::HandoverSeal::decode(&load(s, hash)?)?;
    if seal.version != 4
        || seal.instance != NEW_INSTANCE
        || seal.generation != 6
        || seal.logical_strategy != 8
        || ready["worker_pid"] != pid
        || ready["account_position"] != seal.report_position
        || ready["checkpoint_digest"] != json!(seal.checkpoint_digest)
        || ready["orders_digest"] != json!(seal.orders_digest)
        || ready["ledger_sequence"] != seal.ledger_sequence
        || seal.rebuild_installation_digest != Some(prepared.roots.target_install.digest)
        || seal.rebuild_rollback_digest != Some(prepared.roots.rollback_completion.digest)
    {
        return Err("schema4 final READY binding".into());
    }
    let checkpoint = load(s, seal.checkpoint_digest)?;
    let inputs = load(s, seal.input_digest)?;
    install::verify_seal(&seal, &checkpoint, &inputs, |h| load(s, h))?;
    let installed = install::verify(object(&prepared.roots.target_install), |h| load(s, h))?;
    let request = installed.request();
    let installation = installed.installation();
    let plans = Plans::decode(&load(s, request.plans.digest)?)?;
    let contract = Contract::decode(&load(s, prepared.roots.contract.digest)?)?;
    let captured_inputs = Inputs::decode(&load(s, prepared.roots.inputs.digest)?)?;
    let manifest = serde_json::from_slice(&load(s, request.manifest.digest)?)?;
    let capture: tickwire_strategy_sdk::capture::Receipt =
        tickwire_strategy_sdk::capture::Receipt::decode(&load(s, request.capture_receipt.digest)?)?;
    let window = tickwire_strategy_sdk::replay::rebuild::capture::store::read_back(
        &s.root.join("capture/output"),
        &capture.completion,
    )?;
    window.validate_plan(&plans.members[0])?;
    let cursors = |rows: &[replay::Cursor]| {
        rows.iter()
            .map(|c| tickwire_lifecycle::RebuildCursor {
                identity: c.stream.identity,
                generation: c.stream.generation,
                sequence: c.sequence,
                content_digest: c.content_digest,
            })
            .collect()
    };
    let end = &window.seal().end;
    let frontier = tickwire_lifecycle::RebuildFrontier {
        now_ns: end.now_ns,
        rows: end.rows,
        markets: cursors(&end.markets),
        accounts: cursors(&end.accounts),
        external: cursors(&end.external),
    };
    if request.operation != prepared.operation
        || request.member != prepared.member
        || request.plans != object(&prepared.roots.plans)
        || Object::of(&contract.bytes()?) != object(&prepared.roots.contract)
        || Object::of(&captured_inputs.bytes()?) != object(&prepared.roots.inputs)
        || prepared.roots.rollback_plans.digest != request.operation.rollback_digest
        || installation.warm_completion != object(&prepared.roots.warm_completion)
        || installation.pid != pid
        || capture.pid != pid
        || prepared.end != frontier
        || capture.completion
            != tickwire_strategy_sdk::replay::rebuild::capture::store::Completion::decode(&load(
                s,
                request.capture_completion.digest,
            )?)?
    {
        return Err("schema4 seven roots or same PID binding".into());
    }
    let rollback = probe::Verified::verify(
        object(&prepared.roots.rollback_completion),
        &prepared.operation,
        &prepared.member,
        rollback::Forward {
            plans: &plans,
            contract: &contract,
            inputs: &captured_inputs,
            manifest: &manifest,
        },
        |h| load(s, h),
    )?;
    let probe = rollback.completion();
    if probe.pid == pid
        || probe.instance != [7; 16]
        || probe.generation != 7
        || !probe.success
        || Path::new(&format!("/proc/{}", probe.pid)).exists()
    {
        return Err("rollback is not an independently exited old child".into());
    }
    let retained = replay::retained::decode(&inputs)?;
    if retained.current().source != installation.target
        || Object::of(&retained.anchor().checkpoint().bytes()?) != installation.anchor
    {
        return Err("rebuild retained source or genesis changed".into());
    }
    retained.tail_from(installed.checkpoint())?;
    let c = crate::flavor::CountingState::decode(
        &tickwire_strategy_sdk::replay::application::business_bytes(
            &installed.checkpoint().business,
        )?,
        tickwire_strategy_example::V2,
        true,
    )?;
    let d = crate::flavor::CountingState::decode(
        checkpoint.get(96..).ok_or("final checkpoint")?,
        tickwire_strategy_example::V2,
        true,
    )?;
    crate::flavor::counting_progress(
        &c,
        &d,
        if final_cut {
            crate::flavor::ProgressPhase::Final
        } else {
            crate::flavor::ProgressPhase::Installed
        },
    )?;
    let old = crate::flavor::CountingState::decode(
        &tickwire_strategy_sdk::replay::application::business_bytes(
            &rollback.checkpoint().business,
        )?,
        tickwire_strategy_example::V1,
        true,
    )?;
    if old.action != c.action || old.observations != c.observations || old.reports != c.reports {
        return Err("old rebuild did not consume the same window".into());
    }
    if final_cut && retained.current().cut.sequence <= installed.checkpoint().cut.sequence {
        return Err("missing actual post-install continuation".into());
    }
    Ok(
        json!({"operation_schema":4,"capture_pid":pid,"target_pid":pid,"rollback_pid":probe.pid,"rollback_exited":true,"installation_digest":prepared.roots.target_install.digest,"rollback_digest":prepared.roots.rollback_completion,"fixed_roots":prepared.roots,"window_end":prepared.end,"target_install_sequence":installed.checkpoint().cut.sequence,"target_final_sequence":retained.current().cut.sequence,"prefix_verified":true,"action_remains_consumed":true,"custom_counters":{"target_at_c":c,"target_d":d,"old_probe":old}}),
    )
}
pub(crate) fn check_export(
    s: &Scenario,
    exported: &Path,
    report: &Value,
    count: u64,
) -> Result<()> {
    let order = report["orders"]
        .as_array()
        .and_then(|v| v.first())
        .ok_or("order")?;
    if order["origin_instance"] != hex(&[2; 16])
        || order["origin_generation"] != 5
        || order["manager_instance"] != hex(&NEW_INSTANCE)
        || order["manager_generation"] != 6
    {
        return Err("schema4 late fill ownership".into());
    }
    let records = (1..=count).map(|p| {
        tickwire_account_codec::CanonicalRecord::decode(&read(
            &exported.join("records").join(format!("{p:020}.cbor")),
            1024 * 1024,
        )?)
        .map_err(|e| format!("record {e:?}").into())
    });
    let (position, hash) = crate::handover::check_handover_records(records)?;
    let coordinator = json(&s.root.join("handover/coordinator.json"))?;
    let operation = serde_json::from_value(coordinator["coordinator"]["operation"].clone())?;
    let public =
        serde_json::from_value(coordinator["coordinator"]["members"][0]["public_key"].clone())?;
    let mut prepared = None;
    let mut phases = vec![];
    for path in crate::handover::json_paths(&s.root.join("handover/receipts"))? {
        let signed: tickwire_lifecycle::SignedReceipt =
            serde_json::from_slice(&read(&path, 8 * 1024 * 1024)?)?;
        if serde_json::from_slice::<tickwire_lifecycle::RebuildPrepared>(&signed.payload).is_ok() {
            prepared = Some(tickwire_lifecycle::verify_rebuild_prepared(
                &tickwire_lifecycle::SignedRebuildPrepared {
                    payload: signed.payload,
                    signature: signed.signature,
                },
                public,
                &operation,
                driver::now(s)?,
            )?);
            continue;
        }
        let receipt =
            tickwire_lifecycle::verify_receipt(&signed, public, &operation, driver::now(s)?)?;
        if receipt.handover_sequence != position || receipt.handover_digest != hash {
            return Err("schema4 signed receipt differs from account WAL".into());
        }
        phases.push(receipt.phase);
    }
    if ![
        tickwire_lifecycle::AppliedPhase::Committed,
        tickwire_lifecycle::AppliedPhase::Released,
    ]
    .iter()
    .all(|p| phases.contains(p))
    {
        return Err("missing schema4 applied receipts".into());
    }
    let captured = tickwire_strategy_sdk::capture::Receipt::decode(&read(
        &s.root.join("handover/captured.json"),
        2 * 1024 * 1024,
    )?)?;
    let proof = verify(s, &prepared.ok_or("schema4 prepared")?, captured.pid, true)?;
    crate::generate::json_file(
        &s.root.join("handover/preflight-final-verified.json"),
        &proof,
    )
}
