use super::*;
use std::collections::BTreeMap;
use tickwire_recovery::{BlobStore, StoreKey};
use tickwire_release::{Binding, Method, ReleaseManifest, Target, rebuild::*, replay};
use tickwire_strategy_sdk::replay::rebuild::capture::{VerifiedWindow, store};
use tickwire_strategy_sdk::{registered::Registration, restore::RestoreFactory};

pub(super) struct Captured {
    pub receipt: tickwire_strategy_sdk::capture::Receipt,
    pub window: VerifiedWindow,
}
pub(super) fn captured(s: &Scenario, exec_pid: u32) -> Result<Option<Captured>> {
    let output = s.root.join("capture/output");
    let marker = output.join("CAPTURE_WINDOW.json");
    if !marker.is_file() {
        return Ok(None);
    }
    let completion = store::Completion::decode(&read(&marker, 16 * 1024 * 1024)?)?;
    let window = store::read_back(&output, &completion)?;
    let bootstrap: tickwire_strategy_sdk::capture::Bootstrap = serde_json::from_slice(&read(
        &s.root.join("capture/bootstrap.json"),
        2 * 1024 * 1024,
    )?)?;
    for entry in fs::read_dir(&output)? {
        let path = entry?.path();
        if !path
            .file_name()
            .is_some_and(|n| n.to_string_lossy().starts_with("object-"))
        {
            continue;
        }
        let bytes = read(&path, 16 * 1024 * 1024)?;
        let Ok(receipt) = tickwire_strategy_sdk::capture::Receipt::decode(&bytes) else {
            continue;
        };
        if receipt.completion != completion {
            return Err("capture receipt window changed".into());
        }
        let request = tickwire_strategy_sdk::capture::SealRequest::decode(&read(
            &output.join(format!("object-{}", hex(&receipt.request.digest))),
            2 * 1024 * 1024,
        )?)?;
        receipt.verify(
            &bootstrap,
            &request,
            receipt.pid,
            super::driver::now(s)?,
            &window,
        )?;
        let path = tickwire_md::server::admission_path(
            &s.root.join("md/admissions"),
            receipt.pid,
            bootstrap.market.request_id,
        )
        .map_err(|e| e.to_string())?;
        let admission: tickwire_md::server::Admission =
            serde_json::from_slice(&read(&path, 2 * 1024 * 1024)?)?;
        if admission.pid != receipt.pid
            || admission.supervisor_pid != exec_pid
            || admission.client.endpoint.local.identity()
                != bootstrap.market.endpoint.local.identity()
            || !Path::new(&format!("/proc/{}", receipt.pid)).exists()
        {
            return Err("capture PID differs from actual admitted child".into());
        }
        return Ok(Some(Captured { receipt, window }));
    }
    Ok(None)
}
pub(super) fn prepare(s: &Scenario, captured: &Captured) -> Result<tickwire_lifecycle::Operation> {
    let r = &s.root;
    let deployment: tickwire_app::config::Deployment =
        serde_json::from_value(json(&s.app_config)?)?;
    let member = crate::handover::member(&deployment, s.valid_until_ns);
    let mut operation = tickwire_lifecycle::Operation {
        schema: 4,
        operation_id: [140; 16],
        operation_revision: 1,
        logical_strategy_id: 8,
        members: vec![member.clone()],
        artifacts_digest: [1; 32],
        checkpoint_digest: [2; 32],
        rollback_digest: [3; 32],
        clock_boot_id: s.boot,
        clock_generation: 1,
        valid_until_ns: s.valid_until_ns,
    };
    let binding = Binding {
        operation_id: operation.operation_id,
        revision: 1,
        logical_strategy: 8,
        members_digest: operation.members_digest()?,
    };
    let window = &captured.window;
    let contract = window.contract();
    let inputs = window.inputs();
    let seal = window.seal();
    let b: Bootstrap = serde_json::from_value(json(&r.join("next/bootstrap.json"))?)?;
    let h: tickwire_app::hosted::HostConfig =
        serde_json::from_value(json(&r.join("next/host.json"))?)?;
    let (files, locations) = crate::preflight::artifacts(
        &h,
        r.join("next/host.json"),
        r.join("next/bootstrap.json"),
        crate::flavor::config_path(s.kind, &r.join("next")),
        b.recovery_template.as_deref(),
    )?;
    let mut capabilities = crate::flavor::capabilities(s.kind, &b)?;
    capabilities.methods.push(Method::Rebuild {
        target_schema: contract.state_schema,
        contract: Object::of(&contract.bytes()?).digest,
    });
    let manifest = ReleaseManifest {
        schema: 1,
        binding: binding.clone(),
        targets: vec![Target {
            member: member.clone(),
            version: tickwire_recovery::digest(&serde_json::to_vec(&files)?),
            files,
            capabilities,
        }],
    };
    let plans = Plans {
        schema: 1,
        binding: binding.clone(),
        members: vec![Plan {
            schema: 1,
            binding: binding.clone(),
            member: member.clone(),
            target: replay::Source {
                logical_strategy: 8,
                instance: NEW_INSTANCE,
                generation: 6,
                binary_digest: contract.binary,
                state_schema: contract.state_schema,
                input_stream: [134; 16],
                clock: inputs.clock.clone(),
            },
            contract: Object::of(&contract.bytes()?),
            inputs: Object::of(&inputs.bytes()?),
            end: seal.end.clone(),
        }],
    };
    window.validate_plan(&plans.members[0])?;
    let app = json(&s.app_config)?;
    let old_host: tickwire_app::hosted::HostConfig =
        serde_json::from_value(app["strategy"]["members"][0]["strategy"]["config"].clone())?;
    configure::replace(
        &r.join("rollback/old-host.json"),
        &serde_json::to_vec(&old_host)?,
    )?;
    let old_boot: Bootstrap = serde_json::from_value(json(&s.worker_bootstrap)?)?;
    // The rollback is explicitly approved to rebuild from the same observed
    // market/account rows, with the old registered factory and old parameters.
    let old_config = old_boot.factory.as_ref().ok_or("old factory")?;
    let old_parameters = read(&old_config.configuration_file, 2 * 1024 * 1024)?;
    let mut old_contract = contract.clone();
    old_contract.binary = old_boot.market.local.build_hash;
    old_contract.state_schema = tickwire_strategy_example::V1;
    old_contract.config = Object::of(&old_parameters);
    let mut old_inputs = inputs.clone();
    old_inputs.config = replay::VersionedBytes {
        revision: old_config.revision,
        bytes: old_parameters,
    };
    let target = tickwire_strategy_sdk::restore::Target {
        instance: old_boot.market.local.instance,
        generation: old_boot.market.local.generation,
        logical_strategy: 8,
        command: None,
        minimum_bid_ticks: 0,
        enabled: false,
    };
    let mut old_capabilities = tickwire_strategy_example::Registration::v1()
        .configure(&target, &old_inputs.config)?
        .capabilities();
    old_capabilities.methods.push(Method::Rebuild {
        target_schema: old_contract.state_schema,
        contract: Object::of(&old_contract.bytes()?).digest,
    });
    let (old_files, old_locations) = crate::preflight::artifacts(
        &old_host,
        r.join("rollback/old-host.json"),
        s.worker_bootstrap.clone(),
        old_config.configuration_file.clone(),
        None,
    )?;
    let rollback = rollback::Plans {
        schema: 1,
        binding,
        members: vec![rollback::MemberPlan {
            member: member.clone(),
            version: tickwire_recovery::digest(&serde_json::to_vec(&old_files)?),
            factory: old_contract.factory.clone(),
            files: old_files,
            capabilities: old_capabilities,
            probe: rollback::Probe {
                instance: [7; 16],
                generation: 7,
                input_stream: [152; 16],
            },
            recovery: rollback::Recovery::ApprovedOldRebuild {
                contract: Object::of(&old_contract.bytes()?),
                inputs: Object::of(&old_inputs.bytes()?),
                end: seal.end.clone(),
            },
        }],
    };
    let mut map = BTreeMap::new();
    for bytes in [
        plans.bytes()?,
        tickwire_release::encode(&manifest)?,
        rollback.bytes()?,
        old_contract.bytes()?,
        old_inputs.bytes()?,
        contract.bytes()?,
        inputs.bytes()?,
        seal.bytes()?,
    ] {
        map.insert(tickwire_recovery::digest(&bytes), bytes);
    }
    operation.checkpoint_digest = Object::of(&plans.bytes()?).digest;
    operation.artifacts_digest = Object::of(&tickwire_release::encode(&manifest)?).digest;
    operation.rollback_digest = Object::of(&rollback.bytes()?).digest;
    plans.verify(&operation, &member, contract, inputs, &manifest)?;
    rollback.verify(
        &operation,
        &member,
        rollback::Forward {
            plans: &plans,
            contract,
            inputs,
            manifest: &manifest,
        },
        |hash| {
            map.get(&hash)
                .cloned()
                .ok_or(tickwire_release::Error::Identity)
        },
    )?;
    let mut store = tickwire_recovery::FileBlobStore::open(r.join("publication"))?;
    for (hash, bytes) in &map {
        store.put_sync(StoreKey::Object(*hash), bytes)?;
    }
    publish(
        &r.join("publication/objects.json"),
        &serde_json::to_vec(&map.values().map(|v| Object::of(v)).collect::<Vec<_>>())?,
    )?;
    publish(
        &r.join("publication/forward-files.json"),
        &serde_json::to_vec(&locations)?,
    )?;
    publish(
        &r.join("publication/rollback-files.json"),
        &serde_json::to_vec(&old_locations)?,
    )?;
    publish(
        &r.join("handover/captured.json"),
        &captured.receipt.bytes()?,
    )?;
    Ok(operation)
}

// Read the command actually admitted by execution. The factory refreshes time
// and market marks before submission, so its static template is not this root.
pub(super) fn admitted_command(
    s: &Scenario,
    ready: &Value,
) -> Result<tickwire_venue::CanonicalCommand> {
    let load = |hash: [u8; 32]| -> Result<Vec<u8>> {
        let b = read(
            &s.root
                .join("strategy/recovery")
                .join(format!("{}.bin", hex(&hash))),
            64 * 1024 * 1024,
        )?;
        if tickwire_recovery::digest(&b) != hash {
            return Err("old recovery hash".into());
        }
        Ok(b)
    };
    let seal = tickwire_recovery::HandoverSeal::decode(&load(serde_json::from_value(
        ready["input_seal_digest"].clone(),
    )?)?)?;
    if seal.report_position != ready["account_position"].as_u64().ok_or("old position")? {
        return Err("old account cut".into());
    }
    let input = tickwire_recovery::EncodedInputs::decode(load(seal.input_digest)?)?;
    action_from_inputs(&input)
}
pub(super) fn action_from_inputs(
    input: &tickwire_recovery::EncodedInputs,
) -> Result<tickwire_venue::CanonicalCommand> {
    let bytes = &input.bytes;
    let count = u32::from_be_bytes(bytes.get(32..36).ok_or("input count")?.try_into()?);
    let mut offset = 68usize;
    let mut admitted = None;
    for _ in 0..count {
        let kind = *bytes.get(offset + 8).ok_or("input kind")?;
        let len = u32::from_be_bytes(
            bytes
                .get(offset + 9..offset + 13)
                .ok_or("input length")?
                .try_into()?,
        ) as usize;
        let end = offset
            .checked_add(13)
            .and_then(|v| v.checked_add(len))
            .ok_or("input overflow")?;
        let payload = bytes.get(offset + 13..end).ok_or("input truncated")?;
        offset = end;
        if kind != 2 || payload.len() < 29 || &payload[..8] != b"TWAI0001" || payload[8] != 1 {
            continue;
        }
        let record = tickwire_account_codec::CanonicalRecord::decode(&payload[29..])
            .map_err(|e| format!("actual account record {e:?}"))?;
        if let tickwire_account::RecoveryRecord::Admit { command, .. } = record.record() {
            if command.command().context.strategy.logical_strategy_id != 8 || admitted.is_some() {
                return Err("expected exactly one actual accepted action".into());
            }
            admitted = Some(command.as_ref().clone());
        }
    }
    admitted.ok_or_else(|| "old account cut has no admission".into())
}
