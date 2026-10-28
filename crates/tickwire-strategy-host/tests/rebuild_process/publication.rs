use super::*;
use std::collections::BTreeMap;
use tickwire_recovery::{BlobStore, StoreKey};
use tickwire_release::{
    Artifact, Binding, Capabilities, Member, Method, Operation, OrderKind, PositionMode, Product,
    ReleaseManifest, Target,
    files::Location,
    rebuild::{Plan, Plans, rollback},
    replay::Source,
};
pub fn write(
    host: &tickwire_strategy_host::rebuild::Hosted<NoRecovery>,
    worker: &Path,
    dir: &Path,
) -> (Operation, Member) {
    let b = host.capture_bootstrap();
    let captured = host.captured().unwrap();
    let w = captured.window();
    let member = Member {
        risk_domain: b.identity.risk_domain,
        exec_instance: b.report.peer.instance,
        exec_epoch: 1,
        old_instance: [19; 16],
        old_generation: 5,
        new_instance: b.identity.instance,
        new_generation: b.identity.generation,
        clock_boot_id: b.identity.clock.boot_id,
        clock_generation: 1,
        valid_until_ns: b.deadline_mono_ns,
    };
    let mut op = Operation {
        schema: 4,
        operation_id: [140; 16],
        operation_revision: 1,
        logical_strategy_id: 8,
        members: vec![member.clone()],
        artifacts_digest: [1; 32],
        checkpoint_digest: [2; 32],
        rollback_digest: [3; 32],
        clock_boot_id: b.identity.clock.boot_id,
        clock_generation: 1,
        valid_until_ns: b.deadline_mono_ns,
    };
    let binding = Binding {
        operation_id: op.operation_id,
        revision: 1,
        logical_strategy: 8,
        members_digest: op.members_digest().unwrap(),
    };
    let mut locations = vec![
        Location {
            role: Role::Binary,
            path: worker.to_owned(),
        },
        Location {
            role: Role::HostConfig,
            path: dir.join("host-input.json"),
        },
        Location {
            role: Role::Bootstrap,
            path: dir.join("runtime-bootstrap.json"),
        },
        Location {
            role: Role::StrategyConfig,
            path: dir.join("parameters.json"),
        },
        Location {
            role: Role::RecoveryTemplate,
            path: dir.join("recovery-template.json"),
        },
    ];
    // The Host input artifact records actual registered launch identity; it
    // never stands in for a captured, installed or ready process fact.
    super::write(&dir.join("host-input.json"), &b.bytes().unwrap());
    locations.sort_by(|a, b| a.role.cmp(&b.role));
    let files = locations
        .iter()
        .map(|l| Artifact {
            role: l.role.clone(),
            object: Object::of(&fs::read(&l.path).unwrap()),
        })
        .collect::<Vec<_>>();
    let capabilities = Capabilities {
        state_schema: V2,
        input_schema: 1,
        output_schema: 1,
        products: vec![Product::Spot],
        position_modes: vec![PositionMode::Spot],
        orders: vec![OrderKind::Limit],
        methods: vec![
            Method::Restore { schema: V2 },
            Method::Migrate {
                source_schema: 0x5457434e0001,
                target_schema: V2,
                method_id: 1,
            },
            Method::Rebuild {
                target_schema: V2,
                contract: Object::of(&w.contract().bytes().unwrap()).digest,
            },
        ],
    };
    let manifest = ReleaseManifest {
        schema: 1,
        binding: binding.clone(),
        targets: vec![Target {
            member: member.clone(),
            version: tickwire_recovery::digest(&serde_json::to_vec(&files).unwrap()),
            files: files.clone(),
            capabilities: capabilities.clone(),
        }],
    };
    let plans = Plans {
        schema: 1,
        binding: binding.clone(),
        members: vec![Plan {
            schema: 1,
            binding: binding.clone(),
            member: member.clone(),
            target: Source {
                logical_strategy: 8,
                instance: b.identity.instance,
                generation: b.identity.generation,
                binary_digest: b.identity.binary,
                state_schema: V2,
                input_stream: STREAM,
                clock: w.inputs().clock.clone(),
            },
            contract: Object::of(&w.contract().bytes().unwrap()),
            inputs: Object::of(&w.inputs().bytes().unwrap()),
            end: w.seal().end.clone(),
        }],
    };
    let rollback = rollback::Plans {
        schema: 1,
        binding,
        members: vec![rollback::MemberPlan {
            member: member.clone(),
            version: tickwire_recovery::digest(&serde_json::to_vec(&files).unwrap()),
            factory: w.contract().factory.clone(),
            files,
            capabilities,
            probe: rollback::Probe {
                instance: [7; 16],
                generation: 7,
                input_stream: [152; 16],
            },
            recovery: rollback::Recovery::ApprovedOldRebuild {
                contract: Object::of(&w.contract().bytes().unwrap()),
                inputs: Object::of(&w.inputs().bytes().unwrap()),
                end: w.seal().end.clone(),
            },
        }],
    };
    op.checkpoint_digest = Object::of(&plans.bytes().unwrap()).digest;
    op.artifacts_digest = Object::of(&tickwire_release::encode(&manifest).unwrap()).digest;
    op.rollback_digest = Object::of(&rollback.bytes().unwrap()).digest;
    let mut map = BTreeMap::new();
    for bytes in [
        plans.bytes().unwrap(),
        tickwire_release::encode(&manifest).unwrap(),
        rollback.bytes().unwrap(),
        w.contract().bytes().unwrap(),
        w.inputs().bytes().unwrap(),
        w.seal().bytes().unwrap(),
    ] {
        map.insert(tickwire_recovery::digest(&bytes), bytes);
    }
    plans
        .verify(&op, &member, w.contract(), w.inputs(), &manifest)
        .unwrap();
    rollback
        .verify(
            &op,
            &member,
            rollback::Forward {
                plans: &plans,
                contract: w.contract(),
                inputs: w.inputs(),
                manifest: &manifest,
            },
            |h| map.get(&h).cloned().ok_or(tickwire_release::Error::Missing),
        )
        .unwrap();
    let mut store = tickwire_recovery::FileBlobStore::open(dir.join("publication")).unwrap();
    for (h, b) in &map {
        store.put_sync(StoreKey::Object(*h), b).unwrap();
    }
    super::write(
        &dir.join("publication/objects.json"),
        &serde_json::to_vec(&map.values().map(|b| Object::of(b)).collect::<Vec<_>>()).unwrap(),
    );
    super::write(
        &dir.join("publication/forward-files.json"),
        &serde_json::to_vec(&locations).unwrap(),
    );
    super::write(
        &dir.join("publication/rollback-files.json"),
        &serde_json::to_vec(&locations).unwrap(),
    );
    (op, member)
}
