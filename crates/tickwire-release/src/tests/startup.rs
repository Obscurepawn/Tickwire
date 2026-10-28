use super::*;
use crate::receipt::{Kind, Process, Receipt};
use std::collections::BTreeMap;
use tickwire_account::Account;
use tickwire_account_codec::{CanonicalBaseline, InputSeal};
use tickwire_recovery::{HandoverSeal, InputRecord, digest, encode_inputs};

fn startup_graph(dependency: Vec<u8>) -> (Hash, BTreeMap<Hash, Vec<u8>>) {
    let baseline_bytes =
        include_str!("../../../tickwire-account-codec/tests/data/spot-baseline.hex")
            .trim()
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect::<Vec<_>>();
    let baseline = CanonicalBaseline::decode(&baseline_bytes).unwrap();
    let account =
        Account::recover(baseline.snapshot().clone(), baseline.catalog().clone(), &[]).unwrap();
    let account_input = InputSeal::new(baseline.digest()).unwrap();
    let mut callback = b"TWAI0001".to_vec();
    callback.push(0);
    callback.extend(1u64.to_le_bytes());
    callback.extend(0u64.to_le_bytes());
    callback.extend((baseline_bytes.len() as u32).to_le_bytes());
    callback.extend(&baseline_bytes);
    let inputs = encode_inputs(
        [30; 16],
        1,
        [0; 32],
        &[InputRecord {
            kind: 2,
            payload: &callback,
        }],
    )
    .unwrap();
    let (mut op, mut manifest, mut migrations, mut rollback) = fixture();
    op.schema = 3;
    op.members[0].risk_domain = baseline.snapshot().scope.risk_domain_id;
    let member = op.members[0].clone();
    let mut binding = manifest.binding.clone();
    binding.members_digest = op.members_digest().unwrap();
    manifest.binding = binding.clone();
    migrations.binding = binding.clone();
    rollback.binding = binding;
    manifest.targets[0].member = member.clone();
    migrations.members[0].member = member.clone();
    rollback.members[0].member = member.clone();
    let orders = account.strategy_orders_digest(op.logical_strategy_id);
    let mut checkpoint = b"TWHS0001".to_vec();
    checkpoint.extend(member.old_instance);
    for value in [
        member.old_generation,
        op.logical_strategy_id,
        0,
        account.ledger_sequence(),
    ] {
        checkpoint.extend(value.to_le_bytes());
    }
    checkpoint.extend(orders);
    checkpoint.extend(1u64.to_le_bytes());
    checkpoint.push(1);
    let dep_digest = digest(&dependency);
    let seal = HandoverSeal {
        version: 1,
        owner: member.old_instance,
        instance: member.old_instance,
        generation: member.old_generation,
        logical_strategy: op.logical_strategy_id,
        report_position: 0,
        ledger_sequence: account.ledger_sequence(),
        orders_digest: orders,
        checkpoint_digest: digest(&checkpoint),
        checkpoint_bytes: checkpoint.len() as u64,
        account_input_digest: account_input.digest(),
        account_input_bytes: account_input.object_bytes().len() as u64,
        input_digest: digest(&inputs.bytes),
        input_bytes: inputs.bytes.len() as u64,
        dependencies: (2..=11).map(|kind| (kind, dep_digest)).collect(),
        preflight_receipt_digest: None,
        replay_restore_digest: None,
        replay_installation_digest: None,
        replay_rollback_digest: None,
        rebuild_installation_digest: None,
        rebuild_rollback_digest: None,
    };
    let seal_bytes = seal.bytes().unwrap();
    let migration = &mut migrations.members[0];
    migration.source_checkpoint = Object::of(&checkpoint);
    migration.source_proof = Object::of(&seal_bytes);
    let boundary = inputs.boundary;
    migration.inputs = Some(InputPrefix {
        object: Object::of(&inputs.bytes),
        stream: boundary.stream,
        first_sequence: boundary.first_sequence,
        previous_hash: boundary.previous_hash,
        last_sequence: boundary.last_sequence,
        last_hash: boundary.last_hash,
    });
    let bundle = bundle(&mut op, &manifest, &migrations, &rollback);
    let output = b"restored state".to_vec();
    let receipt = Receipt {
        schema: 1,
        kind: Kind::Restore,
        roots: bundle.roots(),
        member: member.clone(),
        logical_strategy: op.logical_strategy_id,
        process: Process {
            instance: member.new_instance,
            generation: member.new_generation,
            pid: 100,
            uid: 1000,
            gid: 1000,
        },
        binary_digest: manifest.targets[0].files[0].object.digest,
        effective_bootstrap_digest: [31; 32],
        source_proof: digest(&seal_bytes),
        source_checkpoint: digest(&checkpoint),
        source_schema: 1,
        method: Method::Restore { schema: 1 },
        output_schema: 1,
        output: Object::of(&output),
        source_report_position: 0,
    }
    .bytes()
    .unwrap();
    let completion = replay::startup::Completion {
        schema: 1,
        operation: op,
        member,
        restored: Object::of(&receipt),
    }
    .bytes()
    .unwrap();
    let root = digest(&completion);
    let objects = [
        dependency,
        completion,
        receipt,
        output,
        checkpoint,
        seal_bytes,
        inputs.bytes,
        account_input.object_bytes().to_vec(),
        encode(&manifest).unwrap(),
        encode(&migrations).unwrap(),
        encode(&rollback).unwrap(),
    ]
    .into_iter()
    .map(|bytes| (digest(&bytes), bytes))
    .collect();
    (root, objects)
}

#[test]
fn schema3_startup_accepts_recovery_dependency_larger_than_replay_message() {
    let dependency = vec![0x5a; replay::MAX_BYTES + 1];
    let expected = Object::of(&dependency);
    let (root, mut objects) = startup_graph(dependency);
    let verified =
        replay::startup::verify(root, |h| objects.get(&h).cloned().ok_or(Error::Missing)).unwrap();
    assert!(
        verified
            .objects
            .iter()
            .any(|bytes| Object::of(bytes) == expected)
    );
    // A larger accepted object still has to match the authenticated digest.
    objects.get_mut(&expected.digest).unwrap()[0] ^= 1;
    assert!(matches!(
        replay::startup::verify(root, |h| objects.get(&h).cloned().ok_or(Error::Missing)),
        Err(Error::Identity)
    ));
}

#[test]
fn schema3_startup_rejects_dependency_beyond_recovery_object_budget() {
    let dependency = vec![0x5a; tickwire_recovery::MAX_OBJECT_BYTES + 1];
    let (root, objects) = startup_graph(dependency);
    assert!(matches!(
        replay::startup::verify(root, |h| objects.get(&h).cloned().ok_or(Error::Missing)),
        Err(Error::Identity)
    ));
}
