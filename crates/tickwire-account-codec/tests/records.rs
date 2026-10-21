mod common;
use common::*;
use tickwire_account::*;
use tickwire_account_codec::Error;
use tickwire_account_codec::*;
use tickwire_venue as v;

fn records() -> Vec<RecoveryRecord> {
    let (mut account, cmd) = setup();
    let command = canonical(cmd.clone());
    let admission = account.admit(&command, 60).unwrap();
    vec![
        RecoveryRecord::Admit {
            command: Box::new(command.clone()),
            evaluated_at: 60,
            expected: admission.image(),
        },
        RecoveryRecord::VenueEvent(Box::new(event(fill(&cmd, "42", "0.001", "60000", "60")))),
        RecoveryRecord::Aborted {
            intent: admission.intent(),
            command_digest: command.digest(),
        },
        RecoveryRecord::FeesReconciled {
            order: admission.orders()[0].clone(),
            cumulative: f(0),
        },
        RecoveryRecord::UnattributedCashFlow {
            id: venue::text("flow-1"),
            delta: AssetAmount {
                key: key(2),
                amount: f(-1),
            },
        },
        RecoveryRecord::ExternalPosition {
            id: venue::text("position-1"),
            instrument: 1,
            side: v::PositionSide::Both,
            quantity: f(-1),
            entry_bound: units(60),
        },
        RecoveryRecord::TransferAsset {
            key: key(2),
            from: 0,
            to: 8,
            amount: units(1),
        },
        RecoveryRecord::TransferPosition {
            instrument: 1,
            side: v::PositionSide::Both,
            from: 0,
            to: 8,
            quantity: f(-1),
        },
    ]
}
#[test]
fn every_recovery_input_has_lossless_record_and_distinct_tag() {
    let mut hashes = std::collections::BTreeSet::new();
    for record in records() {
        let c = CanonicalRecord::new([7; 32], 1, record.clone()).unwrap();
        let out = CanonicalRecord::decode(c.bytes()).unwrap();
        assert_eq!(out.record(), &record);
        assert_eq!(out, c);
        assert!(hashes.insert(c.digest()));
    }
}
#[test]
fn envelope_and_admission_bindings_reject_substitution() {
    for record in records() {
        assert!(CanonicalRecord::new([0; 32], 1, record.clone()).is_err());
        assert!(CanonicalRecord::new([7; 32], 0, record.clone()).is_err());
        let a = CanonicalRecord::new([7; 32], 1, record.clone()).unwrap();
        assert_ne!(
            a.digest(),
            CanonicalRecord::new([8; 32], 1, record.clone())
                .unwrap()
                .digest()
        );
        assert_ne!(
            a.digest(),
            CanonicalRecord::new([7; 32], 2, record).unwrap().digest()
        );
    }
    let mut admit = records().remove(0);
    if let RecoveryRecord::Admit { expected, .. } = &mut admit {
        expected.command_digest = [5; 32];
    }
    assert!(CanonicalRecord::new([7; 32], 1, admit).is_err());
}
#[test]
fn truncated_corrupted_and_noncanonical_embedded_records_reject() {
    for record in records() {
        let c = CanonicalRecord::new([7; 32], 1, record).unwrap();
        for n in 0..c.bytes().len() {
            assert!(CanonicalRecord::decode(&c.bytes()[..n]).is_err());
        }
        let mut tail = c.bytes().to_vec();
        tail.push(0);
        assert_eq!(
            CanonicalRecord::decode(&tail).unwrap_err(),
            Error::TrailingData
        );
    }
    let mut admit = records().remove(0);
    if let RecoveryRecord::Admit { expected, .. } = &mut admit {
        expected.resources = vec![
            AssetAmount {
                key: key(1),
                amount: f(0)
            };
            MAX_ASSETS + 1
        ];
    }
    assert_eq!(
        CanonicalRecord::new([7; 32], 1, admit).unwrap_err(),
        Error::Bounds
    );
}

#[test]
fn schema_two_structure_event_is_lossless_inside_existing_ingress_record() {
    let (_, cmd) = setup();
    let mut event = fill(&cmd, "42", "0.001", "60000", "60");
    event.body = v::VenueEventBody::Structure(Box::new(v::StructureObservation {
        parent: v::StructureIdentity {
            structure_id: [71; 16],
            original_intent_id: cmd.context.request.intent_id,
            original_intent_exec_epoch: cmd.context.request.intent_exec_epoch,
            kind: v::StructureKind::Oto,
            client_id: venue::text("list-one"),
            venue_parent_id: Some(venue::text("1234")),
        },
        status: v::StructureStatus::Terminal,
        venue_status: venue::text("ALL_DONE"),
        children: v::BoundedList::new(vec![]).unwrap(),
    }));
    let event = v::CanonicalEvent::new(event).unwrap();
    let record = RecoveryRecord::IngressEvent {
        input_stream: [9; 16],
        input_sequence: 7,
        event: Box::new(event.clone()),
    };
    let canonical = CanonicalRecord::new([7; 32], 9, record.clone()).unwrap();
    let restored = CanonicalRecord::decode(canonical.bytes()).unwrap();
    assert_eq!(restored.record(), &record);
    let RecoveryRecord::IngressEvent {
        event: embedded, ..
    } = restored.record()
    else {
        unreachable!()
    };
    assert_eq!(embedded.bytes(), event.bytes());
}

#[test]
fn uncreated_algo_closure_tag16_roundtrips_and_binds_baseline() {
    use tickwire_account::*;
    let b = UncreatedAlgoClosed {
        scope: Scope {
            risk_domain_id: [1; 16],
            venue: 1,
            environment: tickwire_venue::Environment::Test,
            account: 2,
        },
        account_epoch: 1,
        ledger_sequence: 3,
        baseline_digest: [3; 32],
        history_digest: [4; 32],
        input_stream: [5; 16],
        through_sequence: 10,
        state_digest: [6; 32],
        coverage_digest: [7; 32],
        structure_id: [8; 16],
        intent: IntentKey {
            epoch: 2,
            intent_id: 3,
        },
        command_digest: [9; 32],
        venue_parent_id: tickwire_venue::VenueOrderId::new("42").unwrap(),
    };
    let record = RecoveryRecord::UncreatedAlgoClosed(Box::new(b));
    let bytes = tickwire_account_codec::CanonicalRecord::new([3; 32], 4, record.clone()).unwrap();
    assert_eq!(
        tickwire_account_codec::CanonicalRecord::decode(bytes.bytes()).unwrap(),
        bytes
    );
    assert!(tickwire_account_codec::CanonicalRecord::new([2; 32], 4, record).is_err());
}
