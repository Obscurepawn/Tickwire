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
