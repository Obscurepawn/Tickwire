mod common;
use common::*;
use tickwire_account::*;
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
fn tags_one_through_eight_preserve_committed_encoding() {
    let expected =
        include_str!("data/legacy-records.hex");
    for (record, hex) in records().into_iter().zip(expected.lines()) {
        let bytes: Vec<u8> = hex
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|c| u8::from_str_radix(std::str::from_utf8(c).unwrap(), 16).unwrap())
            .collect();
        let encoded = CanonicalRecord::new([7; 32], 1, record).unwrap();
        assert_eq!(encoded.bytes(), bytes);
        assert_eq!(CanonicalRecord::decode(&bytes).unwrap(), encoded);
    }
}
