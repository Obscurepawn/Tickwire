#![forbid(unsafe_code)]

mod common;
use common::*;
use tickwire_account::{Account, RecoveryRecord};
use tickwire_account_codec::*;

#[test]
fn in_memory_stateless_baseline_and_record_replay_restores_spot_and_usdm_resources() {
    for (snapshot, catalog, cmd) in [fixture(), futures_fixture()] {
        let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
        let mut live =
            Account::from_snapshot(baseline.snapshot().clone(), baseline.catalog().clone())
                .unwrap();
        let command = canonical(cmd.clone());
        let admission = live.admit(&command, 60).unwrap();
        let fill = if cmd.context.request.product == tickwire_venue::Product::Spot {
            event(fill(&cmd, "stored-trade", "0.0004", "50000", "20"))
        } else {
            event(fill(&cmd, "stored-trade", "0.4", "100", "40"))
        };
        live.apply_event(&fill).unwrap();

        let inputs = [
            RecoveryRecord::Admit {
                command: Box::new(command),
                evaluated_at: 60,
                expected: admission.image(),
            },
            RecoveryRecord::VenueEvent(Box::new(fill)),
        ];

        let baseline_bytes = baseline.bytes().to_vec();
        let mut seal_write = InputSeal::new(baseline.digest()).unwrap();
        let mut wire_records = Vec::with_capacity(inputs.len());
        for (index, input) in inputs.into_iter().enumerate() {
            let encoded = CanonicalRecord::new(baseline.digest(), index as u64 + 1, input).unwrap();
            seal_write.apply(&encoded).unwrap();
            wire_records.push((index as u64 + 1, encoded.bytes().to_vec()));
        }

        let loaded = CanonicalBaseline::decode(&baseline_bytes).unwrap();
        assert_eq!(loaded.digest(), baseline.digest());

        let mut seal_read = InputSeal::new(loaded.digest()).unwrap();
        let mut decoded = Vec::with_capacity(wire_records.len());
        for (expected_seq, payload) in &wire_records {
            let encoded = CanonicalRecord::decode(payload).unwrap();
            assert_eq!(encoded.sequence(), *expected_seq);
            assert_eq!(encoded.baseline(), loaded.digest());
            seal_read.apply(&encoded).unwrap();
            decoded.push(encoded.record().clone());
        }
        assert_eq!(seal_read.position(), 2);
        assert_eq!(seal_read.digest(), seal_write.digest());

        let restored = Account::recover(
            loaded.snapshot().clone(),
            loaded.catalog().clone(),
            &decoded,
        )
        .unwrap();
        assert!(restored.frozen());
        assert_eq!(restored.asset(key(1)).unwrap(), live.asset(key(1)).unwrap());
        assert_eq!(restored.asset(key(2)).unwrap(), live.asset(key(2)).unwrap());
        assert_eq!(
            restored.admission(admission.intent()).unwrap().image(),
            admission.image()
        );
        assert!(restored.order(&admission.orders()[0]).unwrap().unknown);
        if cmd.context.request.product != tickwire_venue::Product::Spot {
            assert_eq!(
                restored.venue_shadow(1).unwrap(),
                live.venue_shadow(1).unwrap()
            );
            assert_eq!(
                restored.position(12, tickwire_venue::PositionSide::Both),
                live.position(12, tickwire_venue::PositionSide::Both)
            );
        }
    }
}
