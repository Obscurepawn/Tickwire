mod common;
use common::*;
use tickwire_account::*;
use tickwire_account_codec::*;
use tickwire_venue as v;
fn input() -> v::CanonicalAccountInput {
    let (_, _, cmd) = futures_fixture();
    v::CanonicalAccountInput::new(v::AccountInput {
        identity: v::AccountIdentity {
            risk_domain_id: [1; 16],
            venue: 1,
            environment: v::Environment::Test,
            account: 2,
            market: v::AccountMarket::Usdm,
            wallet_id: 1,
        },
        body: v::AccountInputBody::Income(v::IncomeRecord {
            income_type: venue::text("FUNDING_FEE"),
            transaction_id: venue::text("3"),
            delta: v::AssetDelta {
                asset: cmd.context.instrument.settlement,
                amount: "-2".parse().unwrap(),
            },
            symbol: None,
            trade_id: None,
            venue_time_ms: 99,
            info: None,
        }),
    })
    .unwrap()
}
#[test]
fn canonical_account_input_and_baseline_roundtrip_and_replay_exactly() {
    let (s, c, _) = futures_fixture();
    let baseline = CanonicalBaseline::new(s.clone(), c.clone()).unwrap();
    let covered = input();
    let b = IncomeBaseline {
        scope: s.scope.clone(),
        account_epoch: s.account_epoch,
        ledger_sequence: s.ledger_sequence,
        wallet_id: 1,
        through_time_ms: 100,
        coverage_digest: [5; 32],
        baseline_digest: baseline.digest(),
        input_stream: [9; 16],
        through_sequence: 2,
        covered: vec![covered.clone()],
    };
    let mut later = covered.input().clone();
    let v::AccountInputBody::Income(row) = &mut later.body else {
        unreachable!()
    };
    row.transaction_id = venue::text("4");
    row.venue_time_ms = 101;
    let records = vec![
        RecoveryRecord::IncomeBaseline(Box::new(b)),
        RecoveryRecord::IngressAccountInput {
            input_stream: [9; 16],
            input_sequence: 3,
            input: Box::new(covered),
        },
        RecoveryRecord::IngressAccountInput {
            input_stream: [9; 16],
            input_sequence: 4,
            input: Box::new(v::CanonicalAccountInput::new(later).unwrap()),
        },
    ];
    for (n, r) in records.iter().enumerate() {
        let record = CanonicalRecord::new(baseline.digest(), n as u64 + 1, r.clone()).unwrap();
        assert_eq!(CanonicalRecord::decode(record.bytes()).unwrap(), record);
        let mut tail = record.bytes().to_vec();
        tail.push(0);
        assert!(CanonicalRecord::decode(&tail).is_err());
    }
    let a = Account::recover(s, c, &records).unwrap();
    assert_eq!(a.asset(key(2)).unwrap().total, units(998));
    assert_eq!(a.ledger_sequence(), 2);
    assert!(a.frozen());
}
