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

#[test]
fn closure_tag14_roundtrips_and_replays_without_reapplying_funding() {
    let (s, c, _) = futures_fixture();
    let baseline = CanonicalBaseline::new(s.clone(), c.clone()).unwrap();
    let b = IncomeBaseline {
        scope: s.scope.clone(),
        account_epoch: 1,
        ledger_sequence: 0,
        wallet_id: 1,
        through_time_ms: 100,
        coverage_digest: [5; 32],
        baseline_digest: baseline.digest(),
        input_stream: [9; 16],
        through_sequence: 2,
        covered: vec![],
    };
    let mut later = input().input().clone();
    let v::AccountInputBody::Income(row) = &mut later.body else {
        unreachable!()
    };
    row.venue_time_ms = 101;
    let mut records = vec![
        RecoveryRecord::IncomeBaseline(Box::new(b)),
        RecoveryRecord::IngressAccountInput {
            input_stream: [9; 16],
            input_sequence: 3,
            input: Box::new(v::CanonicalAccountInput::new(later).unwrap()),
        },
    ];
    let a = Account::recover(s.clone(), c.clone(), &records).unwrap();
    let binding = AccountInputsReconciled {
        scope: s.scope.clone(),
        account_epoch: 1,
        ledger_sequence: a.ledger_sequence(),
        baseline_digest: baseline.digest(),
        history_digest: [6; 32],
        input_stream: [9; 16],
        through_sequence: 9,
        state_digest: a
            .account_inputs_state_digest(baseline.digest(), [6; 32])
            .unwrap(),
        coverage_digest: [7; 32],
    };
    let r = RecoveryRecord::AccountInputsReconciled(Box::new(binding));
    let encoded = CanonicalRecord::new(baseline.digest(), 3, r.clone()).unwrap();
    assert!(encoded.bytes().windows(3).any(|w| w == [0x82, 14, 0x89]));
    assert_eq!(CanonicalRecord::decode(encoded.bytes()).unwrap(), encoded);
    assert!(CanonicalRecord::new([4; 32], 3, r.clone()).is_err());
    records.push(r);
    let a = Account::recover(s, c, &records).unwrap();
    assert_eq!(a.asset(key(2)).unwrap().total, units(998));
    assert_eq!(a.ledger_sequence(), 3);
    assert_eq!(a.unresolved_account_inputs(), 0);
    assert!(a.frozen());
}

#[test]
fn association_tag15_roundtrips_and_replays_exactly_once() {
    let (s, c, cmd) = futures_fixture();
    let baseline = CanonicalBaseline::new(s.clone(), c.clone()).unwrap();
    let b = IncomeBaseline {
        scope: s.scope.clone(),
        account_epoch: 1,
        ledger_sequence: 0,
        wallet_id: 1,
        through_time_ms: 100,
        coverage_digest: [5; 32],
        baseline_digest: baseline.digest(),
        input_stream: [9; 16],
        through_sequence: 2,
        covered: vec![],
    };
    let mut row = input().input().clone();
    let v::AccountInputBody::Income(income) = &mut row.body else {
        unreachable!()
    };
    income.venue_time_ms = 101;
    income.symbol = Some(cmd.context.instrument.symbol.clone());
    let mut observed = row.clone();
    observed.body = v::AccountInputBody::Observation(v::AccountObservation {
        reason: venue::text("FUNDING_FEE"),
        funding_symbol: Some(cmd.context.instrument.symbol),
        event_time_ms: 101,
        transaction_time_ms: Some(101),
        positions: v::BoundedList::new(vec![]).unwrap(),
        balances: v::BoundedList::new(vec![v::WalletObservation {
            asset: cmd.context.instrument.settlement,
            wallet_balance: "998".parse().unwrap(),
            cross_wallet_balance: "998".parse().unwrap(),
            balance_change: "-2".parse().unwrap(),
        }])
        .unwrap(),
    });
    let observation = v::CanonicalAccountInput::new(observed).unwrap();
    let mut records = vec![
        RecoveryRecord::IncomeBaseline(Box::new(b)),
        RecoveryRecord::IngressAccountInput {
            input_stream: [9; 16],
            input_sequence: 3,
            input: Box::new(v::CanonicalAccountInput::new(row).unwrap()),
        },
        RecoveryRecord::IngressAccountInput {
            input_stream: [9; 16],
            input_sequence: 4,
            input: Box::new(observation.clone()),
        },
    ];
    let a = Account::recover(s.clone(), c.clone(), &records).unwrap();
    let before = a.asset(key(2)).unwrap();
    let binding = IncomeObservationsAssociated {
        scope: s.scope.clone(),
        account_epoch: 1,
        ledger_sequence: a.ledger_sequence(),
        wallet_id: 1,
        request_id: [20; 16],
        baseline_digest: baseline.digest(),
        history_digest: [6; 32],
        input_stream: [9; 16],
        through_sequence: 10,
        state_digest: a
            .income_association_state_digest(baseline.digest(), [6; 32], 1)
            .unwrap(),
        manifest_digest: [7; 32],
        approval_digest: [8; 32],
        associations: vec![ObservationAssociation {
            observation_digest: observation.digest(),
            incomes: vec![IncomeIdentity {
                income_type: venue::text("FUNDING_FEE"),
                transaction_id: venue::text("3"),
            }],
        }],
    };
    let r = RecoveryRecord::IncomeObservationsAssociated(Box::new(binding.clone()));
    let encoded = CanonicalRecord::new(baseline.digest(), 4, r.clone()).unwrap();
    assert!(encoded.bytes().windows(3).any(|w| w == [0x82, 15, 0x8d]));
    assert_eq!(CanonicalRecord::decode(encoded.bytes()).unwrap(), encoded);
    assert!(CanonicalRecord::new([4; 32], 4, r.clone()).is_err());
    let mut invalid = binding.clone();
    let duplicate = invalid.associations[0].incomes[0].clone();
    invalid.associations[0].incomes.push(duplicate);
    assert!(
        CanonicalRecord::new(
            baseline.digest(),
            4,
            RecoveryRecord::IncomeObservationsAssociated(Box::new(invalid))
        )
        .is_err()
    );
    records.push(r);
    let mut recovered = Account::recover(s, c, &records).unwrap();
    assert_eq!(recovered.asset(key(2)).unwrap(), before);
    assert_eq!(recovered.unresolved_account_inputs(), 1);
    assert!(recovered.frozen());
    assert_eq!(
        recovered.associate_income_observations(&binding),
        Ok(Applied::Duplicate)
    );
    assert_eq!(recovered.ledger_sequence(), 4);
}
