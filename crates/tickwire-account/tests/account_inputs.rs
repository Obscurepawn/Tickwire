mod common;
use common::*;
use tickwire_account::*;
use tickwire_venue as v;
fn income(
    cmd: &v::Command,
    kind: &str,
    id: &str,
    amount: &str,
    time: u64,
) -> v::CanonicalAccountInput {
    let r = &cmd.context.request;
    v::CanonicalAccountInput::new(v::AccountInput {
        identity: v::AccountIdentity {
            risk_domain_id: r.risk_domain_id,
            venue: r.venue,
            environment: r.environment,
            account: r.account,
            market: v::AccountMarket::Usdm,
            wallet_id: 1,
        },
        body: v::AccountInputBody::Income(v::IncomeRecord {
            income_type: venue::text(kind),
            transaction_id: venue::text(id),
            delta: v::AssetDelta {
                asset: cmd.context.instrument.settlement.clone(),
                amount: amount.parse().unwrap(),
            },
            symbol: Some(cmd.context.instrument.symbol.clone()),
            trade_id: None,
            venue_time_ms: time,
            info: None,
        }),
    })
    .unwrap()
}
fn baseline(a: &Account, covered: Vec<v::CanonicalAccountInput>) -> IncomeBaseline {
    IncomeBaseline {
        scope: Scope {
            risk_domain_id: [1; 16],
            venue: 1,
            environment: v::Environment::Test,
            account: 2,
        },
        account_epoch: a.account_epoch(),
        ledger_sequence: a.ledger_sequence(),
        wallet_id: 1,
        through_time_ms: 100,
        coverage_digest: [9; 32],
        baseline_digest: [8; 32],
        input_stream: [7; 16],
        through_sequence: 2,
        covered,
    }
}
fn setup_futures() -> (Account, v::Command) {
    let (s, c, cmd) = futures_fixture();
    (Account::from_snapshot(s, c).unwrap(), cmd)
}
#[test]
fn funding_covered_by_baseline_and_reconnect_repeats_never_book_twice() {
    let (mut a, cmd) = setup_futures();
    let old = income(&cmd, "FUNDING_FEE", "10", "-3", 99);
    a.apply_income_baseline(&baseline(&a, vec![old.clone()]))
        .unwrap();
    assert_eq!(a.apply_account_input(&old), Ok(Applied::Duplicate));
    assert_eq!(a.asset(key(2)).unwrap().total, units(1000));
    let current = income(&cmd, "FUNDING_FEE", "11", "-2", 101);
    assert_eq!(a.apply_account_input(&current), Ok(Applied::New));
    assert_eq!(a.asset(key(2)).unwrap().total, units(998));
    let seq = a.ledger_sequence();
    assert_eq!(a.apply_account_input(&current), Ok(Applied::Duplicate));
    assert_eq!(a.ledger_sequence(), seq);
    assert_eq!(
        a.apply_account_input(&income(&cmd, "FUNDING_FEE", "11", "-4", 101)),
        Err(Error::Conflict)
    );
    assert_eq!(a.asset(key(2)).unwrap().total, units(998));
    assert!(a.frozen());
}
#[test]
fn unresolved_income_requires_baseline_and_holds_debit_without_inventing_cashflow() {
    let (mut a, cmd) = setup_futures();
    a.apply_account_input(&income(&cmd, "FUNDING_FEE", "12", "-5", 101))
        .unwrap();
    assert_eq!(a.asset(key(2)).unwrap().total, units(1000));
    assert_eq!(a.pending_account_debit(key(2)).unwrap(), units(5));
    assert_eq!(a.asset(key(2)).unwrap().available, units(995));
    assert!(a.frozen());
    assert!(a.apply_income_baseline(&baseline(&a, vec![])).is_err());
}
#[test]
fn positive_unattributed_funding_is_booked_but_unavailable() {
    let (mut a, cmd) = setup_futures();
    a.apply_income_baseline(&baseline(&a, vec![])).unwrap();
    a.apply_account_input(&income(&cmd, "FUNDING_FEE", "12", "5", 101))
        .unwrap();
    assert_eq!(a.asset(key(2)).unwrap().total, units(1005));
    assert_eq!(a.asset(key(2)).unwrap().available, units(1000));
    assert!(a.frozen());
}
#[test]
fn observations_never_apply_balance_change_as_a_second_cashflow() {
    let (mut a, cmd) = setup_futures();
    let mut raw = income(&cmd, "FUNDING_FEE", "12", "-5", 101).input().clone();
    raw.body = v::AccountInputBody::Observation(v::AccountObservation {
        reason: venue::text("FUNDING_FEE"),
        funding_symbol: Some(cmd.context.instrument.symbol.clone()),
        event_time_ms: 101,
        transaction_time_ms: Some(101),
        positions: v::BoundedList::new(vec![]).unwrap(),
        balances: v::BoundedList::new(vec![v::WalletObservation {
            asset: cmd.context.instrument.settlement.clone(),
            wallet_balance: "995".parse().unwrap(),
            cross_wallet_balance: "995".parse().unwrap(),
            balance_change: "-5".parse().unwrap(),
        }])
        .unwrap(),
    });
    let observation = v::CanonicalAccountInput::new(raw).unwrap();
    a.apply_account_input(&observation).unwrap();
    let seq = a.ledger_sequence();
    a.apply_account_input(&observation).unwrap();
    assert_eq!(a.asset(key(2)).unwrap().total, units(1000));
    assert_eq!(a.asset(key(2)).unwrap().available, units(995));
    assert_eq!(a.ledger_sequence(), seq);
}
#[test]
fn income_and_native_fill_share_pnl_and_commission_in_either_arrival_order() {
    for income_first in [false, true] {
        let (mut a, cmd) = setup_futures();
        a.apply_income_baseline(&baseline(&a, vec![])).unwrap();
        a.admit(&canonical(cmd.clone()), 60).unwrap();
        let mut pnl = income(&cmd, "REALIZED_PNL", "20", "-3", 101)
            .input()
            .clone();
        let v::AccountInputBody::Income(ref mut row) = pnl.body else {
            unreachable!()
        };
        row.trade_id = Some(venue::text("7"));
        let pnl = v::CanonicalAccountInput::new(pnl).unwrap();
        let mut commission = income(&cmd, "COMMISSION", "21", "-0.1", 101)
            .input()
            .clone();
        let v::AccountInputBody::Income(ref mut row) = commission.body else {
            unreachable!()
        };
        row.trade_id = Some(venue::text("7"));
        let commission = v::CanonicalAccountInput::new(commission).unwrap();
        let mut filled_event = fill(&cmd, "7", "1", "100", "100");
        let v::VenueEventBody::Fill(ref mut trade) = filled_event.body else {
            unreachable!()
        };
        trade.observation.order.venue_order_id = Some(venue::text("50"));
        trade.realized_pnl = Some(v::AssetDelta {
            asset: cmd.context.instrument.settlement.clone(),
            amount: "-3".parse().unwrap(),
        });
        trade.fees = v::BoundedList::new(vec![v::AssetDelta {
            asset: cmd.context.instrument.settlement.clone(),
            amount: "-0.1".parse().unwrap(),
        }])
        .unwrap();
        if income_first {
            a.apply_account_input(&pnl).unwrap();
            a.apply_account_input(&commission).unwrap();
            assert_eq!(a.pending_account_debit(key(2)).unwrap(), f(310000000));
        }
        a.apply_event(&event(filled_event)).unwrap();
        a.apply_account_input(&pnl).unwrap();
        a.apply_account_input(&commission).unwrap();
        assert_eq!(a.asset(key(2)).unwrap().total, f(99690000000));
        assert_eq!(a.pending_account_debit(key(2)).unwrap(), f(0));
        assert_eq!(a.position(12, v::PositionSide::Both).unwrap().0, units(1));
    }
}

fn closure(a: &Account) -> AccountInputsReconciled {
    AccountInputsReconciled {
        scope: baseline(a, vec![]).scope,
        account_epoch: a.account_epoch(),
        ledger_sequence: a.ledger_sequence(),
        baseline_digest: [8; 32],
        history_digest: [10; 32],
        input_stream: [7; 16],
        through_sequence: 9,
        state_digest: a.account_inputs_state_digest([8; 32], [10; 32]).unwrap(),
        coverage_digest: [11; 32],
    }
}
#[test]
fn income_closure_changes_status_once_without_changing_cash_or_holds() {
    for amount in ["-2", "2"] {
        let (mut a, cmd) = setup_futures();
        a.apply_income_baseline(&baseline(&a, vec![])).unwrap();
        // Keep an actual order reservation while closing the income status.
        a.admit(&canonical(cmd.clone()), 60).unwrap();
        let row = income(&cmd, "FUNDING_FEE", "50", amount, 101);
        a.apply_account_input(&row).unwrap();
        let before = a.asset(key(2)).unwrap();
        let binding = closure(&a);
        assert_eq!(a.reconcile_account_inputs(&binding), Ok(Applied::New));
        assert_eq!(a.asset(key(2)).unwrap(), before);
        assert_eq!(a.unresolved_account_inputs(), 0);
        assert!(a.frozen());
        assert_eq!(a.ledger_sequence(), binding.ledger_sequence + 1);
        assert_eq!(a.reconcile_account_inputs(&binding), Ok(Applied::Duplicate));
        assert_eq!(a.apply_account_input(&row), Ok(Applied::Duplicate));
        assert_eq!(a.ledger_sequence(), binding.ledger_sequence + 1);
        assert_eq!(a.asset(key(2)).unwrap(), before);
        let mut conflicting = binding.clone();
        conflicting.coverage_digest = [12; 32];
        assert_eq!(
            a.reconcile_account_inputs(&conflicting),
            Err(Error::Conflict)
        );
    }
}
#[test]
fn income_closure_digest_binds_history_and_persistent_state() {
    let (mut a, cmd) = setup_futures();
    a.apply_income_baseline(&baseline(&a, vec![])).unwrap();
    let old = a.account_inputs_state_digest([8; 32], [10; 32]).unwrap();
    a.freeze();
    assert_eq!(
        a.account_inputs_state_digest([8; 32], [10; 32]).unwrap(),
        old
    );
    assert_ne!(
        a.account_inputs_state_digest([8; 32], [12; 32]).unwrap(),
        old
    );
    a.apply_account_input(&income(&cmd, "FUNDING_FEE", "50", "-2", 101))
        .unwrap();
    assert_ne!(
        a.account_inputs_state_digest([8; 32], [10; 32]).unwrap(),
        old
    );
}
#[test]
fn income_closure_rejects_unknown_zero_net_and_old_cut_keys() {
    for (kind, time) in [("UNKNOWN", 101), ("FUNDING_FEE", 99)] {
        let (mut a, cmd) = setup_futures();
        a.apply_income_baseline(&baseline(&a, vec![])).unwrap();
        a.apply_account_input(&income(&cmd, kind, "51", "-2", time))
            .unwrap();
        a.apply_account_input(&income(&cmd, kind, "52", "2", time))
            .unwrap();
        let before = a.asset(key(2)).unwrap();
        let binding = closure(&a);
        assert_eq!(
            a.reconcile_account_inputs(&binding),
            Err(Error::Unreconciled)
        );
        assert_eq!(a.asset(key(2)).unwrap(), before);
        assert_eq!(a.ledger_sequence(), binding.ledger_sequence);
        assert_eq!(a.unresolved_account_inputs(), 2);
    }
}

#[test]
fn income_closure_state_has_a_fixed_empty_reference_vector() {
    let (a, _) = setup_futures();
    // Independent Python struct/hashlib transcription of the specified LE wire fields.
    assert_eq!(
        a.account_inputs_state_digest([8; 32], [10; 32]).unwrap(),
        [
            134, 240, 130, 121, 32, 132, 152, 212, 122, 16, 16, 227, 153, 242, 180, 156, 247, 127,
            83, 237, 248, 138, 46, 193, 162, 60, 78, 130, 96, 36, 35, 128
        ]
    );
}
#[test]
fn income_closure_digest_normalizes_only_recovery_markers() {
    for persistent_unknown in [false, true] {
        let (s, c, cmd) = futures_fixture();
        let mut a = Account::from_snapshot(s.clone(), c.clone()).unwrap();
        let b = baseline(&a, vec![]);
        a.apply_income_baseline(&b).unwrap();
        let command = canonical(cmd.clone());
        let admission = a.admit(&command, 60).unwrap();
        let order = admission.orders()[0].clone();
        let mut records = vec![
            RecoveryRecord::IncomeBaseline(Box::new(b)),
            RecoveryRecord::Admit {
                command: Box::new(command),
                evaluated_at: 60,
                expected: admission.image(),
            },
        ];
        if persistent_unknown {
            let mut e = fill(&cmd, "unused", "0.1", "100", "10");
            let v::VenueEventBody::Fill(f) = e.body else {
                unreachable!()
            };
            let mut observation = f.observation;
            observation.status = v::OrderStatus::Unknown;
            e.body = v::VenueEventBody::Order(Box::new(observation));
            let e = event(e);
            a.apply_event(&e).unwrap();
            records.push(RecoveryRecord::VenueEvent(Box::new(e)));
        }
        let row = income(&cmd, "FUNDING_FEE", "50", "-2", 101);
        a.apply_account_input(&row).unwrap();
        records.push(RecoveryRecord::IngressAccountInput {
            input_stream: [7; 16],
            input_sequence: 3,
            input: Box::new(row),
        });
        let mut recovered = Account::recover(s, c, &records).unwrap();
        assert_eq!(a.order(&order).unwrap().unknown, persistent_unknown);
        assert!(recovered.order(&order).unwrap().unknown);
        assert_eq!(a.ledger_sequence(), recovered.ledger_sequence());
        assert_eq!(
            a.account_inputs_state_digest([8; 32], [10; 32]).unwrap(),
            recovered
                .account_inputs_state_digest([8; 32], [10; 32])
                .unwrap()
        );
        let binding = closure(&a);
        a.reconcile_account_inputs(&binding).unwrap();
        recovered.reconcile_account_inputs(&binding).unwrap();
        assert_eq!(a.order(&order).unwrap().unknown, persistent_unknown);
        assert!(recovered.order(&order).unwrap().unknown);
        assert_eq!(a.asset(key(2)).unwrap(), recovered.asset(key(2)).unwrap());
    }
}
#[test]
fn income_closure_refuses_observation_or_new_state_after_preparation() {
    let (mut a, cmd) = setup_futures();
    a.apply_income_baseline(&baseline(&a, vec![])).unwrap();
    a.apply_account_input(&income(&cmd, "FUNDING_FEE", "50", "-2", 101))
        .unwrap();
    let old = closure(&a);
    let mut raw = income(&cmd, "FUNDING_FEE", "51", "-2", 101).input().clone();
    raw.body = v::AccountInputBody::Observation(v::AccountObservation {
        reason: venue::text("FUNDING_FEE"),
        funding_symbol: None,
        event_time_ms: 101,
        transaction_time_ms: None,
        positions: v::BoundedList::new(vec![]).unwrap(),
        balances: v::BoundedList::new(vec![v::WalletObservation {
            asset: cmd.context.instrument.settlement.clone(),
            wallet_balance: "998".parse().unwrap(),
            cross_wallet_balance: "998".parse().unwrap(),
            balance_change: "-2".parse().unwrap(),
        }])
        .unwrap(),
    });
    a.apply_account_input(&v::CanonicalAccountInput::new(raw).unwrap())
        .unwrap();
    let view = a.asset(key(2)).unwrap();
    let sequence = a.ledger_sequence();
    assert_eq!(a.reconcile_account_inputs(&old), Err(Error::Identity));
    assert_eq!(
        a.reconcile_account_inputs(&closure(&a)),
        Err(Error::Unreconciled)
    );
    assert_eq!(a.asset(key(2)).unwrap(), view);
    assert_eq!(a.ledger_sequence(), sequence);
}

fn funding_observation(
    cmd: &v::Command,
    time: u64,
    amount: &str,
    wallet: &str,
) -> v::CanonicalAccountInput {
    let mut input = income(cmd, "FUNDING_FEE", "1", amount, time)
        .input()
        .clone();
    input.body = v::AccountInputBody::Observation(v::AccountObservation {
        reason: venue::text("FUNDING_FEE"),
        funding_symbol: Some(cmd.context.instrument.symbol.clone()),
        event_time_ms: time,
        transaction_time_ms: Some(time),
        positions: v::BoundedList::new(vec![]).unwrap(),
        balances: v::BoundedList::new(vec![v::WalletObservation {
            asset: cmd.context.instrument.settlement.clone(),
            wallet_balance: wallet.parse().unwrap(),
            cross_wallet_balance: wallet.parse().unwrap(),
            balance_change: amount.parse().unwrap(),
        }])
        .unwrap(),
    });
    v::CanonicalAccountInput::new(input).unwrap()
}
fn association(
    a: &Account,
    observation: &v::CanonicalAccountInput,
    ids: &[&str],
) -> IncomeObservationsAssociated {
    IncomeObservationsAssociated {
        scope: baseline(a, vec![]).scope,
        account_epoch: a.account_epoch(),
        ledger_sequence: a.ledger_sequence(),
        wallet_id: 1,
        request_id: [31; 16],
        baseline_digest: [8; 32],
        history_digest: [10; 32],
        input_stream: [7; 16],
        through_sequence: 12,
        state_digest: a
            .income_association_state_digest([8; 32], [10; 32], 1)
            .unwrap(),
        manifest_digest: [32; 32],
        approval_digest: [33; 32],
        associations: vec![ObservationAssociation {
            observation_digest: observation.digest(),
            incomes: ids
                .iter()
                .map(|id| IncomeIdentity {
                    income_type: venue::text("FUNDING_FEE"),
                    transaction_id: venue::text(id),
                })
                .collect(),
        }],
    }
}
#[test]
fn association_only_explains_observations_and_consumes_exact_approval_once() {
    for (amount, wallet) in [("-2", "998"), ("2", "1002")] {
        let (mut a, cmd) = setup_futures();
        let b = baseline(&a, vec![]);
        a.apply_income_baseline(&b).unwrap();
        // The reducer must preserve even an outstanding order's resources;
        // the owner separately restricts which historical interval is approvable.
        let reservation = a.admit(&canonical(cmd.clone()), 60).unwrap().image();
        let observation = funding_observation(&cmd, 101, amount, wallet);
        // Observation before income establishes a conservative cap for a debit.
        a.apply_account_input(&observation).unwrap();
        let row = income(&cmd, "FUNDING_FEE", "80", amount, 101);
        a.apply_account_input(&row).unwrap();
        let asset_before = a.asset(key(2)).unwrap();
        let order_before = a.order(&reservation.orders[0]).unwrap();
        let binding = association(&a, &observation, &["80"]);
        assert_eq!(a.associate_income_observations(&binding), Ok(Applied::New));
        assert_eq!(a.asset(key(2)).unwrap(), asset_before);
        assert_eq!(a.order(&reservation.orders[0]).unwrap(), order_before);
        assert_eq!(
            a.admission(reservation.intent).unwrap().image(),
            reservation
        );
        assert_eq!(a.income_baseline(1), Some(&b));
        assert_eq!(a.unresolved_account_inputs(), 1); // Funding remains Booked.
        assert_eq!(a.account_inputs_to_reconcile().unwrap(), vec![row.clone()]);
        assert!(a.frozen());
        assert_eq!(a.ledger_sequence(), binding.ledger_sequence + 1);
        assert_eq!(
            a.associate_income_observations(&binding),
            Ok(Applied::Duplicate)
        );
        assert_eq!(a.apply_account_input(&observation), Ok(Applied::Duplicate));
        assert_eq!(a.apply_account_input(&row), Ok(Applied::Duplicate));
        assert_eq!(a.ledger_sequence(), binding.ledger_sequence + 1);
        let mut conflicting = binding.clone();
        conflicting.approval_digest = [34; 32];
        assert_eq!(
            a.associate_income_observations(&conflicting),
            Err(Error::Conflict)
        );
        assert_eq!(a.asset(key(2)).unwrap(), asset_before);
    }
}
#[test]
fn association_rejects_partial_observations_and_reused_income_keys() {
    let (mut a, cmd) = setup_futures();
    a.apply_income_baseline(&baseline(&a, vec![])).unwrap();
    let first = funding_observation(&cmd, 101, "-2", "998");
    let second = funding_observation(&cmd, 102, "-2", "996");
    a.apply_account_input(&income(&cmd, "FUNDING_FEE", "80", "-2", 101))
        .unwrap();
    a.apply_account_input(&first).unwrap();
    a.apply_account_input(&second).unwrap();
    let before = a.asset(key(2)).unwrap();
    let mut binding = association(&a, &first, &["80"]);
    assert_eq!(
        a.associate_income_observations(&binding),
        Err(Error::Unreconciled)
    );
    binding.associations.push(ObservationAssociation {
        observation_digest: second.digest(),
        incomes: binding.associations[0].incomes.clone(),
    });
    assert_eq!(
        a.associate_income_observations(&binding),
        Err(Error::Conflict)
    );
    assert_eq!(a.asset(key(2)).unwrap(), before);
    assert_eq!(a.ledger_sequence(), binding.ledger_sequence);
}
#[test]
fn association_zero_net_keeps_both_income_components_and_credit_isolation() {
    let (mut a, cmd) = setup_futures();
    a.apply_income_baseline(&baseline(&a, vec![])).unwrap();
    for (id, amount) in [("80", "-2"), ("81", "2")] {
        a.apply_account_input(&income(&cmd, "FUNDING_FEE", id, amount, 101))
            .unwrap();
    }
    let observation = funding_observation(&cmd, 101, "0", "1000");
    a.apply_account_input(&observation).unwrap();
    let before = a.asset(key(2)).unwrap();
    let binding = association(&a, &observation, &["80", "81"]);
    assert_eq!(a.associate_income_observations(&binding), Ok(Applied::New));
    assert_eq!(a.asset(key(2)).unwrap(), before);
    assert_eq!(before.total, units(1000));
    assert!(before.available.atoms() <= units(998).atoms());
    assert_eq!(a.account_inputs_to_reconcile().unwrap().len(), 2);
}
