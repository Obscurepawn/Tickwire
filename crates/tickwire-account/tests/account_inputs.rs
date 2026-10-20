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
