use tickwire_venue::*;
fn text<const N: usize>(s: &str) -> BoundedText<N> {
    BoundedText::new(s).unwrap()
}
fn decimal(s: &str) -> Decimal {
    s.parse().unwrap()
}
fn input() -> AccountInput {
    AccountInput {
        identity: AccountIdentity {
            risk_domain_id: [1; 16],
            venue: 1,
            environment: Environment::Test,
            account: 2,
            market: AccountMarket::Usdm,
            wallet_id: 1,
        },
        body: AccountInputBody::Income(IncomeRecord {
            income_type: text("FUNDING_FEE"),
            transaction_id: text("3001"),
            delta: AssetDelta {
                asset: AssetRef {
                    id: 2,
                    version: 1,
                    symbol: AssetSymbol::new("USDT").unwrap(),
                    exponent: -8,
                },
                amount: decimal("-0.125"),
            },
            symbol: Some(Symbol::new("BTCUSDT").unwrap()),
            trade_id: None,
            venue_time_ms: 1728000000,
            info: None,
        }),
    }
}
#[test]
fn account_income_has_bounded_canonical_bytes_without_a_local_order() {
    let a = CanonicalAccountInput::new(input()).unwrap();
    assert_eq!(CanonicalAccountInput::decode(a.bytes()).unwrap(), a);
    assert_eq!(a.input().identity.wallet_id, 1);
    let mut changed = input();
    let AccountInputBody::Income(i) = &mut changed.body else {
        unreachable!()
    };
    i.delta.amount = decimal("-0.126");
    assert_ne!(
        CanonicalAccountInput::new(changed).unwrap().digest(),
        a.digest()
    );
    let mut trailing = a.bytes().to_vec();
    trailing.push(0);
    assert_eq!(
        CanonicalAccountInput::decode(&trailing),
        Err(Error::TrailingData)
    );
    assert_eq!(
        CanonicalAccountInput::decode(&vec![0; MAX_ACCOUNT_INPUT_BYTES + 1]),
        Err(Error::Bounds)
    );
}
#[test]
fn account_income_rejects_noncanonical_id_and_fractional_asset_atoms() {
    for id in ["003001", "+3001", "-0", "3.0"] {
        let mut a = input();
        let AccountInputBody::Income(i) = &mut a.body else {
            unreachable!()
        };
        i.transaction_id = text(id);
        assert_eq!(CanonicalAccountInput::new(a), Err(Error::Identity));
    }
    let mut a = input();
    let AccountInputBody::Income(i) = &mut a.body else {
        unreachable!()
    };
    i.delta.amount = decimal("0.000000001");
    assert_eq!(CanonicalAccountInput::new(a), Err(Error::Quantity));
}
#[test]
fn sparse_account_observation_roundtrips_and_rejects_duplicate_assets() {
    let mut a = input();
    let AccountInputBody::Income(i) = a.body else {
        unreachable!()
    };
    let b = WalletObservation {
        asset: i.delta.asset,
        wallet_balance: decimal("99"),
        cross_wallet_balance: decimal("99"),
        balance_change: decimal("-1"),
    };
    let o = AccountObservation {
        reason: text("FUNDING_FEE"),
        funding_symbol: None,
        event_time_ms: 10,
        transaction_time_ms: Some(10),
        balances: BoundedList::new(vec![b.clone()]).unwrap(),
        positions: BoundedList::new(vec![]).unwrap(),
    };
    a.body = AccountInputBody::Observation(o);
    let c = CanonicalAccountInput::new(a.clone()).unwrap();
    assert_eq!(CanonicalAccountInput::decode(c.bytes()).unwrap(), c);
    let AccountInputBody::Observation(o) = &mut a.body else {
        unreachable!()
    };
    o.balances = BoundedList::new(vec![b.clone(), b]).unwrap();
    assert_eq!(CanonicalAccountInput::new(a), Err(Error::Duplicate));
}
