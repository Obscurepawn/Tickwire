use tickwire_risk::{
    RiskError,
    fixed::{ceil_div, floor_div},
};

#[test]
fn signed_division_rounds_outward_without_minimum_negation() {
    for (n, d, lo, hi) in [
        (-5, 2, -3, -2),
        (5, -2, -3, -2),
        (-5, -2, 2, 3),
        (5, 2, 2, 3),
        (i128::MIN, 1, i128::MIN, i128::MIN),
        (i128::MIN, i128::MIN, 1, 1),
        (1, i128::MIN, -1, 0),
    ] {
        assert_eq!(floor_div(n, d), Ok(lo));
        assert_eq!(ceil_div(n, d), Ok(hi));
    }
}
#[test]
fn signed_division_overflow_and_zero_are_errors() {
    assert_eq!(floor_div(i128::MIN, -1), Err(RiskError::ArithmeticOverflow));
    assert_eq!(ceil_div(i128::MIN, -1), Err(RiskError::ArithmeticOverflow));
    assert_eq!(floor_div(1, 0), Err(RiskError::DivisionByZero));
}

use tickwire_risk::{
    Fixed, Rounding, fixed::product_to, futures::*, reservation::*, settlement::*, spot::*,
};
fn v(text: &str) -> Fixed {
    Fixed::parse(text, 8).unwrap()
}

#[test]
fn decimals_preserve_precision_scale_and_minimum() {
    assert_eq!(
        Fixed::parse(&i128::MIN.to_string(), 0).unwrap().atoms(),
        i128::MIN
    );
    assert_eq!(
        Fixed::parse(&i128::MIN.to_string(), 0).unwrap().to_string(),
        i128::MIN.to_string()
    );
    assert_eq!(Fixed::parse("1.230000000", 2).unwrap().to_string(), "1.23");
    for bad in [
        "NaN",
        "Infinity",
        "1e2",
        " 1",
        "1 ",
        "1.",
        ".1",
        "1.000000001",
        "--1",
        "1..2",
    ] {
        assert_eq!(
            Fixed::parse(bad, 8),
            Err(RiskError::InvalidDecimal),
            "{bad}"
        );
    }
    assert_eq!(Fixed::parse("1", 39), Err(RiskError::InvalidScale));
    assert_eq!(
        Fixed::parse("170141183460469231731687303715884105728", 0),
        Err(RiskError::ArithmeticOverflow)
    );
    assert_eq!(
        Fixed::parse("-170141183460469231731687303715884105729", 0),
        Err(RiskError::ArithmeticOverflow)
    );
}
#[test]
fn scale_conversion_is_explicit_and_rounds_toward_requested_bound() {
    assert_eq!(
        Fixed::parse("1", 2).unwrap().checked_add(v("1")),
        Err(RiskError::ScaleMismatch)
    );
    assert_eq!(
        v("-1.234").rescale(2, Rounding::Floor).unwrap().to_string(),
        "-1.24"
    );
    assert_eq!(
        v("-1.234").rescale(2, Rounding::Ceil).unwrap().to_string(),
        "-1.23"
    );
    let btc = Money {
        asset: AssetId(1),
        value: v("2"),
    };
    let usdt = Money {
        asset: AssetId(2),
        value: v("2"),
    };
    assert_eq!(btc.checked_add(usdt), Err(RiskError::AssetMismatch));
    assert_eq!(
        v("1.234").quantize(v("0.05"), Rounding::Floor).unwrap(),
        v("1.20")
    );
    assert_eq!(
        v("1.234").quantize(v("0.05"), Rounding::Ceil).unwrap(),
        v("1.25")
    );
}
#[test]
fn products_cancel_decimal_powers_before_checked_multiply() {
    let large = Fixed::from_atoms(i128::MAX, 8).unwrap();
    assert_eq!(large.mul_to(v("1"), 8, Rounding::Ceil).unwrap(), large);
    assert_eq!(
        large.mul_to(v("2"), 8, Rounding::Ceil),
        Err(RiskError::ArithmeticOverflow)
    );
    let a = Fixed::from_atoms(1, 38).unwrap();
    assert_eq!(product_to([a, a, a], 0, Rounding::Ceil).unwrap().atoms(), 1);
    assert_eq!(
        product_to([a, a, a], 0, Rounding::Floor).unwrap().atoms(),
        0
    );
    assert_eq!(
        product_to([a.checked_neg().unwrap(), a, a], 0, Rounding::Floor)
            .unwrap()
            .atoms(),
        -1
    );
    assert_eq!(
        Fixed::from_atoms(i128::MIN, 0).unwrap().checked_abs(),
        Err(RiskError::ArithmeticOverflow)
    );
    assert_eq!(
        v("1").div_to(v("0"), 8, Rounding::Ceil),
        Err(RiskError::DivisionByZero)
    );
}
#[test]
fn division_matches_small_exact_integer_oracle() {
    for n in -50_i128..=50 {
        for d in -17_i128..=17 {
            if d == 0 {
                continue;
            }
            let q = n / d;
            let r = n % d;
            let expected_floor = q - i128::from(r != 0 && (n < 0) != (d < 0));
            let expected_ceil = q + i128::from(r != 0 && (n < 0) == (d < 0));
            assert_eq!(floor_div(n, d).unwrap(), expected_floor);
            assert_eq!(ceil_div(n, d).unwrap(), expected_ceil);
        }
    }
}
#[test]
fn reservation_failures_preserve_financial_state_and_overflow_closes_gate() {
    let mut state = ReservationAccount::new(Money {
        asset: AssetId(1),
        value: v("1000"),
    })
    .unwrap();
    state
        .reserve(Money {
            asset: AssetId(1),
            value: v("600"),
        })
        .unwrap();
    let prior = state;
    assert_eq!(
        state.reserve(Money {
            asset: AssetId(1),
            value: v("600")
        }),
        Err(RiskError::InsufficientAsset)
    );
    assert_eq!(state, prior);
    assert_eq!(
        state.reserve(Money {
            asset: AssetId(2),
            value: v("1")
        }),
        Err(RiskError::AssetMismatch)
    );
    assert_eq!(state, prior);
    let error = state.reserve_computed(|| {
        Ok(Money {
            asset: AssetId(1),
            value: Fixed::from_atoms(i128::MAX, 0)?.checked_mul_int(2)?,
        })
    });
    assert_eq!(error, Err(RiskError::ArithmeticOverflow));
    assert_eq!(state.available(), prior.available());
    assert_eq!(state.reserved(), prior.reserved());
    assert!(!state.increase_open());
    assert_eq!(
        state.reserve(Money {
            asset: AssetId(1),
            value: v("1")
        }),
        Err(RiskError::GateClosed)
    );
    assert_eq!(state.on_timeout(), v("0"));
    assert_eq!(state.reserved(), v("600"));
}
#[test]
fn fees_exceeding_received_amount_require_external_inventory() {
    let r = received_fees(v("1"), [v("1.5"), v("0.1"), v("0")], v("0")).unwrap();
    assert_eq!(r.net_received, v("0"));
    assert_eq!(r.external_reserve, v("0.6"));
    assert_eq!(
        third_asset_fee(v("1"), [v("0"); 3], v("0"), v("1"), v("0")),
        Err(RiskError::InvalidInput)
    );
}
#[test]
fn baseline_requires_explained_locked_assets() {
    let baseline = SpotBaseline {
        free: v("600"),
        locked: v("400"),
        nontrade_locked: v("0"),
        net_cashflow: v("0"),
        proven_credits: v("0"),
        other_debits: v("0"),
    };
    assert_eq!(available(baseline, &[]), Err(RiskError::Unreconciled));
}
fn table() -> [Bracket; 2] {
    [
        Bracket {
            floor: v("0"),
            cap: v("50000"),
            rate: v("0.004"),
            cumulative: v("0"),
            max_initial_leverage: v("20"),
        },
        Bracket {
            floor: v("50000"),
            cap: v("250000"),
            rate: v("0.008"),
            cumulative: v("200"),
            max_initial_leverage: v("10"),
        },
    ]
}
#[test]
fn bracket_continuity_and_qualification_are_enforced() {
    let brackets = table();
    let q = QualifiedBrackets::new(
        &brackets,
        v("1.5"),
        BracketInterpretation::EffectiveAsReturned,
    )
    .unwrap();
    assert_eq!(q.maintenance(v("50000")).unwrap().tier, 2);
    assert_eq!(q.maintenance(v("250000")).unwrap().tier, 2);
    assert_eq!(
        q.maintenance(v("250000.00000001")),
        Err(RiskError::InvalidBrackets)
    );
    assert!(matches!(
        QualifiedBrackets::new(&brackets, v("1.5"), BracketInterpretation::Unverified),
        Err(RiskError::BracketSemanticsUnverified)
    ));
    let mut broken = brackets;
    broken[1].cumulative = v("201");
    assert!(matches!(
        QualifiedBrackets::new(&broken, v("1"), BracketInterpretation::EffectiveAsReturned),
        Err(RiskError::InvalidBrackets)
    ));
    broken = brackets;
    broken[1].floor = v("50001");
    assert!(matches!(
        QualifiedBrackets::new(&broken, v("1"), BracketInterpretation::EffectiveAsReturned),
        Err(RiskError::InvalidBrackets)
    ));
}
#[test]
fn invalid_collateral_excludes_assets_but_preserves_liabilities() {
    let mut asset = CollateralAsset {
        balance: v("2"),
        valuation_low: v("90"),
        valuation_high: v("110"),
        eligible_ratio: v("0.8"),
        eligible: false,
    };
    assert_eq!(collateral_component(asset, 8).unwrap(), v("0"));
    asset.balance = v("-2");
    assert_eq!(collateral_component(asset, 8).unwrap(), v("-220"));
    asset.valuation_high = v("0");
    assert_eq!(collateral_component(asset, 8), Err(RiskError::InvalidInput));
}
#[test]
fn reduce_allocation_respects_mode_step_and_real_position() {
    let r = allocate_reduce(
        v("-5"),
        &[v("3")],
        v("4"),
        v("0.3"),
        true,
        PositionSide::Both,
    )
    .unwrap();
    assert_eq!(r.side, Side::Buy);
    assert!(r.reduce_only);
    assert_eq!(r.allocated, v("1.8"));
    let r = allocate_reduce(v("5"), &[], v("2"), v("1"), false, PositionSide::Short).unwrap();
    assert_eq!(r.side, Side::Buy);
    assert!(!r.reduce_only);
    assert_eq!(
        allocate_reduce(v("1"), &[v("2")], v("1"), v("1"), true, PositionSide::Both),
        Err(RiskError::Unreconciled)
    );
    assert_eq!(
        allocate_reduce(v("1"), &[], v("2"), v("1"), false, PositionSide::Both),
        Err(RiskError::InsufficientAsset)
    );
}
fn capital_params() -> CapitalParameters {
    CapitalParameters {
        prices: StressPrices {
            low: v("90"),
            high: v("110"),
        },
        effective_leverage: v("10"),
        maintenance_rate_upper: v("0.01"),
        maintenance_addon: v("0"),
        execution_fee_rate: v("0.001"),
        exit_fee_rate: v("0.001"),
        funding_rate_abs_upper: v("0"),
        funding_settlements: 0,
        pending_outbound: v("0"),
        capital_buffer: v("0"),
        rounding_tails: v("0"),
        collateral_value: v("10000"),
    }
}
#[test]
fn hedge_capital_rejects_wrong_order_direction_for_the_envelope() {
    let position = HedgePosition {
        long: v("0"),
        short: v("0"),
        open_long: v("2"),
        open_short: v("0"),
        close_long: v("0"),
        close_short: v("0"),
    };
    let wrong = [PendingOrder {
        side: Side::Sell,
        quantity: v("2"),
        price_bound: v("95"),
    }];
    assert_eq!(
        hedge_capital(position, v("100"), v("100"), &wrong, capital_params()),
        Err(RiskError::InvalidInput)
    );
}
#[test]
fn funding_counts_and_exit_fees_are_charged_before_new_positions_fill() {
    let orders = [PendingOrder {
        side: Side::Buy,
        quantity: v("1"),
        price_bound: v("90"),
    }];
    let r = one_way_capital(v("0"), v("90"), &orders, capital_params()).unwrap();
    assert_eq!(r.exit_fee, v("0.11"));
    assert_eq!(r.capital, v("11.20"));
    let p = CapitalParameters {
        funding_rate_abs_upper: v("0.0001"),
        funding_settlements: 2,
        ..capital_params()
    };
    assert_eq!(
        one_way_capital(v("0"), v("90"), &orders, p)
            .unwrap()
            .funding_reserve,
        v("0.022")
    );
}
#[test]
fn settlement_timer_does_not_release_and_duplicate_income_is_idempotent() {
    let mut state = Settlement::<2>::new(v("2"), v("1000"), v("0.24")).unwrap();
    assert_eq!(state.on_expiry_timer(), v("0"));
    assert_eq!(state.position(), v("2"));
    assert_eq!(state.fee_reserve(), v("0.24"));
    let income = Income {
        id: 1,
        kind: IncomeKind::SettlementPnl,
        amount: v("20"),
    };
    assert!(state.record_income(income).unwrap());
    let prior = state.clone();
    assert!(!state.record_income(income).unwrap());
    assert_eq!(state, prior);
    let mut conflict = state.clone();
    assert_eq!(
        conflict.record_income(Income {
            amount: v("21"),
            ..income
        }),
        Err(RiskError::DuplicateConflict)
    );
    assert_eq!(conflict.position(), prior.position());
    assert_eq!(conflict.fee_reserve(), prior.fee_reserve());
    assert_eq!(
        conflict
            .report(
                ReferenceSettlement {
                    pnl: v("20"),
                    fee: v("0"),
                    net: v("20")
                },
                v("1020")
            )
            .unwrap()
            .actual_net_income,
        v("20")
    );
    state
        .record_income(Income {
            id: 2,
            kind: IncomeKind::SettlementFee,
            amount: v("-0.22"),
        })
        .unwrap();
    let reference = reference_settlement(v("2"), v("100"), v("110"), v("0.001"), 8).unwrap();
    let facts = ReconcileFacts {
        complete_account_facts: true,
        unresolved_orders: 1,
        actual_position: v("0"),
        observed_wallet: v("1019.78"),
    };
    let prior = state.clone();
    assert_eq!(
        state.reconcile(reference, facts),
        Err(RiskError::Unreconciled)
    );
    assert_eq!(state, prior);
    assert_eq!(
        state
            .reconcile(
                reference,
                ReconcileFacts {
                    unresolved_orders: 0,
                    ..facts
                }
            )
            .unwrap(),
        v("0.02")
    );
    assert_eq!(state.state(), SettlementState::SettledReconciled);
    assert_eq!(
        state.reconcile(
            reference,
            ReconcileFacts {
                unresolved_orders: 0,
                ..facts
            }
        ),
        Err(RiskError::Unreconciled)
    );
}
#[test]
fn settlement_overflow_is_atomic() {
    let mut state = Settlement::<2>::new(
        v("1"),
        Fixed::from_atoms(0, 0).unwrap(),
        Fixed::from_atoms(0, 0).unwrap(),
    )
    .unwrap();
    state
        .record_income(Income {
            id: 1,
            kind: IncomeKind::SettlementPnl,
            amount: Fixed::from_atoms(i128::MAX, 0).unwrap(),
        })
        .unwrap();
    let prior = state.clone();
    assert_eq!(
        state.record_income(Income {
            id: 2,
            kind: IncomeKind::SettlementPnl,
            amount: Fixed::from_atoms(1, 0).unwrap()
        }),
        Err(RiskError::ArithmeticOverflow)
    );
    assert_eq!(state.position(), prior.position());
    assert_eq!(state.fee_reserve(), prior.fee_reserve());
    let maximum = Fixed::from_atoms(i128::MAX, 0).unwrap();
    assert_eq!(
        state
            .report(
                ReferenceSettlement {
                    pnl: maximum,
                    fee: maximum.zero_like(),
                    net: maximum
                },
                maximum
            )
            .unwrap()
            .actual_net_income,
        maximum
    );
    assert_eq!(state.state(), SettlementState::AwaitingSettlement);
}

#[test]
fn late_unrecordable_settlement_evidence_reopens_reconciliation() {
    fn closed<const N: usize>(amount: Fixed) -> Settlement<N> {
        let zero = amount.zero_like();
        let mut state = Settlement::new(v("1"), zero, zero).unwrap();
        state.on_expiry_timer();
        state
            .record_income(Income {
                id: 1,
                kind: IncomeKind::SettlementPnl,
                amount,
            })
            .unwrap();
        state
            .reconcile(
                ReferenceSettlement {
                    pnl: amount,
                    fee: zero,
                    net: amount,
                },
                ReconcileFacts {
                    complete_account_facts: true,
                    unresolved_orders: 0,
                    actual_position: v("0"),
                    observed_wallet: amount,
                },
            )
            .unwrap();
        state
    }
    let mut capacity = closed::<1>(v("10"));
    assert_eq!(
        capacity.record_income(Income {
            id: 2,
            kind: IncomeKind::SettlementFee,
            amount: v("-1")
        }),
        Err(RiskError::Unreconciled)
    );
    assert_eq!(capacity.state(), SettlementState::AwaitingSettlement);
    assert_eq!(capacity.position(), v("0"));
    assert_eq!(capacity.fee_reserve(), v("0"));
    assert_eq!(
        capacity.reconcile(
            ReferenceSettlement {
                pnl: v("10"),
                fee: v("0"),
                net: v("10")
            },
            ReconcileFacts {
                complete_account_facts: true,
                unresolved_orders: 0,
                actual_position: v("0"),
                observed_wallet: v("10")
            }
        ),
        Err(RiskError::Unreconciled)
    );
    let mut conflict = closed::<1>(v("10"));
    assert_eq!(
        conflict.record_income(Income {
            id: 1,
            kind: IncomeKind::SettlementPnl,
            amount: v("9")
        }),
        Err(RiskError::DuplicateConflict)
    );
    assert_eq!(conflict.state(), SettlementState::AwaitingSettlement);
    let maximum = Fixed::from_atoms(i128::MAX, 0).unwrap();
    let mut overflow = closed::<2>(maximum);
    assert_eq!(
        overflow.record_income(Income {
            id: 2,
            kind: IncomeKind::SettlementPnl,
            amount: Fixed::from_atoms(1, 0).unwrap()
        }),
        Err(RiskError::ArithmeticOverflow)
    );
    assert_eq!(overflow.state(), SettlementState::AwaitingSettlement);
    let report = overflow
        .report(
            ReferenceSettlement {
                pnl: maximum,
                fee: maximum.zero_like(),
                net: maximum,
            },
            maximum,
        )
        .unwrap();
    assert_eq!(report.actual_net_income, maximum);
}
