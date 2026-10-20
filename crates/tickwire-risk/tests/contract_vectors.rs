use serde_json::{Value, json};
use tickwire_risk::{
    Fixed, Result, Rounding,
    fixed::{ceil_div, floor_div},
    futures::*,
    reservation::*,
    settlement::*,
    spot::*,
};

fn d(i: &Value, key: &str) -> Fixed {
    Fixed::parse(
        i[key]
            .as_str()
            .unwrap_or_else(|| panic!("missing decimal {key}")),
        8,
    )
    .unwrap()
}
fn f(value: Fixed) -> Value {
    Value::String(value.to_string())
}
fn amounts(values: impl IntoIterator<Item = Fixed>) -> Value {
    Value::Array(values.into_iter().map(f).collect())
}
fn decision(value: Result<()>) -> &'static str {
    if value.is_ok() { "ACCEPT" } else { "REJECT" }
}
fn reason(value: Result<()>) -> Value {
    value.err().map_or(Value::Null, |e| json!(e.code()))
}
fn rates(i: &Value) -> [Fixed; 3] {
    [
        d(i, "standard_rate"),
        d(i, "tax_rate"),
        d(i, "special_rate"),
    ]
}
fn zero() -> Fixed {
    Fixed::zero(8).unwrap()
}
fn bracket_interpretation(i: &Value) -> BracketInterpretation {
    match i["bracket_interpretation"].as_str().unwrap() {
        "effective_as_returned" => BracketInterpretation::EffectiveAsReturned,
        "unverified" => BracketInterpretation::Unverified,
        other => panic!("unsupported bracket interpretation {other}"),
    }
}
fn params(i: &Value, funding: bool) -> CapitalParameters {
    CapitalParameters {
        prices: StressPrices {
            low: d(i, "price_low"),
            high: d(i, "price_high"),
        },
        effective_leverage: d(i, "effective_leverage"),
        maintenance_rate_upper: d(i, "maintenance_rate_upper"),
        maintenance_addon: d(i, "maintenance_addon"),
        execution_fee_rate: d(i, "execution_fee_rate"),
        exit_fee_rate: d(i, "exit_fee_rate"),
        funding_rate_abs_upper: if funding {
            d(i, "funding_rate_abs_upper")
        } else {
            zero()
        },
        funding_settlements: if funding {
            i["funding_settlements"].as_str().unwrap().parse().unwrap()
        } else {
            0
        },
        pending_outbound: d(i, "pending_outbound"),
        capital_buffer: d(i, "capital_buffer"),
        rounding_tails: d(i, "rounding_tails"),
        collateral_value: if funding {
            d(i, "collateral_value")
        } else {
            d(i, "wallet_before")
        },
    }
}

/// The adapter reads only formula/input. Expected data never enters a calculator.
fn evaluate(formula: &str, i: &Value) -> Value {
    match formula {
        "SPOT_PARTIAL_FILL" => {
            let r = partial_buy_fill(
                d(i, "original_quantity"),
                d(i, "buy_cap"),
                d(i, "fill_quantity"),
                d(i, "fill_price"),
                d(i, "fee_tail"),
            )
            .unwrap();
            json!({"original_reserve":f(r.original_reserve),"actual_debit":f(r.actual_debit),"remaining_quantity":f(r.remaining_quantity),"remaining_reserve":f(r.remaining_reserve),"released_upper_bound":f(r.released_upper_bound),"debit_plus_reserve":f(r.debit_plus_reserve)})
        }
        "SPOT_RECEIVED_FEE" => {
            let r =
                received_fees(d(i, "received_amount"), rates(i), d(i, "rounding_tail")).unwrap();
            json!({"standard_fee":f(r.fees[0]),"tax_fee":f(r.fees[1]),"special_fee":f(r.fees[2]),"total_fee":f(r.total),"net_received":f(r.net_received),"existing_base_inventory_reserve":f(r.external_reserve)})
        }
        "SPOT_THIRD_ASSET_FEE_ESTIMATE" => {
            let r = third_asset_fee(
                d(i, "received_quote"),
                rates(i),
                d(i, "bnb_quote_low"),
                d(i, "bnb_fee_pool_balance"),
                d(i, "rounding_tail"),
            )
            .unwrap();
            json!({"fee_quote_upper":f(r.quote_fee),"bnb_fee_estimate":f(r.third_asset_estimate),"fallback_net_received_quote":f(r.fallback_net_received),"exclusive_fee_pool_reserve":f(r.exclusive_pool),"bnb_available_for_other_actions":f(r.available_outside_pool)})
        }
        "SPOT_AVAILABLE" => {
            let baseline = SpotBaseline {
                free: d(i, "free0"),
                locked: d(i, "locked0"),
                nontrade_locked: d(i, "nontrade_locked"),
                net_cashflow: d(i, "net_cashflow_after_baseline"),
                proven_credits: d(i, "credits_after_baseline"),
                other_debits: d(i, "other_debits"),
            };
            let orders: Vec<_> = i["orders"]
                .as_array()
                .unwrap()
                .iter()
                .map(|o| OrderCoverage {
                    coverage: d(o, "coverage"),
                    actual_debit: d(o, "actual_debit"),
                    remaining_reserve: d(o, "remaining_reserve"),
                })
                .collect();
            let r = available(baseline, &orders).unwrap();
            json!({"total_assets":f(r.total_assets),"local_free":f(r.local_free),"venue_cap":f(r.venue_cap),"available":f(r.available)})
        }
        "ONE_WAY_ENVELOPE" => {
            let r = one_way_envelope(d(i, "position"), d(i, "pending_buy"), d(i, "pending_sell"))
                .unwrap();
            json!({"position_min":f(r.min),"position_max":f(r.max),"max_abs_position":f(r.max_abs),"capital_quantity":f(r.capital_quantity),"capital_notional":f(r.capital_quantity.mul_to(d(i,"stress_price_high"),8,Rounding::Ceil).unwrap()),"all_filled_position":f(r.all_filled)})
        }
        "HEDGE_ENVELOPE" => {
            let r = hedge_envelope(HedgePosition {
                long: d(i, "long"),
                short: d(i, "short"),
                open_long: d(i, "open_long"),
                open_short: d(i, "open_short"),
                close_long: d(i, "close_long"),
                close_short: d(i, "close_short"),
            })
            .unwrap();
            json!({"long_max":f(r.long_max),"short_max":f(r.short_max),"net_min":f(r.net_min),"net_max":f(r.net_max),"capital_quantity":f(r.capital_quantity),"capital_notional":f(r.capital_quantity.mul_to(d(i,"stress_price_high"),8,Rounding::Ceil).unwrap())})
        }
        "MM_TIER_AND_UPPER" => {
            let brackets: Vec<_> = i["brackets"]
                .as_array()
                .unwrap()
                .iter()
                .map(|b| Bracket {
                    floor: d(b, "floor"),
                    cap: d(b, "cap"),
                    rate: d(b, "rate"),
                    cumulative: d(b, "cum"),
                    max_initial_leverage: d(b, "max_initial_leverage"),
                })
                .collect();
            let qualified =
                QualifiedBrackets::new(&brackets, d(i, "notional_coef"), bracket_interpretation(i))
                    .unwrap();
            let results: Vec<_> = i["notionals"]
                .as_array()
                .unwrap()
                .iter()
                .map(|n| {
                    qualified
                        .maintenance(Fixed::parse(n.as_str().unwrap(), 8).unwrap())
                        .unwrap()
                })
                .collect();
            json!({"tiers":results.iter().map(|r|r.tier.to_string()).collect::<Vec<_>>(),"tier_maintenance":amounts(results.iter().map(|r|r.tier_maintenance)),"upper_maintenance":amounts(results.iter().map(|r|r.upper_maintenance))})
        }
        "BRACKET_QUALIFICATION" => {
            let r = qualify_brackets(d(i, "notional_coef"), bracket_interpretation(i));
            // The supported policy consumes effective returned values unchanged.
            // Unknown qualification cannot request a cap transformation.
            json!({"decision":decision(r),"reason":reason(r),"multiply_returned_caps":false})
        }
        "COLLATERAL_VALUE" => {
            let mut values = Vec::new();
            let mut invalidated = zero();
            let mut total = zero();
            for a in i["assets"].as_array().unwrap() {
                let asset = CollateralAsset {
                    balance: d(a, "balance"),
                    valuation_low: d(a, "valuation_low"),
                    valuation_high: d(a, "valuation_high"),
                    eligible_ratio: d(a, "eligible_ratio"),
                    eligible: a["ratio_valid"].as_bool().unwrap(),
                };
                let value = collateral_component(asset, 8).unwrap();
                values.push(value);
                total = total.checked_add(value).unwrap();
                let invalid = CollateralAsset {
                    eligible: asset.eligible && a["asset"] != i["invalidate_asset"],
                    ..asset
                };
                invalidated = invalidated
                    .checked_add(collateral_component(invalid, 8).unwrap())
                    .unwrap();
            }
            json!({"components":amounts(values),"collateral_value":f(total),"collateral_after_invalidation":f(invalidated)})
        }
        "POTENTIAL_ORDER_SUM" => {
            let orders: Vec<_> = i["orders"]
                .as_array()
                .unwrap()
                .iter()
                .map(|o| PotentialOrder {
                    quantity: d(o, "quantity"),
                    buy_cap: d(o, "buy_cap"),
                })
                .collect();
            let total = potential_order_sum(&orders, 8).unwrap();
            let mut account = ReservationAccount::new(Money {
                asset: AssetId(0),
                value: total,
            })
            .unwrap();
            account
                .reserve(Money {
                    asset: AssetId(0),
                    value: total,
                })
                .unwrap();
            let r = unknown_exposure_gate(i["has_unknown"].as_bool().unwrap());
            json!({"reserves":amounts(orders.iter().map(|&o|potential_order_reserve(o,8).unwrap())),"total_reserve":f(total),"release_on_timeout":f(account.on_timeout()),"new_risk_decision":decision(r),"reason":reason(r)})
        }
        "SERIAL_RESERVATION" => {
            let mut account = ReservationAccount::new(Money {
                asset: AssetId(0),
                value: d(i, "available"),
            })
            .unwrap();
            let mut decisions = Vec::new();
            let mut values = Vec::new();
            let mut results = Vec::new();
            for request in i["requests"].as_array().unwrap() {
                let r = account.reserve(Money {
                    asset: AssetId(0),
                    value: d(request, "amount"),
                });
                decisions.push(decision(r));
                values.push(account.available().value);
                results.push(r);
            }
            json!({"decisions":decisions,"available_after_each":amounts(values),"second_reason":reason(results[1])})
        }
        "REDUCE_ALLOCATION" => {
            let existing: Vec<_> = i["existing_allocations"]
                .as_array()
                .unwrap()
                .iter()
                .map(|x| Fixed::parse(x.as_str().unwrap(), 8).unwrap())
                .collect();
            let r = allocate_reduce(
                d(i, "actual_long"),
                &existing,
                d(i, "requested_quantity"),
                Fixed::from_atoms(1, 8).unwrap(),
                i["allow_partial_reduce"].as_bool().unwrap(),
                PositionSide::Both,
            )
            .unwrap();
            json!({"reducible_before":f(r.reducible_before),"allocated_quantity":f(r.allocated),"reducible_after":f(r.reducible_after),"position_side":match r.position_side{PositionSide::Both=>"BOTH",PositionSide::Long=>"LONG",PositionSide::Short=>"SHORT"},"side":match r.side{Side::Buy=>"BUY",Side::Sell=>"SELL"},"reduce_only":r.reduce_only})
        }
        "FIXED_POINT_BOUNDARIES" => {
            let raw = |key: &str| i[key].as_str().unwrap().parse::<i128>().unwrap();
            let q = Fixed::from_atoms(
                raw("quantity_atoms"),
                i["quantity_scale"].as_u64().unwrap() as u8,
            )
            .unwrap();
            let p = Fixed::from_atoms(raw("price_atoms"), i["price_scale"].as_u64().unwrap() as u8)
                .unwrap();
            let scale = i["money_scale"].as_u64().unwrap() as u8;
            let lo = q.mul_to(p, scale, Rounding::Floor).unwrap();
            let hi = q.mul_to(p, scale, Rounding::Ceil).unwrap();
            let overflow = Fixed::from_atoms(raw("overflow_left"), 0)
                .unwrap()
                .checked_mul_int(raw("overflow_right"))
                .map(|_| ());
            json!({"notional_floor_atoms":lo.atoms().to_string(),"notional_ceil_atoms":hi.atoms().to_string(),"notional_floor":f(lo),"notional_ceil":f(hi),"signed_floor":floor_div(raw("signed_numerator"),raw("signed_denominator")).unwrap().to_string(),"signed_ceil":ceil_div(raw("signed_numerator"),raw("signed_denominator")).unwrap().to_string(),"overflow_decision":decision(overflow),"overflow_reason":reason(overflow)})
        }
        "FUTURES_CAPITAL" => {
            let orders = [
                PendingOrder {
                    side: Side::Buy,
                    quantity: d(i, "buy_quantity"),
                    price_bound: d(i, "buy_cap"),
                },
                PendingOrder {
                    side: Side::Sell,
                    quantity: d(i, "sell_quantity"),
                    price_bound: d(i, "sell_floor"),
                },
            ];
            let r = one_way_capital(
                d(i, "position_long"),
                d(i, "entry"),
                &orders,
                params(i, true),
            )
            .unwrap();
            json!({"capital_quantity":f(r.capital_quantity),"capital_notional":f(r.capital_notional),"initial_charge":f(r.initial_charge),"maintenance_upper":f(r.maintenance_upper),"position_loss":f(r.position_loss),"order_loss":f(r.order_loss),"execution_fee":f(r.execution_fee),"exit_fee":f(r.exit_fee),"funding_reserve":f(r.funding_reserve),"capital":f(r.capital),"local_headroom":f(r.local_headroom),"exit_quantity":f(r.exit_quantity)})
        }
        "STRATEGY_ACCOUNT_NETTING" => {
            let positions: Vec<_> = i["strategy_positions"]
                .as_array()
                .unwrap()
                .iter()
                .map(|p| Fixed::parse(p.as_str().unwrap(), 8).unwrap())
                .collect();
            let r = account_netting(
                &positions,
                d(i, "strategy_a_sell").checked_neg().unwrap(),
                d(i, "price"),
                8,
            )
            .unwrap();
            json!({"account_position_before":f(r.position_before),"strategy_gross_notional_before":f(r.gross_notional),"account_net_notional_before":f(r.net_notional_before),"account_position_after":f(r.position_after),"account_net_notional_after":f(r.net_notional_after),"risk_class":match r.risk_class {RiskClass::AccountIncreasing=>"ACCOUNT_INCREASING",RiskClass::AccountNonincreasing=>"ACCOUNT_NONINCREASING"}})
        }
        "VENUE_SHADOW_CAP" => {
            let holds: Vec<_> = i["uncovered_holds"]
                .as_array()
                .unwrap()
                .iter()
                .map(|x| Fixed::parse(x.as_str().unwrap(), 8).unwrap())
                .collect();
            let shadow = VenueShadow::calculate(
                d(i, "available0"),
                &holds,
                d(i, "uncovered_other_debits"),
                d(i, "market_additional_hold"),
            )
            .unwrap();
            let r = shadow.admit(d(i, "new_capital_delta"));
            json!({"shadow_available":f(shadow.available),"decision":decision(r),"reason":reason(r)})
        }
        "FEE_ROUNDING_TAIL" => {
            let r = fee_rounding_tail(
                d(i, "received_amount"),
                d(i, "fee_rate"),
                i["maximum_remaining_fills"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap(),
                d(i, "fee_asset_quantum"),
            )
            .unwrap();
            json!({"aggregate_fee_ceil":f(r.aggregate_fee),"rounding_tail":f(r.rounding_tail),"fee_reserve":f(r.reserve)})
        }
        "SIGNED_POSITION_LOSS" => {
            let r = position_loss(
                d(i, "position"),
                d(i, "entry"),
                StressPrices {
                    low: d(i, "price_low"),
                    high: d(i, "price_high"),
                },
                8,
            )
            .unwrap();
            json!({"absolute_quantity":f(r.absolute_quantity),"stress_price":f(r.stress_price),"position_loss":f(r.loss)})
        }
        "POST_FILL_EXIT_FEE" => {
            let p = params(i, false);
            let orders = [PendingOrder {
                side: Side::Buy,
                quantity: d(i, "buy_quantity"),
                price_bound: d(i, "buy_cap"),
            }];
            let before = one_way_capital(d(i, "position"), d(i, "fill_price"), &orders, p).unwrap();
            let after_position = d(i, "position").checked_add(d(i, "fill_quantity")).unwrap();
            let paid_fee = d(i, "fill_quantity")
                .mul_to(d(i, "fill_price"), 8, Rounding::Ceil)
                .unwrap()
                .mul_to(d(i, "execution_fee_rate"), 8, Rounding::Ceil)
                .unwrap();
            let wallet = d(i, "wallet_before").checked_sub(paid_fee).unwrap();
            let after = one_way_capital(
                after_position,
                d(i, "fill_price"),
                &[],
                CapitalParameters {
                    collateral_value: wallet,
                    ..p
                },
            )
            .unwrap();
            let adequate_wallet = before.capital.checked_sub(paid_fee).unwrap();
            let adequate = one_way_capital(
                after_position,
                d(i, "fill_price"),
                &[],
                CapitalParameters {
                    collateral_value: adequate_wallet,
                    ..p
                },
            )
            .unwrap();
            json!({"max_abs_position":f(before.exit_quantity),"initial_charge_before":f(before.initial_charge),"position_loss_before":f(before.position_loss),"order_loss_before":f(before.order_loss),"execution_fee_before":f(before.execution_fee),"exit_fee_before":f(before.exit_fee),"capital_before":f(before.capital),"local_headroom_before":f(before.local_headroom),"decision":decision(before.admission()),"reason":reason(before.admission()),"hypothetical_position_after":f(after_position),"hypothetical_wallet_after":f(wallet),"hypothetical_exit_fee_after":f(after.exit_fee),"hypothetical_capital_after":f(after.capital),"hypothetical_local_headroom_after":f(after.local_headroom),"minimum_wallet_before":f(before.capital),"adequately_funded_wallet_after":f(adequate_wallet),"adequately_funded_headroom_after":f(adequate.local_headroom)})
        }
        "LINEAR_SETTLEMENT_RECONCILE" => {
            let reserve = settlement_fee_reserve(
                d(i, "position_signed").checked_abs().unwrap(),
                d(i, "stress_price_high"),
                zero(),
                d(i, "settlement_fee_rate_upper"),
                d(i, "settlement_rounding_buffer"),
            )
            .unwrap();
            let reference = reference_settlement(
                d(i, "position_signed"),
                d(i, "entry_price"),
                d(i, "actual_settlement_price"),
                d(i, "actual_fee_rate_for_reference"),
                8,
            )
            .unwrap();
            let mut state =
                Settlement::<16>::new(d(i, "position_signed"), d(i, "wallet_before"), reserve)
                    .unwrap();
            let released_on_timer = state.on_expiry_timer();
            let timer_position = state.position();
            let timer_fee = state.fee_reserve();
            let timer_state = state.state();
            let estimated_income = state
                .report(reference, d(i, "wallet_before"))
                .unwrap()
                .actual_net_income;
            for (j, event) in i["actual_income_components"]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
            {
                state
                    .record_income(Income {
                        id: j as u128,
                        kind: if event["canonical_type"] == "SETTLEMENT_FEE" {
                            IncomeKind::SettlementFee
                        } else {
                            IncomeKind::SettlementPnl
                        },
                        amount: d(event, "amount"),
                    })
                    .unwrap();
            }
            let report = state
                .report(reference, d(i, "observed_wallet_after"))
                .unwrap();
            let released = state
                .reconcile(
                    reference,
                    ReconcileFacts {
                        complete_account_facts: i["complete_account_facts_after_reconcile"]
                            .as_bool()
                            .unwrap(),
                        unresolved_orders: i["unresolved_orders_after_reconcile"].as_u64().unwrap()
                            as usize,
                        actual_position: d(i, "actual_position_after_reconcile"),
                        observed_wallet: d(i, "observed_wallet_after"),
                    },
                )
                .unwrap();
            // Linear delivery explicitly has no periodic funding. Reconciliation
            // leaves the old contract's observed position; no new position opens.
            json!({"funding_reserve":f(funding_reserve(d(i,"position_signed").checked_abs().unwrap().mul_to(d(i,"stress_price_high"),8,Rounding::Ceil).unwrap(),FundingMode::None).unwrap()),"fee_reserve_before":f(reserve),"reference_pnl":f(reference.pnl),"reference_fee":f(reference.fee),"reference_net":f(reference.net),"actual_net_income":f(report.actual_net_income),"expected_wallet_from_actual_income":f(report.expected_wallet),"wallet_reconciliation_difference":f(report.wallet_difference),"reference_vs_actual_difference":f(report.reference_difference),"position_after_timer":f(timer_position),"fee_reserve_after_timer":f(timer_fee),"released_on_timer":f(released_on_timer),"estimated_price_income_booked":f(estimated_income),"state_after_timer":timer_state.name(),"position_after_reconcile":f(state.position()),"remaining_fee_reserve_released":f(released),"state_after_reconcile":state.state().name(),"automatically_roll_position":state.position()!=d(i,"actual_position_after_reconcile")})
        }
        other => panic!("unhandled formula: {other}"),
    }
}

#[test]
fn all_design_vectors_match_public_calculators() {
    let contract: Value = serde_json::from_str(include_str!(
        "../../../specs/contracts/risk-vectors-v1.json"
    ))
    .unwrap();
    let cases = contract["cases"].as_array().unwrap();
    let mut fields = 0;
    for case in cases {
        let actual = evaluate(case["formula"].as_str().unwrap(), &case["input"]);
        assert_eq!(actual, case["expected"], "case {}", case["id"]);
        fields += case["expected"].as_object().unwrap().len();
    }
    assert_eq!((cases.len(), fields), (21, 131));
    println!(
        "validated {} risk cases / {fields} expected fields",
        cases.len()
    );
}
