#![allow(dead_code)]
mod common;
use common::*;
use tickwire_venue::*;

fn roundtrip(c: Command) {
    let p = CanonicalCommand::new(c.clone()).unwrap();
    assert_eq!(CanonicalCommand::decode(p.bytes()).unwrap().command(), &c)
}
fn derivative() -> Command {
    let mut c = command();
    c.context.request.product = Product::LinearPerpetual;
    c.context.instrument.contract = ContractKind::Perpetual;
    if let CommandBody::New(ref mut o) = c.body {
        o.mode = ProductMode::Derivative {
            position_mode: PositionMode::OneWay,
            margin_mode: MarginMode::Cross,
        }
    }
    c
}
#[test]
fn all_ten_command_branches_preserve_complete_content() {
    let mut c = command();
    roundtrip(c.clone());
    for body in [
        CommandBody::Cancel(target()),
        CommandBody::Query(target()),
        CommandBody::CancelReplace {
            target: target(),
            replacement: order(),
            mode: CancelReplaceMode::AllowFailure,
        },
        CommandBody::AmendKeepPriority {
            target: target(),
            new_quantity: d(5, -4),
            new_client_id: Some(text("amended")),
        },
        CommandBody::Modify(ModifyOrder {
            target: target(),
            new_client_id: Some(text("modified")),
            side: Side::Sell,
            quantity: d(2, -3),
            price: d(70000, 0),
            position_side: PositionSide::Both,
            reduce_only: false,
        }),
        CommandBody::CancelList(ListTarget {
            list_id: [7; 16],
            expected_owner_revision: 3,
            original_intent_exec_epoch: 2,
            client_id: Some(text("list")),
            venue_list_id: Some(text("native-list")),
        }),
    ] {
        c.body = body;
        roundtrip(c.clone())
    }
    let mut second = order();
    second.client_id = text("second");
    second.order_type = OrderType::StopLimit;
    second.trigger = Some(Trigger {
        price: d(55000, 0),
        working_price: WorkingPrice::Contract,
        price_protect: false,
    });
    c.body = CommandBody::SpotList(ListOrder {
        list_id: [7; 16],
        client_id: text("list"),
        kind: ListKind::Oco,
        legs: BoundedList::new(vec![order(), second]).unwrap(),
    });
    roundtrip(c);
    let mut c = derivative();
    let CommandBody::New(mut o) = c.body else {
        panic!()
    };
    o.order_type = OrderType::StopMarket;
    o.quantity = Quantity::ClosePosition;
    o.price = None;
    o.time_in_force = None;
    o.trigger = Some(Trigger {
        price: d(55000, 0),
        working_price: WorkingPrice::Mark,
        price_protect: true,
    });
    c.body = CommandBody::Algo(AlgoOrder {
        parent_order_id: [8; 16],
        order: o,
    });
    roundtrip(c.clone());
    c.body = CommandBody::CancelAlgo(AlgoTarget {
        parent_order_id: [8; 16],
        expected_owner_revision: 2,
        original_intent_exec_epoch: 2,
        client_id: Some(text("algo-client")),
        venue_algo_id: Some(text("native-algo")),
    });
    roundtrip(c);
}
#[test]
fn spot_conditional_trailing_keeps_base_type_and_units() {
    for kind in [
        OrderType::StopLimit,
        OrderType::TakeProfitLimit,
        OrderType::StopMarket,
        OrderType::TakeProfitMarket,
    ] {
        let mut c = command();
        if let CommandBody::New(ref mut o) = c.body {
            o.order_type = kind;
            o.trailing = Some(Trailing {
                activation_price: None,
                working_price: WorkingPrice::Contract,
                distance: TrailingDistance::BasisPoints(100),
            });
            if matches!(kind, OrderType::StopMarket | OrderType::TakeProfitMarket) {
                o.price = None;
                o.time_in_force = None
            }
        }
        roundtrip(c.clone());
        if let CommandBody::New(ref mut o) = c.body {
            o.trigger = Some(Trigger {
                price: d(50000, 0),
                working_price: WorkingPrice::Contract,
                price_protect: false,
            })
        }
        roundtrip(c);
    }
}
#[test]
fn derivative_trailing_and_delivery_are_distinct() {
    let mut c = derivative();
    if let CommandBody::New(ref mut o) = c.body {
        o.order_type = OrderType::TrailingStop;
        o.price = None;
        o.time_in_force = None;
        o.trailing = Some(Trailing {
            activation_price: Some(d(62000, 0)),
            working_price: WorkingPrice::Mark,
            distance: TrailingDistance::Percent(d(1, -1)),
        })
    }
    roundtrip(c.clone());
    let perpetual = CanonicalCommand::new(c.clone()).unwrap();
    c.context.request.product = Product::LinearFuture;
    c.context.instrument.contract = ContractKind::Delivery {
        expiry_utc_ns: 123456,
    };
    let delivery = CanonicalCommand::new(c.clone()).unwrap();
    assert_ne!(perpetual.digest(), delivery.digest());
    roundtrip(c);
}
#[test]
fn cross_product_and_inapplicable_parameters_are_rejected() {
    let mut c = command();
    if let CommandBody::New(ref mut o) = c.body {
        o.mode = ProductMode::Derivative {
            position_mode: PositionMode::OneWay,
            margin_mode: MarginMode::Cross,
        }
    }
    assert_eq!(
        CanonicalCommand::new(c).unwrap_err(),
        Error::ProductCombination
    );
    let mut c = derivative();
    if let CommandBody::New(ref mut o) = c.body {
        o.mode = ProductMode::Derivative {
            position_mode: PositionMode::Hedge,
            margin_mode: MarginMode::Isolated,
        }
    }
    assert_eq!(
        CanonicalCommand::new(c).unwrap_err(),
        Error::ProductCombination
    );
    let mut c = command();
    if let CommandBody::New(ref mut o) = c.body {
        o.quantity = Quantity::Quote(d(100, 0));
        o.side = Side::Sell
    }
    assert_eq!(
        CanonicalCommand::new(c).unwrap_err(),
        Error::OrderCombination
    );
    let mut c = command();
    if let CommandBody::New(ref mut o) = c.body {
        o.order_type = OrderType::StopMarket;
        o.price = None;
        o.time_in_force = None;
        o.trigger = Some(Trigger {
            price: d(1, 0),
            working_price: WorkingPrice::Mark,
            price_protect: false,
        })
    }
    assert_eq!(
        CanonicalCommand::new(c).unwrap_err(),
        Error::ProductCombination
    );
}
#[test]
fn gtd_iceberg_and_grid_validation() {
    let mut c = command();
    if let CommandBody::New(ref mut o) = c.body {
        o.time_in_force = Some(TimeInForce::Gtd { expire_utc_ns: 0 })
    }
    assert_eq!(CanonicalCommand::new(c).unwrap_err(), Error::Deadline);
    let mut c = command();
    if let CommandBody::New(ref mut o) = c.body {
        o.iceberg_quantity = Some(d(2, -3))
    }
    assert_eq!(CanonicalCommand::new(c).unwrap_err(), Error::Quantity);
    let mut c = command();
    if let CommandBody::New(ref mut o) = c.body {
        o.price = Some(d(60000123, -3))
    }
    assert_eq!(CanonicalCommand::new(c).unwrap_err(), Error::Price);
    let mut c = command();
    if let CommandBody::New(ref mut o) = c.body {
        o.quantity = Quantity::Base(d(1, -6))
    }
    assert_eq!(CanonicalCommand::new(c).unwrap_err(), Error::Quantity);
}
#[test]
fn list_client_identity_and_future_target_epoch_reject() {
    let mut c = command();
    c.body = CommandBody::SpotList(ListOrder {
        list_id: [8; 16],
        client_id: text("list"),
        kind: ListKind::Oco,
        legs: BoundedList::new(vec![order(), order()]).unwrap(),
    });
    assert_eq!(CanonicalCommand::new(c).unwrap_err(), Error::Duplicate);
    let mut c = command();
    let mut t = target();
    t.original_intent_exec_epoch = 5;
    c.body = CommandBody::Cancel(t);
    assert_eq!(CanonicalCommand::new(c).unwrap_err(), Error::Identity);
}
#[test]
fn every_request_context_field_is_digest_bound() {
    let original = CanonicalCommand::new(command()).unwrap();
    macro_rules! changed {($path:ident $(.$field:ident)*, $v:expr)=>{{let mut c=command();c.$path$(.$field)* = $v;let p=CanonicalCommand::new(c).unwrap();assert_ne!(p.digest(),original.digest(),stringify!($path$(.$field)*));}}}
    changed!(context.request.risk_domain_id, [6; 16]);
    changed!(context.request.venue, 2);
    changed!(context.request.environment, Environment::Production);
    changed!(context.request.account, 99);
    changed!(context.request.intent_id, 99);
    changed!(context.request.intent_exec_epoch, 99);
    changed!(context.request.strategy_generation, 99);
    changed!(context.request.config_revision, 99);
    changed!(context.request.deadline, 99);
    changed!(context.request.recovery_object, 99);
    changed!(context.strategy.logical_strategy_id, 99);
    changed!(context.strategy.instance_id, [6; 16]);
    changed!(context.strategy.strategy_intent_seq, 99);
    changed!(context.strategy.authorization_epoch, 99);
    changed!(context.strategy.risk_policy_version, 99);
    changed!(context.strategy.order_config_version, 99);
    changed!(context.strategy.created_mono_ns, 51);
    changed!(context.strategy.economic_intent_id, [6; 16]);
    changed!(context.strategy.hedge_group_id, [6; 16]);
    changed!(context.strategy.leg_id, 99);
    changed!(context.strategy.recovery_manifest_hash, [6; 32]);
    changed!(context.instrument.instrument_id, 99);
    changed!(context.instrument.instrument_version, 99);
    changed!(context.instrument.catalog_version, 99);
    changed!(context.instrument.catalog_hash, [6; 32]);
    changed!(context.instrument.symbol, Symbol::new("BTC-USDT").unwrap());
    changed!(context.instrument.price_tick, d(5, -2));
    changed!(context.instrument.quantity_step, d(5, -5));
    changed!(context.instrument.base.version, 99);
    changed!(context.instrument.quote.version, 99);
    changed!(context.instrument.settlement.version, 99);
}
#[test]
fn price_grid_matches_independent_bounded_integer_arithmetic() {
    for amount in 1..=31i128 {
        for tick in 1..=17i128 {
            for amount_exp in -2..=2i8 {
                for tick_exp in -2..=2i8 {
                    let mut c = command();
                    c.context.instrument.price_tick = d(tick, tick_exp);
                    if let CommandBody::New(ref mut o) = c.body {
                        o.price = Some(d(amount, amount_exp))
                    }
                    let scaled_amount = amount * 10i128.pow((amount_exp + 2) as u32);
                    let scaled_tick = tick * 10i128.pow((tick_exp + 2) as u32);
                    assert_eq!(
                        CanonicalCommand::new(c).is_ok(),
                        scaled_amount % scaled_tick == 0,
                        "{amount}e{amount_exp} / {tick}e{tick_exp}"
                    );
                }
            }
        }
    }
}

#[test]
fn order_parameters_and_trigger_observations_are_digest_bound() {
    let original = CanonicalCommand::new(command()).unwrap();
    macro_rules! changed {
        ($field:ident,$value:expr) => {{
            let mut c = command();
            if let CommandBody::New(ref mut o) = c.body {
                o.$field = $value;
            }
            assert_ne!(
                CanonicalCommand::new(c).unwrap().digest(),
                original.digest(),
                stringify!($field)
            );
        }};
    }
    changed!(client_id, text("different-client"));
    changed!(side, Side::Sell);
    changed!(quantity, Quantity::Base(d(2, -3)));
    changed!(price, Some(d(60001, 0)));
    changed!(
        time_in_force,
        Some(TimeInForce::Gtd {
            expire_utc_ns: 1000
        })
    );
    changed!(self_trade_prevention, SelfTradePrevention::ExpireMaker);
    changed!(iceberg_quantity, Some(d(1, -4)));
    changed!(
        mode,
        ProductMode::Margin {
            margin_mode: MarginMode::Isolated,
            auto_borrow: true,
            auto_repay: false
        }
    );
    let mut c = command();
    let mut t = c.context.triggers.as_slice()[0].clone();
    t.sequence += 1;
    c.context.triggers = BoundedList::new(vec![t]).unwrap();
    assert_ne!(
        CanonicalCommand::new(c).unwrap().digest(),
        original.digest()
    );
    let mut c = derivative();
    if let CommandBody::New(ref mut o) = c.body {
        o.order_type = OrderType::StopLimit;
        o.trigger = Some(Trigger {
            price: d(59000, 0),
            working_price: WorkingPrice::Contract,
            price_protect: false,
        })
    }
    let original = CanonicalCommand::new(c.clone()).unwrap();
    if let CommandBody::New(ref mut o) = c.body {
        o.trigger.as_mut().unwrap().price_protect = true;
    }
    assert_ne!(
        CanonicalCommand::new(c.clone()).unwrap().digest(),
        original.digest()
    );
    if let CommandBody::New(ref mut o) = c.body {
        o.trigger.as_mut().unwrap().working_price = WorkingPrice::Mark;
        o.reduce_only = true;
    }
    assert_ne!(
        CanonicalCommand::new(c).unwrap().digest(),
        original.digest()
    );
}
#[test]
fn each_product_contract_and_list_kind_roundtrips() {
    for product in [
        Product::LinearPerpetual,
        Product::InversePerpetual,
        Product::LinearFuture,
        Product::InverseFuture,
        Product::Option,
    ] {
        let mut c = derivative();
        c.context.request.product = product;
        c.context.instrument.contract = match product {
            Product::LinearFuture | Product::InverseFuture => ContractKind::Delivery {
                expiry_utc_ns: 1000,
            },
            Product::Option => ContractKind::Option {
                expiry_utc_ns: 1000,
                strike: d(60000, 0),
                right: OptionRight::Call,
            },
            _ => ContractKind::Perpetual,
        };
        if product == Product::Option
            && let CommandBody::New(ref mut o) = c.body
        {
            o.mode = ProductMode::Option;
        }
        roundtrip(c);
    }
    for kind in [ListKind::Oco, ListKind::Oto, ListKind::Otoco] {
        let mut c = command();
        let mut legs = vec![order(); if kind == ListKind::Otoco { 3 } else { 2 }];
        for (n, o) in legs.iter_mut().enumerate() {
            o.client_id = ClientId::new(format!("leg-{n}")).unwrap()
        }
        c.body = CommandBody::SpotList(ListOrder {
            list_id: [9; 16],
            client_id: text("list"),
            kind,
            legs: BoundedList::new(legs).unwrap(),
        });
        roundtrip(c);
    }
}

#[test]
fn spot_market_sell_accepts_a_quote_amount_target() {
    let mut c = command();
    if let CommandBody::New(ref mut o) = c.body {
        o.order_type = OrderType::Market;
        o.side = Side::Sell;
        o.quantity = Quantity::Quote(d(100, 0));
        o.price = None;
        o.time_in_force = None;
    }
    roundtrip(c);
}
#[test]
fn oto_pending_quote_amount_remains_unsupported() {
    let mut c = command();
    let mut pending = order();
    pending.client_id = text("pending");
    pending.order_type = OrderType::Market;
    pending.price = None;
    pending.time_in_force = None;
    pending.quantity = Quantity::Quote(d(100, 0));
    c.body = CommandBody::SpotList(ListOrder {
        list_id: [9; 16],
        client_id: text("list"),
        kind: ListKind::Oto,
        legs: BoundedList::new(vec![order(), pending]).unwrap(),
    });
    assert_eq!(
        CanonicalCommand::new(c).unwrap_err(),
        Error::OrderCombination
    );
}
