#![allow(dead_code)]
mod common;
use common::*;
use tickwire_venue::*;

fn roundtrip(c: Command) {
    let p = CanonicalCommand::new(c.clone()).unwrap();
    assert_eq!(CanonicalCommand::decode(p.bytes()).unwrap().command(), &c);
    let hex = |bytes: &[u8]| bytes.iter().map(|v| format!("{v:02x}")).collect::<String>();
    let expected = include_str!("data/legacy-commands.txt");
    assert_eq!(expected.lines().count(), 10);
    assert!(
        expected
            .lines()
            .any(|line| line == format!("{} {}", hex(&p.digest()), hex(p.bytes())))
    );
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
fn old_tags_keep_every_byte_and_digest() {
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
fn pegged() -> SpotOrder {
    let mut o = order();
    o.price = None;
    SpotOrder {
        order: o,
        peg: Some(Peg {
            price_type: PegPriceType::Primary,
            offset_levels: Some(0),
        }),
    }
}
fn advanced(body: SpotAdvancedCommand) -> Command {
    let mut c = command();
    c.body = CommandBody::SpotAdvanced(body);
    c
}
#[test]
fn peg_and_sor_roundtrip_all_subtags_without_changing_legacy_schema() {
    let mut pending = pegged();
    pending.order.client_id = text("pending");
    let mut oco = pegged();
    oco.order.client_id = text("stop");
    oco.order.order_type = OrderType::StopLimit;
    oco.order.trigger = Some(Trigger {
        price: d(55000, 0),
        working_price: WorkingPrice::Contract,
        price_protect: false,
    });
    for body in [
        SpotAdvancedCommand::New(pegged()),
        SpotAdvancedCommand::CancelReplace {
            target: target(),
            replacement: pegged(),
            mode: CancelReplaceMode::AllowFailure,
        },
        SpotAdvancedCommand::List(SpotAdvancedList {
            list_id: [7; 16],
            client_id: text("list"),
            kind: ListKind::Oto,
            legs: BoundedList::new(vec![pegged(), pending]).unwrap(),
        }),
        SpotAdvancedCommand::SorNew(order()),
    ] {
        let c = advanced(body);
        let p = CanonicalCommand::new(c.clone()).unwrap();
        assert_eq!(CanonicalCommand::decode(p.bytes()).unwrap().command(), &c);
        assert_eq!(SCHEMA_VERSION, 1);
    }
}
#[test]
fn peg_fields_and_sor_routing_are_part_of_digest() {
    let p = CanonicalCommand::new(advanced(SpotAdvancedCommand::New(pegged()))).unwrap();
    for change in 0..4 {
        let mut o = pegged();
        match change {
            0 => o.peg.as_mut().unwrap().price_type = PegPriceType::Market,
            1 => o.peg.as_mut().unwrap().offset_levels = Some(100),
            2 => o.peg.as_mut().unwrap().offset_levels = None,
            _ => o.order.price = Some(d(60000, 0)),
        }
        assert_ne!(
            p.digest(),
            CanonicalCommand::new(advanced(SpotAdvancedCommand::New(o)))
                .unwrap()
                .digest()
        );
    }
    assert_ne!(
        CanonicalCommand::new(command()).unwrap().digest(),
        CanonicalCommand::new(advanced(SpotAdvancedCommand::SorNew(order())))
            .unwrap()
            .digest()
    );
}
#[test]
fn advanced_rejects_invalid_combinations_and_duplicate_representations() {
    for change in 0..7 {
        let mut o = pegged();
        match change {
            0 => o.peg = None,
            1 => o.peg.as_mut().unwrap().offset_levels = Some(101),
            2 => o.order.time_in_force = Some(TimeInForce::Ioc),
            3 => {
                o.order.time_in_force = Some(TimeInForce::PostOnly);
                o.peg.as_mut().unwrap().price_type = PegPriceType::Market;
            }
            4 => {
                o.order.order_type = OrderType::Market;
                o.order.time_in_force = None;
            }
            5 => o.order.quantity = Quantity::Quote(d(10, 0)),
            _ => {
                o.order.mode = ProductMode::Margin {
                    margin_mode: MarginMode::Cross,
                    auto_borrow: false,
                    auto_repay: false,
                }
            }
        }
        assert!(CanonicalCommand::new(advanced(SpotAdvancedCommand::New(o))).is_err());
    }
    for change in 0..3 {
        let mut o = order();
        match change {
            0 => o.time_in_force = Some(TimeInForce::PostOnly),
            1 => {
                o.order_type = OrderType::Market;
                o.price = None;
                o.time_in_force = None;
                o.quantity = Quantity::Quote(d(10, 0));
            }
            _ => {
                o.order_type = OrderType::StopLimit;
                o.trigger = Some(Trigger {
                    price: d(50000, 0),
                    working_price: WorkingPrice::Contract,
                    price_protect: false,
                });
            }
        }
        assert!(CanonicalCommand::new(advanced(SpotAdvancedCommand::SorNew(o))).is_err());
    }
}
#[test]
fn peg_requires_bound_product_triggers_and_recovery() {
    for change in 0..3 {
        let mut c = advanced(SpotAdvancedCommand::New(pegged()));
        match change {
            0 => {
                c.context.request.product = Product::LinearPerpetual;
                c.context.instrument.contract = ContractKind::Perpetual;
            }
            1 => c.context.triggers = BoundedList::new(vec![]).unwrap(),
            _ => c.context.strategy.recovery_manifest_hash = [0; 32],
        }
        assert!(CanonicalCommand::new(c).is_err());
    }
}
#[test]
fn strict_tag11_lengths_enums_truncation_and_trailing_data() {
    let p = CanonicalCommand::new(advanced(SpotAdvancedCommand::New(pegged()))).unwrap();
    let bytes = p.bytes();
    for n in 0..bytes.len() {
        assert!(CanonicalCommand::decode(&bytes[..n]).is_err());
    }
    let mut trailing = bytes.to_vec();
    trailing.push(0);
    assert_eq!(
        CanonicalCommand::decode(&trailing).unwrap_err(),
        Error::TrailingData
    );
    let at = bytes
        .windows(4)
        .rposition(|b| b == [0x82, 11, 0x82, 1])
        .unwrap();
    for (offset, value) in [(0, 0x83), (1, 12), (2, 0x83), (3, 5)] {
        let mut bad = bytes.to_vec();
        bad[at + offset] = value;
        assert!(CanonicalCommand::decode(&bad).is_err());
    }
    // Last five bytes are Some(Peg{Primary,Some(0)}); unknown price type is rejected.
    assert_eq!(&bytes[bytes.len() - 5..], &[0x81, 0x82, 1, 0x81, 0]);
    let mut bad = bytes.to_vec();
    bad[bytes.len() - 3] = 3;
    assert_eq!(CanonicalCommand::decode(&bad).unwrap_err(), Error::Enum);
}
#[test]
fn advanced_lists_reject_duplicate_ids_missing_peg_and_wrong_arity() {
    for mutation in 0..4 {
        let mut first = pegged();
        let mut second = pegged();
        second.order.client_id = text("pending");
        let mut id = text("list");
        let mut legs = vec![];
        match mutation {
            0 => {
                first.peg = None;
                first.order.price = Some(d(60000, 0));
                second.peg = None;
                second.order.price = Some(d(60000, 0));
            }
            1 => second.order.client_id = first.order.client_id.clone(),
            2 => id = first.order.client_id.clone(),
            _ => {}
        }
        legs.push(first);
        if mutation != 3 {
            legs.push(second);
        }
        let c = advanced(SpotAdvancedCommand::List(SpotAdvancedList {
            list_id: [7; 16],
            client_id: id,
            kind: ListKind::Oto,
            legs: BoundedList::new(legs).unwrap(),
        }));
        assert!(CanonicalCommand::new(c).is_err());
    }
}
