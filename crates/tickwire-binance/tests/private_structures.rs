mod common;
use common::*;
use tickwire_binance::{events::*, feed::TimeUnit, request::Market};
use tickwire_venue as v;

fn session(market: Market) -> SessionContext {
    SessionContext::new(
        SessionIdentity {
            risk_domain_id: [1; 16],
            venue: 1,
            environment: v::Environment::Test,
            account: 2,
            connection_epoch: 7,
        },
        market,
        match market {
            Market::Spot => PrivateStream::SpotSubscription { subscription_id: 9 },
            Market::Usdm => PrivateStream::UsdmListenKey,
        },
        TimeUnit::Milliseconds,
    )
    .unwrap()
}
fn setup(algo: bool) -> (v::CanonicalCommand, Vec<v::OrderIdentity>) {
    let mut c = command();
    let v::CommandBody::New(mut first) = c.body.clone() else {
        unreachable!()
    };
    let specs = if algo {
        c.context.request.product = v::Product::LinearPerpetual;
        c.context.instrument.contract = v::ContractKind::Perpetual;
        first.mode = v::ProductMode::Derivative {
            position_mode: v::PositionMode::OneWay,
            margin_mode: v::MarginMode::Cross,
        };
        first.order_type = v::OrderType::StopLimit;
        first.trigger = Some(v::Trigger {
            price: d(61000, 0),
            working_price: v::WorkingPrice::Contract,
            price_protect: false,
        });
        c.body = v::CommandBody::Algo(v::AlgoOrder {
            parent_order_id: [7; 16],
            order: first.clone(),
        });
        vec![first]
    } else {
        let mut second = first.clone();
        second.client_id = text("pending");
        c.body = v::CommandBody::SpotList(v::ListOrder {
            list_id: [7; 16],
            client_id: text("parent"),
            kind: v::ListKind::Oto,
            legs: v::BoundedList::new(vec![first.clone(), second.clone()]).unwrap(),
        });
        vec![first, second]
    };
    let ids = specs
        .iter()
        .enumerate()
        .map(|(n, s)| v::OrderIdentity {
            order_id: [n as u8 + 10; 16],
            original_intent_id: 3,
            original_intent_exec_epoch: 4,
            client_id: Some(s.client_id.clone()),
            venue_order_id: None,
            list_id: (!algo).then_some([7; 16]),
            list_client_id: (!algo).then(|| text("parent")),
            venue_list_id: None,
            parent_order_id: algo.then_some([7; 16]),
            algo_client_id: algo.then(|| text("test-1")),
            venue_algo_id: None,
        })
        .collect();
    (v::CanonicalCommand::new(c).unwrap(), ids)
}
const LIST: &[u8] = br#"{"subscriptionId":9,"event":{"e":"listStatus","E":10,"T":9,"s":"BTCUSDT","g":55,"c":"OTO","l":"ALL_DONE","L":"ALL_DONE","r":"NONE","C":"parent","O":[{"s":"BTCUSDT","i":21,"c":"test-1"},{"s":"BTCUSDT","i":22,"c":"pending"}]}}"#;
const ALGO: &[u8] = br#"{"e":"ALGO_UPDATE","E":10,"T":9,"o":{"s":"BTCUSDT","caid":"test-1","aid":55,"at":"CONDITIONAL","o":"STOP","S":"BUY","ps":"BOTH","f":"GTC","q":"0.001","X":"FINISHED","ai":"9007199254740993","ap":"60000","aq":"0.001","act":"LIMIT","tp":"61000","p":"60000","tt":9,"gtd":0,"rm":"","ia":false}}"#;

#[test]
fn parent_reports_bind_original_legs_without_fabricating_fill_or_child_terminal() {
    for (algo, bytes, market) in [(false, LIST, Market::Spot), (true, ALGO, Market::Usdm)] {
        let (command, ids) = setup(algo);
        let message = decode_private::<3>(&session(market), bytes, 4096, 100).unwrap();
        let bound =
            bind_structure(&message, &command, &ids).expect("known parent and exact original legs");
        let v::VenueEventBody::Structure(s) = &bound.event().body else {
            panic!("parent only")
        };
        assert_eq!(s.status, v::StructureStatus::Terminal);
        assert_eq!(s.children.as_slice().len(), ids.len());
        assert_eq!(s.children.as_slice()[0].order.order_id, ids[0].order_id);
        assert_eq!(s.parent.venue_parent_id.as_ref().unwrap().as_str(), "55");
        assert_eq!(bound.event().identity.source_connection_epoch, 7);
        assert_eq!(bound.event().identity.venue_time_ns, Some(10_000_000));
        assert_eq!(v::CanonicalEvent::decode(bound.bytes()).unwrap(), bound);
    }
}

#[test]
fn parent_binding_rejects_cross_account_parent_or_reused_native_child() {
    for (algo, bytes, market) in [(false, LIST, Market::Spot), (true, ALGO, Market::Usdm)] {
        let (command, ids) = setup(algo);
        let message = decode_private::<3>(&session(market), bytes, 4096, 100).unwrap();
        for case in 0..4 {
            let mut command = command.command().clone();
            let mut ids = ids.clone();
            match case {
                0 => command.context.request.account += 1,
                1 => ids[0].original_intent_exec_epoch += 1,
                2 => {
                    if algo {
                        ids[0].parent_order_id = Some([9; 16])
                    } else {
                        ids[0].list_id = Some([9; 16])
                    }
                }
                _ => ids[0].venue_order_id = Some(text("999")),
            }
            assert!(
                bind_structure(&message, &v::CanonicalCommand::new(command).unwrap(), &ids)
                    .is_err()
            );
        }
    }
}

#[test]
fn algo_report_with_fill_fields_but_no_child_is_unknown_not_untriggered() {
    let (command, ids) = setup(true);
    let raw = std::str::from_utf8(ALGO)
        .unwrap()
        .replace("9007199254740993", "");
    let message = decode_private::<3>(&session(Market::Usdm), raw.as_bytes(), 4096, 100).unwrap();
    let bound = bind_structure(&message, &command, &ids).unwrap();
    let v::VenueEventBody::Structure(s) = &bound.event().body else {
        unreachable!()
    };
    assert_eq!(s.status, v::StructureStatus::Unknown);
    assert!(s.children.as_slice().is_empty());
}

#[test]
fn actual_rest_parent_rows_use_the_same_binding_rules_and_keep_unknown_status() {
    let list=br#"{"orderListId":55,"contingencyType":"OTO","listStatusType":"ALL_DONE","listOrderStatus":"ALL_DONE","listClientOrderId":"parent","symbol":"BTCUSDT","orders":[{"symbol":"BTCUSDT","orderId":21,"clientOrderId":"test-1"},{"symbol":"BTCUSDT","orderId":22,"clientOrderId":"pending"}]}"#;
    let algo=br#"{"algoId":55,"clientAlgoId":"test-1","algoType":"CONDITIONAL","orderType":"STOP","symbol":"BTCUSDT","side":"BUY","positionSide":"BOTH","timeInForce":"GTC","quantity":"0.001","algoStatus":"FINISHED","actualOrderId":"9007199254740993","actualPrice":"60000","actualQty":"0.001","actualType":"LIMIT","triggerPrice":"61000","price":"60000","triggerTime":9}"#;
    for (is_algo, bytes, market) in [
        (false, list.as_slice(), Market::Spot),
        (true, algo.as_slice(), Market::Usdm),
    ] {
        let (command, ids) = setup(is_algo);
        let context = session(market);
        let result =
            bind_structure_response(context.identity(), market, &command, &ids, bytes, 100)
                .unwrap();
        let v::VenueEventBody::Structure(s) = &result.event().body else {
            unreachable!()
        };
        assert_eq!(s.status, v::StructureStatus::Terminal);
        assert_eq!(s.children.as_slice().len(), ids.len());
        assert_eq!(result.event().identity.venue_time_ns, None);
        if is_algo {
            let unknown = std::str::from_utf8(bytes)
                .unwrap()
                .replace("FINISHED", "NEW_FUTURE_STATUS");
            let result = bind_structure_response(
                context.identity(),
                market,
                &command,
                &ids,
                unknown.as_bytes(),
                100,
            )
            .unwrap();
            assert!(
                matches!(result.event().body,v::VenueEventBody::Structure(ref s) if s.status==v::StructureStatus::Unknown)
            );
            let contradiction = std::str::from_utf8(bytes)
                .unwrap()
                .replace("9007199254740993", "");
            let result = bind_structure_response(
                context.identity(),
                market,
                &command,
                &ids,
                contradiction.as_bytes(),
                100,
            )
            .unwrap();
            assert!(
                matches!(result.event().body,v::VenueEventBody::Structure(ref s) if s.status==v::StructureStatus::Unknown)
            );
        }
    }
}
