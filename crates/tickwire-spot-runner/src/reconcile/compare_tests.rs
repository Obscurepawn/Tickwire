use super::*;
use gateway_fixture::fixture;
use serde_json::json;
use tickwire_account::{Account, Applied};
use tickwire_spot_execution::{
    SpotVenue,
    private::stream::Channel,
    recovery::{RemoteBalance, RemoteFill},
};
fn d(s: &str) -> Decimal {
    s.parse().unwrap()
}
fn balance(symbol: &str, total: &str, locked: &str, available: &str) -> RemoteBalance {
    RemoteBalance {
        symbol: symbol.into(),
        total: d(total),
        locked: d(locked),
        available: d(available),
    }
}
fn partial() -> (
    CanonicalBaseline,
    Account,
    RestEvidence,
    serde_json::Value,
    v::CanonicalEvent,
) {
    let (mut snap, mut cat, mut cmd) = fixture::fixture();
    snap.scope.venue = 2;
    cmd.context.request.venue = 2;
    cat.entries[0].fees.rates[0] = Fixed::parse("0.01", 8).unwrap();
    let baseline = CanonicalBaseline::new(snap.clone(), cat.clone()).unwrap();
    let mut a = Account::from_snapshot(snap, cat).unwrap();
    let canonical = fixture::canonical(cmd.clone());
    a.admit(&canonical, 60).unwrap();
    let v::CommandBody::New(spec) = &cmd.body else {
        unreachable!()
    };
    let tid = tickwire_spot_execution::trade_id(SpotVenue::Bybit, "fill1").unwrap();
    let mut f = fixture::fill(&cmd, tid.as_str(), "0.0004", "60000", "24");
    f.identity.venue = 2;
    let v::VenueEventBody::Fill(x) = &mut f.body else {
        unreachable!()
    };
    x.observation.order.venue_order_id = Some(v::VenueOrderId::new("native-1").unwrap());
    x.fees = v::BoundedList::new(vec![v::AssetDelta {
        asset: cmd.context.instrument.base.clone(),
        amount: d("-0.000001"),
    }])
    .unwrap();
    let f = v::CanonicalEvent::new(f).unwrap();
    assert_eq!(a.apply_event(&f), Ok(Applied::New));
    assert_eq!(a.apply_event(&f), Ok(Applied::Duplicate));
    let client = spec.client_id.as_str();
    let symbol = cmd.context.instrument.symbol.as_str();
    let row = json!({"category":"spot","symbol":symbol,"orderLinkId":client,"orderId":"native-1","side":"Buy","execType":"Trade","execId":"fill1","execQty":"0.0004","execPrice":"60000","execValue":"24","execFee":"0.000001","feeCurrency":"BTC","execTime":"1002","isMaker":true});
    let o = RemoteOrder {
        symbol: symbol.into(),
        client_id: client.into(),
        native_id: "native-1".into(),
        side: spec.side,
        status: OrderStatus::PartiallyFilled,
        order_type: spec.order_type,
        time_in_force: spec.time_in_force,
        qty: d("0.001"),
        cumulative: d("0.0004"),
        price: spec.price,
        created_ms: 1001,
        updated_ms: 1002,
    };
    let e = RestEvidence {
        venue: SpotVenue::Bybit,
        environment: v::Environment::Test,
        uid: "42".into(),
        start_ms: 1000,
        end_ms: 1004,
        balances: vec![
            balance("BTC", "1.000399", "0", "1.000399"),
            balance("USDT", "976", "36", "940"),
        ],
        open_orders: vec![o.clone()],
        history: vec![],
        target_orders: vec![o],
        fills: vec![RemoteFill {
            symbol: symbol.into(),
            client_id: client.into(),
            native_id: "native-1".into(),
            trade_id: "fill1".into(),
            side: spec.side,
            qty: d("0.0004"),
            price: d("60000"),
            fee: d("-0.000001"),
            fee_currency: "BTC".into(),
            time_ms: 1002,
            wire_row: serde_json::to_vec(&row).unwrap(),
        }],
        fingerprint: [1; 32],
        evidence_bytes: vec![],
    };
    let key = tickwire_account::IntentKey {
        epoch: cmd.context.request.intent_exec_epoch,
        intent_id: cmd.context.request.intent_id,
    };
    let admission = a.admission(key).unwrap().image();
    let mut rest = f.event().clone();
    rest.identity.source_connection_epoch += 1;
    rest.identity.received_mono_ns += 10;
    rest.identity.event_id = v::BoundedText::new("independent-rest-source").unwrap();
    let records = [
        tickwire_account::RecoveryRecord::Admit {
            command: Box::new(canonical),
            evaluated_at: 60,
            expected: admission,
        },
        tickwire_account::RecoveryRecord::IngressEvent {
            input_stream: [1; 16],
            input_sequence: 1,
            event: Box::new(f.clone()),
        },
        tickwire_account::RecoveryRecord::IngressEvent {
            input_stream: [2; 16],
            input_sequence: 1,
            event: Box::new(v::CanonicalEvent::new(rest).unwrap()),
        },
    ];
    let recovered = Account::recover(
        baseline.snapshot().clone(),
        baseline.catalog().clone(),
        &records,
    )
    .unwrap();
    assert!(recovered.frozen());
    for asset in &baseline.snapshot().assets {
        assert_eq!(
            a.asset(asset.key).unwrap(),
            recovered.asset(asset.key).unwrap()
        );
    }
    (baseline, recovered, e, row, f)
}
#[test]
fn partial_fill_and_fee_match_the_wallet_and_remaining_limit_lock_after_replay() {
    let (b, a, mut e, _, _) = partial();
    account(&b, &a, &e).unwrap();
    for bad in ["funds", "locks", "fee", "anonymous", "missing"] {
        let old = e.balances.clone();
        let old_open = e.open_orders.clone();
        let f = e.fills[0].clone();
        match bad {
            "funds" => e.balances[1] = balance("USDT", "977", "36", "941"),
            "locks" => e.balances[1] = balance("USDT", "976", "35", "941"),
            "fee" => e.fills[0].fee = d("0"),
            "anonymous" => e.open_orders[0].client_id = "foreign".into(),
            _ => e.open_orders.clear(),
        }
        assert!(account(&b, &a, &e).is_err(), "accepted {bad}");
        e.balances = old;
        e.open_orders = old_open;
        e.fills[0] = f;
    }
}
#[test]
fn a_limit_order_cannot_change_time_in_force_under_the_same_client_id() {
    let (b, a, mut e, _, _) = partial();
    e.open_orders[0].time_in_force = Some(v::TimeInForce::PostOnly);
    e.target_orders[0].time_in_force = Some(v::TimeInForce::PostOnly);
    assert!(account(&b, &a, &e).is_err());
}
#[test]
fn a_private_trade_cannot_disagree_with_booked_side_or_native_quote() {
    let (b, a, _, row, _) = partial();
    private_row(&b, &a, Channel::Fills, &row).unwrap();
    for (field, value) in [
        ("side", "Sell"),
        ("execValue", "23"),
        ("execType", "Funding"),
        ("execFee", "0"),
    ] {
        let mut bad = row.clone();
        bad[field] = value.into();
        assert!(
            private_row(&b, &a, Channel::Fills, &bad).is_err(),
            "accepted contradictory {field}"
        );
    }
}

#[path = "../../../tickwire-account-gateway/tests/common/mod.rs"]
mod gateway_fixture;

#[test]
fn repeated_native_fills_do_not_change_cash_and_disagreements_freeze_in_either_transport_order() {
    use tickwire_account_gateway::AccountGateway;
    use tickwire_account_journal::FileBackend;
    use tickwire_runtime::authority::RecoveryQueue;
    use tickwire_spot_execution::{Credentials, RestSession, SpotEncoder};
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        for websocket_first in [false, true] {
            for change in [
                "price",
                "quantity",
                "fee",
                "rebate",
                "fee_asset",
                "same_ws_cursor",
                "same_order_cursor",
            ] {
                let (mut snap, mut catalog, mut cmd) = fixture::fixture();
                snap.scope.venue = venue.id();
                cmd.context.request.venue = venue.id();
                let okx = venue == SpotVenue::Okx;
                let symbol = if okx { "BTC-USDT" } else { "BTCUSDT" };
                catalog.entries[0].instrument.symbol = v::Symbol::new(symbol).unwrap();
                cmd.context.instrument.symbol = v::Symbol::new(symbol).unwrap();
                catalog.entries[0].fees.rates[0] = Fixed::parse("0.01", 8).unwrap();
                let v::CommandBody::New(order) = &mut cmd.body else {
                    unreachable!()
                };
                order.client_id = v::ClientId::new("client1").unwrap();
                let command = fixture::canonical(cmd);
                let intent = gateway_fixture::intent();
                let scope = snap.scope.clone();
                let baseline = CanonicalBaseline::new(snap, catalog).unwrap();
                let credentials = if okx {
                    Credentials::okx("key", b"secret", "test")
                } else {
                    Credentials::bybit("key", b"secret")
                };
                let encoder = SpotEncoder::new(credentials, v::Environment::Test, 2, || {
                    Ok(1_700_000_000_000)
                })
                .unwrap();
                let dir = gateway_fixture::Directory::new();
                let mut config = gateway_fixture::config(&baseline);
                config.route = encoder.route();
                let (connection, wire) = gateway_fixture::peer(&config.route.authority, 65536);
                let gateway = AccountGateway::create(
                    FileBackend::open(&dir.0).unwrap(),
                    config,
                    baseline,
                    gateway_fixture::MockAuthority::default(),
                    encoder,
                    connection,
                )
                .unwrap();
                let mut runner = AccountRunner::new(
                    RestSession::new(gateway, 1).unwrap(),
                    RecoveryQueue::new(1, 1024).unwrap(),
                    tickwire_recovery::MockBlobStore::default(),
                );
                runner
                    .origins
                    .register(&scope, intent, "client1", [9; 16], command.digest())
                    .unwrap();
                runner
                    .session
                    .submit(command, 60, gateway_fixture::until())
                    .unwrap();
                let (_, _, mut rest, mut ws, _) = partial();
                let tid = if okx { "123" } else { "fill1" };
                if okx {
                    ws = json!({"instType":"SPOT","instId":symbol,"clOrdId":"client1","ordId":"555","side":"buy","tradeId":tid,"fillSz":"0.0004","fillPx":"60000","fillFee":"-0.000001","fillFeeCcy":"BTC","fillTime":"1002","execType":"M"});
                } else {
                    ws["orderLinkId"] = "client1".into();
                    ws["orderId"] = "555".into();
                }
                rest.venue = venue;
                for o in rest.open_orders.iter_mut().chain(&mut rest.target_orders) {
                    o.symbol = symbol.into();
                    o.client_id = "client1".into();
                    o.native_id = "555".into();
                }
                let f = &mut rest.fills[0];
                f.symbol = symbol.into();
                f.client_id = "client1".into();
                f.native_id = "555".into();
                f.trade_id = tid.into();
                f.wire_row = serde_json::to_vec(&ws).unwrap();
                let original_wire = f.wire_row.clone();
                let first = PrivateCut {
                    stream: [5; 16],
                    epoch: 3,
                    through: 4,
                    gap_generation: 3,
                };
                let reconnected = PrivateCut {
                    epoch: 4,
                    through: 8,
                    gap_generation: 4,
                    ..first
                };
                if websocket_first {
                    runner.apply_private_cash("fills", &ws, first, 62).unwrap();
                    runner.apply_scan(&rest, reconnected, 75).unwrap();
                } else {
                    runner.apply_scan(&rest, first, 62).unwrap();
                    runner
                        .apply_private_cash("fills", &ws, reconnected, 75)
                        .unwrap();
                }
                let order_cut = PrivateCut {
                    through: 10,
                    ..reconnected
                };
                let mut private_order = if okx {
                    json!({"instType":"SPOT","tdMode":"cash","instId":symbol,"clOrdId":"client1","ordId":"555","side":"buy","state":"partially_filled","ordType":"limit","px":"60000","sz":"0.001","accFillSz":"0.0004","cTime":"1001","uTime":"1002"})
                } else {
                    json!({"category":"spot","isLeverage":"0","symbol":symbol,"orderLinkId":"client1","orderId":"555","side":"Buy","orderStatus":"PartiallyFilled","orderType":"Limit","timeInForce":"GTC","price":"60000","qty":"0.001","cumExecQty":"0.0004","createdTime":"1001","updatedTime":"1002"})
                };
                if change == "same_order_cursor" {
                    runner
                        .apply_private_cash("orders", &private_order, order_cut, 81)
                        .unwrap();
                }
                let assets =
                    [fixture::key(1), fixture::key(2)].map(|k| runner.account().asset(k).unwrap());
                assert_eq!(assets[0].total, Fixed::parse("1.000399", 8).unwrap());
                assert_eq!(assets[1].total, Fixed::parse("976", 8).unwrap());
                let seq = runner.account().ledger_sequence();
                assert!(!runner.account().frozen());
                let f = &mut rest.fills[0];
                match change {
                    "price" => {
                        f.price = d("59999");
                        ws[if okx { "fillPx" } else { "execPrice" }] = "59999".into();
                        if !okx {
                            ws["execValue"] = "23.9996".into();
                        }
                    }
                    "quantity" => {
                        f.qty = d("0.0003");
                        ws[if okx { "fillSz" } else { "execQty" }] = "0.0003".into();
                        if !okx {
                            ws["execValue"] = "18".into();
                        }
                    }
                    "rebate" => {
                        f.fee = d("0.000001");
                        ws[if okx { "fillFee" } else { "execFee" }] =
                            if okx { "0.000001" } else { "-0.000001" }.into();
                    }
                    "fee_asset" => {
                        f.fee_currency = "USDT".into();
                        ws[if okx { "fillFeeCcy" } else { "feeCurrency" }] = "USDT".into();
                    }
                    _ => {
                        f.fee = d("-0.000002");
                        ws[if okx { "fillFee" } else { "execFee" }] =
                            if okx { "-0.000002" } else { "0.000002" }.into();
                    }
                }
                f.wire_row = serde_json::to_vec(&ws).unwrap();
                let bad = if change == "same_order_cursor" {
                    private_order[if okx { "ordType" } else { "timeInForce" }] =
                        if okx { "post_only" } else { "PostOnly" }.into();
                    runner
                        .apply_private_cash("orders", &private_order, order_cut, 81)
                        .map(|_| ())
                } else if change == "same_ws_cursor" {
                    let (cut, time) = if websocket_first {
                        (first, 62)
                    } else {
                        (reconnected, 75)
                    };
                    runner
                        .apply_private_cash("fills", &ws, cut, time)
                        .map(|_| ())
                } else if websocket_first {
                    runner.apply_scan(&rest, reconnected, 80)
                } else {
                    runner
                        .apply_private_cash(
                            "fills",
                            &ws,
                            PrivateCut {
                                through: 9,
                                ..reconnected
                            },
                            80,
                        )
                        .map(|_| ())
                };
                assert!(
                    bad.is_err(),
                    "accepted conflicting {change} on {venue:?}, WS first {websocket_first}"
                );
                assert!(runner.account().frozen());
                assert_eq!(seq, runner.account().ledger_sequence());
                assert_eq!(
                    assets,
                    [fixture::key(1), fixture::key(2)].map(|k| runner.account().asset(k).unwrap())
                );
                if change == "fee" && !websocket_first {
                    // The pre-v4 normalizer wrote Unknown at this source position. Replay must
                    // regenerate its bytes, even though new trade-only source positions use v4.
                    let mut old = runner
                        .session
                        .gateway()
                        .ingress_events()
                        .map(|(_, _, e)| e.event())
                        .find(|e| matches!(e.body, v::VenueEventBody::Fill(_)))
                        .unwrap()
                        .clone();
                    let stream = [88; 16];
                    let epoch = old.identity.source_connection_epoch;
                    let received = old.identity.received_mono_ns;
                    old.identity.event_id =
                        v::BoundedText::new(format!("{:032x}-1", u128::from_be_bytes(stream)))
                            .unwrap();
                    let v::VenueEventBody::Fill(fill) = &mut old.body else {
                        unreachable!()
                    };
                    fill.observation.status = v::OrderStatus::Unknown;
                    runner
                        .session
                        .gateway_mut()
                        .record_ingress_event(stream, 1, v::CanonicalEvent::new(old).unwrap())
                        .unwrap();
                    assert_eq!(
                        runner
                            .record_fill("client1", stream, 1, epoch, received, &original_wire)
                            .unwrap(),
                        Applied::Duplicate
                    );
                }
                drop(runner);
                assert!(wire.join().unwrap().is_empty());
            }
        }
    }
}
