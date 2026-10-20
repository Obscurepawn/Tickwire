mod common;
use common::*;
use tickwire_binance::{Error, auth::SigningKey, request::*};
use tickwire_venue::*;

fn options(transport: Transport) -> RequestOptions<'static> {
    RequestOptions {
        transport,
        timestamp: Timestamp::Milliseconds(1668481559918),
        recv_window_micros: 5_000_000,
        request_id: "req-7",
    }
}
fn credentials(key: &SigningKey, market: Market) -> Credentials<'_> {
    Credentials {
        account: 2,
        environment: Environment::Test,
        market,
        api_key: "public-test-api-key",
        signing_key: key,
    }
}
fn prepared(command: Command, market: Market, transport: Transport) -> PreparedRequest {
    let key = SigningKey::hmac(b"test-only").unwrap();
    prepare(
        &CanonicalCommand::new(command).unwrap(),
        &credentials(&key, market),
        options(transport),
        16_384,
    )
    .unwrap()
}
fn target() -> OrderTarget {
    OrderTarget {
        order_id: [1; 16],
        expected_owner_revision: 1,
        original_intent_exec_epoch: 4,
        client_id: Some(text("original-1")),
        venue_order_id: Some(text("123456789")),
    }
}
fn usdm() -> Command {
    let mut c = command();
    c.context.request.product = Product::LinearPerpetual;
    c.context.instrument.contract = ContractKind::Perpetual;
    if let CommandBody::New(ref mut o) = c.body {
        o.mode = ProductMode::Derivative {
            position_mode: PositionMode::OneWay,
            margin_mode: MarginMode::Cross,
        };
    }
    c
}
fn json(request: &PreparedRequest) -> serde_json::Value {
    serde_json::from_slice(request.payload()).unwrap()
}

#[test]
fn spot_new_ws_contains_complete_typed_order_and_original_digest() {
    let canonical = CanonicalCommand::new(command()).unwrap();
    let key = SigningKey::hmac(b"test-only").unwrap();
    let p = prepare(
        &canonical,
        &credentials(&key, Market::Spot),
        options(Transport::WebSocket),
        16_384,
    )
    .unwrap();
    assert_eq!(p.command_digest(), canonical.digest());
    assert_eq!(p.verify_digest([0; 32]), Err(Error::IdentityMismatch));
    assert_eq!(p.authority(), "ws-api.testnet.binance.vision");
    assert_eq!(p.path(), "/ws-api/v3");
    let v = json(&p);
    assert_eq!(v["method"], "order.place");
    assert_eq!(v["id"], "req-7");
    assert_eq!(v["params"]["symbol"], "BTCUSDT");
    assert_eq!(v["params"]["side"], "BUY");
    assert_eq!(v["params"]["price"], "60000");
    assert_eq!(v["params"]["quantity"], "0.001");
    assert_eq!(v["params"]["newClientOrderId"], "test-1");
    assert_eq!(v["params"]["timestamp"], 1668481559918u64);
    assert!(v["params"]["signature"].as_str().unwrap().len() == 64);
}
#[test]
fn rest_request_uses_product_endpoint_and_encoded_query_exactly() {
    let p = prepared(command(), Market::Spot, Transport::Rest);
    assert_eq!(p.authority(), "testnet.binance.vision");
    assert_eq!(p.method(), Method::Post);
    assert!(p.path().starts_with("/api/v3/order?symbol=BTCUSDT&"));
    assert!(p.path().contains("&signature="));
    assert_eq!(p.api_key_header(), Some("public-test-api-key"));
    assert!(p.payload().is_empty());
}
#[test]
fn usdm_limit_hedge_mode_sends_position_side_without_reduce_only() {
    let mut c = usdm();
    if let CommandBody::New(ref mut o) = c.body {
        o.mode = ProductMode::Derivative {
            position_mode: PositionMode::Hedge,
            margin_mode: MarginMode::Isolated,
        };
        o.position_side = PositionSide::Short;
        o.side = Side::Sell;
        o.time_in_force = Some(TimeInForce::PostOnly);
    }
    let p = prepared(c, Market::Usdm, Transport::WebSocket);
    assert_eq!(p.authority(), "testnet.binancefuture.com");
    let v = json(&p);
    assert_eq!(v["params"]["positionSide"], "SHORT");
    assert_eq!(v["params"]["timeInForce"], "GTX");
    assert!(v["params"].get("reduceOnly").is_none());
}
#[test]
fn post_only_spot_is_limit_maker_without_tif() {
    let mut c = command();
    if let CommandBody::New(ref mut o) = c.body {
        o.time_in_force = Some(TimeInForce::PostOnly);
    }
    let v = json(&prepared(c, Market::Spot, Transport::WebSocket));
    assert_eq!(v["params"]["type"], "LIMIT_MAKER");
    assert!(v["params"].get("timeInForce").is_none());
}
#[test]
fn cancel_and_query_preserve_original_target_identity() {
    for (body, ws_method, method) in [
        (
            CommandBody::Cancel(target()),
            "order.cancel",
            Method::Delete,
        ),
        (CommandBody::Query(target()), "order.status", Method::Get),
    ] {
        let mut c = command();
        c.body = body;
        let v = json(&prepared(c.clone(), Market::Spot, Transport::WebSocket));
        assert_eq!(v["method"], ws_method);
        assert_eq!(v["params"]["orderId"], 123456789u64);
        assert_eq!(v["params"]["origClientOrderId"], "original-1");
        assert_eq!(prepared(c, Market::Spot, Transport::Rest).method(), method);
    }
}
#[test]
fn spot_cancel_replace_and_priority_amend_are_distinct() {
    let mut c = command();
    let CommandBody::New(order) = c.body.clone() else {
        unreachable!()
    };
    c.body = CommandBody::CancelReplace {
        target: target(),
        replacement: order,
        mode: CancelReplaceMode::AllowFailure,
    };
    let v = json(&prepared(c.clone(), Market::Spot, Transport::WebSocket));
    assert_eq!(v["method"], "order.cancelReplace");
    assert_eq!(v["params"]["cancelReplaceMode"], "ALLOW_FAILURE");
    assert_eq!(v["params"]["cancelOrderId"], 123456789u64);
    c.body = CommandBody::AmendKeepPriority {
        target: target(),
        new_quantity: d(1, -4),
        new_client_id: Some(text("amended-1")),
    };
    let p = prepared(c, Market::Spot, Transport::Rest);
    assert_eq!(p.method(), Method::Put);
    assert!(p.path().starts_with("/api/v3/order/amend/keepPriority?"));
    assert!(p.path().contains("newQty=0.0001"));
}
#[test]
fn usdm_modify_keeps_side_price_quantity_and_target() {
    let mut c = usdm();
    c.body = CommandBody::Modify(ModifyOrder {
        target: target(),
        new_client_id: None,
        side: Side::Buy,
        quantity: d(2, -3),
        price: d(59000, 0),
        position_side: PositionSide::Both,
        reduce_only: false,
    });
    let v = json(&prepared(c, Market::Usdm, Transport::WebSocket));
    assert_eq!(v["method"], "order.modify");
    assert_eq!(v["params"]["price"], "59000");
    assert_eq!(v["params"]["quantity"], "0.002");
    assert_eq!(v["params"]["side"], "BUY");
}
#[test]
fn usdm_algo_close_position_uses_distinct_method_and_exclusive_fields() {
    let mut c = usdm();
    let CommandBody::New(mut order) = c.body else {
        unreachable!()
    };
    order.order_type = OrderType::StopMarket;
    order.quantity = Quantity::ClosePosition;
    order.price = None;
    order.time_in_force = None;
    order.trigger = Some(Trigger {
        price: d(58000, 0),
        working_price: WorkingPrice::Mark,
        price_protect: true,
    });
    c.body = CommandBody::Algo(AlgoOrder {
        parent_order_id: [9; 16],
        order,
    });
    let v = json(&prepared(c, Market::Usdm, Transport::WebSocket));
    assert_eq!(v["method"], "algoOrder.place");
    assert_eq!(v["params"]["clientAlgoId"], "test-1");
    assert_eq!(v["params"]["triggerPrice"], "58000");
    assert_eq!(v["params"]["closePosition"], "true");
    assert_eq!(v["params"]["priceProtect"], "true");
    assert!(v["params"].get("quantity").is_none());
    assert!(v["params"].get("reduceOnly").is_none());
}
#[test]
fn account_environment_and_market_mismatch_never_produce_request() {
    let canonical = CanonicalCommand::new(command()).unwrap();
    let key = SigningKey::hmac(b"test-only").unwrap();
    for field in 0..3 {
        let mut auth = credentials(&key, Market::Spot);
        match field {
            0 => auth.account = 999,
            1 => auth.environment = Environment::Production,
            _ => auth.market = Market::Usdm,
        }
        assert!(matches!(
            prepare(&canonical, &auth, options(Transport::Rest), 16384),
            Err(Error::IdentityMismatch)
        ));
    }
}
#[test]
fn capacity_and_timing_failure_are_explicit() {
    let canonical = CanonicalCommand::new(command()).unwrap();
    let key = SigningKey::hmac(b"test-only").unwrap();
    assert!(matches!(
        prepare(
            &canonical,
            &credentials(&key, Market::Spot),
            options(Transport::Rest),
            8
        ),
        Err(Error::Capacity)
    ));
    let mut o = options(Transport::Rest);
    o.recv_window_micros = 60_000_001;
    assert!(matches!(
        prepare(&canonical, &credentials(&key, Market::Spot), o, 16384),
        Err(Error::InvalidParameter)
    ));
    let u = CanonicalCommand::new(usdm()).unwrap();
    o = options(Transport::WebSocket);
    o.timestamp = Timestamp::Microseconds(1668481559918000);
    assert!(matches!(
        prepare(&u, &credentials(&key, Market::Usdm), o, 16384),
        Err(Error::InvalidParameter)
    ));
}
#[test]
fn spot_fractional_recv_window_is_a_json_number_with_exact_milliseconds() {
    let canonical = CanonicalCommand::new(command()).unwrap();
    let key = SigningKey::hmac(b"test-only").unwrap();
    let mut o = options(Transport::WebSocket);
    o.recv_window_micros = 6_000_346;
    let p = prepare(&canonical, &credentials(&key, Market::Spot), o, 16384).unwrap();
    let v = json(&p);
    assert!(v["params"]["recvWindow"].is_number());
    assert_eq!(v["params"]["recvWindow"].to_string(), "6000.346");
}

#[test]
fn usdm_gtd_is_milliseconds_with_seconds_precision() {
    let mut c = usdm();
    if let CommandBody::New(ref mut o) = c.body {
        o.time_in_force = Some(TimeInForce::Gtd {
            expire_utc_ns: 1_668_482_200_999_999_999,
        });
    }
    let v = json(&prepared(c, Market::Usdm, Transport::WebSocket));
    assert_eq!(v["params"]["goodTillDate"], 1_668_482_200_000u64);
}
#[test]
fn usdm_trailing_algo_uses_activate_price_and_callback_percent() {
    let mut c = usdm();
    let CommandBody::New(mut order) = c.body else {
        unreachable!()
    };
    order.order_type = OrderType::TrailingStop;
    order.price = None;
    order.time_in_force = None;
    order.trailing = Some(Trailing {
        activation_price: Some(d(58000, 0)),
        working_price: WorkingPrice::Contract,
        distance: TrailingDistance::Percent(d(5, -1)),
    });
    c.body = CommandBody::Algo(AlgoOrder {
        parent_order_id: [9; 16],
        order,
    });
    let v = json(&prepared(c, Market::Usdm, Transport::WebSocket));
    assert_eq!(v["params"]["activatePrice"], "58000");
    assert!(v["params"].get("activationPrice").is_none());
    assert_eq!(v["params"]["callbackRate"], "0.5");
}

#[test]
fn spot_market_sell_quote_quantity_is_sent_as_exact_string() {
    let mut c = command();
    if let CommandBody::New(ref mut o) = c.body {
        o.side = Side::Sell;
        o.order_type = OrderType::Market;
        o.quantity = Quantity::Quote(d(75, -1));
        o.price = None;
        o.time_in_force = None;
    }
    let v = json(&prepared(c, Market::Spot, Transport::WebSocket));
    assert_eq!(v["params"]["side"], "SELL");
    assert_eq!(v["params"]["quoteOrderQty"], "7.5");
    assert!(v["params"].get("quantity").is_none());
}
