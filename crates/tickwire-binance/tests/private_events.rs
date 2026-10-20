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
const SPOT: &[u8] = br#"{"subscriptionId":9,"event":{"e":"executionReport","E":10,"s":"BTCUSDT","c":"test-1","S":"BUY","o":"LIMIT","f":"GTC","q":"0.001","p":"60000","g":-1,"C":"","x":"TRADE","X":"FILLED","i":9007199254740993,"l":"0.001","z":"0.001","L":"59999.99","n":"0.00000001","N":"BNB","T":9,"t":9007199254740995,"I":32,"m":true,"Z":"59.99999","Y":"59.99999","r":"NONE"}}"#;
const USDM: &[u8] = br#"{"e":"ORDER_TRADE_UPDATE","E":10,"T":9,"o":{"s":"BTCUSDT","c":"test-1","S":"BUY","o":"LIMIT","f":"GTC","q":"0.001","p":"60000","x":"TRADE","X":"FILLED","i":9007199254740993,"l":"0.001","z":"0.001","L":"59999.99","n":"0.01","N":"USDT","T":9,"t":9007199254740995,"m":false,"ps":"BOTH","rp":"-2.00"}}"#;
#[test]
fn spot_trade_preserves_exact_ids_cash_and_subscription() {
    let e = decode_private::<8>(&session(Market::Spot), SPOT, 4096, 100).unwrap();
    let PrivateBody::Order(o) = e.body() else {
        panic!("order")
    };
    assert_eq!(o.order_id, 9007199254740993);
    assert_eq!(o.trade_id, Some(9007199254740995));
    assert_eq!(o.last_quote.unwrap().mantissa(), 5999999);
    assert_eq!(e.transaction_time_ns(), Some(9_000_000));
    assert_eq!(o.list_id, None);
}
#[test]
fn usdm_trade_keeps_pnl_separate_from_commission() {
    let e = decode_private::<8>(&session(Market::Usdm), USDM, 4096, 100).unwrap();
    let PrivateBody::Order(o) = e.body() else {
        panic!("order")
    };
    assert_eq!(o.realized_pnl.unwrap().mantissa(), -200);
    assert_eq!(o.commission.unwrap().mantissa(), 1);
    assert!(o.last_quote.is_none());
}

fn identity() -> v::OrderIdentity {
    v::OrderIdentity {
        order_id: [8; 16],
        original_intent_id: 3,
        original_intent_exec_epoch: 4,
        client_id: Some(text("test-1")),
        venue_order_id: None,
        parent_order_id: None,
        list_id: None,
        venue_algo_id: None,
        venue_list_id: None,
        list_client_id: None,
        algo_client_id: None,
    }
}
#[test]
fn exact_trade_binds_only_to_original_command_and_asset_catalog() {
    let command = v::CanonicalCommand::new(command()).unwrap();
    let e = decode_private::<8>(&session(Market::Spot), SPOT, 4096, 100).unwrap();
    let result = bind_order(&e, &command, &identity(), &[asset(3, "BNB")]).unwrap();
    let v::VenueEventBody::Fill(f) = &result.event().body else {
        panic!("fill")
    };
    assert_eq!(f.fees.as_slice()[0].amount, d(-1, -8));
    assert_eq!(f.last_quote, Some(d(5999999, -5)));
    assert_eq!(
        f.observation
            .order
            .venue_order_id
            .as_ref()
            .unwrap()
            .as_str(),
        "9007199254740993"
    );
    assert_eq!(result.event().identity.source_sequence, None);
    assert_eq!(result.event().identity.source_connection_epoch, 7);
}

#[test]
fn binding_rejects_account_product_order_symbol_and_asset_conflicts() {
    let canonical = v::CanonicalCommand::new(command()).unwrap();
    let e = decode_private::<8>(&session(Market::Spot), SPOT, 4096, 100).unwrap();
    let mut wrong = identity();
    wrong.original_intent_id = 99;
    assert!(bind_order(&e, &canonical, &wrong, &[asset(3, "BNB")]).is_err());
    wrong = identity();
    wrong.venue_order_id = Some(text("20"));
    assert!(bind_order(&e, &canonical, &wrong, &[asset(3, "BNB")]).is_err());
    assert!(bind_order(&e, &canonical, &identity(), &[]).is_err());
    assert!(bind_order(&e, &canonical, &identity(), &[asset(2, "BNB")]).is_err());
    for mutation in 0..4 {
        let mut c = command();
        match mutation {
            0 => c.context.request.account = 99,
            1 => c.context.request.environment = v::Environment::Production,
            2 => c.context.instrument.symbol = v::Symbol::new("ETHUSDT").unwrap(),
            _ => c.context.request.risk_domain_id = [9; 16],
        }
        assert!(
            bind_order(
                &e,
                &v::CanonicalCommand::new(c).unwrap(),
                &identity(),
                &[asset(3, "BNB")]
            )
            .is_err()
        );
    }
}
#[test]
fn wrapper_epoch_time_unit_and_product_are_explicit() {
    let raw = std::str::from_utf8(SPOT).unwrap();
    assert!(
        decode_private::<8>(
            &session(Market::Spot),
            raw.replace("\"subscriptionId\":9", "\"subscriptionId\":8")
                .as_bytes(),
            4096,
            1
        )
        .is_err()
    );
    assert!(decode_private::<8>(&session(Market::Usdm), SPOT, 4096, 1).is_err());
    assert!(decode_private::<8>(&session(Market::Spot), USDM, 4096, 1).is_err());
    let s = SessionContext::new(
        session(Market::Spot).identity().clone(),
        Market::Spot,
        PrivateStream::SpotSubscription { subscription_id: 9 },
        TimeUnit::Microseconds,
    )
    .unwrap();
    assert_eq!(
        decode_private::<8>(&s, SPOT, 4096, 1)
            .unwrap()
            .event_time_ns(),
        10_000
    );
    assert!(
        SessionContext::new(
            s.identity().clone(),
            Market::Usdm,
            PrivateStream::UsdmListenKey,
            TimeUnit::Microseconds
        )
        .is_err()
    );
}
#[test]
fn malformed_decimal_duplicate_field_missing_fee_and_overflow_fail() {
    let raw = std::str::from_utf8(SPOT).unwrap();
    for changed in [
        raw.replace("\"q\":\"0.001\"", "\"q\":0.001"),
        raw.replace("\"q\":\"0.001\"", "\"q\":\"1e-3\""),
        raw.replace("\"q\":\"0.001\"", "\"q\":\"-0.001\""),
        raw.replace("\"i\":9007199254740993", "\"i\":1,\"i\":2"),
        raw.replace("\"E\":10", "\"E\":18446744073709551615"),
        raw.replace("\"N\":\"BNB\"", "\"N\":null"),
        raw.replace("\"t\":9007199254740995", "\"t\":-2"),
    ] {
        assert!(
            decode_private::<8>(&session(Market::Spot), changed.as_bytes(), 4096, 1).is_err(),
            "{changed}"
        );
    }
    let missing = raw.replace(",\"n\":\"0.00000001\"", "");
    let e = decode_private::<8>(&session(Market::Spot), missing.as_bytes(), 4096, 1).unwrap();
    assert!(
        bind_order(
            &e,
            &v::CanonicalCommand::new(command()).unwrap(),
            &identity(),
            &[asset(3, "BNB")]
        )
        .is_err()
    );
    assert!(decode_private::<8>(&session(Market::Spot), SPOT, 10, 1).is_err());
}
#[test]
fn unknown_order_status_is_not_normalized_to_new_or_terminal() {
    let changed = std::str::from_utf8(SPOT)
        .unwrap()
        .replace("\"X\":\"FILLED\"", "\"X\":\"FUTURE_STATUS\"");
    let e = decode_private::<8>(&session(Market::Spot), changed.as_bytes(), 4096, 1).unwrap();
    let bound = bind_order(
        &e,
        &v::CanonicalCommand::new(command()).unwrap(),
        &identity(),
        &[asset(3, "BNB")],
    )
    .unwrap();
    let v::VenueEventBody::Fill(f) = &bound.event().body else {
        panic!("fill")
    };
    assert_eq!(f.observation.status, v::OrderStatus::Unknown);
}
#[test]
fn usdm_binding_preserves_reported_pnl_without_inventing_quote_cash() {
    let mut c = command();
    c.context.request.product = v::Product::LinearPerpetual;
    c.context.instrument.contract = v::ContractKind::Perpetual;
    let v::CommandBody::New(o) = &mut c.body else {
        unreachable!()
    };
    o.mode = v::ProductMode::Derivative {
        position_mode: v::PositionMode::OneWay,
        margin_mode: v::MarginMode::Cross,
    };
    let e = decode_private::<8>(&session(Market::Usdm), USDM, 4096, 1).unwrap();
    let bound = bind_order(&e, &v::CanonicalCommand::new(c).unwrap(), &identity(), &[]).unwrap();
    let v::VenueEventBody::Fill(f) = &bound.event().body else {
        panic!("fill")
    };
    assert_eq!(f.realized_pnl.as_ref().unwrap().amount, d(-2, 0));
    assert_eq!(
        f.realized_pnl.as_ref().unwrap().asset.symbol.as_str(),
        "USDT"
    );
    assert_eq!(f.fees.as_slice()[0].amount, d(-1, -2));
    assert!(f.last_quote.is_none());
    assert!(f.observation.cumulative_quote.is_none());
}
#[test]
fn spot_balance_and_external_lock_are_sparse_distinct_updates() {
    let bytes=br#"{"subscriptionId":9,"event":{"e":"outboundAccountPosition","E":10,"u":9,"B":[{"a":"USDT","f":"2.0","l":"1.0"}]}}"#;
    let e = decode_private::<1>(&session(Market::Spot), bytes, 4096, 1).unwrap();
    let PrivateBody::SpotBalances {
        last_update_ns,
        balances,
    } = e.body()
    else {
        panic!("balance")
    };
    assert_eq!(*last_update_ns, 9_000_000);
    assert_eq!(balances.len(), 1);
    for kind in ["balanceUpdate", "externalLockUpdate"] {
        let text = format!(
            r#"{{"subscriptionId":9,"event":{{"e":"{kind}","E":10,"T":9,"a":"USDT","d":"-2.5"}}}}"#
        );
        let e = decode_private::<1>(&session(Market::Spot), text.as_bytes(), 4096, 1).unwrap();
        let PrivateBody::BalanceDelta {
            delta,
            external_lock,
            ..
        } = e.body()
        else {
            panic!("delta")
        };
        assert_eq!(delta.mantissa(), -25);
        assert_eq!(*external_lock, kind == "externalLockUpdate");
    }
}
#[test]
fn usdm_sparse_funding_account_and_config_keep_original_units() {
    let bytes=br#"{"e":"ACCOUNT_UPDATE","E":10,"T":9,"a":{"m":"FUNDING_FEE","S":"BTCUSDT","B":[{"a":"USDT","wb":"100","cw":"90","bc":"-0.01"}],"P":[]}}"#;
    let e = decode_private::<1>(&session(Market::Usdm), bytes, 4096, 1).unwrap();
    let PrivateBody::UsdmAccount {
        balances,
        positions,
        funding_symbol,
        ..
    } = e.body()
    else {
        panic!("account")
    };
    assert!(positions.is_empty());
    assert_eq!(funding_symbol.as_deref(), Some("BTCUSDT"));
    assert_eq!(balances.len(), 1);
    let position=br#"{"e":"ACCOUNT_UPDATE","E":10,"T":9,"a":{"m":"ORDER","B":[],"P":[{"s":"BTCUSDT","pa":"-2","ep":"60000","bep":"60001","cr":"-1","up":"3","mt":"isolated","iw":"20","ps":"BOTH"}]}}"#;
    let e = decode_private::<1>(&session(Market::Usdm), position, 4096, 1).unwrap();
    let PrivateBody::UsdmAccount { positions, .. } = e.body() else {
        panic!("account")
    };
    assert_eq!(positions[0].position_amount.mantissa(), -2);
    let config=br#"{"e":"ACCOUNT_CONFIG_UPDATE","E":10,"T":9,"ac":{"s":"BTCUSDT","l":20},"ai":{"j":true}}"#;
    let e = decode_private::<1>(&session(Market::Usdm), config, 4096, 1).unwrap();
    let PrivateBody::AccountConfig {
        leverage,
        multi_assets,
    } = e.body()
    else {
        panic!("config")
    };
    assert_eq!(leverage.as_ref().unwrap().leverage, 20);
    assert_eq!(*multi_assets, Some(true));
}
#[test]
fn sparse_arrays_reject_capacity_and_duplicate_keys_atomically() {
    let bytes=br#"{"subscriptionId":9,"event":{"e":"outboundAccountPosition","E":10,"u":9,"B":[{"a":"USDT","f":"2","l":"1"},{"a":"BTC","f":"2","l":"1"}]}}"#;
    assert!(decode_private::<1>(&session(Market::Spot), bytes, 4096, 1).is_err());
    assert!(
        decode_private::<2>(
            &session(Market::Spot),
            std::str::from_utf8(bytes)
                .unwrap()
                .replace("BTC", "USDT")
                .as_bytes(),
            4096,
            1
        )
        .is_err()
    );
}
#[test]
fn list_and_algo_statuses_do_not_fabricate_child_terminal_facts() {
    let list=br#"{"subscriptionId":9,"event":{"e":"listStatus","E":10,"T":9,"s":"BTCUSDT","g":55,"c":"OCO","l":"ALL_DONE","L":"ALL_DONE","r":"NONE","C":"list-1","O":[{"s":"BTCUSDT","i":1,"c":"above"},{"s":"BTCUSDT","i":2,"c":"below"}]}}"#;
    let e = decode_private::<2>(&session(Market::Spot), list, 4096, 1).unwrap();
    let PrivateBody::List(l) = e.body() else {
        panic!("list")
    };
    assert_eq!(l.list_id, 55);
    assert_eq!(l.orders.len(), 2);
    assert!(
        bind_order(
            &e,
            &v::CanonicalCommand::new(command()).unwrap(),
            &identity(),
            &[]
        )
        .is_err()
    );
    let algo=br#"{"e":"ALGO_UPDATE","E":10,"T":9,"o":{"s":"BTCUSDT","caid":"algo-1","aid":55,"at":"CONDITIONAL","o":"STOP_MARKET","S":"SELL","ps":"BOTH","f":"GTC","q":"1","X":"FINISHED","ai":"9007199254740993","ap":"60000","aq":"1","act":"MARKET","tp":"61000","p":"0","tt":9,"gtd":0,"rm":"","ia":false}}"#;
    let e = decode_private::<2>(&session(Market::Usdm), algo, 4096, 1).unwrap();
    let PrivateBody::Algo(a) = e.body() else {
        panic!("algo")
    };
    assert_eq!(a.actual_order_id.as_deref(), Some("9007199254740993"));
    assert_eq!(a.executed_quantity.unwrap().mantissa(), 1);
    assert!(
        bind_order(
            &e,
            &v::CanonicalCommand::new(command()).unwrap(),
            &identity(),
            &[]
        )
        .is_err()
    );
}
#[test]
fn unknown_event_and_termination_remain_explicit() {
    let unknown = br#"{"e":"NEW_EVENT","E":10,"value":9007199254740993}"#;
    let e = decode_private::<1>(&session(Market::Usdm), unknown, 4096, 1).unwrap();
    assert!(matches!(e.body(), PrivateBody::Other { .. }));
    assert_eq!(e.raw().get().as_bytes(), unknown);
    let expired = br#"{"e":"listenKeyExpired","E":10,"listenKey":"redacted"}"#;
    assert!(matches!(
        decode_private::<1>(&session(Market::Usdm), expired, 4096, 1)
            .unwrap()
            .body(),
        PrivateBody::ListenKeyExpired
    ));
    let end = br#"{"subscriptionId":9,"event":{"e":"eventStreamTerminated","E":10}}"#;
    assert!(matches!(
        decode_private::<1>(&session(Market::Spot), end, 4096, 1)
            .unwrap()
            .body(),
        PrivateBody::StreamTerminated
    ));
}

#[test]
fn canceled_order_matches_original_client_and_does_not_create_a_fill() {
    let value = std::str::from_utf8(SPOT)
        .unwrap()
        .replace("\"c\":\"test-1\"", "\"c\":\"cancel-1\"")
        .replace("\"C\":\"\"", "\"C\":\"test-1\"")
        .replace("\"x\":\"TRADE\"", "\"x\":\"CANCELED\"")
        .replace("\"X\":\"FILLED\"", "\"X\":\"CANCELED\"")
        .replace("\"l\":\"0.001\"", "\"l\":\"0\"")
        .replace("\"n\":\"0.00000001\"", "\"n\":\"0\"")
        .replace("\"N\":\"BNB\"", "\"N\":null")
        .replace("\"t\":9007199254740995", "\"t\":-1");
    let e = decode_private::<1>(&session(Market::Spot), value.as_bytes(), 4096, 1).unwrap();
    let c = v::CanonicalCommand::new(command()).unwrap();
    let bound = bind_order(&e, &c, &identity(), &[]).unwrap();
    let v::VenueEventBody::Order(o) = &bound.event().body else {
        panic!("order")
    };
    assert_eq!(o.status, v::OrderStatus::Canceled);
    assert_eq!(o.cumulative_quantity, Some(d(1, -3)));
    assert!(o.leaves_quantity.is_none());
    let unknown = value.replace("\"x\":\"CANCELED\"", "\"x\":\"FUTURE_EXECUTION\"");
    let e = decode_private::<1>(&session(Market::Spot), unknown.as_bytes(), 4096, 1).unwrap();
    let bound = bind_order(&e, &c, &identity(), &[]).unwrap();
    let v::VenueEventBody::Order(o) = &bound.event().body else {
        panic!("order")
    };
    assert_eq!(o.status, v::OrderStatus::Unknown);
}
#[test]
fn rebates_preserve_sign_and_finer_than_asset_units_is_rejected() {
    let c = v::CanonicalCommand::new(command()).unwrap();
    let rebate = std::str::from_utf8(SPOT)
        .unwrap()
        .replace("\"n\":\"0.00000001\"", "\"n\":\"-0.00000001\"");
    let e = decode_private::<1>(&session(Market::Spot), rebate.as_bytes(), 4096, 1).unwrap();
    let bound = bind_order(&e, &c, &identity(), &[asset(3, "BNB")]).unwrap();
    let v::VenueEventBody::Fill(f) = &bound.event().body else {
        panic!("fill")
    };
    assert_eq!(f.fees.as_slice()[0].amount, d(1, -8));
    let e = decode_private::<1>(&session(Market::Spot), SPOT, 4096, 1).unwrap();
    let mut fee = asset(3, "BNB");
    fee.exponent = -6;
    assert!(bind_order(&e, &c, &identity(), &[fee]).is_err());
}
#[test]
fn usdm_cm_marker_cannot_be_bound_as_linear_order() {
    let value = std::str::from_utf8(USDM)
        .unwrap()
        .replace("\"ps\":\"BOTH\"", "\"ps\":\"BOTH\",\"st\":2");
    assert!(decode_private::<1>(&session(Market::Usdm), value.as_bytes(), 4096, 1).is_err());
}

#[test]
fn sparse_usdm_update_may_omit_unchanged_balance_or_position_collection() {
    let funding=br#"{"e":"ACCOUNT_UPDATE","E":10,"T":9,"a":{"m":"FUNDING_FEE","S":"BTCUSDT","B":[{"a":"USDT","wb":"100","cw":"90","bc":"-0.01"}]}}"#;
    let e = decode_private::<1>(&session(Market::Usdm), funding, 4096, 1).unwrap();
    let PrivateBody::UsdmAccount {
        balances,
        positions,
        ..
    } = e.body()
    else {
        panic!("account")
    };
    assert_eq!(balances.len(), 1);
    assert!(positions.is_empty());
    let only_position=br#"{"e":"ACCOUNT_UPDATE","E":10,"T":9,"a":{"m":"MARGIN_TYPE_CHANGE","P":[{"s":"BTCUSDT","pa":"-2","ep":"60000","bep":"60001","cr":"-1","up":"3","mt":"isolated","iw":"20","ps":"BOTH"}]}}"#;
    let e = decode_private::<1>(&session(Market::Usdm), only_position, 4096, 1).unwrap();
    let PrivateBody::UsdmAccount {
        balances,
        positions,
        ..
    } = e.body()
    else {
        panic!("account")
    };
    assert!(balances.is_empty());
    assert_eq!(positions.len(), 1);
}

#[test]
fn absent_usdm_pnl_is_retained_and_only_blocks_trade_binding() {
    let raw = std::str::from_utf8(USDM)
        .unwrap()
        .replace(",\"rp\":\"-2.00\"", "");
    let c = {
        let mut c = command();
        c.context.request.product = v::Product::LinearPerpetual;
        c.context.instrument.contract = v::ContractKind::Perpetual;
        let v::CommandBody::New(o) = &mut c.body else {
            unreachable!()
        };
        o.mode = v::ProductMode::Derivative {
            position_mode: v::PositionMode::OneWay,
            margin_mode: v::MarginMode::Cross,
        };
        v::CanonicalCommand::new(c).unwrap()
    };
    let trade = decode_private::<1>(&session(Market::Usdm), raw.as_bytes(), 4096, 1).unwrap();
    let PrivateBody::Order(o) = trade.body() else {
        panic!("order")
    };
    assert!(o.realized_pnl.is_none());
    assert!(bind_order(&trade, &c, &identity(), &[]).is_err());
    let new = raw
        .replace("\"x\":\"TRADE\"", "\"x\":\"NEW\"")
        .replace("\"X\":\"FILLED\"", "\"X\":\"NEW\"")
        .replace("\"l\":\"0.001\"", "\"l\":\"0\"")
        .replace("\"z\":\"0.001\"", "\"z\":\"0\"")
        .replace(",\"n\":\"0.01\"", "");
    let message = decode_private::<1>(&session(Market::Usdm), new.as_bytes(), 4096, 1).unwrap();
    let bound = bind_order(&message, &c, &identity(), &[]).unwrap();
    assert!(matches!(bound.event().body, v::VenueEventBody::Order(_)));
}
