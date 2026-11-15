#![forbid(unsafe_code)]

use base64::{Engine as _, engine::general_purpose::STANDARD};
use ring::hmac;
use serde_json::Value;
use tickwire_account::OrsSymbolSpec;
use tickwire_spot_execution::{
    BufWriter, BybitCategory, BybitPositionIdx, BybitUtaParser, Credentials, Environment,
    OkxInstType, OkxMarginMode, OkxUtaParser, SpotIssueKind, SpotVenue, UtaCancelWireInput,
    UtaFrameEvent, UtaModifyWireInput, UtaNewOrderWireInput, UtaOrderParams, UtaReplyOp,
    check_bybit_auth_reply, check_okx_login_reply, classify_issue, encode_bybit_auth_into,
    encode_bybit_cancel_body_into, encode_bybit_modify_body_into, encode_bybit_new_order_body_into,
    encode_bybit_private_subscribe_step_into, encode_bybit_ws_trade_envelope_into,
    encode_okx_cancel_body_into, encode_okx_login_into, encode_okx_modify_body_into,
    encode_okx_new_order_body_into, encode_okx_subscribe_step_into,
    encode_okx_ws_trade_envelope_into, encode_signed_https_post_into, encode_ws_ping_into,
    instrument_rate_scope, okx_option_family_key,
    private::stream::{UtaPrivatePhase, UtaPrivateStream},
    scan_bybit_frame, scan_okx_frame, try_decode_ws_server_frame,
    ws_envelope::{WS_PING_PERIOD_NS, WsLoginOutcome},
};
use tickwire_transport::ws::Opcode;
use tickwire_venue::{
    Liquidity, OrderStatus, OrderType, PositionSide, Product, SelfTradePrevention, Side,
    TerminalReason, TimeInForce,
};

fn ws_server_frame(opcode: Opcode, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 10);
    out.push(0x80 | (opcode as u8));
    if payload.len() < 126 {
        out.push(payload.len() as u8);
    } else if payload.len() <= 65_535 {
        out.push(126);
        out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    } else {
        out.push(127);
        out.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    }
    out.extend_from_slice(payload);
    out
}

fn spec_for(product: Product) -> OrsSymbolSpec {
    OrsSymbolSpec {
        symbol_id: 0,
        instrument_id: 1,
        product,
        wallet_id: 1,
        base_asset_id: 1,
        base_scale: 8,
        quote_asset_id: 2,
        quote_scale: 6,
        settlement_asset_id: 2,
        settlement_scale: 6,
        price_tick_atoms: 10,
        price_scale: 2,
        qty_step_atoms: 1,
        qty_scale: 3,
        contract_size_atoms: 1,
        contract_scale: 0,
        initial_margin_bps: 1_000,
    }
}

#[test]
fn all_six_uta_products_and_rpi_encode_cleanly_for_bybit_and_okx() {
    let cases = [
        (Product::Spot, "BTCUSDT", "spot", "BTC-USDT", "SPOT"),
        (
            Product::LinearPerp,
            "BTCUSDT",
            "linear",
            "BTC-USDT-SWAP",
            "SWAP",
        ),
        (
            Product::InversePerp,
            "BTCUSD",
            "inverse",
            "BTC-USD-SWAP",
            "SWAP",
        ),
        (
            Product::LinearFutures,
            "BTCUSDT-26DEC26",
            "linear",
            "BTC-USDT-261225",
            "FUTURES",
        ),
        (
            Product::InverseFutures,
            "BTCUSDZ26",
            "inverse",
            "BTC-USD-261225",
            "FUTURES",
        ),
        (
            Product::Options,
            "BTC-25DEC26-90000-C",
            "option",
            "BTC-USD-261225-90000-C",
            "OPTION",
        ),
    ];

    let client_id = "tw10123456789abcdefghjklmnpqrstu";
    assert_eq!(client_id.len(), 32);

    for (product, bybit_sym, expected_cat, okx_sym, expected_inst_type) in cases {
        assert_eq!(
            BybitCategory::from_product(product).unwrap().as_str(),
            expected_cat
        );
        assert_eq!(
            OkxInstType::from_product(product).unwrap().as_str(),
            expected_inst_type
        );

        let spec = spec_for(product);
        let mut params = if product == Product::Spot {
            UtaOrderParams::cash_spot_limit(TimeInForce::Rpi)
        } else {
            let mut p = UtaOrderParams::cross_derivative_limit(TimeInForce::Rpi, true);
            p.position_side = PositionSide::Long;
            p.bybit_position_idx = BybitPositionIdx::BuyHedge;
            p.smp_mode = SelfTradePrevention::ExpireMaker;
            p
        };
        params.time_in_force = TimeInForce::Rpi;

        // 1. Bybit V5 NewOrder JSON + WS trade envelope + Signed HTTPS POST
        let bybit_in = UtaNewOrderWireInput {
            symbol: bybit_sym,
            spec: &spec,
            side: Side::Buy,
            price_ticks: 900_000, // 90000.00
            qty_lots: 1_250,      // 1.250
            client_id,
            params,
        };
        let mut buf = [0u8; 768];
        let mut w = BufWriter::new(&mut buf);
        encode_bybit_new_order_body_into(&bybit_in, &mut w).unwrap();
        let body_len = w.written();
        let body_bytes = &buf[..body_len];
        let body_json: Value = serde_json::from_slice(body_bytes).unwrap();
        assert_eq!(body_json["category"], expected_cat);
        assert_eq!(body_json["symbol"], bybit_sym);
        assert_eq!(body_json["side"], "Buy");
        assert_eq!(body_json["orderType"], "Limit");
        assert_eq!(body_json["timeInForce"], "RPI");
        assert_eq!(body_json["price"], "90000");
        assert_eq!(body_json["qty"], "1.25");
        assert_eq!(body_json["orderLinkId"], client_id);
        if matches!(expected_cat, "linear" | "inverse") {
            assert_eq!(body_json["positionIdx"], 1);
            assert_eq!(body_json["reduceOnly"], true);
            assert_eq!(body_json["smpType"], "CancelMaker");
        } else if expected_cat == "option" {
            assert_eq!(body_json["reduceOnly"], true);
        }

        let mut ws_env = [0u8; 1024];
        let ws_len = encode_bybit_ws_trade_envelope_into(
            42,
            1_700_000_000_123,
            "order.create",
            body_bytes,
            &mut ws_env,
        )
        .unwrap();
        let ws_val: Value = serde_json::from_slice(&ws_env[..ws_len]).unwrap();
        assert_eq!(ws_val["reqId"], "42");
        assert_eq!(ws_val["op"], "order.create");
        assert_eq!(ws_val["header"]["X-BAPI-TIMESTAMP"], "1700000000123");
        assert_eq!(ws_val["args"][0]["timeInForce"], "RPI");

        let bybit_creds = Credentials::bybit("BYBITKEY01", b"bybit-secret-key");
        let mut https_buf = [0u8; 2048];
        let https_len = encode_signed_https_post_into(
            &bybit_creds,
            Environment::Production,
            1_700_000_000_123,
            "/v5/order/create",
            body_bytes,
            &mut https_buf,
        )
        .unwrap();
        let https_str = core::str::from_utf8(&https_buf[..https_len]).unwrap();
        assert!(https_str.starts_with("POST /v5/order/create HTTP/1.1\r\n"));
        assert!(https_str.contains("X-BAPI-API-KEY: BYBITKEY01\r\n"));
        assert!(https_str.contains("X-BAPI-SIGN: "));

        // 2. OKX V5 NewOrder JSON + WS trade envelope + Signed HTTPS POST
        let mut okx_params = params;
        if product == Product::InverseFutures {
            okx_params.okx_td_mode = OkxMarginMode::Isolated;
        }
        let okx_in = UtaNewOrderWireInput {
            symbol: okx_sym,
            spec: &spec,
            side: Side::Sell,
            price_ticks: 900_000,
            qty_lots: 2_500,
            client_id,
            params: okx_params,
        };
        let mut okx_buf = [0u8; 768];
        let mut ow = BufWriter::new(&mut okx_buf);
        encode_okx_new_order_body_into(&okx_in, &mut ow).unwrap();
        let okx_len = ow.written();
        let okx_bytes = &okx_buf[..okx_len];
        let okx_json: Value = serde_json::from_slice(okx_bytes).unwrap();
        assert_eq!(okx_json["instId"], okx_sym);
        assert_eq!(okx_json["side"], "sell");
        assert_eq!(okx_json["ordType"], "rpi");
        assert_eq!(okx_json["px"], "90000");
        assert_eq!(okx_json["sz"], "2.5");
        assert_eq!(okx_json["clOrdId"], client_id);
        let expected_td = if product == Product::Spot {
            "cash"
        } else if product == Product::InverseFutures {
            "isolated"
        } else {
            "cross"
        };
        assert_eq!(okx_json["tdMode"], expected_td);
        if matches!(expected_inst_type, "SWAP" | "FUTURES") {
            assert_eq!(okx_json["posSide"], "long");
            assert_eq!(okx_json["reduceOnly"], true);
        }

        let mut okx_ws = [0u8; 1024];
        let okx_ws_len =
            encode_okx_ws_trade_envelope_into(77, "order", okx_bytes, &mut okx_ws).unwrap();
        let okx_ws_val: Value = serde_json::from_slice(&okx_ws[..okx_ws_len]).unwrap();
        assert_eq!(okx_ws_val["id"], "77");
        assert_eq!(okx_ws_val["op"], "order");
        assert_eq!(okx_ws_val["args"][0]["ordType"], "rpi");

        let okx_creds = Credentials::okx("OKXKEY01", b"okx-secret-key", "okx-pass-01");
        let mut okx_https = [0u8; 2048];
        let okx_https_len = encode_signed_https_post_into(
            &okx_creds,
            Environment::Test,
            1_700_000_000_456,
            "/api/v5/trade/order",
            okx_bytes,
            &mut okx_https,
        )
        .unwrap();
        let okx_https_str = core::str::from_utf8(&okx_https[..okx_https_len]).unwrap();
        assert!(okx_https_str.starts_with("POST /api/v5/trade/order HTTP/1.1\r\n"));
        assert!(okx_https_str.contains("OK-ACCESS-KEY: OKXKEY01\r\n"));
        assert!(okx_https_str.contains("x-simulated-trading: 1\r\n"));
    }
}

#[test]
fn okx_optimal_limit_ioc_and_modify_cancel_wire_builders_match_venue_contracts() {
    let spec = spec_for(Product::LinearPerp);
    let mut params = UtaOrderParams::cross_derivative_limit(TimeInForce::Ioc, false);
    params.order_type = OrderType::Market;
    params.okx_optimal_limit_ioc = true;

    let input = UtaNewOrderWireInput {
        symbol: "BTC-USDT-SWAP",
        spec: &spec,
        side: Side::Buy,
        price_ticks: 0,
        qty_lots: 1_000,
        client_id: "tw10123456789abcdefghjklmnpqrstu",
        params,
    };
    let mut buf = [0u8; 512];
    let mut w = BufWriter::new(&mut buf);
    encode_okx_new_order_body_into(&input, &mut w).unwrap();
    let n = w.written();
    let v: Value = serde_json::from_slice(&buf[..n]).unwrap();
    assert_eq!(v["ordType"], "optimal_limit_ioc");
    assert!(v.get("px").is_none());

    // Amend / Modify for Bybit and OKX
    let mod_in = UtaModifyWireInput {
        symbol: "BTC-USDT-SWAP",
        spec: &spec,
        client_id: Some("tw10123456789abcdefghjklmnpqrstu"),
        venue_order_id: Some("88990011"),
        req_id: Some("99"),
        new_price_ticks: 910_005, // 91000.50
        new_qty_lots: 3_000,      // 3.000
    };
    let mut mbuf = [0u8; 512];
    let mut mw = BufWriter::new(&mut mbuf);
    encode_okx_modify_body_into(&mod_in, &mut mw).unwrap();
    let mn = mw.written();
    let mv: Value = serde_json::from_slice(&mbuf[..mn]).unwrap();
    assert_eq!(mv["instId"], "BTC-USDT-SWAP");
    assert_eq!(mv["ordId"], "88990011");
    assert_eq!(mv["reqId"], "99");
    assert_eq!(mv["newPx"], "91000.5");
    assert_eq!(mv["newSz"], "3");

    let mut bbuf = [0u8; 512];
    let mut bw = BufWriter::new(&mut bbuf);
    let bmod = UtaModifyWireInput {
        symbol: "BTCUSDT",
        ..mod_in
    };
    encode_bybit_modify_body_into(&bmod, &mut bw).unwrap();
    let bn = bw.written();
    let bv: Value = serde_json::from_slice(&bbuf[..bn]).unwrap();
    assert_eq!(bv["category"], "linear");
    assert_eq!(bv["orderId"], "88990011");
    assert_eq!(bv["price"], "91000.5");
    assert_eq!(bv["qty"], "3");

    // Cancel for Bybit and OKX
    let c_okx = UtaCancelWireInput {
        symbol: "BTC-USD-261225-90000-C",
        product: Product::Options,
        client_id: Some("tw10123456789abcdefghjklmnpqrstu"),
        venue_order_id: None,
    };
    let mut cbuf = [0u8; 256];
    let mut cw = BufWriter::new(&mut cbuf);
    encode_okx_cancel_body_into(&c_okx, &mut cw).unwrap();
    let cn = cw.written();
    let cv: Value = serde_json::from_slice(&cbuf[..cn]).unwrap();
    assert_eq!(cv["instId"], "BTC-USD-261225-90000-C");
    assert_eq!(cv["clOrdId"], "tw10123456789abcdefghjklmnpqrstu");

    let c_bybit = UtaCancelWireInput {
        symbol: "BTC-25DEC26-90000-C",
        product: Product::Options,
        client_id: None,
        venue_order_id: Some("opt-ord-77"),
    };
    let mut cbuf2 = [0u8; 256];
    let mut cw2 = BufWriter::new(&mut cbuf2);
    encode_bybit_cancel_body_into(&c_bybit, &mut cw2).unwrap();
    let cn2 = cw2.written();
    let cv2: Value = serde_json::from_slice(&cbuf2[..cn2]).unwrap();
    assert_eq!(cv2["category"], "option");
    assert_eq!(cv2["orderId"], "opt-ord-77");

    // Rate scope mapping checks: Bybit product buckets (1,2,3) & OKX option family extraction
    assert_eq!(
        okx_option_family_key("BTC-USD-261225-90000-C"),
        okx_option_family_key("BTC-USD-270326-100000-P")
    );
    assert_eq!(
        instrument_rate_scope(SpotVenue::Bybit, Product::Spot, "BTCUSDT"),
        tickwire_quota::shared::InstrumentScope::Product(1)
    );
    assert_eq!(
        instrument_rate_scope(SpotVenue::Bybit, Product::LinearPerp, "BTCUSDT"),
        tickwire_quota::shared::InstrumentScope::Product(2)
    );
    assert_eq!(
        instrument_rate_scope(SpotVenue::Bybit, Product::Options, "BTC-25DEC26-90000-C"),
        tickwire_quota::shared::InstrumentScope::Product(3)
    );
}

#[test]
fn ws_login_signatures_two_step_subscriptions_and_20s_heartbeat_work_zero_alloc() {
    // 1. Verify OKX Base64 HMAC-SHA256 `"{unix_sec}GET/users/self/verify"` login frame
    let okx_creds = Credentials::okx("okx-key", b"super-secret", "my-pass");
    let mut login_buf = [0u8; 512];
    let len = encode_okx_login_into(&okx_creds, 1_700_000_999_500, &mut login_buf).unwrap();
    let v: Value = serde_json::from_slice(&login_buf[..len]).unwrap();
    assert_eq!(v["op"], "login");
    assert_eq!(v["args"][0]["apiKey"], "okx-key");
    assert_eq!(v["args"][0]["passphrase"], "my-pass");
    assert_eq!(v["args"][0]["timestamp"], "1700000999");
    let expected_sig = STANDARD.encode(
        hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, b"super-secret"),
            b"1700000999GET/users/self/verify",
        )
        .as_ref(),
    );
    assert_eq!(v["args"][0]["sign"], expected_sig);

    // OKX 2-step private subscribe: step 0 = orders ANY, step 1 = balance_and_position
    let mut s0 = [0u8; 256];
    let n0 = encode_okx_subscribe_step_into(0, &mut s0).unwrap();
    let v0: Value = serde_json::from_slice(&s0[..n0]).unwrap();
    assert_eq!(v0["op"], "subscribe");
    assert_eq!(v0["args"][0]["channel"], "orders");
    assert_eq!(v0["args"][0]["instType"], "ANY");

    let mut s1 = [0u8; 256];
    let n1 = encode_okx_subscribe_step_into(1, &mut s1).unwrap();
    let v1: Value = serde_json::from_slice(&s1[..n1]).unwrap();
    assert_eq!(v1["op"], "subscribe");
    assert_eq!(v1["args"][0]["channel"], "balance_and_position");

    // Bybit auth + private subscribe (`["order","execution","execution.fast","wallet"]`)
    let bybit_creds = Credentials::bybit("bybit-key", b"bybit-secret");
    let mut b_auth = [0u8; 512];
    let blen = encode_bybit_auth_into(&bybit_creds, 1_700_000_000_000, &mut b_auth).unwrap();
    let bv: Value = serde_json::from_slice(&b_auth[..blen]).unwrap();
    assert_eq!(bv["op"], "auth");
    assert_eq!(bv["args"][0], "bybit-key");
    assert_eq!(bv["args"][1], 1_700_000_005_000u64);

    let mut bsub = [0u8; 256];
    let bslen = encode_bybit_private_subscribe_step_into(0, &mut bsub).unwrap();
    let bsv: Value = serde_json::from_slice(&bsub[..bslen]).unwrap();
    assert_eq!(
        bsv["args"],
        serde_json::json!(["order", "execution", "execution.fast", "wallet"])
    );

    // Heartbeat payloads + UtaPrivateStream state machine (`20s` ping/pong + 2-step OKX sub)
    let mut ping_buf = [0u8; 64];
    let p_okx = encode_ws_ping_into(SpotVenue::Okx, &mut ping_buf).unwrap();
    assert_eq!(&ping_buf[..p_okx], b"ping");
    let p_bybit = encode_ws_ping_into(SpotVenue::Bybit, &mut ping_buf).unwrap();
    assert_eq!(&ping_buf[..p_bybit], br#"{"op":"ping"}"#);

    assert_eq!(
        check_okx_login_reply(br#"{"event":"login","code":"0","msg":""}"#),
        WsLoginOutcome::Accepted
    );
    assert_eq!(
        check_bybit_auth_reply(br#"{"op":"auth","success":true,"ret_msg":""}"#),
        WsLoginOutcome::Accepted
    );

    let mut priv_stream = UtaPrivateStream::new(SpotVenue::Okx, 16 * 1024);
    let mut tx_frame = [0u8; 640];
    let login_flen = priv_stream
        .begin_login(&okx_creds, 1_700_000_000_000, 1_000, &mut tx_frame)
        .unwrap();
    assert!(login_flen > 6);
    assert_eq!(priv_stream.phase(), UtaPrivatePhase::Authenticating);

    // Server sends login ack -> UtaPrivateStream emits step 0 subscribe (`orders` `ANY`)
    let srv_login = ws_server_frame(Opcode::Text, br#"{"event":"login","code":"0","msg":""}"#);
    let (_, tx_n1) = priv_stream
        .on_raw_bytes(&srv_login, 2_000, &mut tx_frame, |_| {})
        .unwrap();
    assert!(tx_n1 > 0);
    assert_eq!(
        priv_stream.phase(),
        UtaPrivatePhase::Subscribing { next_step: 1 }
    );

    // Server sends step 0 subscribe ack -> UtaPrivateStream emits step 1 subscribe (`balance_and_position`)
    let srv_sub0 = ws_server_frame(
        Opcode::Text,
        br#"{"event":"subscribe","arg":{"channel":"orders","instType":"ANY"}}"#,
    );
    let (_, tx_n2) = priv_stream
        .on_raw_bytes(&srv_sub0, 3_000, &mut tx_frame, |_| {})
        .unwrap();
    assert!(tx_n2 > 0);
    assert_eq!(
        priv_stream.phase(),
        UtaPrivatePhase::Subscribing { next_step: 2 }
    );

    // Server sends step 1 subscribe ack -> Subscribed!
    let srv_sub1 = ws_server_frame(
        Opcode::Text,
        br#"{"event":"subscribe","arg":{"channel":"balance_and_position"}}"#,
    );
    let (_, tx_n3) = priv_stream
        .on_raw_bytes(&srv_sub1, 4_000, &mut tx_frame, |_| {})
        .unwrap();
    assert_eq!(tx_n3, 0);
    assert!(priv_stream.is_subscribed());

    // 20s heartbeat check
    let not_due = priv_stream
        .encode_due_ping(1_000 + WS_PING_PERIOD_NS - 1, &mut tx_frame)
        .unwrap();
    assert_eq!(not_due, 0);
    let due = priv_stream
        .encode_due_ping(1_000 + WS_PING_PERIOD_NS, &mut tx_frame)
        .unwrap();
    assert!(due > 0);

    // Text `"pong"` from OKX server is decoded cleanly without error
    let srv_pong = ws_server_frame(Opcode::Text, b"pong");
    let mut saw_pong = false;
    priv_stream
        .on_raw_bytes(&srv_pong, 5_000 + WS_PING_PERIOD_NS, &mut tx_frame, |ev| {
            if matches!(
                ev,
                UtaFrameEvent::Control {
                    op: "pong",
                    success: true
                }
            ) {
                saw_pong = true;
            }
        })
        .unwrap();
    assert!(saw_pong);
    assert!(try_decode_ws_server_frame(&srv_pong).unwrap().is_some());
}

#[test]
fn single_pass_json_scan_classifies_all_terminal_reasons_fast_fills_notices_and_rate_limits() {
    // 1. OKX Server Maintenance `notice` (`64008`) -> `graceful_drain_connection == true`
    let okx_notice = br#"{"event":"notice","msg":"Connection will soon be closed for upgrade","code":"64008","connId":"a1b2c3"}"#;
    let (okx_notice_exec, okx_drain) = OkxUtaParser::scan(okx_notice).unwrap();
    assert!(okx_drain);
    assert_eq!(okx_notice_exec.event_kind, Some("notice"));
    assert_eq!(okx_notice_exec.error_code, Some(64_008));

    // 2. Bybit connection-retire notice -> `graceful_drain_connection == true`
    let bybit_notice = br#"{"op":"notice","code":10010,"ret_msg":"server maintenance switchover"}"#;
    let (bybit_notice_exec, bybit_drain) = BybitUtaParser::scan(bybit_notice).unwrap();
    assert!(bybit_drain);
    assert_eq!(bybit_notice_exec.event_kind, Some("notice"));

    // 3. Exact TerminalReason classifications on Bybit `order` push rows:
    //    - CancelBySmp -> MKT_CANCELED_SELFTRADE
    //    - EC_PostOnlyWillTakeLiquidity -> MKT_REJECT_POST_ONLY
    //    - IOC canceled -> MKT_IOC_CANCELED
    //    - EC_ReduceOnlyWillTake -> MKT_REDUCE_ONLY_CANCELED
    //    - Normal cancel (`None` explicit override so `OrderManager::on_canceled` maps
    //      `pending_cxl == 0 -> MKT_CANCELED` and `pending_cxl > 0 -> NO_REASON`)
    let bybit_orders_frame = br#"{
        "topic":"order",
        "data":[
            {"category":"linear","symbol":"BTCUSDT","orderId":"b1","orderLinkId":"tw1a","orderStatus":"Cancelled","cancelType":"CancelBySmp","rejectReason":"EC_NoError","timeInForce":"GTC","side":"Buy","price":"90000","qty":"1","cumExecQty":"0"},
            {"category":"spot","symbol":"BTCUSDT","orderId":"b2","orderLinkId":"tw1b","orderStatus":"Rejected","cancelType":"UNKNOWN","rejectReason":"EC_PostOnlyWillTakeLiquidity","timeInForce":"PostOnly","side":"Buy","price":"90000","qty":"1","cumExecQty":"0"},
            {"category":"option","symbol":"BTC-25DEC26-90000-C","orderId":"b3","orderLinkId":"tw1c","orderStatus":"Cancelled","cancelType":"CancelByUser","rejectReason":"EC_NoError","timeInForce":"IOC","side":"Sell","price":"1200","qty":"2","cumExecQty":"1"},
            {"category":"inverse","symbol":"BTCUSD","orderId":"b4","orderLinkId":"tw1d","orderStatus":"Cancelled","cancelType":"CancelByReduceOnly","rejectReason":"EC_ReduceOnlyWillTake","timeInForce":"GTC","side":"Sell","price":"90000","qty":"1","cumExecQty":"0"},
            {"category":"linear","symbol":"BTCUSDT","orderId":"b5","orderLinkId":"tw1e","orderStatus":"Cancelled","cancelType":"CancelByUser","rejectReason":"EC_NoError","timeInForce":"GTC","side":"Buy","price":"89000","qty":"1","cumExecQty":"0"}
        ]
    }"#;
    let mut rows = Vec::new();
    let summary = scan_bybit_frame(bybit_orders_frame, |ev| {
        if let UtaFrameEvent::OrderRow(r) = ev {
            rows.push(r);
        }
    })
    .unwrap();
    assert_eq!(summary.events_emitted, 5);
    assert_eq!(rows.len(), 5);
    assert_eq!(
        rows[0].normalized_status(SpotVenue::Bybit),
        Some(OrderStatus::Canceled)
    );
    assert_eq!(
        rows[0].classify_explicit_terminal_reason(SpotVenue::Bybit, false, TimeInForce::Gtc),
        Some(TerminalReason::MKT_CANCELED_SELFTRADE)
    );
    assert_eq!(
        rows[1].normalized_status(SpotVenue::Bybit),
        Some(OrderStatus::Rejected)
    );
    assert_eq!(
        rows[1].classify_explicit_terminal_reason(SpotVenue::Bybit, true, TimeInForce::PostOnly),
        Some(TerminalReason::MKT_REJECT_POST_ONLY)
    );
    assert_eq!(
        rows[2].classify_explicit_terminal_reason(SpotVenue::Bybit, false, TimeInForce::Ioc),
        Some(TerminalReason::MKT_IOC_CANCELED)
    );
    assert_eq!(
        rows[3].classify_explicit_terminal_reason(SpotVenue::Bybit, false, TimeInForce::Gtc),
        Some(TerminalReason::MKT_REDUCE_ONLY_CANCELED)
    );
    assert_eq!(
        rows[4].classify_explicit_terminal_reason(SpotVenue::Bybit, false, TimeInForce::Gtc),
        None
    );

    // 4. OKX `orders` push with SMP (`cancelSource: "32"`), `rpi`, AND embedded fill (`tradeId`)
    let okx_orders_frame = br#"{
        "arg":{"channel":"orders","instType":"ANY"},
        "data":[
            {"instType":"SWAP","instId":"BTC-USDT-SWAP","ordId":"o1","clOrdId":"tw1x","state":"canceled","cancelSource":"32","ordType":"limit","side":"buy","px":"90000","sz":"2","accFillSz":"0"},
            {"instType":"OPTION","instId":"BTC-USD-261225-90000-C","ordId":"o2","clOrdId":"tw1y","state":"partially_filled","ordType":"rpi","side":"buy","px":"1500","sz":"5","accFillSz":"2","tradeId":"trd-901","fillPx":"1495","fillSz":"2","fee":"-0.015","feeCcy":"BTC","execType":"M"}
        ]
    }"#;
    let mut okx_order_rows = Vec::new();
    let mut okx_exec_rows = Vec::new();
    scan_okx_frame(okx_orders_frame, |ev| match ev {
        UtaFrameEvent::OrderRow(r) => okx_order_rows.push(r),
        UtaFrameEvent::ExecRow(e) => okx_exec_rows.push(e),
        _ => {}
    })
    .unwrap();
    assert_eq!(okx_order_rows.len(), 2);
    assert_eq!(
        okx_order_rows[0].classify_explicit_terminal_reason(
            SpotVenue::Okx,
            false,
            TimeInForce::Gtc
        ),
        Some(TerminalReason::MKT_CANCELED_SELFTRADE)
    );
    assert_eq!(okx_exec_rows.len(), 1);
    assert_eq!(okx_exec_rows[0].exec_id, "trd-901");
    assert_eq!(okx_exec_rows[0].liquidity, Liquidity::Maker);
    assert_eq!(okx_exec_rows[0].exec_fee_raw, Some("-0.015"));

    // 5. Bybit `execution.fast` (no fee) vs `execution` (with fee) + WS trade rate-limit `10006` + BAPI headers
    let fast_frame = br#"{
        "topic":"execution.fast",
        "data":[{"category":"linear","symbol":"BTCUSDT","execId":"ex-77","orderId":"b9","orderLinkId":"tw1z","side":"Buy","execPrice":"90100.5","execQty":"0.5","isMaker":false,"execTime":"1700000001000"}]
    }"#;
    let mut fast_execs = Vec::new();
    scan_bybit_frame(fast_frame, |ev| {
        if let UtaFrameEvent::ExecRow(e) = ev {
            fast_execs.push(e);
        }
    })
    .unwrap();
    assert_eq!(fast_execs.len(), 1);
    assert!(fast_execs[0].is_fast);
    assert!(fast_execs[0].exec_fee_raw.is_none());
    assert_eq!(fast_execs[0].liquidity, Liquidity::Taker);

    let rl_ws = br#"{"reqId":"88","retCode":10006,"retMsg":"Too many visits!","op":"order.create","header":{"X-Bapi-Limit":"20","X-Bapi-Limit-Status":"0"}}"#;
    let mut replies = Vec::new();
    let rl_summary = scan_bybit_frame(rl_ws, |ev| {
        if let UtaFrameEvent::CommandReply(r) = ev {
            replies.push(r);
        }
    })
    .unwrap();
    assert!(rl_summary.rate_limited);
    assert_eq!(rl_summary.bapi_limit, Some((20, 20)));
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0].reply_id, Some(88));
    assert_eq!(replies[0].op, UtaReplyOp::Create);
    assert_eq!(
        classify_issue(SpotVenue::Bybit, "10006"),
        SpotIssueKind::AccountHazard
    );
    assert_eq!(
        classify_issue(SpotVenue::Bybit, "110007"),
        SpotIssueKind::DeterministicReject
    );
    assert_eq!(
        classify_issue(SpotVenue::Okx, "50011"),
        SpotIssueKind::AccountHazard
    );
    assert_eq!(
        classify_issue(SpotVenue::Okx, "51019"),
        SpotIssueKind::DeterministicReject
    );
}
