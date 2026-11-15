//! Full-market Binance ORS wire encoding, Ed25519 PEM `session.logon`, European Options HTTP/1.1
//! form-urlencoded signing, colo/public `listenKey` URL decoration, and `8.5` error/terminal
//! classifiers (`#![forbid(unsafe_code)]`).
#![forbid(unsafe_code)]

use tickwire_binance::{
    Error,
    auth::{Algorithm, Endpoint, SigningKey},
    full_market::{
        BinanceVenueMarket, OptionsHttpCancelParams, OptionsHttpNewParams, WsOrderCancelParams,
        WsOrderModifyParams, WsOrderNewParams, WsPerFrameAuth, classify_binance_terminal_reason,
        decorate_listen_key_stream_url, encode_options_http_cancel, encode_options_http_place,
        encode_signed_account_target, encode_signed_open_orders_target,
        encode_signed_order_status_target, encode_ws_order_cancel, encode_ws_order_modify,
        encode_ws_order_place, encode_ws_session_logon, encode_ws_user_stream_subscribe,
        explain_binance_error, format_scaled_i64,
    },
};
use tickwire_venue::{
    OrderType, PositionSide, Product, SelfTradePrevention, Side, TerminalReason, TimeInForce,
};

const TEST_ED25519_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MC4CAQAwBQYDK2VwBCIEIIJEYWtGBrhACmb9Dvy+qa8WEf0lQOl1s4CLIAB9m89u\n\
-----END PRIVATE KEY-----\n";

#[test]
fn ed25519_pem_session_logon_across_spot_usdm_coinm_and_rejects_options_or_hmac() {
    let ed_pem = SigningKey::ed25519_pem(TEST_ED25519_PEM).expect("valid Ed25519 PEM");
    let ed_der =
        SigningKey::ed25519_pkcs8(include_bytes!("data/PUBLIC_BINANCE_ED25519.pk8.der")).unwrap();
    assert_eq!(ed_pem.algorithm(), Algorithm::Ed25519);

    for market in [
        BinanceVenueMarket::Spot,
        BinanceVenueMarket::Usdm,
        BinanceVenueMarket::Coinm,
    ] {
        let mut out_pem = [0u8; 1024];
        let mut out_der = [0u8; 1024];
        let n1 = encode_ws_session_logon(
            "logon-1",
            "testApiKey123",
            1_700_000_000_123,
            Some(5_000),
            &ed_pem,
            market,
            &mut out_pem,
        )
        .expect("logon encode via PEM");
        let n2 = encode_ws_session_logon(
            "logon-1",
            "testApiKey123",
            1_700_000_000_123,
            Some(5_000),
            &ed_der,
            market,
            &mut out_der,
        )
        .expect("logon encode via DER");
        assert_eq!(&out_pem[..n1], &out_der[..n2]);
        let frame = core::str::from_utf8(&out_pem[..n1]).unwrap();
        assert!(frame.starts_with(r#"{"id":"logon-1","method":"session.logon","params":{"#));
        assert!(frame.contains(r#""apiKey":"testApiKey123""#));
        assert!(frame.contains(r#""recvWindow":5000"#));
        assert!(frame.contains(r#""timestamp":1700000000123"#));
        assert!(frame.contains(r#""signature":""#));
        assert!(frame.ends_with("}}"));
    }

    // Options does not support WS session.logon.
    let mut scratch = [0u8; 1024];
    assert_eq!(
        encode_ws_session_logon(
            "logon-opt",
            "testApiKey123",
            1_700_000_000_123,
            Some(5_000),
            &ed_pem,
            BinanceVenueMarket::Options,
            &mut scratch,
        ),
        Err(Error::UnsupportedCommand)
    );

    // HMAC-SHA256 is rejected for session.logon (`8.5` requires Ed25519).
    let hmac = SigningKey::hmac(b"options-hmac-secret").unwrap();
    assert_eq!(
        encode_ws_session_logon(
            "logon-hmac",
            "testApiKey123",
            1_700_000_000_123,
            Some(5_000),
            &hmac,
            BinanceVenueMarket::Usdm,
            &mut scratch,
        ),
        Err(Error::UnsupportedAlgorithm)
    );
    assert!(!Endpoint::SpotSessionLogon.permits(Algorithm::HmacSha256));
    assert!(Endpoint::OptionsRest.permits(Algorithm::HmacSha256));
    assert!(!Endpoint::OptionsRest.permits(Algorithm::Ed25519));
}

#[test]
fn subscribe_and_colo_vs_public_listen_key_url_decoration() {
    let mut sub = [0u8; 256];
    let sub_len = encode_ws_user_stream_subscribe("sub-spot-1", &mut sub).unwrap();
    assert_eq!(
        core::str::from_utf8(&sub[..sub_len]).unwrap(),
        r#"{"id":"sub-spot-1","method":"userDataStream.subscribe"}"#
    );

    let mut colo_buf = [0u8; 256];
    let n_colo = decorate_listen_key_stream_url(
        "wss://fstream-mm.binance.com/ws",
        true,
        "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1",
        &mut colo_buf,
    )
    .unwrap();
    assert_eq!(
        core::str::from_utf8(&colo_buf[..n_colo]).unwrap(),
        "wss://fstream-mm.binance.com/ws/pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"
    );

    let mut pub_buf = [0u8; 256];
    let n_pub = decorate_listen_key_stream_url(
        "wss://fstream.binance.com/private/ws",
        false,
        "lk_public_token_01",
        &mut pub_buf,
    )
    .unwrap();
    assert_eq!(
        core::str::from_utf8(&pub_buf[..n_pub]).unwrap(),
        "wss://fstream.binance.com/private/ws?listenKey=lk_public_token_01"
    );
}

#[test]
fn venue_market_product_masks_and_fixed_point_formatting() {
    assert_eq!(
        BinanceVenueMarket::from_product(Product::Spot),
        Some(BinanceVenueMarket::Spot)
    );
    assert_eq!(
        BinanceVenueMarket::from_product(Product::UsdM),
        Some(BinanceVenueMarket::Usdm)
    );
    assert_eq!(
        BinanceVenueMarket::from_product(Product::CoinM),
        Some(BinanceVenueMarket::Coinm)
    );
    assert_eq!(
        BinanceVenueMarket::from_product(Product::Option),
        Some(BinanceVenueMarket::Options)
    );
    assert_ne!(
        BinanceVenueMarket::Spot.product_mask() & BinanceVenueMarket::Usdm.product_mask(),
        BinanceVenueMarket::Spot.product_mask()
    );
    assert_eq!(
        BinanceVenueMarket::Spot.product_mask()
            & (BinanceVenueMarket::Usdm.product_mask()
                | BinanceVenueMarket::Coinm.product_mask()
                | BinanceVenueMarket::Options.product_mask()),
        0
    );

    let mut buf = [0u8; 32];
    let len = format_scaled_i64(5_012_340, 4, &mut buf).unwrap();
    assert_eq!(&buf[..len], b"501.234");
    let len_int = format_scaled_i64(42_000, 3, &mut buf).unwrap();
    assert_eq!(&buf[..len_int], b"42");
}

#[test]
fn ws_order_place_cancel_and_modify_across_spot_usdm_and_coinm() {
    let mut out = [0u8; 1024];

    // 1. Spot PostOnly -> `LIMIT_MAKER` (no `timeInForce`), `DECREMENT` STP supported.
    let n_spot = encode_ws_order_place(
        &WsOrderNewParams {
            market: BinanceVenueMarket::Spot,
            request_id: "p1",
            symbol: "BTCUSDT",
            client_order_id: "tw100000000000000000000000000001",
            side: Side::Buy,
            order_type: OrderType::Limit,
            tif: TimeInForce::PostOnly,
            post_only: true,
            reduce_only: false,
            position_side: PositionSide::Both,
            stp: SelfTradePrevention::Decrement,
            price_ticks: 6_500_050,
            price_scale: 2,
            qty_lots: 1_250,
            qty_scale: 4,
            timestamp_ms: 1_700_000_001_000,
            recv_window_ms: Some(5_000),
            per_frame_auth: None,
        },
        &mut out,
    )
    .unwrap();
    let spot_json = core::str::from_utf8(&out[..n_spot]).unwrap();
    assert!(spot_json.contains(r#""method":"order.place""#));
    assert!(spot_json.contains(r#""type":"LIMIT_MAKER""#));
    assert!(!spot_json.contains(r#""timeInForce""#));
    assert!(spot_json.contains(r#""price":"65000.5""#));
    assert!(spot_json.contains(r#""quantity":"0.125""#));
    assert!(spot_json.contains(r#""selfTradePreventionMode":"DECREMENT""#));

    // 2. USD-M PostOnly (`GTX`) + `reduceOnly` + `LONG` positionSide, and `RPI` (`timeInForce:"RPI"`).
    let n_um_gtx = encode_ws_order_place(
        &WsOrderNewParams {
            market: BinanceVenueMarket::Usdm,
            request_id: "p2",
            symbol: "BTCUSDT",
            client_order_id: "tw100000000000000000000000000002",
            side: Side::Sell,
            order_type: OrderType::Limit,
            tif: TimeInForce::Gtc,
            post_only: true,
            reduce_only: true,
            position_side: PositionSide::Long,
            stp: SelfTradePrevention::ExpireMaker,
            price_ticks: 6_600_000,
            price_scale: 2,
            qty_lots: 25,
            qty_scale: 3,
            timestamp_ms: 1_700_000_002_000,
            recv_window_ms: Some(5_000),
            per_frame_auth: None,
        },
        &mut out,
    )
    .unwrap();
    let um_gtx = core::str::from_utf8(&out[..n_um_gtx]).unwrap();
    assert!(um_gtx.contains(r#""type":"LIMIT""#));
    assert!(um_gtx.contains(r#""timeInForce":"GTX""#));
    assert!(um_gtx.contains(r#""reduceOnly":"true""#));
    assert!(um_gtx.contains(r#""positionSide":"LONG""#));
    assert!(um_gtx.contains(r#""selfTradePreventionMode":"EXPIRE_MAKER""#));

    let n_um_rpi = encode_ws_order_place(
        &WsOrderNewParams {
            market: BinanceVenueMarket::Usdm,
            request_id: "p3",
            symbol: "BTCUSDT",
            client_order_id: "tw100000000000000000000000000003",
            side: Side::Buy,
            order_type: OrderType::Limit,
            tif: TimeInForce::Rpi,
            post_only: true,
            reduce_only: false,
            position_side: PositionSide::Both,
            stp: SelfTradePrevention::Default,
            price_ticks: 6_499_990,
            price_scale: 2,
            qty_lots: 10,
            qty_scale: 3,
            timestamp_ms: 1_700_000_002_500,
            recv_window_ms: Some(5_000),
            per_frame_auth: None,
        },
        &mut out,
    )
    .unwrap();
    let um_rpi = core::str::from_utf8(&out[..n_um_rpi]).unwrap();
    assert!(um_rpi.contains(r#""timeInForce":"RPI""#));

    // 3. Coin-M per-frame signed `order.cancel` and native `order.modify` vs Spot `order.cancelReplace`.
    let ed_pem = SigningKey::ed25519_pem(TEST_ED25519_PEM).unwrap();
    let n_cm_cxl = encode_ws_order_cancel(
        &WsOrderCancelParams {
            market: BinanceVenueMarket::Coinm,
            request_id: "c1",
            symbol: "BTCUSD_PERP",
            orig_client_order_id: Some("tw100000000000000000000000000004"),
            venue_order_id: Some(88990011),
            timestamp_ms: 1_700_000_003_000,
            recv_window_ms: Some(5_000),
            per_frame_auth: Some(WsPerFrameAuth {
                api_key: "cmKey01",
                signer: &ed_pem,
            }),
        },
        &mut out,
    )
    .unwrap();
    let cm_cxl = core::str::from_utf8(&out[..n_cm_cxl]).unwrap();
    assert!(cm_cxl.contains(r#""method":"order.cancel""#));
    assert!(cm_cxl.contains(r#""origClientOrderId":"tw100000000000000000000000000004""#));
    assert!(cm_cxl.contains(r#""orderId":88990011"#));
    assert!(cm_cxl.contains(r#""signature":""#));

    let n_um_mod = encode_ws_order_modify(
        &WsOrderModifyParams {
            market: BinanceVenueMarket::Usdm,
            request_id: "m1",
            symbol: "BTCUSDT",
            orig_client_order_id: Some("tw100000000000000000000000000002"),
            venue_order_id: Some(777),
            new_client_order_id: None,
            side: Side::Sell,
            new_price_ticks: 6_610_000,
            price_scale: 2,
            new_qty_lots: 30,
            qty_scale: 3,
            timestamp_ms: 1_700_000_004_000,
            recv_window_ms: Some(5_000),
            per_frame_auth: None,
        },
        &mut out,
    )
    .unwrap();
    let um_mod = core::str::from_utf8(&out[..n_um_mod]).unwrap();
    assert!(um_mod.contains(r#""method":"order.modify""#));
    assert!(um_mod.contains(r#""price":"66100""#));
    assert!(um_mod.contains(r#""quantity":"0.03""#));

    let n_spot_mod = encode_ws_order_modify(
        &WsOrderModifyParams {
            market: BinanceVenueMarket::Spot,
            request_id: "m2",
            symbol: "BTCUSDT",
            orig_client_order_id: Some("tw100000000000000000000000000001"),
            venue_order_id: Some(555),
            new_client_order_id: Some("tw100000000000000000000000000009"),
            side: Side::Buy,
            new_price_ticks: 6_505_000,
            price_scale: 2,
            new_qty_lots: 1_500,
            qty_scale: 4,
            timestamp_ms: 1_700_000_005_000,
            recv_window_ms: Some(5_000),
            per_frame_auth: None,
        },
        &mut out,
    )
    .unwrap();
    let spot_mod = core::str::from_utf8(&out[..n_spot_mod]).unwrap();
    assert!(spot_mod.contains(r#""method":"order.cancelReplace""#));
    assert!(spot_mod.contains(r#""cancelReplaceMode":"STOP_ON_FAILURE""#));
    assert!(spot_mod.contains(r#""cancelOrderId":555"#));
}

#[test]
fn european_options_http_place_cancel_and_signed_rest_queries() {
    let hmac = SigningKey::hmac(b"options-secret-key-xyz").unwrap();
    let mut out = [0u8; 1536];

    let n_place = encode_options_http_place(
        &OptionsHttpNewParams {
            host: "eapi.binance.com",
            api_key: "optApiKey99",
            hmac_signer: &hmac,
            symbol: "BTC-261225-80000-C",
            client_order_id: "tw100000000000000000000000000010",
            side: Side::Buy,
            tif: TimeInForce::PostOnly,
            post_only: true,
            reduce_only: true,
            price_ticks: 12_500,
            price_scale: 1,
            qty_lots: 25,
            qty_scale: 2,
            timestamp_ms: 1_700_000_010_000,
            recv_window_ms: Some(5_000),
        },
        &mut out,
    )
    .unwrap();
    let http_place = core::str::from_utf8(&out[..n_place]).unwrap();
    assert!(http_place.starts_with("POST /eapi/v1/order HTTP/1.1\r\nHost: eapi.binance.com\r\n"));
    assert!(http_place.contains("\r\nX-MBX-APIKEY: optApiKey99\r\n"));
    assert!(http_place.contains("\r\nContent-Type: application/x-www-form-urlencoded\r\n"));
    assert!(http_place.contains("\r\nConnection: keep-alive\r\n\r\n"));
    let (_, body) = http_place.split_once("\r\n\r\n").unwrap();
    assert!(body.contains("symbol=BTC-261225-80000-C"));
    assert!(body.contains("price=1250"));
    assert!(body.contains("quantity=0.25"));
    assert!(body.contains("timeInForce=GTX"));
    assert!(body.contains("reduceOnly=true"));
    assert!(body.contains("clientOrderId=tw100000000000000000000000000010"));
    assert!(body.contains("&signature="));

    let n_cxl = encode_options_http_cancel(
        &OptionsHttpCancelParams {
            host: "eapi.binance.com",
            api_key: "optApiKey99",
            hmac_signer: &hmac,
            symbol: "BTC-261225-80000-C",
            client_order_id: Some("tw100000000000000000000000000010"),
            venue_order_id: Some(123456),
            timestamp_ms: 1_700_000_011_000,
            recv_window_ms: Some(5_000),
        },
        &mut out,
    )
    .unwrap();
    let http_cxl = core::str::from_utf8(&out[..n_cxl]).unwrap();
    assert!(http_cxl.starts_with("DELETE /eapi/v1/order HTTP/1.1\r\nHost: eapi.binance.com\r\n"));
    assert!(http_cxl.contains("orderId=123456"));
    assert!(http_cxl.contains("clientOrderId=tw100000000000000000000000000010"));

    // Status query uses `origClientOrderId` on Spot/UM/CM and `clientOrderId` on Options (`recvWindow=60000`).
    let ed_pem = SigningKey::ed25519_pem(TEST_ED25519_PEM).unwrap();
    let n_um_status = encode_signed_order_status_target(
        BinanceVenueMarket::Usdm,
        "BTCUSDT",
        "tw100000000000000000000000000002",
        60_000,
        1_700_000_020_000,
        &ed_pem,
        &mut out,
    )
    .unwrap();
    let um_status = core::str::from_utf8(&out[..n_um_status]).unwrap();
    assert!(um_status.starts_with("/fapi/v1/order?"));
    assert!(um_status.contains("origClientOrderId=tw100000000000000000000000000002"));
    assert!(um_status.contains("recvWindow=60000"));

    let n_opt_status = encode_signed_order_status_target(
        BinanceVenueMarket::Options,
        "BTC-261225-80000-C",
        "tw100000000000000000000000000010",
        60_000,
        1_700_000_020_000,
        &hmac,
        &mut out,
    )
    .unwrap();
    let opt_status = core::str::from_utf8(&out[..n_opt_status]).unwrap();
    assert!(opt_status.starts_with("/eapi/v1/order?"));
    assert!(opt_status.contains("clientOrderId=tw100000000000000000000000000010"));
    assert!(!opt_status.contains("origClientOrderId="));

    let n_open = encode_signed_open_orders_target(
        BinanceVenueMarket::Coinm,
        None,
        60_000,
        1_700_000_020_000,
        &ed_pem,
        &mut out,
    )
    .unwrap();
    assert!(
        core::str::from_utf8(&out[..n_open])
            .unwrap()
            .starts_with("/dapi/v1/openOrders?recvWindow=60000")
    );

    let n_acc = encode_signed_account_target(
        BinanceVenueMarket::Usdm,
        60_000,
        1_700_000_020_000,
        &ed_pem,
        &mut out,
    )
    .unwrap();
    assert!(
        core::str::from_utf8(&out[..n_acc])
            .unwrap()
            .starts_with("/fapi/v2/account?recvWindow=60000")
    );
}

#[test]
fn explain_error_and_terminal_reason_match_section_8_5_invariants() {
    assert_eq!(
        explain_binance_error(-1007, "Timeout waiting for backend", None, false),
        TerminalReason::MKT_DELAYED
    );
    assert_eq!(
        explain_binance_error(-1000, "An unknown error occurred", None, false),
        TerminalReason::MKT_DELAYED
    );
    assert_eq!(
        explain_binance_error(-1001, "Internal error; unable to process", None, false),
        TerminalReason::MKT_DELAYED
    );
    assert_eq!(
        explain_binance_error(-1006, "Unexpected response", None, false),
        TerminalReason::MKT_DELAYED
    );
    assert_eq!(
        explain_binance_error(408, "Request Timeout", None, false),
        TerminalReason::MKT_DELAYED
    );
    assert_eq!(
        explain_binance_error(-2013, "Order does not exist.", None, false),
        TerminalReason::MKT_REJECT_NO_SUCH_ORDER
    );
    assert_eq!(
        explain_binance_error(-5022, "Post Only order rejected", None, true),
        TerminalReason::MKT_REJECT_POST_ONLY
    );
    assert_eq!(
        explain_binance_error(-2022, "ReduceOnly Order is rejected", None, false),
        TerminalReason::MKT_REDUCE_ONLY_CANCELED
    );
    assert_eq!(
        explain_binance_error(-1003, "Too many requests", None, false),
        TerminalReason::RateLimited
    );
    assert_eq!(
        explain_binance_error(429, "Too Many Requests", None, false),
        TerminalReason::RateLimited
    );
    assert_eq!(
        explain_binance_error(418, "IP auto-banned", None, false),
        TerminalReason::RateLimited
    );

    // Self-trade prevention (`EXPIRED_IN_MATCH` or `TRADE_PREVENTION`).
    assert_eq!(
        classify_binance_terminal_reason(
            BinanceVenueMarket::Spot,
            "EXPIRED_IN_MATCH",
            "TRADE_PREVENTION",
            "GTC",
            Some(TimeInForce::Gtc),
            false,
            None,
            0,
            false,
        ),
        TerminalReason::MKT_CANCELED_SELFTRADE
    );

    // IOC / FOK `EXPIRED` -> `MKT_IOC_CANCELED`.
    assert_eq!(
        classify_binance_terminal_reason(
            BinanceVenueMarket::Usdm,
            "EXPIRED",
            "EXPIRED",
            "IOC",
            Some(TimeInForce::Ioc),
            false,
            None,
            0,
            false,
        ),
        TerminalReason::MKT_IOC_CANCELED
    );

    // GTX / PostOnly on Spot, Coin-M, and Options -> `MKT_REJECT_POST_ONLY`.
    for market in [
        BinanceVenueMarket::Spot,
        BinanceVenueMarket::Coinm,
        BinanceVenueMarket::Options,
    ] {
        assert_eq!(
            classify_binance_terminal_reason(
                market,
                "EXPIRED",
                "EXPIRED",
                "GTX",
                Some(TimeInForce::PostOnly),
                true,
                None,
                0,
                false,
            ),
            TerminalReason::MKT_REJECT_POST_ONLY
        );
    }

    // USD-M `EXPIRED` -> `MKT_REDUCE_ONLY_CANCELED`.
    assert_eq!(
        classify_binance_terminal_reason(
            BinanceVenueMarket::Usdm,
            "EXPIRED",
            "EXPIRED",
            "GTC",
            Some(TimeInForce::Gtc),
            false,
            None,
            0,
            false,
        ),
        TerminalReason::MKT_REDUCE_ONLY_CANCELED
    );

    // `CANCELED` with `pending_cxl > 0` -> `NO_REASON` (or `ADMIN_ACTION`), `pending_cxl == 0` -> `MKT_CANCELED`.
    assert_eq!(
        classify_binance_terminal_reason(
            BinanceVenueMarket::Usdm,
            "CANCELED",
            "CANCELED",
            "GTC",
            Some(TimeInForce::Gtc),
            false,
            None,
            1,
            false,
        ),
        TerminalReason::NO_REASON
    );
    assert_eq!(
        classify_binance_terminal_reason(
            BinanceVenueMarket::Usdm,
            "CANCELED",
            "CANCELED",
            "GTC",
            Some(TimeInForce::Gtc),
            false,
            None,
            1,
            true,
        ),
        TerminalReason::ADMIN_ACTION
    );
    assert_eq!(
        classify_binance_terminal_reason(
            BinanceVenueMarket::Usdm,
            "CANCELED",
            "CANCELED",
            "GTC",
            Some(TimeInForce::Gtc),
            false,
            None,
            0,
            false,
        ),
        TerminalReason::MKT_CANCELED
    );
}
