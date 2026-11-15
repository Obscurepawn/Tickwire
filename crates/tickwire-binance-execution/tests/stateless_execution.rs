//! End-to-end integration tests for `BinanceExecutionEngine`, `BinanceRestRequestBuilder`,
//! `AsyncRestQuerier` `listenKey` + `StatusAsk` driving, Options disconnect reaping, and
//! `BinanceHotPathWarmer::warm_order_manager` across all four Binance markets (`#![forbid(unsafe_code)]`).
#![forbid(unsafe_code)]

use std::{
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr, TcpListener, TcpStream},
    sync::Arc,
    time::{Duration, Instant},
};

use tickwire_account::{
    OrderManager, OrderManagerConfig, OrsAccountDomainConfig, OrsReport, OrsSymbolSpec,
};
use tickwire_binance_account::pack_ws_reply_id;
use tickwire_binance_execution::{
    BinanceAccountSpec, BinanceAssetBinding, BinanceBootstrapCatalog, BinanceExecutionEngine,
    BinanceHotPathWarmer, BinanceMarketAuth, BinanceMarketConfig, BinanceNewOrderIntent,
    BinanceRequestKind, BinanceRequestRef, BinanceRestRequestBuilder, BinanceSubmitOutcome,
    BinanceSymbolBinding, BinanceUrlId, BinanceVenueMarket, UrlRole, pack_status_task_key,
    unpack_status_task_key,
};
use tickwire_binance_quota::{AccountRateOverrides, IpBudgetSpec, IpKey, SharedBinanceOrsQuota};
use tickwire_transport::{
    Connection, HttpsLink, IoBudget, Limits, LinkOps, Security, UrlBindPair, WsLink, WsLoginState,
    dialer::DialSecurity,
    latency_tracer::ThreadTag,
    rest_querier::{AsyncRestQuerier, RestQuerierConfig},
};
use tickwire_venue::{
    AssetKey, Environment, OrderType, PositionSide, Product, SelfTradePrevention, Side, TimeInForce,
};

const TEST_ED25519_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MC4CAQAwBQYDK2VwBCIEIIJEYWtGBrhACmb9Dvy+qa8WEf0lQOl1s4CLIAB9m89u\n\
-----END PRIVATE KEY-----\n";

fn connect_loopback_pair(local_ip: IpAddr) -> (Connection, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer_addr = listener.local_addr().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut conn = Connection::connect_from(
        peer_addr,
        Some(local_ip),
        Security::Plain,
        Limits::default(),
        deadline,
    )
    .unwrap();
    let (server_stream, _) = listener.accept().unwrap();
    server_stream.set_nonblocking(true).unwrap();
    for _ in 0..30 {
        let _ = conn.drive(Instant::now(), IoBudget::default()).unwrap();
        if conn.is_ready() {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(conn.is_ready());
    (conn, server_stream)
}

#[test]
fn execution_engine_rest_querier_listen_key_status_ask_and_hot_path_warmer() {
    let domain = OrsAccountDomainConfig {
        account_ref: 1,
        risk_domain_id: [0x55; 16],
        venue: 1,
        environment: Environment::Production,
        exchange_account_id: 77_001,
        epoch: 1,
    };
    let usdm_spec = OrsSymbolSpec {
        symbol_id: 0,
        instrument_id: 2001,
        product: Product::UsdM,
        wallet_id: 2,
        base_asset_id: 2,
        base_scale: 8,
        quote_asset_id: 1,
        quote_scale: 8,
        settlement_asset_id: 1,
        settlement_scale: 8,
        price_tick_atoms: 1,
        price_scale: 2,
        qty_step_atoms: 1,
        qty_scale: 3,
        contract_size_atoms: 1,
        contract_scale: 0,
        initial_margin_bps: 1_000,
    };
    let opt_spec = OrsSymbolSpec {
        symbol_id: 1,
        instrument_id: 4001,
        product: Product::Option,
        wallet_id: 4,
        base_asset_id: 2,
        base_scale: 8,
        quote_asset_id: 1,
        quote_scale: 8,
        settlement_asset_id: 1,
        settlement_scale: 8,
        price_tick_atoms: 1,
        price_scale: 1,
        qty_step_atoms: 1,
        qty_scale: 2,
        contract_size_atoms: 1,
        contract_scale: 0,
        initial_margin_bps: 10_000,
    };

    let om = Arc::new(
        OrderManager::<true>::new(OrderManagerConfig::single_account_default(
            domain,
            vec![usdm_spec, opt_spec],
        ))
        .unwrap(),
    );
    om.dop_state().configure_strategy(1, &[1]).unwrap();
    om.dop_state().login(1, 1).unwrap();
    om.wallet_ledger()
        .set_balance(1, AssetKey::new(2, 1), 8, 10_000_000_000_000, 0)
        .unwrap();
    om.wallet_ledger()
        .set_balance(1, AssetKey::new(4, 1), 8, 10_000_000_000_000, 0)
        .unwrap();

    let catalog = Arc::new(
        BinanceBootstrapCatalog::new(
            vec![
                BinanceSymbolBinding {
                    symbol_id: 0,
                    market: BinanceVenueMarket::Usdm,
                    venue_symbol: String::from("BTCUSDT"),
                    spec: usdm_spec,
                    default_fee_asset_code: Some(String::from("USDT")),
                },
                BinanceSymbolBinding {
                    symbol_id: 1,
                    market: BinanceVenueMarket::Options,
                    venue_symbol: String::from("BTC-261225-80000-C"),
                    spec: opt_spec,
                    default_fee_asset_code: Some(String::from("USDT")),
                },
            ],
            vec![
                BinanceAssetBinding {
                    market: BinanceVenueMarket::Usdm,
                    asset_code: String::from("USDT"),
                    key: AssetKey::new(2, 1),
                    scale: 8,
                },
                BinanceAssetBinding {
                    market: BinanceVenueMarket::Options,
                    asset_code: String::from("USDT"),
                    key: AssetKey::new(4, 1),
                    scale: 8,
                },
            ],
        )
        .unwrap(),
    );

    let ip1 = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let spec = BinanceAccountSpec::new(
        domain,
        vec![
            BinanceMarketConfig {
                market: BinanceVenueMarket::Usdm,
                auth: BinanceMarketAuth::ed25519_pem("umKey01", TEST_ED25519_PEM),
                urls: vec![BinanceUrlId::UmOrderWssColo, BinanceUrlId::UmStreamWssColo],
                order_binds: vec![UrlBindPair::new(Some(ip1), 1)],
                stream_binds: vec![UrlBindPair::new(Some(ip1), 1)],
                rate_overrides: AccountRateOverrides::default(),
                ip_budgets: vec![IpBudgetSpec::new(ip1, 6_000)],
            },
            BinanceMarketConfig {
                market: BinanceVenueMarket::Options,
                auth: BinanceMarketAuth::hmac_sha256("optKey01", b"opt-secret-xyz".to_vec()),
                urls: vec![
                    BinanceUrlId::OptionHttpsOrderPublic,
                    BinanceUrlId::OptionStreamWssPublic,
                ],
                order_binds: vec![UrlBindPair::new(Some(ip1), 1)],
                stream_binds: vec![UrlBindPair::new(Some(ip1), 1)],
                rate_overrides: AccountRateOverrides::default(),
                ip_budgets: vec![IpBudgetSpec::new(ip1, 6_000)],
            },
        ],
    );

    let quota = SharedBinanceOrsQuota::new();
    let mut engine =
        BinanceExecutionEngine::configure(spec, om.clone(), quota, catalog, None, None).unwrap();

    // Verify `pack_status_task_key` / `unpack_status_task_key` roundtrip.
    let k_place = pack_status_task_key(0x01_0000_0042, false);
    assert_eq!(
        unpack_status_task_key(k_place),
        (0x01_0000_0042, BinanceRequestKind::Place)
    );
    let k_cancel = pack_status_task_key(0x01_0000_0042, true);
    assert_eq!(
        unpack_status_task_key(k_cancel),
        (0x01_0000_0042, BinanceRequestKind::Cancel)
    );

    // Warm all 4 markets via `BinanceHotPathWarmer::warm_order_manager`!
    for product in [
        Product::Spot,
        Product::UsdM,
        Product::CoinM,
        Product::Option,
    ] {
        let mut warmer = BinanceHotPathWarmer::new(product, Environment::Production).unwrap();
        let bytes = warmer
            .warm_order_manager(
                engine.session().order_manager(),
                0,
                product != Product::Option,
                1_000_000,
            )
            .unwrap();
        assert!(bytes > 0);
    }

    // Install live loopback order links for USD-M (`conn_id = 0`) and Options (`conn_id = 1`).
    let (ws_conn, _ws_peer) = connect_loopback_pair(ip1);
    engine
        .connections_mut()
        .install_order_link(
            BinanceVenueMarket::Usdm,
            0,
            LinkOps::Ws(
                WsLink::with_login(ws_conn, 65_536, 65_536, WsLoginState::Authenticated).unwrap(),
            ),
        )
        .unwrap();
    let (https_conn, _https_peer) = connect_loopback_pair(ip1);
    engine
        .connections_mut()
        .install_order_link(
            BinanceVenueMarket::Options,
            1,
            LinkOps::Https(HttpsLink::new(https_conn, 65_536, 64).unwrap()),
        )
        .unwrap();

    // Spin up a real loopback HTTP server backing `AsyncRestQuerier` + `BinanceRestRequestBuilder`!
    let rest_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let rest_addr = rest_listener.local_addr().unwrap();
    let server_thread = std::thread::spawn(move || {
        let (mut sock, _) = rest_listener.accept().unwrap();
        sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut buf = [0u8; 4096];

        // Request 1: `POST /fapi/v1/listenKey`
        let n1 = sock.read(&mut buf).unwrap();
        let req1 = String::from_utf8_lossy(&buf[..n1]);
        assert!(req1.starts_with("POST /fapi/v1/listenKey HTTP/1.1\r\n"));
        assert!(req1.contains("X-MBX-APIKEY: umKey01\r\n"));
        let body1 = r#"{"listenKey":"exec_engine_lk_001"}"#;
        let resp1 = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
            body1.len(),
            body1
        );
        sock.write_all(resp1.as_bytes()).unwrap();

        // Request 2: `GET /fapi/v1/order?...&recvWindow=60000&...`
        let n2 = sock.read(&mut buf).unwrap();
        let req2 = String::from_utf8_lossy(&buf[..n2]);
        assert!(req2.starts_with("GET /fapi/v1/order?"));
        assert!(req2.contains("recvWindow=60000"));
        let body2 = r#"{"orderId":333001,"symbol":"BTCUSDT","status":"NEW","price":"50000.00","origQty":"0.010","executedQty":"0.000"}"#;
        let resp2 = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
            body2.len(),
            body2
        );
        sock.write_all(resp2.as_bytes()).unwrap();
    });

    let builder = BinanceRestRequestBuilder::new(
        Arc::clone(engine.session()),
        BinanceVenueMarket::Usdm,
        || 1_700_000_500_000,
    );
    let querier = AsyncRestQuerier::new(
        RestQuerierConfig {
            host: String::from("127.0.0.1"),
            port: rest_addr.port(),
            initial_peers: vec![rest_addr],
            local_ip: Some(ip1),
            nic: None,
            security: DialSecurity::Plain,
            limits: Limits::default(),
            capacity: 64,
            connect_timeout_ms: 2_000,
            interval_ms: 10,
            timeout_ms: 2_000,
            housekeeping_cpus: None,
            reply_waker: None,
        },
        builder,
    )
    .unwrap();

    // 1. Enqueue `listenKey` create task -> drain reply -> `apply_rest_reply` populates `GuardedValue`!
    assert!(!engine.session().has_stream_token(BinanceVenueMarket::Usdm));
    assert!(engine.enqueue_listen_key_task(BinanceVenueMarket::Usdm, false, &querier));

    let mut reports: Vec<OrsReport> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !engine.session().has_stream_token(BinanceVenueMarket::Usdm) {
        assert!(Instant::now() < deadline, "timed out waiting for listenKey");
        querier.drain_replies(|rep| {
            engine
                .apply_rest_reply(BinanceVenueMarket::Usdm, &rep, 1_100_000_000, &mut reports)
                .unwrap();
        });
        std::thread::sleep(Duration::from_millis(5));
    }
    let mut stream_url = [0u8; 256];
    let slen = engine
        .session()
        .connection_url(BinanceVenueMarket::Usdm, UrlRole::STREAM, &mut stream_url)
        .unwrap();
    assert_eq!(
        core::str::from_utf8(&stream_url[..slen]).unwrap(),
        "wss://fstream-mm.binance.com/ws/exec_engine_lk_001"
    );

    // 2. Submit USD-M order -> `-1007` timeout -> `pump_due_queries` -> `AsyncRestQuerier` -> `apply_rest_reply` (`Ack`)!
    let mut frame_buf = [0u8; 2048];
    let t0 = 2_000_000_000u64;
    let oid = match engine
        .session()
        .submit_new(
            &BinanceNewOrderIntent {
                strat_id: 1,
                account_ref: 1,
                msg_seqnum: 1,
                order_seqnum: 1,
                symbol_id: 0,
                side: Side::Buy,
                order_type: OrderType::Limit,
                price_ticks: 5_000_000,
                qty_lots: 10,
                tif: TimeInForce::Gtc,
                post_only: false,
                reduce_only: false,
                position_side: PositionSide::Both,
                stp_mode: SelfTradePrevention::Default,
                check_stp: true,
                dry_run: false,
                wall_ms: 1_700_000_500_000,
                now_mono_ns: t0,
            },
            None,
            &mut frame_buf,
            &mut reports,
        )
        .unwrap()
    {
        BinanceSubmitOutcome::Sent { ors_order_id, .. } => ors_order_id,
        other => panic!("unexpected {other:?}"),
    };
    let raw_ws = engine
        .connections()
        .order_pool(BinanceVenueMarket::Usdm)
        .unwrap()
        .connection_handle(0)
        .unwrap()
        .match_ws_reply(pack_ws_reply_id(oid, BinanceRequestKind::Place))
        .unwrap();
    engine
        .session()
        .on_message(
            BinanceVenueMarket::Usdm,
            IpKey::from_ip(ip1),
            br#"{"id":"p1","status":504,"error":{"code":-1007,"msg":"Timeout"}}"#,
            BinanceRequestRef::from_transport_ref(raw_ws),
            t0 + 5_000_000,
            &mut reports,
        )
        .unwrap();

    assert_eq!(
        engine.pump_due_queries(t0 + 6_050_000_000, |m| {
            (m == BinanceVenueMarket::Usdm).then_some(&querier)
        }),
        1
    );

    let deadline2 = Instant::now() + Duration::from_secs(5);
    while !reports
        .iter()
        .any(|r| matches!(r, OrsReport::Ack(a) if a.ors_order_id == oid))
    {
        assert!(
            Instant::now() < deadline2,
            "timed out waiting for StatusAsk Ack"
        );
        querier.drain_replies(|rep| {
            engine
                .apply_rest_reply(
                    BinanceVenueMarket::Usdm,
                    &rep,
                    t0 + 6_100_000_000,
                    &mut reports,
                )
                .unwrap();
        });
        std::thread::sleep(Duration::from_millis(5));
    }

    // 3. Submit Options order on `conn_id = 1`, mark close-pending, and verify `reap_disconnects`
    //    schedules a delayed `StatusAsk`!
    let opt_oid = match engine
        .session()
        .submit_new(
            &BinanceNewOrderIntent {
                strat_id: 1,
                account_ref: 1,
                msg_seqnum: 2,
                order_seqnum: 2,
                symbol_id: 1,
                side: Side::Buy,
                order_type: OrderType::Limit,
                price_ticks: 10_000,
                qty_lots: 10,
                tif: TimeInForce::Gtc,
                post_only: false,
                reduce_only: false,
                position_side: PositionSide::Both,
                stp_mode: SelfTradePrevention::Default,
                check_stp: true,
                dry_run: false,
                wall_ms: 1_700_000_600_000,
                now_mono_ns: t0 + 10_000_000_000,
            },
            None,
            &mut frame_buf,
            &mut reports,
        )
        .unwrap()
    {
        BinanceSubmitOutcome::Sent { ors_order_id, .. } => ors_order_id,
        other => panic!("unexpected {other:?}"),
    };
    engine
        .connections()
        .order_pool(BinanceVenueMarket::Options)
        .unwrap()
        .connection_handle(1)
        .unwrap()
        .request_close(ThreadTag::RESP);
    let reaped = engine
        .reap_disconnects(t0 + 10_010_000_000, &mut reports)
        .unwrap();
    assert_eq!(reaped, 1);
    assert!(
        engine
            .session()
            .order_manager()
            .lock_order(opt_oid)
            .is_some(),
        "Options order remains live awaiting delayed StatusAsk after HttpsLink drop"
    );

    querier.stop();
    server_thread.join().unwrap();
}
