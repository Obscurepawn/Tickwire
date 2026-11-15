#![forbid(unsafe_code)]

use std::{
    io::Read,
    net::{Ipv4Addr, TcpListener, TcpStream},
    sync::Arc,
    time::{Duration, Instant},
};
use tickwire_account::{
    OrderManager, OrderManagerConfig, OrsAccountDomainConfig, OrsCancelRequest, OrsNewOrderRequest,
    OrsReport, OrsSymbolSpec,
};
use tickwire_quota::shared::{AccountRateOverrides, SharedRateLimiter};
use tickwire_spot_execution::{
    BybitUtaSession, Environment, OkxUtaSession, SpotVenue, UtaAssetBinding, UtaConnPoolEntry,
    UtaHotPathWarmer, UtaOrderParams, UtaSessionConfig, UtaSubmitOutcome, UtaSubmitRefusal,
    UtaSymbolBinding, UtaTransportMode, UtaWarmupConfig, network::OwnedCredentials,
};
use tickwire_transport::{
    Connection, IoBudget, Limits, Security,
    dialer::{DialBaton, DialState, DialedLink},
    order_router::RouterConfig,
    ws::Opcode,
};
use tickwire_venue::{AssetKey, OrderRoute, Product, Side, TimeInForce};

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

fn loopback_pair() -> (Connection, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut client =
        Connection::connect_from(addr, None, Security::Plain, Limits::default(), deadline).unwrap();
    let (server, _) = listener.accept().unwrap();
    server.set_nonblocking(true).unwrap();
    for _ in 0..32 {
        if client.is_ready() {
            break;
        }
        let _ = client.drive(Instant::now(), IoBudget::default());
    }
    assert!(client.is_ready());
    (client, server)
}

fn read_loopback(srv: &mut TcpStream, buf: &mut [u8]) -> usize {
    let deadline = Instant::now() + Duration::from_millis(200);
    loop {
        match srv.read(buf) {
            Ok(n) if n > 0 => return n,
            Ok(_) => panic!("unexpected EOF on loopback server"),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for loopback bytes"
                );
                std::thread::yield_now();
            }
            Err(e) => panic!("loopback read error: {e}"),
        }
    }
}

fn uta_symbols_bybit() -> Vec<UtaSymbolBinding> {
    let mk = |id: u32, sym: &str, product: Product, wallet_id: u16| {
        UtaSymbolBinding::new(
            id,
            sym,
            OrsSymbolSpec {
                symbol_id: id,
                instrument_id: u64::from(id) + 100,
                product,
                wallet_id,
                base_asset_id: 1,
                base_scale: 6,
                quote_asset_id: 2,
                quote_scale: 6,
                settlement_asset_id: 2,
                settlement_scale: 6,
                price_tick_atoms: 100,
                price_scale: 2,
                qty_step_atoms: 1_000,
                qty_scale: 6,
                contract_size_atoms: 1,
                contract_scale: 0,
                initial_margin_bps: 1_000,
            },
        )
        .unwrap()
    };
    vec![
        mk(0, "BTCUSDT", Product::Spot, 1),
        mk(1, "BTCUSDT-PERP", Product::LinearPerp, 1),
        mk(2, "BTCUSDZ26", Product::InverseFutures, 1),
        mk(3, "BTC-25DEC26-90000-C", Product::Options, 1),
    ]
}

fn uta_symbols_okx() -> Vec<UtaSymbolBinding> {
    let mk = |id: u32, sym: &str, product: Product| {
        UtaSymbolBinding::new(
            id,
            sym,
            OrsSymbolSpec {
                symbol_id: id,
                instrument_id: u64::from(id) + 200,
                product,
                wallet_id: 1,
                base_asset_id: 1,
                base_scale: 6,
                quote_asset_id: 2,
                quote_scale: 6,
                settlement_asset_id: 2,
                settlement_scale: 6,
                price_tick_atoms: 100,
                price_scale: 2,
                qty_step_atoms: 1_000,
                qty_scale: 6,
                contract_size_atoms: 1,
                contract_scale: 0,
                initial_margin_bps: 1_000,
            },
        )
        .unwrap()
    };
    vec![
        mk(0, "BTC-USDT", Product::Spot),
        mk(1, "BTC-USDT-SWAP", Product::LinearPerp),
        mk(2, "BTC-USD-261225", Product::InverseFutures),
        mk(3, "BTC-USD-261225-90000-C", Product::Options),
    ]
}

fn build_om(venue: SpotVenue, bindings: &[UtaSymbolBinding]) -> Arc<OrderManager<true>> {
    let domain = OrsAccountDomainConfig {
        account_ref: 1,
        risk_domain_id: [0x42; 16],
        venue: venue.venue_id(),
        environment: Environment::Test,
        exchange_account_id: 77_001,
        epoch: 1,
    };
    let specs: Vec<OrsSymbolSpec> = bindings.iter().map(|b| b.spec).collect();
    let mut cfg = OrderManagerConfig::single_account_default(domain, specs);
    cfg.slots_per_strat = 64;
    let om = Arc::new(OrderManager::new(cfg).unwrap());
    // Seed ample USDT & BTC balances in unified wallet 1
    om.wallet_ledger()
        .set_balance(1, AssetKey::new(1, 2), 6, 100_000_000_000_000, 0)
        .unwrap();
    om.wallet_ledger()
        .set_balance(1, AssetKey::new(1, 1), 6, 1_000_000_000, 0)
        .unwrap();
    om.dop_state().configure_strategy(0, &[1]).unwrap();
    om.dop_state().login(0, 1).unwrap();
    om
}

#[test]
fn bybit_ws_trade_pool_supports_16_inflight_9_8_ewma_hysteresis_and_fast_fill_amendment() {
    let symbols = uta_symbols_bybit();
    let om = build_om(SpotVenue::Bybit, &symbols);
    let limiter = Arc::new(SharedRateLimiter::new());

    let cfg = UtaSessionConfig {
        venue: SpotVenue::Bybit,
        environment: Environment::Test,
        account_ref: 1,
        exchange_uid: 77_001,
        risk_domain_id: [0x42; 16],
        epoch: 1,
        credentials: OwnedCredentials::bybit("BYBIT_UTA_KEY", "bybit_uta_secret").unwrap(),
        transport_mode: UtaTransportMode::WebSocketTrade,
        pool: vec![
            UtaConnPoolEntry::new(Ipv4Addr::new(10, 0, 1, 1), 1).with_colo(true),
            UtaConnPoolEntry::new(Ipv4Addr::new(10, 0, 1, 2), 1),
        ],
        router_config: RouterConfig::default(),
        limits: Limits::default(),
        symbols,
        assets: vec![
            UtaAssetBinding::new("BTC", AssetKey::new(1, 1), 6).unwrap(),
            UtaAssetBinding::new("USDT", AssetKey::new(1, 2), 6).unwrap(),
        ],
        rate_limiter: Arc::clone(&limiter),
        rate_overrides: AccountRateOverrides::default(),
        ip_order_weight: 1,
        okx_ws_auto_subscribe_private: false,
        private_stream_baton: None,
        rfq_hook: None,
    };
    let session = BybitUtaSession::new(Arc::clone(&om), cfg).unwrap();

    // Install two WS trade connections (`already_authenticated = true` on slot 0, unauthenticated handshake on slot 1)
    let (conn0, mut srv0) = loopback_pair();
    let (conn1, mut srv1) = loopback_pair();
    session
        .install_order_connection(0, conn0, true, 1_700_000_000_000, 1_000)
        .unwrap();
    session
        .install_order_connection(1, conn1, false, 1_700_000_000_000, 1_000)
        .unwrap();

    // Slot 1 must be `ready == false` until auth ack arrives; every route has `product_mask == 0`!
    let mut snap = [OrderRoute::default(); 64];
    let snap_len = session.published_routes().snapshot(&mut snap);
    assert_eq!(snap_len, 2);
    assert_eq!(snap[0].product_mask, 0);
    assert!(snap[0].ready);
    assert_eq!(snap[1].product_mask, 0);
    assert!(!snap[1].ready);

    // Complete WS auth on slot 1 via `apply_order_ws_payload`
    let mut reports = Vec::new();
    session
        .apply_order_ws_payload(
            1,
            br#"{"op":"auth","success":true,"ret_msg":""}"#,
            2_000,
            1_700_000_000_001,
            &mut reports,
        )
        .unwrap();
    let _ = session.published_routes().snapshot(&mut snap);
    assert!(snap[1].ready);

    // Verify 9/8 EWMA hysteresis across the shared multi-product pool:
    // Give conn 0 score = 100us, conn 1 score = 95us (`ema_min_samples = 8`, 100 * 8 = 800 <= 95 * 9 = 855 => stays sticky on conn 0!)
    for _ in 0..8 {
        session.record_route_rtt_sample_us(0, 100).unwrap();
        session.record_route_rtt_sample_us(1, 95).unwrap();
    }
    assert_eq!(
        session
            .published_routes()
            .pick(1, Some(Product::Options), Some(0), false)
            .unwrap()
            .connection_id,
        0
    );
    // Now drop conn 1 score to 40us so EWMA falls well below 8/9 of 100us => switches to conn 1!
    for _ in 0..16 {
        session.record_route_rtt_sample_us(1, 40).unwrap();
    }
    assert_eq!(
        session
            .published_routes()
            .pick(1, Some(Product::Spot), Some(0), false)
            .unwrap()
            .connection_id,
        1
    );

    // Dispatch 16 concurrent in-flight orders across Spot, LinearPerp, InverseFutures, and Options on conn 1!
    let mut ors_ids = Vec::new();
    for i in 0..16u64 {
        let seq = i + 1;
        let sym_id = (i % 4) as u32;
        let req = OrsNewOrderRequest {
            strat_id: 0,
            account_ref: 1,
            msg_seqnum: seq,
            order_seqnum: seq,
            symbol_id: sym_id,
            side: Side::Buy,
            price_ticks: 50_000 - (i as i64 * 10),
            qty_lots: 10,
            tif: TimeInForce::Rpi,
            post_only: false,
            reduce_only: false,
            check_stp: true,
            dry_run: false,
            now_mono_ns: 10_000 + i * 100,
        };
        let params = if sym_id == 0 {
            UtaOrderParams::cash_spot_limit(TimeInForce::Rpi)
        } else {
            UtaOrderParams::cross_derivative_limit(TimeInForce::Rpi, false)
        };
        let out = session
            .submit_new_order(&req, params, 1_700_000_000_100 + i)
            .unwrap();
        match out {
            UtaSubmitOutcome::Dispatched {
                ors_order_id,
                conn_id,
                ..
            } => {
                assert_eq!(conn_id, 1);
                ors_ids.push(ors_order_id);
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    // Once conn 1 hits MAX_WS_INFLIGHT (16), the 17th order seamlessly spills over to conn 0!
    let req17 = OrsNewOrderRequest {
        strat_id: 0,
        account_ref: 1,
        msg_seqnum: 17,
        order_seqnum: 17,
        symbol_id: 1, // LinearPerp
        side: Side::Buy,
        price_ticks: 49_000,
        qty_lots: 10,
        tif: TimeInForce::Gtc,
        post_only: false,
        reduce_only: false,
        check_stp: true,
        dry_run: false,
        now_mono_ns: 20_000,
    };
    let out17 = session
        .submit_new_order(
            &req17,
            UtaOrderParams::cross_derivative_limit(TimeInForce::Gtc, false),
            1_700_000_000_200,
        )
        .unwrap();
    let (oid17, reply17) = match out17 {
        UtaSubmitOutcome::Dispatched {
            ors_order_id,
            conn_id,
            reply_id,
            ..
        } => {
            assert_eq!(conn_id, 0);
            (ors_order_id, reply_id)
        }
        other => panic!("unexpected 17th outcome: {other:?}"),
    };

    // Confirm real WS bytes were written to both kernel sockets
    let mut drain = [0u8; 4096];
    assert!(read_loopback(&mut srv1, &mut drain) > 0);
    assert!(read_loopback(&mut srv0, &mut drain) > 0);

    // Test `FrontAck` -> `Order` projection (`ProjectedFill`) -> `execution.fast` (no fee) -> full `execution` (`amendment = true`, zero double position!)
    let ack_frame = format!(
        r#"{{"reqId":"{reply17}","retCode":0,"retMsg":"OK","op":"order.create","data":{{"orderId":"bybit-v-17","orderLinkId":""}}}}"#
    );
    reports.clear();
    session
        .apply_order_ws_payload(
            0,
            ack_frame.as_bytes(),
            25_000,
            1_700_000_000_205,
            &mut reports,
        )
        .unwrap();
    assert!(reports.iter().any(|r| matches!(r, OrsReport::FrontAck(_))));

    // Private `order` push with `cumExecQty = "0.004"` (4 lots out of 10) -> emits `Ack` + `ProjectedFill` (`projected == true`)
    let order_push = br#"{
        "topic":"order",
        "data":[{"category":"linear","symbol":"BTCUSDT-PERP","orderId":"bybit-v-17","orderLinkId":"","orderStatus":"PartiallyFilled","timeInForce":"GTC","side":"Buy","price":"49000","qty":"0.01","cumExecQty":"0.004"}]
    }"#;
    reports.clear();
    session
        .apply_private_ws_payload(order_push, 30_000, 1_700_000_000_210, &mut reports)
        .unwrap();
    assert!(reports.iter().any(|r| matches!(r, OrsReport::Ack(_))));
    assert!(
        reports
            .iter()
            .any(|r| matches!(r, OrsReport::Fill(f) if f.projected && !f.amendment))
    );

    // Now `execution.fast` arrives for `execId = "exec-fast-17"` (`0.004`, `fee: None`)
    let pos_before = om.wallet_ledger().position_lots(1, 1).unwrap_or(0);
    assert_eq!(pos_before, 0);
    let fast_push = br#"{
        "topic":"execution.fast",
        "data":[{"category":"linear","symbol":"BTCUSDT-PERP","execId":"exec-fast-17","orderId":"bybit-v-17","orderLinkId":"","side":"Buy","execPrice":"49000","execQty":"0.004","isMaker":true,"execTime":"1700000000211"}]
    }"#;
    reports.clear();
    session
        .apply_private_ws_payload(fast_push, 31_000, 1_700_000_000_211, &mut reports)
        .unwrap();
    assert_eq!(reports.len(), 1);
    match &reports[0] {
        OrsReport::Fill(f) => {
            assert!(!f.projected);
            assert!(!f.amendment);
            assert!(f.fee.is_none());
            assert_eq!(f.ors_order_id, oid17);
        }
        other => panic!("expected fast Fill, got {other:?}"),
    }
    let pos_after_fast = om.wallet_ledger().position_lots(1, 1);
    assert_eq!(pos_after_fast, Some(4));

    // Now full `execution` arrives for the SAME `execId = "exec-fast-17"` with maker rebate (`execFee = "-0.05"` USDT => +50_000 atoms cashflow):
    // MUST emit `amendment == true` and MUST NOT double-update derivative position (`pos` stays 4, NOT 8)!
    let full_push = br#"{
        "topic":"execution",
        "data":[{"category":"linear","symbol":"BTCUSDT-PERP","execId":"exec-fast-17","orderId":"bybit-v-17","orderLinkId":"","execType":"Trade","side":"Buy","execPrice":"49000","execQty":"0.004","execFee":"-0.05","feeCurrency":"USDT","isMaker":true,"execTime":"1700000000212"}]
    }"#;
    reports.clear();
    session
        .apply_private_ws_payload(full_push, 32_000, 1_700_000_000_212, &mut reports)
        .unwrap();
    assert_eq!(reports.len(), 1);
    match &reports[0] {
        OrsReport::Fill(f) => {
            assert!(f.amendment);
            assert!(!f.projected);
            assert!(f.fee.is_some());
            assert_eq!(f.fee.as_ref().unwrap().signed_atoms, 50_000);
        }
        other => panic!("expected amendment Fill, got {other:?}"),
    }
    assert_eq!(
        om.wallet_ledger().position_lots(1, 1),
        Some(4),
        "full execution amendment must NEVER double-apply position lots"
    );
}

#[test]
fn single_pick_rate_limiter_never_retries_another_ip_and_isolates_cancels_plus_dry_run() {
    let symbols = uta_symbols_okx();
    let om = build_om(SpotVenue::Okx, &symbols);
    let limiter = Arc::new(SharedRateLimiter::new());

    // Cap per-symbol 2s order window to 2 orders while leaving cancel window at 60/2s!
    let overrides = AccountRateOverrides {
        per_symbol_order_limit: Some(2),
        ..AccountRateOverrides::default()
    };
    let cfg = UtaSessionConfig {
        venue: SpotVenue::Okx,
        environment: Environment::Test,
        account_ref: 1,
        exchange_uid: 88_001,
        risk_domain_id: [0x42; 16],
        epoch: 1,
        credentials: OwnedCredentials::okx("OKX_KEY", "okx_secret", "okx_pass").unwrap(),
        transport_mode: UtaTransportMode::PooledHttps,
        pool: vec![
            UtaConnPoolEntry::new(Ipv4Addr::new(10, 0, 2, 1), 2),
            UtaConnPoolEntry::new(Ipv4Addr::new(10, 0, 2, 2), 2),
        ],
        router_config: RouterConfig::default(),
        limits: Limits::default(),
        symbols,
        assets: vec![
            UtaAssetBinding::new("BTC", AssetKey::new(1, 1), 6).unwrap(),
            UtaAssetBinding::new("USDT", AssetKey::new(1, 2), 6).unwrap(),
        ],
        rate_limiter: Arc::clone(&limiter),
        rate_overrides: overrides,
        ip_order_weight: 1,
        okx_ws_auto_subscribe_private: false,
        private_stream_baton: None,
        rfq_hook: None,
    };
    let session = OkxUtaSession::new(Arc::clone(&om), cfg).unwrap();

    let mut servers = Vec::new();
    for cid in 0..4u16 {
        let (conn, srv) = loopback_pair();
        session
            .install_order_connection(cid, conn, true, 1_700_000_000_000, 1_000)
            .unwrap();
        servers.push(srv);
    }

    // 1. `dry_run: true` executes admission + wire encoding with ZERO socket bytes and ZERO quota spend!
    for i in 1..=5u64 {
        let dry = OrsNewOrderRequest {
            strat_id: 0,
            account_ref: 1,
            msg_seqnum: i,
            order_seqnum: i,
            symbol_id: 1, // BTC-USDT-SWAP
            side: Side::Buy,
            price_ticks: 40_000,
            qty_lots: 5,
            tif: TimeInForce::Rpi,
            post_only: false,
            reduce_only: false,
            check_stp: true,
            dry_run: true,
            now_mono_ns: 10_000 + i,
        };
        let out = session
            .submit_new_order(
                &dry,
                UtaOrderParams::cross_derivative_limit(TimeInForce::Rpi, false),
                1_700_000_000_010,
            )
            .unwrap();
        assert!(matches!(out, UtaSubmitOutcome::DryRun { wire_len, .. } if wire_len > 100));
    }
    let mut tmp = [0u8; 512];
    for srv in &mut servers {
        assert!(
            srv.read(&mut tmp).is_err(),
            "dry_run must never write to any socket"
        );
    }

    // 2. Place 2 live PooledHttps orders on `BTC-USDT-SWAP` (each claims an Exclusive HTTPS slot: conn 0 and conn 1)
    let mut live_oids = Vec::new();
    for idx in 0..2u64 {
        let seq = 6 + idx;
        let req = OrsNewOrderRequest {
            strat_id: 0,
            account_ref: 1,
            msg_seqnum: seq,
            order_seqnum: seq,
            symbol_id: 1,
            side: Side::Buy,
            price_ticks: 40_000 - (idx as i64 * 10),
            qty_lots: 5,
            tif: TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            check_stp: true,
            dry_run: false,
            now_mono_ns: 50_000 + idx * 100,
        };
        match session
            .submit_new_order(
                &req,
                UtaOrderParams::cross_derivative_limit(TimeInForce::Gtc, false),
                1_700_000_000_050,
            )
            .unwrap()
        {
            UtaSubmitOutcome::Dispatched {
                ors_order_id,
                conn_id,
                ..
            } => {
                assert_eq!(conn_id, idx as u16);
                live_oids.push(ors_order_id);
            }
            other => panic!("expected Dispatched, got {other:?}"),
        }
    }

    // 3. The 3rd order on `BTC-USDT-SWAP` picks conn 2 (on healthy second IP `10.0.2.2`), hits the 2/2s symbol cap,
    //    and NACKs IMMEDIATELY without retrying conn 3!
    let req3 = OrsNewOrderRequest {
        strat_id: 0,
        account_ref: 1,
        msg_seqnum: 8,
        order_seqnum: 8,
        symbol_id: 1,
        side: Side::Buy,
        price_ticks: 39_900,
        qty_lots: 5,
        tif: TimeInForce::Gtc,
        post_only: false,
        reduce_only: false,
        check_stp: true,
        dry_run: false,
        now_mono_ns: 51_000,
    };
    let err = session
        .submit_new_order(
            &req3,
            UtaOrderParams::cross_derivative_limit(TimeInForce::Gtc, false),
            1_700_000_000_051,
        )
        .unwrap_err();
    assert!(matches!(err, UtaSubmitRefusal::RateLimited { .. }));
    assert!(
        servers[2].read(&mut tmp).is_err() && servers[3].read(&mut tmp).is_err(),
        "rate-limited request must never transmit or retry another connection"
    );

    // Complete HTTPS reply on conn 0 (`X-Bapi-Limit` style telemetry + orderId ack) so order 0 gets FrontAck
    let mut reports = Vec::new();
    let ack_body = r#"{"code":"0","msg":"","data":[{"ordId":"okx-ord-10","sCode":"0","sMsg":""}]}"#;
    let http_ack = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nX-Bapi-Limit: 100\r\nX-Bapi-Limit-Status: 15\r\n\r\n{ack_body}",
        ack_body.len()
    );
    let poll_rep = session
        .apply_order_https_bytes(
            0,
            http_ack.as_bytes(),
            60_000,
            1_700_000_000_060,
            &mut reports,
        )
        .unwrap();
    assert_eq!(poll_rep.rate_limit_events.len(), 1); // 85/100 >= 80% warning rising edge!
    assert!(reports.iter().any(|r| matches!(r, OrsReport::FrontAck(_))));

    // Confirm order 0 Open via private push, then verify CANCEL succeeds even though ORDER window is 100% exhausted!
    session
        .apply_private_ws_payload(
            br#"{"arg":{"channel":"orders","instType":"ANY"},"data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","ordId":"okx-ord-10","clOrdId":"","state":"live","ordType":"limit","side":"buy","px":"40000","sz":"0.005","accFillSz":"0"}]}"#,
            61_000,
            1_700_000_000_061,
            &mut reports,
        )
        .unwrap();

    let cxl = OrsCancelRequest {
        strat_id: 0,
        account_ref: 1,
        msg_seqnum: 9,
        ors_order_id: live_oids[0],
        now_mono_ns: 62_000,
    };
    let cxl_out = session
        .submit_cancel_order(&cxl, 1_700_000_000_062)
        .unwrap();
    assert!(matches!(cxl_out, UtaSubmitOutcome::Dispatched { .. }));

    // 4. HTTP 429 on conn 1 (`10.0.2.1`) cools down IP `10.0.2.1` (`Retry-After: 2`), while warmer stays isolated!
    let rl_body = r#"{"code":"50011","msg":"rl"}"#;
    let http_429 = format!(
        "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 2\r\nContent-Length: {}\r\n\r\n{rl_body}",
        rl_body.len()
    );
    session
        .apply_order_https_bytes(
            1,
            http_429.as_bytes(),
            70_000,
            1_700_000_000_070,
            &mut reports,
        )
        .unwrap();

    let mut warmer =
        UtaHotPathWarmer::new(UtaWarmupConfig::new(SpotVenue::Okx, Environment::Test)).unwrap();
    let wrep = warmer.warm_once(80_000, 1_700_000_000_080).unwrap();
    assert_eq!(wrep.http_status, 200);
    assert!(wrep.ws_frame_bytes > 0);
}

#[test]
fn okx_64008_maintenance_notice_switches_connections_gracefully_with_zero_dropped_open_orders() {
    let symbols = uta_symbols_okx();
    let om = build_om(SpotVenue::Okx, &symbols);
    let limiter = Arc::new(SharedRateLimiter::new());

    let baton0 = Arc::new(DialBaton::new());
    let baton1 = Arc::new(DialBaton::new());

    let cfg = UtaSessionConfig {
        venue: SpotVenue::Okx,
        environment: Environment::Production,
        account_ref: 1,
        exchange_uid: 99_500,
        risk_domain_id: [0x55; 16],
        epoch: 1,
        credentials: OwnedCredentials::okx("OKX_MAINT_K", "okx_maint_s", "okx_maint_p").unwrap(),
        transport_mode: UtaTransportMode::WebSocketTrade,
        pool: vec![
            UtaConnPoolEntry::new(Ipv4Addr::new(10, 10, 0, 1), 1)
                .with_batons(vec![Arc::clone(&baton0)]),
            UtaConnPoolEntry::new(Ipv4Addr::new(10, 10, 0, 2), 1)
                .with_batons(vec![Arc::clone(&baton1)]),
        ],
        router_config: RouterConfig::default(),
        limits: Limits::default(),
        symbols,
        assets: vec![
            UtaAssetBinding::new("BTC", AssetKey::new(1, 1), 6).unwrap(),
            UtaAssetBinding::new("USDT", AssetKey::new(1, 2), 6).unwrap(),
        ],
        rate_limiter: limiter,
        rate_overrides: AccountRateOverrides::default(),
        ip_order_weight: 1,
        okx_ws_auto_subscribe_private: false,
        private_stream_baton: None,
        rfq_hook: None,
    };
    let session = OkxUtaSession::new(Arc::clone(&om), cfg).unwrap();

    let (conn0, _srv0) = loopback_pair();
    let (conn1, _srv1) = loopback_pair();
    session
        .install_order_connection(0, conn0, true, 1_700_000_000_000, 1_000)
        .unwrap();
    session
        .install_order_connection(1, conn1, true, 1_700_000_000_000, 1_000)
        .unwrap();
    // Make conn 0 preferred initially (`ema_min_samples = 8`)
    for _ in 0..8 {
        session.record_route_rtt_sample_us(0, 30).unwrap();
        session.record_route_rtt_sample_us(1, 120).unwrap();
    }

    // Place 2 orders on conn 0:
    //  - Order 1: already acknowledged (`Open`) resting on the book
    //  - Order 2: still in-flight (`PendingNew`) waiting for its WS `op: "order"` reply on conn 0!
    let req1 = OrsNewOrderRequest {
        strat_id: 0,
        account_ref: 1,
        msg_seqnum: 1,
        order_seqnum: 1,
        symbol_id: 1,
        side: Side::Buy,
        price_ticks: 45_000,
        qty_lots: 10,
        tif: TimeInForce::Gtc,
        post_only: true,
        reduce_only: false,
        check_stp: true,
        dry_run: false,
        now_mono_ns: 10_000,
    };
    let (oid1, rep_id1) = match session
        .submit_new_order(
            &req1,
            UtaOrderParams::cross_derivative_limit(TimeInForce::PostOnly, false),
            1_700_000_000_010,
        )
        .unwrap()
    {
        UtaSubmitOutcome::Dispatched {
            ors_order_id,
            conn_id,
            reply_id,
            ..
        } => {
            assert_eq!(conn_id, 0);
            (ors_order_id, reply_id)
        }
        other => panic!("unexpected: {other:?}"),
    };

    let mut reports = Vec::new();
    let ack1 = format!(
        r#"{{"id":"{rep_id1}","op":"order","code":"0","msg":"","data":[{{"ordId":"okx-live-1","clOrdId":"","sCode":"0","sMsg":""}}]}}"#
    );
    session
        .apply_order_ws_payload(0, ack1.as_bytes(), 12_000, 1_700_000_000_012, &mut reports)
        .unwrap();
    session
        .apply_private_ws_payload(
            br#"{"arg":{"channel":"orders","instType":"ANY"},"data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","ordId":"okx-live-1","clOrdId":"","state":"live","ordType":"post_only","side":"buy","px":"45000","sz":"0.01","accFillSz":"0"}]}"#,
            13_000,
            1_700_000_000_013,
            &mut reports,
        )
        .unwrap();

    // Order 2 is sent on conn 0 and stays in-flight when OKX sends `{"event":"notice","code":"64008"}`!
    let req2 = OrsNewOrderRequest {
        strat_id: 0,
        account_ref: 1,
        msg_seqnum: 2,
        order_seqnum: 2,
        symbol_id: 3, // Options
        side: Side::Buy,
        price_ticks: 1_200,
        qty_lots: 5,
        tif: TimeInForce::Rpi,
        post_only: false,
        reduce_only: false,
        check_stp: true,
        dry_run: false,
        now_mono_ns: 15_000,
    };
    let (oid2, rep_id2) = match session
        .submit_new_order(
            &req2,
            UtaOrderParams::cross_derivative_limit(TimeInForce::Rpi, false),
            1_700_000_000_015,
        )
        .unwrap()
    {
        UtaSubmitOutcome::Dispatched {
            ors_order_id,
            conn_id,
            reply_id,
            ..
        } => {
            assert_eq!(conn_id, 0);
            (ors_order_id, reply_id)
        }
        other => panic!("unexpected: {other:?}"),
    };

    // Inject OKX server maintenance `notice` (`64008`) on conn 0!
    reports.clear();
    let notice = br#"{"event":"notice","msg":"Upgrade soon, please switch connection","code":"64008","connId":"old-0"}"#;
    let prep = session
        .apply_order_ws_payload(0, notice, 20_000, 1_700_000_000_020, &mut reports)
        .unwrap();
    assert_eq!(prep.maintenance_drains_started, 1);
    assert!(
        reports.is_empty(),
        "maintenance notice must emit ZERO spurious cancels or rejects"
    );

    // Verify:
    // 1. Conn 0 is immediately marked `ready == false` in `PublishedRoutes`!
    // 2. `baton0` was requested (`DialState::Wanted`) so `Dialer` dials replacement in parallel!
    // 3. Open order `oid1` and in-flight order `oid2` are BOTH still 100% live (`0` dropped open orders)!
    let mut routes_during_drain = [OrderRoute::default(); 64];
    let _ = session
        .published_routes()
        .snapshot(&mut routes_during_drain);
    assert!(!routes_during_drain[0].ready);
    assert!(routes_during_drain[1].ready);
    assert_eq!(baton0.state(), DialState::Wanted);
    assert!(om.lock_order(oid1).is_some());
    assert!(om.lock_order(oid2).is_some());

    // 4. Any new order (`req3`) placed during drain routes seamlessly to healthy `conn_id == 1`!
    let req3 = OrsNewOrderRequest {
        strat_id: 0,
        account_ref: 1,
        msg_seqnum: 3,
        order_seqnum: 3,
        symbol_id: 0, // Spot
        side: Side::Buy,
        price_ticks: 44_000,
        qty_lots: 5,
        tif: TimeInForce::Gtc,
        post_only: false,
        reduce_only: false,
        check_stp: true,
        dry_run: false,
        now_mono_ns: 22_000,
    };
    match session
        .submit_new_order(
            &req3,
            UtaOrderParams::cash_spot_limit(TimeInForce::Gtc),
            1_700_000_000_022,
        )
        .unwrap()
    {
        UtaSubmitOutcome::Dispatched { conn_id, .. } => assert_eq!(conn_id, 1),
        other => panic!("expected conn 1 dispatch during drain, got {other:?}"),
    }

    // 5. Simulate `Dialer` finishing replacement dial on `baton0` (`Wanted -> Dialing -> Ready`),
    //    AND drain the in-flight reply for `oid2` on retiring conn 0!
    assert!(baton0.claim(25_000));
    let (replacement_conn0, mut srv_replacement0) = loopback_pair();
    let peer_addr = srv_replacement0.local_addr().unwrap();
    assert!(baton0.deliver_ready(DialedLink::new(replacement_conn0, peer_addr, 25)));
    assert_eq!(baton0.state(), DialState::Ready);

    let ack2 = format!(
        r#"{{"id":"{rep_id2}","op":"order","code":"0","msg":"","data":[{{"ordId":"okx-live-2","clOrdId":"","sCode":"0","sMsg":""}}]}}"#
    );
    session
        .apply_order_ws_payload(0, ack2.as_bytes(), 26_000, 1_700_000_000_026, &mut reports)
        .unwrap();
    assert!(
        reports
            .iter()
            .any(|r| matches!(r, OrsReport::FrontAck(f) if f.ors_order_id == oid2))
    );

    // 6. Next `poll_resp` retires drained old conn 0, installs `replacement_conn0` from `baton0`,
    //    and sends its WS `op: "login"` frame!
    let poll_out = session
        .poll_resp(Instant::now(), 27_000, 1_700_000_000_027, &mut reports)
        .unwrap();
    assert_eq!(poll_out.replacements_installed, 1);

    // Replacement server receives login frame; send back login ack over the real replacement socket!
    let mut login_rx = [0u8; 1024];
    let n_login = read_loopback(&mut srv_replacement0, &mut login_rx);
    assert!(n_login > 0);
    let srv_login_ack = ws_server_frame(Opcode::Text, br#"{"event":"login","code":"0","msg":""}"#);
    use std::io::Write as _;
    srv_replacement0.write_all(&srv_login_ack).unwrap();

    let mut routes_after = [OrderRoute::default(); 64];
    let deadline = Instant::now() + Duration::from_millis(200);
    loop {
        session
            .poll_resp(Instant::now(), 28_000, 1_700_000_000_028, &mut reports)
            .unwrap();
        let _ = session.published_routes().snapshot(&mut routes_after);
        if routes_after[0].ready {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for replacement WS login ack"
        );
        std::thread::yield_now();
    }

    // Conn 0 is now re-authenticated and `ready == true` again, with 100% of open orders preserved!
    assert!(routes_after[0].ready);
    assert!(routes_after[1].ready);
    assert_ne!(routes_after[0].connection_id, OrderRoute::NO_CONNECTION);
    assert!(om.lock_order(oid1).is_some());
    assert!(om.lock_order(oid2).is_some());
}

fn try_decode_masked_ws_client_frame(buf: &[u8]) -> Option<(Vec<u8>, usize)> {
    if buf.len() < 6 {
        return None;
    }
    let masked = (buf[1] & 0x80) != 0;
    assert!(masked, "client WS frame must be masked per RFC 6455");
    let len7 = usize::from(buf[1] & 0x7f);
    let (payload_len, hdr_len) = if len7 < 126 {
        (len7, 2usize)
    } else if len7 == 126 {
        if buf.len() < 8 {
            return None;
        }
        (usize::from(u16::from_be_bytes([buf[2], buf[3]])), 4usize)
    } else {
        if buf.len() < 14 {
            return None;
        }
        let mut b = [0u8; 8];
        b.copy_from_slice(&buf[2..10]);
        (u64::from_be_bytes(b) as usize, 10usize)
    };
    let total = hdr_len + 4 + payload_len;
    if buf.len() < total {
        return None;
    }
    let mask = &buf[hdr_len..hdr_len + 4];
    let mut out = Vec::with_capacity(payload_len);
    for (idx, &byte) in buf[hdr_len + 4..total].iter().enumerate() {
        out.push(byte ^ mask[idx & 3]);
    }
    Some((out, total))
}

fn extract_json_str_field<'a>(json: &'a str, key: &str) -> Option<&'a str> {
    let needle = format!("\"{key}\":\"");
    let start = json.find(&needle)? + needle.len();
    let end = start + json[start..].find('"')?;
    Some(&json[start..end])
}

#[test]
fn concurrent_req_and_resp_threads_drive_shared_arc_uta_session_safely() {
    use std::{
        io::Write as _,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    // Part A: Concurrent `REQ` + `RESP` + loopback WS venue threads over `Arc<BybitUtaSession>` (`Shared` 16-in-flight)
    let symbols = uta_symbols_bybit();
    let om = build_om(SpotVenue::Bybit, &symbols);
    let limiter = Arc::new(SharedRateLimiter::new());
    let cfg = UtaSessionConfig {
        venue: SpotVenue::Bybit,
        environment: Environment::Test,
        account_ref: 1,
        exchange_uid: 77_001,
        risk_domain_id: [0x42; 16],
        epoch: 1,
        credentials: OwnedCredentials::bybit("BYBIT_MT_KEY", "bybit_mt_secret").unwrap(),
        transport_mode: UtaTransportMode::WebSocketTrade,
        pool: vec![
            UtaConnPoolEntry::new(Ipv4Addr::new(10, 20, 0, 1), 1).with_colo(true),
            UtaConnPoolEntry::new(Ipv4Addr::new(10, 20, 0, 2), 1),
        ],
        router_config: RouterConfig::default(),
        limits: Limits::default(),
        symbols,
        assets: vec![
            UtaAssetBinding::new("BTC", AssetKey::new(1, 1), 6).unwrap(),
            UtaAssetBinding::new("USDT", AssetKey::new(1, 2), 6).unwrap(),
        ],
        rate_limiter: Arc::clone(&limiter),
        rate_overrides: AccountRateOverrides::default(),
        ip_order_weight: 1,
        okx_ws_auto_subscribe_private: false,
        private_stream_baton: None,
        rfq_hook: None,
    };
    let session = Arc::new(BybitUtaSession::new(Arc::clone(&om), cfg).unwrap());

    let (conn0, srv0) = loopback_pair();
    let (conn1, srv1) = loopback_pair();
    let (priv_conn, mut priv_srv) = loopback_pair();
    session
        .install_order_connection(0, conn0, true, 1_700_000_000_000, 1_000)
        .unwrap();
    session
        .install_order_connection(1, conn1, true, 1_700_000_000_000, 1_000)
        .unwrap();
    session
        .install_private_connection(priv_conn, 1_700_000_000_000, 1_000)
        .unwrap();

    // Drain initial private WS auth frame and send back Bybit auth + subscribe acks
    let mut priv_rx = [0u8; 1024];
    assert!(read_loopback(&mut priv_srv, &mut priv_rx) > 0);
    priv_srv
        .write_all(&ws_server_frame(
            Opcode::Text,
            br#"{"op":"auth","success":true,"ret_msg":""}"#,
        ))
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let front_acks = Arc::new(AtomicUsize::new(0));
    let open_acks = Arc::new(AtomicUsize::new(0));
    let cancel_terminals = Arc::new(AtomicUsize::new(0));

    // Spawn mock Bybit WS trade + private server thread handling `srv0`, `srv1`, and `priv_srv`
    let stop_srv = Arc::clone(&stop);
    let srv_handle = std::thread::spawn(move || {
        let mut s0 = srv0;
        let mut s1 = srv1;
        let mut p_srv = priv_srv;
        let mut acc0 = Vec::new();
        let mut acc1 = Vec::new();
        let mut scratch = [0u8; 4096];

        while !stop_srv.load(Ordering::Acquire) {
            let mut progressed = false;
            for (sock, acc) in [(&mut s0, &mut acc0), (&mut s1, &mut acc1)] {
                if let Ok(n) = sock.read(&mut scratch)
                    && n > 0
                {
                    acc.extend_from_slice(&scratch[..n]);
                    progressed = true;
                }
                while let Some((payload, used)) = try_decode_masked_ws_client_frame(acc) {
                    acc.drain(..used);
                    let txt = core::str::from_utf8(&payload).unwrap();
                    let Some(req_id) = extract_json_str_field(txt, "reqId") else {
                        continue;
                    };
                    let op = extract_json_str_field(txt, "op").unwrap_or("order.create");
                    if op == "order.create" {
                        let cl_id = extract_json_str_field(txt, "orderLinkId").unwrap_or("");
                        let sym = extract_json_str_field(txt, "symbol").unwrap_or("BTCUSDT");
                        let cat = extract_json_str_field(txt, "category").unwrap_or("spot");
                        let v_oid = format!("bybit-mt-{req_id}");
                        let reply = format!(
                            r#"{{"reqId":"{req_id}","retCode":0,"retMsg":"OK","op":"order.create","data":{{"orderId":"{v_oid}","orderLinkId":"{cl_id}"}}}}"#
                        );
                        sock.write_all(&ws_server_frame(Opcode::Text, reply.as_bytes()))
                            .unwrap();
                        let push_open = format!(
                            r#"{{"topic":"order","data":[{{"category":"{cat}","symbol":"{sym}","orderId":"{v_oid}","orderLinkId":"{cl_id}","orderStatus":"New","timeInForce":"GTC","side":"Buy","price":"48000","qty":"0.01","cumExecQty":"0"}}]}}"#
                        );
                        p_srv
                            .write_all(&ws_server_frame(Opcode::Text, push_open.as_bytes()))
                            .unwrap();
                    } else if op == "order.cancel" {
                        let cl_id = extract_json_str_field(txt, "orderLinkId").unwrap_or("");
                        let v_oid = extract_json_str_field(txt, "orderId").unwrap_or("");
                        let sym = extract_json_str_field(txt, "symbol").unwrap_or("BTCUSDT");
                        let cat = extract_json_str_field(txt, "category").unwrap_or("spot");
                        let reply = format!(
                            r#"{{"reqId":"{req_id}","retCode":0,"retMsg":"OK","op":"order.cancel","data":{{"orderId":"{v_oid}","orderLinkId":"{cl_id}"}}}}"#
                        );
                        sock.write_all(&ws_server_frame(Opcode::Text, reply.as_bytes()))
                            .unwrap();
                        let push_cxl = format!(
                            r#"{{"topic":"order","data":[{{"category":"{cat}","symbol":"{sym}","orderId":"{v_oid}","orderLinkId":"{cl_id}","orderStatus":"Cancelled","cancelType":"CancelByUser","timeInForce":"GTC","side":"Buy","price":"48000","qty":"0.01","cumExecQty":"0"}}]}}"#
                        );
                        p_srv
                            .write_all(&ws_server_frame(Opcode::Text, push_cxl.as_bytes()))
                            .unwrap();
                    }
                }
            }
            if !progressed {
                std::thread::yield_now();
            }
        }
    });

    // Spawn dedicated `RESP` thread driving `&session` concurrently with `REQ`
    let stop_resp = Arc::clone(&stop);
    let session_resp = Arc::clone(&session);
    let front_acks_resp = Arc::clone(&front_acks);
    let open_acks_resp = Arc::clone(&open_acks);
    let cancel_terminals_resp = Arc::clone(&cancel_terminals);
    let resp_handle = std::thread::spawn(move || {
        let mut reports = Vec::with_capacity(64);
        let mut tick = 0u64;
        while !stop_resp.load(Ordering::Acquire) {
            tick += 1;
            reports.clear();
            session_resp
                .poll_resp(
                    Instant::now(),
                    10_000 + tick * 100,
                    1_700_000_000_010 + tick,
                    &mut reports,
                )
                .unwrap();
            for rep in &reports {
                match rep {
                    OrsReport::FrontAck(_) => {
                        front_acks_resp.fetch_add(1, Ordering::Release);
                    }
                    OrsReport::Ack(_) => {
                        open_acks_resp.fetch_add(1, Ordering::Release);
                    }
                    OrsReport::Canceled(_) => {
                        cancel_terminals_resp.fetch_add(1, Ordering::Release);
                    }
                    _ => {}
                }
            }
            if reports.is_empty() {
                std::thread::yield_now();
            }
        }
    });

    // `REQ` thread dispatches 20 concurrent UTA orders across Spot, LinearPerp, InverseFutures, and Options via `&session`!
    const ORDER_COUNT: usize = 20;
    let mut placed_oids = Vec::with_capacity(ORDER_COUNT);
    for i in 0..ORDER_COUNT {
        let seq = (i + 1) as u64;
        let sym_id = (i % 4) as u32;
        let req = OrsNewOrderRequest {
            strat_id: 0,
            account_ref: 1,
            msg_seqnum: seq,
            order_seqnum: seq,
            symbol_id: sym_id,
            side: Side::Buy,
            price_ticks: 48_000,
            qty_lots: 10,
            tif: TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            check_stp: false,
            dry_run: false,
            now_mono_ns: 20_000 + seq * 50,
        };
        let params = if sym_id == 0 {
            UtaOrderParams::cash_spot_limit(TimeInForce::Gtc)
        } else {
            UtaOrderParams::cross_derivative_limit(TimeInForce::Gtc, false)
        };
        match session
            .submit_new_order(&req, params, 1_700_000_000_100 + seq)
            .unwrap()
        {
            UtaSubmitOutcome::Dispatched { ors_order_id, .. } => placed_oids.push(ors_order_id),
            other => panic!("expected Dispatched in concurrent REQ test, got {other:?}"),
        }
    }

    let deadline = Instant::now() + Duration::from_secs(2);
    while open_acks.load(Ordering::Acquire) < ORDER_COUNT {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for concurrent WS open acks (got {} front, {} open)",
            front_acks.load(Ordering::Acquire),
            open_acks.load(Ordering::Acquire)
        );
        std::thread::yield_now();
    }
    assert_eq!(front_acks.load(Ordering::Acquire), ORDER_COUNT);

    // Now `REQ` cancels all 20 open orders concurrently while `RESP` continues polling!
    for (idx, &oid) in placed_oids.iter().enumerate() {
        let cxl_seq = (ORDER_COUNT + 1 + idx) as u64;
        let cxl = OrsCancelRequest {
            strat_id: 0,
            account_ref: 1,
            msg_seqnum: cxl_seq,
            ors_order_id: oid,
            now_mono_ns: 90_000 + cxl_seq * 50,
        };
        match session
            .submit_cancel_order(&cxl, 1_700_000_000_500 + cxl_seq)
            .unwrap()
        {
            UtaSubmitOutcome::Dispatched { .. } => {}
            other => panic!("expected cancel Dispatched, got {other:?}"),
        }
    }

    while cancel_terminals.load(Ordering::Acquire) < ORDER_COUNT {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for concurrent WS cancel terminals (got {})",
            cancel_terminals.load(Ordering::Acquire)
        );
        std::thread::yield_now();
    }

    stop.store(true, Ordering::Release);
    resp_handle.join().unwrap();
    srv_handle.join().unwrap();

    // Part B: Concurrent Exclusive `PooledHttps` claiming on `Arc<OkxUtaSession>` across 4 sockets
    let okx_syms = uta_symbols_okx();
    let okx_om = build_om(SpotVenue::Okx, &okx_syms);
    let okx_cfg = UtaSessionConfig {
        venue: SpotVenue::Okx,
        environment: Environment::Test,
        account_ref: 1,
        exchange_uid: 88_777,
        risk_domain_id: [0x42; 16],
        epoch: 1,
        credentials: OwnedCredentials::okx("OKX_MT_KEY", "okx_mt_secret", "okx_mt_pass").unwrap(),
        transport_mode: UtaTransportMode::PooledHttps,
        pool: vec![UtaConnPoolEntry::new(Ipv4Addr::new(10, 30, 0, 1), 4)],
        router_config: RouterConfig::default(),
        limits: Limits::default(),
        symbols: okx_syms,
        assets: vec![
            UtaAssetBinding::new("BTC", AssetKey::new(1, 1), 6).unwrap(),
            UtaAssetBinding::new("USDT", AssetKey::new(1, 2), 6).unwrap(),
        ],
        rate_limiter: Arc::new(SharedRateLimiter::new()),
        rate_overrides: AccountRateOverrides::default(),
        ip_order_weight: 1,
        okx_ws_auto_subscribe_private: false,
        private_stream_baton: None,
        rfq_hook: None,
    };
    let okx_session = Arc::new(OkxUtaSession::new(Arc::clone(&okx_om), okx_cfg).unwrap());
    let mut http_srvs = Vec::new();
    for cid in 0..4u16 {
        let (conn, srv) = loopback_pair();
        okx_session
            .install_order_connection(cid, conn, true, 1_700_000_000_000, 1_000)
            .unwrap();
        http_srvs.push(srv);
    }

    // Dispatch 4 concurrent PooledHttps orders: atomic in-flight claiming must assign each to a distinct Exclusive slot `{0, 1, 2, 3}`!
    let mut claimed_conns = Vec::new();
    for idx in 0..4u64 {
        let seq = idx + 1;
        let req = OrsNewOrderRequest {
            strat_id: 0,
            account_ref: 1,
            msg_seqnum: seq,
            order_seqnum: seq,
            symbol_id: 1,
            side: Side::Buy,
            price_ticks: 42_000 - (idx as i64 * 10),
            qty_lots: 5,
            tif: TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            check_stp: true,
            dry_run: false,
            now_mono_ns: 10_000 + idx * 10,
        };
        match okx_session
            .submit_new_order(
                &req,
                UtaOrderParams::cross_derivative_limit(TimeInForce::Gtc, false),
                1_700_000_000_010 + idx,
            )
            .unwrap()
        {
            UtaSubmitOutcome::Dispatched { conn_id, .. } => claimed_conns.push(conn_id),
            other => panic!("expected Exclusive HTTPS Dispatched, got {other:?}"),
        }
    }
    claimed_conns.sort_unstable();
    assert_eq!(claimed_conns, vec![0, 1, 2, 3]);

    // A 5th order while all 4 Exclusive HTTPS links are in-flight must refuse cleanly with `NoReadyRoute`!
    let req5 = OrsNewOrderRequest {
        strat_id: 0,
        account_ref: 1,
        msg_seqnum: 5,
        order_seqnum: 5,
        symbol_id: 1,
        side: Side::Buy,
        price_ticks: 41_900,
        qty_lots: 5,
        tif: TimeInForce::Gtc,
        post_only: false,
        reduce_only: false,
        check_stp: true,
        dry_run: false,
        now_mono_ns: 11_000,
    };
    assert!(matches!(
        okx_session
            .submit_new_order(
                &req5,
                UtaOrderParams::cross_derivative_limit(TimeInForce::Gtc, false),
                1_700_000_000_015
            )
            .unwrap_err(),
        UtaSubmitRefusal::NoReadyRoute { .. }
    ));

    // Complete all 4 HTTP replies from server sockets while `RESP` thread polls `&okx_session`
    let okx_stop = Arc::new(AtomicBool::new(false));
    let okx_acks = Arc::new(AtomicUsize::new(0));
    let okx_stop_resp = Arc::clone(&okx_stop);
    let okx_session_resp = Arc::clone(&okx_session);
    let okx_acks_resp = Arc::clone(&okx_acks);
    let okx_resp_thread = std::thread::spawn(move || {
        let mut reps = Vec::new();
        while !okx_stop_resp.load(Ordering::Acquire) {
            reps.clear();
            okx_session_resp
                .poll_resp(Instant::now(), 20_000, 1_700_000_000_020, &mut reps)
                .unwrap();
            for r in &reps {
                if matches!(r, OrsReport::FrontAck(_)) {
                    okx_acks_resp.fetch_add(1, Ordering::Release);
                }
            }
            std::thread::yield_now();
        }
    });

    let mut req_buf = [0u8; 2048];
    for (cid, srv) in http_srvs.iter_mut().enumerate() {
        assert!(read_loopback(srv, &mut req_buf) > 0);
        let body = format!(
            r#"{{"code":"0","msg":"","data":[{{"ordId":"okx-http-{cid}","sCode":"0","sMsg":""}}]}}"#
        );
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        srv.write_all(resp.as_bytes()).unwrap();
    }

    let http_deadline = Instant::now() + Duration::from_secs(2);
    while okx_acks.load(Ordering::Acquire) < 4 {
        assert!(
            Instant::now() < http_deadline,
            "timed out waiting for concurrent PooledHttps FrontAcks"
        );
        std::thread::yield_now();
    }
    okx_stop.store(true, Ordering::Release);
    okx_resp_thread.join().unwrap();
}
