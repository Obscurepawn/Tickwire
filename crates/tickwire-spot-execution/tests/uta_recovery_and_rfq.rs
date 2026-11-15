#![forbid(unsafe_code)]

use serde_json::Value;
use std::{
    io::Read,
    net::{Ipv4Addr, TcpListener, TcpStream},
    sync::Arc,
    time::{Duration, Instant},
};
use tickwire_account::{
    InlineAscii, OrderManager, OrderManagerConfig, OrsAccountDomainConfig, OrsCancelRequest,
    OrsNewOrderRequest, OrsQueryOrigin, OrsQueryReconcileAction, OrsReport, OrsSymbolSpec,
    make_ors_order_id,
};
use tickwire_quota::shared::{AccountRateOverrides, SharedRateLimiter};
use tickwire_spot_execution::{
    BybitCategory, BybitUtaSession, Environment, OkxInstType, OkxUtaSession,
    RfqCancelQuotesRequest, RfqQuoteHook, RfqQuoteLeg, RfqQuoteRequest, SpotVenue, UtaAssetBinding,
    UtaBootstrapDecoder, UtaConnPoolEntry, UtaOrderParams, UtaRestRequestBuilder, UtaRfqQuoteBook,
    UtaSessionConfig, UtaSubmitOutcome, UtaSubmitRefusal, UtaSymbolBinding, UtaTransportMode,
    network::OwnedCredentials,
    rest_bootstrap::{
        bybit_open_orders_path, bybit_positions_path, okx_open_orders_path, okx_positions_path,
        wallet_balance_path,
    },
};
use tickwire_transport::{
    Connection, IoBudget, Limits, Security,
    order_router::RouterConfig,
    rest_querier::{RestReply, RestRequestBuilder as _, RestStatus, RestTask},
};
use tickwire_venue::{
    AssetKey, ClientIdDomain, Product, SelfDescribingClientId, Side, TerminalReason, TimeInForce,
};

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

fn bindings_for(venue: SpotVenue) -> Vec<UtaSymbolBinding> {
    let names = match venue {
        SpotVenue::Bybit => [
            ("BTCUSDT", Product::Spot),
            ("BTCUSDT-PERP", Product::LinearPerp),
            ("BTCUSDZ26", Product::InverseFutures),
            ("BTC-25DEC26-90000-C", Product::Options),
        ],
        SpotVenue::Okx => [
            ("BTC-USDT", Product::Spot),
            ("BTC-USDT-SWAP", Product::LinearPerp),
            ("BTC-USD-261225", Product::InverseFutures),
            ("BTC-USD-261225-90000-C", Product::Options),
        ],
    };
    names
        .into_iter()
        .enumerate()
        .map(|(idx, (sym, product))| {
            let id = idx as u32;
            UtaSymbolBinding::new(
                id,
                sym,
                OrsSymbolSpec {
                    symbol_id: id,
                    instrument_id: u64::from(id) + 500,
                    product,
                    wallet_id: 1,
                    base_asset_id: 1,
                    base_scale: 6,
                    quote_asset_id: 2,
                    quote_scale: 6,
                    settlement_asset_id: 2,
                    settlement_scale: 6,
                    price_tick_atoms: 100, // 1.00 tick
                    price_scale: 2,
                    qty_step_atoms: 1_000, // 0.001 lot
                    qty_scale: 6,
                    contract_size_atoms: 1,
                    contract_scale: 0,
                    initial_margin_bps: 1_000, // 10% IM
                },
            )
            .unwrap()
        })
        .collect()
}

fn assets() -> Vec<UtaAssetBinding> {
    vec![
        UtaAssetBinding::new("BTC", AssetKey::new(1, 1), 6).unwrap(),
        UtaAssetBinding::new("USDT", AssetKey::new(1, 2), 6).unwrap(),
    ]
}

#[test]
fn stateless_cold_rest_bootstrap_recovers_tw1_orders_holds_positions_and_watermarks_for_bybit_and_okx()
 {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let risk_domain_id = [0x77; 16];
        let uid = 55_123u64;
        let epoch = 3u32;
        let bindings = bindings_for(venue);

        let domain_cfg = OrsAccountDomainConfig {
            account_ref: 1,
            risk_domain_id,
            venue: venue.venue_id(),
            environment: Environment::Production,
            exchange_account_id: uid,
            epoch,
        };
        let specs: Vec<OrsSymbolSpec> = bindings.iter().map(|b| b.spec).collect();
        let mut om_cfg = OrderManagerConfig::single_account_default(domain_cfg, specs);
        om_cfg.slots_per_strat = 64;
        let om = OrderManager::<true>::new(om_cfg).unwrap();
        om.dop_state().configure_strategy(2, &[1]).unwrap();
        om.dop_state().configure_strategy(5, &[1]).unwrap();

        // Construct two genuine 32-char `tw1...` `SelfDescribingClientId` tokens:
        //  - strat 2, seqnum 19 on LinearPerp (`symbol_id = 1`, `RPI` limit buy)
        //  - strat 5, seqnum 41 on Options (`symbol_id = 3`, `PostOnly` limit sell)
        let cid_perp = SelfDescribingClientId::from_ors_order(2, 0, epoch, 19)
            .unwrap()
            .encode(&ClientIdDomain {
                risk_domain_id,
                venue: venue.venue_id(),
                environment: Environment::Production,
                account: uid,
                product: Product::LinearPerp,
            })
            .unwrap();
        let cid_opt = SelfDescribingClientId::from_ors_order(5, 0, epoch, 41)
            .unwrap()
            .encode(&ClientIdDomain {
                risk_domain_id,
                venue: venue.venue_id(),
                environment: Environment::Production,
                account: uid,
                product: Product::Options,
            })
            .unwrap();
        assert_eq!(cid_perp.as_str().len(), 32);
        assert!(cid_perp.as_str().starts_with("tw1"));

        let mut decoder = UtaBootstrapDecoder::new(venue, bindings.clone(), assets());

        match venue {
            SpotVenue::Bybit => {
                assert_eq!(
                    wallet_balance_path(SpotVenue::Bybit),
                    "/v5/account/wallet-balance?accountType=UNIFIED"
                );
                assert!(
                    bybit_positions_path(BybitCategory::Linear, Some("USDT"), None, None)
                        .unwrap()
                        .starts_with("/v5/position/list?category=linear")
                );
                assert!(
                    bybit_open_orders_path(BybitCategory::Option, None, Some("BTC"), None, None)
                        .unwrap()
                        .starts_with("/v5/order/realtime?category=option")
                );

                decoder
                    .ingest_wallet_balance_json(
                        br#"{"retCode":0,"result":{"list":[{"accountType":"UNIFIED","coin":[
                            {"coin":"USDT","walletBalance":"50000","locked":"0","totalOrderIM":"1000"},
                            {"coin":"BTC","walletBalance":"2.5","locked":"0","totalOrderIM":"0"}
                        ]}]}}"#,
                    )
                    .unwrap();
                decoder
                    .ingest_positions_json(
                        br#"{"retCode":0,"result":{"list":[
                            {"symbol":"BTCUSDT-PERP","side":"Buy","size":"0.025"},
                            {"symbol":"BTCUSDZ26","side":"Sell","size":"0.010"}
                        ]}}"#,
                    )
                    .unwrap();
                let open_json = serde_json::json!({
                    "retCode": 0,
                    "result": {
                        "nextPageCursor": "",
                        "list": [
                            {
                                "symbol": "BTCUSDT-PERP",
                                "orderId": "byb-perp-19",
                                "orderLinkId": cid_perp.as_str(),
                                "side": "Buy",
                                "timeInForce": "RPI",
                                "reduceOnly": false,
                                "price": "80000",
                                "qty": "0.010",
                                "cumExecQty": "0.002"
                            },
                            {
                                "symbol": "BTC-25DEC26-90000-C",
                                "orderId": "byb-opt-41",
                                "orderLinkId": cid_opt.as_str(),
                                "side": "Sell",
                                "timeInForce": "PostOnly",
                                "reduceOnly": false,
                                "price": "2000",
                                "qty": "0.005",
                                "cumExecQty": "0"
                            },
                            {
                                "symbol": "BTCUSDT",
                                "orderId": "manual-gui-1",
                                "orderLinkId": "manualOrder99",
                                "side": "Buy",
                                "timeInForce": "GTC",
                                "reduceOnly": false,
                                "price": "70000",
                                "qty": "0.001",
                                "cumExecQty": "0"
                            }
                        ]
                    }
                });
                let (ingested, cursor) = decoder
                    .ingest_open_orders_json(&serde_json::to_vec(&open_json).unwrap(), 100_000)
                    .unwrap();
                assert_eq!(ingested, 3);
                assert!(cursor.is_none());
            }
            SpotVenue::Okx => {
                assert_eq!(
                    wallet_balance_path(SpotVenue::Okx),
                    "/api/v5/account/balance"
                );
                assert_eq!(
                    okx_positions_path(Some(OkxInstType::Swap)),
                    "/api/v5/account/positions?instType=SWAP"
                );
                assert!(
                    okx_open_orders_path(OkxInstType::Option, None)
                        .unwrap()
                        .starts_with("/api/v5/trade/orders-pending?instType=OPTION")
                );

                decoder
                    .ingest_wallet_balance_json(
                        br#"{"code":"0","data":[{"details":[
                            {"ccy":"USDT","cashBal":"50000","availBal":"49000","ordFrozen":"1000"},
                            {"ccy":"BTC","cashBal":"2.5","availBal":"2.5","ordFrozen":"0"}
                        ]}]}"#,
                    )
                    .unwrap();
                decoder
                    .ingest_positions_json(
                        br#"{"code":"0","data":[
                            {"instId":"BTC-USDT-SWAP","pos":"0.025","posSide":"net"},
                            {"instId":"BTC-USD-261225","pos":"0.010","posSide":"short"}
                        ]}"#,
                    )
                    .unwrap();
                let open_json = serde_json::json!({
                    "code": "0",
                    "data": [
                        {
                            "instId": "BTC-USDT-SWAP",
                            "ordId": "okx-perp-19",
                            "clOrdId": cid_perp.as_str(),
                            "side": "buy",
                            "ordType": "rpi",
                            "reduceOnly": "false",
                            "px": "80000",
                            "sz": "0.010",
                            "accFillSz": "0.002"
                        },
                        {
                            "instId": "BTC-USD-261225-90000-C",
                            "ordId": "okx-opt-41",
                            "clOrdId": cid_opt.as_str(),
                            "side": "sell",
                            "ordType": "post_only",
                            "reduceOnly": false,
                            "px": "2000",
                            "sz": "0.005",
                            "accFillSz": "0"
                        },
                        {
                            "instId": "BTC-USDT",
                            "ordId": "manual-gui-2",
                            "clOrdId": "manualOrder88",
                            "side": "buy",
                            "ordType": "limit",
                            "px": "70000",
                            "sz": "0.001",
                            "accFillSz": "0"
                        }
                    ]
                });
                let (ingested, cursor) = decoder
                    .ingest_open_orders_json(&serde_json::to_vec(&open_json).unwrap(), 100_000)
                    .unwrap();
                assert_eq!(ingested, 3);
                assert!(cursor.is_none());
            }
        }

        let summary = decoder.bootstrap_order_manager(&om, 1).unwrap();
        assert_eq!(summary.managed_open_orders, 2);
        assert_eq!(summary.external_open_orders, 1);

        // Verify strategy sequence watermarks (`next_order_seqnum = recovered + 1`)!
        assert_eq!(om.dop_state().next_order_seqnum(2), Some(20));
        assert_eq!(om.dop_state().next_order_seqnum(5), Some(42));

        // Verify derivative positions (+25 lots on symbol 1, -10 lots on symbol 2)
        assert_eq!(om.wallet_ledger().position_lots(1, 1), Some(25));
        assert_eq!(om.wallet_ledger().position_lots(1, 2), Some(-10));

        // Verify live recovered order slots & `RPI` / `PostOnly` metadata
        let oid_perp = make_ors_order_id(19, 2);
        let locked_perp = om
            .lock_order(oid_perp)
            .expect("recovered perp order must be Live");
        assert_eq!(locked_perp.order().tif, TimeInForce::Rpi);
        assert_eq!(locked_perp.order().filled_qty_lots, 2);
        assert_eq!(locked_perp.order().original_qty_lots, 10);
        drop(locked_perp);

        let oid_opt = make_ors_order_id(41, 5);
        let locked_opt = om
            .lock_order(oid_opt)
            .expect("recovered option order must be Live");
        assert!(locked_opt.order().post_only);
        drop(locked_opt);
    }
}

#[test]
fn ors_query_desk_status_ask_reconciles_fills_and_dispatches_coalesced_cancel() {
    let bindings = bindings_for(SpotVenue::Bybit);
    let domain = OrsAccountDomainConfig {
        account_ref: 1,
        risk_domain_id: [0x33; 16],
        venue: SpotVenue::Bybit.venue_id(),
        environment: Environment::Test,
        exchange_account_id: 66_001,
        epoch: 1,
    };
    let specs: Vec<OrsSymbolSpec> = bindings.iter().map(|b| b.spec).collect();
    let mut om_cfg = OrderManagerConfig::single_account_default(domain, specs);
    om_cfg.slots_per_strat = 64;
    let om = Arc::new(OrderManager::<true>::new(om_cfg).unwrap());
    om.wallet_ledger()
        .set_balance(1, AssetKey::new(1, 2), 6, 50_000_000_000_000, 0)
        .unwrap();
    om.dop_state().configure_strategy(0, &[1]).unwrap();
    om.dop_state().login(0, 1).unwrap();

    let creds = OwnedCredentials::bybit("BYBIT_QD_KEY", "bybit_qd_secret").unwrap();
    let cfg = UtaSessionConfig {
        venue: SpotVenue::Bybit,
        environment: Environment::Test,
        account_ref: 1,
        exchange_uid: 66_001,
        risk_domain_id: [0x33; 16],
        epoch: 1,
        credentials: creds.clone(),
        transport_mode: UtaTransportMode::WebSocketTrade,
        pool: vec![UtaConnPoolEntry::new(Ipv4Addr::new(10, 3, 0, 1), 1)],
        router_config: RouterConfig::default(),
        limits: Limits::default(),
        symbols: bindings,
        assets: assets(),
        rate_limiter: Arc::new(SharedRateLimiter::new()),
        rate_overrides: AccountRateOverrides::default(),
        ip_order_weight: 1,
        okx_ws_auto_subscribe_private: false,
        private_stream_baton: None,
        rfq_hook: None,
    };
    let session = BybitUtaSession::new(Arc::clone(&om), cfg).unwrap();
    let (conn0, mut srv0) = loopback_pair();
    session
        .install_order_connection(0, conn0, true, 1_700_000_000_000, 1_000)
        .unwrap();

    // Place order 1 on LinearPerp (`symbol_id = 1`)
    let req = OrsNewOrderRequest {
        strat_id: 0,
        account_ref: 1,
        msg_seqnum: 1,
        order_seqnum: 1,
        symbol_id: 1,
        side: Side::Buy,
        price_ticks: 60_000,
        qty_lots: 10,
        tif: TimeInForce::Gtc,
        post_only: false,
        reduce_only: false,
        check_stp: true,
        dry_run: false,
        now_mono_ns: 10_000,
    };
    let (oid, rep_id) = match session
        .submit_new_order(
            &req,
            UtaOrderParams::cross_derivative_limit(TimeInForce::Gtc, false),
            1_700_000_000_010,
        )
        .unwrap()
    {
        UtaSubmitOutcome::Dispatched {
            ors_order_id,
            reply_id,
            ..
        } => (ors_order_id, reply_id),
        other => panic!("unexpected: {other:?}"),
    };
    let mut drain = [0u8; 2048];
    let _ = read_loopback(&mut srv0, &mut drain);

    // Inject an ambiguous 5xx-equivalent venue error (`retCode: 10016` internal server error)
    // -> schedules `StatusAsk` in `OrsQueryDesk` instead of falsely rejecting a potentially live order!
    let mut reports = Vec::new();
    let ambig = format!(
        r#"{{"reqId":"{rep_id}","retCode":10016,"retMsg":"Server timeout","op":"order.create"}}"#
    );
    session
        .apply_order_ws_payload(0, ambig.as_bytes(), 20_000, 1_700_000_000_020, &mut reports)
        .unwrap();
    assert!(
        reports.is_empty(),
        "ambiguous venue timeout must schedule StatusAsk, not reject"
    );

    // Strategy sends Cancel while StatusAsk is pending in `OrsQueryDesk`:
    // Cancel is coalesced (`cancel_after = true`) inside `OrsQueryDesk`!
    let cxl = OrsCancelRequest {
        strat_id: 0,
        account_ref: 1,
        msg_seqnum: 2,
        ors_order_id: oid,
        now_mono_ns: 21_000,
    };
    let cxl_out = session
        .submit_cancel_order(&cxl, 1_700_000_000_021)
        .unwrap();
    assert!(matches!(
        cxl_out,
        UtaSubmitOutcome::Queued {
            coalesced_in_query_desk: true,
            ..
        }
    ));

    // Poll due queries and verify `UtaRestRequestBuilder` builds signed `/v5/order/realtime` GET!
    let _ = session.poll_due_queries(None, 2_000_000_000, 1_700_000_002_000);
    let mut builder = UtaRestRequestBuilder::new(
        creds,
        Environment::Test,
        Arc::clone(session.order_manager()),
        session.symbols(),
        || 1_700_000_002_000,
    );
    let task = RestTask::status_ask(oid, 1, 2_000_000_000, 7_000_000_000);
    let mut scratch = Vec::new();
    let built = builder
        .build_request(&task, "api-testnet.bybit.com", &mut scratch)
        .unwrap()
        .unwrap();
    assert_eq!(built.method, "GET");
    assert!(
        built
            .path
            .starts_with("/v5/order/realtime?category=linear&symbol=BTCUSDT-PERP&orderLinkId=tw1")
    );
    assert!(built.headers.iter().any(|(k, _)| k == "X-BAPI-SIGN"));

    // Simulate `AsyncRestQuerier` returning `PartiallyFilled` (3 lots filled out of 10) + granular fill page:
    // `apply_status_ask_reply` reconciles the 3-lot trade AND immediately dispatches the coalesced wire Cancel!
    let order_body = br#"{"retCode":0,"result":{"list":[{"symbol":"BTCUSDT-PERP","orderId":"byb-q-1","orderLinkId":"","orderStatus":"PartiallyFilled","price":"60000","qty":"0.010","cumExecQty":"0.003"}]}}"#.to_vec();
    let fills_body = br#"{"retCode":0,"result":{"list":[{"symbol":"BTCUSDT-PERP","execId":"q-fill-1","orderId":"byb-q-1","execPrice":"60000","execQty":"0.003","execFee":"0.18","feeCurrency":"USDT","isMaker":false}]}}"#;
    let reply = RestReply {
        task,
        status: RestStatus::Ok,
        http_code: 200,
        query_count: 1,
        body: order_body,
        headers: Vec::new(),
    };
    let action = session
        .apply_status_ask_reply(
            &reply,
            Some(fills_body),
            2_001_000_000,
            1_700_000_002_001,
            &mut reports,
        )
        .unwrap();
    assert!(matches!(
        action,
        OrsQueryReconcileAction::DispatchCancel {
            ors_order_id,
            ..
        } if ors_order_id == oid
    ));
    assert!(reports.iter().any(|r| matches!(r, OrsReport::Ack(_))));
    assert!(
        reports
            .iter()
            .any(|r| matches!(r, OrsReport::Fill(f) if !f.projected))
    );
    assert_eq!(om.wallet_ledger().position_lots(1, 1), Some(3));

    // Confirm the coalesced `order.cancel` frame was automatically written to `srv0`!
    let n_cxl_bytes = read_loopback(&mut srv0, &mut drain);
    assert!(n_cxl_bytes > 0);
    let _ = OrsQueryOrigin::Place;
}

#[test]
fn rfq_quote_hook_defaults_to_not_supported_and_encodes_bybit_and_okx_rfq_endpoints_when_enabled() {
    let bindings = bindings_for(SpotVenue::Okx);
    let domain = OrsAccountDomainConfig {
        account_ref: 1,
        risk_domain_id: [0x22; 16],
        venue: SpotVenue::Okx.venue_id(),
        environment: Environment::Test,
        exchange_account_id: 44_001,
        epoch: 1,
    };
    let specs: Vec<OrsSymbolSpec> = bindings.iter().map(|b| b.spec).collect();
    let mut om_cfg = OrderManagerConfig::single_account_default(domain, specs);
    om_cfg.slots_per_strat = 32;
    let om = Arc::new(OrderManager::<true>::new(om_cfg).unwrap());

    let cfg = UtaSessionConfig {
        venue: SpotVenue::Okx,
        environment: Environment::Test,
        account_ref: 1,
        exchange_uid: 44_001,
        risk_domain_id: [0x22; 16],
        epoch: 1,
        credentials: OwnedCredentials::okx("OKX_RFQ_K", "okx_rfq_s", "okx_rfq_p").unwrap(),
        transport_mode: UtaTransportMode::PooledHttps,
        pool: vec![UtaConnPoolEntry::new(Ipv4Addr::new(10, 4, 0, 1), 2)],
        router_config: RouterConfig::default(),
        limits: Limits::default(),
        symbols: bindings,
        assets: assets(),
        rate_limiter: Arc::new(SharedRateLimiter::new()),
        rate_overrides: AccountRateOverrides::default(),
        ip_order_weight: 1,
        okx_ws_auto_subscribe_private: false,
        private_stream_baton: None,
        rfq_hook: None, // Default `NoopRfqQuoteHook`
    };
    let mut session = OkxUtaSession::new(om, cfg).unwrap();
    let (conn0, mut srv0) = loopback_pair();
    let (conn1, _srv1) = loopback_pair();
    session
        .install_order_connection(0, conn0, true, 1_700_000_000_000, 1_000)
        .unwrap();
    session
        .install_order_connection(1, conn1, true, 1_700_000_000_000, 1_000)
        .unwrap();

    let leg = RfqQuoteLeg {
        symbol: InlineAscii::try_from_str("BTC-USD-261225-90000-C").unwrap(),
        product: Product::Options,
        side: Side::Buy,
        price_atoms: 155_000,
        price_scale: 2,
        qty_atoms: 10_000,
        qty_scale: 2,
        ratio: 1,
    };
    let quote_req = RfqQuoteRequest::single_leg(
        make_ors_order_id(1, 0),
        0,
        "rfq-block-9001",
        "tw10123456789abcdefghjklmnpqrstu",
        Side::Buy,
        leg,
        1_700_000_060_000,
    )
    .unwrap();

    // 1. Default `NoopRfqQuoteHook` refuses with `TerminalReason::RFQ_QUOTE_NOT_SUPPORTED`!
    assert!(!session.rfq_hook().is_enabled());
    assert_eq!(session.rfq_hook().open_quotes(), 0);
    assert!(!session.rfq_hook().has_quote(quote_req.ors_order_id));
    let refused = session
        .submit_rfq_quote(&quote_req, 1_700_000_000_100, 10_000)
        .unwrap_err();
    assert_eq!(
        refused,
        UtaSubmitRefusal::RfqRefused {
            ors_order_id: quote_req.ors_order_id,
            reason: TerminalReason::RFQ_QUOTE_NOT_SUPPORTED,
        }
    );

    // 2. Enable `UtaRfqQuoteBook` on `OkxUtaSession`, submit quote, verify `/api/v5/rfq/create-quote` wire request,
    //    acknowledge quote, and cancel quote (`/api/v5/rfq/cancel-quote`)!
    session.set_rfq_hook(Box::new(UtaRfqQuoteBook::new(16)));
    assert!(session.rfq_hook().is_enabled());

    let out = session
        .submit_rfq_quote(&quote_req, 1_700_000_000_100, 11_000)
        .unwrap();
    assert!(matches!(
        out,
        UtaSubmitOutcome::Dispatched { conn_id: 0, .. }
    ));
    assert!(session.rfq_hook().has_quote(quote_req.ors_order_id));
    assert_eq!(session.rfq_hook().open_quotes(), 1);

    let mut raw_tx = [0u8; 2048];
    let n = read_loopback(&mut srv0, &mut raw_tx);
    let tx_str = core::str::from_utf8(&raw_tx[..n]).unwrap();
    assert!(tx_str.starts_with("POST /api/v5/rfq/create-quote HTTP/1.1\r\n"));
    let body_str = tx_str.split("\r\n\r\n").nth(1).unwrap();
    let body_json: Value = serde_json::from_str(body_str).unwrap();
    assert_eq!(body_json["rfqId"], "rfq-block-9001");
    assert_eq!(body_json["clQuoteId"], "tw10123456789abcdefghjklmnpqrstu");
    assert_eq!(body_json["quoteSide"], "buy");
    assert_eq!(body_json["legs"][0]["instId"], "BTC-USD-261225-90000-C");

    // Acknowledge RFQ quote over HTTP 200
    let mut reports = Vec::new();
    let rfq_ack_body = r#"{"code":"0","data":[{"quoteId":"okx-q-9001"}]}"#;
    let rfq_ack = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{rfq_ack_body}",
        rfq_ack_body.len()
    );
    session
        .apply_order_https_bytes(
            0,
            rfq_ack.as_bytes(),
            12_000,
            1_700_000_000_101,
            &mut reports,
        )
        .unwrap();
    assert!(session.rfq_hook().has_quote(quote_req.ors_order_id));

    // Cancel RFQ quote and settle terminal response
    let cxl_req = RfqCancelQuotesRequest {
        ors_order_id: Some(quote_req.ors_order_id),
        rfq_id: Some(InlineAscii::try_from_str("rfq-block-9001").unwrap()),
        client_quote_id: Some(
            InlineAscii::try_from_str("tw10123456789abcdefghjklmnpqrstu").unwrap(),
        ),
        venue_quote_id: None,
    };
    let cxl_out = session
        .cancel_rfq_quotes(&cxl_req, 1_700_000_000_102, 13_000)
        .unwrap();
    let cxl_conn = match cxl_out {
        UtaSubmitOutcome::Dispatched { conn_id, .. } => conn_id,
        other => panic!("unexpected: {other:?}"),
    };
    let rfq_cxl_body = r#"{"code":"0","data":[{}]}"#;
    let rfq_cxl_ack = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{rfq_cxl_body}",
        rfq_cxl_body.len()
    );
    session
        .apply_order_https_bytes(
            cxl_conn,
            rfq_cxl_ack.as_bytes(),
            14_000,
            1_700_000_000_103,
            &mut reports,
        )
        .unwrap();
    assert_eq!(session.rfq_hook().open_quotes(), 0);
    assert!(!session.rfq_hook().has_quote(quote_req.ors_order_id));

    // 3. Also verify Bybit `/v5/rfq/{create-quote,cancel-all-quotes}` payload builder directly
    let bybit_book = UtaRfqQuoteBook::new(8);
    let mut bbuf = [0u8; 1024];
    let bybit_leg = RfqQuoteLeg {
        symbol: InlineAscii::try_from_str("BTC-25DEC26-90000-C").unwrap(),
        ..leg
    };
    let bybit_req = RfqQuoteRequest::single_leg(
        make_ors_order_id(2, 0),
        0,
        "bybit-rfq-77",
        "tw10123456789abcdefghjklmnpqrstu",
        Side::Sell,
        bybit_leg,
        1_700_000_090_000,
    )
    .unwrap();
    let built_b = bybit_book
        .submit_quote(SpotVenue::Bybit, &bybit_req, &mut bbuf)
        .unwrap();
    assert_eq!(built_b.path, "/v5/rfq/create-quote");
    let bv: Value = serde_json::from_slice(&bbuf[..built_b.body_len]).unwrap();
    assert_eq!(bv["rfqId"], "bybit-rfq-77");
    assert_eq!(bv["quoteSide"], "Sell");
    assert_eq!(bv["quoteBuyList"][0]["category"], "option");

    let built_cxl_all = bybit_book
        .cancel_quotes(
            SpotVenue::Bybit,
            &RfqCancelQuotesRequest::default(),
            &mut bbuf,
        )
        .unwrap();
    assert_eq!(built_cxl_all.path, "/v5/rfq/cancel-all-quotes");
}
