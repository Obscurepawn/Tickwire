//! Comprehensive integration tests for the unified `Send + Sync` full-market `BinanceSession`
//! across Spot, USD-M, Coin-M, and European Options (`#![forbid(unsafe_code)]`).
#![forbid(unsafe_code)]

use std::{
    io::Read,
    net::{IpAddr, Ipv4Addr, TcpListener, TcpStream},
    sync::Arc,
    time::{Duration, Instant},
};

use tickwire_account::{
    DueQueryAction, OrderManager, OrderManagerConfig, OrsAccountDomainConfig, OrsCancelRequest,
    OrsModifyRequest, OrsReport, OrsSymbolSpec,
};
use tickwire_binance_account::{
    BinanceAccountSpec, BinanceAssetBinding, BinanceBootstrapCatalog, BinanceCancelOutcome,
    BinanceConfigError, BinanceConnectionSet, BinanceEventSink, BinanceLoginReply,
    BinanceMarketAuth, BinanceMarketConfig, BinanceModifyOutcome, BinanceNewOrderIntent,
    BinanceRequestKind, BinanceRequestRef, BinanceSession, BinanceSessionError,
    BinanceStatusApplyOutcome, BinanceSubmitOutcome, BinanceSymbolBinding, BinanceUrlId,
    BinanceVenueMarket, ExchRateLimitNotice, UrlRole, pack_ws_reply_id, resolve_market_urls,
    unmask_ws_client_text_frame,
};
use tickwire_binance_quota::{AccountRateOverrides, IpBudgetSpec, IpKey, SharedBinanceOrsQuota};
use tickwire_transport::{
    Connection, HttpsLink, IoBudget, Limits, LinkOps, Security, UrlBindPair, WsLink, WsLoginState,
    order_router::SampleKind,
};
use tickwire_venue::{
    AssetKey, Environment, Liquidity, OrderType, PositionSide, Product, RefusalReason,
    SelfTradePrevention, Side, TerminalReason, TimeInForce,
};

const TEST_ED25519_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MC4CAQAwBQYDK2VwBCIEIIJEYWtGBrhACmb9Dvy+qa8WEf0lQOl1s4CLIAB9m89u\n\
-----END PRIVATE KEY-----\n";

const ASSET_USDT: u64 = 1;
const ASSET_BTC: u64 = 2;

const WALLET_SPOT: u16 = 1;
const WALLET_USDM: u16 = 2;
const WALLET_COINM: u16 = 3;
const WALLET_OPTIONS: u16 = 4;

const SYM_SPOT_BTCUSDT: u32 = 0;
const SYM_USDM_BTCUSDT: u32 = 1;
const SYM_COINM_BTCUSD: u32 = 2;
const SYM_OPT_BTC_CALL: u32 = 3;

#[derive(Default, Debug)]
struct TestSink {
    reports: Vec<OrsReport>,
    rate_notices: Vec<ExchRateLimitNotice>,
    expired_streams: Vec<(u8, BinanceVenueMarket)>,
    venue_amendments: Vec<(u8, u64, i64, i64)>,
    exhausted_cancels: Vec<(u8, u64)>,
}

impl BinanceEventSink for TestSink {
    fn on_report(&mut self, report: OrsReport) {
        self.reports.push(report);
    }

    fn on_exch_rate_limit(&mut self, notice: ExchRateLimitNotice) {
        self.rate_notices.push(notice);
    }

    fn on_stream_token_expired(&mut self, account_ref: u8, market: BinanceVenueMarket) {
        self.expired_streams.push((account_ref, market));
    }

    fn on_venue_amendment(
        &mut self,
        account_ref: u8,
        ors_order_id: u64,
        new_price_ticks: i64,
        new_qty_lots: i64,
    ) {
        self.venue_amendments
            .push((account_ref, ors_order_id, new_price_ticks, new_qty_lots));
    }

    fn on_cancel_query_exhausted(&mut self, account_ref: u8, ors_order_id: u64) {
        self.exhausted_cancels.push((account_ref, ors_order_id));
    }
}

fn test_account_domain(account_ref: u8) -> OrsAccountDomainConfig {
    OrsAccountDomainConfig {
        account_ref,
        risk_domain_id: [0x12; 16],
        venue: 1,
        environment: Environment::Production,
        exchange_account_id: 9_001_001,
        epoch: 1,
    }
}

fn four_market_symbols_and_catalog() -> (Vec<OrsSymbolSpec>, Arc<BinanceBootstrapCatalog>) {
    let spot_spec = OrsSymbolSpec {
        symbol_id: SYM_SPOT_BTCUSDT,
        instrument_id: 1001,
        product: Product::Spot,
        wallet_id: WALLET_SPOT,
        base_asset_id: ASSET_BTC,
        base_scale: 8,
        quote_asset_id: ASSET_USDT,
        quote_scale: 8,
        settlement_asset_id: ASSET_USDT,
        settlement_scale: 8,
        price_tick_atoms: 1,
        price_scale: 2,
        qty_step_atoms: 1,
        qty_scale: 4,
        contract_size_atoms: 1,
        contract_scale: 0,
        initial_margin_bps: 10_000,
    };
    let usdm_spec = OrsSymbolSpec {
        symbol_id: SYM_USDM_BTCUSDT,
        instrument_id: 2001,
        product: Product::UsdM,
        wallet_id: WALLET_USDM,
        base_asset_id: ASSET_BTC,
        base_scale: 8,
        quote_asset_id: ASSET_USDT,
        quote_scale: 8,
        settlement_asset_id: ASSET_USDT,
        settlement_scale: 8,
        price_tick_atoms: 1,
        price_scale: 2,
        qty_step_atoms: 1,
        qty_scale: 3,
        contract_size_atoms: 1,
        contract_scale: 0,
        initial_margin_bps: 1_000, // 10x
    };
    let coinm_spec = OrsSymbolSpec {
        symbol_id: SYM_COINM_BTCUSD,
        instrument_id: 3001,
        product: Product::CoinM,
        wallet_id: WALLET_COINM,
        base_asset_id: ASSET_BTC,
        base_scale: 8,
        quote_asset_id: ASSET_USDT,
        quote_scale: 8,
        settlement_asset_id: ASSET_BTC,
        settlement_scale: 8,
        price_tick_atoms: 1,
        price_scale: 1,
        qty_step_atoms: 1,
        qty_scale: 0,
        contract_size_atoms: 1,
        contract_scale: 0,
        initial_margin_bps: 2_000, // 5x
    };
    let opt_spec = OrsSymbolSpec {
        symbol_id: SYM_OPT_BTC_CALL,
        instrument_id: 4001,
        product: Product::Option,
        wallet_id: WALLET_OPTIONS,
        base_asset_id: ASSET_BTC,
        base_scale: 8,
        quote_asset_id: ASSET_USDT,
        quote_scale: 8,
        settlement_asset_id: ASSET_USDT,
        settlement_scale: 8,
        price_tick_atoms: 1,
        price_scale: 1,
        qty_step_atoms: 1,
        qty_scale: 2,
        contract_size_atoms: 1,
        contract_scale: 0,
        initial_margin_bps: 10_000, // 100% premium
    };

    let symbols = vec![spot_spec, usdm_spec, coinm_spec, opt_spec];
    let bindings = vec![
        BinanceSymbolBinding {
            symbol_id: SYM_SPOT_BTCUSDT,
            market: BinanceVenueMarket::Spot,
            venue_symbol: String::from("BTCUSDT"),
            spec: spot_spec,
            default_fee_asset_code: Some(String::from("USDT")),
        },
        BinanceSymbolBinding {
            symbol_id: SYM_USDM_BTCUSDT,
            market: BinanceVenueMarket::Usdm,
            venue_symbol: String::from("BTCUSDT"),
            spec: usdm_spec,
            default_fee_asset_code: Some(String::from("USDT")),
        },
        BinanceSymbolBinding {
            symbol_id: SYM_COINM_BTCUSD,
            market: BinanceVenueMarket::Coinm,
            venue_symbol: String::from("BTCUSD_PERP"),
            spec: coinm_spec,
            default_fee_asset_code: Some(String::from("BTC")),
        },
        BinanceSymbolBinding {
            symbol_id: SYM_OPT_BTC_CALL,
            market: BinanceVenueMarket::Options,
            venue_symbol: String::from("BTC-261225-80000-C"),
            spec: opt_spec,
            default_fee_asset_code: Some(String::from("USDT")),
        },
    ];
    let assets = vec![
        BinanceAssetBinding {
            market: BinanceVenueMarket::Spot,
            asset_code: String::from("USDT"),
            key: AssetKey::new(WALLET_SPOT, ASSET_USDT),
            scale: 8,
        },
        BinanceAssetBinding {
            market: BinanceVenueMarket::Spot,
            asset_code: String::from("BTC"),
            key: AssetKey::new(WALLET_SPOT, ASSET_BTC),
            scale: 8,
        },
        BinanceAssetBinding {
            market: BinanceVenueMarket::Usdm,
            asset_code: String::from("USDT"),
            key: AssetKey::new(WALLET_USDM, ASSET_USDT),
            scale: 8,
        },
        BinanceAssetBinding {
            market: BinanceVenueMarket::Coinm,
            asset_code: String::from("BTC"),
            key: AssetKey::new(WALLET_COINM, ASSET_BTC),
            scale: 8,
        },
        BinanceAssetBinding {
            market: BinanceVenueMarket::Options,
            asset_code: String::from("USDT"),
            key: AssetKey::new(WALLET_OPTIONS, ASSET_USDT),
            scale: 8,
        },
    ];
    let catalog = Arc::new(BinanceBootstrapCatalog::new(bindings, assets).unwrap());
    (symbols, catalog)
}

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
    assert!(conn.is_ready(), "loopback socket must become ready");
    (conn, server_stream)
}

fn read_nonblocking_with_retry(stream: &mut TcpStream, buf: &mut [u8]) -> usize {
    for _ in 0..50 {
        match stream.read(buf) {
            Ok(n) if n > 0 => return n,
            Ok(_) => panic!("unexpected EOF on loopback server stream"),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(e) => panic!("unexpected read error on loopback server stream: {e}"),
        }
    }
    panic!("timed out waiting for nonblocking loopback bytes");
}

fn install_ws_order_link(
    conns: &mut BinanceConnectionSet,
    market: BinanceVenueMarket,
    conn_id: u16,
    local_ip: IpAddr,
) -> (TcpStream, u64) {
    let (conn, peer) = connect_loopback_pair(local_ip);
    let ws = WsLink::with_login(conn, 65_536, 65_536, WsLoginState::Authenticated).unwrap();
    let epoch = conns
        .install_order_link(market, conn_id, LinkOps::Ws(ws))
        .unwrap();
    (peer, epoch)
}

fn install_https_order_link(
    conns: &mut BinanceConnectionSet,
    conn_id: u16,
    local_ip: IpAddr,
) -> (TcpStream, u64) {
    let (conn, peer) = connect_loopback_pair(local_ip);
    let https = HttpsLink::new(conn, 65_536, 64).unwrap();
    let epoch = conns
        .install_order_link(BinanceVenueMarket::Options, conn_id, LinkOps::Https(https))
        .unwrap();
    (peer, epoch)
}

fn default_intent(
    strat_id: u8,
    msg_seqnum: u64,
    order_seqnum: u64,
    symbol_id: u32,
    price_ticks: i64,
    qty_lots: i64,
) -> BinanceNewOrderIntent {
    BinanceNewOrderIntent {
        strat_id,
        account_ref: 1,
        msg_seqnum,
        order_seqnum,
        symbol_id,
        side: Side::Buy,
        order_type: OrderType::Limit,
        price_ticks,
        qty_lots,
        tif: TimeInForce::Gtc,
        post_only: false,
        reduce_only: false,
        position_side: PositionSide::Both,
        stp_mode: SelfTradePrevention::Default,
        check_stp: true,
        dry_run: false,
        wall_ms: 1_700_000_100_000,
        now_mono_ns: 1_000_000_000,
    }
}

#[test]
fn colo_vs_public_url_resolution_and_conflict_rejection() {
    // 1. Default public URLs across all 4 markets.
    let spot_pub =
        resolve_market_urls(BinanceVenueMarket::Spot, Environment::Production, &[]).unwrap();
    assert_eq!(spot_pub.order_url, BinanceUrlId::SpotWssPublic);
    assert_eq!(spot_pub.stream_url, None);
    assert_eq!(spot_pub.query_url, BinanceUrlId::SpotHttpsPublic);

    let um_colo = resolve_market_urls(
        BinanceVenueMarket::Usdm,
        Environment::Production,
        &[BinanceUrlId::UmOrderWssColo, BinanceUrlId::UmStreamWssColo],
    )
    .unwrap();
    assert_eq!(um_colo.order_url, BinanceUrlId::UmOrderWssColo);
    assert_eq!(um_colo.stream_url, Some(BinanceUrlId::UmStreamWssColo));
    assert!(um_colo.order_url.is_colo());

    let cm_colo = resolve_market_urls(
        BinanceVenueMarket::Coinm,
        Environment::Production,
        &[BinanceUrlId::CmOrderWssColo, BinanceUrlId::CmStreamWssColo],
    )
    .unwrap();
    assert_eq!(cm_colo.order_url, BinanceUrlId::CmOrderWssColo);
    assert_eq!(cm_colo.stream_url, Some(BinanceUrlId::CmStreamWssColo));

    let opt_colo = resolve_market_urls(
        BinanceVenueMarket::Options,
        Environment::Production,
        &[
            BinanceUrlId::OptionHttpsOrderColo,
            BinanceUrlId::OptionStreamWssColo,
        ],
    )
    .unwrap();
    assert_eq!(opt_colo.order_url, BinanceUrlId::OptionHttpsOrderColo);
    assert_eq!(opt_colo.stream_url, Some(BinanceUrlId::OptionStreamWssColo));

    // 2. Reject simultaneous colo + public URLs for the same role (`8.5`).
    let conflict = resolve_market_urls(
        BinanceVenueMarket::Usdm,
        Environment::Production,
        &[BinanceUrlId::UmOrderWssColo, BinanceUrlId::UmOrderWssPublic],
    );
    assert_eq!(
        conflict,
        Err(BinanceConfigError::ConflictingColoAndPublicUrl {
            market: BinanceVenueMarket::Usdm,
            role: UrlRole::ORDER,
        })
    );

    // 3. Reject query-only URL in socket list.
    assert_eq!(
        resolve_market_urls(
            BinanceVenueMarket::Usdm,
            Environment::Production,
            &[BinanceUrlId::UmHttpsPublic],
        ),
        Err(BinanceConfigError::QueryUrlNotInstallable(
            BinanceUrlId::UmHttpsPublic
        ))
    );

    // 4. Reject Spot non-empty `stream_binds` and mismatched auth algorithms in `configure`.
    let (symbols, catalog) = four_market_symbols_and_catalog();
    let domain = test_account_domain(1);
    let om = Arc::new(
        OrderManager::<true>::new(OrderManagerConfig::single_account_default(
            domain,
            symbols.clone(),
        ))
        .unwrap(),
    );
    let quota = SharedBinanceOrsQuota::new();
    let ip1 = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

    let bad_spot = BinanceAccountSpec::new(
        domain,
        vec![BinanceMarketConfig {
            market: BinanceVenueMarket::Spot,
            auth: BinanceMarketAuth::ed25519_pem("spotKey", TEST_ED25519_PEM),
            urls: vec![BinanceUrlId::SpotWssPublic],
            order_binds: vec![UrlBindPair::new(Some(ip1), 1)],
            stream_binds: vec![UrlBindPair::new(Some(ip1), 1)],
            rate_overrides: AccountRateOverrides::default(),
            ip_budgets: vec![],
        }],
    );
    assert!(matches!(
        BinanceSession::configure(
            bad_spot,
            om.clone(),
            quota.clone(),
            catalog.clone(),
            None,
            None
        ),
        Err(BinanceSessionError::InvalidConfig(_))
    ));

    let bad_opt_auth = BinanceAccountSpec::new(
        domain,
        vec![BinanceMarketConfig {
            market: BinanceVenueMarket::Options,
            auth: BinanceMarketAuth::ed25519_pem("optKey", TEST_ED25519_PEM),
            urls: vec![],
            order_binds: vec![UrlBindPair::new(Some(ip1), 1)],
            stream_binds: vec![UrlBindPair::new(Some(ip1), 1)],
            rate_overrides: AccountRateOverrides::default(),
            ip_budgets: vec![],
        }],
    );
    assert!(matches!(
        BinanceSession::configure(bad_opt_auth, om, quota, catalog, None, None),
        Err(BinanceSessionError::UrlConfig(
            BinanceConfigError::MissingHmacSecret {
                market: BinanceVenueMarket::Options
            }
        ))
    ));
}

#[test]
fn unified_four_market_uid_session_routing_ws_shared_https_exclusive_and_uta_isolation() {
    let (symbols, catalog) = four_market_symbols_and_catalog();
    let domain = test_account_domain(1);
    let om = Arc::new(
        OrderManager::<true>::new(OrderManagerConfig::single_account_default(domain, symbols))
            .unwrap(),
    );
    om.dop_state().configure_strategy(1, &[1]).unwrap();
    om.dop_state().login(1, 1).unwrap();

    // Seed isolated balances across all 4 market wallets (`AssetKey { wallet_id, asset_id }`).
    let ledger = om.wallet_ledger();
    ledger
        .set_balance(
            1,
            AssetKey::new(WALLET_SPOT, ASSET_USDT),
            8,
            10_000_000_000_000,
            0,
        )
        .unwrap();
    ledger
        .set_balance(
            1,
            AssetKey::new(WALLET_USDM, ASSET_USDT),
            8,
            10_000_000_000_000,
            0,
        )
        .unwrap();
    ledger
        .set_balance(1, AssetKey::new(WALLET_COINM, ASSET_BTC), 8, 500_000_000, 0)
        .unwrap();
    ledger
        .set_balance(
            1,
            AssetKey::new(WALLET_OPTIONS, ASSET_USDT),
            8,
            10_000_000_000_000,
            0,
        )
        .unwrap();

    let ip_spot = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let ip_um = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
    let ip_cm = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 3));
    let ip_opt = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 4));

    let spec = BinanceAccountSpec::new(
        domain,
        vec![
            BinanceMarketConfig {
                market: BinanceVenueMarket::Spot,
                auth: BinanceMarketAuth::ed25519_pem("spotKey", TEST_ED25519_PEM),
                urls: vec![BinanceUrlId::SpotWssPublic],
                order_binds: vec![UrlBindPair::new(Some(ip_spot), 1)],
                stream_binds: vec![],
                rate_overrides: AccountRateOverrides::default(),
                ip_budgets: vec![IpBudgetSpec::new(ip_spot, 6_000)],
            },
            BinanceMarketConfig {
                market: BinanceVenueMarket::Usdm,
                auth: BinanceMarketAuth::ed25519_pem("umKey", TEST_ED25519_PEM),
                urls: vec![BinanceUrlId::UmOrderWssColo, BinanceUrlId::UmStreamWssColo],
                order_binds: vec![UrlBindPair::new(Some(ip_um), 1)],
                stream_binds: vec![UrlBindPair::new(Some(ip_um), 1)],
                rate_overrides: AccountRateOverrides::default(),
                ip_budgets: vec![IpBudgetSpec::new(ip_um, 2_400)],
            },
            BinanceMarketConfig {
                market: BinanceVenueMarket::Coinm,
                auth: BinanceMarketAuth::ed25519_pem("cmKey", TEST_ED25519_PEM),
                urls: vec![
                    BinanceUrlId::CmOrderWssPublic,
                    BinanceUrlId::CmStreamWssPublic,
                ],
                order_binds: vec![UrlBindPair::new(Some(ip_cm), 1)],
                stream_binds: vec![UrlBindPair::new(Some(ip_cm), 1)],
                rate_overrides: AccountRateOverrides::default(),
                ip_budgets: vec![IpBudgetSpec::new(ip_cm, 2_400)],
            },
            BinanceMarketConfig {
                market: BinanceVenueMarket::Options,
                auth: BinanceMarketAuth::hmac_sha256("optKey", b"options-hmac-secret-01".to_vec()),
                urls: vec![
                    BinanceUrlId::OptionHttpsOrderPublic,
                    BinanceUrlId::OptionStreamWssPublic,
                ],
                // Two Exclusive HTTP/1.1 keep-alive connections (`conn_id = 3` and `conn_id = 4`).
                order_binds: vec![UrlBindPair::new(Some(ip_opt), 2)],
                stream_binds: vec![UrlBindPair::new(Some(ip_opt), 1)],
                rate_overrides: AccountRateOverrides::default(),
                ip_budgets: vec![IpBudgetSpec::new(ip_opt, 2_400)],
            },
        ],
    );

    let quota = SharedBinanceOrsQuota::new();
    let (session, mut conns) =
        BinanceSession::configure(spec, om.clone(), quota, catalog, None, None).unwrap();

    let (mut spot_srv, _) = install_ws_order_link(&mut conns, BinanceVenueMarket::Spot, 0, ip_spot);
    let (mut um_srv, _) = install_ws_order_link(&mut conns, BinanceVenueMarket::Usdm, 1, ip_um);
    let (mut cm_srv, _) = install_ws_order_link(&mut conns, BinanceVenueMarket::Coinm, 2, ip_cm);
    let (mut opt_srv_0, _) = install_https_order_link(&mut conns, 3, ip_opt);
    let (mut opt_srv_1, _) = install_https_order_link(&mut conns, 4, ip_opt);

    let mut frame_buf = [0u8; 2048];
    let mut sink = TestSink::default();

    // 1. `dry_run = true` on USD-M: encodes masked WS frame, writes 0 bytes to socket, leaves 0 hold.
    let mut dry = default_intent(1, 1, 1, SYM_USDM_BTCUSDT, 5_000_000, 10);
    dry.dry_run = true;
    let out_dry = session
        .submit_new(&dry, None, &mut frame_buf, &mut sink)
        .unwrap();
    match out_dry {
        BinanceSubmitOutcome::DryRunEncoded { encoded_len, .. } => {
            let mut unmasked = [0u8; 2048];
            let json_len =
                unmask_ws_client_text_frame(&frame_buf[..encoded_len], &mut unmasked).unwrap();
            let json = core::str::from_utf8(&unmasked[..json_len]).unwrap();
            assert!(json.contains(r#""method":"order.place""#));
            assert!(json.contains(r#""symbol":"BTCUSDT""#));
        }
        other => panic!("expected DryRunEncoded, got {other:?}"),
    }
    let mut sock_probe = [0u8; 64];
    assert!(
        um_srv.read(&mut sock_probe).is_err(),
        "dry_run must send 0 bytes onto socket"
    );
    assert_eq!(
        ledger
            .balance(1, AssetKey::new(WALLET_USDM, ASSET_USDT))
            .unwrap()
            .1,
        0,
        "dry_run must leave zero locked hold"
    );

    // 2. Dispatch real Spot, USD-M, and Coin-M orders—each routes strictly to its market's `WsLink`.
    let out_spot = session
        .submit_new(
            &default_intent(1, 2, 2, SYM_SPOT_BTCUSDT, 5_000_000, 100),
            None,
            &mut frame_buf,
            &mut sink,
        )
        .unwrap();
    assert!(matches!(
        out_spot,
        BinanceSubmitOutcome::Sent { conn_id: 0, .. }
    ));

    let out_um = session
        .submit_new(
            &default_intent(1, 3, 3, SYM_USDM_BTCUSDT, 5_000_000, 10),
            None,
            &mut frame_buf,
            &mut sink,
        )
        .unwrap();
    assert!(matches!(
        out_um,
        BinanceSubmitOutcome::Sent { conn_id: 1, .. }
    ));

    let out_cm = session
        .submit_new(
            &default_intent(1, 4, 4, SYM_COINM_BTCUSD, 500_000, 1),
            None,
            &mut frame_buf,
            &mut sink,
        )
        .unwrap();
    assert!(matches!(
        out_cm,
        BinanceSubmitOutcome::Sent { conn_id: 2, .. }
    ));

    // Verify server sockets received valid masked RFC 6455 client Text frames!
    for srv in [&mut spot_srv, &mut um_srv, &mut cm_srv] {
        let mut wire = [0u8; 2048];
        let n = read_nonblocking_with_retry(srv, &mut wire);
        let mut unmasked = [0u8; 2048];
        let json_len = unmask_ws_client_text_frame(&wire[..n], &mut unmasked).unwrap();
        let txt = core::str::from_utf8(&unmasked[..json_len]).unwrap();
        assert!(txt.contains(r#""method":"order.place""#));
    }

    // 3. Options `HttpsLink` (`Exclusive`): back-to-back `REQ` dispatches IMMEDIATELY pick
    //    `conn_id = 3`, then `conn_id = 4` (via live atomic `LockedConnection::is_ready_relaxed()`
    //    without waiting on `RESP` `republish_combined()`), and NACK the 3rd when all links are busy!
    let out_opt_1 = session
        .submit_new(
            &default_intent(1, 5, 5, SYM_OPT_BTC_CALL, 10_000, 10),
            None,
            &mut frame_buf,
            &mut sink,
        )
        .unwrap();
    let opt1_oid = match out_opt_1 {
        BinanceSubmitOutcome::Sent {
            ors_order_id,
            conn_id: 3,
            ..
        } => ors_order_id,
        other => panic!("expected Options Sent on conn_id 3, got {other:?}"),
    };

    let out_opt_2 = session
        .submit_new(
            &default_intent(1, 6, 6, SYM_OPT_BTC_CALL, 10_000, 10),
            None,
            &mut frame_buf,
            &mut sink,
        )
        .unwrap();
    assert!(matches!(
        out_opt_2,
        BinanceSubmitOutcome::Sent { conn_id: 4, .. }
    ));

    // Both Options Exclusive links are now in-flight -> 3rd Options submit NACKs as MarketDown!
    let out_opt_3 = session
        .submit_new(
            &default_intent(1, 7, 7, SYM_OPT_BTC_CALL, 10_000, 10),
            None,
            &mut frame_buf,
            &mut sink,
        )
        .unwrap();
    assert_eq!(
        out_opt_3,
        BinanceSubmitOutcome::LocalNack {
            ors_order_id: match out_opt_3 {
                BinanceSubmitOutcome::LocalNack { ors_order_id, .. } => ors_order_id,
                _ => 0,
            },
            reason: TerminalReason::MKT_REJECT_MARKET_DOWN,
        }
    );

    let mut opt_wire_0 = [0u8; 2048];
    let n0 = read_nonblocking_with_retry(&mut opt_srv_0, &mut opt_wire_0);
    assert!(
        core::str::from_utf8(&opt_wire_0[..n0])
            .unwrap()
            .starts_with("POST /eapi/v1/order HTTP/1.1\r\n")
    );
    let mut opt_wire_1 = [0u8; 2048];
    let n1 = read_nonblocking_with_retry(&mut opt_srv_1, &mut opt_wire_1);
    assert!(
        core::str::from_utf8(&opt_wire_1[..n1])
            .unwrap()
            .starts_with("POST /eapi/v1/order HTTP/1.1\r\n")
    );

    // Complete Options #1 HTTP reply -> atomically frees `conn_id = 3` and transitions `opt1_oid` to `Open`.
    let raw_ref = conns
        .order_pool(BinanceVenueMarket::Options)
        .unwrap()
        .connection_handle(3)
        .unwrap()
        .complete_https_reply()
        .unwrap();
    let hint = BinanceRequestRef::from_transport_ref(raw_ref);
    session
        .on_message(
            BinanceVenueMarket::Options,
            IpKey::from_ip(ip_opt),
            br#"{"orderId":99001,"clientOrderId":"ignored","status":"ACCEPTED","price":"1000","quantity":"0.10","executedQty":"0"}"#,
            hint,
            2_000_000_000,
            &mut sink,
        )
        .unwrap();
    assert!(
        sink.reports
            .iter()
            .any(|r| matches!(r, OrsReport::Ack(a) if a.ors_order_id == opt1_oid))
    );

    // Immediately dispatch a 4th Options order WITHOUT calling `conns.republish_combined()` -> reuses `conn_id = 3`!
    let out_opt_4 = session
        .submit_new(
            &default_intent(1, 8, 8, SYM_OPT_BTC_CALL, 10_000, 10),
            None,
            &mut frame_buf,
            &mut sink,
        )
        .unwrap();
    assert!(matches!(
        out_opt_4,
        BinanceSubmitOutcome::Sent { conn_id: 3, .. }
    ));

    // Verify all 4 market wallets independently reserved holds!
    assert!(
        ledger
            .balance(1, AssetKey::new(WALLET_SPOT, ASSET_USDT))
            .unwrap()
            .1
            > 0
    );
    assert!(
        ledger
            .balance(1, AssetKey::new(WALLET_USDM, ASSET_USDT))
            .unwrap()
            .1
            > 0
    );
    assert!(
        ledger
            .balance(1, AssetKey::new(WALLET_COINM, ASSET_BTC))
            .unwrap()
            .1
            > 0
    );
    assert!(
        ledger
            .balance(1, AssetKey::new(WALLET_OPTIONS, ASSET_USDT))
            .unwrap()
            .1
            > 0
    );
}

#[test]
fn trade_lite_post_trade_rebate_amendment_rate_limits_and_single_pick_nack() {
    let (symbols, catalog) = four_market_symbols_and_catalog();
    let domain = test_account_domain(1);
    let om = Arc::new(
        OrderManager::<true>::new(OrderManagerConfig::single_account_default(domain, symbols))
            .unwrap(),
    );
    om.dop_state().configure_strategy(1, &[1]).unwrap();
    om.dop_state().login(1, 1).unwrap();

    let spot_usdt = AssetKey::new(WALLET_SPOT, ASSET_USDT);
    let usdm_usdt = AssetKey::new(WALLET_USDM, ASSET_USDT);
    let initial_free: i128 = 10_000_000_000_000; // 100,000.00000000 USDT
    om.wallet_ledger()
        .set_balance(1, spot_usdt, 8, initial_free, 0)
        .unwrap();
    om.wallet_ledger()
        .set_balance(1, usdm_usdt, 8, initial_free, 0)
        .unwrap();

    let ip1 = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let ip2 = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
    let spec = BinanceAccountSpec::new(
        domain,
        vec![
            BinanceMarketConfig {
                market: BinanceVenueMarket::Spot,
                auth: BinanceMarketAuth::ed25519_pem("spotKey", TEST_ED25519_PEM),
                urls: vec![BinanceUrlId::SpotWssPublic],
                order_binds: vec![
                    UrlBindPair::new(Some(ip1), 1),
                    UrlBindPair::new(Some(ip2), 1),
                ],
                stream_binds: vec![],
                rate_overrides: AccountRateOverrides::default(),
                // Spot WS `order.place` charges `ip_weight = 1`: `ip1` has budget `1` while `ip2`
                // has `6,000`, proving single-pick NACK on `ip1` without retrying `ip2`!
                ip_budgets: vec![IpBudgetSpec::new(ip1, 1), IpBudgetSpec::new(ip2, 6_000)],
            },
            BinanceMarketConfig {
                market: BinanceVenueMarket::Usdm,
                auth: BinanceMarketAuth::ed25519_pem("umKey", TEST_ED25519_PEM),
                urls: vec![BinanceUrlId::UmOrderWssColo, BinanceUrlId::UmStreamWssColo],
                order_binds: vec![UrlBindPair::new(Some(ip2), 1)],
                stream_binds: vec![UrlBindPair::new(Some(ip2), 1)],
                rate_overrides: AccountRateOverrides::default(),
                ip_budgets: vec![IpBudgetSpec::new(ip2, 2_400)],
            },
        ],
    );

    let quota = SharedBinanceOrsQuota::new();
    let (session, mut conns) =
        BinanceSession::configure(spec, om.clone(), quota, catalog, None, None).unwrap();
    let (_srv0, epoch0) = install_ws_order_link(&mut conns, BinanceVenueMarket::Spot, 0, ip1);
    let (_srv1, epoch1) = install_ws_order_link(&mut conns, BinanceVenueMarket::Spot, 1, ip2);
    let (_srv_um, _epoch_um) = install_ws_order_link(&mut conns, BinanceVenueMarket::Usdm, 2, ip2);

    // Give Spot `conn_id = 0` (`ip1`) a fast measured EMA score (`100us`) and `conn_id = 1` (`ip2`) `500us`
    // so `OrderRouter::pick` deterministically picks `conn_id = 0` (`ip1`)!
    for _ in 0..8 {
        conns
            .note_order_sample(BinanceVenueMarket::Spot, 0, epoch0, SampleKind::Order, 100)
            .unwrap();
        conns
            .note_order_sample(BinanceVenueMarket::Spot, 1, epoch1, SampleKind::Order, 500)
            .unwrap();
    }

    let mut frame_buf = [0u8; 2048];
    let mut sink = TestSink::default();

    // Spot Order #1 picks `conn_id = 0` (`ip1`), spending `ip1`'s entire 1-weight budget.
    let out_spot1 = session
        .submit_new(
            &default_intent(1, 1, 1, SYM_SPOT_BTCUSDT, 5_000_000, 10),
            None,
            &mut frame_buf,
            &mut sink,
        )
        .unwrap();
    let (spot_oid1, spot_cid1) = match out_spot1 {
        BinanceSubmitOutcome::Sent {
            ors_order_id,
            conn_id: 0,
            client_id,
            ..
        } => (ors_order_id, client_id),
        other => panic!("expected Spot Sent on conn_id 0, got {other:?}"),
    };

    // Spot Order #2 ALSO picks `conn_id = 0` (`ip1`), hits `ip1`'s exhausted budget, and MUST single-shot
    // NACK immediately as `RateLimited` WITHOUT retrying `conn_id = 1` (`ip2`)!
    let out_spot2 = session
        .submit_new(
            &default_intent(1, 2, 2, SYM_SPOT_BTCUSDT, 5_000_000, 10),
            None,
            &mut frame_buf,
            &mut sink,
        )
        .unwrap();
    assert!(matches!(
        out_spot2,
        BinanceSubmitOutcome::LocalNack {
            reason: TerminalReason::RateLimited,
            ..
        }
    ));

    // Also verify Spot `order.place` ack with nested `fills[]` array skip AND Spot `executionReport`
    // cancel correlated via `"C"` (`origClientOrderId`) while `"c"` carries a venue cancel token!
    let spot_ack = format!(
        r#"{{"id":"{}","status":200,"result":{{"symbol":"BTCUSDT","orderId":44401,"orderListId":-1,"clientOrderId":"{}","transactTime":1700000100000,"price":"50000.00","origQty":"0.0010","executedQty":"0.0000","cummulativeQuoteQty":"0.0000","status":"NEW","timeInForce":"GTC","type":"LIMIT","side":"BUY","fills":[]}}}}"#,
        spot_oid1,
        spot_cid1.as_str()
    );
    let raw_spot_ref = conns
        .order_pool(BinanceVenueMarket::Spot)
        .unwrap()
        .connection_handle(0)
        .unwrap()
        .match_ws_reply(session.reply_id(spot_ack.as_bytes()).unwrap())
        .unwrap();
    session
        .on_message(
            BinanceVenueMarket::Spot,
            IpKey::from_ip(ip1),
            spot_ack.as_bytes(),
            BinanceRequestRef::from_transport_ref(raw_spot_ref),
            1_050_000_000,
            &mut sink,
        )
        .unwrap();
    let spot_cxl_push = format!(
        r#"{{"e":"executionReport","E":1700000100050,"s":"BTCUSDT","c":"venueGeneratedCancelToken99","C":"{}","S":"BUY","o":"LIMIT","f":"GTC","q":"0.0010","p":"50000.00","x":"CANCELED","X":"CANCELED","i":44401,"l":"0.0000","z":"0.0000","L":"0.00","n":"0","N":null,"t":-1}}"#,
        spot_cid1.as_str()
    );
    sink.reports.clear();
    session
        .on_message(
            BinanceVenueMarket::Spot,
            IpKey::NONE,
            spot_cxl_push.as_bytes(),
            None,
            1_060_000_000,
            &mut sink,
        )
        .unwrap();
    assert!(sink.reports.iter().any(|r| matches!(
        r,
        OrsReport::Canceled(c)
            if c.ors_order_id == spot_oid1 && c.reason == TerminalReason::MKT_CANCELED
    )));

    // Dispatch USD-M order on `conn_id = 2` (`ip2`) to test `rateLimits[]` + `TRADE_LITE` + maker rebate!
    let out1 = session
        .submit_new(
            &default_intent(1, 3, 3, SYM_USDM_BTCUSDT, 5_000_000, 10),
            None,
            &mut frame_buf,
            &mut sink,
        )
        .unwrap();
    let (oid1, cid1) = match out1 {
        BinanceSubmitOutcome::Sent {
            ors_order_id,
            conn_id: 2,
            client_id,
            ..
        } => (ors_order_id, client_id),
        other => panic!("expected USD-M Sent on conn_id 2, got {other:?}"),
    };

    // Match WS `order.place` ack with `rateLimits[]` at `85%` (`255/300`) -> rising-edge notice!
    let reply_id = pack_ws_reply_id(oid1, BinanceRequestKind::Place);
    let raw_ref = conns
        .order_pool(BinanceVenueMarket::Usdm)
        .unwrap()
        .connection_handle(2)
        .unwrap()
        .match_ws_reply(reply_id)
        .unwrap();
    let ack_json = format!(
        r#"{{"id":"{}","status":200,"result":{{"orderId":55501,"symbol":"BTCUSDT","status":"NEW","clientOrderId":"{}","price":"50000.00","origQty":"0.010","executedQty":"0.000","timeInForce":"GTC"}},"rateLimits":[{{"rateLimitType":"ORDERS","interval":"SECOND","intervalNum":10,"limit":300,"count":255}}]}}"#,
        oid1,
        cid1.as_str()
    );
    session
        .on_message(
            BinanceVenueMarket::Usdm,
            IpKey::from_ip(ip2),
            ack_json.as_bytes(),
            BinanceRequestRef::from_transport_ref(raw_ref),
            1_100_000_000,
            &mut sink,
        )
        .unwrap();
    assert_eq!(sink.rate_notices.len(), 1);
    assert_eq!(sink.rate_notices[0].count, 255);
    assert_eq!(sink.rate_notices[0].limit, 300);
    assert!(!sink.rate_notices[0].hard_limit_hit);

    // 1. `TRADE_LITE` arrives FIRST for full quantity (`0.010` BTC = 10 lots, `t = 777001`, NO fee!).
    //    Order immediately fills & retires its slot (`SlotState::Retired`), updating position to `+10`!
    let trade_lite = format!(
        r#"{{"e":"TRADE_LITE","E":1700000100100,"T":1700000100100,"s":"BTCUSDT","q":"0.010","p":"50000.00","m":true,"c":"{}","S":"BUY","L":"50000.00","l":"0.010","t":777001,"i":55501}}"#,
        cid1.as_str()
    );
    sink.reports.clear();
    session
        .on_message(
            BinanceVenueMarket::Usdm,
            IpKey::NONE,
            trade_lite.as_bytes(),
            None,
            1_200_000_000,
            &mut sink,
        )
        .unwrap();
    assert_eq!(sink.reports.len(), 1);
    match sink.reports[0] {
        OrsReport::Fill(f) => {
            assert_eq!(f.ors_order_id, oid1);
            assert_eq!(f.last_qty_lots, 10);
            assert_eq!(f.cumulative_filled_lots, 10);
            assert!(f.fee.is_none());
            assert!(!f.amendment);
            assert_eq!(f.liquidity, Liquidity::Maker);
        }
        other => panic!("expected Fill report from TRADE_LITE, got {other:?}"),
    }
    assert!(
        session.order_manager().lock_order(oid1).is_none(),
        "full fill via TRADE_LITE must immediately retire the order slot"
    );
    assert_eq!(
        om.wallet_ledger()
            .position_lots(1, SYM_USDM_BTCUSDT as usize),
        Some(10)
    );
    let balance_after_lite = om.wallet_ledger().balance(1, usdm_usdt).unwrap();
    assert_eq!(balance_after_lite.1, 0, "all holds released on full fill");

    // 2. Full `ORDER_TRADE_UPDATE` arrives AFTER slot retirement carrying maker rebate (`n = "-0.05"` USDT)!
    //    `FillRecordPool` deduplicates `t = 777001`, applies ONLY the `+5,000,000` atom maker rebate to
    //    `UtaWalletLedger`, leaves position at `+10` (zero double fill!), and emits `amendment = true`!
    let full_otu = format!(
        r#"{{"e":"ORDER_TRADE_UPDATE","E":1700000100105,"T":1700000100100,"o":{{"s":"BTCUSDT","c":"{}","S":"BUY","o":"LIMIT","f":"GTC","q":"0.010","p":"50000.00","ap":"50000.00","X":"FILLED","x":"TRADE","i":55501,"l":"0.010","z":"0.010","L":"50000.00","n":"-0.05000000","N":"USDT","t":777001,"m":true}}}}"#,
        cid1.as_str()
    );
    sink.reports.clear();
    session
        .on_message(
            BinanceVenueMarket::Usdm,
            IpKey::NONE,
            full_otu.as_bytes(),
            None,
            1_250_000_000,
            &mut sink,
        )
        .unwrap();
    assert_eq!(sink.reports.len(), 1);
    match sink.reports[0] {
        OrsReport::Fill(f) => {
            assert_eq!(f.ors_order_id, oid1);
            assert!(
                f.amendment,
                "post-TRADE_LITE fee patch must set amendment = true"
            );
            assert_eq!(f.fee.unwrap().signed_atoms, 5_000_000); // +0.05 USDT rebate!
        }
        other => panic!("expected amendment Fill report, got {other:?}"),
    }
    assert_eq!(
        om.wallet_ledger()
            .position_lots(1, SYM_USDM_BTCUSDT as usize),
        Some(10),
        "position must NOT be double-applied on post-TRADE_LITE ORDER_TRADE_UPDATE"
    );
    let balance_after_rebate = om.wallet_ledger().balance(1, usdm_usdt).unwrap();
    assert_eq!(balance_after_rebate.0, balance_after_lite.0 + 5_000_000);
}

#[test]
fn query_desk_delayed_place_cancel_backoff_options_link_drop_and_listen_key_expiry() {
    let (symbols, catalog) = four_market_symbols_and_catalog();
    let domain = test_account_domain(1);
    let mut om_cfg = OrderManagerConfig::single_account_default(domain, symbols);
    om_cfg.max_query_count = Some(2);
    let om = Arc::new(OrderManager::<true>::new(om_cfg).unwrap());
    om.dop_state().configure_strategy(1, &[1]).unwrap();
    om.dop_state().login(1, 1).unwrap();

    om.wallet_ledger()
        .set_balance(
            1,
            AssetKey::new(WALLET_USDM, ASSET_USDT),
            8,
            10_000_000_000_000,
            0,
        )
        .unwrap();
    om.wallet_ledger()
        .set_balance(
            1,
            AssetKey::new(WALLET_OPTIONS, ASSET_USDT),
            8,
            10_000_000_000_000,
            0,
        )
        .unwrap();

    let ip1 = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let spec = BinanceAccountSpec::new(
        domain,
        vec![
            BinanceMarketConfig {
                market: BinanceVenueMarket::Usdm,
                auth: BinanceMarketAuth::ed25519_pem("umKey", TEST_ED25519_PEM),
                urls: vec![BinanceUrlId::UmOrderWssColo, BinanceUrlId::UmStreamWssColo],
                order_binds: vec![UrlBindPair::new(Some(ip1), 1)],
                stream_binds: vec![UrlBindPair::new(Some(ip1), 1)],
                rate_overrides: AccountRateOverrides::default(),
                ip_budgets: vec![IpBudgetSpec::new(ip1, 6_000)],
            },
            BinanceMarketConfig {
                market: BinanceVenueMarket::Options,
                auth: BinanceMarketAuth::hmac_sha256("optKey", b"opt-secret".to_vec()),
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
    let (session, mut conns) =
        BinanceSession::configure(spec, om.clone(), quota, catalog, None, None).unwrap();
    let (_um_srv, _) = install_ws_order_link(&mut conns, BinanceVenueMarket::Usdm, 0, ip1);
    let (_opt_srv, _) = install_https_order_link(&mut conns, 1, ip1);

    // Seed `listenKey` via `on_stream_token_reply`, verify colo stream URL, then expire via `listenKeyExpired`!
    assert!(!session.has_stream_token(BinanceVenueMarket::Usdm));
    assert!(
        session
            .on_stream_token_reply(
                BinanceVenueMarket::Usdm,
                200,
                br#"{"listenKey":"lk_um_colo_99"}"#,
            )
            .unwrap()
    );
    assert!(session.has_stream_token(BinanceVenueMarket::Usdm));
    let mut url_buf = [0u8; 256];
    let url_len = session
        .connection_url(BinanceVenueMarket::Usdm, UrlRole::STREAM, &mut url_buf)
        .unwrap();
    assert_eq!(
        core::str::from_utf8(&url_buf[..url_len]).unwrap(),
        "wss://fstream-mm.binance.com/ws/lk_um_colo_99"
    );

    let mut sink = TestSink::default();
    session
        .on_message(
            BinanceVenueMarket::Usdm,
            IpKey::NONE,
            br#"{"e":"listenKeyExpired","E":1700000100999}"#,
            None,
            1_050_000_000,
            &mut sink,
        )
        .unwrap();
    assert!(!session.has_stream_token(BinanceVenueMarket::Usdm));
    assert_eq!(sink.expired_streams, vec![(1, BinanceVenueMarket::Usdm)]);

    let mut frame_buf = [0u8; 2048];

    // 1. USD-M `-1007` timeout on `Place` -> schedules `StatusAsk` at `+6000ms` (`recvWindow=60000`);
    //    `-2013` on `StatusAsk` closes `PendingNew` (`WaitMkt`) as `Rejected` (`NACK`)!
    let t0 = 2_000_000_000u64;
    let mut i1 = default_intent(1, 1, 1, SYM_USDM_BTCUSDT, 5_000_000, 10);
    i1.now_mono_ns = t0;
    let oid1 = match session
        .submit_new(&i1, None, &mut frame_buf, &mut sink)
        .unwrap()
    {
        BinanceSubmitOutcome::Sent { ors_order_id, .. } => ors_order_id,
        other => panic!("unexpected {other:?}"),
    };
    let raw_ref1 = conns
        .order_pool(BinanceVenueMarket::Usdm)
        .unwrap()
        .connection_handle(0)
        .unwrap()
        .match_ws_reply(pack_ws_reply_id(oid1, BinanceRequestKind::Place))
        .unwrap();
    session
        .on_message(
            BinanceVenueMarket::Usdm,
            IpKey::from_ip(ip1),
            br#"{"id":"p1","status":504,"error":{"code":-1007,"msg":"Timeout waiting for response from backend server."}}"#,
            BinanceRequestRef::from_transport_ref(raw_ref1),
            t0 + 10_000_000,
            &mut sink,
        )
        .unwrap();

    // Not due at `+5.9s`, due at `+6.01s`!
    let mut due = [DueQueryAction::StatusAsk {
        ors_order_id: 0,
        account_ref: 0,
        symbol_id: 0,
        attempt: 0,
        cancel_after: false,
    }; 8];
    assert_eq!(
        om.query_desk()
            .poll_due(1, t0 + 5_900_000_000, &mut due, |s, id| om
                .is_slot_live(s, id)),
        0
    );
    assert_eq!(
        om.query_desk()
            .poll_due(1, t0 + 6_020_000_000, &mut due, |s, id| om
                .is_slot_live(s, id)),
        1
    );
    let q_call = session
        .order_status_query(oid1, BinanceRequestKind::Place, 1_700_000_200_000)
        .unwrap();
    assert!(q_call.target.contains("recvWindow=60000"));

    // `-2013` closes `PendingNew` (`WaitMkt`) as `Reconciled` (`OrsReport::Rejected`)!
    sink.reports.clear();
    let out_2013 = session
        .apply_status(
            oid1,
            BinanceRequestKind::Place,
            true,
            400,
            br#"{"code":-2013,"msg":"Order does not exist."}"#,
            t0 + 6_050_000_000,
            &mut sink,
        )
        .unwrap();
    assert_eq!(out_2013, BinanceStatusApplyOutcome::Reconciled);
    assert!(
        sink.reports
            .iter()
            .any(|r| matches!(r, OrsReport::Rejected(rej) if rej.ors_order_id == oid1))
    );

    // 2. Options `HttpsLink` drops with in-flight `Place` -> `on_disconnected` schedules `StatusAsk`;
    //    `max_query_count = 2` transient `503` failures retire as `ClosedInternalCancel` (`INTERNAL_CANCEL`)!
    let mut i2 = default_intent(1, 2, 2, SYM_OPT_BTC_CALL, 10_000, 10);
    i2.now_mono_ns = t0 + 7_000_000_000;
    let oid2 = match session
        .submit_new(&i2, None, &mut frame_buf, &mut sink)
        .unwrap()
    {
        BinanceSubmitOutcome::Sent { ors_order_id, .. } => ors_order_id,
        other => panic!("unexpected {other:?}"),
    };
    let raw_opt_ref = conns
        .order_pool(BinanceVenueMarket::Options)
        .unwrap()
        .connection_handle(1)
        .unwrap()
        .complete_https_reply()
        .unwrap();
    session.on_disconnected(
        1,
        BinanceRequestRef::from_transport_ref(raw_opt_ref),
        t0 + 7_010_000_000,
        &mut sink,
    );
    // Poll due at `+6s`, fail with HTTP 503 #1 (`RetryingPlace` at `+1s`), then HTTP 503 #2 -> `ClosedInternalCancel`!
    assert_eq!(
        om.query_desk()
            .poll_due(1, t0 + 13_020_000_000, &mut due, |s, id| om
                .is_slot_live(s, id)),
        1
    );
    let r1 = session
        .apply_status(
            oid2,
            BinanceRequestKind::Place,
            true,
            503,
            b"Service Unavailable",
            t0 + 13_030_000_000,
            &mut sink,
        )
        .unwrap();
    assert!(matches!(
        r1,
        BinanceStatusApplyOutcome::RetryingPlace { .. }
    ));
    assert_eq!(
        om.query_desk()
            .poll_due(1, t0 + 14_040_000_000, &mut due, |s, id| om
                .is_slot_live(s, id)),
        1
    );
    let r2 = session
        .apply_status(
            oid2,
            BinanceRequestKind::Place,
            true,
            503,
            b"Service Unavailable",
            t0 + 14_050_000_000,
            &mut sink,
        )
        .unwrap();
    assert_eq!(
        r2,
        BinanceStatusApplyOutcome::ClosedInternalCancel { ors_order_id: oid2 }
    );

    // 3. Delayed `Cancel` (`-1007`) answered with `NEW` decrements `pending_cxl` and emits `CancelRejected` (`MKT_REJECT`),
    //    and consecutive `503` errors on a delayed `Cancel` exhaust exponential backoff (`1..64s`)!
    let mut i3 = default_intent(1, 3, 3, SYM_USDM_BTCUSDT, 5_000_000, 10);
    i3.now_mono_ns = t0 + 20_000_000_000;
    let (oid3, cid3) = match session
        .submit_new(&i3, None, &mut frame_buf, &mut sink)
        .unwrap()
    {
        BinanceSubmitOutcome::Sent {
            ors_order_id,
            client_id,
            ..
        } => (ors_order_id, client_id),
        other => panic!("unexpected {other:?}"),
    };
    let _ = conns
        .order_pool(BinanceVenueMarket::Usdm)
        .unwrap()
        .connection_handle(0)
        .unwrap()
        .match_ws_reply(pack_ws_reply_id(oid3, BinanceRequestKind::Place));
    let ack3 = format!(
        r#"{{"e":"ORDER_TRADE_UPDATE","o":{{"s":"BTCUSDT","c":"{}","X":"NEW","x":"NEW","i":88803,"l":"0","z":"0","p":"50000.00","q":"0.010"}}}}"#,
        cid3.as_str()
    );
    session
        .on_message(
            BinanceVenueMarket::Usdm,
            IpKey::NONE,
            ack3.as_bytes(),
            None,
            t0 + 20_010_000_000,
            &mut sink,
        )
        .unwrap();

    let cxl_out = session
        .submit_cancel(
            &OrsCancelRequest {
                strat_id: 1,
                account_ref: 1,
                msg_seqnum: 4,
                ors_order_id: oid3,
                now_mono_ns: t0 + 20_020_000_000,
            },
            None,
            1_700_000_300_000,
            &mut frame_buf,
            &mut sink,
        )
        .unwrap();
    assert!(matches!(cxl_out, BinanceCancelOutcome::Sent { .. }));
    let raw_cxl_ref = conns
        .order_pool(BinanceVenueMarket::Usdm)
        .unwrap()
        .connection_handle(0)
        .unwrap()
        .match_ws_reply(pack_ws_reply_id(oid3, BinanceRequestKind::Cancel))
        .unwrap();
    session
        .on_message(
            BinanceVenueMarket::Usdm,
            IpKey::from_ip(ip1),
            br#"{"id":"c3","status":504,"error":{"code":-1007,"msg":"Timeout"}}"#,
            BinanceRequestRef::from_transport_ref(raw_cxl_ref),
            t0 + 20_030_000_000,
            &mut sink,
        )
        .unwrap();
    assert_eq!(
        om.query_desk()
            .poll_due(1, t0 + 26_040_000_000, &mut due, |s, id| om
                .is_slot_live(s, id)),
        1
    );
    let still_new = format!(
        r#"{{"orderId":88803,"symbol":"BTCUSDT","status":"NEW","clientOrderId":"{}","origQty":"0.010","executedQty":"0.000"}}"#,
        cid3.as_str()
    );
    sink.reports.clear();
    let cxl_status = session
        .apply_status(
            oid3,
            BinanceRequestKind::Cancel,
            true,
            200,
            still_new.as_bytes(),
            t0 + 26_050_000_000,
            &mut sink,
        )
        .unwrap();
    assert_eq!(
        cxl_status,
        BinanceStatusApplyOutcome::CancelRejectedOpen {
            ors_order_id: oid3,
            remaining_pending_cxl: 0,
        }
    );
    assert!(sink.reports.iter().any(|r| matches!(
        r,
        OrsReport::CancelRejected(cr)
            if cr.ors_order_id == oid3 && cr.reason == TerminalReason::MKT_REJECT
    )));

    // Now send another Cancel on `oid3`, trigger `-1007`, and walk the `1, 2, 4, 8, 16, 32, 64s`
    // exponential backoff across 8 transient `503` responses until `exhausted_cancels` fires!
    let _ = session
        .submit_cancel(
            &OrsCancelRequest {
                strat_id: 1,
                account_ref: 1,
                msg_seqnum: 5,
                ors_order_id: oid3,
                now_mono_ns: t0 + 30_000_000_000,
            },
            None,
            1_700_000_310_000,
            &mut frame_buf,
            &mut sink,
        )
        .unwrap();
    let raw_cxl_ref2 = conns
        .order_pool(BinanceVenueMarket::Usdm)
        .unwrap()
        .connection_handle(0)
        .unwrap()
        .match_ws_reply(pack_ws_reply_id(oid3, BinanceRequestKind::Cancel))
        .unwrap();
    session
        .on_message(
            BinanceVenueMarket::Usdm,
            IpKey::from_ip(ip1),
            br#"{"id":"c4","status":504,"error":{"code":-1007,"msg":"Timeout"}}"#,
            BinanceRequestRef::from_transport_ref(raw_cxl_ref2),
            t0 + 30_010_000_000,
            &mut sink,
        )
        .unwrap();

    let mut cursor_ns = t0 + 36_020_000_000;
    let expected_backoffs_s = [1u64, 2, 4, 8, 16, 32, 64];
    for &step_s in &expected_backoffs_s {
        assert_eq!(
            om.query_desk()
                .poll_due(1, cursor_ns, &mut due, |s, id| om.is_slot_live(s, id)),
            1
        );
        let out = session
            .apply_status(
                oid3,
                BinanceRequestKind::Cancel,
                true,
                503,
                b"Service Unavailable",
                cursor_ns,
                &mut sink,
            )
            .unwrap();
        let next_ns = cursor_ns + step_s * 1_000_000_000;
        assert_eq!(
            out,
            BinanceStatusApplyOutcome::RetryingCancel {
                ors_order_id: oid3,
                next_due_ns: Some(next_ns),
            }
        );
        cursor_ns = next_ns + 1_000_000;
    }
    // 8th consecutive `503` exhausts the cancel backoff schedule (`next_due_ns == None`)!
    assert_eq!(
        om.query_desk()
            .poll_due(1, cursor_ns, &mut due, |s, id| om.is_slot_live(s, id)),
        1
    );
    let exhausted = session
        .apply_status(
            oid3,
            BinanceRequestKind::Cancel,
            true,
            503,
            b"Service Unavailable",
            cursor_ns,
            &mut sink,
        )
        .unwrap();
    assert_eq!(
        exhausted,
        BinanceStatusApplyOutcome::RetryingCancel {
            ors_order_id: oid3,
            next_due_ns: None,
        }
    );
    assert_eq!(sink.exhausted_cancels, vec![(1, oid3)]);
}

#[test]
fn send_sync_multithread_concurrency_and_stateless_cold_rest_bootstrap() {
    let (symbols, catalog) = four_market_symbols_and_catalog();
    let domain = test_account_domain(1);
    let om = Arc::new(
        OrderManager::<true>::new(OrderManagerConfig::single_account_default(domain, symbols))
            .unwrap(),
    );
    om.dop_state().configure_strategy(1, &[1]).unwrap();
    om.dop_state().configure_strategy(2, &[1]).unwrap();

    // Generate authentic `tw1...` `SelfDescribingClientId`s via a scratch `OrderManager`!
    let (tmp_syms, _) = four_market_symbols_and_catalog();
    let tmp_om =
        OrderManager::<true>::new(OrderManagerConfig::single_account_default(domain, tmp_syms))
            .unwrap();
    tmp_om.dop_state().configure_strategy(2, &[1]).unwrap();
    tmp_om.dop_state().login(2, 40).unwrap();
    tmp_om
        .wallet_ledger()
        .set_balance(
            1,
            AssetKey::new(WALLET_USDM, ASSET_USDT),
            8,
            10_000_000_000_000,
            0,
        )
        .unwrap();
    let placed_tw1 = tmp_om
        .place_order(
            &BinanceNewOrderIntent {
                strat_id: 2,
                account_ref: 1,
                msg_seqnum: 1,
                order_seqnum: 42,
                symbol_id: SYM_USDM_BTCUSDT,
                side: Side::Buy,
                order_type: OrderType::Limit,
                price_ticks: 4_800_000,
                qty_lots: 20,
                tif: TimeInForce::Gtc,
                post_only: true,
                reduce_only: false,
                position_side: PositionSide::Both,
                stp_mode: SelfTradePrevention::Default,
                check_stp: true,
                dry_run: true,
                wall_ms: 1_700_000_000_000,
                now_mono_ns: 100,
            }
            .to_ors_request(),
        )
        .unwrap();
    let tw1_cid = placed_tw1.client_id;

    let ip1 = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let spec = BinanceAccountSpec::new(
        domain,
        vec![BinanceMarketConfig {
            market: BinanceVenueMarket::Usdm,
            auth: BinanceMarketAuth::ed25519_pem("umKey", TEST_ED25519_PEM),
            urls: vec![BinanceUrlId::UmOrderWssColo, BinanceUrlId::UmStreamWssColo],
            order_binds: vec![UrlBindPair::new(Some(ip1), 1)],
            stream_binds: vec![UrlBindPair::new(Some(ip1), 1)],
            rate_overrides: AccountRateOverrides::default(),
            ip_budgets: vec![IpBudgetSpec::new(ip1, 6_000)],
        }],
    );
    let quota = SharedBinanceOrsQuota::new();
    let (session, mut conns) =
        BinanceSession::configure(spec, om.clone(), quota, catalog, None, None).unwrap();
    let (_srv, _) = install_ws_order_link(&mut conns, BinanceVenueMarket::Usdm, 0, ip1);

    // 1. Cold bootstrap USD-M from authenticated Exchange REST `Account + OpenOrders` snapshots!
    let account_json = br#"{
        "assets":[{"asset":"USDT","walletBalance":"50000.00000000","availableBalance":"49000.00000000"}],
        "positions":[{"symbol":"BTCUSDT","positionAmt":"0.050","positionSide":"BOTH"}]
    }"#;
    let open_orders_json = format!(
        r#"[
            {{"orderId":70001,"symbol":"BTCUSDT","status":"NEW","clientOrderId":"{}","price":"48000.00","origQty":"0.020","executedQty":"0.005","side":"BUY","type":"LIMIT","timeInForce":"GTX","reduceOnly":false}},
            {{"orderId":70002,"symbol":"BTCUSDT","status":"NEW","clientOrderId":"manual-ui-order-1","price":"47000.00","origQty":"0.010","executedQty":"0.000","side":"BUY","type":"LIMIT","timeInForce":"GTC","reduceOnly":false}}
        ]"#,
        tw1_cid.as_str()
    );
    let summary = session
        .bootstrap_market_from_rest(
            BinanceVenueMarket::Usdm,
            account_json,
            open_orders_json.as_bytes(),
            None,
            500_000_000,
        )
        .unwrap();
    assert_eq!(summary.managed_open_orders, 1);
    assert_eq!(summary.external_open_orders, 1);

    // Strategy 2 login receives recovered `next_order_seqnum >= 43` floor without any local WAL!
    let login_ack = om.dop_state().login(2, 1).unwrap();
    assert_eq!(login_ack.next_order_seqnum, 43);
    assert_eq!(
        om.wallet_ledger()
            .position_lots(1, SYM_USDM_BTCUSDT as usize),
        Some(50)
    );

    // 2. Spawn 4 concurrent threads (`REQ`, `RESP`, `Dialer`, `REST_QUERY`) sharing `Arc<BinanceSession>`!
    om.dop_state().login(1, 1).unwrap();
    std::thread::scope(|s| {
        let s_req = Arc::clone(&session);
        let t_req = s.spawn(move || {
            let mut buf = [0u8; 2048];
            let mut sink = TestSink::default();
            for seq in 1..=16u64 {
                let mut intent = default_intent(1, seq, seq, SYM_USDM_BTCUSDT, 4_750_000, 1);
                intent.dry_run = true;
                let res = s_req
                    .submit_new(&intent, None, &mut buf, &mut sink)
                    .unwrap();
                assert!(matches!(res, BinanceSubmitOutcome::DryRunEncoded { .. }));
            }
        });

        let s_dial = Arc::clone(&session);
        let t_dial = s.spawn(move || {
            let mut out = [0u8; 1024];
            for i in 0..16u64 {
                let len = s_dial
                    .encode_login(
                        BinanceVenueMarket::Usdm,
                        UrlRole::ORDER,
                        "dial-logon",
                        1_700_000_000_000 + i,
                        &mut out,
                    )
                    .unwrap()
                    .unwrap();
                assert!(len > 64);
                let _ = s_dial.has_stream_token(BinanceVenueMarket::Usdm);
            }
        });

        let s_rest = Arc::clone(&session);
        let t_rest = s.spawn(move || {
            for i in 0..16u64 {
                let body = format!(r#"{{"listenKey":"concurrent_lk_{i}"}}"#);
                assert!(
                    s_rest
                        .on_stream_token_reply(BinanceVenueMarket::Usdm, 200, body.as_bytes())
                        .unwrap()
                );
                let qs = s_rest.open_orders_queries(1_700_000_000_000 + i).unwrap();
                assert_eq!(qs.len(), 1);
            }
        });

        let s_resp = Arc::clone(&session);
        let t_resp = s.spawn(move || {
            let mut sink = TestSink::default();
            for i in 0..16u64 {
                let msg = format!(
                    r#"{{"id":"noop-{i}","status":200,"result":{{"authorizedSince":1700000000000}}}}"#
                );
                assert_eq!(s_resp.login_reply(msg.as_bytes()), BinanceLoginReply::Ok);
                s_resp
                    .on_message(
                        BinanceVenueMarket::Usdm,
                        IpKey::NONE,
                        msg.as_bytes(),
                        None,
                        600_000_000 + i,
                        &mut sink,
                    )
                    .unwrap();
            }
        });

        t_req.join().unwrap();
        t_dial.join().unwrap();
        t_rest.join().unwrap();
        t_resp.join().unwrap();
    });

    // Also verify native `submit_modify` and unsolicited venue `AMENDMENT` handling!
    let mut buf = [0u8; 2048];
    let mut sink = TestSink::default();
    let placed_live = session
        .submit_new(
            &default_intent(1, 17, 17, SYM_USDM_BTCUSDT, 4_700_000, 10),
            None,
            &mut buf,
            &mut sink,
        )
        .unwrap();
    let (live_oid, live_cid) = match placed_live {
        BinanceSubmitOutcome::Sent {
            ors_order_id,
            client_id,
            ..
        } => (ors_order_id, client_id),
        other => panic!("expected Sent, got {other:?}"),
    };
    let ack_live = format!(
        r#"{{"e":"ORDER_TRADE_UPDATE","o":{{"s":"BTCUSDT","c":"{}","X":"NEW","x":"NEW","i":99100,"l":"0","z":"0","p":"47000.00","q":"0.010"}}}}"#,
        live_cid.as_str()
    );
    session
        .on_message(
            BinanceVenueMarket::Usdm,
            IpKey::NONE,
            ack_live.as_bytes(),
            None,
            700_000_000,
            &mut sink,
        )
        .unwrap();

    // Unsolicited venue `AMENDMENT` (`modify_pending == false`) notifies `on_venue_amendment` without a strategy `Modified` report!
    sink.reports.clear();
    let venue_amend = format!(
        r#"{{"e":"ORDER_TRADE_UPDATE","o":{{"s":"BTCUSDT","c":"{}","X":"NEW","x":"AMENDMENT","i":99100,"l":"0","z":"0","p":"46990.00","q":"0.008"}}}}"#,
        live_cid.as_str()
    );
    session
        .on_message(
            BinanceVenueMarket::Usdm,
            IpKey::NONE,
            venue_amend.as_bytes(),
            None,
            710_000_000,
            &mut sink,
        )
        .unwrap();
    assert!(sink.reports.is_empty());
    assert_eq!(sink.venue_amendments, vec![(1, live_oid, 4_699_000, 8)]);

    // Strategy-initiated `submit_modify` (`modify_pending == true`) emits `OrsReport::Modified`!
    let mod_out = session
        .submit_modify(
            &OrsModifyRequest {
                strat_id: 1,
                account_ref: 1,
                msg_seqnum: 18,
                ors_order_id: live_oid,
                new_price_ticks: 4_701_000,
                new_qty_lots: 9,
                now_mono_ns: 720_000_000,
            },
            None,
            1_700_000_400_000,
            &mut buf,
            &mut sink,
        )
        .unwrap();
    assert!(matches!(mod_out, BinanceModifyOutcome::Sent { .. }));
    let strat_amend = format!(
        r#"{{"e":"ORDER_TRADE_UPDATE","o":{{"s":"BTCUSDT","c":"{}","X":"NEW","x":"AMENDMENT","i":99100,"l":"0","z":"0","p":"47010.00","q":"0.009"}}}}"#,
        live_cid.as_str()
    );
    session
        .on_message(
            BinanceVenueMarket::Usdm,
            IpKey::NONE,
            strat_amend.as_bytes(),
            None,
            730_000_000,
            &mut sink,
        )
        .unwrap();
    assert!(
        sink.reports
            .iter()
            .any(|r| matches!(r, OrsReport::Modified(m) if m.ors_order_id == live_oid))
    );

    // Confirm STP self-cross refusal works cleanly on `submit_new`.
    let crossing_sell = BinanceNewOrderIntent {
        strat_id: 1,
        account_ref: 1,
        msg_seqnum: 19,
        order_seqnum: 19,
        symbol_id: SYM_USDM_BTCUSDT,
        side: Side::Sell,
        order_type: OrderType::Limit,
        price_ticks: 4_700_000, // <= resting Buy at 4_701_000!
        qty_lots: 1,
        tif: TimeInForce::Gtc,
        post_only: false,
        reduce_only: false,
        position_side: PositionSide::Both,
        stp_mode: SelfTradePrevention::Default,
        check_stp: true,
        dry_run: false,
        wall_ms: 1_700_000_500_000,
        now_mono_ns: 740_000_000,
    };
    let stp_err = session
        .submit_new(&crossing_sell, None, &mut buf, &mut sink)
        .unwrap_err();
    assert_eq!(stp_err.reason, RefusalReason::MKT_CANCELED_SELFTRADE);
}
