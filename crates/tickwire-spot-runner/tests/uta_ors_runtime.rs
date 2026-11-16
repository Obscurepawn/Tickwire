//! End-to-end integration tests for `tickwire-spot-runner` (`UtaVenueSession`, `OrsProcess`,
//! 6-product UTA routing, `TimeInForce::Rpi`, `max_order_age_ns = 23_000` `RejectStaleOrder`,
//! `dry_run`, cold Exchange REST `tw1...` `SelfDescribingClientId` recovery, `execution.fast` ->
//! `execution` fee `amendment`, `OrsQueryDesk` `StatusAsk`, single-pick `SharedRateLimiter` NACK,
//! `RfqQuoteHook`, server-maintenance `notice` graceful switchover, zero-allocation hot-path
//! warmup, and loopback TLS `paper` mode for Bybit V5 and OKX V5).

#![forbid(unsafe_code)]

use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::json;
use tickwire_account::{
    OrderManagerConfig, OrsExchangeBootstrapSnapshot, OrsRestBalanceRow, OrsRestOpenOrderRow,
    OrsSymbolSpec, make_ors_order_id,
};
use tickwire_account_gateway::AccountManager;
use tickwire_control_sys::{OrsSignal, PrivateDirectory};
use tickwire_ipc::{ResourceHeader, intent_flags};
use tickwire_quota::shared::IpBudgetSpec;
use tickwire_risk::RmsEngine;
use tickwire_runtime::{
    ControlServerConfig, OrderRoutingSystem, OrsCpuConfig, OrsProcess, OrsProcessConfig,
    OrsRuntimeConfig, OrsSignalOutcome, SessionRestEndpoint,
};
use tickwire_spot_execution::network::OwnedCredentials;
use tickwire_spot_execution::rest_bootstrap::{UtaAssetBinding, UtaSymbolBinding};
use tickwire_spot_execution::rfq::{DEFAULT_MAX_OPEN_RFQ_QUOTES, UtaRfqQuoteBook};
use tickwire_spot_runner::{
    Environment, SpotVenue, UtaEndpointSpec, UtaTransportMode, UtaVenueSession,
    UtaVenueSessionConfig, run_paper_demo,
};
use tickwire_strategy_host::{prepare_cross_thread_intent_lane, prepare_cross_thread_report_lane};
use tickwire_strategy_sdk::{
    DopConfig, DopSession, ExecutionReport, ExecutionReportKind, FillKind, IntentWriter,
    OutboundIntent, ReportIdentity, ReportReceiver,
};
use tickwire_transport::links::{LinkOps, WsLink};
use tickwire_transport::rest_querier::DialSecurity;
use tickwire_transport::{Connection, IoBudget, Limits, Security};
use tickwire_venue::{
    AssetKey, ClientIdDomain, Liquidity, OrderType, Product, RefusalReason, SelfDescribingClientId,
    Side, TerminalReason, TimeInForce,
};

const STRAT_1: u8 = 1;
const STRAT_2: u8 = 2;
const ACCT_REF: u8 = 1;
const EXCHANGE_UID: u64 = 777_001;
const RISK_DOMAIN: [u8; 16] = [0x5A; 16];
const ORS_INSTANCE: [u8; 16] = [0x77; 16];

fn deterministic_clock(initial_ns: u64) -> (Arc<AtomicU64>, tickwire_transport::ClockFn) {
    let cell = Arc::new(AtomicU64::new(initial_ns));
    let reader = Arc::clone(&cell);
    let clock_fn: tickwire_transport::ClockFn = Arc::new(move || reader.load(Ordering::Acquire));
    (cell, clock_fn)
}

fn six_product_symbols(venue: SpotVenue) -> Vec<UtaSymbolBinding> {
    let names = match venue {
        SpotVenue::Bybit => [
            (0, "BTCUSDT", Product::Spot, 1, 2, 2),
            (1, "ETHUSDT", Product::LinearPerpetual, 3, 2, 2),
            (2, "BTCUSD", Product::InversePerpetual, 1, 2, 1),
            (3, "BTC-26DEC26", Product::LinearFuture, 1, 2, 2),
            (4, "BTCUSDZ26", Product::InverseFuture, 1, 2, 1),
            (5, "BTC-26DEC26-90000-C", Product::Option, 1, 2, 2),
        ],
        SpotVenue::Okx => [
            (0, "BTC-USDT", Product::Spot, 1, 2, 2),
            (1, "ETH-USDT-SWAP", Product::LinearPerpetual, 3, 2, 2),
            (2, "BTC-USD-SWAP", Product::InversePerpetual, 1, 2, 1),
            (3, "BTC-USDT-261225", Product::LinearFuture, 1, 2, 2),
            (4, "BTC-USD-261225", Product::InverseFuture, 1, 2, 1),
            (5, "BTC-USD-261225-90000-C", Product::Option, 1, 2, 2),
        ],
    };
    names
        .into_iter()
        .map(|(sid, name, prod, base_id, quote_id, settle_id)| {
            let spec = OrsSymbolSpec {
                symbol_id: sid,
                instrument_id: u64::from(sid),
                product: prod,
                wallet_id: 0,
                base_asset_id: base_id,
                base_scale: 8,
                quote_asset_id: quote_id,
                quote_scale: 8,
                settlement_asset_id: settle_id,
                settlement_scale: 8,
                price_tick_atoms: 1_000_000, // 0.01
                price_scale: 8,
                qty_step_atoms: 100_000, // 0.001
                qty_scale: 8,
                contract_size_atoms: 1,
                contract_scale: 0,
                initial_margin_bps: if prod == Product::Spot { 10_000 } else { 1_000 },
            };
            UtaSymbolBinding::new(sid, name, spec).unwrap()
        })
        .collect()
}

fn uta_assets() -> Vec<UtaAssetBinding> {
    vec![
        UtaAssetBinding::new(
            "BTC",
            AssetKey {
                wallet_id: 0,
                asset_id: 1,
            },
            8,
        )
        .unwrap(),
        UtaAssetBinding::new(
            "USDT",
            AssetKey {
                wallet_id: 0,
                asset_id: 2,
            },
            8,
        )
        .unwrap(),
        UtaAssetBinding::new(
            "ETH",
            AssetKey {
                wallet_id: 0,
                asset_id: 3,
            },
            8,
        )
        .unwrap(),
    ]
}

fn build_account_manager(venue: SpotVenue, strats: Vec<u8>) -> AccountManager {
    let url_id = format!("{}-uta-order", venue.as_str());
    let doc = json!({
        "enabled_accounts": ["uta_main"],
        "uta_main": {
            "account_ref": ACCT_REF,
            "exchange_account_id": EXCHANGE_UID,
            "strategies": strats,
            "local_ip": "127.0.0.1",
            "epoch": 1,
            "risk_domain_id": RISK_DOMAIN,
            "credentials": {
                "key": "uta-key",
                "secret": "uta-secret",
                "passphrase": "uta-pass"
            },
            "urls": {
                url_id: { "local_ip/connection_count": [["127.0.0.1", 1]] }
            }
        }
    });
    AccountManager::from_json_str(&doc.to_string()).unwrap()
}

fn enabled_rms(symbol_count: usize) -> RmsEngine {
    let engine = RmsEngine::new(symbol_count);
    let mut rules = String::new();
    for sid in 0..symbol_count {
        rules.push_str(&format!(
            "symbol_id={sid} enabled=true max_qty=1000000 max_notional=1000000000000 new_count=1000/5000/10000 order_qty=1000000/5000000/10000000\n"
        ));
    }
    engine.reload_handle().reload_limits(&rules).unwrap();
    engine
}

fn connect_ws_pair() -> (LinkOps, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut client = Connection::connect_from(
        addr,
        Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        Security::Plain,
        Limits::default(),
        deadline,
    )
    .unwrap();
    let (server, _) = listener.accept().unwrap();
    server.set_nonblocking(true).unwrap();
    server.set_nodelay(true).unwrap();
    while !client.is_ready() {
        assert!(Instant::now() < deadline, "TCP connect timed out");
        client.drive(Instant::now(), IoBudget::default()).unwrap();
        std::thread::sleep(Duration::from_millis(1));
    }
    let ws_link = WsLink::new(client, 64 * 1024, 64 * 1024).unwrap();
    (LinkOps::Ws(ws_link), server)
}

fn target_arch_code() -> u16 {
    if cfg!(target_arch = "x86_64") {
        1
    } else if cfg!(target_arch = "aarch64") {
        2
    } else {
        panic!("unsupported architecture")
    }
}

fn make_lanes(
    strat_id: u8,
) -> (
    tickwire_strategy_host::CrossThreadIntentStarter,
    tickwire_strategy_host::CrossThreadIntentReaderFactory,
    tickwire_strategy_host::CrossThreadReportStarter,
    tickwire_strategy_host::CrossThreadReportWriterFactory,
) {
    let strat_inst = [strat_id.wrapping_add(0x20); 16];
    let capacity = 128u32;

    let ih = ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
        abi_minor: 0,
        header_bytes: 4096,
        resource_kind: 2,
        endian: 1,
        atomic_width: 8,
        page_shift: 12,
        isolation_shift: 7,
        target_arch: target_arch_code(),
        resource_id: [strat_id.wrapping_add(0x30); 16],
        channel_id: [strat_id.wrapping_add(0x40); 16],
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        producer_instance: strat_inst,
        producer_generation: 1,
        consumer_instance: ORS_INSTANCE,
        host_boot_id: [0x11; 16],
        namespace_id: [0x22; 16],
        risk_scope: 1,
        producer_page_offset: 4096,
        slot_offset: 8192,
        capacity,
        record_bytes: 1024,
        resource_bytes: 8192 + u64::from(capacity) * 1024,
        first_seq: 1,
        initial_membership_version: 1,
        max_consumers: 1,
        required_features: 0x1c | tickwire_strategy_sdk::outbound::FEATURE,
        schema_major: 1,
        clock_domain: 1,
        qos: 1,
        event_class: 8,
        ..Default::default()
    };

    let rh = ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
        abi_minor: 0,
        header_bytes: 4096,
        resource_kind: 3,
        endian: 1,
        atomic_width: 8,
        page_shift: 12,
        isolation_shift: 7,
        target_arch: target_arch_code(),
        resource_id: [strat_id.wrapping_add(0x50); 16],
        channel_id: [strat_id.wrapping_add(0x60); 16],
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        producer_instance: ORS_INSTANCE,
        producer_generation: 1,
        consumer_instance: strat_inst,
        host_boot_id: [0x11; 16],
        namespace_id: [0x22; 16],
        risk_scope: 1,
        producer_page_offset: 4096,
        slot_offset: 8192,
        capacity,
        record_bytes: 512,
        resource_bytes: 8192 + u64::from(capacity) * 512,
        first_seq: 1,
        initial_membership_version: 1,
        max_consumers: 1,
        required_features: 0x1c | tickwire_strategy_sdk::reports::FEATURE,
        schema_major: 1,
        clock_domain: 1,
        qos: 1,
        event_class: 9,
        ..Default::default()
    };

    let (is, ifac) =
        prepare_cross_thread_intent_lane(ih, 1, [strat_id.wrapping_add(0x80); 16], strat_id, 1)
            .unwrap();
    let (rs, rfac) = prepare_cross_thread_report_lane(
        rh,
        1,
        [strat_id.wrapping_add(0x90); 16],
        ReportIdentity {
            strat_id,
            instance_id: strat_inst,
            strategy_generation: 1,
        },
    )
    .unwrap();
    (is, ifac, rs, rfac)
}

fn recv_report(rx: &mut ReportReceiver) -> ExecutionReport {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(rep) = rx.poll().unwrap() {
            return rep;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for ExecutionReport"
        );
        std::thread::sleep(Duration::from_micros(250));
    }
}

fn try_read_ws_client_text(stream: &mut TcpStream, raw_buf: &mut Vec<u8>) -> Option<String> {
    let mut tmp = [0u8; 4096];
    loop {
        match stream.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => raw_buf.extend_from_slice(&tmp[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("unexpected server read error: {e}"),
        }
    }
    if raw_buf.len() < 6 {
        return None;
    }
    let b1 = raw_buf[1];
    assert_ne!(b1 & 0x80, 0, "client WS frame must be masked");
    let len_code = usize::from(b1 & 0x7f);
    let (hdr_len, payload_len) = if len_code < 126 {
        (2usize, len_code)
    } else if len_code == 126 {
        if raw_buf.len() < 8 {
            return None;
        }
        (
            4usize,
            usize::from(u16::from_be_bytes([raw_buf[2], raw_buf[3]])),
        )
    } else {
        panic!("unexpected 64-bit WS client test frame");
    };
    let total_len = hdr_len + 4 + payload_len;
    if raw_buf.len() < total_len {
        return None;
    }
    let mask = [
        raw_buf[hdr_len],
        raw_buf[hdr_len + 1],
        raw_buf[hdr_len + 2],
        raw_buf[hdr_len + 3],
    ];
    let mut unmasked = Vec::with_capacity(payload_len);
    for (idx, &b) in raw_buf[hdr_len + 4..total_len].iter().enumerate() {
        unmasked.push(b ^ mask[idx % 4]);
    }
    raw_buf.drain(..total_len);
    Some(String::from_utf8(unmasked).unwrap())
}

fn read_client_ws_json(server: &mut TcpStream, raw_buf: &mut Vec<u8>) -> String {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(msg) = try_read_ws_client_text(server, raw_buf) {
            if msg == "ping" || msg == r#"{"op":"ping"}"# {
                continue;
            }
            return msg;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for outbound WS frame"
        );
        std::thread::sleep(Duration::from_micros(250));
    }
}

fn send_server_ws_json(server: &mut TcpStream, payload: &str) {
    let bytes = payload.as_bytes();
    let mut frame = Vec::with_capacity(bytes.len() + 10);
    frame.push(0x81);
    if bytes.len() < 126 {
        frame.push(bytes.len() as u8);
    } else {
        frame.push(126);
        frame.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    }
    frame.extend_from_slice(bytes);
    server.write_all(&frame).unwrap();
    server.flush().unwrap();
}

#[test]
fn bybit_six_product_uta_rpi_fast_exec_amendment_stale_gate_and_rfq() {
    let (clock_cell, clock_fn) = deterministic_clock(10_000_000_000);
    let symbols = six_product_symbols(SpotVenue::Bybit);
    let assets = uta_assets();

    let spot_domain = ClientIdDomain {
        risk_domain_id: RISK_DOMAIN,
        venue: SpotVenue::Bybit.venue_id(),
        environment: Environment::Test,
        account: EXCHANGE_UID,
        product: Product::Spot,
    };
    let recovered_cid = SelfDescribingClientId::from_ors_order(STRAT_1, 0, 1, 7)
        .unwrap()
        .encode(&spot_domain)
        .unwrap()
        .as_str()
        .to_owned();

    let bootstrap = OrsExchangeBootstrapSnapshot {
        balances: vec![
            OrsRestBalanceRow {
                key: AssetKey {
                    wallet_id: 0,
                    asset_id: 1,
                },
                scale: 8,
                free_atoms: 10_000_000_000,
                locked_atoms: 0,
            },
            OrsRestBalanceRow {
                key: AssetKey {
                    wallet_id: 0,
                    asset_id: 2,
                },
                scale: 8,
                free_atoms: 500_000_000_000_000,
                locked_atoms: 5_000_000_000,
            },
            OrsRestBalanceRow {
                key: AssetKey {
                    wallet_id: 0,
                    asset_id: 3,
                },
                scale: 8,
                free_atoms: 50_000_000_000,
                locked_atoms: 0,
            },
        ],
        positions: Vec::new(),
        open_orders: vec![OrsRestOpenOrderRow {
            symbol_id: 0,
            client_id: recovered_cid,
            venue_order_id: String::from("bybit-boot-7"),
            side: Side::Buy,
            tif: TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            price_ticks: 5_000_000,
            orig_qty_lots: 1,
            cum_filled_lots: 0,
            created_mono_ns: 9_000_000_000,
        }],
        balances_include_open_order_locks: true,
    };

    let session = Arc::new(
        UtaVenueSession::new(UtaVenueSessionConfig {
            venue: SpotVenue::Bybit,
            environment: Environment::Test,
            exchange_uid: EXCHANGE_UID,
            risk_domain_id: RISK_DOMAIN,
            credentials: OwnedCredentials::bybit("uta-key", "uta-secret").unwrap(),
            transport_mode: UtaTransportMode::WebSocketTrade,
            order_endpoint: UtaEndpointSpec::manual("/v5/trade"),
            stream_endpoint: None,
            rest_endpoint: None,
            symbols: symbols.clone(),
            assets,
            ip_order_weight: 1,
            status_query_delay_ms: 100,
            okx_ws_auto_subscribe_private: false,
            rfq_hook: Some(Box::new(UtaRfqQuoteBook::new(DEFAULT_MAX_OPEN_RFQ_QUOTES))),
            clock_utc_ms: Some(Arc::new(|| 1_700_000_000_000)),
            bootstrap_override: Some(bootstrap),
        })
        .unwrap(),
    );

    let acct_mgr = build_account_manager(SpotVenue::Bybit, vec![STRAT_1]);
    let mut om_cfg = OrderManagerConfig::single_account_default(
        acct_mgr.specs()[0].to_domain_config(SpotVenue::Bybit.venue_id(), Environment::Test),
        symbols.iter().map(|b| b.spec).collect(),
    );
    om_cfg.slots_per_strat = 512;
    let rms = enabled_rms(symbols.len());

    let (is1, ifac1, rs1, rfac1) = make_lanes(STRAT_1);
    let (ws_link, mut server_sock) = connect_ws_pair();

    let sock_dir = PrivateDirectory::create_in(&std::env::temp_dir(), "tw-bybit-test").unwrap();
    let dop_sock = sock_dir.path().join("dop.sock");

    let rt_cfg = OrsRuntimeConfig {
        cpu: OrsCpuConfig::default(),
        max_order_age_ns: 23_000,
        clock: clock_fn,
        ..OrsRuntimeConfig::default()
    };
    let proc_cfg = OrsProcessConfig {
        control_socket_path: Some(dop_sock.clone()),
        control_config: ControlServerConfig {
            ors_instance: ORS_INSTANCE,
            ..ControlServerConfig::default()
        },
        shutdown_drain_timeout: Duration::from_millis(200),
        block_signals_on_spawn: false,
        ..OrsProcessConfig::default()
    };

    let process = OrsProcess::build_and_spawn(
        rt_cfg,
        &acct_mgr,
        &[STRAT_1],
        om_cfg,
        rms,
        vec![(ACCT_REF, session)],
        proc_cfg,
        move |ors: &mut OrderRoutingSystem<UtaVenueSession>| {
            ors.install_link(ACCT_REF, 1, ws_link)?;
            ors.attach_strategy_lane_factories(STRAT_1, ifac1, rfac1)
        },
    )
    .unwrap();

    let mut writer: IntentWriter = is1.finish(Duration::from_secs(3)).unwrap();
    let mut receiver: ReportReceiver = rs1.finish(Duration::from_secs(3)).unwrap();

    let mut dop = DopSession::connect_and_login(
        &dop_sock,
        DopConfig {
            strat_id: STRAT_1,
            client_id: 101,
            local_instance: [STRAT_1.wrapping_add(0x20); 16],
            peer_instance: ORS_INSTANCE,
            control_epoch: 1,
            expected_peer_uid: None,
            max_order_age_ns: 23_000,
        },
        1,
        clock_cell.load(Ordering::Acquire),
        Duration::from_secs(3),
    )
    .unwrap();
    assert_eq!(dop.next_order_seqnum(), 8);
    writer.sync_from_dop(&dop).unwrap();

    let mut rx_raw = Vec::new();
    // Consume the initial post-install WS `auth` frame emitted by `RespWorker` before replying OK.
    let auth_req = read_client_ws_json(&mut server_sock, &mut rx_raw);
    assert!(auth_req.contains("\"op\":\"auth\""));
    send_server_ws_json(
        &mut server_sock,
        r#"{"req_id":"twauth","op":"auth","retCode":0,"retMsg":"OK"}"#,
    );
    let ready_deadline = Instant::now() + Duration::from_secs(3);
    while !process.accounts()[0].is_ready() {
        assert!(Instant::now() < ready_deadline);
        std::thread::sleep(Duration::from_millis(1));
    }

    // 1. Test `23_000 ns` microsecond stale-order gate (`RejectStaleOrder`).
    let now_ns = clock_cell.load(Ordering::Acquire);
    let stale = OutboundIntent::new_order(
        STRAT_1,
        0,
        0,
        1,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        5_000_000,
        1,
        "stale-8",
        now_ns - 25_000,
    )
    .unwrap();
    writer.publish(&stale, now_ns).unwrap();
    dop.observe_published_sequences(writer.next_msg_seqnum(), writer.next_order_seqnum());
    let stale_rep = recv_report(&mut receiver);
    assert_eq!(stale_rep.kind, ExecutionReportKind::Nack);
    assert_eq!(stale_rep.reason, TerminalReason::RejectStaleOrder);

    // 2. Route orders across ALL 6 UTA products (`Spot`, `LinearPerpetual`, `InversePerpetual`,
    //    `LinearFuture`, `InverseFuture`, `Option`) including `TimeInForce::Rpi`!
    let expected = [
        (0u64, Product::Spot, "spot"),
        (1u64, Product::LinearPerpetual, "linear"),
        (2u64, Product::InversePerpetual, "inverse"),
        (3u64, Product::LinearFuture, "linear"),
        (4u64, Product::InverseFuture, "inverse"),
        (5u64, Product::Option, "option"),
    ];
    let mut last_spot_cid = String::new();
    for (sid, prod, expected_cat) in expected {
        let now = clock_cell.load(Ordering::Acquire);
        let tif = if sid == 1 {
            TimeInForce::PostOnly
        } else {
            TimeInForce::Gtc
        };
        let intent = OutboundIntent::new_order(
            STRAT_1,
            0,
            sid,
            1,
            prod,
            Side::Buy,
            OrderType::Limit,
            Some(tif),
            5_000_000,
            2,
            &format!("cid-{sid}"),
            now - 5_000,
        )
        .unwrap();
        writer.publish(&intent, now).unwrap();
        dop.observe_published_sequences(writer.next_msg_seqnum(), writer.next_order_seqnum());

        let outbound_json = read_client_ws_json(&mut server_sock, &mut rx_raw);
        let parsed: serde_json::Value = serde_json::from_str(&outbound_json).unwrap();
        assert_eq!(parsed["op"], "order.create");
        assert_eq!(parsed["args"][0]["category"], expected_cat);
        if sid == 1 {
            assert_eq!(parsed["args"][0]["timeInForce"], "PostOnly");
        }
        let req_id = parsed["reqId"].as_str().unwrap().to_owned();
        let cid = parsed["args"][0]["orderLinkId"]
            .as_str()
            .unwrap()
            .to_owned();
        assert!(cid.starts_with("tw1") && cid.len() == 32);
        if sid == 0 {
            last_spot_cid = cid.clone();
        }

        let vid = format!("bybit-vid-{sid}");
        let cmd_ack = json!({
            "reqId": req_id,
            "retCode": 0,
            "retMsg": "OK",
            "op": "order.create",
            "data": { "orderId": vid, "orderLinkId": cid }
        });
        send_server_ws_json(&mut server_sock, &cmd_ack.to_string());
        let order_push = json!({
            "topic": "order",
            "data": [{
                "category": expected_cat,
                "symbol": parsed["args"][0]["symbol"],
                "orderId": vid,
                "orderLinkId": cid,
                "side": "Buy",
                "orderType": "Limit",
                "timeInForce": if sid == 1 { "PostOnly" } else { "GTC" },
                "price": "50000",
                "qty": "0.002",
                "cumExecQty": "0",
                "leavesQty": "0.002",
                "orderStatus": "New"
            }]
        });
        send_server_ws_json(&mut server_sock, &order_push.to_string());

        // Expect `FrontAck` (from `CommandReply`) followed by `Ack` (from `order` stream `New`).
        let mut saw_ack = false;
        while !saw_ack {
            let rep = recv_report(&mut receiver);
            match rep.kind {
                ExecutionReportKind::FrontAck => {}
                ExecutionReportKind::Ack => {
                    assert_eq!(rep.instrument_id, sid);
                    saw_ack = true;
                }
                other => panic!("unexpected report for sid={sid}: {other:?}"),
            }
        }
    }

    // 3. Test `execution.fast` (`fee = None`) -> full `execution` (`fee = Some`) emitting `FeeAmendment`!
    let fast_exec = json!({
        "topic": "execution.fast",
        "data": [{
            "category": "spot",
            "symbol": "BTCUSDT",
            "execId": "exec-fast-101",
            "orderId": "bybit-vid-0",
            "orderLinkId": last_spot_cid,
            "side": "Buy",
            "execPrice": "50000",
            "execQty": "0.001",
            "isMaker": true
        }]
    });
    send_server_ws_json(&mut server_sock, &fast_exec.to_string());
    match recv_report(&mut receiver).kind {
        ExecutionReportKind::Fill(f) => {
            assert_eq!(f.kind, FillKind::Execution);
            assert_eq!(f.liquidity, Liquidity::Maker);
            assert_eq!(f.last_qty_lots, 1);
            assert!(f.fee.is_none());
        }
        other => panic!("expected initial fast Fill, got {other:?}"),
    }

    let full_exec = json!({
        "topic": "execution",
        "data": [{
            "category": "spot",
            "symbol": "BTCUSDT",
            "execId": "exec-fast-101",
            "orderId": "bybit-vid-0",
            "orderLinkId": last_spot_cid,
            "execType": "Trade",
            "side": "Buy",
            "execPrice": "50000",
            "execQty": "0.001",
            "execFee": "0.05",
            "feeCurrency": "USDT",
            "isMaker": true
        }]
    });
    send_server_ws_json(&mut server_sock, &full_exec.to_string());
    match recv_report(&mut receiver).kind {
        ExecutionReportKind::Fill(f) => {
            assert_eq!(f.kind, FillKind::FeeAmendment);
            let fee = f.fee.expect("expected attached fee on FeeAmendment");
            assert_eq!(fee.fee_units, -5_000_000);
        }
        other => panic!("expected FeeAmendment Fill, got {other:?}"),
    }

    // 4. Test `dry_run` order admission + wire warmup with zero socket transmission!
    let now = clock_cell.load(Ordering::Acquire);
    let mut dry = OutboundIntent::new_order(
        STRAT_1,
        0,
        0,
        1,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        5_000_000,
        1,
        "dry-run-ord",
        now - 1_000,
    )
    .unwrap();
    dry.intent_flags |= intent_flags::DRY_RUN;
    writer.publish(&dry, now).unwrap();
    let dry_rep = recv_report(&mut receiver);
    assert_eq!(dry_rep.kind, ExecutionReportKind::Canceled);
    assert_eq!(dry_rep.reason, TerminalReason::AbortedBeforeSend);
    assert!(try_read_ws_client_text(&mut server_sock, &mut rx_raw).is_none());

    // 5. Test Options RFQ quote submission via `UtaRfqQuoteBook`!
    let rfq_ors_id = make_ors_order_id(writer.next_order_seqnum(), STRAT_1);
    let rfq_intent = OutboundIntent::new_rfq_quote(
        STRAT_1,
        0,
        5,
        1,
        Product::Option,
        rfq_ors_id,
        100_000,
        1,
        0,
        0,
        "rfq-opt-1",
        now - 1_000,
    )
    .unwrap();
    writer.publish(&rfq_intent, now).unwrap();
    let rfq_frame = read_client_ws_json(&mut server_sock, &mut rx_raw);
    assert!(rfq_frame.contains("BTC-26DEC26-90000-C"));
    let rfq_rep = recv_report(&mut receiver);
    assert_eq!(rfq_rep.kind, ExecutionReportKind::RfqQuoteAck);

    assert_eq!(
        process.handle_signal(OrsSignal::SigTerm),
        OrsSignalOutcome::ShutdownInitiated
    );
}

#[test]
fn okx_notice_64008_graceful_drain_rate_limit_nack_and_status_ask() {
    let (clock_cell, clock_fn) = deterministic_clock(20_000_000_000);
    let symbols = six_product_symbols(SpotVenue::Okx);
    let assets = uta_assets();

    let bootstrap = OrsExchangeBootstrapSnapshot {
        balances: vec![
            OrsRestBalanceRow {
                key: AssetKey {
                    wallet_id: 0,
                    asset_id: 1,
                },
                scale: 8,
                free_atoms: 10_000_000_000,
                locked_atoms: 0,
            },
            OrsRestBalanceRow {
                key: AssetKey {
                    wallet_id: 0,
                    asset_id: 2,
                },
                scale: 8,
                free_atoms: 500_000_000_000_000,
                locked_atoms: 0,
            },
        ],
        positions: Vec::new(),
        open_orders: Vec::new(),
        balances_include_open_order_locks: true,
    };

    let rest_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let rest_addr = rest_listener.local_addr().unwrap();
    let rest_thread = std::thread::spawn(move || {
        use std::io::{BufRead, BufReader};
        if let Ok((sock, _)) = rest_listener.accept() {
            let _ = sock.set_read_timeout(Some(Duration::from_secs(3)));
            let mut reader = BufReader::new(sock);
            let mut req_line = String::new();
            if reader.read_line(&mut req_line).unwrap_or(0) > 0 {
                let mut cl_ord_id = String::new();
                if let Some(path) = req_line.split_whitespace().nth(1)
                    && let Some((_, q)) = path.split_once("clOrdId=")
                {
                    cl_ord_id = q.split('&').next().unwrap_or("").to_owned();
                }
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0
                        || line == "\r\n"
                        || line == "\n"
                    {
                        break;
                    }
                }
                let body = json!({
                    "code": "0",
                    "data": [{
                        "instId": "BTC-USDT",
                        "ordId": "okx-rest-oid-1",
                        "clOrdId": cl_ord_id,
                        "px": "50000",
                        "sz": "0.001",
                        "accFillSz": "0",
                        "avgPx": "0",
                        "side": "buy",
                        "ordType": "limit",
                        "state": "live"
                    }]
                })
                .to_string();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = reader.get_mut().write_all(resp.as_bytes());
                let _ = reader.get_mut().flush();
            }
        }
    });

    let session = Arc::new(
        UtaVenueSession::new(UtaVenueSessionConfig {
            venue: SpotVenue::Okx,
            environment: Environment::Test,
            exchange_uid: EXCHANGE_UID,
            risk_domain_id: RISK_DOMAIN,
            credentials: OwnedCredentials::okx("uta-key", "uta-secret", "uta-pass").unwrap(),
            transport_mode: UtaTransportMode::WebSocketTrade,
            order_endpoint: UtaEndpointSpec::manual("/ws/v5/private"),
            stream_endpoint: None,
            rest_endpoint: Some(SessionRestEndpoint {
                url_id: None,
                host: String::from("127.0.0.1"),
                port: rest_addr.port(),
                security: DialSecurity::Plain,
                initial_peers: vec![rest_addr],
            }),
            symbols: symbols.clone(),
            assets,
            ip_order_weight: 5,
            status_query_delay_ms: 10,
            okx_ws_auto_subscribe_private: false,
            rfq_hook: None,
            clock_utc_ms: Some(Arc::new(|| 1_700_000_000_000)),
            bootstrap_override: Some(bootstrap),
        })
        .unwrap(),
    );

    let acct_mgr = build_account_manager(SpotVenue::Okx, vec![STRAT_1, STRAT_2]);
    let mut om_cfg = OrderManagerConfig::single_account_default(
        acct_mgr.specs()[0].to_domain_config(SpotVenue::Okx.venue_id(), Environment::Test),
        symbols.iter().map(|b| b.spec).collect(),
    );
    om_cfg.slots_per_strat = 256;
    let rms = enabled_rms(symbols.len());

    let (is1, ifac1, rs1, rfac1) = make_lanes(STRAT_1);
    let (ws_link_primary, mut primary_srv) = connect_ws_pair();
    let (ws_link_standby, mut standby_srv) = connect_ws_pair();

    let sock_dir = PrivateDirectory::create_in(&std::env::temp_dir(), "tw-okx-test").unwrap();
    let dop_sock = sock_dir.path().join("dop.sock");

    let rt_cfg = OrsRuntimeConfig {
        cpu: OrsCpuConfig {
            epoll_wait_ms: 2,
            ..OrsCpuConfig::default()
        },
        max_order_age_ns: 23_000,
        ip_budgets: vec![IpBudgetSpec {
            ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            ip_weight_min: 10,
        }],
        clock: clock_fn,
        ..OrsRuntimeConfig::default()
    };
    let proc_cfg = OrsProcessConfig {
        control_socket_path: Some(dop_sock.clone()),
        control_config: ControlServerConfig {
            ors_instance: ORS_INSTANCE,
            ..ControlServerConfig::default()
        },
        shutdown_drain_timeout: Duration::from_millis(200),
        block_signals_on_spawn: false,
        ..OrsProcessConfig::default()
    };

    let process = OrsProcess::build_and_spawn(
        rt_cfg,
        &acct_mgr,
        &[STRAT_1, STRAT_2],
        om_cfg,
        rms,
        vec![(ACCT_REF, session)],
        proc_cfg,
        move |ors: &mut OrderRoutingSystem<UtaVenueSession>| {
            ors.install_link(ACCT_REF, 1, ws_link_primary)?;
            ors.install_link(ACCT_REF, 2, ws_link_standby)?;
            ors.attach_strategy_lane_factories(STRAT_1, ifac1, rfac1)
        },
    )
    .unwrap();

    let mut writer = is1.finish(Duration::from_secs(3)).unwrap();
    let mut receiver = rs1.finish(Duration::from_secs(3)).unwrap();

    let dop = DopSession::connect_and_login(
        &dop_sock,
        DopConfig {
            strat_id: STRAT_1,
            client_id: 201,
            local_instance: [STRAT_1.wrapping_add(0x20); 16],
            peer_instance: ORS_INSTANCE,
            control_epoch: 1,
            expected_peer_uid: None,
            max_order_age_ns: 23_000,
        },
        1,
        clock_cell.load(Ordering::Acquire),
        Duration::from_secs(3),
    )
    .unwrap();
    writer.sync_from_dop(&dop).unwrap();

    let mut primary_raw = Vec::new();
    let mut standby_raw = Vec::new();
    let login1 = read_client_ws_json(&mut primary_srv, &mut primary_raw);
    assert!(login1.contains("\"op\":\"login\""));
    let login2 = read_client_ws_json(&mut standby_srv, &mut standby_raw);
    assert!(login2.contains("\"op\":\"login\""));

    send_server_ws_json(&mut primary_srv, r#"{"event":"login","code":"0","msg":""}"#);
    send_server_ws_json(&mut standby_srv, r#"{"event":"login","code":"0","msg":""}"#);
    let ready_deadline = Instant::now() + Duration::from_secs(3);
    while !process.accounts()[0].is_ready() {
        assert!(Instant::now() < ready_deadline);
        std::thread::sleep(Duration::from_millis(1));
    }

    // Order 1 on primary -> return ambiguous `50004` timeout so `OrsQueryDesk` reconciles via REST `StatusAsk`!
    let now = clock_cell.load(Ordering::Acquire);
    let o1 = OutboundIntent::new_order(
        STRAT_1,
        0,
        0,
        1,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        5_000_000,
        1,
        "okx-ord-1",
        now - 2_000,
    )
    .unwrap();
    writer.publish(&o1, now).unwrap();

    let o1_json = read_client_ws_json(&mut primary_srv, &mut primary_raw);
    let o1_val: serde_json::Value = serde_json::from_str(&o1_json).unwrap();
    let rid1 = o1_val["id"].as_str().unwrap().to_owned();
    let cid1 = o1_val["args"][0]["clOrdId"].as_str().unwrap().to_owned();
    let ambig = json!({
        "id": rid1,
        "op": "order",
        "code": "1",
        "data": [{ "sCode": "50004", "sMsg": "Endpoint request timeout", "clOrdId": cid1 }]
    });
    send_server_ws_json(&mut primary_srv, &ambig.to_string());

    // Advance the deterministic clock in 10ms steps while polling `receiver` so `RespWorker`
    // observes `due_ns` after processing `ambig` and `AsyncRestQuerier` dispatches `StatusAsk`.
    let ack1 = {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(rep) = receiver.poll().unwrap() {
                break rep;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for StatusAsk Ack"
            );
            clock_cell.fetch_add(10_000_000, Ordering::Release);
            std::thread::sleep(Duration::from_millis(2));
        }
    };
    assert_eq!(ack1.kind, ExecutionReportKind::Ack);
    assert_eq!(
        ack1.venue_order_id.as_ref().map(|v| v.as_str()),
        Some("okx-rest-oid-1")
    );
    rest_thread.join().unwrap();

    // Emit OKX server maintenance `notice` `64008` on `primary_srv` (`conn_id = 1`) -> graceful drain!
    send_server_ws_json(
        &mut primary_srv,
        r#"{"event":"notice","code":"64008","msg":"Connection will soon close for maintenance"}"#,
    );
    std::thread::sleep(Duration::from_millis(20));

    // Snap to a fresh 1-second IP window (`limit = 10`, `ip_order_weight = 5` -> 2 orders pass, 3rd NACKed!).
    clock_cell.store(22_000_000_000, Ordering::Release);
    let now2 = clock_cell.load(Ordering::Acquire);
    let o2 = OutboundIntent::new_order(
        STRAT_1,
        0,
        0,
        1,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::PostOnly),
        5_000_000,
        1,
        "okx-ord-2",
        now2 - 2_000,
    )
    .unwrap();
    writer.publish(&o2, now2).unwrap();
    let o2_json = read_client_ws_json(&mut standby_srv, &mut standby_raw);
    let o2_val: serde_json::Value = serde_json::from_str(&o2_json).unwrap();
    assert_eq!(o2_val["args"][0]["ordType"], "post_only");

    let o2b = OutboundIntent::new_order(
        STRAT_1,
        0,
        0,
        1,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        5_000_000,
        1,
        "okx-ord-2b",
        now2 - 2_000,
    )
    .unwrap();
    writer.publish(&o2b, now2).unwrap();
    let _ = read_client_ws_json(&mut standby_srv, &mut standby_raw);

    // 3rd order in the fresh 1s IP window exceeds `limit = 10` (`3 * 5 = 15 > 10`) -> single-pick NACK `RateLimited`!
    let o3 = OutboundIntent::new_order(
        STRAT_1,
        0,
        0,
        1,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        5_000_000,
        1,
        "okx-ord-3",
        now2 - 2_000,
    )
    .unwrap();
    writer.publish(&o3, now2).unwrap();
    let nack3 = recv_report(&mut receiver);
    assert_eq!(nack3.kind, ExecutionReportKind::Nack);
    assert_eq!(nack3.reason, RefusalReason::RateLimited);

    assert_eq!(
        process.handle_signal(OrsSignal::SigTerm),
        OrsSignalOutcome::ShutdownInitiated
    );
}

#[test]
fn uta_hotpath_encode_rpi_and_scan_perform_zero_heap_allocations() {
    use tickwire_account::{InlineAscii, OrsNewOrderRequest, OrsPlaceOutcome};
    use tickwire_runtime::{SessionMessageEffects, UrlRole, VenueSession};

    let symbols = six_product_symbols(SpotVenue::Bybit);
    let acct_mgr = build_account_manager(SpotVenue::Bybit, vec![STRAT_1]);
    let spec = acct_mgr.specs()[0].clone();
    let session = UtaVenueSession::new(UtaVenueSessionConfig {
        venue: SpotVenue::Bybit,
        environment: Environment::Test,
        exchange_uid: EXCHANGE_UID,
        risk_domain_id: RISK_DOMAIN,
        credentials: OwnedCredentials::bybit("uta-key", "uta-secret").unwrap(),
        transport_mode: UtaTransportMode::WebSocketTrade,
        order_endpoint: UtaEndpointSpec::manual("/v5/trade"),
        stream_endpoint: None,
        rest_endpoint: None,
        symbols,
        assets: uta_assets(),
        ip_order_weight: 1,
        status_query_delay_ms: 100,
        okx_ws_auto_subscribe_private: false,
        rfq_hook: None,
        clock_utc_ms: Some(Arc::new(|| 1_700_000_000_000)),
        bootstrap_override: None,
    })
    .unwrap();

    // Also run the full TLS loopback warmer once off the measured hot-path block.
    let warm = session.warm_hot_path(1).unwrap();
    assert_eq!(warm.http_status, 200);

    let domain = ClientIdDomain {
        risk_domain_id: RISK_DOMAIN,
        venue: SpotVenue::Bybit.venue_id(),
        environment: Environment::Test,
        account: EXCHANGE_UID,
        product: Product::LinearPerpetual,
    };
    let cid = SelfDescribingClientId::from_ors_order(STRAT_1, 0, 1, 42)
        .unwrap()
        .encode(&domain)
        .unwrap();
    let req = OrsNewOrderRequest {
        strat_id: STRAT_1,
        account_ref: ACCT_REF,
        msg_seqnum: 42,
        order_seqnum: 42,
        symbol_id: 1,
        side: Side::Buy,
        price_ticks: 300_000,
        qty_lots: 10,
        tif: TimeInForce::Rpi,
        post_only: true,
        reduce_only: false,
        check_stp: true,
        dry_run: false,
        now_mono_ns: 1_000_000,
    };
    let placed = OrsPlaceOutcome {
        ors_order_id: make_ors_order_id(42, STRAT_1),
        slot_idx: 0,
        account_ref: ACCT_REF,
        product: Product::LinearPerpetual,
        client_id: InlineAscii::lossy_ascii(cid.as_str()),
        should_send_to_venue: true,
    };
    let reply_json = format!(
        r#"{{"reqId":"1","retCode":0,"retMsg":"OK","op":"order.create","data":{{"orderId":"vid-42","orderLinkId":"{}"}}}}"#,
        cid.as_str()
    );
    let mut scratch = [0u8; 2048];
    let mut effects = SessionMessageEffects {
        events: Vec::with_capacity(8),
        rate_limits: Vec::with_capacity(4),
        query_status: Vec::with_capacity(4),
        direct_reports: Vec::with_capacity(2),
        stream_token_expired: false,
        graceful_drain_connection: false,
    };

    let info = allocation_counter::measure(|| {
        let enc = session
            .encode_new_order(&spec, &req, &placed, 1, &mut scratch)
            .unwrap();
        assert!(enc.frame_len > 0 && enc.reply_or_seq_id > 0);
        effects.clear();
        session.on_message(
            ACCT_REF,
            "bybit-uta-order",
            UrlRole::ORDER,
            1,
            None,
            reply_json.as_bytes(),
            1_000_500,
            &mut effects,
        );
        assert_eq!(effects.events.len(), 1);
    });
    assert_eq!(
        info.count_total, 0,
        "UTA VenueSession hot-path encode + scan must perform zero heap allocations"
    );
}

#[test]
fn paper_tls_loopback_succeeds_for_bybit_and_okx() {
    let bybit = run_paper_demo(SpotVenue::Bybit).unwrap();
    assert_eq!(bybit.venue, "bybit");
    assert_eq!(bybit.orders_acknowledged, 2);
    assert_eq!(bybit.client_ids.len(), 2);
    assert!(
        bybit
            .client_ids
            .iter()
            .all(|c| c.starts_with("tw1") && c.len() == 32)
    );

    let okx = run_paper_demo(SpotVenue::Okx).unwrap();
    assert_eq!(okx.venue, "okx");
    assert_eq!(okx.orders_acknowledged, 2);
    assert_eq!(okx.client_ids.len(), 2);
    assert!(
        okx.client_ids
            .iter()
            .all(|c| c.starts_with("tw1") && c.len() == 32)
    );
}
