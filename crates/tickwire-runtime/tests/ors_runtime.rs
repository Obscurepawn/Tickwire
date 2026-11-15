//! End-to-end multi-threaded integration tests for the venue-agnostic Unified ORS runtime
//! (`OrderRoutingSystem`, `OrsProcess`, `REQ` busy-spin, `RESP` epoll, DOP `ControlServer`,
//! `AdminCommands`, microsecond stale-order gate, `dry_run`, single-pick `SharedRateLimiter` NACK,
//! `graceful_drain_connection`, and OS signal lifecycle).

#![forbid(unsafe_code)]

use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tickwire_account::{
    InlineAscii, OrderManagerConfig, OrsAccountDomainConfig, OrsCancelDispatch, OrsFeeSpec,
    OrsFillObservation, OrsNewOrderRequest, OrsPlaceOutcome, OrsRestBalanceRow,
    OrsRestOpenOrderRow, OrsSymbolSpec, make_ors_order_id,
};
use tickwire_account_gateway::{AccountManager, AccountSpec};
use tickwire_control_sys::{OrsSignal, PrivateDirectory};
use tickwire_ipc::{ResourceHeader, intent_flags};
use tickwire_quota::shared::{IpBudgetSpec, OrsVenueProfile};
use tickwire_risk::RmsEngine;
use tickwire_runtime::{
    ControlServerConfig, EncodedOrderFrame, OrderRoutingSystem, OrsCpuConfig, OrsProcess,
    OrsProcessConfig, OrsRuntimeConfig, OrsSignalOutcome, SessionMessageEffects,
    SessionUrlPoolSpec, UrlRole, VenueSession,
};
use tickwire_runtime_policy::Mode;
use tickwire_strategy_host::{prepare_cross_thread_intent_lane, prepare_cross_thread_report_lane};
use tickwire_strategy_sdk::{
    DopConfig, DopSession, ExecutionReport, ExecutionReportKind, FillKind, IntentWriter,
    OutboundIntent, ReportIdentity, ReportReceiver,
};
use tickwire_transport::links::{LinkKind, LinkOps, RequestRef, WsLink};
use tickwire_transport::rest_querier::DialSecurity;
use tickwire_transport::{Connection, IoBudget, Limits, Security, mono_now_ns, ws};
use tickwire_venue::{
    AssetKey, Environment, Liquidity, OrderRoute, OrderType, Product, RefusalReason,
    SelfDescribingClientId, Side, TerminalReason, TimeInForce,
};

const STRAT_ID: u8 = 1;
const STRAT_INSTANCE: [u8; 16] = [0x31; 16];
const ORS_INSTANCE: [u8; 16] = [0x77; 16];
const STRAT_GENERATION: u64 = 1;
const EXEC_GENERATION: u64 = 1;
const RISK_SCOPE: u64 = 1;
const SYMBOL_ID: u32 = 0;
const INSTRUMENT_VERSION: u64 = 1;

struct MockVenueSession {
    bootstrap: Option<tickwire_account::OrsExchangeBootstrapSnapshot>,
    place_ip_weight: u32,
}

impl MockVenueSession {
    fn new(
        bootstrap: Option<tickwire_account::OrsExchangeBootstrapSnapshot>,
        place_ip_weight: u32,
    ) -> Self {
        Self {
            bootstrap,
            place_ip_weight,
        }
    }
}

impl VenueSession for MockVenueSession {
    fn venue_profile(&self) -> OrsVenueProfile {
        OrsVenueProfile::OkxUta
    }

    fn url_pools(&self, _spec: &AccountSpec) -> Result<Vec<SessionUrlPoolSpec>, String> {
        Ok(vec![SessionUrlPoolSpec {
            url_id: String::from("order_ws"),
            role: UrlRole::ORDER_AND_STREAM,
            kind: LinkKind::Ws,
            product_mask: 0,
            host: String::new(),
            port: 0,
            path: String::from("/ws"),
            security: DialSecurity::Plain,
            initial_peers: Vec::new(),
        }])
    }

    fn place_ip_weight(&self, _product: Product) -> u32 {
        self.place_ip_weight
    }

    fn cancel_ip_weight(&self, _product: Option<Product>) -> u32 {
        0
    }

    fn encode_new_order(
        &self,
        _spec: &AccountSpec,
        req: &OrsNewOrderRequest,
        placed: &OrsPlaceOutcome,
        _conn_id: u16,
        scratch: &mut [u8],
    ) -> Result<EncodedOrderFrame, RefusalReason> {
        let payload = format!(
            "NEW|oid={}|seq={}|acct={}|cid={}|px={}|qty={}",
            placed.ors_order_id,
            req.order_seqnum,
            placed.account_ref,
            placed.client_id.as_str(),
            req.price_ticks,
            req.qty_lots,
        );
        let ws_frame = ws::encode_client(ws::Opcode::Text, payload.as_bytes(), scratch.len())
            .map_err(|_| RefusalReason::AbortedBeforeSend)?;
        scratch[..ws_frame.len()].copy_from_slice(&ws_frame);
        Ok(EncodedOrderFrame {
            frame_len: ws_frame.len(),
            reply_or_seq_id: placed.ors_order_id,
        })
    }

    fn encode_cancel_order(
        &self,
        _spec: &AccountSpec,
        cancel: &OrsCancelDispatch,
        _conn_id: u16,
        scratch: &mut [u8],
    ) -> Result<EncodedOrderFrame, RefusalReason> {
        let payload = format!(
            "CXL|oid={}|acct={}|cid={}",
            cancel.ors_order_id,
            cancel.account_ref,
            cancel.client_id.as_str(),
        );
        let ws_frame = ws::encode_client(ws::Opcode::Text, payload.as_bytes(), scratch.len())
            .map_err(|_| RefusalReason::AbortedBeforeSend)?;
        scratch[..ws_frame.len()].copy_from_slice(&ws_frame);
        Ok(EncodedOrderFrame {
            frame_len: ws_frame.len(),
            reply_or_seq_id: cancel.ors_order_id ^ (1u64 << 62),
        })
    }

    fn reply_id(&self, payload: &[u8]) -> Option<u64> {
        let text = core::str::from_utf8(payload).ok()?;
        for part in text.split('|') {
            if let Some(raw) = part.strip_prefix("rid=") {
                return raw.parse::<u64>().ok();
            }
        }
        None
    }

    fn on_message(
        &self,
        account_ref: u8,
        _url_id: &str,
        _role: UrlRole,
        _conn_id: u16,
        _hint: Option<RequestRef>,
        payload: &[u8],
        now_mono_ns: u64,
        effects: &mut SessionMessageEffects,
    ) {
        let Ok(text) = core::str::from_utf8(payload) else {
            return;
        };
        let mut kind = "";
        let mut oid: u64 = 0;
        let mut vid = "vid-1";
        let mut tid = "tid-1";
        let mut px: i64 = 100;
        let mut qty: i64 = 10;
        let mut cum: i64 = 0;
        let mut fee: i128 = 0;

        for token in text.split('|') {
            if let Some((k, v)) = token.split_once('=') {
                match k {
                    "ev" => kind = v,
                    "oid" => oid = v.parse().unwrap_or(0),
                    "vid" => vid = v,
                    "tid" => tid = v,
                    "px" => px = v.parse().unwrap_or(100),
                    "qty" => qty = v.parse().unwrap_or(10),
                    "cum" => cum = v.parse().unwrap_or(0),
                    "fee" => fee = v.parse().unwrap_or(0),
                    _ => {}
                }
            }
        }

        match kind {
            "NOTICE_DRAIN" => {
                effects.graceful_drain_connection = true;
            }
            "ACK" => {
                effects
                    .events
                    .push(tickwire_runtime::SessionVenueEvent::Ack {
                        ors_order_id: oid,
                        venue_order_id: InlineAscii::try_from_str(vid),
                        ack_price_ticks: Some(px),
                        ack_qty_lots: Some(qty),
                        received_mono_ns: now_mono_ns,
                    });
            }
            "FILL" => {
                let fee_spec = (fee != 0).then_some(OrsFeeSpec {
                    asset_key: AssetKey {
                        wallet_id: 0,
                        asset_id: 2,
                    },
                    scale: 8,
                    signed_atoms: fee,
                });
                effects
                    .events
                    .push(tickwire_runtime::SessionVenueEvent::Fill(
                        OrsFillObservation {
                            ors_order_id: oid,
                            account_ref,
                            symbol_id: SYMBOL_ID,
                            side: Side::Buy,
                            liquidity: Liquidity::Maker,
                            exec_ref: 0,
                            trade_id: InlineAscii::lossy_ascii(tid),
                            qty_lots: qty,
                            price_ticks: px,
                            fee: fee_spec,
                            received_mono_ns: now_mono_ns,
                        },
                    ));
            }
            "CANCELED" => {
                effects
                    .events
                    .push(tickwire_runtime::SessionVenueEvent::Canceled {
                        ors_order_id: oid,
                        cum_qty_lots: Some(cum),
                        error_code: None,
                        status_or_msg: InlineAscii::lossy_ascii("CANCELED"),
                        stp_mode_or_reason: None,
                        explicit_reason: None,
                        received_mono_ns: now_mono_ns,
                    });
            }
            _ => {}
        }
    }

    fn bootstrap_snapshots(
        &self,
        _spec: &AccountSpec,
    ) -> Result<tickwire_account::OrsExchangeBootstrapSnapshot, String> {
        if let Some(ref snap) = self.bootstrap {
            Ok(snap.clone())
        } else {
            Ok(tickwire_account::OrsExchangeBootstrapSnapshot {
                balances: vec![
                    OrsRestBalanceRow {
                        key: AssetKey {
                            wallet_id: 0,
                            asset_id: 1,
                        },
                        scale: 8,
                        free_atoms: 1_000_000_000_000,
                        locked_atoms: 0,
                    },
                    OrsRestBalanceRow {
                        key: AssetKey {
                            wallet_id: 0,
                            asset_id: 2,
                        },
                        scale: 8,
                        free_atoms: 1_000_000_000_000_000,
                        locked_atoms: 0,
                    },
                ],
                positions: Vec::new(),
                open_orders: Vec::new(),
                balances_include_open_order_locks: true,
            })
        }
    }
}

fn domain_config(account_ref: u8, exchange_account_id: u64) -> OrsAccountDomainConfig {
    OrsAccountDomainConfig {
        account_ref,
        risk_domain_id: [0x22; 16],
        venue: 1,
        environment: Environment::Test,
        exchange_account_id,
        epoch: 1,
    }
}

fn symbol_spec_spot_0() -> OrsSymbolSpec {
    OrsSymbolSpec {
        symbol_id: SYMBOL_ID,
        instrument_id: u64::from(SYMBOL_ID),
        product: Product::Spot,
        wallet_id: 0,
        base_asset_id: 1,
        base_scale: 8,
        quote_asset_id: 2,
        quote_scale: 8,
        settlement_asset_id: 2,
        settlement_scale: 8,
        price_tick_atoms: 1_000_000,
        price_scale: 8,
        qty_step_atoms: 100_000,
        qty_scale: 8,
        contract_size_atoms: 1,
        contract_scale: 0,
        initial_margin_bps: 10_000,
    }
}

fn deterministic_clock(initial_ns: u64) -> (Arc<AtomicU64>, tickwire_transport::ClockFn) {
    let cell = Arc::new(AtomicU64::new(initial_ns));
    let reader = Arc::clone(&cell);
    let clock_fn: tickwire_transport::ClockFn = Arc::new(move || reader.load(Ordering::Acquire));
    (cell, clock_fn)
}

fn enabled_rms_engine() -> RmsEngine {
    let engine = RmsEngine::new(1);
    engine
        .reload_handle()
        .reload_limits(
            "symbol_id=0 enabled=true max_qty=100 max_notional=1000000000 new_count=100/500/1000 order_qty=10000/50000/100000\n",
        )
        .unwrap();
    engine
}

fn connect_loopback_ws_pair() -> (LinkOps, TcpStream) {
    connect_loopback_ws_pair_from(IpAddr::V4(Ipv4Addr::LOCALHOST))
}

fn connect_loopback_ws_pair_from(local_ip: IpAddr) -> (LinkOps, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut client_conn = Connection::connect_from(
        addr,
        Some(local_ip),
        Security::Plain,
        Limits::default(),
        deadline,
    )
    .unwrap();
    let (server_stream, _) = listener.accept().unwrap();
    server_stream.set_nonblocking(true).unwrap();
    server_stream.set_nodelay(true).unwrap();

    while !client_conn.is_ready() {
        assert!(Instant::now() < deadline, "loopback TCP connect timed out");
        client_conn
            .drive(Instant::now(), IoBudget::default())
            .unwrap();
        std::thread::sleep(Duration::from_millis(1));
    }

    let ws_link = WsLink::new(client_conn, 64 * 1024, 64 * 1024).unwrap();
    (LinkOps::Ws(ws_link), server_stream)
}

fn encode_ws_server_text(payload: &str) -> Vec<u8> {
    let bytes = payload.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() + 10);
    out.push(0x81);
    if bytes.len() < 126 {
        out.push(bytes.len() as u8);
    } else {
        out.push(126);
        out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    }
    out.extend_from_slice(bytes);
    out
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
        let plen = usize::from(u16::from_be_bytes([raw_buf[2], raw_buf[3]]));
        (4usize, plen)
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

fn wait_ws_client_text(stream: &mut TcpStream, raw_buf: &mut Vec<u8>, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(msg) = try_read_ws_client_text(stream, raw_buf) {
            return msg;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("timed out waiting for outbound WS client frame");
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

fn make_intent_bank_header() -> ResourceHeader {
    let capacity = 128u32;
    let record_bytes = 1024u32;
    ResourceHeader {
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
        resource_id: [11; 16],
        channel_id: [12; 16],
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        producer_instance: STRAT_INSTANCE,
        producer_generation: STRAT_GENERATION,
        consumer_instance: ORS_INSTANCE,
        host_boot_id: [0x13; 16],
        namespace_id: [0x14; 16],
        risk_scope: RISK_SCOPE,
        producer_page_offset: 4096,
        slot_offset: 8192,
        capacity,
        record_bytes,
        resource_bytes: 8192 + u64::from(capacity) * u64::from(record_bytes),
        first_seq: 1,
        initial_membership_version: 1,
        max_consumers: 1,
        required_features: 0x1c | tickwire_strategy_sdk::outbound::FEATURE,
        schema_major: 1,
        clock_domain: 1,
        qos: 1,
        event_class: 8,
        ..Default::default()
    }
}

fn make_report_bank_header() -> ResourceHeader {
    let capacity = 128u32;
    let record_bytes = 512u32;
    ResourceHeader {
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
        resource_id: [21; 16],
        channel_id: [22; 16],
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        producer_instance: ORS_INSTANCE,
        producer_generation: EXEC_GENERATION,
        consumer_instance: STRAT_INSTANCE,
        host_boot_id: [0x13; 16],
        namespace_id: [0x14; 16],
        risk_scope: RISK_SCOPE,
        producer_page_offset: 4096,
        slot_offset: 8192,
        capacity,
        record_bytes,
        resource_bytes: 8192 + u64::from(capacity) * u64::from(record_bytes),
        first_seq: 1,
        initial_membership_version: 1,
        max_consumers: 1,
        required_features: 0x1c | tickwire_strategy_sdk::reports::FEATURE,
        schema_major: 1,
        clock_domain: 1,
        qos: 1,
        event_class: 9,
        ..Default::default()
    }
}

fn wait_report(rx: &mut ReportReceiver, timeout: Duration) -> ExecutionReport {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(rep) = rx.poll().unwrap() {
            return rep;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("timed out waiting for ExecutionReport on SHM ReportReceiver");
}

fn attach_strategy_lanes<S: VenueSession>(
    ors: &mut OrderRoutingSystem<S>,
) -> (
    tickwire_strategy_host::CrossThreadIntentStarter,
    tickwire_strategy_host::CrossThreadReportStarter,
) {
    let (intent_starter, intent_factory) =
        prepare_cross_thread_intent_lane(make_intent_bank_header(), 1, [0x41; 16], STRAT_ID, 1)
            .unwrap();
    let (report_starter, report_factory) = prepare_cross_thread_report_lane(
        make_report_bank_header(),
        1,
        [0x42; 16],
        ReportIdentity {
            strat_id: STRAT_ID,
            instance_id: STRAT_INSTANCE,
            strategy_generation: STRAT_GENERATION,
        },
    )
    .unwrap();
    ors.attach_strategy_lane_factories(STRAT_ID, intent_factory, report_factory)
        .unwrap();
    (intent_starter, report_starter)
}

#[test]
fn cold_rest_bootstrap_dop_logon_hot_roundtrip_stale_gate_dry_run_and_rfq_default() {
    let dir = PrivateDirectory::create_in(&std::env::temp_dir(), "ors-rt1").unwrap();
    let dop_sock = dir.path().join("dop.sock");

    // Configure two accounts in `enabled_accounts` order `[acct_two (ref=2), acct_five (ref=5)]`
    // so `account_ref = 0` routes to `2` (first authorized in `enabled_accounts` order!).
    let acct_mgr = AccountManager::from_json_str(
        r#"{
            "enabled_accounts": ["acct_two", "acct_five"],
            "acct_two": {
                "account_ref": 2,
                "exchange_account_id": 200,
                "strategies": [1],
                "local_ip": "127.0.0.1",
                "credentials": { "key": "k2", "secret": "s2" },
                "urls": {
                    "order_ws": { "local_ip/connection_count": [["127.0.0.1", 1]] }
                }
            },
            "acct_five": {
                "account_ref": 5,
                "exchange_account_id": 500,
                "strategies": [1],
                "local_ip": "127.0.0.1",
                "credentials": { "key": "k5", "secret": "s5" },
                "urls": {
                    "order_ws": { "local_ip/connection_count": [["127.0.0.1", 1]] }
                }
            }
        }"#,
    )
    .unwrap();

    let dom2 = domain_config(2, 200);
    let dom5 = domain_config(5, 500);
    let recovered_seq = 7u64;
    let recovered_cid =
        SelfDescribingClientId::from_ors_order(STRAT_ID, 0, dom2.epoch, recovered_seq)
            .unwrap()
            .encode(&dom2.client_id_domain(Product::Spot))
            .unwrap();

    let boot_acct2 = tickwire_account::OrsExchangeBootstrapSnapshot {
        balances: vec![
            OrsRestBalanceRow {
                key: AssetKey {
                    wallet_id: 0,
                    asset_id: 1,
                },
                scale: 8,
                free_atoms: 1_000_000_000_000,
                locked_atoms: 0,
            },
            OrsRestBalanceRow {
                key: AssetKey {
                    wallet_id: 0,
                    asset_id: 2,
                },
                scale: 8,
                free_atoms: 1_000_000_000_000_000,
                locked_atoms: 100_000_000_000,
            },
        ],
        positions: Vec::new(),
        open_orders: vec![OrsRestOpenOrderRow {
            symbol_id: SYMBOL_ID,
            client_id: recovered_cid.as_str().to_owned(),
            venue_order_id: String::from("boot-vid-7"),
            side: Side::Buy,
            tif: TimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            price_ticks: 100,
            orig_qty_lots: 10,
            cum_filled_lots: 0,
            created_mono_ns: 1_000,
        }],
        balances_include_open_order_locks: true,
    };

    let mut om_cfg = OrderManagerConfig::single_account_default(dom2, vec![symbol_spec_spot_0()]);
    om_cfg.accounts.push(dom5);

    let (clock_ns, clock_fn) = deterministic_clock(10_000_000_000);
    assert_eq!(OrsRuntimeConfig::default().max_order_age_ns, 23_000);

    let mut ors = OrderRoutingSystem::build(
        OrsRuntimeConfig {
            cpu: OrsCpuConfig::default(),
            max_order_age_ns: 23_000,
            clock: clock_fn,
            ..OrsRuntimeConfig::default()
        },
        &acct_mgr,
        &[STRAT_ID],
        om_cfg,
        enabled_rms_engine(),
        vec![
            (2, Arc::new(MockVenueSession::new(Some(boot_acct2), 0))),
            (5, Arc::new(MockVenueSession::new(None, 0))),
        ],
    )
    .unwrap();

    // Install loopback WS sockets on `acct_two` (`conn_id = 1`, with standby `conn_id = 2`) and
    // `acct_five` (`conn_id = 3`).
    let (link2, mut srv2) = connect_loopback_ws_pair();
    let (link5, mut _srv5) = connect_loopback_ws_pair();
    ors.install_link(2, 1, link2).unwrap();
    ors.install_link(5, 3, link5).unwrap();

    let (intent_starter, report_starter) = attach_strategy_lanes(&mut ors);
    let proc_cfg = OrsProcessConfig {
        block_signals_on_spawn: false,
        control_socket_path: Some(dop_sock.clone()),
        control_config: ControlServerConfig {
            ors_instance: ORS_INSTANCE,
            ..ControlServerConfig::default()
        },
        ..OrsProcessConfig::default()
    };
    let proc_handle = OrsProcess::spawn(ors, proc_cfg).unwrap();
    let mut intent_writer: IntentWriter = intent_starter.finish(Duration::from_secs(3)).unwrap();
    let mut report_rx: ReportReceiver = report_starter.finish(Duration::from_secs(3)).unwrap();
    assert!(proc_handle.worker_init_error().is_none());

    // Recovered open order `seq = 7` is already tracked in `OrderManager`!
    assert_eq!(proc_handle.open_order_and_quote_count(), 1);

    // 1. DOP logon (`requested_next_source_seqnum = 3`) adopts `max(3, 8) = 8` from cold REST bootstrap
    //    and lists `account_ref = 2` before `5` (`account_ref = 0 -> 2`).
    let mut dop = DopSession::connect_and_login(
        &dop_sock,
        DopConfig {
            strat_id: STRAT_ID,
            client_id: 101,
            local_instance: STRAT_INSTANCE,
            peer_instance: ORS_INSTANCE,
            control_epoch: 1,
            expected_peer_uid: None,
            max_order_age_ns: 23_000,
        },
        3,
        mono_now_ns(),
        Duration::from_secs(3),
    )
    .unwrap();
    assert!(dop.ready_to_trade());
    assert_eq!(dop.next_order_seqnum(), 8);
    assert_eq!(dop.resolve_account_ref(0).unwrap(), 2);
    intent_writer.sync_from_dop(&dop).unwrap();

    let mut srv2_buf = Vec::new();

    // 2. Microsecond stale-order gate (`tick_mono_ns` 23_001 ns in the past > 23_000 ns threshold):
    //    refuses ONLY this single order (`RejectStaleOrder`) with zero socket bytes and zero freeze!
    let now_stale = clock_ns.load(Ordering::Acquire);
    let stale_intent = OutboundIntent::new_order(
        STRAT_ID,
        0,
        u64::from(SYMBOL_ID),
        INSTRUMENT_VERSION,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        101,
        5,
        "stale-cid-8",
        now_stale - 23_001,
    )
    .unwrap();
    let stale_pub = intent_writer.publish(&stale_intent, now_stale).unwrap();
    assert_eq!(stale_pub.order_seqnum, 8);
    dop.observe_published_sequences(
        intent_writer.next_msg_seqnum(),
        intent_writer.next_order_seqnum(),
    );

    let stale_rep = wait_report(&mut report_rx, Duration::from_secs(3));
    assert_eq!(stale_rep.kind, ExecutionReportKind::Nack);
    assert_eq!(stale_rep.reason, RefusalReason::RejectStaleOrder);
    assert_eq!(stale_rep.account_ref, 2);
    assert!(try_read_ws_client_text(&mut srv2, &mut srv2_buf).is_none());

    // 3. `dry_run` order (`intent_flags::DRY_RUN`, exact boundary `23_000 ns` age <= `23_000 ns`):
    //    validates on scratch slot, skips socket write!
    let now_dry = clock_ns.load(Ordering::Acquire);
    let mut dry_intent = OutboundIntent::new_order(
        STRAT_ID,
        0,
        u64::from(SYMBOL_ID),
        INSTRUMENT_VERSION,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        102,
        4,
        "dry-cid-9",
        now_dry - 23_000,
    )
    .unwrap();
    dry_intent.intent_flags |= intent_flags::DRY_RUN;
    let dry_pub = intent_writer.publish(&dry_intent, now_dry).unwrap();
    assert_eq!(dry_pub.order_seqnum, 9);
    dop.observe_published_sequences(
        intent_writer.next_msg_seqnum(),
        intent_writer.next_order_seqnum(),
    );

    let dry_rep = wait_report(&mut report_rx, Duration::from_secs(3));
    assert_eq!(dry_rep.kind, ExecutionReportKind::Canceled);
    assert_eq!(dry_rep.reason, TerminalReason::AbortedBeforeSend);
    assert_eq!(dry_rep.account_ref, 2);
    assert_eq!(proc_handle.open_order_and_quote_count(), 1);
    assert!(try_read_ws_client_text(&mut srv2, &mut srv2_buf).is_none());

    // 4. Default RFQ quote hook returns `RfqQuoteNotSupported`.
    let now_rfq = clock_ns.load(Ordering::Acquire);
    let rfq_intent = OutboundIntent::new_rfq_quote(
        STRAT_ID,
        0,
        u64::from(SYMBOL_ID),
        INSTRUMENT_VERSION,
        Product::Spot,
        777,
        99,
        5,
        0,
        0,
        "rfq-cid-10",
        now_rfq,
    )
    .unwrap();
    let rfq_pub = intent_writer.publish(&rfq_intent, now_rfq).unwrap();
    assert_eq!(rfq_pub.order_seqnum, 10);
    dop.observe_published_sequences(
        intent_writer.next_msg_seqnum(),
        intent_writer.next_order_seqnum(),
    );

    let rfq_rep = wait_report(&mut report_rx, Duration::from_secs(3));
    assert_eq!(rfq_rep.kind, ExecutionReportKind::RfqQuoteNack);
    assert_eq!(rfq_rep.reason, RefusalReason::RfqQuoteNotSupported);

    // 5. Fresh live `NewOrder` (`account_ref = 0 -> 2`, `order_seqnum = 11`) -> `Ack` -> `Fill` -> `Cancel` -> `Canceled`!
    let now_live = clock_ns.load(Ordering::Acquire);
    let live_intent = OutboundIntent::new_order(
        STRAT_ID,
        0,
        u64::from(SYMBOL_ID),
        INSTRUMENT_VERSION,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        105,
        10,
        "live-cid-11",
        now_live - 5_000,
    )
    .unwrap();
    let live_pub = intent_writer
        .publish_with_dop(&mut dop, &live_intent, now_live)
        .unwrap();
    assert_eq!(live_pub.order_seqnum, 11);
    let live_oid = make_ors_order_id(11, STRAT_ID);

    let wire_new = wait_ws_client_text(&mut srv2, &mut srv2_buf, Duration::from_secs(3));
    assert!(wire_new.starts_with(&format!("NEW|oid={live_oid}|seq=11|acct=2|cid=tw1")));

    // Venue sends `ACK` and a partial `FILL` (4 of 10 lots).
    srv2.write_all(&encode_ws_server_text(&format!(
        "rid={live_oid}|ev=ACK|oid={live_oid}|vid=v-11|px=105|qty=10"
    )))
    .unwrap();
    let ack_rep = wait_report(&mut report_rx, Duration::from_secs(3));
    assert_eq!(ack_rep.kind, ExecutionReportKind::Ack);
    assert_eq!(ack_rep.ors_order_id, live_oid);
    assert_eq!(ack_rep.account_ref, 2);
    assert_eq!(ack_rep.leaves_qty_lots, 10);

    srv2.write_all(&encode_ws_server_text(&format!(
        "ev=FILL|oid={live_oid}|tid=trd-11-a|px=105|qty=4|cum=4|fee=-250"
    )))
    .unwrap();
    let fill_rep = wait_report(&mut report_rx, Duration::from_secs(3));
    match fill_rep.kind {
        ExecutionReportKind::Fill(ref f) => {
            assert_eq!(f.kind, FillKind::Execution);
            assert_eq!(f.last_qty_lots, 4);
            assert_eq!(f.cumulative_qty_lots, 4);
            assert_eq!(f.leaves_qty_lots, 6);
            assert_eq!(f.fee.unwrap().fee_units, -250);
        }
        other => panic!("expected Fill report, got {other:?}"),
    }

    // Strategy sends `Cancel` for `orig_order_seqnum = 11`.
    let now_cxl = mono_now_ns();
    let cxl_intent = OutboundIntent::cancel(
        STRAT_ID,
        2,
        u64::from(SYMBOL_ID),
        INSTRUMENT_VERSION,
        Product::Spot,
        Some(Side::Buy),
        11,
        None,
        Some("v-11"),
        now_cxl,
    )
    .unwrap();
    intent_writer
        .publish_with_dop(&mut dop, &cxl_intent, now_cxl)
        .unwrap();

    let wire_cxl = wait_ws_client_text(&mut srv2, &mut srv2_buf, Duration::from_secs(3));
    assert!(wire_cxl.starts_with(&format!("CXL|oid={live_oid}|acct=2|")));

    let cxl_rid = live_oid ^ (1u64 << 62);
    srv2.write_all(&encode_ws_server_text(&format!(
        "rid={cxl_rid}|ev=CANCELED|oid={live_oid}|cum=4"
    )))
    .unwrap();
    let cxl_rep = wait_report(&mut report_rx, Duration::from_secs(3));
    assert_eq!(cxl_rep.kind, ExecutionReportKind::Canceled);
    assert_eq!(cxl_rep.ors_order_id, live_oid);
    assert_eq!(cxl_rep.cumulative_qty_lots, 4);
    assert_eq!(cxl_rep.leaves_qty_lots, 0);

    // Cancel the cold-bootstrapped order (`seq = 7`) on graceful shutdown:
    let srv_shutdown = std::thread::spawn(move || {
        let msg = wait_ws_client_text(&mut srv2, &mut srv2_buf, Duration::from_secs(3));
        let boot_oid = make_ors_order_id(recovered_seq, STRAT_ID);
        assert!(msg.starts_with(&format!("CXL|oid={boot_oid}|acct=2|")));
        let rid = boot_oid ^ (1u64 << 62);
        srv2.write_all(&encode_ws_server_text(&format!(
            "rid={rid}|ev=CANCELED|oid={boot_oid}|cum=0"
        )))
        .unwrap();
    });

    assert_eq!(
        proc_handle.handle_signal(OrsSignal::SigTerm),
        OrsSignalOutcome::ShutdownInitiated
    );
    assert!(proc_handle.is_shut_down());
    assert_eq!(proc_handle.open_order_and_quote_count(), 0);
    srv_shutdown.join().unwrap();

    let boot_cxl_rep = wait_report(&mut report_rx, Duration::from_secs(3));
    assert_eq!(boot_cxl_rep.kind, ExecutionReportKind::Canceled);
    let shut_rep = wait_report(&mut report_rx, Duration::from_secs(3));
    assert_eq!(shut_rep.kind, ExecutionReportKind::OrsShutDown);
}

#[test]
fn single_pick_ip_rate_limiter_nacks_without_retrying_sibling_connection_and_graceful_drain_switches_smoothly()
 {
    let dir = PrivateDirectory::create_in(&std::env::temp_dir(), "ors-rt2").unwrap();
    let dop_sock = dir.path().join("dop.sock");

    // Two connections on `acct1`:
    // - `conn_id = 1` bound to `127.0.0.1` (`colo = true`, preferred by `PublishedRoutes::pick`)
    // - `conn_id = 2` bound to `127.0.0.2` (`colo = false`)
    let acct_mgr = AccountManager::from_json_str(
        r#"{
            "enabled_accounts": ["acct1"],
            "acct1": {
                "account_ref": 1,
                "exchange_account_id": 100,
                "strategies": [1],
                "local_ip": "127.0.0.1",
                "credentials": { "key": "k1", "secret": "s1" },
                "urls": {
                    "order_ws": {
                        "local_ip/connection_count": [
                            { "local_ip": "127.0.0.1", "count": 1, "colo": true },
                            { "local_ip": "127.0.0.2", "count": 1, "colo": false }
                        ]
                    }
                }
            }
        }"#,
    )
    .unwrap();

    // Configure IP budget on `127.0.0.1` = `1` weight/min (`place_ip_weight = 1`), while `127.0.0.2`
    // has `100` weight/min.
    let (clock_ns, clock_fn) = deterministic_clock(20_000_000_000);
    let mut ors = OrderRoutingSystem::build(
        OrsRuntimeConfig {
            clock: clock_fn,
            ip_budgets: vec![
                IpBudgetSpec::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1),
                IpBudgetSpec::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 100),
            ],
            ..OrsRuntimeConfig::default()
        },
        &acct_mgr,
        &[STRAT_ID],
        OrderManagerConfig::single_account_default(
            domain_config(1, 100),
            vec![symbol_spec_spot_0()],
        ),
        enabled_rms_engine(),
        vec![(1, Arc::new(MockVenueSession::new(None, 1)))],
    )
    .unwrap();

    let (link_c1, mut srv_c1) =
        connect_loopback_ws_pair_from(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
    let (link_c2, mut srv_c2) =
        connect_loopback_ws_pair_from(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)));
    ors.install_link(1, 1, link_c1).unwrap();
    ors.install_link(1, 2, link_c2).unwrap();

    let (intent_starter, report_starter) = attach_strategy_lanes(&mut ors);
    let proc_handle = OrsProcess::spawn(
        ors,
        OrsProcessConfig {
            block_signals_on_spawn: false,
            control_socket_path: Some(dop_sock.clone()),
            control_config: ControlServerConfig {
                ors_instance: ORS_INSTANCE,
                ..ControlServerConfig::default()
            },
            ..OrsProcessConfig::default()
        },
    )
    .unwrap();
    let mut intent_writer = intent_starter.finish(Duration::from_secs(3)).unwrap();
    let mut report_rx = report_starter.finish(Duration::from_secs(3)).unwrap();

    let mut dop = DopSession::connect_and_login(
        &dop_sock,
        DopConfig {
            strat_id: STRAT_ID,
            client_id: 202,
            local_instance: STRAT_INSTANCE,
            peer_instance: ORS_INSTANCE,
            control_epoch: 1,
            expected_peer_uid: None,
            max_order_age_ns: 23_000,
        },
        1,
        mono_now_ns(),
        Duration::from_secs(3),
    )
    .unwrap();
    intent_writer.sync_from_dop(&dop).unwrap();

    let mut buf_c1 = Vec::new();
    let mut buf_c2 = Vec::new();

    // Warmup round-robin across unmeasured routes:
    // - Order #1 picks `conn_id = 1` (`127.0.0.1`), spending 1/1 IP unit on `127.0.0.1`.
    // - Order #2 picks `conn_id = 2` (`127.0.0.2`), spending 1/100 IP unit on `127.0.0.2`.
    let now1 = clock_ns.load(Ordering::Acquire);
    let intent1 = OutboundIntent::new_order(
        STRAT_ID,
        1,
        u64::from(SYMBOL_ID),
        INSTRUMENT_VERSION,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        100,
        2,
        "cid-1",
        now1,
    )
    .unwrap();
    intent_writer
        .publish_with_dop(&mut dop, &intent1, now1)
        .unwrap();
    let frame1 = wait_ws_client_text(&mut srv_c1, &mut buf_c1, Duration::from_secs(3));
    assert!(frame1.contains("seq=1|"));

    let now2 = clock_ns.load(Ordering::Acquire);
    let intent2 = OutboundIntent::new_order(
        STRAT_ID,
        1,
        u64::from(SYMBOL_ID),
        INSTRUMENT_VERSION,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        100,
        2,
        "cid-2",
        now2,
    )
    .unwrap();
    intent_writer
        .publish_with_dop(&mut dop, &intent2, now2)
        .unwrap();
    let frame2 = wait_ws_client_text(&mut srv_c2, &mut buf_c2, Duration::from_secs(3));
    assert!(frame2.contains("seq=2|"));

    // Order #3 wraps round-robin back to `conn_id = 1` (`127.0.0.1`) whose 1-unit IP budget is now
    // exhausted -> MUST immediately NACK with `RateLimited` WITHOUT retrying `conn_id = 2`
    // (even though `conn_id = 2` is ready with 99/100 IP units available)!
    let now3 = clock_ns.load(Ordering::Acquire);
    let intent3 = OutboundIntent::new_order(
        STRAT_ID,
        1,
        u64::from(SYMBOL_ID),
        INSTRUMENT_VERSION,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        100,
        2,
        "cid-3",
        now3,
    )
    .unwrap();
    intent_writer
        .publish_with_dop(&mut dop, &intent3, now3)
        .unwrap();
    let nack3 = wait_report(&mut report_rx, Duration::from_secs(3));
    assert_eq!(nack3.kind, ExecutionReportKind::Nack);
    assert_eq!(nack3.reason, RefusalReason::RateLimited);
    assert!(try_read_ws_client_text(&mut srv_c2, &mut buf_c2).is_none());

    // Now test `8.8` server maintenance `NOTICE_DRAIN` smooth switchover on `conn_id = 1`:
    // Order #1 is STILL in-flight (`inflight_count == 1`) on `conn_id = 1`.
    srv_c1
        .write_all(&encode_ws_server_text("ev=NOTICE_DRAIN"))
        .unwrap();

    // Wait until `conn_id = 1` is marked not-ready in `PublishedRoutes` while `conn_id = 2` stays ready.
    let pub_routes = proc_handle.accounts()[0].published_routes();
    let drain_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let mut snap = [OrderRoute::default(); 64];
        let n = pub_routes.snapshot(&mut snap);
        let c1_ready = snap[..n]
            .iter()
            .find(|r| r.connection_id == 1)
            .is_some_and(|r| r.ready);
        let c2_ready = snap[..n]
            .iter()
            .find(|r| r.connection_id == 2)
            .is_some_and(|r| r.ready);
        if !c1_ready && c2_ready {
            break;
        }
        assert!(
            Instant::now() < drain_deadline,
            "conn_id=1 was not unpublished upon graceful_drain_connection"
        );
        std::thread::sleep(Duration::from_millis(1));
    }

    // Order #4 now routes to `conn_id = 2` (`127.0.0.2`, fresh 99-unit IP budget)!
    let now4 = clock_ns.load(Ordering::Acquire);
    let intent4 = OutboundIntent::new_order(
        STRAT_ID,
        1,
        u64::from(SYMBOL_ID),
        INSTRUMENT_VERSION,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        100,
        2,
        "cid-4",
        now4,
    )
    .unwrap();
    intent_writer
        .publish_with_dop(&mut dop, &intent4, now4)
        .unwrap();
    let frame4 = wait_ws_client_text(&mut srv_c2, &mut buf_c2, Duration::from_secs(3));
    assert!(frame4.contains("seq=4|"));

    // Meanwhile, `conn_id = 1` stays open to receive the in-flight reply for Order #1!
    let oid1 = make_ors_order_id(1, STRAT_ID);
    srv_c1
        .write_all(&encode_ws_server_text(&format!(
            "rid={oid1}|ev=ACK|oid={oid1}|vid=v-1|px=100|qty=2"
        )))
        .unwrap();
    let rep1 = wait_report(&mut report_rx, Duration::from_secs(3));
    assert_eq!(rep1.kind, ExecutionReportKind::Ack);
    assert_eq!(rep1.ors_order_id, oid1);

    // Once `inflight_count` on `conn_id = 1` hits 0, `poll_graceful_drains` closes `conn_id = 1`!
    let c1_handle = proc_handle.accounts()[0]
        .connections()
        .iter()
        .find(|(cid, _, _)| *cid == 1)
        .map(|(_, _, h)| Arc::clone(h))
        .unwrap();
    let close_deadline = Instant::now() + Duration::from_secs(3);
    while c1_handle.is_ready_relaxed() {
        assert!(
            Instant::now() < close_deadline,
            "draining conn_id=1 did not close after inflight reached 0"
        );
        std::thread::sleep(Duration::from_millis(1));
    }

    // Ack Orders #2 and #4 on `conn_id = 2` and let shutdown cancel #1, #2, #4 cleanly.
    for seq in [2u64, 4u64] {
        let oid = make_ors_order_id(seq, STRAT_ID);
        srv_c2
            .write_all(&encode_ws_server_text(&format!(
                "rid={oid}|ev=ACK|oid={oid}|vid=v-{seq}|px=100|qty=2"
            )))
            .unwrap();
        let _ = wait_report(&mut report_rx, Duration::from_secs(3));
    }

    let srv_c2_drain = std::thread::spawn(move || {
        for _ in 0..3 {
            let cxl = wait_ws_client_text(&mut srv_c2, &mut buf_c2, Duration::from_secs(3));
            assert!(cxl.starts_with("CXL|oid="));
            let oid: u64 = cxl
                .split('|')
                .find_map(|p| p.strip_prefix("oid="))
                .unwrap()
                .parse()
                .unwrap();
            let rid = oid ^ (1u64 << 62);
            srv_c2
                .write_all(&encode_ws_server_text(&format!(
                    "rid={rid}|ev=CANCELED|oid={oid}|cum=0"
                )))
                .unwrap();
        }
    });

    proc_handle.shutdown();
    srv_c2_drain.join().unwrap();
    assert_eq!(proc_handle.open_order_and_quote_count(), 0);
}

#[test]
fn set_strat_enabled_dop_logout_and_signals_sigusr1_sigusr2_enforce_lifecycle_contracts() {
    let dir = PrivateDirectory::create_in(&std::env::temp_dir(), "ors-rt3").unwrap();
    let dop_sock = dir.path().join("dop.sock");
    let rms_path = dir.path().join("rms.cfg");

    // Start with generous `max_qty=100` in static RMS file.
    std::fs::write(
        &rms_path,
        "symbol_id=0 enabled=true max_qty=100 max_notional=1000000000 new_count=100/500/1000 order_qty=10000/50000/100000\n",
    )
    .unwrap();

    let acct_mgr = AccountManager::from_json_str(
        r#"{
            "enabled_accounts": ["acct1"],
            "acct1": {
                "account_ref": 1,
                "exchange_account_id": 100,
                "strategies": [1],
                "local_ip": "127.0.0.1",
                "credentials": { "key": "k1", "secret": "s1" },
                "urls": {
                    "order_ws": { "local_ip/connection_count": [["127.0.0.1", 1]] }
                }
            }
        }"#,
    )
    .unwrap();

    let (rms_engine, _) = tickwire_runtime::build_rms_engine_from_file(1, &rms_path).unwrap();
    let (clock_ns, clock_fn) = deterministic_clock(30_000_000_000);

    let mut ors = OrderRoutingSystem::build(
        OrsRuntimeConfig {
            clock: clock_fn,
            ..OrsRuntimeConfig::default()
        },
        &acct_mgr,
        &[STRAT_ID],
        OrderManagerConfig::single_account_default(
            domain_config(1, 100),
            vec![symbol_spec_spot_0()],
        ),
        rms_engine,
        vec![(1, Arc::new(MockVenueSession::new(None, 0)))],
    )
    .unwrap();

    let (link1, mut srv1) = connect_loopback_ws_pair();
    ors.install_link(1, 1, link1).unwrap();

    let (intent_starter, report_starter) = attach_strategy_lanes(&mut ors);
    let proc_handle = OrsProcess::spawn(
        ors,
        OrsProcessConfig {
            block_signals_on_spawn: false,
            control_socket_path: Some(dop_sock.clone()),
            rms_config_path: Some(rms_path.clone()),
            control_config: ControlServerConfig {
                ors_instance: ORS_INSTANCE,
                ..ControlServerConfig::default()
            },
            ..OrsProcessConfig::default()
        },
    )
    .unwrap();
    let mut intent_writer = intent_starter.finish(Duration::from_secs(3)).unwrap();
    let mut report_rx = report_starter.finish(Duration::from_secs(3)).unwrap();

    let mut dop = DopSession::connect_and_login(
        &dop_sock,
        DopConfig {
            strat_id: STRAT_ID,
            client_id: 303,
            local_instance: STRAT_INSTANCE,
            peer_instance: ORS_INSTANCE,
            control_epoch: 1,
            expected_peer_uid: None,
            max_order_age_ns: 23_000,
        },
        1,
        mono_now_ns(),
        Duration::from_secs(3),
    )
    .unwrap();
    intent_writer.sync_from_dop(&dop).unwrap();
    let mut srv1_buf = Vec::new();

    // 1. Place resting Order #1 (`qty_lots = 20 <= 100`) and ACK it.
    let now1 = clock_ns.load(Ordering::Acquire);
    let o1 = OutboundIntent::new_order(
        STRAT_ID,
        1,
        u64::from(SYMBOL_ID),
        INSTRUMENT_VERSION,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        100,
        20,
        "cid-1",
        now1,
    )
    .unwrap();
    intent_writer.publish_with_dop(&mut dop, &o1, now1).unwrap();
    let _ = wait_ws_client_text(&mut srv1, &mut srv1_buf, Duration::from_secs(3));
    let oid1 = make_ors_order_id(1, STRAT_ID);
    srv1.write_all(&encode_ws_server_text(&format!(
        "rid={oid1}|ev=ACK|oid={oid1}|vid=v-1|px=100|qty=20"
    )))
    .unwrap();
    assert_eq!(
        wait_report(&mut report_rx, Duration::from_secs(3)).kind,
        ExecutionReportKind::Ack
    );

    // 2. Disable `(account_ref = 1, strat_id = 1)` via `ControlServer::set_strat_enabled`:
    //    auto-cancels Order #1 on the wire and refuses subsequent NewOrder with `AdminAction`!
    proc_handle
        .control_server()
        .unwrap()
        .set_strat_enabled(1, STRAT_ID, false)
        .unwrap();
    let auto_cxl = wait_ws_client_text(&mut srv1, &mut srv1_buf, Duration::from_secs(3));
    assert!(auto_cxl.starts_with(&format!("CXL|oid={oid1}|acct=1|")));
    let rid1 = oid1 ^ (1u64 << 62);
    srv1.write_all(&encode_ws_server_text(&format!(
        "rid={rid1}|ev=CANCELED|oid={oid1}|cum=0"
    )))
    .unwrap();
    assert_eq!(
        wait_report(&mut report_rx, Duration::from_secs(3)).kind,
        ExecutionReportKind::Canceled
    );

    let now2 = clock_ns.load(Ordering::Acquire);
    let o2 = OutboundIntent::new_order(
        STRAT_ID,
        1,
        u64::from(SYMBOL_ID),
        INSTRUMENT_VERSION,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        100,
        5,
        "cid-2",
        now2,
    )
    .unwrap();
    intent_writer.publish_with_dop(&mut dop, &o2, now2).unwrap();
    let nack2 = wait_report(&mut report_rx, Duration::from_secs(3));
    assert_eq!(nack2.kind, ExecutionReportKind::Nack);
    assert_eq!(nack2.reason, RefusalReason::AdminAction);

    // Re-enable `(1, STRAT_ID)`.
    proc_handle
        .control_server()
        .unwrap()
        .set_strat_enabled(1, STRAT_ID, true)
        .unwrap();

    // 3. `SIGUSR1`: reload valid tighter RMS limit (`max_qty=5`) -> `qty_lots = 10` is NACKed;
    //    then write malformed RMS config and send `SIGUSR1` -> returns `Err` while retaining `max_qty=5`!
    std::fs::write(
        &rms_path,
        "symbol_id=0 enabled=true max_qty=5 max_notional=1000000000 new_count=100/500/1000 order_qty=10000/50000/100000\n",
    )
    .unwrap();
    let out_valid = proc_handle.handle_signal(OrsSignal::SigUsr1);
    assert!(matches!(
        out_valid,
        OrsSignalOutcome::ReloadedRms { generation: Ok(_) }
    ));
    let active_gen = proc_handle.rms_reload_handle().generation();

    std::fs::write(&rms_path, "symbol_id=0 broken_key=???\n").unwrap();
    let out_invalid = proc_handle.handle_signal(OrsSignal::SigUsr1);
    assert!(matches!(
        out_invalid,
        OrsSignalOutcome::ReloadedRms { generation: Err(_) }
    ));
    assert_eq!(proc_handle.rms_reload_handle().generation(), active_gen);

    // Order #3 with `qty_lots = 10 > 5` is still rejected by the retained `max_qty=5` RMS limit!
    let now3 = clock_ns.load(Ordering::Acquire);
    let o3 = OutboundIntent::new_order(
        STRAT_ID,
        1,
        u64::from(SYMBOL_ID),
        INSTRUMENT_VERSION,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        100,
        10,
        "cid-3",
        now3,
    )
    .unwrap();
    intent_writer.publish_with_dop(&mut dop, &o3, now3).unwrap();
    let nack3 = wait_report(&mut report_rx, Duration::from_secs(3));
    assert_eq!(nack3.kind, ExecutionReportKind::Nack);
    assert_eq!(nack3.reason, RefusalReason::InvalidQuantity);

    // 4. Place Order #4 (`qty_lots = 3 <= 5`), ACK it, then send explicit `DopLogout(cancel_on_logout = true)`!
    let now4 = clock_ns.load(Ordering::Acquire);
    let o4 = OutboundIntent::new_order(
        STRAT_ID,
        1,
        u64::from(SYMBOL_ID),
        INSTRUMENT_VERSION,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        100,
        3,
        "cid-4",
        now4,
    )
    .unwrap();
    intent_writer.publish_with_dop(&mut dop, &o4, now4).unwrap();
    let _ = wait_ws_client_text(&mut srv1, &mut srv1_buf, Duration::from_secs(3));
    let oid4 = make_ors_order_id(4, STRAT_ID);
    srv1.write_all(&encode_ws_server_text(&format!(
        "rid={oid4}|ev=ACK|oid={oid4}|vid=v-4|px=100|qty=3"
    )))
    .unwrap();
    assert_eq!(
        wait_report(&mut report_rx, Duration::from_secs(3)).kind,
        ExecutionReportKind::Ack
    );

    dop.logout(
        true,
        TerminalReason::AdminAction,
        Instant::now() + Duration::from_secs(2),
    )
    .unwrap();
    let logout_cxl = wait_ws_client_text(&mut srv1, &mut srv1_buf, Duration::from_secs(3));
    assert!(logout_cxl.starts_with(&format!("CXL|oid={oid4}|acct=1|")));
    let rid4 = oid4 ^ (1u64 << 62);
    srv1.write_all(&encode_ws_server_text(&format!(
        "rid={rid4}|ev=CANCELED|oid={oid4}|cum=0"
    )))
    .unwrap();
    assert_eq!(
        wait_report(&mut report_rx, Duration::from_secs(3)).kind,
        ExecutionReportKind::Canceled
    );

    // 5. Re-login via DOP (`next_order_seqnum >= 5` preserved across logout!), place Order #5,
    //    then trigger `SIGUSR2` emergency `CancelAll` + `OrsRestricted` broadcast!
    let mut dop2 = DopSession::connect_and_login(
        &dop_sock,
        DopConfig {
            strat_id: STRAT_ID,
            client_id: 304,
            local_instance: STRAT_INSTANCE,
            peer_instance: ORS_INSTANCE,
            control_epoch: 1,
            expected_peer_uid: None,
            max_order_age_ns: 23_000,
        },
        1,
        mono_now_ns(),
        Duration::from_secs(3),
    )
    .unwrap();
    assert_eq!(dop2.next_order_seqnum(), 5);
    intent_writer.sync_from_dop(&dop2).unwrap();

    let now5 = clock_ns.load(Ordering::Acquire);
    let o5 = OutboundIntent::new_order(
        STRAT_ID,
        1,
        u64::from(SYMBOL_ID),
        INSTRUMENT_VERSION,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        100,
        3,
        "cid-5",
        now5,
    )
    .unwrap();
    intent_writer
        .publish_with_dop(&mut dop2, &o5, now5)
        .unwrap();
    let _ = wait_ws_client_text(&mut srv1, &mut srv1_buf, Duration::from_secs(3));
    let oid5 = make_ors_order_id(5, STRAT_ID);
    srv1.write_all(&encode_ws_server_text(&format!(
        "rid={oid5}|ev=ACK|oid={oid5}|vid=v-5|px=100|qty=3"
    )))
    .unwrap();
    assert_eq!(
        wait_report(&mut report_rx, Duration::from_secs(3)).kind,
        ExecutionReportKind::Ack
    );

    assert_eq!(
        proc_handle.handle_signal(OrsSignal::SigUsr2),
        OrsSignalOutcome::EmergencyCancelAll
    );
    assert_eq!(proc_handle.policy().mode(), Mode::ManageOnly);

    let sigusr2_cxl = wait_ws_client_text(&mut srv1, &mut srv1_buf, Duration::from_secs(3));
    assert!(sigusr2_cxl.starts_with(&format!("CXL|oid={oid5}|acct=1|")));
    let rid5 = oid5 ^ (1u64 << 62);
    srv1.write_all(&encode_ws_server_text(&format!(
        "rid={rid5}|ev=CANCELED|oid={oid5}|cum=0"
    )))
    .unwrap();

    // Drain `OrsRestricted` and `Canceled` (in whichever cross-thread arrival order).
    let mut saw_restricted = false;
    let mut saw_canceled = false;
    for _ in 0..2 {
        match wait_report(&mut report_rx, Duration::from_secs(3)).kind {
            ExecutionReportKind::OrsRestricted => saw_restricted = true,
            ExecutionReportKind::Canceled => saw_canceled = true,
            other => panic!("unexpected report after SIGUSR2: {other:?}"),
        }
    }
    assert!(saw_restricted && saw_canceled);

    proc_handle.shutdown();
    let shut_rep = wait_report(&mut report_rx, Duration::from_secs(3));
    assert_eq!(shut_rep.kind, ExecutionReportKind::OrsShutDown);
}
