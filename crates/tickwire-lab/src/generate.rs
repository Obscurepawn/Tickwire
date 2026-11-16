//! Stateless 3-process laboratory directory & configuration generator (`tickwire-md`,
//! `tickwire-app`, and `tickwire-strategy-worker`) with zero WAL, journal, or disk blob files.

#![forbid(unsafe_code)]

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::net::{SocketAddr, TcpListener};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::Path;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ring::rand::{SecureRandom, SystemRandom};
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tickwire_account_gateway::AccountManagerConfig;
use tickwire_app::{
    BinanceAssetConfig, BinanceMarketConfigBlock, BinanceMarketKind, BinanceSymbolConfig,
    ExternalStrategyLaneConfig, OrsAppConfig, OrsVenueKind, UtaCpuSpec, UtaEndpointConfig,
    UtaEnvironmentKind, UtaTransportKind,
};
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_market_source::protocol::{Endpoint, IdentityConfig, Instrument, wire_bytes};
use tickwire_strategy_sdk::DopConfig;
use tickwire_strategy_sdk::bootstrap::Bootstrap;

use crate::model::{
    LAB_ACCOUNT_REF, LAB_BTC_INSTRUMENT_ID, LAB_CATALOG_HASH, LAB_CATALOG_VERSION,
    LAB_COLD_OPEN_SEQNUM, LAB_EXCHANGE_UID, LAB_INSTRUMENT_VERSION, LAB_LOGICAL_STRATEGY_ID,
    LAB_RISK_DOMAIN, LAB_STRAT_ID, build_demo_command,
};
use crate::{Result, Scenario};

const SCENARIO_LIFETIME_NS: u64 = 3_600_000_000_000; // 1 hour
/// Generous 2-second order-age ceiling for unpinned debug-build 3-process laboratory scheduling.
const LAB_MAX_ORDER_AGE_NS: u64 = 2_000_000_000;

fn sha256_bytes(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn read_linux_boot_id() -> Result<[u8; 16]> {
    let raw = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    let hex_only: String = raw.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    if hex_only.len() != 32 {
        return Err("unexpected /proc/sys/kernel/random/boot_id length".into());
    }
    let mut boot = [0u8; 16];
    for (idx, pair) in hex_only.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let s = core::str::from_utf8(pair)?;
        boot[idx] = u8::from_str_radix(s, 16)?;
    }
    Ok(boot)
}

/// Generates a fresh, single-use `0o700` laboratory directory at `root` referencing stripped
/// binaries in `bins`.
pub fn generate(root: &Path, bins: &Path) -> Result<Scenario> {
    if !root.is_absolute() || !bins.is_absolute() || root.as_os_str().len() > 65 {
        return Err(
            "absolute root (<= 65 bytes for AF_UNIX sockets) and binary dir required".into(),
        );
    }
    for bin in [
        "tickwire-app",
        "tickwire-md",
        "tickwire-strategy-worker",
        "tickwire-lab",
    ] {
        if !bins.join(bin).is_file() {
            return Err(format!("missing required binary in {}: {bin}", bins.display()).into());
        }
    }

    fs::DirBuilder::new().mode(0o700).create(root)?;
    for subdir in [
        "tls",
        "keys",
        "md",
        "md/admissions",
        "ors",
        "strategy",
        "logs",
    ] {
        create_private_dir(&root.join(subdir))?;
    }

    let rng = SystemRandom::new();
    let boot = read_linux_boot_id()?;

    let clock_domain =
        tickwire_clock::ClockDomain::new(boot, 1).map_err(|_| "ClockDomain::new failed")?;
    let now_mono_ns = tickwire_clock_sys::SystemClock::new(clock_domain)?
        .sample()?
        .monotonic
        .nanos();
    let valid_until_ns = now_mono_ns
        .checked_add(SCENARIO_LIFETIME_NS)
        .ok_or("clock deadline overflow")?;

    let demo_cmd = build_demo_command(now_mono_ns, valid_until_ns)?;
    write_private(&root.join("strategy/command.bin"), demo_cmd.bytes())?;

    let listen = reserve_loopback_port()?;
    let tls_bundle = generate_loopback_tls()?;
    write_private(&root.join("tls/ca.der"), &tls_bundle.ca_der)?;
    write_private(&root.join("tls/ca.pem"), tls_bundle.ca_pem.as_bytes())?;
    write_private(&root.join("tls/fixture.der"), &tls_bundle.leaf_der)?;
    write_private(&root.join("tls/fixture.pk8"), &tls_bundle.leaf_pk8)?;

    write_private(&root.join("keys/api-key"), b"LAB_SPOT_API_KEY_01")?;
    let mut raw_secret = [0u8; 16];
    rng.fill(&mut raw_secret)
        .map_err(|_| "SystemRandom hmac secret failed")?;
    let hex_secret: String = raw_secret.iter().map(|b| format!("{b:02x}")).collect();
    write_private(&root.join("keys/api-secret"), hex_secret.as_bytes())?;

    let ed_pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).map_err(|_| "Ed25519 keygen failed")?;
    let ed_pair =
        Ed25519KeyPair::from_pkcs8(ed_pkcs8.as_ref()).map_err(|_| "Ed25519 parse failed")?;
    let ed_pem = format!(
        "-----BEGIN PRIVATE KEY-----\n{}\n-----END PRIVATE KEY-----\n",
        BASE64.encode(ed_pkcs8.as_ref())
    );
    write_private(&root.join("keys/ed25519.pem"), ed_pem.as_bytes())?;
    write_private(
        &root.join("keys/ed25519.pub"),
        ed_pair.public_key().as_ref(),
    )?;

    let meta = fs::metadata(root)?;
    let uid = meta.uid();
    let gid = meta.gid();

    let md_id = build_identity(
        3,
        2,
        0,
        1,
        boot,
        sha256_bytes(&fs::read(bins.join("tickwire-md"))?),
    );
    let ors_id = build_identity(
        4,
        4,
        0,
        1,
        boot,
        sha256_bytes(&fs::read(bins.join("tickwire-app"))?),
    );
    let strategy_id = build_identity(
        2,
        3,
        LAB_LOGICAL_STRATEGY_ID,
        5,
        boot,
        sha256_bytes(&fs::read(bins.join("tickwire-strategy-worker"))?),
    );
    let supervisor_id = build_identity(
        9,
        1,
        0,
        1,
        boot,
        sha256_bytes(&fs::read(bins.join("tickwire-lab"))?),
    );
    write_json(
        &root.join("supervisor.json"),
        &serde_json::to_value(&supervisor_id)?,
    )?;

    // 1. `tickwire-md` SHM topic resource & static configuration (`0` disk WAL).
    let md_resource = tickwire_md::config::ResourceSpec {
        identity: md_id.clone(),
        resource_id: [101; 16],
        channel_id: [102; 16],
        topic_id: 101,
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        catalog_version: LAB_CATALOG_VERSION,
        first_seq: 1,
        initial_membership_version: 1,
        capacity: 1024,
        created_mono_ns: now_mono_ns,
        arena_classes: vec![(4096, 128), (65536, 32)],
    }
    .header()
    .map_err(|e| e.to_string())?;
    write_private(&root.join("md/topic.bin"), &wire_bytes(&md_resource)?)?;

    let asset =
        |id: u64, sym: &str| json!({ "id": id, "version": 1, "symbol": sym, "exponent": -8 });
    let btc_instrument = json!({
        "id": LAB_BTC_INSTRUMENT_ID,
        "version": LAB_INSTRUMENT_VERSION,
        "catalog_version": LAB_CATALOG_VERSION,
        "catalog_hash": LAB_CATALOG_HASH,
        "symbol": "BTCUSDT",
        "base": asset(1, "BTC"),
        "quote": asset(2, "USDT"),
        "settlement": asset(2, "USDT"),
        "price_tick": "0.01",
        "quantity_step": "0.00001",
        "expiry_utc_ns": null
    });
    let md_config = json!({
        "identity": md_id,
        "clock_generation": 1,
        "socket": root.join("md/market.sock"),
        "admission_directory": root.join("md/admissions"),
        "feeds": [{
            "owner_id": ([42u8; 16]),
            "source": "spot_test",
            "instrument": btc_instrument,
            "venue_id": 1,
            "stream_id": 1,
            "first_connection_epoch": 1,
            "first_operation": 1,
            "initial_book_generation": 1,
            "snapshot_limit": 100,
            "stream_address": listen.to_string(),
            "snapshot_address": listen.to_string(),
            "trust_anchors": [root.join("tls/ca.der")],
            "topics": [101]
        }],
        "topics": [{
            "resource": root.join("md/topic.bin"),
            "instrument_set_version": 1,
            "maximum_levels": 100,
            "maximum_cache_bytes": 1_048_576,
            "maximum_snapshot_bytes": 2_097_152,
            "maximum_delta_bytes": 2_097_152
        }],
        "maximum_idle_ns": 1_000_000
    });
    write_json(&root.join("md/config.json"), &md_config)?;

    // 2. Inline `1,024B` `IntentRecord` and `512B` `ReportRecord` SPSC SHM bank headers.
    let intent_hdr = build_spsc_bank_header(2, &strategy_id, &ors_id, now_mono_ns)?;
    let report_hdr = build_spsc_bank_header(3, &ors_id, &strategy_id, now_mono_ns)?;
    tickwire_shm::validate_bank(&intent_hdr).map_err(|e| format!("intent bank invalid: {e}"))?;
    tickwire_shm::validate_bank(&report_hdr).map_err(|e| format!("report bank invalid: {e}"))?;

    let mk_endpoint =
        |local: IdentityConfig, peer: IdentityConfig, rel_sock: &str, epoch: u64, nonce_tag: u8| {
            Endpoint {
                path: root.join(rel_sock),
                local,
                peer,
                peer_pid: 1, // Patched in `launch::run` once `tickwire-md` and `tickwire-app` PIDs exist.
                peer_uid: uid,
                peer_gid: gid,
                required_features: 0x1c,
                supported_features: 0x3ff,
                control_epoch: epoch,
                lease_duration_ns: SCENARIO_LIFETIME_NS,
                max_frame_bytes: 65_536,
                nonce: [nonce_tag; 32],
            }
        };

    let worker_md_ep = mk_endpoint(
        strategy_id.clone(),
        md_id.clone(),
        "md/market.sock",
        1,
        0x61,
    );
    let worker_intent_ep = mk_endpoint(
        strategy_id.clone(),
        ors_id.clone(),
        "ors/intent.sock",
        10,
        0x62,
    );
    let worker_report_ep = mk_endpoint(
        strategy_id.clone(),
        ors_id.clone(),
        "ors/report.sock",
        11,
        0x63,
    );

    // 3. Static `rms.cfg` and `OrsAppConfig` for `tickwire-app` (`1 ORS process = 1 Venue`).
    let rms_text = "\
symbol_id=0 enabled=true max_qty=1000000 max_notional=1000000000000 new_count=1000/5000/10000 order_qty=1000000/5000000/10000000
symbol_id=1 enabled=true max_qty=1000000 max_notional=1000000000000 new_count=1000/5000/10000 order_qty=1000000/5000000/10000000
";
    write_private(&root.join("ors/rms.cfg"), rms_text.as_bytes())?;

    let acct_mgr_doc = json!({
        "enabled_accounts": ["spot_main"],
        "spot_main": {
            "account_ref": LAB_ACCOUNT_REF,
            "exchange_account_id": LAB_EXCHANGE_UID,
            "strategies": [LAB_STRAT_ID],
            "local_ip": "127.0.0.1",
            "epoch": 1,
            "risk_domain_id": LAB_RISK_DOMAIN,
            "credentials": {
                "key": "LAB_SPOT_API_KEY_01",
                "secret": hex_secret
            },
            "urls": {
                "binance-spot-order": { "local_ip/connection_count": [["127.0.0.1", 1]] }
            }
        }
    });
    let account_manager: AccountManagerConfig = serde_json::from_value(acct_mgr_doc)?;

    let tls_ep = |path: &str| UtaEndpointConfig {
        host: String::from("localhost"),
        port: listen.port(),
        path: path.to_owned(),
        tls: true,
        ca_pem_path: Some(root.join("tls/ca.pem")),
        initial_peers: vec![listen],
    };

    let ors_app_cfg = OrsAppConfig {
        venue: OrsVenueKind::Binance,
        environment: UtaEnvironmentKind::Test,
        uta_transport: UtaTransportKind::Ws,
        cpu: UtaCpuSpec {
            req_cpu: None,
            resp_cpu: None,
            housekeeping_cpus: Vec::new(),
            epoll_wait_ms: 1,
        },
        max_order_age_ns: LAB_MAX_ORDER_AGE_NS,
        rms_on_dry_run: true,
        exec_generation: 1,
        instrument_version: LAB_INSTRUMENT_VERSION,
        ws_ping_interval_ms: 15_000,
        slots_per_strat: 64,
        ors_instance: Some(ors_id.instance),
        allowed_strategies: vec![LAB_STRAT_ID],
        account_manager,
        binance_markets: vec![BinanceMarketConfigBlock {
            market: BinanceMarketKind::Spot,
            ed25519_pem_path: Some(root.join("keys/ed25519.pem")),
            order_endpoint: Some(tls_ep("/ws-api/v3")),
            stream_endpoint: None,
            rest_endpoint: Some(tls_ep("")),
            stream_colo: true,
        }],
        // Dense `symbol_id = 0..=1` so `symbol_id = 1` matches `LAB_BTC_INSTRUMENT_ID = 1`.
        binance_symbols: vec![
            BinanceSymbolConfig {
                symbol_id: 0,
                instrument_id: Some(999),
                market: BinanceMarketKind::Spot,
                venue_symbol: String::from("ETHUSDT"),
                wallet_id: 1,
                base_asset_id: 3,
                base_scale: 8,
                quote_asset_id: 2,
                quote_scale: 8,
                settlement_asset_id: 2,
                settlement_scale: 8,
                price_tick_atoms: 1_000_000,
                price_scale: 8,
                qty_step_atoms: 1_000,
                qty_scale: 8,
                contract_size_atoms: 1,
                contract_scale: 0,
                initial_margin_bps: 10_000,
                default_fee_asset_code: Some(String::from("USDT")),
            },
            BinanceSymbolConfig {
                symbol_id: 1,
                instrument_id: Some(LAB_BTC_INSTRUMENT_ID),
                market: BinanceMarketKind::Spot,
                venue_symbol: String::from("BTCUSDT"),
                wallet_id: 1,
                base_asset_id: 1,
                base_scale: 8,
                quote_asset_id: 2,
                quote_scale: 8,
                settlement_asset_id: 2,
                settlement_scale: 8,
                price_tick_atoms: 1_000_000, // 0.01 USDT
                price_scale: 8,
                qty_step_atoms: 1_000, // 0.00001 BTC (`100` lots == `0.001` BTC)
                qty_scale: 8,
                contract_size_atoms: 1,
                contract_scale: 0,
                initial_margin_bps: 10_000,
                default_fee_asset_code: Some(String::from("USDT")),
            },
        ],
        binance_assets: vec![
            BinanceAssetConfig {
                market: BinanceMarketKind::Spot,
                asset_code: String::from("BTC"),
                wallet_id: 1,
                asset_id: 1,
                scale: 8,
            },
            BinanceAssetConfig {
                market: BinanceMarketKind::Spot,
                asset_code: String::from("USDT"),
                wallet_id: 1,
                asset_id: 2,
                scale: 8,
            },
            BinanceAssetConfig {
                market: BinanceMarketKind::Spot,
                asset_code: String::from("ETH"),
                wallet_id: 1,
                asset_id: 3,
                scale: 8,
            },
        ],
        binance_order_recv_window_ms: 5_000,
        binance_status_query_delay_ms: 400,
        uta_symbols: Vec::new(),
        uta_assets: Vec::new(),
        uta_order_endpoint: None,
        uta_stream_endpoint: None,
        uta_rest_endpoint: None,
        uta_enable_rfq_quotes: false,
        instrument_catalog: None,
        rms_config_path: Some(root.join("ors/rms.cfg")),
        control_socket_path: Some(root.join("ors/dop.sock")),
        dop_hb_interval_ms: Some(40),
        external_strategy_lanes: vec![ExternalStrategyLaneConfig {
            strat_id: LAB_STRAT_ID,
            instance_id: strategy_id.instance,
            strategy_generation: strategy_id.generation,
            intent_endpoint: worker_intent_ep.clone(),
            intent_header_bytes: wire_bytes(&intent_hdr)?.to_vec(),
            intent_cursor_id: [35; 16],
            intent_subscription_generation: 1,
            report_endpoint: worker_report_ep.clone(),
            report_header_bytes: wire_bytes(&report_hdr)?.to_vec(),
            report_cursor_id: [36; 16],
            report_subscription_generation: 1,
            clock_domain: 1,
            handshake_timeout_ms: 30_000,
        }],
        summary_output_path: Some(root.join("account-report.json")),
    };
    write_json(
        &root.join("ors/config.json"),
        &serde_json::to_value(&ors_app_cfg)?,
    )?;

    // 4. `tickwire-strategy-worker` `Bootstrap` (`DOP` + inline `IntentRecord` + `ReportRecord`).
    let bootstrap = Bootstrap {
        version: 1,
        logical_strategy_id: LAB_LOGICAL_STRATEGY_ID,
        strat_id: LAB_STRAT_ID,
        instance_id: strategy_id.instance,
        generation: strategy_id.generation,
        account_ref: LAB_ACCOUNT_REF,
        worker_cpu: None,
        housekeeping_cpus: None,
        market: tickwire_market_source::ConnectConfig {
            endpoint: worker_md_ep,
            bank_header: wire_bytes(&md_resource)?.to_vec(),
            catalog_hash: LAB_CATALOG_HASH,
            instrument_set_version: 1,
            instruments: vec![Instrument {
                id: LAB_BTC_INSTRUMENT_ID,
                version: LAB_INSTRUMENT_VERSION,
                required_states: 0,
            }],
            cursor_id: [31; 16],
            subscription_generation: 1,
            request_id: [32; 16],
            maximum_levels: 100,
            startup_timeout_ms: 30_000,
            clock_generation: 1,
        },
        dop_socket: Some(root.join("ors/dop.sock")),
        dop: Some(DopConfig {
            strat_id: LAB_STRAT_ID,
            client_id: 1,
            local_instance: strategy_id.instance,
            peer_instance: ors_id.instance,
            control_epoch: 1,
            expected_peer_uid: Some(uid),
            max_order_age_ns: LAB_MAX_ORDER_AGE_NS,
        }),
        intent: Some(worker_intent_ep),
        intent_header: Some(wire_bytes(&intent_hdr)?.to_vec()),
        intent_cursor_id: [35; 16],
        intent_subscription_generation: 1,
        report: Some(worker_report_ep),
        report_header: Some(wire_bytes(&report_hdr)?.to_vec()),
        report_cursor_id: [36; 16],
        report_subscription_generation: 1,
        risk_domain: LAB_RISK_DOMAIN,
        instrument_catalog: None,
        catalog_file: None,
        catalog_digest: None,
        factory: None,
        clock_generation: 1,
        startup_timeout_ms: 30_000,
        maximum_market_age_ns: 10_000_000_000,
        maximum_levels: 100,
        poll_records: 64,
        idle_sleep_us: 100,
        demo_enabled: true,
        demo_command_file: Some(root.join("strategy/command.bin")),
        demo_minimum_bid_ticks: 6_000_000, // Fires once `fixture` raises BTCUSDT bid to 60,000.00!
    };
    write_json(
        &root.join("strategy/bootstrap.json"),
        &serde_json::to_value(&bootstrap)?,
    )?;

    let scenario = Scenario {
        schema: 2,
        fixture_only: true,
        root: root.to_path_buf(),
        bin_directory: bins.to_path_buf(),
        listen,
        valid_until_ns,
        boot,
        risk_domain_id: LAB_RISK_DOMAIN,
        exchange_uid: LAB_EXCHANGE_UID,
        strat_id: LAB_STRAT_ID,
        cold_open_order_seqnum: LAB_COLD_OPEN_SEQNUM,
        certificate: root.join("tls/fixture.der"),
        private_key: root.join("tls/fixture.pk8"),
        api_key: root.join("keys/api-key"),
        api_secret: root.join("keys/api-secret"),
        ed25519_public_key: root.join("keys/ed25519.pub"),
        md_config: root.join("md/config.json"),
        app_config: root.join("ors/config.json"),
        worker_bootstrap: root.join("strategy/bootstrap.json"),
    };
    write_json(
        &root.join("scenario.json"),
        &serde_json::to_value(&scenario)?,
    )?;
    Ok(scenario)
}

fn build_identity(
    id: u8,
    role: u16,
    logical_strategy: u64,
    generation: u64,
    host_boot: [u8; 16],
    build_hash: [u8; 32],
) -> IdentityConfig {
    IdentityConfig {
        instance: [id; 16],
        generation,
        role,
        logical_strategy,
        host_boot,
        namespace: [8; 16],
        build_hash,
        qualification_hash: sha256_bytes(b"tickwire-stateless-3proc-lab-qualification"),
        manifest_hash: sha256_bytes(b"tickwire-stateless-3proc-spot-fill"),
        arch: if cfg!(target_arch = "x86_64") { 1 } else { 2 },
        trust_profile: 1,
    }
}

fn build_spsc_bank_header(
    kind: u16,
    producer: &IdentityConfig,
    consumer: &IdentityConfig,
    created_mono_ns: u64,
) -> Result<ResourceHeader> {
    let record_bytes: u32 = if kind == 2 { 1024 } else { 512 };
    let capacity: u32 = 256;
    let feature_bit: u64 = if kind == 2 {
        tickwire_strategy_sdk::outbound::FEATURE
    } else {
        tickwire_strategy_sdk::reports::FEATURE
    };
    let mut header = ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
        abi_minor: 0,
        header_bytes: 4096,
        resource_kind: kind,
        target_arch: producer.arch,
        endian: 1,
        atomic_width: 8,
        page_shift: 12,
        isolation_shift: 7,
        record_bytes,
        capacity,
        resource_bytes: 8192 + u64::from(capacity) * u64::from(record_bytes),
        resource_id: [70 + kind as u8; 16],
        channel_id: [74 + kind as u8; 16],
        producer_instance: producer.instance,
        consumer_instance: consumer.instance,
        producer_generation: producer.generation,
        host_boot_id: producer.host_boot,
        namespace_id: producer.namespace,
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        first_seq: 1,
        initial_membership_version: 1,
        producer_page_offset: 4096,
        slot_offset: 8192,
        max_consumers: 1,
        schema_major: 1,
        clock_domain: 1,
        trust_profile: 1,
        risk_scope: LAB_EXCHANGE_UID,
        required_features: 0x1c | feature_bit,
        qos: 1,
        event_class: if kind == 2 { 8 } else { 9 },
        created_mono_ns,
        ..Default::default()
    };
    let mut page = [0u8; ResourceHeader::SIZE];
    header.descriptor_hash = [0; 32];
    header
        .encode(&mut page)
        .map_err(|e| format!("ResourceHeader::encode failed: {e:?}"))?;
    header.descriptor_hash = sha256_bytes(&page);
    Ok(header)
}

fn reserve_loopback_port() -> Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?)
}

struct GeneratedTlsBundle {
    ca_der: Vec<u8>,
    ca_pem: String,
    leaf_der: Vec<u8>,
    leaf_pk8: Vec<u8>,
}

fn generate_loopback_tls() -> Result<GeneratedTlsBundle> {
    use rcgen::{BasicConstraints, CertificateParams, DistinguishedName, IsCa, Issuer, KeyPair};
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.distinguished_name = DistinguishedName::new();
    let ca_key = KeyPair::generate()?;
    let ca_cert = ca_params.self_signed(&ca_key)?;
    let issuer = Issuer::new(ca_params, ca_key);

    let leaf_params = CertificateParams::new(vec![
        "localhost".into(),
        "127.0.0.1".into(),
        "testnet.binance.vision".into(),
        "stream.testnet.binance.vision".into(),
        "ws-api.testnet.binance.vision".into(),
    ])?;
    let leaf_key = KeyPair::generate()?;
    let leaf_cert = leaf_params.signed_by(&leaf_key, &issuer)?;
    Ok(GeneratedTlsBundle {
        ca_der: ca_cert.der().to_vec(),
        ca_pem: ca_cert.pem(),
        leaf_der: leaf_cert.der().to_vec(),
        leaf_pk8: leaf_key.serialize_der(),
    })
}

pub fn create_private_dir(path: &Path) -> Result<()> {
    fs::DirBuilder::new().mode(0o700).create(path)?;
    File::open(path.parent().ok_or("missing parent directory")?)?.sync_all()?;
    Ok(())
}

pub fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    File::open(path.parent().ok_or("missing parent directory")?)?.sync_all()?;
    Ok(())
}

pub fn write_json(path: &Path, value: &Value) -> Result<()> {
    let mut raw = serde_json::to_vec_pretty(value)?;
    raw.push(b'\n');
    write_private(path, &raw)
}
