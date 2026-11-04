use crate::*;
use ring::rand::{SecureRandom, SystemRandom};
use serde_json::{Value, json};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    net::TcpListener,
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::Path,
};
use tickwire_ipc::ResourceHeader;
use tickwire_market_source::protocol::{Endpoint, IdentityConfig, Instrument, wire_bytes};
use tickwire_recovery::digest;
const LIFE: u64 = 3_600_000_000_000;
pub fn generate(root: &Path, bins: &Path) -> Result<Scenario> {
    if !root.is_absolute() || !bins.is_absolute() || root.as_os_str().len() > 65 {
        return Err("absolute root <=65 bytes and binary directory required".into());
    }
    for bin in [
        "tickwire-app",
        "tickwire-md",
        "tickwire-strategy-worker",
        "tickwire-lab",
    ] {
        if !bins.join(bin).is_file() {
            return Err(format!("build binary before generation: {bin}").into());
        }
    }
    fs::DirBuilder::new().mode(0o700).create(root)?;
    for d in [
        "tls",
        "keys",
        "md",
        "md/admissions",
        "md/control",
        "exec",
        "exec/control-inbox",
        "exec/control-wal",
        "host",
        "host/control",
        "host/runtime",
        "strategy",
        "strategy/commands",
        "strategy/recovery",
        "strategy/dependencies",
        "logs",
    ] {
        directory(&root.join(d))?;
    }
    let mut boot = [0; 16];
    SystemRandom::new().fill(&mut boot).map_err(|_| "random")?;
    let domain = tickwire_clock::ClockDomain::new(boot, 1).map_err(|_| "clock identity")?;
    let now = tickwire_clock_sys::SystemClock::new(domain)?
        .sample()?
        .monotonic
        .nanos();
    let expires = now.checked_add(LIFE).ok_or("clock bounds")?;
    let (baseline, command) = crate::model::model(now, expires)?;
    write(&root.join("baseline.bin"), baseline.bytes())?;
    write(&root.join("strategy/command.bin"), command.bytes())?;
    let listen = free_address()?;
    let (ca, fixture_cert, fixture_key) = certificates()?;
    for (name, bytes) in [
        ("ca.der", ca),
        ("fixture.der", fixture_cert),
        ("fixture.pk8", fixture_key),
    ] {
        write(&root.join("tls").join(name), &bytes)?;
    }
    write(&root.join("keys/api-key"), b"LAB-ONLY-API-KEY")?;
    let mut secret = [0u8; 32];
    SystemRandom::new()
        .fill(&mut secret)
        .map_err(|_| "random")?;
    write(&root.join("keys/api-secret"), &secret)?;
    let uid = fs::metadata(root)?.uid();
    let gid = fs::metadata(root)?.gid();
    let md = identity(
        3,
        2,
        0,
        1,
        boot,
        digest(&fs::read(bins.join("tickwire-md"))?),
    );
    let exec = identity(
        4,
        4,
        0,
        1,
        boot,
        digest(&fs::read(bins.join("tickwire-app"))?),
    );
    let strategy = identity(
        2,
        3,
        8,
        5,
        boot,
        digest(&fs::read(bins.join("tickwire-strategy-worker"))?),
    );
    let supervisor = identity(
        9,
        1,
        0,
        1,
        boot,
        digest(&fs::read(bins.join("tickwire-lab"))?),
    );
    let resource = tickwire_md::config::ResourceSpec {
        identity: md.clone(),
        resource_id: ([101; 16]),
        channel_id: ([102; 16]),
        topic_id: 101,
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        catalog_version: 14,
        first_seq: 1,
        initial_membership_version: 1,
        capacity: 4096,
        created_mono_ns: now,
        arena_classes: vec![(4096, 256), (65536, 64)],
    }
    .header()
    .map_err(|e| e.to_string())?;
    write(&root.join("md/topic.bin"), &wire_bytes(&resource)?)?;
    let endpoint = |local: IdentityConfig, peer: IdentityConfig, name: &str| Endpoint {
        path: root.join(name),
        local,
        peer,
        peer_pid: 1,
        peer_uid: uid,
        peer_gid: gid,
        required_features: 0x5e,
        supported_features: 511,
        control_epoch: 1,
        lease_duration_ns: LIFE,
        max_frame_bytes: 65536,
        nonce: ([61; 32]),
    };
    let connect =
        |local: IdentityConfig, cursor: u8, request: u8| tickwire_market_source::ConnectConfig {
            endpoint: endpoint(local, md.clone(), "md/market.sock"),
            bank_header: wire_bytes(&resource).unwrap(),
            catalog_hash: ([5; 32]),
            instrument_set_version: 1,
            instruments: vec![Instrument {
                id: 12,
                version: 13,
                required_states: 1,
            }],
            cursor_id: ([cursor; 16]),
            subscription_generation: 1,
            request_id: ([request; 16]),
            maximum_levels: 100,
            startup_timeout_ms: 30000,
            clock_generation: 1,
        };
    let external = connect(strategy.clone(), 31, 32);
    let exec_source = connect(exec.clone(), 33, 34);
    let intent = bank(2, &strategy, &exec, now);
    let report = bank(3, &exec, &strategy, now);
    for (name, header) in [("intent", &intent), ("report", &report)] {
        tickwire_shm::validate_bank(header).map_err(|error|format!("generated {name} bank: features={:#x},kind={},record_bytes={},capacity={}: {error}",header.required_features,header.resource_kind,header.record_bytes,header.capacity))?;
    }
    let bootstrap = tickwire_strategy_sdk::bootstrap::Bootstrap {
        instrument_catalog: None,
        version: 1,
        market: endpoint(strategy.clone(), md.clone(), "unused-market.sock"),
        external_market: Some(external),
        market_admission_ready: None,
        intent: endpoint(strategy.clone(), exec.clone(), "unused-intent.sock"),
        report: endpoint(strategy.clone(), exec.clone(), "unused-report.sock"),
        report_header: wire_bytes(&report)?,
        report_cursor_id: ([36; 16]),
        report_subscription_generation: 1,
        report_directory: root.join("exec/reports"),
        join_file: root.join("strategy/join.json"),
        intent_header: wire_bytes(&intent)?,
        intent_cursor_id: ([35; 16]),
        intent_subscription_generation: 1,
        risk_domain: ([1; 16]),
        command_directory: root.join("strategy/commands"),
        clock_generation: 1,
        startup_timeout_ms: 30000,
        maximum_market_age_ns: 10_000_000_000,
        maximum_levels: 100,
        poll_records: 64,
        idle_sleep_us: 100,
        recovery_template: Some(root.join("strategy/recovery-template.json")),
        publish_checkpoints: true,
        preflight_job: None,
        factory: None,
        demo_enabled: true,
        demo_command_file: Some(root.join("strategy/command.bin")),
        demo_minimum_bid_ticks: 6_000_000,
    };
    json_file(
        &root.join("strategy/bootstrap.json"),
        &serde_json::to_value(&bootstrap)?,
    )?;
    let policy = policy(boot, expires);
    let asset = |id, s: &str| json!({"id":id,"version":1,"symbol":s,"exponent":-8});
    let instrument = json!({"id":12,"version":13,"catalog_version":14,"catalog_hash":( [5;32] ),"symbol":"BTCUSDT","base":asset(1,"BTC"),"quote":asset(2,"USDT"),"settlement":asset(2,"USDT"),"price_tick":"0.01","quantity_step":"0.00001","expiry_utc_ns":null});
    let md_config = json!({"identity":md,"clock_generation":1,"socket":root.join("md/market.sock"),"admission_directory":root.join("md/admissions"),"quota_directory":root.join("md/quota"),"quota":quota(now,expires,40,true),"control_directory":root.join("md/control"),"control_stream":( [50;16] ),"control_domain":( [51;16] ),"control_writer_epoch":1,"feeds":[{"owner_id":( [42;16] ),"source":"spot_test","instrument":instrument,"venue_id":1,"stream_id":1,"first_connection_epoch":1,"first_operation":1,"initial_book_generation":1,"snapshot_limit":100,"stream_address":listen.to_string(),"snapshot_address":listen.to_string(),"trust_anchors":[root.join("tls/ca.der")],"topics":[101]}],"topics":[{"resource":root.join("md/topic.bin"),"instrument_set_version":1,"maximum_levels":100,"maximum_cache_bytes":1048576,"maximum_snapshot_bytes":2097152,"maximum_delta_bytes":2097152}],"maximum_idle_ns":1_000_000});
    json_file(&root.join("md/config.json"), &md_config)?;
    let app = deployment(
        root,
        bins,
        boot,
        now,
        expires,
        uid,
        gid,
        listen,
        supervisor,
        exec_source,
        policy,
    );
    json_file(&root.join("exec/config.json"), &app)?;
    dependencies(root, bins, &app, &baseline)?;
    let scenario = Scenario {
        schema: 1,
        fixture_only: true,
        root: root.into(),
        bin_directory: bins.into(),
        listen,
        valid_until_ns: expires,
        boot,
        certificate: root.join("tls/fixture.der"),
        private_key: root.join("tls/fixture.pk8"),
        api_key: root.join("keys/api-key"),
        api_secret: root.join("keys/api-secret"),
        md_config: root.join("md/config.json"),
        app_config: root.join("exec/config.json"),
        worker_bootstrap: root.join("strategy/bootstrap.json"),
    };
    json_file(
        &root.join("scenario.json"),
        &serde_json::to_value(&scenario)?,
    )?;
    Ok(scenario)
}
fn identity(
    id: u8,
    role: u16,
    strategy: u64,
    generation: u64,
    boot: [u8; 16],
    build: [u8; 32],
) -> IdentityConfig {
    IdentityConfig {
        instance: ([id; 16]),
        generation,
        role,
        logical_strategy: strategy,
        host_boot: boot,
        namespace: ([8; 16]),
        build_hash: build,
        qualification_hash: digest(
            b"tickwire SDK qualification source contract for local laboratory",
        ),
        manifest_hash: digest(b"local single Spot strategy scenario"),
        arch: if cfg!(target_arch = "x86_64") { 1 } else { 2 },
        trust_profile: 1,
    }
}
fn bank(
    kind: u16,
    producer: &IdentityConfig,
    consumer: &IdentityConfig,
    now: u64,
) -> ResourceHeader {
    let size = if kind == 2 { 1024 } else { 512 };
    ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
        header_bytes: 4096,
        resource_kind: kind,
        target_arch: producer.arch,
        endian: 1,
        atomic_width: 8,
        page_shift: 12,
        isolation_shift: 7,
        record_bytes: size,
        capacity: 1024,
        resource_bytes: 8192 + 1024 * u64::from(size),
        resource_id: [70 + kind as u8; 16],
        channel_id: [74 + kind as u8; 16],
        producer_instance: producer.instance,
        consumer_instance: consumer.instance,
        producer_generation: producer.generation,
        host_boot_id: producer.host_boot,
        namespace_id: producer.namespace,
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 0,
        first_seq: 1,
        initial_membership_version: 1,
        producer_page_offset: 4096,
        slot_offset: 8192,
        max_consumers: 1,
        schema_major: 1,
        clock_domain: 1,
        trust_profile: 1,
        risk_scope: 1,
        required_features: 0x1c | if kind == 2 { 128 } else { 256 },
        qos: 1,
        created_mono_ns: now,
        ..Default::default()
    }
}
fn quota(now: u64, end: u64, id: u8, md: bool) -> Value {
    let mut rules = vec![];
    let mut add = |action: Value, weight: u32, messages: u32, recovery: bool| {
        rules.push(json!({"source":"spot_test","action":action,"class":if recovery{"recovery"}else{"normal"},"api_weight":weight,"websocket_messages":messages,"costs":[{"window":"lab-budget","units":1}]}))
    };
    for lane in ["stream", "snapshot"] {
        for kind in [
            "connect",
            "upgrade",
            "subscribe",
            "pong",
            "close",
            "snapshot",
        ] {
            add(
                json!({"family":"public","lane":lane,"kind":kind}),
                if kind == "snapshot" { 5 } else { 0 },
                u32::from(["subscribe", "pong", "close"].contains(&kind)),
                false,
            );
        }
    }
    if !md {
        for kind in ["connect", "query"] {
            add(
                json!({"family":"time","kind":kind}),
                u32::from(kind == "query"),
                0,
                true,
            );
        }
        for kind in [
            "stream_connect",
            "upgrade",
            "pong",
            "close",
            "spot_subscribe_signature",
            "spot_status",
            "spot_subscriptions",
            "spot_unsubscribe",
        ] {
            let api = kind.starts_with("spot_");
            add(
                json!({"family":"private","kind":kind}),
                if api || kind == "stream_connect" {
                    2
                } else {
                    0
                },
                u32::from(api || ["pong", "close"].contains(&kind)),
                true,
            );
        }
        for kind in [
            "connect",
            "trade_page",
            "account_snapshot",
            "open_orders",
            "conditional_orders",
            "order_query",
        ] {
            add(json!({"family":"read","kind":kind}), 0, 0, true);
        }
        for kind in ["new", "cancel", "query"] {
            add(
                json!({"family":"command","transport":"rest","action":{"kind":kind}}),
                0,
                0,
                kind != "new",
            );
        }
    }
    json!({"supervisor_generation":1,"consumer_instance":( [id;16] ),"consumer_generation":1,"consumer_lease_until_ns":end,"producer_id":( [id+1;16] ),"policy_generation":1,"policy_starts_ns":now,"policy_ends_ns":end,"windows":[{"name":"lab-budget","namespace":( [90;16] ),"scope":"ip","subject":( [id;16] ),"pool":1,"window":1,"revision":1,"starts_ns":now,"ends_ns":end,"limit":100000,"recovery_reserve":50000}],"grants":[{"request_id":1,"class":"normal","expires_ns":end,"costs":[{"window":"lab-budget","units":50000}]},{"request_id":2,"class":"recovery","expires_ns":end,"costs":[{"window":"lab-budget","units":50000}]}],"rules":rules})
}
fn dependencies(
    root: &Path,
    bins: &Path,
    app: &Value,
    baseline: &tickwire_account_codec::CanonicalBaseline,
) -> Result<()> {
    let mut deps = vec![];
    for kind in 2..=11u8 {
        let bytes = match kind {
            2 => baseline.bytes().to_vec(),
            3 => serde_json::to_vec(
                &json!({"initial_strategy_sequence":1,"account_baseline":baseline.digest()}),
            )?,
            4 => serde_json::to_vec(&app["policy"]["strategy_grants"])?,
            5 => serde_json::to_vec(&app["policy"]["owner_lease"])?,
            6 => serde_json::to_vec(&app["policy"])?,
            7 => serde_json::to_vec(
                &json!({"scenario":"spot-new-fill","revision":1,"strategy":( [2;16] ),"generation":5}),
            )?,
            8 => fs::read(bins.join("tickwire-strategy-worker"))?,
            9 => serde_json::to_vec(app)?,
            10 => include_bytes!("../../../specs/contracts/ipc-wire-v1.json").to_vec(),
            11 => fs::read(root.join("strategy/command.bin"))?,
            _ => unreachable!(),
        };
        let path = root.join(format!("strategy/dependencies/{kind}.bin"));
        write(&path, &bytes)?;
        deps.push(tickwire_strategy_sdk::recovery::DependencyFile {
            kind,
            schema_id: if kind == 11 {
                tickwire_strategy_sdk::recovery::CHECKPOINT_SCHEMA
            } else {
                1
            },
            owner: ([2; 16]),
            path,
            digest: digest(&bytes),
        });
    }
    let template = tickwire_strategy_sdk::recovery::Template {
        dependencies: deps,
        declared_rebuild: false,
        fault_model_id: digest(b"local durable files and process restart"),
        pin_id: ([91; 16]),
        owner: ([2; 16]),
        input_stream: ([92; 16]),
        output_directory: root.join("strategy/recovery"),
        maximum_input_bytes: 32 * 1024 * 1024,
        order_ids: vec![([7; 16])],
    };
    json_file(
        &root.join("strategy/recovery-template.json"),
        &serde_json::to_value(template)?,
    )
}
fn free_address() -> Result<std::net::SocketAddr> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?)
}
fn directory(path: &Path) -> Result<()> {
    fs::DirBuilder::new().mode(0o700).create(path)?;
    Ok(())
}
pub(crate) fn write(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    File::open(path.parent().ok_or("parent")?)?.sync_all()?;
    Ok(())
}
pub(crate) fn json_file(path: &Path, v: &Value) -> Result<()> {
    write(path, &serde_json::to_vec_pretty(v)?)
}
type Certificates = (Vec<u8>, Vec<u8>, Vec<u8>);
fn certificates() -> Result<Certificates> {
    use rcgen::*;
    let mut params = CertificateParams::new(vec![])?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let key = rcgen::KeyPair::generate()?;
    let ca = params.self_signed(&key)?;
    let issuer = Issuer::new(params, key);
    let leaf = |names: Vec<String>| -> Result<(Vec<u8>, Vec<u8>)> {
        let mut p = CertificateParams::new(names)?;
        p.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let k = rcgen::KeyPair::generate()?;
        let c = p.signed_by(&k, &issuer)?;
        Ok((c.der().to_vec(), k.serialize_der()))
    };
    let (fixture, fkey) = leaf(vec![
        "testnet.binance.vision".into(),
        "ws-api.testnet.binance.vision".into(),
        "stream.testnet.binance.vision".into(),
    ])?;
    Ok((ca.der().to_vec(), fixture, fkey))
}

fn policy(boot: [u8; 16], expires: u64) -> Value {
    json!({"scope":{"risk_domain_id":( [1;16] ),"venue":1,"environment":"test","account":2},"product":"spot","executor_id":( [4;16] ),"owner_generation":1,"writer_epoch":4,"domain":{"process_instance":boot,"generation":1},"versions":{"config_revision":6,"risk_policy_version":10,"order_config_version":11,"catalog_version":14,"catalog_hash":( [5;32] )},"max_strategies":8,"max_feeds":8,"marks_per_feed":1024,"control_revision":1,"mode":"trading","owner_lease":{"lease_epoch":1,"valid_until_ns":expires,"isolation_record":digest(b"lab initial exclusive owner, held child, no predecessor")},"strategy_grants":[{"logical_strategy_id":8,"instance_id":( [2;16] ),"strategy_generation":5,"authorization_epoch":9,"config_revision":6,"valid_until_ns":expires,"actions":{"submit":true,"cancel":true,"query":true,"amend":false},"required_feeds":[{"key":{"topic_id":101,"instrument_id":12},"instrument_version":13,"max_age_ns":10_000_000_000u64,"max_sequence_lag":1024}]}],"management_grant":null})
}

#[allow(clippy::too_many_arguments)]
fn deployment(
    root: &Path,
    bins: &Path,
    boot: [u8; 16],
    now: u64,
    expires: u64,
    uid: u32,
    gid: u32,
    listen: std::net::SocketAddr,
    supervisor: IdentityConfig,
    exec_source: tickwire_market_source::ConnectConfig,
    policy: Value,
) -> Value {
    json!({"baseline":root.join("baseline.bin"),"directories":{"account":root.join("exec/account"),"input":root.join("exec/input"),"recovery":root.join("exec/recovery")},"recover":false,"product":"spot","boot_id":boot,"clock_generation":1,"owner_id":( [4;16] ),"writer_epoch":4,"initial_writer_epoch":4,"account_stream":( [52;16] ),"input_stream":( [53;16] ),"first_connection_epoch":1,"instrument_id":12,"book_source":1,"credential":{"api_key_file":root.join("keys/api-key"),"signing_key_file":root.join("keys/api-secret"),"algorithm":"hmac","generation":1,"spot_logon":false},"network":{"rest":listen.to_string(),"time":listen.to_string(),"public_stream":listen.to_string(),"snapshot":listen.to_string(),"private_stream":listen.to_string(),"private_management":null,"trust_anchors":[root.join("tls/ca.der")]},"clock":{"max_round_trip_ns":500_000_000,"max_width_ns":1_000_000_000u64,"max_age_ns":30_000_000_000u64,"quantization_error_ns":1_000_000,"drift_ppb":100000,"max_adjustment_ns":1_000_000_000u64,"calibration_interval_ns":5_000_000_000u64,"calibration_retry_ns":100_000_000},"tuning":{"connect_timeout_ns":10_000_000_000u64,"request_timeout_ns":10_000_000_000u64,"delivery_budget_ns":1_000_000,"recv_window_micros":5_000_000,"maximum_book_age_ns":10_000_000_000u64,"maximum_idle_wait_ns":1_000_000,"snapshot_limit":100,"maximum_intents":64,"maximum_input_records":4096,"maximum_input_bytes":64*1024*1024,"wal_segment_bytes":8*1024*1024},"policy":policy,"quota":quota(now,expires,43,false),"strategy":{"mode":"hosted","config":{"bootstrap":root.join("strategy/bootstrap.json"),"market_resource":root.join("md/topic.bin"),"md_admission_directory":root.join("md/admissions"),"executable":bins.join("tickwire-strategy-worker"),"working_directory":root,"uid":uid,"gid":gid,"supervisor":supervisor,"directory_parent":root.join("host/runtime"),"control_directory":root.join("host/control"),"control_stream":( [54;16] ),"control_domain":( [55;16] ),"control_writer_epoch":1,"snapshot_id":( [56;16] ),"market_cursor_id":( [31;16] ),"subscription_generation":1,"instrument_set_version":1,"maximum_levels_per_side":100,"maximum_cache_bytes":1048576,"maximum_snapshot_bytes":2097152,"maximum_delta_bytes":2097152,"recovery_directory":root.join("strategy/recovery")}},"market_sources":[exec_source],"reconciliation":{"enabled":true,"reader_id":( [44;16] ),"first_request_epoch":1,"first_execution_connection_epoch":100,"retry_ns":100_000_000,"valid_for_ns":30_000_000_000u64},"control":{"inbox_directory":root.join("exec/control-inbox"),"journal_directory":root.join("exec/control-wal"),"stream_epoch":( [57;16] ),"recover":false,"segment_max_bytes":8*1024*1024,"maximum_commands":1024,"maximum_per_poll":16,"startup_consumer_lease_until_ns":expires},"stop_file":root.join("STOP"),"maximum_runtime_ns":60_000_000_000u64})
}
