use crate::*;
use ring::{
    rand::{SecureRandom, SystemRandom},
    signature::{Ed25519KeyPair, KeyPair},
};
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
const REPLICA_PENDING_BYTES: usize = 8 * 1024 * 1024;
pub fn generate(root: &Path, bins: &Path) -> Result<Scenario> {
    generate_with(root, bins, ScenarioKind::SpotFill)
}
fn generate_with(root: &Path, bins: &Path, kind: ScenarioKind) -> Result<Scenario> {
    if !root.is_absolute() || !bins.is_absolute() || root.as_os_str().len() > 65 {
        return Err("absolute root <=65 bytes and binary directory required".into());
    }
    for bin in [
        "tickwire-app",
        "tickwire-md",
        "tickwire-strategy-worker",
        "tickwire-replica-receiver",
        "tickwire-lab",
    ] {
        if !bins.join(bin).is_file() {
            return Err(format!("build binary before generation: {bin}").into());
        }
    }
    for bin in [
        crate::flavor::binary(kind, true),
        crate::flavor::binary(kind, false),
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
        "mirror",
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
    let mirror = free_address()?;
    let (ca, fixture_cert, fixture_key, source_cert, source_key, mirror_cert, mirror_key) =
        certificates()?;
    for (name, bytes) in [
        ("ca.der", ca),
        ("fixture.der", fixture_cert),
        ("fixture.pk8", fixture_key),
        ("source.der", source_cert.clone()),
        ("source.pk8", source_key),
        ("mirror.der", mirror_cert.clone()),
        ("mirror.pk8", mirror_key),
    ] {
        write(&root.join("tls").join(name), &bytes)?;
    }
    write(&root.join("keys/api-key"), b"LAB-ONLY-API-KEY")?;
    let mut secret = [0u8; 32];
    SystemRandom::new()
        .fill(&mut secret)
        .map_err(|_| "random")?;
    write(&root.join("keys/api-secret"), &secret)?;
    let mut public_keys = vec![];
    for name in ["source-signing.pk8", "mirror-signing.pk8"] {
        let doc =
            Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).map_err(|_| "key generation")?;
        let pair = Ed25519KeyPair::from_pkcs8(doc.as_ref()).map_err(|_| "key decode")?;
        public_keys.push(<[u8; 32]>::try_from(pair.public_key().as_ref()).unwrap());
        write(&root.join("keys").join(name), doc.as_ref())?;
    }
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
        digest(&fs::read(bins.join(crate::flavor::binary(kind, true)))?),
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
    let mut bootstrap = tickwire_strategy_sdk::bootstrap::Bootstrap {
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
        handover_enabled: true,
        preflight_job: None,
        factory: None,
        demo_enabled: true,
        demo_command_file: Some(root.join("strategy/command.bin")),
        demo_minimum_bid_ticks: 6_000_000,
    };
    crate::flavor::configure(
        kind,
        &mut bootstrap,
        &root.join("strategy"),
        &command,
        6_000_000,
    )?;
    json_file(
        &root.join("strategy/bootstrap.json"),
        &serde_json::to_value(&bootstrap)?,
    )?;
    let policy = policy(boot, expires);
    let asset = |id, s: &str| json!({"id":id,"version":1,"symbol":s,"exponent":-8});
    let instrument = json!({"id":12,"version":13,"catalog_version":14,"catalog_hash":( [5;32] ),"symbol":"BTCUSDT","base":asset(1,"BTC"),"quote":asset(2,"USDT"),"settlement":asset(2,"USDT"),"price_tick":"0.01","quantity_step":"0.00001","expiry_utc_ns":null});
    let md_config = json!({"identity":md,"clock_generation":1,"socket":root.join("md/market.sock"),"admission_directory":root.join("md/admissions"),"quota_directory":root.join("md/quota"),"quota":quota(now,expires,40,true),"control_directory":root.join("md/control"),"control_stream":( [50;16] ),"control_domain":( [51;16] ),"control_writer_epoch":1,"feeds":[{"owner_id":( [42;16] ),"source":"spot_test","instrument":instrument,"venue_id":1,"stream_id":1,"first_connection_epoch":1,"first_operation":1,"initial_book_generation":1,"snapshot_limit":100,"stream_address":listen.to_string(),"snapshot_address":listen.to_string(),"trust_anchors":[root.join("tls/ca.der")],"topics":[101]}],"topics":[{"resource":root.join("md/topic.bin"),"instrument_set_version":1,"maximum_levels":100,"maximum_cache_bytes":1048576,"maximum_snapshot_bytes":2097152,"maximum_delta_bytes":2097152}],"maximum_idle_ns":1_000_000});
    json_file(&root.join("md/config.json"), &md_config)?;
    let tls = |name: &str, pin| json!({"certificate_chain_der":[root.join(format!("tls/{name}.der"))],"private_key_der":root.join(format!("tls/{name}.pk8")),"trust_anchors_der":[root.join("tls/ca.der")],"peer_certificate_sha256":pin});
    let mut app = deployment(
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
    let config: tickwire_app::config::Deployment = serde_json::from_value(app.clone())?;
    let assembled = config
        .assemble(baseline.clone())
        .map_err(|e| e.to_string())?;
    let bootstrap_bytes = tickwire_account_journal::bootstrap_bytes(&assembled.account.journal)
        .map_err(|e| format!("{e:?}"))?;
    let mut objects = vec![
        tickwire_replica::ObjectRef::new(baseline.bytes())?,
        tickwire_replica::ObjectRef::new(&bootstrap_bytes)?,
    ];
    objects.sort_by_key(|o| o.hash);
    objects.dedup_by_key(|o| o.hash);
    let stream = tickwire_replica::Stream {
        domain: ([1; 16]),
        journal_epoch: ([52; 16]),
        source: tickwire_fencing::ExecutionIdentity {
            host_id: ([58; 16]),
            boot_id: boot,
            exec_instance_id: ([4; 16]),
            exec_epoch: 4,
            network_identity: digest(listen.to_string().as_bytes()),
            credential_access_identity: digest(&secret),
        },
        source_storage_id: ([59; 16]),
        format: tickwire_replica::Format::Account {
            baseline: baseline.digest(),
        },
        initial_objects: objects,
        profile_version: 1,
        commit_mode: tickwire_replica::CommitMode::LocalDurableAsync {
            max_unreplicated_age_ns: 30_000_000_000,
        },
    };
    stream.validate()?;
    write(&root.join("stream.bin"), &stream.bytes())?;
    let network = json!({"timeout_ms":3000,"requests_per_poll":16,"bytes_per_poll":8*1024*1024});
    app["replication"] = json!({"stream_file":root.join("stream.bin"),"mirror":mirror,"mirror_server_name":"mirror.tickwire.test","tls":tls("source",digest(&mirror_cert)),"signing_key_pkcs8":root.join("keys/source-signing.pk8"),"mirror_signing_public_key":public_keys[1],"profile_version":1,"profile":{"mode":"local_durable_async","maximum_unreplicated_age_ns":30_000_000_000u64},"maximum_records":4096,"queue_capacity":8,"maximum_pending_bytes":REPLICA_PENDING_BYTES,"network":network});
    app["strategy"]["config"]["executable"] = json!(bins.join(crate::flavor::binary(kind, true)));
    json_file(&root.join("exec/config.json"), &app)?;
    let receiver = json!({"listen":mirror,"stream_file":root.join("stream.bin"),"journal_dir":root.join("mirror/journal"),"object_dir":root.join("mirror/objects"),"receiver_id":( [60;16] ),"mirror_assignment_epoch":1,"profile_version":1,"max_records":4096,"physical":{"domain":( [1;16] ),"stream_epoch":( [61;16] ),"segment_max_bytes":8*1024*1024},"tls":tls("mirror",digest(&source_cert)),"signing_key_pkcs8":root.join("keys/mirror-signing.pk8"),"source_signing_public_key":public_keys[0],"network":network,"create":true,"required_prefix":null});
    json_file(&root.join("mirror/config.json"), &receiver)?;
    dependencies(root, bins, &app, &baseline, kind)?;
    let mut scenario = Scenario {
        schema: 1,
        kind,
        fixture_only: true,
        root: root.into(),
        bin_directory: bins.into(),
        listen,
        mirror,
        valid_until_ns: expires,
        boot,
        certificate: root.join("tls/fixture.der"),
        private_key: root.join("tls/fixture.pk8"),
        api_key: root.join("keys/api-key"),
        api_secret: root.join("keys/api-secret"),
        md_config: root.join("md/config.json"),
        app_config: root.join("exec/config.json"),
        mirror_config: root.join("mirror/config.json"),
        worker_bootstrap: root.join("strategy/bootstrap.json"),
    };
    if kind.handover() {
        crate::handover::configure(&mut scenario)?;
    }
    json_file(
        &root.join("scenario.json"),
        &serde_json::to_value(&scenario)?,
    )?;
    Ok(scenario)
}
pub fn generate_handover(root: &Path, bins: &Path) -> Result<Scenario> {
    if !bins.join("tickwire-lifecycle").is_file() {
        return Err("build tickwire-lifecycle before generating handover".into());
    }
    generate_with(root, bins, ScenarioKind::SpotHandover)
}
pub fn generate_custom_handover(root: &Path, bins: &Path) -> Result<Scenario> {
    generate_with(root, bins, ScenarioKind::SpotHandoverCustom)
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
    scenario_kind: ScenarioKind,
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
            8 => fs::read(bins.join(crate::flavor::binary(scenario_kind, true)))?,
            9 => serde_json::to_vec(app)?,
            10 => include_bytes!("../../../specs/contracts/ipc-wire-v1.json").to_vec(),
            11 => fs::read(crate::flavor::config_path(
                scenario_kind,
                &root.join("strategy"),
            ))?,
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
        fault_model_id: digest(b"source-and-mirror durable files, process crash, qualified SDK"),
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
type Certificates = (
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
);
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
    let (source, skey) = leaf(vec!["source.tickwire.test".into()])?;
    let (mirror, mkey) = leaf(vec!["mirror.tickwire.test".into()])?;
    Ok((ca.der().to_vec(), fixture, fkey, source, skey, mirror, mkey))
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
    json!({"baseline":root.join("baseline.bin"),"directories":{"account":root.join("exec/account"),"input":root.join("exec/input"),"recovery":root.join("exec/recovery")},"recover":false,"product":"spot","boot_id":boot,"clock_generation":1,"owner_id":( [4;16] ),"writer_epoch":4,"initial_writer_epoch":4,"account_stream":( [52;16] ),"input_stream":( [53;16] ),"first_connection_epoch":1,"instrument_id":12,"book_source":1,"credential":{"api_key_file":root.join("keys/api-key"),"signing_key_file":root.join("keys/api-secret"),"algorithm":"hmac","generation":1,"spot_logon":false},"network":{"rest":listen.to_string(),"time":listen.to_string(),"public_stream":listen.to_string(),"snapshot":listen.to_string(),"private_stream":listen.to_string(),"private_management":null,"trust_anchors":[root.join("tls/ca.der")]},"clock":{"max_round_trip_ns":500_000_000,"max_width_ns":1_000_000_000u64,"max_age_ns":30_000_000_000u64,"quantization_error_ns":1_000_000,"drift_ppb":100000,"max_adjustment_ns":1_000_000_000u64,"calibration_interval_ns":5_000_000_000u64,"calibration_retry_ns":100_000_000},"tuning":{"connect_timeout_ns":10_000_000_000u64,"request_timeout_ns":10_000_000_000u64,"delivery_budget_ns":1_000_000,"recv_window_micros":5_000_000,"maximum_book_age_ns":10_000_000_000u64,"maximum_idle_wait_ns":1_000_000,"snapshot_limit":100,"maximum_intents":64,"maximum_input_records":4096,"maximum_input_bytes":64*1024*1024,"wal_segment_bytes":8*1024*1024},"policy":policy,"quota":quota(now,expires,43,false),"strategy":{"mode":"hosted","config":{"shadow":false,"bootstrap":root.join("strategy/bootstrap.json"),"market_resource":root.join("md/topic.bin"),"md_admission_directory":root.join("md/admissions"),"executable":bins.join("tickwire-strategy-worker"),"working_directory":root,"uid":uid,"gid":gid,"supervisor":supervisor,"directory_parent":root.join("host/runtime"),"control_directory":root.join("host/control"),"control_stream":( [54;16] ),"control_domain":( [55;16] ),"control_writer_epoch":1,"snapshot_id":( [56;16] ),"market_cursor_id":( [31;16] ),"subscription_generation":1,"instrument_set_version":1,"maximum_levels_per_side":100,"maximum_cache_bytes":1048576,"maximum_snapshot_bytes":2097152,"maximum_delta_bytes":2097152,"recovery_directory":root.join("strategy/recovery"),"rotation_directory":null,"first_rotation_sequence":1}},"market_sources":[exec_source],"replication":null,"lifecycle":null,"reconciliation":{"enabled":true,"reader_id":( [44;16] ),"first_request_epoch":1,"first_execution_connection_epoch":100,"retry_ns":100_000_000,"valid_for_ns":30_000_000_000u64},"control":{"inbox_directory":root.join("exec/control-inbox"),"journal_directory":root.join("exec/control-wal"),"stream_epoch":( [57;16] ),"recover":false,"segment_max_bytes":8*1024*1024,"maximum_commands":1024,"maximum_per_poll":16,"startup_consumer_lease_until_ns":expires},"stop_file":root.join("STOP"),"maximum_runtime_ns":60_000_000_000u64})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn handover_generation_publishes_one_final_scenario_and_refuses_reuse() {
        struct Directory(std::path::PathBuf);
        impl Drop for Directory {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        let base = std::path::PathBuf::from(format!(
            "/tmp/tw-lab-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&base).unwrap();
        let base = Directory(base);
        let bins = base.0.join("bin");
        fs::create_dir(&bins).unwrap();
        for name in [
            "tickwire-app",
            "tickwire-md",
            "tickwire-strategy-worker",
            "tickwire-replica-receiver",
            "tickwire-lab",
            "tickwire-lifecycle",
        ] {
            fs::write(bins.join(name), name.as_bytes()).unwrap();
        }
        let run = base.0.join("run");
        let scenario = generate_handover(&run, &bins).unwrap();
        assert_eq!(scenario.kind, ScenarioKind::SpotHandover);
        let original = fs::read(run.join("scenario.json")).unwrap();
        let persisted: Scenario = serde_json::from_slice(&original).unwrap();
        assert_eq!(persisted.kind, ScenarioKind::SpotHandover);
        let app: Value = serde_json::from_slice(&fs::read(&scenario.app_config).unwrap()).unwrap();
        assert_eq!(app["strategy"]["mode"], "multi");
        let deployment: tickwire_app::config::Deployment = serde_json::from_value(app).unwrap();
        let member = crate::handover::member(&deployment, scenario.valid_until_ns);
        assert_ne!(deployment.policy.owner_generation, deployment.writer_epoch);
        assert_eq!(member.exec_epoch, deployment.policy.owner_generation);
        assert_eq!(member.exec_instance, deployment.owner_id);
        assert_eq!(member.risk_domain, deployment.policy.scope.risk_domain_id);
        assert!(run.join("handover/keys.json").is_file());
        let target: tickwire_strategy_sdk::bootstrap::Bootstrap =
            serde_json::from_slice(&fs::read(run.join("next/bootstrap.json")).unwrap()).unwrap();
        assert_eq!(target.preflight_job, Some(run.join("next/preflight.json")));
        assert!(target.handover_enabled);
        assert!(
            !run.join("next/preflight.json").exists(),
            "job requires the real old process cut"
        );
        assert!(
            !run.join("handover/preflight-verified.json").exists(),
            "generation cannot manufacture callback completion"
        );
        assert!(generate_handover(&run, &bins).is_err());
        assert_eq!(fs::read(run.join("scenario.json")).unwrap(), original);
    }
    #[test]
    fn custom_handover_binds_independent_factory_binaries_and_canonical_parameters() {
        struct Temp(std::path::PathBuf);
        impl Drop for Temp {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        let t = Temp(std::path::PathBuf::from(format!(
            "/tmp/tw-custom-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )));
        fs::create_dir(&t.0).unwrap();
        let bins = t.0.join("bin");
        fs::create_dir(&bins).unwrap();
        for n in [
            "tickwire-app",
            "tickwire-md",
            "tickwire-strategy-worker",
            "tickwire-replica-receiver",
            "tickwire-lab",
            "tickwire-lifecycle",
            "tickwire-counting-v1-worker",
            "tickwire-counting-worker",
        ] {
            fs::write(bins.join(n), n).unwrap();
        }
        let s = generate_custom_handover(&t.0.join("run"), &bins).unwrap();
        assert_eq!(s.kind, ScenarioKind::SpotHandoverCustom);
        let old: tickwire_strategy_sdk::bootstrap::Bootstrap =
            serde_json::from_slice(&fs::read(&s.worker_bootstrap).unwrap()).unwrap();
        let new: tickwire_strategy_sdk::bootstrap::Bootstrap =
            serde_json::from_slice(&fs::read(s.root.join("next/bootstrap.json")).unwrap()).unwrap();
        for (b, dir, binary, instance, generation) in [
            (&old, "strategy", "tickwire-counting-v1-worker", [2; 16], 5),
            (&new, "next", "tickwire-counting-worker", [6; 16], 6),
        ] {
            let f = b
                .factory
                .as_ref()
                .expect("custom worker requires actual registered factory");
            assert_eq!(f.id, "counting_example");
            assert!(!b.demo_enabled);
            assert!(b.demo_command_file.is_none());
            let bytes = fs::read(&f.configuration_file).unwrap();
            assert_eq!(f.configuration, tickwire_release::Object::of(&bytes));
            let parameters: Value = serde_json::from_slice(&bytes).unwrap();
            let command: Vec<u8> = serde_json::from_value(parameters["command"].clone()).unwrap();
            let c = tickwire_venue::CanonicalCommand::decode(&command).unwrap();
            assert_eq!(c.command().context.strategy.instance_id, instance);
            assert_eq!(c.command().context.request.strategy_generation, generation);
            assert_eq!(c.command().context.request.config_revision, f.revision);
            assert_eq!(
                b.market.local.build_hash,
                digest(&fs::read(bins.join(binary)).unwrap())
            );
            let template: tickwire_strategy_sdk::recovery::Template = serde_json::from_slice(
                &fs::read(s.root.join(format!("{dir}/recovery-template.json"))).unwrap(),
            )
            .unwrap();
            assert_eq!(
                template
                    .dependencies
                    .iter()
                    .find(|d| d.kind == 11)
                    .unwrap()
                    .digest,
                digest(&bytes)
            );
        }
        assert_ne!(
            old.factory.unwrap().configuration,
            new.factory.unwrap().configuration
        );
    }
    #[test]
    fn generated_replica_pending_capacity_is_accepted_by_real_source() {
        use tickwire_replica::{CommitMode, Format, ObjectRef, SourceConfig, SourceWorker, Stream};
        let (baseline, _) = crate::model::model(1, 1_000_000).unwrap();
        let mut store =
            tickwire_recovery::Persistence::new(tickwire_recovery::MockBlobStore::default());
        store.put_object(baseline.bytes()).unwrap();
        let mode = CommitMode::LocalDurableAsync {
            max_unreplicated_age_ns: 30_000_000_000,
        };
        let stream = Stream {
            domain: [1; 16],
            journal_epoch: [52; 16],
            source: tickwire_fencing::ExecutionIdentity {
                host_id: [58; 16],
                boot_id: [1; 16],
                exec_instance_id: [4; 16],
                exec_epoch: 4,
                network_identity: [2; 32],
                credential_access_identity: [3; 32],
            },
            source_storage_id: [59; 16],
            format: Format::Account {
                baseline: baseline.digest(),
            },
            profile_version: 1,
            commit_mode: mode.clone(),
            initial_objects: vec![ObjectRef::new(baseline.bytes()).unwrap()],
        };
        SourceWorker::new(
            SourceConfig {
                stream,
                profile_version: 1,
                mode,
                max_records: 4096,
                max_pending_batches: 8,
                max_pending_bytes: REPLICA_PENDING_BYTES,
            },
            store,
        )
        .unwrap();
    }
    #[test]
    fn all_rules_deserialize_to_actual_quota_actions() {
        for md in [true, false] {
            let q: tickwire_binance_quota::bootstrap::Config =
                serde_json::from_value(quota(1, 1_000_000, 43, md)).unwrap();
            q.policy().unwrap();
        }
    }
    #[test]
    fn policy_and_baseline_are_real_and_start_without_reconciliation() {
        let p: tickwire_runtime_policy::bootstrap::Config =
            serde_json::from_value(policy([3; 16], 1_000_000)).unwrap();
        p.build().unwrap();
        let (b, c) = crate::model::model(1, 1_000_000).unwrap();
        assert_eq!(b.snapshot().scope.account, 2);
        assert_eq!(c.command().context.request.account, 2);
    }
    #[test]
    fn generated_deployment_schema_and_account_configuration_are_valid() {
        let md = identity(3, 2, 0, 1, [1; 16], [2; 32]);
        let exec = identity(4, 4, 0, 1, [1; 16], [3; 32]);
        let source = tickwire_market_source::ConnectConfig {
            endpoint: Endpoint {
                path: "/tmp/lab/m.sock".into(),
                local: exec.clone(),
                peer: md,
                peer_pid: 1,
                peer_uid: 1,
                peer_gid: 1,
                required_features: 0x5e,
                supported_features: 511,
                control_epoch: 1,
                lease_duration_ns: 1_000_000,
                max_frame_bytes: 65536,
                nonce: [1; 32],
            },
            bank_header: vec![],
            catalog_hash: [5; 32],
            instrument_set_version: 1,
            instruments: vec![Instrument {
                id: 12,
                version: 13,
                required_states: 1,
            }],
            cursor_id: [33; 16],
            subscription_generation: 1,
            request_id: [34; 16],
            maximum_levels: 100,
            startup_timeout_ms: 30000,
            clock_generation: 1,
        };
        let value = deployment(
            Path::new("/tmp/lab"),
            Path::new("/binaries"),
            [1; 16],
            1,
            1_000_000,
            1,
            1,
            "127.0.0.1:1234".parse().unwrap(),
            identity(9, 1, 0, 1, [1; 16], [9; 32]),
            source,
            policy([1; 16], 1_000_000),
        );
        let d: tickwire_app::config::Deployment = serde_json::from_value(value).unwrap();
        let (b, _) = crate::model::model(1, 1_000_000).unwrap();
        d.assemble(b).unwrap();
        let strategy = identity(2, 3, 8, 5, [1; 16], [4; 32]);
        let intent = bank(2, &strategy, &exec, 1);
        let report = bank(3, &exec, &strategy, 1);
        tickwire_shm::validate_bank(&intent).unwrap();
        tickwire_shm::validate_bank(&report).unwrap();
        assert_eq!(intent.required_features, 0x9c);
        assert_eq!(report.required_features, 0x11c);
        wire_bytes(&intent).unwrap();
        wire_bytes(&report).unwrap();
    }
}
