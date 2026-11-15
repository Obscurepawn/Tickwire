//! Integration tests for stateless strategy supervision (`RemoteHostedStrategy`), cross-thread
//! SPSC SHM lane factories (`prepare_cross_thread_intent_lane`, `prepare_cross_thread_report_lane`),
//! and cold-path double-sampled `/proc` telemetry (`DoubleSampledObservation`, `FreshObservation`).

use std::{
    collections::BTreeMap,
    os::{fd::AsFd, unix::fs::MetadataExt},
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use sha2::{Digest, Sha256};
use tickwire_clock_sys::SystemClock;
use tickwire_control::{ClientHandshake, Identity, ProcessIdentity, Session};
use tickwire_control_sys::{Connection, PrivateDirectory};
use tickwire_ipc::{ReportBody, ResourceHeader};
use tickwire_market_source::{
    ConnectConfig,
    protocol::{Endpoint, Instrument, accept_offer, cursor_header, resource_offer, wire_bytes},
};
use tickwire_md::server::admission_path;
use tickwire_runtime_policy::{AccountAuthSpec, Mode, OrsRuntimePolicy, PolicyConfig};
use tickwire_shm::{BankKey, Publisher, SubscriptionKey};
use tickwire_strategy_host::{
    ObservationError, ObservationRequest, RemoteHostedStrategy, RemoteLaunch,
    prepare_cross_thread_intent_lane, prepare_cross_thread_report_lane,
};
use tickwire_strategy_sdk::{
    Bootstrap, IntentWriter, OutboundIntent, ReportIdentity, ReportReceiver, worker::read_config,
};
use tickwire_supervisor::LaunchSpec;
use tickwire_venue::{OrderType, Product, Side, TimeInForce};

fn target_arch_code() -> u16 {
    if cfg!(target_arch = "x86_64") {
        1
    } else if cfg!(target_arch = "aarch64") {
        2
    } else {
        panic!("unsupported architecture")
    }
}

fn intent_bank_header(
    producer_instance: [u8; 16],
    producer_generation: u64,
    consumer_instance: [u8; 16],
    host_boot_id: [u8; 16],
    namespace_id: [u8; 16],
) -> ResourceHeader {
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
        producer_instance,
        producer_generation,
        consumer_instance,
        host_boot_id,
        namespace_id,
        risk_scope: 900,
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

fn report_bank_header(
    producer_instance: [u8; 16],
    producer_generation: u64,
    consumer_instance: [u8; 16],
    host_boot_id: [u8; 16],
    namespace_id: [u8; 16],
) -> ResourceHeader {
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
        producer_instance,
        producer_generation,
        consumer_instance,
        host_boot_id,
        namespace_id,
        risk_scope: 900,
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

fn md_bank_header(host_boot_id: [u8; 16], namespace_id: [u8; 16]) -> ResourceHeader {
    let capacity = 128u32;
    let record_bytes = 256u32;
    ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
        abi_minor: 0,
        header_bytes: 4096,
        resource_kind: 1,
        endian: 1,
        atomic_width: 8,
        page_shift: 12,
        isolation_shift: 7,
        target_arch: target_arch_code(),
        resource_id: [31; 16],
        channel_id: [32; 16],
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        producer_instance: [50; 16],
        producer_generation: 2,
        consumer_instance: [0; 16],
        host_boot_id,
        namespace_id,
        risk_scope: 0,
        producer_page_offset: 4096,
        slot_offset: 8192,
        capacity,
        record_bytes,
        resource_bytes: 8192 + u64::from(capacity) * u64::from(record_bytes),
        first_seq: 1,
        initial_membership_version: 1,
        max_consumers: 32,
        required_features: 0x1c,
        schema_major: 1,
        clock_domain: 1,
        qos: 1,
        event_class: 1,
        ..Default::default()
    }
}

fn sample_policy() -> OrsRuntimePolicy {
    OrsRuntimePolicy::from_config(&PolicyConfig {
        initial_mode: Mode::Trading,
        strategies: vec![7],
        enabled_accounts: vec![
            AccountAuthSpec::new(2, "acct-2", vec![7]),
            AccountAuthSpec::new(5, "acct-5", vec![7]),
        ],
        default_active: true,
        default_enabled: true,
    })
    .unwrap()
}

#[test]
fn cross_thread_intent_and_report_lanes_round_trip_without_unsafe_send() {
    let strat_instance = [0x11; 16];
    let ors_instance = [0x99; 16];
    let boot = [0x13; 16];
    let ns = [0x14; 16];

    let (intent_starter, intent_reader_factory) = prepare_cross_thread_intent_lane(
        intent_bank_header(strat_instance, 5, ors_instance, boot, ns),
        1,
        [0x88; 16],
        7,
        1,
    )
    .unwrap();
    let (report_starter, report_writer_factory) = prepare_cross_thread_report_lane(
        report_bank_header(ors_instance, 9, strat_instance, boot, ns),
        1,
        [0x89; 16],
        ReportIdentity {
            strat_id: 7,
            instance_id: strat_instance,
            strategy_generation: 5,
        },
    )
    .unwrap();

    let req_thread = thread::spawn(move || {
        let mut reader = intent_reader_factory().unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if let Some((record, payload)) = reader.poll().unwrap() {
                assert!(payload.is_none());
                return (
                    record.body.strat_id,
                    record.body.order_seqnum,
                    record.body.price_ticks,
                    record.body.qty_lots,
                );
            }
            thread::yield_now();
        }
        panic!("timed out waiting for cross-thread IntentRecord");
    });

    let resp_thread = thread::spawn(move || {
        let mut writer = report_writer_factory().unwrap();
        let body = ReportBody {
            report_kind: 2, // Ack
            strat_id: 7,
            account_ref: 2,
            order_seqnum: 1,
            instrument_id: 1001,
            order_status: 2, // New
            price_ticks: 6_000_000,
            leaves_qty_lots: 10,
            ..Default::default()
        };
        writer.publish_report(body, 5, None, 1_000_000).unwrap()
    });

    let mut intent_writer = intent_starter.finish(Duration::from_secs(3)).unwrap();
    let mut report_receiver = report_starter.finish(Duration::from_secs(3)).unwrap();

    let intent = OutboundIntent::new_order(
        7,
        2,
        1001,
        5,
        Product::Spot,
        Side::Buy,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        6_000_000,
        10,
        "tw1cross01",
        999_000,
    )
    .unwrap();
    let receipt = intent_writer.publish(&intent, 1_000_000).unwrap();
    assert_eq!(receipt.order_seqnum, 1);

    let (strat_id, order_seqnum, px, qty) = req_thread.join().unwrap();
    assert_eq!((strat_id, order_seqnum, px, qty), (7, 1, 6_000_000, 10));

    let published_seq = resp_thread.join().unwrap();
    assert_eq!(published_seq, 1);

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut got_ack = false;
    while Instant::now() < deadline {
        if let Some(report) = report_receiver.poll().unwrap() {
            assert_eq!(report.strat_id, 7);
            assert_eq!(report.account_ref, 2);
            assert_eq!(report.order_seqnum, 1);
            got_ack = true;
            break;
        }
        thread::yield_now();
    }
    assert!(got_ack, "expected cross-thread ReportRecord Ack");
}

#[allow(unsafe_code)]
fn run_child_worker_mode(bootstrap_path: &Path) {
    let config: Bootstrap = read_config(bootstrap_path, 512 * 1024).unwrap();
    let domain = config.market.domain().unwrap();
    let mut clock = SystemClock::new(domain).unwrap();
    let deadline = Instant::now() + Duration::from_millis(config.startup_timeout_ms);

    // 1. Connect & authenticate `intent.sock`, create `Publisher<1>`, offer bank fd (`[7; 16]`),
    //    and register ORS cursor fd (`[8; 16]`).
    let intent_ep = config.intent.as_ref().unwrap();
    let mut intent_conn = Connection::connect(&intent_ep.path, deadline).unwrap();
    let hello = ClientHandshake::new(intent_ep.policy(), [6; 16]).unwrap();
    intent_conn.send(hello.hello(), &[], deadline).unwrap();
    let reply = intent_conn.recv(deadline).unwrap();
    let peer = intent_conn.peer_credentials();
    let mut intent_session: Session = hello
        .finish(
            ProcessIdentity {
                pid: peer.pid,
                uid: peer.uid,
                gid: peer.gid,
            },
            reply.bytes(),
            clock.sample().unwrap().monotonic.nanos(),
        )
        .unwrap();

    let intent_hdr = config.intent_resource().unwrap().unwrap();
    let mut publisher = Publisher::<1>::create(intent_hdr).unwrap();
    let intent_sub = SubscriptionKey {
        bank: publisher.key(),
        instance: u128::from_le_bytes(intent_ep.peer.instance),
        generation: config.intent_subscription_generation,
        cursor: u128::from_le_bytes(config.intent_cursor_id),
    };
    publisher.join(intent_sub).unwrap();
    let membership = publisher.membership_version();
    let expires = intent_session.deadline();
    let offered = resource_offer(
        &intent_session,
        publisher.header(),
        [7; 16],
        expires,
        membership,
        false,
    )
    .unwrap();
    let bank_fd = publisher.reader_fd().unwrap();
    intent_conn
        .send(&offered, &[bank_fd.as_fd()], deadline)
        .unwrap();
    let ch = cursor_header(publisher.header(), intent_sub).unwrap();
    let cursor_fd = accept_offer(
        &mut intent_session,
        intent_conn.recv(deadline).unwrap(),
        &ch,
        [8; 16],
        expires,
        membership,
        false,
        clock.sample().unwrap().monotonic.nanos(),
    )
    .unwrap();
    // SAFETY: `cursor_fd` is the verified ORS `Subscriber` cursor descriptor for `intent_sub`.
    unsafe {
        publisher.register_cursor(intent_sub, cursor_fd).unwrap();
    }
    let mut intent_writer = IntentWriter::with_arena(
        publisher,
        Some(intent_sub),
        None,
        config.strat_id,
        config.instance_id,
        config.generation,
        1,
    )
    .unwrap();

    // 2. Connect & authenticate `report.sock`, accept ORS report bank fd (`[14; 16]`), and return
    //    `ReportReceiver` cursor fd (`[15; 16]`).
    let report_ep = config.report.as_ref().unwrap();
    let mut report_conn = Connection::connect(&report_ep.path, deadline).unwrap();
    let rhello = ClientHandshake::new(report_ep.policy(), [13; 16]).unwrap();
    report_conn.send(rhello.hello(), &[], deadline).unwrap();
    let rreply = report_conn.recv(deadline).unwrap();
    let rpeer = report_conn.peer_credentials();
    let mut report_session = rhello
        .finish(
            ProcessIdentity {
                pid: rpeer.pid,
                uid: rpeer.uid,
                gid: rpeer.gid,
            },
            rreply.bytes(),
            clock.sample().unwrap().monotonic.nanos(),
        )
        .unwrap();
    let report_bank = config.report_resource().unwrap().unwrap();
    let report_sub = SubscriptionKey {
        bank: BankKey {
            resource: u128::from_le_bytes(report_bank.resource_id),
            channel: u128::from_le_bytes(report_bank.channel_id),
            channel_epoch: report_bank.channel_epoch,
            ring_epoch: report_bank.ring_epoch,
            feed_generation: report_bank.feed_generation,
        },
        instance: u128::from_le_bytes(config.instance_id),
        generation: config.report_subscription_generation,
        cursor: u128::from_le_bytes(config.report_cursor_id),
    };
    let r_membership = report_bank.initial_membership_version + 1;
    let r_expires = report_session.deadline();
    let r_fd = accept_offer(
        &mut report_session,
        report_conn.recv(deadline).unwrap(),
        &report_bank,
        [14; 16],
        r_expires,
        r_membership,
        false,
        clock.sample().unwrap().monotonic.nanos(),
    )
    .unwrap();
    // SAFETY: `r_fd` is the verified ORS read-only report ring descriptor.
    let mut report_receiver = unsafe {
        ReportReceiver::attach(
            r_fd,
            report_bank.clone(),
            report_sub,
            None,
            ReportIdentity {
                strat_id: config.strat_id,
                instance_id: config.instance_id,
                strategy_generation: config.generation,
            },
        )
        .unwrap()
    };
    let r_cursor = report_receiver.cursor_fd().unwrap();
    let r_ch = cursor_header(&report_bank, report_sub).unwrap();
    let r_offer = resource_offer(
        &report_session,
        &r_ch,
        [15; 16],
        r_expires,
        r_membership,
        false,
    )
    .unwrap();
    report_conn
        .send(&r_offer, &[r_cursor.as_fd()], deadline)
        .unwrap();

    // 3. Publish one inline 1,024B `IntentRecord` and wait for one 512B `ReportRecord` reply,
    //    then remain alive briefly until parent finishes `/proc` observation or terminates us.
    let now_ns = clock.sample().unwrap().monotonic.nanos();
    let out = OutboundIntent::new_order(
        config.strat_id,
        0,
        1001,
        5,
        Product::Spot,
        Side::Sell,
        OrderType::Limit,
        Some(TimeInForce::Gtc),
        7_500_000,
        4,
        "tw1child01",
        now_ns,
    )
    .unwrap();
    intent_writer.publish(&out, now_ns).unwrap();

    let wait_until = Instant::now() + Duration::from_secs(5);
    while Instant::now() < wait_until {
        if let Some(rep) = report_receiver.poll().unwrap()
            && rep.order_seqnum == 1
        {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }

    // Hold open descriptors while parent performs double-sampled `/proc` observation.
    thread::sleep(Duration::from_secs(10));
}

#[test]
fn child_worker_entrypoint() {
    if let Ok(path) = std::env::var("TICKWIRE_HOST_TEST_CHILD_BOOTSTRAP") {
        run_child_worker_mode(Path::new(&path));
        return;
    }
    if let Ok(parent) = std::env::var("TICKWIRE_HOST_TEST_PARENT_DIR") {
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if let Ok(entries) = std::fs::read_dir(&parent) {
                for entry in entries.flatten() {
                    let candidate = entry.path().join("worker.json");
                    if candidate.exists() {
                        run_child_worker_mode(&candidate);
                        return;
                    }
                }
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("timed out locating worker.json under {parent}");
    }
}

#[test]
#[allow(unsafe_code)]
fn remote_hosted_strategy_launches_sealed_elf_registers_md_admission_and_observes_live_child() {
    if std::env::var("TICKWIRE_HOST_TEST_CHILD_BOOTSTRAP").is_ok()
        || std::env::var("TICKWIRE_HOST_TEST_PARENT_DIR").is_ok()
    {
        return;
    }

    let root = PrivateDirectory::create_in(&std::env::temp_dir(), "h").unwrap();
    let md_admissions = PrivateDirectory::create_in(root.path(), "adm").unwrap();

    let boot_hex = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").unwrap();
    let clean_hex: String = boot_hex.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    let mut host_boot = [0u8; 16];
    for i in 0..16 {
        host_boot[i] = u8::from_str_radix(&clean_hex[i * 2..i * 2 + 2], 16).unwrap();
    }
    let namespace = [0x22; 16];

    let current_exe = std::env::current_exe().unwrap();
    let canonical_exe = std::fs::canonicalize(&current_exe).unwrap();
    let exe_hash: [u8; 32] = Sha256::digest(std::fs::read(&canonical_exe).unwrap()).into();
    let proc_meta = std::fs::metadata("/proc/self").unwrap();
    let parent_proc = ProcessIdentity {
        pid: std::process::id(),
        uid: proc_meta.uid(),
        gid: proc_meta.gid(),
    };

    let worker_id = Identity {
        instance: [0x31; 16],
        generation: 5,
        role: 3,
        logical_strategy: 77,
        host_boot,
        namespace,
        build_hash: exe_hash,
        qualification_hash: [0x41; 32],
        manifest_hash: [0x42; 32],
        arch: target_arch_code(),
        trust_profile: 1,
    };
    let md_id = Identity {
        instance: [0x50; 16],
        generation: 2,
        role: 2,
        logical_strategy: 0,
        host_boot,
        namespace,
        build_hash: exe_hash,
        qualification_hash: [0x41; 32],
        manifest_hash: [0x42; 32],
        arch: target_arch_code(),
        trust_profile: 1,
    };
    let ors_id = Identity {
        instance: [0x99; 16],
        generation: 9,
        role: 4,
        logical_strategy: 0,
        host_boot,
        namespace,
        build_hash: exe_hash,
        qualification_hash: [0x41; 32],
        manifest_hash: [0x42; 32],
        arch: target_arch_code(),
        trust_profile: 1,
    };
    let supervisor_id = Identity {
        instance: [0x70; 16],
        generation: 1,
        role: 1,
        logical_strategy: 0,
        host_boot,
        namespace,
        build_hash: exe_hash,
        qualification_hash: [0x41; 32],
        manifest_hash: [0x42; 32],
        arch: target_arch_code(),
        trust_profile: 1,
    };

    let md_ep = Endpoint {
        path: root.path().join("md.sock"),
        local: (&worker_id).into(),
        peer: (&md_id).into(),
        peer_pid: parent_proc.pid,
        peer_uid: parent_proc.uid,
        peer_gid: parent_proc.gid,
        required_features: 0x1c,
        supported_features: 0x3ff,
        control_epoch: 1,
        lease_duration_ns: 30_000_000_000,
        max_frame_bytes: 65536,
        nonce: [0x61; 32],
    };
    let intent_ep = Endpoint {
        path: PathBuf::from("/placeholder/intent.sock"),
        local: (&worker_id).into(),
        peer: (&ors_id).into(),
        peer_pid: parent_proc.pid,
        peer_uid: parent_proc.uid,
        peer_gid: parent_proc.gid,
        required_features: 0x1c,
        supported_features: 0x3ff,
        control_epoch: 10,
        lease_duration_ns: 30_000_000_000,
        max_frame_bytes: 65536,
        nonce: [0x62; 32],
    };
    let report_ep = Endpoint {
        path: PathBuf::from("/placeholder/report.sock"),
        local: (&worker_id).into(),
        peer: (&ors_id).into(),
        peer_pid: parent_proc.pid,
        peer_uid: parent_proc.uid,
        peer_gid: parent_proc.gid,
        required_features: 0x1c,
        supported_features: 0x3ff,
        control_epoch: 11,
        lease_duration_ns: 30_000_000_000,
        max_frame_bytes: 65536,
        nonce: [0x63; 32],
    };

    let request_id = [0xAA; 16];
    let snapshot_id = [0xBB; 16];
    let intent_hdr =
        intent_bank_header(worker_id.instance, 5, ors_id.instance, host_boot, namespace);
    let report_hdr =
        report_bank_header(ors_id.instance, 9, worker_id.instance, host_boot, namespace);

    let bootstrap = Bootstrap {
        version: 1,
        logical_strategy_id: 77,
        strat_id: 7,
        instance_id: worker_id.instance,
        generation: 5,
        account_ref: 0,
        worker_cpu: None,
        housekeeping_cpus: None,
        market: ConnectConfig {
            endpoint: md_ep,
            bank_header: wire_bytes(&md_bank_header(host_boot, namespace)).unwrap(),
            catalog_hash: [0x22; 32],
            instrument_set_version: 1,
            instruments: vec![Instrument {
                id: 1001,
                version: 5,
                required_states: 1,
            }],
            cursor_id: [0x71; 16],
            subscription_generation: 1,
            request_id,
            maximum_levels: 10,
            startup_timeout_ms: 5_000,
            clock_generation: 1,
        },
        dop_socket: None,
        dop: None,
        intent: Some(intent_ep),
        intent_header: Some(wire_bytes(&intent_hdr).unwrap()),
        intent_cursor_id: [0x81; 16],
        intent_subscription_generation: 1,
        report: Some(report_ep),
        report_header: Some(wire_bytes(&report_hdr).unwrap()),
        report_cursor_id: [0x82; 16],
        report_subscription_generation: 1,
        risk_domain: [0x01; 16],
        instrument_catalog: None,
        catalog_file: None,
        catalog_digest: None,
        factory: None,
        clock_generation: 1,
        startup_timeout_ms: 5_000,
        maximum_market_age_ns: 1_000_000_000,
        maximum_levels: 10,
        poll_records: 64,
        idle_sleep_us: 100,
        demo_enabled: false,
        demo_command_file: None,
        demo_minimum_bid_ticks: 0,
    };

    // Child reads `worker.json` from its current working directory via env var!
    let mut env = BTreeMap::new();
    env.insert(
        "TICKWIRE_HOST_TEST_CHILD_BOOTSTRAP".to_string(),
        "worker.json".to_string(),
    );
    // Point child's working directory to a dedicated subdir? Wait: `RemoteHostedStrategy::launch`
    // writes `worker.json` into its private directory and passes `["--bootstrap", "<dir>/worker.json"]`
    // when `arguments` is empty, OR we can use a tiny shell-free trick: run the test filter
    // `child_worker_entrypoint --exact --nocapture` and read `--bootstrap` or locate `worker.json`!
    // Wait: Rust's libtest harness rejects unknown CLI flags like `--bootstrap`!
    // Instead, pass `arguments: vec!["--exact".into(), "child_worker_entrypoint".into()]` and have
    // `on_admission_registered` symlink/copy? No, `read_config` rejects symlinks, and `RemoteLaunch`
    // creates `PrivateDirectory` inside `directory_parent` (`tickwire-strategy-*`), so the child can
    // simply find `directory_parent/tickwire-strategy-*/worker.json`!
    env.insert(
        "TICKWIRE_HOST_TEST_PARENT_DIR".to_string(),
        root.path().to_str().unwrap().to_string(),
    );
    // Update `TICKWIRE_HOST_TEST_CHILD_BOOTSTRAP` handling via a wrapper script? No! Let's check:
    // if `TICKWIRE_HOST_TEST_PARENT_DIR` is set, `child_worker_entrypoint` scans `TICKWIRE_HOST_TEST_PARENT_DIR`
    // for `tickwire-strategy-*/worker.json`! Let's support that directly in `child_worker_entrypoint`!
    env.remove("TICKWIRE_HOST_TEST_CHILD_BOOTSTRAP");

    let spec = LaunchSpec {
        identity: worker_id,
        executable: canonical_exe,
        arguments: vec!["--exact".into(), "child_worker_entrypoint".into()],
        working_directory: root.path().to_path_buf(),
        environment: env,
        uid: parent_proc.uid,
        gid: parent_proc.gid,
        inherit_output: false,
    };

    let admitted_pid = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let admitted_pid_cl = std::sync::Arc::clone(&admitted_pid);

    let launch = RemoteLaunch {
        resources: None,
        spec,
        bootstrap,
        directory_parent: root.path().to_path_buf(),
        supervisor: supervisor_id,
        parent_process: parent_proc,
        md_admission_directory: md_admissions.path().to_path_buf(),
        snapshot_id,
        on_admission_registered: Some(Box::new(move |proc_id| {
            admitted_pid_cl.store(proc_id.pid, std::sync::atomic::Ordering::Release);
            Ok(())
        })),
    };

    // SAFETY: Test owns qualified local identities and single-writer SPSC rings.
    let mut host = unsafe { RemoteHostedStrategy::launch(launch) }.unwrap();
    let child_pid = admitted_pid.load(std::sync::atomic::Ordering::Acquire);
    assert_ne!(child_pid, 0);
    assert_eq!(host.child_process().pid, child_pid);
    assert_eq!(host.market_consumer(), Some((child_pid, [0x31; 16], 5)));

    // Verify `tickwire-md` admission file was written with exact child PID and request_id.
    let adm_file = admission_path(md_admissions.path(), child_pid, request_id).unwrap();
    assert!(
        adm_file.exists(),
        "expected MD admission file at {adm_file:?}"
    );

    // Poll the 1,024B `IntentRecord` emitted by the supervised child process.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut received_intent = None;
    while Instant::now() < deadline {
        if let Some((rec, payload)) = host.poll_intent().unwrap() {
            assert!(payload.is_none());
            received_intent = Some(rec);
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    let rec = received_intent.expect("expected IntentRecord from supervised child");
    assert_eq!(rec.body.strat_id, 7);
    assert_eq!(rec.body.price_ticks, 7_500_000);
    assert_eq!(rec.body.qty_lots, 4);

    // Send a 512B `ReportRecord` reply back to the child process.
    let rep_seq = host
        .publish_report(
            ReportBody {
                report_kind: 2,
                strat_id: 7,
                account_ref: 2,
                order_seqnum: 1,
                instrument_id: 1001,
                order_status: 2,
                price_ticks: 7_500_000,
                leaves_qty_lots: 4,
                ..Default::default()
            },
            5,
            None,
            2_000_000,
        )
        .unwrap();
    assert_eq!(rep_seq, 1);

    // Verify cold-path `FreshObservation` & `WorkerReadiness` against `OrsRuntimePolicy`.
    let policy = sample_policy();
    let fresh = host
        .observe_fresh(&policy, &ObservationRequest::default())
        .unwrap();
    assert_eq!(fresh.sample.before().process().pid, child_pid);
    assert_eq!(fresh.sample.after().process().pid, child_pid);
    assert!(fresh.sample.resources().is_none());
    assert_eq!(fresh.readiness.resolved_account_ref, 2); // `account_ref = 0` resolves to first enabled account (`2`)!
    assert!(fresh.readiness.is_ready_to_trade());
    assert!(fresh.readiness.can_manage_orders());
    assert_eq!(fresh.readiness.intent_last_msg_seqnum, 1);
    assert_eq!(fresh.readiness.report_next_seq, 2);

    // Suspending ORS policy transitions readiness out of `is_ready_to_trade` while keeping `can_manage_orders`.
    policy.suspend();
    let ready_suspended = host.readiness(&policy).unwrap();
    assert!(!ready_suspended.is_ready_to_trade());
    assert!(ready_suspended.can_manage_orders());
    policy.resume();

    // Requiring cgroup resources on an unconstrained launch fails closed with `ResourcesRequired`.
    let res_err = host
        .observe_fresh(
            &policy,
            &ObservationRequest {
                require_resources: true,
                ..ObservationRequest::default()
            },
        )
        .unwrap_err();
    assert!(matches!(
        res_err,
        ObservationError::Supervisor(tickwire_supervisor::Error::ResourcesRequired)
    ));

    // Terminating the owned child reaps it cleanly and marks `strat_id` inactive in `policy`.
    let exit = host.terminate(Some(&policy)).unwrap();
    assert_eq!(exit.pid(), child_pid);
    assert!(!policy.is_active(7));
    assert!(host.market_consumer().is_none());
}
