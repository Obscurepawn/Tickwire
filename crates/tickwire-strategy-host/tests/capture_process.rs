//! Explicit Linux acceptance with a separately built registered worker. MD's
//! actual JOIN server and Publisher run in a parent thread, not another process.
#![cfg(target_os = "linux")]
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::mpsc,
    time::{Duration, Instant},
};
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_clock_sys::SystemClock;
use tickwire_control_sys::PrivateDirectory;
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_release::{
    Object,
    rebuild::{Contract, MarketRequirement, Ordering, window::Identity},
    replay::{Rng, VersionedBytes},
};
use tickwire_strategy_host::capture::{Launch, Owner};
use tickwire_strategy_sdk::capture::Bootstrap;
#[path = "../../tickwire-account/tests/common/mod.rs"]
mod account_fixture;
#[path = "capture_process/market.rs"]
mod market;

enum Action {
    Update,
    Stop,
    Detached(mpsc::Sender<(bool, bool)>),
}
struct Market {
    action: mpsc::Sender<Action>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Drop for Market {
    fn drop(&mut self) {
        let _ = self.action.send(Action::Stop);
        if let Some(t) = self.thread.take() {
            t.join().unwrap();
        }
    }
}
fn market() -> (Market, tickwire_md::server::Admission, PathBuf) {
    let (tx, rx) = mpsc::channel();
    let (info_tx, info_rx) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        let mut clock = SystemClock::new(ClockDomain::new([8; 16], 1).unwrap()).unwrap();
        let mut fixture = market::Fixture::new(clock.sample().unwrap().monotonic.nanos());
        let admission = fixture.admission(1);
        info_tx
            .send((admission, fixture.config.admission_directory.clone()))
            .unwrap();
        loop {
            match rx.try_recv() {
                Ok(Action::Stop) | Err(mpsc::TryRecvError::Disconnected) => break,
                Ok(Action::Update) => fixture.update(clock.sample().unwrap().monotonic.nanos()),
                Ok(Action::Detached(reply)) => {
                    let (owner, state) = fixture.topics[0].split();
                    reply.send((owner.all_detached(), state.live())).unwrap();
                }
                Err(mpsc::TryRecvError::Empty) => {}
            }
            fixture
                .server
                .poll(
                    &mut fixture.topics,
                    clock.sample().unwrap().monotonic.nanos(),
                )
                .unwrap();
            std::thread::sleep(Duration::from_millis(1));
        }
    });
    let (admission, directory) = info_rx.recv().unwrap();
    (
        Market {
            action: tx,
            thread: Some(thread),
        },
        admission,
        directory,
    )
}
fn launch(worker: &Path, life: Duration) -> (Owner, Market, PrivateDirectory) {
    let (market, mut admission, md_directory) = market();
    let directory = PrivateDirectory::create_in(Path::new("/tmp"), "twc").unwrap();
    let mut local = admission.client.endpoint.local.clone();
    local.role = 3;
    local.logical_strategy = 8;
    local.build_hash = tickwire_recovery::digest(&fs::read(worker).unwrap());
    admission.client.endpoint.local = local.clone();
    let mut report = admission.client.endpoint.clone();
    report.path = directory.path().join("report.sock");
    report.peer = local.clone();
    report.peer.instance = [44; 16];
    report.peer.role = 4;
    report.peer.logical_strategy = 0;
    report.nonce = [55; 32];
    let domain = admission.client.domain().unwrap();
    let mut clock = SystemClock::new(domain).unwrap();
    let now = clock.sample().unwrap().monotonic.nanos();
    let bank = ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
        header_bytes: 4096,
        resource_kind: 3,
        target_arch: local.arch,
        endian: 1,
        atomic_width: 8,
        page_shift: 12,
        isolation_shift: 7,
        record_bytes: 512,
        capacity: 64,
        resource_bytes: 8192 + 64 * 512,
        resource_id: [73; 16],
        channel_id: [77; 16],
        producer_instance: report.peer.instance,
        consumer_instance: local.instance,
        producer_generation: report.peer.generation,
        host_boot_id: local.host_boot,
        namespace_id: local.namespace,
        channel_epoch: 1,
        ring_epoch: 1,
        first_seq: 1,
        initial_membership_version: 1,
        producer_page_offset: 4096,
        slot_offset: 8192,
        max_consumers: 1,
        schema_major: 1,
        clock_domain: 1,
        trust_profile: 1,
        risk_scope: 1,
        required_features: 0x1c | 256,
        qos: 1,
        created_mono_ns: now,
        ..Default::default()
    };
    // A real counting Registration cannot configure these bytes as its JSON configuration.
    let config = VersionedBytes {
        revision: 1,
        bytes: b"this is not counting factory JSON".to_vec(),
    };
    let header = ResourceHeader::decode(&admission.client.bank_header).unwrap();
    let contract = Contract {
        schema: 1,
        factory: "counting_example".into(),
        binary: local.build_hash,
        state_schema: 2,
        config: Object::of(&config.bytes),
        models: vec![],
        ordering: Ordering::ObservedRebuildWindow,
        markets: vec![MarketRequirement {
            stream: tickwire_strategy_sdk::replay::application::market_stream(&header).unwrap(),
            instrument: admission.client.instruments[0].id,
            category: 1,
            minimum_observations: 1,
            known_bid_prices: vec![],
            known_ask_prices: vec![],
        }],
        actions: vec![],
        minimum_span_ns: 0,
        maximum_span_ns: 60_000_000_000,
        maximum_age_ns: 60_000_000_000,
        maximum_rows: 64,
        maximum_bytes: 1_000_000,
        rng: Rng::Disabled,
    };
    let bootstrap = Bootstrap {
        schema: 1,
        identity: Identity {
            capture: [90; 16],
            logical_strategy: 8,
            risk_domain: [1; 16],
            instance: local.instance,
            generation: local.generation,
            binary: local.build_hash,
            clock: tickwire_release::replay::ClockDomain {
                boot_id: local.host_boot,
                generation: 1,
            },
        },
        contract,
        config,
        models: vec![],
        market: admission.client,
        market_admission_ready: directory.path().join("ready.json"),
        report,
        report_header: tickwire_strategy_sdk::bootstrap::wire_bytes(&bank).unwrap(),
        report_cursor_id: [89; 16],
        report_subscription_generation: 1,
        report_directory: directory.path().join("reports"),
        output_directory: directory.path().join("output"),
        initial_account_position: 0,
        deadline_mono_ns: now + u64::try_from(life.as_nanos()).unwrap(),
        poll_records: 64,
        idle_sleep_us: 100,
    };
    bootstrap.validate(MonoTime::new(domain, now)).unwrap();
    let meta = fs::metadata("/proc/self").unwrap();
    let args = Launch {
        spec: tickwire_supervisor::LaunchSpec {
            identity: local.identity(),
            executable: worker.to_owned(),
            arguments: vec![],
            working_directory: directory.path().to_owned(),
            environment: Default::default(),
            uid: meta.uid(),
            gid: meta.gid(),
            inherit_output: true,
        },
        bootstrap,
        directory_parent: directory.path().to_owned(),
        supervisor: admission.supervisor.identity(),
        parent_process: tickwire_control::ProcessIdentity {
            pid: std::process::id(),
            uid: meta.uid(),
            gid: meta.gid(),
        },
        md_admission_directory: md_directory,
        snapshot_id: admission.snapshot_id,
    };
    // SAFETY: the exact built registered SDK worker and test-owned Publisher use
    // the qualified immutable header/cursor protocol; Registry retains the child.
    #[allow(unsafe_code)]
    let owner = unsafe { Owner::launch(args) }.unwrap();
    (owner, market, directory)
}
fn wait(mut ready: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(20);
    while !ready() {
        assert!(Instant::now() < end, "capture wait deadline");
        std::thread::sleep(Duration::from_millis(2));
    }
}
fn seed(owner: &mut Owner) {
    let (mut snapshot, catalog, _) = account_fixture::fixture();
    snapshot.valid_until = u64::MAX;
    owner
        .publish_baseline(
            &tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap(),
        )
        .unwrap();
    let path = owner.bootstrap().output_directory.join("ATTEMPT.json");
    wait(|| {
        owner.poll().unwrap();
        path.exists()
    });
}
#[test]
#[ignore = "build tickwire-counting-worker, set TICKWIRE_CAPTURE_WORKER, then run --ignored --nocapture"]
fn registered_capture_process_seals_without_factory_and_closes_failures() {
    let worker = fs::canonicalize(
        std::env::var_os("TICKWIRE_CAPTURE_WORKER").expect("explicit registered worker executable"),
    )
    .unwrap();
    let (mut owner, market, _dir) = launch(&worker, Duration::from_secs(30));
    let pid = owner.process().pid;
    seed(&mut owner);
    let baseline_path = owner.bootstrap().output_directory.join("ATTEMPT.json");
    assert!(baseline_path.exists());
    let (mut snapshot, catalog, _) = account_fixture::fixture();
    snapshot.valid_until = u64::MAX;
    let baseline = tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap();
    let record = tickwire_account_codec::CanonicalRecord::new(
        baseline.digest(),
        1,
        tickwire_account::RecoveryRecord::UnattributedCashFlow {
            id: tickwire_venue::BoundedText::new("capture-fact").unwrap(),
            delta: tickwire_account::AssetAmount {
                key: account_fixture::key(1),
                amount: account_fixture::f(1),
            },
        },
    )
    .unwrap();
    owner.publish_record(1, &record).unwrap();
    owner.seal().unwrap();
    market.action.send(Action::Update).unwrap();
    wait(|| {
        owner.poll().unwrap();
        owner.captured().is_some()
    });
    let captured = owner.captured().unwrap();
    let exported = captured.objects().map(Object::of).collect::<Vec<_>>();
    assert!(exported.contains(&captured.receipt().bootstrap));
    assert!(exported.contains(&captured.receipt().request));
    assert!(exported.contains(&Object::of(&captured.receipt().completion.bytes().unwrap())));
    assert_eq!(captured.receipt().pid, pid);
    assert_eq!(captured.receipt().report_ring_sequence, 2);
    assert_eq!(captured.window().seal().end.accounts[0].sequence, 1);
    assert!(captured.window().seal().end.rows > 0);
    assert!(Path::new(&format!("/proc/{pid}")).exists());
    println!(
        "{}",
        serde_json::json!({"case":"captured","pid":pid,"worker_sha256":owner.bootstrap().identity.binary,"report_sequence":captured.receipt().report_ring_sequence,"market_applied":captured.receipt().market_applied,"seal":Object::of(&captured.window().seal().bytes().unwrap()),"factory_configuration":"invalid JSON; configure/create unreachable","md_scope":"actual JOIN server and memfd publisher in parent thread"})
    );
    if let Some(destination) = std::env::var_os("TICKWIRE_CAPTURE_EVIDENCE") {
        let destination = PathBuf::from(destination);
        fs::create_dir(&destination).unwrap();
        fs::set_permissions(&destination, fs::Permissions::from_mode(0o700)).unwrap();
        for entry in fs::read_dir(&owner.bootstrap().output_directory).unwrap() {
            let entry = entry.unwrap();
            if entry.file_name() == "LOCK" {
                continue;
            }
            assert!(entry.file_type().unwrap().is_file());
            fs::copy(entry.path(), destination.join(entry.file_name())).unwrap();
        }
        fs::write(
            destination.join("bootstrap.json"),
            owner.bootstrap().bytes().unwrap(),
        )
        .unwrap();
        fs::write(
            destination.join("receipt.json"),
            captured.receipt().bytes().unwrap(),
        )
        .unwrap();
        for bytes in captured.objects() {
            let object = Object::of(bytes);
            let name = object
                .digest
                .iter()
                .map(|v| format!("{v:02x}"))
                .collect::<String>();
            fs::write(destination.join(format!("export-{name}")), bytes).unwrap();
        }
        println!(
            "{}",
            serde_json::json!({"evidence_directory":destination,"export_objects":captured.objects().count()})
        );
    }
    for _ in 0..3 {
        owner.poll().unwrap();
        std::thread::sleep(Duration::from_millis(2));
    }
    let exit = owner.stop().unwrap();
    assert_eq!(exit.pid(), pid);
    assert!(!Path::new(&format!("/proc/{pid}")).exists());
    let (tx, rx) = mpsc::channel();
    market.action.send(Action::Detached(tx)).unwrap();
    assert_eq!(
        rx.recv().unwrap(),
        (true, true),
        "normal retirement must detach MD while keeping its bank live"
    );
    println!(
        "{}",
        serde_json::json!({"case":"graceful_stop","pid":pid,"successful_exit":exit.status().success(),"md_all_detached":true,"md_bank_live":true})
    );
    drop(market);
    let (mut owner, market, _dir) = launch(&worker, Duration::from_secs(15));
    let pid = owner.process().pid;
    seed(&mut owner);
    owner.seal().unwrap();
    wait(|| {
        let result = owner.poll();
        assert!(
            owner.captured().is_none(),
            "failure must never expose Captured"
        );
        result.is_err()
    });
    assert!(owner.failed());
    assert!(owner.captured().is_none());
    assert!(!Path::new(&format!("/proc/{pid}")).exists());
    assert!(owner.poll().is_err());
    drop(market);
    let (mut owner, market, _dir) = launch(&worker, Duration::from_secs(30));
    let pid = owner.process().pid;
    seed(&mut owner);
    owner.seal().unwrap();
    owner.poll().unwrap();
    market.action.send(Action::Update).unwrap();
    let marker = owner
        .bootstrap()
        .output_directory
        .join("CAPTURE_WINDOW.json");
    wait(|| {
        fs::read_dir(&owner.bootstrap().output_directory)
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| {
                entry.file_name().to_string_lossy().starts_with("object-")
                    && fs::read(entry.path()).is_ok_and(|raw| {
                        tickwire_strategy_sdk::capture::Receipt::decode(&raw).is_ok()
                    })
            })
    });
    fs::write(&marker, b"{}").unwrap();
    wait(|| {
        let result = owner.poll();
        assert!(
            owner.captured().is_none(),
            "failure must never expose Captured"
        );
        result.is_err()
    });
    assert!(owner.failed());
    assert!(owner.captured().is_none());
    assert!(!Path::new(&format!("/proc/{pid}")).exists());
    drop(market);
    let (owner, market, _dir) = launch(&worker, Duration::from_secs(30));
    let pid = owner.process().pid;
    drop(owner);
    assert!(!Path::new(&format!("/proc/{pid}")).exists());
    drop(market);
    println!(
        "{}",
        serde_json::json!({"case":"failure_and_drop","timeout_closed":true,"tampered_completion_closed":true,"children_reaped":true})
    );
}

#[test]
#[ignore = "requires TICKWIRE_CAPTURE_WORKER; independently verifies durable input roots"]
fn capture_rejects_missing_durable_bootstrap_or_request() {
    let worker = fs::canonicalize(
        std::env::var_os("TICKWIRE_CAPTURE_WORKER").expect("explicit registered worker executable"),
    )
    .unwrap();
    for bootstrap in [true, false] {
        let (mut owner, market, _directory) = launch(&worker, Duration::from_secs(30));
        seed(&mut owner);
        owner.seal().unwrap();
        owner.poll().unwrap();
        market.action.send(Action::Update).unwrap();
        let mut receipt = None;
        wait(|| {
            receipt = fs::read_dir(&owner.bootstrap().output_directory)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| entry.file_name().to_string_lossy().starts_with("object-"))
                .find_map(|entry| {
                    fs::read(entry.path())
                        .ok()
                        .and_then(|raw| tickwire_strategy_sdk::capture::Receipt::decode(&raw).ok())
                });
            receipt.is_some()
        });
        let receipt = receipt.unwrap();
        let root = if bootstrap {
            receipt.bootstrap
        } else {
            receipt.request
        };
        let name = root
            .digest
            .iter()
            .map(|v| format!("{v:02x}"))
            .collect::<String>();
        fs::remove_file(
            owner
                .bootstrap()
                .output_directory
                .join(format!("object-{name}")),
        )
        .unwrap();
        wait(|| {
            let result = owner.poll();
            assert!(
                owner.captured().is_none(),
                "missing durable root must not yield Captured"
            );
            result.is_err()
        });
        assert!(owner.failed());
        assert!(!Path::new(&format!("/proc/{}", owner.process().pid)).exists());
        drop(market);
    }
}
