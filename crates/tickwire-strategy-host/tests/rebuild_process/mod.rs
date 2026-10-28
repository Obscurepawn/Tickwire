//! Same registered child and live JOIN across capture, rebuild and real C+ inputs.
use super::common::*;
use std::{
    fs,
    os::unix::fs::{DirBuilderExt, MetadataExt},
    path::{Path, PathBuf},
    sync::mpsc,
    time::Duration,
};
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_release::{Object, Role};
use tickwire_runtime::node::Strategy;
use tickwire_strategy_sdk::{
    bootstrap::{Bootstrap, wire_bytes},
    capture::Bootstrap as Capture,
};
mod fresh;
mod publication;
const V2: u64 = 0x5457434e0002;
const STREAM: [u8; 16] = [134; 16];
fn private(path: &Path) {
    fs::DirBuilder::new().mode(0o700).create(path).unwrap();
}
fn write(path: &Path, bytes: &[u8]) {
    fs::write(path, bytes).unwrap();
}
fn setup(c: &mut Capture, dir: &Path, worker: &Path) -> RebuildSetup {
    c.identity.generation = 6;
    c.market.endpoint.local.generation = 6;
    c.report.local.generation = 6;
    let meta = fs::metadata("/proc/self").unwrap();
    c.report.peer_pid = std::process::id();
    c.report.peer_uid = meta.uid();
    c.report.peer_gid = meta.gid();
    let mut report = ResourceHeader::decode(&c.report_header).unwrap();
    report.descriptor_hash = [0; 32];
    report.descriptor_hash = tickwire_recovery::digest(&wire_bytes(&report).unwrap());
    c.report_header = wire_bytes(&report).unwrap();
    // Exact field order is the public counting Parameters canonical encoding.
    c.config.bytes=br#"{"schema":1,"enabled":false,"minimum_observations":1,"minimum_bid_ticks":1,"command":null}"#.to_vec();
    c.contract.config = Object::of(&c.config.bytes);
    c.contract.state_schema = V2;
    let parameters = dir.join("parameters.json");
    write(&parameters, &c.config.bytes);
    let publication = dir.join("publication");
    private(&publication);
    let commands = dir.join("commands");
    private(&commands);
    let mut intent = c.report.clone();
    intent.path = dir.join("intent.sock");
    intent.nonce = [56; 32];
    let mut h = report.clone();
    h.resource_kind = 2;
    h.resource_id = [74; 16];
    h.channel_id = [78; 16];
    h.record_bytes = 1024;
    h.resource_bytes = 8192 + 64 * 1024;
    h.required_features = 0x1c | 128;
    h.producer_instance = c.identity.instance;
    h.consumer_instance = intent.peer.instance;
    h.producer_generation = 6;
    h.descriptor_hash = [0; 32];
    let template_path = dir.join("recovery-template.json");
    let b = Bootstrap {
        version: 1,
        market: c.market.endpoint.clone(),
        external_market: Some(c.market.clone()),
        market_admission_ready: Some(c.market_admission_ready.clone()),
        intent,
        report: c.report.clone(),
        report_header: c.report_header.clone(),
        report_cursor_id: c.report_cursor_id,
        report_subscription_generation: c.report_subscription_generation,
        report_directory: c.report_directory.clone(),
        join_file: dir.join("unused-join.json"),
        intent_header: wire_bytes(&h).unwrap(),
        intent_cursor_id: [88; 16],
        intent_subscription_generation: 1,
        risk_domain: c.identity.risk_domain,
        command_directory: commands,
        clock_generation: 1,
        startup_timeout_ms: 30000,
        maximum_market_age_ns: c.contract.maximum_age_ns,
        maximum_levels: c.market.maximum_levels,
        poll_records: 64,
        idle_sleep_us: 100,
        recovery_template: Some(template_path.clone()),
        handover_enabled: true,
        preflight_job: None,
        factory: Some(tickwire_strategy_sdk::registered::FactoryBinding {
            id: "counting_example".into(),
            revision: c.config.revision,
            configuration: c.contract.config,
            configuration_file: parameters,
        }),
        demo_enabled: false,
        demo_command_file: None,
        demo_minimum_bid_ticks: 0,
    };
    write(
        &dir.join("runtime-bootstrap.json"),
        &serde_json::to_vec(&b).unwrap(),
    );
    let (snapshot, catalog, _) = account_fixture::fixture();
    let policy = tickwire_runtime_policy::Config {
        scope: snapshot.scope.clone(),
        product: tickwire_venue::Product::Spot,
        executor_id: [1; 16],
        owner_generation: 1,
        writer_epoch: 4,
        domain: c.market.domain().unwrap(),
        versions: tickwire_runtime_policy::Versions {
            config_revision: 6,
            risk_policy_version: 10,
            order_config_version: 11,
            catalog_version: 14,
            catalog_hash: [5; 32],
        },
        max_strategies: 4,
        max_feeds: 4,
        marks_per_feed: 2,
    };
    // All retained dependency bytes are actual fixture inputs. The installed
    // v4 proof separately binds the complete real application/merged history.
    let baseline = tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap();
    let dependency_bytes = [
        baseline.bytes().to_vec(),
        serde_json::to_vec(&serde_json::json!({"initial_strategy_sequence":1,"account_baseline":baseline.digest()})).unwrap(),
        b"[]".to_vec(), // no strategy execution grants in this installation test
        b"null".to_vec(), // no executor owner lease or order submission
        serde_json::to_vec(&policy.versions).unwrap(),
        serde_json::to_vec(&serde_json::json!({"instance":c.identity.instance,"generation":c.identity.generation,"logical_strategy":8})).unwrap(),
        fs::read(worker).unwrap(),
        serde_json::to_vec(&b).unwrap(),
        include_bytes!("../../../../specs/contracts/ipc-wire-v1.json").to_vec(),
        c.config.bytes.clone(),
    ];
    let dependencies = dependency_bytes
        .iter()
        .enumerate()
        .map(|(index, bytes)| {
            let path = dir.join(format!("dependency-{}.bin", index + 2));
            write(&path, bytes);
            tickwire_strategy_sdk::recovery::DependencyFile {
                kind: index as u8 + 2,
                schema_id: 1,
                owner: c.identity.instance,
                path,
                digest: tickwire_recovery::digest(bytes),
            }
        })
        .collect();
    let template = tickwire_strategy_sdk::recovery::Template {
        dependencies,
        declared_rebuild: true,
        fault_model_id: [121; 32],
        pin_id: [122; 16],
        owner: c.identity.instance,
        input_stream: STREAM,
        output_directory: dir.join("recovery-output"),
        maximum_input_bytes: 16 * 1024 * 1024,
        order_ids: vec![],
    };
    write(&template_path, &serde_json::to_vec(&template).unwrap());
    RebuildSetup {
        bootstrap: b,
        publication,
        policy: tickwire_runtime_policy::ControlPlane::new(policy).unwrap(),
    }
}
#[test]
#[ignore = "requires freshly built release TICKWIRE_CAPTURE_WORKER; same-PID actual installation"]
fn registered_child_rebuilds_and_continues_real_inputs() {
    let worker = fs::canonicalize(
        std::env::var_os("TICKWIRE_CAPTURE_WORKER").expect("explicit release worker"),
    )
    .unwrap();
    let (started, market, dir) = launch_selected(&worker, Duration::from_secs(50), |c, d| {
        Some(setup(c, d, &worker))
    });
    let Launched::Rebuild(mut host) = started else {
        panic!("rebuild launch")
    };
    let pid = host.process().unwrap().pid;
    let b = host.capture_bootstrap().clone();
    let (mut snapshot, catalog, _) = account_fixture::fixture();
    snapshot.valid_until = u64::MAX;
    let account =
        tickwire_account::Account::from_snapshot(snapshot.clone(), catalog.clone()).unwrap();
    let baseline = tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap();
    assert!(host.on_account_baseline(&baseline, &account));
    wait(|| {
        host.poll().unwrap();
        b.output_directory.join("ATTEMPT.json").exists()
    });
    market.action.send(Action::Update).unwrap();
    let seal = host.seal_capture().unwrap().window().seal().clone();
    assert_eq!(host.process().unwrap().pid, pid);
    let (op, member) = publication::write(&host, &worker, dir.path());
    let input = host
        .handover_rebuild_input(&op, &member)
        .expect("qualified actual captured input");
    let installed = host
        .handover_install_rebuild(input)
        .expect("same-PID installation and actual intent connection");
    assert_eq!(host.process().unwrap().pid, pid);
    let c = installed.verified().checkpoint().clone();
    assert_eq!(c.cut.sequence, seal.end.rows);
    assert_eq!(c.source.input_stream, STREAM);
    assert_eq!(installed.verified().installation().pid, pid);
    let input = installed.input();
    let wrong_pid = tickwire_runtime::node::RebuildInput::from_authenticated_child(
        input.request().clone(),
        input.plans().clone(),
        input.contract().clone(),
        input.inputs().clone(),
        input.manifest().clone(),
        input.seal().clone(),
        input.rollback_plans().clone(),
        pid + 1,
        input.objects().to_vec(),
    )
    .unwrap();
    assert!(
        tickwire_runtime::node::InstalledRebuild::from_authenticated_child(
            wrong_pid,
            installed.root(),
            installed.readiness().clone(),
            installed.objects().to_vec()
        )
        .is_err(),
        "same installation cannot attest a different child"
    );
    // Post-window traffic uses the original mapped consumers and live object.
    market.action.send(Action::Update).unwrap();
    let record = tickwire_account_codec::CanonicalRecord::new(
        baseline.digest(),
        1,
        tickwire_account::RecoveryRecord::UnattributedCashFlow {
            id: tickwire_venue::BoundedText::new("post-window-real-fact").unwrap(),
            delta: tickwire_account::AssetAmount {
                key: account_fixture::key(1),
                amount: account_fixture::f(1),
            },
        },
    )
    .unwrap();
    assert!(host.on_account_record(1, &record, &baseline, &account));
    let mut end = None;
    wait(|| {
        host.poll().unwrap();
        if let Some((ready, objects)) =
            host.handover_checkpoint(b.identity.instance, b.identity.generation)
            && ready.exec_report_sequence == 1
        {
            end = Some((ready, objects));
            return true;
        }
        false
    });
    let (ready, objects) = end.unwrap();
    let current_seal = tickwire_recovery::HandoverSeal::decode(
        objects
            .iter()
            .find(|bytes| tickwire_recovery::digest(bytes) == ready.input_seal_digest)
            .expect("authenticated READY seal"),
    )
    .unwrap();
    let state = tickwire_release::replay::retained::decode(
        objects
            .iter()
            .find(|bytes| tickwire_recovery::digest(bytes) == current_seal.input_digest)
            .expect("exact retained root named by READY"),
    )
    .unwrap();
    let counts = |checkpoint: &tickwire_release::replay::Checkpoint| -> serde_json::Value {
        serde_json::from_slice(
            &tickwire_strategy_sdk::replay::application::business_bytes(&checkpoint.business)
                .unwrap(),
        )
        .unwrap()
    };
    let before = counts(&c);
    let after = counts(state.current());
    assert!(before["observations"].as_u64().unwrap() > 0);
    assert!(after["observations"].as_u64().unwrap() > before["observations"].as_u64().unwrap());
    assert_eq!(
        after["reports"].as_u64().unwrap(),
        before["reports"].as_u64().unwrap() + 1
    );
    assert!(state.current().cut.sequence > c.cut.sequence);
    assert!(state.current().runtime.markets[0].sequence > c.runtime.markets[0].sequence);
    let tail = state.tail_from(&c).unwrap();
    let anchor = tickwire_release::replay::verify_anchor(
        &c.bytes().unwrap(),
        &c.source,
        Object::of(&c.runtime.config.bytes),
        &[],
    )
    .unwrap();
    let suffix =
        tickwire_release::replay::verify_tail(&anchor, &tail, &state.current().cut).unwrap();
    assert!(suffix.rows().iter().any(|r|matches!(&r.input,tickwire_release::replay::Input::AccountCallback{position:1,canonical,..} if canonical==record.bytes())));
    println!(
        "{}",
        serde_json::json!({"case":"same_pid_rebuild","pid":pid,"worker":b.identity.binary,"capture_rows":seal.end.rows,"installed_cut":c.cut,"continued_cut":state.current().cut,"account_position":ready.exec_report_sequence,"authenticated_intent_lane":true,"factory":"real counting v2; command disabled","md_scope":"actual JOIN server/Publisher in parent thread"})
    );
    let prepared = host
        .handover_finish_rebuild(installed)
        .expect("actual isolated rollback child, kind10 and new READY");
    assert_eq!(host.process().unwrap().pid, pid);
    let saved = prepared
        .objects()
        .iter()
        .map(|bytes| (tickwire_recovery::digest(bytes), bytes.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let final_seal =
        tickwire_recovery::HandoverSeal::decode(&saved[&prepared.readiness().input_seal_digest])
            .unwrap();
    assert_eq!(
        final_seal.rebuild_rollback_digest,
        Some(prepared.rollback_completion().digest)
    );
    let completion = tickwire_release::rebuild::probe::Completion::decode(
        &saved[&prepared.rollback_completion().digest],
    )
    .unwrap();
    assert_ne!(completion.pid, pid);
    assert!(completion.success);
    assert!(!PathBuf::from(format!("/proc/{}", completion.pid)).exists());
    tickwire_release::rebuild::install::verify_seal(
        &final_seal,
        &saved[&final_seal.checkpoint_digest],
        &saved[&final_seal.input_digest],
        |hash| {
            saved
                .get(&hash)
                .cloned()
                .ok_or(tickwire_release::Error::Missing)
        },
    )
    .unwrap();
    let prior = prepared.installed();
    let same = tickwire_runtime::node::InstalledRebuild::from_authenticated_child(
        prior.input().clone(),
        prior.root(),
        prior.readiness().clone(),
        prior.objects().to_vec(),
    )
    .unwrap();
    assert!(
        tickwire_runtime::node::PreparedRebuild::from_authenticated_ready(
            same,
            prior.root(),
            prepared.readiness().clone(),
            prepared.objects().to_vec()
        )
        .is_err(),
        "an existing unrelated object cannot replace the exact probe completion root"
    );
    if let Some(path) = std::env::var_os("TICKWIRE_REBUILD_EVIDENCE") {
        use tickwire_recovery::{BlobStore, StoreKey};
        let path = PathBuf::from(path);
        private(&path);
        let mut store = tickwire_recovery::FileBlobStore::open(&path).unwrap();
        for (hash, bytes) in &saved {
            store.put_sync(StoreKey::Object(*hash), bytes).unwrap();
        }
        write(&path.join("result.json"),&serde_json::to_vec(&serde_json::json!({"worker_sha256":b.identity.binary,"pid":pid,"probe_pid":completion.pid,"installation":prepared.installed().root(),"rollback":prepared.rollback_completion(),"final_seal":Object::of(&final_seal.bytes().unwrap()),"C":c.cut,"D":state.current().cut,"account_position":1,"intent_connection":true,"new_commands":0})).unwrap());
    }
    println!(
        "{}",
        serde_json::json!({"case":"same_pid_rollback_finish","pid":pid,"probe_pid":completion.pid,"probe_successful_exit":true,"rollback":prepared.rollback_completion(),"final_seal":Object::of(&final_seal.bytes().unwrap())})
    );
    host.stop().unwrap();
    assert!(!PathBuf::from(format!("/proc/{pid}")).exists());
    let (tx, rx) = mpsc::channel();
    market.action.send(Action::Detached(tx)).unwrap();
    assert_eq!(rx.recv().unwrap(), (true, true));
}
