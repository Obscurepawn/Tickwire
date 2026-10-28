use super::*;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    sync::atomic::{AtomicU64, Ordering},
};
use tickwire_config::{
    Candidate, Format, Limits,
    projection::{PinnedPolicy, project},
    projection_object::ProjectionObject,
};
use tickwire_strategy_sdk::bootstrap::IdentityConfig;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: PathBuf,
    config: super::super::HostConfig,
    bootstrap: Bootstrap,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}
fn hex(value: &[u8]) -> String {
    value.iter().map(|v| format!("{v:02x}")).collect()
}
fn fixture() -> Fixture {
    let root = std::env::temp_dir().join(format!(
        "tw-profile-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&root).unwrap();
    // The same kernel identity used by Host's existing same-UID SHM contract.
    let status = fs::read_to_string("/proc/self/status").unwrap();
    let field = |name: &str| {
        status
            .lines()
            .find(|l| l.starts_with(name))
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse::<u32>()
            .unwrap()
    };
    let (uid, gid) = (field("Uid:"), field("Gid:"));
    let policy_bytes = serde_json::to_vec(&json!({"schema":1,"revision":1,"targets":[{
        "process_id":"strategy-01-active","account_ids":["spot-main","usdm-main"],
        "venue_ids":["binance-spot","binance-usdm"],"topic_ids":[1,2],"secret_purposes":[]
    }]}))
    .unwrap();
    let reference = format!(
        "artifact://fixture/config/policy@sha256:{}",
        hex(&Sha256::digest(&policy_bytes))
    );
    let policy = PinnedPolicy::decode(&reference, &policy_bytes).unwrap();
    let mut value = Candidate::parse(
        Format::Toml,
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../config/reference.toml"
        )),
        Limits::default(),
    )
    .unwrap()
    .value()
    .clone();
    value["local_config"]["projection_policy_ref"] = reference.into();
    let process = value["processes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|p| p["id"] == "strategy-01-active")
        .unwrap();
    process["restart_policy"] = "never".into();
    let candidate = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&value).unwrap(),
        Limits::default(),
    )
    .unwrap();
    let object = ProjectionObject::build(
        41,
        &project(&candidate, "strategy-01-active", &policy).unwrap(),
        MAX_OBJECT_BYTES,
    )
    .unwrap();
    let payload = object.payload();
    let process = &payload["process"];
    let mut namespace = [0; 16];
    namespace[6] = 0x40;
    namespace[8] = 0x80;
    namespace[15] = 1;
    let identity = IdentityConfig {
        instance: ([1; 16]),
        generation: 1,
        role: 3,
        logical_strategy: 1,
        host_boot: ([2; 16]),
        namespace,
        build_hash: tickwire_config::artifacts::reference_hash(
            process["artifact_ref"].as_str().unwrap(),
        )
        .unwrap(),
        qualification_hash: ([3; 32]),
        manifest_hash: ([4; 32]),
        arch: 1,
        trust_profile: 1,
    };
    let endpoint = json!({"path":root.join("channel.sock"),"local":identity,"peer":identity,
        "peer_pid":std::process::id(),"peer_uid":uid,"peer_gid":gid,"required_features":0,
        "supported_features":0,"control_epoch":1,"lease_duration_ns":1_000_000_000u64,
        "max_frame_bytes":16384,"nonce": ( [1;32] )});
    let bootstrap: Bootstrap = serde_json::from_value(json!({"version":1,"market":endpoint,"intent":endpoint,"report":endpoint,
        "report_header":[],"report_cursor_id": ( [1;16] ),"report_subscription_generation":1,
        "report_directory":root.join("reports"),"join_file":root.join("join"),"intent_header":[],
        "intent_cursor_id": ( [2;16] ),"intent_subscription_generation":1,"risk_domain": ( [3;16] ),
        "command_directory":root.join("commands"),"clock_generation":1,"startup_timeout_ms":1000,
        "maximum_market_age_ns":1_000_000u64,"maximum_levels":10,"poll_records":10,"idle_sleep_us":1,
        "demo_enabled":false,"demo_command_file":null,"demo_minimum_bid_ticks":0,
        "factory":{"id":"counting","configuration_file":root.join("factory.json"),"configuration":{"digest": ( [4;32] ),"bytes":2},"revision":1}
    })).unwrap();
    let group = |name: &str| json!({"cgroup":root.join(name),"cpus":process["cpus"],"numa_node":process["numa_node"],"memory_max":process["memory_limit_bytes"]});
    let object_file = root.join("projection.json");
    fs::write(&object_file, object.bytes()).unwrap();
    let projection = Config {
        object_file,
        object: object.binding().into(),
        scope: Scope {
            cluster_id: payload["identity"]["cluster_id"].as_str().unwrap().into(),
            namespace_id: namespace,
            host_id: payload["host"]["id"].as_str().unwrap().into(),
            process_id: "strategy-01-active".into(),
        },
        os_identities: vec![OsIdentity {
            name: process["os_identity"].as_str().unwrap().into(),
            uid,
            gid,
        }],
    };
    let config = serde_json::from_value(json!({"projection_launch":projection,"resources":{"worker":group("worker"),"source_replay":group("source"),"rollback":group("rollback")},
        "bootstrap":root.join("bootstrap"),"executable":root.join("worker-elf"),"working_directory":root,"uid":uid,"gid":gid,"supervisor":identity,
        "directory_parent":root,"control_directory":root.join("control"),"control_stream": ( [5;16] ),"control_domain": ( [6;16] ),"control_writer_epoch":1,
        "snapshot_id": ( [7;16] ),"market_cursor_id": ( [8;16] ),"subscription_generation":1,"instrument_set_version":1,
        "maximum_levels_per_side":10,"maximum_cache_bytes":65536,"maximum_snapshot_bytes":65536,"maximum_delta_bytes":65536,"recovery_directory":root.join("recovery")})).unwrap();
    Fixture {
        root,
        config,
        bootstrap,
    }
}
#[test]
fn full_projection_prepares_registered_startup_without_claiming_application() {
    let f = fixture();
    assert!(prepare(&f.config, &f.bootstrap).unwrap().is_some());
}
#[test]
fn legacy_configuration_does_not_open_an_object() {
    let mut f = fixture();
    f.config.projection_launch = None;
    assert!(prepare(&f.config, &f.bootstrap).unwrap().is_none());
}

impl Fixture {
    fn projection(&mut self) -> &mut Config {
        self.config.projection_launch.as_mut().unwrap()
    }
    // Rebind both hashes after changing the payload: semantic negatives must not
    // pass merely because they contain a stale object digest.
    fn payload(&mut self, change: impl FnOnce(&mut Value)) {
        let path = self.projection().object_file.clone();
        let mut value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        change(&mut value["payload"]);
        let payload = Candidate::parse(
            Format::Json,
            &serde_json::to_vec(&value["payload"]).unwrap(),
            Limits::default(),
        )
        .unwrap();
        value["projection_hash"] = format!("sha256:{}", hex(&payload.hash())).into();
        let object = Candidate::parse(
            Format::Json,
            &serde_json::to_vec(&value).unwrap(),
            Limits::default(),
        )
        .unwrap();
        self.projection().object.projection_hash = payload.hash();
        self.projection().object.object_hash = object.hash();
        fs::write(path, object.canonical_bytes()).unwrap();
    }
    fn rejected(&self) {
        let error = match prepare(&self.config, &self.bootstrap) {
            Err(error) => error,
            Ok(_) => panic!("unexpected successful launch preparation"),
        };
        assert!(error.to_string().contains("projection launch"), "{error}");
    }
}
#[test]
fn exact_scope_and_artifact_bindings_are_required() {
    for field in [
        "cluster",
        "namespace",
        "host",
        "process",
        "artifact",
        "logical",
    ] {
        let mut f = fixture();
        match field {
            "cluster" => f.projection().scope.cluster_id.push('x'),
            "namespace" => f.projection().scope.namespace_id[0] ^= 1,
            "host" => f.projection().scope.host_id.push('x'),
            "process" => f.projection().scope.process_id.push('x'),
            "artifact" => f.payload(|p| {
                p["process"]["artifact_ref"] =
                    format!("artifact://fixture/build/other@sha256:{}", hex(&[5; 32])).into()
            }),
            "logical" => f.payload(|p| p["strategy"]["logical_strategy_id"] = 2.into()),
            _ => unreachable!(),
        }
        f.rejected();
    }
}
#[test]
fn every_local_bootstrap_channel_must_bind_the_same_identity() {
    for channel in 0..3 {
        let mut f = fixture();
        match channel {
            0 => f.bootstrap.market.local.generation += 1,
            1 => f.bootstrap.intent.local.generation += 1,
            _ => f.bootstrap.report.local.generation += 1,
        }
        f.rejected();
    }
    for change in 0..3 {
        let mut f = fixture();
        for local in [
            &mut f.bootstrap.market.local,
            &mut f.bootstrap.intent.local,
            &mut f.bootstrap.report.local,
        ] {
            match change {
                0 => local.role = 1,
                1 => local.trust_profile = 2,
                _ => local.namespace[0] ^= 1,
            }
        }
        f.rejected();
    }
}
#[test]
fn supported_profile_is_exact_and_shadow_is_explicit() {
    for field in ["restart", "standby", "trust", "role", "shadow"] {
        let mut f = fixture();
        f.payload(|p| match field {
            "restart" => p["process"]["restart_policy"] = "recover-gated".into(),
            "standby" => p["process"]["activation"] = "standby".into(),
            "trust" => p["strategy"]["trust_profile"] = "isolated_adapter".into(),
            "role" => p["process"]["role"] = "supervisor".into(),
            "shadow" => {
                p["process"]["activation"] = "shadow".into();
                p["strategy"]["role"] = "Shadow".into();
            }
            _ => unreachable!(),
        });
        f.rejected();
    }
    let mut f = fixture();
    f.config.shadow = true;
    f.payload(|p| {
        p["process"]["activation"] = "shadow".into();
        p["strategy"]["role"] = "Shadow".into();
    });
    assert!(prepare(&f.config, &f.bootstrap).unwrap().is_some());
}
#[test]
fn capture_restore_rotation_and_demo_are_not_ordinary_registered_startup() {
    for mode in 0..7 {
        let mut f = fixture();
        match mode {
            0 => f.bootstrap.factory = None,
            1 => f.bootstrap.preflight_job = Some(f.root.join("job")),
            2 => {
                f.config.capture = Some(super::super::CaptureConfig {
                    bootstrap: f.root.join("capture"),
                    publication_directory: f.root.join("publication"),
                })
            }
            3 => f.config.rotation_directory = Some(f.root.join("rotation")),
            4 => f.bootstrap.demo_enabled = true,
            5 => f.bootstrap.demo_command_file = Some(f.root.join("demo")),
            _ => f.bootstrap.demo_minimum_bid_ticks = 1,
        }
        f.rejected();
    }
}
#[test]
fn os_mapping_is_bounded_unique_and_matches_the_actual_app_identity() {
    for change in 0..8 {
        let mut f = fixture();
        match change {
            0 => f.projection().os_identities.clear(),
            1 => {
                let mapping = f.projection().os_identities[0].clone();
                f.projection().os_identities.push(mapping);
            }
            2 => f.projection().os_identities[0].name = "unknown".into(),
            3 => f.projection().os_identities[0].uid ^= 1,
            4 => f.config.gid ^= 1,
            5 => {
                f.config.uid ^= 1;
                let uid = f.config.uid;
                f.projection().os_identities[0].uid = uid;
            }
            6 => f.projection().os_identities.push(OsIdentity {
                name: "Bad_name".into(),
                uid: 0,
                gid: 0,
            }),
            _ => {
                for i in 0..128 {
                    f.projection().os_identities.push(OsIdentity {
                        name: format!("name-{i}"),
                        uid: 0,
                        gid: 0,
                    });
                }
            }
        }
        f.rejected();
    }
}
#[test]
fn full_resource_plan_and_exact_worker_limits_are_required() {
    for change in 0..7 {
        let mut f = fixture();
        match change {
            0 => f.config.resources = None,
            1 => f.config.resources.as_mut().unwrap().worker.cpus = vec![1],
            2 => f.config.resources.as_mut().unwrap().worker.numa_node = 1,
            3 => f.config.resources.as_mut().unwrap().worker.memory_max += 1,
            4 => {
                f.config
                    .resources
                    .as_mut()
                    .unwrap()
                    .source_replay
                    .memory_max = 0
            }
            5 => {
                let worker = f.config.resources.as_ref().unwrap().worker.cgroup.clone();
                f.config.resources.as_mut().unwrap().rollback.cgroup = worker;
            }
            _ => f.payload(|p| p["process"]["cpus"] = json!([32, 32])),
        }
        f.rejected();
    }
    let mut f = fixture();
    f.config.resources.as_mut().unwrap().worker.cpus = vec![32, 33];
    f.payload(|p| p["process"]["cpus"] = json!([33, 32]));
    assert!(prepare(&f.config, &f.bootstrap).unwrap().is_some());
}
#[test]
fn complete_projection_and_all_transport_bindings_are_checked() {
    for change in 0..7 {
        let mut f = fixture();
        match change {
            0 => f.projection().object.revision += 1,
            1 => f.projection().object.global_hash[0] ^= 1,
            2 => f.projection().object.policy_revision += 1,
            3 => f.projection().object.process_id.push('x'),
            4 => f.projection().object.projection_hash[0] ^= 1,
            5 => f.projection().object.object_hash[0] ^= 1,
            _ => f.payload(|p| {
                assert!(
                    p.as_object_mut()
                        .unwrap()
                        .remove("secret_bindings")
                        .is_some()
                );
            }),
        }
        f.rejected();
    }
}
#[test]
fn projection_file_must_be_a_bounded_regular_non_symlink_object() {
    use std::os::unix::fs::symlink;
    for kind in 0..4 {
        let mut f = fixture();
        let path = f.projection().object_file.clone();
        match kind {
            0 => {
                let alias = f.root.join("alias");
                symlink(path, &alias).unwrap();
                f.projection().object_file = alias;
            }
            1 => {
                fs::OpenOptions::new()
                    .write(true)
                    .open(path)
                    .unwrap()
                    .set_len(MAX_OBJECT_BYTES as u64 + 1)
                    .unwrap();
            }
            2 => f.projection().object_file = f.root.clone(),
            _ => f.projection().object_file = PathBuf::from("relative.json"),
        }
        f.rejected();
    }
}
#[test]
fn profile_configuration_rejects_unknown_and_duplicate_fields() {
    let f = fixture();
    let mut value = serde_json::to_value(f.config.projection_launch.as_ref().unwrap()).unwrap();
    value["os_identities"][0]["privileged"] = true.into();
    assert!(serde_json::from_value::<Config>(value).is_err());
    assert!(serde_json::from_str::<Scope>(r#"{"cluster_id":"a","cluster_id":"b","namespace_id":[],"host_id":"c","process_id":"d"}"#).is_err());
}

#[test]
fn external_market_identity_is_bound_as_well_as_legacy_channels() {
    let mut f = fixture();
    f.bootstrap.external_market = Some(tickwire_market_source::ConnectConfig {
        endpoint: f.bootstrap.market.clone(),
        bank_header: vec![],
        catalog_hash: [1; 32],
        instrument_set_version: 1,
        instruments: vec![],
        cursor_id: [1; 16],
        subscription_generation: 1,
        request_id: [1; 16],
        maximum_levels: 10,
        startup_timeout_ms: 1000,
        clock_generation: 1,
    });
    assert!(prepare(&f.config, &f.bootstrap).unwrap().is_some());
    f.bootstrap
        .external_market
        .as_mut()
        .unwrap()
        .endpoint
        .local
        .generation += 1;
    f.rejected();
}
#[test]
fn a_fifo_object_is_rejected_without_waiting_for_a_writer() {
    let mut f = fixture();
    let path = f.root.join("fifo");
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .unwrap()
            .success()
    );
    f.projection().object_file = path;
    f.rejected();
}

#[test]
fn zero_global_hash_is_rejected_even_when_the_transport_hashes_are_consistent() {
    let mut f = fixture();
    let path = f.projection().object_file.clone();
    let mut value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    value["global_config_hash"] = format!("sha256:{}", hex(&[0; 32])).into();
    let object = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&value).unwrap(),
        Limits::default(),
    )
    .unwrap();
    f.projection().object.global_hash = [0; 32];
    f.projection().object.object_hash = object.hash();
    fs::write(path, object.canonical_bytes()).unwrap();
    f.rejected();
}

#[test]
fn zero_or_unbounded_kernel_channel_identity_is_rejected_before_launch() {
    for change in 0..7 {
        let mut f = fixture();
        for local in [
            &mut f.bootstrap.market.local,
            &mut f.bootstrap.intent.local,
            &mut f.bootstrap.report.local,
        ] {
            match change {
                0 => local.instance = [0; 16],
                1 => local.generation = 0,
                2 => local.host_boot = [0; 16],
                3 => local.generation = 1 << 63,
                4 => local.qualification_hash = [0; 32],
                5 => local.manifest_hash = [0; 32],
                _ => local.arch = 0,
            }
        }
        f.rejected();
    }
}

#[test]
fn namespace_cannot_be_zero_even_with_a_consistent_projection() {
    let mut f = fixture();
    f.projection().scope.namespace_id = [0; 16];
    f.payload(|p| p["identity"]["namespace_id"] = "00000000-0000-0000-0000-000000000000".into());
    for local in [
        &mut f.bootstrap.market.local,
        &mut f.bootstrap.intent.local,
        &mut f.bootstrap.report.local,
    ] {
        local.namespace = [0; 16];
    }
    f.rejected();
}
