use super::{account_fixture, channels, runtime};
use serde_json::{Value, json};
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::PathBuf,
};
use tickwire_app::{
    hosted::{self, HostConfig},
    strategy::AppStrategy,
};
use tickwire_config::{
    Candidate, Format, Limits,
    projection::{PinnedPolicy, project},
    projection_object::ProjectionObject,
};
use tickwire_market_source::protocol::wire_bytes;
use tickwire_recovery::digest;
use tickwire_runtime::files;
use tickwire_strategy_sdk::bootstrap::Bootstrap;

pub struct Fixture {
    pub root: PathBuf,
    pub host: HostConfig,
    pub bootstrap: Bootstrap,
    pub runtime: files::Configuration,
    pub policy: tickwire_runtime_policy::ControlPlane,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}
impl Fixture {
    pub fn new() -> Self {
        Self::with_shadow(false)
    }
    pub fn with_shadow(shadow: bool) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        // Keep enough room for Host's private directory and Unix socket names.
        let root = PathBuf::from(format!(
            "/tmp/tp-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let current = fs::metadata("/proc/self").unwrap();
        let executable = fs::canonicalize("/bin/true").unwrap();
        let build = digest(&fs::read(&executable).unwrap());
        let (bootstrap, header) = channels::bootstrap(&root, build, current.uid(), current.gid());
        let object = projection(build, shadow);
        write(&root.join("projection.json"), object.bytes());
        write(
            &root.join("bootstrap.json"),
            &serde_json::to_vec(&bootstrap).unwrap(),
        );
        write(&root.join("market.bin"), &wire_bytes(&header).unwrap());
        write(&root.join("parameters.json"), b"{}");
        let worker = &object.payload()["process"];
        let group = |name| json!({"cgroup":root.join(name),"cpus":worker["cpus"],"numa_node":worker["numa_node"],"memory_max":worker["memory_limit_bytes"]});
        let b = object.binding();
        let host: HostConfig = serde_json::from_value(json!({
            "projection_launch":{
                "object_file":root.join("projection.json"),
                "object":{"revision":b.revision,"global_hash":b.global_hash,"policy_revision":b.policy_revision,"process_id":b.process_id,"projection_hash":b.projection_hash,"object_hash":b.object_hash},
                "scope":{"cluster_id":"tickwire-qualification","namespace_id":channels::NAMESPACE,"host_id":worker["host_id"],"process_id":b.process_id},
                "os_identities":[{"name":worker["os_identity"],"uid":current.uid(),"gid":current.gid()}]
            },
            "resources":{"worker":group("cg-worker"),"source_replay":group("cg-source"),"rollback":group("cg-rollback")},
            "shadow":shadow,"bootstrap":root.join("bootstrap.json"),"market_resource":root.join("market.bin"),"md_admission_directory":root.join("admissions"),
            "executable":executable,"working_directory":root,"uid":current.uid(),"gid":current.gid(),"supervisor":channels::identity(1,1,[7;32]),
            "directory_parent":root.join("runtime"),"control_directory":root.join("control"),"control_stream":([54;16]),"control_domain":([55;16]),"control_writer_epoch":1,
            "snapshot_id":([56;16]),"market_cursor_id":([31;16]),"subscription_generation":1,"instrument_set_version":1,"maximum_levels_per_side":100,
            "maximum_cache_bytes":1048576,"maximum_snapshot_bytes":2097152,"maximum_delta_bytes":2097152,"recovery_directory":root.join("recovery"),
            "rotation_directory":null,"first_rotation_sequence":1
        })).unwrap();
        let deployment = runtime::deployment(12);
        let (snapshot, catalog, _) = account_fixture::fixture();
        let baseline = tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap();
        let runtime = deployment.assemble(baseline).unwrap();
        let policy = deployment.policy.build().unwrap();
        Self {
            root,
            host,
            bootstrap,
            runtime,
            policy,
        }
    }
    pub fn change_host(&mut self, change: impl FnOnce(&mut Value)) {
        let mut value = serde_json::to_value(&self.host).unwrap();
        change(&mut value);
        self.host = serde_json::from_value(value).unwrap();
    }
    pub fn save_bootstrap(&self) {
        write(
            &self.host.bootstrap,
            &serde_json::to_vec(&self.bootstrap).unwrap(),
        );
    }
    pub fn build(&self) -> tickwire_app::Result<Box<dyn AppStrategy>> {
        hosted::build(&self.host, &self.runtime, self.policy.clone())
    }
    pub fn assert_no_launch_files(&self) {
        for name in [
            "runtime",
            "control",
            "commands",
            "reports",
            "recovery",
            "join.json",
            "admissions",
        ] {
            assert!(
                !self.root.join(name).exists(),
                "unexpected launch side effect {name}"
            );
        }
    }
    pub fn remote(&mut self) {
        let header =
            tickwire_ipc::ResourceHeader::decode(&fs::read(&self.host.market_resource).unwrap())
                .unwrap();
        self.bootstrap.external_market = Some(tickwire_market_source::ConnectConfig {
            endpoint: self.bootstrap.market.clone(),
            bank_header: wire_bytes(&header).unwrap(),
            catalog_hash: [5; 32],
            instrument_set_version: 1,
            instruments: vec![tickwire_market_source::protocol::Instrument {
                id: 12,
                version: 13,
                required_states: 1,
            }],
            cursor_id: ([31; 16]),
            subscription_generation: 1,
            request_id: [32; 16],
            maximum_levels: 100,
            startup_timeout_ms: 1000,
            clock_generation: 1,
        });
        self.save_bootstrap();
    }
    pub fn create_launch_parents(&self) {
        for name in [
            "runtime",
            "admissions",
            "cg-worker",
            "cg-source",
            "cg-rollback",
        ] {
            let p = self.root.join(name);
            fs::create_dir(&p).unwrap();
            fs::set_permissions(p, fs::Permissions::from_mode(0o700)).unwrap();
        }
    }
    pub fn assert_failed_launch_artifacts(&self) {
        let directories: Vec<_> = fs::read_dir(self.root.join("runtime"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(
            directories.len(),
            1,
            "must reach one actual Host launch attempt"
        );
        let retained: Bootstrap =
            serde_json::from_slice(&fs::read(directories[0].join("worker.json")).unwrap()).unwrap();
        assert_eq!(
            retained.report.local.identity(),
            self.bootstrap.report.local.identity()
        );
        for name in ["intent.sock", "report.sock"] {
            assert!(
                !directories[0].join(name).exists(),
                "failed owner must release its control listener"
            );
        }
        assert_eq!(
            fs::read_dir(self.root.join("admissions")).unwrap().count(),
            0,
            "pre-execution rejection must precede child MD admission"
        );
    }
}
use tickwire_ipc::Wire;
fn write(path: &std::path::Path, bytes: &[u8]) {
    fs::write(path, bytes).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}
fn projection(build: [u8; 32], shadow: bool) -> ProjectionObject {
    let process_id = if shadow {
        "strategy-01-shadow"
    } else {
        "strategy-01-active"
    };
    let bytes = serde_json::to_vec(&json!({"schema":1,"revision":1,"targets":[{
        "process_id":process_id,"account_ids":["spot-main","usdm-main"],
        "venue_ids":["binance-spot","binance-usdm"],"topic_ids":[1,2],"secret_purposes":[]
    }]}))
    .unwrap();
    let hex = |h: [u8; 32]| h.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let reference = format!(
        "artifact://fixture/config/policy@sha256:{}",
        hex(digest(&bytes))
    );
    let mut root = Candidate::parse(
        Format::Toml,
        include_bytes!("../../../../config/reference.toml"),
        Limits::default(),
    )
    .unwrap()
    .value()
    .clone();
    root["local_config"]["projection_policy_ref"] = reference.clone().into();
    root["local_config"]["projection_policy_revision"] = 1.into();
    let process = root["processes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|p| p["id"] == process_id)
        .unwrap();
    process["artifact_ref"] =
        format!("artifact://fixture/strategy/worker@sha256:{}", hex(build)).into();
    process["restart_policy"] = "never".into();
    let candidate = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&root).unwrap(),
        Limits::default(),
    )
    .unwrap();
    let policy = PinnedPolicy::decode(&reference, &bytes).unwrap();
    ProjectionObject::build(
        41,
        &project(&candidate, process_id, &policy).unwrap(),
        4 * 1024 * 1024,
    )
    .unwrap()
}
