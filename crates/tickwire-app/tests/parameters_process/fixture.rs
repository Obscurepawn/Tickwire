use super::{account_fixture, channels, inputs, market::Market, runtime};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tickwire_app::{
    hosted::{self, HostConfig},
    strategy::AppStrategy,
};
use tickwire_ipc::Wire;
use tickwire_release::{
    Object,
    replay::{Checkpoint, Head},
};
use tickwire_strategy_sdk::{bootstrap::Bootstrap, registered::FactoryBinding};

#[derive(Serialize)]
struct Parameters {
    schema: u16,
    enabled: bool,
    minimum_observations: u64,
    minimum_bid_ticks: i64,
    command: Vec<u8>,
}
pub struct Fixture {
    pub root: PathBuf,
    pub market: Market,
    pub bootstrap: Bootstrap,
    pub host: Value,
    pub params: Vec<u8>,
    pub baseline: tickwire_account_codec::CanonicalBaseline,
    pub account: tickwire_account::Account,
    runtime: tickwire_runtime::files::Configuration,
    policy: tickwire_runtime_policy::ControlPlane,
}
impl Fixture {
    pub fn new(worker: &Path, minimum_observations: u64, minimum_bid_ticks: i64) -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = PathBuf::from(format!(
            "/tmp/tpp-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        private(&root);
        private(&root.join("runtime"));
        let market = Market::start();
        let meta = fs::metadata("/proc/self").unwrap();
        let build = tickwire_recovery::digest(&fs::read(worker).unwrap());
        let (mut b, _) = channels::bootstrap(&root, build, meta.uid(), meta.gid());
        let mut external = market.admission.client.clone();
        external.endpoint.local.role = 3;
        external.endpoint.local.logical_strategy = 1;
        external.endpoint.local.build_hash = build;
        let local = external.endpoint.local.clone();
        b.market = external.endpoint.clone();
        b.external_market = Some(external);
        b.market_admission_ready = Some(root.join("md-ready.json"));
        for endpoint in [&mut b.intent, &mut b.report] {
            endpoint.local = local.clone();
            endpoint.peer.host_boot = local.host_boot;
            endpoint.peer.namespace = local.namespace;
            endpoint.lease_duration_ns = 60_000_000_000;
        }
        for bytes in [&mut b.intent_header, &mut b.report_header] {
            let mut h = tickwire_ipc::ResourceHeader::decode(bytes).unwrap();
            h.host_boot_id = local.host_boot;
            h.namespace_id = local.namespace;
            if h.resource_kind == 2 {
                h.producer_instance = local.instance;
                h.producer_generation = local.generation;
            } else {
                h.consumer_instance = local.instance;
            }
            h.descriptor_hash = [0; 32];
            *bytes = tickwire_market_source::protocol::wire_bytes(&h).unwrap();
        }
        b.maximum_levels = 8;
        b.startup_timeout_ms = 10000;
        b.handover_enabled = true;
        b.maximum_market_age_ns = 60_000_000_000;
        b.recovery_template = Some(root.join("recovery-template.json"));
        let now = market.now().nanos();
        let command = inputs::command(&b, &market.instrument, now);
        let params = serde_json::to_vec(&Parameters {
            schema: 1,
            enabled: true,
            minimum_observations,
            minimum_bid_ticks,
            command: command.bytes().to_vec(),
        })
        .unwrap();
        b.factory = Some(FactoryBinding {
            id: "counting_example".into(),
            configuration_file: root.join("parameters.json"),
            configuration: Object::of(&params),
            revision: 6,
        });
        write(&root.join("parameters.json"), &params);
        write(
            &root.join("bootstrap.json"),
            &serde_json::to_vec(&b).unwrap(),
        );
        let (mut snapshot, catalog, _) = account_fixture::fixture();
        snapshot.valid_until = u64::MAX;
        let account =
            tickwire_account::Account::from_snapshot(snapshot.clone(), catalog.clone()).unwrap();
        let baseline = tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap();
        let mut deployment = runtime::deployment(12);
        deployment.boot_id = local.host_boot;
        deployment.policy.domain.process_instance = local.host_boot;
        let settings = deployment.assemble(baseline.clone()).unwrap();
        let policy = deployment.policy.build().unwrap();
        inputs::recovery(
            &root,
            worker,
            &b,
            &baseline,
            &params,
            &deployment.policy.versions,
        );
        let host = json!({"shadow":false,"bootstrap":root.join("bootstrap.json"),"md_admission_directory":market.directory,"executable":worker,"working_directory":root,"uid":meta.uid(),"gid":meta.gid(),"supervisor":market.admission.supervisor,"directory_parent":root.join("runtime"),"control_directory":root.join("control"),"control_stream":([54;16]),"control_domain":([55;16]),"control_writer_epoch":1,"snapshot_id":market.admission.snapshot_id,"market_cursor_id":([31;16]),"subscription_generation":1,"instrument_set_version":1,"maximum_levels_per_side":8,"maximum_cache_bytes":1048576,"maximum_snapshot_bytes":2097152,"maximum_delta_bytes":2097152,"recovery_directory":root.join("recovery"),"rotation_directory":null,"first_rotation_sequence":1});
        Self {
            root,
            market,
            bootstrap: b,
            host,
            params,
            baseline,
            account,
            runtime: settings,
            policy,
        }
    }
    pub fn bind_projection(&mut self, mismatch: bool) {
        let mut params: Value = serde_json::from_slice(&self.params).unwrap();
        if mismatch {
            params["minimum_observations"] = 999.into();
        }
        let schema = inputs::schema();
        let object = inputs::projection(self.bootstrap.market.local.build_hash, &params, &schema);
        write(&self.root.join("projection.json"), object.bytes());
        write(&self.root.join("parameter-schema.json"), &schema);
        self.host["projection_parameters"] = json!({"object_file":self.root.join("projection.json"),"object":tickwire_app::hosted::profile::Binding::from(object.binding()),"scope":{"cluster_id":"tickwire-qualification","namespace_id":self.bootstrap.market.local.namespace,"host_id":object.payload()["process"]["host_id"],"process_id":object.binding().process_id},"schema_file":self.root.join("parameter-schema.json"),"timeout_ns":10_000_000_000u64});
    }
    pub fn build(&self) -> tickwire_app::Result<Box<dyn AppStrategy>> {
        let host: HostConfig = serde_json::from_value(self.host.clone())?;
        hosted::build(&host, &self.runtime, self.policy.clone())
    }
    pub fn seed(&self, owner: &mut dyn AppStrategy) {
        assert!(owner.on_account_baseline(&self.baseline, &self.account));
    }
    pub fn now(&self) -> tickwire_binance_clock::LocalTime {
        tickwire_binance_clock::LocalTime {
            instant: Instant::now(),
            monotonic: self.market.now(),
        }
    }
    pub fn tick(&self, owner: &mut dyn AppStrategy) -> Option<tickwire_runtime::node::Submission> {
        owner.next(
            &self.account,
            tickwire_runtime::node::Health::default(),
            self.now(),
        )
    }
    pub fn target(&self) -> tickwire_app::observation::Target {
        tickwire_app::observation::Target {
            logical_strategy_id: self.bootstrap.market.local.logical_strategy,
            instance: self.bootstrap.market.local.instance,
            generation: self.bootstrap.market.local.generation,
        }
    }
    pub fn next_ready(
        &self,
        owner: &mut dyn AppStrategy,
    ) -> Option<tickwire_runtime::node::Submission> {
        owner.next(
            &self.account,
            tickwire_runtime::node::Health {
                calibrated: true,
                market_live: true,
                private_receiving: true,
                account_frozen: false,
                sender_halted: false,
            },
            self.now(),
        )
    }
    pub fn checkpoint(&self) -> Option<Checkpoint> {
        let directory = self.root.join("commands/merged-inputs");
        let raw = fs::read(directory.join("HEAD.json")).ok()?;
        let head = Head::decode(&raw).unwrap();
        let bytes =
            fs::read(directory.join(format!("object-{}", inputs::hex(head.checkpoint.digest))))
                .unwrap();
        assert_eq!(Object::of(&bytes), head.checkpoint);
        let checkpoint = Checkpoint::decode(&bytes).unwrap();
        assert_eq!(checkpoint.cut, head.cut);
        assert_eq!(checkpoint.source, head.source);
        Some(checkpoint)
    }
    pub fn wait_checkpoint(
        &self,
        owner: &mut dyn AppStrategy,
        ready: impl Fn(&Checkpoint) -> bool,
    ) -> Checkpoint {
        let until = Instant::now() + Duration::from_secs(15);
        loop {
            assert!(self.tick(owner).is_none());
            assert!(!owner.failed(), "actual App owner failed");
            if let Some(c) = self.checkpoint()
                && ready(&c)
            {
                return c;
            }
            assert!(Instant::now() < until, "actual worker checkpoint deadline");
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    pub fn wait_durable(&self, ready: impl Fn(&Checkpoint) -> bool) -> Checkpoint {
        let until = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(c) = self.checkpoint()
                && ready(&c)
            {
                return c;
            }
            assert!(Instant::now() < until, "worker durable state deadline");
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    pub fn wait_consumed(&self, owner: &mut dyn AppStrategy, published: u64) -> Checkpoint {
        self.wait_checkpoint(owner, |c| c.runtime.markets[0].sequence >= published)
    }
    pub fn prove(&self, owner: &mut dyn AppStrategy) {
        let until = Instant::now() + Duration::from_secs(15);
        while owner.projection_parameters(self.target()).is_none() {
            assert!(self.tick(owner).is_none());
            assert!(!owner.failed(), "parameter query failed");
            assert!(Instant::now() < until, "parameter proof deadline");
            std::thread::sleep(Duration::from_millis(2));
        }
        let mut wrong = self.target();
        wrong.generation += 1;
        assert!(owner.projection_parameters(wrong).is_none());
        let proof = owner.projection_parameters(self.target()).unwrap();
        assert_eq!(proof.private_revision(), 6);
        assert_eq!(proof.object().revision, 41);
        assert_eq!(proof.private_object(), Object::of(&self.params));
        assert_eq!(
            proof.schema_hash(),
            tickwire_recovery::digest(&inputs::schema())
        );
        assert_eq!(
            proof.parameter_hash(),
            tickwire_config::Candidate::parse(
                tickwire_config::Format::Json,
                &self.params,
                tickwire_config::Limits::default()
            )
            .unwrap()
            .hash()
        );
        let observed = proof.observation();
        let actual = observed.configuration().unwrap();
        assert_eq!(actual.revision(), 6);
        assert_eq!(actual.bytes(), self.params);
        assert_eq!(actual.object(), Object::of(&self.params));
        assert_eq!(
            actual.source().binary_digest,
            self.bootstrap.market.local.build_hash
        );
        assert!(observed.observed_at().nanos() <= observed.deadline().nanos());
        assert_ne!(observed.challenge(), [0; 16]);
        assert_eq!(
            observed.process().process().pid,
            observed.before_process().process().pid
        );
        println!(
            "{}",
            json!({"case":"parameter_consumed","pid":observed.process().process().pid,"global_revision":proof.object().revision,"private_revision":proof.private_revision(),"private_object":proof.private_object(),"parameter_hash":proof.parameter_hash(),"schema_hash":proof.schema_hash(),"challenge":observed.challenge(),"current_cut":actual.cut(),"checkpoint":actual.checkpoint(),"worker_binary":actual.source().binary_digest,"root":self.root})
        );
    }
    pub fn assert_no_worker(&self) {
        assert_eq!(fs::read_dir(self.root.join("runtime")).unwrap().count(), 0);
        for name in ["commands", "reports", "recovery"] {
            assert!(
                !self.root.join(name).exists(),
                "unexpected worker output {name}"
            );
        }
        assert_eq!(fs::read_dir(&self.market.directory).unwrap().count(), 0);
    }
    pub fn assert_command(&self, bid: i64) -> tickwire_venue::CanonicalCommand {
        let until = Instant::now() + Duration::from_secs(5);
        let (descriptor, raw) = loop {
            let files = fs::read_dir(self.root.join("recovery"))
                .unwrap()
                .filter_map(Result::ok)
                .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
                .collect::<Vec<_>>();
            assert!(
                files.len() <= 1,
                "only one actual pre-publication descriptor"
            );
            if let Some(file) = files.first() {
                let descriptor: tickwire_strategy_sdk::recovery::Descriptor =
                    serde_json::from_slice(&fs::read(file.path()).unwrap()).unwrap();
                let path = self
                    .root
                    .join("commands")
                    .join(format!("object-{}", inputs::hex(descriptor.command_digest)));
                if let Ok(raw) = fs::read(path) {
                    break (descriptor, raw);
                }
            }
            assert!(
                Instant::now() < until,
                "actual command publication deadline"
            );
            std::thread::sleep(Duration::from_millis(2));
        };
        let command = tickwire_venue::CanonicalCommand::decode(&raw).unwrap();
        assert_eq!(command.digest(), descriptor.command_digest);
        assert_eq!(
            command.command().context.strategy.instance_id,
            self.bootstrap.market.local.instance
        );
        assert_eq!(command.command().context.request.config_revision, 6);
        assert!(
            bid >= serde_json::from_slice::<Value>(&self.params).unwrap()["minimum_bid_ticks"]
                .as_i64()
                .unwrap()
        );
        command
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if std::env::var_os("TICKWIRE_PARAMETERS_KEEP").is_none() {
            let _ = fs::remove_dir_all(&self.root);
        }
    }
}
pub fn write(path: &Path, bytes: &[u8]) {
    fs::write(path, bytes).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}
fn private(path: &Path) {
    fs::create_dir(path).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
}
