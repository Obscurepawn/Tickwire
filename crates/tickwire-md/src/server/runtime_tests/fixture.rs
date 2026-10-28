use super::*;
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    process::{Child, Command, Stdio},
    sync::atomic::Ordering,
};
use tickwire_control::{Policy, ProcessIdentity};
use tickwire_ipc::ResourceHeader;
use tickwire_market_source::protocol::{Endpoint, Instrument, wire_bytes};

pub struct Fixture {
    _root: tickwire_control_sys::PrivateDirectory,
    pub config: config::Config,
    pub addition: config::Feed,
    pub child: Child,
    pub clock: tickwire_clock_sys::SystemClock,
    pub old: network::Network,
    pub new: network::Network,
    log: PathBuf,
}
impl Fixture {
    pub fn new() -> Self {
        Self::with_recovery(false, false)
    }
    pub fn new_with_recovery() -> Self {
        Self::with_recovery(true, false)
    }
    pub fn new_with_recovery_paused_trade() -> Self {
        Self::with_recovery(true, true)
    }
    fn with_recovery(recovery: bool, paused_trade: bool) -> Self {
        let root =
            tickwire_control_sys::PrivateDirectory::create_in(Path::new("/tmp"), "tw-md-runtime")
                .unwrap();
        let path = root.path();
        let mut config: config::Config =
            serde_json::from_str(include_str!("../../../examples/md.json")).unwrap();
        config.identity.arch = if cfg!(target_arch = "aarch64") { 2 } else { 1 };
        let mut clock = tickwire_clock_sys::SystemClock::new(
            tickwire_clock::ClockDomain::new(config.identity.host_boot, 1).unwrap(),
        )
        .unwrap();
        let now = clock.sample().unwrap().monotonic.nanos();
        let until = now + 120_000_000_000;
        config.quota_directory = path.join("quota");
        config.control_directory = path.join("journal");
        config.admission_directory = path.join("admissions");
        config.socket = path.join("market.sock");
        for dir in [&config.admission_directory, &path.join("consumers")] {
            fs::create_dir(dir).unwrap();
            fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).unwrap();
        }
        config.quota.consumer_lease_until_ns = until;
        config.quota.policy_starts_ns = now;
        config.quota.policy_ends_ns = until;
        for window in &mut config.quota.windows {
            window.starts_ns = now;
            window.ends_ns = until;
        }
        for grant in &mut config.quota.grants {
            grant.expires_ns = until;
        }
        for rule in &mut config.quota.rules {
            if rule.api_weight == 250 {
                rule.api_weight = 5;
                if recovery {
                    // These feeds request limit=5: charge that same actual
                    // snapshot weight, leaving old grants fully retained.
                    for cost in &mut rule.costs {
                        assert_eq!(cost.units, 250);
                        cost.units = 5;
                    }
                }
            }
        }
        let old = if paused_trade {
            network::Network::start_paused_trade(&path.join("old.der"), "BTCUSDT", 99)
        } else {
            network::Network::start(&path.join("old.der"), "BTCUSDT", 99, true)
        };
        let new = network::Network::start(&path.join("new.der"), "ETHUSDT", 199, false);
        let configure = |feed: &mut config::Feed, network: &network::Network, root: PathBuf| {
            feed.stream_address = network.stream.clone();
            feed.snapshot_address = network.snapshot.clone();
            feed.trust_anchors = vec![root];
            feed.snapshot_limit = 5;
            feed.topics = vec![101];
        };
        configure(&mut config.feeds[0], &old, path.join("old.der"));
        configure(&mut config.feeds[1], &new, path.join("new.der"));
        config.feeds[0].instrument.id = u64::MAX;
        config.feeds[0].events = vec![config::EventFeed {
            owner_id: [90; 16],
            stream_id: 55,
            kind: config::EventType::Trade,
            first_connection_epoch: 1,
            first_operation: 1,
            stream_address: old.event.clone(),
            sbe_api_key_file: None,
            fast_mark: false,
        }];
        config.feeds[1].instrument.id = (1 << 63) | 1001;
        let addition = config.feeds.pop().unwrap();
        config.topics.truncate(1);
        config.topics[0].resource = path.join("topic.bin");
        config.topics[0].maximum_levels = 8;
        config.topics[0].maximum_cache_bytes = 1 << 20;
        let mut spec: config::ResourceSpec =
            serde_json::from_str(include_str!("../../../examples/topic-101.json")).unwrap();
        spec.identity = config.identity.clone();
        spec.capacity = 4096;
        spec.created_mono_ns = now;
        config::write_bytes(
            &config.topics[0].resource,
            &wire_bytes(&spec.header_with_exact_events().unwrap()).unwrap(),
        )
        .unwrap();
        config.expansion = Some(config::ExpansionControl {
            recovery: recovery.then(|| crate::recovery::Config {
                directory: path.join("recovery"),
                restart_from: None,
            }),
            plan_path: path.join("plan.json"),
            result_path: path.join("result.json"),
            effective_path: path.join("effective.json"),
            consumer_directory: path.join("consumers"),
            initial_revision: 1,
        });
        if recovery {
            config.quota.consumer_instance = config.identity.instance;
            config.quota.consumer_generation = config.identity.generation;
            // A failed old process retains its grant. Leave real window capacity
            // for a new owner without resetting or reclaiming the old budget.
            config.quota.grants[0].costs[0].units = 350;
        }
        let input = path.join("config.json");
        config::write(&input, &config).unwrap();
        let log = path.join("md.log");
        let child = spawn(&input, &log);
        let mut fixture = Self {
            _root: root,
            config,
            addition,
            child,
            clock,
            old,
            new,
            log,
        };
        let end = Instant::now() + Duration::from_secs(5);
        while !fixture.config.socket.exists() {
            fixture.alive();
            assert!(Instant::now() < end, "{}", fixture.log());
            std::thread::sleep(Duration::from_millis(1));
        }
        fixture
    }
    pub fn now(&mut self) -> tickwire_clock::MonoTime {
        self.clock.sample().unwrap().monotonic
    }
    pub fn log(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }
    pub fn alive(&mut self) {
        assert!(self.child.try_wait().unwrap().is_none(), "{}", self.log());
    }
    pub fn bank(&self) -> ResourceHeader {
        ResourceHeader::decode(
            &config::read(
                &config::active_resource_path(&self.config.topics[0].resource).unwrap(),
                4096,
            )
            .unwrap(),
        )
        .unwrap()
    }
    pub fn admission(&self) -> Admission {
        let mut local = self.config.identity.clone();
        local.instance = [31; 16];
        local.role = 3;
        local.logical_strategy = 7;
        let mut supervisor = self.config.identity.clone();
        supervisor.instance = [32; 16];
        supervisor.role = 1;
        let metadata = fs::metadata("/proc/self").unwrap();
        let policy = Policy {
            local: local.identity(),
            peer: self.config.identity.identity(),
            peer_process: ProcessIdentity {
                pid: self.child.id(),
                uid: metadata.uid(),
                gid: metadata.gid(),
            },
            required_features: 0x1c,
            supported_features: 1023,
            control_epoch: 7,
            lease_duration_ns: 60_000_000_000,
            max_frame_bytes: 65536,
            nonce: [33; 32],
        };
        let mut old = self.bank();
        old.descriptor_hash = [0; 32];
        Admission {
            client: tickwire_market_source::ConnectConfig {
                endpoint: Endpoint::new(self.config.socket.clone(), &policy),
                bank_header: wire_bytes(&old).unwrap(),
                catalog_hash: self.config.feeds[0].instrument.catalog_hash,
                instrument_set_version: 1,
                instruments: vec![Instrument {
                    id: u64::MAX,
                    version: 1,
                    required_states: 3,
                }],
                cursor_id: [34; 16],
                subscription_generation: 1,
                request_id: [35; 16],
                maximum_levels: 8,
                startup_timeout_ms: 5000,
                clock_generation: 1,
            },
            pid: std::process::id(),
            uid: metadata.uid(),
            gid: metadata.gid(),
            supervisor,
            supervisor_pid: std::process::id(),
            snapshot_id: [36; 16],
        }
    }
    pub fn connect(&mut self, admission: &Admission) -> ShmSource {
        register_admission(&self.config.admission_directory, admission).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let client = loop {
            self.alive();
            let result = self.connect_client(admission);
            match result {
                Ok(client) => break client,
                Err(tickwire_market_source::Error::Transport(
                    tickwire_control_sys::Error::System { operation, source },
                )) if operation == "pending connect"
                    && source.kind() == std::io::ErrorKind::ConnectionRefused
                    && Instant::now() < deadline =>
                {
                    // bind creates the pathname before listen admits connections. Retry only
                    // this pre-session startup race; a failed JOIN may have an owner gate.
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => panic!(
                    "initial JOIN: {error:?}, old connections={}, new connections={}\n{}",
                    self.old.connections.load(Ordering::Relaxed),
                    self.new.connections.load(Ordering::Relaxed),
                    self.log()
                ),
            }
        };
        ShmSource::new(client)
    }
    // This is the original qualified mapping seam, shared by successful JOINs
    // and negative protocol tests against this exact owned MD process.
    pub fn connect_client(
        &mut self,
        admission: &Admission,
    ) -> tickwire_market_source::Result<tickwire_market_source::Client> {
        // SAFETY: the test owns this exact qualified MD child, its trusted configuration,
        // private admissions and lifetime; only the production Runtime writes these mappings.
        unsafe {
            tickwire_market_source::Client::connect(&admission.client, &mut || Ok(self.now()))
        }
    }
    pub fn stop_process(&mut self) {
        let _ = self.child.kill();
        self.child.wait().unwrap();
    }
    pub fn spawn_config(&mut self, config: config::Config, label: &str) {
        assert!(
            self.child.try_wait().unwrap().is_some(),
            "old MD is still live"
        );
        let root = self._root.path();
        let input = root.join(format!("{label}.json"));
        self.log = root.join(format!("{label}.log"));
        config::write(&input, &config).unwrap();
        self.child = spawn(&input, &self.log);
        self.config = config;
    }
    pub fn plan(&mut self, old: &Admission) -> crate::expansion::Plan {
        let bank = self.bank();
        let mut template = bank.clone();
        template.resource_id = [38; 16];
        template.predecessor_ring_epoch = bank.ring_epoch;
        template.ring_epoch += 1;
        template.feed_generation += 1;
        template.first_seq = 1;
        template.cutover_seq = 0;
        template.created_mono_ns = 0;
        template.descriptor_hash = [0; 32];
        let mut target = self.config.clone();
        target.feeds.push(self.addition.clone());
        target.topics[0].instrument_set_version = 2;
        let mut next = old.clone();
        next.client.bank_header = wire_bytes(&template).unwrap();
        next.client.instrument_set_version = 2;
        next.client.instruments.insert(
            0,
            Instrument {
                id: self.addition.instrument.id,
                version: 1,
                required_states: 1,
            },
        );
        next.client.subscription_generation = 2;
        next.client.request_id = [39; 16];
        next.client.cursor_id = [40; 16];
        next.snapshot_id = [41; 16];
        next.client.endpoint.control_epoch += 1;
        next.client.endpoint.nonce = [42; 32];
        crate::expansion::Plan {
            schema: 1,
            operation_id: [43; 16],
            base_revision: 1,
            base_config_hash: crate::expansion::config_hash(&self.config).unwrap(),
            base_bank: wire_bytes(&bank).unwrap(),
            target_revision: 2,
            target_bank: wire_bytes(&template).unwrap(),
            target_config: target,
            topic_id: 101,
            consumers: vec![crate::expansion::Consumer {
                previous: old.clone(),
                next,
            }],
            warmup_deadline_mono_ns: self.now().nanos() + 8_000_000_000,
            maximum_age_ns: 1_000_000_000,
            peak_cache_bytes: 2 << 20,
            maximum_retained_bank_bytes: bank.resource_bytes * 2,
        }
    }
    pub fn path(&self, kind: &str) -> PathBuf {
        self.config
            .expansion
            .as_ref()
            .unwrap()
            .consumer_path(std::process::id(), [31; 16], kind)
    }
    pub fn write_plan(&self, plan: &crate::expansion::Plan) {
        config::write(
            self.config.expansion.as_ref().unwrap().plan_path.as_path(),
            plan,
        )
        .unwrap();
    }
    pub fn assert_original_connections(&self) {
        assert_eq!(self.old.connections.load(Ordering::Relaxed), 2);
        assert_eq!(self.new.connections.load(Ordering::Relaxed), 1);
    }
    pub fn stop(&mut self) {
        self.old.stop();
        self.new.stop();
        let _ = self.child.kill();
        self.child.wait().unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// The identical scenario can run the actual fixed release binary during final
// acceptance. Normal unit runs still use the child entry into production engine.
fn spawn(input: &Path, log: &Path) -> Child {
    let file = fs::File::create(log).unwrap();
    let mut command = match std::env::var_os("TICKWIRE_MD_RUNTIME_TEST_BINARY") {
        Some(binary) => {
            let mut command = Command::new(binary);
            command.arg("--config").arg(input);
            command
        }
        None => {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "server::runtime_tests::process_entry",
                    "--nocapture",
                ])
                .env("TICKWIRE_MD_RUNTIME_TEST_CONFIG", input)
                .env("RUST_MIN_STACK", "8388608");
            command
        }
    };
    command
        .stdout(Stdio::from(file.try_clone().unwrap()))
        .stderr(Stdio::from(file))
        .spawn()
        .unwrap()
}
