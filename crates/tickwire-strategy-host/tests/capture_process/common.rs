use std::{
    fs,
    os::unix::fs::MetadataExt,
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
#[path = "../../../tickwire-account/tests/common/mod.rs"]
pub mod account_fixture;
#[path = "market.rs"]
mod market;

pub enum Action {
    Update,
    Stop,
    Detached(mpsc::Sender<(bool, bool)>),
}
pub struct Market {
    pub action: mpsc::Sender<Action>,
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
pub fn launch(worker: &Path, life: Duration) -> (Owner, Market, PrivateDirectory) {
    match launch_selected(worker, life, |_, _| None) {
        (Launched::Capture(owner), market, directory) => (*owner, market, directory),
        _ => unreachable!("capture selection"),
    }
}
pub struct NoRecovery;
impl tickwire_strategy_sdk::runtime::RecoveryResolver for NoRecovery {
    fn resolve(
        &mut self,
        _: &tickwire_venue::CanonicalCommand,
    ) -> tickwire_strategy_sdk::Result<Option<tickwire_strategy_sdk::runtime::ResolvedRecovery>>
    {
        panic!("unapproved rebuild test must never submit an intent")
    }
}
pub struct RebuildSetup {
    pub bootstrap: tickwire_strategy_sdk::bootstrap::Bootstrap,
    pub publication: PathBuf,
    pub policy: tickwire_runtime_policy::ControlPlane,
}
pub enum Launched {
    Capture(Box<Owner>),
    Rebuild(Box<tickwire_strategy_host::rebuild::Hosted<NoRecovery>>),
}
pub fn launch_selected(
    worker: &Path,
    life: Duration,
    configure: impl FnOnce(&mut Bootstrap, &Path) -> Option<RebuildSetup>,
) -> (Launched, Market, PrivateDirectory) {
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
    let mut bootstrap = Bootstrap {
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
    let rebuild = configure(&mut bootstrap, directory.path());
    bootstrap.validate(MonoTime::new(domain, now)).unwrap();
    let meta = fs::metadata("/proc/self").unwrap();
    let args = Launch {
        spec: tickwire_supervisor::LaunchSpec {
            identity: bootstrap.market.endpoint.local.identity(),
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
    let owner = unsafe {
        match rebuild {
            None => Launched::Capture(Box::new(Owner::launch(args).unwrap())),
            Some(rebuild) => Launched::Rebuild(Box::new(
                tickwire_strategy_host::rebuild::Hosted::launch(
                    tickwire_strategy_host::rebuild::Launch {
                        capture: args,
                        bootstrap: rebuild.bootstrap,
                        publication_directory: rebuild.publication,
                    },
                    NoRecovery,
                    rebuild.policy,
                )
                .unwrap(),
            )),
        }
    };
    (owner, market, directory)
}
pub fn wait(mut ready: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(20);
    while !ready() {
        assert!(Instant::now() < end, "capture wait deadline");
        std::thread::sleep(Duration::from_millis(2));
    }
}
pub fn seed(owner: &mut Owner) {
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
