use super::network::Network;
use serde_json::json;
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_clock_sys::SystemClock;
use tickwire_control::{Policy, ProcessIdentity};
use tickwire_control_sys::PrivateDirectory;
use tickwire_instrument_catalog::{Artifact, Catalog};
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_market_source::{
    ConnectConfig,
    protocol::{Endpoint, Instrument, wire_bytes},
};
use tickwire_md::{config, server::Admission};

pub struct Market {
    _root: PrivateDirectory,
    pub admission: Admission,
    pub directory: PathBuf,
    pub instrument: tickwire_venue::InstrumentBinding,
    pub artifact: Artifact,
    pub network: Network,
    config_bytes: Vec<u8>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Market {
    pub fn start() -> Self {
        Self::with_tick("0.01")
    }
    pub fn with_tick(tick: &str) -> Self {
        let root = PrivateDirectory::create_in(Path::new("/tmp"), "tcat").unwrap();
        let path = root.path();
        let mut config: config::Config =
            serde_json::from_str(include_str!("../../../tickwire-md/examples/md.json")).unwrap();
        config.identity.arch = if cfg!(target_arch = "aarch64") { 2 } else { 1 };
        config.socket = path.join("md.sock");
        config.control_directory = path.join("control");
        config.quota_directory = path.join("quota");
        config.admission_directory = path.join("admissions");
        fs::create_dir(&config.admission_directory).unwrap();
        fs::set_permissions(
            &config.admission_directory,
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        config.expansion = None;
        let mut clock =
            SystemClock::new(ClockDomain::new(config.identity.host_boot, 1).unwrap()).unwrap();
        let now = clock.sample().unwrap().monotonic.nanos();
        let until = now + 120_000_000_000;
        config.quota.consumer_lease_until_ns = until;
        config.quota.policy_starts_ns = now;
        config.quota.policy_ends_ns = until;
        for w in &mut config.quota.windows {
            w.starts_ns = now;
            w.ends_ns = until;
        }
        for g in &mut config.quota.grants {
            g.expires_ns = until;
        }
        for r in &mut config.quota.rules {
            r.source = tickwire_binance_quota::bootstrap::Source::SpotTest;
            if r.api_weight == 250 {
                r.api_weight = 5;
                for c in &mut r.costs {
                    assert_eq!(c.units, 250);
                    c.units = 5;
                }
            }
        }
        let network = Network::start(&path.join("root.der"));
        config.feeds.truncate(1);
        let feed = &mut config.feeds[0];
        feed.source = tickwire_binance_quota::bootstrap::Source::SpotTest;
        feed.stream_address = network.stream.clone();
        feed.snapshot_address = network.snapshot.clone();
        feed.trust_anchors = vec![path.join("root.der")];
        feed.snapshot_limit = 5;
        feed.topics = vec![101];
        feed.events.clear();
        feed.instrument.id = u64::MAX;
        feed.instrument.price_tick = tick.into();
        let instrument = &feed.instrument;
        let asset = |a: &config::Asset| json!({"id":a.id.to_string(),"version":a.version.to_string(),"symbol":a.symbol,"exponent":a.exponent});
        let raw=serde_json::to_vec(&json!({"schema":1,"venue":feed.venue_id,"environment":"Test","market":"Spot","catalog_version":instrument.catalog_version.to_string(),"assets":[asset(&instrument.base),asset(&instrument.quote),{"id":"4","version":"1","symbol":"BNB","exponent":-8}],"instruments":[{"id":instrument.id.to_string(),"version":instrument.version.to_string(),"symbol":instrument.symbol,"product":"Spot","base":instrument.base.id.to_string(),"quote":instrument.quote.id.to_string(),"settlement":instrument.settlement.id.to_string(),"price_tick":instrument.price_tick,"quantity_step":instrument.quantity_step,"contract":{"kind":"Cash"}}]})).unwrap();
        let artifact = Artifact {
            digest: tickwire_recovery::digest(&raw),
            bytes: raw,
        };
        let catalog = artifact.catalog().unwrap();
        feed.instrument.catalog_hash = catalog.catalog_hash();
        let instrument = feed
            .instrument
            .build(tickwire_binance_public::Source::SpotTest)
            .unwrap();
        assert!(catalog.instrument_matches(tickwire_venue::Product::Spot, &instrument));
        let catalog_path = path.join("catalog.json");
        config::write_bytes(&catalog_path, &artifact.bytes).unwrap();
        config.catalogs = Some(vec![config::CatalogFile {
            reference: format!(
                "artifact://fixture/instrument-catalog@sha256:{}",
                super::inputs::hex(artifact.digest)
            ),
            path: catalog_path,
        }]);
        config.topics.truncate(1);
        config.topics[0].resource = path.join("topic.bin");
        config.topics[0].maximum_levels = 8;
        config.topics[0].maximum_cache_bytes = 1 << 20;
        let mut spec: config::ResourceSpec =
            serde_json::from_str(include_str!("../../../tickwire-md/examples/topic-101.json"))
                .unwrap();
        spec.identity = config.identity.clone();
        spec.capacity = 4096;
        spec.created_mono_ns = now;
        config::write_bytes(
            &config.topics[0].resource,
            &wire_bytes(&spec.header_with_exact_events().unwrap()).unwrap(),
        )
        .unwrap();
        let config_bytes = serde_json::to_vec(&config).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let run = stop.clone();
        let child_config = config.clone();
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("catalog-md-runtime".into())
            .stack_size(16 * 1024 * 1024)
            .spawn(move || {
                let mut runtime = tickwire_md::engine::Runtime::new(child_config).unwrap();
                ready_tx.send(()).unwrap();
                while !run.load(Ordering::Relaxed) {
                    let poll = runtime.poll_once().unwrap();
                    runtime.wait(poll).unwrap();
                }
            })
            .unwrap();
        ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let bank = ResourceHeader::decode(
            &config::read(
                &config::active_resource_path(&config.topics[0].resource).unwrap(),
                4096,
            )
            .unwrap(),
        )
        .unwrap();
        let admission = admission(&config, &bank, &catalog);
        let end = Instant::now() + Duration::from_secs(5);
        while network.snapshots.load(Ordering::Relaxed) == 0
            || network.frames.load(Ordering::Relaxed) < 2
        {
            assert!(
                !thread.is_finished(),
                "MD Runtime ended before real snapshot and delta"
            );
            assert!(Instant::now() < end, "actual MD network readiness");
            std::thread::sleep(Duration::from_millis(1));
        }
        Self {
            _root: root,
            directory: config.admission_directory,
            admission,
            instrument,
            artifact,
            network,
            config_bytes,
            stop,
            thread: Some(thread),
        }
    }
    pub fn now(&self) -> MonoTime {
        SystemClock::new(self.admission.client.domain().unwrap())
            .unwrap()
            .sample()
            .unwrap()
            .monotonic
    }
    pub fn save_evidence(&self, directory: &Path) {
        super::fixture::write(&directory.join("md-config.json"), &self.config_bytes);
        super::fixture::write(
            &directory.join("md-instrument-catalog.json"),
            &self.artifact.bytes,
        );
        for (source, name) in [
            (&self.network.first_delta, "actual-first-delta.json"),
            (&self.network.first_snapshot, "actual-first-snapshot.json"),
        ] {
            super::fixture::write(&directory.join(name), &fs::read(source).unwrap());
        }
        super::fixture::write(
            &directory.join("md-root.der"),
            &fs::read(self._root.path().join("root.der")).unwrap(),
        );
    }
}
fn admission(config: &config::Config, bank: &ResourceHeader, catalog: &Catalog) -> Admission {
    let mut local = config.identity.clone();
    local.instance = [21; 16];
    local.role = 3;
    local.logical_strategy = 1;
    let mut supervisor = config.identity.clone();
    supervisor.instance = [1; 16];
    supervisor.role = 1;
    let meta = fs::metadata("/proc/self").unwrap();
    let policy = Policy {
        local: local.identity(),
        peer: config.identity.identity(),
        peer_process: ProcessIdentity {
            pid: std::process::id(),
            uid: meta.uid(),
            gid: meta.gid(),
        },
        required_features: 0x1c,
        supported_features: 1023,
        control_epoch: 7,
        lease_duration_ns: 60_000_000_000,
        max_frame_bytes: 65536,
        nonce: [1; 32],
    };
    Admission {
        client: ConnectConfig {
            endpoint: Endpoint::new(config.socket.clone(), &policy),
            bank_header: wire_bytes(bank).unwrap(),
            catalog_hash: catalog.catalog_hash(),
            instrument_set_version: 1,
            instruments: vec![Instrument {
                id: u64::MAX,
                version: 1,
                required_states: 1,
            }],
            cursor_id: [34; 16],
            subscription_generation: 1,
            request_id: [35; 16],
            maximum_levels: 8,
            startup_timeout_ms: 5000,
            clock_generation: 1,
        },
        pid: std::process::id(),
        uid: meta.uid(),
        gid: meta.gid(),
        supervisor,
        supervisor_pid: std::process::id(),
        snapshot_id: [36; 16],
    }
}
impl Drop for Market {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
        self.network.stop();
    }
}
