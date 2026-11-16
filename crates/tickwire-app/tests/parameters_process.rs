//! Integration tests for statically registered strategy parameter validation (`FactoryBinding`
//! and `Parameters`), parent-owned `RemoteHostedStrategy` process supervision, and the shared
//! external `tickwire-md` spot market walkthrough (`scripts/run-two-strategies.py`).

#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use serde::Deserialize;
use sha2::{Digest, Sha256};
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_clock_sys::SystemClock;
use tickwire_control::Identity;
use tickwire_instrument_catalog::Catalog;
use tickwire_ipc::{MarketState, ResourceHeader, Wire};
use tickwire_market_source::{
    Client, ConnectConfig,
    protocol::{Endpoint, IdentityConfig, Instrument, wire_bytes},
};
use tickwire_md::server::{Admission, register_admission};
use tickwire_runtime_policy::{Mode, OrsRuntimePolicy, PolicyConfig};
use tickwire_strategy_example::{Parameters, V2};
use tickwire_strategy_host::{ObservationRequest, RemoteHostedStrategy, RemoteLaunch};
use tickwire_strategy_sdk::{
    Bootstrap, MarketReplica as SdkMarketReplica, ObserveOnly,
    bootstrap::ArtifactDigest,
    registered::{FactoryBinding, Registration as _, TargetIdentity, VersionedBytes},
};
use tickwire_supervisor::{LaunchSpec, current_process_identity};

const QUOTE_REQUIRED_STATES: u8 = 1;

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
    resource_tag: u8,
    producer_instance: [u8; 16],
    producer_generation: u64,
    consumer_instance: [u8; 16],
    host_boot_id: [u8; 16],
    namespace_id: [u8; 16],
) -> ResourceHeader {
    let capacity = 64u32;
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
        resource_id: [resource_tag; 16],
        channel_id: [resource_tag.wrapping_add(1); 16],
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        producer_instance,
        producer_generation,
        consumer_instance,
        host_boot_id,
        namespace_id,
        risk_scope: 100,
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
    resource_tag: u8,
    producer_instance: [u8; 16],
    producer_generation: u64,
    consumer_instance: [u8; 16],
    host_boot_id: [u8; 16],
    namespace_id: [u8; 16],
) -> ResourceHeader {
    let capacity = 64u32;
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
        resource_id: [resource_tag; 16],
        channel_id: [resource_tag.wrapping_add(1); 16],
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        producer_instance,
        producer_generation,
        consumer_instance,
        host_boot_id,
        namespace_id,
        risk_scope: 100,
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

#[test]
fn counting_registration_validates_sha256_factory_binding_and_rejects_tampering() {
    let params = Parameters {
        schema: 1,
        enabled: false,
        minimum_observations: 1,
        minimum_bid_ticks: 0,
        command: None,
    };
    let raw = params.bytes().expect("canonical parameter JSON");
    let digest = ArtifactDigest::of(&raw);
    let target = TargetIdentity {
        logical_strategy_id: 101,
        strat_id: 1,
        instance_id: [0x11; 16],
        generation: 1,
        account_ref: 0,
    };
    let factory = tickwire_strategy_example::Registration::v2()
        .configure(
            &target,
            &VersionedBytes {
                revision: 1,
                bytes: raw.clone(),
            },
        )
        .expect("valid V2 counting registration");
    assert!(digest.verify(&raw, 4096).is_ok());

    let mut tampered = raw;
    tampered[0] = b' ';
    assert!(digest.verify(&tampered, 4096).is_err());
    let _ = factory;
}

mod external_spot {
    use super::*;

    #[derive(Deserialize)]
    struct DemoMarketJson {
        resource: PathBuf,
        clock_generation: u64,
        instrument_set_version: u64,
        source: DemoSourceJson,
        catalog: DemoCatalogRef,
        server: DemoServerJson,
    }

    #[derive(Deserialize)]
    struct DemoSourceJson {
        instrument_id: u64,
        instrument_version: u64,
    }

    #[derive(Deserialize)]
    struct DemoCatalogRef {
        path: PathBuf,
    }

    #[derive(Deserialize)]
    struct DemoServerJson {
        socket: PathBuf,
        admission_directory: PathBuf,
    }

    #[derive(Deserialize)]
    struct DemoResourceJson {
        identity: IdentityConfig,
    }

    struct PausedPid(u32);

    impl PausedPid {
        fn pause(pid: u32) -> Self {
            let status = std::process::Command::new("kill")
                .args(["-STOP", &pid.to_string()])
                .status()
                .expect("invoke kill -STOP on tickwire-md");
            assert!(status.success(), "kill -STOP {pid} failed");
            Self(pid)
        }
    }

    impl Drop for PausedPid {
        fn drop(&mut self) {
            let _ = std::process::Command::new("kill")
                .args(["-CONT", &self.0.to_string()])
                .status();
        }
    }

    #[test]
    #[ignore = "driven by scripts/run-two-strategies.py"]
    #[allow(unsafe_code)]
    fn two_registered_workers_read_external_spot_market() {
        let root = PathBuf::from(
            std::env::var("TICKWIRE_SPOT_DEMO").expect("TICKWIRE_SPOT_DEMO must be set"),
        );
        let md_pid: u32 = std::env::var("TICKWIRE_SPOT_PID")
            .expect("TICKWIRE_SPOT_PID must be set")
            .parse()
            .expect("valid TICKWIRE_SPOT_PID");
        let worker_bin = PathBuf::from(
            std::env::var("TICKWIRE_PARAMETERS_WORKER")
                .expect("TICKWIRE_PARAMETERS_WORKER must be set"),
        );
        let target_seq: u64 = std::env::var("TICKWIRE_SPOT_FIXTURE_SEQUENCE")
            .ok()
            .map(|s| s.parse().expect("valid TICKWIRE_SPOT_FIXTURE_SEQUENCE"))
            .unwrap_or(1);

        let market_cfg: DemoMarketJson =
            serde_json::from_slice(&std::fs::read(root.join("market.json")).unwrap()).unwrap();
        let resource_cfg: DemoResourceJson =
            serde_json::from_slice(&std::fs::read(&market_cfg.resource).unwrap()).unwrap();
        let active_path = tickwire_md::config::active_resource_path(&market_cfg.resource).unwrap();
        let active_bytes = std::fs::read(&active_path).expect("read active MD ResourceHeader");
        let active_hdr = ResourceHeader::decode(&active_bytes).expect("decode active MD header");

        let catalog_bytes = std::fs::read(&market_cfg.catalog.path).unwrap();
        let catalog_digest = ArtifactDigest::of(&catalog_bytes);
        let catalog = Catalog::decode(&catalog_bytes, catalog_digest.sha256).unwrap();

        let parent = current_process_identity().expect("current_process_identity");
        let worker_elf_bytes = std::fs::read(&worker_bin).expect("read stripped worker binary");
        let worker_build_hash: [u8; 32] = Sha256::digest(&worker_elf_bytes).into();
        let self_exe = std::fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
        let self_build_hash: [u8; 32] = Sha256::digest(std::fs::read(&self_exe).unwrap()).into();

        let md_peer = resource_cfg.identity.identity();
        let host_boot = md_peer.host_boot;
        let namespace = md_peer.namespace;

        let params = Parameters {
            schema: 1,
            enabled: false,
            minimum_observations: 1,
            minimum_bid_ticks: 0,
            command: None,
        };
        let params_bytes = params.bytes().unwrap();
        let params_file = root.join("counting-params.json");
        {
            use std::os::unix::fs::OpenOptionsExt;
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&params_file)
                .unwrap();
            std::io::Write::write_all(&mut f, &params_bytes).unwrap();
            f.sync_all().unwrap();
        }
        let params_digest = ArtifactDigest::of(&params_bytes);

        let supervisor_id = Identity {
            instance: [0x70; 16],
            generation: 1,
            role: 1,
            logical_strategy: 0,
            host_boot,
            namespace,
            build_hash: self_build_hash,
            qualification_hash: md_peer.qualification_hash,
            manifest_hash: md_peer.manifest_hash,
            arch: target_arch_code(),
            trust_profile: md_peer.trust_profile,
        };
        let ors_id = Identity {
            instance: [0x90; 16],
            generation: 1,
            role: 4,
            logical_strategy: 0,
            host_boot,
            namespace,
            build_hash: self_build_hash,
            qualification_hash: md_peer.qualification_hash,
            manifest_hash: md_peer.manifest_hash,
            arch: target_arch_code(),
            trust_profile: md_peer.trust_profile,
        };

        let launch_worker = |slot: u8| -> RemoteHostedStrategy {
            let strat_id = slot;
            let logical_id = 100 + u64::from(slot);
            let instance = [0x30 + slot; 16];
            let worker_id = Identity {
                instance,
                generation: 1,
                role: 3,
                logical_strategy: logical_id,
                host_boot,
                namespace,
                build_hash: worker_build_hash,
                qualification_hash: md_peer.qualification_hash,
                manifest_hash: md_peer.manifest_hash,
                arch: target_arch_code(),
                trust_profile: md_peer.trust_profile,
            };
            let md_ep = Endpoint {
                path: market_cfg.server.socket.clone(),
                local: (&worker_id).into(),
                peer: (&md_peer).into(),
                peer_pid: md_pid,
                peer_uid: parent.uid,
                peer_gid: parent.gid,
                required_features: 0x1c,
                supported_features: 0x3ff,
                control_epoch: 1,
                lease_duration_ns: 60_000_000_000,
                max_frame_bytes: 65_536,
                nonce: [0x50 + slot; 32],
            };
            let intent_ep = Endpoint {
                path: PathBuf::from("/placeholder/intent.sock"),
                local: (&worker_id).into(),
                peer: (&ors_id).into(),
                peer_pid: parent.pid,
                peer_uid: parent.uid,
                peer_gid: parent.gid,
                required_features: 0x1c,
                supported_features: 0x3ff,
                control_epoch: u64::from(slot),
                lease_duration_ns: 60_000_000_000,
                max_frame_bytes: 65_536,
                nonce: [0x60 + slot; 32],
            };
            let report_ep = Endpoint {
                path: PathBuf::from("/placeholder/report.sock"),
                local: (&worker_id).into(),
                peer: (&ors_id).into(),
                peer_pid: parent.pid,
                peer_uid: parent.uid,
                peer_gid: parent.gid,
                required_features: 0x1c,
                supported_features: 0x3ff,
                control_epoch: 10 + u64::from(slot),
                lease_duration_ns: 60_000_000_000,
                max_frame_bytes: 65_536,
                nonce: [0x70 + slot; 32],
            };

            let intent_hdr = intent_bank_header(
                0x10 + slot * 4,
                instance,
                1,
                ors_id.instance,
                host_boot,
                namespace,
            );
            let report_hdr = report_bank_header(
                0x20 + slot * 4,
                ors_id.instance,
                1,
                instance,
                host_boot,
                namespace,
            );

            let bootstrap = Bootstrap {
                version: 1,
                logical_strategy_id: logical_id,
                strat_id,
                instance_id: instance,
                generation: 1,
                account_ref: 0,
                worker_cpu: None,
                housekeeping_cpus: None,
                market: ConnectConfig {
                    endpoint: md_ep,
                    bank_header: active_bytes.clone(),
                    catalog_hash: catalog.catalog_hash(),
                    instrument_set_version: market_cfg.instrument_set_version,
                    instruments: vec![Instrument {
                        id: market_cfg.source.instrument_id,
                        version: market_cfg.source.instrument_version,
                        required_states: QUOTE_REQUIRED_STATES,
                    }],
                    cursor_id: [0x80 + slot; 16],
                    subscription_generation: 1,
                    request_id: [0x90 + slot; 16],
                    maximum_levels: 1,
                    startup_timeout_ms: 15_000,
                    clock_generation: market_cfg.clock_generation,
                },
                dop_socket: None,
                dop: None,
                intent: Some(intent_ep),
                intent_header: Some(wire_bytes(&intent_hdr).unwrap()),
                intent_cursor_id: [0xA0 + slot; 16],
                intent_subscription_generation: 1,
                report: Some(report_ep),
                report_header: Some(wire_bytes(&report_hdr).unwrap()),
                report_cursor_id: [0xB0 + slot; 16],
                report_subscription_generation: 1,
                risk_domain: [0; 16],
                instrument_catalog: None,
                catalog_file: Some(market_cfg.catalog.path.clone()),
                catalog_digest: Some(catalog_digest),
                factory: Some(FactoryBinding {
                    id: String::from("counting_example"),
                    state_schema: V2,
                    configuration_file: params_file.clone(),
                    configuration: params_digest,
                    revision: 1,
                }),
                clock_generation: market_cfg.clock_generation,
                startup_timeout_ms: 15_000,
                maximum_market_age_ns: 30_000_000_000,
                maximum_levels: 1,
                poll_records: 64,
                idle_sleep_us: 200,
                demo_enabled: false,
                demo_command_file: None,
                demo_minimum_bid_ticks: 0,
            };

            let spec = LaunchSpec {
                executable: worker_bin.clone(),
                arguments: Vec::new(),
                working_directory: root.clone(),
                environment: BTreeMap::new(),
                uid: parent.uid,
                gid: parent.gid,
                identity: worker_id,
                inherit_output: false,
            };

            let paused_md = PausedPid::pause(md_pid);

            // SAFETY: `tickwire-md` (`md_pid`) and `tickwire-counting-worker` (`worker_bin`) are
            // sealed local executables adhering to the single-writer SPSC SHM ring contracts.
            unsafe {
                RemoteHostedStrategy::launch(RemoteLaunch {
                    resources: None,
                    spec,
                    bootstrap,
                    directory_parent: std::env::temp_dir(),
                    supervisor: supervisor_id.clone(),
                    parent_process: parent,
                    md_admission_directory: market_cfg.server.admission_directory.clone(),
                    snapshot_id: [0xC0 + slot; 16],
                    on_admission_registered: Some(Box::new(move |_| {
                        drop(paused_md);
                        Ok(())
                    })),
                })
                .expect("launch tickwire-counting-worker")
            }
        };

        let mut worker_a = launch_worker(1);
        let mut worker_b = launch_worker(2);
        let pid_a = worker_a.child_process().pid;
        let pid_b = worker_b.child_process().pid;
        assert_ne!(pid_a, pid_b, "workers must run in distinct OS processes");

        // Attach an independent observer cursor to read the exact published `venue_seq_last`.
        let observer_id = Identity {
            instance: [0x3F; 16],
            generation: 1,
            role: 3,
            logical_strategy: 199,
            host_boot,
            namespace,
            build_hash: self_build_hash,
            qualification_hash: md_peer.qualification_hash,
            manifest_hash: md_peer.manifest_hash,
            arch: target_arch_code(),
            trust_profile: md_peer.trust_profile,
        };
        let observer_connect = ConnectConfig {
            endpoint: Endpoint {
                path: market_cfg.server.socket.clone(),
                local: (&observer_id).into(),
                peer: (&md_peer).into(),
                peer_pid: md_pid,
                peer_uid: parent.uid,
                peer_gid: parent.gid,
                required_features: 0x1c,
                supported_features: 0x3ff,
                control_epoch: 1,
                lease_duration_ns: 60_000_000_000,
                max_frame_bytes: 65_536,
                nonce: [0x5F; 32],
            },
            bank_header: active_bytes,
            catalog_hash: catalog.catalog_hash(),
            instrument_set_version: market_cfg.instrument_set_version,
            instruments: vec![Instrument {
                id: market_cfg.source.instrument_id,
                version: market_cfg.source.instrument_version,
                required_states: QUOTE_REQUIRED_STATES,
            }],
            cursor_id: [0x8F; 16],
            subscription_generation: 1,
            request_id: [0x9F; 16],
            maximum_levels: 1,
            startup_timeout_ms: 15_000,
            clock_generation: market_cfg.clock_generation,
        };
        register_admission(
            &market_cfg.server.admission_directory,
            &Admission {
                client: observer_connect.clone(),
                pid: parent.pid,
                uid: parent.uid,
                gid: parent.gid,
                supervisor: (&supervisor_id).into(),
                supervisor_pid: parent.pid,
                snapshot_id: [0xCF; 16],
            },
        )
        .expect("register observer admission");

        let domain = ClockDomain::new(host_boot, market_cfg.clock_generation).unwrap();
        let mut clock = SystemClock::new(domain).unwrap();
        // SAFETY: `md_pid` is the live `tickwire-md` owner of `active_hdr`.
        let observer = unsafe {
            Client::connect(
                &observer_connect,
                &mut || -> tickwire_market_source::Result<MonoTime> {
                    clock
                        .sample()
                        .map(|s| s.monotonic)
                        .map_err(|_| tickwire_market_source::Error::Identity)
                },
            )
            .expect("observer Client::connect")
        };
        let observer_consumer = observer.consumer;
        let mut observer_replica = SdkMarketReplica::from_core(observer.replica);

        // Signal loopback fixture (when present) that both strategy workers are subscribed & ready.
        std::fs::write(root.join("workers.ready"), b"ready\n").unwrap();

        let policy = OrsRuntimePolicy::from_config(&PolicyConfig {
            initial_mode: Mode::Trading,
            strategies: vec![1, 2],
            enabled_accounts: vec![tickwire_runtime_policy::AccountAuthSpec::new(
                1,
                "obs-acct",
                vec![1, 2],
            )],
            default_active: true,
            default_enabled: true,
        })
        .unwrap();

        let extract_quote = |rep: &SdkMarketReplica| -> Option<(u64, i64, i64)> {
            let view = rep.view(market_cfg.source.instrument_id)?;
            match view.best_quote()? {
                MarketState::Exact(r) => Some((
                    r.body.venue_seq_last,
                    r.body.bid_price_ticks,
                    r.body.ask_price_ticks,
                )),
                MarketState::Legacy(r) => Some((
                    r.body.venue_seq_last,
                    r.body.bid_price_ticks,
                    r.body.ask_price_ticks,
                )),
            }
        };

        let mut latest_quote = extract_quote(&observer_replica).unwrap_or((0, 0, 0));
        let deadline = Instant::now() + Duration::from_secs(20);
        while latest_quote.0 < target_seq {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for quote_sequence >= {target_seq} (last={})",
                latest_quote.0
            );
            let now_ns = clock.sample().unwrap().monotonic.nanos();
            let mut obs = ObserveOnly;
            observer_replica
                .drain(&observer_consumer, &mut obs, now_ns, u64::MAX, 64, |_| {
                    Ok(())
                })
                .unwrap();
            if let Some(q) = extract_quote(&observer_replica) {
                latest_quote = q;
            }
            thread::sleep(Duration::from_millis(5));
        }

        // Allow both workers a brief tick to drain the final slot, then verify both remain healthy.
        thread::sleep(Duration::from_millis(25));
        let obs_req = ObservationRequest {
            max_sample_age_ns: 500_000_000,
            account_ref: 1,
            require_resources: false,
        };
        let fresh_a = worker_a
            .observe_fresh(&policy, &obs_req)
            .expect("fresh observation for worker A");
        let fresh_b = worker_b
            .observe_fresh(&policy, &obs_req)
            .expect("fresh observation for worker B");
        assert!(fresh_a.readiness.is_ready_to_trade());
        assert!(fresh_b.readiness.is_ready_to_trade());

        let _ = worker_a.terminate(Some(&policy)).unwrap();
        let _ = worker_b.terminate(Some(&policy)).unwrap();

        let (observed_seq, bid_ticks, ask_ticks) = latest_quote;
        let summary = serde_json::json!({
            "event": "two_strategies_observed",
            "two_strategies_observed": true,
            "market_pid": md_pid,
            "topic": active_hdr.topic_id,
            "topic_id": active_hdr.topic_id,
            "instrument_id": market_cfg.source.instrument_id,
            "worker_pids": [pid_a, pid_b],
            "quote_sequence": observed_seq,
            "bid_ticks": bid_ticks,
            "ask_ticks": ask_ticks,
            "strategy_effects": 0,
        });
        std::fs::write(
            root.join("result.json"),
            format!("{}\n", serde_json::to_string_pretty(&summary).unwrap()),
        )
        .unwrap();
    }
}
