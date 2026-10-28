use std::path::Path;
use tickwire_ipc::ResourceHeader;
use tickwire_market_source::protocol::{Endpoint, IdentityConfig, wire_bytes};
use tickwire_strategy_sdk::bootstrap::Bootstrap;

pub const BOOT: [u8; 16] = [71; 16];
pub const NAMESPACE: [u8; 16] = [0, 0, 0, 0, 0, 0, 64, 0, 128, 0, 0, 0, 0, 0, 0, 1];

pub fn identity(id: u8, role: u16, build: [u8; 32]) -> IdentityConfig {
    IdentityConfig {
        instance: [id; 16],
        generation: 1,
        role,
        logical_strategy: if role == 3 { 1 } else { 0 },
        host_boot: BOOT,
        namespace: NAMESPACE,
        build_hash: build,
        qualification_hash: [3; 32],
        manifest_hash: [4; 32],
        arch: if cfg!(target_arch = "x86_64") { 1 } else { 2 },
        trust_profile: 1,
    }
}

fn bank(kind: u16, producer: &IdentityConfig, consumer: &IdentityConfig) -> ResourceHeader {
    let record_bytes = if kind == 2 { 1024 } else { 512 };
    let h = ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
        header_bytes: 4096,
        resource_kind: kind,
        target_arch: producer.arch,
        endian: 1,
        atomic_width: 8,
        page_shift: 12,
        isolation_shift: 7,
        record_bytes,
        capacity: 64,
        resource_bytes: 8192 + 64 * u64::from(record_bytes),
        resource_id: [70 + kind as u8; 16],
        channel_id: [74 + kind as u8; 16],
        producer_instance: producer.instance,
        consumer_instance: consumer.instance,
        producer_generation: producer.generation,
        host_boot_id: producer.host_boot,
        namespace_id: producer.namespace,
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
        required_features: 0x1c | if kind == 2 { 128 } else { 256 },
        qos: 1,
        ..Default::default()
    };
    tickwire_shm::validate_bank(&h).unwrap();
    h
}

pub fn bootstrap(root: &Path, build: [u8; 32], uid: u32, gid: u32) -> (Bootstrap, ResourceHeader) {
    let worker = identity(2, 3, build);
    let exec = identity(4, 4, [5; 32]);
    let md = identity(3, 2, [6; 32]);
    let endpoint = |peer: IdentityConfig, name: &str| Endpoint {
        path: root.join(name),
        local: worker.clone(),
        peer,
        peer_pid: std::process::id(),
        peer_uid: uid,
        peer_gid: gid,
        required_features: 0x5e,
        supported_features: 511,
        control_epoch: 1,
        lease_duration_ns: 10_000_000_000,
        max_frame_bytes: 65536,
        nonce: [61; 32],
    };
    let mut spec: tickwire_md::config::ResourceSpec =
        serde_json::from_str(include_str!("../../../tickwire-md/examples/topic-101.json")).unwrap();
    spec.identity = md.clone();
    spec.catalog_version = 14;
    spec.capacity = 64;
    spec.arena_classes = vec![(65536, 32)];
    let mut market = spec.header().unwrap();
    market.max_consumers = 1;
    let intent = bank(2, &worker, &exec);
    let report = bank(3, &exec, &worker);
    (
        Bootstrap {
            version: 1,
            market: endpoint(md, "market.sock"),
            external_market: None,
            market_admission_ready: None,
            intent: endpoint(exec.clone(), "intent.sock"),
            report: endpoint(exec, "report.sock"),
            report_header: wire_bytes(&report).unwrap(),
            report_cursor_id: [36; 16],
            report_subscription_generation: 1,
            report_directory: root.join("reports"),
            join_file: root.join("join.json"),
            intent_header: wire_bytes(&intent).unwrap(),
            intent_cursor_id: [35; 16],
            intent_subscription_generation: 1,
            risk_domain: [1; 16],
            command_directory: root.join("commands"),
            clock_generation: 1,
            startup_timeout_ms: 1000,
            maximum_market_age_ns: 10_000_000_000,
            maximum_levels: 100,
            poll_records: 64,
            idle_sleep_us: 100,
            recovery_template: None,
            handover_enabled: false,
            preflight_job: None,
            factory: Some(tickwire_strategy_sdk::registered::FactoryBinding {
                id: "counting".into(),
                configuration_file: root.join("parameters.json"),
                configuration: tickwire_release::Object::of(b"{}"),
                revision: 1,
            }),
            demo_enabled: false,
            demo_command_file: None,
            demo_minimum_bid_ticks: 0,
        },
        market,
    )
}
