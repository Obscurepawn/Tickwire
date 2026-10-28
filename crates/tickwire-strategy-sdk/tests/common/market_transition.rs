#![allow(dead_code)]
use tickwire_control::{ClientHandshake, Identity, Policy, ProcessIdentity, Session};
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_market_source::{
    ConnectConfig,
    protocol::{Endpoint, Instrument, wire_bytes},
    transition::{Approval, Commit},
};
pub const NOW: u64 = 1_500_000_000;
pub fn identity(id: u8, role: u16) -> Identity {
    Identity {
        instance: [id; 16],
        generation: 1,
        role,
        logical_strategy: if role == 3 { 7 } else { 0 },
        host_boot: [8; 16],
        namespace: [9; 16],
        build_hash: [10; 32],
        qualification_hash: [11; 32],
        manifest_hash: [12; 32],
        arch: if cfg!(target_arch = "x86_64") { 1 } else { 2 },
        trust_profile: 1,
    }
}
pub fn policy(local: Identity, peer: Identity, process: ProcessIdentity) -> Policy {
    Policy {
        local,
        peer,
        peer_process: process,
        required_features: 0x1c,
        supported_features: 0x5f,
        control_epoch: 7,
        lease_duration_ns: 3_000_000_000,
        max_frame_bytes: 65536,
        nonce: [71; 32],
    }
}
pub fn approval() -> Approval {
    let mut bank = ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
        header_bytes: 4096,
        resource_kind: 1,
        target_arch: identity(3, 2).arch,
        endian: 1,
        atomic_width: 8,
        page_shift: 12,
        isolation_shift: 7,
        record_bytes: 512,
        capacity: 64,
        resource_bytes: 8192 + 64 * 512,
        resource_id: [13; 16],
        channel_id: [14; 16],
        producer_instance: [3; 16],
        producer_generation: 1,
        host_boot_id: [8; 16],
        namespace_id: [9; 16],
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        first_seq: 1,
        initial_membership_version: 1,
        producer_page_offset: 4096,
        slot_offset: 8192,
        max_consumers: 4,
        schema_major: 1,
        clock_domain: 1,
        required_features: 0x5c,
        trust_profile: 1,
        event_class: 7,
        topic_id: 1,
        catalog_version: 1,
        qos: 1,
        ..Default::default()
    };
    let policy = policy(
        identity(4, 3),
        identity(3, 2),
        ProcessIdentity {
            pid: 42,
            uid: 1,
            gid: 1,
        },
    );
    let previous = ConnectConfig {
        endpoint: Endpoint::new("/tmp/qualified-md.sock".into(), &policy),
        bank_header: wire_bytes(&bank).unwrap(),
        catalog_hash: [0x22; 32],
        instrument_set_version: 1,
        instruments: vec![Instrument {
            id: 1001,
            version: 5,
            required_states: 1,
        }],
        cursor_id: [104; 16],
        subscription_generation: 1,
        request_id: [25; 16],
        maximum_levels: 32,
        startup_timeout_ms: 500,
        clock_generation: 1,
    };
    let mut next_template = previous.clone();
    bank.resource_id = [93; 16];
    bank.predecessor_ring_epoch = 1;
    bank.ring_epoch = 2;
    bank.feed_generation = 2;
    next_template.bank_header = wire_bytes(&bank).unwrap();
    next_template.cursor_id = [105; 16];
    next_template.request_id = [26; 16];
    next_template.subscription_generation = 2;
    next_template.instrument_set_version = 2;
    next_template.endpoint.control_epoch = 8;
    next_template.endpoint.nonce = [72; 32];
    next_template.instruments.push(Instrument {
        id: u64::MAX,
        version: 5,
        required_states: 1,
    });
    Approval {
        schema: 1,
        operation_id: [11; 16],
        approval_digest: [12; 32],
        revision: 2,
        previous,
        next_template,
        deadline_mono_ns: NOW + 1_000_000,
    }
}
pub fn commit(a: &Approval) -> Commit {
    let mut config = a.next_template.clone();
    let mut bank = ResourceHeader::decode(&config.bank_header).unwrap();
    bank.created_mono_ns = NOW;
    bank.cutover_seq = 6;
    bank.first_seq = 7;
    bank.descriptor_hash = tickwire_recovery::digest(&wire_bytes(&bank).unwrap());
    config.bank_header = wire_bytes(&bank).unwrap();
    Commit {
        schema: 1,
        operation_id: a.operation_id,
        approval_digest: a.approval_digest,
        revision: a.revision,
        config,
    }
}
pub fn sessions() -> (Session, Session) {
    let host = identity(6, 4);
    let worker = identity(4, 3);
    let process = ProcessIdentity {
        pid: 100,
        uid: 0,
        gid: 0,
    };
    let start =
        ClientHandshake::new(policy(worker.clone(), host.clone(), process), [1; 16]).unwrap();
    let (server, response) =
        Session::accept(&policy(host, worker, process), process, start.hello(), NOW).unwrap();
    (server, start.finish(process, &response, NOW).unwrap())
}
