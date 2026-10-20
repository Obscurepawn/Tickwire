#![allow(dead_code)]

use sha2::{Digest, Sha256};
use tickwire_control::*;
use tickwire_ipc::*;
use tickwire_join::*;
use tickwire_shm::{Publisher, SubscriptionKey};
use tickwire_snapshot::*;

pub const NOW: u64 = 1_500_000_000;
pub const DEADLINE: u64 = 2_000_000_000;
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
pub fn policy(local: Identity, peer: Identity, process: ProcessIdentity, now: u64) -> Policy {
    Policy {
        local,
        peer,
        peer_process: process,
        required_features: 0x1c,
        supported_features: 0x1f,
        control_epoch: 7,
        lease_duration_ns: 3_000_000_000,
        max_frame_bytes: 65536,
        nonce: Sha256::digest(now.to_le_bytes()).into(),
    }
}
pub fn session_pair(peer: Identity) -> (Session, Session) {
    let owner = identity(3, 2);
    let process = ProcessIdentity {
        pid: 100,
        uid: 0,
        gid: 0,
    };
    let server_policy = policy(owner.clone(), peer.clone(), process, NOW);
    let client_policy = policy(peer, owner, process, NOW);
    let start = ClientHandshake::new(client_policy, [1; 16]).unwrap();
    let (server, response) = Session::accept(&server_policy, process, start.hello(), NOW).unwrap();
    let client = start.finish(process, &response, NOW).unwrap();
    (server, client)
}
pub fn header() -> ResourceHeader {
    ResourceHeader {
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
        required_features: 0x1c,
        trust_profile: 1,
        event_class: 7,
        topic_id: 1,
        catalog_version: 1,
        qos: 1,
        ..Default::default()
    }
}
pub fn registry() -> TopicRegistry {
    TopicRegistry {
        catalog_hash: [0x22; 32],
        instrument_set_version: 1,
        instruments: vec![InstrumentSpec {
            instrument_id: 1001,
            instrument_version: 5,
            required_states: QUOTE | TRADE | MARK_FUNDING,
        }],
    }
}
pub struct Data {
    pub states: Vec<MarketRecord>,
    pub bids: Vec<DepthLevel>,
    pub asks: Vec<DepthLevel>,
}
impl Data {
    pub fn new() -> Self {
        let vectors: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../specs/contracts/ipc-vectors-v1.json"
        ))
        .unwrap();
        let get = |name| {
            let hex = vectors["vectors"]
                .as_array()
                .unwrap()
                .iter()
                .find(|v| v["name"] == name)
                .unwrap()["hex"]
                .as_str()
                .unwrap();
            (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect::<Vec<_>>()
        };
        let raw = get("normalized_topic_snapshot");
        let states = [8576, 9088, 9600, 10112]
            .into_iter()
            .enumerate()
            .map(|(i, offset)| {
                let mut r = MarketRecord::decode(&raw[offset..offset + 512]).unwrap();
                r.header.sequence = i as u64 + 1;
                r.header.topic_or_scope_id = 1;
                r.header.producer_generation = 1;
                r.header.feed_generation = 1;
                r
            })
            .collect();
        Self {
            states,
            bids: vec![DepthLevel {
                price_ticks: 100,
                qty_lots: 2,
            }],
            asks: vec![DepthLevel {
                price_ticks: 101,
                qty_lots: 3,
            }],
        }
    }
    pub fn record(&self, seq: u64) -> [u8; 512] {
        let mut r = self.states[0].clone();
        r.header.sequence = seq;
        let mut out = [0; 512];
        r.encode(&mut out).unwrap();
        out
    }
    pub fn build(&self, plan: &JoinPlan) -> tickwire_ipc::Result<BuiltTopic> {
        self.build_at(plan, NOW, DEADLINE)
    }
    pub fn build_at(
        &self,
        plan: &JoinPlan,
        now: u64,
        deadline: u64,
    ) -> tickwire_ipc::Result<BuiltTopic> {
        let members = [InstrumentSnapshot {
            instrument_id: 1001,
            instrument_version: 5,
            snapshot_id: [32; 16],
            at_sequence: plan.sequence,
            last_event_sequence: 4,
            feed_generation: 1,
            catalog_version: 1,
            catalog_hash: [0x22; 32],
            created_mono_ns: now,
            expires_mono_ns: deadline,
            state_version: 1,
            coverage_depth: 10,
            coverage_flags: 3,
            states: &self.states,
            bids: &self.bids,
            asks: &self.asks,
        }];
        build_topic(&BuildRequest {
            resource: &plan.resource,
            join: &plan.join,
            catalog_hash: [0x22; 32],
            instrument_set_version: 1,
            sequence: plan.sequence,
            created_mono_ns: now,
            expires_mono_ns: deadline,
            now_mono_ns: now,
            logical_strategy_id: 7,
            instruments: &registry().instruments,
            members: &members,
            byte_limit: 64 * 1024 * 1024,
        })
    }
}
pub fn owner() -> (JoinOwner<4>, Data) {
    let data = Data::new();
    let mut publisher = Publisher::<4>::create(header()).unwrap();
    for seq in 1..=4 {
        publisher.publish(&[&data.record(seq)]).unwrap();
    }
    (JoinOwner::new(publisher, registry()).unwrap(), data)
}
pub fn registration(owner: &JoinOwner<4>, id: u8) -> SubscriberRegistration {
    let h = owner.header();
    SubscriberRegistration {
        identity: identity(id, 3),
        subscription: SubscriptionKey {
            bank: tickwire_shm::BankKey {
                resource: u128::from_le_bytes(h.resource_id),
                channel: u128::from_le_bytes(h.channel_id),
                channel_epoch: h.channel_epoch,
                ring_epoch: h.ring_epoch,
                feed_generation: h.feed_generation,
            },
            instance: u128::from_le_bytes([id; 16]),
            generation: 1,
            cursor: u128::from_le_bytes([id + 100; 16]),
        },
        qos: 1,
    }
}
pub fn join_request(owner: &JoinOwner<4>, reg: &SubscriberRegistration) -> JoinBody {
    let h = owner.header();
    let mut set = Sha256::new();
    for i in &registry().instruments {
        set.update(i.instrument_id.to_le_bytes());
        set.update(i.instrument_version.to_le_bytes());
    }
    JoinBody {
        subscriber_instance: reg.identity.instance,
        logical_strategy_id: 7,
        subscription_generation: reg.subscription.generation,
        topic_id: h.topic_id,
        channel_id: h.channel_id,
        channel_epoch: h.channel_epoch,
        feed_generation: h.feed_generation,
        ring_epoch: h.ring_epoch,
        cursor_resource_id: reg.subscription.cursor.to_le_bytes(),
        bank_resource_id: h.resource_id,
        readiness_deadline_mono_ns: DEADLINE,
        expected_catalog_version: h.catalog_version,
        qos: 1,
        role: 3,
        join_flags: 3,
        topic_instrument_set_version: 1,
        topic_instrument_set_hash: set.finalize().into(),
        ..Default::default()
    }
}
pub fn request(session: &Session, body: &JoinBody, id: u8, version: u64) -> Vec<u8> {
    frame(session, 16, 1, id, version, body)
}
pub fn frame<T: Wire>(
    session: &Session,
    opcode: u16,
    flags: u32,
    id: u8,
    version: u64,
    body: &T,
) -> Vec<u8> {
    let mut bytes = vec![0; T::SIZE];
    body.encode(&mut bytes).unwrap();
    session
        .encode(
            ControlEnvelope {
                opcode,
                flags,
                request_id: [id; 16],
                expected_state_version: version,
                ..Default::default()
            },
            &bytes,
        )
        .unwrap()
}
pub fn fill_pending(owner: &mut JoinOwner<4>, data: &Data) {
    for seq in 5..=68 {
        owner.publish(&[&data.record(seq)]).unwrap();
    }
}
pub fn assert_full(owner: &mut JoinOwner<4>, data: &Data) {
    assert!(matches!(
        owner.publish(&[&data.record(69)]),
        Err(tickwire_join::Error::Ring(
            tickwire_shm::MappedError::Protocol(tickwire_shm::Error::Full)
        ))
    ));
}
