//! Authenticated JOIN identities shared by MD, execution and qualified strategy SDKs.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tickwire_control::{Identity, Policy, ProcessIdentity};
use tickwire_ipc::{JoinBody, ResourceHeader, Wire};
use tickwire_join::TopicRegistry;
use tickwire_snapshot::InstrumentSpec;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityConfig {
    pub instance: [u8; 16],
    pub generation: u64,
    pub role: u16,
    pub logical_strategy: u64,
    pub host_boot: [u8; 16],
    pub namespace: [u8; 16],
    pub build_hash: [u8; 32],
    pub qualification_hash: [u8; 32],
    pub manifest_hash: [u8; 32],
    pub arch: u16,
    pub trust_profile: u8,
}
impl From<&Identity> for IdentityConfig {
    fn from(v: &Identity) -> Self {
        Self {
            instance: v.instance,
            generation: v.generation,
            role: v.role,
            logical_strategy: v.logical_strategy,
            host_boot: v.host_boot,
            namespace: v.namespace,
            build_hash: v.build_hash,
            qualification_hash: v.qualification_hash,
            manifest_hash: v.manifest_hash,
            arch: v.arch,
            trust_profile: v.trust_profile,
        }
    }
}
impl IdentityConfig {
    pub fn identity(&self) -> Identity {
        Identity {
            instance: self.instance,
            generation: self.generation,
            role: self.role,
            logical_strategy: self.logical_strategy,
            host_boot: self.host_boot,
            namespace: self.namespace,
            build_hash: self.build_hash,
            qualification_hash: self.qualification_hash,
            manifest_hash: self.manifest_hash,
            arch: self.arch,
            trust_profile: self.trust_profile,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    pub path: PathBuf,
    pub local: IdentityConfig,
    pub peer: IdentityConfig,
    pub peer_pid: u32,
    pub peer_uid: u32,
    pub peer_gid: u32,
    pub required_features: u64,
    pub supported_features: u64,
    pub control_epoch: u64,
    pub lease_duration_ns: u64,
    pub max_frame_bytes: u32,
    pub nonce: [u8; 32],
}
impl Endpoint {
    pub fn new(path: PathBuf, p: &Policy) -> Self {
        Self {
            path,
            local: (&p.local).into(),
            peer: (&p.peer).into(),
            peer_pid: p.peer_process.pid,
            peer_uid: p.peer_process.uid,
            peer_gid: p.peer_process.gid,
            required_features: p.required_features,
            supported_features: p.supported_features,
            control_epoch: p.control_epoch,
            lease_duration_ns: p.lease_duration_ns,
            max_frame_bytes: p.max_frame_bytes,
            nonce: p.nonce,
        }
    }
    pub fn policy(&self) -> Policy {
        Policy {
            local: self.local.identity(),
            peer: self.peer.identity(),
            peer_process: ProcessIdentity {
                pid: self.peer_pid,
                uid: self.peer_uid,
                gid: self.peer_gid,
            },
            required_features: self.required_features,
            supported_features: self.supported_features,
            control_epoch: self.control_epoch,
            lease_duration_ns: self.lease_duration_ns,
            max_frame_bytes: self.max_frame_bytes,
            nonce: self.nonce,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Instrument {
    pub id: u64,
    pub version: u64,
    pub required_states: u8,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinBootstrap {
    pub bank_header: Vec<u8>,
    pub snapshot_header: Vec<u8>,
    pub join: Vec<u8>,
    pub catalog_hash: [u8; 32],
    pub instrument_set_version: u64,
    pub instruments: Vec<Instrument>,
}
pub fn wire_bytes<T: Wire>(v: &T) -> Result<Vec<u8>> {
    let mut out = vec![0; T::SIZE];
    v.encode(&mut out)?;
    Ok(out)
}
impl JoinBootstrap {
    pub fn new(
        bank: &ResourceHeader,
        snapshot: &ResourceHeader,
        join: &JoinBody,
        registry: &TopicRegistry,
    ) -> Result<Self> {
        Ok(Self {
            bank_header: wire_bytes(bank)?,
            snapshot_header: wire_bytes(snapshot)?,
            join: wire_bytes(join)?,
            catalog_hash: registry.catalog_hash,
            instrument_set_version: registry.instrument_set_version,
            instruments: registry
                .instruments
                .iter()
                .map(|i| Instrument {
                    id: i.instrument_id,
                    version: i.instrument_version,
                    required_states: i.required_states,
                })
                .collect(),
        })
    }
    pub fn decode(&self) -> Result<(ResourceHeader, ResourceHeader, JoinBody, TopicRegistry)> {
        if self.instruments.is_empty() || self.instruments.len() > 4096 {
            return Err(Error::Bounds);
        }
        Ok((
            ResourceHeader::decode(&self.bank_header)?,
            ResourceHeader::decode(&self.snapshot_header)?,
            JoinBody::decode(&self.join)?,
            TopicRegistry {
                catalog_hash: self.catalog_hash,
                instrument_set_version: self.instrument_set_version,
                instruments: self
                    .instruments
                    .iter()
                    .map(|i| InstrumentSpec {
                        instrument_id: i.id,
                        instrument_version: i.version,
                        required_states: i.required_states,
                    })
                    .collect(),
            },
        ))
    }
}

use sha2::{Digest, Sha256};
use std::os::fd::OwnedFd;
use tickwire_control::Session;
use tickwire_control_sys::ReceivedFrame;
use tickwire_ipc::{ControlEnvelope, OfferPrefix, ResourceDescriptor, StatusBody};
use tickwire_shm::SubscriptionKey;
pub fn descriptor(h: &ResourceHeader, profile: u16) -> ResourceDescriptor {
    ResourceDescriptor {
        resource_id: h.resource_id,
        channel_id: h.channel_id,
        parent_resource_id: h.parent_resource_id,
        resource_bytes: h.resource_bytes,
        channel_epoch: h.channel_epoch,
        ring_epoch: h.ring_epoch,
        resource_kind: h.resource_kind,
        access: 1,
        fd_index: 0,
        seal_profile: profile,
        header_hash: h.descriptor_hash,
        writer_instance: h.producer_instance,
    }
}
pub fn offer_id(join: &JoinBody, kind: u8) -> [u8; 16] {
    let mut h = Sha256::new();
    h.update(b"tickwire.join-offer.v1");
    h.update([kind]);
    h.update(join.snapshot_id);
    h.update(join.cursor_resource_id);
    h.update(join.subscription_generation.to_le_bytes());
    h.finalize()[..16].try_into().unwrap()
}
pub fn resource_offer(
    session: &Session,
    h: &ResourceHeader,
    id: [u8; 16],
    expires: u64,
    membership: u64,
    immutable: bool,
) -> Result<Vec<u8>> {
    let mut body = wire_bytes(&OfferPrefix {
        offer_id: id,
        descriptor_count: 1,
        descriptor_bytes: 128,
        required_seals: if immutable { 15 } else { 23 },
        expires_mono_ns: expires,
        membership_version: membership,
    })?;
    body.extend(wire_bytes(&descriptor(h, if immutable { 2 } else { 1 }))?);
    Ok(session.encode(
        ControlEnvelope {
            opcode: 3,
            flags: 32,
            fd_count: 1,
            request_id: id,
            expected_state_version: membership,
            ..Default::default()
        },
        &body,
    )?)
}
// Independent peer, resource and FD proofs remain explicit at this boundary.
#[allow(clippy::too_many_arguments)]
pub fn accept_offer(
    session: &mut Session,
    packet: ReceivedFrame,
    h: &ResourceHeader,
    id: [u8; 16],
    expires: u64,
    membership: u64,
    immutable: bool,
    now: u64,
) -> Result<OwnedFd> {
    let (bytes, mut fds) = packet.into_parts();
    let frame = session.validate(&bytes, now)?;
    if frame.envelope.opcode != 3
        || frame.envelope.flags != 32
        || frame.envelope.fd_count != 1
        || fds.len() != 1
        || frame.envelope.expected_state_version != membership
    {
        return Err(Error::Identity);
    }
    let p = OfferPrefix::decode(&frame.body[..64])?;
    if p.offer_id != id
        || p.descriptor_count != 1
        || p.descriptor_bytes != 128
        || p.expires_mono_ns != expires
        || now >= expires
        || p.membership_version != membership
        || p.required_seals != if immutable { 15 } else { 23 }
        || ResourceDescriptor::decode(&frame.body[64..])?
            != descriptor(h, if immutable { 2 } else { 1 })
    {
        return Err(Error::Identity);
    }
    fds.pop().ok_or(Error::Identity)
}
pub fn cursor_header(bank: &ResourceHeader, sub: SubscriptionKey) -> Result<ResourceHeader> {
    let mut h = bank.clone();
    h.resource_kind = 4;
    h.resource_id = sub.cursor.to_le_bytes();
    h.parent_resource_id = bank.resource_id;
    h.producer_instance = sub.instance.to_le_bytes();
    h.producer_generation = sub.generation;
    h.consumer_instance = bank.producer_instance;
    h.capacity = 0;
    h.record_bytes = 0;
    h.resource_bytes = 8192;
    h.slot_offset = 0;
    h.producer_page_offset = 0;
    h.cursor_page_offset = 4096;
    h.max_consumers = 1;
    h.arena_offset = 0;
    h.arena_bytes = 0;
    h.arena_class_count = 0;
    h.class_descriptors = Default::default();
    h.descriptor_hash = [0; 32];
    h.descriptor_hash = Sha256::digest(wire_bytes(&h)?).into();
    Ok(h)
}
/// Result of one authenticated JOIN_READY attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivationStatus {
    Active {
        published: u64,
    },
    /// The pending baseline gate remains installed. Drain through this target,
    /// then retry JOIN_READY; publication may advance before the next attempt.
    CatchUp {
        published: u64,
    },
}

/// Parent replies only after JoinOwner::join_ready succeeds.
pub fn activation_ack(
    session: &Session,
    request: &ControlEnvelope,
    join: &JoinBody,
    published: u64,
) -> Result<Vec<u8>> {
    activation_reply(session, request, join, published, 10, 3)
}

/// Received, not Applied: a valid registered cursor still needs to catch up.
/// The caller must keep the subscription pending and preserve its baseline gate.
pub fn activation_pending(
    session: &Session,
    request: &ControlEnvelope,
    join: &JoinBody,
    published: u64,
) -> Result<Vec<u8>> {
    activation_reply(session, request, join, published, 6, 2)
}

fn activation_reply(
    session: &Session,
    request: &ControlEnvelope,
    join: &JoinBody,
    published: u64,
    flags: u32,
    state: u16,
) -> Result<Vec<u8>> {
    Ok(session.encode(
        ControlEnvelope {
            opcode: 49,
            flags,
            request_id: request.request_id,
            expected_state_version: request.expected_state_version,
            ..Default::default()
        },
        &wire_bytes(&StatusBody {
            subject_id: join.bank_resource_id,
            original_request_id: request.request_id,
            original_control_epoch: session.epoch(),
            state_version: join.membership_version,
            membership_version: join.membership_version,
            ring_epoch: join.ring_epoch,
            published_seq: published,
            state,
            ..Default::default()
        })?,
    )?)
}

pub fn activation_status(
    session: &mut Session,
    packet: ReceivedFrame,
    join: &JoinBody,
    id: [u8; 16],
    now: u64,
) -> Result<ActivationStatus> {
    if !packet.fds().is_empty() {
        return Err(Error::Identity);
    }
    activation_status_bytes(session, packet.bytes(), join, id, now)
}

fn activation_status_bytes(
    session: &mut Session,
    bytes: &[u8],
    join: &JoinBody,
    id: [u8; 16],
    now: u64,
) -> Result<ActivationStatus> {
    let f = session.validate(bytes, now)?;
    let s = StatusBody::decode(f.body)?;
    if f.envelope.opcode != 49
        || f.envelope.request_id != id
        || f.envelope.expected_state_version != join.membership_version
        || s.original_request_id != id
        || s.original_control_epoch != session.epoch()
        || s.subject_id != join.bank_resource_id
        || s.state_version != join.membership_version
        || s.membership_version != join.membership_version
        || s.ring_epoch != join.ring_epoch
        || s.published_seq < join.snapshot_seq
    {
        return Err(Error::Identity);
    }
    match (f.envelope.flags, s.state) {
        (10, 3) => Ok(ActivationStatus::Active {
            published: s.published_seq,
        }),
        (6, 2) => Ok(ActivationStatus::CatchUp {
            published: s.published_seq,
        }),
        _ => Err(Error::Identity),
    }
}

/// Compatibility helper for callers that require immediate activation.
pub fn activated(
    session: &mut Session,
    packet: ReceivedFrame,
    join: &JoinBody,
    id: [u8; 16],
    now: u64,
) -> Result<()> {
    require_active(activation_status(session, packet, join, id, now)?)
}

fn require_active(status: ActivationStatus) -> Result<()> {
    match status {
        ActivationStatus::Active { .. } => Ok(()),
        ActivationStatus::CatchUp { .. } => Err(Error::NotReady),
    }
}

#[cfg(test)]
mod activation_tests {
    use super::*;
    use tickwire_control::ClientHandshake;
    use tickwire_ipc::ControlFrame;

    fn identity(id: u8, role: u16) -> Identity {
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
            arch: 2,
            trust_profile: 1,
        }
    }
    fn policy(server: bool) -> Policy {
        Policy {
            local: identity(if server { 1 } else { 2 }, if server { 1 } else { 3 }),
            peer: identity(if server { 2 } else { 1 }, if server { 3 } else { 1 }),
            peer_process: ProcessIdentity {
                pid: if server { 200 } else { 100 },
                uid: 501,
                gid: 20,
            },
            required_features: 0x1c,
            supported_features: 0x1f,
            control_epoch: 13,
            lease_duration_ns: 3000,
            max_frame_bytes: 65536,
            nonce: [14; 32],
        }
    }
    fn connected() -> (Session, Session) {
        let p = policy(true);
        let handshake = ClientHandshake::new(policy(false), [15; 16]).unwrap();
        let (server, response) =
            Session::accept(&p, p.peer_process, handshake.hello(), 100).unwrap();
        let client = handshake
            .finish(policy(false).peer_process, &response, 101)
            .unwrap();
        (server, client)
    }
    fn join() -> JoinBody {
        JoinBody {
            bank_resource_id: [20; 16],
            membership_version: 2,
            ring_epoch: 3,
            snapshot_seq: 64,
            ..Default::default()
        }
    }
    fn request() -> ControlEnvelope {
        ControlEnvelope {
            opcode: 17,
            flags: 1,
            request_id: [21; 16],
            expected_state_version: 2,
            ..Default::default()
        }
    }

    #[test]
    fn received_catchup_can_repeat_before_applied_activation() {
        let (server, mut client) = connected();
        for published in [65, 67] {
            let bytes = activation_pending(&server, &request(), &join(), published).unwrap();
            let status =
                activation_status_bytes(&mut client, &bytes, &join(), [21; 16], 102).unwrap();
            assert_eq!(status, ActivationStatus::CatchUp { published });
            assert!(matches!(require_active(status), Err(Error::NotReady)));
            assert_eq!(ControlFrame::decode(&bytes).unwrap().envelope.flags, 6);
        }
        let bytes = activation_ack(&server, &request(), &join(), 67).unwrap();
        let status = activation_status_bytes(&mut client, &bytes, &join(), [21; 16], 103).unwrap();
        assert_eq!(status, ActivationStatus::Active { published: 67 });
        require_active(status).unwrap();
    }

    #[test]
    fn activation_rejects_wrong_binding_or_completion_class() {
        for pending in [false, true] {
            for mutation in 0..10 {
                let (server, mut client) = connected();
                let bytes = if pending {
                    activation_pending(&server, &request(), &join(), 65)
                } else {
                    activation_ack(&server, &request(), &join(), 65)
                }
                .unwrap();
                let frame = ControlFrame::decode(&bytes).unwrap();
                let mut envelope = frame.envelope;
                let mut body = StatusBody::decode(frame.body).unwrap();
                match mutation {
                    0 => envelope.flags = if pending { 10 } else { 6 },
                    1 => envelope.request_id = [22; 16],
                    2 => envelope.expected_state_version += 1,
                    3 => body.original_request_id = [22; 16],
                    4 => body.original_control_epoch += 1,
                    5 => body.subject_id = [22; 16],
                    6 => body.state_version += 1,
                    7 => body.membership_version += 1,
                    8 => body.ring_epoch += 1,
                    _ => body.published_seq = 63,
                }
                let bytes = server
                    .encode(envelope, &wire_bytes(&body).unwrap())
                    .unwrap();
                assert!(
                    activation_status_bytes(&mut client, &bytes, &join(), [21; 16], 102).is_err(),
                    "pending={pending}, mutation={mutation}"
                );
            }
        }
    }

    #[test]
    fn catchup_response_does_not_extend_the_session_lease() {
        let (server, mut client) = connected();
        let bytes = activation_pending(&server, &request(), &join(), 65).unwrap();
        assert!(activation_status_bytes(&mut client, &bytes, &join(), [21; 16], 3100).is_err());
    }
}
