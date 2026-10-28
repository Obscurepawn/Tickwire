use crate::{Result, config, invalid};
use serde::{Deserialize, Serialize};
use std::{
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tickwire_control::{ClientHandshake, Policy, ProcessIdentity, Session};
use tickwire_control_sys::{Connection, ControlListener};
use tickwire_ipc::{ControlEnvelope, JoinBody, Wire};
use tickwire_join::{PrepareOutcome, SubscriberRegistration};
use tickwire_market_source::{
    ConnectConfig,
    protocol::{IdentityConfig, activation_ack, activation_pending, wire_bytes},
};
use tickwire_shm::SubscriptionKey;
pub type TopicOwner = tickwire_market_publisher::TopicPublisher<32>;
mod expansion;
#[cfg(all(test, target_os = "linux"))]
mod expansion_tests;
#[cfg(all(test, target_os = "linux"))]
mod runtime_tests;
/// Written by the trusted launcher only after it holds the actual process identity.
/// This file authorizes one qualified principal and exact topic/subscription, not arbitrary HELLOs.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Admission {
    pub client: ConnectConfig,
    pub pid: u32,
    pub uid: u32,
    pub gid: u32,
    pub supervisor: IdentityConfig,
    pub supervisor_pid: u32,
    pub snapshot_id: [u8; 16],
}
/// HELLO has epoch zero; its request ID identifies the exact pending admission.
/// This only selects a file. The authenticated process and both identities still
/// have to match its contents before a control session can be established.
pub fn admission_path(directory: &Path, pid: u32, request_id: [u8; 16]) -> Result<PathBuf> {
    if pid == 0 || request_id == [0; 16] {
        return Err(invalid("MD admission key"));
    }
    let request = request_id
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    Ok(directory.join(format!("{pid}-{request}.json")))
}
fn same_admission(old: &Admission, new: &Admission) -> Result<()> {
    if serde_json::to_vec(old)? != serde_json::to_vec(new)? {
        return Err(invalid("MD admission identity reused"));
    }
    Ok(())
}
fn check_hello(
    admission: &Admission,
    envelope: &ControlEnvelope,
    actual: ProcessIdentity,
    server: &IdentityConfig,
    server_pid: u32,
) -> Result<()> {
    if envelope.opcode != 1
        || envelope.flags != 1
        || envelope.control_epoch != 0
        || envelope.request_id != admission.client.request_id
        || envelope.sender_instance != admission.client.endpoint.local.instance
        || envelope.target_instance != admission.client.endpoint.peer.instance
        || admission.pid != actual.pid
        || admission.uid != actual.uid
        || admission.gid != actual.gid
        || admission.client.endpoint.peer.identity() != server.identity()
        || admission.client.endpoint.peer_pid != server_pid
    {
        return Err(invalid("admitted process identity"));
    }
    Ok(())
}
pub fn register_admission(directory: &Path, admission: &Admission) -> Result<PathBuf> {
    let meta = std::fs::symlink_metadata(directory)?;
    if !meta.is_dir()
        || meta.mode() & 0o077 != 0
        || admission.pid == 0
        || admission.supervisor_pid == 0
        || admission.snapshot_id == [0; 16]
        || admission.supervisor.role != 1
    {
        return Err(invalid("MD admission authority"));
    }
    let path = admission_path(directory, admission.pid, admission.client.request_id)?;
    if path.exists() {
        let old: Admission = serde_json::from_slice(&config::read(&path, 2 * 1024 * 1024)?)?;
        same_admission(&old, admission)?;
        return Ok(path);
    }
    config::write(&path, admission)?;
    Ok(path)
}
fn policy(a: &Admission) -> Policy {
    let mut p = a.client.endpoint.policy();
    std::mem::swap(&mut p.local, &mut p.peer);
    p.peer_process = ProcessIdentity {
        pid: a.pid,
        uid: a.uid,
        gid: a.gid,
    };
    p
}
fn supervisor_session(a: &Admission, now: u64) -> Result<(Session, Session)> {
    // The private admission is the supervisor's authorization. These local
    // logical sessions submit that already-approved exact request to JoinOwner's
    // durable idempotency reducer; they do not authenticate an unknown process.
    let mut client = a.client.endpoint.policy();
    client.local = a.supervisor.identity();
    client.peer = a.client.endpoint.peer.identity();
    client.nonce[0] ^= 0x80;
    let md = ProcessIdentity {
        pid: std::process::id(),
        uid: a.client.endpoint.peer_uid,
        gid: a.client.endpoint.peer_gid,
    };
    client.peer_process = md;
    let actual = ProcessIdentity {
        pid: a.supervisor_pid,
        uid: a.uid,
        gid: a.gid,
    };
    let mut server = client.clone();
    std::mem::swap(&mut server.local, &mut server.peer);
    server.peer_process = actual;
    let hello = ClientHandshake::new(client, a.client.request_id)?;
    let (mut s, response) = Session::accept(&server, actual, hello.hello(), now)?;
    let c = hello.finish(md, &response, now)?;
    let _ = &mut s;
    Ok((c, s))
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Hello,
    Request,
    Waiting,
    Cursor,
    Readies,
    Ready,
    Active,
    QuiesceQueued,
    Quiescing,
    Detached,
    Rejected,
}
struct Peer {
    connection: Connection,
    session: Option<Session>,
    admission: Option<Admission>,
    topic: Option<usize>,
    subscription: Option<SubscriptionKey>,
    stage: Stage,
    request: Option<Vec<u8>>,
    ready_count: usize,
    departure: Option<ControlEnvelope>,
    outgoing: Option<Vec<u8>>,
    expansion: Option<[u8; 16]>,
    quiesced_applied: Option<u64>,
    activated_at: Option<u64>,
    deadline: Instant,
}
pub struct Server {
    listener: ControlListener,
    admissions: PathBuf,
    identity: IdentityConfig,
    peers: Vec<Peer>,
    requests: tickwire_control_store::DurableRequests<tickwire_journal::store::FileStore>,
    expansions: Vec<expansion::Expansion>,
}
fn requires_rebuild_on_remove(stage: Stage) -> bool {
    !matches!(
        stage,
        Stage::Hello | Stage::Request | Stage::Detached | Stage::Rejected
    )
}

impl Server {
    pub fn new(c: &config::Config) -> Result<Self> {
        if c.identity.role != 2 || !c.socket.is_absolute() || !c.admission_directory.is_absolute() {
            return Err(invalid("MD server configuration"));
        }
        let m = std::fs::symlink_metadata(&c.admission_directory)?;
        if !m.is_dir() || m.mode() & 0o077 != 0 {
            return Err(invalid("MD admission directory permissions"));
        }
        let store = tickwire_journal::store::FileStore::open(&c.control_directory)?;
        let cfg = tickwire_control_store::Config {
            physical: tickwire_journal::Config::new(c.control_stream, c.control_domain),
            writer_epoch: c.control_writer_epoch,
            owner: c.identity.instance,
            max_requests: 8192,
            max_pending: 128,
            max_response_bytes: 65536,
            max_retained_bytes: 64 * 1024 * 1024,
        };
        let requests = if c.control_directory.join("MANIFEST.json").exists() {
            tickwire_control_store::DurableRequests::recover(store, cfg, None)?
        } else {
            tickwire_control_store::DurableRequests::create(store, cfg)?
        };
        Ok(Self {
            listener: ControlListener::bind(&c.socket)?,
            admissions: c.admission_directory.clone(),
            identity: c.identity.clone(),
            peers: vec![],
            requests,
            expansions: Vec::new(),
        })
    }
    pub fn poll(&mut self, topics: &mut [TopicOwner], now: u64) -> Result<()> {
        if self.peers.len() < 128 {
            match self.listener.try_accept() {
                Ok(Some(connection)) => self.peers.push(Peer {
                    connection,
                    session: None,
                    admission: None,
                    topic: None,
                    subscription: None,
                    stage: Stage::Hello,
                    request: None,
                    ready_count: 0,
                    departure: None,
                    outgoing: None,
                    expansion: None,
                    quiesced_applied: None,
                    activated_at: None,
                    deadline: Instant::now() + Duration::from_secs(60),
                }),
                Ok(None) => {}
                Err(e) => return Err(e.into()),
            }
        }
        let mut i = 0;
        while i < self.peers.len() {
            let result = self.drive(i, topics, now);
            if matches!(self.peers[i].stage, Stage::Detached | Stage::Rejected) {
                let peer = self.peers.remove(i);
                if let (Some(topic), Some(operation), Some(applied), Some(admission)) = (
                    peer.topic,
                    peer.expansion,
                    peer.quiesced_applied,
                    peer.admission,
                ) {
                    self.record_quiescence(topic, operation, &admission, applied);
                }
                continue;
            }
            if let Err(e) = result {
                eprintln!(
                    "{}",
                    serde_json::json!({"event":"md_subscriber_closed","error":e.to_string(),"stage":format!("{:?}", self.peers[i].stage),"pid":self.peers[i].connection.peer_credentials().pid})
                );
                let peer = self.peers.remove(i);
                if let (Some(topic), Some(operation)) = (peer.topic, peer.expansion) {
                    self.fail_expansion(topic, operation);
                }
                if let Some(t) = peer
                    .topic
                    .filter(|_| requires_rebuild_on_remove(peer.stage))
                {
                    topics[t].require_rebuild();
                }
            } else {
                i += 1;
            }
        }
        Ok(())
    }
    fn drive(&mut self, index: usize, topics: &mut [TopicOwner], now: u64) -> Result<()> {
        let blocked = self.peers[index]
            .topic
            .is_some_and(|topic| self.blocked(topic));
        let p = &mut self.peers[index];
        if Instant::now() >= p.deadline && p.stage != Stage::Active {
            return Err(invalid("JOIN timeout"));
        }
        if p.session.as_ref().is_some_and(|s| now >= s.deadline()) {
            return Err(invalid("control lease expired"));
        }
        if blocked && matches!(p.stage, Stage::Request | Stage::Waiting) {
            p.stage = Stage::Rejected;
            return Err(invalid("topic JOIN paused for expansion"));
        }
        if p.stage == Stage::QuiesceQueued {
            let frame = p
                .outgoing
                .as_ref()
                .ok_or_else(|| invalid("missing expansion outbox"))?;
            match p.connection.try_send(frame, &[]) {
                Ok(true) => {
                    p.outgoing = None;
                    p.stage = Stage::Quiescing;
                }
                Ok(false) => {}
                Err(tickwire_control_sys::Error::System { source, .. })
                    if source.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error.into()),
            }
            return Ok(());
        }
        if p.stage == Stage::Waiting {
            let t = p.topic.ok_or_else(|| invalid("missing topic"))?;
            if !topics[t].state().live() || topics[t].needs_bank() {
                return Ok(());
            }
            let a = p
                .admission
                .as_ref()
                .ok_or_else(|| invalid("missing admission"))?;
            let bytes = p.request.as_ref().ok_or_else(|| invalid("missing JOIN"))?;
            let request = JoinBody::decode(&bytes[128..])?;
            let (supervisor, mut control) = supervisor_session(a, now)?;
            let (owner, state) = topics[t].split();
            let sub = SubscriptionKey {
                bank: tickwire_shm::BankKey {
                    resource: u128::from_le_bytes(owner.header().resource_id),
                    channel: u128::from_le_bytes(owner.header().channel_id),
                    channel_epoch: owner.header().channel_epoch,
                    ring_epoch: owner.header().ring_epoch,
                    feed_generation: owner.header().feed_generation,
                },
                instance: u128::from_le_bytes(a.client.endpoint.local.instance),
                generation: a.client.subscription_generation,
                cursor: u128::from_le_bytes(a.client.cursor_id),
            };
            let registration = SubscriberRegistration {
                identity: a.client.endpoint.local.identity(),
                subscription: sub,
                qos: owner.header().qos,
            };
            let frame = supervisor.encode(
                ControlEnvelope {
                    opcode: 16,
                    flags: 1,
                    request_id: a.client.request_id,
                    expected_state_version: owner.membership_version(),
                    ..Default::default()
                },
                &wire_bytes(&request)?,
            )?;
            let outcome = owner.prepare(
                &mut control,
                &mut self.requests,
                &frame,
                &registration,
                a.snapshot_id,
                now,
                |plan| state.snapshot(plan, now, request.readiness_deadline_mono_ns),
            )?;
            if !matches!(outcome, PrepareOutcome::Prepared { .. }) {
                return Err(invalid("JOIN request already consumed"));
            }
            let session = p
                .session
                .as_mut()
                .ok_or_else(|| invalid("missing session"))?;
            let reply = session.encode(
                ControlEnvelope {
                    opcode: 16,
                    flags: 10,
                    request_id: a.client.request_id,
                    expected_state_version: owner.membership_version(),
                    ..Default::default()
                },
                &wire_bytes(owner.join(sub)?)?,
            )?;
            p.connection.send(&reply, &[], p.deadline)?;
            owner.deliver(sub, session, &mut p.connection, now, p.deadline)?;
            p.subscription = Some(sub);
            p.stage = Stage::Cursor;
            return Ok(());
        }
        let packet = match p.connection.try_recv() {
            Ok(Some(v)) => v,
            Ok(None) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        if p.stage == Stage::Hello {
            if !packet.fds().is_empty() {
                return Err(invalid("HELLO descriptors"));
            }
            let h = ControlEnvelope::decode(
                packet
                    .bytes()
                    .get(..128)
                    .ok_or_else(|| invalid("HELLO length"))?,
            )?;
            let actual = p.connection.peer_credentials();
            let path = admission_path(&self.admissions, actual.pid, h.request_id)?;
            let a: Admission = serde_json::from_slice(&config::read(&path, 2 * 1024 * 1024)?)?;
            check_hello(
                &a,
                &h,
                ProcessIdentity {
                    pid: actual.pid,
                    uid: actual.uid,
                    gid: actual.gid,
                },
                &self.identity,
                std::process::id(),
            )?;
            let expected = tickwire_ipc::ResourceHeader::decode(&a.client.bank_header)?;
            let topic = topics
                .iter_mut()
                .position(|topic| topic.split().0.header().resource_id == expected.resource_id)
                .ok_or_else(|| invalid("unknown admitted bank"))?;
            if self.expansions.iter().any(|e| e.blocks(topic)) {
                p.stage = Stage::Rejected;
                return Err(invalid("topic JOIN paused for expansion"));
            }
            let (session, welcome) = Session::accept(
                &policy(&a),
                ProcessIdentity {
                    pid: actual.pid,
                    uid: actual.uid,
                    gid: actual.gid,
                },
                packet.bytes(),
                now,
            )?;
            p.connection.send(&welcome, &[], p.deadline)?;
            p.topic = Some(topic);
            p.session = Some(session);
            p.admission = Some(a);
            p.stage = Stage::Request;
            return Ok(());
        }
        let a = p.admission.as_ref().ok_or_else(|| invalid("admission"))?;
        let session = p.session.as_mut().ok_or_else(|| invalid("session"))?;
        if p.stage == Stage::Request {
            if !packet.fds().is_empty() {
                return Err(invalid("JOIN descriptors"));
            }
            let frame = session.validate(packet.bytes(), now)?;
            if frame.envelope.opcode != 16
                || frame.envelope.flags != 1
                || frame.envelope.request_id != a.client.request_id
            {
                return Err(invalid("JOIN frame"));
            }
            let j = JoinBody::decode(frame.body)?;
            let expected = tickwire_ipc::ResourceHeader::decode(&a.client.bank_header)?;
            if j.subscriber_instance != a.client.endpoint.local.instance
                || j.logical_strategy_id != a.client.endpoint.local.logical_strategy
                || j.role != a.client.endpoint.local.role
                || j.subscription_generation != a.client.subscription_generation
                || j.cursor_resource_id != a.client.cursor_id
                || j.bank_resource_id != expected.resource_id
            {
                return Err(invalid("JOIN binding"));
            }
            let t = topics
                .iter_mut()
                .position(|topic| topic.split().0.header().resource_id == j.bank_resource_id)
                .ok_or_else(|| invalid("unknown bank"))?;
            p.topic = Some(t);
            p.request = Some(packet.bytes().to_vec());
            p.stage = Stage::Waiting;
            return Ok(());
        }
        let t = p.topic.ok_or_else(|| invalid("topic"))?;
        let sub = p.subscription.ok_or_else(|| invalid("subscription"))?;
        let (owner, _) = topics[t].split();
        match p.stage {
            Stage::Cursor => {
                // SAFETY: private supervisor admission bound this exact actual
                // process and qualified build before its authenticated HELLO.
                unsafe {
                    owner.receive_cursor(sub, session, packet, now)?;
                }
                p.stage = Stage::Readies;
            }
            Stage::Readies => {
                if !packet.fds().is_empty() {
                    return Err(invalid("READ_READY fds"));
                }
                owner.read_ready(sub, session, packet.bytes(), now)?;
                p.ready_count += 1;
                if p.ready_count == 2 {
                    p.stage = Stage::Ready;
                }
            }
            Stage::Ready => {
                if !packet.fds().is_empty() {
                    return Err(invalid("JOIN_READY fds"));
                }
                let h = ControlEnvelope::decode(&packet.bytes()[..128])?;
                match owner.join_ready(sub, session, packet.bytes(), now) {
                    Ok(_) => {}
                    Err(tickwire_join::Error::Ring(tickwire_shm::MappedError::Protocol(
                        tickwire_shm::Error::Cursor,
                    ))) => {
                        if let Some(target) = owner.catchup_target(sub)? {
                            let reply = activation_pending(session, &h, owner.join(sub)?, target)?;
                            p.connection.send(&reply, &[], p.deadline)?;
                            return Ok(());
                        }
                        // The cursor may have caught up while the failed attempt
                        // was being checked. Activation still requires its real transition.
                        owner.join_ready(sub, session, packet.bytes(), now)?;
                    }
                    Err(error) => return Err(error.into()),
                }
                let ack = activation_ack(session, &h, owner.join(sub)?, owner.published())?;
                p.connection.send(&ack, &[], p.deadline)?;
                p.stage = Stage::Active;
                p.activated_at = Some(owner.published());
            }
            Stage::Active => {
                if !packet.fds().is_empty() {
                    return Err(invalid("DETACH descriptors"));
                }
                let frame = session.validate(packet.bytes(), now)?;
                tickwire_market_source::retirement::verify_request(
                    &frame.envelope,
                    frame.body,
                    owner.join(sub)?,
                )?;
                let request = frame.envelope.clone();
                let response = owner.quiesce(sub, session, request.request_id, 2)?;
                p.connection
                    .send(&response, &[], Instant::now() + Duration::from_secs(1))?;
                p.departure = Some(request);
                p.deadline = Instant::now() + Duration::from_secs(5);
                p.stage = Stage::Quiescing;
            }
            Stage::Quiescing => {
                if !packet.fds().is_empty() {
                    return Err(invalid("QUIESCED fds"));
                }
                owner.quiesced(sub, session, packet.bytes(), now)?;
                p.quiesced_applied =
                    Some(tickwire_ipc::QuiesceBody::decode(&packet.bytes()[128..])?.applied_seq);
                // Only this successful mapped-cursor transition permits removing
                // one peer without invalidating everyone on the same topic.
                p.stage = Stage::Detached;
                if let Some(request) = &p.departure {
                    let proof = tickwire_ipc::QuiesceBody::decode(&packet.bytes()[128..])?;
                    let ack =
                        tickwire_market_source::retirement::applied(session, request, &proof)?;
                    p.connection
                        .send(&ack, &[], Instant::now() + Duration::from_secs(1))?;
                }
            }
            _ => return Err(invalid("unexpected subscriber frame")),
        }
        Ok(())
    }
    /// A continuity fault is sent over control even if the old market bank is full.
    pub fn invalidate_topic(&mut self, topic: usize, owner: &mut TopicOwner) -> Result<()> {
        for p in &mut self.peers {
            if p.topic == Some(topic) && p.stage == Stage::Active {
                let sub = p.subscription.ok_or_else(|| invalid("subscription"))?;
                let session = p.session.as_ref().ok_or_else(|| invalid("session"))?;
                let frame = owner.split().0.quiesce(sub, session, [20; 16], 1)?;
                if p.connection
                    .send(&frame, &[], Instant::now() + Duration::from_millis(1))
                    .is_err()
                {
                    p.deadline = Instant::now();
                }
                p.stage = Stage::Quiescing;
            }
        }
        Ok(())
    }
    pub fn has_peers(&self, topic: usize) -> bool {
        self.peers.iter().any(|p| p.topic == Some(topic))
    }
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    use tickwire_market_source::protocol::Endpoint;

    fn admission() -> Admission {
        let identity = |id, role, logical_strategy| IdentityConfig {
            instance: [id; 16],
            generation: 1,
            role,
            logical_strategy,
            host_boot: [4; 16],
            namespace: [5; 16],
            build_hash: [6; 32],
            qualification_hash: [7; 32],
            manifest_hash: [8; 32],
            arch: 2,
            trust_profile: 1,
        };
        Admission {
            client: ConnectConfig {
                endpoint: Endpoint {
                    path: "/private/md.sock".into(),
                    local: identity(2, 3, 8),
                    peer: identity(3, 2, 0),
                    peer_pid: 77,
                    peer_uid: 10,
                    peer_gid: 20,
                    required_features: 0x1c,
                    supported_features: 511,
                    control_epoch: 9,
                    lease_duration_ns: 1000,
                    max_frame_bytes: 65536,
                    nonce: [11; 32],
                },
                bank_header: vec![],
                catalog_hash: [12; 32],
                instrument_set_version: 1,
                instruments: vec![],
                cursor_id: [13; 16],
                subscription_generation: 1,
                request_id: [14; 16],
                maximum_levels: 100,
                startup_timeout_ms: 30000,
                clock_generation: 1,
            },
            pid: 42,
            uid: 10,
            gid: 20,
            supervisor: identity(1, 1, 0),
            supervisor_pid: 41,
            snapshot_id: [15; 16],
        }
    }

    #[test]
    fn epoch_zero_hello_selects_its_admission_and_completes_real_handshake() {
        let a = admission();
        let hello = ClientHandshake::new(a.client.endpoint.policy(), a.client.request_id).unwrap();
        let envelope = ControlEnvelope::decode(&hello.hello()[..128]).unwrap();
        assert_eq!(envelope.control_epoch, 0);
        let directory = Path::new("/private/admissions");
        assert_eq!(
            admission_path(directory, a.pid, envelope.request_id).unwrap(),
            admission_path(directory, a.pid, a.client.request_id).unwrap(),
        );
        let actual = ProcessIdentity {
            pid: a.pid,
            uid: a.uid,
            gid: a.gid,
        };
        check_hello(&a, &envelope, actual, &a.client.endpoint.peer, 77).unwrap();
        let (server, welcome) = Session::accept(&policy(&a), actual, hello.hello(), 1).unwrap();
        let client = hello
            .finish(
                ProcessIdentity {
                    pid: 77,
                    uid: 10,
                    gid: 20,
                },
                &welcome,
                1,
            )
            .unwrap();
        assert_eq!(server.epoch(), 9);
        assert_eq!(client.epoch(), 9);
    }

    #[test]
    fn one_lookup_key_cannot_select_two_different_admissions() {
        let a = admission();
        let mut other = a.clone();
        other.client.endpoint.control_epoch += 1;
        assert_eq!(
            admission_path(Path::new("/private"), a.pid, a.client.request_id).unwrap(),
            admission_path(Path::new("/private"), other.pid, other.client.request_id).unwrap(),
        );
        same_admission(&a, &a).unwrap();
        assert!(same_admission(&a, &other).is_err());
    }

    #[test]
    fn request_key_does_not_replace_peer_and_sender_identity_checks() {
        let a = admission();
        let hello = ClientHandshake::new(a.client.endpoint.policy(), a.client.request_id).unwrap();
        let envelope = ControlEnvelope::decode(&hello.hello()[..128]).unwrap();
        let actual = ProcessIdentity {
            pid: a.pid,
            uid: a.uid,
            gid: a.gid,
        };
        let mut wrong = envelope.clone();
        wrong.sender_instance = [99; 16];
        assert!(check_hello(&a, &wrong, actual, &a.client.endpoint.peer, 77).is_err());
        let mut wrong = envelope.clone();
        wrong.target_instance = [99; 16];
        assert!(check_hello(&a, &wrong, actual, &a.client.endpoint.peer, 77).is_err());
        let mut wrong = actual;
        wrong.pid += 1;
        assert!(check_hello(&a, &envelope, wrong, &a.client.endpoint.peer, 77).is_err());
    }
}

#[cfg(test)]
mod retirement_tests {
    use super::*;
    #[test]
    fn verified_detach_does_not_invalidate_other_subscribers() {
        assert!(!requires_rebuild_on_remove(Stage::Detached));
        for stage in [Stage::Active, Stage::Quiescing, Stage::Cursor, Stage::Ready] {
            assert!(requires_rebuild_on_remove(stage));
        }
    }
}
