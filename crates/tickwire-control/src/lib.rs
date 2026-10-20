//! Authenticated control-session and request-registration state.
#![forbid(unsafe_code)]

use tickwire_ipc::{ControlEnvelope, ControlFrame, HelloBody, WelcomeBody, Wire, crc32c};

const LIMIT: u64 = 1 << 63;
const KNOWN_FEATURES: u64 = 0x3f;
const REQUIRED_FEATURES: u64 = 0x1c;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Wire(tickwire_ipc::Error),
    Identity,
    Negotiation,
    Protocol,
    Expired,
    ClockRegression,
    Revision,
    Bounds,
    Conflict,
    Version,
    Capacity,
    Store,
}
pub type Result<T> = std::result::Result<T, Error>;
impl From<tickwire_ipc::Error> for Error {
    fn from(error: tickwire_ipc::Error) -> Self {
        Self::Wire(error)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
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
impl Identity {
    fn valid(&self) -> bool {
        self.instance != [0; 16]
            && (1..LIMIT).contains(&self.generation)
            && (1..=9).contains(&self.role)
            && self.logical_strategy < LIMIT
            && (self.role != 3 || self.logical_strategy != 0)
            && self.host_boot != [0; 16]
            && self.namespace != [0; 16]
            && self.build_hash != [0; 32]
            && self.qualification_hash != [0; 32]
            && self.manifest_hash != [0; 32]
            && (1..=2).contains(&self.arch)
            && (1..=2).contains(&self.trust_profile)
    }
}
/// Obtained from the kernel, then compared to the supervisor's live process registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub uid: u32,
    pub gid: u32,
}

/// Trusted launcher inputs. The launcher must never reuse an epoch/nonce for a new session.
#[derive(Debug, Clone)]
pub struct Policy {
    pub local: Identity,
    pub peer: Identity,
    pub peer_process: ProcessIdentity,
    pub required_features: u64,
    pub supported_features: u64,
    pub control_epoch: u64,
    pub lease_duration_ns: u64,
    pub max_frame_bytes: u32,
    pub nonce: [u8; 32],
}
impl Policy {
    fn validate(&self) -> Result<()> {
        if !self.local.valid()
            || !self.peer.valid()
            || self.local.instance == self.peer.instance
            || self.peer_process.pid == 0
            || self.local.host_boot != self.peer.host_boot
            || self.local.namespace != self.peer.namespace
            || self.nonce == [0; 32]
            || !(1..LIMIT).contains(&self.control_epoch)
        {
            return Err(Error::Identity);
        }
        if self.supported_features & !KNOWN_FEATURES != 0
            || self.required_features & !self.supported_features != 0
            || self.required_features & REQUIRED_FEATURES != REQUIRED_FEATURES
            || ((self.local.role == 9 || self.peer.role == 9) && self.required_features & 32 == 0)
            || !(320..=65536).contains(&self.max_frame_bytes)
            || !(1..LIMIT).contains(&self.lease_duration_ns)
        {
            return Err(Error::Negotiation);
        }
        Ok(())
    }
}

/// Encode and structurally validate a complete frame; no transport or state change occurs.
pub fn encode_frame(mut envelope: ControlEnvelope, body: &[u8]) -> Result<Vec<u8>> {
    let length = ControlEnvelope::SIZE
        .checked_add(body.len())
        .ok_or(Error::Bounds)?;
    if length > 65536 {
        return Err(Error::Bounds);
    }
    envelope.magic = *b"TWCTL001";
    envelope.abi_major = 1;
    envelope.abi_minor = 0;
    envelope.header_bytes = ControlEnvelope::SIZE as u16;
    envelope.frame_bytes = length as u32;
    envelope.body_crc32c = crc32c(body);
    envelope.header_crc32c = 0;
    let mut bytes = vec![0; length];
    envelope.encode(&mut bytes[..ControlEnvelope::SIZE])?;
    envelope.header_crc32c = crc32c(&bytes[..ControlEnvelope::SIZE]);
    envelope.encode(&mut bytes[..ControlEnvelope::SIZE])?;
    bytes[ControlEnvelope::SIZE..].copy_from_slice(body);
    ControlFrame::decode(&bytes)?;
    Ok(bytes)
}
fn body_bytes<T: Wire>(body: &T) -> Result<Vec<u8>> {
    let mut bytes = vec![0; T::SIZE];
    body.encode(&mut bytes)?;
    Ok(bytes)
}
fn handshake_envelope(e: &ControlEnvelope, p: &Policy, epoch: u64) -> Result<()> {
    if e.sender_instance != p.peer.instance
        || e.target_instance != p.local.instance
        || e.control_epoch != epoch
    {
        return Err(Error::Identity);
    }
    if e.request_id == [0; 16]
        || e.operation_id != [0; 16]
        || e.operation_revision != 0
        || e.expected_state_version != 0
    {
        return Err(Error::Protocol);
    }
    Ok(())
}

pub struct ClientHandshake {
    policy: Policy,
    request_id: [u8; 16],
    hello: Vec<u8>,
}
impl ClientHandshake {
    pub fn new(policy: Policy, request_id: [u8; 16]) -> Result<Self> {
        policy.validate()?;
        if request_id == [0; 16] {
            return Err(Error::Protocol);
        }
        let i = &policy.local;
        let body = HelloBody {
            min_major: 1,
            max_major: 1,
            min_minor: 0,
            max_minor: 0,
            role: i.role,
            target_arch: i.arch,
            page_shift: 12,
            isolation_shift: 7,
            atomic_width: 8,
            trust_profile: i.trust_profile,
            required_features: policy.required_features,
            optional_features: policy.supported_features & !policy.required_features,
            host_boot_id: i.host_boot,
            namespace_id: i.namespace,
            logical_strategy_id: i.logical_strategy,
            instance_generation: i.generation,
            build_hash: i.build_hash,
            sdk_qualification_hash: i.qualification_hash,
            manifest_hash: i.manifest_hash,
            max_frame_bytes: policy.max_frame_bytes,
        };
        let hello = encode_frame(
            ControlEnvelope {
                opcode: 1,
                flags: 1,
                request_id,
                sender_instance: i.instance,
                target_instance: policy.peer.instance,
                ..Default::default()
            },
            &body_bytes(&body)?,
        )?;
        Ok(Self {
            policy,
            request_id,
            hello,
        })
    }
    pub fn hello(&self) -> &[u8] {
        &self.hello
    }
    pub fn finish(self, actual_peer: ProcessIdentity, bytes: &[u8], now: u64) -> Result<Session> {
        let p = &self.policy;
        if actual_peer != p.peer_process {
            return Err(Error::Identity);
        }
        let frame = ControlFrame::decode(bytes)?;
        handshake_envelope(&frame.envelope, p, p.control_epoch)?;
        if frame.envelope.opcode != 2 || frame.envelope.request_id != self.request_id {
            return Err(Error::Protocol);
        }
        let w = WelcomeBody::decode(frame.body)?;
        if w.server_instance != p.peer.instance
            || w.host_boot_id != p.peer.host_boot
            || w.assigned_control_epoch != p.control_epoch
            || w.session_nonce != p.nonce
            || w.role != p.local.role
            || w.trust_profile != u16::from(p.local.trust_profile)
        {
            return Err(Error::Identity);
        }
        if w.selected_major != 1
            || w.selected_minor != 0
            || w.features & !p.supported_features != 0
            || w.features & p.required_features != p.required_features
            || !(320..=p.max_frame_bytes).contains(&w.max_frame_bytes)
            || bytes.len() > w.max_frame_bytes as usize
            || w.max_fds != 16
            || w.max_consumers != 32
            || w.lease_revision != 1
        {
            return Err(Error::Negotiation);
        }
        if now >= w.lease_deadline_mono_ns
            || w.lease_deadline_mono_ns >= LIMIT
            || w.lease_deadline_mono_ns - now > p.lease_duration_ns
        {
            return Err(Error::Expired);
        }
        Ok(Session::new(
            self.policy,
            w.features,
            w.max_frame_bytes,
            w.lease_deadline_mono_ns,
            now,
        ))
    }
}

/// Single-owner session. Expiry and clock regression permanently close this instance.
pub struct Session {
    policy: Policy,
    features: u64,
    max_frame_bytes: u32,
    deadline: u64,
    revision: u64,
    last_now: u64,
    closed: bool,
}
impl Session {
    fn new(policy: Policy, features: u64, max_frame_bytes: u32, deadline: u64, now: u64) -> Self {
        Self {
            policy,
            features,
            max_frame_bytes,
            deadline,
            revision: 1,
            last_now: now,
            closed: false,
        }
    }
    pub fn accept(
        p: &Policy,
        actual_peer: ProcessIdentity,
        bytes: &[u8],
        now: u64,
    ) -> Result<(Self, Vec<u8>)> {
        p.validate()?;
        if actual_peer != p.peer_process {
            return Err(Error::Identity);
        }
        let f = ControlFrame::decode(bytes)?;
        handshake_envelope(&f.envelope, p, 0)?;
        if f.envelope.opcode != 1 {
            return Err(Error::Protocol);
        }
        let h = HelloBody::decode(f.body)?;
        let peer = &p.peer;
        if h.role != peer.role
            || h.target_arch != peer.arch
            || h.target_arch != p.local.arch
            || h.trust_profile != peer.trust_profile
            || h.host_boot_id != peer.host_boot
            || h.namespace_id != peer.namespace
            || h.logical_strategy_id != peer.logical_strategy
            || h.instance_generation != peer.generation
            || h.build_hash != peer.build_hash
            || h.sdk_qualification_hash != peer.qualification_hash
            || h.manifest_hash != peer.manifest_hash
        {
            return Err(Error::Identity);
        }
        let offered = h.required_features | h.optional_features;
        let features = offered & p.supported_features;
        if h.min_major > 1
            || h.max_major < 1
            || h.min_major > h.max_major
            || h.min_minor != 0
            || h.min_minor > h.max_minor
            || h.page_shift != 12
            || h.isolation_shift != 7
            || h.atomic_width != 8
            || h.required_features & !KNOWN_FEATURES != 0
            || h.required_features & REQUIRED_FEATURES != REQUIRED_FEATURES
            || features & h.required_features != h.required_features
            || features & p.required_features != p.required_features
            || !(320..=65536).contains(&h.max_frame_bytes)
        {
            return Err(Error::Negotiation);
        }
        let max_frame_bytes = p.max_frame_bytes.min(h.max_frame_bytes);
        if bytes.len() > max_frame_bytes as usize {
            return Err(Error::Bounds);
        }
        let deadline = now
            .checked_add(p.lease_duration_ns)
            .filter(|&v| v < LIMIT)
            .ok_or(Error::Bounds)?;
        let welcome = WelcomeBody {
            selected_major: 1,
            selected_minor: 0,
            role: peer.role,
            trust_profile: u16::from(peer.trust_profile),
            features,
            assigned_control_epoch: p.control_epoch,
            server_instance: p.local.instance,
            host_boot_id: p.local.host_boot,
            lease_deadline_mono_ns: deadline,
            lease_revision: 1,
            max_frame_bytes,
            max_fds: 16,
            max_consumers: 32,
            session_nonce: p.nonce,
        };
        let response = encode_frame(
            ControlEnvelope {
                opcode: 2,
                flags: 10,
                request_id: f.envelope.request_id,
                control_epoch: p.control_epoch,
                sender_instance: p.local.instance,
                target_instance: peer.instance,
                ..Default::default()
            },
            &body_bytes(&welcome)?,
        )?;
        Ok((
            Self::new(p.clone(), features, max_frame_bytes, deadline, now),
            response,
        ))
    }
    pub fn features(&self) -> u64 {
        self.features
    }
    pub fn deadline(&self) -> u64 {
        self.deadline
    }
    pub fn epoch(&self) -> u64 {
        self.policy.control_epoch
    }
    pub fn peer(&self) -> &Identity {
        &self.policy.peer
    }
    fn check_time(&mut self, now: u64) -> Result<()> {
        if self.closed {
            return Err(Error::Expired);
        }
        if now < self.last_now {
            self.closed = true;
            return Err(Error::ClockRegression);
        }
        self.last_now = now;
        if now >= self.deadline {
            self.closed = true;
            return Err(Error::Expired);
        }
        Ok(())
    }
    /// Serialize an outbound frame. The receiver must validate its lease at processing time.
    pub fn encode(&self, mut envelope: ControlEnvelope, body: &[u8]) -> Result<Vec<u8>> {
        if self.closed {
            return Err(Error::Expired);
        }
        envelope.sender_instance = self.policy.local.instance;
        envelope.target_instance = self.policy.peer.instance;
        envelope.control_epoch = self.policy.control_epoch;
        let bytes = encode_frame(envelope, body)?;
        self.check_frame(&bytes, false)?;
        Ok(bytes)
    }
    fn check_frame<'a>(&self, bytes: &'a [u8], incoming: bool) -> Result<ControlFrame<'a>> {
        if bytes.len() > self.max_frame_bytes as usize {
            return Err(Error::Bounds);
        }
        let f = ControlFrame::decode_negotiated(bytes, self.features)?;
        let (sender, target) = if incoming {
            (&self.policy.peer, &self.policy.local)
        } else {
            (&self.policy.local, &self.policy.peer)
        };
        if f.envelope.sender_instance != sender.instance
            || f.envelope.target_instance != target.instance
            || f.envelope.control_epoch != self.policy.control_epoch
        {
            return Err(Error::Identity);
        }
        if matches!(f.envelope.opcode, 1 | 2)
            || f.envelope.request_id == [0; 16]
            || f.envelope.operation_revision >= LIMIT
            || f.envelope.expected_state_version >= LIMIT
        {
            return Err(Error::Protocol);
        }
        Ok(f)
    }
    pub fn validate<'a>(&mut self, bytes: &'a [u8], now: u64) -> Result<ControlFrame<'a>> {
        self.check_time(now)?;
        self.check_frame(bytes, true)
    }
    pub fn renew(&mut self, nonce: [u8; 32], revision: u64, now: u64) -> Result<u64> {
        self.check_time(now)?;
        if nonce != self.policy.nonce {
            return Err(Error::Identity);
        }
        if revision <= self.revision || revision >= LIMIT {
            return Err(Error::Revision);
        }
        let deadline = now
            .checked_add(self.policy.lease_duration_ns)
            .filter(|&v| v < LIMIT)
            .ok_or(Error::Bounds)?;
        self.deadline = deadline;
        self.revision = revision;
        Ok(deadline)
    }
}

mod requests;
pub use requests::*;
