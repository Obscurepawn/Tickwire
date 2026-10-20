//! Signed member facts and a durable multi-domain release coordinator.
#![forbid(unsafe_code)]
use ring::signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tickwire_clock::MonoTime;
pub type Hash = [u8; 32];
pub const MAX_MEMBERS: usize = 64;
pub const MAX_SIGNED_BYTES: usize = 1024 * 1024;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Invalid,
    Identity,
    Signature,
    Expired,
    Capacity,
    Version,
    Phase,
    Conflict,
    Store,
    Frozen,
    Missing,
}
pub type Result<T> = core::result::Result<T, Error>;
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Member {
    pub risk_domain: [u8; 16],
    pub exec_instance: [u8; 16],
    pub exec_epoch: u64,
    pub old_instance: [u8; 16],
    pub old_generation: u64,
    pub new_instance: [u8; 16],
    pub new_generation: u64,
    pub clock_boot_id: [u8; 16],
    pub clock_generation: u64,
    pub valid_until_ns: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Operation {
    pub schema: u32,
    pub operation_id: [u8; 16],
    pub operation_revision: u64,
    pub logical_strategy_id: u64,
    pub members: Vec<Member>,
    pub artifacts_digest: Hash,
    pub checkpoint_digest: Hash,
    pub rollback_digest: Hash,
    pub clock_boot_id: [u8; 16],
    pub clock_generation: u64,
    pub valid_until_ns: u64,
}
impl Operation {
    pub fn validate(&self) -> Result<()> {
        if !matches!(self.schema, 1 | 2)
            || self.operation_id == [0; 16]
            || self.operation_revision == 0
            || self.logical_strategy_id == 0
            || self.members.is_empty()
            || self.members.len() > MAX_MEMBERS
            || self
                .members
                .windows(2)
                .any(|m| m[0].risk_domain >= m[1].risk_domain)
            || [
                self.artifacts_digest,
                self.checkpoint_digest,
                self.rollback_digest,
            ]
            .contains(&[0; 32])
            || self.clock_boot_id == [0; 16]
            || self.clock_generation == 0
            || self.valid_until_ns == 0
        {
            return Err(Error::Invalid);
        }
        for m in &self.members {
            if [
                m.risk_domain,
                m.exec_instance,
                m.old_instance,
                m.new_instance,
            ]
            .contains(&[0; 16])
                || m.exec_epoch == 0
                || m.old_generation == 0
                || m.new_generation <= m.old_generation
                || m.new_instance == m.old_instance
                || m.clock_boot_id == [0; 16]
                || m.clock_generation == 0
                || m.valid_until_ns == 0
            {
                return Err(Error::Identity);
            }
        }
        Ok(())
    }
    pub fn members_digest(&self) -> Result<Hash> {
        self.validate()?;
        digest(b"tickwire.lifecycle.members.v1", &self.members)
    }
    pub fn check_time(&self, now: MonoTime) -> Result<()> {
        if now.domain().boot_id() != self.clock_boot_id
            || now.domain().generation() != self.clock_generation
        {
            return Err(Error::Identity);
        }
        if now.nanos() >= self.valid_until_ns {
            return Err(Error::Expired);
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppliedPhase {
    Prepared,
    Committed,
    Released,
    Halted,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppliedReceipt {
    pub schema: u32,
    pub operation_id: [u8; 16],
    pub operation_revision: u64,
    pub members_digest: Hash,
    pub logical_strategy_id: u64,
    pub member: Member,
    pub phase: AppliedPhase,
    pub state_version: u64,
    pub handover_sequence: u64,
    pub handover_digest: Hash,
    pub risk_gate_revision: u64,
    pub control_request_id: [u8; 16],
    pub issued_ns: u64,
    pub valid_until_ns: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberGate {
    pub member: Member,
    pub risk_gate_revision: u64,
    pub handover_sequence: u64,
    pub handover_digest: Hash,
    pub committed_receipt_digest: Hash,
    pub release_request_id: [u8; 16],
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupRelease {
    pub schema: u32,
    pub operation_id: [u8; 16],
    pub operation_revision: u64,
    pub members_digest: Hash,
    pub logical_strategy_id: u64,
    pub coordinator_id: [u8; 16],
    pub coordinator_version: u64,
    pub members: Vec<MemberGate>,
    pub clock_boot_id: [u8; 16],
    pub clock_generation: u64,
    pub issued_ns: u64,
    pub valid_until_ns: u64,
}
/// Signatures cover the domain prefix followed by the exact payload bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedReceipt {
    pub payload: Vec<u8>,
    pub signature: Vec<u8>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedGroupRelease {
    pub payload: Vec<u8>,
    pub signature: Vec<u8>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Readiness {
    pub checkpoint_digest: Hash,
    pub exec_report_sequence: u64,
    pub orders_digest: Hash,
    pub ledger_sequence: u64,
    pub input_seal_digest: Hash,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlAction {
    Prepare,
    Commit,
    Release,
    QueryApplied,
    Halt,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlRequest {
    pub schema: u32,
    pub coordinator_id: [u8; 16],
    pub operation: Operation,
    pub member: Member,
    pub request_id: [u8; 16],
    pub action: ControlAction,
    pub proof: Option<Readiness>,
    pub release: Option<SignedGroupRelease>,
    /// These fields are in the recipient Member's local clock domain.
    pub valid_until_ns: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedControlRequest {
    pub payload: Vec<u8>,
    pub signature: Vec<u8>,
}
pub struct VerifiedControlRequest {
    request: ControlRequest,
    digest: Hash,
}
impl VerifiedControlRequest {
    pub fn request(&self) -> &ControlRequest {
        &self.request
    }
    pub fn digest(&self) -> Hash {
        self.digest
    }
}
pub struct Signer(Ed25519KeyPair);
impl Signer {
    pub fn from_pkcs8(bytes: &[u8]) -> Result<Self> {
        Ed25519KeyPair::from_pkcs8(bytes)
            .map(Self)
            .map_err(|_| Error::Signature)
    }
    pub fn public_key(&self) -> Hash {
        self.0
            .public_key()
            .as_ref()
            .try_into()
            .expect("Ed25519 public key width")
    }
    pub fn sign_receipt(&self, receipt: &AppliedReceipt) -> Result<SignedReceipt> {
        validate_receipt_shape(receipt)?;
        let payload = canonical(receipt)?;
        let signature = self.sign(b"tickwire.lifecycle.applied.v1", &payload);
        Ok(SignedReceipt { payload, signature })
    }
    fn sign(&self, domain: &[u8], bytes: &[u8]) -> Vec<u8> {
        let mut message = domain.to_vec();
        message.extend(bytes);
        self.0.sign(&message).as_ref().to_vec()
    }
    pub(crate) fn sign_release(&self, release: &GroupRelease) -> Result<SignedGroupRelease> {
        let payload = canonical(release)?;
        let signature = self.sign(b"tickwire.lifecycle.release.v1", &payload);
        Ok(SignedGroupRelease { payload, signature })
    }
    pub(crate) fn sign_request(&self, request: &ControlRequest) -> Result<SignedControlRequest> {
        let payload = canonical(request)?;
        let signature = self.sign(b"tickwire.lifecycle.control.v1", &payload);
        Ok(SignedControlRequest { payload, signature })
    }
}
impl SignedReceipt {
    pub fn digest(&self) -> Hash {
        signed_digest(
            b"tickwire.lifecycle.applied.envelope.v1",
            &self.payload,
            &self.signature,
        )
    }
}
impl SignedGroupRelease {
    pub fn digest(&self) -> Hash {
        signed_digest(
            b"tickwire.lifecycle.release.envelope.v1",
            &self.payload,
            &self.signature,
        )
    }
}
impl SignedControlRequest {
    pub fn digest(&self) -> Hash {
        signed_digest(
            b"tickwire.lifecycle.control.envelope.v1",
            &self.payload,
            &self.signature,
        )
    }
}
/// Collector verification: remote timestamps are not compared with local time.
/// The coordinator must additionally match its outstanding request challenge.
pub fn verify_receipt(
    signed: &SignedReceipt,
    public_key: Hash,
    operation: &Operation,
    now: MonoTime,
) -> Result<AppliedReceipt> {
    operation.validate()?;
    authenticate(
        b"tickwire.lifecycle.applied.v1",
        &signed.payload,
        &signed.signature,
        public_key,
    )?;
    let receipt: AppliedReceipt = decode_canonical(&signed.payload)?;
    validate_receipt_shape(&receipt)?;
    if receipt.phase != AppliedPhase::Halted {
        operation.check_time(now)?;
    } else if now.domain().boot_id() != operation.clock_boot_id
        || now.domain().generation() != operation.clock_generation
    {
        return Err(Error::Identity);
    }
    if receipt.operation_id != operation.operation_id
        || receipt.operation_revision != operation.operation_revision
        || receipt.members_digest != operation.members_digest()?
        || receipt.logical_strategy_id != operation.logical_strategy_id
        || !operation.members.contains(&receipt.member)
    {
        return Err(Error::Identity);
    }
    if receipt.valid_until_ns > receipt.member.valid_until_ns {
        return Err(Error::Expired);
    }
    Ok(receipt)
}
pub struct VerifiedGroupRelease {
    release: GroupRelease,
    digest: Hash,
}
impl VerifiedGroupRelease {
    pub fn release(&self) -> &GroupRelease {
        &self.release
    }
    pub fn digest(&self) -> Hash {
        self.digest
    }
    pub fn member(&self, domain: [u8; 16]) -> Option<&MemberGate> {
        self.release
            .members
            .iter()
            .find(|g| g.member.risk_domain == domain)
    }
}
pub fn verify_group_release(
    signed: &SignedGroupRelease,
    coordinator_key: Hash,
    expected_coordinator: [u8; 16],
    operation: &Operation,
    now: MonoTime,
) -> Result<VerifiedGroupRelease> {
    operation.validate()?;
    operation.check_time(now)?;
    authenticate(
        b"tickwire.lifecycle.release.v1",
        &signed.payload,
        &signed.signature,
        coordinator_key,
    )?;
    let release: GroupRelease = decode_canonical(&signed.payload)?;
    if release.schema != 1
        || release.operation_id != operation.operation_id
        || release.operation_revision != operation.operation_revision
        || release.logical_strategy_id != operation.logical_strategy_id
        || release.members_digest != operation.members_digest()?
        || release.coordinator_id != expected_coordinator
        || expected_coordinator == [0; 16]
        || release.coordinator_version == 0
        || release.clock_boot_id != operation.clock_boot_id
        || release.clock_generation != operation.clock_generation
        || release.members.len() != operation.members.len()
    {
        return Err(Error::Identity);
    }
    if release.issued_ns > now.nanos()
        || now.nanos() >= release.valid_until_ns
        || release.valid_until_ns > operation.valid_until_ns
    {
        return Err(Error::Expired);
    }
    for (member, gate) in operation.members.iter().zip(&release.members) {
        if &gate.member != member
            || gate.risk_gate_revision == 0
            || gate.handover_sequence == 0
            || gate.handover_digest == [0; 32]
            || gate.committed_receipt_digest == [0; 32]
            || gate.release_request_id == [0; 16]
        {
            return Err(Error::Identity);
        }
    }
    Ok(VerifiedGroupRelease {
        release,
        digest: signed.digest(),
    })
}
/// Production recipient verification uses this member's clock, not the
/// coordinator's monotonic zero point.
pub fn verify_group_release_for_member(
    signed: &SignedGroupRelease,
    coordinator_key: Hash,
    expected_coordinator: [u8; 16],
    operation: &Operation,
    member: &Member,
    now: MonoTime,
) -> Result<VerifiedGroupRelease> {
    operation.validate()?;
    member_time(member, now)?;
    if !operation.members.contains(member) {
        return Err(Error::Identity);
    }
    authenticate(
        b"tickwire.lifecycle.release.v1",
        &signed.payload,
        &signed.signature,
        coordinator_key,
    )?;
    let release: GroupRelease = decode_canonical(&signed.payload)?;
    if release.schema != 1
        || release.operation_id != operation.operation_id
        || release.operation_revision != operation.operation_revision
        || release.logical_strategy_id != operation.logical_strategy_id
        || release.members_digest != operation.members_digest()?
        || release.coordinator_id != expected_coordinator
        || expected_coordinator == [0; 16]
        || release.coordinator_version == 0
        || release.clock_boot_id != operation.clock_boot_id
        || release.clock_generation != operation.clock_generation
        || release.issued_ns >= release.valid_until_ns
        || release.valid_until_ns > operation.valid_until_ns
        || release.members.len() != operation.members.len()
    {
        return Err(Error::Identity);
    }
    for (expected, gate) in operation.members.iter().zip(&release.members) {
        if &gate.member != expected
            || gate.risk_gate_revision == 0
            || gate.handover_sequence == 0
            || gate.handover_digest == [0; 32]
            || gate.committed_receipt_digest == [0; 32]
            || gate.release_request_id == [0; 16]
        {
            return Err(Error::Identity);
        }
    }
    Ok(VerifiedGroupRelease {
        release,
        digest: signed.digest(),
    })
}
pub fn verify_control_request(
    signed: &SignedControlRequest,
    coordinator_key: Hash,
    expected_coordinator: [u8; 16],
    expected_member: &Member,
    now: MonoTime,
) -> Result<VerifiedControlRequest> {
    authenticate(
        b"tickwire.lifecycle.control.v1",
        &signed.payload,
        &signed.signature,
        coordinator_key,
    )?;
    let request: ControlRequest = decode_canonical(&signed.payload)?;
    request.operation.validate()?;
    if request.action != ControlAction::Halt {
        member_time(expected_member, now)?;
    } else if now.domain().boot_id() != expected_member.clock_boot_id
        || now.domain().generation() != expected_member.clock_generation
    {
        return Err(Error::Identity);
    }
    if request.schema != 1
        || request.coordinator_id != expected_coordinator
        || expected_coordinator == [0; 16]
        || request.member != *expected_member
        || !request.operation.members.contains(expected_member)
        || request.request_id == [0; 16]
        || (request.action != ControlAction::Halt && now.nanos() >= request.valid_until_ns)
        || request.valid_until_ns > expected_member.valid_until_ns
    {
        return Err(Error::Identity);
    }
    match request.action {
        ControlAction::Prepare | ControlAction::Commit => {
            let p = request.proof.as_ref().ok_or(Error::Missing)?;
            if [p.checkpoint_digest, p.orders_digest, p.input_seal_digest].contains(&[0; 32])
                || request.release.is_some()
            {
                return Err(Error::Invalid);
            }
        }
        ControlAction::Release => {
            let release = request.release.as_ref().ok_or(Error::Missing)?;
            let verified = verify_group_release_for_member(
                release,
                coordinator_key,
                expected_coordinator,
                &request.operation,
                expected_member,
                now,
            )?;
            if verified
                .member(expected_member.risk_domain)
                .is_none_or(|m| m.release_request_id != request.request_id)
                || request.proof.is_some()
            {
                return Err(Error::Identity);
            }
        }
        ControlAction::QueryApplied | ControlAction::Halt => {
            if request.proof.is_some() || request.release.is_some() {
                return Err(Error::Invalid);
            }
        }
    }
    Ok(VerifiedControlRequest {
        digest: signed.digest(),
        request,
    })
}
fn member_time(member: &Member, now: MonoTime) -> Result<()> {
    if now.domain().boot_id() != member.clock_boot_id
        || now.domain().generation() != member.clock_generation
    {
        return Err(Error::Identity);
    }
    if now.nanos() >= member.valid_until_ns {
        return Err(Error::Expired);
    }
    Ok(())
}
fn validate_receipt_shape(r: &AppliedReceipt) -> Result<()> {
    if r.schema != 1
        || r.operation_id == [0; 16]
        || r.operation_revision == 0
        || r.logical_strategy_id == 0
        || r.members_digest == [0; 32]
        || r.state_version == 0
        || r.risk_gate_revision == 0
        || r.control_request_id == [0; 16]
        || (r.phase != AppliedPhase::Halted && r.issued_ns >= r.valid_until_ns)
    {
        return Err(Error::Invalid);
    }
    match r.phase {
        AppliedPhase::Prepared => {
            if r.handover_sequence != 0 || r.handover_digest != [0; 32] {
                return Err(Error::Invalid);
            }
        }
        AppliedPhase::Committed | AppliedPhase::Released => {
            if r.handover_sequence == 0 || r.handover_digest == [0; 32] {
                return Err(Error::Invalid);
            }
        }
        AppliedPhase::Halted => {
            if (r.handover_sequence == 0) != (r.handover_digest == [0; 32]) {
                return Err(Error::Invalid);
            }
        }
    }
    Ok(())
}
fn canonical<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let b = serde_json::to_vec(value).map_err(|_| Error::Invalid)?;
    if b.len() > MAX_SIGNED_BYTES {
        return Err(Error::Capacity);
    }
    Ok(b)
}
fn decode_canonical<T: for<'a> Deserialize<'a> + Serialize>(bytes: &[u8]) -> Result<T> {
    let value = serde_json::from_slice(bytes).map_err(|_| Error::Invalid)?;
    if canonical(&value)? != bytes {
        return Err(Error::Invalid);
    }
    Ok(value)
}
fn digest<T: Serialize>(domain: &[u8], value: &T) -> Result<Hash> {
    let mut h = Sha256::new();
    h.update(domain);
    h.update(canonical(value)?);
    Ok(h.finalize().into())
}
fn signed_digest(domain: &[u8], payload: &[u8], signature: &[u8]) -> Hash {
    let mut h = Sha256::new();
    h.update(domain);
    h.update((payload.len() as u64).to_le_bytes());
    h.update(payload);
    h.update(signature);
    h.finalize().into()
}
fn authenticate(domain: &[u8], payload: &[u8], signature: &[u8], key: Hash) -> Result<()> {
    if payload.is_empty()
        || payload.len() > MAX_SIGNED_BYTES
        || signature.len() != 64
        || key == [0; 32]
    {
        return Err(Error::Capacity);
    }
    let mut message = domain.to_vec();
    message.extend(payload);
    UnparsedPublicKey::new(&ED25519, key)
        .verify(&message, signature)
        .map_err(|_| Error::Signature)
}
mod wal;
#[doc(hidden)]
pub use wal::Wal as EventJournal;
mod coordinator;
pub use coordinator::*;
pub mod cli;
#[cfg(test)]
mod tests;
