//! Explicit installed-replay preparation, separate from legacy Applied receipts.
use crate::*;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayObject {
    pub digest: Hash,
    pub bytes: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayCut {
    pub checkpoint: ReplayObject,
    pub sequence: u64,
    pub chain: Hash,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayPrepared {
    pub schema: u32,
    pub operation: Operation,
    pub member: Member,
    pub prepare: SignedControlRequest,
    pub challenge: SignedControlRequest,
    pub fixed: ReplayCut,
    pub current: ReplayCut,
    pub source_completion: ReplayObject,
    pub target_install: ReplayObject,
    pub readiness: Readiness,
    pub state_version: u64,
    pub risk_gate_revision: u64,
    pub issued_ns: u64,
    pub valid_until_ns: u64,
}
/// Bounded reference to the exact adopted result. It retains the original
/// Prepare envelope without recursively embedding earlier refresh receipts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayAdoption {
    pub prepared_digest: Hash,
    pub prepare: SignedControlRequest,
    pub fixed: ReplayCut,
    pub current: ReplayCut,
    pub source_completion: ReplayObject,
    pub target_install: ReplayObject,
    pub readiness: Readiness,
}
impl ReplayAdoption {
    pub fn from_receipt(signed: &SignedReplayPrepared) -> Result<Self> {
        let r: ReplayPrepared = decode_canonical(&signed.payload)?;
        validate(&r)?;
        Ok(Self {
            prepared_digest: signed.digest(),
            prepare: r.prepare,
            fixed: r.fixed,
            current: r.current,
            source_completion: r.source_completion,
            target_install: r.target_install,
            readiness: r.readiness,
        })
    }
    pub(crate) fn validate(&self, operation: &Operation, member: &Member) -> Result<()> {
        let p: ControlRequest = decode_canonical(&self.prepare.payload)?;
        if self.prepared_digest == [0; 32]
            || self.prepare.signature.len() != 64
            || p.schema != 2
            || p.operation != *operation
            || p.member != *member
            || p.action != ControlAction::Prepare
            || p.request_id == [0; 16]
            || p.rebuild.is_some()
            || p.replay.is_some()
            || p.release.is_some()
            || p.proof.is_none()
            || !cut(&self.fixed)
            || !cut(&self.current)
            || self.fixed.sequence > self.current.sequence
            || (self.fixed.sequence == self.current.sequence && self.fixed != self.current)
            || !object(&self.source_completion)
            || !object(&self.target_install)
            || [
                self.readiness.checkpoint_digest,
                self.readiness.orders_digest,
                self.readiness.input_seal_digest,
            ]
            .contains(&[0; 32])
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedReplayPrepared {
    pub payload: Vec<u8>,
    pub signature: Vec<u8>,
}
impl SignedReplayPrepared {
    pub fn digest(&self) -> Hash {
        signed_digest(
            b"tickwire.lifecycle.replay-prepared.envelope.v1",
            &self.payload,
            &self.signature,
        )
    }
}
impl Signer {
    pub fn sign_replay_prepared(&self, r: &ReplayPrepared) -> Result<SignedReplayPrepared> {
        validate(r)?;
        let payload = canonical(r)?;
        let signature = self.sign(b"tickwire.lifecycle.replay-prepared.v1", &payload);
        Ok(SignedReplayPrepared { payload, signature })
    }
}
fn object(o: &ReplayObject) -> bool {
    o.digest != [0; 32] && o.bytes > 0 && o.bytes <= 64 * 1024 * 1024
}
fn cut(c: &ReplayCut) -> bool {
    object(&c.checkpoint) && (c.sequence == 0) == (c.chain == [0; 32])
}
pub(crate) fn validate(r: &ReplayPrepared) -> Result<ControlRequest> {
    r.operation.validate()?;
    let request: ControlRequest = decode_canonical(&r.prepare.payload)?;
    if r.schema != 1
        || r.operation.schema != 3
        || !r.operation.members.contains(&r.member)
        || request.schema != 2
        || request.operation != r.operation
        || request.member != r.member
        || request.action != ControlAction::Prepare
        || request.request_id == [0; 16]
        || request.rebuild.is_some()
        || request.replay.is_some()
        || request.proof.is_none()
        || request.release.is_some()
        || r.prepare.signature.len() != 64
        || !cut(&r.fixed)
        || !cut(&r.current)
        || r.fixed.sequence > r.current.sequence
        || (r.fixed.sequence == r.current.sequence && r.fixed != r.current)
        || !object(&r.source_completion)
        || !object(&r.target_install)
        || [
            r.readiness.checkpoint_digest,
            r.readiness.orders_digest,
            r.readiness.input_seal_digest,
        ]
        .contains(&[0; 32])
        || r.state_version == 0
        || r.risk_gate_revision == 0
        || r.issued_ns >= r.valid_until_ns
        || r.valid_until_ns > r.member.valid_until_ns
    {
        return Err(Error::Identity);
    }
    let challenge: ControlRequest = decode_canonical(&r.challenge.payload)?;
    if challenge.rebuild.is_some()
        || challenge.schema != 2
        || challenge.operation != r.operation
        || challenge.member != r.member
        || challenge.action != ControlAction::Prepare
        || challenge.request_id == [0; 16]
        || challenge.release.is_some()
        || challenge.proof.is_none()
        || r.challenge.signature.len() != 64
        || r.valid_until_ns > challenge.valid_until_ns
    {
        return Err(Error::Identity);
    }
    if r.challenge != r.prepare {
        let adopted = challenge.replay.as_ref().ok_or(Error::Missing)?;
        adopted.validate(&r.operation, &r.member)?;
        if adopted.prepare != r.prepare
            || adopted.fixed != r.fixed
            || adopted.current != r.current
            || adopted.source_completion != r.source_completion
            || adopted.target_install != r.target_install
            || challenge.proof.as_ref() != Some(&adopted.readiness)
            || r.readiness.exec_report_sequence < adopted.readiness.exec_report_sequence
            || r.readiness.ledger_sequence < adopted.readiness.ledger_sequence
        {
            return Err(Error::Identity);
        }
    }
    Ok(challenge)
}
/// The collector additionally compares the entire saved Prepare envelope with
/// its outstanding request. Remote timestamp values are not local clock values.
pub fn verify_replay_prepared(
    s: &SignedReplayPrepared,
    key: Hash,
    op: &Operation,
    now: MonoTime,
) -> Result<ReplayPrepared> {
    authenticate(
        b"tickwire.lifecycle.replay-prepared.v1",
        &s.payload,
        &s.signature,
        key,
    )?;
    let r: ReplayPrepared = decode_canonical(&s.payload)?;
    validate(&r)?;
    if r.operation != *op {
        return Err(Error::Identity);
    }
    op.check_time(now)?;
    Ok(r)
}

/// A fresh query observes a saved result as history. This never substitutes for
/// a new Prepare/READY refresh or directly authorizes Commit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayObserved {
    pub schema: u16,
    pub query: SignedControlRequest,
    pub applied: AppliedReceipt,
    pub prepared: SignedReplayPrepared,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedReplayObserved {
    pub payload: Vec<u8>,
    pub signature: Vec<u8>,
}
impl SignedReplayObserved {
    pub fn digest(&self) -> Hash {
        signed_digest(
            b"tickwire.lifecycle.replay-observed.envelope.v1",
            &self.payload,
            &self.signature,
        )
    }
}
impl Signer {
    pub fn sign_replay_observed(&self, value: &ReplayObserved) -> Result<SignedReplayObserved> {
        validate_observed(value)?;
        let payload = canonical(value)?;
        let signature = self.sign(b"tickwire.lifecycle.replay-observed.v1", &payload);
        Ok(SignedReplayObserved { payload, signature })
    }
}
fn validate_observed(value: &ReplayObserved) -> Result<ReplayPrepared> {
    let prepared: ReplayPrepared = decode_canonical(&value.prepared.payload)?;
    validate(&prepared)?;
    let query: ControlRequest = decode_canonical(&value.query.payload)?;
    let a = &value.applied;
    validate_receipt_shape(a)?;
    if value.schema != 1
        || query.schema != 2
        || query.action != ControlAction::QueryApplied
        || query.operation != prepared.operation
        || query.member != prepared.member
        || query.request_id == [0; 16]
        || query.proof.is_some()
        || query.rebuild.is_some()
        || query.replay.is_some()
        || query.release.is_some()
        || value.query.signature.len() != 64
        || a.phase != AppliedPhase::Prepared
        || a.operation_id != prepared.operation.operation_id
        || a.operation_revision != prepared.operation.operation_revision
        || a.members_digest != prepared.operation.members_digest()?
        || a.logical_strategy_id != prepared.operation.logical_strategy_id
        || a.member != prepared.member
        || a.control_request_id != query.request_id
        || a.state_version < prepared.state_version
        || a.risk_gate_revision != prepared.risk_gate_revision
        || a.issued_ns < prepared.issued_ns
        || a.valid_until_ns > query.valid_until_ns
        || a.valid_until_ns > a.member.valid_until_ns
    {
        return Err(Error::Identity);
    }
    Ok(prepared)
}
pub fn verify_replay_observed(
    signed: &SignedReplayObserved,
    key: Hash,
    op: &Operation,
    now: MonoTime,
) -> Result<ReplayObserved> {
    authenticate(
        b"tickwire.lifecycle.replay-observed.v1",
        &signed.payload,
        &signed.signature,
        key,
    )?;
    let value: ReplayObserved = decode_canonical(&signed.payload)?;
    let prepared = validate_observed(&value)?;
    verify_replay_prepared(&value.prepared, key, op, now)?;
    if prepared.operation != *op {
        return Err(Error::Identity);
    }
    Ok(value)
}
