//! Explicit declared-rebuild facts. These signatures do not replace the
//! executor's actual input, installation, process, and durable-state checks.
use crate::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebuildCursor {
    pub identity: Hash,
    pub generation: u64,
    pub sequence: u64,
    pub content_digest: Hash,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebuildFrontier {
    pub now_ns: u64,
    pub rows: u64,
    pub markets: Vec<RebuildCursor>,
    pub accounts: Vec<RebuildCursor>,
    pub external: Vec<RebuildCursor>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebuildRoots {
    pub plans: ReplayObject,
    pub contract: ReplayObject,
    pub inputs: ReplayObject,
    pub rollback_plans: ReplayObject,
    pub warm_completion: ReplayObject,
    pub target_install: ReplayObject,
    pub rollback_completion: ReplayObject,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebuildPrepared {
    pub schema: u16,
    pub operation: Operation,
    pub member: Member,
    pub prepare: SignedControlRequest,
    pub challenge: SignedControlRequest,
    pub roots: RebuildRoots,
    pub end: RebuildFrontier,
    pub readiness: Readiness,
    pub state_version: u64,
    pub risk_gate_revision: u64,
    pub issued_ns: u64,
    pub valid_until_ns: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebuildAdoption {
    pub prepared_digest: Hash,
    pub prepare: SignedControlRequest,
    pub roots: RebuildRoots,
    pub end: RebuildFrontier,
    pub readiness: Readiness,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedRebuildPrepared {
    pub payload: Vec<u8>,
    pub signature: Vec<u8>,
}
impl SignedRebuildPrepared {
    pub fn digest(&self) -> Hash {
        signed_digest(
            b"tickwire.lifecycle.rebuild-prepared.envelope.v1",
            &self.payload,
            &self.signature,
        )
    }
}
impl Signer {
    pub fn sign_rebuild_prepared(&self, value: &RebuildPrepared) -> Result<SignedRebuildPrepared> {
        validate(value)?;
        let payload = canonical(value)?;
        let signature = self.sign(b"tickwire.lifecycle.rebuild-prepared.v1", &payload);
        Ok(SignedRebuildPrepared { payload, signature })
    }
}
fn object(value: &ReplayObject) -> bool {
    value.digest != [0; 32] && (1..=64 * 1024 * 1024).contains(&value.bytes)
}
fn readiness(value: &Readiness) -> bool {
    ![
        value.checkpoint_digest,
        value.orders_digest,
        value.input_seal_digest,
    ]
    .contains(&[0; 32])
}
impl RebuildFrontier {
    fn validate(&self) -> Result<()> {
        if self.rows > 65_536 || self.markets.is_empty() || self.accounts.is_empty() {
            return Err(Error::Capacity);
        }
        for cursors in [&self.markets, &self.accounts, &self.external] {
            if cursors.len() > 64 {
                return Err(Error::Capacity);
            }
            let mut prior = None;
            for c in cursors {
                let key = (c.identity, c.generation);
                if c.identity == [0; 32]
                    || c.generation == 0
                    || c.content_digest == [0; 32]
                    || prior.is_some_and(|p| p >= key)
                {
                    return Err(Error::Identity);
                }
                prior = Some(key);
            }
        }
        Ok(())
    }
}
impl RebuildRoots {
    fn validate(&self, operation: &Operation) -> Result<()> {
        if [
            &self.plans,
            &self.contract,
            &self.inputs,
            &self.rollback_plans,
            &self.warm_completion,
            &self.target_install,
            &self.rollback_completion,
        ]
        .into_iter()
        .any(|o| !object(o))
            || self.plans.digest != operation.checkpoint_digest
            || self.rollback_plans.digest != operation.rollback_digest
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
impl RebuildAdoption {
    pub fn from_receipt(signed: &SignedRebuildPrepared) -> Result<Self> {
        let r: RebuildPrepared = decode_canonical(&signed.payload)?;
        validate(&r)?;
        Ok(Self {
            prepared_digest: signed.digest(),
            prepare: r.prepare,
            roots: r.roots,
            end: r.end,
            readiness: r.readiness,
        })
    }
    pub(crate) fn validate(&self, operation: &Operation, member: &Member) -> Result<()> {
        original(&self.prepare, operation, member)?;
        self.roots.validate(operation)?;
        self.end.validate()?;
        if self.prepared_digest == [0; 32] || !readiness(&self.readiness) {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
fn original(
    signed: &SignedControlRequest,
    operation: &Operation,
    member: &Member,
) -> Result<ControlRequest> {
    let p: ControlRequest = decode_canonical(&signed.payload)?;
    operation.validate()?;
    if operation.schema != 4
        || !operation.members.contains(member)
        || p.schema != 3
        || p.operation != *operation
        || p.member != *member
        || p.coordinator_id == [0; 16]
        || p.action != ControlAction::Prepare
        || p.request_id == [0; 16]
        || p.rebuild.is_some()
        || p.replay.is_some()
        || p.release.is_some()
        || p.proof.as_ref().is_some_and(|p| !readiness(p))
        || signed.signature.len() != 64
        || p.valid_until_ns == 0
        || p.valid_until_ns > member.valid_until_ns
    {
        return Err(Error::Identity);
    }
    Ok(p)
}
pub(crate) fn validate(value: &RebuildPrepared) -> Result<ControlRequest> {
    let p = original(&value.prepare, &value.operation, &value.member)?;
    value.roots.validate(&value.operation)?;
    value.end.validate()?;
    if value.schema != 1
        || !readiness(&value.readiness)
        || value.state_version == 0
        || value.risk_gate_revision == 0
        || value.issued_ns >= value.valid_until_ns
        || value.valid_until_ns > value.member.valid_until_ns
        || value.end.now_ns > value.issued_ns
    {
        return Err(Error::Identity);
    }
    let c: ControlRequest = decode_canonical(&value.challenge.payload)?;
    if c.schema != 3
        || c.operation != value.operation
        || c.member != value.member
        || c.coordinator_id != p.coordinator_id
        || c.action != ControlAction::Prepare
        || c.request_id == [0; 16]
        || c.replay.is_some()
        || c.release.is_some()
        || c.proof.as_ref().is_some_and(|p| !readiness(p))
        || value.challenge.signature.len() != 64
        || value.valid_until_ns > c.valid_until_ns
    {
        return Err(Error::Identity);
    }
    if value.challenge != value.prepare {
        let a = c.rebuild.as_ref().ok_or(Error::Missing)?;
        a.validate(&value.operation, &value.member)?;
        if a.prepare != value.prepare
            || a.roots != value.roots
            || a.end != value.end
            || c.proof.as_ref() != Some(&a.readiness)
            || value.readiness.exec_report_sequence < a.readiness.exec_report_sequence
            || value.readiness.ledger_sequence < a.readiness.ledger_sequence
        {
            return Err(Error::Identity);
        }
    }
    Ok(c)
}
/// The collector additionally pins the complete challenge to its actual pending
/// envelope. Recipient timestamps cannot be compared with a coordinator clock.
pub fn verify_rebuild_prepared(
    signed: &SignedRebuildPrepared,
    key: Hash,
    operation: &Operation,
    now: MonoTime,
) -> Result<RebuildPrepared> {
    authenticate(
        b"tickwire.lifecycle.rebuild-prepared.v1",
        &signed.payload,
        &signed.signature,
        key,
    )?;
    let value: RebuildPrepared = decode_canonical(&signed.payload)?;
    validate(&value)?;
    if value.operation != *operation {
        return Err(Error::Identity);
    }
    operation.check_time(now)?;
    Ok(value)
}

/// A new authenticated query exposes an installed historical result. It still
/// requires a fresh Prepare/READY before a new Commit can be constructed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebuildObserved {
    pub schema: u16,
    pub query: SignedControlRequest,
    pub applied: AppliedReceipt,
    pub prepared: SignedRebuildPrepared,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedRebuildObserved {
    pub payload: Vec<u8>,
    pub signature: Vec<u8>,
}
impl SignedRebuildObserved {
    pub fn digest(&self) -> Hash {
        signed_digest(
            b"tickwire.lifecycle.rebuild-observed.envelope.v1",
            &self.payload,
            &self.signature,
        )
    }
}
impl Signer {
    pub fn sign_rebuild_observed(&self, value: &RebuildObserved) -> Result<SignedRebuildObserved> {
        validate_observed(value)?;
        let payload = canonical(value)?;
        let signature = self.sign(b"tickwire.lifecycle.rebuild-observed.v1", &payload);
        Ok(SignedRebuildObserved { payload, signature })
    }
}
fn validate_observed(value: &RebuildObserved) -> Result<RebuildPrepared> {
    let p: RebuildPrepared = decode_canonical(&value.prepared.payload)?;
    let challenge = validate(&p)?;
    let q: ControlRequest = decode_canonical(&value.query.payload)?;
    let a = &value.applied;
    validate_receipt_shape(a)?;
    if value.schema != 1
        || q.schema != 3
        || q.action != ControlAction::QueryApplied
        || q.operation != p.operation
        || q.member != p.member
        || q.request_id == [0; 16]
        || q.coordinator_id != challenge.coordinator_id
        || q.proof.is_some()
        || q.replay.is_some()
        || q.rebuild.is_some()
        || q.release.is_some()
        || value.query.signature.len() != 64
        || a.phase != AppliedPhase::Prepared
        || a.operation_id != p.operation.operation_id
        || a.operation_revision != p.operation.operation_revision
        || a.members_digest != p.operation.members_digest()?
        || a.logical_strategy_id != p.operation.logical_strategy_id
        || a.member != p.member
        || a.control_request_id != q.request_id
        || a.state_version < p.state_version
        || a.risk_gate_revision != p.risk_gate_revision
        || a.issued_ns < p.issued_ns
        || a.valid_until_ns > q.valid_until_ns
        || a.valid_until_ns > a.member.valid_until_ns
    {
        return Err(Error::Identity);
    }
    Ok(p)
}
pub fn verify_rebuild_observed(
    signed: &SignedRebuildObserved,
    key: Hash,
    operation: &Operation,
    now: MonoTime,
) -> Result<RebuildObserved> {
    authenticate(
        b"tickwire.lifecycle.rebuild-observed.v1",
        &signed.payload,
        &signed.signature,
        key,
    )?;
    let value: RebuildObserved = decode_canonical(&signed.payload)?;
    validate_observed(&value)?;
    verify_rebuild_prepared(&value.prepared, key, operation, now)?;
    Ok(value)
}
