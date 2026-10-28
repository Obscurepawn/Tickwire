//! Explicit old-version recovery policy for a declared rebuild. A validated
//! object contains no evidence that the probe was executed or may trade.
use super::*;
use crate::{Artifact, Capabilities};
use replay::{Checkpoint, VerifiedAnchor, VerifiedTail};
use std::collections::BTreeMap;
mod exact;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Probe {
    pub instance: [u8; 16],
    pub generation: u64,
    pub input_stream: [u8; 16],
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Recovery {
    ExactOldRestore {
        source: Source,
        anchor: Object,
        tail: Object,
        checkpoint: Object,
        source_proof: Object,
    },
    ApprovedOldRebuild {
        contract: Object,
        inputs: Object,
        end: Frontier,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberPlan {
    pub member: Member,
    pub version: Hash,
    pub factory: String,
    pub files: Vec<Artifact>,
    pub capabilities: Capabilities,
    pub probe: Probe,
    pub recovery: Recovery,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plans {
    pub schema: u16,
    pub binding: Binding,
    pub members: Vec<MemberPlan>,
}
pub struct Forward<'a> {
    pub plans: &'a super::Plans,
    pub contract: &'a Contract,
    pub inputs: &'a Inputs,
    pub manifest: &'a ReleaseManifest,
}
pub struct ExactInput {
    anchor: VerifiedAnchor,
    tail: VerifiedTail,
    checkpoint: Checkpoint,
}
impl ExactInput {
    pub fn anchor(&self) -> &VerifiedAnchor {
        &self.anchor
    }
    pub fn tail(&self) -> &VerifiedTail {
        &self.tail
    }
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
}
pub struct DeclaredInput {
    contract: Contract,
    inputs: Inputs,
    end: Frontier,
}
impl DeclaredInput {
    pub fn contract(&self) -> &Contract {
        &self.contract
    }
    pub fn inputs(&self) -> &Inputs {
        &self.inputs
    }
    pub fn end(&self) -> &Frontier {
        &self.end
    }
}
enum Input {
    Exact(Box<ExactInput>),
    Declared(Box<DeclaredInput>),
}
pub struct Verified {
    logical_strategy: u64,
    root: Object,
    plan: MemberPlan,
    forward_contract: Contract,
    forward_inputs: Inputs,
    input: Input,
    objects: BTreeMap<Hash, Vec<u8>>,
}
impl Verified {
    pub fn logical_strategy(&self) -> u64 {
        self.logical_strategy
    }
    pub fn root(&self) -> Object {
        self.root
    }
    pub fn plan(&self) -> &MemberPlan {
        &self.plan
    }
    pub fn forward_contract(&self) -> &Contract {
        &self.forward_contract
    }
    pub fn forward_inputs(&self) -> &Inputs {
        &self.forward_inputs
    }
    pub fn exact(&self) -> Option<&ExactInput> {
        match &self.input {
            Input::Exact(i) => Some(i),
            _ => None,
        }
    }
    pub fn declared(&self) -> Option<&DeclaredInput> {
        match &self.input {
            Input::Declared(i) => Some(i),
            _ => None,
        }
    }
    pub fn objects(&self) -> impl Iterator<Item = &[u8]> {
        self.objects.values().map(Vec::as_slice)
    }
}
impl Plans {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        crate::encode(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > crate::MAX_DOCUMENT {
            return Err(Error::Bounds);
        }
        let plans: Self = super::decode(bytes)?;
        plans.validate()?;
        Ok(plans)
    }
    fn validate(&self) -> Result<()> {
        if self.schema != 1
            || self.members.is_empty()
            || self.members.len() > tickwire_lifecycle::MAX_MEMBERS
            || self
                .members
                .windows(2)
                .any(|w| w[0].member.risk_domain >= w[1].member.risk_domain)
        {
            return Err(Error::Identity);
        }
        let mut instances = std::collections::BTreeSet::new();
        let mut streams = std::collections::BTreeSet::new();
        for plan in &self.members {
            crate::validate_files(&plan.files)?;
            plan.capabilities.validate()?;
            if plan.version == [0; 32]
                || plan.factory.is_empty()
                || plan.factory.len() > 64
                || !plan
                    .factory
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                || plan.probe.instance == [0; 16]
                || plan.probe.generation == 0
                || plan.probe.input_stream == [0; 16]
                || !instances.insert(plan.probe.instance)
                || !streams.insert(plan.probe.input_stream)
                || self.members.iter().any(|p| {
                    [p.member.old_instance, p.member.new_instance].contains(&plan.probe.instance)
                })
            {
                return Err(Error::Identity);
            }
            match &plan.recovery {
                Recovery::ExactOldRestore {
                    source,
                    anchor,
                    tail,
                    checkpoint,
                    source_proof,
                } => {
                    if source.input_stream == plan.probe.input_stream {
                        return Err(Error::Identity);
                    }
                    for o in [anchor, tail, checkpoint, source_proof] {
                        o.validate(replay::MAX_BYTES as u64)?;
                    }
                }
                Recovery::ApprovedOldRebuild {
                    contract, inputs, ..
                } => {
                    contract.validate(crate::MAX_DOCUMENT as u64)?;
                    inputs.validate(replay::MAX_BYTES as u64)?;
                }
            }
        }
        Ok(())
    }
    pub fn verify(
        &self,
        operation: &Operation,
        member: &Member,
        forward: Forward<'_>,
        mut read: impl FnMut(Hash) -> Result<Vec<u8>>,
    ) -> Result<Verified> {
        self.verify_at(operation, member, forward, &mut read, 0)
    }
    pub fn verify_at(
        &self,
        operation: &Operation,
        member: &Member,
        forward: Forward<'_>,
        read: &mut dyn FnMut(Hash) -> Result<Vec<u8>>,
        depth: usize,
    ) -> Result<Verified> {
        if depth >= 16 {
            return Err(Error::Bounds);
        }
        let root = Object::of(&self.bytes()?);
        let fixed = forward.plans.verify(
            operation,
            member,
            forward.contract,
            forward.inputs,
            forward.manifest,
        )?;
        if root.digest != operation.rollback_digest
            || self.binding != forward.plans.binding
            || self.members.len() != operation.members.len()
            || self
                .members
                .iter()
                .zip(&operation.members)
                .any(|(a, b)| &a.member != b)
        {
            return Err(Error::Identity);
        }
        let plan = self
            .members
            .iter()
            .find(|p| &p.member == member)
            .ok_or(Error::Identity)?;
        if plan.probe.input_stream == fixed.target.input_stream {
            return Err(Error::Identity);
        }
        let mut objects = Objects {
            read,
            values: BTreeMap::from([(root.digest, self.bytes()?)]),
            bytes: root.bytes as usize,
        };
        let input = match &plan.recovery {
            Recovery::ApprovedOldRebuild {
                contract,
                inputs,
                end,
            } => {
                let contract = Contract::decode(&objects.get(*contract)?)?;
                let inputs = Inputs::decode(&objects.get(*inputs)?)?;
                if contract.factory != plan.factory
                    || contract.actions != forward.contract.actions
                    || inputs.clock != forward.inputs.clock
                    || inputs.start_ns != forward.inputs.start_ns
                    || inputs.markets != forward.inputs.markets
                    || inputs.accounts != forward.inputs.accounts
                    || inputs.warm != forward.inputs.warm
                    || *end != fixed.end
                {
                    return Err(Error::Identity);
                }
                check_declared(plan, &contract, &inputs, end)?;
                Input::Declared(Box::new(DeclaredInput {
                    contract,
                    inputs,
                    end: end.clone(),
                }))
            }
            Recovery::ExactOldRestore { .. } => Input::Exact(Box::new(exact::verify(
                plan,
                operation,
                &forward,
                &fixed.end,
                &mut objects,
                depth,
            )?)),
        };
        Ok(Verified {
            logical_strategy: operation.logical_strategy_id,
            root,
            plan: plan.clone(),
            forward_contract: forward.contract.clone(),
            forward_inputs: forward.inputs.clone(),
            input,
            objects: objects.values,
        })
    }
}
fn check_declared(
    plan: &MemberPlan,
    contract: &Contract,
    inputs: &Inputs,
    end: &Frontier,
) -> Result<()> {
    if contract.ordering != Ordering::ObservedRebuildWindow {
        return Err(Error::Unsupported);
    }
    let object = |role: &Role| {
        plan.files
            .iter()
            .find(|f| &f.role == role)
            .map(|f| f.object)
    };
    if object(&Role::Binary).map(|o| o.digest) != Some(contract.binary)
        || object(&Role::StrategyConfig) != Some(contract.config)
        || Object::of(&inputs.config.bytes) != contract.config
        || inputs.config.revision == 0
        || plan.capabilities.state_schema != contract.state_schema
        || !plan.capabilities.methods.contains(&Method::Rebuild {
            target_schema: contract.state_schema,
            contract: Object::of(&contract.bytes()?).digest,
        })
        || inputs.models.len() != contract.models.len()
    {
        return Err(Error::Identity);
    }
    for (actual, expected) in inputs.models.iter().zip(&contract.models) {
        if actual.id != expected.id
            || actual.value.revision == 0
            || Object::of(&actual.value.bytes) != expected.value
            || object(&Role::Model(expected.id)) != Some(expected.value)
        {
            return Err(Error::Identity);
        }
    }
    let span = end
        .now_ns
        .checked_sub(inputs.start_ns)
        .ok_or(Error::Invalid)?;
    if inputs.warm.len() > contract.maximum_rows as usize
        || inputs.bytes()?.len() as u64 > contract.maximum_bytes
        || end.rows != inputs.warm.len() as u64
        || end.now_ns != inputs.warm.last().map_or(inputs.start_ns, |r| r.now_ns)
        || span < contract.minimum_span_ns
        || span > contract.maximum_span_ns
    {
        return Err(Error::Bounds);
    }
    replay::RuntimeState {
        now_ns: inputs.start_ns,
        config: inputs.config.clone(),
        models: inputs.models.clone(),
        markets: end.markets.clone(),
        accounts: end.accounts.clone(),
        external: end.external.clone(),
        timers: vec![],
        rng: contract.rng.clone(),
        effects: replay::EffectCut {
            count: 0,
            digest: [0; 32],
        },
    }
    .validate()
}
struct Objects<R> {
    read: R,
    values: BTreeMap<Hash, Vec<u8>>,
    bytes: usize,
}
impl<R: FnMut(Hash) -> Result<Vec<u8>>> Objects<R> {
    fn by_hash(&mut self, hash: Hash) -> Result<Vec<u8>> {
        if let Some(bytes) = self.values.get(&hash) {
            return Ok(bytes.clone());
        }
        if hash == [0; 32] || self.values.len() >= 512 {
            return Err(Error::Bounds);
        }
        let bytes = (self.read)(hash)?;
        if bytes.is_empty()
            || bytes.len() > tickwire_recovery::MAX_OBJECT_BYTES
            || Object::of(&bytes).digest != hash
        {
            return Err(Error::Identity);
        }
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES as usize)
            .ok_or(Error::Bounds)?;
        self.values.insert(hash, bytes.clone());
        Ok(bytes)
    }
    fn get(&mut self, object: Object) -> Result<Vec<u8>> {
        object.validate(replay::MAX_BYTES as u64)?;
        if let Some(bytes) = self.values.get(&object.digest) {
            if bytes.len() as u64 != object.bytes {
                return Err(Error::Identity);
            }
            return Ok(bytes.clone());
        }
        if self.values.len() >= 512 {
            return Err(Error::Bounds);
        }
        let bytes = (self.read)(object.digest)?;
        object.verify(&bytes, replay::MAX_BYTES as u64)?;
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES as usize)
            .ok_or(Error::Bounds)?;
        self.values.insert(object.digest, bytes.clone());
        Ok(bytes)
    }
}
#[cfg(test)]
mod tests;
