//! Content binding for a same-process declared-rebuild installation. A valid
//! graph does not attest a live PID or grant publication authority.
use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub schema: u16,
    pub operation: Operation,
    pub member: Member,
    pub plans: Object,
    pub manifest: Object,
    pub capture_completion: Object,
    pub capture_seal: Object,
    pub capture_receipt: Object,
    pub target_bootstrap: Object,
    pub deadline_mono_ns: u64,
}
impl Request {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        self.operation.validate().map_err(|_| Error::Identity)?;
        if self.schema != 1
            || self.operation.schema != 4
            || !self.operation.members.contains(&self.member)
            || self.plans.digest != self.operation.checkpoint_digest
            || self.manifest.digest != self.operation.artifacts_digest
            || self.deadline_mono_ns == 0
            || self.deadline_mono_ns > self.member.valid_until_ns
        {
            return Err(Error::Identity);
        }
        for object in [
            self.plans,
            self.manifest,
            self.capture_completion,
            self.capture_seal,
            self.capture_receipt,
            self.target_bootstrap,
        ] {
            object.validate(replay::MAX_BYTES as u64)?;
        }
        replay::canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Self = super::decode(bytes)?;
        if value.bytes()? != bytes {
            return Err(Error::Identity);
        }
        Ok(value)
    }
    pub fn verify<'a>(
        &self,
        plans: &'a Plans,
        contract: &Contract,
        inputs: &Inputs,
        manifest: &ReleaseManifest,
        seal: &window::Seal,
    ) -> Result<&'a Plan> {
        self.bytes()?;
        self.plans
            .verify(&plans.bytes()?, replay::MAX_BYTES as u64)?;
        self.manifest
            .verify(&crate::encode(manifest)?, replay::MAX_BYTES as u64)?;
        self.capture_seal
            .verify(&seal.bytes()?, replay::MAX_BYTES as u64)?;
        let plan = plans.verify(&self.operation, &self.member, contract, inputs, manifest)?;
        if seal.identity.logical_strategy != self.operation.logical_strategy_id
            || seal.identity.risk_domain != self.member.risk_domain
            || seal.identity.instance != plan.target.instance
            || seal.identity.generation != plan.target.generation
            || seal.identity.binary != plan.target.binary_digest
            || seal.identity.clock != plan.target.clock
            || seal.contract != plan.contract
            || seal.inputs != plan.inputs
            || seal.end != plan.end
            || seal.start.now_ns != inputs.start_ns
            || seal.end.now_ns >= self.deadline_mono_ns
        {
            return Err(Error::Identity);
        }
        Ok(plan)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Installation {
    pub schema: u16,
    pub request: Object,
    pub capture_completion: Object,
    pub capture_seal: Object,
    pub capture_receipt: Object,
    pub warm_completion: Object,
    pub target: Source,
    pub anchor: Object,
    pub checkpoint: Object,
    pub genesis_commit: Object,
    pub commit: Object,
    pub head: Object,
    pub pid: u32,
    pub completed_ns: u64,
}
impl Installation {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        if self.schema != 1
            || self.pid == 0
            || self.completed_ns == 0
            || self.target.logical_strategy == 0
            || self.target.instance == [0; 16]
            || self.target.generation == 0
            || self.target.binary_digest == [0; 32]
            || self.target.state_schema == 0
            || self.target.input_stream == [0; 16]
            || self.target.clock.boot_id == [0; 16]
            || self.target.clock.generation == 0
        {
            return Err(Error::Identity);
        }
        for object in [
            self.request,
            self.capture_completion,
            self.capture_seal,
            self.capture_receipt,
            self.warm_completion,
            self.anchor,
            self.checkpoint,
            self.genesis_commit,
            self.commit,
            self.head,
        ] {
            object.validate(replay::MAX_BYTES as u64)?;
        }
        replay::canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Self = super::decode(bytes)?;
        if value.bytes()? != bytes {
            return Err(Error::Identity);
        }
        Ok(value)
    }
}
mod capture;
mod verify;
pub use verify::Verified;
#[cfg(test)]
mod tests;

/// Load a persisted installation graph without relying on its original paths.
pub fn verify(root: Object, mut get: impl FnMut(Hash) -> Result<Vec<u8>>) -> Result<Verified> {
    let mut objects = std::collections::BTreeMap::<Hash, Vec<u8>>::new();
    let mut total = 0u64;
    let mut read = |o: Object| -> Result<Vec<u8>> {
        o.validate(replay::MAX_BYTES as u64)?;
        if let Some(bytes) = objects.get(&o.digest) {
            o.verify(bytes, replay::MAX_BYTES as u64)?;
            return Ok(bytes.clone());
        }
        if objects.len() >= 512 {
            return Err(Error::Bounds);
        }
        total = total
            .checked_add(o.bytes)
            .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES)
            .ok_or(Error::Bounds)?;
        let bytes = get(o.digest)?;
        o.verify(&bytes, replay::MAX_BYTES as u64)?;
        objects.insert(o.digest, bytes.clone());
        Ok(bytes)
    };
    let installed = Installation::decode(&read(root)?)?;
    let request = Request::decode(&read(installed.request)?)?;
    let plans = Plans::decode(&read(request.plans)?)?;
    let manifest: ReleaseManifest = super::decode(&read(request.manifest)?)?;
    let seal = window::Seal::decode(&read(request.capture_seal)?)?;
    let contract = Contract::decode(&read(seal.contract)?)?;
    let inputs = Inputs::decode(&read(seal.inputs)?)?;
    Verified::verify(
        root, &request, &plans, &contract, &inputs, &manifest, &seal, read,
    )
}
mod seal;
pub use seal::verify_seal;
pub(crate) use seal::verify_seal_at;
