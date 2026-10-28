//! Explicit declared-rebuild objects. Content verification does not establish a
//! live process, authorize output, or certify a rollback strategy.
use crate::replay::{
    self, ClockDomain, Cursor, Input, Model, Rng, Source, StreamKey, VersionedBytes,
};
use crate::{
    Binding, Error, Hash, Member, Method, Object, Operation, ReleaseManifest, Result, Role,
};
use serde::{Deserialize, Serialize};
pub mod install;
pub mod probe;
pub mod rollback;
pub mod window;
pub const MAX_ACTIONS: usize = 256;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub stream: StreamKey,
    pub instrument: u64,
    pub category: u8,
    pub count: u32,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    pub schema: u16,
    pub operation: Object,
    pub plans: Object,
    pub manifest: Object,
    pub contract: Object,
    pub inputs: Object,
    pub anchor: Object,
    pub checkpoint: Object,
    pub transcript: Object,
    pub account_position: u64,
    pub account_seal: Hash,
    pub observations: Vec<Observation>,
    pub consumed_actions: Vec<[u8; 16]>,
}
impl Completion {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        replay::canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        decode(bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Ordering {
    ObservedRebuildWindow,
    RequiresOldProcessOrder,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketRequirement {
    pub stream: StreamKey,
    pub instrument: u64,
    /// 1 quote, 2 trade, 3 depth, 4 mark/funding.
    pub category: u8,
    pub minimum_observations: u32,
    pub known_bid_prices: Vec<i64>,
    pub known_ask_prices: Vec<i64>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelBinding {
    pub id: u16,
    pub value: Object,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionBinding {
    pub economic_intent: [u8; 16],
    /// The exact accepted command; an absent historical action is not inferred.
    pub admitted_command: Object,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Contract {
    pub schema: u16,
    pub factory: String,
    pub binary: Hash,
    pub state_schema: u64,
    pub config: Object,
    pub models: Vec<ModelBinding>,
    pub ordering: Ordering,
    pub markets: Vec<MarketRequirement>,
    pub actions: Vec<ActionBinding>,
    pub minimum_span_ns: u64,
    pub maximum_span_ns: u64,
    pub maximum_age_ns: u64,
    pub maximum_rows: u32,
    pub maximum_bytes: u64,
    /// A fresh, approved generator state, not an inferred old random state.
    pub rng: Rng,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketSeed {
    pub resource: Vec<u8>,
    pub checkpoint: Vec<u8>,
    pub maximum_levels: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountPrefix {
    pub baseline: Vec<u8>,
    pub records: Vec<Vec<u8>>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimedInput {
    pub now_ns: u64,
    pub input: Input,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Inputs {
    pub schema: u16,
    pub clock: ClockDomain,
    pub start_ns: u64,
    pub config: VersionedBytes,
    pub models: Vec<Model>,
    pub markets: Vec<MarketSeed>,
    pub accounts: Vec<AccountPrefix>,
    pub warm: Vec<TimedInput>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Frontier {
    pub now_ns: u64,
    pub rows: u64,
    pub markets: Vec<Cursor>,
    pub accounts: Vec<Cursor>,
    pub external: Vec<Cursor>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub schema: u16,
    pub binding: Binding,
    pub member: Member,
    pub target: Source,
    pub contract: Object,
    pub inputs: Object,
    pub end: Frontier,
}
fn decode<T: for<'a> Deserialize<'a> + Serialize>(bytes: &[u8]) -> Result<T> {
    if bytes.is_empty() || bytes.len() > replay::MAX_BYTES {
        return Err(Error::Bounds);
    }
    let value = serde_json::from_slice(bytes).map_err(|_| Error::Encoding)?;
    if replay::canonical(&value)? != bytes {
        return Err(Error::Encoding);
    }
    Ok(value)
}
impl Contract {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        replay::canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let v: Self = decode(bytes)?;
        v.validate()?;
        Ok(v)
    }
    pub fn validate(&self) -> Result<()> {
        if self.schema != 1
            || self.factory.is_empty()
            || self.factory.len() > 64
            || !self
                .factory
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            || self.binary == [0; 32]
            || self.state_schema == 0
        {
            return Err(Error::Invalid);
        }
        self.config.validate(replay::MAX_BYTES as u64)?;
        if self.markets.is_empty()
            || self.markets.len() > replay::MAX_STREAMS
            || self.models.len() > replay::MAX_STREAMS
            || self.actions.len() > MAX_ACTIONS
            || self.maximum_rows == 0
            || self.maximum_rows as usize > replay::MAX_ROWS
            || self.maximum_bytes == 0
            || self.maximum_bytes > replay::MAX_BYTES as u64
            || self.maximum_age_ns == 0
            || self.minimum_span_ns > self.maximum_span_ns
        {
            return Err(Error::Bounds);
        }
        if self.models.windows(2).any(|w| w[0].id >= w[1].id)
            || self
                .actions
                .windows(2)
                .any(|w| w[0].economic_intent >= w[1].economic_intent)
        {
            return Err(Error::Invalid);
        }
        let mut previous = None;
        for m in &self.markets {
            let key = (&m.stream, m.instrument, m.category);
            if m.stream.identity == [0; 32]
                || m.stream.generation == 0
                || m.instrument == 0
                || !(1..=4).contains(&m.category)
                || m.minimum_observations == 0
                || m.minimum_observations > self.maximum_rows
                || previous.is_some_and(|p| p >= key)
            {
                return Err(Error::Invalid);
            }
            for prices in [&m.known_bid_prices, &m.known_ask_prices] {
                if prices.len() > 256
                    || prices.iter().any(|p| *p <= 0)
                    || prices.windows(2).any(|w| w[0] >= w[1])
                {
                    return Err(Error::Invalid);
                }
            }
            previous = Some(key);
        }
        for model in &self.models {
            model.value.validate(replay::MAX_BYTES as u64)?;
        }
        for action in &self.actions {
            if action.economic_intent == [0; 16] {
                return Err(Error::Identity);
            }
            action.admitted_command.validate(replay::MAX_BYTES as u64)?;
        }
        if matches!(
            self.rng,
            Rng::Xoshiro256StarStarV1 {
                state: [0, 0, 0, 0],
                ..
            } | Rng::Xoshiro256StarStarV1 { draws: 1.., .. }
        ) {
            return Err(Error::Invalid);
        }
        Ok(())
    }
}
impl Inputs {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        replay::canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let v: Self = decode(bytes)?;
        v.validate()?;
        Ok(v)
    }
    pub fn validate(&self) -> Result<()> {
        if self.schema != 1
            || self.clock.boot_id == [0; 16]
            || self.clock.generation == 0
            || self.markets.is_empty()
            || self.markets.len() > replay::MAX_STREAMS
            || self.accounts.is_empty()
            || self.accounts.len() > replay::MAX_STREAMS
            || self.warm.len() > replay::MAX_ROWS
        {
            return Err(Error::Bounds);
        }
        let mut now = self.start_ns;
        for (i, row) in self.warm.iter().enumerate() {
            if row.now_ns < now {
                return Err(Error::Invalid);
            }
            replay::MergedInput {
                sequence: i as u64 + 1,
                now_ns: row.now_ns,
                input: row.input.clone(),
                effects: vec![],
            }
            .validate()?;
            now = row.now_ns;
        }
        for m in &self.markets {
            if m.resource.is_empty()
                || m.checkpoint.is_empty()
                || m.maximum_levels == 0
                || m.maximum_levels > 10000
            {
                return Err(Error::Bounds);
            }
        }
        for a in &self.accounts {
            if a.baseline.is_empty() || a.records.len() > replay::MAX_ROWS {
                return Err(Error::Bounds);
            }
        }
        Ok(())
    }
}
impl Plan {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        if self.schema != 1 {
            return Err(Error::Unsupported);
        }
        self.contract.validate(replay::MAX_BYTES as u64)?;
        self.inputs.validate(replay::MAX_BYTES as u64)?;
        replay::canonical(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let v: Self = decode(bytes)?;
        v.bytes()?;
        Ok(v)
    }
    fn verify_member(
        &self,
        op: &Operation,
        member: &Member,
        contract: &Contract,
        inputs: &Inputs,
        manifest: &ReleaseManifest,
    ) -> Result<()> {
        self.bytes()?;
        self.contract
            .verify(&contract.bytes()?, replay::MAX_BYTES as u64)?;
        self.inputs
            .verify(&inputs.bytes()?, replay::MAX_BYTES as u64)?;
        let binding = Binding {
            operation_id: op.operation_id,
            revision: op.operation_revision,
            logical_strategy: op.logical_strategy_id,
            members_digest: op.members_digest().map_err(|_| Error::Identity)?,
        };
        if self.binding != binding
            || &self.member != member
            || manifest.binding != binding
            || manifest.schema != 1
            || manifest.targets.len() != op.members.len()
            || Object::of(&crate::encode(manifest)?).digest != op.artifacts_digest
        {
            return Err(Error::Identity);
        }
        for (target, expected) in manifest.targets.iter().zip(&op.members) {
            if &target.member != expected {
                return Err(Error::Identity);
            }
            crate::validate_files(&target.files)?;
            target.capabilities.validate()?;
        }
        let target = manifest
            .targets
            .iter()
            .find(|t| &t.member == member)
            .ok_or(Error::Identity)?;
        if self.target.logical_strategy != op.logical_strategy_id
            || self.target.instance != member.new_instance
            || self.target.generation != member.new_generation
            || self.target.binary_digest != contract.binary
            || self.target.state_schema != contract.state_schema
            || self.target.input_stream == [0; 16]
            || self.target.clock != inputs.clock
            || inputs.clock.boot_id != member.clock_boot_id
            || inputs.clock.generation != member.clock_generation
            || target.capabilities.state_schema != contract.state_schema
            || !target.capabilities.methods.contains(&Method::Rebuild {
                target_schema: contract.state_schema,
                contract: self.contract.digest,
            })
        {
            return Err(Error::Identity);
        }
        let object = |role: &Role| {
            target
                .files
                .iter()
                .find(|f| &f.role == role)
                .map(|f| f.object)
        };
        if object(&Role::Binary).map(|o| o.digest) != Some(contract.binary)
            || object(&Role::StrategyConfig) != Some(contract.config)
            || Object::of(&inputs.config.bytes) != contract.config
            || inputs.config.revision == 0
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
        if contract.ordering != Ordering::ObservedRebuildWindow {
            return Err(Error::Unsupported);
        }
        let span = self
            .end
            .now_ns
            .checked_sub(inputs.start_ns)
            .ok_or(Error::Invalid)?;
        if inputs.warm.len() > contract.maximum_rows as usize
            || self.inputs.bytes > contract.maximum_bytes
            || self.end.rows != inputs.warm.len() as u64
            || self.end.now_ns != inputs.warm.last().map_or(inputs.start_ns, |r| r.now_ns)
            || span < contract.minimum_span_ns
            || span > contract.maximum_span_ns
        {
            return Err(Error::Bounds);
        }
        let runtime = replay::RuntimeState {
            now_ns: inputs.start_ns,
            config: inputs.config.clone(),
            models: inputs.models.clone(),
            markets: self.end.markets.clone(),
            accounts: self.end.accounts.clone(),
            external: self.end.external.clone(),
            timers: vec![],
            rng: contract.rng.clone(),
            effects: replay::EffectCut {
                count: 0,
                digest: [0; 32],
            },
        };
        runtime.validate()?;
        Ok(())
    }
}
/// One fixed root for every operation member, preserving sorted membership.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plans {
    pub schema: u16,
    pub binding: Binding,
    pub members: Vec<Plan>,
}
impl Plans {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        if self.schema != 1
            || self.members.is_empty()
            || self.members.len() > tickwire_lifecycle::MAX_MEMBERS
            || self
                .members
                .windows(2)
                .any(|w| w[0].member.risk_domain >= w[1].member.risk_domain)
            || self.members.iter().any(|p| p.binding != self.binding)
        {
            return Err(Error::Identity);
        }
        for p in &self.members {
            p.bytes()?;
        }
        crate::encode(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > crate::MAX_DOCUMENT {
            return Err(Error::Bounds);
        }
        let v: Self = decode(bytes)?;
        v.bytes()?;
        Ok(v)
    }
    pub fn verify<'a>(
        &'a self,
        op: &Operation,
        member: &Member,
        contract: &Contract,
        inputs: &Inputs,
        manifest: &ReleaseManifest,
    ) -> Result<&'a Plan> {
        op.validate().map_err(|_| Error::Identity)?;
        if Object::of(&self.bytes()?).digest != op.checkpoint_digest
            || self.members.len() != op.members.len()
            || self
                .members
                .iter()
                .zip(&op.members)
                .any(|(a, b)| &a.member != b)
        {
            return Err(Error::Identity);
        }
        let plan = self
            .members
            .iter()
            .find(|p| &p.member == member)
            .ok_or(Error::Identity)?;
        plan.verify_member(op, member, contract, inputs, manifest)?;
        Ok(plan)
    }
}
#[cfg(test)]
mod tests;
