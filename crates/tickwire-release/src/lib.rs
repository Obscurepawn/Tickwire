//! Immutable release commitments. These types validate content and binding;
//! only the qualified loader can attest that a process loaded or restored it.
#![forbid(unsafe_code)]
pub mod closure;
pub mod files;
pub mod rebuild;
pub mod receipt;
pub mod replay;
use serde::{Deserialize, Serialize};
pub use tickwire_lifecycle::{Member, Operation};
pub type Hash = [u8; 32];
pub const MAX_DOCUMENT: usize = 1024 * 1024;
pub const MAX_FILES: usize = 64;
pub const MAX_BINARY: u64 = 512 * 1024 * 1024;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Encoding,
    Bounds,
    Identity,
    Missing,
    Unsupported,
    Invalid,
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
pub struct Binding {
    pub operation_id: [u8; 16],
    pub revision: u64,
    pub logical_strategy: u64,
    pub members_digest: Hash,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Object {
    pub digest: Hash,
    pub bytes: u64,
}
impl Object {
    pub fn of(bytes: &[u8]) -> Self {
        Self {
            digest: tickwire_recovery::digest(bytes),
            bytes: bytes.len() as u64,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum Role {
    Binary,
    HostConfig,
    Bootstrap,
    StrategyConfig,
    RecoveryTemplate,
    Model(u16),
    Dependency(u16),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub role: Role,
    pub object: Object,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Product {
    Spot,
    LinearPerpetual,
    InversePerpetual,
    Option,
    LinearFuture,
    InverseFuture,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionMode {
    Spot,
    OneWay,
    Hedge,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderKind {
    Limit,
    Market,
    StopLimit,
    StopMarket,
    TakeProfitLimit,
    TakeProfitMarket,
    TrailingStop,
    SpotList,
    Algo,
    Peg,
    Sor,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields, rename_all = "snake_case")]
pub enum Method {
    Restore {
        schema: u64,
    },
    Migrate {
        source_schema: u64,
        target_schema: u64,
        method_id: u64,
    },
    Rebuild {
        target_schema: u64,
        contract: Hash,
    },
}
impl Method {
    pub fn target_schema(&self) -> u64 {
        match self {
            Self::Restore { schema } => *schema,
            Self::Migrate { target_schema, .. } | Self::Rebuild { target_schema, .. } => {
                *target_schema
            }
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capabilities {
    pub state_schema: u64,
    pub input_schema: u64,
    pub output_schema: u64,
    pub products: Vec<Product>,
    pub position_modes: Vec<PositionMode>,
    pub orders: Vec<OrderKind>,
    pub methods: Vec<Method>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Requirements {
    pub products: Vec<Product>,
    pub position_modes: Vec<PositionMode>,
    pub orders: Vec<OrderKind>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub member: Member,
    pub version: Hash,
    pub files: Vec<Artifact>,
    pub capabilities: Capabilities,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseManifest {
    pub schema: u16,
    pub binding: Binding,
    pub targets: Vec<Target>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputPrefix {
    pub object: Object,
    pub stream: [u8; 16],
    pub first_sequence: u64,
    pub previous_hash: Hash,
    pub last_sequence: u64,
    pub last_hash: Hash,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Migration {
    pub member: Member,
    pub source_checkpoint: Object,
    pub source_schema: u64,
    pub source_proof: Object,
    pub inputs: Option<InputPrefix>,
    pub method: Method,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationSet {
    pub schema: u16,
    pub binding: Binding,
    pub members: Vec<Migration>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeInput {
    TargetRestoreOutput,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rollback {
    pub member: Member,
    pub version: Hash,
    pub files: Vec<Artifact>,
    pub capabilities: Capabilities,
    pub method: Method,
    pub probe_input: ProbeInput,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RollbackPlan {
    pub schema: u16,
    pub binding: Binding,
    pub members: Vec<Rollback>,
}

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|_| Error::Encoding)?;
    if bytes.len() > MAX_DOCUMENT {
        return Err(Error::Bounds);
    }
    Ok(bytes)
}
fn decode<T: for<'a> Deserialize<'a> + Serialize>(bytes: &[u8]) -> Result<T> {
    if bytes.is_empty() || bytes.len() > MAX_DOCUMENT {
        return Err(Error::Bounds);
    }
    let value: T = serde_json::from_slice(bytes).map_err(|_| Error::Encoding)?;
    if encode(&value)? != bytes {
        return Err(Error::Encoding);
    }
    Ok(value)
}
pub struct ReleaseBundle {
    manifest: ReleaseManifest,
    migration: MigrationSet,
    rollback: RollbackPlan,
    roots: [Hash; 3],
}
pub struct MemberRelease<'a> {
    operation: Operation,
    roots: [Hash; 3],
    logical_strategy: u64,
    pub target: &'a Target,
    pub migration: &'a Migration,
    pub rollback: &'a Rollback,
}
impl ReleaseBundle {
    pub fn decode(manifest: &[u8], migration: &[u8], rollback: &[u8]) -> Result<Self> {
        Ok(Self {
            manifest: decode(manifest)?,
            migration: decode(migration)?,
            rollback: decode(rollback)?,
            roots: [
                tickwire_recovery::digest(manifest),
                tickwire_recovery::digest(migration),
                tickwire_recovery::digest(rollback),
            ],
        })
    }
    pub fn roots(&self) -> [Hash; 3] {
        self.roots
    }
    pub fn object_bytes(&self) -> Result<[Vec<u8>; 3]> {
        Ok([
            encode(&self.manifest)?,
            encode(&self.migration)?,
            encode(&self.rollback)?,
        ])
    }
    pub fn verify_operation(&self, op: &Operation, member: &Member) -> Result<MemberRelease<'_>> {
        op.validate().map_err(|_| Error::Identity)?;
        if self.roots
            != [
                op.artifacts_digest,
                op.checkpoint_digest,
                op.rollback_digest,
            ]
            || self.manifest.schema != 1
            || self.migration.schema != 1
            || self.rollback.schema != 1
        {
            return Err(Error::Identity);
        }
        let binding = Binding {
            operation_id: op.operation_id,
            revision: op.operation_revision,
            logical_strategy: op.logical_strategy_id,
            members_digest: op.members_digest().map_err(|_| Error::Identity)?,
        };
        if self.manifest.binding != binding
            || self.migration.binding != binding
            || self.rollback.binding != binding
            || self.manifest.targets.len() != op.members.len()
            || self.migration.members.len() != op.members.len()
            || self.rollback.members.len() != op.members.len()
        {
            return Err(Error::Identity);
        }
        let mut found = None;
        for (i, expected) in op.members.iter().enumerate() {
            let target = &self.manifest.targets[i];
            let migration = &self.migration.members[i];
            let rollback = &self.rollback.members[i];
            if &target.member != expected
                || &migration.member != expected
                || &rollback.member != expected
            {
                return Err(Error::Identity);
            }
            if target.version == [0; 32] || rollback.version == [0; 32] {
                return Err(Error::Invalid);
            }
            validate_files(&target.files)?;
            validate_files(&rollback.files)?;
            target.capabilities.validate()?;
            rollback.capabilities.validate()?;
            migration
                .source_checkpoint
                .validate(tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
            migration
                .source_proof
                .validate(tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
            if migration.source_schema == 0 {
                return Err(Error::Invalid);
            }
            migration.method.check_source(migration.source_schema)?;
            if migration.method.target_schema() != target.capabilities.state_schema
                || !target.capabilities.methods.contains(&migration.method)
            {
                return Err(Error::Unsupported);
            }
            if let Some(input) = &migration.inputs {
                input.validate()?;
            } else if !matches!(migration.method, Method::Rebuild { .. }) {
                return Err(Error::Missing);
            }
            rollback
                .method
                .check_source(target.capabilities.state_schema)?;
            if rollback.method.target_schema() != rollback.capabilities.state_schema
                || !rollback.capabilities.methods.contains(&rollback.method)
            {
                return Err(Error::Unsupported);
            }
            if expected == member {
                found = Some(i)
            }
        }
        let i = found.ok_or(Error::Identity)?;
        Ok(MemberRelease {
            operation: op.clone(),
            roots: self.roots,
            logical_strategy: op.logical_strategy_id,
            target: &self.manifest.targets[i],
            migration: &self.migration.members[i],
            rollback: &self.rollback.members[i],
        })
    }
}
impl MemberRelease<'_> {
    pub fn operation(&self) -> &Operation {
        &self.operation
    }
    pub fn roots(&self) -> [Hash; 3] {
        self.roots
    }
    pub fn logical_strategy(&self) -> u64 {
        self.logical_strategy
    }
    pub fn check_capabilities(
        &self,
        compiled: &Capabilities,
        required: &Requirements,
    ) -> Result<()> {
        compiled.validate()?;
        if compiled != &self.target.capabilities {
            return Err(Error::Unsupported);
        }
        compiled.check_requirements(required)?;
        self.rollback.capabilities.check_requirements(required)
    }
}
#[cfg(test)]
mod tests;

impl Object {
    pub fn validate(&self, max: u64) -> Result<()> {
        if self.digest == [0; 32] || self.bytes == 0 || self.bytes > max {
            Err(Error::Bounds)
        } else {
            Ok(())
        }
    }
    pub fn verify(&self, bytes: &[u8], max: u64) -> Result<()> {
        self.validate(max)?;
        if bytes.len() as u64 != self.bytes || tickwire_recovery::digest(bytes) != self.digest {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
fn sorted<T: Ord>(xs: &[T], max: usize, empty: bool) -> Result<()> {
    if xs.len() > max || !empty && xs.is_empty() || xs.windows(2).any(|p| p[0] >= p[1]) {
        return Err(Error::Invalid);
    }
    Ok(())
}
fn validate_files(files: &[Artifact]) -> Result<()> {
    if files.len() < 4 || files.len() > MAX_FILES {
        return Err(Error::Bounds);
    }
    let required = [
        Role::Binary,
        Role::HostConfig,
        Role::Bootstrap,
        Role::StrategyConfig,
    ];
    if files[..4].iter().map(|f| &f.role).ne(required.iter())
        || files.windows(2).any(|w| w[0].role >= w[1].role)
    {
        return Err(Error::Invalid);
    }
    let mut seen = std::collections::BTreeSet::new();
    for f in files {
        if !seen.insert(f.object.digest) {
            return Err(Error::Invalid);
        }
        match f.role {
            Role::Model(0) => return Err(Error::Invalid),
            Role::Dependency(k) if !(2..=11).contains(&k) => return Err(Error::Invalid),
            _ => {}
        }
        f.object.validate(if f.role == Role::Binary {
            MAX_BINARY
        } else {
            tickwire_recovery::MAX_OBJECT_BYTES as u64
        })?;
    }
    Ok(())
}
impl Method {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Restore { schema } if *schema != 0 => Ok(()),
            Self::Migrate {
                source_schema,
                target_schema,
                method_id,
            } if *source_schema != 0
                && *target_schema != 0
                && source_schema != target_schema
                && *method_id != 0 =>
            {
                Ok(())
            }
            Self::Rebuild {
                target_schema,
                contract,
            } if *target_schema != 0 && *contract != [0; 32] => Ok(()),
            _ => Err(Error::Invalid),
        }
    }
    pub fn check_source(&self, source: u64) -> Result<()> {
        self.validate()?;
        match self {
            Self::Restore { schema } if *schema != source => Err(Error::Unsupported),
            Self::Migrate { source_schema, .. } if *source_schema != source => {
                Err(Error::Unsupported)
            }
            _ => Ok(()),
        }
    }
}
impl Capabilities {
    pub fn validate(&self) -> Result<()> {
        if self.state_schema == 0 || self.input_schema == 0 || self.output_schema == 0 {
            return Err(Error::Invalid);
        }
        sorted(&self.products, 6, false)?;
        sorted(&self.position_modes, 3, false)?;
        sorted(&self.orders, 11, true)?;
        sorted(&self.methods, 32, false)?;
        for method in &self.methods {
            method.validate()?;
            if method.target_schema() != self.state_schema {
                return Err(Error::Invalid);
            }
        }
        Ok(())
    }
    pub fn check_requirements(&self, r: &Requirements) -> Result<()> {
        self.validate()?;
        sorted(&r.products, 6, false)?;
        sorted(&r.position_modes, 3, false)?;
        sorted(&r.orders, 11, true)?;
        if r.products.iter().any(|p| !self.products.contains(p))
            || r.position_modes
                .iter()
                .any(|p| !self.position_modes.contains(p))
            || r.orders.iter().any(|p| !self.orders.contains(p))
        {
            return Err(Error::Unsupported);
        }
        Ok(())
    }
}
impl InputPrefix {
    pub fn validate(&self) -> Result<()> {
        self.object
            .validate(tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
        if self.object.bytes < 68
            || self.stream == [0; 16]
            || self.first_sequence == 0
            || self.first_sequence >= 1 << 63
            || self.last_sequence >= 1 << 63
            || self.last_sequence < self.first_sequence - 1
            || self.last_sequence - self.first_sequence.saturating_sub(1) > 1_000_000
            || self.last_sequence == self.first_sequence - 1 && self.last_hash != self.previous_hash
        {
            return Err(Error::Invalid);
        }
        Ok(())
    }
    pub fn verify(&self, bytes: Vec<u8>) -> Result<tickwire_recovery::EncodedInputs> {
        self.validate()?;
        self.object
            .verify(&bytes, tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
        let input = tickwire_recovery::EncodedInputs::decode(bytes).map_err(|_| Error::Encoding)?;
        let b = &input.boundary;
        if b.stream != self.stream
            || b.first_sequence != self.first_sequence
            || b.last_sequence != self.last_sequence
            || b.previous_hash != self.previous_hash
            || b.last_hash != self.last_hash
        {
            return Err(Error::Identity);
        }
        Ok(input)
    }
}
