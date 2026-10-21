use crate::{CHUNK_BYTES, Change, Config, Error, Hash, Identity, ObjectCommand, Result};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub type ObjectKey = [u8; 16];
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Purpose {
    Candidate = 1,
    Projection = 2,
    Qualification = 3,
    Receipt = 4,
    PlanMetadata = 5,
}
impl Purpose {
    pub(crate) fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            1 => Ok(Self::Candidate),
            2 => Ok(Self::Projection),
            3 => Ok(Self::Qualification),
            4 => Ok(Self::Receipt),
            5 => Ok(Self::PlanMetadata),
            _ => Err(Error::Protocol),
        }
    }
    pub(crate) fn max_bytes(self) -> u32 {
        match self {
            Self::Candidate | Self::Qualification => 16 * 1024 * 1024,
            Self::Projection => 4 * 1024 * 1024,
            Self::Receipt | Self::PlanMetadata => 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectLimits {
    pub max_objects: u32,
    pub max_inflight: u32,
    pub max_payload_bytes: u64,
}
impl ObjectLimits {
    pub(crate) fn validate(self) -> Result<()> {
        if !(1..=4096).contains(&self.max_objects)
            || self.max_inflight == 0
            || self.max_inflight > self.max_objects.min(64)
            || !(1..=512 * 1024 * 1024).contains(&self.max_payload_bytes)
        {
            return Err(Error::Bounds);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectSpec {
    pub key: ObjectKey,
    pub owner: Identity,
    pub purpose: Purpose,
    pub length: u32,
    pub hash: Hash,
}
impl ObjectSpec {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.key == [0; 16] || self.owner == [0; 16] {
            return Err(Error::Identity);
        }
        if self.length == 0 || self.length > self.purpose.max_bytes() {
            return Err(Error::Bounds);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Object {
    spec: ObjectSpec,
    bytes: Vec<u8>,
}
impl Object {
    pub fn spec(&self) -> &ObjectSpec {
        &self.spec
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}
pub(crate) struct State {
    config: Config,
    initialized: bool,
    objects: BTreeMap<ObjectKey, Entry>,
    reserved: u64,
    inflight: u32,
}
struct Entry {
    spec: ObjectSpec,
    bytes: Vec<u8>,
    sealed: bool,
}
impl State {
    pub(crate) fn initialized(&self) -> bool {
        self.initialized
    }
    pub(crate) fn snapshot_entries(&self) -> impl Iterator<Item = (&ObjectSpec, &[u8], bool)> {
        self.objects
            .values()
            .map(|entry| (&entry.spec, entry.bytes.as_slice(), entry.sealed))
    }
    pub(crate) fn restore(
        config: Config,
        initialized: bool,
        entries: Vec<(ObjectSpec, Vec<u8>, bool)>,
    ) -> Result<Self> {
        let mut result = Self::new(config);
        if !initialized && !entries.is_empty() {
            return Err(Error::Protocol);
        }
        if entries.len() > config.objects.max_objects as usize {
            return Err(Error::Capacity);
        }
        let mut previous = None;
        for (spec, bytes, sealed) in entries {
            spec.validate()?;
            if previous.is_some_and(|key| key >= spec.key)
                || bytes.len() > spec.length as usize
                || (bytes.len() < spec.length as usize && !bytes.len().is_multiple_of(CHUNK_BYTES))
                || (sealed
                    && (bytes.len() != spec.length as usize
                        || <Hash>::from(Sha256::digest(&bytes)) != spec.hash))
            {
                return Err(Error::Protocol);
            }
            previous = Some(spec.key);
            result.reserved = result
                .reserved
                .checked_add(u64::from(spec.length))
                .ok_or(Error::Capacity)?;
            result.inflight = result
                .inflight
                .checked_add(u32::from(!sealed))
                .ok_or(Error::Capacity)?;
            if result.reserved > config.objects.max_payload_bytes
                || result.inflight > config.objects.max_inflight
            {
                return Err(Error::Capacity);
            }
            result.objects.insert(
                spec.key,
                Entry {
                    spec,
                    bytes,
                    sealed,
                },
            );
        }
        result.initialized = initialized;
        Ok(result)
    }
    pub(crate) fn new(config: Config) -> Self {
        Self {
            config,
            initialized: false,
            objects: BTreeMap::new(),
            reserved: 0,
            inflight: 0,
        }
    }
    pub(crate) fn initialize(&mut self, config: Config) -> Result<Result<Change>> {
        if config != self.config {
            return Err(Error::Identity);
        }
        self.initialized = true;
        Ok(Ok(Change::Initialized))
    }
    pub(crate) fn apply(&mut self, command: ObjectCommand) -> Result<Change> {
        if !self.initialized {
            return Err(Error::NotReady);
        }
        match command {
            ObjectCommand::Begin(spec) => self.begin(spec),
            ObjectCommand::Chunk {
                key,
                owner,
                offset,
                bytes,
            } => self.chunk(key, owner, offset, bytes),
            ObjectCommand::Seal { key, owner } => self.seal(key, owner),
        }
    }
    fn begin(&mut self, spec: ObjectSpec) -> Result<Change> {
        if let Some(old) = self.objects.get(&spec.key) {
            return if old.spec == spec {
                Ok(Change::Begun)
            } else {
                Err(Error::Conflict)
            };
        }
        let limits = self.config.objects;
        let reserved = self
            .reserved
            .checked_add(u64::from(spec.length))
            .ok_or(Error::Capacity)?;
        if self.objects.len() >= limits.max_objects as usize
            || self.inflight >= limits.max_inflight
            || reserved > limits.max_payload_bytes
        {
            return Err(Error::Capacity);
        }
        self.objects.insert(
            spec.key,
            Entry {
                spec,
                bytes: Vec::new(),
                sealed: false,
            },
        );
        self.reserved = reserved;
        self.inflight += 1;
        Ok(Change::Begun)
    }
    fn chunk(
        &mut self,
        key: ObjectKey,
        owner: Identity,
        offset: u32,
        bytes: Vec<u8>,
    ) -> Result<Change> {
        let entry = self.objects.get_mut(&key).ok_or(Error::NotFound)?;
        if entry.spec.owner != owner {
            return Err(Error::Identity);
        }
        let offset = offset as usize;
        let total = entry.spec.length as usize;
        if offset >= total
            || !offset.is_multiple_of(CHUNK_BYTES)
            || bytes.len() != CHUNK_BYTES.min(total - offset)
        {
            return Err(Error::Bounds);
        }
        if offset > entry.bytes.len() {
            return Err(Error::Incomplete);
        }
        if offset < entry.bytes.len() {
            if entry.bytes.get(offset..offset + bytes.len()) != Some(bytes.as_slice()) {
                return Err(Error::Conflict);
            }
        } else {
            entry.bytes.extend_from_slice(&bytes);
        }
        Ok(Change::ChunkStored {
            received: entry.bytes.len() as u32,
        })
    }
    fn seal(&mut self, key: ObjectKey, owner: Identity) -> Result<Change> {
        let entry = self.objects.get_mut(&key).ok_or(Error::NotFound)?;
        if entry.spec.owner != owner {
            return Err(Error::Identity);
        }
        if entry.sealed {
            return Ok(Change::Sealed);
        }
        if entry.bytes.len() != entry.spec.length as usize {
            return Err(Error::Incomplete);
        }
        let hash: Hash = Sha256::digest(&entry.bytes).into();
        if hash != entry.spec.hash {
            return Err(Error::Digest);
        }
        entry.sealed = true;
        self.inflight -= 1;
        Ok(Change::Sealed)
    }
    pub(crate) fn object(&self, key: ObjectKey) -> Option<Object> {
        self.objects
            .get(&key)
            .filter(|e| e.sealed)
            .map(|entry| Object {
                spec: entry.spec.clone(),
                bytes: entry.bytes.clone(),
            })
    }
}
