//! Durable local observations. These receipts confer no runtime authority.
#![forbid(unsafe_code)]
use serde::{Deserialize, Serialize};
use tickwire_journal::PhysicalFrontier;
mod codec;
mod journal;
mod ledger;
mod store;
pub use journal::Journal;
pub use ledger::OperationView;
pub type Hash = [u8; 32];
pub type Identity = [u8; 16];
#[derive(Debug)]
pub enum Error {
    Invalid(&'static str),
    Conflict,
    Capacity,
    Physical(tickwire_journal::Error),
    Encoding,
    Poisoned,
}
pub type Result<T> = core::result::Result<T, Error>;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub cluster: String,
    pub namespace: Identity,
    pub host: String,
    pub process: String,
    pub slot: Identity,
    pub locator_generation: u64,
}
#[derive(Debug, Clone)]
pub struct Config {
    pub scope: Scope,
    pub stream_epoch: Identity,
    pub writer_epoch: u64,
    pub max_operations: usize,
    pub max_recorded_bytes: usize,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Version {
    pub revision: u64,
    pub global_hash: Hash,
    pub projection_hash: Hash,
    pub policy_revision: u64,
    pub object_hash: Hash,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prepared {
    pub operation: Identity,
    pub base: Option<Version>,
    pub next: Version,
    pub control_commit_ref: Hash,
    pub target_set_hash: Hash,
    pub expected_instance: Identity,
    pub expected_boot: Identity,
    pub expected_host_boot: Identity,
    pub exec_generation: u64,
    pub projection_object_bytes: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileFact {
    pub render_hash: Hash,
    pub bytes: u64,
    pub device: u64,
    pub inode: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeFact {
    pub instance: Identity,
    pub boot: Identity,
    pub host_boot: Identity,
    pub exec_generation: u64,
    pub applied_event_seq: u64,
    pub witness_ref: Hash,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum Event {
    Prepared(Box<Prepared>),
    Desired {
        operation: Identity,
        prepared_ref: Hash,
        file: FileFact,
    },
    RuntimeObserved {
        operation: Identity,
        desired_ref: Hash,
        fact: RuntimeFact,
    },
    Effective {
        operation: Identity,
        runtime_ref: Hash,
        file: FileFact,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Phase {
    Prepared,
    Desired,
    RuntimeObserved,
    Effective,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    sequence: u64,
    record_hash: Hash,
    frontier: PhysicalFrontier,
}
impl Receipt {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn record_hash(&self) -> Hash {
        self.record_hash
    }
    pub fn frontier(&self) -> &PhysicalFrontier {
        &self.frontier
    }
}
impl From<tickwire_journal::Error> for Error {
    fn from(value: tickwire_journal::Error) -> Self {
        Self::Physical(value)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
