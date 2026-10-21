//! Deterministic control state over the durable control consensus group.
//! Object replication alone does not authorize a configuration deployment.
#![forbid(unsafe_code)]

pub mod account_facts;
pub mod api;
pub mod auth;
pub mod authorization;
pub mod bootstrap;
mod codec;
pub mod credential_facts;
pub mod deployment;
pub mod deployment_owner;
pub mod evidence;
pub mod https;
mod objects;
mod peer_driver;
pub mod qualification;
pub mod request;
pub mod resource_facts;
pub mod snapshot;
pub mod source_bundle;
pub use objects::{Object, ObjectKey, ObjectLimits, ObjectSpec, Purpose};
pub use peer_driver::ReplicaControl;
use tickwire_control_raft::{Applied, Message, Node, Proposal, ReadId};
use tickwire_journal::store::Store;

pub const CHUNK_BYTES: usize = 64 * 1024;
pub const MAX_COMMAND_BYTES: usize = 1024 * 1024;
pub type Hash = [u8; 32];
pub type Identity = [u8; 16];
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Identity,
    Bounds,
    Protocol,
    NotReady,
    Conflict,
    NotFound,
    Incomplete,
    Digest,
    Capacity,
    Consensus(tickwire_control_raft::Error),
    Poisoned,
    Authentication,
    Authorization,
    Transport,
    Configuration(tickwire_config::Error),
}
pub type Result<T> = core::result::Result<T, Error>;
impl From<tickwire_control_raft::Error> for Error {
    fn from(error: tickwire_control_raft::Error) -> Self {
        Self::Consensus(error)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    pub cluster: Identity,
    pub objects: ObjectLimits,
}
impl Config {
    fn validate(self) -> Result<()> {
        if self.cluster == [0; 16] {
            return Err(Error::Identity);
        }
        self.objects.validate()
    }
}
/// Internal consensus command. This is not a public API authorization token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectCommand {
    Begin(ObjectSpec),
    Chunk {
        key: ObjectKey,
        owner: Identity,
        offset: u32,
        bytes: Vec<u8>,
    },
    Seal {
        key: ObjectKey,
        owner: Identity,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Change {
    Initialized,
    Begun,
    ChunkStored { received: u32 },
    Sealed,
    Bootstrapped { revision: u64 },
}
pub struct AppliedResult {
    applied: Applied,
    result: Result<Change>,
}
impl AppliedResult {
    pub fn matches(&self, proposal: &Proposal) -> bool {
        proposal.matches(&self.applied)
    }
    pub fn index(&self) -> u64 {
        self.applied.index()
    }
    pub fn result(&self) -> Result<Change> {
        self.result
    }
}
#[derive(Default)]
pub struct Output {
    pub messages: Vec<Message>,
    pub applied: Vec<AppliedResult>,
    pub more_ready: bool,
}
#[derive(Debug)]
pub enum Read {
    Pending,
    Ready(Option<Object>),
}
/// A single-owner driver. The caller supplies authenticated peer identities to
/// `step`; external users reach this through the service authentication layer.
pub struct Replica<S: Store> {
    node: Node<S>,
    config: Config,
    state: objects::State,
    bootstrap: bootstrap::State,
    deployment: Option<deployment_owner::Owner>,
    scanned_through: u64,
    poisoned: bool,
}
impl<S: Store> Replica<S> {
    pub fn new(node: Node<S>, config: Config) -> Result<Self> {
        Self::open(node, config, None)
    }
    pub fn new_with_bootstrap(
        node: Node<S>,
        config: Config,
        trust: bootstrap::BootstrapTrust,
    ) -> Result<Self> {
        if trust.cluster() != config.cluster {
            return Err(Error::Identity);
        }
        Self::open(node, config, Some(trust))
    }
    fn open(
        node: Node<S>,
        config: Config,
        trust: Option<bootstrap::BootstrapTrust>,
    ) -> Result<Self> {
        config.validate()?;
        if node.snapshot_base_index() != 0 {
            return Err(Error::NotReady);
        }
        let mut out = Self {
            node,
            config,
            state: objects::State::new(config),
            bootstrap: bootstrap::State::new(trust),
            deployment: None,
            scanned_through: 0,
            poisoned: false,
        };
        for applied in out.node.history() {
            let _ = out.apply(&applied)?;
        }
        out.scanned_through = out.node.applied_index();
        Ok(out)
    }
    fn healthy(&self) -> Result<()> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }
    pub fn tick(&mut self) -> Result<()> {
        self.healthy()?;
        self.node.tick().map_err(Into::into)
    }
    pub fn campaign(&mut self) -> Result<()> {
        self.healthy()?;
        self.node.campaign().map_err(Into::into)
    }
    pub fn step(&mut self, peer: u64, message: Message) -> Result<()> {
        self.healthy()?;
        self.node.step(peer, message).map_err(Into::into)
    }
    pub fn initialize(&mut self) -> Result<Proposal> {
        self.healthy()?;
        self.node
            .propose(codec::initialize_with_trust(
                self.config,
                self.bootstrap.trust_hash(),
            ))
            .map_err(Into::into)
    }
    pub fn bootstrap(&mut self, request: bootstrap::Bootstrap) -> Result<Proposal> {
        self.healthy()?;
        self.node
            .propose(codec::bootstrap(self.config.cluster, &request)?)
            .map_err(Into::into)
    }
    pub fn propose(&mut self, command: ObjectCommand) -> Result<Proposal> {
        self.healthy()?;
        self.node
            .propose(codec::encode(self.config.cluster, &command)?)
            .map_err(Into::into)
    }
    pub fn drive(&mut self) -> Result<Output> {
        self.drive_with_budget(tickwire_control_raft::driver::ReadyBudget::new(128)?)
    }
    fn drive_with_budget(
        &mut self,
        budget: tickwire_control_raft::driver::ReadyBudget,
    ) -> Result<Output> {
        self.healthy()?;
        let result = self.drive_inner(budget);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn drive_inner(
        &mut self,
        budget: tickwire_control_raft::driver::ReadyBudget,
    ) -> Result<Output> {
        let mut restore = snapshot::ReplicaRestore::new(
            self.config,
            self.node.status().group,
            &mut self.state,
            &mut self.bootstrap,
            &mut self.scanned_through,
        );
        let output = self
            .node
            .drive_with_snapshot(budget, &mut restore)
            .map_err(|error| match error {
                tickwire_control_raft::snapshot::SnapshotDriveError::Raft(e) => Error::Consensus(e),
                tickwire_control_raft::snapshot::SnapshotDriveError::Application(e) => e,
            })?;
        let through = self.node.applied_index();
        let mut applied_results = Vec::with_capacity(output.applied.len());
        for applied in output.applied {
            let result = self.apply(&applied)?;
            applied_results.push(AppliedResult { applied, result });
        }
        self.scanned_through = through;
        Ok(Output {
            messages: output.messages,
            applied: applied_results,
            more_ready: output.more_ready,
        })
    }
    fn apply(&mut self, applied: &Applied) -> Result<Result<Change>> {
        if applied.index() <= self.scanned_through {
            return Err(Error::Protocol);
        }
        let command = codec::decode(self.config.cluster, applied.command())?;
        let result = match command {
            codec::Decoded::Initialize(config) => {
                self.bootstrap.initialize(None)?;
                self.state.initialize(config)?
            }
            codec::Decoded::InitializeTrusted(config, hash) => {
                self.bootstrap.initialize(Some(hash))?;
                self.state.initialize(config)?
            }
            codec::Decoded::Object(command) => self.state.apply(command),
            codec::Decoded::Bootstrap(request) => self.bootstrap.apply(
                request,
                &self.state,
                self.node.configured_voters(),
                applied.index(),
                applied.term(),
            ),
        };
        self.scanned_through = applied.index();
        Ok(result)
    }
    pub fn request_read(&mut self) -> Result<ReadId> {
        self.healthy()?;
        self.node.request_read().map_err(Into::into)
    }
    pub fn read_object(&mut self, ticket: &ReadId, key: ObjectKey) -> Result<Read> {
        self.healthy()?;
        let Some(barrier) = self.node.take_read(ticket)? else {
            return Ok(Read::Pending);
        };
        if self.scanned_through < barrier.index() {
            return Err(Error::NotReady);
        }
        Ok(Read::Ready(self.state.object(key)))
    }
    pub fn read_head(&mut self, ticket: &ReadId) -> Result<bootstrap::HeadRead> {
        self.healthy()?;
        let Some(barrier) = self.node.take_read(ticket)? else {
            return Ok(bootstrap::HeadRead::Pending);
        };
        if self.scanned_through < barrier.index() {
            return Err(Error::NotReady);
        }
        Ok(bootstrap::HeadRead::Ready(
            self.bootstrap.head().map(Box::new),
        ))
    }
    pub fn is_leader(&self) -> bool {
        !self.poisoned && self.node.is_leader()
    }
    pub fn scanned_through(&self) -> u64 {
        self.scanned_through
    }
}
