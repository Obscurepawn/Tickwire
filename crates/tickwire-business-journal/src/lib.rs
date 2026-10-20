//! Ordered, synchronized business WAL and a frozen-by-default recovery runtime.
#![forbid(unsafe_code)]

mod backend;
mod runtime;

pub use backend::{Backend, FileBackend};
pub use runtime::{Runtime, RuntimeView};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tickwire_execution::{Domain, DurableRecord, OrderRequest};
use tickwire_journal::{Config, Journal, PhysicalFrontier};
use tickwire_journal_codec::{self as codec, LogicalChain, Record};
use tickwire_recovery::{ClosureBinding, VerifiedClosure};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessConfig {
    pub physical: Config,
    pub domain: Domain,
    pub initial_capital: i128,
}
#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Physical(tickwire_journal::Error),
    Codec(tickwire_journal_codec::DecodeError),
    Chain(tickwire_journal_codec::ChainError),
    Engine(tickwire_execution::Error),
    Authorization(AuthorizationError),
    Invalid(&'static str),
    Quarantined(&'static str),
    Conflict,
    Frozen,
    UnsupportedProfile,
}
impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<tickwire_journal::Error> for Error {
    fn from(value: tickwire_journal::Error) -> Self {
        Self::Physical(value)
    }
}
impl From<tickwire_execution::Error> for Error {
    fn from(value: tickwire_execution::Error) -> Self {
        Self::Engine(value)
    }
}
impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum AuthorizationError {
    Unavailable,
    Denied,
    Recovery(tickwire_recovery::Error),
}
pub struct AuthorizationRequest<'a> {
    pub binding: &'a ClosureBinding,
    pub record: &'a DurableRecord,
    /// Exact immutable bytes whose SHA-256 equals binding.business_digest.
    pub intent_payload: &'a [u8],
}
/// Trusted authorization service. The closure capability alone does not assert
/// strategy permission. No permissive implementation is provided by this crate.
pub trait CommitAuthorizer {
    /// Fresh trusted time, fencing and validity state, sampled at each call.
    fn current_gate(&mut self)
    -> std::result::Result<tickwire_execution::Gate, AuthorizationError>;
    fn authorize(
        &mut self,
        request: AuthorizationRequest<'_>,
    ) -> std::result::Result<VerifiedClosure, AuthorizationError>;
    /// Verify an existing closure, allowing unchanged bytes to be resynchronized.
    /// Missing recovery dependencies must fail; they must never be re-created.
    fn verify_recovery(
        &mut self,
        request: AuthorizationRequest<'_>,
    ) -> std::result::Result<VerifiedClosure, AuthorizationError>;
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalAppendReceipt {
    journal_seq: u64,
    logical_hash: [u8; 32],
    physical: PhysicalFrontier,
}
impl WalAppendReceipt {
    pub fn journal_seq(&self) -> u64 {
        self.journal_seq
    }
    pub fn logical_hash(&self) -> [u8; 32] {
        self.logical_hash
    }
    pub fn physical_frontier(&self) -> &PhysicalFrontier {
        &self.physical
    }
}
pub struct BusinessJournal<S: Backend> {
    physical: Journal<S>,
    config: BusinessConfig,
    writer_exec_epoch: u64,
    chain: LogicalChain,
    events: Vec<Record>,
    intents: BTreeMap<u64, (DurableRecord, WalAppendReceipt)>,
    poisoned: bool,
}
impl<S: Backend> BusinessJournal<S> {
    pub fn create(mut backend: S, config: BusinessConfig, writer_exec_epoch: u64) -> Result<Self> {
        if writer_exec_epoch == 0 || config.initial_capital < 0 {
            return Err(Error::Invalid("writer epoch or capital"));
        }
        if backend.read_bootstrap()?.is_some()
            || backend.read_manifest()?.is_some()
            || !backend.segment_ids()?.is_empty()
        {
            return Err(Error::Invalid("business journal already exists"));
        }
        backend.publish_bootstrap(&bootstrap_bytes(&config)?)?;
        let physical = Journal::create(backend, config.physical.clone())?;
        let chain = LogicalChain::new(config.physical.domain, config.physical.stream_epoch);
        Ok(Self {
            physical,
            config,
            writer_exec_epoch,
            chain,
            events: Vec::new(),
            intents: BTreeMap::new(),
            poisoned: false,
        })
    }
    pub fn recover(
        mut backend: S,
        expected: BusinessConfig,
        writer_exec_epoch: u64,
        required: Option<&PhysicalFrontier>,
    ) -> Result<Self> {
        let bootstrap = backend
            .read_bootstrap()?
            .ok_or(Error::Quarantined("business bootstrap missing"))?;
        if bootstrap != bootstrap_bytes(&expected)? {
            return Err(Error::Quarantined(
                "business bootstrap does not match trusted expected baseline",
            ));
        }
        if writer_exec_epoch == 0 {
            return Err(Error::Invalid("writer epoch"));
        }
        let (physical, recovery) = Journal::recover(backend, required)?;
        if physical.config() != &expected.physical {
            return Err(Error::Quarantined("physical identity/config mismatch"));
        }
        if recovery
            .frontier
            .as_ref()
            .is_some_and(|f| f.exec_epoch > writer_exec_epoch)
        {
            return Err(Error::Quarantined("writer epoch rollback"));
        }
        let mut result = Self {
            physical,
            chain: LogicalChain::new(expected.physical.domain, expected.physical.stream_epoch),
            config: expected,
            writer_exec_epoch,
            events: Vec::new(),
            intents: BTreeMap::new(),
            poisoned: false,
        };
        for raw in recovery.records {
            let record = codec::decode(&raw.payload).map_err(Error::Codec)?;
            if record.journal_seq != raw.seq || record.writer_exec_epoch != raw.exec_epoch {
                return Err(Error::Quarantined(
                    "physical/business record identity mismatch",
                ));
            }
            result.check_domain(record.event.order())?;
            let hash = result.chain.append(&record).map_err(Error::Chain)?;
            if let codec::Event::PreparedIntent(_) = record.event {
                let intent = request_from_record(&record)?;
                if intent.sequence == 0 || intent.request.epoch != record.writer_exec_epoch {
                    return Err(Error::Quarantined("prepared identity mismatch"));
                }
                if result.intents.contains_key(&intent.request.key.intent)
                    || result
                        .intents
                        .values()
                        .any(|(r, _)| r.sequence == intent.sequence)
                {
                    return Err(Error::Quarantined("duplicate prepared identity"));
                }
                if intent.request.commit_profile != tickwire_execution::CommitProfile::LocalDurable
                {
                    return Err(Error::UnsupportedProfile);
                }
                let receipt = WalAppendReceipt {
                    journal_seq: record.journal_seq,
                    logical_hash: hash,
                    physical: recovery
                        .frontier
                        .clone()
                        .ok_or(Error::Quarantined("missing physical frontier"))?,
                };
                result
                    .intents
                    .insert(intent.request.key.intent, (intent, receipt));
            }
            result.events.push(record);
        }
        Ok(result)
    }
    pub fn commit_intent(
        &mut self,
        record: DurableRecord,
        closure: &VerifiedClosure,
    ) -> Result<WalAppendReceipt> {
        if self.poisoned {
            return Err(Error::Frozen);
        }
        check_request_domain(&self.config, record.request)?;
        if record.request.commit_profile != tickwire_execution::CommitProfile::LocalDurable {
            return Err(Error::UnsupportedProfile);
        }
        if self
            .intents
            .get(&record.request.key.intent)
            .is_some_and(|(old, _)| *old != record)
            || self
                .intents
                .values()
                .any(|(old, _)| old.sequence == record.sequence && *old != record)
        {
            self.poisoned = true;
            return Err(Error::Conflict);
        }
        let seq = self
            .position()
            .checked_add(1)
            .ok_or(Error::Invalid("journal sequence exhausted"))?;
        let event = intent_record(&self.config, self.writer_exec_epoch, seq, record);
        if closure.binding() != &closure_binding(&event)? {
            return Err(Error::Invalid("closure does not bind intent"));
        }
        if closure.command_digest() != Some(record.request.command_digest) {
            return Err(Error::Invalid("closure does not include command"));
        }
        if let Some((old, receipt)) = self.intents.get(&record.request.key.intent)
            && *old == record
        {
            return Ok(receipt.clone());
        }
        if record.sequence == 0 || record.request.epoch != self.writer_exec_epoch {
            return Err(Error::Invalid("prepared sequence/epoch"));
        }
        let receipt = self.append(event)?;
        self.intents
            .insert(record.request.key.intent, (record, receipt.clone()));
        Ok(receipt)
    }
    pub fn append_fact(&mut self, event: codec::Event) -> Result<WalAppendReceipt> {
        if matches!(event, codec::Event::PreparedIntent(_)) {
            return Err(Error::Invalid("use commit_intent"));
        }
        self.check_domain(event.order())?;
        let journal_seq = self
            .position()
            .checked_add(1)
            .ok_or(Error::Invalid("journal sequence exhausted"))?;
        self.append(Record {
            risk_domain_id: self.config.physical.domain,
            journal_epoch: self.config.physical.stream_epoch,
            journal_seq,
            writer_exec_epoch: self.writer_exec_epoch,
            event,
        })
    }
    pub fn events(&self) -> &[Record] {
        &self.events
    }
    pub fn position(&self) -> u64 {
        self.chain.last_seq()
    }
    pub fn logical_hash(&self) -> [u8; 32] {
        self.chain.hash()
    }
    pub fn config(&self) -> &BusinessConfig {
        &self.config
    }
    fn check_domain(&self, order: codec::OrderIdentity) -> Result<()> {
        if order.venue != self.config.domain.venue
            || order.account != self.config.domain.account
            || to_product(order.product) != self.config.domain.product
        {
            return Err(Error::Quarantined("event route differs from risk domain"));
        }
        Ok(())
    }
    fn append(&mut self, record: Record) -> Result<WalAppendReceipt> {
        if self.poisoned {
            return Err(Error::Frozen);
        }
        let mut chain = self.chain;
        let hash = chain.append(&record).map_err(Error::Chain)?;
        let bytes = codec::encode(&record);
        let synced = match self
            .physical
            .append_batch(self.writer_exec_epoch, &[&bytes])
        {
            Ok(receipt) => receipt,
            Err(e) => {
                self.poisoned = true;
                return Err(e.into());
            }
        };
        let receipt = WalAppendReceipt {
            journal_seq: record.journal_seq,
            logical_hash: hash,
            physical: synced.frontier().clone(),
        };
        self.chain = chain;
        self.events.push(record);
        Ok(receipt)
    }
}

/// Canonical intent data used by a trusted authorizer to obtain closure storage.
pub fn intent_record(
    config: &BusinessConfig,
    writer_exec_epoch: u64,
    journal_seq: u64,
    record: DurableRecord,
) -> Record {
    let r = record.request;
    Record {
        risk_domain_id: config.physical.domain,
        journal_epoch: config.physical.stream_epoch,
        journal_seq,
        writer_exec_epoch,
        event: codec::Event::PreparedIntent(codec::PreparedIntent {
            command_digest: r.command_digest,
            order: codec::OrderIdentity {
                venue: r.key.venue,
                account: r.key.account,
                product: from_product(r.key.product),
                intent_id: r.key.intent,
                intent_seq: record.sequence,
                intent_exec_epoch: r.epoch,
            },
            quantity: r.quantity,
            unit_budget: r.unit_budget,
            fee_budget: r.fee_budget,
            strategy_generation: r.strategy_generation,
            config_revision: r.config_revision,
            deadline: r.deadline,
            recovery_object: r.recovery_object,
            commit_profile: match r.commit_profile {
                tickwire_execution::CommitProfile::LocalDurable => {
                    codec::CommitProfile::LocalDurable
                }
                tickwire_execution::CommitProfile::SyncReplica => codec::CommitProfile::SyncReplica,
            },
        }),
    }
}
pub fn closure_binding(record: &Record) -> Result<ClosureBinding> {
    let codec::Event::PreparedIntent(p) = record.event else {
        return Err(Error::Invalid("closure requires prepared intent"));
    };
    Ok(ClosureBinding {
        risk_domain_id: record.risk_domain_id,
        intent_id: p.order.intent_id,
        intent_exec_epoch: p.order.intent_exec_epoch,
        business_digest: codec::business_digest(record),
        strategy_generation: p.strategy_generation,
        config_revision: p.config_revision,
        recovery_object: p.recovery_object,
    })
}
fn request_from_record(record: &Record) -> Result<DurableRecord> {
    let codec::Event::PreparedIntent(p) = record.event else {
        return Err(Error::Invalid("expected prepared intent"));
    };
    Ok(DurableRecord {
        sequence: p.order.intent_seq,
        request: OrderRequest {
            command_digest: p.command_digest,
            key: tickwire_execution::OrderKey {
                venue: p.order.venue,
                account: p.order.account,
                product: to_product(p.order.product),
                intent: p.order.intent_id,
            },
            quantity: p.quantity,
            unit_budget: p.unit_budget,
            fee_budget: p.fee_budget,
            epoch: p.order.intent_exec_epoch,
            strategy_generation: p.strategy_generation,
            config_revision: p.config_revision,
            deadline: p.deadline,
            recovery_object: p.recovery_object,
            commit_profile: match p.commit_profile {
                codec::CommitProfile::LocalDurable => {
                    tickwire_execution::CommitProfile::LocalDurable
                }
                codec::CommitProfile::SyncReplica => tickwire_execution::CommitProfile::SyncReplica,
            },
        },
    })
}
fn check_request_domain(config: &BusinessConfig, request: OrderRequest) -> Result<()> {
    if request.key.domain() != config.domain {
        return Err(Error::Invalid("request domain"));
    }
    Ok(())
}
fn from_product(p: tickwire_execution::Product) -> codec::Product {
    match p {
        tickwire_execution::Product::Spot => codec::Product::Spot,
        tickwire_execution::Product::UsdM => codec::Product::UsdM,
    }
}
fn to_product(p: codec::Product) -> tickwire_execution::Product {
    match p {
        codec::Product::Spot => tickwire_execution::Product::Spot,
        codec::Product::UsdM => tickwire_execution::Product::UsdM,
    }
}
#[derive(Serialize, Deserialize)]
struct Bootstrap {
    version: u64,
    codec_schema: u64,
    physical: Config,
    venue: u32,
    account: u64,
    product: u8,
    initial_capital: String,
}
#[derive(Serialize, Deserialize)]
struct BootstrapEnvelope {
    body: Bootstrap,
    sha256: [u8; 32],
}
fn bootstrap_bytes(config: &BusinessConfig) -> Result<Vec<u8>> {
    if config.initial_capital < 0 {
        return Err(Error::Invalid("negative capital"));
    }
    let body = Bootstrap {
        version: 1,
        codec_schema: codec::SCHEMA_VERSION,
        physical: config.physical.clone(),
        venue: config.domain.venue,
        account: config.domain.account,
        product: match config.domain.product {
            tickwire_execution::Product::Spot => 1,
            tickwire_execution::Product::UsdM => 2,
        },
        initial_capital: config.initial_capital.to_string(),
    };
    let bytes = serde_json::to_vec(&body).map_err(|_| Error::Invalid("bootstrap encoding"))?;
    serde_json::to_vec(&BootstrapEnvelope {
        body,
        sha256: Sha256::digest(bytes).into(),
    })
    .map_err(|_| Error::Invalid("bootstrap encoding"))
}
