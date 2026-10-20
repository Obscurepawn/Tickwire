//! Deterministic business-event CBOR and logical hashing, without durability authority.
#![forbid(unsafe_code)]

mod cbor;
mod chain;

use cbor::{Reader, Writer};
use sha2::{Digest, Sha256};

pub type Hash = [u8; 32];
pub const SCHEMA_VERSION: u64 = 1;
pub const MAX_ENCODED_EVENT_BYTES: usize = 201;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Product {
    Spot,
    UsdM,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitProfile {
    LocalDurable,
    SyncReplica,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalStatus {
    Filled,
    Canceled,
    Rejected,
    Expired,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderIdentity {
    pub venue: u32,
    pub account: u64,
    pub product: Product,
    pub intent_id: u64,
    pub intent_seq: u64,
    pub intent_exec_epoch: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreparedIntent {
    pub order: OrderIdentity,
    pub quantity: u64,
    pub unit_budget: i128,
    pub fee_budget: i128,
    pub strategy_generation: u64,
    pub config_revision: u64,
    pub deadline: u64,
    pub recovery_object: u64,
    pub commit_profile: CommitProfile,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    PreparedIntent(PreparedIntent),
    Fill {
        order: OrderIdentity,
        trade_id: u64,
        quantity: u64,
        cost: i128,
    },
    Fee {
        order: OrderIdentity,
        fee_id: u64,
        amount: i128,
    },
    Terminal {
        order: OrderIdentity,
        status: TerminalStatus,
        cumulative: u64,
    },
    AbortedBeforeSend {
        order: OrderIdentity,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Record {
    pub risk_domain_id: [u8; 16],
    pub journal_epoch: [u8; 16],
    pub journal_seq: u64,
    pub writer_exec_epoch: u64,
    pub event: Event,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    TooLong,
    Truncated,
    NonCanonical,
    UnexpectedType,
    Schema,
    Field,
    Enum,
    IntegerOverflow,
    TrailingData,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainError {
    WrongDomain,
    WrongJournalEpoch,
    Sequence,
    SequenceExhausted,
    ExecutionEpochRollback,
    HashMismatch,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalChain {
    domain: [u8; 16],
    journal_epoch: [u8; 16],
    last_seq: u64,
    writer_exec_epoch: Option<u64>,
    hash: Hash,
}

impl Event {
    pub fn kind(&self) -> u64 {
        match self {
            Self::PreparedIntent(_) => 1,
            Self::Fill { .. } => 2,
            Self::Fee { .. } => 3,
            Self::Terminal { .. } => 4,
            Self::AbortedBeforeSend { .. } => 5,
        }
    }
    pub fn order(&self) -> OrderIdentity {
        match self {
            Self::PreparedIntent(p) => p.order,
            Self::Fill { order, .. }
            | Self::Fee { order, .. }
            | Self::Terminal { order, .. }
            | Self::AbortedBeforeSend { order } => *order,
        }
    }
}

/// Encodes schema 1. No business authorization or economic validation is performed.
pub fn encode(record: &Record) -> Vec<u8> {
    let mut w = Writer::new();
    w.map(7);
    w.uint(0);
    w.uint(SCHEMA_VERSION);
    w.uint(1);
    w.bytes(&record.risk_domain_id);
    w.uint(2);
    w.bytes(&record.journal_epoch);
    w.uint(3);
    w.uint(record.journal_seq);
    w.uint(4);
    w.uint(record.writer_exec_epoch);
    w.uint(5);
    w.uint(record.event.kind());
    w.uint(6);
    write_payload(&mut w, &record.event);
    w.finish()
}

/// Requires exactly one canonical schema-1 record, with no trailing bytes.
pub fn decode(bytes: &[u8]) -> Result<Record, DecodeError> {
    if bytes.len() > MAX_ENCODED_EVENT_BYTES {
        return Err(DecodeError::TooLong);
    }
    let mut r = Reader::new(bytes);
    r.map(7)?;
    r.key(0)?;
    if r.uint()? != SCHEMA_VERSION {
        return Err(DecodeError::Schema);
    }
    r.key(1)?;
    let risk_domain_id = r.bytes()?;
    r.key(2)?;
    let journal_epoch = r.bytes()?;
    r.key(3)?;
    let journal_seq = r.uint()?;
    r.key(4)?;
    let writer_exec_epoch = r.uint()?;
    r.key(5)?;
    let kind = r.uint()?;
    r.key(6)?;
    let event = read_payload(&mut r, kind)?;
    r.end()?;
    Ok(Record {
        risk_domain_id,
        journal_epoch,
        journal_seq,
        writer_exec_epoch,
        event,
    })
}

/// Hashes type, business fields (including the original intent epoch) and domain.
/// Writer epoch, journal placement and physical WAL placement are excluded.
pub fn business_digest(record: &Record) -> Hash {
    let mut w = Writer::new();
    w.array(5);
    w.text("tickwire.business-event");
    w.uint(SCHEMA_VERSION);
    w.bytes(&record.risk_domain_id);
    w.uint(record.event.kind());
    write_payload(&mut w, &record.event);
    Sha256::digest(w.finish()).into()
}

/// Binds this log writer's epoch to the stable business event digest.
/// This is the payload_hash input of the logical chain, not a commit proof.
pub fn logical_payload_hash(record: &Record) -> Hash {
    let mut w = Writer::new();
    w.array(4);
    w.text("tickwire.logical-payload");
    w.uint(SCHEMA_VERSION);
    w.uint(record.writer_exec_epoch);
    w.bytes(&business_digest(record));
    Sha256::digest(w.finish()).into()
}

fn write_payload(w: &mut Writer, event: &Event) {
    let len = match event {
        Event::PreparedIntent(_) => 14,
        Event::Fill { .. } => 9,
        Event::Fee { .. } | Event::Terminal { .. } => 8,
        Event::AbortedBeforeSend { .. } => 6,
    };
    w.map(len);
    let order = event.order();
    w.uint(0);
    w.uint(u64::from(order.venue));
    w.uint(1);
    w.uint(order.account);
    w.uint(2);
    w.uint(match order.product {
        Product::Spot => 1,
        Product::UsdM => 2,
    });
    w.uint(3);
    w.uint(order.intent_id);
    w.uint(4);
    w.uint(order.intent_seq);
    w.uint(5);
    w.uint(order.intent_exec_epoch);
    match event {
        Event::PreparedIntent(p) => {
            w.uint(6);
            w.uint(p.quantity);
            w.uint(7);
            w.int(p.unit_budget);
            w.uint(8);
            w.int(p.fee_budget);
            w.uint(9);
            w.uint(p.strategy_generation);
            w.uint(10);
            w.uint(p.config_revision);
            w.uint(11);
            w.uint(p.deadline);
            w.uint(12);
            w.uint(p.recovery_object);
            w.uint(13);
            w.uint(match p.commit_profile {
                CommitProfile::LocalDurable => 1,
                CommitProfile::SyncReplica => 2,
            });
        }
        Event::Fill {
            trade_id,
            quantity,
            cost,
            ..
        } => {
            w.uint(6);
            w.uint(*trade_id);
            w.uint(7);
            w.uint(*quantity);
            w.uint(8);
            w.int(*cost);
        }
        Event::Fee { fee_id, amount, .. } => {
            w.uint(6);
            w.uint(*fee_id);
            w.uint(7);
            w.int(*amount);
        }
        Event::Terminal {
            status, cumulative, ..
        } => {
            w.uint(6);
            w.uint(match status {
                TerminalStatus::Filled => 1,
                TerminalStatus::Canceled => 2,
                TerminalStatus::Rejected => 3,
                TerminalStatus::Expired => 4,
            });
            w.uint(7);
            w.uint(*cumulative);
        }
        Event::AbortedBeforeSend { .. } => {}
    }
}

fn read_payload(r: &mut Reader<'_>, kind: u64) -> Result<Event, DecodeError> {
    r.map(match kind {
        1 => 14,
        2 => 9,
        3 | 4 => 8,
        5 => 6,
        _ => return Err(DecodeError::Enum),
    })?;
    r.key(0)?;
    let venue = u32::try_from(r.uint()?).map_err(|_| DecodeError::IntegerOverflow)?;
    r.key(1)?;
    let account = r.uint()?;
    r.key(2)?;
    let product = match r.uint()? {
        1 => Product::Spot,
        2 => Product::UsdM,
        _ => return Err(DecodeError::Enum),
    };
    r.key(3)?;
    let intent_id = r.uint()?;
    r.key(4)?;
    let intent_seq = r.uint()?;
    r.key(5)?;
    let intent_exec_epoch = r.uint()?;
    let order = OrderIdentity {
        venue,
        account,
        product,
        intent_id,
        intent_seq,
        intent_exec_epoch,
    };
    Ok(match kind {
        1 => {
            r.key(6)?;
            let quantity = r.uint()?;
            r.key(7)?;
            let unit_budget = r.int()?;
            r.key(8)?;
            let fee_budget = r.int()?;
            r.key(9)?;
            let strategy_generation = r.uint()?;
            r.key(10)?;
            let config_revision = r.uint()?;
            r.key(11)?;
            let deadline = r.uint()?;
            r.key(12)?;
            let recovery_object = r.uint()?;
            r.key(13)?;
            let commit_profile = match r.uint()? {
                1 => CommitProfile::LocalDurable,
                2 => CommitProfile::SyncReplica,
                _ => return Err(DecodeError::Enum),
            };
            Event::PreparedIntent(PreparedIntent {
                order,
                quantity,
                unit_budget,
                fee_budget,
                strategy_generation,
                config_revision,
                deadline,
                recovery_object,
                commit_profile,
            })
        }
        2 => {
            r.key(6)?;
            let trade_id = r.uint()?;
            r.key(7)?;
            let quantity = r.uint()?;
            r.key(8)?;
            let cost = r.int()?;
            Event::Fill {
                order,
                trade_id,
                quantity,
                cost,
            }
        }
        3 => {
            r.key(6)?;
            let fee_id = r.uint()?;
            r.key(7)?;
            let amount = r.int()?;
            Event::Fee {
                order,
                fee_id,
                amount,
            }
        }
        4 => {
            r.key(6)?;
            let status = match r.uint()? {
                1 => TerminalStatus::Filled,
                2 => TerminalStatus::Canceled,
                3 => TerminalStatus::Rejected,
                4 => TerminalStatus::Expired,
                _ => return Err(DecodeError::Enum),
            };
            r.key(7)?;
            let cumulative = r.uint()?;
            Event::Terminal {
                order,
                status,
                cumulative,
            }
        }
        5 => Event::AbortedBeforeSend { order },
        _ => return Err(DecodeError::Enum),
    })
}

impl core::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "business event decoding failed: {self:?}")
    }
}
impl std::error::Error for DecodeError {}
impl core::fmt::Display for ChainError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "logical chain validation failed: {self:?}")
    }
}
impl std::error::Error for ChainError {}
