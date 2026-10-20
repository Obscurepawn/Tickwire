//! Service boundaries shared by live adapters and deterministic mocks.
use crate::{CommitReceipt, DurableRecord, Engine, Error, Fill, OrderRequest, SendState, Terminal};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceError {
    Unavailable,
    Pending,
    Conflict,
    MissingDependency,
    Capacity,
}
pub trait Journal {
    /// Idempotent by (domain, sequence); receipt binds the exact record and durable closure.
    /// Reactor adapters return Pending while asynchronous persistence is in progress.
    fn commit(&mut self, record: DurableRecord) -> Result<CommitReceipt, ServiceError>;
    fn committed(&self) -> &[DurableRecord];
    /// Persist a received economic or trusted transport fact before applying it.
    fn append_fact(&mut self, fact: ExecutionFact) -> Result<(), ServiceError>;
    fn facts(&self) -> &[ExecutionFact];
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitOutcome {
    /// Bytes accepted; response is still within its normal waiting window.
    PendingResponse,
    NoBytesAccepted,
    MayHaveSent,
    Acknowledged,
}
pub trait Exchange {
    /// Return PendingResponse without blocking on the network response.
    /// Err-like uncertainty is represented as MayHaveSent, never as a retry instruction.
    fn submit(&mut self, request: OrderRequest) -> SubmitOutcome;
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigRow {
    pub revision: u64,
    pub payload: Vec<u8>,
}
pub trait ConfigStore {
    fn load(&self) -> Result<ConfigRow, ServiceError>;
    fn compare_exchange(
        &mut self,
        expected: u64,
        payload: &[u8],
    ) -> Result<ConfigRow, ServiceError>;
}
pub trait ObjectStore {
    fn put_immutable(&mut self, key: u64, payload: &[u8]) -> Result<(), ServiceError>;
    fn get(&self, key: u64) -> Result<&[u8], ServiceError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionFact {
    /// Only a trusted transport proof that no output-capable bytes were accepted.
    /// The complete record binds the original request, epoch, and journal sequence.
    AbortedBeforeSend {
        record: DurableRecord,
    },
    Fill {
        intent: u64,
        fill: Fill,
    },
    Fee {
        intent: u64,
        id: u64,
        amount: i128,
    },
    Terminal {
        intent: u64,
        status: Terminal,
        cumulative: u64,
    },
}
impl ExecutionFact {
    pub fn apply<const N: usize, const T: usize>(self, e: &mut Engine<N, T>) -> Result<(), Error> {
        match self {
            Self::AbortedBeforeSend { record } => {
                let intent = record.request.key.intent;
                let current = match e.durable_record(intent) {
                    Ok(current) => current,
                    Err(error) => {
                        e.freeze();
                        return Err(error);
                    }
                };
                if current != record {
                    e.freeze();
                    return Err(Error::ReceiptMismatch);
                }
                let order = e.order(intent)?;
                if order.send == SendState::NotSubmitted
                    && order.terminal == Some(Terminal::Rejected)
                    && order.fee_released
                    && order.filled == 0
                    && order.observed_filled == 0
                    && order.spent == 0
                    && order.fees_paid == 0
                {
                    return Ok(());
                }
                e.transport_rejected_without_bytes(intent)
            }
            Self::Fill { intent, fill } => e.fill(intent, fill),
            Self::Fee { intent, id, amount } => e.fee(intent, id, amount),
            Self::Terminal {
                intent,
                status,
                cumulative,
            } => e.terminal(intent, status, cumulative),
        }
    }
}
