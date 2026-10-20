//! Single in-flight REST response owner sharing the durable private-input journal.
use crate::*;
use std::time::Instant;
use tickwire_account_gateway::{AccountGateway, Authority, Encoder, Status};
use tickwire_account_journal::Backend;
use tickwire_binance_private::{GapNotice, PrivateEvent};
use tickwire_transport::{IoBudget, http::ResponseDecoder};
pub(crate) mod binding;
mod persistence;

#[derive(Clone, Copy, Debug)]
pub struct RestConfig {
    pub connection_epoch: u64,
    pub max_response_bytes: usize,
    pub max_headers: usize,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestDisposition {
    Observed,
    NeedsReconciliation,
    Rejected,
    Unknown,
    NoBytes,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestOutcome {
    pub request: IntentKey,
    pub input: Option<InputKey>,
    pub disposition: RestDisposition,
    pub applied_events: usize,
}
#[derive(Clone)]
pub(crate) struct RestRecord {
    pub command: CanonicalCommand,
    pub query: Option<crate::reconciliation::TradeQuery>,
    pub order_query: Option<crate::reconciliation::orders::OrderQuery>,
    pub epoch: u64,
    pub received: u64,
    pub status: u16,
    pub raw: Vec<u8>,
    pub disposition: RestDisposition,
    pub events: Vec<CanonicalEvent>,
}
pub struct StoredRest<'a> {
    pub input: InputKey,
    pub command: &'a CanonicalCommand,
    pub query: Option<crate::reconciliation::TradeQuery>,
    pub order_query: Option<crate::reconciliation::orders::OrderQuery>,
    pub connection_epoch: u64,
    pub received_mono_ns: u64,
    pub status: u16,
    pub raw: &'a [u8],
    pub disposition: RestDisposition,
    pub events: &'a [CanonicalEvent],
}
struct Pending {
    command: CanonicalCommand,
    deadline: Instant,
}
pub struct RestOwner<S: Store, B: Backend, A: Authority, E: Encoder> {
    pub(crate) ingress: AccountIngress<S>,
    pub(crate) gateway: AccountGateway<B, A, E>,
    pub(crate) config: RestConfig,
    decoder: ResponseDecoder,
    raw: Vec<u8>,
    pending: Option<Pending>,
    last_mono: Option<u64>,
}
impl<S: Store, B: Backend, A: Authority, E: Encoder> RestOwner<S, B, A, E> {
    pub fn execution_connection_epoch(&self) -> u64 {
        self.config.connection_epoch
    }
    pub fn from_parts(
        ingress: AccountIngress<S>,
        gateway: AccountGateway<B, A, E>,
        config: RestConfig,
    ) -> Result<Self> {
        ingress.paired(&gateway)?;
        if config.connection_epoch == 0
            || config.max_response_bytes == 0
            || config.max_response_bytes > ingress.config.max_raw_bytes
        {
            return Err(Error::Identity);
        }
        let decoder = ResponseDecoder::new(config.max_response_bytes, config.max_headers)
            .map_err(Error::Transport)?;
        Ok(Self {
            ingress,
            gateway,
            config,
            decoder,
            raw: Vec::new(),
            pending: None,
            last_mono: None,
        })
    }
    pub fn submit(
        &mut self,
        command: CanonicalCommand,
        order_ids: &[[u8; 16]],
        evaluated_at: u64,
        deadline: Instant,
    ) -> Result<Status> {
        let key = intent(&command);
        if let Some(old) = self.gateway.command(key) {
            if old == &command {
                return self.gateway.status(key).ok_or(Error::Identity);
            }
            self.gateway.stop_sending();
            return Err(Error::Conflict);
        }
        if self.pending.is_some() {
            return Err(Error::Busy);
        }
        let status = self.ingress.submit(
            &mut self.gateway,
            command.clone(),
            order_ids,
            evaluated_at,
            deadline,
        )?;
        if status != Status::Aborted {
            self.pending = Some(Pending { command, deadline });
        }
        Ok(status)
    }
    pub fn poll(
        &mut self,
        now: Instant,
        received_mono_ns: u64,
        budget: IoBudget,
    ) -> Result<Option<RestOutcome>> {
        if budget.max_bytes == 0 || budget.max_syscalls == 0 {
            return Err(Error::Capacity);
        }
        if self.last_mono.is_some_and(|old| received_mono_ns < old) {
            self.gateway.stop_sending();
            return Err(Error::Identity);
        }
        self.last_mono = Some(received_mono_ns);
        // Even an EOF/error may have left a complete response in the bounded RX queue.
        if !self.gateway.halted() {
            let _ = self.gateway.poll(now, budget);
        }
        let mut bytes = [0u8; 4096];
        let mut drained = 0usize;
        while drained < budget.max_bytes {
            let limit = bytes.len().min(budget.max_bytes - drained);
            let n = self.gateway.read(&mut bytes[..limit]);
            if n == 0 {
                break;
            }
            drained += n;
            if self.pending.is_none() {
                self.gateway.stop_sending();
                return Err(Error::Identity);
            }
            if self
                .raw
                .len()
                .checked_add(n)
                .is_none_or(|n| n > self.config.max_response_bytes)
            {
                self.gateway.stop_sending();
                return self.finish_unknown(received_mono_ns);
            }
            self.raw.extend_from_slice(&bytes[..n]);
            if self.decoder.push(&bytes[..n]).is_err() {
                self.gateway.stop_sending();
                return self.finish_unknown(received_mono_ns);
            }
        }
        // A budget yield must drain retained EOF bytes on the following poll first.
        if drained == budget.max_bytes && drained != 0 {
            return Ok(None);
        }
        if self.gateway.halted() {
            self.decoder.finish();
        }
        let response = match self.decoder.next_response() {
            Ok(response) => response,
            Err(_) => {
                self.gateway.stop_sending();
                return self.finish_unknown(received_mono_ns);
            }
        };
        if let Some(response) = response {
            let extra = self.decoder.take_buffered();
            let Some(pending) = self.pending.take() else {
                self.gateway.stop_sending();
                return Err(Error::Identity);
            };
            let key = intent(&pending.command);
            let complete = self
                .gateway
                .output_progress(key)
                .is_some_and(|p| p.total_bytes > 0 && p.accepted_bytes == p.total_bytes);
            let raw = std::mem::take(&mut self.raw);
            let outcome = if complete && extra.is_empty() {
                self.ingress.persist_rest(
                    &mut self.gateway,
                    pending.command,
                    self.config.connection_epoch,
                    received_mono_ns,
                    response.status,
                    raw,
                    Some(&response.body),
                )
            } else {
                self.gateway.stop_sending();
                self.ingress.persist_rest(
                    &mut self.gateway,
                    pending.command,
                    self.config.connection_epoch,
                    received_mono_ns,
                    response.status,
                    raw,
                    None,
                )
            };
            self.decoder =
                ResponseDecoder::new(self.config.max_response_bytes, self.config.max_headers)
                    .map_err(Error::Transport)?;
            if outcome.is_err() {
                self.gateway.stop_sending();
            }
            return outcome.map(Some);
        }
        if let Some(pending) = &self.pending {
            let key = intent(&pending.command);
            if self.gateway.status(key) == Some(Status::Aborted) && self.raw.is_empty() {
                self.pending = None;
                return Ok(Some(RestOutcome {
                    request: key,
                    input: None,
                    disposition: RestDisposition::NoBytes,
                    applied_events: 0,
                }));
            }
            if self.gateway.halted() || now >= pending.deadline {
                self.gateway.stop_sending();
                return self.finish_unknown(received_mono_ns);
            }
        }
        Ok(None)
    }
    fn finish_unknown(&mut self, received: u64) -> Result<Option<RestOutcome>> {
        let Some(pending) = self.pending.take() else {
            return Ok(None);
        };
        let raw = std::mem::take(&mut self.raw);
        self.decoder =
            ResponseDecoder::new(self.config.max_response_bytes, self.config.max_headers)
                .map_err(Error::Transport)?;
        self.ingress
            .persist_rest(
                &mut self.gateway,
                pending.command,
                self.config.connection_epoch,
                received,
                0,
                raw,
                None,
            )
            .map(Some)
    }
    pub fn stop_sending(&mut self) {
        self.gateway.stop_sending();
    }
    pub fn latest_applied_input(&self) -> Option<InputKey> {
        Some(InputKey {
            stream: self.ingress.input_stream(),
            sequence: self.ingress.applied_position(),
        })
    }
    pub fn applied_position(&self) -> u64 {
        self.ingress.applied_position()
    }
    pub fn busy(&self) -> bool {
        self.pending.is_some()
    }
    pub fn ingest_private(&mut self, event: &PrivateEvent<'_>) -> Result<Outcome> {
        self.ingress.ingest(&mut self.gateway, event)
    }
    pub fn source_gap(&mut self, gap: &GapNotice) -> Result<InputKey> {
        self.ingress.source_gap(&mut self.gateway, gap)
    }
    pub fn replay_pending(&mut self, maximum: usize) -> Result<usize> {
        self.ingress.replay_pending(&mut self.gateway, maximum)
    }
    pub fn account(&self) -> &tickwire_account::Account {
        self.gateway.account()
    }
    pub fn status(&self, key: IntentKey) -> Option<Status> {
        self.gateway.status(key)
    }
    pub fn ingress(&self) -> &AccountIngress<S> {
        &self.ingress
    }
    pub fn with_ingress<T>(&mut self, operation: impl FnOnce(&mut AccountIngress<S>) -> T) -> T {
        operation(&mut self.ingress)
    }
    pub fn gateway(&self) -> &AccountGateway<B, A, E> {
        &self.gateway
    }
    pub fn with_gateway<T>(
        &mut self,
        operation: impl FnOnce(&mut AccountGateway<B, A, E>) -> T,
    ) -> T {
        operation(&mut self.gateway)
    }
    pub fn register(
        &mut self,
        registry: &tickwire_transport::mio::Registry,
        token: tickwire_transport::mio::Token,
    ) -> Result<()> {
        self.gateway
            .register(registry, token)
            .map_err(Error::Gateway)
    }
    pub fn reregister(
        &mut self,
        registry: &tickwire_transport::mio::Registry,
        token: tickwire_transport::mio::Token,
    ) -> Result<()> {
        self.gateway
            .reregister(registry, token)
            .map_err(Error::Gateway)
    }
}
