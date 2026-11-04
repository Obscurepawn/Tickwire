//! Single-flight private session. AccountGateway remains the sole send and journal owner.
use crate::{OrderSnapshot, Reply, ReportError, SpotEncoder, SpotVenue, decode_reply, hex};
use std::time::Instant;
use tickwire_account::IntentKey;
use tickwire_account_gateway::{AccountGateway, Authority, Status};
use tickwire_account_journal::Backend;
use tickwire_transport::{IoBudget, http::ResponseDecoder};
use tickwire_venue as v;

#[derive(Debug)]
pub enum SessionError {
    Gateway(tickwire_account_gateway::Error),
    Transport(tickwire_transport::Error),
    Report(ReportError),
    RecoveryWriterEpoch { configured: u64, durable_floor: u64 },
    Identity,
    Busy,
}
impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for SessionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Gateway(e) => Some(e),
            Self::Transport(e) => Some(e),
            _ => None,
        }
    }
}
type Result<T> = std::result::Result<T, SessionError>;
pub struct RestSession<'a, B: Backend, A: Authority, T>
where
    T: FnMut() -> std::result::Result<u64, tickwire_gateway::Error>,
{
    gateway: AccountGateway<B, A, SpotEncoder<'a, T>>,
    decoder: ResponseDecoder,
    pending: Option<(IntentKey, v::CanonicalCommand, Instant)>,
    connection_epoch: u64,
}
impl<'a, B: Backend, A: Authority, T: FnMut() -> std::result::Result<u64, tickwire_gateway::Error>>
    RestSession<'a, B, A, T>
{
    pub fn new(
        gateway: AccountGateway<B, A, SpotEncoder<'a, T>>,
        connection_epoch: u64,
    ) -> Result<Self> {
        if let Some(durable_floor) = gateway.recovery_writer_floor()
            && gateway.writer_epoch() <= durable_floor
        {
            return Err(SessionError::RecoveryWriterEpoch {
                configured: gateway.writer_epoch(),
                durable_floor,
            });
        }
        if connection_epoch == 0
            || SpotVenue::from_id(gateway.baseline().snapshot().scope.venue).is_none()
        {
            return Err(SessionError::Identity);
        }
        Ok(Self {
            gateway,
            decoder: ResponseDecoder::new(256 * 1024, 128).map_err(SessionError::Transport)?,
            pending: None,
            connection_epoch,
        })
    }
    pub fn gateway(&self) -> &AccountGateway<B, A, SpotEncoder<'a, T>> {
        &self.gateway
    }
    /// Sole account-owner access for applying authenticated cash evidence and qualified recovery.
    pub fn gateway_mut(&mut self) -> &mut AccountGateway<B, A, SpotEncoder<'a, T>> {
        &mut self.gateway
    }
    pub fn connection_epoch(&self) -> u64 {
        self.connection_epoch
    }
    pub fn reactivate(
        &mut self,
        binding: tickwire_account::Reactivation,
        connection: Option<tickwire_account_gateway::ConnectionConfig>,
        verifier: &mut impl tickwire_account_gateway::ReactivationVerifier,
    ) -> Result<tickwire_account_gateway::ActivationReceipt> {
        let epoch = binding.execution_connection_epoch;
        if self.pending.is_some()
            || (connection.is_some() && epoch <= self.connection_epoch)
            || (connection.is_none() && epoch != self.connection_epoch)
        {
            return Err(SessionError::Identity);
        }
        let decoder = ResponseDecoder::new(256 * 1024, 128).map_err(SessionError::Transport)?;
        let receipt = self
            .gateway
            .reactivate(binding, connection, verifier)
            .map_err(SessionError::Gateway)?;
        self.decoder = decoder;
        self.connection_epoch = epoch;
        Ok(receipt)
    }
    /// Stops output and freezes the account when the owner loses its trusted input or routing identity.
    pub fn stop_sending(&mut self) {
        self.gateway.stop_sending();
    }
    /// Records one authenticated private trade through the same durable account owner.
    pub fn record_fill(
        &mut self,
        source: crate::FillSource<'_>,
        row: &[u8],
    ) -> Result<tickwire_account::Applied> {
        // Re-reading an old source position must reproduce the bytes written by that
        // version. Legacy trade-only observations encoded the missing lifecycle as Unknown.
        let legacy = self.gateway.ingress_event(source.input_stream, source.input_sequence)
            .is_some_and(|e| matches!(&e.event().body, v::VenueEventBody::Fill(f) if f.observation.status == v::OrderStatus::Unknown));
        let event = self
            .gateway
            .command(source.intent)
            .ok_or(ReportError::Identity)
            .and_then(|cmd| {
                crate::fills::normalize(
                    cmd,
                    &source,
                    &self.gateway.baseline().snapshot().assets,
                    row,
                    if legacy {
                        v::OrderStatus::Unknown
                    } else {
                        v::OrderStatus::Unreported
                    },
                )
            });
        let event = match event {
            Ok(e) => e,
            Err(e) => {
                self.gateway.freeze_account();
                return Err(SessionError::Report(e));
            }
        };
        self.gateway
            .record_ingress_event(source.input_stream, source.input_sequence, event)
            .map_err(SessionError::Gateway)
    }
    pub fn submit(
        &mut self,
        command: v::CanonicalCommand,
        evaluated_at: u64,
        deadline: Instant,
    ) -> Result<Status> {
        let r = &command.command().context.request;
        let intent = IntentKey {
            epoch: r.intent_exec_epoch,
            intent_id: r.intent_id,
        };
        // Let the gateway enforce same-byte retries and freeze conflicting reuse,
        // including while the original request is waiting for its HTTP response.
        if self.gateway.command(intent).is_some() {
            return self
                .gateway
                .submit(command, evaluated_at, deadline)
                .map_err(SessionError::Gateway);
        }
        if self.pending.is_some() {
            return Err(SessionError::Busy);
        }
        let result = self
            .gateway
            .submit(command.clone(), evaluated_at, deadline)
            .map_err(SessionError::Gateway)?;
        if result != Status::Aborted {
            self.pending = Some((intent, command, deadline));
        }
        Ok(result)
    }
    /// Drives TLS with a bounded byte/syscall budget. A missing reply never re-submits.
    pub fn poll(
        &mut self,
        now: Instant,
        received_mono_ns: u64,
        budget: IoBudget,
    ) -> Result<Option<Reply>> {
        if budget.max_bytes == 0 || budget.max_syscalls == 0 || received_mono_ns == 0 {
            return Err(SessionError::Identity);
        }
        let result = self.drive(now, received_mono_ns, budget);
        if result.is_err() {
            self.gateway.stop_sending();
        }
        result
    }
    fn drive(
        &mut self,
        now: Instant,
        received_mono_ns: u64,
        budget: IoBudget,
    ) -> Result<Option<Reply>> {
        let transport = self.gateway.poll(now, budget);
        let mut buffer = [0; 4096];
        let mut remaining = budget.max_bytes;
        let mut exhausted = false;
        while remaining != 0 {
            let cap = remaining.min(buffer.len());
            let n = self.gateway.read(&mut buffer[..cap]);
            if n == 0 {
                exhausted = true;
                break;
            }
            if self.pending.is_none() {
                return Err(SessionError::Identity);
            }
            self.decoder
                .push(&buffer[..n])
                .map_err(SessionError::Transport)?;
            remaining -= n;
        }
        if exhausted && self.gateway.halted() {
            self.decoder.finish();
        }
        if let Some(raw) = self
            .decoder
            .next_response()
            .map_err(SessionError::Transport)?
        {
            let (intent, cmd, _) = self.pending.take().ok_or(SessionError::Identity)?;
            if self
                .gateway
                .output_progress(intent)
                .is_none_or(|p| p.accepted_bytes != p.total_bytes)
                || !self.decoder.take_buffered().is_empty()
            {
                return Err(SessionError::Identity);
            }
            let reply = decode_reply(&cmd, raw.status, &raw.body).map_err(SessionError::Report)?;
            match &reply {
                Reply::Order(snapshot) => self.observe(&cmd, snapshot, received_mono_ns)?,
                Reply::Unknown | Reply::RequestIssue { .. } => self.gateway.freeze_account(),
                Reply::Acknowledged { .. } => {}
            }
            return Ok(Some(reply));
        }
        if let Some((key, _, deadline)) = &self.pending {
            if self.gateway.status(*key) == Some(Status::Aborted) {
                self.pending = None;
                return Ok(None);
            }
            if now >= *deadline || exhausted && (self.gateway.halted() || transport.is_err()) {
                self.gateway.stop_sending();
                self.pending = None;
                return Ok(Some(Reply::Unknown));
            }
        } else if let Err(e) = transport {
            return Err(SessionError::Gateway(e));
        }
        Ok(None)
    }
    fn observe(
        &mut self,
        cmd: &v::CanonicalCommand,
        snapshot: &OrderSnapshot,
        received: u64,
    ) -> Result<()> {
        let c = cmd.command();
        let v::CommandBody::Query(target) = &c.body else {
            return Err(SessionError::Identity);
        };
        let client = snapshot.client_id.as_ref().ok_or(SessionError::Identity)?;
        let mut candidates = self.gateway.account().order_keys().filter(|k| {
            k.intent.epoch == target.original_intent_exec_epoch
                && k.client_id == *client
                && self.gateway.command(k.intent).is_some_and(|original| {
                    original.command().context.instrument == c.context.instrument
                })
        });
        let original = candidates.next().ok_or(SessionError::Identity)?.intent;
        if candidates.next().is_some() {
            return Err(SessionError::Identity);
        }
        drop(candidates);
        let original_command = self
            .gateway
            .command(original)
            .ok_or(SessionError::Identity)?;
        let v::CommandBody::New(order) = &original_command.command().body else {
            return Err(SessionError::Identity);
        };
        let v::Quantity::Base(size) = order.quantity else {
            return Err(SessionError::Identity);
        };
        let executed = snapshot.cumulative_base;
        let consistent = match snapshot.status {
            v::OrderStatus::Filled => executed == size,
            v::OrderStatus::New | v::OrderStatus::Rejected => !executed.is_positive(),
            v::OrderStatus::PartiallyFilled => executed.is_positive() && executed != size,
            _ => true,
        };
        if !consistent || order.side != snapshot.side {
            return Err(SessionError::Identity);
        }
        let r = &c.context.request;
        let event = v::CanonicalEvent::new(v::VenueEvent {
            identity: v::EventIdentity {
                risk_domain_id: r.risk_domain_id,
                venue: r.venue,
                environment: r.environment,
                account: r.account,
                product: r.product,
                instrument_id: c.context.instrument.instrument_id,
                instrument_version: c.context.instrument.instrument_version,
                source_connection_epoch: self.connection_epoch,
                source_sequence: None,
                event_id: v::BoundedText::new(hex(&cmd.digest()))
                    .map_err(|_| SessionError::Identity)?,
                venue_time_ns: Some(snapshot.venue_time_ns),
                received_mono_ns: received,
            },
            body: v::VenueEventBody::Order(Box::new(v::OrderObservation {
                order: v::OrderIdentity {
                    order_id: target.order_id,
                    original_intent_id: original.intent_id,
                    original_intent_exec_epoch: original.epoch,
                    client_id: Some(client.clone()),
                    venue_order_id: Some(snapshot.venue_order_id.clone()),
                    parent_order_id: None,
                    list_id: None,
                    venue_algo_id: None,
                    venue_list_id: None,
                    list_client_id: None,
                    algo_client_id: None,
                },
                status: snapshot.status,
                side: snapshot.side,
                position_side: v::PositionSide::Both,
                original_quantity: None,
                cumulative_quantity: Some(snapshot.cumulative_base),
                cumulative_quote: None,
                leaves_quantity: None,
                order_price: None,
            })),
        })
        .map_err(|_| SessionError::Identity)?;
        self.gateway
            .record_event(event)
            .map_err(SessionError::Gateway)?;
        Ok(())
    }
}
