//! Shared intake for hosted or file-backed strategies and a single qualified cash account.
#![forbid(unsafe_code)]
mod origins;
use origins::Origins;
use tickwire_account::{IntentKey, Scope};
use tickwire_account_gateway::{Authority, Status};
use tickwire_account_journal::Backend;
use tickwire_binance_clock::LocalTime;
use tickwire_clock::MonoTime;
use tickwire_recovery::BlobStore;
use tickwire_runtime::{
    authority::RecoveryQueue,
    node::{Health, Strategy, Submission},
};
use tickwire_spot_execution::{FillSource, Reply, RestSession, SessionError};
use tickwire_transport::IoBudget;
use tickwire_venue::{CanonicalCommand, CommandBody};
#[derive(Debug)]
pub enum Error {
    Session(SessionError),
    Origin(&'static str),
    Storage(tickwire_recovery::Error),
    Closure(tickwire_account_gateway::AuthorityError),
    Clock,
    Busy,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Session(e) => Some(e),
            Self::Storage(e) => Some(e),
            _ => None,
        }
    }
}
type Result<T> = std::result::Result<T, Error>;
pub struct Completion {
    pub logical_strategy_id: u64,
    pub intent: IntentKey,
    pub response: Reply,
}
pub struct AccountRunner<
    'a,
    B: Backend,
    A: Authority,
    T: FnMut() -> std::result::Result<u64, tickwire_gateway::Error>,
    S: BlobStore,
> {
    session: RestSession<'a, B, A, T>,
    queue: RecoveryQueue,
    origins: Origins<S>,
    pending: Option<(u64, IntentKey)>,
    last: Option<MonoTime>,
}
impl<
    'a,
    B: Backend,
    A: Authority,
    T: FnMut() -> std::result::Result<u64, tickwire_gateway::Error>,
    S: BlobStore,
> AccountRunner<'a, B, A, T, S>
{
    /// The recovery queue must be the one used by the gateway's actual local authority.
    /// The immutable origin index must be retained with this account's recovery files.
    pub fn new(session: RestSession<'a, B, A, T>, queue: RecoveryQueue, store: S) -> Self {
        Self {
            session,
            queue,
            origins: Origins(store),
            pending: None,
            last: None,
        }
    }
    /// A trusted input owner calls this immediately on lost account qualification.
    pub fn stop_sending(&mut self) {
        self.session.stop_sending();
    }
    pub fn account(&self) -> &tickwire_account::Account {
        self.session.gateway().account()
    }
    pub fn position(&self) -> u64 {
        self.session.gateway().journal_position()
    }
    pub fn available(&self) -> bool {
        self.pending.is_none() && !self.session.gateway().halted()
    }
    pub fn scope(&self) -> &Scope {
        &self.session.gateway().baseline().snapshot().scope
    }
    fn clock(&mut self, now: LocalTime) -> Result<()> {
        if now.monotonic.nanos() == 0
            || self.last.is_some_and(|t| {
                t.domain() != now.monotonic.domain() || t.nanos() > now.monotonic.nanos()
            })
        {
            self.session.stop_sending();
            return Err(Error::Clock);
        }
        self.last = Some(now.monotonic);
        Ok(())
    }
    /// Drive one real strategy intake turn. The account owner's current authority independently
    /// rechecks each new request immediately before the transport accepts application bytes.
    pub fn poll_strategy(
        &mut self,
        strategy: &mut impl Strategy,
        mut health: Health,
        now: LocalTime,
    ) -> Result<bool> {
        self.clock(now)?;
        strategy.on_account_history(self.session.gateway());
        health.account_frozen |= self.account().frozen();
        health.sender_halted |= self.session.gateway().halted();
        strategy.on_health(health, now);
        if !self.available() {
            return Ok(false);
        }
        let Some(submission) = strategy.next(self.account(), health, now) else {
            return Ok(false);
        };
        let command = submission.command.clone();
        let result = self.submit(submission, now);
        strategy.on_submission(
            &command,
            &result
                .as_ref()
                .copied()
                .map_err(|e| e as &(dyn std::error::Error + 'static)),
        );
        // Ordinary pre-admission resource refusals are local to this strategy. It already
        // received the typed result, and the remaining members keep their turns.
        match result {
            Err(Error::Session(SessionError::Gateway(
                tickwire_account_gateway::Error::Account(
                    tickwire_account::Error::InsufficientAsset
                    | tickwire_account::Error::InsufficientCapital
                    | tickwire_account::Error::Limit,
                ),
            ))) if !self.account().frozen() => Ok(true),
            other => other.map(|_| true),
        }
    }
    pub fn submit(&mut self, sub: Submission, now: LocalTime) -> Result<Status> {
        self.clock(now)?;
        let Submission {
            command,
            order_ids,
            deadline,
            recovery,
        } = sub;
        let c = command.command();
        let r = &c.context.request;
        let intent = IntentKey {
            epoch: r.intent_exec_epoch,
            intent_id: r.intent_id,
        };
        if self.pending.is_some() && self.session.gateway().command(intent).is_none() {
            return Err(Error::Busy);
        }
        let scope = self.scope();
        if r.risk_domain_id != scope.risk_domain_id
            || r.venue != scope.venue
            || r.account != scope.account
            || r.environment != scope.environment
            || r.product != tickwire_venue::Product::Spot
            || command.digest() != recovery.command_digest
        {
            self.session.stop_sending();
            return Err(Error::Origin(
                "strategy command scope or recovery digest mismatch",
            ));
        }
        let prior = self.session.gateway().command(intent).is_some();
        let deadline = if prior {
            deadline
        } else {
            let remaining = r
                .deadline
                .checked_sub(now.monotonic.nanos())
                .filter(|ns| *ns > 0)
                .ok_or(Error::Clock)?;
            let canonical_deadline = now
                .instant
                .checked_add(std::time::Duration::from_nanos(remaining))
                .ok_or(Error::Clock)?;
            let deadline = deadline.min(canonical_deadline);
            if deadline <= now.instant {
                return Err(Error::Clock);
            }
            deadline
        };
        let scope = scope.clone();
        let origin_check = match &c.body {
            CommandBody::New(order) => match order_ids.as_slice() {
                [id] if *id != [0; 16] => self.origins.register(
                    &scope,
                    intent,
                    order.client_id.as_str(),
                    *id,
                    command.digest(),
                ),
                _ => Err(Error::Origin(
                    "one nonzero local order ID is required for a new cash order",
                )),
            },
            CommandBody::Cancel(target) | CommandBody::Query(target) if order_ids.is_empty() => {
                self.origins
                    .lookup(&scope, b"order", &target.order_id)
                    .and_then(|origin| {
                        let original = self
                            .session
                            .gateway()
                            .command(origin.intent)
                            .ok_or(Error::Origin("unadmitted original order"))?;
                        if origin.intent.epoch != target.original_intent_exec_epoch
                            || original.digest() != origin.digest
                            || original.command().context.instrument != c.context.instrument
                        {
                            return Err(Error::Origin("unexpected original order binding"));
                        }
                        let CommandBody::New(order) = &original.command().body else {
                            return Err(Error::Origin("unexpected original command"));
                        };
                        if target
                            .client_id
                            .as_ref()
                            .is_none_or(|id| *id != order.client_id)
                        {
                            return Err(Error::Origin("original client ID required"));
                        }
                        // Both exchanges prioritize native IDs when both are supplied. The native ID
                        // must already be bound by independent account evidence before it can override the client ID.
                        if let Some(native) = &target.venue_order_id {
                            let key = tickwire_account::OrderKey {
                                intent: origin.intent,
                                client_id: order.client_id.clone(),
                            };
                            if self.session.gateway().account().venue_order_id(&key) != Some(native)
                            {
                                return Err(Error::Origin("unconfirmed native order ID"));
                            }
                        }
                        Ok(())
                    })
            }
            _ => Err(Error::Origin(
                "unsupported cash order or local order ID count",
            )),
        };
        if let Err(e) = origin_check {
            self.session.stop_sending();
            return Err(e);
        }
        if prior {
            return self
                .session
                .submit(command, now.monotonic.nanos(), deadline)
                .map_err(Error::Session);
        }
        let digest = command.digest();
        self.queue.stage(recovery).map_err(Error::Closure)?;
        let strategy = c.context.strategy.logical_strategy_id;
        let status = self
            .session
            .submit(command, now.monotonic.nanos(), deadline);
        if let Err(e) = self.queue.remove(digest) {
            self.session.stop_sending();
            return Err(Error::Closure(e));
        }
        let status = status.map_err(Error::Session)?;
        if status != Status::Aborted {
            self.pending = Some((strategy, intent));
        }
        Ok(status)
    }
    /// Acknowledgement goes to the member that originated this command, including a manager's
    /// cancellation. Account history still attributes the affected order to its actual owner.
    pub fn poll(
        &mut self,
        strategy: &mut impl Strategy,
        now: LocalTime,
        budget: IoBudget,
    ) -> Result<Option<Completion>> {
        self.clock(now)?;
        let response = self
            .session
            .poll(now.instant, now.monotonic.nanos(), budget)
            .map_err(Error::Session)?;
        if let Some(response) = response {
            let (logical_strategy_id, intent) = self
                .pending
                .take()
                .ok_or(Error::Origin("unsolicited response"))?;
            let command = self
                .session
                .gateway()
                .command(intent)
                .ok_or(Error::Origin("missing admitted request"))?;
            strategy.on_request_reply(command, &response);
            return Ok(Some(Completion {
                logical_strategy_id,
                intent,
                response,
            }));
        }
        if self
            .pending
            .is_some_and(|(_, key)| self.session.gateway().status(key) == Some(Status::Aborted))
        {
            self.pending = None;
        }
        Ok(None)
    }
    /// The authenticated input owner supplies the verified private row and its durable source
    /// position. The original intent and local order ID come exclusively from the account index.
    pub fn record_fill(
        &mut self,
        client: &str,
        stream: [u8; 16],
        sequence: u64,
        epoch: u64,
        received_ns: u64,
        row: &[u8],
    ) -> Result<tickwire_account::Applied> {
        let scope = self.scope().clone();
        let origin = match self.origins.lookup(&scope, b"client", client.as_bytes()) {
            Ok(origin)
                if self
                    .session
                    .gateway()
                    .command(origin.intent)
                    .map(CanonicalCommand::digest)
                    == Some(origin.digest) =>
            {
                origin
            }
            _ => {
                self.session.stop_sending();
                return Err(Error::Origin("unrecognized private execution origin"));
            }
        };
        self.session
            .record_fill(
                FillSource {
                    scope: &scope,
                    intent: origin.intent,
                    order_id: origin.order,
                    input_stream: stream,
                    input_sequence: sequence,
                    connection_epoch: epoch,
                    received_mono_ns: received_ns,
                },
                row,
            )
            .map_err(Error::Session)
    }
}
