use super::*;
use tickwire_execution::{
    CommitProfile, Engine, Fill, Gate, Order, SendState, Terminal,
    ports::{Exchange, ExecutionFact, SubmitOutcome},
};
use tickwire_journal_codec::OrderIdentity;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeView {
    pub frozen: bool,
    pub held: i128,
    pub spent: i128,
    pub available: i128,
    pub journal_seq: u64,
}
pub struct Runtime<S: Backend, A: CommitAuthorizer, E: Exchange, const N: usize, const T: usize> {
    engine: Engine<N, T>,
    journal: BusinessJournal<S>,
    authorizer: A,
    exchange: E,
}
impl<S: Backend, A: CommitAuthorizer, E: Exchange, const N: usize, const T: usize>
    Runtime<S, A, E, N, T>
{
    pub fn create(
        backend: S,
        config: BusinessConfig,
        gate: Gate,
        authorizer: A,
        exchange: E,
    ) -> Result<Self> {
        let engine = Engine::new(config.domain, config.initial_capital, gate)?;
        let journal = BusinessJournal::create(backend, config, gate.epoch)?;
        Ok(Self {
            engine,
            journal,
            authorizer,
            exchange,
        })
    }
    pub fn recover(
        backend: S,
        config: BusinessConfig,
        gate: Gate,
        authorizer: A,
        exchange: E,
        required: Option<&PhysicalFrontier>,
    ) -> Result<Self> {
        let mut engine = Engine::new(config.domain, config.initial_capital, gate)?;
        engine.freeze();
        let journal = BusinessJournal::recover(backend, config, gate.epoch, required)?;
        let mut runtime = Self {
            engine,
            journal,
            authorizer,
            exchange,
        };
        // Clone immutable decoded records; no reconstruction is published until
        // all closures and all reducer applications have succeeded.
        for event in runtime.journal.events.clone() {
            if matches!(event.event, codec::Event::PreparedIntent(_)) {
                let record = request_from_record(&event)?;
                let binding = closure_binding(&event)?;
                let bytes = codec::business_payload(&event);
                let cap = runtime
                    .authorizer
                    .verify_recovery(AuthorizationRequest {
                        binding: &binding,
                        record: &record,
                        intent_payload: &bytes,
                    })
                    .map_err(Error::Authorization)?;
                if cap.binding() != &binding
                    || cap.command_digest() != Some(record.request.command_digest)
                {
                    return Err(Error::Quarantined("recovery closure mismatch"));
                }
                runtime.engine.recover_intent(record)?;
            } else {
                runtime.apply_fact(event.event)?;
            }
        }
        Ok(runtime)
    }
    pub fn submit(&mut self, request: OrderRequest) -> Result<()> {
        if request.commit_profile != CommitProfile::LocalDurable {
            return Err(Error::UnsupportedProfile);
        }
        if self.engine.frozen() {
            return Err(Error::Frozen);
        }
        self.refresh_gate()?;
        if let Ok(old) = self.engine.order(request.key.intent) {
            if old.request != request {
                self.engine.freeze();
                return Err(Error::Conflict);
            }
        } else {
            self.engine.reserve(request)?;
        }
        self.resume_after_admission(request.key.intent)
    }
    pub fn resume_submit(&mut self, intent_id: u64) -> Result<()> {
        if self.engine.frozen() {
            return Err(Error::Frozen);
        }
        self.refresh_gate()?;
        self.resume_after_admission(intent_id)
    }
    fn resume_after_admission(&mut self, intent_id: u64) -> Result<()> {
        let order = self.engine.order(intent_id)?;
        if order.send != SendState::NotSubmitted || order.terminal.is_some() {
            return Err(tickwire_execution::Error::AlreadySubmitted.into());
        }
        let record = self.engine.durable_record(intent_id)?;
        if record.request.commit_profile != CommitProfile::LocalDurable {
            return Err(Error::UnsupportedProfile);
        }
        let seq = self
            .journal
            .position()
            .checked_add(1)
            .ok_or(Error::Invalid("journal sequence exhausted"))?;
        let canonical = intent_record(
            &self.journal.config,
            self.journal.writer_exec_epoch,
            seq,
            record,
        );
        let binding = closure_binding(&canonical)?;
        let bytes = codec::business_payload(&canonical);
        let outcome = (|| {
            let cap = self
                .authorizer
                .authorize(AuthorizationRequest {
                    binding: &binding,
                    record: &record,
                    intent_payload: &bytes,
                })
                .map_err(Error::Authorization)?;
            self.journal.commit_intent(record, &cap)?;
            self.refresh_gate()?;
            // This legacy reducer parameter is assembled only inside the strict
            // wrapper after trusted authorization, verified closure and WAL sync.
            self.engine.accept_commit(
                tickwire_execution::CommitReceipt {
                    record,
                    local_durable: true,
                    replica_durable: false,
                },
                CommitProfile::LocalDurable,
            )?;
            let request = self.engine.begin_send(intent_id)?;
            match self.exchange.submit(request) {
                SubmitOutcome::PendingResponse => {}
                SubmitOutcome::Acknowledged => self.engine.ack(intent_id)?,
                SubmitOutcome::MayHaveSent => self.engine.uncertain(intent_id)?,
                SubmitOutcome::NoBytesAccepted => {
                    let event = codec::Event::AbortedBeforeSend {
                        order: canonical.event.order(),
                    };
                    self.journal.append_fact(event)?;
                    self.apply_fact(event)?;
                }
            }
            Ok(())
        })();
        if outcome.is_err() {
            self.engine.freeze();
        }
        outcome
    }
    pub fn record_fact(&mut self, identity: OrderIdentity, fact: ExecutionFact) -> Result<()> {
        let (id, event) = match fact {
            ExecutionFact::AbortedBeforeSend { .. } => {
                self.engine.freeze();
                return Err(Error::Invalid(
                    "abort observations only come from the exchange call",
                ));
            }
            ExecutionFact::Fill { intent, fill } => (
                intent,
                codec::Event::Fill {
                    order: identity,
                    trade_id: fill.trade_id,
                    quantity: fill.quantity,
                    cost: fill.cost,
                },
            ),
            ExecutionFact::Fee { intent, id, amount } => (
                intent,
                codec::Event::Fee {
                    order: identity,
                    fee_id: id,
                    amount,
                },
            ),
            ExecutionFact::Terminal {
                intent,
                status,
                cumulative,
            } => (
                intent,
                codec::Event::Terminal {
                    order: identity,
                    status: match status {
                        Terminal::Filled => codec::TerminalStatus::Filled,
                        Terminal::Canceled => codec::TerminalStatus::Canceled,
                        Terminal::Rejected => codec::TerminalStatus::Rejected,
                        Terminal::Expired => codec::TerminalStatus::Expired,
                    },
                    cumulative,
                },
            ),
        };
        let outcome = (|| {
            if id != identity.intent_id {
                return Err(Error::Invalid("fact routing identity mismatch"));
            }
            self.journal.append_fact(event)?;
            self.apply_fact(event)
        })();
        if outcome.is_err() {
            self.engine.freeze();
        }
        outcome
    }
    pub fn record_fill(&mut self, identity: OrderIdentity, fill: Fill) -> Result<()> {
        self.record_fact(
            identity,
            ExecutionFact::Fill {
                intent: identity.intent_id,
                fill,
            },
        )
    }
    pub fn order(&self, id: u64) -> Result<&Order<T>> {
        Ok(self.engine.order(id)?)
    }
    pub fn view(&self) -> RuntimeView {
        RuntimeView {
            frozen: self.engine.frozen(),
            held: self.engine.held(),
            spent: self.engine.spent(),
            available: self.engine.available(),
            journal_seq: self.journal.position(),
        }
    }
    pub fn exchange(&self) -> &E {
        &self.exchange
    }
    pub fn update_gate(&mut self, gate: Gate) -> Result<()> {
        if gate.epoch != self.journal.writer_exec_epoch {
            self.engine.freeze();
            return Err(Error::Invalid("writer epoch transition requires recovery"));
        }
        self.engine.update_gate(gate);
        Ok(())
    }
    fn refresh_gate(&mut self) -> Result<()> {
        match self.authorizer.current_gate() {
            Ok(gate) => self.update_gate(gate),
            Err(error) => {
                self.engine.freeze();
                Err(Error::Authorization(error))
            }
        }
    }
    fn apply_fact(&mut self, event: codec::Event) -> Result<()> {
        let identity = event.order();
        let order = self.engine.order(identity.intent_id)?;
        let expected = intent_record(
            &self.journal.config,
            identity.intent_exec_epoch,
            0,
            DurableRecord {
                request: order.request,
                sequence: order.sequence,
            },
        )
        .event
        .order();
        if expected != identity {
            return Err(Error::Quarantined(
                "fact references a different original order",
            ));
        }
        match event {
            codec::Event::Fill {
                trade_id,
                quantity,
                cost,
                ..
            } => self.engine.fill(
                identity.intent_id,
                Fill {
                    trade_id,
                    quantity,
                    cost,
                },
            )?,
            codec::Event::Fee { fee_id, amount, .. } => {
                self.engine.fee(identity.intent_id, fee_id, amount)?
            }
            codec::Event::Terminal {
                status, cumulative, ..
            } => self.engine.terminal(
                identity.intent_id,
                match status {
                    codec::TerminalStatus::Filled => Terminal::Filled,
                    codec::TerminalStatus::Canceled => Terminal::Canceled,
                    codec::TerminalStatus::Rejected => Terminal::Rejected,
                    codec::TerminalStatus::Expired => Terminal::Expired,
                },
                cumulative,
            )?,
            codec::Event::AbortedBeforeSend { .. } => self
                .engine
                .transport_rejected_without_bytes(identity.intent_id)?,
            codec::Event::PreparedIntent(_) => {
                return Err(Error::Invalid("prepared event is not a fact"));
            }
        }
        Ok(())
    }
}
