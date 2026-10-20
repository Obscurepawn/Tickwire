//! One account owner, one ordered journal, and fresh authorization at first output.
#![forbid(unsafe_code)]
use std::{collections::BTreeMap, time::Instant};
use tickwire_account::{Account, Admission, Applied, IntentKey, RecoveryRecord};
use tickwire_account_codec::{CanonicalBaseline, CanonicalRecord};
use tickwire_account_journal::{AccountJournal, AppendReceipt, Backend, HistoryFrontier};
pub use tickwire_gateway::{ConnectionConfig, EncodedRequest, Encoder, Route};
use tickwire_journal::PhysicalFrontier;
use tickwire_recovery::{ClosureBinding, VerifiedClosure};
use tickwire_transport::{Connection, IoBudget, PreparedSend, Security, SendReport, SendState};
use tickwire_venue::{CanonicalCommand, CanonicalEvent, Hash};
mod binding;
mod recovery;

#[derive(Debug)]
pub enum Error {
    Account(tickwire_account::Error),
    Journal(tickwire_account_journal::Error),
    Codec(tickwire_account_codec::Error),
    Authority(AuthorityError),
    Encoding(tickwire_gateway::Error),
    Transport(tickwire_transport::Error),
    Identity,
    Invalid,
    Conflict,
    Busy,
    Capacity,
    Halted,
}
pub type Result<T> = std::result::Result<T, Error>;
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<tickwire_transport::Error> for Error {
    fn from(value: tickwire_transport::Error) -> Self {
        Self::Transport(value)
    }
}
#[derive(Debug)]
pub enum AuthorityError {
    Denied,
    Unavailable,
    Recovery(tickwire_recovery::Error),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gate {
    pub now: u64,
    pub lease_until: u64,
    pub writer_epoch: u64,
    pub strategy_generation: u64,
    pub config_revision: u64,
    pub owner_isolated: bool,
    pub action_permitted: bool,
    pub market_valid: bool,
}
pub struct AdmissionRequest<'a> {
    pub baseline: &'a CanonicalBaseline,
    pub record: &'a CanonicalRecord,
    pub preceding: &'a HistoryFrontier,
    pub binding: ClosureBinding,
}
/// Trusted action policy and historical dependency verification; no permissive default.
pub trait Authority {
    fn prepare(
        &mut self,
        request: AdmissionRequest<'_>,
    ) -> std::result::Result<VerifiedClosure, AuthorityError>;
    fn verify(
        &mut self,
        request: AdmissionRequest<'_>,
    ) -> std::result::Result<VerifiedClosure, AuthorityError>;
    /// Must sample the current control state and trusted monotonic time synchronously.
    fn current_gate(
        &mut self,
        command: &CanonicalCommand,
        admission: &Admission,
    ) -> std::result::Result<Gate, AuthorityError>;
}
pub struct Config {
    pub journal: tickwire_account_journal::Config,
    pub writer_epoch: u64,
    pub route: Route,
    pub max_intents: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Queued,
    PartiallyAccepted,
    Flushed,
    Unknown,
    Aborted,
}
#[derive(Debug, Clone)]
pub struct CommittedAdmission {
    admission: Admission,
    receipt: AppendReceipt,
}
impl CommittedAdmission {
    pub fn admission(&self) -> &Admission {
        &self.admission
    }
    pub fn receipt(&self) -> &AppendReceipt {
        &self.receipt
    }
    pub fn record_digest(&self) -> Hash {
        self.receipt.record_digest()
    }
}
pub struct AccountGateway<S: Backend, A: Authority, E: Encoder> {
    journal: AccountJournal<S>,
    account: Account,
    config: Config,
    authority: A,
    encoder: E,
    connection: Option<Connection>,
    max_send_bytes: usize,
    entries: BTreeMap<IntentKey, Entry>,
    pending: Option<(IntentKey, u64)>,
    halted: bool,
}
struct Entry {
    command: CanonicalCommand,
    committed: Option<CommittedAdmission>,
    evaluated_at: u64,
    increases: bool,
    status: Status,
}
impl<S: Backend, A: Authority, E: Encoder> AccountGateway<S, A, E> {
    pub fn create(
        backend: S,
        config: Config,
        baseline: CanonicalBaseline,
        authority: A,
        encoder: E,
        connection: ConnectionConfig,
    ) -> Result<Self> {
        binding::config(&config)?;
        binding::baseline(&config, &baseline)?;
        if config.writer_epoch != config.journal.initial_writer_epoch {
            return Err(Error::Identity);
        }
        let account =
            Account::from_snapshot(baseline.snapshot().clone(), baseline.catalog().clone())
                .map_err(Error::Account)?;
        let server_name = config
            .route
            .authority
            .clone()
            .try_into()
            .map_err(|_| Error::Identity)?;
        let max_send_bytes = connection.limits.max_send_bytes;
        let connection = Connection::connect(
            connection.address,
            Security::Tls {
                server_name,
                config: connection.tls,
            },
            connection.limits,
            connection.connect_deadline,
        )?;
        let journal = AccountJournal::create(backend, config.journal.clone(), baseline)
            .map_err(Error::Journal)?;
        Ok(Self {
            journal,
            account,
            config,
            authority,
            encoder,
            connection: Some(connection),
            max_send_bytes,
            entries: BTreeMap::new(),
            pending: None,
            halted: false,
        })
    }
    pub fn recover(
        backend: S,
        config: Config,
        mut authority: A,
        encoder: E,
        required: Option<&PhysicalFrontier>,
    ) -> Result<Self> {
        binding::config(&config)?;
        let recovered = AccountJournal::recover(
            backend,
            config.journal.clone(),
            config.writer_epoch,
            required,
            &mut recovery::Verifier {
                authority: &mut authority,
            },
        )
        .map_err(Error::Journal)?;
        binding::baseline(&config, recovered.journal.baseline())?;
        let entries = recovery::entries(&config, recovered.journal.records())?;
        Ok(Self {
            journal: recovered.journal,
            account: recovered.account,
            config,
            authority,
            encoder,
            connection: None,
            max_send_bytes: 0,
            entries,
            pending: None,
            halted: true,
        })
    }
    pub fn submit(
        &mut self,
        command: CanonicalCommand,
        evaluated_at: u64,
        deadline: Instant,
    ) -> Result<Status> {
        let key = binding::intent(&command);
        if let Some(old) = self.entries.get(&key) {
            if old.command.bytes() == command.bytes() {
                return Ok(old.status);
            }
            self.halt();
            return Err(Error::Conflict);
        }
        if self.halted {
            return Err(Error::Halted);
        }
        if self.pending.is_some() {
            return Err(Error::Busy);
        }
        if self.entries.len() >= self.config.max_intents {
            return Err(Error::Capacity);
        }
        binding::command(&self.config, &command, true)?;
        let wire = self.encoder.encode(&command).map_err(Error::Encoding)?;
        if wire.command_digest() != command.digest()
            || wire.authority() != self.config.route.authority
        {
            return Err(Error::Identity);
        }
        let send = PreparedSend::new(wire.bytes(), deadline, self.max_send_bytes)?;
        let mut candidate = self.account.clone();
        let admission = match candidate.admit(&command, evaluated_at) {
            Ok(admission) => admission,
            Err(error) => {
                if candidate.frozen() {
                    self.account.freeze();
                }
                return Err(Error::Account(error));
            }
        };
        let expected = admission.image();
        let increases = expected.increases;
        let record = CanonicalRecord::new(
            self.config.journal.baseline_digest,
            self.journal.position() + 1,
            RecoveryRecord::Admit {
                command: Box::new(command.clone()),
                evaluated_at,
                expected,
            },
        )
        .map_err(Error::Codec)?;
        let request = binding::request(
            self.journal.baseline(),
            &record,
            self.journal.history_frontier(),
        )?;
        let capability = match self.authority.prepare(request) {
            Ok(capability) => capability,
            Err(error) => {
                self.halt();
                return Err(Error::Authority(error));
            }
        };
        if let Err(error) = binding::closure(
            self.journal.baseline(),
            &record,
            self.journal.history_frontier(),
            &capability,
        ) {
            self.halt();
            return Err(error);
        }
        let receipt = match self.journal.append(record.record().clone()) {
            Ok(receipt) => receipt,
            Err(error) => {
                // A complete record may exist despite losing the append/sync result.
                self.account = candidate;
                self.halt();
                return Err(Error::Journal(error));
            }
        };
        self.account = candidate;
        if receipt.sequence() != record.sequence()
            || receipt.record_digest() != record.digest()
            || receipt.baseline_digest() != record.baseline()
        {
            self.halt();
            return Err(Error::Identity);
        }
        self.entries.insert(
            key,
            Entry {
                command,
                committed: Some(CommittedAdmission { admission, receipt }),
                evaluated_at,
                increases,
                status: Status::Queued,
            },
        );
        match self.connection.as_mut().ok_or(Error::Halted)?.queue(send) {
            Ok(token) => {
                self.pending = Some((key, token));
                Ok(Status::Queued)
            }
            Err(error) => {
                // queue does not accept application bytes, even when it fails.
                self.abort(key)?;
                self.halt();
                Err(error.into())
            }
        }
    }
    pub fn poll(&mut self, now: Instant, budget: IoBudget) -> Result<()> {
        if self.halted {
            return Err(Error::Halted);
        }
        let driven = self
            .connection
            .as_mut()
            .ok_or(Error::Halted)?
            .drive(now, budget);
        if self.pending.is_some() {
            let report = self
                .connection
                .as_ref()
                .and_then(Connection::send_report)
                .ok_or(Error::Invalid)?;
            self.apply_report(report)?;
        }
        if let Some((key, _)) = self.pending
            && !self.halted
            && driven.as_ref().is_ok_and(|r| !r.peer_closed)
        {
            let entry = &self.entries[&key];
            let admission = &entry.committed.as_ref().ok_or(Error::Invalid)?.admission;
            let authority = &mut self.authority;
            let account = &self.account;
            let epoch = self.config.writer_epoch;
            let result = self
                .connection
                .as_mut()
                .ok_or(Error::Halted)?
                .try_send(now, || {
                    authority
                        .current_gate(&entry.command, admission)
                        .is_ok_and(|gate| {
                            binding::permits(gate, entry, epoch)
                                && account.check_admission(admission, gate.now).is_ok()
                        })
                });
            match result {
                Ok(report) => self.apply_report(report)?,
                Err(error) => {
                    self.halt();
                    return Err(error.into());
                }
            }
        }
        match driven {
            Ok(report) if !report.peer_closed => Ok(()),
            Ok(_) => {
                self.halt();
                Ok(())
            }
            Err(error) => {
                self.halt();
                Err(error.into())
            }
        }
    }
    pub fn cancel_pending(&mut self) -> Result<()> {
        if self.halted {
            return Err(Error::Halted);
        }
        if self.pending.is_none() {
            return Err(Error::Invalid);
        }
        let report = match self.connection.as_mut().ok_or(Error::Halted)?.cancel() {
            Ok(report) => report,
            Err(error) => {
                self.halt();
                return Err(error.into());
            }
        };
        self.apply_report(report)
    }
    pub fn record_event(&mut self, event: CanonicalEvent) -> Result<Applied> {
        self.record_fact(RecoveryRecord::VenueEvent(Box::new(event)))
    }
    pub fn record_fact(&mut self, record: RecoveryRecord) -> Result<Applied> {
        if matches!(
            record,
            RecoveryRecord::Admit { .. } | RecoveryRecord::Aborted { .. }
        ) {
            return Err(Error::Invalid);
        }
        if let Err(error) = self.journal.append(record.clone()) {
            self.halt();
            return Err(Error::Journal(error));
        }
        let applied = recovery::apply_fact(&mut self.account, &record);
        if applied.is_err() {
            self.account.freeze();
        }
        applied
    }
    pub fn status(&self, intent: IntentKey) -> Option<Status> {
        self.entries.get(&intent).map(|e| e.status)
    }
    pub fn committed(&self, intent: IntentKey) -> Option<&CommittedAdmission> {
        self.entries.get(&intent).and_then(|e| e.committed.as_ref())
    }
    pub fn account(&self) -> &Account {
        &self.account
    }
    pub fn journal_position(&self) -> u64 {
        self.journal.position()
    }
    pub fn halted(&self) -> bool {
        self.halted
    }
    pub fn read(&mut self, out: &mut [u8]) -> usize {
        self.connection.as_mut().map_or(0, |c| c.read(out))
    }
    pub fn register(
        &mut self,
        registry: &tickwire_transport::mio::Registry,
        token: tickwire_transport::mio::Token,
    ) -> Result<()> {
        Ok(self
            .connection
            .as_mut()
            .ok_or(Error::Halted)?
            .register(registry, token)?)
    }
    pub fn reregister(
        &mut self,
        registry: &tickwire_transport::mio::Registry,
        token: tickwire_transport::mio::Token,
    ) -> Result<()> {
        Ok(self
            .connection
            .as_mut()
            .ok_or(Error::Halted)?
            .reregister(registry, token)?)
    }
    fn apply_report(&mut self, report: SendReport) -> Result<()> {
        let (key, token) = self.pending.ok_or(Error::Invalid)?;
        if token != report.id {
            self.halt();
            return Err(Error::Identity);
        }
        match report.state {
            SendState::Queued => {}
            SendState::MayHaveSent => {
                self.entries.get_mut(&key).unwrap().status = Status::PartiallyAccepted
            }
            SendState::Flushed => {
                self.entries.get_mut(&key).unwrap().status = Status::Flushed;
                self.connection.as_mut().unwrap().take_finished();
                self.pending = None;
            }
            SendState::NoBytes(_) => {
                if report.accepted_bytes != 0 {
                    self.halt();
                    return Err(Error::Invalid);
                }
                self.abort(key)?;
                self.connection.as_mut().unwrap().take_finished();
                self.pending = None;
            }
            SendState::Failed(_) => self.halt(),
        }
        Ok(())
    }
    fn abort(&mut self, key: IntentKey) -> Result<()> {
        let admission = self
            .entries
            .get(&key)
            .and_then(|e| e.committed.as_ref())
            .ok_or(Error::Invalid)?
            .admission
            .clone();
        // Local abort is a decision, not an external observation. Never log a
        // decision already contradicted by an accepted exchange fact.
        let mut candidate = self.account.clone();
        if let Err(error) = candidate.apply_abort(&admission) {
            self.halt();
            return Err(Error::Account(error));
        }
        if let Err(error) = self.journal.append(RecoveryRecord::Aborted {
            intent: key,
            command_digest: admission.command_digest(),
        }) {
            self.halt();
            return Err(Error::Journal(error));
        }
        self.account = candidate;
        self.entries.get_mut(&key).unwrap().status = Status::Aborted;
        Ok(())
    }
    fn halt(&mut self) {
        self.halted = true;
        self.account.freeze();
        self.connection = None;
        self.pending = None;
        for entry in self.entries.values_mut() {
            if entry.status != Status::Aborted {
                entry.status = Status::Unknown;
            }
        }
    }
}
