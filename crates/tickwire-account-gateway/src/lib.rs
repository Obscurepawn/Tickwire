//! One account owner, one ordered journal, and fresh authorization at first output.
#![forbid(unsafe_code)]
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::Instant,
};
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
pub mod websocket;

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
    fn verify_handover(
        &mut self,
        _baseline: &CanonicalBaseline,
        _record: &CanonicalRecord,
        _prior: &[CanonicalRecord],
    ) -> std::result::Result<(), AuthorityError> {
        Err(AuthorityError::Denied)
    }
    fn prepare(
        &mut self,
        request: AdmissionRequest<'_>,
    ) -> std::result::Result<VerifiedClosure, AuthorityError>;
    fn verify(
        &mut self,
        request: AdmissionRequest<'_>,
    ) -> std::result::Result<VerifiedClosure, AuthorityError>;
    /// Called after the local WAL commit and before the first application byte.
    /// Pending preserves the admission and waits; it never releases a reservation.
    fn commit_state(
        &mut self,
        _admission: &Admission,
        _receipt: &AppendReceipt,
    ) -> std::result::Result<CommitState, AuthorityError> {
        Ok(CommitState::Ready)
    }
    /// Must sample the current control state and trusted monotonic time synchronously.
    fn current_gate(
        &mut self,
        command: &CanonicalCommand,
        admission: &Admission,
    ) -> std::result::Result<Gate, AuthorityError>;
}
/// Verifies durable, authenticated remote observations against this exact account prefix.
pub trait ReactivationVerifier {
    /// Called only after exact-prefix verification and candidate reactivation,
    /// immediately before a replacement connection is opened. REST needs no extra state.
    fn before_connect(
        &mut self,
        _binding: &tickwire_account::Reactivation,
    ) -> std::result::Result<(), AuthorityError> {
        Ok(())
    }

    fn verify_income_association_live(
        &mut self,
        _baseline: &CanonicalBaseline,
        _account: &Account,
        _preceding: &HistoryFrontier,
        _prior: &[CanonicalRecord],
        _binding: &tickwire_account::IncomeObservationsAssociated,
        _writer_epoch: u64,
    ) -> std::result::Result<(), AuthorityError> {
        Err(AuthorityError::Denied)
    }
    fn verify_income_association(
        &mut self,
        _baseline: &CanonicalBaseline,
        _account: &Account,
        _preceding: &HistoryFrontier,
        _prior: &[CanonicalRecord],
        _binding: &tickwire_account::IncomeObservationsAssociated,
        _writer_epoch: u64,
    ) -> std::result::Result<(), AuthorityError> {
        Err(AuthorityError::Denied)
    }
    fn verify_uncreated_algo_live(
        &mut self,
        _baseline: &CanonicalBaseline,
        _account: &Account,
        _preceding: &HistoryFrontier,
        _binding: &tickwire_account::UncreatedAlgoClosed,
        _writer_epoch: u64,
    ) -> std::result::Result<(), AuthorityError> {
        Err(AuthorityError::Denied)
    }

    fn verify_uncreated_algo(
        &mut self,
        _baseline: &CanonicalBaseline,
        _account: &Account,
        _preceding: &HistoryFrontier,
        _binding: &tickwire_account::UncreatedAlgoClosed,
        _writer_epoch: u64,
    ) -> std::result::Result<(), AuthorityError> {
        Err(AuthorityError::Denied)
    }

    fn verify_account_inputs_closure_live(
        &mut self,
        _baseline: &CanonicalBaseline,
        _account: &Account,
        _preceding: &HistoryFrontier,
        _binding: &tickwire_account::AccountInputsReconciled,
        _writer_epoch: u64,
    ) -> std::result::Result<(), AuthorityError> {
        Err(AuthorityError::Denied)
    }

    fn verify_account_inputs_closure(
        &mut self,
        _baseline: &CanonicalBaseline,
        _account: &Account,
        _preceding: &HistoryFrontier,
        _binding: &tickwire_account::AccountInputsReconciled,
        _writer_epoch: u64,
    ) -> std::result::Result<(), AuthorityError> {
        Err(AuthorityError::Denied)
    }

    fn verify_income_baseline(
        &mut self,
        _baseline: &CanonicalBaseline,
        _account: &Account,
        _preceding: &HistoryFrontier,
        _binding: &tickwire_account::IncomeBaseline,
    ) -> std::result::Result<(), AuthorityError> {
        Err(AuthorityError::Denied)
    }
    fn verify(
        &mut self,
        baseline: &CanonicalBaseline,
        account: &Account,
        preceding: &HistoryFrontier,
        binding: &tickwire_account::Reactivation,
    ) -> std::result::Result<(), AuthorityError>;
}
struct RejectReactivation;
impl ReactivationVerifier for RejectReactivation {
    fn verify(
        &mut self,
        _: &CanonicalBaseline,
        _: &Account,
        _: &HistoryFrontier,
        _: &tickwire_account::Reactivation,
    ) -> std::result::Result<(), AuthorityError> {
        Err(AuthorityError::Denied)
    }
}
pub struct ActivationReceipt {
    pub ledger_sequence: u64,
    pub history_digest: Hash,
}
pub struct Config {
    pub journal: tickwire_account_journal::Config,
    pub writer_epoch: u64,
    pub route: Route,
    pub max_intents: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    AwaitingCommit,
    Queued,
    PartiallyAccepted,
    Flushed,
    Unknown,
    Aborted,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitState {
    Pending,
    Ready,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputProgress {
    pub accepted_bytes: usize,
    pub total_bytes: usize,
}

/// Verified cold-exchange bootstrap pairing a `CanonicalBaseline` with its reconstructed `Account`.
#[derive(Debug, Clone)]
pub struct VerifiedLiveBootstrap {
    baseline: CanonicalBaseline,
    account: Account,
    imported_orders: Vec<tickwire_account::OrderKey>,
    imported_commands: Vec<CanonicalCommand>,
    external_open_orders: usize,
}
impl VerifiedLiveBootstrap {
    pub fn new(
        snapshot: tickwire_account::Snapshot,
        catalog: tickwire_account::Catalog,
        open_orders: &[tickwire_account::LiveOpenOrder],
        freeze_on_external: bool,
    ) -> Result<Self> {
        let boot =
            Account::from_live_exchange(snapshot, catalog.clone(), open_orders, freeze_on_external)
                .map_err(Error::Account)?;
        let baseline =
            CanonicalBaseline::new(boot.normalized_snapshot, catalog).map_err(Error::Codec)?;
        Ok(Self {
            baseline,
            account: boot.account,
            imported_orders: boot.imported_orders,
            imported_commands: Vec::new(),
            external_open_orders: boot.external_open_orders,
        })
    }
    pub fn with_imported_commands(mut self, commands: Vec<CanonicalCommand>) -> Self {
        self.imported_commands = commands;
        self
    }
    pub fn baseline(&self) -> &CanonicalBaseline {
        &self.baseline
    }
    pub fn account(&self) -> &Account {
        &self.account
    }
    pub fn imported_orders(&self) -> &[tickwire_account::OrderKey] {
        &self.imported_orders
    }
    pub fn imported_commands(&self) -> &[CanonicalCommand] {
        &self.imported_commands
    }
    pub fn external_open_orders(&self) -> usize {
        self.external_open_orders
    }
}

pub struct AccountGateway<S: Backend, A: Authority, E: Encoder> {
    journal: AccountJournal<S>,
    account: Account,
    config: Config,
    authority: A,
    encoder: E,
    connection: Option<Connection>,
    websocket: Option<websocket::WebSocketState>,
    rest_started: bool,
    max_send_bytes: usize,
    retained_receive: VecDeque<u8>,
    max_receive_bytes: usize,
    entries: BTreeMap<IntentKey, Entry>,
    completed_mono_ns: BTreeMap<IntentKey, u64>,
    retention_window_ns: Option<u64>,
    last_mono_ns: u64,
    last_reactivation: Option<Box<tickwire_account::Reactivation>>,
    pending: Option<(IntentKey, u64)>,
    halted: bool,
    per_order_doubt_isolation: bool,
}
struct Entry {
    command: CanonicalCommand,
    committed: Option<CommittedAdmission>,
    evaluated_at: u64,
    increases: bool,
    status: Status,
    output: Option<OutputProgress>,
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
        Self::create_with_account(
            backend,
            config,
            baseline,
            account,
            BTreeMap::new(),
            authority,
            encoder,
            connection,
        )
    }
    pub fn create_from_live_exchange(
        backend: S,
        config: Config,
        live: VerifiedLiveBootstrap,
        authority: A,
        encoder: E,
        connection: ConnectionConfig,
    ) -> Result<Self> {
        if let Some(max_epoch) = live.account.max_imported_epoch()
            && config.writer_epoch < max_epoch
        {
            return Err(Error::Identity);
        }
        binding::config(&config)?;
        if live.imported_commands.len() > config.max_intents {
            return Err(Error::Capacity);
        }
        let mut entries = BTreeMap::new();
        for command in live.imported_commands {
            binding::command(&config, &command, false)?;
            let increases = match &command.command().body {
                tickwire_venue::CommandBody::New(spec) => !spec.reduce_only,
                _ => true,
            };
            let key = binding::intent(&command);
            if entries
                .insert(
                    key,
                    Entry {
                        command,
                        committed: None,
                        evaluated_at: 0,
                        increases,
                        status: Status::Flushed,
                        output: None,
                    },
                )
                .is_some()
            {
                return Err(Error::Conflict);
            }
        }
        Self::create_with_account(
            backend,
            config,
            live.baseline,
            live.account,
            entries,
            authority,
            encoder,
            connection,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn create_with_account(
        backend: S,
        config: Config,
        baseline: CanonicalBaseline,
        account: Account,
        entries: BTreeMap<IntentKey, Entry>,
        authority: A,
        encoder: E,
        connection: ConnectionConfig,
    ) -> Result<Self> {
        binding::config(&config)?;
        binding::baseline(&config, &baseline)?;
        if config.writer_epoch != config.journal.initial_writer_epoch {
            return Err(Error::Identity);
        }
        let server_name = config
            .route
            .authority
            .clone()
            .try_into()
            .map_err(|_| Error::Identity)?;
        let max_send_bytes = connection.limits.max_send_bytes;
        let max_receive_bytes = connection.limits.max_receive_bytes;
        let connection = Connection::connect(
            connection.address,
            Security::Tls {
                server_name,
                config: connection.tls,
            },
            connection.limits,
            connection.connect_deadline,
        )?;
        let retention_window_ns = Some(tickwire_account::DEFAULT_RETENTION_WINDOW_NS);
        let mut account = account;
        account.set_retention_window_ns(retention_window_ns);
        let journal = AccountJournal::create(backend, config.journal.clone(), baseline)
            .map_err(Error::Journal)?
            .with_retention_window_ns(retention_window_ns);
        let mut gateway = Self {
            journal,
            account,
            config,
            authority,
            encoder,
            connection: Some(connection),
            websocket: None,
            rest_started: false,
            max_send_bytes,
            max_receive_bytes,
            retained_receive: VecDeque::new(),
            entries,
            completed_mono_ns: BTreeMap::new(),
            retention_window_ns,
            last_mono_ns: 0,
            last_reactivation: None,
            pending: None,
            halted: false,
            per_order_doubt_isolation: false,
        };
        gateway.refresh_completed_entries();
        Ok(gateway)
    }
    pub fn recover(
        backend: S,
        config: Config,
        authority: A,
        encoder: E,
        required: Option<&PhysicalFrontier>,
    ) -> Result<Self> {
        Self::recover_with_reactivation(
            backend,
            config,
            authority,
            encoder,
            required,
            &mut RejectReactivation,
        )
    }
    pub fn recover_with_reactivation(
        backend: S,
        config: Config,
        mut authority: A,
        encoder: E,
        required: Option<&PhysicalFrontier>,
        verifier: &mut impl ReactivationVerifier,
    ) -> Result<Self> {
        binding::config(&config)?;
        let recovered = AccountJournal::recover(
            backend,
            config.journal.clone(),
            config.writer_epoch,
            required,
            &mut recovery::Verifier {
                authority: &mut authority,
                reactivation: verifier,
            },
        )
        .map_err(Error::Journal)?;
        binding::baseline(&config, recovered.journal.baseline())?;
        let entries = recovery::entries(&config, recovered.journal.records())?;
        let last_reactivation =
            recovered
                .journal
                .records()
                .iter()
                .rev()
                .find_map(|r| match r.record() {
                    RecoveryRecord::Reactivated(b) => Some(b.clone()),
                    _ => None,
                });
        let retention_window_ns = Some(tickwire_account::DEFAULT_RETENTION_WINDOW_NS);
        let mut account = recovered.account;
        account.set_retention_window_ns(retention_window_ns);
        for (key, entry) in &entries {
            if is_management_command(&entry.command)
                && !matches!(entry.status, Status::Aborted | Status::Flushed)
            {
                account.set_management_retired(*key, None);
            }
        }
        let journal = recovered
            .journal
            .with_retention_window_ns(retention_window_ns);
        let mut gateway = Self {
            journal,
            account,
            config,
            authority,
            encoder,
            connection: None,
            websocket: None,
            rest_started: false,
            max_send_bytes: 0,
            max_receive_bytes: 0,
            retained_receive: VecDeque::new(),
            entries,
            completed_mono_ns: BTreeMap::new(),
            retention_window_ns,
            last_mono_ns: 0,
            last_reactivation,
            pending: None,
            halted: true,
            per_order_doubt_isolation: false,
        };
        gateway.refresh_completed_entries();
        Ok(gateway)
    }

    pub fn with_retention_window_ns(mut self, window_ns: Option<u64>) -> Self {
        self.set_retention_window_ns(window_ns);
        self
    }

    pub fn set_retention_window_ns(&mut self, window_ns: Option<u64>) {
        self.retention_window_ns = window_ns;
        self.account.set_retention_window_ns(window_ns);
        self.journal.set_retention_window_ns(window_ns);
    }

    pub fn retention_window_ns(&self) -> Option<u64> {
        self.retention_window_ns
    }

    fn refresh_completed_entries(&mut self) {
        let pending_key = self.pending.map(|(k, _)| k);
        let mut remove_keys = Vec::new();
        let mut insert_keys = Vec::new();
        for (key, entry) in &self.entries {
            let completed = matches!(entry.status, Status::Aborted | Status::Flushed)
                && pending_key != Some(*key)
                && !self.account.is_intent_active(*key);
            if !completed {
                if self.completed_mono_ns.contains_key(key) {
                    remove_keys.push(*key);
                }
            } else if !self.completed_mono_ns.contains_key(key) {
                let stamp = self
                    .last_mono_ns
                    .max(entry.evaluated_at)
                    .max(self.account.intent_retired_mono_ns(*key).unwrap_or(0));
                insert_keys.push((*key, stamp));
            }
        }
        for key in remove_keys {
            self.completed_mono_ns.remove(&key);
        }
        for (key, stamp) in insert_keys {
            self.completed_mono_ns.insert(key, stamp);
        }
    }

    fn reclaim_one_retired_management_entry(&mut self) -> bool {
        let pending_key = self.pending.map(|(k, _)| k);
        let candidate = self
            .entries
            .iter()
            .filter(|(key, entry)| {
                is_management_command(&entry.command)
                    && matches!(entry.status, Status::Aborted | Status::Flushed)
                    && pending_key != Some(**key)
                    && !self.account.is_intent_active(**key)
                    && !self.account.is_confirmed_amendment(**key)
            })
            .min_by_key(|(key, entry)| {
                (
                    self.completed_mono_ns
                        .get(key)
                        .copied()
                        .unwrap_or(entry.evaluated_at),
                    entry.evaluated_at,
                    **key,
                )
            })
            .map(|(key, _)| *key);
        if let Some(key) = candidate {
            let _ = self.account.evict_retired_management_admission(key);
            self.entries.remove(&key);
            self.completed_mono_ns.remove(&key);
            true
        } else {
            false
        }
    }

    /// Evicts completed (`Aborted` or `Flushed`) gateway entries whose underlying `Account`
    /// intent is retired and whose monotonic completion window has elapsed.
    pub fn evict_retired_entries(&mut self, now_mono_ns: u64) -> usize {
        if now_mono_ns > 0 {
            self.last_mono_ns = self.last_mono_ns.max(now_mono_ns);
            self.account.evict_expired(now_mono_ns);
        }
        self.refresh_completed_entries();
        let Some(window_ns) = self.retention_window_ns else {
            return 0;
        };
        if now_mono_ns == 0 {
            return 0;
        }
        let pending_key = self.pending.map(|(k, _)| k);
        let to_evict: Vec<IntentKey> = self
            .entries
            .iter()
            .filter_map(|(key, entry)| {
                if !matches!(entry.status, Status::Aborted | Status::Flushed) {
                    return None;
                }
                if pending_key == Some(*key)
                    || self.account.is_intent_active(*key)
                    || self.account.has_admission(*key)
                {
                    return None;
                }
                let completed_at = *self.completed_mono_ns.get(key)?;
                (now_mono_ns >= completed_at.saturating_add(window_ns)).then_some(*key)
            })
            .collect();
        let count = to_evict.len();
        for key in to_evict {
            self.entries.remove(&key);
            self.completed_mono_ns.remove(&key);
        }
        count
    }

    fn maybe_compact_journal(&mut self, now_mono_ns: u64) {
        if self.retention_window_ns.is_none()
            || self.journal.records().len() + 1 < tickwire_account_journal::MAX_RECORDS
        {
            return;
        }
        if now_mono_ns > 0 {
            self.evict_retired_entries(now_mono_ns);
        }
        let mut keep: BTreeSet<IntentKey> = self.account.admission_keys().collect();
        keep.extend(self.entries.keys().copied());
        self.journal
            .compact_before(&keep, tickwire_account_journal::MAX_RECORDS / 2);
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
        if self.pending.is_some()
            || self.connection.is_none()
            || self.websocket_blocks_application()
        {
            return Err(Error::Busy);
        }
        if self.websocket.is_none() {
            self.rest_started = true;
        }
        binding::command(&self.config, &command, true)?;
        if evaluated_at > 0 {
            self.evict_retired_entries(evaluated_at);
        }
        if is_management_command(&command) {
            let mgmt_count = self
                .entries
                .iter()
                .filter(|(k, e)| {
                    is_management_command(&e.command) && !self.account.is_confirmed_amendment(**k)
                })
                .count();
            if mgmt_count >= tickwire_account::MAX_MANAGEMENT_ADMISSIONS
                && !self.reclaim_one_retired_management_entry()
            {
                return Err(Error::Capacity);
            }
        } else {
            let order_count = self
                .entries
                .values()
                .filter(|e| !is_management_command(&e.command))
                .count();
            if order_count >= self.config.max_intents {
                return Err(Error::Capacity);
            }
        }
        let wire = self.encoder.encode(&command).map_err(Error::Encoding)?;
        if wire.command_digest() != command.digest()
            || wire.authority() != self.config.route.authority
        {
            return Err(Error::Identity);
        }
        let send = PreparedSend::new(wire.bytes(), deadline, self.max_send_bytes)?;
        self.maybe_compact_journal(evaluated_at);
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
        candidate.set_management_retired(key, None);
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
        let commit = match self.authority.commit_state(&admission, &receipt) {
            Ok(state) => state,
            Err(_) => {
                // Local evidence already exists. Retain its identity/reservation
                // and resolve through the normal no-output path below.
                CommitState::Pending
            }
        };
        let initial_status = if commit == CommitState::Ready {
            Status::Queued
        } else {
            Status::AwaitingCommit
        };
        self.entries.insert(
            key,
            Entry {
                command,
                committed: Some(CommittedAdmission { admission, receipt }),
                evaluated_at,
                increases,
                status: initial_status,
                output: Some(OutputProgress {
                    accepted_bytes: 0,
                    total_bytes: wire.bytes().len(),
                }),
            },
        );
        match self.connection.as_mut().ok_or(Error::Halted)?.queue(send) {
            Ok(token) => {
                self.pending = Some((key, token));
                Ok(initial_status)
            }
            Err(error) => {
                // queue does not accept application bytes, even when it fails.
                self.abort(key)?;
                self.on_transport_error();
                Err(error.into())
            }
        }
    }
    pub fn poll(&mut self, now: Instant, budget: IoBudget) -> Result<()> {
        if self.websocket_blocks_application() {
            return Err(Error::Busy);
        }
        if self.halted {
            return Err(Error::Halted);
        }
        let driven = self
            .connection
            .as_mut()
            .ok_or(Error::Busy)?
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
            && self.connection.is_some()
            && driven.as_ref().is_ok_and(|r| !r.peer_closed)
        {
            let entry = &self.entries[&key];
            if entry.output.is_some_and(|p| p.accepted_bytes == 0) {
                let committed = entry.committed.as_ref().ok_or(Error::Invalid)?;
                match self
                    .authority
                    .commit_state(&committed.admission, &committed.receipt)
                {
                    Ok(CommitState::Pending) => {
                        self.entries.get_mut(&key).unwrap().status = Status::AwaitingCommit;
                        return match driven {
                            Ok(_) => Ok(()),
                            Err(error) => {
                                self.on_transport_error();
                                Err(error.into())
                            }
                        };
                    }
                    Ok(CommitState::Ready) => {
                        self.entries.get_mut(&key).unwrap().status = Status::Queued;
                    }
                    Err(_) => {
                        self.cancel_pending()?;
                        return Ok(());
                    }
                }
            }
            let entry = &self.entries[&key];
            let admission = &entry.committed.as_ref().ok_or(Error::Invalid)?.admission;
            let authority = &mut self.authority;
            let account = &self.account;
            let epoch = self.config.writer_epoch;
            let mut sampled_now = 0u64;
            let result = self
                .connection
                .as_mut()
                .ok_or(Error::Busy)?
                .try_send(now, || {
                    authority
                        .current_gate(&entry.command, admission)
                        .is_ok_and(|gate| {
                            sampled_now = gate.now;
                            binding::permits(gate, entry, epoch)
                                && account.check_admission(admission, gate.now).is_ok()
                        })
                });
            if sampled_now > 0 {
                self.last_mono_ns = self.last_mono_ns.max(sampled_now);
            }
            match result {
                Ok(report) => self.apply_report(report)?,
                Err(error) => {
                    self.on_transport_error();
                    return Err(error.into());
                }
            }
        }
        match driven {
            Ok(report) if !report.peer_closed => Ok(()),
            Ok(_) => {
                self.on_transport_error();
                Ok(())
            }
            Err(error) => {
                self.on_transport_error();
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
        let report = match self.connection.as_mut().ok_or(Error::Busy)?.cancel() {
            Ok(report) => report,
            Err(error) => {
                self.on_transport_error();
                return Err(error.into());
            }
        };
        self.apply_report(report)
    }
    /// Only this live transport owner can prove that no command byte has crossed
    /// the output boundary. Positive or unknown progress retains its reservation.
    pub fn terminate_unsent_strategy(
        &mut self,
        strategy: u64,
        manager: tickwire_account::ManagerIdentity,
    ) -> Result<bool> {
        let Some((key, _)) = self.pending else {
            return Ok(false);
        };
        let entry = self.entries.get(&key).ok_or(Error::Invalid)?;
        let context = &entry.command.command().context;
        if context.strategy.logical_strategy_id != strategy
            || context.strategy.instance_id != manager.instance
            || context.request.strategy_generation != manager.generation
            || entry.output.is_none_or(|p| p.accepted_bytes != 0)
        {
            return Ok(false);
        }
        self.cancel_pending()?;
        Ok(true)
    }
    /// The input owner has already persisted the raw observation. Publish only
    /// after this journal synchronizes the validated account transition.
    pub fn record_ingress_event(
        &mut self,
        input_stream: [u8; 16],
        input_sequence: u64,
        event: CanonicalEvent,
    ) -> Result<Applied> {
        if let Some(old) = self.journal.ingress_record(input_stream, input_sequence) {
            if matches!(old,RecoveryRecord::IngressEvent {event: prior,..} if prior.as_ref()==&event)
            {
                return Ok(Applied::Duplicate);
            }
            self.halt();
            return Err(Error::Conflict);
        }
        let event_now = event.event().identity.received_mono_ns;
        if event_now > 0 {
            self.last_mono_ns = self.last_mono_ns.max(event_now);
        }
        self.maybe_compact_journal(self.last_mono_ns);
        let mut candidate = self.account.clone();
        let applied = match candidate.apply_event(&event) {
            Ok(applied) => applied,
            Err(error) => {
                self.account.freeze();
                return Err(Error::Account(error));
            }
        };
        if let Err(error) = self.journal.append(RecoveryRecord::IngressEvent {
            input_stream,
            input_sequence,
            event: Box::new(event),
        }) {
            self.halt();
            return Err(Error::Journal(error));
        }
        self.account = candidate;
        self.refresh_completed_entries();
        Ok(applied)
    }
    pub fn record_account_input(
        &mut self,
        input_stream: [u8; 16],
        input_sequence: u64,
        input: tickwire_venue::CanonicalAccountInput,
    ) -> Result<Applied> {
        let record = RecoveryRecord::IngressAccountInput {
            input_stream,
            input_sequence,
            input: Box::new(input.clone()),
        };
        if let Some(old) = self.journal.ingress_record(input_stream, input_sequence) {
            if old == &record {
                return Ok(Applied::Duplicate);
            }
            self.halt();
            return Err(Error::Conflict);
        }
        self.maybe_compact_journal(self.last_mono_ns);
        let mut candidate = self.account.clone();
        let applied = match candidate.apply_account_input(&input) {
            Ok(v) => v,
            Err(e) => {
                self.account.freeze();
                return Err(Error::Account(e));
            }
        };
        if let Err(e) = self.journal.append(record) {
            self.halt();
            return Err(Error::Journal(e));
        }
        self.account = candidate;
        Ok(applied)
    }
    /// Verify the original query closure, synchronize tag14, then publish the candidate.
    /// This records reconciliation without granting permission to send or releasing funds.
    pub fn close_uncreated_algo(
        &mut self,
        binding: tickwire_account::UncreatedAlgoClosed,
        verifier: &mut impl ReactivationVerifier,
    ) -> Result<Applied> {
        if let Some(old) = self.account.uncreated_algo_closure(&binding.structure_id) {
            if old == &binding {
                return Ok(Applied::Duplicate);
            }
            self.halt();
            return Err(Error::Conflict);
        }
        if self.pending.is_some() {
            return Err(Error::Busy);
        }
        if binding.baseline_digest != self.baseline().digest()
            || binding.history_digest != self.history_frontier().digest()
            || binding.scope != self.config.journal.scope
            || binding.ledger_sequence != self.account.ledger_sequence()
        {
            return Err(Error::Identity);
        }
        verifier
            .verify_uncreated_algo_live(
                self.baseline(),
                &self.account,
                self.history_frontier(),
                &binding,
                self.config.writer_epoch,
            )
            .map_err(Error::Authority)?;
        let mut candidate = self.account.clone();
        let applied = candidate
            .close_uncreated_algo(&binding)
            .map_err(Error::Account)?;
        if let Err(error) = self
            .journal
            .append(RecoveryRecord::UncreatedAlgoClosed(Box::new(binding)))
        {
            self.halt();
            return Err(Error::Journal(error));
        }
        self.account = candidate;
        self.refresh_completed_entries();
        Ok(applied)
    }
    pub fn reconcile_account_inputs(
        &mut self,
        binding: tickwire_account::AccountInputsReconciled,
        verifier: &mut impl ReactivationVerifier,
    ) -> Result<Applied> {
        if let Some(old) = self.account.account_inputs_closure(binding.history_digest) {
            if old == &binding {
                return Ok(Applied::Duplicate);
            }
            self.halt();
            return Err(Error::Conflict);
        }
        if self.pending.is_some() {
            return Err(Error::Busy);
        }
        if binding.baseline_digest != self.baseline().digest()
            || binding.history_digest != self.history_frontier().digest()
            || binding.scope != self.config.journal.scope
            || binding.ledger_sequence != self.account.ledger_sequence()
        {
            return Err(Error::Identity);
        }
        verifier
            .verify_account_inputs_closure_live(
                self.baseline(),
                &self.account,
                self.history_frontier(),
                &binding,
                self.config.writer_epoch,
            )
            .map_err(Error::Authority)?;
        let mut candidate = self.account.clone();
        let applied = candidate
            .reconcile_account_inputs(&binding)
            .map_err(Error::Account)?;
        if let Err(error) = self
            .journal
            .append(RecoveryRecord::AccountInputsReconciled(Box::new(binding)))
        {
            self.halt();
            return Err(Error::Journal(error));
        }
        self.account = candidate;
        Ok(applied)
    }
    /// Only a complete verified approval can clear observation explanations.
    /// Candidate validation precedes durable tag15; synchronization precedes publication.
    pub fn associate_income_observations(
        &mut self,
        binding: tickwire_account::IncomeObservationsAssociated,
        verifier: &mut impl ReactivationVerifier,
    ) -> Result<Applied> {
        if let Some(old) = self.account.income_association(binding.request_id) {
            if old == &binding {
                return Ok(Applied::Duplicate);
            }
            self.halt();
            return Err(Error::Conflict);
        }
        if self.pending.is_some() {
            return Err(Error::Busy);
        }
        if binding.baseline_digest != self.baseline().digest()
            || binding.history_digest != self.history_frontier().digest()
            || binding.scope != self.config.journal.scope
            || binding.ledger_sequence != self.account.ledger_sequence()
        {
            return Err(Error::Identity);
        }
        verifier
            .verify_income_association_live(
                self.baseline(),
                &self.account,
                self.history_frontier(),
                self.journal.records(),
                &binding,
                self.config.writer_epoch,
            )
            .map_err(Error::Authority)?;
        let mut candidate = self.account.clone();
        let applied = candidate
            .associate_income_observations(&binding)
            .map_err(Error::Account)?;
        if let Err(error) = self
            .journal
            .append(RecoveryRecord::IncomeObservationsAssociated(Box::new(
                binding,
            )))
        {
            self.halt();
            return Err(Error::Journal(error));
        }
        self.account = candidate;
        Ok(applied)
    }
    pub fn install_income_baseline(
        &mut self,
        binding: tickwire_account::IncomeBaseline,
        verifier: &mut impl ReactivationVerifier,
    ) -> Result<Applied> {
        if self.pending.is_some() {
            return Err(Error::Invalid);
        }
        verifier
            .verify_income_baseline(
                self.journal.baseline(),
                &self.account,
                self.history_frontier(),
                &binding,
            )
            .map_err(Error::Authority)?;
        let mut candidate = self.account.clone();
        let applied = candidate
            .apply_income_baseline(&binding)
            .map_err(Error::Account)?;
        if applied == Applied::Duplicate {
            return Ok(applied);
        }
        if let Err(e) = self
            .journal
            .append(RecoveryRecord::IncomeBaseline(Box::new(binding)))
        {
            self.halt();
            return Err(Error::Journal(e));
        }
        self.account = candidate;
        Ok(applied)
    }
    pub fn ingress_account_inputs(
        &self,
    ) -> impl Iterator<Item = ([u8; 16], u64, &tickwire_venue::CanonicalAccountInput)> {
        self.journal.ingress_account_inputs()
    }
    pub fn ingress_event(&self, stream: [u8; 16], sequence: u64) -> Option<&CanonicalEvent> {
        self.journal.ingress_event(stream, sequence)
    }
    pub fn ingress_events(&self) -> impl Iterator<Item = ([u8; 16], u64, &CanonicalEvent)> {
        self.journal.ingress_events()
    }
    pub fn command(&self, key: IntentKey) -> Option<&CanonicalCommand> {
        self.entries.get(&key).map(|e| &e.command)
    }
    pub fn baseline(&self) -> &CanonicalBaseline {
        self.journal.baseline()
    }
    pub fn admission_origin(
        &self,
        key: IntentKey,
    ) -> Option<&tickwire_account_journal::AdmissionOrigin> {
        self.journal.admission_origin(key)
    }
    pub fn history_frontier(&self) -> &HistoryFrontier {
        self.journal.history_frontier()
    }
    pub fn journal_config(&self) -> &tickwire_account_journal::Config {
        self.journal.config()
    }
    pub fn freeze_account(&mut self) {
        self.account.freeze();
    }
    /// Walk back only to the latest activation of this account owner.
    pub fn latest_reactivation(&self) -> Option<&tickwire_account::Reactivation> {
        self.journal
            .records()
            .iter()
            .rev()
            .find_map(|r| match r.record() {
                RecoveryRecord::Reactivated(b) => Some(b.as_ref()),
                _ => None,
            })
            .or(self.last_reactivation.as_deref())
    }
    pub fn recovery_writer_floor(&self) -> Option<u64> {
        self.journal.recovery_writer_floor()
    }
    pub fn writer_epoch(&self) -> u64 {
        self.config.writer_epoch
    }
    /// Synchronize the verified activation before publishing the unfrozen account.
    /// A healthy existing sender can keep its writer; recovery requires a fresh connection.
    pub fn reactivate(
        &mut self,
        binding: tickwire_account::Reactivation,
        connection: Option<ConnectionConfig>,
        verifier: &mut impl ReactivationVerifier,
    ) -> Result<ActivationReceipt> {
        if self.pending.is_some() || !self.retained_receive.is_empty() {
            return Err(Error::Busy);
        }
        if binding.baseline_digest != self.baseline().digest()
            || binding.history_digest != self.history_frontier().digest()
            || binding.scope != self.config.journal.scope
            || binding.ledger_sequence != self.account.ledger_sequence()
        {
            return Err(Error::Identity);
        }
        if self.halted != connection.is_some()
            || (!self.halted && binding.writer_epoch != self.config.writer_epoch)
            || (self.halted
                && !self.journal.recovered()
                && binding.writer_epoch <= self.config.writer_epoch)
        {
            return Err(Error::Identity);
        }
        verifier
            .verify(
                self.baseline(),
                &self.account,
                self.history_frontier(),
                &binding,
            )
            .map_err(Error::Authority)?;
        let mut candidate = self.account.clone();
        candidate
            .apply_reactivation(&binding)
            .map_err(Error::Account)?;
        let replacement = if let Some(c) = connection {
            verifier
                .before_connect(&binding)
                .map_err(Error::Authority)?;
            let limits = c.limits;
            let server_name = self
                .config
                .route
                .authority
                .clone()
                .try_into()
                .map_err(|_| Error::Identity)?;
            Some((
                Connection::connect(
                    c.address,
                    Security::Tls {
                        server_name,
                        config: c.tls,
                    },
                    limits,
                    c.connect_deadline,
                )?,
                limits,
            ))
        } else {
            None
        };
        if let Err(error) = self.journal.append_reactivation(binding.clone()) {
            self.halt();
            return Err(Error::Journal(error));
        }
        if binding.evaluated_at > 0 {
            self.last_mono_ns = self.last_mono_ns.max(binding.evaluated_at);
        }
        self.account = candidate;
        self.config.writer_epoch = binding.writer_epoch;
        self.last_reactivation = Some(Box::new(binding));
        if let Some((connection, limits)) = replacement {
            self.connection = Some(connection);
            self.websocket = None;
            self.rest_started = false;
            self.max_send_bytes = limits.max_send_bytes;
            self.max_receive_bytes = limits.max_receive_bytes;
        }
        self.halted = false;
        self.refresh_completed_entries();
        // Historical command keys remain in entries, so retries return their old outcome.
        Ok(ActivationReceipt {
            ledger_sequence: self.account.ledger_sequence(),
            history_digest: self.history_frontier().digest(),
        })
    }
    pub fn record_event(&mut self, event: CanonicalEvent) -> Result<Applied> {
        self.record_fact(RecoveryRecord::VenueEvent(Box::new(event)))
    }
    pub fn record_fact(&mut self, record: RecoveryRecord) -> Result<Applied> {
        if matches!(
            record,
            RecoveryRecord::Admit { .. }
                | RecoveryRecord::Aborted { .. }
                | RecoveryRecord::IngressEvent { .. }
                | RecoveryRecord::Reactivated(_)
                | RecoveryRecord::StrategyHandover(_)
                | RecoveryRecord::AccountInputsReconciled(_)
                | RecoveryRecord::UncreatedAlgoClosed(_)
                | RecoveryRecord::IncomeObservationsAssociated(_)
                | RecoveryRecord::IncomeBaseline(_)
                | RecoveryRecord::IngressAccountInput { .. }
        ) {
            return Err(Error::Invalid);
        }
        if let RecoveryRecord::FeesReconciled { order, cumulative } = &record {
            // This owner holds the account across append and apply.
            self.account
                .check_fee_closure(order, *cumulative)
                .map_err(|error| {
                    self.account.freeze();
                    Error::Account(error)
                })?;
        }
        if let RecoveryRecord::VenueEvent(ev) = &record {
            let t = ev.event().identity.received_mono_ns;
            if t > 0 {
                self.last_mono_ns = self.last_mono_ns.max(t);
            }
        }
        self.maybe_compact_journal(self.last_mono_ns);
        if let Err(error) = self.journal.append(record.clone()) {
            self.halt();
            return Err(Error::Journal(error));
        }
        let applied = recovery::apply_fact(&mut self.account, &record);
        if applied.is_err() {
            self.account.freeze();
        }
        self.refresh_completed_entries();
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
    /// Ordered durable account records after the supplied exclusive WAL cursor.
    /// This includes local outcomes as well as externally received facts.
    pub fn journal_records_after(
        &self,
        position: u64,
        maximum: usize,
    ) -> Result<impl Iterator<Item = (u64, &CanonicalRecord)>> {
        let start = usize::try_from(position).map_err(|_| Error::Invalid)?;
        let evicted = self.journal.evicted_prefix();
        if start < evicted || start > evicted + self.journal.records().len() || maximum == 0 {
            return Err(Error::Invalid);
        }
        let local_start = start - evicted;
        Ok(self.journal.records()[local_start..]
            .iter()
            .take(maximum)
            .enumerate()
            .map(move |(index, record)| (position + index as u64 + 1, record)))
    }
    pub fn journal_position(&self) -> u64 {
        self.journal.position()
    }
    pub fn export_durable(
        &mut self,
        first_sequence: u64,
        maximum_records: usize,
        maximum_bytes: usize,
    ) -> Result<tickwire_journal::DurableExport> {
        self.journal
            .export_durable(first_sequence, maximum_records, maximum_bytes)
            .map_err(Error::Journal)
    }
    pub fn halted(&self) -> bool {
        self.halted
    }
    pub fn transport_ready(&self) -> bool {
        self.connection.as_ref().is_some_and(Connection::is_ready)
    }
    pub fn read(&mut self, out: &mut [u8]) -> usize {
        let retained = out.len().min(self.retained_receive.len());
        for slot in &mut out[..retained] {
            *slot = self.retained_receive.pop_front().unwrap();
        }
        retained
            + self
                .connection
                .as_mut()
                .map_or(0, |c| c.read(&mut out[retained..]))
    }
    /// Volatile evidence from this process. Recovery never fabricates output progress.
    pub fn output_progress(&self, intent: IntentKey) -> Option<OutputProgress> {
        self.entries.get(&intent).and_then(|e| e.output)
    }
    pub fn stop_sending(&mut self) {
        self.halt();
    }
    pub fn register(
        &mut self,
        registry: &tickwire_transport::mio::Registry,
        token: tickwire_transport::mio::Token,
    ) -> Result<()> {
        if self.halted {
            return Err(Error::Halted);
        }
        Ok(self
            .connection
            .as_mut()
            .ok_or(Error::Busy)?
            .register(registry, token)?)
    }
    pub fn reregister(
        &mut self,
        registry: &tickwire_transport::mio::Registry,
        token: tickwire_transport::mio::Token,
    ) -> Result<()> {
        if self.halted {
            return Err(Error::Halted);
        }
        Ok(self
            .connection
            .as_mut()
            .ok_or(Error::Busy)?
            .reregister(registry, token)?)
    }
    fn apply_report(&mut self, report: SendReport) -> Result<()> {
        let (key, token) = self.pending.ok_or(Error::Invalid)?;
        if token != report.id {
            self.halt();
            return Err(Error::Identity);
        }
        if let Some(output) = self.entries.get_mut(&key).and_then(|e| e.output.as_mut()) {
            if report.accepted_bytes < output.accepted_bytes
                || report.accepted_bytes > output.total_bytes
            {
                self.halt();
                return Err(Error::Invalid);
            }
            output.accepted_bytes = report.accepted_bytes;
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
                let stamp = self.last_mono_ns.max(self.entries[&key].evaluated_at);
                self.account.set_management_retired(key, Some(stamp));
                self.refresh_completed_entries();
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
            SendState::Failed(_) => self.on_transport_error(),
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
        self.maybe_compact_journal(self.last_mono_ns);
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
        let stamp = self.last_mono_ns.max(self.entries[&key].evaluated_at);
        self.account.set_management_retired(key, Some(stamp));
        self.refresh_completed_entries();
        Ok(())
    }
    fn halt(&mut self) {
        self.halted = true;
        self.account.freeze();
        if let Some(connection) = self.connection.as_mut() {
            let mut bytes = [0u8; 4096];
            loop {
                let available = self
                    .max_receive_bytes
                    .saturating_sub(self.retained_receive.len());
                if available == 0 {
                    break;
                }
                let limit = available.min(bytes.len());
                let n = connection.read(&mut bytes[..limit]);
                if n == 0 {
                    break;
                }
                self.retained_receive.extend(&bytes[..n]);
            }
        }
        self.connection = None;
        self.pending = None;
        for entry in self.entries.values_mut() {
            if entry.status != Status::Aborted {
                entry.status = Status::Unknown;
            }
        }
        self.completed_mono_ns.retain(|k, _| {
            self.entries
                .get(k)
                .is_some_and(|e| e.status == Status::Aborted)
        });
    }

    pub(crate) fn on_transport_error(&mut self) {
        if self.per_order_doubt_isolation {
            self.drop_broken_transport_isolated();
        } else {
            self.halt();
        }
    }

    pub fn with_per_order_doubt_isolation(mut self, enabled: bool) -> Self {
        self.per_order_doubt_isolation = enabled;
        self
    }

    pub fn set_per_order_doubt_isolation(&mut self, enabled: bool) {
        self.per_order_doubt_isolation = enabled;
    }

    pub fn per_order_doubt_isolation(&self) -> bool {
        self.per_order_doubt_isolation
    }

    pub fn has_transport_connection(&self) -> bool {
        self.connection.is_some()
    }

    pub fn isolate_intent_in_doubt(
        &mut self,
        key: IntentKey,
        reason: tickwire_account::DoubtReason,
    ) -> Result<Vec<tickwire_account::OrderKey>> {
        let (command, orders) = {
            let entry = self.entries.get(&key).ok_or(Error::Invalid)?;
            let orders = entry
                .committed
                .as_ref()
                .map(|c| c.admission.orders().to_vec())
                .or_else(|| self.account.admission(key).map(|a| a.orders().to_vec()))
                .unwrap_or_default();
            (entry.command.clone(), orders)
        };
        let body = &command.command().body;
        let instrument = command.command().context.instrument.instrument_id;
        let mut affected = Vec::new();
        if !orders.is_empty() {
            if matches!(body, tickwire_venue::CommandBody::New(_)) && orders.len() == 1 {
                let order_key = orders[0].clone();
                if let Some(view) = self.account.order(&order_key) {
                    if !view.aborted
                        && !view.uncreated
                        && !(view.terminal && view.remaining_quantity.atoms() == 0 && !view.unknown)
                    {
                        self.account
                            .mark_order_in_doubt(&order_key, reason)
                            .map_err(Error::Account)?;
                        affected.push(order_key);
                    }
                } else {
                    self.freeze_account();
                    return Err(Error::Account(tickwire_account::Error::UnknownOrder));
                }
            } else {
                self.freeze_account();
                return Err(Error::Conflict);
            }
        } else {
            match body {
                tickwire_venue::CommandBody::Cancel(target)
                | tickwire_venue::CommandBody::AmendKeepPriority { target, .. } => {
                    if let Some(order_key) = self.account.resolve_order_target(instrument, target)
                        && let Some(view) = self.account.order(&order_key)
                        && !view.terminal
                        && !view.uncreated
                        && !view.aborted
                    {
                        self.account
                            .mark_order_in_doubt(&order_key, reason)
                            .map_err(Error::Account)?;
                        affected.push(order_key);
                    }
                }
                tickwire_venue::CommandBody::Query(_) => {}
                _ => {
                    self.freeze_account();
                    return Err(Error::Conflict);
                }
            }
        }
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.status = Status::Unknown;
        }
        self.completed_mono_ns.remove(&key);
        if self.pending.is_some_and(|(k, _)| k == key) {
            self.pending = None;
        }
        Ok(affected)
    }

    pub fn clear_order_in_doubt(&mut self, key: &tickwire_account::OrderKey) -> Result<()> {
        self.account
            .clear_order_in_doubt(key)
            .map_err(Error::Account)
    }

    pub fn drop_broken_transport_isolated(&mut self) -> Option<IntentKey> {
        if let Some(connection) = self.connection.as_mut() {
            let mut bytes = [0u8; 4096];
            loop {
                let available = self
                    .max_receive_bytes
                    .saturating_sub(self.retained_receive.len());
                if available == 0 {
                    break;
                }
                let limit = available.min(bytes.len());
                let n = connection.read(&mut bytes[..limit]);
                if n == 0 {
                    break;
                }
                self.retained_receive.extend(&bytes[..n]);
            }
        }
        self.connection = None;
        self.websocket = None;
        self.rest_started = false;
        let failed_pending = self.pending.take().map(|(k, _)| k);
        if let Some(key) = failed_pending {
            let zero_bytes_unsent = self.entries.get(&key).is_some_and(|e| {
                matches!(e.status, Status::Queued | Status::AwaitingCommit)
                    && e.output.is_some_and(|p| p.accepted_bytes == 0)
            });
            if zero_bytes_unsent {
                if self.abort(key).is_err() {
                    self.halt();
                }
            } else {
                let reason = match self.entries.get(&key).map(|e| &e.command.command().body) {
                    Some(tickwire_venue::CommandBody::Cancel(_)) => {
                        tickwire_account::DoubtReason::CancelAmbiguous
                    }
                    Some(tickwire_venue::CommandBody::AmendKeepPriority { .. }) => {
                        tickwire_account::DoubtReason::AmendAmbiguous
                    }
                    _ => tickwire_account::DoubtReason::PlaceAmbiguous,
                };
                if self.isolate_intent_in_doubt(key, reason).is_err() {
                    self.halt();
                }
            }
        }
        failed_pending
    }

    pub fn install_transport_connection(
        &mut self,
        connection: Connection,
        limits: tickwire_transport::Limits,
    ) -> Result<()> {
        if self.halted || self.account.frozen() {
            return Err(Error::Halted);
        }
        if self.connection.is_some() || self.pending.is_some() || !self.retained_receive.is_empty()
        {
            return Err(Error::Busy);
        }
        if [
            limits.max_send_bytes,
            limits.max_receive_bytes,
            limits.tls_buffer_bytes,
            limits.max_accept_bytes,
        ]
        .iter()
        .any(|n| *n == 0 || *n > 16 * 1024 * 1024)
        {
            return Err(Error::Transport(tickwire_transport::Error::Bounds));
        }
        self.connection = Some(connection);
        self.websocket = None;
        self.rest_started = false;
        self.max_send_bytes = limits.max_send_bytes;
        self.max_receive_bytes = limits.max_receive_bytes;
        Ok(())
    }
}

fn is_management_command(command: &CanonicalCommand) -> bool {
    matches!(
        command.command().body,
        tickwire_venue::CommandBody::Cancel(_)
            | tickwire_venue::CommandBody::Query(_)
            | tickwire_venue::CommandBody::CancelList(_)
            | tickwire_venue::CommandBody::CancelAlgo(_)
            | tickwire_venue::CommandBody::AmendKeepPriority { .. }
    )
}
