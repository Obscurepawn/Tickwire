//! Stateless in-memory execution gateway (`AccountGateway`), cold Exchange REST bootstrap
//! (`VerifiedLiveBootstrap`), multi-slot `OrderConnectionPool` + `SharedRateLimiter` single-pick
//! admission, per-order transport-doubt isolation, and `AccountSpec` / `AccountManager` (`8.3`).
#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, VecDeque},
    net::SocketAddr,
    sync::Arc,
    time::Instant,
};
use tickwire_account::{Account, Admission, Applied, IntentKey, RecoveryRecord, Scope};
pub use tickwire_account::{ProjectedFill, ProjectedPriceInput, StpBook};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_quota::rate_limiter::RateReject;
use tickwire_transport::{
    Connection, IoBudget, Limits, PreparedSend, Security, SendReport, SendState, rustls,
};
use tickwire_venue::{
    CanonicalAccountInput, CanonicalCommand, CanonicalEvent, Environment, Hash, Product,
};

pub mod account_manager;
mod binding;
pub mod pool;
pub mod websocket;

pub use account_manager::{
    AccountManager, AccountManagerConfig, AccountManagerError, AccountRateLimitConfig,
    AccountRoutingError, AccountSpec, Credentials, DEFAULT_URL_CONNECTIONS, MAX_ACCOUNT_REF,
    MAX_STRAT_ID, RawAccountBlock, UrlBindSpec, UrlPoolConfig,
};
pub use pool::{
    MAX_ORDER_CONNECTIONS, MAX_SHARED_INFLIGHT, OrderConnectionPool, OrderRoute, OrderRouter,
    OrderSlotSpec, RouteHealth, RouterConfig, SampleKind, SendSlot,
};

/// Venue-agnostic target route metadata for an `AccountGateway` instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub venue: u32,
    pub environment: Environment,
    pub product: Product,
    pub account: u64,
    /// TLS SNI / HTTP `Host` authority (e.g., `"ws-fapi.example.invalid"`).
    pub authority: String,
}

impl Route {
    pub fn default_ws_path(&self) -> Result<&'static str> {
        match self.product {
            Product::Spot => Ok("/ws-api/v3"),
            Product::LinearPerpetual | Product::LinearFuture => Ok("/ws-fapi/v1"),
            Product::InversePerpetual | Product::InverseFuture => Ok("/ws-dapi/v1"),
            Product::Option => Ok("/ws-eapi/v1"),
            _ => Err(Error::Identity),
        }
    }
}

/// Outbound TLS socket connection parameters for single-socket `AccountGateway` creation.
#[derive(Clone)]
pub struct ConnectionConfig {
    pub address: SocketAddr,
    pub tls: Arc<rustls::ClientConfig>,
    pub limits: Limits,
    pub connect_deadline: Instant,
}

/// Deterministically encoded wire request bound to its `CanonicalCommand` digest and target host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedRequest {
    command_digest: Hash,
    authority: String,
    bytes: Vec<u8>,
}

impl EncodedRequest {
    pub fn new(command_digest: Hash, authority: impl Into<String>, bytes: Vec<u8>) -> Result<Self> {
        let authority = authority.into();
        if authority.trim().is_empty() || bytes.is_empty() {
            return Err(Error::Encoding(EncodingError::InvalidRequest));
        }
        Ok(Self {
            command_digest,
            authority,
            bytes,
        })
    }

    #[inline]
    pub fn authority(&self) -> &str {
        &self.authority
    }

    #[inline]
    pub fn command_digest(&self) -> Hash {
        self.command_digest
    }

    #[inline]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Venue-agnostic wire encoding error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodingError {
    InvalidRequest,
    UnsupportedCommand,
    Rejected(&'static str),
}

/// Stateless outbound wire encoder for [`CanonicalCommand`]s.
pub trait Encoder {
    fn encode(
        &mut self,
        command: &CanonicalCommand,
    ) -> std::result::Result<EncodedRequest, EncodingError>;
}

impl<F> Encoder for F
where
    F: FnMut(&CanonicalCommand) -> std::result::Result<EncodedRequest, EncodingError>,
{
    fn encode(
        &mut self,
        command: &CanonicalCommand,
    ) -> std::result::Result<EncodedRequest, EncodingError> {
        self(command)
    }
}

/// Errors produced by [`AccountGateway`].
#[derive(Debug)]
pub enum Error {
    Account(tickwire_account::Error),
    Codec(tickwire_account_codec::Error),
    Authority(AuthorityError),
    Encoding(EncodingError),
    Transport(tickwire_transport::Error),
    RateLimited(RateReject),
    NoReadyRoute,
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

/// Policy refusal from [`Authority`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityError {
    Denied,
    Unavailable,
}

/// Stateless synchronous send-boundary gate sampled immediately before writing bytes to a socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gate {
    pub now: u64,
    pub writer_epoch: u64,
    pub strategy_generation: u64,
    pub config_revision: u64,
    pub action_permitted: bool,
    pub market_valid: bool,
}

/// Stateless runtime authority hook evaluated at command admission and first byte output.
pub trait Authority {
    fn authorize(
        &mut self,
        _baseline: &CanonicalBaseline,
        _command: &CanonicalCommand,
        _admission: &Admission,
        _evaluated_at: u64,
    ) -> std::result::Result<(), AuthorityError> {
        Ok(())
    }

    /// Samples current control state and monotonic time synchronously before writing to the socket.
    fn current_gate(
        &mut self,
        command: &CanonicalCommand,
        admission: &Admission,
    ) -> std::result::Result<Gate, AuthorityError>;
}

/// Convenience stateless [`Authority`] backed by a gate closure.
pub struct FnAuthority<F>(pub F);

impl<F> Authority for FnAuthority<F>
where
    F: FnMut(&CanonicalCommand, &Admission) -> std::result::Result<Gate, AuthorityError>,
{
    fn current_gate(
        &mut self,
        command: &CanonicalCommand,
        admission: &Admission,
    ) -> std::result::Result<Gate, AuthorityError> {
        (self.0)(command, admission)
    }
}

/// Immutable configuration for a stateless [`AccountGateway`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub scope: Scope,
    pub baseline_digest: Hash,
    pub writer_epoch: u64,
    pub route: Route,
    pub max_intents: usize,
}

/// In-memory lifecycle status of an admitted command inside [`AccountGateway`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Queued,
    PartiallyAccepted,
    Flushed,
    Unknown,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputProgress {
    pub accepted_bytes: usize,
    pub total_bytes: usize,
}

/// Verified cold Exchange REST bootstrap pairing a `CanonicalBaseline` with its reconstructed
/// in-memory `Account` (zero local WAL or disk files).
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

pub(crate) struct Entry {
    pub(crate) command: CanonicalCommand,
    pub(crate) admission: Option<Admission>,
    pub(crate) evaluated_at: u64,
    pub(crate) increases: bool,
    pub(crate) status: Status,
    pub(crate) output: Option<OutputProgress>,
}

/// Stateless in-memory execution gateway coupling `Account`, `StpBook`, `OrderConnectionPool`,
/// per-order transport-doubt isolation, and `SharedRateLimiter` admission.
pub struct AccountGateway<A: Authority, E: Encoder> {
    baseline: CanonicalBaseline,
    account: Account,
    config: Config,
    authority: A,
    encoder: E,
    connection: Option<Connection>,
    websocket: Option<websocket::WebSocketState>,
    rest_started: bool,
    max_send_bytes: usize,
    max_receive_bytes: usize,
    retained_receive: VecDeque<u8>,
    entries: BTreeMap<IntentKey, Entry>,
    ingress_events_seen: BTreeMap<([u8; 16], u64), Hash>,
    ingress_inputs_seen: BTreeMap<([u8; 16], u64), Hash>,
    completed_mono_ns: BTreeMap<IntentKey, u64>,
    retention_window_ns: Option<u64>,
    last_mono_ns: u64,
    pending: Option<(IntentKey, u64)>,
    pool: Option<pool::GatewayPool>,
    halted: bool,
    per_order_doubt_isolation: bool,
}

impl<A: Authority, E: Encoder> AccountGateway<A, E> {
    /// Creates a connected single-socket stateless gateway from an initial `CanonicalBaseline`.
    pub fn create(
        config: Config,
        baseline: CanonicalBaseline,
        authority: A,
        encoder: E,
        connection: ConnectionConfig,
    ) -> Result<Self> {
        binding::config(&config)?;
        binding::baseline(&config, &baseline)?;
        let account =
            Account::from_snapshot(baseline.snapshot().clone(), baseline.catalog().clone())
                .map_err(Error::Account)?;
        Self::create_with_account(
            config,
            baseline,
            account,
            BTreeMap::new(),
            authority,
            encoder,
            Some(connection),
        )
    }

    /// Creates an unconnected stateless gateway (ready for `with_connection_pool` or `install_transport_connection`).
    pub fn create_unconnected(
        config: Config,
        baseline: CanonicalBaseline,
        authority: A,
        encoder: E,
    ) -> Result<Self> {
        binding::config(&config)?;
        binding::baseline(&config, &baseline)?;
        let account =
            Account::from_snapshot(baseline.snapshot().clone(), baseline.catalog().clone())
                .map_err(Error::Account)?;
        Self::create_with_account(
            config,
            baseline,
            account,
            BTreeMap::new(),
            authority,
            encoder,
            None,
        )
    }

    /// Creates a connected stateless gateway from an Exchange REST [`VerifiedLiveBootstrap`].
    pub fn create_from_live_exchange(
        config: Config,
        live: VerifiedLiveBootstrap,
        authority: A,
        encoder: E,
        connection: ConnectionConfig,
    ) -> Result<Self> {
        Self::create_from_live_exchange_inner(config, live, authority, encoder, Some(connection))
    }

    /// Creates an unconnected/pool-ready stateless gateway from an Exchange REST [`VerifiedLiveBootstrap`].
    pub fn create_from_live_exchange_unconnected(
        config: Config,
        live: VerifiedLiveBootstrap,
        authority: A,
        encoder: E,
    ) -> Result<Self> {
        Self::create_from_live_exchange_inner(config, live, authority, encoder, None)
    }

    fn create_from_live_exchange_inner(
        config: Config,
        live: VerifiedLiveBootstrap,
        authority: A,
        encoder: E,
        connection: Option<ConnectionConfig>,
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
                        admission: None,
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
        config: Config,
        baseline: CanonicalBaseline,
        mut account: Account,
        entries: BTreeMap<IntentKey, Entry>,
        authority: A,
        encoder: E,
        connection: Option<ConnectionConfig>,
    ) -> Result<Self> {
        binding::config(&config)?;
        binding::baseline(&config, &baseline)?;
        let (conn, max_send_bytes, max_receive_bytes) = if let Some(c) = connection {
            pool::validate_transport_limits(c.limits)?;
            let server_name = config
                .route
                .authority
                .clone()
                .try_into()
                .map_err(|_| Error::Identity)?;
            let max_s = c.limits.max_send_bytes;
            let max_r = c.limits.max_receive_bytes;
            let dialed = Connection::connect(
                c.address,
                Security::Tls {
                    server_name,
                    config: c.tls,
                },
                c.limits,
                c.connect_deadline,
            )?;
            (Some(dialed), max_s, max_r)
        } else {
            let defaults = Limits::default();
            (None, defaults.max_send_bytes, defaults.max_receive_bytes)
        };

        let retention_window_ns = Some(tickwire_account::DEFAULT_RETENTION_WINDOW_NS);
        account.set_retention_window_ns(retention_window_ns);
        let mut gateway = Self {
            baseline,
            account,
            config,
            authority,
            encoder,
            connection: conn,
            websocket: None,
            rest_started: false,
            max_send_bytes,
            max_receive_bytes,
            retained_receive: VecDeque::new(),
            entries,
            ingress_events_seen: BTreeMap::new(),
            ingress_inputs_seen: BTreeMap::new(),
            completed_mono_ns: BTreeMap::new(),
            retention_window_ns,
            last_mono_ns: 0,
            pending: None,
            pool: None,
            halted: false,
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
    }

    pub fn retention_window_ns(&self) -> Option<u64> {
        self.retention_window_ns
    }

    #[inline]
    pub(crate) fn is_key_pending(&self, key: IntentKey) -> bool {
        self.pending.is_some_and(|(k, _)| k == key)
            || self.pool.as_ref().is_some_and(|p| p.is_key_pending(key))
    }

    pub(crate) fn refresh_completed_entries(&mut self) {
        let mut remove_keys = Vec::new();
        let mut insert_keys = Vec::new();
        for (key, entry) in &self.entries {
            let completed = matches!(entry.status, Status::Aborted | Status::Flushed)
                && !self.is_key_pending(*key)
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
        let candidate = self
            .entries
            .iter()
            .filter(|(key, entry)| {
                is_management_command(&entry.command)
                    && matches!(entry.status, Status::Aborted | Status::Flushed)
                    && !self.is_key_pending(**key)
                    && !self.account.is_intent_active(**key)
                    && !self.account.is_confirmed_amendment(**key)
                    && !self.account.is_confirmed_modification(**key)
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
            if let Some(gpool) = self.pool.as_mut() {
                gpool.slot_bindings.remove(&key);
            }
            true
        } else {
            false
        }
    }

    /// Evicts completed (`Aborted` or `Flushed`) gateway entries whose underlying `Account`
    /// intent is retired and whose monotonic retention window has elapsed.
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
        let to_evict: Vec<IntentKey> = self
            .entries
            .iter()
            .filter_map(|(key, entry)| {
                if !matches!(entry.status, Status::Aborted | Status::Flushed) {
                    return None;
                }
                if self.is_key_pending(*key)
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
            if let Some(gpool) = self.pool.as_mut() {
                gpool.slot_bindings.remove(&key);
            }
        }
        count
    }

    /// Submits a command on the single-connection transport path.
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
        let (send, initial_status) =
            self.admit_in_memory(key, command, evaluated_at, deadline, self.max_send_bytes)?;
        match self.connection.as_mut().ok_or(Error::Halted)?.queue(send) {
            Ok(token) => {
                self.pending = Some((key, token));
                Ok(initial_status)
            }
            Err(error) => {
                self.abort(key)?;
                self.on_transport_error();
                Err(error.into())
            }
        }
    }

    pub(crate) fn admit_in_memory(
        &mut self,
        key: IntentKey,
        command: CanonicalCommand,
        evaluated_at: u64,
        deadline: Instant,
        max_send_bytes: usize,
    ) -> Result<(PreparedSend, Status)> {
        binding::command(&self.config, &command, true)?;
        if evaluated_at > 0 {
            self.evict_retired_entries(evaluated_at);
        }
        if is_management_command(&command) {
            let mgmt_count = self
                .entries
                .iter()
                .filter(|(k, e)| {
                    is_management_command(&e.command)
                        && !self.account.is_confirmed_amendment(**k)
                        && !self.account.is_confirmed_modification(**k)
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
        let send = PreparedSend::new(wire.bytes(), deadline, max_send_bytes)?;
        let admission = self
            .account
            .admit(&command, evaluated_at)
            .map_err(Error::Account)?;
        if let Err(auth_err) =
            self.authority
                .authorize(&self.baseline, &command, &admission, evaluated_at)
        {
            let _ = self.account.apply_abort(&admission);
            return Err(Error::Authority(auth_err));
        }
        self.account.set_management_retired(key, None);
        let increases = admission.image().increases;
        let initial_status = Status::Queued;
        self.entries.insert(
            key,
            Entry {
                command,
                admission: Some(admission),
                evaluated_at,
                increases,
                status: initial_status,
                output: Some(OutputProgress {
                    accepted_bytes: 0,
                    total_bytes: wire.bytes().len(),
                }),
            },
        );
        Ok((send, initial_status))
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
            let admission = entry.admission.as_ref().ok_or(Error::Invalid)?;
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

    pub fn terminate_unsent_strategy(
        &mut self,
        strategy: u64,
        manager: tickwire_account::ManagerIdentity,
    ) -> Result<bool> {
        let mut terminated = false;
        if let Some((key, _)) = self.pending {
            let entry = self.entries.get(&key).ok_or(Error::Invalid)?;
            let context = &entry.command.command().context;
            if context.strategy.logical_strategy_id == strategy
                && context.strategy.instance_id == manager.instance
                && context.request.strategy_generation == manager.generation
                && entry.output.is_some_and(|p| p.accepted_bytes == 0)
            {
                self.cancel_pending()?;
                terminated = true;
            }
        }
        if let Some(gpool) = self.pool.as_ref() {
            let mut matching_slots = Vec::new();
            for (idx, io) in gpool.slot_io.iter().enumerate() {
                let Some((key, _, _)) = io.pending_send else {
                    continue;
                };
                let entry = self.entries.get(&key).ok_or(Error::Invalid)?;
                let context = &entry.command.command().context;
                if context.strategy.logical_strategy_id == strategy
                    && context.strategy.instance_id == manager.instance
                    && context.request.strategy_generation == manager.generation
                    && entry.output.is_some_and(|p| p.accepted_bytes == 0)
                {
                    matching_slots.push(idx as u16);
                }
            }
            for conn_id in matching_slots {
                if self.halted {
                    break;
                }
                self.cancel_slot_pending(conn_id)?;
                terminated = true;
            }
        }
        Ok(terminated)
    }

    /// Applies an authoritative venue execution event directly to the in-memory `Account`.
    pub fn record_event(&mut self, event: CanonicalEvent) -> Result<Applied> {
        let event_now = event.event().identity.received_mono_ns;
        if event_now > 0 {
            self.last_mono_ns = self.last_mono_ns.max(event_now);
        }
        let applied = match self.account.apply_event(&event) {
            Ok(applied) => applied,
            Err(error) => {
                self.account.freeze();
                return Err(Error::Account(error));
            }
        };
        self.refresh_completed_entries();
        Ok(applied)
    }

    /// Applies an ingress event deduplicated by `(input_stream, input_sequence)`.
    pub fn record_ingress_event(
        &mut self,
        input_stream: [u8; 16],
        input_sequence: u64,
        event: CanonicalEvent,
    ) -> Result<Applied> {
        let digest = event.digest();
        if let Some(&prior_digest) = self
            .ingress_events_seen
            .get(&(input_stream, input_sequence))
        {
            if prior_digest == digest {
                return Ok(Applied::Duplicate);
            }
            self.halt();
            return Err(Error::Conflict);
        }
        let applied = self.record_event(event)?;
        self.ingress_events_seen
            .insert((input_stream, input_sequence), digest);
        Ok(applied)
    }

    /// Applies a balance/position stream update deduplicated by `(input_stream, input_sequence)`.
    pub fn record_account_input(
        &mut self,
        input_stream: [u8; 16],
        input_sequence: u64,
        input: CanonicalAccountInput,
    ) -> Result<Applied> {
        let digest = input.digest();
        if let Some(&prior_digest) = self
            .ingress_inputs_seen
            .get(&(input_stream, input_sequence))
        {
            if prior_digest == digest {
                return Ok(Applied::Duplicate);
            }
            self.halt();
            return Err(Error::Conflict);
        }
        let applied = match self.account.apply_account_input(&input) {
            Ok(v) => v,
            Err(e) => {
                self.account.freeze();
                return Err(Error::Account(e));
            }
        };
        self.ingress_inputs_seen
            .insert((input_stream, input_sequence), digest);
        Ok(applied)
    }

    /// Applies an external reconciliation fact (`FeesReconciled`, `UnattributedCashFlow`, etc.)
    /// directly to the in-memory `Account`, rejecting forged `Admit` / `Aborted` records.
    pub fn record_fact(&mut self, record: RecoveryRecord) -> Result<Applied> {
        let applied = match record {
            RecoveryRecord::Admit { .. }
            | RecoveryRecord::Aborted { .. }
            | RecoveryRecord::IngressEvent { .. }
            | RecoveryRecord::IngressAccountInput { .. } => return Err(Error::Invalid),
            RecoveryRecord::VenueEvent(event) => return self.record_event(*event),
            RecoveryRecord::FeesReconciled { order, cumulative } => self
                .account
                .reconcile_fees(&order, cumulative)
                .map_err(Error::Account)?,
            RecoveryRecord::UnattributedCashFlow { id, delta } => self
                .account
                .apply_unattributed_cashflow(&id, delta)
                .map_err(Error::Account)?,
            RecoveryRecord::ExternalPosition {
                id,
                instrument,
                side,
                quantity,
                entry_bound,
            } => self
                .account
                .apply_external_position(&id, instrument, side, quantity, entry_bound)
                .map_err(Error::Account)?,
            RecoveryRecord::TransferAsset {
                key,
                from,
                to,
                amount,
            } => {
                self.account
                    .transfer_asset_attribution(key, from, to, amount)
                    .map_err(Error::Account)?;
                Applied::New
            }
            RecoveryRecord::TransferPosition {
                instrument,
                side,
                from,
                to,
                quantity,
            } => {
                self.account
                    .transfer_position_attribution(instrument, side, from, to, quantity)
                    .map_err(Error::Account)?;
                Applied::New
            }
            RecoveryRecord::UncreatedAlgoClosed(b) => self
                .account
                .close_uncreated_algo(&b)
                .map_err(Error::Account)?,
            RecoveryRecord::IncomeObservationsAssociated(b) => self
                .account
                .associate_income_observations(&b)
                .map_err(Error::Account)?,
            RecoveryRecord::AccountInputsReconciled(b) => self
                .account
                .reconcile_account_inputs(&b)
                .map_err(Error::Account)?,
            RecoveryRecord::IncomeBaseline(b) => self
                .account
                .apply_income_baseline(&b)
                .map_err(Error::Account)?,
            RecoveryRecord::StrategyHandover(h) => {
                self.account
                    .apply_strategy_handover(&h)
                    .map_err(Error::Account)?;
                Applied::New
            }
            RecoveryRecord::Reactivated(r) => {
                self.account
                    .apply_reactivation(&r)
                    .map_err(Error::Account)?;
                Applied::New
            }
        };
        self.refresh_completed_entries();
        Ok(applied)
    }

    pub fn command(&self, key: IntentKey) -> Option<&CanonicalCommand> {
        self.entries.get(&key).map(|e| &e.command)
    }

    pub fn admission(&self, key: IntentKey) -> Option<&Admission> {
        self.entries.get(&key).and_then(|e| e.admission.as_ref())
    }

    pub fn admission_origin(&self, key: IntentKey) -> Option<u64> {
        self.entries.get(&key).map(|e| e.evaluated_at)
    }

    pub fn baseline(&self) -> &CanonicalBaseline {
        &self.baseline
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn freeze_account(&mut self) {
        self.account.freeze();
    }

    pub fn writer_epoch(&self) -> u64 {
        self.config.writer_epoch
    }

    pub fn status(&self, intent: IntentKey) -> Option<Status> {
        self.entries.get(&intent).map(|e| e.status)
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub fn account(&self) -> &Account {
        &self.account
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
                self.entries.get_mut(&key).unwrap().status = Status::PartiallyAccepted;
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

    pub(crate) fn abort(&mut self, key: IntentKey) -> Result<()> {
        let admission = self
            .entries
            .get(&key)
            .and_then(|e| e.admission.as_ref())
            .ok_or(Error::Invalid)?
            .clone();
        if let Err(error) = self.account.apply_abort(&admission) {
            self.halt();
            return Err(Error::Account(error));
        }
        self.entries.get_mut(&key).unwrap().status = Status::Aborted;
        let stamp = self.last_mono_ns.max(self.entries[&key].evaluated_at);
        self.account.set_management_retired(key, Some(stamp));
        self.refresh_completed_entries();
        Ok(())
    }

    pub(crate) fn halt(&mut self) {
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
        if let Some(gpool) = self.pool.as_mut() {
            gpool.halt_all(self.last_mono_ns);
        }
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

    pub fn with_stp_enforcement(mut self, enabled: bool) -> Self {
        self.account.set_stp_enforcement(enabled);
        self
    }

    pub fn set_stp_enforcement(&mut self, enabled: bool) {
        self.account.set_stp_enforcement(enabled);
    }

    pub fn stp_enforcement(&self) -> bool {
        self.account.stp_enforcement()
    }

    pub fn stp_book(&self) -> &StpBook {
        self.account.stp_book()
    }

    pub fn with_projected_fill(mut self, enabled: bool) -> Self {
        self.account.set_projected_fill(enabled);
        self
    }

    pub fn set_projected_fill(&mut self, enabled: bool) {
        self.account.set_projected_fill(enabled);
    }

    pub fn projected_fill_enabled(&self) -> bool {
        self.account.projected_fill_enabled()
    }

    pub fn on_cum_qty(
        &mut self,
        key: &tickwire_account::OrderKey,
        cum_qty: tickwire_account::Fixed,
        price_input: ProjectedPriceInput,
    ) -> std::result::Result<Option<ProjectedFill>, tickwire_account::Error> {
        self.account.on_cum_qty(key, cum_qty, price_input)
    }

    pub fn drain_projected_fills(&mut self) -> Vec<ProjectedFill> {
        self.account.drain_projected_fills()
    }

    pub fn pop_projected_fill(&mut self) -> Option<ProjectedFill> {
        self.account.pop_projected_fill()
    }

    pub fn order_projected_filled(
        &self,
        key: &tickwire_account::OrderKey,
    ) -> Option<tickwire_account::Fixed> {
        self.account.order_projected_filled(key)
    }

    pub fn order_terminal_reason(
        &self,
        key: &tickwire_account::OrderKey,
    ) -> Option<tickwire_venue::TerminalReason> {
        self.account.order_terminal_reason(key)
    }

    pub fn order_last_modify_rejection(
        &self,
        key: &tickwire_account::OrderKey,
    ) -> Option<tickwire_venue::TerminalReason> {
        self.account.order_last_modify_rejection(key)
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
                .admission
                .as_ref()
                .map(|a| a.orders().to_vec())
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
                | tickwire_venue::CommandBody::AmendKeepPriority { target, .. }
                | tickwire_venue::CommandBody::Modify(tickwire_venue::ModifyOrder {
                    target, ..
                }) => {
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
        if let Some(gpool) = self.pool.as_mut() {
            gpool.clear_pending_for_key(key);
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
                matches!(e.status, Status::Queued)
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
                    Some(
                        tickwire_venue::CommandBody::AmendKeepPriority { .. }
                        | tickwire_venue::CommandBody::Modify(_),
                    ) => tickwire_account::DoubtReason::AmendAmbiguous,
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
        limits: Limits,
    ) -> Result<()> {
        if self.halted || self.account.frozen() {
            return Err(Error::Halted);
        }
        if self.pool.is_some()
            || self.connection.is_some()
            || self.pending.is_some()
            || !self.retained_receive.is_empty()
        {
            return Err(Error::Busy);
        }
        pool::validate_transport_limits(limits)?;
        self.connection = Some(connection);
        self.websocket = None;
        self.rest_started = false;
        self.max_send_bytes = limits.max_send_bytes;
        self.max_receive_bytes = limits.max_receive_bytes;
        Ok(())
    }
}

pub(crate) fn is_management_command(command: &CanonicalCommand) -> bool {
    matches!(
        command.command().body,
        tickwire_venue::CommandBody::Cancel(_)
            | tickwire_venue::CommandBody::Query(_)
            | tickwire_venue::CommandBody::CancelList(_)
            | tickwire_venue::CommandBody::CancelAlgo(_)
            | tickwire_venue::CommandBody::AmendKeepPriority { .. }
            | tickwire_venue::CommandBody::Modify(_)
    )
}
