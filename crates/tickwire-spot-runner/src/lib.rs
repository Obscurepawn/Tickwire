//! Shared intake for hosted or file-backed strategies and a single qualified cash account.
#![forbid(unsafe_code)]
pub mod live_bootstrap;
mod origins;
pub mod query_desk_driver;
pub mod reconcile;
use origins::Origins;
pub use query_desk_driver::{PerOrderQueryDriver, QueryDriverError, QueryStepOutcome};
use std::collections::BTreeMap;
use tickwire_account::{
    Applied, AskKind, IntentKey, OrderKey, QueryConfig, QueryDesk, RecoveryRecord, Scope,
};
use tickwire_account_gateway::{Authority, Status};
use tickwire_account_journal::Backend;
use tickwire_binance_clock::LocalTime;
use tickwire_clock::MonoTime;
use tickwire_recovery::BlobStore;
use tickwire_runtime::{
    authority::RecoveryQueue,
    node::{Health, Strategy, Submission},
};
use tickwire_spot_execution::{
    FillSource, Reply, RestSession, SessionError, SpotIssueKind, SpotVenue, classify_issue,
    is_venue_rate_limit_code,
};
use tickwire_transport::IoBudget;
use tickwire_venue::{CanonicalCommand, CommandBody, VenueOrderId};
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
pub type Result<T> = std::result::Result<T, Error>;
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
    inflight: BTreeMap<IntentKey, u64>,
    last: Option<MonoTime>,
    query_desk: Option<QueryDesk>,
    acked_native_ids: BTreeMap<OrderKey, VenueOrderId>,
    expected_fill_atoms: BTreeMap<OrderKey, i128>,
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
            origins: Origins::new(store),
            inflight: BTreeMap::new(),
            last: None,
            query_desk: None,
            acked_native_ids: BTreeMap::new(),
            expected_fill_atoms: BTreeMap::new(),
        }
    }
    /// Constructs an `AccountRunner` pre-populated with cold-bootstrapped open order origins.
    pub fn new_with_imported_origins(
        session: RestSession<'a, B, A, T>,
        queue: RecoveryQueue,
        store: S,
        imported: &[live_bootstrap::ImportedSpotOrigin],
    ) -> Result<Self> {
        let mut runner = Self::new(session, queue, store);
        let scope = runner.scope().clone();
        for item in imported {
            runner.origins.register(
                &scope,
                item.intent,
                &item.client_id,
                item.order_id,
                item.command.digest(),
                0,
            )?;
        }
        Ok(runner)
    }
    pub fn with_per_order_query_desk(mut self, config: QueryConfig) -> Self {
        self.enable_per_order_query_desk(config);
        self
    }
    pub fn enable_per_order_query_desk(&mut self, config: QueryConfig) {
        self.session.set_per_order_doubt_isolation(true);
        self.query_desk = Some(QueryDesk::new(config));
    }
    pub fn query_desk(&self) -> Option<&QueryDesk> {
        self.query_desk.as_ref()
    }
    pub fn query_desk_mut(&mut self) -> Option<&mut QueryDesk> {
        self.query_desk.as_mut()
    }
    pub fn session(&self) -> &RestSession<'a, B, A, T> {
        &self.session
    }
    pub fn session_mut(&mut self) -> &mut RestSession<'a, B, A, T> {
        &mut self.session
    }
    pub fn poll_pool_dials(&mut self, now_mono_ns: u64) -> usize {
        self.session.poll_pool_dials(now_mono_ns)
    }
    fn active_inflight_len(&self) -> usize {
        self.inflight
            .keys()
            .filter(|&&k| self.session.gateway().status(k) != Some(Status::Aborted))
            .count()
    }
    fn reap_aborted_inflight(&mut self, now_mono_ns: u64) {
        let aborted_keys: Vec<IntentKey> = self
            .inflight
            .keys()
            .copied()
            .filter(|k| self.session.gateway().status(*k) == Some(Status::Aborted))
            .collect();
        for key in aborted_keys {
            self.inflight.remove(&key);
            self.origins.retire_intent(key, now_mono_ns);
        }
    }
    pub fn inflight_count(&self) -> usize {
        self.active_inflight_len()
    }
    pub fn has_transport_connection(&self) -> bool {
        self.session.has_transport_connection()
    }
    pub fn install_transport_connection(
        &mut self,
        connection: tickwire_transport::Connection,
        limits: tickwire_transport::Limits,
    ) -> Result<()> {
        self.session
            .install_transport_connection(connection, limits)
            .map_err(Error::Session)
    }
    pub fn with_retention_window_ns(mut self, window_ns: Option<u64>) -> Self {
        self.set_retention_window_ns(window_ns);
        self
    }
    pub fn set_retention_window_ns(&mut self, window_ns: Option<u64>) {
        self.origins.set_retention_window_ns(window_ns);
        self.session
            .gateway_mut()
            .set_retention_window_ns(window_ns);
    }
    pub fn retention_window_ns(&self) -> Option<u64> {
        self.origins.retention_window_ns()
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
        let active = self.active_inflight_len();
        !self.session.gateway().halted()
            && self.session.has_ready_slot()
            && (self.session.gateway().has_connection_pool() || active == 0)
            && active < tickwire_account_gateway::pool::MAX_ORDER_CONNECTIONS
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
        self.reap_aborted_inflight(now.monotonic.nanos());
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
        // The originating member received the refusal above. Continue with its peer.
        match result {
            Err(Error::Session(SessionError::Gateway(
                tickwire_account_gateway::Error::Account(tickwire_account::Error::ManagementDenied),
            ))) => Ok(true),
            Err(Error::Session(SessionError::Gateway(
                tickwire_account_gateway::Error::Account(
                    tickwire_account::Error::InsufficientAsset
                    | tickwire_account::Error::InsufficientCapital
                    | tickwire_account::Error::Limit,
                ),
            ))) if !self.account().frozen() => Ok(true),
            Err(Error::Session(SessionError::RateLimited(_))) if !self.account().frozen() => {
                Ok(true)
            }
            other => other.map(|_| true),
        }
    }
    pub fn submit(&mut self, sub: Submission, now: LocalTime) -> Result<Status> {
        self.clock(now)?;
        self.reap_aborted_inflight(now.monotonic.nanos());
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
        let prior = self.session.gateway().command(intent).is_some();
        if !prior {
            if !self.session.gateway().has_connection_pool() {
                if !self.inflight.is_empty() {
                    return Err(Error::Busy);
                }
            } else if self.inflight.len() >= tickwire_account_gateway::pool::MAX_ORDER_CONNECTIONS
                || !self.session.has_ready_slot()
            {
                return Err(Error::Busy);
            }
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
        if matches!(&c.body, CommandBody::New(_)) && self.origins.is_at_capacity() {
            self.origins
                .sync_account(self.session.gateway().account(), now.monotonic.nanos());
        }
        match &c.body {
            CommandBody::New(order) => {
                let [id] = order_ids.as_slice() else {
                    self.session.stop_sending();
                    return Err(Error::Origin(
                        "one nonzero local order ID is required for a new cash order",
                    ));
                };
                if *id == [0; 16] {
                    self.session.stop_sending();
                    return Err(Error::Origin(
                        "one nonzero local order ID is required for a new cash order",
                    ));
                }
                if !prior {
                    self.session
                        .precheck_submit(&command, now.monotonic.nanos())
                        .map_err(Error::Session)?;
                }
                let same_prior_cmd =
                    prior && self.session.gateway().command(intent) == Some(&command);
                if !same_prior_cmd
                    && let Err(e) = self.origins.register(
                        &scope,
                        intent,
                        order.client_id.as_str(),
                        *id,
                        command.digest(),
                        now.monotonic.nanos(),
                    )
                {
                    self.session.stop_sending();
                    return Err(e);
                }
            }
            CommandBody::Cancel(target) | CommandBody::Query(target) if order_ids.is_empty() => {
                let verify_origin = self
                    .origins
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
                    });
                if let Err(e) = verify_origin {
                    self.session.stop_sending();
                    return Err(e);
                }
                if !prior {
                    self.session
                        .precheck_submit(&command, now.monotonic.nanos())
                        .map_err(Error::Session)?;
                }
            }
            _ => {
                self.session.stop_sending();
                return Err(Error::Origin(
                    "unsupported cash order or local order ID count",
                ));
            }
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
        let status = match status {
            Ok(s) => s,
            Err(e) => {
                if self.session.gateway().command(intent).is_none() {
                    self.origins.retire_intent(intent, now.monotonic.nanos());
                }
                return Err(Error::Session(e));
            }
        };
        if status == Status::Aborted {
            self.origins.retire_intent(intent, now.monotonic.nanos());
        } else {
            self.inflight.insert(intent, strategy);
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
        self.reap_aborted_inflight(now.monotonic.nanos());
        let tagged = self
            .session
            .poll_tagged(now.instant, now.monotonic.nanos(), budget)
            .map_err(Error::Session)?;
        self.reap_aborted_inflight(now.monotonic.nanos());
        if let Some((intent, response)) = tagged {
            let logical_strategy_id = self
                .inflight
                .remove(&intent)
                .ok_or(Error::Origin("unsolicited response"))?;
            let command = self
                .session
                .gateway()
                .command(intent)
                .cloned()
                .ok_or(Error::Origin("missing admitted request"))?;
            self.on_completed_command(&command, intent, &response, now.monotonic.nanos());
            strategy.on_request_reply(&command, &response);
            return Ok(Some(Completion {
                logical_strategy_id,
                intent,
                response,
            }));
        }
        Ok(None)
    }
    /// Polls transport across all slots and drains every completed reply ready in this turn.
    pub fn poll_all(
        &mut self,
        strategy: &mut impl Strategy,
        now: LocalTime,
        budget: IoBudget,
    ) -> Result<Vec<Completion>> {
        let mut out = Vec::new();
        if let Some(first) = self.poll(strategy, now, budget)? {
            out.push(first);
            while self.session.has_queued_replies() {
                if let Some(next) = self.poll(strategy, now, budget)? {
                    out.push(next);
                } else {
                    break;
                }
            }
        }
        Ok(out)
    }
    fn on_completed_command(
        &mut self,
        command: &CanonicalCommand,
        intent: IntentKey,
        response: &Reply,
        now_mono_ns: u64,
    ) {
        let c = command.command();
        let instrument_id = c.context.instrument.instrument_id;
        match &c.body {
            CommandBody::New(order) => {
                let order_key = OrderKey {
                    intent,
                    client_id: order.client_id.clone(),
                };
                if let Reply::Acknowledged { venue_order_id, .. } = response {
                    if self.acked_native_ids.len() >= tickwire_account::MAX_ORDERS {
                        let account = self.session.gateway().account();
                        self.acked_native_ids.retain(|k, _| {
                            account
                                .order(k)
                                .is_some_and(|v| !v.aborted && !v.uncreated && !v.terminal)
                        });
                    }
                    self.acked_native_ids
                        .insert(order_key.clone(), venue_order_id.clone());
                }
                if self.query_desk.is_some() {
                    let uncreated = self
                        .account()
                        .order(&order_key)
                        .is_some_and(|v| v.uncreated);
                    if uncreated {
                        self.acked_native_ids.remove(&order_key);
                        self.expected_fill_atoms.remove(&order_key);
                        if !self.account().is_intent_active(intent) {
                            self.origins.retire_intent(intent, now_mono_ns);
                        }
                    } else if !self.account().frozen()
                        && self.account().order_in_doubt(&order_key).is_some()
                    {
                        let native = self.known_native_order_id(&order_key);
                        if !self.enqueue_query_desk_ask(
                            order_key,
                            instrument_id,
                            native,
                            AskKind::Place,
                            true,
                            now_mono_ns,
                        ) {
                            self.freeze_cash();
                        }
                    }
                }
            }
            CommandBody::Cancel(target)
                if self.query_desk.is_some() && !self.account().frozen() =>
            {
                let is_ambiguous = match response {
                    Reply::Unknown => true,
                    Reply::RequestIssue { code } => SpotVenue::from_id(c.context.request.venue)
                        .is_some_and(|v| {
                            classify_issue(v, code) == SpotIssueKind::Ambiguous
                                || is_venue_rate_limit_code(v, code)
                        }),
                    _ => false,
                };
                if is_ambiguous
                    && let Some(order_key) = self
                        .account()
                        .resolve_order_target(instrument_id, target)
                        .or_else(|| {
                            let scope = self.scope().clone();
                            let origin = self
                                .origins
                                .lookup(&scope, b"order", &target.order_id)
                                .ok()?;
                            let client_id = target.client_id.clone()?;
                            Some(OrderKey {
                                intent: origin.intent,
                                client_id,
                            })
                        })
                    && self
                        .account()
                        .order(&order_key)
                        .is_some_and(|v| !v.terminal && !v.uncreated && !v.aborted)
                {
                    let place_unacked = !self.is_order_confirmed_or_acked(&order_key);
                    let native = self.known_native_order_id(&order_key);
                    if !self.enqueue_query_desk_ask(
                        order_key,
                        instrument_id,
                        native,
                        AskKind::Cancel,
                        place_unacked,
                        now_mono_ns,
                    ) {
                        self.freeze_cash();
                    }
                }
            }
            _ => {}
        }
    }
    pub(crate) fn is_order_confirmed_or_acked(&self, key: &OrderKey) -> bool {
        self.acked_native_ids.contains_key(key)
            || self.account().venue_order_id(key).is_some()
            || self
                .account()
                .order(key)
                .is_some_and(|v| v.confirmed || v.filled_quantity.atoms() > 0 || v.terminal)
    }
    pub(crate) fn known_native_order_id(&self, key: &OrderKey) -> Option<VenueOrderId> {
        self.account()
            .venue_order_id(key)
            .cloned()
            .or_else(|| self.acked_native_ids.get(key).cloned())
    }
    pub(crate) fn record_acked_native_order_id(&mut self, key: OrderKey, native: VenueOrderId) {
        self.acked_native_ids.insert(key, native);
    }
    pub(crate) fn note_expected_fill_atoms(&mut self, key: &OrderKey, atoms: i128) {
        self.expected_fill_atoms
            .entry(key.clone())
            .and_modify(|cur| *cur = (*cur).max(atoms))
            .or_insert(atoms);
    }
    pub(crate) fn expected_fill_atoms(&self, key: &OrderKey) -> i128 {
        self.expected_fill_atoms.get(key).copied().unwrap_or(0)
    }
    pub(crate) fn clear_expected_fill_atoms(&mut self, key: &OrderKey) {
        self.expected_fill_atoms.remove(key);
    }
    pub(crate) fn poll_due_query_ticket(
        &mut self,
        now_mono_ns: u64,
    ) -> Option<tickwire_account::QueryTicket> {
        let account = self.session.gateway().account();
        let expected_fills = &self.expected_fill_atoms;
        let desk = self.query_desk.as_mut()?;
        desk.poll_due(now_mono_ns, |order, kind| {
            account.order(order).is_some_and(|v| {
                if v.aborted || v.uncreated {
                    return false;
                }
                if kind == AskKind::Trades {
                    let expected = expected_fills.get(order).copied().unwrap_or(0);
                    account.order_in_doubt(order)
                        == Some(tickwire_account::DoubtReason::FillCatchup)
                        || expected > v.filled_quantity.atoms()
                        || (v.terminal && !v.fee_reconciled)
                } else {
                    account.order_in_doubt(order).is_some()
                        || (!v.terminal && kind == AskKind::Cancel)
                }
            })
        })
    }
    pub(crate) fn enqueue_query_desk_ask(
        &mut self,
        order_key: OrderKey,
        instrument_id: u64,
        native: Option<VenueOrderId>,
        kind: AskKind,
        place_unacked: bool,
        now_mono_ns: u64,
    ) -> bool {
        let Some(desk) = self.query_desk.as_mut() else {
            return false;
        };
        let account = self.session.gateway().account();
        desk.enqueue_ask(
            order_key,
            instrument_id,
            native,
            kind,
            place_unacked,
            now_mono_ns,
            |k| {
                account.order(k).is_some_and(|v| {
                    !v.aborted
                        && !v.uncreated
                        && (!v.terminal
                            || !v.fee_reconciled
                            || account.order_in_doubt(k).is_some()
                            || v.remaining_quantity.atoms() > 0)
                })
            },
        )
        .is_ok()
    }
    pub(crate) fn enqueue_query_desk_trades(&mut self, key: &OrderKey, now_mono_ns: u64) -> bool {
        let Some((instrument, _)) = self.account().order_terms(key) else {
            return false;
        };
        let instrument_id = instrument.instrument_id;
        let native = self.known_native_order_id(key);
        self.enqueue_query_desk_ask(
            key.clone(),
            instrument_id,
            native,
            AskKind::Trades,
            false,
            now_mono_ns,
        )
    }
    pub(crate) fn close_uncreated_order(
        &mut self,
        key: &OrderKey,
        code: &str,
        now_mono_ns: u64,
    ) -> Result<()> {
        let cmd = self
            .session
            .gateway()
            .command(key.intent)
            .cloned()
            .ok_or(Error::Origin(
                "missing admitted command for uncreated order",
            ))?;
        self.session
            .record_rejected_new(&cmd, code, now_mono_ns)
            .map_err(Error::Session)?;
        self.acked_native_ids.remove(key);
        self.expected_fill_atoms.remove(key);
        if let Some(desk) = self.query_desk.as_mut() {
            desk.remove_order(key);
        }
        if !self.account().is_intent_active(key.intent) {
            self.origins.retire_intent(key.intent, now_mono_ns);
        }
        Ok(())
    }
    pub(crate) fn close_terminal_fees_if_ready(
        &mut self,
        key: &OrderKey,
        now_mono_ns: u64,
    ) -> Result<bool> {
        let Some(view) = self.account().order(key) else {
            return Ok(false);
        };
        let mut closed = view.fee_reconciled;
        if !closed
            && self.account().check_fee_closure(key, view.filled_quantity) == Ok(Applied::New)
        {
            self.session
                .gateway_mut()
                .record_fact(RecoveryRecord::FeesReconciled {
                    order: key.clone(),
                    cumulative: view.filled_quantity,
                })
                .map_err(|e| Error::Session(SessionError::Gateway(e)))?;
            closed = true;
        }
        if closed
            && self.account().order(key).is_some_and(|v| {
                v.terminal
                    && self.account().order_in_doubt(key).is_none()
                    && v.remaining_quantity.atoms() == 0
            })
        {
            self.acked_native_ids.remove(key);
            self.expected_fill_atoms.remove(key);
            if let Some(desk) = self.query_desk.as_mut() {
                desk.remove_order(key);
            }
            if !self.account().is_intent_active(key.intent) {
                let stamp = self
                    .account()
                    .intent_retired_mono_ns(key.intent)
                    .unwrap_or(now_mono_ns);
                self.origins.retire_intent(key.intent, stamp);
            }
        }
        Ok(closed)
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
