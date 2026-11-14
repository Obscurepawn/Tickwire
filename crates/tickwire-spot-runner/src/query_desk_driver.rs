//! Off-hot-path driver connecting [`AccountRunner`]'s per-order [`QueryDesk`] to
//! [`AsyncRestQuerier`] (and optional background [`LiveNetwork`] execution reconnects).

use crate::{
    AccountRunner,
    reconcile::{PrivateCut, compare},
};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tickwire_account::{
    AskKind, DoubtReason, MAX_REST_SLOTS, OrderKey, QueryResolution, QueryTicket, Scope,
    SnapshotSummary,
};
use tickwire_account_gateway::Authority;
use tickwire_account_journal::Backend;
use tickwire_recovery::{BlobStore, digest};
use tickwire_spot_execution::{
    OrderRowExtras, SessionError, SpotIssueKind, SpotVenue, build_order_status_get,
    build_order_trades_get, classify_issue, classify_order_row_terminal_reason,
    extract_order_row_extras, is_http_account_hazard,
    network::{LiveNetwork, OwnedCredentials},
    recovery::{RemoteFill, RemoteOrder, decode_fill_row, decode_order_row},
};
use tickwire_transport::{
    ClockFn, Limits,
    rest_querier::{
        AsyncRestQuerier, BuiltRequest, RestQuerierConfig, RestReply, RestStatus, RestTask,
    },
};
use tickwire_venue::{Environment, Quantity, TerminalReason, TimeInForce, VenueOrderId};

#[derive(Debug)]
pub enum QueryDriverError {
    Transport(tickwire_transport::Error),
    Runner(crate::Error),
    Protocol(&'static str),
}

impl std::fmt::Display for QueryDriverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for QueryDriverError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(e) => Some(e),
            Self::Runner(e) => Some(e),
            Self::Protocol(_) => None,
        }
    }
}

#[derive(Debug, Default)]
pub struct QueryStepOutcome {
    pub resolutions: Vec<QueryResolution>,
    pub dispatched_tasks: usize,
    pub processed_replies: usize,
    pub reconnected_execution: bool,
    pub account_hazard_frozen: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryWireStep {
    OrderStatusPrimary,
    BybitOrderStatusHistory,
    OrderTrades,
}

#[derive(Clone, Debug)]
struct SlotRequestSpec {
    wire_key: u64,
    step: QueryWireStep,
    symbol: String,
    client_id: String,
    native_order_id: Option<String>,
}

#[derive(Clone, Debug)]
struct InFlightTicket {
    wire_key: u64,
    ticket: QueryTicket,
    step: QueryWireStep,
}

type SharedSlotTable = Arc<Mutex<[Option<SlotRequestSpec>; MAX_REST_SLOTS]>>;

/// Drives per-order `OrderStatus` and `OrderTrades` queries off the hot path via [`AsyncRestQuerier`].
pub struct PerOrderQueryDriver {
    venue: SpotVenue,
    querier: AsyncRestQuerier,
    slots: SharedSlotTable,
    in_flight: [Option<InFlightTicket>; MAX_REST_SLOTS],
    pending_terminal_orders: BTreeMap<OrderKey, RemoteOrder>,
    next_wire_key: u64,
    rate_budget: Option<Box<dyn FnMut(u64) -> bool + Send>>,
    exec_network: Option<LiveNetwork>,
    exec_reconnect_pending: bool,
}

impl PerOrderQueryDriver {
    pub fn new<F>(
        venue: SpotVenue,
        environment: Environment,
        config: RestQuerierConfig,
        credentials: OwnedCredentials,
        utc_ms_fn: F,
    ) -> std::result::Result<Self, QueryDriverError>
    where
        F: FnMut() -> std::result::Result<u64, tickwire_gateway::Error> + Send + 'static,
    {
        Self::with_clock(
            venue,
            environment,
            config,
            credentials,
            utc_ms_fn,
            Arc::new(tickwire_transport::mono_now_ns),
        )
    }

    pub fn with_clock<F>(
        venue: SpotVenue,
        environment: Environment,
        config: RestQuerierConfig,
        credentials: OwnedCredentials,
        mut utc_ms_fn: F,
        clock: ClockFn,
    ) -> std::result::Result<Self, QueryDriverError>
    where
        F: FnMut() -> std::result::Result<u64, tickwire_gateway::Error> + Send + 'static,
    {
        if credentials.venue() != venue {
            return Err(QueryDriverError::Protocol("credential venue mismatch"));
        }
        let slots: SharedSlotTable = Arc::new(Mutex::new(std::array::from_fn(|_| None)));
        let builder_slots = Arc::clone(&slots);
        let builder = move |task: &RestTask,
                            _host: &str,
                            _scratch: &mut Vec<u8>|
              -> tickwire_transport::Result<Option<BuiltRequest>> {
            let slot_idx = task.kind as usize;
            if slot_idx >= MAX_REST_SLOTS {
                return Ok(None);
            }
            let spec = {
                let guard = builder_slots.lock().unwrap_or_else(|e| e.into_inner());
                guard[slot_idx]
                    .as_ref()
                    .filter(|s| s.wire_key == task.key)
                    .cloned()
            };
            let Some(spec) = spec else {
                return Ok(None);
            };
            let utc_ms = utc_ms_fn().map_err(|_| tickwire_transport::Error::Protocol("clock"))?;
            let creds = credentials.credentials();
            let built = match spec.step {
                QueryWireStep::OrderStatusPrimary => build_order_status_get(
                    &creds,
                    environment,
                    utc_ms,
                    &spec.symbol,
                    &spec.client_id,
                    false,
                ),
                QueryWireStep::BybitOrderStatusHistory => build_order_status_get(
                    &creds,
                    environment,
                    utc_ms,
                    &spec.symbol,
                    &spec.client_id,
                    true,
                ),
                QueryWireStep::OrderTrades => build_order_trades_get(
                    &creds,
                    environment,
                    utc_ms,
                    &spec.symbol,
                    Some(&spec.client_id),
                    spec.native_order_id.as_deref(),
                ),
            }
            .map_err(|_| tickwire_transport::Error::Protocol("sign rest query"))?;
            Ok(Some(built))
        };
        let querier = AsyncRestQuerier::with_clock(config, builder, clock)
            .map_err(QueryDriverError::Transport)?;
        Ok(Self {
            venue,
            querier,
            slots,
            in_flight: std::array::from_fn(|_| None),
            pending_terminal_orders: BTreeMap::new(),
            next_wire_key: 1,
            rate_budget: None,
            exec_network: None,
            exec_reconnect_pending: false,
        })
    }

    pub fn with_rate_budget<R>(mut self, budget: R) -> Self
    where
        R: FnMut(u64) -> bool + Send + 'static,
    {
        self.rate_budget = Some(Box::new(budget));
        self
    }

    pub fn set_rate_budget<R>(&mut self, budget: R)
    where
        R: FnMut(u64) -> bool + Send + 'static,
    {
        self.rate_budget = Some(Box::new(budget));
    }

    pub fn with_allow_query<R>(self, allow_query: R) -> Self
    where
        R: FnMut(u64) -> bool + Send + 'static,
    {
        self.with_rate_budget(allow_query)
    }

    pub fn set_allow_query<R>(&mut self, allow_query: R)
    where
        R: FnMut(u64) -> bool + Send + 'static,
    {
        self.set_rate_budget(allow_query);
    }

    pub fn with_execution_network(mut self, network: LiveNetwork) -> Self {
        self.exec_network = Some(network);
        self
    }

    pub fn set_execution_network(&mut self, network: LiveNetwork) {
        self.exec_network = Some(network);
    }

    pub fn querier(&self) -> &AsyncRestQuerier {
        &self.querier
    }

    pub fn stop(&self) {
        self.querier.stop();
    }

    fn alloc_wire_key(&mut self) -> u64 {
        let key = self.next_wire_key;
        self.next_wire_key = self.next_wire_key.saturating_add(1);
        key
    }

    /// Alias for [`Self::poll`] advancing one off-hot-path [`QueryDesk`] + pool reconnect step.
    pub fn step<
        'a,
        B: Backend,
        A: Authority,
        T: FnMut() -> std::result::Result<u64, tickwire_gateway::Error>,
        S: BlobStore,
    >(
        &mut self,
        runner: &mut AccountRunner<'a, B, A, T, S>,
        cut: Option<PrivateCut>,
        now_mono_ns: u64,
    ) -> std::result::Result<QueryStepOutcome, QueryDriverError> {
        self.poll(runner, cut, now_mono_ns)
    }

    /// Polls ready [`AsyncRestQuerier`] replies, applies order/fill reconciliations to `runner`,
    /// dispatches due [`QueryDesk`] asks, and optionally installs pre-warmed execution sockets.
    pub fn poll<
        'a,
        B: Backend,
        A: Authority,
        T: FnMut() -> std::result::Result<u64, tickwire_gateway::Error>,
        S: BlobStore,
    >(
        &mut self,
        runner: &mut AccountRunner<'a, B, A, T, S>,
        cut: Option<PrivateCut>,
        now_mono_ns: u64,
    ) -> std::result::Result<QueryStepOutcome, QueryDriverError> {
        let mut outcome = QueryStepOutcome::default();

        if runner.session().gateway().has_connection_pool() {
            if !runner.halted() && !runner.account().frozen() {
                let installed = runner.poll_pool_dials(now_mono_ns);
                if installed > 0 {
                    outcome.reconnected_execution = true;
                }
            }
        } else if let Some(network) = self.exec_network.as_ref() {
            if !runner.halted() && !runner.account().frozen() && !runner.has_transport_connection()
            {
                if !self.exec_reconnect_pending {
                    network.notify_disconnected(now_mono_ns);
                    self.exec_reconnect_pending = true;
                }
                if let Some(ready) = network.try_take_ready() {
                    runner
                        .install_transport_connection(ready.into_connection(), Limits::default())
                        .map_err(QueryDriverError::Runner)?;
                    self.exec_reconnect_pending = false;
                    outcome.reconnected_execution = true;
                }
            } else if runner.has_transport_connection() {
                self.exec_reconnect_pending = false;
            }
        }

        let effective_cut = match cut {
            Some(c) => c,
            None => runner
                .qualified_cut()
                .map_err(QueryDriverError::Runner)?
                .unwrap_or_else(|| fallback_cut(runner.scope(), runner.execution_epoch())),
        };

        while let Some(reply) = self.querier.dequeue() {
            outcome.processed_replies += 1;
            self.handle_reply(runner, reply, effective_cut, now_mono_ns, &mut outcome)?;
        }

        self.dispatch_due(runner, now_mono_ns, &mut outcome);
        Ok(outcome)
    }

    fn handle_reply<
        'a,
        B: Backend,
        A: Authority,
        T: FnMut() -> std::result::Result<u64, tickwire_gateway::Error>,
        S: BlobStore,
    >(
        &mut self,
        runner: &mut AccountRunner<'a, B, A, T, S>,
        reply: RestReply,
        cut: PrivateCut,
        now_mono_ns: u64,
        outcome: &mut QueryStepOutcome,
    ) -> std::result::Result<(), QueryDriverError> {
        let slot_idx = reply.task.kind as usize;
        if slot_idx >= MAX_REST_SLOTS {
            return Ok(());
        }
        let matches = self.in_flight[slot_idx]
            .as_ref()
            .is_some_and(|st| st.wire_key == reply.task.key);
        if !matches {
            return Ok(());
        }
        let in_flight = self.in_flight[slot_idx].take().expect("checked in_flight");
        {
            let mut guard = self.slots.lock().unwrap_or_else(|e| e.into_inner());
            if guard[slot_idx]
                .as_ref()
                .is_some_and(|s| s.wire_key == reply.task.key)
            {
                guard[slot_idx] = None;
            }
        }

        if runner.query_desk().is_none() || runner.account().frozen() {
            return Ok(());
        }

        let ticket = in_flight.ticket;
        match reply.status {
            RestStatus::Expired | RestStatus::NotBuilt => {
                if let Some(res) = runner
                    .query_desk_mut()
                    .and_then(|d| d.on_not_sent(&ticket, now_mono_ns))
                {
                    outcome.resolutions.push(res);
                }
                return Ok(());
            }
            RestStatus::SendFail | RestStatus::ReadFail | RestStatus::Timeout => {
                self.record_transport_failure(runner, &ticket, now_mono_ns, outcome);
                return Ok(());
            }
            RestStatus::HttpError => {
                if is_http_account_hazard(reply.http_code) {
                    self.freeze_on_hazard(runner, &ticket.order, outcome);
                } else {
                    self.record_transport_failure(runner, &ticket, now_mono_ns, outcome);
                }
                return Ok(());
            }
            RestStatus::Ok => {}
        }

        if reply.body.len() > 2 * 1024 * 1024 {
            runner.freeze_cash();
            return Err(QueryDriverError::Protocol(
                "REST query response exceeded 2 MiB limit",
            ));
        }
        let body: Value = serde_json::from_slice(&reply.body).map_err(|_| {
            runner.freeze_cash();
            QueryDriverError::Protocol("invalid JSON in REST query response")
        })?;
        let bybit = self.venue == SpotVenue::Bybit;
        let code = if bybit {
            body.get("retCode")
                .and_then(Value::as_i64)
                .map(|n| n.to_string())
        } else {
            body.get("code").and_then(Value::as_str).map(str::to_owned)
        }
        .filter(|s| {
            !s.is_empty() && s.len() <= 32 && s.bytes().all(|b| b.is_ascii_digit() || b == b'-')
        })
        .ok_or_else(|| {
            runner.freeze_cash();
            QueryDriverError::Protocol("missing or invalid venue status code")
        })?;

        if code != "0" {
            match classify_issue(self.venue, &code) {
                SpotIssueKind::AccountHazard => {
                    self.freeze_on_hazard(runner, &ticket.order, outcome);
                }
                SpotIssueKind::NotFound => match in_flight.step {
                    QueryWireStep::OrderStatusPrimary if bybit => {
                        self.dispatch_bybit_history(runner, ticket, now_mono_ns, outcome);
                    }
                    QueryWireStep::OrderStatusPrimary | QueryWireStep::BybitOrderStatusHistory => {
                        self.handle_order_not_found(
                            runner,
                            &ticket,
                            &code,
                            cut,
                            now_mono_ns,
                            outcome,
                        )?;
                    }
                    QueryWireStep::OrderTrades => {
                        self.handle_order_trades(runner, &ticket, &[], cut, now_mono_ns, outcome)?;
                    }
                },
                SpotIssueKind::Ambiguous | SpotIssueKind::DeterministicReject => {
                    self.record_transport_failure(runner, &ticket, now_mono_ns, outcome);
                }
            }
            return Ok(());
        }

        let rows = if bybit {
            if body["result"]["category"].as_str() != Some("spot") {
                runner.freeze_cash();
                return Err(QueryDriverError::Protocol(
                    "unexpected Bybit result category",
                ));
            }
            body["result"]["list"].as_array()
        } else {
            body["data"].as_array()
        }
        .ok_or_else(|| {
            runner.freeze_cash();
            QueryDriverError::Protocol("missing REST query result list")
        })?;

        match in_flight.step {
            QueryWireStep::OrderStatusPrimary | QueryWireStep::BybitOrderStatusHistory => {
                if rows.is_empty() {
                    if bybit && in_flight.step == QueryWireStep::OrderStatusPrimary {
                        self.dispatch_bybit_history(runner, ticket, now_mono_ns, outcome);
                    } else {
                        self.handle_order_not_found(
                            runner,
                            &ticket,
                            "not_found",
                            cut,
                            now_mono_ns,
                            outcome,
                        )?;
                    }
                } else if rows.len() == 1 {
                    let remote = decode_order_row(self.venue, &rows[0]).map_err(|_| {
                        runner.freeze_cash();
                        QueryDriverError::Protocol("invalid REST order row")
                    })?;
                    let row_extras = extract_order_row_extras(self.venue, &rows[0]);
                    let (post_only, tif) = runner
                        .account()
                        .order_terms(&ticket.order)
                        .map(|(_, s)| {
                            (
                                s.time_in_force == Some(TimeInForce::PostOnly),
                                s.time_in_force,
                            )
                        })
                        .unwrap_or((false, remote.time_in_force));
                    let row_terminal_reason = classify_order_row_terminal_reason(
                        self.venue,
                        &rows[0],
                        remote.status,
                        post_only,
                        tif,
                    );
                    self.handle_order_snapshot(
                        runner,
                        &ticket,
                        remote,
                        row_extras,
                        row_terminal_reason,
                        cut,
                        now_mono_ns,
                        outcome,
                    )?;
                } else {
                    runner.freeze_cash();
                    return Err(QueryDriverError::Protocol(
                        "multiple rows returned for single-order status query",
                    ));
                }
            }
            QueryWireStep::OrderTrades => {
                self.handle_order_trades(runner, &ticket, rows, cut, now_mono_ns, outcome)?;
            }
        }
        Ok(())
    }

    fn freeze_on_hazard<
        'a,
        B: Backend,
        A: Authority,
        T: FnMut() -> std::result::Result<u64, tickwire_gateway::Error>,
        S: BlobStore,
    >(
        &mut self,
        runner: &mut AccountRunner<'a, B, A, T, S>,
        order: &OrderKey,
        outcome: &mut QueryStepOutcome,
    ) {
        self.pending_terminal_orders.remove(order);
        if let Some(desk) = runner.query_desk_mut() {
            desk.remove_order(order);
        }
        runner.freeze_cash();
        outcome.account_hazard_frozen = true;
    }

    fn record_transport_failure<
        'a,
        B: Backend,
        A: Authority,
        T: FnMut() -> std::result::Result<u64, tickwire_gateway::Error>,
        S: BlobStore,
    >(
        &mut self,
        runner: &mut AccountRunner<'a, B, A, T, S>,
        ticket: &QueryTicket,
        now_mono_ns: u64,
        outcome: &mut QueryStepOutcome,
    ) {
        let res = runner
            .query_desk_mut()
            .and_then(|d| d.on_transport_failure(ticket, now_mono_ns));
        if let Some(res) = res {
            if matches!(res, QueryResolution::FreezeAccount { .. }) {
                self.pending_terminal_orders.remove(&ticket.order);
                runner.freeze_cash();
            }
            outcome.resolutions.push(res);
        }
    }

    fn dispatch_bybit_history<
        'a,
        B: Backend,
        A: Authority,
        T: FnMut() -> std::result::Result<u64, tickwire_gateway::Error>,
        S: BlobStore,
    >(
        &mut self,
        runner: &mut AccountRunner<'a, B, A, T, S>,
        ticket: QueryTicket,
        now_mono_ns: u64,
        outcome: &mut QueryStepOutcome,
    ) {
        let slot_idx = ticket.slot as usize;
        if slot_idx >= MAX_REST_SLOTS
            || !runner
                .query_desk()
                .is_some_and(|d| d.is_ticket_in_flight(&ticket))
        {
            return;
        }
        if self.in_flight[slot_idx].is_some() {
            if let Some(res) = runner
                .query_desk_mut()
                .and_then(|d| d.on_not_sent(&ticket, now_mono_ns))
            {
                outcome.resolutions.push(res);
            }
            return;
        }
        if let Some(view) = runner.account().order(&ticket.order) {
            let expected = runner.expected_fill_atoms(&ticket.order);
            if expected > view.filled_quantity.atoms() && !view.terminal {
                let snap = SnapshotSummary {
                    confirmed: view.confirmed,
                    terminal: false,
                    cumulative_atoms: expected,
                    local_filled_atoms: view.filled_quantity.atoms(),
                    amend_resolved: true,
                };
                if let Some(res) = runner
                    .query_desk_mut()
                    .and_then(|d| d.on_status_snapshot(&ticket, now_mono_ns, snap))
                {
                    outcome.resolutions.push(res);
                }
                return;
            }
        }
        let Some((instrument, spec)) = runner.account().order_terms(&ticket.order) else {
            if let Some(desk) = runner.query_desk_mut() {
                desk.remove_order(&ticket.order);
            }
            return;
        };
        let symbol = instrument.symbol.as_str().to_owned();
        let active_client_id = spec.client_id.as_str().to_owned();
        if let Some(budget) = self.rate_budget.as_mut()
            && !budget(now_mono_ns)
        {
            if let Some(res) = runner
                .query_desk_mut()
                .and_then(|d| d.on_not_sent(&ticket, now_mono_ns))
            {
                outcome.resolutions.push(res);
            }
            return;
        }
        if !runner.session_mut().try_debit_query_budget(now_mono_ns) {
            if let Some(res) = runner
                .query_desk_mut()
                .and_then(|d| d.on_not_sent(&ticket, now_mono_ns))
            {
                outcome.resolutions.push(res);
            }
            return;
        }
        let slot_idx = ticket.slot as usize;
        let wire_key = self.alloc_wire_key();
        let spec = SlotRequestSpec {
            wire_key,
            step: QueryWireStep::BybitOrderStatusHistory,
            symbol,
            client_id: active_client_id,
            native_order_id: ticket
                .native_order_id
                .as_ref()
                .map(|v| v.as_str().to_owned()),
        };
        {
            let mut guard = self.slots.lock().unwrap_or_else(|e| e.into_inner());
            guard[slot_idx] = Some(spec);
        }
        let task = RestTask {
            key: wire_key,
            kind: u32::from(ticket.slot),
            max_query_count: 1,
            start_mono_ns: 0,
            end_mono_ns: 0,
        };
        if self.querier.enqueue(task) {
            self.in_flight[slot_idx] = Some(InFlightTicket {
                wire_key,
                ticket,
                step: QueryWireStep::BybitOrderStatusHistory,
            });
            outcome.dispatched_tasks += 1;
        } else {
            {
                let mut guard = self.slots.lock().unwrap_or_else(|e| e.into_inner());
                guard[slot_idx] = None;
            }
            if let Some(res) = runner
                .query_desk_mut()
                .and_then(|d| d.on_not_sent(&ticket, now_mono_ns))
            {
                outcome.resolutions.push(res);
            }
        }
    }

    fn handle_order_not_found<
        'a,
        B: Backend,
        A: Authority,
        T: FnMut() -> std::result::Result<u64, tickwire_gateway::Error>,
        S: BlobStore,
    >(
        &mut self,
        runner: &mut AccountRunner<'a, B, A, T, S>,
        ticket: &QueryTicket,
        code: &str,
        cut: PrivateCut,
        now_mono_ns: u64,
        outcome: &mut QueryStepOutcome,
    ) -> std::result::Result<(), QueryDriverError> {
        let confirmed = runner.is_order_confirmed_or_acked(&ticket.order);
        if confirmed
            && runner.account().order_in_doubt(&ticket.order) == Some(DoubtReason::AmendAmbiguous)
            && runner
                .record_query_modify_rejected(
                    &ticket.order,
                    None,
                    TerminalReason::OrderNotOpen,
                    cut,
                    now_mono_ns,
                )
                .is_err()
        {
            runner.freeze_cash();
        }
        let res = runner
            .query_desk_mut()
            .and_then(|d| d.on_not_found(ticket, now_mono_ns, confirmed));
        if let Some(res) = res {
            match &res {
                QueryResolution::CloseUncreated { order, .. } => {
                    self.pending_terminal_orders.remove(order);
                    runner
                        .close_uncreated_order(order, code, now_mono_ns)
                        .map_err(QueryDriverError::Runner)?;
                }
                QueryResolution::FreezeAccount { order, .. } => {
                    self.pending_terminal_orders.remove(order);
                    runner.freeze_cash();
                }
                QueryResolution::FetchTradesBeforeClose { order, .. }
                    if runner
                        .session
                        .gateway_mut()
                        .isolate_intent_in_doubt(order.intent, DoubtReason::FillCatchup)
                        .is_err() =>
                {
                    runner.freeze_cash();
                }
                _ => {}
            }
            outcome.resolutions.push(res);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_order_snapshot<
        'a,
        B: Backend,
        A: Authority,
        T: FnMut() -> std::result::Result<u64, tickwire_gateway::Error>,
        S: BlobStore,
    >(
        &mut self,
        runner: &mut AccountRunner<'a, B, A, T, S>,
        ticket: &QueryTicket,
        remote: RemoteOrder,
        row_extras: OrderRowExtras,
        row_terminal_reason: Option<TerminalReason>,
        cut: PrivateCut,
        now_mono_ns: u64,
        outcome: &mut QueryStepOutcome,
    ) -> std::result::Result<(), QueryDriverError> {
        let amend_in_doubt =
            runner.account().order_in_doubt(&ticket.order) == Some(DoubtReason::AmendAmbiguous);
        if amend_in_doubt || ticket.kind == AskKind::Amend {
            let Some((instrument, spec)) = runner
                .account()
                .order_terms(&ticket.order)
                .map(|(i, s)| (i.clone(), s.clone()))
            else {
                runner.freeze_cash();
                return Err(QueryDriverError::Runner(crate::Error::Origin(
                    "unknown remote order",
                )));
            };
            if remote.symbol != instrument.symbol.as_str()
                || (remote.client_id != ticket.order.client_id.as_str()
                    && remote.client_id != spec.client_id.as_str())
                || remote.side != spec.side
                || remote.order_type != spec.order_type
                || remote.time_in_force != spec.time_in_force
                || runner
                    .account()
                    .venue_order_id(&ticket.order)
                    .is_some_and(|id| id.as_str() != remote.native_id)
            {
                runner.freeze_cash();
                return Err(QueryDriverError::Runner(crate::Error::Origin(
                    "remote order terms differ from the admitted command",
                )));
            }
            let terms_changed =
                Quantity::Base(remote.qty) != spec.quantity || remote.price != spec.price;
            if terms_changed {
                let Some(new_px) = remote.price else {
                    runner.freeze_cash();
                    return Err(QueryDriverError::Protocol(
                        "modified limit order missing price",
                    ));
                };
                if let Err(e) =
                    runner.record_query_modified(&ticket.order, &remote, new_px, cut, now_mono_ns)
                {
                    runner.freeze_cash();
                    return Err(QueryDriverError::Runner(e));
                }
            } else if amend_in_doubt {
                let reason = if compare::open(remote.status) {
                    TerminalReason::VenueRejected
                } else {
                    TerminalReason::OrderNotOpen
                };
                if let Err(e) = runner.record_query_modify_rejected(
                    &ticket.order,
                    Some(&remote),
                    reason,
                    cut,
                    now_mono_ns,
                ) {
                    runner.freeze_cash();
                    return Err(QueryDriverError::Runner(e));
                }
            }
        }

        if let Err(e) = compare::admitted_terms(runner.account(), &ticket.order, &remote) {
            runner.freeze_cash();
            return Err(QueryDriverError::Runner(e));
        }
        let native = VenueOrderId::new(&remote.native_id).map_err(|_| {
            runner.freeze_cash();
            QueryDriverError::Protocol("invalid native order ID in status snapshot")
        })?;
        runner.record_acked_native_order_id(ticket.order.clone(), native);

        if runner.projected_fill_enabled()
            && remote.cumulative.is_positive()
            && let Err(e) =
                runner.apply_order_row_projected_fill(&ticket.order, remote.cumulative, row_extras)
        {
            runner.freeze_cash();
            return Err(QueryDriverError::Runner(e));
        }

        if let Some(reason) = row_terminal_reason {
            runner.note_order_terminal_reason(&ticket.order, reason);
        }

        let Some(view) = runner.account().order(&ticket.order) else {
            return Ok(());
        };
        let remote_cum =
            compare::amount(remote.cumulative, view.filled_quantity.scale()).map_err(|e| {
                runner.freeze_cash();
                QueryDriverError::Runner(e)
            })?;
        let local_filled = view.filled_quantity;
        let terminal = !compare::open(remote.status);
        if terminal && remote_cum.atoms() < local_filled.atoms() {
            runner.freeze_cash();
            return Err(QueryDriverError::Protocol(
                "remote cumulative regressed below recorded fills",
            ));
        }

        let prev_expected = runner.expected_fill_atoms(&ticket.order);
        let mut effective_cum = remote_cum
            .atoms()
            .max(local_filled.atoms())
            .max(prev_expected);
        if (view.terminal && view.remaining_quantity.atoms() > 0)
            || runner.account().order_in_doubt(&ticket.order) == Some(DoubtReason::FillCatchup)
        {
            effective_cum = effective_cum.max(local_filled.atoms().saturating_add(1));
        }
        let effective_terminal = terminal || view.terminal;
        if effective_cum > local_filled.atoms() {
            runner.note_expected_fill_atoms(&ticket.order, effective_cum);
            if !effective_terminal {
                let raw = canonical_order_raw(&remote)?;
                let input = runner.rest_input(cut, b"rest-order", &raw, now_mono_ns);
                if let Err(e) = runner.record_order(&remote, input) {
                    runner.freeze_cash();
                    return Err(QueryDriverError::Runner(e));
                }
            } else if terminal {
                self.pending_terminal_orders
                    .insert(ticket.order.clone(), remote);
            }
            if effective_terminal
                || runner.account().order_in_doubt(&ticket.order)
                    != Some(DoubtReason::CancelAmbiguous)
            {
                runner
                    .session
                    .gateway_mut()
                    .isolate_intent_in_doubt(ticket.order.intent, DoubtReason::FillCatchup)
                    .map_err(|e| {
                        runner.freeze_cash();
                        QueryDriverError::Runner(crate::Error::Session(SessionError::Gateway(e)))
                    })?;
            }
            let snap = SnapshotSummary {
                confirmed: true,
                terminal: effective_terminal,
                cumulative_atoms: effective_cum,
                local_filled_atoms: local_filled.atoms(),
                amend_resolved: true,
            };
            if let Some(res) = runner
                .query_desk_mut()
                .and_then(|d| d.on_status_snapshot(ticket, now_mono_ns, snap))
            {
                outcome.resolutions.push(res);
            }
            return Ok(());
        }

        runner.clear_expected_fill_atoms(&ticket.order);
        self.pending_terminal_orders.remove(&ticket.order);
        if terminal || (!view.terminal && remote_cum.atoms() >= local_filled.atoms()) {
            let raw = canonical_order_raw(&remote)?;
            let input = runner.rest_input(cut, b"rest-order", &raw, now_mono_ns);
            if let Err(e) = runner.record_order(&remote, input) {
                runner.freeze_cash();
                return Err(QueryDriverError::Runner(e));
            }
        }
        let Some(after_view) = runner.account().order(&ticket.order) else {
            return Ok(());
        };
        let snap = SnapshotSummary {
            confirmed: after_view.confirmed,
            terminal: after_view.terminal,
            cumulative_atoms: effective_cum,
            local_filled_atoms: after_view.filled_quantity.atoms(),
            amend_resolved: true,
        };
        let res = runner
            .query_desk_mut()
            .and_then(|d| d.on_status_snapshot(ticket, now_mono_ns, snap));
        if matches!(
            &res,
            Some(QueryResolution::CancelRejectedStillOpen {
                remaining_pending_cancels: 0,
                ..
            })
        ) && !after_view.terminal
            && runner.account().order_in_doubt(&ticket.order) == Some(DoubtReason::CancelAmbiguous)
        {
            runner
                .session
                .gateway_mut()
                .clear_order_in_doubt(&ticket.order)
                .map_err(|e| {
                    runner.freeze_cash();
                    QueryDriverError::Runner(crate::Error::Session(SessionError::Gateway(e)))
                })?;
        }
        if after_view.terminal {
            runner
                .close_terminal_fees_if_ready(&ticket.order, now_mono_ns)
                .map_err(QueryDriverError::Runner)?;
        }
        if let Some(res) = res {
            outcome.resolutions.push(res);
        }
        Ok(())
    }

    fn handle_order_trades<
        'a,
        B: Backend,
        A: Authority,
        T: FnMut() -> std::result::Result<u64, tickwire_gateway::Error>,
        S: BlobStore,
    >(
        &mut self,
        runner: &mut AccountRunner<'a, B, A, T, S>,
        ticket: &QueryTicket,
        rows: &[Value],
        cut: PrivateCut,
        now_mono_ns: u64,
        outcome: &mut QueryStepOutcome,
    ) -> std::result::Result<(), QueryDriverError> {
        let Some((instrument, spec)) = runner
            .account()
            .order_terms(&ticket.order)
            .map(|(i, s)| (i.clone(), s.clone()))
        else {
            return Ok(());
        };
        let mut fills: Vec<RemoteFill> = Vec::new();
        for row in rows {
            let fill = decode_fill_row(self.venue, row).map_err(|_| {
                runner.freeze_cash();
                QueryDriverError::Protocol("invalid REST fill row")
            })?;
            if fill.client_id != ticket.order.client_id.as_str()
                && fill.client_id != spec.client_id.as_str()
            {
                continue;
            }
            if fill.symbol != instrument.symbol.as_str()
                || fill.side != spec.side
                || runner
                    .account()
                    .venue_order_id(&ticket.order)
                    .is_some_and(|id| id.as_str() != fill.native_id)
            {
                runner.freeze_cash();
                return Err(QueryDriverError::Protocol(
                    "REST fill terms conflict with admitted order",
                ));
            }
            fills.push(fill);
        }
        fills.sort_by(|a, b| (a.time_ms, &a.trade_id).cmp(&(b.time_ms, &b.trade_id)));

        for fill in &fills {
            let input = runner.rest_input(cut, b"rest-fill", &fill.wire_row, now_mono_ns);
            runner
                .record_fill(
                    &fill.client_id,
                    input.stream,
                    input.seq,
                    input.epoch,
                    input.received,
                    &fill.wire_row,
                )
                .map_err(QueryDriverError::Runner)?;
        }

        if let Some(pending) = self.pending_terminal_orders.get(&ticket.order).cloned()
            && let Some(view) = runner.account().order(&ticket.order)
        {
            let target_cum = compare::amount(pending.cumulative, view.filled_quantity.scale())
                .map_err(|e| {
                    runner.freeze_cash();
                    QueryDriverError::Runner(e)
                })?;
            if view.filled_quantity == target_cum {
                self.pending_terminal_orders.remove(&ticket.order);
                if !view.terminal {
                    let raw = canonical_order_raw(&pending)?;
                    let input = runner.rest_input(cut, b"rest-order", &raw, now_mono_ns);
                    runner.record_order(&pending, input).map_err(|e| {
                        runner.freeze_cash();
                        QueryDriverError::Runner(e)
                    })?;
                }
            }
        }

        if let Some(view) = runner.account().order(&ticket.order) {
            let min_expected = runner.expected_fill_atoms(&ticket.order);
            if view.filled_quantity.atoms() >= min_expected {
                runner.clear_expected_fill_atoms(&ticket.order);
                if (min_expected > 0 || !fills.is_empty())
                    && !self.pending_terminal_orders.contains_key(&ticket.order)
                    && (!view.terminal || view.remaining_quantity.atoms() == 0)
                    && runner.account().order_in_doubt(&ticket.order)
                        == Some(DoubtReason::FillCatchup)
                    && runner
                        .session
                        .gateway_mut()
                        .clear_order_in_doubt(&ticket.order)
                        .is_err()
                {
                    runner.freeze_cash();
                }
            } else {
                runner
                    .session
                    .gateway_mut()
                    .isolate_intent_in_doubt(ticket.order.intent, DoubtReason::FillCatchup)
                    .map_err(|e| {
                        runner.freeze_cash();
                        QueryDriverError::Runner(crate::Error::Session(SessionError::Gateway(e)))
                    })?;
            }
        }

        let caught_up = !self.pending_terminal_orders.contains_key(&ticket.order)
            && runner.expected_fill_atoms(&ticket.order) == 0
            && runner.account().order(&ticket.order).is_some_and(|v| {
                let doubt = runner.account().order_in_doubt(&ticket.order);
                if v.terminal {
                    doubt.is_none() && v.remaining_quantity.atoms() == 0
                } else {
                    doubt != Some(DoubtReason::FillCatchup)
                }
            });

        let res = runner
            .query_desk_mut()
            .and_then(|d| d.on_trades_applied(ticket, now_mono_ns, caught_up));
        if let Some(ref resolution) = res
            && let Some(view) = runner.account().order(&ticket.order)
            && !view.terminal
        {
            if let QueryResolution::RetryScheduled { order, .. } = resolution {
                if runner.query_desk().and_then(|d| d.ask_kind(order)) == Some(AskKind::Cancel) {
                    let _ = runner
                        .session
                        .gateway_mut()
                        .isolate_intent_in_doubt(order.intent, DoubtReason::CancelAmbiguous);
                }
            } else if let QueryResolution::Resolved { order } = resolution
                && runner.account().order_in_doubt(order) == Some(DoubtReason::CancelAmbiguous)
                && runner
                    .session
                    .gateway_mut()
                    .clear_order_in_doubt(order)
                    .is_err()
            {
                runner.freeze_cash();
            }
        }
        if caught_up {
            runner
                .close_terminal_fees_if_ready(&ticket.order, now_mono_ns)
                .map_err(QueryDriverError::Runner)?;
        }
        if let Some(res) = res {
            outcome.resolutions.push(res);
        }
        Ok(())
    }

    fn dispatch_due<
        'a,
        B: Backend,
        A: Authority,
        T: FnMut() -> std::result::Result<u64, tickwire_gateway::Error>,
        S: BlobStore,
    >(
        &mut self,
        runner: &mut AccountRunner<'a, B, A, T, S>,
        now_mono_ns: u64,
        outcome: &mut QueryStepOutcome,
    ) {
        while !runner.account().frozen() {
            let Some(mut ticket) = runner.poll_due_query_ticket(now_mono_ns) else {
                break;
            };
            let slot_idx = ticket.slot as usize;
            if slot_idx >= MAX_REST_SLOTS || self.in_flight[slot_idx].is_some() {
                if let Some(res) = runner
                    .query_desk_mut()
                    .and_then(|d| d.on_not_sent(&ticket, now_mono_ns))
                {
                    outcome.resolutions.push(res);
                }
                break;
            }
            if ticket.native_order_id.is_none() {
                ticket.native_order_id = runner.known_native_order_id(&ticket.order);
            }
            let Some((instrument, spec)) = runner.account().order_terms(&ticket.order) else {
                if let Some(desk) = runner.query_desk_mut() {
                    desk.remove_order(&ticket.order);
                }
                continue;
            };
            let symbol = instrument.symbol.as_str().to_owned();
            let active_client_id = spec.client_id.as_str().to_owned();

            if let Some(budget) = self.rate_budget.as_mut()
                && !budget(now_mono_ns)
            {
                if let Some(res) = runner
                    .query_desk_mut()
                    .and_then(|d| d.on_not_sent(&ticket, now_mono_ns))
                {
                    outcome.resolutions.push(res);
                }
                break;
            }
            if !runner.session_mut().try_debit_query_budget(now_mono_ns) {
                if let Some(res) = runner
                    .query_desk_mut()
                    .and_then(|d| d.on_not_sent(&ticket, now_mono_ns))
                {
                    outcome.resolutions.push(res);
                }
                break;
            }

            let step = if ticket.kind == AskKind::Trades {
                QueryWireStep::OrderTrades
            } else {
                QueryWireStep::OrderStatusPrimary
            };
            let slot_idx = ticket.slot as usize;
            let wire_key = self.alloc_wire_key();
            let spec = SlotRequestSpec {
                wire_key,
                step,
                symbol,
                client_id: active_client_id,
                native_order_id: ticket
                    .native_order_id
                    .as_ref()
                    .map(|v| v.as_str().to_owned()),
            };
            {
                let mut guard = self.slots.lock().unwrap_or_else(|e| e.into_inner());
                guard[slot_idx] = Some(spec);
            }
            let task = RestTask {
                key: wire_key,
                kind: u32::from(ticket.slot),
                max_query_count: 1,
                start_mono_ns: 0,
                end_mono_ns: 0,
            };
            if self.querier.enqueue(task) {
                self.in_flight[slot_idx] = Some(InFlightTicket {
                    wire_key,
                    ticket,
                    step,
                });
                outcome.dispatched_tasks += 1;
            } else {
                {
                    let mut guard = self.slots.lock().unwrap_or_else(|e| e.into_inner());
                    guard[slot_idx] = None;
                }
                if let Some(res) = runner
                    .query_desk_mut()
                    .and_then(|d| d.on_not_sent(&ticket, now_mono_ns))
                {
                    outcome.resolutions.push(res);
                }
                break;
            }
        }
    }
}

fn canonical_order_raw(o: &RemoteOrder) -> std::result::Result<Vec<u8>, QueryDriverError> {
    serde_json::to_vec(&(
        &o.symbol,
        &o.client_id,
        &o.native_id,
        o.updated_ms,
        format!("{:?}", o.status),
        o.qty.as_plain_string(),
        o.price.map(|p| p.as_plain_string()),
        o.cumulative.as_plain_string(),
    ))
    .map_err(|_| QueryDriverError::Protocol("invalid REST order source"))
}

fn fallback_cut(scope: &Scope, epoch: u64) -> PrivateCut {
    let mut seed = b"tickwire.spot.query-desk.v1".to_vec();
    seed.extend_from_slice(&scope.risk_domain_id);
    seed.extend_from_slice(&scope.venue.to_be_bytes());
    seed.extend_from_slice(&scope.account.to_be_bytes());
    let mut stream: [u8; 16] = digest(&seed)[..16].try_into().expect("sha256 prefix");
    if stream == [0; 16] {
        stream[0] = 1;
    }
    let epoch = epoch.max(1);
    PrivateCut {
        stream,
        epoch,
        gap_generation: epoch,
        through: 1,
    }
}
