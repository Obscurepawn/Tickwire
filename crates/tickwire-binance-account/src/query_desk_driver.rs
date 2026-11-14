//! Off-hot-path per-order `QueryDesk` driver backed by `AsyncRestQuerier`.
//!
//! Dispatches `GET /api/v3/order` / `GET /fapi/v1/order` (`OrderStatus`) and
//! `GET /api/v3/myTrades` / `GET /fapi/v1/userTrades` (`OrderTrades`) over a dedicated
//! background keep-alive HTTPS worker without blocking the single-writer order sender,
//! and journals every raw HTTP observation through `AccountIngress` before mutating
//! `AccountGateway`.

use crate::{
    Error, Result, Store,
    reconciliation::{ReconcileConfig, TradeQuery, authority, orders::OrderQuery, source},
    rest::{RestDisposition, RestOwner},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
};
use tickwire_account::{
    Account, AskKind, DoubtReason, Fixed, IntentKey, MAX_REST_SLOTS, OrderKey, QueryResolution,
    QueryTicket, RecoveryRecord, SnapshotSummary,
};
use tickwire_account_gateway::{Authority, Encoder};
use tickwire_account_journal::Backend;
use tickwire_binance::{
    auth::{Endpoint, MAX_SIGNATURE},
    params::{Params, Value},
    request::{Credentials, Market},
    response::{self, FailureKind},
};
use tickwire_binance_private::{AccountScope, Clock, ClockSample, SignedTime};
use tickwire_transport::{
    ClockFn, Limits, mono_now_ns,
    rest_querier::{
        AsyncRestQuerier, BuiltRequest, DialSecurity, RestQuerierConfig, RestReply, RestStatus,
        RestTask,
    },
    rustls,
};
use tickwire_venue as v;

const TASK_SLOT_SHIFT: u32 = 56;
const TASK_GEN_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryOperation {
    OrderStatus,
    OrderTrades,
}

pub trait PerOrderQueryPermit {
    fn allow(
        &mut self,
        scope: &AccountScope,
        order: &OrderKey,
        request_epoch: u64,
        operation: QueryOperation,
        now: &ClockSample,
    ) -> bool;
}

impl<F> PerOrderQueryPermit for F
where
    F: FnMut(&AccountScope, &OrderKey, u64, QueryOperation, &ClockSample) -> bool,
{
    fn allow(
        &mut self,
        scope: &AccountScope,
        order: &OrderKey,
        request_epoch: u64,
        operation: QueryOperation,
        now: &ClockSample,
    ) -> bool {
        self(scope, order, request_epoch, operation, now)
    }
}

#[derive(Clone)]
pub struct PerOrderQueryConfig {
    pub scope: AccountScope,
    pub address: SocketAddr,
    pub local_ip: Option<IpAddr>,
    pub tls: Arc<rustls::ClientConfig>,
    pub limits: Limits,
    pub first_connection_epoch: u64,
    pub request_timeout_ns: u64,
    pub connect_timeout_ms: u64,
    pub retry_interval_ms: u64,
    pub recv_window_micros: u64,
    pub max_response_bytes: usize,
    pub trade_page_limit: u16,
    pub queue_capacity: usize,
}

impl PerOrderQueryConfig {
    pub fn from_reconcile(config: ReconcileConfig) -> Self {
        let timeout_ms = (config.request_timeout_ns / 1_000_000).max(1);
        Self {
            scope: config.scope,
            address: config.address,
            local_ip: None,
            tls: config.tls,
            limits: config.limits,
            first_connection_epoch: config.first_connection_epoch,
            request_timeout_ns: config.request_timeout_ns,
            connect_timeout_ms: timeout_ms,
            retry_interval_ms: 50,
            recv_window_micros: config.recv_window_micros,
            max_response_bytes: config.max_response_bytes,
            trade_page_limit: 16,
            queue_capacity: MAX_REST_SLOTS * 2,
        }
    }

    fn validate(&self) -> Result<()> {
        self.scope.validate().map_err(Error::Clock)?;
        if self.first_connection_epoch == 0
            || self.request_timeout_ns == 0
            || self.connect_timeout_ms == 0
            || self.recv_window_micros == 0
            || self.recv_window_micros > 60_000_000
            || (self.scope.market == Market::Usdm && !self.recv_window_micros.is_multiple_of(1000))
            || self.max_response_bytes == 0
            || self.max_response_bytes > 32 * 1024
            || self.trade_page_limit == 0
            || self.trade_page_limit > 16
            || self.queue_capacity == 0
        {
            return Err(Error::Capacity);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PerOrderQueryStep {
    pub dispatched: usize,
    pub replies_processed: usize,
    pub resolutions: Vec<QueryResolution>,
}

struct InflightQuery {
    ticket: QueryTicket,
    epoch: u64,
    operation: QueryOperation,
    order_query: Option<OrderQuery>,
    trade_query: Option<TradeQuery>,
    probed_spot_new_cid: Option<(IntentKey, v::ClientId)>,
}

type StagedTable = [Option<(u64, BuiltRequest)>; MAX_REST_SLOTS];

pub struct PerOrderQueryDriver {
    config: PerOrderQueryConfig,
    querier: AsyncRestQuerier,
    querier_clock: ClockFn,
    staged: Arc<Mutex<StagedTable>>,
    inflight: [Option<InflightQuery>; MAX_REST_SLOTS],
    trade_next_from_id: BTreeMap<OrderKey, u64>,
    expected_cumulative_atoms: BTreeMap<OrderKey, i128>,
    spot_probed_new_cid_missing: BTreeSet<(OrderKey, IntentKey)>,
    next_epoch: u64,
    issued_requests: u64,
    last_clock: Option<ClockSample>,
    faulted: bool,
}

impl PerOrderQueryDriver {
    pub fn new(config: PerOrderQueryConfig) -> Result<Self> {
        Self::with_querier_clock(config, Arc::new(mono_now_ns))
    }

    pub fn with_querier_clock(config: PerOrderQueryConfig, querier_clock: ClockFn) -> Result<Self> {
        config.validate()?;
        let host = authority(&config.scope).to_owned();
        let server_name =
            rustls::pki_types::ServerName::try_from(host.clone()).map_err(|_| Error::Identity)?;
        let timeout_ms = (config.request_timeout_ns / 1_000_000).max(1);
        let querier_cfg = RestQuerierConfig {
            host,
            port: config.address.port(),
            initial_peers: vec![config.address],
            local_ip: config.local_ip,
            security: DialSecurity::Tls {
                server_name,
                config: Arc::clone(&config.tls),
            },
            limits: config.limits,
            capacity: config.queue_capacity,
            connect_timeout_ms: config.connect_timeout_ms,
            interval_ms: config.retry_interval_ms.max(1),
            timeout_ms,
        };
        let staged: Arc<Mutex<StagedTable>> = Arc::new(Mutex::new(std::array::from_fn(|_| None)));
        let staged_worker = Arc::clone(&staged);
        let querier = AsyncRestQuerier::with_clock(
            querier_cfg,
            move |task: &RestTask, _host: &str, _scratch: &mut Vec<u8>| {
                let slot = (task.key >> TASK_SLOT_SHIFT) as usize;
                let generation = task.key & TASK_GEN_MASK;
                if slot >= MAX_REST_SLOTS {
                    return Ok(None);
                }
                let mut guard = staged_worker.lock().unwrap_or_else(|e| e.into_inner());
                if guard[slot]
                    .as_ref()
                    .is_some_and(|(staged_gen, _)| *staged_gen == generation)
                {
                    return Ok(guard[slot].take().map(|(_, req)| req));
                }
                Ok(None)
            },
            Arc::clone(&querier_clock),
        )
        .map_err(Error::Transport)?;
        Ok(Self {
            next_epoch: config.first_connection_epoch,
            config,
            querier,
            querier_clock,
            staged,
            inflight: std::array::from_fn(|_| None),
            trade_next_from_id: BTreeMap::new(),
            expected_cumulative_atoms: BTreeMap::new(),
            spot_probed_new_cid_missing: BTreeSet::new(),
            issued_requests: 0,
            last_clock: None,
            faulted: false,
        })
    }

    pub fn querier(&self) -> &AsyncRestQuerier {
        &self.querier
    }

    pub fn issued_requests(&self) -> u64 {
        self.issued_requests
    }

    pub fn in_flight(&self) -> usize {
        self.inflight.iter().filter(|s| s.is_some()).count()
    }

    pub fn stop(&self) {
        self.querier.stop();
    }

    fn sample(&mut self, clock: &mut impl Clock) -> Result<ClockSample> {
        let now = clock.sample().map_err(Error::Clock)?;
        if self.last_clock.is_some_and(|old| {
            now.monotonic.domain() != old.monotonic.domain()
                || now.monotonic_ns() < old.monotonic_ns()
                || now.instant < old.instant
        }) {
            self.faulted = true;
            return Err(Error::Clock(tickwire_binance_private::Error::ClockDomain));
        }
        self.last_clock = Some(now);
        Ok(now)
    }

    fn clear_staged_slot(&self, slot: usize, generation: u64) {
        if slot < MAX_REST_SLOTS {
            let mut guard = self.staged.lock().unwrap_or_else(|e| e.into_inner());
            if guard[slot].as_ref().is_some_and(|(g, _)| *g == generation) {
                guard[slot] = None;
            }
        }
    }

    pub fn poll<S: Store, B: Backend, A: Authority, E: Encoder>(
        &mut self,
        owner: &mut RestOwner<S, B, A, E>,
        credentials: &Credentials<'_>,
        clock: &mut impl Clock,
        permit: &mut impl PerOrderQueryPermit,
    ) -> Result<PerOrderQueryStep> {
        if self.faulted {
            return Err(Error::Stopped);
        }
        owner.ingress.ready(&owner.gateway)?;
        let scope = &owner.ingress.config.baseline.snapshot().scope;
        if self.config.scope.risk_domain_id != scope.risk_domain_id
            || self.config.scope.venue != scope.venue
            || self.config.scope.account != scope.account
            || self.config.scope.environment != scope.environment
            || self.config.scope.market != owner.ingress.market()?
            || credentials.market != self.config.scope.market
            || credentials.account != scope.account
            || credentials.environment != scope.environment
        {
            return Err(Error::Identity);
        }
        if self.config.max_response_bytes > owner.ingress.config.max_raw_bytes {
            return Err(Error::Capacity);
        }

        let now_sample = self.sample(clock)?;
        let now_mono_ns = now_sample.monotonic_ns();
        let mut step = PerOrderQueryStep::default();
        let mut spot_fallback_tickets: [Option<QueryTicket>; MAX_REST_SLOTS] =
            [const { None }; MAX_REST_SLOTS];
        let mut spot_fallback_idx = 0usize;
        let mut spot_fallback_len = 0usize;

        while let Some(reply) = self.querier.dequeue() {
            let slot = (reply.task.key >> TASK_SLOT_SHIFT) as usize;
            let generation = reply.task.key & TASK_GEN_MASK;
            self.clear_staged_slot(slot, generation);
            if slot >= MAX_REST_SLOTS {
                continue;
            }
            let Some(inflight) = self.inflight[slot].take() else {
                continue;
            };
            if inflight.ticket.generation != generation {
                continue;
            }
            step.replies_processed += 1;
            if !owner.gateway.per_order_doubt_isolation()
                || owner.query_desk.is_none()
                || owner.account().frozen()
            {
                continue;
            }
            self.handle_reply(
                owner,
                inflight,
                reply,
                now_mono_ns,
                &mut step,
                &mut spot_fallback_tickets,
                &mut spot_fallback_len,
            )?;
        }

        if !owner.gateway.per_order_doubt_isolation()
            || owner.query_desk.is_none()
            || owner.account().frozen()
        {
            return Ok(step);
        }

        let (priv_epoch, gap_gen) = owner
            .ingress
            .latest_gap_marker
            .map(|(e, g)| (e.max(1), g.max(1)))
            .unwrap_or((1, 1));

        while !owner.account().frozen() {
            let next_fallback = if spot_fallback_idx < spot_fallback_len {
                let t = spot_fallback_tickets[spot_fallback_idx].take();
                spot_fallback_idx += 1;
                t
            } else {
                None
            };
            let account_snapshot = owner.gateway.account();
            let Some(ticket) = next_fallback.or_else(|| {
                owner.query_desk.as_mut().and_then(|desk| {
                    desk.poll_due(now_mono_ns, |order, kind| {
                        should_query_order(account_snapshot, order, kind)
                    })
                })
            }) else {
                break;
            };

            let slot = ticket.slot as usize;
            if slot >= MAX_REST_SLOTS || self.inflight[slot].is_some() {
                if let Some(res) = owner
                    .query_desk
                    .as_mut()
                    .and_then(|d| d.on_not_sent(&ticket, now_mono_ns))
                {
                    step.resolutions.push(res);
                }
                break;
            }

            let Some((command, identity)) = owner
                .ingress
                .registry
                .get(&ticket.order.intent)
                .and_then(|reg| {
                    let id = reg
                        .orders
                        .iter()
                        .find(|o| o.client_id.as_ref() == Some(&ticket.order.client_id))?
                        .clone();
                    Some((reg.command.clone(), id))
                })
            else {
                owner.gateway.freeze_account();
                break;
            };

            let operation = if ticket.kind == AskKind::Trades && identity.venue_order_id.is_some() {
                QueryOperation::OrderTrades
            } else {
                QueryOperation::OrderStatus
            };

            let Some(stamp) = now_sample.signing_time else {
                if let Some(res) = owner
                    .query_desk
                    .as_mut()
                    .and_then(|d| d.on_not_sent(&ticket, now_mono_ns))
                {
                    step.resolutions.push(res);
                }
                break;
            };
            if source(&self.config.scope, &stamp, &now_sample).is_err()
                || clock
                    .check_signed(
                        &self.config.scope,
                        &stamp,
                        &now_sample,
                        self.config.recv_window_micros,
                    )
                    .is_err()
            {
                if let Some(res) = owner
                    .query_desk
                    .as_mut()
                    .and_then(|d| d.on_not_sent(&ticket, now_mono_ns))
                {
                    step.resolutions.push(res);
                }
                break;
            }

            let epoch = self.next_epoch;
            self.next_epoch = self.next_epoch.checked_add(1).ok_or(Error::Capacity)?;

            if !permit.allow(
                &self.config.scope,
                &ticket.order,
                epoch,
                operation,
                &now_sample,
            ) {
                if let Some(res) = owner
                    .query_desk
                    .as_mut()
                    .and_then(|d| d.on_not_sent(&ticket, now_mono_ns))
                {
                    step.resolutions.push(res);
                }
                break;
            }

            let (built, order_query, trade_query, probed_spot_new_cid) = match operation {
                QueryOperation::OrderStatus => {
                    let inst_id = command.command().context.instrument.instrument_id;
                    let probed_new_cid = if self.config.scope.market == Market::Spot {
                        crate::modify::find_unsettled_modify(&owner.gateway, inst_id, &ticket.order)
                            .and_then(|(mod_intent, m)| {
                                let new_cid = m.new_client_id?;
                                if identity.client_id.as_ref() != Some(&new_cid)
                                    && !self
                                        .spot_probed_new_cid_missing
                                        .contains(&(ticket.order.clone(), mod_intent))
                                {
                                    Some((mod_intent, new_cid))
                                } else {
                                    None
                                }
                            })
                    } else {
                        None
                    };
                    let oq = OrderQuery {
                        intent: ticket.order.intent,
                        order_id: identity.order_id,
                        private_connection_epoch: priv_epoch,
                        gap_generation: gap_gen,
                    };
                    match build_order_status_request(
                        &self.config,
                        &command,
                        &identity,
                        probed_new_cid.as_ref().map(|(_, cid)| cid),
                        credentials,
                        stamp,
                    ) {
                        Ok(req) => (req, Some(oq), None, probed_new_cid),
                        Err(_) => {
                            if let Some(res) = owner
                                .query_desk
                                .as_mut()
                                .and_then(|d| d.on_not_sent(&ticket, now_mono_ns))
                            {
                                step.resolutions.push(res);
                            }
                            continue;
                        }
                    }
                }
                QueryOperation::OrderTrades => {
                    let from_id = self
                        .trade_next_from_id
                        .get(&ticket.order)
                        .copied()
                        .unwrap_or(0);
                    let tq = TradeQuery {
                        intent: ticket.order.intent,
                        order_id: identity.order_id,
                        from_id,
                        limit: self.config.trade_page_limit,
                        private_connection_epoch: priv_epoch,
                        gap_generation: gap_gen,
                    };
                    match build_order_trades_request(
                        &self.config,
                        &command,
                        &identity,
                        tq,
                        credentials,
                        stamp,
                    ) {
                        Ok(req) => (req, None, Some(tq), None),
                        Err(_) => {
                            if let Some(res) = owner
                                .query_desk
                                .as_mut()
                                .and_then(|d| d.on_not_sent(&ticket, now_mono_ns))
                            {
                                step.resolutions.push(res);
                            }
                            continue;
                        }
                    }
                }
            };

            let generation = ticket.generation & TASK_GEN_MASK;
            {
                let mut guard = self.staged.lock().unwrap_or_else(|e| e.into_inner());
                guard[slot] = Some((generation, built));
            }
            let worker_now = (self.querier_clock)();
            let task = RestTask {
                key: (u64::from(ticket.slot) << TASK_SLOT_SHIFT) | generation,
                kind: match operation {
                    QueryOperation::OrderStatus => 1,
                    QueryOperation::OrderTrades => 2,
                },
                max_query_count: 1,
                start_mono_ns: 0,
                end_mono_ns: worker_now.saturating_add(self.config.request_timeout_ns),
            };
            if !self.querier.enqueue(task) {
                self.clear_staged_slot(slot, generation);
                if let Some(res) = owner
                    .query_desk
                    .as_mut()
                    .and_then(|d| d.on_not_sent(&ticket, now_mono_ns))
                {
                    step.resolutions.push(res);
                }
                break;
            }
            self.inflight[slot] = Some(InflightQuery {
                ticket,
                epoch,
                operation,
                order_query,
                trade_query,
                probed_spot_new_cid,
            });
            self.issued_requests = self.issued_requests.saturating_add(1);
            step.dispatched += 1;
        }

        while spot_fallback_idx < spot_fallback_len {
            if let Some(rem_ticket) = spot_fallback_tickets[spot_fallback_idx].take()
                && let Some(res) = owner
                    .query_desk
                    .as_mut()
                    .and_then(|d| d.on_not_sent(&rem_ticket, now_mono_ns))
            {
                step.resolutions.push(res);
            }
            spot_fallback_idx += 1;
        }

        Ok(step)
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_reply<S: Store, B: Backend, A: Authority, E: Encoder>(
        &mut self,
        owner: &mut RestOwner<S, B, A, E>,
        inflight: InflightQuery,
        reply: RestReply,
        reply_mono_ns: u64,
        step: &mut PerOrderQueryStep,
        spot_fallback_tickets: &mut [Option<QueryTicket>; MAX_REST_SLOTS],
        spot_fallback_len: &mut usize,
    ) -> Result<()> {
        match reply.status {
            RestStatus::NotBuilt | RestStatus::Expired => {
                if let Some(res) = owner
                    .query_desk
                    .as_mut()
                    .and_then(|d| d.on_not_sent(&inflight.ticket, reply_mono_ns))
                {
                    step.resolutions.push(res);
                }
                return Ok(());
            }
            RestStatus::SendFail | RestStatus::ReadFail | RestStatus::Timeout => {
                self.record_transport_failure(owner, &inflight.ticket, reply_mono_ns, step);
                return Ok(());
            }
            RestStatus::Ok | RestStatus::HttpError => {}
        }

        let market = owner.ingress.market()?;
        let raw = encode_canonical_http_response(reply.http_code, &reply.body);
        if raw.len() > self.config.max_response_bytes
            || raw.len() > owner.ingress.config.max_raw_bytes
        {
            if matches!(reply.http_code, 401 | 403 | 418 | 429)
                || (200..300).contains(&reply.http_code)
            {
                owner.gateway.freeze_account();
                if let Some(d) = &mut owner.query_desk {
                    d.remove_order(&inflight.ticket.order);
                }
            } else {
                self.record_transport_failure(owner, &inflight.ticket, reply_mono_ns, step);
            }
            return Ok(());
        }

        let decoded_rest = response::decode_rest(market, reply.http_code, &reply.body);
        let is_cat_b_http = matches!(reply.http_code, 401 | 403 | 418 | 429)
            || matches!(
                decoded_rest.as_ref().ok().and_then(|d| d.failure),
                Some(
                    FailureKind::Banned
                        | FailureKind::RateLimited
                        | FailureKind::Authentication
                        | FailureKind::ClockSkew
                        | FailureKind::Partial
                )
            );
        if is_cat_b_http {
            match inflight.operation {
                QueryOperation::OrderStatus => {
                    if let Some(oq) = inflight.order_query {
                        let _ = owner.ingress.persist_order_query(
                            &mut owner.gateway,
                            oq,
                            inflight.epoch,
                            reply_mono_ns,
                            reply.http_code,
                            raw,
                            Some(&reply.body),
                        );
                    }
                }
                QueryOperation::OrderTrades => {
                    if let Some(tq) = inflight.trade_query {
                        let _ = owner.ingress.persist_per_order_trade_query(
                            &mut owner.gateway,
                            tq,
                            inflight.epoch,
                            reply_mono_ns,
                            reply.http_code,
                            raw,
                            None,
                        );
                    }
                }
            }
            owner.gateway.freeze_account();
            if let Some(d) = &mut owner.query_desk {
                d.remove_order(&inflight.ticket.order);
            }
            return Ok(());
        }

        if reply.http_code >= 500
            || reply.http_code == 408
            || matches!(
                decoded_rest.as_ref().ok().and_then(|d| d.failure),
                Some(FailureKind::Unknown)
            )
        {
            self.record_transport_failure(owner, &inflight.ticket, reply_mono_ns, step);
            return Ok(());
        }

        match inflight.operation {
            QueryOperation::OrderStatus => {
                let Some(oq) = inflight.order_query else {
                    owner.gateway.freeze_account();
                    return Ok(());
                };
                if let Ok(ref dec) = decoded_rest
                    && dec.failure == Some(FailureKind::NotFound)
                    && dec.error.as_ref().is_some_and(|e| e.code == -2013)
                {
                    if let Some((mod_intent, _)) = inflight.probed_spot_new_cid {
                        self.spot_probed_new_cid_missing
                            .insert((inflight.ticket.order.clone(), mod_intent));
                        if *spot_fallback_len < MAX_REST_SLOTS
                            && owner
                                .query_desk
                                .as_ref()
                                .is_some_and(|d| d.is_ticket_in_flight(&inflight.ticket))
                        {
                            spot_fallback_tickets[*spot_fallback_len] = Some(inflight.ticket);
                            *spot_fallback_len += 1;
                        } else if let Some(res) = owner
                            .query_desk
                            .as_mut()
                            .and_then(|d| d.on_not_sent(&inflight.ticket, reply_mono_ns))
                        {
                            step.resolutions.push(res);
                        }
                        return Ok(());
                    }
                    let order_confirmed = owner
                        .account()
                        .order(&inflight.ticket.order)
                        .is_some_and(|v| v.confirmed);
                    let res = owner.query_desk.as_mut().and_then(|d| {
                        d.on_not_found(&inflight.ticket, reply_mono_ns, order_confirmed)
                    });
                    if let Some(ref resolution) = res {
                        match resolution {
                            QueryResolution::FreezeAccount { .. } => {
                                owner.gateway.freeze_account();
                            }
                            QueryResolution::CloseUncreated { order, .. } => {
                                self.trade_next_from_id.remove(order);
                                self.expected_cumulative_atoms.remove(order);
                                let still_unconfirmed = owner
                                    .account()
                                    .order(order)
                                    .is_some_and(|v| !v.confirmed && !v.uncreated && !v.aborted);
                                if still_unconfirmed
                                    && owner
                                        .ingress
                                        .persist_uncreated_order_query(
                                            &mut owner.gateway,
                                            oq,
                                            inflight.epoch,
                                            reply_mono_ns,
                                            reply.http_code,
                                            raw,
                                            &reply.body,
                                        )
                                        .is_err()
                                {
                                    owner.gateway.freeze_account();
                                }
                            }
                            QueryResolution::FetchTradesBeforeClose { order, .. } => {
                                self.trade_next_from_id.entry(order.clone()).or_insert(0);
                                let _ = owner.gateway.isolate_intent_in_doubt(
                                    order.intent,
                                    DoubtReason::FillCatchup,
                                );
                            }
                            _ => {}
                        }
                    }
                    if let Some(r) = res {
                        step.resolutions.push(r);
                    }
                    return Ok(());
                }

                if reply.http_code != 200 {
                    owner.gateway.freeze_account();
                    if let Some(d) = &mut owner.query_desk {
                        d.remove_order(&inflight.ticket.order);
                    }
                    return Ok(());
                }
                if let Some((mod_intent, _)) = inflight.probed_spot_new_cid {
                    self.spot_probed_new_cid_missing
                        .remove(&(inflight.ticket.order.clone(), mod_intent));
                }

                let outcome = match owner.ingress.persist_order_query(
                    &mut owner.gateway,
                    oq,
                    inflight.epoch,
                    reply_mono_ns,
                    reply.http_code,
                    raw,
                    Some(&reply.body),
                ) {
                    Ok(out) => out,
                    Err(_) => {
                        owner.gateway.freeze_account();
                        if let Some(d) = &mut owner.query_desk {
                            d.remove_order(&inflight.ticket.order);
                        }
                        return Ok(());
                    }
                };
                if owner.account().frozen()
                    || !(1..=2).contains(&outcome.applied_events)
                    || !matches!(
                        outcome.disposition,
                        RestDisposition::Observed | RestDisposition::NeedsReconciliation
                    )
                {
                    owner.gateway.freeze_account();
                    if let Some(d) = &mut owner.query_desk {
                        d.remove_order(&inflight.ticket.order);
                    }
                    return Ok(());
                }

                let Some(view) = owner.account().order(&inflight.ticket.order) else {
                    owner.gateway.freeze_account();
                    return Ok(());
                };
                let obs_cum_atoms = owner
                    .ingress
                    .stored_rest()
                    .last()
                    .and_then(|s| {
                        s.events.iter().find_map(|ev| match &ev.event().body {
                            v::VenueEventBody::Order(o) => o.cumulative_quantity,
                            _ => None,
                        })
                    })
                    .and_then(|d| {
                        Fixed::parse(&d.as_plain_string(), view.filled_quantity.scale()).ok()
                    })
                    .map_or(view.filled_quantity.atoms(), |f| f.atoms());
                let prev_expected = self
                    .expected_cumulative_atoms
                    .get(&inflight.ticket.order)
                    .copied()
                    .unwrap_or(0);
                let mut effective_cum_atoms = obs_cum_atoms.max(prev_expected);
                if (view.terminal && view.remaining_quantity.atoms() > 0)
                    || owner.account().order_in_doubt(&inflight.ticket.order)
                        == Some(DoubtReason::FillCatchup)
                {
                    effective_cum_atoms =
                        effective_cum_atoms.max(view.filled_quantity.atoms().saturating_add(1));
                }
                if effective_cum_atoms > 0 {
                    self.expected_cumulative_atoms
                        .insert(inflight.ticket.order.clone(), effective_cum_atoms);
                }

                let amend_resolved = !owner.account().amendments().any(|a| {
                    a.order == inflight.ticket.order
                        && !a.confirmed
                        && a.rejected.is_none()
                        && !a.aborted
                }) && !crate::modify::has_unconfirmed_modify(
                    &owner.gateway,
                    &inflight.ticket.order,
                );
                let snap = SnapshotSummary {
                    confirmed: view.confirmed,
                    terminal: view.terminal,
                    cumulative_atoms: effective_cum_atoms,
                    local_filled_atoms: view.filled_quantity.atoms(),
                    amend_resolved,
                };
                let res = owner
                    .query_desk
                    .as_mut()
                    .and_then(|d| d.on_status_snapshot(&inflight.ticket, reply_mono_ns, snap));
                if let Some(ref resolution) = res {
                    match resolution {
                        QueryResolution::FetchTradesBeforeClose { order, .. } => {
                            self.trade_next_from_id.entry(order.clone()).or_insert(0);
                            if view.remaining_quantity.atoms() > 0
                                && (view.terminal
                                    || owner.account().order_in_doubt(order)
                                        != Some(DoubtReason::CancelAmbiguous))
                            {
                                let _ = owner.gateway.isolate_intent_in_doubt(
                                    order.intent,
                                    DoubtReason::FillCatchup,
                                );
                            }
                        }
                        QueryResolution::CancelRejectedStillOpen {
                            order,
                            remaining_pending_cancels: 0,
                        } => {
                            if !view.terminal
                                && owner.account().order_in_doubt(order)
                                    == Some(DoubtReason::CancelAmbiguous)
                                && owner.gateway.clear_order_in_doubt(order).is_err()
                            {
                                owner.gateway.freeze_account();
                            }
                        }
                        QueryResolution::Resolved { order } => {
                            if view.terminal && !view.fee_reconciled {
                                if owner
                                    .account()
                                    .check_fee_closure(order, view.filled_quantity)
                                    == Ok(tickwire_account::Applied::New)
                                {
                                    if owner
                                        .gateway
                                        .record_fact(RecoveryRecord::FeesReconciled {
                                            order: order.clone(),
                                            cumulative: view.filled_quantity,
                                        })
                                        .is_err()
                                    {
                                        owner.gateway.freeze_account();
                                    }
                                } else {
                                    self.trade_next_from_id.insert(order.clone(), 0);
                                    let native =
                                        owner.ingress.registry.get(&order.intent).and_then(|reg| {
                                            reg.orders
                                                .iter()
                                                .find(|o| {
                                                    o.client_id.as_ref() == Some(&order.client_id)
                                                })
                                                .and_then(|o| o.venue_order_id.clone())
                                        });
                                    let account_ref = owner.gateway.account();
                                    if let Some(desk) = &mut owner.query_desk
                                        && desk
                                            .enqueue_ask(
                                                order.clone(),
                                                inflight.ticket.instrument_id,
                                                native,
                                                AskKind::Trades,
                                                false,
                                                reply_mono_ns,
                                                |k| account_ref.order_in_doubt(k).is_some(),
                                            )
                                            .is_err()
                                    {
                                        owner.gateway.freeze_account();
                                    }
                                }
                            }
                        }
                        QueryResolution::FreezeAccount { .. } => {
                            owner.gateway.freeze_account();
                        }
                        _ => {}
                    }
                }
                if let Some(r) = res {
                    step.resolutions.push(r);
                }
                Ok(())
            }
            QueryOperation::OrderTrades => {
                let Some(tq) = inflight.trade_query else {
                    owner.gateway.freeze_account();
                    return Ok(());
                };
                if reply.http_code != 200 {
                    owner.gateway.freeze_account();
                    if let Some(d) = &mut owner.query_desk {
                        d.remove_order(&inflight.ticket.order);
                    }
                    return Ok(());
                }
                let (outcome, returned, next_from_id) =
                    match owner.ingress.persist_per_order_trade_query(
                        &mut owner.gateway,
                        tq,
                        inflight.epoch,
                        reply_mono_ns,
                        reply.http_code,
                        raw,
                        Some(&reply.body),
                    ) {
                        Ok(res) => res,
                        Err(_) => {
                            owner.gateway.freeze_account();
                            if let Some(d) = &mut owner.query_desk {
                                d.remove_order(&inflight.ticket.order);
                            }
                            return Ok(());
                        }
                    };
                if owner.account().frozen()
                    || outcome.disposition != RestDisposition::NeedsReconciliation
                {
                    owner.gateway.freeze_account();
                    if let Some(d) = &mut owner.query_desk {
                        d.remove_order(&inflight.ticket.order);
                    }
                    return Ok(());
                }

                let page_exhausted = returned < tq.limit as usize;
                let Some(view) = owner.account().order(&inflight.ticket.order) else {
                    owner.gateway.freeze_account();
                    return Ok(());
                };
                let all_caught_up = if !page_exhausted {
                    if let Some(next_id) = next_from_id {
                        self.trade_next_from_id
                            .insert(inflight.ticket.order.clone(), next_id);
                    }
                    false
                } else if view.terminal {
                    if owner
                        .account()
                        .check_fee_closure(&inflight.ticket.order, view.filled_quantity)
                        .is_ok()
                    {
                        if !view.fee_reconciled
                            && owner
                                .gateway
                                .record_fact(RecoveryRecord::FeesReconciled {
                                    order: inflight.ticket.order.clone(),
                                    cumulative: view.filled_quantity,
                                })
                                .is_err()
                        {
                            owner.gateway.freeze_account();
                            return Ok(());
                        }
                        self.trade_next_from_id.remove(&inflight.ticket.order);
                        self.expected_cumulative_atoms
                            .remove(&inflight.ticket.order);
                        true
                    } else {
                        self.trade_next_from_id
                            .insert(inflight.ticket.order.clone(), 0);
                        false
                    }
                } else {
                    let expected = self
                        .expected_cumulative_atoms
                        .get(&inflight.ticket.order)
                        .copied()
                        .unwrap_or(0);
                    let filled_matches =
                        view.filled_quantity.atoms() >= expected && (expected > 0 || returned > 0);
                    if filled_matches
                        && owner.account().order_in_doubt(&inflight.ticket.order)
                            == Some(DoubtReason::FillCatchup)
                    {
                        let _ = owner.gateway.clear_order_in_doubt(&inflight.ticket.order);
                    }
                    if filled_matches
                        && owner.account().order_in_doubt(&inflight.ticket.order)
                            != Some(DoubtReason::FillCatchup)
                    {
                        self.trade_next_from_id.remove(&inflight.ticket.order);
                        self.expected_cumulative_atoms
                            .remove(&inflight.ticket.order);
                        true
                    } else {
                        if let Some(next_id) = next_from_id {
                            self.trade_next_from_id
                                .insert(inflight.ticket.order.clone(), next_id);
                        }
                        false
                    }
                };

                if let Some(res) = owner.query_desk.as_mut().and_then(|d| {
                    d.on_trades_applied(&inflight.ticket, reply_mono_ns, all_caught_up)
                }) {
                    if let QueryResolution::RetryScheduled { ref order, .. } = res {
                        if !view.terminal
                            && owner.query_desk.as_ref().and_then(|d| d.ask_kind(order))
                                == Some(AskKind::Cancel)
                        {
                            let _ = owner.gateway.isolate_intent_in_doubt(
                                order.intent,
                                DoubtReason::CancelAmbiguous,
                            );
                        }
                    } else if let QueryResolution::Resolved { ref order } = res
                        && !view.terminal
                        && owner.account().order_in_doubt(order)
                            == Some(DoubtReason::CancelAmbiguous)
                        && owner.gateway.clear_order_in_doubt(order).is_err()
                    {
                        owner.gateway.freeze_account();
                    }
                    step.resolutions.push(res);
                }
                Ok(())
            }
        }
    }

    fn record_transport_failure<S: Store, B: Backend, A: Authority, E: Encoder>(
        &self,
        owner: &mut RestOwner<S, B, A, E>,
        ticket: &QueryTicket,
        reply_mono_ns: u64,
        step: &mut PerOrderQueryStep,
    ) {
        if let Some(res) = owner
            .query_desk
            .as_mut()
            .and_then(|d| d.on_transport_failure(ticket, reply_mono_ns))
        {
            if matches!(res, QueryResolution::FreezeAccount { .. }) {
                owner.gateway.freeze_account();
            }
            step.resolutions.push(res);
        }
    }
}

fn should_query_order(account: &Account, order: &OrderKey, kind: AskKind) -> bool {
    let Some(view) = account.order(order) else {
        return false;
    };
    if view.aborted || view.uncreated {
        return false;
    }
    if kind == AskKind::Trades {
        account.order_in_doubt(order) == Some(DoubtReason::FillCatchup)
            || (view.terminal && !view.fee_reconciled)
    } else {
        account.order_in_doubt(order).is_some() || (!view.terminal && kind == AskKind::Cancel)
    }
}

fn format_recv_window(recv_window_micros: u64) -> String {
    if recv_window_micros.is_multiple_of(1000) {
        (recv_window_micros / 1000).to_string()
    } else {
        format!(
            "{}.{:03}",
            recv_window_micros / 1000,
            recv_window_micros % 1000
        )
        .trim_end_matches('0')
        .to_owned()
    }
}

fn validate_api_key(credentials: &Credentials<'_>) -> Result<()> {
    if credentials.api_key.is_empty()
        || credentials.api_key.len() > 256
        || !credentials
            .api_key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
    {
        return Err(Error::Identity);
    }
    Ok(())
}

fn sign_query_string(
    market: Market,
    credentials: &Credentials<'_>,
    preimage: &[u8],
) -> Result<String> {
    let endpoint = if market == Market::Spot {
        Endpoint::SpotRest
    } else {
        Endpoint::UsdmRest
    };
    let mut signature = [0u8; MAX_SIGNATURE];
    let size = credentials
        .signing_key
        .sign(endpoint, preimage, &mut signature)
        .map_err(|_| Error::Encoding)?;
    let mut signed = String::from_utf8(preimage.to_vec()).map_err(|_| Error::Encoding)?;
    signed.push_str("&signature=");
    for b in &signature[..size] {
        if b.is_ascii_alphanumeric() || b"-._~".contains(b) {
            signed.push(*b as char);
        } else {
            signed.push_str(&format!("%{b:02X}"));
        }
    }
    Ok(signed)
}

fn build_order_status_request(
    config: &PerOrderQueryConfig,
    command: &v::CanonicalCommand,
    identity: &v::OrderIdentity,
    override_client_id: Option<&v::ClientId>,
    credentials: &Credentials<'_>,
    stamp: SignedTime,
) -> Result<BuiltRequest> {
    validate_api_key(credentials)?;
    if config.scope.market == Market::Usdm && stamp.unit() != tickwire_clock::TimeUnit::Milliseconds
    {
        return Err(Error::Identity);
    }
    let window = format_recv_window(config.recv_window_micros);
    let mut params = Params::<4>::new();
    params
        .push(
            "symbol",
            Value::String(command.command().context.instrument.symbol.as_str()),
        )
        .map_err(|_| Error::Encoding)?;
    if let Some(override_cid) = override_client_id {
        params
            .push("origClientOrderId", Value::String(override_cid.as_str()))
            .map_err(|_| Error::Encoding)?;
    } else if let Some(native) = &identity.venue_order_id {
        let order = native
            .as_str()
            .parse::<u64>()
            .map_err(|_| Error::Identity)?;
        if order > i64::MAX as u64 {
            return Err(Error::Identity);
        }
        params
            .push("orderId", Value::Integer(order))
            .map_err(|_| Error::Encoding)?;
    } else {
        let cid = identity.client_id.as_ref().ok_or(Error::Identity)?;
        params
            .push("origClientOrderId", Value::String(cid.as_str()))
            .map_err(|_| Error::Encoding)?;
    }
    params
        .push("timestamp", Value::Integer(stamp.timestamp()))
        .map_err(|_| Error::Encoding)?;
    params
        .push("recvWindow", Value::Number(&window))
        .map_err(|_| Error::Encoding)?;
    let mut preimage = [0u8; 8192];
    let n = params.rest(&mut preimage).map_err(|_| Error::Encoding)?;
    let signed = sign_query_string(config.scope.market, credentials, &preimage[..n])?;
    let endpoint = if config.scope.market == Market::Spot {
        "/api/v3/order"
    } else {
        "/fapi/v1/order"
    };
    Ok(BuiltRequest {
        method: "GET".to_owned(),
        path: format!("{endpoint}?{signed}"),
        headers: vec![
            ("Host".to_owned(), authority(&config.scope).to_owned()),
            ("X-MBX-APIKEY".to_owned(), credentials.api_key.to_owned()),
        ],
        body: Vec::new(),
    })
}

fn build_order_trades_request(
    config: &PerOrderQueryConfig,
    command: &v::CanonicalCommand,
    identity: &v::OrderIdentity,
    query: TradeQuery,
    credentials: &Credentials<'_>,
    stamp: SignedTime,
) -> Result<BuiltRequest> {
    validate_api_key(credentials)?;
    if config.scope.market == Market::Usdm && stamp.unit() != tickwire_clock::TimeUnit::Milliseconds
    {
        return Err(Error::Identity);
    }
    let order = identity
        .venue_order_id
        .as_ref()
        .ok_or(Error::Identity)?
        .as_str()
        .parse::<u64>()
        .map_err(|_| Error::Identity)?;
    if order > i64::MAX as u64 {
        return Err(Error::Identity);
    }
    let window = format_recv_window(config.recv_window_micros);
    let mut params = Params::<8>::new();
    params
        .push(
            "symbol",
            Value::String(command.command().context.instrument.symbol.as_str()),
        )
        .map_err(|_| Error::Encoding)?;
    for (key, value) in [
        ("orderId", order),
        ("fromId", query.from_id),
        ("limit", u64::from(query.limit)),
        ("timestamp", stamp.timestamp()),
    ] {
        params
            .push(key, Value::Integer(value))
            .map_err(|_| Error::Encoding)?;
    }
    params
        .push("recvWindow", Value::Number(&window))
        .map_err(|_| Error::Encoding)?;
    let mut preimage = [0u8; 8192];
    let n = params.rest(&mut preimage).map_err(|_| Error::Encoding)?;
    let signed = sign_query_string(config.scope.market, credentials, &preimage[..n])?;
    let endpoint = if config.scope.market == Market::Spot {
        "/api/v3/myTrades"
    } else {
        "/fapi/v1/userTrades"
    };
    Ok(BuiltRequest {
        method: "GET".to_owned(),
        path: format!("{endpoint}?{signed}"),
        headers: vec![
            ("Host".to_owned(), authority(&config.scope).to_owned()),
            ("X-MBX-APIKEY".to_owned(), credentials.api_key.to_owned()),
        ],
        body: Vec::new(),
    })
}

fn http_reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        408 => "Request Timeout",
        409 => "Conflict",
        418 => "I'm a teapot",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Response",
    }
}

fn encode_canonical_http_response(status: u16, body: &[u8]) -> Vec<u8> {
    let head = format!(
        "HTTP/1.1 {status} {}\r\nContent-Length: {}\r\n\r\n",
        http_reason_phrase(status),
        body.len()
    );
    let mut out = Vec::with_capacity(head.len() + body.len());
    out.extend_from_slice(head.as_bytes());
    out.extend_from_slice(body);
    out
}
