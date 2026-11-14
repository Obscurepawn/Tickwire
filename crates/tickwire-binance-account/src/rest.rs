//! Single and pooled HTTP/1.1 REST response owner sharing the durable private-input journal.
use crate::*;
use std::{
    collections::VecDeque,
    net::{IpAddr, SocketAddr},
    time::Instant,
};
use tickwire_account_gateway::{
    AccountGateway, Authority, Encoder, Status,
    pool::{MAX_ORDER_CONNECTIONS, OrderSlotSpec, RouteHealth, RouterConfig, SampleKind},
};
use tickwire_account_journal::Backend;
use tickwire_binance_private::{GapNotice, PrivateEvent};
use tickwire_quota::rate_limiter::{IpKey, RateReject, VenueRateLimiter};
use tickwire_transport::{IoBudget, Limits, http::ResponseDecoder};
pub(crate) mod amend_history;
#[cfg(test)]
mod amend_tests;
mod amendment;
pub(crate) mod binding;
pub(crate) mod persistence;
pub(crate) mod structure;
#[cfg(test)]
mod tests;

pub const POOL_MIO_TOKEN_BASE: usize = 100;

#[derive(Clone, Copy, Debug)]
pub(crate) enum QuotaAction {
    Place {
        symbol: u64,
        order_units: u32,
        weight: u32,
    },
    Modify {
        symbol: u64,
        weight: u32,
        counts_as_order: bool,
    },
    Cancel {
        symbol: u64,
        weight: u32,
    },
    Query {
        weight: u32,
    },
}

fn parse_ascii_u64(bytes: &[u8]) -> Option<u64> {
    let s = std::str::from_utf8(bytes).ok()?.trim();
    s.parse::<u64>().ok()
}

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
    pub structure_query: Option<crate::reconciliation::structures::StructureQuery>,
    pub amendment_query: Option<crate::reconciliation::amendments::AmendmentQuery>,
    pub structure_domain: Option<tickwire_clock::ClockDomain>,
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
    pub structure_query: Option<crate::reconciliation::structures::StructureQuery>,
    pub amendment_query: Option<crate::reconciliation::amendments::AmendmentQuery>,
    pub structure_domain: Option<tickwire_clock::ClockDomain>,
    pub connection_epoch: u64,
    pub received_mono_ns: u64,
    pub status: u16,
    pub raw: &'a [u8],
    pub disposition: RestDisposition,
    pub events: &'a [CanonicalEvent],
}
pub(crate) struct Pending {
    pub(crate) command: CanonicalCommand,
    pub(crate) deadline: Instant,
}
pub(crate) struct SlotPending {
    pub(crate) command: CanonicalCommand,
    pub(crate) deadline: Instant,
    pub(crate) sent_mono_ns: u64,
    pub(crate) slot_epoch: u64,
    pub(crate) connection_epoch: u64,
}
pub(crate) struct RestSlotState {
    decoder: ResponseDecoder,
    raw: Vec<u8>,
    pub(crate) pending: Option<SlotPending>,
    pub(crate) connection_epoch: u64,
    pub(crate) last_router_epoch: u64,
    pub(crate) configured_local_ip: Option<IpAddr>,
    pub(crate) registered_mio_epoch: Option<u64>,
}
pub struct RestOwner<S: Store, B: Backend, A: Authority, E: Encoder> {
    pub(crate) ingress: AccountIngress<S>,
    pub(crate) gateway: AccountGateway<B, A, E>,
    pub(crate) config: RestConfig,
    decoder: ResponseDecoder,
    raw: Vec<u8>,
    pub(crate) pending: Option<Pending>,
    pub(crate) slot_states: [Option<RestSlotState>; MAX_ORDER_CONNECTIONS],
    pub(crate) completed_outcomes: VecDeque<RestOutcome>,
    pub(crate) venue_limiter: Option<VenueRateLimiter>,
    single_registered_mio: bool,
    pub(crate) websocket: Option<crate::ws::owner::Runtime>,
    pub(crate) query_desk: Option<tickwire_account::QueryDesk>,
    last_mono: Option<u64>,
}
impl<S: Store, B: Backend, A: Authority, E: Encoder> RestOwner<S, B, A, E> {
    pub fn execution_connection_epoch(&self) -> u64 {
        self.config.connection_epoch
    }
    pub fn with_connection_pool(mut self, config: RouterConfig) -> Result<Self> {
        self.enable_connection_pool(config)?;
        Ok(self)
    }
    pub fn enable_connection_pool(&mut self, config: RouterConfig) -> Result<()> {
        if self.pending.is_some() || self.websocket.is_some() {
            return Err(Error::Busy);
        }
        self.gateway
            .enable_connection_pool(config)
            .map_err(Error::Gateway)?;
        self.slot_states = std::array::from_fn(|_| None);
        self.completed_outcomes.clear();
        self.single_registered_mio = false;
        Ok(())
    }
    pub fn has_connection_pool(&self) -> bool {
        self.gateway.has_connection_pool()
    }
    pub fn add_pool_slot(&mut self, spec: OrderSlotSpec, limits: Limits) -> Result<u16> {
        let configured_local_ip = spec.local_ip;
        let decoder = ResponseDecoder::new(self.config.max_response_bytes, self.config.max_headers)
            .map_err(Error::Transport)?;
        let conn_id = self
            .gateway
            .add_pool_slot(spec, limits)
            .map_err(Error::Gateway)?;
        let router_epoch = self.gateway.slot_route(conn_id).map_or(0, |r| r.epoch);
        let slot_conn_epoch = if router_epoch > 0 {
            let next = self
                .config
                .connection_epoch
                .checked_add(1)
                .ok_or(Error::Capacity)?;
            self.config.connection_epoch = next;
            next
        } else {
            self.config.connection_epoch
        };
        self.slot_states[usize::from(conn_id)] = Some(RestSlotState {
            decoder,
            raw: Vec::new(),
            pending: None,
            connection_epoch: slot_conn_epoch,
            last_router_epoch: router_epoch,
            configured_local_ip,
            registered_mio_epoch: None,
        });
        Ok(conn_id)
    }
    pub fn install_pool_connection(
        &mut self,
        conn_id: u16,
        connection: tickwire_transport::Connection,
        peer_addr: Option<SocketAddr>,
        limits: Limits,
    ) -> Result<u64> {
        let idx = usize::from(conn_id);
        if idx >= MAX_ORDER_CONNECTIONS {
            return Err(Error::Transport(tickwire_transport::Error::Bounds));
        }
        if self.slot_states[idx]
            .as_ref()
            .is_some_and(|s| s.pending.is_some())
        {
            return Err(Error::Busy);
        }
        if self.slot_states[idx].is_none() {
            return Err(Error::Transport(tickwire_transport::Error::Bounds));
        }
        let next_conn_epoch = self
            .config
            .connection_epoch
            .checked_add(1)
            .ok_or(Error::Capacity)?;
        let decoder = ResponseDecoder::new(self.config.max_response_bytes, self.config.max_headers)
            .map_err(Error::Transport)?;
        let router_epoch = self
            .gateway
            .install_pool_connection(conn_id, connection, peer_addr, limits)
            .map_err(Error::Gateway)?;
        self.config.connection_epoch = next_conn_epoch;
        let slot = self.slot_states[idx].as_mut().unwrap();
        slot.decoder = decoder;
        slot.raw.clear();
        slot.pending = None;
        slot.connection_epoch = next_conn_epoch;
        slot.last_router_epoch = router_epoch;
        slot.registered_mio_epoch = None;
        Ok(router_epoch)
    }
    pub fn poll_pool_dials(&mut self, now_mono_ns: u64) -> usize {
        let slot_unbusy: [bool; MAX_ORDER_CONNECTIONS] = std::array::from_fn(|idx| {
            self.slot_states[idx]
                .as_ref()
                .is_some_and(|slot| slot.pending.is_none())
        });
        let installed = self
            .gateway
            .poll_pool_dials_filtered(now_mono_ns, |cid| slot_unbusy[usize::from(cid)]);
        if installed > 0 {
            for idx in 0..MAX_ORDER_CONNECTIONS {
                let conn_id = idx as u16;
                let has_conn = self.gateway.has_slot_connection(conn_id);
                let router_epoch = self.gateway.slot_route(conn_id).map_or(0, |r| r.epoch);
                if let Some(slot) = self.slot_states[idx].as_mut()
                    && slot.pending.is_none()
                    && has_conn
                    && router_epoch != slot.last_router_epoch
                {
                    if let Ok(dec) = ResponseDecoder::new(
                        self.config.max_response_bytes,
                        self.config.max_headers,
                    ) {
                        slot.decoder = dec;
                    }
                    slot.raw.clear();
                    slot.last_router_epoch = router_epoch;
                    self.config.connection_epoch =
                        self.config.connection_epoch.saturating_add(1).max(1);
                    slot.connection_epoch = self.config.connection_epoch;
                    slot.registered_mio_epoch = None;
                }
            }
        }
        installed
    }
    pub fn has_ready_pool_slot(&self) -> bool {
        if self.gateway.halted() || !self.gateway.has_connection_pool() {
            return false;
        }
        for idx in 0..MAX_ORDER_CONNECTIONS {
            let conn_id = idx as u16;
            let Some(slot) = self.slot_states[idx].as_ref() else {
                continue;
            };
            if slot.pending.is_some()
                || !self.gateway.has_slot_connection(conn_id)
                || self.gateway.slot_pending_intent(conn_id).is_some()
            {
                continue;
            }
            if self
                .gateway
                .slot_route(conn_id)
                .is_some_and(|r| r.established && r.is_ready() && r.matches_product(0))
            {
                return true;
            }
        }
        false
    }
    pub fn has_queued_outcomes(&self) -> bool {
        !self.completed_outcomes.is_empty()
            || self
                .websocket
                .as_ref()
                .is_some_and(|w| !w.completed_ws_outcomes.is_empty())
    }
    pub fn with_venue_rate_limiter(mut self, limiter: VenueRateLimiter) -> Self {
        self.venue_limiter = Some(limiter);
        self
    }
    pub fn set_venue_rate_limiter(&mut self, limiter: VenueRateLimiter) {
        self.venue_limiter = Some(limiter);
    }
    pub fn venue_rate_limiter(&self) -> Option<&VenueRateLimiter> {
        self.venue_limiter.as_ref()
    }
    pub fn venue_rate_limiter_mut(&mut self) -> Option<&mut VenueRateLimiter> {
        self.venue_limiter.as_mut()
    }
    pub fn websocket_route_health(&self) -> Option<RouteHealth> {
        self.websocket.as_ref().map(|w| w.route_health)
    }
    pub fn websocket_inflight_count(&self) -> usize {
        self.websocket.as_ref().map_or(0, |w| w.inflight.len())
    }
    pub fn with_per_order_query_desk(mut self, config: tickwire_account::QueryConfig) -> Self {
        self.enable_per_order_query_desk(config);
        self
    }
    pub fn enable_per_order_query_desk(&mut self, config: tickwire_account::QueryConfig) {
        self.gateway.set_per_order_doubt_isolation(true);
        let mut desk = tickwire_account::QueryDesk::new(config);
        let now_mono = self.last_mono.unwrap_or(0);
        let existing: Vec<(tickwire_account::OrderKey, tickwire_account::DoubtReason)> = self
            .gateway
            .account()
            .in_doubt_orders()
            .map(|(k, r)| (k.clone(), r))
            .collect();
        for (order_key, reason) in existing {
            if let Some(reg) = self.ingress.registry.get(&order_key.intent) {
                let instrument_id = reg.command.command().context.instrument.instrument_id;
                let native = reg
                    .orders
                    .iter()
                    .find(|o| o.client_id.as_ref() == Some(&order_key.client_id))
                    .and_then(|o| o.venue_order_id.clone());
                let confirmed = self
                    .gateway
                    .account()
                    .order(&order_key)
                    .is_some_and(|v| v.confirmed);
                let (kind, place_unacked) = match reason {
                    tickwire_account::DoubtReason::PlaceAmbiguous => {
                        (tickwire_account::AskKind::Place, !confirmed)
                    }
                    tickwire_account::DoubtReason::CancelAmbiguous => {
                        (tickwire_account::AskKind::Cancel, !confirmed)
                    }
                    tickwire_account::DoubtReason::AmendAmbiguous => {
                        (tickwire_account::AskKind::Amend, !confirmed)
                    }
                    tickwire_account::DoubtReason::FillCatchup => {
                        (tickwire_account::AskKind::Trades, false)
                    }
                };
                let _ = desk.enqueue_ask(
                    order_key,
                    instrument_id,
                    native,
                    kind,
                    place_unacked,
                    now_mono,
                    |k| self.gateway.account().order_in_doubt(k).is_some(),
                );
            }
        }
        self.query_desk = Some(desk);
    }
    pub fn query_desk(&self) -> Option<&tickwire_account::QueryDesk> {
        self.query_desk.as_ref()
    }
    pub fn query_desk_mut(&mut self) -> Option<&mut tickwire_account::QueryDesk> {
        self.query_desk.as_mut()
    }
    pub fn install_transport_connection(
        &mut self,
        connection: tickwire_transport::Connection,
        limits: tickwire_transport::Limits,
    ) -> Result<()> {
        if self.pending.is_some() || self.websocket.is_some() || self.gateway.has_connection_pool()
        {
            return Err(Error::Busy);
        }
        let next_epoch = self
            .config
            .connection_epoch
            .checked_add(1)
            .ok_or(Error::Capacity)?;
        let decoder = ResponseDecoder::new(self.config.max_response_bytes, self.config.max_headers)
            .map_err(Error::Transport)?;
        self.gateway
            .install_transport_connection(connection, limits)
            .map_err(Error::Gateway)?;
        self.decoder = decoder;
        self.raw.clear();
        self.config.connection_epoch = next_epoch;
        self.single_registered_mio = false;
        Ok(())
    }
    pub fn with_retention_window_ns(mut self, window_ns: Option<u64>) -> Self {
        self.set_retention_window_ns(window_ns);
        self
    }
    pub fn set_retention_window_ns(&mut self, window_ns: Option<u64>) {
        self.ingress.set_retention_window_ns(window_ns);
        self.gateway.set_retention_window_ns(window_ns);
    }
    pub fn retention_window_ns(&self) -> Option<u64> {
        self.ingress.retention_window_ns()
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
            slot_states: std::array::from_fn(|_| None),
            completed_outcomes: VecDeque::new(),
            venue_limiter: None,
            single_registered_mio: false,
            websocket: None,
            query_desk: None,
            last_mono: None,
        })
    }
    pub(crate) fn classify_quota_action(
        &self,
        command: &CanonicalCommand,
        is_ws: bool,
    ) -> Result<QuotaAction> {
        let is_usdm = matches!(
            self.ingress.market()?,
            tickwire_binance::request::Market::Usdm
        );
        let usdm_ws_zero = is_ws
            && self.venue_limiter.as_ref().is_some_and(|l| {
                l.preset() == tickwire_quota::rate_limiter::VenuePreset::BinanceUsdm
            });
        let cmd_weight = if usdm_ws_zero { 0 } else { 1 };
        let symbol = command.command().context.instrument.instrument_id.max(1);
        match &command.command().body {
            v::CommandBody::Cancel(_)
            | v::CommandBody::CancelList(_)
            | v::CommandBody::CancelAlgo(_) => Ok(QuotaAction::Cancel {
                symbol,
                weight: cmd_weight,
            }),
            v::CommandBody::Query(_) => Ok(QuotaAction::Query {
                weight: if is_usdm { 1 } else { 4 },
            }),
            v::CommandBody::AmendKeepPriority { .. } => Ok(QuotaAction::Modify {
                symbol,
                weight: if is_usdm { cmd_weight } else { 4 },
                counts_as_order: is_usdm,
            }),
            v::CommandBody::Modify(_) => Ok(QuotaAction::Modify {
                symbol,
                weight: cmd_weight,
                counts_as_order: true,
            }),
            v::CommandBody::SpotList(list) => Ok(QuotaAction::Place {
                symbol,
                order_units: (list.legs.as_slice().len() as u32).max(1),
                weight: cmd_weight,
            }),
            v::CommandBody::SpotAdvanced(v::SpotAdvancedCommand::List(list)) => {
                Ok(QuotaAction::Place {
                    symbol,
                    order_units: (list.legs.as_slice().len() as u32).max(1),
                    weight: cmd_weight,
                })
            }
            v::CommandBody::New(_)
            | v::CommandBody::CancelReplace { .. }
            | v::CommandBody::Algo(_)
            | v::CommandBody::SpotAdvanced(
                v::SpotAdvancedCommand::New(_)
                | v::SpotAdvancedCommand::CancelReplace { .. }
                | v::SpotAdvancedCommand::SorNew(_),
            ) => Ok(QuotaAction::Place {
                symbol,
                order_units: 1,
                weight: cmd_weight,
            }),
        }
    }
    pub(crate) fn can_afford_quota_on_ip(
        &self,
        ip_key: IpKey,
        action: QuotaAction,
        now_ns: u64,
    ) -> core::result::Result<(), RateReject> {
        let Some(limiter) = self.venue_limiter.as_ref() else {
            return Ok(());
        };
        can_afford_quota_with_limiter(limiter, ip_key, action, now_ns)
    }
    pub(crate) fn allow_quota_on_ip(
        &mut self,
        ip_key: IpKey,
        action: QuotaAction,
        now_ns: u64,
    ) -> core::result::Result<(), RateReject> {
        let Some(limiter) = self.venue_limiter.as_mut() else {
            return Ok(());
        };
        allow_quota_with_limiter(limiter, ip_key, action, now_ns)
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
        if self.websocket.is_some() {
            return self.submit_websocket(command, order_ids, evaluated_at, deadline);
        }
        if self.gateway.has_connection_pool() {
            return self.submit_pooled_rest(command, order_ids, evaluated_at, deadline);
        }
        if self.pending.is_some() {
            return Err(Error::Busy);
        }
        let pending_quota = if let Some(limiter) = &self.venue_limiter {
            self.ingress.ready(&self.gateway)?;
            let action = self.classify_quota_action(&command, false)?;
            let ip_key = limiter.local_ips().first().copied().unwrap_or(IpKey::NONE);
            can_afford_quota_with_limiter(limiter, ip_key, action, evaluated_at)
                .map_err(Error::RateLimited)?;
            Some((ip_key, action))
        } else {
            None
        };
        let status = self.ingress.submit(
            &mut self.gateway,
            command.clone(),
            order_ids,
            evaluated_at,
            deadline,
        )?;
        if status != Status::Aborted {
            if let Some((ip_key, action)) = pending_quota {
                self.allow_quota_on_ip(ip_key, action, evaluated_at)
                    .map_err(Error::RateLimited)?;
            }
            self.pending = Some(Pending { command, deadline });
        }
        Ok(status)
    }
    fn submit_pooled_rest(
        &mut self,
        command: CanonicalCommand,
        order_ids: &[[u8; 16]],
        evaluated_at: u64,
        deadline: Instant,
    ) -> Result<Status> {
        if self.gateway.halted() {
            return Err(Error::Gateway(tickwire_account_gateway::Error::Halted));
        }
        self.ingress.ready(&self.gateway)?;
        match command.command().body {
            v::CommandBody::New(_)
            | v::CommandBody::CancelReplace { .. }
            | v::CommandBody::SpotList(_)
            | v::CommandBody::Algo(_) => {}
            v::CommandBody::Cancel(_)
            | v::CommandBody::Query(_)
            | v::CommandBody::CancelList(_)
            | v::CommandBody::CancelAlgo(_)
            | v::CommandBody::AmendKeepPriority { .. }
                if order_ids.is_empty() => {}
            _ => return Err(Error::Unsupported),
        }
        if !self.has_ready_pool_slot() {
            return Err(Error::Busy);
        }
        let (conn_id, pending_quota) = if let Some(limiter) = &self.venue_limiter {
            let action = self.classify_quota_action(&command, false)?;
            let mut configured_ips = [None; MAX_ORDER_CONNECTIONS];
            let mut slot_busy = [true; MAX_ORDER_CONNECTIONS];
            for idx in 0..MAX_ORDER_CONNECTIONS {
                if let Some(slot) = self.slot_states[idx].as_ref() {
                    configured_ips[idx] = slot.configured_local_ip;
                    slot_busy[idx] = slot.pending.is_some();
                }
            }
            let mut first_reject: Option<RateReject> = None;
            let picked = self.gateway.pick_pool_slot_filtered(0, |route| {
                let idx = usize::from(route.conn_id);
                if idx >= MAX_ORDER_CONNECTIONS || slot_busy[idx] {
                    return false;
                }
                let ip_key = resolve_slot_ip_key(configured_ips[idx], route.local_ip, limiter);
                match can_afford_quota_with_limiter(limiter, ip_key, action, evaluated_at) {
                    Ok(()) => true,
                    Err(rej) => {
                        if first_reject.is_none() {
                            first_reject = Some(rej);
                        }
                        false
                    }
                }
            });
            let Some(cid) = picked else {
                if let Some(rej) = first_reject {
                    return Err(Error::RateLimited(rej));
                }
                return Err(Error::Busy);
            };
            let route_ip = self.gateway.slot_route(cid).and_then(|r| r.local_ip);
            let chosen_ip =
                resolve_slot_ip_key(configured_ips[usize::from(cid)], route_ip, limiter);
            (cid, Some((chosen_ip, action)))
        } else {
            let slot_busy: [bool; MAX_ORDER_CONNECTIONS] = std::array::from_fn(|idx| {
                self.slot_states[idx]
                    .as_ref()
                    .is_none_or(|slot| slot.pending.is_some())
            });
            let cid = self
                .gateway
                .pick_pool_slot_filtered(0, |route| {
                    let idx = usize::from(route.conn_id);
                    idx < MAX_ORDER_CONNECTIONS && !slot_busy[idx]
                })
                .ok_or(Error::Busy)?;
            (cid, None)
        };
        let (status, slot_epoch) = self.ingress.submit_on_slot(
            &mut self.gateway,
            conn_id,
            command.clone(),
            order_ids,
            evaluated_at,
            deadline,
        )?;
        if status != Status::Aborted {
            if let Some((chosen_ip, action)) = pending_quota {
                self.allow_quota_on_ip(chosen_ip, action, evaluated_at)
                    .map_err(Error::RateLimited)?;
            }
            if let Some(slot) = self.slot_states[usize::from(conn_id)].as_mut() {
                let connection_epoch = slot.connection_epoch;
                slot.pending = Some(SlotPending {
                    command,
                    deadline,
                    sent_mono_ns: evaluated_at,
                    slot_epoch,
                    connection_epoch,
                });
            }
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
        if self.websocket.is_some() {
            return self.poll_websocket_owner(now, received_mono_ns, budget);
        }
        if self.gateway.has_connection_pool() {
            return self.poll_pooled_rest(now, received_mono_ns, budget);
        }
        // Even an EOF/error may have left a complete response in the bounded RX queue.
        if !self.gateway.halted() && self.gateway.has_transport_connection() {
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
                self.on_transport_fail();
                return self.finish_unknown(received_mono_ns);
            }
            self.raw.extend_from_slice(&bytes[..n]);
            if self.decoder.push(&bytes[..n]).is_err() {
                self.on_transport_fail();
                return self.finish_unknown(received_mono_ns);
            }
        }
        // A budget yield must drain retained EOF bytes on the following poll first.
        if drained == budget.max_bytes && drained != 0 {
            return Ok(None);
        }
        if self.gateway.halted() || !self.gateway.has_transport_connection() {
            self.decoder.finish();
        }
        let response = match self.decoder.next_response() {
            Ok(response) => response,
            Err(_) => {
                self.on_transport_fail();
                return self.finish_unknown(received_mono_ns);
            }
        };
        if let Some(response) = response {
            let extra = self.decoder.take_buffered();
            let Some(pending) = self.pending.take() else {
                self.gateway.stop_sending();
                return Err(Error::Identity);
            };
            let _ = self.reconcile_http_response_quota(None, &response, received_mono_ns);
            let cmd_for_desk = pending.command.clone();
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
                self.on_transport_fail();
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
            match &outcome {
                Ok(_) => self.sync_query_desk_for_command(&cmd_for_desk, received_mono_ns),
                Err(_) => self.gateway.stop_sending(),
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
            if self.gateway.halted()
                || !self.gateway.has_transport_connection()
                || now >= pending.deadline
            {
                self.on_transport_fail();
                return self.finish_unknown(received_mono_ns);
            }
        }
        Ok(None)
    }
    fn poll_pooled_rest(
        &mut self,
        now: Instant,
        received_mono_ns: u64,
        budget: IoBudget,
    ) -> Result<Option<RestOutcome>> {
        if let Some(queued) = self.completed_outcomes.pop_front() {
            return Ok(Some(queued));
        }
        self.poll_pool_dials(received_mono_ns);
        let active_slots = self.slot_states.iter().filter(|s| s.is_some()).count();
        if active_slots == 0 {
            return Ok(None);
        }
        let slot_budget = IoBudget {
            max_syscalls: (budget.max_syscalls / active_slots).max(1),
            max_bytes: (budget.max_bytes / active_slots).max(1),
        };
        let mut bytes = [0u8; 4096];
        for idx in 0..MAX_ORDER_CONNECTIONS {
            if self.slot_states[idx].is_none() {
                continue;
            }
            let conn_id = idx as u16;
            let stale_pending_epoch = self.slot_states[idx]
                .as_ref()
                .and_then(|s| s.pending.as_ref())
                .is_some_and(|p| {
                    self.gateway.has_slot_connection(conn_id)
                        && self
                            .gateway
                            .slot_route(conn_id)
                            .is_some_and(|r| r.epoch != p.slot_epoch)
                });
            if stale_pending_epoch {
                self.finalize_stale_pool_pending(conn_id, received_mono_ns)?;
                continue;
            }
            if !self.gateway.halted() && self.gateway.has_slot_connection(conn_id) {
                let _ = self.gateway.poll_slot(conn_id, now, slot_budget);
            }
            if let Some(aborted_key) = self.slot_states[idx].as_ref().and_then(|slot| {
                let p = slot.pending.as_ref()?;
                let key = intent(&p.command);
                (self.gateway.status(key) == Some(Status::Aborted) && slot.raw.is_empty())
                    .then_some(key)
            }) {
                let _ = self.slot_states[idx].as_mut().unwrap().pending.take();
                self.completed_outcomes.push_back(RestOutcome {
                    request: aborted_key,
                    input: None,
                    disposition: RestDisposition::NoBytes,
                    applied_events: 0,
                });
                continue;
            }
            let mut drained = 0usize;
            let mut read_failed = false;
            while drained < slot_budget.max_bytes {
                let limit = bytes.len().min(slot_budget.max_bytes - drained);
                let n = self.gateway.read_slot(conn_id, &mut bytes[..limit]);
                if n == 0 {
                    break;
                }
                drained += n;
                if self.slot_states[idx].as_ref().unwrap().pending.is_none() {
                    if self.gateway.per_order_doubt_isolation()
                        && !self.gateway.halted()
                        && !self.gateway.account().frozen()
                    {
                        self.fail_pool_slot(conn_id, received_mono_ns)?;
                        read_failed = true;
                        break;
                    }
                    self.gateway.stop_sending();
                    return Err(Error::Identity);
                }
                let overflow = self.slot_states[idx]
                    .as_ref()
                    .unwrap()
                    .raw
                    .len()
                    .checked_add(n)
                    .is_none_or(|len| len > self.config.max_response_bytes);
                if overflow {
                    self.fail_pool_slot(conn_id, received_mono_ns)?;
                    read_failed = true;
                    break;
                }
                let slot = self.slot_states[idx].as_mut().unwrap();
                slot.raw.extend_from_slice(&bytes[..n]);
                if slot.decoder.push(&bytes[..n]).is_err() {
                    self.fail_pool_slot(conn_id, received_mono_ns)?;
                    read_failed = true;
                    break;
                }
            }
            if read_failed || (drained == slot_budget.max_bytes && drained != 0) {
                continue;
            }
            if self.gateway.halted() || !self.gateway.has_slot_connection(conn_id) {
                self.slot_states[idx].as_mut().unwrap().decoder.finish();
            }
            let response = match self.slot_states[idx]
                .as_mut()
                .unwrap()
                .decoder
                .next_response()
            {
                Ok(resp) => resp,
                Err(_) => {
                    self.fail_pool_slot(conn_id, received_mono_ns)?;
                    continue;
                }
            };
            if let Some(response) = response {
                let extra = self.slot_states[idx]
                    .as_mut()
                    .unwrap()
                    .decoder
                    .take_buffered();
                let Some(pending) = self.slot_states[idx].as_mut().unwrap().pending.take() else {
                    if self.gateway.per_order_doubt_isolation()
                        && !self.gateway.halted()
                        && !self.gateway.account().frozen()
                    {
                        self.fail_pool_slot(conn_id, received_mono_ns)?;
                        continue;
                    }
                    self.gateway.stop_sending();
                    return Err(Error::Identity);
                };
                let sample_us =
                    u32::try_from(received_mono_ns.saturating_sub(pending.sent_mono_ns) / 1_000)
                        .unwrap_or(u32::MAX);
                let _ = self
                    .gateway
                    .release_slot_inflight(conn_id, pending.slot_epoch);
                let _ = self.gateway.note_slot_sample(
                    conn_id,
                    pending.slot_epoch,
                    SampleKind::Order,
                    sample_us,
                );
                let isolated_ip_rate_limit =
                    self.reconcile_http_response_quota(Some(conn_id), &response, received_mono_ns);
                let cmd_for_desk = pending.command.clone();
                let key = intent(&pending.command);
                let complete = self
                    .gateway
                    .output_progress(key)
                    .is_some_and(|p| p.total_bytes > 0 && p.accepted_bytes == p.total_bytes);
                let raw = std::mem::take(&mut self.slot_states[idx].as_mut().unwrap().raw);
                self.slot_states[idx].as_mut().unwrap().decoder =
                    ResponseDecoder::new(self.config.max_response_bytes, self.config.max_headers)
                        .map_err(Error::Transport)?;
                let outcome = if complete && extra.is_empty() {
                    self.ingress.persist_rest_with_options(
                        &mut self.gateway,
                        pending.command,
                        pending.connection_epoch,
                        received_mono_ns,
                        response.status,
                        raw,
                        Some(&response.body),
                        isolated_ip_rate_limit,
                    )
                } else {
                    let _ = self
                        .gateway
                        .drop_broken_slot_isolated(conn_id, received_mono_ns, true);
                    self.gateway.drain_slot_retained_receive(conn_id);
                    self.slot_states[idx].as_mut().unwrap().registered_mio_epoch = None;
                    self.ingress.persist_rest_with_options(
                        &mut self.gateway,
                        pending.command,
                        pending.connection_epoch,
                        received_mono_ns,
                        response.status,
                        raw,
                        None,
                        false,
                    )
                };
                match outcome {
                    Ok(out) => {
                        self.sync_query_desk_for_command(&cmd_for_desk, received_mono_ns);
                        self.completed_outcomes.push_back(out);
                    }
                    Err(err) => {
                        self.gateway.stop_sending();
                        return Err(err);
                    }
                }
                continue;
            }
            if self.slot_states[idx].as_ref().unwrap().pending.is_some() {
                let (key, past_deadline, raw_empty) = {
                    let slot = self.slot_states[idx].as_ref().unwrap();
                    let p = slot.pending.as_ref().unwrap();
                    (intent(&p.command), now >= p.deadline, slot.raw.is_empty())
                };
                if self.gateway.status(key) == Some(Status::Aborted) && raw_empty {
                    let _ = self.slot_states[idx]
                        .as_mut()
                        .unwrap()
                        .pending
                        .take()
                        .unwrap();
                    self.completed_outcomes.push_back(RestOutcome {
                        request: key,
                        input: None,
                        disposition: RestDisposition::NoBytes,
                        applied_events: 0,
                    });
                    continue;
                }
                if self.gateway.halted()
                    || !self.gateway.has_slot_connection(conn_id)
                    || past_deadline
                {
                    self.fail_pool_slot(conn_id, received_mono_ns)?;
                }
            }
        }
        self.poll_pool_dials(received_mono_ns);
        Ok(self.completed_outcomes.pop_front())
    }
    fn finalize_stale_pool_pending(&mut self, conn_id: u16, received_mono_ns: u64) -> Result<()> {
        let idx = usize::from(conn_id);
        let decoder = ResponseDecoder::new(self.config.max_response_bytes, self.config.max_headers)
            .map_err(Error::Transport)?;
        let Some(slot) = self.slot_states[idx].as_mut() else {
            return Ok(());
        };
        slot.decoder = decoder;
        let raw = std::mem::take(&mut slot.raw);
        let Some(pending) = slot.pending.take() else {
            return Ok(());
        };
        let key = intent(&pending.command);
        if self.gateway.status(key) == Some(Status::Aborted) && raw.is_empty() {
            self.completed_outcomes.push_back(RestOutcome {
                request: key,
                input: None,
                disposition: RestDisposition::NoBytes,
                applied_events: 0,
            });
            return Ok(());
        }
        let _ = self
            .gateway
            .release_slot_inflight(conn_id, pending.slot_epoch);
        let cmd_for_desk = pending.command.clone();
        let outcome = self.ingress.persist_rest(
            &mut self.gateway,
            pending.command,
            pending.connection_epoch,
            received_mono_ns,
            0,
            raw,
            None,
        );
        match outcome {
            Ok(out) => {
                self.sync_query_desk_for_command(&cmd_for_desk, received_mono_ns);
                self.completed_outcomes.push_back(out);
                Ok(())
            }
            Err(err) => {
                self.gateway.stop_sending();
                Err(err)
            }
        }
    }
    fn fail_pool_slot(&mut self, conn_id: u16, received_mono_ns: u64) -> Result<()> {
        let idx = usize::from(conn_id);
        if !self.gateway.halted() {
            let _ = self
                .gateway
                .drop_broken_slot_isolated(conn_id, received_mono_ns, true);
            self.gateway.drain_slot_retained_receive(conn_id);
        }
        let decoder = ResponseDecoder::new(self.config.max_response_bytes, self.config.max_headers)
            .map_err(Error::Transport)?;
        let Some(slot) = self.slot_states[idx].as_mut() else {
            return Ok(());
        };
        slot.decoder = decoder;
        let raw = std::mem::take(&mut slot.raw);
        slot.registered_mio_epoch = None;
        let Some(pending) = slot.pending.take() else {
            return Ok(());
        };
        let key = intent(&pending.command);
        if self.gateway.status(key) == Some(Status::Aborted) && raw.is_empty() {
            self.completed_outcomes.push_back(RestOutcome {
                request: key,
                input: None,
                disposition: RestDisposition::NoBytes,
                applied_events: 0,
            });
            return Ok(());
        }
        let _ = self
            .gateway
            .release_slot_inflight(conn_id, pending.slot_epoch);
        let cmd_for_desk = pending.command.clone();
        let outcome = self.ingress.persist_rest(
            &mut self.gateway,
            pending.command,
            pending.connection_epoch,
            received_mono_ns,
            0,
            raw,
            None,
        );
        match outcome {
            Ok(out) => {
                self.sync_query_desk_for_command(&cmd_for_desk, received_mono_ns);
                self.completed_outcomes.push_back(out);
                Ok(())
            }
            Err(err) => {
                self.gateway.stop_sending();
                Err(err)
            }
        }
    }
    fn reconcile_http_response_quota(
        &mut self,
        conn_id: Option<u16>,
        response: &tickwire_transport::http::Response,
        received_mono_ns: u64,
    ) -> bool {
        if self.venue_limiter.is_none() {
            return false;
        }
        let market_opt = self.ingress.market().ok();
        let has_pool = self.gateway.has_connection_pool();
        let per_order_iso = self.gateway.per_order_doubt_isolation();
        let (configured_ip, route_ip) = match conn_id {
            Some(cid) => {
                let cfg_ip = self
                    .slot_states
                    .get(usize::from(cid))
                    .and_then(|s| s.as_ref())
                    .and_then(|s| s.configured_local_ip);
                let rt_ip = self.gateway.slot_route(cid).and_then(|r| r.local_ip);
                (cfg_ip, rt_ip)
            }
            None => (None, None),
        };
        let limiter = self.venue_limiter.as_mut().unwrap();
        let ip_key = match conn_id {
            Some(_) => resolve_slot_ip_key(configured_ip, route_ip, limiter),
            None => limiter.local_ips().first().copied().unwrap_or(IpKey::NONE),
        };
        if let Some(val) = response
            .header("x-mbx-used-weight-1m")
            .and_then(parse_ascii_u64)
            && let Some(win_id) = limiter.ip_window_id(ip_key)
        {
            let _ = limiter.reconcile_exchange_usage(win_id, val, received_mono_ns);
        }
        for (hdr, interval_s) in [
            ("x-mbx-order-count-10s", 10u32),
            ("x-mbx-order-count-1m", 60u32),
            ("x-mbx-order-count-1d", 86_400u32),
        ] {
            if let Some(val) = response.header(hdr).and_then(parse_ascii_u64)
                && let Some(win_id) = limiter
                    .account_order_window_ids()
                    .iter()
                    .copied()
                    .find(|id| id.interval_s == interval_s)
            {
                let _ = limiter.reconcile_exchange_usage(win_id, val, received_mono_ns);
            }
        }
        if response.status == 418 {
            let _ = limiter.freeze_account();
            return false;
        }
        if response.status == 429 {
            let non_rate_hazard = market_opt
                .and_then(|m| {
                    tickwire_binance::response::decode_rest(m, response.status, &response.body).ok()
                })
                .and_then(|d| d.error)
                .is_some_and(|e| matches!(e.code, -1021 | -1002 | -2014 | -2015 | -1022));
            if non_rate_hazard {
                return false;
            }
            let retry_secs = response
                .header("retry-after")
                .and_then(parse_ascii_u64)
                .unwrap_or(1)
                .max(1);
            let until_ns =
                received_mono_ns.saturating_add(retry_secs.saturating_mul(1_000_000_000));
            if let Some(ip) = configured_ip.or(route_ip) {
                let slot_ip_key = IpKey::from_ip(ip);
                if limiter.ip_window_id(slot_ip_key).is_some() {
                    let _ = limiter.note_ip_cooldown(slot_ip_key, until_ns);
                    if conn_id.is_some()
                        && has_pool
                        && per_order_iso
                        && limiter.local_ips().len() > 1
                    {
                        return true;
                    }
                }
            } else if !ip_key.is_none() {
                let _ = limiter.note_ip_cooldown(ip_key, until_ns);
            }
        }
        false
    }
    fn on_transport_fail(&mut self) {
        if self.gateway.per_order_doubt_isolation()
            && !self.gateway.halted()
            && !self.gateway.account().frozen()
        {
            self.gateway.drop_broken_transport_isolated();
            self.single_registered_mio = false;
        } else {
            self.gateway.stop_sending();
        }
    }
    fn finish_unknown(&mut self, received: u64) -> Result<Option<RestOutcome>> {
        let Some(pending) = self.pending.take() else {
            return Ok(None);
        };
        let cmd_for_desk = pending.command.clone();
        let raw = std::mem::take(&mut self.raw);
        self.decoder =
            ResponseDecoder::new(self.config.max_response_bytes, self.config.max_headers)
                .map_err(Error::Transport)?;
        let outcome = self.ingress.persist_rest(
            &mut self.gateway,
            pending.command,
            self.config.connection_epoch,
            received,
            0,
            raw,
            None,
        );
        match &outcome {
            Ok(_) => self.sync_query_desk_for_command(&cmd_for_desk, received),
            Err(_) => self.gateway.stop_sending(),
        }
        outcome.map(Some)
    }
    pub(crate) fn sync_query_desk_for_command(
        &mut self,
        command: &CanonicalCommand,
        now_mono_ns: u64,
    ) {
        if !self.gateway.per_order_doubt_isolation() || self.gateway.account().frozen() {
            return;
        }
        let instrument_id = command.command().context.instrument.instrument_id;
        let resolved = match &command.command().body {
            v::CommandBody::New(_) => {
                let key = intent(command);
                self.ingress.registry.get(&key).and_then(|reg| {
                    if reg.orders.len() != 1 {
                        return None;
                    }
                    let cid = reg.orders[0].client_id.clone()?;
                    let native = reg.orders[0].venue_order_id.clone();
                    Some((
                        tickwire_account::OrderKey {
                            intent: key,
                            client_id: cid,
                        },
                        native,
                    ))
                })
            }
            v::CommandBody::Cancel(target)
            | v::CommandBody::AmendKeepPriority { target, .. }
            | v::CommandBody::Query(target) => self
                .gateway
                .account()
                .resolve_order_target(instrument_id, target)
                .map(|order_key| {
                    let native = self
                        .ingress
                        .registry
                        .get(&order_key.intent)
                        .and_then(|reg| {
                            reg.orders
                                .iter()
                                .find(|o| o.client_id.as_ref() == Some(&order_key.client_id))
                                .and_then(|o| o.venue_order_id.clone())
                        });
                    (order_key, native)
                }),
            _ => None,
        };
        let Some((order_key, native)) = resolved else {
            return;
        };
        let Some(reason) = self.gateway.account().order_in_doubt(&order_key) else {
            return;
        };
        let confirmed = self
            .gateway
            .account()
            .order(&order_key)
            .is_some_and(|v| v.confirmed);
        let (kind, place_unacked) = match reason {
            tickwire_account::DoubtReason::PlaceAmbiguous => {
                (tickwire_account::AskKind::Place, !confirmed)
            }
            tickwire_account::DoubtReason::CancelAmbiguous => {
                (tickwire_account::AskKind::Cancel, !confirmed)
            }
            tickwire_account::DoubtReason::AmendAmbiguous => {
                (tickwire_account::AskKind::Amend, !confirmed)
            }
            tickwire_account::DoubtReason::FillCatchup => {
                (tickwire_account::AskKind::Trades, false)
            }
        };
        let account_ref = self.gateway.account();
        let Some(desk) = &mut self.query_desk else {
            return;
        };
        if desk
            .enqueue_ask(
                order_key,
                instrument_id,
                native,
                kind,
                place_unacked,
                now_mono_ns,
                |k| account_ref.order_in_doubt(k).is_some(),
            )
            .is_err()
        {
            self.gateway.freeze_account();
        }
    }
    pub fn stop_sending(&mut self) {
        self.ingress.invalidate_ws_session();
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
        if let Some(ws) = &self.websocket {
            return ws.inflight.len()
                >= tickwire_account_gateway::pool::MAX_SHARED_INFLIGHT as usize
                || self.ws_frame_flush_pending();
        }
        if self.gateway.has_connection_pool() {
            return !self.has_ready_pool_slot();
        }
        self.pending.is_some()
    }
    pub fn submission_ready(&self) -> bool {
        if self.websocket.is_some() {
            return !self.busy()
                && self.websocket_ready()
                && !self.gateway.websocket_control_pending()
                && !self.websocket_deferred();
        }
        if self.gateway.has_connection_pool() {
            return !self.gateway.halted() && self.has_ready_pool_slot();
        }
        !self.busy()
    }
    pub fn ingest_private(&mut self, event: &PrivateEvent<'_>) -> Result<Outcome> {
        let outcome = self.ingress.ingest(&mut self.gateway, event)?;
        if self.gateway.per_order_doubt_isolation()
            && self.query_desk.is_some()
            && !self.gateway.account().frozen()
        {
            let now_mono_ns = event.received_mono_ns();
            let fill_catchup_orders: Vec<_> = self
                .gateway
                .account()
                .in_doubt_orders()
                .filter(|&(_, r)| r == tickwire_account::DoubtReason::FillCatchup)
                .map(|(k, _)| k.clone())
                .collect();
            for order_key in fill_catchup_orders {
                let instrument_id = self
                    .gateway
                    .account()
                    .order_terms(&order_key)
                    .map(|(inst, _)| inst.instrument_id);
                let native = self
                    .gateway
                    .account()
                    .venue_order_id(&order_key)
                    .cloned()
                    .or_else(|| {
                        self.ingress
                            .registry
                            .get(&order_key.intent)
                            .and_then(|reg| {
                                reg.orders
                                    .iter()
                                    .find(|o| o.client_id.as_ref() == Some(&order_key.client_id))
                                    .and_then(|o| o.venue_order_id.clone())
                            })
                    });
                if let Some(inst_id) = instrument_id {
                    let account_ref = self.gateway.account();
                    if let Some(desk) = &mut self.query_desk
                        && desk
                            .enqueue_ask(
                                order_key,
                                inst_id,
                                native,
                                tickwire_account::AskKind::Trades,
                                false,
                                now_mono_ns,
                                |k| account_ref.order_in_doubt(k).is_some(),
                            )
                            .is_err()
                    {
                        self.gateway.freeze_account();
                    }
                }
            }
        }
        Ok(outcome)
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
    pub fn reset_mio_registrations(&mut self) {
        self.single_registered_mio = false;
        for slot in self.slot_states.iter_mut().flatten() {
            slot.registered_mio_epoch = None;
        }
    }
    pub fn register_all(&mut self, registry: &tickwire_transport::mio::Registry) -> Result<()> {
        self.reset_mio_registrations();
        self.reregister_all(registry)
    }
    pub fn reregister_all(&mut self, registry: &tickwire_transport::mio::Registry) -> Result<()> {
        if self.gateway.halted() {
            return Ok(());
        }
        if !self.gateway.has_connection_pool() {
            if !self.gateway.has_transport_connection() {
                self.single_registered_mio = false;
                return Ok(());
            }
            if self.single_registered_mio {
                self.reregister(registry, tickwire_transport::mio::Token(6))?;
            } else {
                self.register(registry, tickwire_transport::mio::Token(6))?;
                self.single_registered_mio = true;
            }
            return Ok(());
        }
        for idx in 0..MAX_ORDER_CONNECTIONS {
            let conn_id = idx as u16;
            if !self.gateway.has_slot_connection(conn_id) {
                if let Some(slot) = self.slot_states[idx].as_mut() {
                    slot.registered_mio_epoch = None;
                }
                continue;
            }
            let router_epoch = self.gateway.slot_route(conn_id).map_or(0, |r| r.epoch);
            let already = self.slot_states[idx]
                .as_ref()
                .is_some_and(|s| s.registered_mio_epoch == Some(router_epoch) && router_epoch > 0);
            let token = tickwire_transport::mio::Token(POOL_MIO_TOKEN_BASE + idx);
            if already {
                self.gateway
                    .reregister_slot(conn_id, registry, token)
                    .map_err(Error::Gateway)?;
            } else {
                self.gateway
                    .register_slot(conn_id, registry, token)
                    .map_err(Error::Gateway)?;
                if let Some(slot) = self.slot_states[idx].as_mut() {
                    slot.registered_mio_epoch = Some(router_epoch);
                }
            }
        }
        Ok(())
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

fn resolve_slot_ip_key(
    configured_ip: Option<IpAddr>,
    route_ip: Option<IpAddr>,
    limiter: &VenueRateLimiter,
) -> IpKey {
    if let Some(ip) = configured_ip {
        return IpKey::from_ip(ip);
    }
    if let Some(ip) = route_ip {
        let key = IpKey::from_ip(ip);
        if limiter.ip_window_id(key).is_some() {
            return key;
        }
        if limiter.local_ips().len() == 1 {
            return limiter.local_ips()[0];
        }
        return key;
    }
    if limiter.local_ips().len() == 1 {
        return limiter.local_ips()[0];
    }
    IpKey::NONE
}

fn can_afford_quota_with_limiter(
    limiter: &VenueRateLimiter,
    ip_key: IpKey,
    action: QuotaAction,
    now_ns: u64,
) -> core::result::Result<(), RateReject> {
    match action {
        QuotaAction::Place {
            symbol,
            order_units,
            weight,
        } => limiter.can_afford_place_orders(ip_key, symbol, order_units, weight, now_ns),
        QuotaAction::Modify {
            symbol,
            weight,
            counts_as_order,
        } => limiter.can_afford_modify(ip_key, symbol, weight, counts_as_order, now_ns),
        QuotaAction::Cancel { symbol, weight } => {
            limiter.can_afford_cancel(ip_key, symbol, weight, now_ns)
        }
        QuotaAction::Query { weight } => limiter.can_afford_query(ip_key, weight, now_ns),
    }
}

pub(crate) fn allow_quota_with_limiter(
    limiter: &mut VenueRateLimiter,
    ip_key: IpKey,
    action: QuotaAction,
    now_ns: u64,
) -> core::result::Result<(), RateReject> {
    match action {
        QuotaAction::Place {
            symbol,
            order_units,
            weight,
        } => limiter
            .allow_place_orders(ip_key, symbol, order_units, weight, now_ns)
            .map(|_| ()),
        QuotaAction::Modify {
            symbol,
            weight,
            counts_as_order,
        } => limiter
            .allow_modify(ip_key, symbol, weight, counts_as_order, now_ns)
            .map(|_| ()),
        QuotaAction::Cancel { symbol, weight } => limiter
            .allow_cancel(ip_key, symbol, weight, now_ns)
            .map(|_| ()),
        QuotaAction::Query { weight } => limiter.allow_query(ip_key, weight, now_ns).map(|_| ()),
    }
}
