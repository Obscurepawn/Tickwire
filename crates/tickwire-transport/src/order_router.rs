//! Allocation-free integer EWMA execution order router and multi-connection pool.
#![forbid(unsafe_code)]

use crate::{
    Connection, Error, Result,
    dialer::{DialBackoff, DialBaton, DialState, IpPool},
};
use std::{
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
};

pub const MAX_ORDER_CONNECTIONS: usize = 16;
pub const MAX_SHARED_INFLIGHT: u16 = 16;
pub const UNMEASURED_SCORE: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendSlot {
    Exclusive,
    Shared,
}

impl SendSlot {
    #[inline]
    pub const fn max_inflight(self) -> u16 {
        match self {
            Self::Exclusive => 1,
            Self::Shared => MAX_SHARED_INFLIGHT,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SampleKind {
    Order,
    Ping,
    App,
    Tcping,
}

impl SampleKind {
    #[inline]
    pub const fn updates_route_ewma(self) -> bool {
        matches!(self, Self::Order | Self::Ping | Self::App)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RouterConfig {
    pub ema_new: u16,
    pub ema_den: u16,
    pub ema_min_samples: u16,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            ema_new: 1,
            ema_den: 8,
            ema_min_samples: 8,
        }
    }
}

impl RouterConfig {
    #[inline]
    pub const fn new(ema_new: u16, ema_den: u16, ema_min_samples: u16) -> Self {
        Self {
            ema_new,
            ema_den,
            ema_min_samples,
        }
    }

    pub fn validate(self) -> Result<()> {
        if self.ema_den < 2
            || self.ema_new < 1
            || self.ema_new >= self.ema_den
            || self.ema_min_samples < 1
        {
            return Err(Error::Bounds);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RouteHealth {
    pub ema_us: u32,
    pub ema_samples: u16,
    pub ema_min_samples: u16,
    pub inflight: u16,
}

impl Default for RouteHealth {
    fn default() -> Self {
        Self::new(8)
    }
}

impl RouteHealth {
    #[inline]
    pub const fn new(ema_min_samples: u16) -> Self {
        Self {
            ema_us: 0,
            ema_samples: 0,
            ema_min_samples,
            inflight: 0,
        }
    }

    #[inline]
    pub const fn ema_us(&self) -> u32 {
        self.ema_us
    }

    #[inline]
    pub const fn ema_samples(&self) -> u16 {
        self.ema_samples
    }

    #[inline]
    pub const fn ema_min_samples(&self) -> u16 {
        self.ema_min_samples
    }

    #[inline]
    pub const fn inflight(&self) -> u16 {
        self.inflight
    }

    #[inline]
    pub const fn is_measured(&self) -> bool {
        self.ema_samples >= self.ema_min_samples
    }

    #[inline]
    pub fn note_sample(&mut self, sample_us: u32, cfg: RouterConfig) {
        if self.ema_samples == 0 {
            self.ema_us = sample_us;
            self.ema_samples = 1;
            return;
        }
        let den = u64::from(cfg.ema_den.max(1));
        let new_w = u64::from(cfg.ema_new).min(den);
        let keep = den - new_w;
        let next = (u64::from(self.ema_us) * keep + u64::from(sample_us) * new_w) / den;
        self.ema_us = next as u32;
        self.ema_samples = self.ema_samples.saturating_add(1);
    }

    #[inline]
    pub const fn score(&self) -> Option<u32> {
        if self.ema_samples >= self.ema_min_samples {
            Some(self.ema_us)
        } else {
            None
        }
    }

    #[inline]
    pub const fn raw_score(&self) -> u32 {
        match self.score() {
            Some(s) => s,
            None => UNMEASURED_SCORE,
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        self.ema_us = 0;
        self.ema_samples = 0;
        self.inflight = 0;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderRoute {
    pub conn_id: u16,
    pub local_ip: Option<IpAddr>,
    pub peer_addr: Option<SocketAddr>,
    pub send_slot: SendSlot,
    pub product_mask: u32,
    pub established: bool,
    pub epoch: u64,
    pub health: RouteHealth,
}

impl OrderRoute {
    #[inline]
    pub const fn is_ready(&self) -> bool {
        self.established && self.health.inflight < self.send_slot.max_inflight()
    }

    #[inline]
    pub const fn matches_product(&self, sym_type: u32) -> bool {
        sym_type == 0 || self.product_mask == 0 || (self.product_mask & sym_type) != 0
    }
}

#[inline]
fn next_epoch(current: u64) -> u64 {
    current.wrapping_add(1).max(1)
}

#[derive(Clone, Debug)]
pub struct OrderRouter {
    slots: [Option<OrderRoute>; MAX_ORDER_CONNECTIONS],
    config: RouterConfig,
    last_conn_id: Option<u16>,
}

impl OrderRouter {
    pub fn new(config: RouterConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            slots: [None; MAX_ORDER_CONNECTIONS],
            config,
            last_conn_id: None,
        })
    }

    #[inline]
    pub const fn config(&self) -> RouterConfig {
        self.config
    }

    #[inline]
    pub const fn last_conn_id(&self) -> Option<u16> {
        self.last_conn_id
    }

    #[inline]
    pub const fn routes(&self) -> &[Option<OrderRoute>; MAX_ORDER_CONNECTIONS] {
        &self.slots
    }

    #[inline]
    pub fn route(&self, conn_id: u16) -> Option<&OrderRoute> {
        self.slots.get(usize::from(conn_id))?.as_ref()
    }

    pub fn slot_count(&self) -> usize {
        self.slots.iter().filter(|s| s.is_some()).count()
    }

    pub fn established_count(&self) -> usize {
        self.slots
            .iter()
            .flatten()
            .filter(|r| r.established)
            .count()
    }

    pub fn ready_count(&self, sym_type: u32) -> usize {
        self.slots
            .iter()
            .flatten()
            .filter(|r| r.is_ready() && r.matches_product(sym_type))
            .count()
    }

    #[inline]
    pub fn has_ready_capacity(&self, sym_type: u32) -> bool {
        self.slots
            .iter()
            .flatten()
            .any(|r| r.is_ready() && r.matches_product(sym_type))
    }

    pub fn register_slot(
        &mut self,
        conn_id: u16,
        local_ip: Option<IpAddr>,
        send_slot: SendSlot,
        product_mask: u32,
    ) -> Result<()> {
        let idx = usize::from(conn_id);
        if idx >= MAX_ORDER_CONNECTIONS {
            return Err(Error::Bounds);
        }
        if self.slots[idx].is_some() {
            return Err(Error::Busy);
        }
        self.slots[idx] = Some(OrderRoute {
            conn_id,
            local_ip,
            peer_addr: None,
            send_slot,
            product_mask,
            established: false,
            epoch: 0,
            health: RouteHealth::new(self.config.ema_min_samples),
        });
        Ok(())
    }

    #[inline]
    fn slot_mut(&mut self, conn_id: u16) -> Result<&mut OrderRoute> {
        let idx = usize::from(conn_id);
        if idx >= MAX_ORDER_CONNECTIONS {
            return Err(Error::Bounds);
        }
        self.slots[idx].as_mut().ok_or(Error::Bounds)
    }

    #[inline]
    pub fn on_connected(
        &mut self,
        conn_id: u16,
        local_ip: Option<IpAddr>,
        peer_addr: Option<SocketAddr>,
    ) -> Result<u64> {
        let min_samples = self.config.ema_min_samples;
        let route = self.slot_mut(conn_id)?;
        route.epoch = next_epoch(route.epoch);
        route.health.reset();
        route.health.ema_min_samples = min_samples;
        route.established = true;
        if let Some(ip) = local_ip {
            route.local_ip = Some(ip);
        }
        route.peer_addr = peer_addr;
        Ok(route.epoch)
    }

    #[inline]
    pub fn on_disconnect(&mut self, conn_id: u16) -> Result<u64> {
        let min_samples = self.config.ema_min_samples;
        let route = self.slot_mut(conn_id)?;
        route.established = false;
        route.peer_addr = None;
        route.health.reset();
        route.health.ema_min_samples = min_samples;
        route.epoch = next_epoch(route.epoch);
        Ok(route.epoch)
    }

    #[inline]
    pub fn acquire_inflight(&mut self, conn_id: u16) -> Result<u64> {
        let route = self.slot_mut(conn_id)?;
        if !route.established {
            return Err(Error::Closed);
        }
        if route.health.inflight >= route.send_slot.max_inflight() {
            return Err(Error::Busy);
        }
        route.health.inflight += 1;
        let epoch = route.epoch;
        self.last_conn_id = Some(conn_id);
        Ok(epoch)
    }

    #[inline]
    pub fn release_inflight(&mut self, conn_id: u16, epoch: u64) -> Result<bool> {
        let route = self.slot_mut(conn_id)?;
        if route.epoch != epoch {
            return Ok(false);
        }
        route.health.inflight = route.health.inflight.saturating_sub(1);
        Ok(true)
    }

    #[inline]
    pub fn note_sample(
        &mut self,
        conn_id: u16,
        epoch: u64,
        kind: SampleKind,
        sample_us: u32,
    ) -> Result<bool> {
        let cfg = self.config;
        let route = self.slot_mut(conn_id)?;
        if !route.established || route.epoch != epoch || !kind.updates_route_ewma() {
            return Ok(false);
        }
        route.health.note_sample(sample_us, cfg);
        Ok(true)
    }

    #[inline]
    pub fn pick(&self, sym_type: u32, last_conn_id: Option<u16>, force: bool) -> Option<u16> {
        let mut first_eligible: Option<u16> = None;
        let mut next_after_last: Option<u16> = None;
        let mut best_measured: Option<(u16, u32)> = None;
        let mut last_measured_score: Option<u32> = None;

        for slot in &self.slots {
            let Some(r) = slot.as_ref() else {
                continue;
            };
            if !(r.established && (force || r.is_ready()) && r.matches_product(sym_type)) {
                continue;
            }
            let cid = r.conn_id;
            if first_eligible.is_none_or(|first| cid < first) {
                first_eligible = Some(cid);
            }
            if let Some(last) = last_conn_id
                && cid > last
                && next_after_last.is_none_or(|nxt| cid < nxt)
            {
                next_after_last = Some(cid);
            }
            if let Some(score) = r.health.score() {
                if best_measured.is_none_or(|(best_cid, best_s)| (score, cid) < (best_s, best_cid))
                {
                    best_measured = Some((cid, score));
                }
                if last_conn_id == Some(cid) {
                    last_measured_score = Some(score);
                }
            }
        }

        let first = first_eligible?;
        match best_measured {
            None => {
                if last_conn_id.is_some() {
                    next_after_last.or(Some(first))
                } else {
                    Some(first)
                }
            }
            Some((best_cid, best_score)) => {
                if let (Some(last), Some(last_score)) = (last_conn_id, last_measured_score)
                    && u64::from(last_score) * 8 <= u64::from(best_score) * 9
                {
                    return Some(last);
                }
                Some(best_cid)
            }
        }
    }

    #[inline]
    pub fn pick_next(&mut self, sym_type: u32, force: bool) -> Option<u16> {
        let picked = self.pick(sym_type, self.last_conn_id, force);
        if picked.is_some() {
            self.last_conn_id = picked;
        }
        picked
    }
}

#[derive(Debug)]
pub struct OrderSlotSpec {
    pub conn_id: Option<u16>,
    pub local_ip: Option<IpAddr>,
    pub send_slot: SendSlot,
    pub product_mask: u32,
    pub connection: Option<Connection>,
    pub peer_addr: Option<SocketAddr>,
    pub baton: Option<Arc<DialBaton>>,
    pub backoff: DialBackoff,
    pub ip_pool: Option<Arc<Mutex<IpPool>>>,
}

impl Default for OrderSlotSpec {
    fn default() -> Self {
        Self {
            conn_id: None,
            local_ip: None,
            send_slot: SendSlot::Shared,
            product_mask: 0,
            connection: None,
            peer_addr: None,
            baton: None,
            backoff: DialBackoff::default(),
            ip_pool: None,
        }
    }
}

impl OrderSlotSpec {
    pub fn new(send_slot: SendSlot) -> Self {
        Self {
            send_slot,
            ..Self::default()
        }
    }

    pub fn with_baton(send_slot: SendSlot, baton: Arc<DialBaton>, backoff: DialBackoff) -> Self {
        Self {
            send_slot,
            baton: Some(baton),
            backoff,
            ..Self::default()
        }
    }

    pub fn with_connection(send_slot: SendSlot, connection: Connection) -> Self {
        let peer_addr = Some(connection.peer_addr_target());
        let local_ip = connection.local_ip();
        Self {
            local_ip,
            send_slot,
            connection: Some(connection),
            peer_addr,
            ..Self::default()
        }
    }
}

#[derive(Debug)]
struct PoolSlot {
    conn_id: u16,
    local_ip: Option<IpAddr>,
    send_slot: SendSlot,
    product_mask: u32,
    connection: Option<Connection>,
    baton: Option<Arc<DialBaton>>,
    backoff: DialBackoff,
    peer_addr: Option<SocketAddr>,
    ip_pool: Option<Arc<Mutex<IpPool>>>,
    leased_peer: Option<SocketAddr>,
    reconnect_failures: u32,
}

impl PoolSlot {
    fn release_leased_peer(&mut self) {
        if let Some(peer) = self.leased_peer.take()
            && let Some(pool) = &self.ip_pool
        {
            pool.lock()
                .unwrap_or_else(|e| e.into_inner())
                .release_ip(peer);
        }
    }
}

#[derive(Debug)]
pub struct OrderConnectionPool {
    slots: [Option<PoolSlot>; MAX_ORDER_CONNECTIONS],
    router: OrderRouter,
}

impl OrderConnectionPool {
    pub fn new(config: RouterConfig) -> Result<Self> {
        Ok(Self {
            slots: std::array::from_fn(|_| None),
            router: OrderRouter::new(config)?,
        })
    }

    pub fn add_slot(&mut self, spec: OrderSlotSpec) -> Result<u16> {
        let conn_id = match spec.conn_id {
            Some(id) => {
                let idx = usize::from(id);
                if idx >= MAX_ORDER_CONNECTIONS {
                    return Err(Error::Bounds);
                }
                if self.slots[idx].is_some() {
                    return Err(Error::Busy);
                }
                id
            }
            None => {
                let idx = self
                    .slots
                    .iter()
                    .position(|s| s.is_none())
                    .ok_or(Error::Bounds)?;
                idx as u16
            }
        };

        self.router
            .register_slot(conn_id, spec.local_ip, spec.send_slot, spec.product_mask)?;

        let mut slot = PoolSlot {
            conn_id,
            local_ip: spec.local_ip,
            send_slot: spec.send_slot,
            product_mask: spec.product_mask,
            connection: None,
            baton: spec.baton,
            backoff: spec.backoff,
            peer_addr: None,
            ip_pool: spec.ip_pool,
            leased_peer: None,
            reconnect_failures: 0,
        };

        if let Some(conn) = spec.connection {
            let effective_local = conn.local_ip().or(slot.local_ip);
            let effective_peer = spec.peer_addr.or_else(|| Some(conn.peer_addr_target()));
            slot.peer_addr = effective_peer;
            if slot.ip_pool.is_some() {
                slot.leased_peer = effective_peer;
            }
            slot.connection = Some(conn);
            let _ = self
                .router
                .on_connected(conn_id, effective_local, effective_peer)?;
        }

        self.slots[usize::from(conn_id)] = Some(slot);
        Ok(conn_id)
    }

    pub fn install_connection(
        &mut self,
        conn_id: u16,
        conn: Connection,
        peer_addr: Option<SocketAddr>,
    ) -> Result<u64> {
        let idx = usize::from(conn_id);
        if idx >= MAX_ORDER_CONNECTIONS {
            return Err(Error::Bounds);
        }
        let slot = self.slots[idx].as_mut().ok_or(Error::Bounds)?;

        let _old_conn = slot.connection.take();
        slot.release_leased_peer();

        let effective_local = conn.local_ip().or(slot.local_ip);
        let effective_peer = peer_addr.or_else(|| Some(conn.peer_addr_target()));
        slot.peer_addr = effective_peer;
        if slot.ip_pool.is_some() {
            slot.leased_peer = effective_peer;
        }
        slot.connection = Some(conn);
        slot.reconnect_failures = 0;

        self.router
            .on_connected(conn_id, effective_local, effective_peer)
    }

    pub fn poll_dials(&mut self, now_mono_ns: u64) -> usize {
        self.poll_dials_filtered(now_mono_ns, |_| true)
    }

    pub fn poll_dials_filtered(
        &mut self,
        now_mono_ns: u64,
        mut can_install: impl FnMut(u16) -> bool,
    ) -> usize {
        let mut installed = 0usize;
        for entry in &mut self.slots {
            let Some(slot) = entry.as_mut() else {
                continue;
            };
            if slot.connection.is_some() {
                continue;
            }
            let Some(baton) = slot.baton.as_ref() else {
                continue;
            };
            match baton.state() {
                DialState::Ready => {
                    if !can_install(slot.conn_id) {
                        continue;
                    }
                    if let Some(link) = baton.take_ready() {
                        slot.release_leased_peer();
                        let effective_local = link.connection.local_ip().or(slot.local_ip);
                        let effective_peer = Some(link.peer);
                        slot.peer_addr = effective_peer;
                        if slot.ip_pool.is_some() {
                            slot.leased_peer = effective_peer;
                        }
                        slot.connection = Some(link.connection);
                        slot.reconnect_failures = 0;
                        let _ =
                            self.router
                                .on_connected(slot.conn_id, effective_local, effective_peer);
                        installed += 1;
                    }
                }
                DialState::Idle => {
                    let _ = baton.want(now_mono_ns);
                }
                DialState::Wanted | DialState::Dialing | DialState::Stopped => {}
            }
        }
        installed
    }

    pub fn disconnect_slot(
        &mut self,
        conn_id: u16,
        now_mono_ns: u64,
        schedule_redial: bool,
    ) -> Result<u64> {
        let idx = usize::from(conn_id);
        if idx >= MAX_ORDER_CONNECTIONS {
            return Err(Error::Bounds);
        }
        let slot = self.slots[idx].as_mut().ok_or(Error::Bounds)?;

        let _old_conn = slot.connection.take();
        slot.release_leased_peer();
        slot.peer_addr = None;

        let epoch = self.router.on_disconnect(conn_id)?;

        if let Some(baton) = slot.baton.as_ref() {
            if schedule_redial {
                let reconnect_delay_ns = slot.backoff.delay_ns(slot.reconnect_failures);
                slot.reconnect_failures = slot.reconnect_failures.saturating_add(1);
                if baton.state() == DialState::Ready {
                    baton.reset();
                }
                let _ = baton.want(now_mono_ns.saturating_add(reconnect_delay_ns));
            } else if matches!(
                baton.state(),
                DialState::Wanted | DialState::Dialing | DialState::Ready
            ) {
                baton.reset();
            }
        }

        Ok(epoch)
    }

    #[inline]
    pub fn pick_ready(&self, sym_type: u32) -> Option<u16> {
        self.router
            .pick(sym_type, self.router.last_conn_id(), false)
    }

    #[inline]
    pub fn pick_and_acquire(&mut self, sym_type: u32) -> Result<Option<(u16, u64)>> {
        let Some(conn_id) = self.pick_ready(sym_type) else {
            return Ok(None);
        };
        let epoch = self.router.acquire_inflight(conn_id)?;
        Ok(Some((conn_id, epoch)))
    }

    #[inline]
    pub fn release_inflight(&mut self, conn_id: u16, epoch: u64) -> Result<bool> {
        self.router.release_inflight(conn_id, epoch)
    }

    #[inline]
    pub fn note_sample(
        &mut self,
        conn_id: u16,
        epoch: u64,
        kind: SampleKind,
        sample_us: u32,
    ) -> Result<bool> {
        self.router.note_sample(conn_id, epoch, kind, sample_us)
    }

    #[inline]
    pub fn connection(&self, conn_id: u16) -> Option<&Connection> {
        self.slots
            .get(usize::from(conn_id))?
            .as_ref()?
            .connection
            .as_ref()
    }

    #[inline]
    pub fn connection_mut(&mut self, conn_id: u16) -> Option<&mut Connection> {
        self.slots
            .get_mut(usize::from(conn_id))?
            .as_mut()?
            .connection
            .as_mut()
    }

    #[inline]
    pub fn baton(&self, conn_id: u16) -> Option<&Arc<DialBaton>> {
        self.slots
            .get(usize::from(conn_id))?
            .as_ref()?
            .baton
            .as_ref()
    }

    #[inline]
    pub fn peer_addr(&self, conn_id: u16) -> Option<SocketAddr> {
        self.slots.get(usize::from(conn_id))?.as_ref()?.peer_addr
    }

    #[inline]
    pub fn slot_send_mode(&self, conn_id: u16) -> Option<SendSlot> {
        Some(self.slots.get(usize::from(conn_id))?.as_ref()?.send_slot)
    }

    #[inline]
    pub fn slot_product_mask(&self, conn_id: u16) -> Option<u32> {
        Some(self.slots.get(usize::from(conn_id))?.as_ref()?.product_mask)
    }

    #[inline]
    pub fn route(&self, conn_id: u16) -> Option<&OrderRoute> {
        self.router.route(conn_id)
    }

    #[inline]
    pub const fn router(&self) -> &OrderRouter {
        &self.router
    }

    #[inline]
    pub fn router_mut(&mut self) -> &mut OrderRouter {
        &mut self.router
    }

    pub fn slot_count(&self) -> usize {
        self.router.slot_count()
    }

    #[inline]
    pub fn has_ready_capacity(&self, sym_type: u32) -> bool {
        self.router.has_ready_capacity(sym_type)
    }

    #[inline]
    pub fn established_count(&self) -> usize {
        self.router.established_count()
    }

    #[inline]
    pub fn ready_count(&self, sym_type: u32) -> usize {
        self.router.ready_count(sym_type)
    }
}

impl Drop for OrderConnectionPool {
    fn drop(&mut self) {
        for slot in self.slots.iter_mut().flatten() {
            let _ = slot.connection.take();
            slot.release_leased_peer();
        }
    }
}
