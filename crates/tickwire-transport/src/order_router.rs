//! Allocation-free integer EWMA execution order router, lock-free double-buffered
//! `PublishedRoutes` snapshots, and multi-connection pool.
#![forbid(unsafe_code)]

use crate::{
    Connection, Error, Result,
    dialer::{DialBackoff, DialBaton, DialState, IpPool},
};
use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tickwire_venue::{OrderRoute as VenueOrderRoute, Product};

pub const MAX_ORDER_CONNECTIONS: usize = 16;
pub const MAX_PUBLISHED_ROUTES: usize = 64;
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
    /// Returns `true` for passive in-band wire RTT samples (`Order`, `Ping`, `App`).
    /// Active out-of-band `Tcping` probes are gated via [`SampleMask::TCPING`] / [`OrderRouter::note_tcping_sample`].
    #[inline]
    pub const fn updates_route_ewma(self) -> bool {
        matches!(self, Self::Order | Self::Ping | Self::App)
    }

    #[inline]
    pub const fn bit(self) -> u8 {
        match self {
            Self::Order => SampleMask::ORDER.0,
            Self::Ping => SampleMask::PING.0,
            Self::App => SampleMask::APP.0,
            Self::Tcping => SampleMask::TCPING.0,
        }
    }
}

/// Bitmask selecting which [`SampleKind`] sources feed [`RouteHealth`] EWMA scoring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SampleMask(pub u8);

impl SampleMask {
    pub const ORDER: Self = Self(1 << 0);
    pub const PING: Self = Self(1 << 1);
    pub const APP: Self = Self(1 << 2);
    pub const TCPING: Self = Self(1 << 3);
    pub const IN_BAND: Self = Self(Self::ORDER.0 | Self::PING.0 | Self::APP.0);
    pub const ALL: Self = Self(Self::IN_BAND.0 | Self::TCPING.0);

    #[inline]
    pub const fn contains(self, kind: SampleKind) -> bool {
        (self.0 & kind.bit()) != 0
    }
}

impl Default for SampleMask {
    fn default() -> Self {
        Self::IN_BAND
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

#[inline]
fn ipv4_key(ip: Option<IpAddr>) -> u32 {
    match ip {
        Some(IpAddr::V4(v4)) => u32::from_be_bytes(v4.octets()),
        Some(IpAddr::V6(v6)) => {
            let octets = v6.octets();
            u32::from_be_bytes([octets[12], octets[13], octets[14], octets[15]])
        }
        None => 0,
    }
}

#[derive(Clone, Debug)]
pub struct OrderRouter {
    slots: [Option<OrderRoute>; MAX_ORDER_CONNECTIONS],
    config: RouterConfig,
    sample_mask: SampleMask,
    last_conn_id: Option<u16>,
}

impl OrderRouter {
    pub fn new(config: RouterConfig) -> Result<Self> {
        Self::with_sample_mask(config, SampleMask::IN_BAND)
    }

    pub fn with_sample_mask(config: RouterConfig, sample_mask: SampleMask) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            slots: [None; MAX_ORDER_CONNECTIONS],
            config,
            sample_mask,
            last_conn_id: None,
        })
    }

    #[inline]
    pub const fn config(&self) -> RouterConfig {
        self.config
    }

    #[inline]
    pub const fn sample_mask(&self) -> SampleMask {
        self.sample_mask
    }

    #[inline]
    pub fn set_sample_mask(&mut self, sample_mask: SampleMask) {
        self.sample_mask = sample_mask;
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
        let mask = self.sample_mask;
        let route = self.slot_mut(conn_id)?;
        if !route.established || route.epoch != epoch || !mask.contains(kind) {
            return Ok(false);
        }
        route.health.note_sample(sample_us, cfg);
        Ok(true)
    }

    /// Explicitly folds an active `SampleKind::Tcping` probe RTT into `RouteHealth` regardless of
    /// whether `SampleMask::TCPING` is set on the passive `note_sample` mask.
    #[inline]
    pub fn note_tcping_sample(&mut self, conn_id: u16, epoch: u64, sample_us: u32) -> Result<bool> {
        let cfg = self.config;
        let route = self.slot_mut(conn_id)?;
        if !route.established || route.epoch != epoch {
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

struct AtomicRouteSlot {
    w0: AtomicU64,
    w1: AtomicU64,
}

impl AtomicRouteSlot {
    const fn empty() -> Self {
        Self {
            w0: AtomicU64::new(VenueOrderRoute::NO_CONNECTION as u64),
            w1: AtomicU64::new((VenueOrderRoute::UNMEASURED_SCORE as u64) << 32),
        }
    }

    #[inline]
    fn store_relaxed(&self, route: VenueOrderRoute) {
        let w0 = u64::from(route.connection_id)
            | (u64::from(route.account_ref) << 16)
            | (u64::from(route.ready) << 24)
            | (u64::from(route.colo) << 25)
            | (u64::from(route.product_mask) << 32);
        let w1 = u64::from(route.local_ip) | (u64::from(route.ema_score_us) << 32);
        self.w0.store(w0, Ordering::Relaxed);
        self.w1.store(w1, Ordering::Relaxed);
    }

    #[inline]
    fn load_relaxed(&self) -> VenueOrderRoute {
        let w0 = self.w0.load(Ordering::Relaxed);
        let w1 = self.w1.load(Ordering::Relaxed);
        VenueOrderRoute {
            connection_id: (w0 & 0xffff) as u16,
            account_ref: ((w0 >> 16) & 0xff) as u8,
            ready: ((w0 >> 24) & 1) != 0,
            colo: ((w0 >> 25) & 1) != 0,
            product_mask: ((w0 >> 32) & 0xffff) as u16,
            local_ip: (w1 & 0xffff_ffff) as u32,
            ema_score_us: (w1 >> 32) as u32,
        }
    }
}

/// Lock-free double-buffered route snapshot table written by `RESP` and queried by `REQ` via
/// Acquire/Release index publication (`9/8` integer EWMA hysteresis).
pub struct PublishedRoutes {
    banks: [[AtomicRouteSlot; MAX_PUBLISHED_ROUTES]; 2],
    /// Packs `(generation: u32) << 32 | (len: u16) << 8 | (active_bank: u8)`.
    header: AtomicU64,
}

impl std::fmt::Debug for PublishedRoutes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublishedRoutes")
            .field("generation", &self.generation())
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

impl Default for PublishedRoutes {
    fn default() -> Self {
        Self::new()
    }
}

impl PublishedRoutes {
    pub fn new() -> Self {
        Self {
            banks: [
                core::array::from_fn(|_| AtomicRouteSlot::empty()),
                core::array::from_fn(|_| AtomicRouteSlot::empty()),
            ],
            header: AtomicU64::new(0),
        }
    }

    #[inline]
    fn unpack_header(h: u64) -> (u32, usize, usize) {
        let generation = (h >> 32) as u32;
        let len = (((h >> 8) & 0xffff) as usize).min(MAX_PUBLISHED_ROUTES);
        let bank = (h & 1) as usize;
        (generation, len, bank)
    }

    #[inline]
    pub fn generation(&self) -> u32 {
        Self::unpack_header(self.header.load(Ordering::Acquire)).0
    }

    #[inline]
    pub fn len(&self) -> usize {
        Self::unpack_header(self.header.load(Ordering::Acquire)).1
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Single-writer (`RESP`) publication of a fresh immutable route snapshot.
    pub fn publish_routes(&self, routes: &[VenueOrderRoute]) -> Result<u32> {
        if routes.len() > MAX_PUBLISHED_ROUTES {
            return Err(Error::Bounds);
        }
        let (cur_gen, _, cur_bank) = Self::unpack_header(self.header.load(Ordering::Acquire));
        let next_bank = 1 - cur_bank;
        let target = &self.banks[next_bank];
        for (idx, route) in routes.iter().copied().enumerate() {
            target[idx].store_relaxed(route);
        }
        let next_gen = cur_gen.wrapping_add(1);
        let next_header =
            (u64::from(next_gen) << 32) | ((routes.len() as u64) << 8) | (next_bank as u64);
        self.header.store(next_header, Ordering::Release);
        Ok(next_gen)
    }

    /// Publishes all registered slots from an [`OrderRouter`] into `PublishedRoutes`.
    pub fn publish_router(
        &self,
        account_ref: u8,
        colo_mask: u16,
        router: &OrderRouter,
    ) -> Result<u32> {
        let mut scratch = [VenueOrderRoute::default(); MAX_ORDER_CONNECTIONS];
        let mut count = 0usize;
        for (bit_idx, slot) in router.routes().iter().enumerate() {
            let Some(r) = slot.as_ref() else {
                continue;
            };
            scratch[count] = VenueOrderRoute {
                connection_id: r.conn_id,
                account_ref,
                ready: r.is_ready(),
                colo: ((colo_mask >> (bit_idx.min(15))) & 1) != 0,
                product_mask: (r.product_mask & 0xffff) as u16,
                local_ip: ipv4_key(r.local_ip),
                ema_score_us: r.health.raw_score(),
            };
            count += 1;
        }
        self.publish_routes(&scratch[..count])
    }

    /// Copies the currently published snapshot into caller-provided stack storage.
    #[inline]
    pub fn snapshot(&self, out: &mut [VenueOrderRoute; MAX_PUBLISHED_ROUTES]) -> usize {
        let (_, len, bank) = Self::unpack_header(self.header.load(Ordering::Acquire));
        let active = &self.banks[bank];
        for (dst, src) in out.iter_mut().zip(active.iter()).take(len) {
            *dst = src.load_relaxed();
        }
        len
    }

    /// Lock-free zero-allocation `REQ` route selection using `9/8` integer EWMA hysteresis
    /// (`last_score * 8 <= best_score * 9`), warmup round-robin across unmeasured routes, and
    /// lowest `connection_id` tie-breaking.
    #[inline]
    pub fn pick(
        &self,
        account_ref: u8,
        product: Option<Product>,
        last_conn_id: Option<u16>,
        force: bool,
    ) -> Option<VenueOrderRoute> {
        let (_, len, bank) = Self::unpack_header(self.header.load(Ordering::Acquire));
        let active = &self.banks[bank];

        let mut first_eligible: Option<VenueOrderRoute> = None;
        let mut next_after_last: Option<VenueOrderRoute> = None;
        let mut best_measured: Option<VenueOrderRoute> = None;
        let mut last_measured: Option<VenueOrderRoute> = None;

        for slot in active.iter().take(len) {
            let r = slot.load_relaxed();
            if r.connection_id == VenueOrderRoute::NO_CONNECTION {
                continue;
            }
            if account_ref != 0 && r.account_ref != 0 && r.account_ref != account_ref {
                continue;
            }
            if !((force || r.ready) && r.matches_product(product)) {
                continue;
            }

            if first_eligible.is_none_or(|f| r.connection_id < f.connection_id) {
                first_eligible = Some(r);
            }
            if let Some(last_cid) = last_conn_id
                && r.connection_id > last_cid
                && next_after_last.is_none_or(|nxt| r.connection_id < nxt.connection_id)
            {
                next_after_last = Some(r);
            }
            if r.ema_score_us != VenueOrderRoute::UNMEASURED_SCORE {
                if best_measured.is_none_or(|best| {
                    (r.ema_score_us, r.connection_id) < (best.ema_score_us, best.connection_id)
                }) {
                    best_measured = Some(r);
                }
                if last_conn_id == Some(r.connection_id) {
                    last_measured = Some(r);
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
            Some(best) => {
                if let Some(last) = last_measured
                    && u64::from(last.ema_score_us) * 8 <= u64::from(best.ema_score_us) * 9
                {
                    return Some(last);
                }
                Some(best)
            }
        }
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
    pub fn note_tcping_sample(&mut self, conn_id: u16, epoch: u64, sample_us: u32) -> Result<bool> {
        self.router.note_tcping_sample(conn_id, epoch, sample_us)
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

    pub fn established_count(&self) -> usize {
        self.router.established_count()
    }

    pub fn ready_count(&self, sym_type: u32) -> usize {
        self.router.ready_count(sym_type)
    }

    #[inline]
    pub fn has_ready_capacity(&self, sym_type: u32) -> bool {
        self.router.has_ready_capacity(sym_type)
    }

    pub fn shutdown_all(&mut self) {
        for entry in &mut self.slots {
            if let Some(slot) = entry.as_mut() {
                let _old_conn = slot.connection.take();
                slot.release_leased_peer();
                slot.peer_addr = None;
                let _ = self.router.on_disconnect(slot.conn_id);
                if let Some(baton) = slot.baton.as_ref() {
                    baton.stop();
                }
            }
        }
    }
}

impl Drop for OrderConnectionPool {
    fn drop(&mut self) {
        self.shutdown_all();
    }
}
