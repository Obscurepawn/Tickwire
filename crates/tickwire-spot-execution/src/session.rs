//! Single-flight and pooled multi-connection private session. AccountGateway remains the sole send and journal owner.
use crate::{
    ModifyReply, OrderSnapshot, Reply, ReportError, SpotEncoder, SpotIssueKind, SpotVenue,
    classify_issue, decode_modify_reply, decode_reply, extract_error_message, hex,
    is_http_account_hazard, is_venue_rate_limit_code,
};
use std::{
    collections::VecDeque,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Instant,
};
use tickwire_account::{DoubtReason, IntentKey};
use tickwire_account_gateway::{
    AccountGateway, Authority, Status,
    pool::{MAX_ORDER_CONNECTIONS, OrderRoute, OrderSlotSpec, RouterConfig, SampleKind, SendSlot},
};
use tickwire_account_journal::Backend;
use tickwire_quota::rate_limiter::{IpKey, RateReject, VenuePreset, VenueRateLimiter};
use tickwire_transport::{IoBudget, Limits, http::ResponseDecoder};
use tickwire_venue as v;

/// Derives a deterministic non-zero `u64` symbol rate-limit key from `instrument_id`.
///
/// `0` is reserved for symbol-independent windows in [`tickwire_quota::rate_limiter::RateLimitId`],
/// so the folded FNV-1a digest is clamped away from `0`.
pub const fn symbol_rate_key(instrument_id: u64) -> u64 {
    let bytes = instrument_id.to_le_bytes();
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut i = 0;
    while i < 8 {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        i += 1;
    }
    if hash == 0 { 1 } else { hash }
}

fn resolve_slot_ip_key(limiter: &VenueRateLimiter, route_local_ip: Option<IpAddr>) -> IpKey {
    if let Some(ip) = route_local_ip {
        let key = IpKey::from_ip(ip);
        if limiter.ip_window_id(key).is_some() {
            return key;
        }
    }
    let unspec = IpKey::from_v4(Ipv4Addr::UNSPECIFIED);
    if limiter.ip_window_id(unspec).is_some() {
        return unspec;
    }
    if limiter.local_ips().len() == 1 {
        return limiter.local_ips()[0];
    }
    IpKey::from_opt_ip(route_local_ip)
}

fn can_afford_command(
    limiter: &VenueRateLimiter,
    body: &v::CommandBody,
    ip_key: IpKey,
    sym_key: u64,
    evaluated_at: u64,
) -> core::result::Result<(), RateReject> {
    match body {
        v::CommandBody::New(_) => limiter.can_afford_place(ip_key, sym_key, 1, evaluated_at),
        v::CommandBody::Modify(_) => {
            limiter.can_afford_modify(ip_key, sym_key, 1, true, evaluated_at)
        }
        v::CommandBody::Cancel(_) => limiter.can_afford_cancel(ip_key, sym_key, 1, evaluated_at),
        v::CommandBody::Query(_) => limiter.can_afford_query(ip_key, 1, evaluated_at),
        _ => Ok(()),
    }
}

fn allow_command(
    limiter: &mut VenueRateLimiter,
    body: &v::CommandBody,
    ip_key: IpKey,
    sym_key: u64,
    evaluated_at: u64,
) -> core::result::Result<(), RateReject> {
    match body {
        v::CommandBody::New(_) => limiter
            .allow_place(ip_key, sym_key, 1, evaluated_at)
            .map(|_| ()),
        v::CommandBody::Modify(_) => limiter
            .allow_modify(ip_key, sym_key, 1, true, evaluated_at)
            .map(|_| ()),
        v::CommandBody::Cancel(_) => limiter
            .allow_cancel(ip_key, sym_key, 1, evaluated_at)
            .map(|_| ()),
        v::CommandBody::Query(_) => limiter.allow_query(ip_key, 1, evaluated_at).map(|_| ()),
        _ => Ok(()),
    }
}

fn parse_retry_after_ns(raw: &tickwire_transport::http::Response) -> u64 {
    const DEFAULT_COOLDOWN_NS: u64 = 1_000_000_000;
    let Some(val) = raw.header("retry-after") else {
        return DEFAULT_COOLDOWN_NS;
    };
    let Ok(s) = std::str::from_utf8(val) else {
        return DEFAULT_COOLDOWN_NS;
    };
    match s.trim().parse::<u64>() {
        Ok(secs) if secs > 0 => secs.saturating_mul(1_000_000_000),
        _ => DEFAULT_COOLDOWN_NS,
    }
}

struct SpotPending {
    intent: IntentKey,
    command: v::CanonicalCommand,
    deadline: Instant,
    sent_mono_ns: u64,
    slot_epoch: u64,
}

struct SpotSlotState {
    decoder: ResponseDecoder,
    pending: Option<SpotPending>,
    last_seen_epoch: u64,
}

#[derive(Debug)]
pub enum SessionError {
    Gateway(tickwire_account_gateway::Error),
    Transport(tickwire_transport::Error),
    Report(ReportError),
    RateLimited(RateReject),
    RecoveryWriterEpoch { configured: u64, durable_floor: u64 },
    Identity,
    Busy,
}
impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for SessionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Gateway(e) => Some(e),
            Self::Transport(e) => Some(e),
            _ => None,
        }
    }
}
type Result<T> = std::result::Result<T, SessionError>;
pub struct RestSession<'a, B: Backend, A: Authority, T>
where
    T: FnMut() -> std::result::Result<u64, tickwire_gateway::Error>,
{
    gateway: AccountGateway<B, A, SpotEncoder<'a, T>>,
    decoder: ResponseDecoder,
    pending: Option<(IntentKey, v::CanonicalCommand, Instant)>,
    connection_epoch: u64,
    slot_state: [Option<SpotSlotState>; MAX_ORDER_CONNECTIONS],
    completed_replies: VecDeque<(IntentKey, Reply)>,
    venue_limiter: Option<VenueRateLimiter>,
    warmup_tracker: tickwire_transport::WarmupTracker,
    warmer: Option<Box<crate::warmup::SpotHotPathWarmer>>,
}
impl<'a, B: Backend, A: Authority, T: FnMut() -> std::result::Result<u64, tickwire_gateway::Error>>
    RestSession<'a, B, A, T>
{
    pub fn new(
        gateway: AccountGateway<B, A, SpotEncoder<'a, T>>,
        connection_epoch: u64,
    ) -> Result<Self> {
        if let Some(durable_floor) = gateway.recovery_writer_floor()
            && gateway.writer_epoch() <= durable_floor
        {
            return Err(SessionError::RecoveryWriterEpoch {
                configured: gateway.writer_epoch(),
                durable_floor,
            });
        }
        if connection_epoch == 0
            || SpotVenue::from_id(gateway.baseline().snapshot().scope.venue).is_none()
        {
            return Err(SessionError::Identity);
        }
        Ok(Self {
            gateway,
            decoder: ResponseDecoder::new(256 * 1024, 128).map_err(SessionError::Transport)?,
            pending: None,
            connection_epoch,
            slot_state: std::array::from_fn(|_| None),
            completed_replies: VecDeque::new(),
            venue_limiter: None,
            warmup_tracker: tickwire_transport::WarmupTracker::disabled(),
            warmer: None,
        })
    }
    pub const fn symbol_rate_key(instrument_id: u64) -> u64 {
        symbol_rate_key(instrument_id)
    }
    pub fn gateway(&self) -> &AccountGateway<B, A, SpotEncoder<'a, T>> {
        &self.gateway
    }
    /// Sole account-owner access for applying authenticated cash evidence and qualified recovery.
    pub fn gateway_mut(&mut self) -> &mut AccountGateway<B, A, SpotEncoder<'a, T>> {
        &mut self.gateway
    }
    pub fn connection_epoch(&self) -> u64 {
        self.connection_epoch
    }
    pub fn with_per_order_doubt_isolation(mut self, enabled: bool) -> Self {
        self.gateway.set_per_order_doubt_isolation(enabled);
        self
    }
    pub fn set_per_order_doubt_isolation(&mut self, enabled: bool) {
        self.gateway.set_per_order_doubt_isolation(enabled);
    }
    pub fn per_order_doubt_isolation(&self) -> bool {
        self.gateway.per_order_doubt_isolation()
    }
    pub fn with_connection_pool(mut self, config: RouterConfig) -> Result<Self> {
        self.enable_connection_pool(config)?;
        Ok(self)
    }
    pub fn enable_connection_pool(&mut self, config: RouterConfig) -> Result<()> {
        if self.pending.is_some() || !self.completed_replies.is_empty() {
            return Err(SessionError::Busy);
        }
        self.gateway
            .enable_connection_pool(config)
            .map_err(SessionError::Gateway)?;
        self.slot_state = std::array::from_fn(|_| None);
        self.completed_replies.clear();
        Ok(())
    }
    pub fn add_pool_slot(&mut self, mut spec: OrderSlotSpec, limits: Limits) -> Result<u16> {
        spec.send_slot = SendSlot::Exclusive;
        let decoder = ResponseDecoder::new(256 * 1024, 128).map_err(SessionError::Transport)?;
        let conn_id = self
            .gateway
            .add_pool_slot(spec, limits)
            .map_err(SessionError::Gateway)?;
        let epoch = self.gateway.slot_route(conn_id).map_or(0, |r| r.epoch);
        self.slot_state[usize::from(conn_id)] = Some(SpotSlotState {
            decoder,
            pending: None,
            last_seen_epoch: epoch,
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
        if idx >= MAX_ORDER_CONNECTIONS || self.slot_state[idx].is_none() {
            return Err(SessionError::Transport(tickwire_transport::Error::Bounds));
        }
        if self.slot_state[idx]
            .as_ref()
            .is_some_and(|s| s.pending.is_some())
        {
            return Err(SessionError::Busy);
        }
        let decoder = ResponseDecoder::new(256 * 1024, 128).map_err(SessionError::Transport)?;
        let epoch = self
            .gateway
            .install_pool_connection(conn_id, connection, peer_addr, limits)
            .map_err(SessionError::Gateway)?;
        let slot = self.slot_state[idx].as_mut().expect("checked slot_state");
        slot.decoder = decoder;
        slot.last_seen_epoch = epoch;
        Ok(epoch)
    }
    pub fn poll_pool_dials(&mut self, now_mono_ns: u64) -> usize {
        let slot_unbusy: [bool; MAX_ORDER_CONNECTIONS] = std::array::from_fn(|idx| {
            self.slot_state[idx]
                .as_ref()
                .is_some_and(|slot| slot.pending.is_none())
        });
        let installed = self
            .gateway
            .poll_pool_dials_filtered(now_mono_ns, |cid| slot_unbusy[usize::from(cid)]);
        if installed > 0 {
            for (idx, slot_opt) in self.slot_state.iter_mut().enumerate() {
                let conn_id = idx as u16;
                if let (Some(slot), Some(route)) =
                    (slot_opt.as_mut(), self.gateway.slot_route(conn_id))
                    && slot.pending.is_none()
                    && route.established
                    && slot.last_seen_epoch != route.epoch
                {
                    if let Ok(dec) = ResponseDecoder::new(256 * 1024, 128) {
                        slot.decoder = dec;
                    }
                    slot.last_seen_epoch = route.epoch;
                }
            }
        }
        installed
    }
    fn reap_aborted_pending(&mut self) {
        if self
            .pending
            .as_ref()
            .is_some_and(|(intent, _, _)| self.gateway.status(*intent) == Some(Status::Aborted))
        {
            self.pending = None;
        }
        for slot in self.slot_state.iter_mut().flatten() {
            if slot
                .pending
                .as_ref()
                .is_some_and(|p| self.gateway.status(p.intent) == Some(Status::Aborted))
            {
                slot.pending = None;
            }
        }
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
    pub fn has_queued_replies(&self) -> bool {
        !self.completed_replies.is_empty()
    }
    pub fn has_ready_slot(&self) -> bool {
        if self.gateway.halted() {
            return false;
        }
        if !self.gateway.has_connection_pool() {
            let pending_active = self.pending.as_ref().is_some_and(|(intent, _, _)| {
                self.gateway.status(*intent) != Some(Status::Aborted)
            });
            return !pending_active && self.gateway.has_transport_connection();
        }
        let Some(pool) = self.gateway.connection_pool() else {
            return false;
        };
        if !pool.router().has_ready_capacity(0) {
            return false;
        }
        for (idx, slot_opt) in self.slot_state.iter().enumerate() {
            let Some(slot) = slot_opt.as_ref() else {
                continue;
            };
            if slot
                .pending
                .as_ref()
                .is_some_and(|p| self.gateway.status(p.intent) != Some(Status::Aborted))
            {
                continue;
            }
            let conn_id = idx as u16;
            if self.gateway.has_slot_connection(conn_id)
                && self.gateway.slot_pending_intent(conn_id).is_none()
                && self
                    .gateway
                    .slot_route(conn_id)
                    .is_some_and(|r| r.established && r.is_ready() && r.matches_product(0))
            {
                return true;
            }
        }
        false
    }
    pub fn pool_needs_poll(&self) -> bool {
        if !self.gateway.has_connection_pool() || self.gateway.halted() {
            return false;
        }
        if !self.completed_replies.is_empty() {
            return true;
        }
        for (idx, slot_opt) in self.slot_state.iter().enumerate() {
            let Some(slot) = slot_opt.as_ref() else {
                continue;
            };
            let conn_id = idx as u16;
            if slot.pending.is_some()
                || (self.gateway.has_slot_connection(conn_id)
                    && !self.gateway.slot_transport_ready(conn_id))
            {
                return true;
            }
        }
        false
    }
    /// Attempts to debit `1` off-hot-path query unit from the configured [`VenueRateLimiter`]
    /// (`REQUEST` + `PER_IP` windows, strictly isolated from `ORDER` and `CANCEL` caps).
    /// Returns `true` when no [`VenueRateLimiter`] is attached or when an egress IP affords the query.
    pub fn try_debit_query_budget(&mut self, now_mono_ns: u64) -> bool {
        let Some(limiter) = self.venue_limiter.as_mut() else {
            return true;
        };
        let mut ips = [IpKey::NONE; MAX_ORDER_CONNECTIONS];
        let count = limiter.local_ips().len().min(MAX_ORDER_CONNECTIONS);
        ips[..count].copy_from_slice(&limiter.local_ips()[..count]);
        for &ip_key in &ips[..count] {
            if limiter.can_afford_query(ip_key, 1, now_mono_ns).is_ok() {
                return limiter.allow_query(ip_key, 1, now_mono_ns).is_ok();
            }
        }
        false
    }
    pub fn has_transport_connection(&self) -> bool {
        if self.gateway.has_connection_pool() {
            self.gateway
                .connection_pool()
                .is_some_and(|p| p.router().established_count() > 0)
        } else {
            self.gateway.has_transport_connection()
        }
    }
    fn has_any_slot_pending(&self) -> bool {
        self.slot_state
            .iter()
            .flatten()
            .any(|s| s.pending.is_some())
    }
    fn preferred_ready_slot_route(&self) -> Option<&OrderRoute> {
        let pool = self.gateway.connection_pool()?;
        let last_conn_id = pool.router().last_conn_id();
        let mut first_eligible: Option<&OrderRoute> = None;
        let mut next_after_last: Option<&OrderRoute> = None;
        let mut best_measured: Option<(&OrderRoute, u32)> = None;
        let mut last_measured_score: Option<u32> = None;

        for (idx, slot_opt) in self.slot_state.iter().enumerate() {
            let Some(slot) = slot_opt.as_ref() else {
                continue;
            };
            if slot.pending.is_some() {
                continue;
            }
            let conn_id = idx as u16;
            if !self.gateway.has_slot_connection(conn_id)
                || self.gateway.slot_pending_intent(conn_id).is_some()
            {
                continue;
            }
            let Some(r) = self.gateway.slot_route(conn_id) else {
                continue;
            };
            if !(r.established && r.is_ready() && r.matches_product(0)) {
                continue;
            }
            if first_eligible.is_none_or(|first| conn_id < first.conn_id) {
                first_eligible = Some(r);
            }
            if let Some(last) = last_conn_id
                && conn_id > last
                && next_after_last.is_none_or(|nxt| conn_id < nxt.conn_id)
            {
                next_after_last = Some(r);
            }
            if let Some(score) = r.health.score() {
                if best_measured
                    .is_none_or(|(best_r, best_s)| (score, conn_id) < (best_s, best_r.conn_id))
                {
                    best_measured = Some((r, score));
                }
                if last_conn_id == Some(conn_id) {
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
            Some((best_r, best_score)) => {
                if let (Some(last), Some(last_score)) = (last_conn_id, last_measured_score)
                    && u64::from(last_score) * 8 <= u64::from(best_score) * 9
                {
                    self.gateway.slot_route(last).or(Some(best_r))
                } else {
                    Some(best_r)
                }
            }
        }
    }
    /// Read-only capacity and rate-limit precheck before staging origins or recovery closures.
    pub fn precheck_submit(
        &mut self,
        command: &v::CanonicalCommand,
        evaluated_at: u64,
    ) -> Result<()> {
        self.reap_aborted_pending();
        let sym_key = symbol_rate_key(command.command().context.instrument.instrument_id);
        let body = &command.command().body;
        if let Some(limiter) = self.venue_limiter.as_mut()
            && limiter.preset() == VenuePreset::OkxSpot
            && matches!(
                body,
                v::CommandBody::New(_) | v::CommandBody::Cancel(_) | v::CommandBody::Modify(_)
            )
        {
            limiter
                .register_symbol(sym_key)
                .map_err(SessionError::RateLimited)?;
        }
        if !self.gateway.has_connection_pool() {
            if self.pending.is_some() {
                return Err(SessionError::Busy);
            }
            if let Some(limiter) = self.venue_limiter.as_ref() {
                let ip_key = resolve_slot_ip_key(limiter, None);
                can_afford_command(limiter, body, ip_key, sym_key, evaluated_at)
                    .map_err(SessionError::RateLimited)?;
            }
            return Ok(());
        }
        if self.gateway.halted() {
            return Err(SessionError::Gateway(
                tickwire_account_gateway::Error::Halted,
            ));
        }
        if !self.has_ready_slot() {
            return Err(SessionError::Busy);
        }
        if let Some(limiter) = self.venue_limiter.as_ref() {
            let any_affordable = (0..MAX_ORDER_CONNECTIONS).any(|idx| {
                let conn_id = idx as u16;
                self.slot_state[idx]
                    .as_ref()
                    .is_some_and(|s| s.pending.is_none())
                    && self.gateway.has_slot_connection(conn_id)
                    && self.gateway.slot_pending_intent(conn_id).is_none()
                    && self.gateway.slot_route(conn_id).is_some_and(|r| {
                        r.established
                            && r.is_ready()
                            && r.matches_product(0)
                            && can_afford_command(
                                limiter,
                                body,
                                resolve_slot_ip_key(limiter, r.local_ip),
                                sym_key,
                                evaluated_at,
                            )
                            .is_ok()
                    })
            });
            if !any_affordable {
                let pref_ip = self.preferred_ready_slot_route().and_then(|r| r.local_ip);
                let ip_key = resolve_slot_ip_key(limiter, pref_ip);
                let reject = can_afford_command(limiter, body, ip_key, sym_key, evaluated_at)
                    .err()
                    .unwrap_or(RateReject::Invalid);
                return Err(SessionError::RateLimited(reject));
            }
        }
        Ok(())
    }
    pub fn install_transport_connection(
        &mut self,
        connection: tickwire_transport::Connection,
        limits: tickwire_transport::Limits,
    ) -> Result<()> {
        if self.pending.is_some() {
            return Err(SessionError::Busy);
        }
        let decoder = ResponseDecoder::new(256 * 1024, 128).map_err(SessionError::Transport)?;
        self.gateway
            .install_transport_connection(connection, limits)
            .map_err(SessionError::Gateway)?;
        self.decoder = decoder;
        Ok(())
    }
    /// Records a deterministic business rejection for an uncreated `New` order (`uncreated = true`),
    /// releasing its Spot reservation without freezing `Account`.
    pub fn record_rejected_new(
        &mut self,
        cmd: &v::CanonicalCommand,
        code: &str,
        received_mono_ns: u64,
    ) -> Result<tickwire_account::Applied> {
        self.record_rejected_new_detail(cmd, code, None, received_mono_ns)
    }

    fn record_rejected_new_detail(
        &mut self,
        cmd: &v::CanonicalCommand,
        code: &str,
        venue_msg: Option<&str>,
        received_mono_ns: u64,
    ) -> Result<tickwire_account::Applied> {
        let c = cmd.command();
        let v::CommandBody::New(order) = &c.body else {
            return Err(SessionError::Identity);
        };
        if received_mono_ns == 0 {
            return Err(SessionError::Identity);
        }
        let venue_code = v::BoundedText::new(code).map_err(|_| SessionError::Identity)?;
        let fallback_msg = format!("spot_new_rejected:{code}");
        let chosen_msg = venue_msg
            .filter(|m| !m.trim().is_empty())
            .and_then(|m| v::BoundedMessage::new(m).ok());
        let message = match chosen_msg {
            Some(m) => m,
            None => v::BoundedMessage::new(fallback_msg).map_err(|_| SessionError::Identity)?,
        };
        let mut seed = Vec::with_capacity(64 + code.len());
        seed.extend_from_slice(b"tickwire.spot.new-reject.v1");
        seed.extend_from_slice(&cmd.digest());
        seed.extend_from_slice(code.as_bytes());
        let hash = ring::digest::digest(&ring::digest::SHA256, &seed);
        let mut input_stream: [u8; 16] = hash.as_ref()[..16].try_into().expect("sha256 prefix");
        if input_stream == [0; 16] {
            input_stream[0] = 1;
        }
        let input_sequence = 1u64;
        let (epoch, received) = self
            .gateway
            .ingress_event(input_stream, input_sequence)
            .map_or((self.connection_epoch, received_mono_ns), |prior| {
                let id = &prior.event().identity;
                (id.source_connection_epoch, id.received_mono_ns)
            });
        let r = &c.context.request;
        let event = v::CanonicalEvent::new(v::VenueEvent {
            identity: v::EventIdentity {
                risk_domain_id: r.risk_domain_id,
                venue: r.venue,
                environment: r.environment,
                account: r.account,
                product: r.product,
                instrument_id: c.context.instrument.instrument_id,
                instrument_version: c.context.instrument.instrument_version,
                source_connection_epoch: epoch,
                source_sequence: Some(input_sequence),
                event_id: v::BoundedText::new(hex(hash.as_ref()))
                    .map_err(|_| SessionError::Identity)?,
                venue_time_ns: None,
                received_mono_ns: received,
            },
            body: v::VenueEventBody::Rejected {
                client_id: Some(order.client_id.clone()),
                venue_code,
                message,
            },
        })
        .map_err(|_| SessionError::Identity)?;
        self.gateway
            .record_ingress_event(input_stream, input_sequence, event)
            .map_err(SessionError::Gateway)
    }

    /// Decodes and records a deterministic `Modified` or `ModifyRejected` ingress event for a
    /// synchronous Bybit `/v5/order/amend` or OKX `/api/v5/trade/amend-order` response.
    ///
    /// Returns `Ok(true)` when a deterministic `Modified` or `ModifyRejected` outcome was applied
    /// to `AccountGateway` without freezing the account, or `Ok(false)` when the HTTP response is
    /// ambiguous or an account-wide hazard requiring caller isolation/freeze handling.
    pub fn record_modify_outcome(
        &mut self,
        cmd: &v::CanonicalCommand,
        http_status: u16,
        raw_body: &[u8],
        received_mono_ns: u64,
    ) -> Result<bool> {
        let c = cmd.command();
        let v::CommandBody::Modify(m) = &c.body else {
            return Err(SessionError::Identity);
        };
        if received_mono_ns == 0 {
            return Err(SessionError::Identity);
        }
        let venue = SpotVenue::from_id(c.context.request.venue).ok_or(SessionError::Identity)?;
        let instrument_id = c.context.instrument.instrument_id;
        let (post_only, tif) = self
            .gateway
            .account()
            .resolve_order_target(instrument_id, &m.target)
            .and_then(|key| self.gateway.account().order_terms(&key))
            .map(|(_, spec)| {
                (
                    spec.time_in_force == Some(v::TimeInForce::PostOnly),
                    spec.time_in_force,
                )
            })
            .unwrap_or((false, None));

        let decoded = decode_modify_reply(cmd, http_status, raw_body, post_only, tif)
            .map_err(SessionError::Report)?;

        let body = match decoded {
            ModifyReply::Unknown => return Ok(false),
            ModifyReply::Modified(mut obs) => {
                if let Some(intent) = obs.intent
                    && self.gateway.account().is_confirmed_modification(intent)
                    && let Some(order_key) = self
                        .gateway
                        .account()
                        .resolve_order_target(instrument_id, &obs.target)
                    && let Some((_, applied_spec)) = self.gateway.account().order_terms(&order_key)
                {
                    // If private WebSocket confirmed this modification (possibly with venue clamping)
                    // before the synchronous HTTP acknowledgement arrived without explicit price/qty,
                    // preserve the already-confirmed terms so ingress deduplication succeeds cleanly.
                    if obs.new_price == m.price
                        && let Some(px) = applied_spec.price
                    {
                        obs.new_price = px;
                    }
                    if obs.new_quantity == m.quantity
                        && let v::Quantity::Base(q) = applied_spec.quantity
                    {
                        obs.new_quantity = q;
                    }
                }
                v::VenueEventBody::Modified(Box::new(obs))
            }
            ModifyReply::Rejected {
                rejection,
                code_str,
                ..
            } => {
                if is_http_account_hazard(http_status) {
                    return Ok(false);
                }
                match classify_issue(venue, &code_str) {
                    SpotIssueKind::DeterministicReject | SpotIssueKind::NotFound => {
                        v::VenueEventBody::ModifyRejected(Box::new(rejection))
                    }
                    SpotIssueKind::Ambiguous | SpotIssueKind::AccountHazard => return Ok(false),
                }
            }
        };

        let mut seed = Vec::with_capacity(64);
        seed.extend_from_slice(b"tickwire.spot.modify-outcome.v1");
        seed.extend_from_slice(&cmd.digest());
        let hash = ring::digest::digest(&ring::digest::SHA256, &seed);
        let mut input_stream: [u8; 16] = hash.as_ref()[..16].try_into().expect("sha256 prefix");
        if input_stream == [0; 16] {
            input_stream[0] = 1;
        }
        let input_sequence = 1u64;
        let (epoch, received) = self
            .gateway
            .ingress_event(input_stream, input_sequence)
            .map_or((self.connection_epoch, received_mono_ns), |prior| {
                let id = &prior.event().identity;
                (id.source_connection_epoch, id.received_mono_ns)
            });
        let r = &c.context.request;
        let event = v::CanonicalEvent::new(v::VenueEvent {
            identity: v::EventIdentity {
                risk_domain_id: r.risk_domain_id,
                venue: r.venue,
                environment: r.environment,
                account: r.account,
                product: r.product,
                instrument_id,
                instrument_version: c.context.instrument.instrument_version,
                source_connection_epoch: epoch,
                source_sequence: Some(input_sequence),
                event_id: v::BoundedText::new(hex(hash.as_ref()))
                    .map_err(|_| SessionError::Identity)?,
                venue_time_ns: None,
                received_mono_ns: received,
            },
            body,
        })
        .map_err(|_| SessionError::Identity)?;
        self.gateway
            .record_ingress_event(input_stream, input_sequence, event)
            .map_err(SessionError::Gateway)?;
        Ok(true)
    }
    pub fn reactivate(
        &mut self,
        binding: tickwire_account::Reactivation,
        connection: Option<tickwire_account_gateway::ConnectionConfig>,
        verifier: &mut impl tickwire_account_gateway::ReactivationVerifier,
    ) -> Result<tickwire_account_gateway::ActivationReceipt> {
        let epoch = binding.execution_connection_epoch;
        if self.pending.is_some()
            || self.has_any_slot_pending()
            || !self.completed_replies.is_empty()
            || (connection.is_some() && epoch <= self.connection_epoch)
            || (connection.is_none() && epoch != self.connection_epoch)
        {
            return Err(SessionError::Identity);
        }
        let decoder = ResponseDecoder::new(256 * 1024, 128).map_err(SessionError::Transport)?;
        let receipt = self
            .gateway
            .reactivate(binding, connection, verifier)
            .map_err(SessionError::Gateway)?;
        self.decoder = decoder;
        if self.gateway.has_connection_pool()
            && self.gateway.has_slot_connection(0)
            && self.slot_state[0].is_none()
        {
            let slot0_dec =
                ResponseDecoder::new(256 * 1024, 128).map_err(SessionError::Transport)?;
            let slot0_epoch = self.gateway.slot_route(0).map_or(0, |r| r.epoch);
            self.slot_state[0] = Some(SpotSlotState {
                decoder: slot0_dec,
                pending: None,
                last_seen_epoch: slot0_epoch,
            });
        }
        self.connection_epoch = epoch;
        Ok(receipt)
    }
    /// Stops output and freezes the account when the owner loses its trusted input or routing identity.
    pub fn stop_sending(&mut self) {
        self.gateway.stop_sending();
    }
    /// Records one authenticated private trade through the same durable account owner.
    pub fn record_fill(
        &mut self,
        source: crate::FillSource<'_>,
        row: &[u8],
    ) -> Result<tickwire_account::Applied> {
        // Re-reading an old source position must reproduce the bytes written by that
        // version. Legacy trade-only observations encoded the missing lifecycle as Unknown.
        let legacy = self.gateway.ingress_event(source.input_stream, source.input_sequence)
            .is_some_and(|e| matches!(&e.event().body, v::VenueEventBody::Fill(f) if f.observation.status == v::OrderStatus::Unknown));
        let event = self
            .gateway
            .command(source.intent)
            .ok_or(ReportError::Identity)
            .and_then(|cmd| {
                crate::fills::normalize(
                    cmd,
                    &source,
                    &self.gateway.baseline().snapshot().assets,
                    row,
                    if legacy {
                        v::OrderStatus::Unknown
                    } else {
                        v::OrderStatus::Unreported
                    },
                )
            });
        let event = match event {
            Ok(e) => e,
            Err(e) => {
                self.gateway.freeze_account();
                return Err(SessionError::Report(e));
            }
        };
        self.gateway
            .record_ingress_event(source.input_stream, source.input_sequence, event)
            .map_err(SessionError::Gateway)
    }
    pub fn submit(
        &mut self,
        command: v::CanonicalCommand,
        evaluated_at: u64,
        deadline: Instant,
    ) -> Result<Status> {
        self.reap_aborted_pending();
        let r = &command.command().context.request;
        let intent = IntentKey {
            epoch: r.intent_exec_epoch,
            intent_id: r.intent_id,
        };
        // Let the gateway enforce same-byte retries and freeze conflicting reuse,
        // including while the original request is waiting for its HTTP response.
        if self.gateway.command(intent).is_some() {
            if self.gateway.has_connection_pool() {
                let conn_id = self.gateway.intent_slot(intent).map_or(0, |(cid, _)| cid);
                return self
                    .gateway
                    .submit_on_slot(conn_id, command, evaluated_at, deadline)
                    .map(|(status, _)| status)
                    .map_err(SessionError::Gateway);
            }
            return self
                .gateway
                .submit(command, evaluated_at, deadline)
                .map_err(SessionError::Gateway);
        }
        let sym_key = symbol_rate_key(command.command().context.instrument.instrument_id);
        if !self.gateway.has_connection_pool() {
            self.precheck_submit(&command, evaluated_at)?;
            let result = self
                .gateway
                .submit(command.clone(), evaluated_at, deadline)
                .map_err(SessionError::Gateway)?;
            if result != Status::Aborted {
                if let Some(limiter) = self.venue_limiter.as_mut() {
                    let ip_key = resolve_slot_ip_key(limiter, None);
                    allow_command(
                        limiter,
                        &command.command().body,
                        ip_key,
                        sym_key,
                        evaluated_at,
                    )
                    .map_err(SessionError::RateLimited)?;
                }
                self.pending = Some((intent, command, deadline));
                self.warmup_tracker.record_activity(evaluated_at);
            }
            return Ok(result);
        }

        if self.gateway.halted() {
            return Err(SessionError::Gateway(
                tickwire_account_gateway::Error::Halted,
            ));
        }
        if !self.has_ready_slot() {
            return Err(SessionError::Busy);
        }
        if let Some(limiter) = self.venue_limiter.as_mut()
            && limiter.preset() == VenuePreset::OkxSpot
            && matches!(
                command.command().body,
                v::CommandBody::New(_) | v::CommandBody::Cancel(_) | v::CommandBody::Modify(_)
            )
        {
            limiter
                .register_symbol(sym_key)
                .map_err(SessionError::RateLimited)?;
        }
        let body = &command.command().body;
        let picked = {
            let Self {
                gateway,
                slot_state,
                venue_limiter,
                ..
            } = self;
            gateway.pick_pool_slot_filtered(0, |route| {
                let idx = usize::from(route.conn_id);
                if !slot_state[idx]
                    .as_ref()
                    .is_some_and(|s| s.pending.is_none())
                {
                    return false;
                }
                let Some(limiter) = venue_limiter.as_ref() else {
                    return true;
                };
                let ip_key = resolve_slot_ip_key(limiter, route.local_ip);
                can_afford_command(limiter, body, ip_key, sym_key, evaluated_at).is_ok()
            })
        };
        let Some(conn_id) = picked else {
            if let Some(limiter) = self.venue_limiter.as_ref() {
                let pref_ip = self.preferred_ready_slot_route().and_then(|r| r.local_ip);
                let ip_key = resolve_slot_ip_key(limiter, pref_ip);
                let reject = can_afford_command(limiter, body, ip_key, sym_key, evaluated_at)
                    .err()
                    .unwrap_or(RateReject::Invalid);
                return Err(SessionError::RateLimited(reject));
            }
            return Err(SessionError::Busy);
        };
        let chosen_local_ip = self.gateway.slot_route(conn_id).and_then(|r| r.local_ip);
        let (status, slot_epoch) = self
            .gateway
            .submit_on_slot(conn_id, command.clone(), evaluated_at, deadline)
            .map_err(SessionError::Gateway)?;
        if status != Status::Aborted {
            if let Some(limiter) = self.venue_limiter.as_mut() {
                let ip_key = resolve_slot_ip_key(limiter, chosen_local_ip);
                allow_command(
                    limiter,
                    &command.command().body,
                    ip_key,
                    sym_key,
                    evaluated_at,
                )
                .map_err(SessionError::RateLimited)?;
            }
            if let Some(slot) = self.slot_state[usize::from(conn_id)].as_mut() {
                slot.pending = Some(SpotPending {
                    intent,
                    command,
                    deadline,
                    sent_mono_ns: evaluated_at,
                    slot_epoch,
                });
            }
            self.warmup_tracker.record_activity(evaluated_at);
        }
        Ok(status)
    }
    /// Drives TLS across single-flight or pooled slots and returns `(IntentKey, Reply)` on completion.
    pub fn poll_tagged(
        &mut self,
        now: Instant,
        received_mono_ns: u64,
        budget: IoBudget,
    ) -> Result<Option<(IntentKey, Reply)>> {
        if budget.max_bytes == 0 || budget.max_syscalls == 0 || received_mono_ns == 0 {
            return Err(SessionError::Identity);
        }
        if !self.gateway.has_connection_pool() {
            let pending_intent = self.pending.as_ref().map(|(intent, _, _)| *intent);
            let result = self.drive(now, received_mono_ns, budget);
            if result.is_err() {
                self.gateway.stop_sending();
            }
            return match result? {
                Some(reply) => {
                    self.warmup_tracker.record_activity(received_mono_ns);
                    let intent = pending_intent.ok_or(SessionError::Identity)?;
                    Ok(Some((intent, reply)))
                }
                None => Ok(None),
            };
        }
        let result = self.drive_pool(now, received_mono_ns, budget);
        if let Err(err) = result {
            self.gateway.stop_sending();
            return Err(err);
        }
        let out = self.completed_replies.pop_front();
        if out.is_some() {
            self.warmup_tracker.record_activity(received_mono_ns);
        }
        Ok(out)
    }
    /// Drives TLS with a bounded byte/syscall budget. A missing reply never re-submits.
    pub fn poll(
        &mut self,
        now: Instant,
        received_mono_ns: u64,
        budget: IoBudget,
    ) -> Result<Option<Reply>> {
        if self.gateway.has_connection_pool() {
            return Ok(self
                .poll_tagged(now, received_mono_ns, budget)?
                .map(|(_, reply)| reply));
        }
        if budget.max_bytes == 0 || budget.max_syscalls == 0 || received_mono_ns == 0 {
            return Err(SessionError::Identity);
        }
        let result = self.drive(now, received_mono_ns, budget);
        if result.is_err() {
            self.gateway.stop_sending();
        }
        if let Ok(Some(_)) = &result {
            self.warmup_tracker.record_activity(received_mono_ns);
        }
        result
    }
    fn doubt_reason_for_command(cmd: &v::CanonicalCommand) -> DoubtReason {
        match &cmd.command().body {
            v::CommandBody::Cancel(_) => DoubtReason::CancelAmbiguous,
            v::CommandBody::Modify(_) | v::CommandBody::AmendKeepPriority { .. } => {
                DoubtReason::AmendAmbiguous
            }
            _ => DoubtReason::PlaceAmbiguous,
        }
    }
    fn drain_retained_receive(&mut self) {
        let mut drain = [0u8; 4096];
        while self.gateway.read(&mut drain) > 0 {}
    }
    fn isolate_broken_transport_pending(
        &mut self,
        intent: IntentKey,
        cmd: &v::CanonicalCommand,
    ) -> Result<Option<Reply>> {
        self.gateway.drop_broken_transport_isolated();
        self.drain_retained_receive();
        self.decoder = ResponseDecoder::new(256 * 1024, 128).map_err(SessionError::Transport)?;
        if self.gateway.status(intent) == Some(Status::Aborted) {
            return Ok(None);
        }
        self.gateway
            .isolate_intent_in_doubt(intent, Self::doubt_reason_for_command(cmd))
            .map_err(SessionError::Gateway)?;
        Ok(Some(Reply::Unknown))
    }
    fn isolate_broken_pool_slot(
        &mut self,
        conn_id: u16,
        intent: IntentKey,
        cmd: &v::CanonicalCommand,
        received_mono_ns: u64,
    ) -> Result<()> {
        let _ = self
            .gateway
            .drop_broken_slot_isolated(conn_id, received_mono_ns, true);
        self.gateway.drain_slot_retained_receive(conn_id);
        let fresh_decoder =
            ResponseDecoder::new(256 * 1024, 128).map_err(SessionError::Transport)?;
        if let Some(slot) = self.slot_state[usize::from(conn_id)].as_mut() {
            slot.decoder = fresh_decoder;
        }
        if self.gateway.status(intent) == Some(Status::Aborted) {
            return Ok(());
        }
        self.gateway
            .isolate_intent_in_doubt(intent, Self::doubt_reason_for_command(cmd))
            .map_err(SessionError::Gateway)?;
        self.completed_replies.push_back((intent, Reply::Unknown));
        Ok(())
    }
    fn drive_pool(&mut self, now: Instant, received_mono_ns: u64, budget: IoBudget) -> Result<()> {
        let _ = self.poll_pool_dials(received_mono_ns);
        let mut buffer = [0u8; 4096];
        for idx in 0..MAX_ORDER_CONNECTIONS {
            if self.slot_state[idx].is_none() {
                continue;
            }
            let conn_id = idx as u16;
            let stale_pending_epoch = self.slot_state[idx]
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
                let pending = self.slot_state[idx]
                    .as_mut()
                    .and_then(|s| s.pending.take())
                    .expect("checked pending");
                let fresh_decoder =
                    ResponseDecoder::new(256 * 1024, 128).map_err(SessionError::Transport)?;
                if let Some(slot) = self.slot_state[idx].as_mut() {
                    slot.decoder = fresh_decoder;
                }
                if self.gateway.status(pending.intent) != Some(Status::Aborted) {
                    let _ = self
                        .gateway
                        .release_slot_inflight(conn_id, pending.slot_epoch);
                    self.gateway
                        .isolate_intent_in_doubt(
                            pending.intent,
                            Self::doubt_reason_for_command(&pending.command),
                        )
                        .map_err(SessionError::Gateway)?;
                    self.completed_replies
                        .push_back((pending.intent, Reply::Unknown));
                }
                continue;
            }
            let has_conn = self.gateway.has_slot_connection(conn_id);
            let has_pending = self.slot_state[idx]
                .as_ref()
                .is_some_and(|s| s.pending.is_some());
            if !has_conn && !has_pending {
                self.gateway.drain_slot_retained_receive(conn_id);
                continue;
            }
            let slot_local_ip = self.gateway.slot_route(conn_id).and_then(|r| r.local_ip);
            let transport = if has_conn {
                self.gateway.poll_slot(conn_id, now, budget)
            } else {
                Ok(())
            };
            if self.slot_state[idx]
                .as_ref()
                .and_then(|s| s.pending.as_ref())
                .is_some_and(|p| self.gateway.status(p.intent) == Some(Status::Aborted))
            {
                if !self.gateway.has_slot_connection(conn_id) {
                    self.gateway.drain_slot_retained_receive(conn_id);
                    let fresh_decoder =
                        ResponseDecoder::new(256 * 1024, 128).map_err(SessionError::Transport)?;
                    if let Some(slot) = self.slot_state[idx].as_mut() {
                        slot.decoder = fresh_decoder;
                    }
                }
                if let Some(slot) = self.slot_state[idx].as_mut() {
                    slot.pending = None;
                }
                continue;
            }
            let mut remaining = budget.max_bytes;
            let mut exhausted = false;
            let mut decode_err = None;
            while remaining != 0 {
                let cap = remaining.min(buffer.len());
                let n = self.gateway.read_slot(conn_id, &mut buffer[..cap]);
                if n == 0 {
                    exhausted = true;
                    break;
                }
                if self.slot_state[idx]
                    .as_ref()
                    .is_none_or(|s| s.pending.is_none())
                {
                    return Err(SessionError::Identity);
                }
                if let Err(e) = self.slot_state[idx]
                    .as_mut()
                    .expect("checked slot_state")
                    .decoder
                    .push(&buffer[..n])
                {
                    decode_err = Some(e);
                    break;
                }
                remaining -= n;
            }
            if let Some(e) = decode_err {
                if let Some(pending) = self.slot_state[idx].as_mut().and_then(|s| s.pending.take())
                {
                    self.isolate_broken_pool_slot(
                        conn_id,
                        pending.intent,
                        &pending.command,
                        received_mono_ns,
                    )?;
                    continue;
                }
                return Err(SessionError::Transport(e));
            }
            if exhausted && (!self.gateway.has_slot_connection(conn_id) || self.gateway.halted()) {
                self.slot_state[idx]
                    .as_mut()
                    .expect("checked slot_state")
                    .decoder
                    .finish();
            }
            let next = match self.slot_state[idx]
                .as_mut()
                .expect("checked slot_state")
                .decoder
                .next_response()
            {
                Ok(next) => next,
                Err(e) => {
                    if let Some(pending) =
                        self.slot_state[idx].as_mut().and_then(|s| s.pending.take())
                    {
                        self.isolate_broken_pool_slot(
                            conn_id,
                            pending.intent,
                            &pending.command,
                            received_mono_ns,
                        )?;
                        continue;
                    }
                    return Err(SessionError::Transport(e));
                }
            };
            if let Some(raw) = next {
                let pending = self.slot_state[idx]
                    .as_mut()
                    .and_then(|s| s.pending.take())
                    .ok_or(SessionError::Identity)?;
                let buffered_non_empty = !self.slot_state[idx]
                    .as_mut()
                    .expect("checked slot_state")
                    .decoder
                    .take_buffered()
                    .is_empty();
                if self
                    .gateway
                    .output_progress(pending.intent)
                    .is_none_or(|p| p.accepted_bytes != p.total_bytes)
                    || buffered_non_empty
                {
                    return Err(SessionError::Identity);
                }
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
                if !self.gateway.has_slot_connection(conn_id) {
                    self.gateway.drain_slot_retained_receive(conn_id);
                    let fresh_decoder =
                        ResponseDecoder::new(256 * 1024, 128).map_err(SessionError::Transport)?;
                    if let Some(slot) = self.slot_state[idx].as_mut() {
                        slot.decoder = fresh_decoder;
                    }
                }
                let reply = decode_reply(&pending.command, raw.status, &raw.body)
                    .map_err(SessionError::Report)?;
                let venue = SpotVenue::from_id(pending.command.command().context.request.venue)
                    .ok_or(SessionError::Identity)?;
                let is_rate_limit = raw.status == 429
                    || matches!(&reply, Reply::RequestIssue { code } if is_venue_rate_limit_code(venue, code));
                let reason = Self::doubt_reason_for_command(&pending.command);
                if is_rate_limit
                    && let (Some(limiter), Some(ip)) = (self.venue_limiter.as_mut(), slot_local_ip)
                {
                    let cooldown_until_ns =
                        received_mono_ns.saturating_add(parse_retry_after_ns(&raw));
                    let ip_key = resolve_slot_ip_key(limiter, Some(ip));
                    let _ = limiter.note_ip_cooldown(ip_key, cooldown_until_ns);
                    self.gateway
                        .isolate_intent_in_doubt(pending.intent, reason)
                        .map_err(SessionError::Gateway)?;
                    self.completed_replies.push_back((pending.intent, reply));
                    continue;
                }
                let http_hazard = is_http_account_hazard(raw.status);
                match &reply {
                    Reply::Order(snapshot) => {
                        self.observe(&pending.command, snapshot, received_mono_ns)?;
                    }
                    Reply::Acknowledged { .. } => {
                        if matches!(pending.command.command().body, v::CommandBody::Modify(_)) {
                            self.record_modify_outcome(
                                &pending.command,
                                raw.status,
                                &raw.body,
                                received_mono_ns,
                            )?;
                        }
                    }
                    Reply::RequestIssue { code } => {
                        let kind = classify_issue(venue, code);
                        if http_hazard || kind == SpotIssueKind::AccountHazard {
                            self.gateway.freeze_account();
                        } else {
                            match (&pending.command.command().body, kind) {
                                (v::CommandBody::New(_), SpotIssueKind::DeterministicReject) => {
                                    let venue_msg = extract_error_message(venue, &raw.body);
                                    self.record_rejected_new_detail(
                                        &pending.command,
                                        code,
                                        venue_msg.as_deref(),
                                        received_mono_ns,
                                    )?;
                                }
                                (v::CommandBody::Modify(_), _) => {
                                    if !self.record_modify_outcome(
                                        &pending.command,
                                        raw.status,
                                        &raw.body,
                                        received_mono_ns,
                                    )? {
                                        self.gateway
                                            .isolate_intent_in_doubt(
                                                pending.intent,
                                                DoubtReason::AmendAmbiguous,
                                            )
                                            .map_err(SessionError::Gateway)?;
                                    }
                                }
                                (
                                    v::CommandBody::Cancel(_),
                                    SpotIssueKind::DeterministicReject | SpotIssueKind::NotFound,
                                ) => {}
                                (
                                    v::CommandBody::New(_),
                                    SpotIssueKind::Ambiguous | SpotIssueKind::NotFound,
                                ) => {
                                    self.gateway
                                        .isolate_intent_in_doubt(
                                            pending.intent,
                                            DoubtReason::PlaceAmbiguous,
                                        )
                                        .map_err(SessionError::Gateway)?;
                                }
                                (v::CommandBody::Cancel(_), SpotIssueKind::Ambiguous) => {
                                    self.gateway
                                        .isolate_intent_in_doubt(
                                            pending.intent,
                                            DoubtReason::CancelAmbiguous,
                                        )
                                        .map_err(SessionError::Gateway)?;
                                }
                                _ => {
                                    self.gateway
                                        .isolate_intent_in_doubt(pending.intent, reason)
                                        .map_err(SessionError::Gateway)?;
                                }
                            }
                        }
                    }
                    Reply::Unknown => {
                        if http_hazard {
                            self.gateway.freeze_account();
                        } else {
                            self.gateway
                                .isolate_intent_in_doubt(pending.intent, reason)
                                .map_err(SessionError::Gateway)?;
                        }
                    }
                }
                self.completed_replies.push_back((pending.intent, reply));
                continue;
            }
            let should_check_pending = self.slot_state[idx]
                .as_ref()
                .is_some_and(|s| s.pending.is_some());
            if should_check_pending {
                let (intent, deadline) = {
                    let p = self.slot_state[idx]
                        .as_ref()
                        .and_then(|s| s.pending.as_ref())
                        .expect("checked pending");
                    (p.intent, p.deadline)
                };
                if self.gateway.status(intent) == Some(Status::Aborted) {
                    if !self.gateway.has_slot_connection(conn_id) {
                        self.gateway.drain_slot_retained_receive(conn_id);
                        let fresh_decoder = ResponseDecoder::new(256 * 1024, 128)
                            .map_err(SessionError::Transport)?;
                        if let Some(slot) = self.slot_state[idx].as_mut() {
                            slot.decoder = fresh_decoder;
                        }
                    }
                    if let Some(slot) = self.slot_state[idx].as_mut() {
                        slot.pending = None;
                    }
                } else if now >= deadline
                    || exhausted
                        && (!self.gateway.has_slot_connection(conn_id)
                            || self.gateway.halted()
                            || transport.is_err())
                {
                    let pending = self.slot_state[idx]
                        .as_mut()
                        .and_then(|s| s.pending.take())
                        .expect("checked pending");
                    self.isolate_broken_pool_slot(
                        conn_id,
                        pending.intent,
                        &pending.command,
                        received_mono_ns,
                    )?;
                }
            } else if !self.gateway.has_slot_connection(conn_id) && !self.gateway.halted() {
                self.gateway.drain_slot_retained_receive(conn_id);
                let fresh_decoder =
                    ResponseDecoder::new(256 * 1024, 128).map_err(SessionError::Transport)?;
                if let Some(slot) = self.slot_state[idx].as_mut() {
                    slot.decoder = fresh_decoder;
                }
            } else if let Err(e) = transport
                && self.gateway.halted()
            {
                return Err(SessionError::Gateway(e));
            }
        }
        let _ = self.poll_pool_dials(received_mono_ns);
        Ok(())
    }
    fn drive(
        &mut self,
        now: Instant,
        received_mono_ns: u64,
        budget: IoBudget,
    ) -> Result<Option<Reply>> {
        let isolated = self.gateway.per_order_doubt_isolation();
        if isolated
            && !self.gateway.halted()
            && !self.gateway.has_transport_connection()
            && self.pending.is_none()
        {
            self.drain_retained_receive();
            return Ok(None);
        }
        let transport = self.gateway.poll(now, budget);
        let mut buffer = [0; 4096];
        let mut remaining = budget.max_bytes;
        let mut exhausted = false;
        let mut decode_err = None;
        while remaining != 0 {
            let cap = remaining.min(buffer.len());
            let n = self.gateway.read(&mut buffer[..cap]);
            if n == 0 {
                exhausted = true;
                break;
            }
            if self.pending.is_none() {
                return Err(SessionError::Identity);
            }
            if let Err(e) = self.decoder.push(&buffer[..n]) {
                decode_err = Some(e);
                break;
            }
            remaining -= n;
        }
        if !isolated {
            if let Some(e) = decode_err {
                return Err(SessionError::Transport(e));
            }
            if exhausted && self.gateway.halted() {
                self.decoder.finish();
            }
            if let Some(raw) = self
                .decoder
                .next_response()
                .map_err(SessionError::Transport)?
            {
                let (intent, cmd, _) = self.pending.take().ok_or(SessionError::Identity)?;
                if self
                    .gateway
                    .output_progress(intent)
                    .is_none_or(|p| p.accepted_bytes != p.total_bytes)
                    || !self.decoder.take_buffered().is_empty()
                {
                    return Err(SessionError::Identity);
                }
                let reply =
                    decode_reply(&cmd, raw.status, &raw.body).map_err(SessionError::Report)?;
                match &reply {
                    Reply::Order(snapshot) => self.observe(&cmd, snapshot, received_mono_ns)?,
                    Reply::Acknowledged { .. } => {
                        if matches!(cmd.command().body, v::CommandBody::Modify(_)) {
                            self.record_modify_outcome(
                                &cmd,
                                raw.status,
                                &raw.body,
                                received_mono_ns,
                            )?;
                        }
                    }
                    Reply::RequestIssue { .. }
                        if matches!(cmd.command().body, v::CommandBody::Modify(_)) =>
                    {
                        if !self.record_modify_outcome(
                            &cmd,
                            raw.status,
                            &raw.body,
                            received_mono_ns,
                        )? {
                            self.gateway.freeze_account();
                        }
                    }
                    Reply::Unknown | Reply::RequestIssue { .. } => self.gateway.freeze_account(),
                }
                return Ok(Some(reply));
            }
            if let Some((key, _, deadline)) = &self.pending {
                if self.gateway.status(*key) == Some(Status::Aborted) {
                    self.pending = None;
                    return Ok(None);
                }
                if now >= *deadline || exhausted && (self.gateway.halted() || transport.is_err()) {
                    self.gateway.stop_sending();
                    self.pending = None;
                    return Ok(Some(Reply::Unknown));
                }
            } else if let Err(e) = transport {
                return Err(SessionError::Gateway(e));
            }
            return Ok(None);
        }

        if let Some(e) = decode_err {
            if self.pending.is_some()
                && (!self.gateway.has_transport_connection() || transport.is_err())
            {
                let (intent, cmd, _) = self.pending.take().ok_or(SessionError::Identity)?;
                return self.isolate_broken_transport_pending(intent, &cmd);
            }
            return Err(SessionError::Transport(e));
        }
        if exhausted && (!self.gateway.has_transport_connection() || self.gateway.halted()) {
            self.decoder.finish();
        }
        let next = match self.decoder.next_response() {
            Ok(next) => next,
            Err(e) => {
                if self.pending.is_some()
                    && (!self.gateway.has_transport_connection() || transport.is_err())
                {
                    let (intent, cmd, _) = self.pending.take().ok_or(SessionError::Identity)?;
                    return self.isolate_broken_transport_pending(intent, &cmd);
                }
                return Err(SessionError::Transport(e));
            }
        };
        if let Some(raw) = next {
            let (intent, cmd, _) = self.pending.take().ok_or(SessionError::Identity)?;
            if self
                .gateway
                .output_progress(intent)
                .is_none_or(|p| p.accepted_bytes != p.total_bytes)
                || !self.decoder.take_buffered().is_empty()
            {
                return Err(SessionError::Identity);
            }
            if !self.gateway.has_transport_connection() {
                self.drain_retained_receive();
                self.decoder =
                    ResponseDecoder::new(256 * 1024, 128).map_err(SessionError::Transport)?;
            }
            let reply = decode_reply(&cmd, raw.status, &raw.body).map_err(SessionError::Report)?;
            let venue = SpotVenue::from_id(cmd.command().context.request.venue)
                .ok_or(SessionError::Identity)?;
            let http_hazard = is_http_account_hazard(raw.status);
            let reason = Self::doubt_reason_for_command(&cmd);
            match &reply {
                Reply::Order(snapshot) => self.observe(&cmd, snapshot, received_mono_ns)?,
                Reply::Acknowledged { .. } => {
                    if matches!(cmd.command().body, v::CommandBody::Modify(_)) {
                        self.record_modify_outcome(&cmd, raw.status, &raw.body, received_mono_ns)?;
                    }
                }
                Reply::RequestIssue { code } => {
                    let kind = classify_issue(venue, code);
                    if http_hazard || kind == SpotIssueKind::AccountHazard {
                        self.gateway.freeze_account();
                    } else {
                        match (&cmd.command().body, kind) {
                            (v::CommandBody::New(_), SpotIssueKind::DeterministicReject) => {
                                let venue_msg = extract_error_message(venue, &raw.body);
                                self.record_rejected_new_detail(
                                    &cmd,
                                    code,
                                    venue_msg.as_deref(),
                                    received_mono_ns,
                                )?;
                            }
                            (v::CommandBody::Modify(_), _) => {
                                if !self.record_modify_outcome(
                                    &cmd,
                                    raw.status,
                                    &raw.body,
                                    received_mono_ns,
                                )? {
                                    self.gateway
                                        .isolate_intent_in_doubt(
                                            intent,
                                            DoubtReason::AmendAmbiguous,
                                        )
                                        .map_err(SessionError::Gateway)?;
                                }
                            }
                            (
                                v::CommandBody::Cancel(_),
                                SpotIssueKind::DeterministicReject | SpotIssueKind::NotFound,
                            ) => {}
                            (
                                v::CommandBody::New(_),
                                SpotIssueKind::Ambiguous | SpotIssueKind::NotFound,
                            ) => {
                                self.gateway
                                    .isolate_intent_in_doubt(intent, DoubtReason::PlaceAmbiguous)
                                    .map_err(SessionError::Gateway)?;
                            }
                            (v::CommandBody::Cancel(_), SpotIssueKind::Ambiguous) => {
                                self.gateway
                                    .isolate_intent_in_doubt(intent, DoubtReason::CancelAmbiguous)
                                    .map_err(SessionError::Gateway)?;
                            }
                            _ => {
                                self.gateway
                                    .isolate_intent_in_doubt(intent, reason)
                                    .map_err(SessionError::Gateway)?;
                            }
                        }
                    }
                }
                Reply::Unknown => {
                    if http_hazard {
                        self.gateway.freeze_account();
                    } else {
                        self.gateway
                            .isolate_intent_in_doubt(intent, reason)
                            .map_err(SessionError::Gateway)?;
                    }
                }
            }
            return Ok(Some(reply));
        }
        if let Some((key, _, deadline)) = &self.pending {
            if self.gateway.status(*key) == Some(Status::Aborted) {
                if !self.gateway.has_transport_connection() {
                    self.drain_retained_receive();
                    self.decoder =
                        ResponseDecoder::new(256 * 1024, 128).map_err(SessionError::Transport)?;
                }
                self.pending = None;
                return Ok(None);
            }
            if now >= *deadline
                || exhausted
                    && (!self.gateway.has_transport_connection()
                        || self.gateway.halted()
                        || transport.is_err())
            {
                let (intent, cmd, _) = self.pending.take().ok_or(SessionError::Identity)?;
                return self.isolate_broken_transport_pending(intent, &cmd);
            }
        } else if !self.gateway.has_transport_connection() && !self.gateway.halted() {
            self.drain_retained_receive();
            self.decoder =
                ResponseDecoder::new(256 * 1024, 128).map_err(SessionError::Transport)?;
            return Ok(None);
        } else if let Err(e) = transport {
            return Err(SessionError::Gateway(e));
        }
        Ok(None)
    }
    fn observe(
        &mut self,
        cmd: &v::CanonicalCommand,
        snapshot: &OrderSnapshot,
        received: u64,
    ) -> Result<()> {
        let c = cmd.command();
        let v::CommandBody::Query(target) = &c.body else {
            return Err(SessionError::Identity);
        };
        let client = snapshot.client_id.as_ref().ok_or(SessionError::Identity)?;
        let resolved_key = self
            .gateway
            .account()
            .resolve_order_target(c.context.instrument.instrument_id, target)
            .filter(|k| k.intent.epoch == target.original_intent_exec_epoch);
        let order_key = if let Some(key) = resolved_key {
            key
        } else {
            let mut candidates = self.gateway.account().order_keys().filter(|k| {
                k.intent.epoch == target.original_intent_exec_epoch
                    && k.client_id == *client
                    && self.gateway.command(k.intent).is_some_and(|original| {
                        original.command().context.instrument == c.context.instrument
                    })
            });
            let found = candidates.next().ok_or(SessionError::Identity)?.clone();
            if candidates.next().is_some() {
                return Err(SessionError::Identity);
            }
            found
        };
        let original = order_key.intent;
        let (inst, spec) = self
            .gateway
            .account()
            .order_terms(&order_key)
            .ok_or(SessionError::Identity)?;
        if *inst != c.context.instrument {
            return Err(SessionError::Identity);
        }
        let v::Quantity::Base(size) = spec.quantity else {
            return Err(SessionError::Identity);
        };
        let executed = snapshot.cumulative_base;
        let consistent = match snapshot.status {
            v::OrderStatus::Filled => executed == size,
            v::OrderStatus::New | v::OrderStatus::Rejected => !executed.is_positive(),
            v::OrderStatus::PartiallyFilled => executed.is_positive() && executed != size,
            _ => true,
        };
        if !consistent || spec.side != snapshot.side {
            return Err(SessionError::Identity);
        }
        let r = &c.context.request;
        let event = v::CanonicalEvent::new(v::VenueEvent {
            identity: v::EventIdentity {
                risk_domain_id: r.risk_domain_id,
                venue: r.venue,
                environment: r.environment,
                account: r.account,
                product: r.product,
                instrument_id: c.context.instrument.instrument_id,
                instrument_version: c.context.instrument.instrument_version,
                source_connection_epoch: self.connection_epoch,
                source_sequence: None,
                event_id: v::BoundedText::new(hex(&cmd.digest()))
                    .map_err(|_| SessionError::Identity)?,
                venue_time_ns: Some(snapshot.venue_time_ns),
                received_mono_ns: received,
            },
            body: v::VenueEventBody::Order(Box::new(v::OrderObservation {
                order: v::OrderIdentity {
                    order_id: target.order_id,
                    original_intent_id: original.intent_id,
                    original_intent_exec_epoch: original.epoch,
                    client_id: Some(client.clone()),
                    venue_order_id: Some(snapshot.venue_order_id.clone()),
                    parent_order_id: None,
                    list_id: None,
                    venue_algo_id: None,
                    venue_list_id: None,
                    list_client_id: None,
                    algo_client_id: None,
                },
                status: snapshot.status,
                side: snapshot.side,
                position_side: v::PositionSide::Both,
                original_quantity: None,
                cumulative_quantity: Some(snapshot.cumulative_base),
                cumulative_quote: None,
                leaves_quantity: None,
                order_price: None,
            })),
        })
        .map_err(|_| SessionError::Identity)?;
        self.gateway
            .record_event(event)
            .map_err(SessionError::Gateway)?;
        Ok(())
    }

    fn ensure_warmer(&mut self) -> Result<&mut crate::warmup::SpotHotPathWarmer> {
        if self.warmer.is_none() {
            let snap = self.gateway.baseline().snapshot();
            let venue = SpotVenue::from_id(snap.scope.venue).ok_or(SessionError::Identity)?;
            let environment = snap.scope.environment;
            let catalog = self.gateway.baseline().catalog();
            let (instrument_id, symbol, sample_price, money_scale) =
                if let Some(policy) = catalog.entries.first() {
                    let quote_scale =
                        u8::try_from((-i16::from(policy.instrument.quote.exponent)).max(0))
                            .unwrap_or(8)
                            .min(18);
                    let tick_text = policy.instrument.price_tick.as_plain_string();
                    let tick_frac = tick_text
                        .split_once('.')
                        .map_or(0u8, |(_, frac)| u8::try_from(frac.len()).unwrap_or(8));
                    let price_scale = quote_scale.max(tick_frac).min(18);
                    let sample_px = tickwire_account::Fixed::parse(tick_text.as_str(), price_scale)
                        .and_then(|t| t.checked_mul_int(1_000))
                        .or_else(|_| tickwire_account::Fixed::from_atoms(1_000, price_scale))
                        .map_err(|_| SessionError::Identity)?;
                    (
                        policy.instrument.instrument_id,
                        policy.instrument.symbol.as_str(),
                        sample_px,
                        quote_scale,
                    )
                } else {
                    let default_sym = match venue {
                        SpotVenue::Bybit => "BTCUSDT",
                        SpotVenue::Okx => "BTC-USDT",
                    };
                    let sample_px = tickwire_account::Fixed::from_atoms(5_000_000, 2)
                        .map_err(|_| SessionError::Identity)?;
                    (0u64, default_sym, sample_px, 8u8)
                };
            let scratch_account = self.gateway.account().clone();
            let warmer = crate::warmup::SpotHotPathWarmer::new(
                venue,
                environment,
                instrument_id,
                symbol,
                sample_price,
                money_scale,
                scratch_account,
            )
            .map_err(SessionError::Transport)?;
            self.warmer = Some(Box::new(warmer));
        }
        Ok(self.warmer.as_mut().expect("warmer initialized"))
    }

    pub fn with_warmup_interval_ns(mut self, warmup_interval_ns: u64) -> Result<Self> {
        self.set_warmup_interval_ns(warmup_interval_ns)?;
        Ok(self)
    }

    pub fn set_warmup_interval_ns(&mut self, warmup_interval_ns: u64) -> Result<()> {
        self.warmup_tracker
            .set_warmup_interval_ns(warmup_interval_ns);
        if warmup_interval_ns > 0 {
            let _ = self.ensure_warmer()?;
        }
        Ok(())
    }

    #[inline]
    pub const fn warmup_interval_ns(&self) -> u64 {
        self.warmup_tracker.warmup_interval_ns()
    }

    #[inline]
    pub const fn warmup_count(&self) -> u64 {
        self.warmup_tracker.warm_count()
    }

    #[inline]
    pub const fn last_activity_ns(&self) -> u64 {
        self.warmup_tracker.last_activity_ns()
    }

    #[inline]
    pub const fn last_warm_ns(&self) -> u64 {
        self.warmup_tracker.last_warm_ns()
    }

    #[inline]
    pub const fn next_warmup_due_ns(&self) -> Option<u64> {
        self.warmup_tracker.next_due_ns()
    }

    #[inline]
    pub fn record_activity(&mut self, now_mono_ns: u64) {
        self.warmup_tracker.record_activity(now_mono_ns);
    }

    /// Refreshes the isolated warmup scratch `Account` clone outside active trading bursts.
    pub fn refresh_warmup_scratch(&mut self) -> Result<()> {
        let scratch = self.gateway.account().clone();
        let warmer = self.ensure_warmer()?;
        warmer.refresh_scratch_account(&scratch);
        Ok(())
    }

    /// Unconditionally executes one side-effect-free hot-path dry-run iteration.
    pub fn warm_hot_path(&mut self, now_mono_ns: u64) -> Result<usize> {
        let _ = self.ensure_warmer()?;
        let live_account = self.gateway.account();
        let warmer = self.warmer.as_mut().expect("warmer initialized");
        let bytes = warmer
            .warm_once(live_account, now_mono_ns)
            .map_err(SessionError::Transport)?;
        self.warmup_tracker.record_warm(now_mono_ns);
        Ok(bytes)
    }

    /// Executes [`Self::warm_hot_path`] iff `warmup_interval_ns > 0`, no order/reply is active,
    /// and `now_mono_ns - max(last_activity_ns, last_warm_ns) >= warmup_interval_ns`.
    #[inline]
    pub fn maybe_warm_hot_path(&mut self, now_mono_ns: u64) -> Result<bool> {
        if self.warmup_tracker.warmup_interval_ns() == 0 {
            return Ok(false);
        }
        let pending_single = self
            .pending
            .as_ref()
            .is_some_and(|(intent, _, _)| self.gateway.status(*intent) != Some(Status::Aborted));
        if pending_single || self.has_any_slot_pending() || !self.completed_replies.is_empty() {
            self.warmup_tracker.record_activity(now_mono_ns);
            return Ok(false);
        }
        if !self.warmup_tracker.can_warm(now_mono_ns) {
            return Ok(false);
        }
        let _ = self.ensure_warmer()?;
        let live_account = self.gateway.account();
        let warmer = self.warmer.as_mut().expect("warmer initialized");
        warmer
            .warm_once(live_account, now_mono_ns)
            .map_err(SessionError::Transport)?;
        self.warmup_tracker.record_warm(now_mono_ns);
        Ok(true)
    }
}
