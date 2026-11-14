use super::*;
use crate::rest::{RestConfig, RestDisposition, RestOutcome, RestOwner};
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};
use tickwire_account_gateway::{
    AccountGateway, Authority, Encoder, Status,
    pool::{MAX_SHARED_INFLIGHT, RouteHealth, RouterConfig},
    websocket::WebSocketControl,
};
use tickwire_account_journal::Backend;
use tickwire_transport::{
    IoBudget,
    ws::{Decoder, Opcode, PollMessage},
};

#[derive(serde::Deserialize)]
struct WsRateLimitEntry<'a> {
    #[serde(borrow, rename = "rateLimitType")]
    rate_limit_type: &'a str,
    #[serde(borrow)]
    interval: &'a str,
    #[serde(rename = "intervalNum")]
    interval_num: u32,
    count: u64,
}

pub(crate) struct WsPending {
    pub(crate) request_id: WsRequestId,
    pub(crate) intent: IntentKey,
    pub(crate) command: CanonicalCommand,
    pub(crate) deadline: Instant,
    pub(crate) sent_mono_ns: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlAction {
    Connect,
    Upgrade,
    Ping,
    Pong,
    Close,
}
/// Implemented by the original-signature state and its shared quota endpoint.
/// Protocol permits cannot authorize an application frame.
pub trait SessionState {
    fn prepare(&mut self, plan: WsRequestPlan) -> Result<()>;
    fn signed_wire(&self, command: v::Hash) -> Result<SignedWire>;
    fn install(&mut self, binding: DurableWsBinding) -> Result<()>;
    fn feedback(&mut self, status: u16) -> Result<()>;
    fn control(
        &mut self,
        session: &DurableWsSession,
        action: ControlAction,
        operation: u64,
        deadline_mono_ns: u64,
    ) -> bool;
}
#[derive(Clone, Copy, Debug)]
pub struct WsConfig {
    pub lifetime_ns: u64,
    pub control_timeout_ns: u64,
    pub max_messages_per_poll: usize,
}
#[derive(Clone, Copy, Debug)]
pub struct WebSocketSettings {
    pub execution_instance: [u8; 16],
    pub credential_ref_digest: v::Hash,
    pub session: WsConfig,
}
pub(crate) struct Runtime {
    session: Option<DurableWsSession>,
    settings: WebSocketSettings,
    state: Box<dyn SessionState>,
    decoder: Decoder,
    expires: u64,
    control_timeout: u64,
    max_messages: usize,
    control: ControlAction,
    operation: u64,
    control_deadline: u64,
    upgraded: bool,
    stopped: bool,
    closing: bool,
    closed_event: bool,
    draining_eof: bool,
    wire_prefix: Vec<u8>,
    deferred: Option<(Opcode, Vec<u8>, Instant, u64)>,
    pub(crate) inflight: VecDeque<WsPending>,
    pub(crate) route_health: RouteHealth,
    pub(crate) completed_ws_outcomes: VecDeque<RestOutcome>,
    pending_error: Option<Error>,
}
impl<S: Store, B: Backend, A: Authority, E: Encoder> RestOwner<S, B, A, E> {
    pub fn from_parts_websocket(
        ingress: AccountIngress<S>,
        mut gateway: AccountGateway<B, A, E>,
        session: DurableWsSession,
        state: Box<dyn SessionState>,
        config: WsConfig,
        now: Instant,
        mono: u64,
    ) -> Result<Self> {
        if config.lifetime_ns == 0
            || config.lifetime_ns > 86_400_000_000_000
            || config.control_timeout_ns == 0
            || config.control_timeout_ns > 60_000_000_000
            || config.max_messages_per_poll == 0
            || config.max_messages_per_poll > 64
            || !session.is_current()
            || !Rc::ptr_eq(&session.owner, &ingress.ws_owner.0)
            || ingress
                .latest_ws_session()
                .is_none_or(|s| s.identity != session.identity)
        {
            return Err(Error::Identity);
        }
        let expires = mono
            .checked_add(config.lifetime_ns)
            .ok_or(Error::Capacity)?;
        let control_deadline = mono
            .checked_add(config.control_timeout_ns)
            .ok_or(Error::Capacity)?
            .min(expires);
        gateway
            .begin_websocket(
                now.checked_add(Duration::from_nanos(config.control_timeout_ns))
                    .ok_or(Error::Capacity)?,
            )
            .map_err(Error::Gateway)?;
        let mut out = Self::from_parts(
            ingress,
            gateway,
            RestConfig {
                connection_epoch: session.identity.connection_epoch,
                max_response_bytes: 32768,
                max_headers: 64,
            },
        )?;
        let settings = WebSocketSettings {
            execution_instance: session.identity.execution_instance,
            credential_ref_digest: session.identity.credential_ref_digest,
            session: config,
        };
        out.websocket = Some(Runtime {
            session: Some(session),
            settings,
            state,
            decoder: Decoder::new(32768, 32768).map_err(Error::Transport)?,
            expires,
            control_timeout: config.control_timeout_ns,
            max_messages: config.max_messages_per_poll,
            control: ControlAction::Upgrade,
            operation: 2,
            control_deadline,
            upgraded: false,
            stopped: false,
            closing: false,
            closed_event: false,
            draining_eof: false,
            wire_prefix: Vec::new(),
            deferred: None,
            inflight: VecDeque::with_capacity(MAX_SHARED_INFLIGHT as usize),
            route_health: RouteHealth::default(),
            completed_ws_outcomes: VecDeque::new(),
            pending_error: None,
        });
        Ok(out)
    }
    pub fn from_parts_websocket_recovered(
        ingress: AccountIngress<S>,
        gateway: AccountGateway<B, A, E>,
        state: Box<dyn SessionState>,
        settings: WebSocketSettings,
    ) -> Result<Self> {
        if !gateway.halted()
            || settings.execution_instance == [0; 16]
            || settings.credential_ref_digest == [0; 32]
            || settings.session.lifetime_ns == 0
            || settings.session.lifetime_ns > 86_400_000_000_000
            || settings.session.control_timeout_ns == 0
            || settings.session.control_timeout_ns > 60_000_000_000
            || settings.session.max_messages_per_poll == 0
            || settings.session.max_messages_per_poll > 64
        {
            return Err(Error::Identity);
        }
        let epoch = ingress
            .latest_ws_session()
            .map_or(0, |r| r.identity.connection_epoch);
        let mut out = Self::from_parts(
            ingress,
            gateway,
            RestConfig {
                connection_epoch: epoch.max(1),
                max_response_bytes: 32768,
                max_headers: 64,
            },
        )?;
        out.config.connection_epoch = epoch;
        out.websocket = Some(Runtime {
            session: None,
            settings,
            state,
            decoder: Decoder::new(32768, 32768).map_err(Error::Transport)?,
            expires: 0,
            control_timeout: settings.session.control_timeout_ns,
            max_messages: settings.session.max_messages_per_poll,
            control: ControlAction::Upgrade,
            operation: 2,
            control_deadline: 0,
            upgraded: false,
            stopped: true,
            closing: false,
            closed_event: false,
            draining_eof: false,
            wire_prefix: Vec::new(),
            deferred: None,
            inflight: VecDeque::with_capacity(MAX_SHARED_INFLIGHT as usize),
            route_health: RouteHealth::default(),
            completed_ws_outcomes: VecDeque::new(),
            pending_error: None,
        });
        Ok(out)
    }
    pub fn websocket_next_epoch(&self) -> Result<Option<u64>> {
        if self.websocket.is_none() {
            return Ok(None);
        }
        self.ingress.latest_ws_session().map_or(Ok(Some(1)), |s| {
            s.identity
                .connection_epoch
                .checked_add(1)
                .map(Some)
                .ok_or(Error::Capacity)
        })
    }
    pub(crate) fn websocket_deferred(&self) -> bool {
        self.websocket
            .as_ref()
            .is_some_and(|r| r.deferred.is_some())
    }
    pub(crate) fn ws_frame_flush_pending(&self) -> bool {
        self.websocket.as_ref().is_some_and(|r| {
            r.inflight.iter().any(|p| {
                matches!(
                    self.gateway.status(p.intent),
                    Some(Status::AwaitingCommit | Status::Queued | Status::PartiallyAccepted)
                )
            })
        })
    }
    pub fn websocket_ready(&self) -> bool {
        self.websocket.as_ref().is_some_and(|s| {
            !s.stopped && !s.closing && s.session.as_ref().is_some_and(DurableWsSession::is_current)
        }) && self.gateway.websocket_ready()
    }
    pub(crate) fn submit_websocket(
        &mut self,
        command: CanonicalCommand,
        ids: &[[u8; 16]],
        evaluated_at: u64,
        deadline: Instant,
    ) -> Result<Status> {
        if !self.websocket_ready()
            || self.ws_frame_flush_pending()
            || self.websocket.as_ref().unwrap().inflight.len() >= MAX_SHARED_INFLIGHT as usize
            || self.websocket.as_ref().unwrap().deferred.is_some()
            || self.gateway.websocket_control_pending()
        {
            return Err(Error::Busy);
        }
        self.ingress.ready(&self.gateway)?;
        self.ingress.evict_expired(&mut self.gateway, evaluated_at);
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
                if ids.is_empty() => {}
            _ => return Err(Error::Unsupported),
        }
        let pending_quota = if self.venue_limiter.is_some() {
            let action = self.classify_quota_action(&command, true)?;
            let ip_key = self
                .venue_limiter
                .as_ref()
                .and_then(|l| l.local_ips().first().copied())
                .unwrap_or(tickwire_quota::rate_limiter::IpKey::NONE);
            self.can_afford_quota_on_ip(ip_key, action, evaluated_at)
                .map_err(Error::RateLimited)?;
            Some((ip_key, action))
        } else {
            None
        };
        if matches!(
            command.command().body,
            v::CommandBody::New(_)
                | v::CommandBody::CancelReplace { .. }
                | v::CommandBody::SpotList(_)
                | v::CommandBody::Algo(_)
        ) {
            self.ingress.register_command(command.clone(), ids)?;
        }
        let runtime = self.websocket.as_mut().unwrap();
        let plan = self.ingress.plan_ws_request(
            runtime.session.as_ref().ok_or(Error::Identity)?,
            command.clone(),
        )?;
        let request_id = plan.request_id().clone();
        let intent_key = intent(&command);
        runtime.state.prepare(plan.clone())?;
        let result = self.ingress.submit(
            &mut self.gateway,
            command.clone(),
            ids,
            evaluated_at,
            deadline,
        );
        let status = match result {
            Ok(s) => s,
            Err(e) => {
                self.stop_sending();
                return Err(e);
            }
        };
        if status == Status::Aborted {
            return Ok(status);
        }
        let binding = (|| {
            let wire = runtime.state.signed_wire(command.digest())?;
            let binding = self.ingress.persist_ws_binding(plan, &self.gateway, wire)?;
            runtime.state.install(binding)
        })();
        if let Err(error) = binding {
            self.stop_sending();
            return Err(error);
        }
        if let Some((ip_key, action)) = pending_quota
            && let Some(limiter) = self.venue_limiter.as_mut()
        {
            crate::rest::allow_quota_with_limiter(limiter, ip_key, action, evaluated_at)
                .map_err(Error::RateLimited)?;
        }
        runtime.inflight.push_back(WsPending {
            request_id,
            intent: intent_key,
            command,
            deadline,
            sent_mono_ns: evaluated_at,
        });
        runtime.route_health.inflight = runtime.inflight.len().min(u16::MAX as usize) as u16;
        Ok(status)
    }
    pub(crate) fn poll_websocket_owner(
        &mut self,
        now: Instant,
        mono: u64,
        budget: IoBudget,
    ) -> Result<Option<RestOutcome>> {
        let mut runtime = self.websocket.take().ok_or(Error::Identity)?;
        let mut result = self.poll_ws(&mut runtime, now, mono, budget);
        let disconnected =
            (!self.gateway.has_transport_connection() && !runtime.draining_eof) || runtime.stopped;
        if (result.is_err() || self.gateway.halted() || disconnected)
            && let Some(session) = runtime.session.as_ref()
        {
            if !runtime.closed_event {
                let kind = if result.is_err() {
                    lifecycle::SessionEventKind::ProtocolFault
                } else {
                    lifecycle::SessionEventKind::Disconnected
                };
                let logged = self.ingress.ws_session_event(
                    session,
                    mono,
                    kind,
                    std::mem::take(&mut runtime.wire_prefix),
                );
                if let Err(error) = logged
                    && result.is_ok()
                {
                    result = Err(error);
                }
                runtime.closed_event = true;
            }
            runtime.stopped = true;
            self.ingress.invalidate_ws_session();
            if self.gateway.per_order_doubt_isolation()
                && result.is_ok()
                && !self.gateway.halted()
                && !self.gateway.account().frozen()
            {
                self.gateway.drop_broken_transport_isolated();
            } else {
                self.gateway.stop_sending();
            }
        }
        self.websocket = Some(runtime);
        result
    }
    fn poll_ws(
        &mut self,
        r: &mut Runtime,
        now: Instant,
        mono: u64,
        budget: IoBudget,
    ) -> Result<Option<RestOutcome>> {
        if let Some(queued) = r.completed_ws_outcomes.pop_front() {
            return Ok(Some(queued));
        }
        if let Some(err) = r.pending_error.take() {
            return Err(err);
        }
        let Some(session) = r.session.as_ref() else {
            return Ok(None);
        };
        if r.stopped && r.inflight.is_empty() {
            return Ok(None);
        }
        if mono >= r.expires {
            if self.gateway.per_order_doubt_isolation() {
                self.gateway.drop_broken_transport_isolated();
                r.stopped = true;
            } else {
                self.gateway.stop_sending();
            }
        }
        if !r.stopped && !self.gateway.halted() && self.gateway.has_transport_connection() {
            let _ = self.gateway.poll_websocket(now, budget, || {
                r.state
                    .control(session, r.control, r.operation, r.control_deadline)
            });
        }
        if !r.upgraded && self.gateway.websocket_ready() {
            self.ingress.ws_session_event(
                session,
                mono,
                lifecycle::SessionEventKind::Upgraded,
                Vec::new(),
            )?;
            r.upgraded = true;
        }
        let mut bytes = [0u8; 4096];
        let mut drained = 0;
        while drained < budget.max_bytes && r.upgraded {
            let limit = bytes.len().min(budget.max_bytes - drained);
            let n = self.gateway.read(&mut bytes[..limit]);
            if n == 0 {
                break;
            }
            drained += n;
            let retain = (32768 - r.wire_prefix.len()).min(n);
            r.wire_prefix.extend_from_slice(&bytes[..retain]);
            r.decoder.push(&bytes[..n]).map_err(Error::Transport)?;
        }
        let hit_byte_budget = drained == budget.max_bytes && drained != 0;
        let mut decoder_exhausted = !r.upgraded;
        for _ in 0..r.max_messages {
            if r.closing || r.stopped || self.gateway.halted() {
                break;
            }
            match r.decoder.poll_message().map_err(Error::Transport)? {
                PollMessage::NeedInput => {
                    decoder_exhausted = true;
                    break;
                }
                PollMessage::Yielded => break,
                PollMessage::Message(message) => match message.opcode {
                    Opcode::Text => {
                        r.wire_prefix.clear();
                        let market = self.ingress.market()?;
                        let decoded_info =
                            tickwire_binance::response::decode_websocket(market, &message.payload)
                                .ok()
                                .map(|d| {
                                    let id_str = d.request_id.and_then(|raw_id| {
                                        serde_json::from_str::<String>(raw_id.get()).ok()
                                    });
                                    let rl_raw = d.rate_limits.map(|rl| rl.get().to_owned());
                                    (d.status, d.failure, id_str, rl_raw)
                                });
                        let matched_pending = if let Some((_, _, Some(ref id_str), _)) =
                            decoded_info
                            && let Some(pos) = r
                                .inflight
                                .iter()
                                .position(|p| p.request_id.as_str() == id_str.as_str())
                        {
                            r.inflight.remove(pos)
                        } else if let Some((_, _, Some(ref id_str), _)) = decoded_info
                            && self.ingress.ws_bindings().any(|b| {
                                b.session.identity == *session.identity()
                                    && request_id(&b.session.identity, b.sequence).as_str()
                                        == id_str.as_str()
                                    && self.ingress.records.iter().any(|rec| {
                                        matches!(
                                            rec,
                                            Record::WsResponse(old)
                                                if old.binding == Some(b.sequence)
                                                    && old.decision == ResponseDecision::Matched
                                        )
                                    })
                            })
                        {
                            None
                        } else {
                            r.inflight.pop_front()
                        };
                        r.route_health.inflight = r.inflight.len().min(u16::MAX as usize) as u16;
                        let key = matched_pending.as_ref().map(|p| p.intent);
                        let outcome = self.ingress.persist_ws_response(
                            &mut self.gateway,
                            session,
                            key,
                            mono,
                            message.payload,
                        )?;
                        if let Some(p) = &matched_pending {
                            if outcome.decision == ResponseDecision::Matched {
                                let sample_us =
                                    u32::try_from(mono.saturating_sub(p.sent_mono_ns) / 1_000)
                                        .unwrap_or(u32::MAX);
                                r.route_health
                                    .note_sample(sample_us, RouterConfig::default());
                            }
                            self.sync_query_desk_for_command(&p.command, mono);
                        }
                        if let Some((status, failure, _, rl_raw)) = decoded_info {
                            self.reconcile_ws_response_quota(status, rl_raw.as_deref(), mono);
                            r.state.feedback(status)?;
                            if matches!(
                                failure,
                                Some(
                                    tickwire_binance::response::FailureKind::Authentication
                                        | tickwire_binance::response::FailureKind::RateLimited
                                        | tickwire_binance::response::FailureKind::Banned
                                        | tickwire_binance::response::FailureKind::ClockSkew
                                )
                            ) {
                                self.gateway.stop_sending();
                            }
                        }
                        let Some(request) = outcome.request else {
                            if r.completed_ws_outcomes.is_empty() {
                                return Err(Error::Identity);
                            }
                            r.pending_error = Some(Error::Identity);
                            break;
                        };
                        r.completed_ws_outcomes.push_back(RestOutcome {
                            request,
                            input: Some(outcome.input),
                            disposition: outcome.disposition,
                            applied_events: outcome.applied_events,
                        });
                    }
                    Opcode::Ping => {
                        if r.deferred.is_some() {
                            return Err(Error::Capacity);
                        }
                        let end = now
                            .checked_add(Duration::from_nanos(r.control_timeout))
                            .ok_or(Error::Capacity)?;
                        let mono_end = mono
                            .checked_add(r.control_timeout)
                            .ok_or(Error::Capacity)?
                            .min(r.expires);
                        r.deferred = Some((Opcode::Pong, message.payload, end, mono_end));
                    }
                    Opcode::Pong => {}
                    Opcode::Close => {
                        self.ingress.ws_session_event(
                            session,
                            mono,
                            lifecycle::SessionEventKind::PeerClose,
                            message.payload.clone(),
                        )?;
                        r.closed_event = true;
                        r.closing = true;
                        if !self.gateway.per_order_doubt_isolation() {
                            self.gateway.freeze_account();
                        }
                        let deadline = now
                            .checked_add(Duration::from_nanos(r.control_timeout))
                            .ok_or(Error::Capacity)?;
                        // A partial application frame cannot be interleaved with
                        // Close. If its slot is occupied, retain Unknown and stop.
                        match self.gateway.queue_websocket_control(
                            WebSocketControl::Close(&message.payload),
                            deadline,
                        ) {
                            Ok(()) => {
                                r.control = ControlAction::Close;
                                r.control_deadline = mono
                                    .checked_add(r.control_timeout)
                                    .ok_or(Error::Capacity)?
                                    .min(r.expires);
                                r.operation = r.operation.checked_add(1).ok_or(Error::Capacity)?;
                                r.deferred = None;
                            }
                            Err(_) => {
                                if self.gateway.per_order_doubt_isolation() {
                                    self.gateway.drop_broken_transport_isolated();
                                } else {
                                    self.gateway.stop_sending();
                                }
                                r.stopped = true;
                            }
                        }
                    }
                    Opcode::Binary => return Err(Error::Encoding),
                },
            }
        }
        if let Some((_, _, deadline, mono_deadline)) = &r.deferred
            && (now >= *deadline || mono >= *mono_deadline)
        {
            if self.gateway.per_order_doubt_isolation() {
                self.gateway.drop_broken_transport_isolated();
                r.stopped = true;
            } else {
                self.gateway.stop_sending();
            }
        }
        if !self.gateway.halted()
            && self.gateway.has_transport_connection()
            && !self.gateway.websocket_control_pending()
            && let Some((opcode, payload, deadline, mono_deadline)) = r.deferred.take()
        {
            let control = match opcode {
                Opcode::Pong => WebSocketControl::Pong(&payload),
                _ => return Err(Error::Identity),
            };
            match self.gateway.queue_websocket_control(control, deadline) {
                Ok(()) => {
                    r.control = ControlAction::Pong;
                    r.control_deadline = mono_deadline;
                    r.operation = r.operation.checked_add(1).ok_or(Error::Capacity)?;
                }
                Err(tickwire_account_gateway::Error::Busy) => {
                    r.deferred = Some((opcode, payload, deadline, mono_deadline))
                }
                Err(error) => return Err(Error::Gateway(error)),
            }
        }
        let more_buffered_rx = r.upgraded
            && !self.gateway.halted()
            && !r.stopped
            && !r.closing
            && mono < r.expires
            && (hit_byte_budget || !decoder_exhausted);
        r.draining_eof = !self.gateway.has_transport_connection() && more_buffered_rx;
        let mut idx = 0usize;
        while idx < r.inflight.len() {
            if self.gateway.status(r.inflight[idx].intent) == Some(Status::Aborted) {
                let p = r.inflight.remove(idx).unwrap();
                r.completed_ws_outcomes.push_back(RestOutcome {
                    request: p.intent,
                    input: None,
                    disposition: RestDisposition::NoBytes,
                    applied_events: 0,
                });
            } else {
                idx += 1;
            }
        }
        r.route_health.inflight = r.inflight.len().min(u16::MAX as usize) as u16;
        if !r.inflight.is_empty() {
            let any_expired = r.inflight.iter().any(|p| now >= p.deadline);
            if self.gateway.halted()
                || (!self.gateway.has_transport_connection() && !r.draining_eof)
                || r.stopped
                || any_expired
            {
                if self.gateway.per_order_doubt_isolation()
                    && !self.gateway.halted()
                    && !self.gateway.account().frozen()
                {
                    self.gateway.drop_broken_transport_isolated();
                    r.stopped = true;
                } else {
                    self.gateway.stop_sending();
                }
                r.route_health.reset();
                while let Some(pending) = r.inflight.pop_front() {
                    if self.gateway.status(pending.intent) == Some(Status::Aborted) {
                        r.completed_ws_outcomes.push_back(RestOutcome {
                            request: pending.intent,
                            input: None,
                            disposition: RestDisposition::NoBytes,
                            applied_events: 0,
                        });
                        continue;
                    }
                    let out = self.ingress.persist_ws_response(
                        &mut self.gateway,
                        session,
                        Some(pending.intent),
                        mono,
                        Vec::new(),
                    )?;
                    self.sync_query_desk_for_command(&pending.command, mono);
                    r.completed_ws_outcomes.push_back(RestOutcome {
                        request: pending.intent,
                        input: Some(out.input),
                        disposition: RestDisposition::Unknown,
                        applied_events: 0,
                    });
                }
            }
        }
        if let Some(queued) = r.completed_ws_outcomes.pop_front() {
            return Ok(Some(queued));
        }
        if let Some(err) = r.pending_error.take() {
            return Err(err);
        }
        Ok(None)
    }
    fn reconcile_ws_response_quota(&mut self, status: u16, rl_json: Option<&str>, mono: u64) {
        let Some(limiter) = self.venue_limiter.as_mut() else {
            return;
        };
        let ip_key = limiter
            .local_ips()
            .first()
            .copied()
            .unwrap_or(tickwire_quota::rate_limiter::IpKey::NONE);
        if let Some(raw_rl) = rl_json
            && let Ok(entries) = serde_json::from_str::<Vec<WsRateLimitEntry<'_>>>(raw_rl)
        {
            for entry in entries {
                let interval_s = match entry.interval {
                    "SECOND" => entry.interval_num,
                    "MINUTE" => entry.interval_num.saturating_mul(60),
                    "HOUR" => entry.interval_num.saturating_mul(3_600),
                    "DAY" => entry.interval_num.saturating_mul(86_400),
                    _ => continue,
                };
                if entry.rate_limit_type == "REQUEST_WEIGHT"
                    && interval_s == 60
                    && let Some(win_id) = limiter.ip_window_id(ip_key)
                {
                    let _ = limiter.reconcile_exchange_usage(win_id, entry.count, mono);
                } else if entry.rate_limit_type == "ORDERS"
                    && let Some(win_id) = limiter
                        .account_order_window_ids()
                        .iter()
                        .copied()
                        .find(|id| id.interval_s == interval_s)
                {
                    let _ = limiter.reconcile_exchange_usage(win_id, entry.count, mono);
                }
            }
        }
        if status == 418 {
            let _ = limiter.freeze_account();
        } else if status == 429 && !ip_key.is_none() {
            let _ = limiter.note_ip_cooldown(ip_key, mono.saturating_add(1_000_000_000));
        }
    }
}

pub(crate) struct ReconnectVerifier<'a, S: Store> {
    pub input: &'a mut AccountIngress<S>,
    pub runtime: &'a mut Runtime,
}
impl<S: Store> tickwire_account_gateway::ReactivationVerifier for ReconnectVerifier<'_, S> {
    fn verify(
        &mut self,
        baseline: &tickwire_account_codec::CanonicalBaseline,
        account: &tickwire_account::Account,
        preceding: &tickwire_account_journal::HistoryFrontier,
        binding: &tickwire_account::Reactivation,
    ) -> std::result::Result<(), tickwire_account_gateway::AuthorityError> {
        self.input
            .activation_verifier()
            .verify(baseline, account, preceding, binding)
    }
    fn before_connect(
        &mut self,
        binding: &tickwire_account::Reactivation,
    ) -> std::result::Result<(), tickwire_account_gateway::AuthorityError> {
        let run = || -> Result<()> {
            if binding.writer_epoch < self.input.config.writer_epoch {
                return Err(Error::Identity);
            }
            self.input.invalidate_ws_session();
            self.runtime.session = None;
            self.runtime.stopped = true;
            self.input.config.writer_epoch = binding.writer_epoch;
            let session = self.input.reserve_ws_session(
                self.runtime.settings.execution_instance,
                self.runtime.settings.credential_ref_digest,
            )?;
            if session.identity.connection_epoch != binding.execution_connection_epoch {
                self.input.invalidate_ws_session();
                return Err(Error::Identity);
            }
            let deadline = binding
                .evaluated_at
                .checked_add(self.runtime.control_timeout)
                .ok_or(Error::Capacity)?
                .min(binding.valid_until);
            if !self
                .runtime
                .state
                .control(&session, ControlAction::Connect, 1, deadline)
            {
                self.input.invalidate_ws_session();
                return Err(Error::Identity);
            }
            self.runtime.expires = binding
                .evaluated_at
                .checked_add(self.runtime.settings.session.lifetime_ns)
                .ok_or(Error::Capacity)?;
            self.runtime.control_deadline = deadline;
            self.runtime.operation = 2;
            self.runtime.control = ControlAction::Upgrade;
            self.runtime.decoder = Decoder::new(32768, 32768).map_err(Error::Transport)?;
            self.runtime.upgraded = false;
            self.runtime.closing = false;
            self.runtime.deferred = None;
            self.runtime.closed_event = false;
            self.runtime.wire_prefix.clear();
            self.runtime.inflight.clear();
            self.runtime.route_health.reset();
            self.runtime.completed_ws_outcomes.clear();
            self.runtime.pending_error = None;
            self.runtime.session = Some(session);
            Ok(())
        };
        let mut run = run;
        run().map_err(|_| tickwire_account_gateway::AuthorityError::Denied)
    }
}
impl<S: Store, B: Backend, A: Authority, E: Encoder> RestOwner<S, B, A, E> {
    pub(crate) fn finish_ws_reconnect(&mut self) -> Result<()> {
        let Some(r) = self.websocket.as_mut() else {
            return Ok(());
        };
        let deadline = Instant::now()
            .checked_add(Duration::from_nanos(r.control_timeout))
            .ok_or(Error::Capacity)?;
        if let Err(e) = self.gateway.begin_websocket(deadline) {
            self.ingress.invalidate_ws_session();
            self.gateway.stop_sending();
            return Err(Error::Gateway(e));
        }
        r.stopped = false;
        Ok(())
    }
}
