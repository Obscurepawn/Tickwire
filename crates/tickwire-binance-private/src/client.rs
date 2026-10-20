use crate::*;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};
use tickwire_binance::{
    events::{PrivateBody, PrivateStream},
    feed::TimeUnit,
};
use tickwire_transport::{self as net, http, ws};

const KEY_LIFETIME: u64 = 3_600_000_000_000;
struct Job {
    bytes: Vec<u8>,
    charge: Charge,
    token: Option<RequestToken>,
    deadline: Instant,
}
struct Pending {
    request: request::PreparedControlRequest,
    started: u64,
    deadline: u64,
    accepted: bool,
    complete: bool,
}
struct Lane {
    connection: net::Connection,
    http: http::ResponseDecoder,
    handshake: Option<ws::ClientHandshake>,
    decoder: ws::Decoder,
    upgraded: bool,
    jobs: VecDeque<Job>,
    active: Option<Job>,
    registered: bool,
    ended: bool,
    needs_input: bool,
    opened: u64,
    last_frame: u64,
}
struct QueuedEvent {
    bytes: Vec<u8>,
    arrived: u64,
}
enum Notice {
    Gap(GapNotice),
    Control(ControlOutcome),
}
/// One exclusive credential/account owner. Dropping only closes local sockets.
/// A remote listen-key deletion is exclusively the explicit terminal stop action.
pub struct PrivateClient<'a> {
    config: SessionConfig,
    auth: Authentication<'a>,
    network: NetworkConfig,
    next_epoch: u64,
    epoch: u64,
    serial: u64,
    attempt: u64,
    phase: Phase,
    gap: GapToken,
    stream: Option<Lane>,
    management: Option<Lane>,
    pending: Option<Pending>,
    planned: Option<Operation>,
    subscription: Option<u64>,
    listen_key: Option<String>,
    lease: Option<u64>,
    renew: Option<u64>,
    backoff_until: u64,
    cooldown_until: u64,
    failures: u32,
    last_clock: u64,
    last_instant: Option<Instant>,
    clock_pair: Option<ClockSample>,
    clock_domain: Option<ClockDomain>,
    early: VecDeque<QueuedEvent>,
    early_bytes: usize,
    notices: VecDeque<Notice>,
    stopping: bool,
    remote_close_unknown: bool,
    draining: bool,
    closing_frame: bool,
}
impl std::fmt::Debug for PrivateClient<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrivateClient")
            .field("scope", &self.config.scope)
            .field("status", &self.status())
            .finish()
    }
}
fn add(now: u64, duration: u64) -> Result<u64> {
    now.checked_add(duration).ok_or(Error::Exhausted)
}
fn deadline(now: Instant, ns: u64) -> Result<Instant> {
    now.checked_add(Duration::from_nanos(ns))
        .ok_or(Error::Bounds)
}
impl<'a> PrivateClient<'a> {
    pub fn new(
        config: SessionConfig,
        auth: Authentication<'a>,
        network: NetworkConfig,
        first_connection_epoch: u64,
    ) -> Result<Self> {
        request::validate_auth(&config.scope, &auth)?;
        let t = config.timing;
        let l = config.limits;
        if first_connection_epoch == 0 {
            return Err(Error::Identity);
        }
        if t.connect_ns == 0
            || t.request_ns == 0
            || t.renew_ns == 0
            || t.renew_ns >= KEY_LIFETIME
            || t.request_ns >= KEY_LIFETIME - t.renew_ns
            || t.rotate_ns == 0
            || t.rotate_ns >= 86_400_000_000_000
            || t.heartbeat_ns == 0
            || t.backoff_ns == 0
            || t.max_backoff_ns < t.backoff_ns
            || t.recv_window_micros == 0
            || t.recv_window_micros > 60_000_000
            || l.request_bytes == 0
            || l.request_bytes > request::MAX_REQUEST_BYTES
            || l.message_bytes == 0
            || l.message_bytes > 1 << 20
            || l.early_messages == 0
            || l.early_messages > 1024
            || l.early_bytes == 0
            || l.early_bytes > 16 << 20
            || l.control_frames == 0
            || l.control_frames > 1024
            || (config.scope.market == Market::Usdm && network.management_address.is_none())
        {
            return Err(Error::Bounds);
        }
        if network.tls.enable_early_data
            || [
                l.io.max_send_bytes,
                l.io.max_receive_bytes,
                l.io.tls_buffer_bytes,
                l.io.max_accept_bytes,
            ]
            .iter()
            .any(|n| *n == 0 || *n > 16 << 20)
        {
            return Err(Error::Bounds);
        }
        Ok(Self {
            gap: GapToken {
                scope: config.scope.clone(),
                epoch: 0,
                generation: 1,
            },
            config,
            auth,
            network,
            next_epoch: first_connection_epoch,
            epoch: 0,
            serial: 0,
            attempt: 0,
            phase: Phase::Disconnected,
            stream: None,
            management: None,
            pending: None,
            planned: None,
            subscription: None,
            listen_key: None,
            lease: None,
            renew: None,
            backoff_until: 0,
            cooldown_until: 0,
            failures: 0,
            last_clock: 0,
            last_instant: None,
            clock_pair: None,
            clock_domain: None,
            early: VecDeque::new(),
            early_bytes: 0,
            notices: VecDeque::new(),
            stopping: false,
            remote_close_unknown: false,
            draining: false,
            closing_frame: false,
        })
    }
    pub fn register(&mut self, registry: &mio::Registry, tokens: RegistrationTokens) -> Result<()> {
        self.registrations(registry, tokens, false)
    }
    pub fn reregister(
        &mut self,
        registry: &mio::Registry,
        tokens: RegistrationTokens,
    ) -> Result<()> {
        self.registrations(registry, tokens, true)
    }
    fn registrations(
        &mut self,
        registry: &mio::Registry,
        tokens: RegistrationTokens,
        refresh: bool,
    ) -> Result<()> {
        if tokens.stream == tokens.management {
            return Err(Error::Identity);
        }
        for (lane, token) in [
            (&mut self.stream, tokens.stream),
            (&mut self.management, tokens.management),
        ] {
            if let Some(lane) = lane {
                if lane.registered {
                    if refresh {
                        lane.connection
                            .reregister(registry, token)
                            .map_err(|_| Error::Transport)?;
                    }
                } else {
                    lane.connection
                        .register(registry, token)
                        .map_err(|_| Error::Transport)?;
                    lane.registered = true;
                }
            }
        }
        Ok(())
    }
    pub fn status(&self) -> SessionStatus {
        SessionStatus {
            phase: self.phase,
            connection_epoch: self.epoch,
            subscription_id: self.subscription,
            lease_until_ns: self.lease,
            renew_at_ns: self.renew,
            request: self.pending.as_ref().map(|p| p.request.token()),
            remote_close_unknown: self.remote_close_unknown,
            registration_changed: [&self.stream, &self.management]
                .iter()
                .any(|l| l.as_ref().is_some_and(|l| !l.registered)),
        }
    }
    pub fn current_gap(&self) -> &GapToken {
        &self.gap
    }
    pub fn next_deadline(&self) -> Option<Instant> {
        let pair = self.clock_pair?;
        let mut times = Vec::with_capacity(6);
        if let Some(p) = &self.pending {
            times.push(p.deadline);
        }
        if let Some(v) = self.lease {
            times.push(v);
        }
        if let Some(v) = self.renew {
            times.push(v);
        }
        if self.phase == Phase::Backoff {
            times.push(self.backoff_until);
        }
        if let Some(s) = &self.stream {
            times.push(s.opened.saturating_add(if s.upgraded {
                self.config.timing.rotate_ns
            } else {
                self.config.timing.connect_ns
            }));
            times.push(s.last_frame.saturating_add(self.config.timing.heartbeat_ns));
        }
        let direct = [&self.stream, &self.management]
            .into_iter()
            .filter_map(|l| l.as_ref())
            .flat_map(|l| l.active.iter().chain(l.jobs.iter()))
            .map(|j| j.deadline)
            .min();
        let timed = times.into_iter().min().and_then(|n| {
            pair.instant
                .checked_add(Duration::from_nanos(n.saturating_sub(pair.monotonic_ns())))
        });
        match (direct, timed) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }
    pub fn request_status(&mut self) -> Result<()> {
        if self.config.scope.market != Market::Spot {
            return Err(Error::Unsupported);
        }
        if self.phase != Phase::Receiving || self.pending.is_some() || self.planned.is_some() {
            return Err(Error::Busy);
        }
        self.planned = Some(Operation::SpotStatus);
        Ok(())
    }
    pub fn stop(&mut self, mode: StopMode) -> Result<()> {
        if self.phase == Phase::Stopped {
            return Ok(());
        }
        if matches!(mode, StopMode::CloseRemoteStream)
            && (self.phase != Phase::Receiving || self.pending.is_some() || self.planned.is_some())
        {
            return Err(Error::Busy);
        }
        self.stopping = true;
        self.new_gap(Error::Closed)?;
        self.early.clear();
        self.early_bytes = 0;
        match mode {
            StopMode::Disconnect => {
                self.reject_pending();
                self.clear_network();
                self.phase = Phase::Stopped;
            }
            StopMode::CloseRemoteStream => {
                self.phase = Phase::Closing;
                self.planned = Some(match self.config.scope.market {
                    Market::Spot => Operation::SpotUnsubscribe {
                        subscription_id: self.subscription.ok_or(Error::Identity)?,
                    },
                    Market::Usdm => {
                        self.stream = None;
                        Operation::UsdmClose
                    }
                });
            }
        }
        Ok(())
    }
    fn exhaust(&mut self) {
        self.reject_pending();
        self.clear_network();
        self.phase = Phase::Exhausted;
        self.stopping = true;
    }
    fn new_gap(&mut self, reason: Error) -> Result<()> {
        let Some(generation) = self.gap.generation.checked_add(1) else {
            self.exhaust();
            return Err(Error::Exhausted);
        };
        self.gap.generation = generation;
        self.gap.epoch = self.epoch;
        self.notices.push_back(Notice::Gap(GapNotice {
            token: self.gap.clone(),
            reason,
        }));
        Ok(())
    }
    fn reject_pending(&mut self) {
        if let Some(p) = self.pending.take() {
            if p.request.token().operation() == Operation::UsdmClose && p.accepted {
                self.remote_close_unknown = true;
            }
            self.notices.push_back(Notice::Control(ControlOutcome {
                token: p.request.token(),
                state: if p.accepted {
                    ControlState::MayHaveApplied
                } else {
                    ControlState::NoBytes
                },
                status: None,
                code: None,
            }));
        }
    }
    fn clear_network(&mut self) {
        self.draining = false;
        self.closing_frame = false;
        self.stream = None;
        self.management = None;
        self.planned = None;
        self.subscription = None;
        self.listen_key = None;
        self.lease = None;
        self.renew = None;
        self.early.clear();
        self.early_bytes = 0;
    }
    fn fail(&mut self, reason: Error, now: u64) -> Result<()> {
        self.reject_pending();
        self.clear_network();
        self.new_gap(reason)?;
        if self.stopping {
            self.phase = Phase::Stopped;
        } else {
            self.failures = self.failures.saturating_add(1);
            let factor = 1u64 << self.failures.saturating_sub(1).min(20);
            let wait = self
                .config
                .timing
                .backoff_ns
                .saturating_mul(factor)
                .min(self.config.timing.max_backoff_ns);
            let Ok(until) = add(now, wait) else {
                self.exhaust();
                return Err(Error::Exhausted);
            };
            self.backoff_until = until.max(self.cooldown_until);
            self.phase = Phase::Backoff;
        }
        Ok(())
    }
    fn sample(&mut self, clock: &mut impl Clock) -> Result<ClockSample> {
        let now = clock.sample()?;
        if self
            .clock_domain
            .is_some_and(|d| d != now.monotonic.domain())
        {
            return Err(Error::ClockDomain);
        }
        self.clock_domain = Some(now.monotonic.domain());
        if now.monotonic_ns() < self.last_clock
            || self.last_instant.is_some_and(|n| now.instant < n)
        {
            return Err(Error::Clock);
        }
        self.last_clock = now.monotonic_ns();
        self.last_instant = Some(now.instant);
        self.clock_pair = Some(now);
        Ok(now)
    }
    fn charge(&mut self, kind: ChargeKind, bytes: &[u8]) -> Result<Charge> {
        self.attempt = self.attempt.checked_add(1).ok_or(Error::Exhausted)?;
        Ok(Charge {
            scope: self.config.scope.clone(),
            connection_epoch: self.epoch,
            attempt: self.attempt,
            kind,
            bytes: bytes.len(),
            digest: Sha256::digest(bytes).into(),
            api_weight: match kind {
                ChargeKind::StreamConnect if self.config.scope.market == Market::Spot => 2,
                ChargeKind::Api(_) => {
                    if self.config.scope.market == Market::Spot {
                        2
                    } else {
                        1
                    }
                }
                _ => 0,
            },
            websocket_messages: u32::from(
                matches!(kind, ChargeKind::Pong | ChargeKind::Close)
                    || matches!(kind, ChargeKind::Api(_))
                        && self.config.scope.market == Market::Spot,
            ),
        })
    }
    fn lane(
        &mut self,
        stream: bool,
        now: ClockSample,
        permit: &mut impl ControlPermit,
    ) -> Result<Option<Lane>> {
        let host = request::authority(
            self.config.scope.market,
            self.config.scope.environment,
            stream,
        );
        let charge = self.charge(
            if stream {
                ChargeKind::StreamConnect
            } else {
                ChargeKind::ManagementConnect
            },
            host.as_bytes(),
        )?;
        if !permit.allow(&charge, &now) {
            return Ok(None);
        }
        let address = if stream {
            self.network.stream_address
        } else {
            self.network.management_address.ok_or(Error::Identity)?
        };
        let connection = net::Connection::connect(
            address,
            net::Security::Tls {
                server_name: host.to_owned().try_into().map_err(|_| Error::Identity)?,
                config: self.network.tls.clone(),
            },
            self.config.limits.io,
            deadline(now.instant, self.config.timing.connect_ns)?,
        )
        .map_err(|_| Error::Transport)?;
        let mut lane = Lane {
            connection,
            http: http::ResponseDecoder::new(self.config.limits.message_bytes, 64)
                .map_err(|_| Error::Bounds)?,
            handshake: None,
            decoder: ws::Decoder::new(
                self.config.limits.message_bytes,
                self.config.limits.message_bytes,
            )
            .map_err(|_| Error::Bounds)?,
            upgraded: false,
            jobs: VecDeque::new(),
            active: None,
            registered: false,
            ended: false,
            needs_input: true,
            opened: now.monotonic_ns(),
            last_frame: now.monotonic_ns(),
        };
        if stream {
            let path = if self.config.scope.market == Market::Spot {
                "/ws-api/v3".to_owned()
            } else {
                format!(
                    "/private/ws/{}",
                    self.listen_key.as_deref().ok_or(Error::Identity)?
                )
            };
            let handshake =
                ws::ClientHandshake::new(host, &path, &[], self.config.limits.request_bytes)
                    .map_err(|_| Error::Capacity)?;
            let bytes = handshake.request().to_vec();
            let charge = self.charge(ChargeKind::Upgrade, &bytes)?;
            lane.jobs.push_back(Job {
                bytes,
                charge,
                token: None,
                deadline: deadline(now.instant, self.config.timing.connect_ns)?,
            });
            lane.handshake = Some(handshake);
        }
        Ok(Some(lane))
    }
    fn begin(&mut self, now: ClockSample, permit: &mut impl ControlPermit) -> Result<()> {
        self.epoch = self.next_epoch;
        self.next_epoch = self.next_epoch.checked_add(1).ok_or(Error::Exhausted)?;
        self.gap.epoch = self.epoch;
        if self.config.scope.market == Market::Spot {
            self.stream = self.lane(true, now, permit)?;
            if self.stream.is_none() {
                return self.fail(Error::Busy, now.monotonic_ns());
            }
            self.phase = Phase::Connecting;
        } else {
            self.phase = Phase::CreatingKey;
            self.planned = Some(Operation::UsdmCreate);
        }
        Ok(())
    }
    fn plan_request(
        &mut self,
        op: Operation,
        now: ClockSample,
        permit: &mut impl ControlPermit,
    ) -> Result<bool> {
        if self.pending.is_some() {
            return Err(Error::Busy);
        }
        self.serial = self.serial.checked_add(1).ok_or(Error::Exhausted)?;
        let token = RequestToken::new(self.epoch, self.serial, op)?;
        let request = request::prepare(
            &self.config.scope,
            &self.auth,
            token,
            &now,
            self.config.timing.recv_window_micros,
            self.config.limits.request_bytes,
        )?;
        let bytes = if self.config.scope.market == Market::Spot {
            ws::encode_client(
                ws::Opcode::Text,
                request.payload(),
                self.config.limits.request_bytes + 14,
            )
            .map_err(|_| Error::Capacity)?
        } else {
            http::encode_request(
                request.method().as_str(),
                request.path(),
                &[
                    ("Host", request.authority()),
                    (
                        "X-MBX-APIKEY",
                        request.api_key_header().ok_or(Error::Authentication)?,
                    ),
                    ("Connection", "close"),
                ],
                &[],
                self.config.limits.request_bytes,
            )
            .map_err(|_| Error::Capacity)?
        };
        if self.config.scope.market == Market::Usdm {
            self.management = self.lane(false, now, permit)?;
            if self.management.is_none() {
                return Ok(false);
            }
        }
        let charge = self.charge(ChargeKind::Api(op), &bytes)?;
        let job = Job {
            bytes,
            charge,
            token: Some(token),
            deadline: deadline(now.instant, self.config.timing.request_ns)?,
        };
        let lane = if self.config.scope.market == Market::Spot {
            self.stream.as_mut()
        } else {
            self.management.as_mut()
        }
        .ok_or(Error::Closed)?;
        if lane.jobs.len() >= self.config.limits.control_frames {
            return Err(Error::Capacity);
        }
        lane.jobs.push_back(job);
        self.pending = Some(Pending {
            request,
            started: now.monotonic_ns(),
            deadline: add(now.monotonic_ns(), self.config.timing.request_ns)?,
            accepted: false,
            complete: false,
        });
        Ok(true)
    }
    fn notify(&mut self, sink: &mut impl FnMut(Delivery<'_>) -> Result<()>) -> Result<()> {
        while let Some(n) = self.notices.pop_front() {
            let d = match n {
                Notice::Gap(n) => Delivery::Gap(n),
                Notice::Control(n) => Delivery::Control(n),
            };
            if sink(d).is_err() {
                self.sink_failed();
                return Err(Error::Sink);
            }
        }
        Ok(())
    }
    fn sink_failed(&mut self) {
        self.reject_pending();
        self.clear_network();
        self.phase = Phase::Stopped;
        self.stopping = true;
        let _ = self.new_gap(Error::Sink);
    }
    pub fn poll(
        &mut self,
        clock: &mut impl Clock,
        budget: PollBudget,
        permit: &mut impl ControlPermit,
        mut sink: impl FnMut(Delivery<'_>) -> Result<()>,
    ) -> Result<PollReport> {
        if budget.io.max_bytes == 0
            || budget.io.max_syscalls == 0
            || budget.max_messages == 0
            || budget.max_control_requests == 0
        {
            return Err(Error::Bounds);
        }
        let before = self.status();
        let now = match self.sample(clock) {
            Ok(n) => n,
            Err(e) => {
                if e == Error::ClockDomain {
                    self.stopping = true;
                }
                self.fail(e, self.last_clock)?;
                self.notify(&mut sink)?;
                return Err(e);
            }
        };
        let mut report = PollReport::default();
        let result = self.poll_inner(now, clock, budget, permit, &mut sink, &mut report);
        if let Err(e) = result {
            if e == Error::ClockDomain {
                self.stopping = true;
            }
            if e == Error::Sink {
                return Err(e);
            }
            if e == Error::Exhausted {
                self.exhaust();
            } else {
                self.fail(e, self.last_clock)?;
            }
        }
        self.notify(&mut sink)?;
        report.registration_changed = self.status().registration_changed;
        if self.status() != before && sink(Delivery::State(self.status())).is_err() {
            self.sink_failed();
            return Err(Error::Sink);
        }
        Ok(report)
    }
    fn poll_inner(
        &mut self,
        now: ClockSample,
        clock: &mut impl Clock,
        budget: PollBudget,
        permit: &mut impl ControlPermit,
        sink: &mut impl FnMut(Delivery<'_>) -> Result<()>,
        report: &mut PollReport,
    ) -> Result<()> {
        self.notify(sink)?;
        if matches!(self.phase, Phase::Stopped | Phase::Exhausted) {
            return Ok(());
        }
        if self
            .pending
            .as_ref()
            .is_some_and(|p| now.monotonic_ns() >= p.deadline)
            || self.lease.is_some_and(|v| now.monotonic_ns() >= v)
        {
            return Err(Error::Stale);
        }
        if !self.draining
            && self.stream.as_ref().is_some_and(|s| {
                now.monotonic_ns()
                    >= s.opened.saturating_add(if s.upgraded {
                        self.config.timing.rotate_ns
                    } else {
                        self.config.timing.connect_ns
                    })
                    || now.monotonic_ns()
                        >= s.last_frame.saturating_add(self.config.timing.heartbeat_ns)
            })
        {
            return Err(Error::Stale);
        }
        if self.phase == Phase::Disconnected
            || self.phase == Phase::Backoff
                && !self.draining
                && now.monotonic_ns() >= self.backoff_until
        {
            self.begin(now, permit)?;
        }
        if self.phase == Phase::Backoff && !self.draining {
            return Ok(());
        }
        if self.config.scope.market == Market::Usdm
            && self.phase == Phase::Connecting
            && self.stream.is_none()
        {
            self.stream = self.lane(true, now, permit)?;
            if self.stream.is_none() {
                return Err(Error::Busy);
            }
        }
        if self.phase == Phase::Receiving
            && self.config.scope.market == Market::Usdm
            && self.pending.is_none()
            && self.renew.is_some_and(|n| now.monotonic_ns() >= n)
        {
            self.planned = Some(Operation::UsdmKeepalive);
        }
        if let Some(op) = self.planned
            && report.control_requests < budget.max_control_requests
        {
            if self.plan_request(op, now, permit)? {
                self.planned = None;
                report.control_requests += 1;
            } else {
                return Err(Error::Busy);
            }
        }
        self.drain_events(budget, sink, report)?;
        // Management first bounds renewal latency even while private data is busy.
        for stream in [false, true] {
            if stream && report.messages >= budget.max_messages {
                report.yielded = true;
                continue;
            }
            let Some(mut lane) = (if stream {
                self.stream.take()
            } else {
                self.management.take()
            }) else {
                continue;
            };
            let outcome = drive_lane(
                &mut lane,
                self.pending.as_mut(),
                clock,
                permit,
                now,
                budget,
                report,
                &mut self.last_clock,
                &mut self.last_instant,
            );
            let (bytes, closed) = match outcome {
                Ok(v) => v,
                Err(e) => {
                    if stream {
                        self.stream = Some(lane);
                    } else {
                        self.management = Some(lane);
                    }
                    return Err(e);
                }
            };
            let now = self.sample(clock)?;
            if self
                .pending
                .as_ref()
                .is_some_and(|p| now.monotonic_ns() >= p.deadline)
                || self.lease.is_some_and(|n| now.monotonic_ns() >= n)
            {
                return Err(Error::Stale);
            }
            if stream {
                if lane.upgraded {
                    lane.decoder.push(&bytes).map_err(|_| Error::Protocol)?;
                } else {
                    lane.http.push(&bytes).map_err(|_| Error::Protocol)?;
                    if closed {
                        lane.http.finish();
                    }
                    if let Some(response) =
                        lane.http.next_response().map_err(|_| Error::Protocol)?
                    {
                        lane.handshake
                            .take()
                            .ok_or(Error::Protocol)?
                            .validate(&response)
                            .map_err(|_| Error::Protocol)?;
                        lane.decoder
                            .push(&lane.http.take_buffered())
                            .map_err(|_| Error::Capacity)?;
                        lane.upgraded = true;
                        lane.last_frame = now.monotonic_ns();
                        if self.config.scope.market == Market::Spot {
                            self.planned =
                                Some(if matches!(self.auth, Authentication::SpotLogon { .. }) {
                                    self.phase = Phase::Authenticating;
                                    Operation::SpotLogon
                                } else {
                                    self.phase = Phase::Subscribing;
                                    Operation::SpotSubscribeSignature
                                });
                        } else {
                            self.phase = Phase::Receiving;
                            self.failures = 0;
                        }
                    }
                }
                if self.closing_frame && lane.active.is_none() && lane.jobs.is_empty() {
                    return Err(Error::Closed);
                }
                self.stream = Some(lane);
                self.revoke_ended_stream(sink)?;
                self.stream_messages(now, budget, report)?;
                self.revoke_ended_stream(sink)?;
                self.drain_events(budget, sink, report)?;
                if closed {
                    if !self.draining {
                        return Err(Error::Transport);
                    }
                    if !report.yielded && self.early.is_empty() {
                        self.clear_network();
                        self.backoff_until =
                            add(now.monotonic_ns(), self.config.timing.backoff_ns)?
                                .max(self.cooldown_until);
                    }
                }
            } else {
                lane.http.push(&bytes).map_err(|_| Error::Protocol)?;
                if closed {
                    lane.http.finish();
                }
                if let Some(response) = lane.http.next_response().map_err(|_| Error::Protocol)? {
                    self.http_response(response, now)?;
                } else {
                    self.management = Some(lane);
                    if closed {
                        return Err(Error::Transport);
                    }
                }
            }
        }
        self.drain_events(budget, sink, report)?;
        Ok(())
    }
    fn revoke_ended_stream(
        &mut self,
        sink: &mut impl FnMut(Delivery<'_>) -> Result<()>,
    ) -> Result<()> {
        if self.stream.as_ref().is_some_and(|s| s.ended)
            && self.phase == Phase::Receiving
            && !self.draining
        {
            // Run before and after decoding: an ACK can share a read with EOF.
            self.reject_pending();
            self.management = None;
            self.planned = None;
            self.lease = None;
            self.renew = None;
            self.draining = true;
            self.phase = Phase::Backoff;
            self.new_gap(Error::Transport)?;
            self.notify(sink)?;
        }
        Ok(())
    }
    fn stream_messages(
        &mut self,
        now: ClockSample,
        budget: PollBudget,
        report: &mut PollReport,
    ) -> Result<()> {
        while !self.closing_frame
            && report.messages < budget.max_messages
            && self.stream.as_ref().is_some_and(|s| s.upgraded)
        {
            let message = self
                .stream
                .as_mut()
                .unwrap()
                .decoder
                .poll_message()
                .map_err(|_| Error::Protocol)?;
            match message {
                ws::PollMessage::NeedInput => {
                    self.stream.as_mut().unwrap().needs_input = true;
                    break;
                }
                ws::PollMessage::Yielded => {
                    self.stream.as_mut().unwrap().needs_input = false;
                    report.yielded = true;
                    break;
                }
                ws::PollMessage::Message(message) => {
                    report.messages += 1;
                    self.stream.as_mut().unwrap().last_frame = now.monotonic_ns();
                    match message.opcode {
                        ws::Opcode::Ping => {
                            if self.draining {
                                continue;
                            }
                            let bytes = ws::encode_client(ws::Opcode::Pong, &message.payload, 139)
                                .map_err(|_| Error::Protocol)?;
                            let charge = self.charge(ChargeKind::Pong, &bytes)?;
                            let lane = self.stream.as_mut().unwrap();
                            if lane.active.as_ref().is_some_and(|j| j.token.is_some())
                                && lane
                                    .connection
                                    .send_report()
                                    .is_some_and(|r| r.accepted_bytes == 0)
                            {
                                lane.connection.cancel().map_err(|_| Error::Transport)?;
                                lane.connection.take_finished();
                                lane.jobs.push_front(lane.active.take().unwrap());
                            }
                            if lane.jobs.len() >= self.config.limits.control_frames {
                                return Err(Error::Capacity);
                            }
                            lane.jobs.push_front(Job {
                                bytes,
                                charge,
                                token: None,
                                deadline: deadline(now.instant, self.config.timing.request_ns)?,
                            });
                        }
                        ws::Opcode::Pong => {}
                        ws::Opcode::Close => {
                            let lane = self.stream.as_mut().unwrap();
                            // A partially accepted application frame cannot be spliced with Close.
                            if lane.connection.send_report().is_some_and(|r| {
                                r.accepted_bytes > 0 && r.accepted_bytes < r.total_bytes
                            }) {
                                return Err(Error::Closed);
                            }
                            if lane.active.is_some() {
                                lane.connection.cancel().map_err(|_| Error::Transport)?;
                                lane.connection.take_finished();
                                lane.active = None;
                            }
                            lane.jobs.clear();
                            let bytes = ws::encode_client(ws::Opcode::Close, &message.payload, 139)
                                .map_err(|_| Error::Protocol)?;
                            let charge = self.charge(ChargeKind::Close, &bytes)?;
                            self.stream.as_mut().unwrap().jobs.push_back(Job {
                                bytes,
                                charge,
                                token: None,
                                deadline: deadline(now.instant, self.config.timing.request_ns)?,
                            });
                            self.reject_pending();
                            self.management = None;
                            self.planned = None;
                            self.lease = None;
                            self.renew = None;
                            self.draining = true;
                            self.closing_frame = true;
                            self.phase = Phase::Closing;
                            self.new_gap(Error::Closed)?;
                            break;
                        }
                        ws::Opcode::Binary => return Err(Error::Protocol),
                        ws::Opcode::Text => self.text_message(message.payload, now)?,
                    }
                }
            }
        }
        if report.messages == budget.max_messages {
            if let Some(s) = self.stream.as_mut() {
                s.needs_input = false;
            }
            report.yielded = true;
        }
        Ok(())
    }
    fn text_message(&mut self, bytes: Vec<u8>, now: ClockSample) -> Result<()> {
        let value: Value = crate::response::parse(&bytes)?;
        if self.config.scope.market == Market::Spot && value.get("id").is_some() {
            if value["id"].is_null() && value["status"] == 401 {
                return Err(Error::Authentication);
            }
            let Some(p) = &self.pending else {
                return Ok(());
            };
            if value["id"].as_str() != Some(p.request.token().id().as_str()) {
                return Ok(());
            }
            if !p.complete {
                return Err(Error::Protocol);
            }
            let status = value["status"]
                .as_u64()
                .and_then(|n| u16::try_from(n).ok())
                .ok_or(Error::Protocol)?;
            let code = value["error"]["code"].as_i64();
            if status != 200 {
                let retry = value["error"]["data"]["retryAfter"]
                    .as_u64()
                    .or_else(|| value["retryAfter"].as_u64());
                let delay = retry.and_then(|t| {
                    now.signing_timestamp()
                        .and_then(request::timestamp_us)
                        .and_then(|us| u64::try_from(us / 1000).ok())
                        .map(|unix| t.saturating_sub(unix).saturating_mul(1_000_000))
                });
                self.control_error(
                    status,
                    code,
                    value["error"]["msg"].as_str().unwrap_or(""),
                    delay,
                    now,
                )?;
                return Err(Error::Authentication);
            }
            let result = value.get("result").ok_or(Error::Protocol)?;
            let op = p.request.token().operation();
            match op {
                Operation::SpotLogon => {
                    if result["apiKey"].as_str() != Some(request::auth_key(&self.auth)) {
                        return Err(Error::Identity);
                    }
                    self.phase = Phase::Subscribing;
                    self.planned = Some(Operation::SpotSubscribe);
                }
                Operation::SpotSubscribe | Operation::SpotSubscribeSignature => {
                    let id = result["subscriptionId"]
                        .as_u64()
                        .filter(|v| *v <= i64::MAX as u64)
                        .ok_or(Error::Protocol)?;
                    self.subscription = Some(id);
                    self.phase = Phase::Receiving;
                    self.failures = 0;
                }
                Operation::SpotStatus => {
                    if result["userDataStream"] != true
                        || matches!(self.auth, Authentication::SpotLogon { .. })
                            && result["apiKey"].as_str() != Some(request::auth_key(&self.auth))
                        || matches!(self.auth, Authentication::SpotSignature { .. })
                            && result.get("apiKey") != Some(&Value::Null)
                    {
                        return Err(Error::Identity);
                    }
                    self.planned = Some(Operation::SpotSubscriptions);
                }
                Operation::SpotSubscriptions => {
                    let entries = result.as_array().ok_or(Error::Protocol)?;
                    if entries.len() != 1
                        || entries[0]["subscriptionId"].as_u64() != self.subscription
                    {
                        return Err(Error::Identity);
                    }
                }
                Operation::SpotUnsubscribe { .. } => {
                    self.subscription = None;
                    if matches!(self.auth, Authentication::SpotLogon { .. }) {
                        self.planned = Some(Operation::SpotLogout);
                    } else {
                        self.phase = Phase::Stopped;
                        self.stream = None;
                    }
                }
                Operation::SpotLogout => {
                    self.phase = Phase::Stopped;
                    self.stream = None;
                }
                _ => return Err(Error::Protocol),
            }
            self.confirm(status, code, true)?;
            return Ok(());
        }
        if value["event"]["e"] == "serverShutdown" {
            return Err(Error::Closed);
        }
        if self.stopping {
            return Ok(());
        }
        if self.config.scope.market == Market::Spot {
            if !self.draining && !matches!(self.phase, Phase::Subscribing | Phase::Receiving) {
                return Err(Error::Protocol);
            }
            if value["subscriptionId"].as_u64().is_none() {
                return Err(Error::Protocol);
            }
        }
        if self.early.len() >= self.config.limits.early_messages
            || bytes.len() > self.config.limits.early_bytes - self.early_bytes
        {
            return Err(Error::Capacity);
        }
        self.early_bytes += bytes.len();
        self.early.push_back(QueuedEvent {
            bytes,
            arrived: now.monotonic_ns(),
        });
        Ok(())
    }
    fn confirm(&mut self, status: u16, code: Option<i64>, success: bool) -> Result<()> {
        if !self.pending.as_ref().is_some_and(|p| p.complete) {
            return Err(Error::Protocol);
        }
        let p = self.pending.take().ok_or(Error::Protocol)?;
        self.notices.push_back(Notice::Control(ControlOutcome {
            token: p.request.token(),
            state: if success {
                ControlState::Confirmed
            } else {
                ControlState::Rejected
            },
            status: Some(status),
            code,
        }));
        Ok(())
    }
    fn control_error(
        &mut self,
        status: u16,
        code: Option<i64>,
        message: &str,
        retry_ns: Option<u64>,
        now: ClockSample,
    ) -> Result<()> {
        use tickwire_binance::response::{FailureKind, classify};
        let kind = classify(self.config.scope.market, status, code, message);
        if matches!(kind, FailureKind::RateLimited | FailureKind::Banned) {
            let minimum = if kind == FailureKind::Banned {
                120_000_000_000
            } else {
                60_000_000_000
            };
            self.cooldown_until =
                add(now.monotonic_ns(), retry_ns.unwrap_or(minimum).max(minimum))?;
        }
        let unknown =
            matches!(kind, FailureKind::Unknown | FailureKind::Partial) && code != Some(-1125);
        if !self.pending.as_ref().is_some_and(|p| p.complete) {
            return Err(Error::Protocol);
        }
        let p = self.pending.take().ok_or(Error::Protocol)?;
        if unknown && p.request.token().operation() == Operation::UsdmClose {
            self.remote_close_unknown = true;
        }
        self.notices.push_back(Notice::Control(ControlOutcome {
            token: p.request.token(),
            state: if unknown {
                ControlState::MayHaveApplied
            } else {
                ControlState::Rejected
            },
            status: Some(status),
            code,
        }));
        Ok(())
    }
    fn http_response(&mut self, response: http::Response, now: ClockSample) -> Result<()> {
        let p = self.pending.as_ref().ok_or(Error::Protocol)?;
        if !p.complete {
            return Err(Error::Protocol);
        }
        let op = p.request.token().operation();
        let started = p.started;
        let value: Value = if response.body.is_empty() {
            Value::Null
        } else {
            crate::response::parse(&response.body)?
        };
        let code = value["code"].as_i64();
        if response.status != 200 {
            let delay = response
                .header("retry-after")
                .and_then(|v| std::str::from_utf8(v).ok())
                .and_then(|v| v.parse::<u64>().ok())
                .and_then(|n| n.checked_mul(1_000_000_000));
            self.control_error(
                response.status,
                code,
                value["msg"].as_str().unwrap_or(""),
                delay,
                now,
            )?;
            return Err(Error::Authentication);
        }
        match op {
            Operation::UsdmCreate | Operation::UsdmKeepalive => {
                let key = value["listenKey"]
                    .as_str()
                    .filter(|k| {
                        !k.is_empty()
                            && k.len() <= 512
                            && k.bytes()
                                .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
                    })
                    .ok_or(Error::Protocol)?;
                if op == Operation::UsdmKeepalive && self.listen_key.as_deref() != Some(key) {
                    return Err(Error::Identity);
                }
                let lease = add(started, KEY_LIFETIME)?;
                if now.monotonic_ns() >= lease
                    || self.lease.is_some_and(|old| now.monotonic_ns() >= old)
                {
                    return Err(Error::Stale);
                }
                self.listen_key = Some(key.to_owned());
                self.lease = Some(lease);
                self.renew = Some(add(started, self.config.timing.renew_ns)?);
                if op == Operation::UsdmCreate {
                    self.phase = Phase::Connecting;
                }
            }
            Operation::UsdmClose => {
                self.clear_network();
                self.phase = Phase::Stopped;
            }
            _ => return Err(Error::Protocol),
        }
        self.confirm(response.status, code, true)
    }
    fn drain_events(
        &mut self,
        budget: PollBudget,
        sink: &mut impl FnMut(Delivery<'_>) -> Result<()>,
        report: &mut PollReport,
    ) -> Result<()> {
        self.notify(sink)?;
        while (self.phase == Phase::Receiving || self.draining)
            && report.messages < budget.max_messages
        {
            let Some(event) = self.early.pop_front() else {
                break;
            };
            self.early_bytes -= event.bytes.len();
            report.messages += 1;
            let scope = &self.config.scope;
            let stream = if scope.market == Market::Spot {
                PrivateStream::SpotSubscription {
                    subscription_id: self.subscription.ok_or(Error::Identity)?,
                }
            } else {
                PrivateStream::UsdmListenKey
            };
            let context = SessionContext::new(
                SessionIdentity {
                    risk_domain_id: scope.risk_domain_id,
                    venue: scope.venue,
                    environment: scope.environment,
                    account: scope.account,
                    connection_epoch: self.epoch,
                },
                scope.market,
                stream,
                TimeUnit::Milliseconds,
            )
            .map_err(|_| Error::Identity)?;
            let decoded = tickwire_binance::events::decode_private::<4096>(
                &context,
                &event.bytes,
                self.config.limits.message_bytes,
                event.arrived,
            )
            .map_err(|_| Error::Protocol)?;
            if matches!(
                decoded.body(),
                PrivateBody::StreamTerminated | PrivateBody::ListenKeyExpired
            ) {
                self.fail(Error::Closed, self.last_clock)?;
                self.notify(sink)?;
            }
            if sink(Delivery::Event(PrivateEvent(decoded))).is_err() {
                self.sink_failed();
                return Err(Error::Sink);
            }
        }
        if !self.early.is_empty() && (self.phase == Phase::Receiving || self.draining) {
            report.yielded = true;
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn drive_lane(
    lane: &mut Lane,
    mut pending: Option<&mut Pending>,
    clock: &mut impl Clock,
    permit: &mut impl ControlPermit,
    now: ClockSample,
    budget: PollBudget,
    report: &mut PollReport,
    last_clock: &mut u64,
    last_instant: &mut Option<Instant>,
) -> Result<(Vec<u8>, bool)> {
    if !lane.ended
        && lane.active.is_none()
        && let Some(job) = lane.jobs.pop_front()
    {
        lane.connection
            .queue(
                net::PreparedSend::new(&job.bytes, job.deadline, 16 << 20)
                    .map_err(|_| Error::Capacity)?,
            )
            .map_err(|_| Error::Transport)?;
        lane.active = Some(job);
    }
    if !lane.ended
        && let Some(job) = &lane.active
        && report.control_requests < budget.max_control_requests
    {
        let p = pending.as_deref();
        let mut clock_fault = None;
        let send = lane
            .connection
            .try_send(now.instant, || {
                let first = match clock.sample() {
                    Ok(sample) => sample,
                    Err(e) => {
                        clock_fault = Some(e);
                        return false;
                    }
                };
                if first.monotonic.domain() != now.monotonic.domain() {
                    clock_fault = Some(Error::ClockDomain);
                    return false;
                }
                if first.monotonic_ns() < *last_clock
                    || last_instant.is_some_and(|t| first.instant < t)
                {
                    return false;
                }
                *last_clock = first.monotonic_ns();
                *last_instant = Some(first.instant);
                if job.token.is_some()
                    && !p.is_some_and(|p| {
                        Some(p.request.token()) == job.token
                            && match request::check_clock(&p.request, &first, clock) {
                                Ok(()) => first.monotonic_ns() < p.deadline,
                                Err(e) => {
                                    clock_fault = Some(e);
                                    false
                                }
                            }
                    })
                {
                    return false;
                }
                if !permit.allow(&job.charge, &first) {
                    return false;
                }
                let second = match clock.sample() {
                    Ok(sample) => sample,
                    Err(e) => {
                        clock_fault = Some(e);
                        return false;
                    }
                };
                if second.monotonic.domain() != now.monotonic.domain() {
                    clock_fault = Some(Error::ClockDomain);
                    return false;
                }
                if second.monotonic_ns() < *last_clock
                    || last_instant.is_some_and(|t| second.instant < t)
                {
                    return false;
                }
                *last_clock = second.monotonic_ns();
                *last_instant = Some(second.instant);
                let checked = job.token.is_none()
                    || p.is_some_and(|p| match request::check_clock(&p.request, &second, clock) {
                        Ok(()) => second.monotonic_ns() < p.deadline,
                        Err(e) => {
                            clock_fault = Some(e);
                            false
                        }
                    });
                if !checked {
                    return false;
                }
                // check_clock may invoke a delivery-budget callback. Sample once
                // after it and use only pure checks before returning to write.
                let last = match clock.sample() {
                    Ok(sample) => sample,
                    Err(e) => {
                        clock_fault = Some(e);
                        return false;
                    }
                };
                if last.monotonic.domain() != now.monotonic.domain() {
                    clock_fault = Some(Error::ClockDomain);
                    return false;
                }
                if last.monotonic_ns() < *last_clock
                    || last_instant.is_some_and(|t| last.instant < t)
                {
                    clock_fault = Some(Error::Clock);
                    return false;
                }
                *last_clock = last.monotonic_ns();
                *last_instant = Some(last.instant);
                job.token.is_none()
                    || p.is_some_and(|p| p.request.fresh(&last) && last.monotonic_ns() < p.deadline)
            })
            .map_err(|_| Error::Transport)?;
        report.control_requests += 1;
        if let Some(p) = pending.as_deref_mut()
            && job.token == Some(p.request.token())
        {
            p.accepted |= send.accepted_bytes > 0;
            p.complete |= send.accepted_bytes == send.total_bytes;
        }
        if let Some(e) = clock_fault {
            return Err(e);
        }
        if matches!(
            send.state,
            net::SendState::NoBytes(_) | net::SendState::Failed(_)
        ) {
            return Err(Error::Transport);
        }
    }
    let mut closed = false;
    if !lane.ended
        && !report.io_budget_exhausted
        && report.syscalls < budget.io.max_syscalls
        && report.network_bytes < budget.io.max_bytes
    {
        match lane.connection.drive(
            now.instant,
            net::IoBudget {
                max_syscalls: budget.io.max_syscalls - report.syscalls,
                max_bytes: budget.io.max_bytes - report.network_bytes,
            },
        ) {
            Ok(d) => {
                report.syscalls += d.syscalls;
                report.network_bytes += d.network_read + d.network_written;
                closed = d.peer_closed;
            }
            // A complete authenticated HTTP response can precede a TCP EOF or
            // missing TLS close_notify. Consume already decrypted bytes first.
            Err(_) => {
                report.io_budget_exhausted = true;
                closed = true;
            }
        }
    }
    if let Some(done) = lane.connection.take_finished() {
        if let Some(p) = pending
            && lane.active.as_ref().and_then(|j| j.token) == Some(p.request.token())
        {
            p.accepted |= done.accepted_bytes > 0;
            p.complete |= done.accepted_bytes == done.total_bytes;
        }
        if done.state != net::SendState::Flushed {
            closed = true;
        }
        lane.active = None;
    }
    lane.ended |= closed;
    let read = !lane.upgraded || lane.needs_input;
    let mut bytes = vec![
        0;
        if read {
            8192.min(budget.io.max_bytes)
        } else {
            0
        }
    ];
    let n = lane.connection.read(&mut bytes);
    bytes.truncate(n);
    let drained = lane.ended && read && bytes.is_empty();
    Ok((bytes, drained))
}

#[cfg(test)]
mod counter_boundaries {
    use super::*;
    fn client(key: &SigningKey) -> PrivateClient<'_> {
        let scope = AccountScope {
            risk_domain_id: [1; 16],
            venue: 1,
            environment: Environment::Test,
            account: 1,
            market: Market::Spot,
            credential_generation: 1,
        };
        let tls = std::sync::Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        );
        PrivateClient::new(
            SessionConfig::new(scope),
            Authentication::SpotSignature {
                api_key: "fixture",
                signing_key: key,
            },
            NetworkConfig {
                stream_address: "127.0.0.1:9".parse().unwrap(),
                management_address: None,
                tls,
            },
            1,
        )
        .unwrap()
    }
    #[test]
    fn exhausted_gap_cannot_leave_receiving_status() {
        let key = SigningKey::hmac(b"fixture").unwrap();
        let mut c = client(&key);
        c.phase = Phase::Receiving;
        c.gap.generation = u64::MAX;
        assert_eq!(c.stop(StopMode::Disconnect), Err(Error::Exhausted));
        assert_eq!(c.status().phase, Phase::Exhausted);
        assert!(c.stopping);
    }
    #[test]
    fn exhausted_backoff_cannot_leave_receiving_status() {
        let key = SigningKey::hmac(b"fixture").unwrap();
        let mut c = client(&key);
        c.phase = Phase::Receiving;
        assert_eq!(c.fail(Error::Transport, u64::MAX), Err(Error::Exhausted));
        assert_eq!(c.status().phase, Phase::Exhausted);
        assert!(c.stopping);
    }
}
