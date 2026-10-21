//! Explicit single-stream owner for typed public market events.
use crate::*;
pub use tickwire_binance::market::{
    Event, EventContext, EventEncoding, EventKind, EventTimes, EventValue, StreamIdentity,
};
#[derive(Clone, Debug)]
pub struct EventConfig {
    pub owner_id: [u8; 16],
    pub source: Source,
    pub domain: ClockDomain,
    pub first_connection_epoch: u64,
    pub first_operation: u64,
    pub context: EventContext,
    pub encoding: EventEncoding,
    pub subscribe: SubscribeMode,
    pub fast_mark: bool,
    pub timing: Timing,
    pub limits: Limits,
    pub max_events: usize,
}
pub struct EventNetworkConfig {
    pub stream_address: SocketAddr,
    pub tls: Arc<rustls::ClientConfig>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventStatus {
    pub phase: Phase,
    pub connection_epoch: u64,
    pub kind: EventKind,
    pub usable: bool,
    pub last_event_mono_ns: Option<u64>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventGap {
    pub identity: StreamIdentity,
    pub kind: EventKind,
    pub reason: Error,
}
pub enum EventNotice<'a> {
    Event {
        event: &'a Event,
        raw: &'a [u8],
        usable: bool,
    },
    Gap(EventGap),
    Http(&'a HttpFeedback),
}
pub trait EventSink {
    fn on_event(&mut self, notice: EventNotice<'_>) -> bool;
}
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};
use tickwire_transport::{self as net, http, ws};
struct Job {
    bytes: Vec<u8>,
    charge: PublicCharge,
    deadline: u64,
    instant: Instant,
    marked: bool,
}
struct Batch {
    raw: Vec<u8>,
    events: Vec<Event>,
    position: usize,
}
struct Stream {
    connection: net::Connection,
    http: http::ResponseDecoder,
    handshake: ws::ClientHandshake,
    decoder: ws::Decoder,
    spans: VecDeque<(usize, u64)>,
    jobs: VecDeque<Job>,
    active: Option<Job>,
    pending: VecDeque<Batch>,
    pending_count: usize,
    pending_bytes: usize,
    registered: bool,
    upgraded: bool,
    upgrade_sent: bool,
    subscribe_id: Option<u32>,
    subscribe_sent: bool,
    ready: bool,
    needs_input: bool,
    ended: bool,
    opened: u64,
    last_frame: u64,
    phase_deadline: u64,
}
pub struct PublicEventClient<C> {
    config: EventConfig,
    network: EventNetworkConfig,
    clock: C,
    sbe: Option<SbeAccess>,
    last: Option<LocalTime>,
    phase: Phase,
    epoch: u64,
    next_epoch: Option<u64>,
    operation: Option<u64>,
    stream: Option<Stream>,
    gap: Option<EventGap>,
    retry_at: u64,
    failures: u32,
    usable: bool,
    revoked: bool,
    last_event: Option<u64>,
}
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).ok_or(Error::Exhausted)
}
fn instant(t: LocalTime, ns: u64) -> Result<Instant> {
    t.instant
        .checked_add(Duration::from_nanos(ns))
        .ok_or(Error::Exhausted)
}
impl EventConfig {
    fn validate(&self, n: &EventNetworkConfig, sbe: bool) -> Result<()> {
        self.context.validate().map_err(|_| Error::Invalid)?;
        let spot = matches!(self.source, Source::SpotProduction | Source::SpotTest);
        if self.owner_id == [0; 16]
            || self.first_connection_epoch == 0
            || self.first_connection_epoch >= 1 << 63
            || self.context.identity.connection_epoch != self.first_connection_epoch
            || self.first_operation == 0
            || self.first_operation > u32::MAX as u64
            || spot != (self.context.market == tickwire_binance::request::Market::Spot)
            || self.fast_mark && self.context.kind != EventKind::MarkFunding
            || n.tls.enable_early_data
            || sbe != (self.encoding == EventEncoding::SpotSbeV1)
            || sbe
                && (self.source != Source::SpotProduction
                    || self.context.time_unit != TimeUnit::Microseconds)
        {
            return Err(Error::Invalid);
        }
        self.context
            .stream_name(self.encoding, self.fast_mark)
            .map_err(|_| Error::Invalid)?;
        let t = self.timing;
        let l = self.limits;
        if [
            t.connect_ns,
            t.request_ns,
            t.first_event_ns,
            t.heartbeat_ns,
            t.pong_ns,
            t.rotate_ns,
            t.backoff_ns,
        ]
        .contains(&0)
            || t.max_backoff_ns < t.backoff_ns
            || t.rotate_ns >= 86_400_000_000_000
            || t.pong_ns
                >= if spot {
                    60_000_000_000
                } else {
                    600_000_000_000
                }
            || self.max_events == 0
            || self.max_events > 65536
            || l.request_bytes == 0
            || l.request_bytes > 1 << 20
            || l.request_bytes > l.io.max_send_bytes
            || l.message_bytes == 0
            || l.message_bytes > 16 << 20
            || l.http_bytes == 0
            || l.http_bytes > 16 << 20
            || l.http_headers == 0
            || l.http_headers > 256
            || l.control_frames == 0
            || l.control_frames > 1024
            || l.timestamp_segments == 0
            || l.timestamp_segments > 65536
            || l.total_buffer_bytes > 256 << 20
            || [
                l.io.max_send_bytes,
                l.io.max_receive_bytes,
                l.io.tls_buffer_bytes,
                l.io.max_accept_bytes,
            ]
            .iter()
            .any(|v| *v == 0 || *v > 16 << 20)
        {
            return Err(Error::Bounds);
        }
        let total = l
            .message_bytes
            .checked_mul(4)
            .and_then(|v| v.checked_add(l.http_bytes))
            .and_then(|v| v.checked_add(l.io.max_send_bytes))
            .and_then(|v| v.checked_add(l.io.max_receive_bytes.saturating_mul(2)))
            .and_then(|v| v.checked_add(l.io.tls_buffer_bytes))
            .and_then(|v| v.checked_add(self.max_events * std::mem::size_of::<Event>()))
            .and_then(|v| v.checked_add(l.request_bytes * (l.control_frames + 1)))
            .and_then(|v| v.checked_add(l.timestamp_segments * std::mem::size_of::<(usize, u64)>()))
            .ok_or(Error::Bounds)?;
        if total > l.total_buffer_bytes {
            return Err(Error::Bounds);
        }
        Ok(())
    }
}
impl<C: MonotonicClock> PublicEventClient<C> {
    pub fn new(config: EventConfig, network: EventNetworkConfig, clock: C) -> Result<Self> {
        Self::create(config, network, clock, None)
    }
    pub fn new_sbe(
        config: EventConfig,
        network: EventNetworkConfig,
        clock: C,
        access: SbeAccess,
    ) -> Result<Self> {
        Self::create(config, network, clock, Some(access))
    }
    fn create(
        config: EventConfig,
        network: EventNetworkConfig,
        clock: C,
        sbe: Option<SbeAccess>,
    ) -> Result<Self> {
        config.validate(&network, sbe.is_some())?;
        Ok(Self {
            next_epoch: Some(config.first_connection_epoch),
            operation: Some(config.first_operation),
            config,
            network,
            clock,
            sbe,
            last: None,
            phase: Phase::Idle,
            epoch: 0,
            stream: None,
            gap: None,
            retry_at: 0,
            failures: 0,
            usable: false,
            revoked: false,
            last_event: None,
        })
    }
    fn local(&mut self) -> Result<LocalTime> {
        let n = self.clock.sample().map_err(|_| Error::Clock)?;
        if n.monotonic.domain() != self.config.domain
            || self
                .last
                .is_some_and(|p| n.instant < p.instant || n.monotonic.nanos() < p.monotonic.nanos())
        {
            return Err(Error::Clock);
        }
        self.last = Some(n);
        Ok(n)
    }
    fn charge(&mut self, kind: ChargeKind) -> Result<PublicCharge> {
        let operation = self.operation.ok_or(Error::Exhausted)?;
        self.operation = operation.checked_add(1);
        Ok(PublicCharge {
            owner_id: self.config.owner_id,
            domain: self.config.domain,
            source: self.config.source,
            lane: Lane::Stream,
            connection_epoch: self.epoch,
            operation,
            kind,
            api_weight: 0,
        })
    }
    fn job(&mut self, kind: ChargeKind, bytes: Vec<u8>, now: LocalTime, span: u64) -> Result<Job> {
        Ok(Job {
            bytes,
            charge: self.charge(kind)?,
            deadline: add(now.monotonic.nanos(), span)?,
            instant: instant(now, span)?,
            marked: false,
        })
    }
    fn host(&self) -> &'static str {
        if self.sbe.is_some() {
            "stream-sbe.binance.com"
        } else {
            protocol::endpoints(self.config.source).0
        }
    }
    fn path(&self) -> Result<String> {
        let ctx = &self.config.context;
        let prefix = if ctx.market == tickwire_binance::request::Market::Spot {
            ""
        } else if ctx.kind == EventKind::BestBidAsk {
            "/public"
        } else {
            "/market"
        };
        let mut path = match self.config.subscribe {
            SubscribeMode::RawUrl => format!(
                "{prefix}/ws/{}",
                ctx.stream_name(self.config.encoding, self.config.fast_mark)
                    .map_err(|_| Error::Invalid)?
            ),
            SubscribeMode::Explicit => {
                if prefix.is_empty() {
                    "/ws".into()
                } else {
                    format!("{prefix}/stream")
                }
            }
        };
        if self.sbe.is_none() && ctx.time_unit == TimeUnit::Microseconds {
            path.push_str("?timeUnit=MICROSECOND");
        }
        Ok(path)
    }
    fn open(&mut self, permit: &mut impl PublicPermit) -> Result<()> {
        self.epoch = self.next_epoch.ok_or(Error::Exhausted)?;
        self.next_epoch = self.epoch.checked_add(1).filter(|e| *e < 1 << 63);
        self.config.context.identity.connection_epoch = self.epoch;
        let before = self.local()?;
        let charge = self.charge(ChargeKind::Connect)?;
        let allow = permit.allow(&charge, before.monotonic);
        let now = self.local()?;
        if !allow {
            return Err(Error::Denied);
        }
        let connection = net::Connection::connect(
            self.network.stream_address,
            net::Security::Tls {
                server_name: self
                    .host()
                    .to_owned()
                    .try_into()
                    .map_err(|_| Error::Invalid)?,
                config: self.network.tls.clone(),
            },
            self.config.limits.io,
            instant(now, self.config.timing.connect_ns)?,
        )
        .map_err(|_| Error::Transport)?;
        let hs = self
            .sbe
            .as_ref()
            .map(|s| [("X-MBX-APIKEY", s.api_key.as_str())]);
        let handshake = ws::ClientHandshake::new(
            self.host(),
            &self.path()?,
            hs.as_ref().map_or(&[], |h| h.as_slice()),
            self.config.limits.request_bytes,
        )
        .map_err(|_| Error::Protocol)?;
        let span = add(self.config.timing.connect_ns, self.config.timing.request_ns)?;
        let job = self.job(ChargeKind::Upgrade, handshake.request().to_vec(), now, span)?;
        self.stream = Some(Stream {
            connection,
            http: http::ResponseDecoder::new(
                self.config.limits.http_bytes,
                self.config.limits.http_headers,
            )
            .map_err(|_| Error::Bounds)?,
            handshake,
            decoder: ws::Decoder::new(
                self.config.limits.message_bytes,
                self.config.limits.message_bytes,
            )
            .map_err(|_| Error::Bounds)?,
            spans: VecDeque::new(),
            jobs: VecDeque::from([job]),
            active: None,
            pending: VecDeque::new(),
            pending_count: 0,
            pending_bytes: 0,
            registered: false,
            upgraded: false,
            upgrade_sent: false,
            subscribe_id: None,
            subscribe_sent: false,
            ready: false,
            needs_input: true,
            ended: false,
            opened: now.monotonic.nanos(),
            last_frame: now.monotonic.nanos(),
            phase_deadline: add(now.monotonic.nanos(), span)?,
        });
        self.usable = false;
        self.revoked = false;
        self.last_event = None;
        self.phase = Phase::Connecting;
        Ok(())
    }
    fn revoke(&mut self, reason: Error) {
        if !self.revoked {
            self.revoked = true;
            self.gap = Some(EventGap {
                identity: self.config.context.identity,
                kind: self.config.context.kind,
                reason,
            });
        }
        self.usable = false;
    }
    fn fail(&mut self, reason: Error) {
        self.revoke(reason);
        self.stream = None;
        if matches!(reason, Error::Clock | Error::Exhausted | Error::Sink) {
            self.phase = Phase::Stopped;
        } else {
            let delay = self
                .config
                .timing
                .backoff_ns
                .saturating_mul(1u64.checked_shl(self.failures.min(63)).unwrap_or(u64::MAX))
                .min(self.config.timing.max_backoff_ns);
            self.failures = self.failures.saturating_add(1);
            if let Some(next) = self
                .last
                .and_then(|n| n.monotonic.nanos().checked_add(delay))
            {
                self.retry_at = next;
                self.phase = Phase::Backoff;
            } else {
                self.phase = Phase::Stopped;
            }
        }
    }
    pub fn start(&mut self, permit: &mut impl PublicPermit) -> Result<()> {
        if self.phase != Phase::Idle {
            return Err(Error::State);
        }
        let r = self.open(permit);
        if let Err(e) = r {
            self.fail(e);
        }
        r
    }
    fn notify_gap(&mut self, sink: &mut impl EventSink, n: &mut usize) -> Result<()> {
        if *n > 0
            && let Some(g) = self.gap.take()
        {
            *n -= 1;
            if !sink.on_event(EventNotice::Gap(g)) {
                return Err(Error::Sink);
            }
        }
        Ok(())
    }
    fn send(&mut self, s: &mut Stream, permit: &mut impl PublicPermit) -> Result<()> {
        if s.active.is_none() {
            if let Some(j) = s.jobs.pop_front() {
                s.connection
                    .queue(
                        net::PreparedSend::new(
                            &j.bytes,
                            j.instant,
                            self.config.limits.request_bytes,
                        )
                        .map_err(|_| Error::Bounds)?,
                    )
                    .map_err(|_| Error::Transport)?;
                s.active = Some(j);
            } else {
                return Ok(());
            }
        }
        let j = s.active.as_ref().unwrap();
        let charge = j.charge;
        let deadline = j
            .deadline
            .min(add(s.opened, self.config.timing.rotate_ns)?)
            .min(add(s.last_frame, self.config.timing.heartbeat_ns)?);
        let now = self.local()?;
        if now.monotonic.nanos() >= deadline {
            return Err(Error::Timeout);
        }
        let mut fault = None;
        let report = s
            .connection
            .try_send(now.instant, || {
                let result = (|| {
                    let before = self.local()?;
                    if before.monotonic.nanos() >= deadline {
                        return Err(Error::Timeout);
                    }
                    let allowed = permit.allow(&charge, before.monotonic);
                    let after = self.local()?;
                    if after.monotonic.nanos() >= deadline {
                        return Err(Error::Timeout);
                    }
                    if !allowed {
                        return Err(Error::Denied);
                    }
                    Ok(())
                })();
                if let Err(e) = result {
                    fault = Some(e);
                    false
                } else {
                    true
                }
            })
            .map_err(|_| Error::Transport)?;
        if let Some(e) = fault {
            return Err(e);
        }
        if matches!(
            report.state,
            net::SendState::NoBytes(_) | net::SendState::Failed(_)
        ) {
            return Err(Error::Transport);
        }
        if report.accepted_bytes == report.total_bytes && !s.active.as_ref().unwrap().marked {
            s.active.as_mut().unwrap().marked = true;
            match charge.kind {
                ChargeKind::Upgrade => s.upgrade_sent = true,
                ChargeKind::Subscribe => s.subscribe_sent = true,
                _ => {}
            }
        }
        if report.state == net::SendState::Flushed {
            s.connection.take_finished();
            s.active = None;
        }
        Ok(())
    }
    fn push(&self, s: &mut Stream, bytes: &[u8], stamp: u64) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        if let Some(last) = s.spans.back_mut().filter(|(_, t)| *t == stamp) {
            last.0 = last.0.checked_add(bytes.len()).ok_or(Error::Bounds)?;
        } else {
            if s.spans.len() >= self.config.limits.timestamp_segments {
                return Err(Error::Bounds);
            }
            s.spans.push_back((bytes.len(), stamp));
        }
        s.decoder.push(bytes).map_err(|_| Error::Protocol)?;
        s.needs_input = false;
        Ok(())
    }
    fn consumed(s: &mut Stream, mut n: usize) -> Result<Option<u64>> {
        let mut stamp = None;
        while n > 0 {
            let (size, t) = s.spans.front_mut().ok_or(Error::Protocol)?;
            let take = n.min(*size);
            *size -= take;
            n -= take;
            stamp = Some(*t);
            if *size == 0 {
                s.spans.pop_front();
            }
        }
        Ok(stamp)
    }
    fn receive(
        &mut self,
        s: &mut Stream,
        budget: IoBudget,
        permit: &mut impl PublicPermit,
        r: &mut PollReport,
    ) -> Result<()> {
        self.send(s, permit)?;
        let now = self.local()?;
        let ended = match s.connection.drive(now.instant, budget) {
            Ok(d) => {
                r.syscalls += d.syscalls;
                r.network_bytes += d.network_read + d.network_written;
                d.peer_closed
            }
            Err(_) => true,
        };
        let stamp = self.local()?.monotonic.nanos();
        let mut bytes = Vec::new();
        let mut scratch = [0; 16384];
        loop {
            let n = s.connection.read(&mut scratch);
            if n == 0 {
                break;
            }
            if bytes.len() + n > self.config.limits.io.max_receive_bytes {
                return Err(Error::Bounds);
            }
            bytes.extend_from_slice(&scratch[..n]);
        }
        if ended {
            s.ended = true;
            self.revoke(Error::Closed);
            self.phase = Phase::Closing;
        } else {
            self.send(s, permit)?;
        }
        if !s.upgraded {
            s.http.push(&bytes).map_err(|_| Error::Protocol)?;
            if ended {
                s.http.finish();
            }
            if let Some(response) = s.http.next_response().map_err(|_| Error::Protocol)? {
                if !s.upgrade_sent {
                    return Err(Error::Protocol);
                }
                s.handshake
                    .validate(&response)
                    .map_err(|_| Error::Protocol)?;
                s.upgraded = true;
                if self.config.subscribe == SubscribeMode::RawUrl {
                    s.ready = true;
                    if !s.ended {
                        self.phase = Phase::Receiving;
                    }
                } else {
                    let now = self.local()?;
                    let id = u32::try_from(self.operation.ok_or(Error::Exhausted)?)
                        .map_err(|_| Error::Exhausted)?;
                    let raw=serde_json::to_vec(&serde_json::json!({"method":"SUBSCRIBE","params":[self.config.context.stream_name(self.config.encoding,self.config.fast_mark).map_err(|_|Error::Invalid)?],"id":id})).map_err(|_|Error::Protocol)?;
                    let bytes =
                        ws::encode_client(ws::Opcode::Text, &raw, self.config.limits.request_bytes)
                            .map_err(|_| Error::Protocol)?;
                    s.jobs.push_back(self.job(
                        ChargeKind::Subscribe,
                        bytes,
                        now,
                        self.config.timing.request_ns,
                    )?);
                    s.subscribe_id = Some(id);
                    if !s.ended {
                        self.phase = Phase::Subscribing;
                    }
                }
                s.phase_deadline = add(
                    stamp,
                    if s.ready {
                        self.config.timing.first_event_ns
                    } else {
                        self.config.timing.request_ns
                    },
                )?;
                let remaining = s.http.take_buffered();
                self.push(s, &remaining, stamp)?;
            }
        } else {
            self.push(s, &bytes, stamp)?;
        }
        Ok(())
    }
    fn deliver(
        &mut self,
        s: &mut Stream,
        sink: &mut impl EventSink,
        n: &mut usize,
        r: &mut PollReport,
    ) -> Result<()> {
        while s.ready && *n > 0 {
            let Some(batch) = s.pending.front_mut() else {
                break;
            };
            let event = &batch.events[batch.position];
            if !s.ended {
                self.usable = true;
                self.last_event = Some(event.times.received_mono_ns);
                self.failures = 0;
            }
            *n -= 1;
            if !sink.on_event(EventNotice::Event {
                event,
                raw: &batch.raw,
                usable: !s.ended,
            }) {
                return Err(Error::Sink);
            }
            r.applied += 1;
            s.pending_count -= 1;
            batch.position += 1;
            if batch.position == batch.events.len() {
                let batch = s.pending.pop_front().unwrap();
                s.pending_bytes -= batch.raw.len();
            }
        }
        Ok(())
    }
    fn decode(
        &mut self,
        s: &mut Stream,
        budget: PollBudget,
        sink: &mut impl EventSink,
        n: &mut usize,
        r: &mut PollReport,
    ) -> Result<()> {
        self.deliver(s, sink, n, r)?;
        let mut messages = 0;
        for _ in 0..budget.max_decoder_turns {
            if !s.upgraded || s.needs_input || messages >= budget.max_ws_messages || *n == 0 {
                break;
            }
            let before = s.decoder.buffered_len();
            let message = s.decoder.poll_message().map_err(|_| Error::Protocol)?;
            let stamp = Self::consumed(s, before - s.decoder.buffered_len())?;
            let message = match message {
                ws::PollMessage::NeedInput => {
                    s.needs_input = true;
                    break;
                }
                ws::PollMessage::Yielded => {
                    r.runnable = true;
                    continue;
                }
                ws::PollMessage::Message(m) => m,
            };
            messages += 1;
            r.decoded += 1;
            let stamp = stamp.ok_or(Error::Protocol)?;
            s.last_frame = stamp;
            match message.opcode {
                ws::Opcode::Ping => {
                    if !s.ended {
                        if s.jobs.len() + usize::from(s.active.is_some())
                            >= self.config.limits.control_frames
                        {
                            return Err(Error::Bounds);
                        }
                        let now = self.local()?;
                        let bytes = ws::encode_client(
                            ws::Opcode::Pong,
                            &message.payload,
                            self.config.limits.request_bytes,
                        )
                        .map_err(|_| Error::Protocol)?;
                        let mut job =
                            self.job(ChargeKind::Pong, bytes, now, self.config.timing.pong_ns)?;
                        job.deadline = add(stamp, self.config.timing.pong_ns)?;
                        s.jobs.push_back(job);
                    }
                }
                ws::Opcode::Pong => {}
                ws::Opcode::Close => {
                    s.ended = true;
                    self.revoke(Error::Closed);
                    self.phase = Phase::Closing;
                    self.notify_gap(sink, n)?;
                }
                ws::Opcode::Text | ws::Opcode::Binary => {
                    let raw = message.payload;
                    if message.opcode == ws::Opcode::Text {
                        #[derive(serde::Deserialize)]
                        struct Control<'a> {
                            id: Option<u32>,
                            e: Option<&'a str>,
                        }
                        let ctrl: Control<'_> =
                            serde_json::from_slice(&raw).map_err(|_| Error::Protocol)?;
                        if ctrl.e == Some("serverShutdown") {
                            s.ended = true;
                            self.revoke(Error::Closed);
                            self.notify_gap(sink, n)?;
                            continue;
                        }
                        if let Some(id) = ctrl.id {
                            #[derive(serde::Deserialize)]
                            #[serde(deny_unknown_fields)]
                            struct Ack<'a> {
                                id: u32,
                                #[serde(borrow)]
                                result: &'a serde_json::value::RawValue,
                            }
                            let ack: Ack<'_> =
                                serde_json::from_slice(&raw).map_err(|_| Error::Protocol)?;
                            if ack.id != id
                                || Some(id) != s.subscribe_id
                                || !s.subscribe_sent
                                || ack.result.get() != "null"
                                || s.ready
                            {
                                return Err(Error::Protocol);
                            }
                            s.ready = true;
                            s.phase_deadline = add(stamp, self.config.timing.first_event_ns)?;
                            if !s.ended {
                                self.phase = Phase::Receiving;
                            }
                            self.deliver(s, sink, n, r)?;
                            continue;
                        }
                    }
                    let events = match (self.config.encoding, message.opcode) {
                        (EventEncoding::Json, ws::Opcode::Text) => vec![
                            tickwire_binance::market::decode_json(
                                &raw,
                                &self.config.context,
                                stamp,
                                self.config.limits.message_bytes,
                            )
                            .map_err(|_| Error::Protocol)?,
                        ],
                        (EventEncoding::SpotSbeV1, ws::Opcode::Binary) => {
                            tickwire_binance::market::decode_sbe(
                                &raw,
                                &self.config.context,
                                stamp,
                                self.config.limits.message_bytes,
                                self.config.max_events,
                            )
                            .map_err(|_| Error::Protocol)?
                        }
                        _ => return Err(Error::Protocol),
                    };
                    if events.is_empty() {
                        continue;
                    }
                    if s.pending_count + events.len() > self.config.max_events
                        || s.pending_bytes + raw.len() > self.config.limits.message_bytes
                    {
                        return Err(Error::Bounds);
                    }
                    s.pending_count += events.len();
                    s.pending_bytes += raw.len();
                    s.pending.push_back(Batch {
                        raw,
                        events,
                        position: 0,
                    });
                    self.deliver(s, sink, n, r)?;
                }
            }
        }
        Ok(())
    }
    pub fn poll(
        &mut self,
        budget: PollBudget,
        permit: &mut impl PublicPermit,
        sink: &mut impl EventSink,
    ) -> Result<PollReport> {
        if budget.io.max_syscalls < 2
            || budget.io.max_bytes == 0
            || budget.max_ws_messages == 0
            || budget.max_decoder_turns == 0
            || budget.max_notifications == 0
        {
            return Err(Error::Bounds);
        }
        let mut r = PollReport::default();
        let mut notices = budget.max_notifications;
        let result = (|| {
            let now = self.local()?;
            self.notify_gap(sink, &mut notices)?;
            if self.gap.is_some() || self.phase == Phase::Stopped {
                return Ok(());
            }
            if self.phase == Phase::Backoff && now.monotonic.nanos() >= self.retry_at {
                self.open(permit)?;
            }
            let Some(mut s) = self.stream.take() else {
                return Ok(());
            };
            let processing = (|| {
                let n = now.monotonic.nanos();
                if !s.ended
                    && (n >= add(s.opened, self.config.timing.rotate_ns)?
                        || n >= add(s.last_frame, self.config.timing.heartbeat_ns)?
                        || (!s.ready || self.last_event.is_none()) && n >= s.phase_deadline
                        || self.config.context.kind == EventKind::MarkFunding
                            && self.last_event.is_some_and(|t| {
                                n.saturating_sub(t)
                                    >= if self.config.fast_mark {
                                        3_000_000_000
                                    } else {
                                        9_000_000_000
                                    }
                            }))
                {
                    s.ended = true;
                    self.revoke(Error::Timeout);
                    self.phase = Phase::Closing;
                }
                if !s.ended && s.needs_input && (s.pending.is_empty() || !s.ready) {
                    self.receive(&mut s, budget.io, permit, &mut r)?;
                }
                self.notify_gap(sink, &mut notices)?;
                if self.gap.is_none() {
                    self.decode(&mut s, budget, sink, &mut notices, &mut r)?;
                }
                Ok(())
            })();
            if processing.is_ok() && s.ended && s.needs_input && (!s.ready || s.pending.is_empty())
            {
                self.fail(Error::Closed);
            } else {
                r.runnable |= !s.needs_input || !s.pending.is_empty();
                r.needs_registration = !s.registered;
                self.stream = Some(s);
            }
            processing
        })();
        if let Err(e) = result {
            self.fail(e);
            self.notify_gap(sink, &mut notices)?;
            if matches!(e, Error::Clock | Error::Exhausted | Error::Sink) {
                return Err(e);
            }
        }
        r.runnable |= self.gap.is_some();
        r.next_deadline_ns = match self.phase {
            Phase::Backoff => Some(self.retry_at),
            Phase::Stopped | Phase::Idle => None,
            _ => self.stream.as_ref().map(|s| {
                s.opened
                    .saturating_add(self.config.timing.rotate_ns)
                    .min(s.last_frame.saturating_add(self.config.timing.heartbeat_ns))
                    .min(if self.last_event.is_none() {
                        s.phase_deadline
                    } else {
                        u64::MAX
                    })
                    .min(if self.config.context.kind == EventKind::MarkFunding {
                        self.last_event.map_or(u64::MAX, |t| {
                            t.saturating_add(if self.config.fast_mark {
                                3_000_000_000
                            } else {
                                9_000_000_000
                            })
                        })
                    } else {
                        u64::MAX
                    })
            }),
        };
        Ok(r)
    }
    pub fn status(&self) -> EventStatus {
        EventStatus {
            phase: self.phase,
            connection_epoch: self.epoch,
            kind: self.config.context.kind,
            usable: self.usable,
            last_event_mono_ns: self.last_event,
        }
    }
    pub fn register(&mut self, registry: &mio::Registry, token: mio::Token) -> Result<()> {
        if let Some(s) = &mut self.stream {
            s.connection
                .register(registry, token)
                .map_err(|_| Error::Transport)?;
            s.registered = true;
        }
        Ok(())
    }
    pub fn reregister(&mut self, registry: &mio::Registry, token: mio::Token) -> Result<()> {
        if let Some(s) = &mut self.stream {
            s.connection
                .reregister(registry, token)
                .map_err(|_| Error::Transport)?;
            s.registered = true;
        }
        Ok(())
    }
    pub fn stop(&mut self, sink: &mut impl EventSink) -> Result<()> {
        if self.phase != Phase::Stopped {
            self.revoke(Error::Closed);
            self.stream = None;
            self.phase = Phase::Stopped;
        }
        self.notify_gap(sink, &mut 1)
    }
}
