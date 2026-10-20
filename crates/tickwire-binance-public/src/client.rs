use crate::*;
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};
use tickwire_binance::depth_sync::{ConnectionToken, DepthSync, SnapshotRequest};
use tickwire_transport::{self as net, http, ws};

struct Job {
    bytes: Vec<u8>,
    charge: PublicCharge,
    deadline: u64,
    instant: Instant,
    marked: bool,
}
struct NetworkLane {
    connection: net::Connection,
    http: http::ResponseDecoder,
    active: Option<Job>,
    jobs: VecDeque<Job>,
    registered: bool,
    completed: Option<ChargeKind>,
    hard_deadline: u64,
}
struct StreamLane {
    net: NetworkLane,
    handshake: ws::ClientHandshake,
    decoder: ws::Decoder,
    spans: VecDeque<(usize, u64)>,
    upgraded: bool,
    upgrade_sent: bool,
    subscribe_id: Option<u32>,
    subscribe_sent: bool,
    needs_input: bool,
    first_event: bool,
    opened: u64,
    last_frame: u64,
    phase_deadline: u64,
    closing: bool,
}
struct SnapshotLane {
    net: NetworkLane,
    request: SnapshotRequest,
    charge: PublicCharge,
    sent: bool,
    result: Option<std::result::Result<http::Response, Error>>,
    feedback_sent: bool,
}
struct Batch {
    bytes: Vec<u8>,
    stamp: u64,
    ended: bool,
}
struct Budget {
    io: IoBudget,
    messages: usize,
    turns: usize,
    notices: usize,
}
pub struct PublicDepthClient<P, C, const L: usize, const U: usize, const B: usize> {
    config: Config,
    network: NetworkConfig,
    sync: DepthSync<P, L, U, B>,
    clock: C,
    last: Option<LocalTime>,
    phase: Phase,
    epoch: u64,
    next_epoch: Option<u64>,
    next_operation: Option<u64>,
    token: Option<ConnectionToken>,
    stream: Option<StreamLane>,
    snapshot: Option<SnapshotLane>,
    gap: Option<GapNotice>,
    retry_at: u64,
    snapshot_retry_at: u64,
    cooldown: Option<u64>,
    failures: u32,
    http_first: bool,
}
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).ok_or(Error::Exhausted)
}
fn instant(now: LocalTime, ns: u64) -> Result<Instant> {
    now.instant
        .checked_add(Duration::from_nanos(ns))
        .ok_or(Error::Exhausted)
}
impl<P: DepthPolicy, C: MonotonicClock, const L: usize, const U: usize, const B: usize>
    PublicDepthClient<P, C, L, U, B>
{
    pub fn new(config: Config, network: NetworkConfig, clock: C) -> Result<Self> {
        protocol::validate::<P, L, U, B>(&config, &network)?;
        let sync = DepthSync::new(config.depth.clone()).map_err(Error::Sync)?;
        Ok(Self {
            next_epoch: Some(config.first_connection_epoch),
            next_operation: Some(config.first_operation),
            config,
            network,
            sync,
            clock,
            last: None,
            phase: Phase::Idle,
            epoch: 0,
            token: None,
            stream: None,
            snapshot: None,
            gap: None,
            retry_at: 0,
            snapshot_retry_at: 0,
            cooldown: Some(0),
            failures: 0,
            http_first: false,
        })
    }
    fn local(&mut self) -> Result<LocalTime> {
        let now = self.clock.sample().map_err(|_| Error::Clock)?;
        if now.monotonic.domain() != self.config.domain
            || self.last.is_some_and(|p| {
                now.instant < p.instant || now.monotonic.nanos() < p.monotonic.nanos()
            })
        {
            return Err(Error::Clock);
        }
        self.last = Some(now);
        Ok(now)
    }
    fn operation(&mut self) -> Result<u64> {
        let n = self.next_operation.ok_or(Error::Exhausted)?;
        self.next_operation = n.checked_add(1);
        Ok(n)
    }
    fn charge(&self, lane: Lane, operation: u64, kind: ChargeKind) -> PublicCharge {
        PublicCharge {
            owner_id: self.config.owner_id,
            domain: self.config.domain,
            source: self.config.source,
            lane,
            connection_epoch: self.epoch,
            operation,
            kind,
            api_weight: if kind == ChargeKind::Snapshot {
                protocol::weight(&self.config)
            } else {
                0
            },
        }
    }
    fn invalidate(&mut self, reason: Error, permanent: bool, now: u64) {
        let now = now.max(self.last.map_or(now, |time| time.monotonic.nanos()));
        if let Some(token) = self.token.take() {
            let _ = self.sync.disconnect(token);
        }
        self.stream = None;
        self.snapshot = None;
        if !matches!(self.phase, Phase::Stopped | Phase::Closing) {
            self.gap = Some(GapNotice {
                connection_epoch: self.epoch,
                book_generation: self.sync.generation(),
                reason,
            });
        }
        if permanent {
            self.phase = Phase::Stopped;
            return;
        }
        let factor = 1u64.checked_shl(self.failures.min(63)).unwrap_or(u64::MAX);
        let delay = self
            .config
            .timing
            .backoff_ns
            .saturating_mul(factor)
            .min(self.config.timing.max_backoff_ns);
        self.failures = self.failures.saturating_add(1);
        if let Some(t) = now.checked_add(delay) {
            self.retry_at = t;
            self.phase = Phase::Backoff;
        } else {
            self.phase = Phase::Stopped;
            if let Some(gap) = &mut self.gap {
                gap.reason = Error::Exhausted;
            }
        }
    }
    fn fatal(&mut self, e: Error) -> Error {
        self.invalidate(e, true, self.last.map_or(0, |n| n.monotonic.nanos()));
        e
    }
    fn notify_gap(&mut self, sink: &mut impl PublicSink, b: &mut Budget) -> Result<()> {
        if b.notices > 0
            && let Some(g) = self.gap.take()
        {
            b.notices -= 1;
            if !sink.on_notice(Notice::Gap(g)) {
                return Err(self.fatal(Error::Sink));
            }
        }
        Ok(())
    }
    fn open_net(
        &self,
        address: std::net::SocketAddr,
        host: &str,
        now: LocalTime,
        timeout: u64,
    ) -> Result<NetworkLane> {
        let connection = net::Connection::connect(
            address,
            net::Security::Tls {
                server_name: host.to_owned().try_into().map_err(|_| Error::Invalid)?,
                config: self.network.tls.clone(),
            },
            self.config.limits.io,
            instant(now, timeout)?,
        )
        .map_err(|_| Error::Transport)?;
        Ok(NetworkLane {
            connection,
            http: http::ResponseDecoder::new(
                self.config.limits.http_bytes,
                self.config.limits.http_headers,
            )
            .map_err(|_| Error::Bounds)?,
            active: None,
            jobs: VecDeque::new(),
            registered: false,
            completed: None,
            hard_deadline: u64::MAX,
        })
    }
    fn job(
        &mut self,
        lane: Lane,
        kind: ChargeKind,
        bytes: Vec<u8>,
        now: LocalTime,
        span: u64,
    ) -> Result<Job> {
        let op = self.operation()?;
        Ok(Job {
            bytes,
            charge: self.charge(lane, op, kind),
            deadline: add(now.monotonic.nanos(), span)?,
            instant: instant(now, span)?,
            marked: false,
        })
    }
    fn open_stream(&mut self, permit: &mut impl PublicPermit, now: LocalTime) -> Result<()> {
        self.epoch = self.next_epoch.ok_or(Error::Exhausted)?;
        self.next_epoch = self.epoch.checked_add(1);
        let op = self.operation()?;
        let charge = self.charge(Lane::Stream, op, ChargeKind::Connect);
        let allowed = permit.allow(&charge, now.monotonic);
        let after = self.local()?;
        if !allowed {
            return Err(Error::Denied);
        }
        let (host, _, _) = protocol::endpoints(self.config.source);
        let mut net = self.open_net(
            self.network.stream_address,
            host,
            after,
            self.config.timing.connect_ns,
        )?;
        let handshake = ws::ClientHandshake::new(
            host,
            &protocol::path(&self.config),
            &[],
            self.config.limits.request_bytes,
        )
        .map_err(|_| Error::Protocol)?;
        let span = add(self.config.timing.connect_ns, self.config.timing.request_ns)?;
        net.jobs.push_back(self.job(
            Lane::Stream,
            ChargeKind::Upgrade,
            handshake.request().to_vec(),
            after,
            span,
        )?);
        let token = self.sync.connect(self.epoch).map_err(Error::Sync)?;
        self.token = Some(token);
        self.phase = Phase::Connecting;
        self.snapshot_retry_at = 0;
        self.stream = Some(StreamLane {
            net,
            handshake,
            decoder: ws::Decoder::new(
                self.config.limits.message_bytes,
                self.config.limits.message_bytes,
            )
            .map_err(|_| Error::Bounds)?,
            spans: VecDeque::new(),
            upgraded: false,
            upgrade_sent: false,
            subscribe_id: None,
            subscribe_sent: false,
            needs_input: true,
            first_event: false,
            opened: after.monotonic.nanos(),
            last_frame: after.monotonic.nanos(),
            phase_deadline: add(after.monotonic.nanos(), span)?,
            closing: false,
        });
        Ok(())
    }
    pub fn start(&mut self, permit: &mut impl PublicPermit) -> Result<()> {
        if self.phase != Phase::Idle {
            return Err(Error::State);
        }
        let now = self.local().map_err(|e| self.fatal(e))?;
        if let Err(e) = self.open_stream(permit, now) {
            self.invalidate(
                e,
                matches!(e, Error::Clock | Error::Exhausted | Error::Sync(_)),
                now.monotonic.nanos(),
            );
            return Err(e);
        }
        Ok(())
    }
    fn send_job(&mut self, lane: &mut NetworkLane, permit: &mut impl PublicPermit) -> Result<()> {
        if lane.active.is_none() {
            if let Some(job) = lane.jobs.pop_front() {
                let prepared = net::PreparedSend::new(
                    &job.bytes,
                    job.instant,
                    self.config.limits.request_bytes,
                )
                .map_err(|_| Error::Bounds)?;
                lane.connection
                    .queue(prepared)
                    .map_err(|_| Error::Transport)?;
                lane.active = Some(job);
            } else {
                return Ok(());
            }
        }
        let job = lane.active.as_ref().expect("active job");
        let (charge, deadline) = (job.charge, job.deadline.min(lane.hard_deadline));
        let now = self.local()?;
        if now.monotonic.nanos() >= deadline {
            return Err(Error::Timeout);
        }
        let mut gate_error = None;
        let sent = lane
            .connection
            .try_send(now.instant, || {
                let checked = (|| {
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
                if let Err(e) = checked {
                    gate_error = Some(e);
                    false
                } else {
                    true
                }
            })
            .map_err(|_| Error::Transport)?;
        if let Some(e) = gate_error {
            return Err(e);
        }
        if matches!(
            sent.state,
            net::SendState::NoBytes(_) | net::SendState::Failed(_)
        ) {
            return Err(Error::Transport);
        }
        let job = lane.active.as_mut().expect("active job");
        if sent.accepted_bytes == sent.total_bytes && !job.marked {
            lane.completed = Some(job.charge.kind);
            job.marked = true;
        }
        if sent.state == net::SendState::Flushed {
            lane.connection.take_finished();
            lane.active = None;
        }
        Ok(())
    }
    fn pump(
        &mut self,
        lane: &mut NetworkLane,
        permit: &mut impl PublicPermit,
        b: &mut Budget,
        r: &mut PollReport,
    ) -> Result<Batch> {
        self.send_job(lane, permit)?;
        let now = self.local()?;
        let mut ended = false;
        if b.io.max_syscalls >= 2 && b.io.max_bytes > 0 {
            match lane.connection.drive(now.instant, b.io) {
                Ok(d) => {
                    b.io.max_syscalls = b.io.max_syscalls.saturating_sub(d.syscalls);
                    b.io.max_bytes =
                        b.io.max_bytes
                            .saturating_sub(d.network_read + d.network_written);
                    r.syscalls += d.syscalls;
                    r.network_bytes += d.network_read + d.network_written;
                    ended = d.peer_closed;
                }
                Err(_) => {
                    r.io_budget_exhausted = true;
                    b.io.max_syscalls = 0;
                    b.io.max_bytes = 0;
                    ended = true;
                }
            }
        } else {
            r.runnable = true;
        }
        let mut bytes = Vec::new();
        let mut scratch = [0; 16384];
        loop {
            let n = lane.connection.read(&mut scratch);
            if n == 0 {
                break;
            }
            if bytes
                .len()
                .checked_add(n)
                .is_none_or(|n| n > self.config.limits.io.max_receive_bytes)
            {
                return Err(Error::Bounds);
            }
            bytes.extend_from_slice(&scratch[..n]);
        }
        if !ended {
            self.send_job(lane, permit)?;
        }
        Ok(Batch {
            bytes,
            stamp: now.monotonic.nanos(),
            ended,
        })
    }
    fn complete_stream(lane: &mut StreamLane) {
        match lane.net.completed.take() {
            Some(ChargeKind::Upgrade) => lane.upgrade_sent = true,
            Some(ChargeKind::Subscribe) => lane.subscribe_sent = true,
            _ => {}
        }
    }
    fn push_ws(&self, lane: &mut StreamLane, bytes: &[u8], stamp: u64) -> Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        if let Some(last) = lane.spans.back_mut().filter(|(_, t)| *t == stamp) {
            last.0 = last.0.checked_add(bytes.len()).ok_or(Error::Bounds)?;
        } else {
            if lane.spans.len() == self.config.limits.timestamp_segments {
                return Err(Error::Bounds);
            }
            lane.spans.push_back((bytes.len(), stamp));
        }
        lane.decoder.push(bytes).map_err(|_| Error::Protocol)?;
        lane.needs_input = false;
        Ok(())
    }
    fn consumed(lane: &mut StreamLane, mut n: usize) -> Result<Option<u64>> {
        let mut stamp = None;
        while n > 0 {
            let (size, time) = lane.spans.front_mut().ok_or(Error::Protocol)?;
            let take = n.min(*size);
            *size -= take;
            n -= take;
            stamp = Some(*time);
            if *size == 0 {
                lane.spans.pop_front();
            }
        }
        Ok(stamp)
    }
    fn ready(&mut self, lane: &mut StreamLane, now: u64) -> Result<()> {
        self.sync
            .subscription_ready(self.token.ok_or(Error::State)?)
            .map_err(Error::Sync)?;
        self.phase = Phase::Receiving;
        lane.phase_deadline = add(now, self.config.timing.first_event_ns)?;
        Ok(())
    }
    fn notice_sync(
        &mut self,
        outcome: SyncOutcome,
        event: Option<(u64, &[u8])>,
        now: u64,
        sink: &mut impl PublicSink,
        b: &mut Budget,
        r: &mut PollReport,
    ) -> Result<()> {
        if b.notices == 0 {
            return Err(Error::Bounds);
        }
        b.notices -= 1;
        if matches!(
            outcome,
            SyncOutcome::Applied(tickwire_book::ApplyOutcome::Applied { .. })
                | SyncOutcome::SnapshotInstalled { live: true, .. }
        ) {
            r.applied += 1;
        }
        if matches!(
            outcome,
            SyncOutcome::SnapshotInstalled { live: true, .. }
                | SyncOutcome::Applied(tickwire_book::ApplyOutcome::Applied {
                    became_live: true,
                    ..
                })
        ) {
            self.failures = 0;
        }
        if matches!(
            outcome,
            SyncOutcome::IgnoredObsolete
                | SyncOutcome::Applied(tickwire_book::ApplyOutcome::IgnoredStale)
        ) {
            r.ignored += 1;
        }
        let view = self.sync.view(now, u64::MAX).ok();
        if !sink.on_notice(Notice::Sync {
            connection_epoch: self.epoch,
            book_generation: self.sync.generation(),
            received_mono_ns: event.map(|(stamp, _)| stamp),
            raw_delta: event.map(|(_, bytes)| bytes),
            outcome,
            view,
        }) {
            return Err(Error::Sink);
        }
        Ok(())
    }
    fn decode_stream(
        &mut self,
        lane: &mut StreamLane,
        sink: &mut impl PublicSink,
        b: &mut Budget,
        r: &mut PollReport,
    ) -> Result<()> {
        while lane.upgraded && !lane.needs_input && !lane.closing {
            if b.messages == 0 || b.turns == 0 || b.notices == 0 {
                r.runnable = true;
                break;
            }
            b.turns -= 1;
            let before = lane.decoder.buffered_len();
            let message = lane.decoder.poll_message().map_err(|_| Error::Protocol)?;
            let stamp = Self::consumed(lane, before - lane.decoder.buffered_len())?;
            match message {
                ws::PollMessage::NeedInput => {
                    lane.needs_input = true;
                    break;
                }
                ws::PollMessage::Yielded => {
                    r.runnable = true;
                    continue;
                }
                ws::PollMessage::Message(message) => {
                    b.messages -= 1;
                    r.decoded += 1;
                    let stamp = stamp.ok_or(Error::Protocol)?;
                    lane.last_frame = stamp;
                    match message.opcode {
                        ws::Opcode::Ping => {
                            if lane.net.jobs.len() + usize::from(lane.net.active.is_some())
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
                            let mut job = self.job(
                                Lane::Stream,
                                ChargeKind::Pong,
                                bytes,
                                now,
                                self.config.timing.pong_ns,
                            )?;
                            job.deadline = add(stamp, self.config.timing.pong_ns)?;
                            if now.monotonic.nanos() >= job.deadline {
                                return Err(Error::Timeout);
                            }
                            if lane
                                .net
                                .active
                                .as_ref()
                                .is_some_and(|j| j.charge.kind == ChargeKind::Subscribe)
                                && lane
                                    .net
                                    .connection
                                    .send_report()
                                    .is_some_and(|s| s.accepted_bytes == 0)
                            {
                                lane.net.connection.cancel().map_err(|_| Error::Transport)?;
                                lane.net.connection.take_finished();
                                lane.net
                                    .jobs
                                    .push_back(lane.net.active.take().expect("queued subscribe"));
                            }
                            let position = lane
                                .net
                                .jobs
                                .iter()
                                .position(|j| j.charge.kind == ChargeKind::Subscribe)
                                .unwrap_or(lane.net.jobs.len());
                            lane.net.jobs.insert(position, job);
                        }
                        ws::Opcode::Pong => {}
                        ws::Opcode::Close => {
                            self.close_frame(lane, &message.payload)?;
                        }
                        ws::Opcode::Binary => return Err(Error::Protocol),
                        ws::Opcode::Text => match protocol::incoming(
                            &message.payload,
                            &protocol::stream(&self.config),
                        )? {
                            protocol::Incoming::Shutdown => return Err(Error::Closed),
                            protocol::Incoming::Ack(id) => {
                                if lane.subscribe_id != Some(id) || !lane.subscribe_sent {
                                    return Err(Error::Protocol);
                                }
                                if self.phase == Phase::Subscribing {
                                    self.ready(lane, stamp)?;
                                } else if self.phase != Phase::Receiving {
                                    return Err(Error::Protocol);
                                }
                            }
                            protocol::Incoming::Depth(bytes) => {
                                let outcome = self
                                    .sync
                                    .on_delta(self.token.ok_or(Error::State)?, bytes, stamp)
                                    .map_err(Error::Sync)?;
                                lane.first_event = true;
                                if matches!(outcome, SyncOutcome::Rebuild(_)) {
                                    self.snapshot = None;
                                    lane.first_event = false;
                                    lane.phase_deadline = add(
                                        self.local()?.monotonic.nanos(),
                                        self.config.timing.first_event_ns,
                                    )?;
                                }
                                self.notice_sync(outcome, Some((stamp, bytes)), stamp, sink, b, r)?;
                            }
                        },
                    }
                }
            }
        }
        Ok(())
    }
    fn close_frame(&mut self, lane: &mut StreamLane, payload: &[u8]) -> Result<()> {
        if let Some(token) = self.token.take() {
            self.sync.disconnect(token).map_err(Error::Sync)?;
        }
        self.snapshot = None;
        self.gap = Some(GapNotice {
            connection_epoch: self.epoch,
            book_generation: self.sync.generation(),
            reason: Error::Closed,
        });
        // Finish an already accepted frame; discard only jobs with zero output.
        if lane
            .net
            .connection
            .send_report()
            .is_some_and(|r| r.accepted_bytes == 0)
        {
            lane.net.connection.cancel().map_err(|_| Error::Transport)?;
            lane.net.connection.take_finished();
            lane.net.active = None;
        }
        lane.net.jobs.clear();
        let now = self.local()?;
        let bytes = ws::encode_client(ws::Opcode::Close, payload, self.config.limits.request_bytes)
            .map_err(|_| Error::Protocol)?;
        let job = self.job(
            Lane::Stream,
            ChargeKind::Close,
            bytes,
            now,
            self.config.timing.pong_ns,
        )?;
        lane.phase_deadline = job.deadline;
        lane.net.jobs.push_back(job);
        lane.closing = true;
        lane.needs_input = true;
        self.phase = Phase::Closing;
        Ok(())
    }
    fn poll_stream(
        &mut self,
        lane: &mut StreamLane,
        permit: &mut impl PublicPermit,
        sink: &mut impl PublicSink,
        b: &mut Budget,
        r: &mut PollReport,
    ) -> Result<()> {
        lane.net.hard_deadline = add(lane.opened, self.config.timing.rotate_ns)?
            .min(add(lane.last_frame, self.config.timing.heartbeat_ns)?);
        if self.phase != Phase::Receiving || !lane.first_event {
            lane.net.hard_deadline = lane.net.hard_deadline.min(lane.phase_deadline);
        }
        if !lane.needs_input {
            return self.decode_stream(lane, sink, b, r);
        }
        let batch = self.pump(&mut lane.net, permit, b, r)?;
        Self::complete_stream(lane);
        if batch.ended {
            return Err(Error::Transport);
        }
        if lane.closing {
            return if lane.net.active.is_none() && lane.net.jobs.is_empty() {
                Err(Error::Closed)
            } else {
                Ok(())
            };
        }
        if !lane.upgraded {
            if lane.net.connection.is_ready() {
                self.phase = Phase::Upgrading;
            }
            lane.net
                .http
                .push(&batch.bytes)
                .map_err(|_| Error::Protocol)?;
            if let Some(response) = lane.net.http.next_response().map_err(|_| Error::Protocol)? {
                if !lane.upgrade_sent {
                    return Err(Error::Protocol);
                }
                lane.handshake
                    .validate(&response)
                    .map_err(|_| Error::Protocol)?;
                lane.upgraded = true;
                if self.config.subscribe == SubscribeMode::RawUrl {
                    self.ready(lane, batch.stamp)?;
                } else {
                    let now = self.local()?;
                    let id = u32::try_from(self.next_operation.ok_or(Error::Exhausted)?)
                        .map_err(|_| Error::Exhausted)?;
                    let json=serde_json::to_vec(&serde_json::json!({"method":"SUBSCRIBE","params":[protocol::stream(&self.config)],"id":id})).map_err(|_|Error::Protocol)?;
                    let bytes = ws::encode_client(
                        ws::Opcode::Text,
                        &json,
                        self.config.limits.request_bytes,
                    )
                    .map_err(|_| Error::Protocol)?;
                    lane.net.jobs.push_back(self.job(
                        Lane::Stream,
                        ChargeKind::Subscribe,
                        bytes,
                        now,
                        self.config.timing.request_ns,
                    )?);
                    lane.subscribe_id = Some(id);
                    self.phase = Phase::Subscribing;
                    lane.phase_deadline =
                        add(now.monotonic.nanos(), self.config.timing.request_ns)?;
                }
                let remaining = lane.net.http.take_buffered();
                self.push_ws(lane, &remaining, batch.stamp)?;
            }
        } else {
            self.push_ws(lane, &batch.bytes, batch.stamp)?;
        }
        self.decode_stream(lane, sink, b, r)
    }
    fn begin_snapshot(&mut self, permit: &mut impl PublicPermit, now: LocalTime) -> Result<()> {
        let deadline = add(now.monotonic.nanos(), self.config.timing.request_ns)?;
        let request = self
            .sync
            .request_snapshot(now.monotonic.nanos(), deadline)
            .map_err(Error::Sync)?;
        let op = self.operation()?;
        let connect = self.charge(Lane::Snapshot, op, ChargeKind::Connect);
        let allowed = permit.allow(&connect, now.monotonic);
        let after = self.local()?;
        if !allowed || after.monotonic.nanos() >= deadline {
            self.sync
                .snapshot_failed(request.token(), after.monotonic.nanos())
                .map_err(Error::Sync)?;
            self.snapshot_retry_at = add(after.monotonic.nanos(), self.config.timing.backoff_ns)?;
            return Ok(());
        }
        let (_, host, _) = protocol::endpoints(self.config.source);
        let opened = self.open_net(
            self.network.snapshot_address,
            host,
            after,
            self.config
                .timing
                .connect_ns
                .min(deadline - after.monotonic.nanos()),
        );
        let mut lane = match opened {
            Ok(lane) => lane,
            Err(Error::Transport) => {
                self.sync
                    .snapshot_failed(request.token(), after.monotonic.nanos())
                    .map_err(Error::Sync)?;
                self.snapshot_retry_at =
                    add(after.monotonic.nanos(), self.config.timing.backoff_ns)?;
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        lane.hard_deadline = deadline;
        let bytes = http::encode_request(
            "GET",
            request.path(),
            &[("Host", host), ("Connection", "close")],
            &[],
            self.config.limits.request_bytes,
        )
        .map_err(|_| Error::Protocol)?;
        let job = self.job(
            Lane::Snapshot,
            ChargeKind::Snapshot,
            bytes,
            after,
            deadline - after.monotonic.nanos(),
        )?;
        let charge = job.charge;
        lane.jobs.push_back(job);
        self.snapshot = Some(SnapshotLane {
            net: lane,
            request,
            charge,
            sent: false,
            result: None,
            feedback_sent: false,
        });
        Ok(())
    }
    fn poll_snapshot(
        &mut self,
        lane: &mut SnapshotLane,
        permit: &mut impl PublicPermit,
        b: &mut Budget,
        r: &mut PollReport,
    ) {
        if lane.result.is_some() {
            return;
        }
        let result = (|| {
            let batch = self.pump(&mut lane.net, permit, b, r)?;
            if lane.net.completed.take() == Some(ChargeKind::Snapshot) {
                lane.sent = true;
            }
            lane.net
                .http
                .push(&batch.bytes)
                .map_err(|_| Error::Protocol)?;
            if batch.ended {
                lane.net.http.finish();
            }
            if let Some(response) = lane.net.http.next_response().map_err(|_| Error::Protocol)? {
                if !lane.sent || !lane.net.http.take_buffered().is_empty() {
                    return Err(Error::Protocol);
                }
                return Ok(Some(response));
            }
            if batch.ended {
                return Err(Error::Transport);
            }
            Ok(None)
        })();
        match result {
            Ok(Some(response)) => lane.result = Some(Ok(response)),
            Ok(None) => {}
            Err(e) => lane.result = Some(Err(e)),
        }
    }
    fn barrier(
        &mut self,
        permit: &mut impl PublicPermit,
        sink: &mut impl PublicSink,
        b: &mut Budget,
        r: &mut PollReport,
    ) -> Result<()> {
        if !self.stream.as_ref().is_some_and(|s| s.needs_input) || b.notices == 0 {
            return Ok(());
        }
        let now = self.local()?;
        if let Some(mut lane) = self.snapshot.take() {
            if let Some(Ok(response)) = &lane.result
                && !lane.feedback_sent
            {
                let feedback = HttpFeedback {
                    charge: lane.charge,
                    status: response.status,
                    headers: response.headers.clone(),
                };
                permit.observe_http(&feedback);
                if matches!(response.status, 429 | 418) {
                    self.cooldown = response
                        .header("retry-after")
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .and_then(|s| s.parse::<u64>().ok())
                        .filter(|&s| s > 0)
                        .and_then(|s| s.checked_mul(1_000_000_000))
                        .and_then(|ns| now.monotonic.nanos().checked_add(ns));
                }
                b.notices -= 1;
                lane.feedback_sent = true;
                if !sink.on_notice(Notice::Http(&feedback)) {
                    return Err(Error::Sink);
                }
            }
            if b.notices == 0 {
                self.snapshot = Some(lane);
                r.runnable = true;
                return Ok(());
            }
            // Feedback callbacks may advance time; use a new sample before the coordinator call.
            let now = self.local()?;
            let outcome = if let Some(result) = lane.result.take() {
                match result {
                    Ok(v) => Some(
                        self.sync
                            .on_snapshot(
                                lane.request.token(),
                                v.status,
                                &v.body,
                                now.monotonic.nanos(),
                            )
                            .map_err(Error::Sync)?,
                    ),
                    Err(e @ (Error::Clock | Error::Exhausted)) => return Err(e),
                    Err(_) => Some(
                        self.sync
                            .snapshot_failed(lane.request.token(), now.monotonic.nanos())
                            .map_err(Error::Sync)?,
                    ),
                }
            } else if now.monotonic.nanos() >= lane.request.deadline_ns() {
                self.sync
                    .expire_snapshot(now.monotonic.nanos())
                    .map_err(Error::Sync)?
            } else {
                None
            };
            if let Some(outcome) = outcome {
                if matches!(outcome, SyncOutcome::RetrySnapshot(_)) {
                    self.snapshot_retry_at =
                        add(now.monotonic.nanos(), self.config.timing.backoff_ns)?;
                }
                if matches!(outcome, SyncOutcome::Rebuild(_))
                    && let Some(s) = self.stream.as_mut()
                {
                    s.first_event = false;
                    s.phase_deadline =
                        add(now.monotonic.nanos(), self.config.timing.first_event_ns)?;
                }
                self.notice_sync(outcome, None, now.monotonic.nanos(), sink, b, r)?;
            } else {
                self.snapshot = Some(lane);
            }
        }
        let now = self.local()?;
        if self.snapshot.is_none()
            && self.phase == Phase::Receiving
            && self.sync.state() == DepthState::Buffering
            && self.stream.as_ref().is_some_and(|s| s.first_event)
            && now.monotonic.nanos() >= self.snapshot_retry_at
            && self.cooldown.is_some_and(|t| now.monotonic.nanos() >= t)
        {
            self.begin_snapshot(permit, now)?;
        }
        Ok(())
    }
    fn stream_stage(
        &mut self,
        io: bool,
        permit: &mut impl PublicPermit,
        sink: &mut impl PublicSink,
        b: &mut Budget,
        r: &mut PollReport,
    ) -> Result<()> {
        if let Some(mut lane) = self.stream.take() {
            let result = if io {
                self.poll_stream(&mut lane, permit, sink, b, r)
            } else {
                self.decode_stream(&mut lane, sink, b, r)
            };
            self.stream = Some(lane);
            result?;
        }
        Ok(())
    }
    pub fn poll(
        &mut self,
        permit: &mut impl PublicPermit,
        budget: PollBudget,
        sink: &mut impl PublicSink,
    ) -> Result<PollReport> {
        if budget.max_ws_messages == 0
            || budget.max_decoder_turns == 0
            || budget.max_notifications == 0
            || budget.io.max_syscalls == 0
            || budget.io.max_syscalls > 1024
            || budget.io.max_bytes == 0
            || budget.io.max_bytes > 16 << 20
        {
            return Err(Error::Bounds);
        }
        let mut b = Budget {
            io: budget.io,
            messages: budget.max_ws_messages,
            turns: budget.max_decoder_turns,
            notices: budget.max_notifications,
        };
        let mut r = PollReport::default();
        self.notify_gap(sink, &mut b)?;
        if matches!(self.phase, Phase::Idle | Phase::Stopped) {
            return Ok(r);
        }
        let now = self.local().map_err(|e| self.fatal(e))?;
        let result = (|| {
            if self.phase == Phase::Backoff {
                if now.monotonic.nanos() < self.retry_at {
                    return Ok(());
                }
                self.open_stream(permit, now)?;
                r.needs_registration = true;
            }
            if let Some(lane) = &self.stream {
                let n = now.monotonic.nanos();
                if n >= add(lane.opened, self.config.timing.rotate_ns)?
                    || n >= add(lane.last_frame, self.config.timing.heartbeat_ns)?
                    || ((self.phase != Phase::Receiving || !lane.first_event)
                        && n >= lane.phase_deadline)
                {
                    return Err(Error::Timeout);
                }
            }
            self.stream_stage(false, permit, sink, &mut b, &mut r)?;
            self.barrier(permit, sink, &mut b, &mut r)?;
            self.http_first = !self.http_first;
            if self.http_first
                && let Some(mut lane) = self.snapshot.take()
            {
                self.poll_snapshot(&mut lane, permit, &mut b, &mut r);
                self.snapshot = Some(lane);
            }
            self.stream_stage(true, permit, sink, &mut b, &mut r)?;
            if !self.http_first
                && let Some(mut lane) = self.snapshot.take()
            {
                self.poll_snapshot(&mut lane, permit, &mut b, &mut r);
                self.snapshot = Some(lane);
            }
            self.barrier(permit, sink, &mut b, &mut r)?;
            Ok(())
        })();
        if let Err(e) = result {
            let permanent = matches!(
                e,
                Error::Clock | Error::Exhausted | Error::Sink | Error::Sync(_)
            );
            self.invalidate(e, permanent, now.monotonic.nanos());
            self.notify_gap(sink, &mut b)?;
            if permanent {
                return Err(e);
            }
        }
        self.notify_gap(sink, &mut b)?;
        r.needs_registration |= self.stream.as_ref().is_some_and(|s| !s.net.registered)
            || self.snapshot.as_ref().is_some_and(|s| !s.net.registered);
        r.runnable |= self.gap.is_some()
            || self.stream.as_ref().is_some_and(|s| !s.needs_input)
            || self.snapshot.as_ref().is_some_and(|s| s.result.is_some());
        r.next_deadline_ns = match self.phase {
            Phase::Backoff => Some(self.retry_at),
            Phase::Idle | Phase::Stopped => None,
            _ => self.stream.as_ref().map(|s| {
                let mut d = s
                    .opened
                    .saturating_add(self.config.timing.rotate_ns)
                    .min(s.last_frame.saturating_add(self.config.timing.heartbeat_ns));
                if self.phase != Phase::Receiving || !s.first_event {
                    d = d.min(s.phase_deadline);
                }
                if let Some(snapshot) = &self.snapshot {
                    d = d.min(snapshot.request.deadline_ns());
                } else if self.phase == Phase::Receiving
                    && self.sync.state() == DepthState::Buffering
                    && s.first_event
                    && let Some(cooldown) = self.cooldown
                {
                    d = d.min(self.snapshot_retry_at.max(cooldown));
                }
                if let Some(job) = &s.net.active {
                    d = d.min(job.deadline);
                }
                for job in &s.net.jobs {
                    d = d.min(job.deadline);
                }
                d
            }),
        };
        Ok(r)
    }
    pub fn status(&self) -> Status {
        Status {
            phase: self.phase,
            connection_epoch: self.epoch,
            book_generation: self.sync.generation(),
            depth: self.sync.state(),
            last_update_id: self.sync.last_update_id(),
            snapshot_pending: self.snapshot.is_some(),
        }
    }
    pub fn view(&mut self, age: u64) -> Result<BookView<'_>> {
        if self.phase == Phase::Stopped {
            return Err(Error::View(tickwire_book::ViewError::NotLive));
        }
        let now = self.local().map_err(|e| self.fatal(e))?;
        self.sync
            .view(now.monotonic.nanos(), age)
            .map_err(Error::View)
    }
    pub fn stop(&mut self, sink: &mut impl PublicSink) -> Result<()> {
        if self.phase != Phase::Stopped {
            self.invalidate(
                Error::Closed,
                true,
                self.last.map_or(0, |x| x.monotonic.nanos()),
            );
        }
        self.notify_gap(
            sink,
            &mut Budget {
                io: IoBudget::default(),
                messages: 0,
                turns: 0,
                notices: 1,
            },
        )
    }
    pub fn register(&mut self, registry: &mio::Registry, tokens: IoTokens) -> Result<()> {
        self.registrations(registry, tokens, false)
    }
    pub fn reregister(&mut self, registry: &mio::Registry, tokens: IoTokens) -> Result<()> {
        self.registrations(registry, tokens, true)
    }
    fn registrations(
        &mut self,
        registry: &mio::Registry,
        tokens: IoTokens,
        refresh: bool,
    ) -> Result<()> {
        if tokens.stream == tokens.snapshot {
            return Err(Error::Invalid);
        }
        for (lane, token) in [
            (self.stream.as_mut().map(|s| &mut s.net), tokens.stream),
            (self.snapshot.as_mut().map(|s| &mut s.net), tokens.snapshot),
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
}
