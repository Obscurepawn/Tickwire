use crate::{Error, tls::PeerTls};
use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpListener},
    time::{Duration, Instant},
};
use tickwire_control_raft::driver::{DriverStatus, PeerDriver};
#[derive(Debug)]
pub enum ServiceError<E> {
    Transport(Error),
    Driver(E),
    Contract,
    Stopped,
}
pub type ServiceResult<T, E> = core::result::Result<T, ServiceError<E>>;
#[derive(Debug, Clone)]
pub struct Limits {
    pub handshake_timeout: Duration,
    pub frame_timeout: Duration,
    pub idle_timeout: Duration,
    pub reconnect_delay: Duration,
    pub max_payload_bytes: usize,
    pub max_peer_frames: usize,
    pub max_peer_bytes: usize,
    pub max_queued_bytes: usize,
    pub max_applied_items: usize,
    pub max_applied_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(3),
            frame_timeout: Duration::from_secs(3),
            idle_timeout: Duration::from_secs(3),
            reconnect_delay: Duration::from_millis(20),
            max_payload_bytes: 64 * 1024 * 1024,
            max_peer_frames: 64,
            max_peer_bytes: 128 * 1024 * 1024,
            max_queued_bytes: 256 * 1024 * 1024,
            max_applied_items: 8192,
            max_applied_bytes: 128 * 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct PeerBudget {
    pub accepts: usize,
    pub read_bytes: usize,
    pub write_bytes: usize,
    pub frames: usize,
    pub ticks: usize,
    pub ready_rounds: usize,
}
impl Default for PeerBudget {
    fn default() -> Self {
        Self {
            accepts: 4,
            read_bytes: 256 * 1024,
            write_bytes: 256 * 1024,
            frames: 32,
            ticks: 8,
            ready_rounds: 1,
        }
    }
}
#[derive(Debug, Default)]
pub struct PollStatus {
    pub connected: usize,
    pub received: usize,
    pub sent_bytes: usize,
    pub application_backpressure: bool,
    pub faults: Vec<(Option<u64>, Error)>,
}

pub struct PeerService<D: PeerDriver> {
    driver: D,
    listener: mio::net::TcpListener,
    tls: PeerTls,
    incarnation: [u8; 16],
    peers: BTreeMap<u64, SocketAddr>,
    retry: BTreeMap<u64, Instant>,
    lanes: Vec<crate::lane::Lane>,
    limits: Limits,
    applied: std::collections::VecDeque<D::Applied>,
    applied_bytes: usize,
    stopped: bool,
    last: Instant,
    next_tick: Instant,
    rotation: usize,
    incoming: BTreeMap<u64, (crate::snapshot_transfer::Incoming, Instant)>,
    outgoing: BTreeMap<u64, (crate::snapshot_transfer::Outgoing, Instant)>,
    snapshot_acks: BTreeMap<u64, ([u8; 32], u64)>,
}
impl<D: PeerDriver> PeerService<D> {
    pub fn new(
        driver: D,
        listener: TcpListener,
        incarnation: [u8; 16],
        peers: BTreeMap<u64, SocketAddr>,
        tls: PeerTls,
        limits: Limits,
    ) -> ServiceResult<Self, D::Error> {
        limits.validate().map_err(ServiceError::Transport)?;
        let status = driver.status();
        let member = driver.current_membership();
        let mut all = peers.keys().copied().collect::<Vec<_>>();
        all.push(status.node_id);
        all.sort_unstable();
        if incarnation == [0; 16]
            || status.node_id != tls.node_id()
            || status.group != tls.scope().group
            || status.poisoned
            || peers.is_empty()
            || peers.len() > 16
            || all.contains(&0)
            || all.windows(2).any(|w| w[0] == w[1])
            || all.len() > 16
            || (!status.joining
                && member
                    .voters()
                    .iter()
                    .chain(member.voters_outgoing())
                    .chain(member.learners())
                    .any(|n| !all.contains(n)))
            || peers.keys().copied().collect::<Vec<_>>() != tls.peer_ids().collect::<Vec<_>>()
        {
            return Err(ServiceError::Transport(Error::Identity));
        }
        listener
            .set_nonblocking(true)
            .map_err(|_| ServiceError::Transport(Error::Transport))?;
        let applied = std::collections::VecDeque::with_capacity(limits.max_applied_items);
        let applied_bytes = applied
            .capacity()
            .checked_mul(std::mem::size_of::<D::Applied>())
            .ok_or(ServiceError::Transport(Error::Bounds))?;
        if applied_bytes > limits.max_applied_bytes / 2 {
            return Err(ServiceError::Transport(Error::Bounds));
        }
        let now = Instant::now();
        Ok(Self {
            driver,
            listener: mio::net::TcpListener::from_std(listener),
            tls,
            incarnation,
            peers,
            retry: BTreeMap::new(),
            lanes: Vec::new(),
            limits,
            applied,
            applied_bytes,
            stopped: false,
            last: now,
            next_tick: now + Duration::from_millis(10),
            rotation: 0,
            incoming: BTreeMap::new(),
            outgoing: BTreeMap::new(),
            snapshot_acks: BTreeMap::new(),
        })
    }
    pub fn poll(
        &mut self,
        now: Instant,
        budget: PeerBudget,
    ) -> ServiceResult<PollStatus, D::Error> {
        if self.stopped {
            return Err(ServiceError::Stopped);
        }
        if now < self.last
            || budget.accepts > 32
            || budget.frames == 0
            || budget.frames > 1024
            || budget.read_bytes == 0
            || budget.read_bytes > 16 * 1024 * 1024
            || budget.write_bytes == 0
            || budget.write_bytes > 16 * 1024 * 1024
            || budget.ticks == 0
            || budget.ticks > 128
            || budget.ready_rounds == 0
            || budget.ready_rounds > 128
        {
            return Err(ServiceError::Transport(Error::Bounds));
        }
        self.last = now;
        let result = self.poll_inner(now, budget);
        if result.is_err() {
            self.stop();
        }
        result
    }
    fn can_drive(
        &self,
        budget: tickwire_control_raft::driver::ReadyBudget,
    ) -> ServiceResult<bool, D::Error> {
        let bound = self
            .driver
            .output_bound(budget)
            .map_err(ServiceError::Driver)?;
        if bound.items > self.limits.max_applied_items
            || bound.bytes > self.limits.max_applied_bytes
        {
            return Err(ServiceError::Transport(Error::Bounds));
        }
        Ok(self
            .applied
            .len()
            .checked_add(bound.items)
            .is_some_and(|n| n <= self.limits.max_applied_items)
            && self
                .applied_bytes
                .checked_add(bound.bytes)
                .is_some_and(|n| n <= self.limits.max_applied_bytes))
    }
    fn poll_inner(
        &mut self,
        now: Instant,
        mut budget: PeerBudget,
    ) -> ServiceResult<PollStatus, D::Error> {
        use crate::lane::Lane;
        use protobuf::Message as _;
        use std::io::ErrorKind;
        use tickwire_control_raft::driver::{ReadyBudget, RetainedSize};
        let mut output = PollStatus::default();
        let ready = ReadyBudget::new(budget.ready_rounds)
            .map_err(|_| ServiceError::Transport(Error::Bounds))?;
        let dispatch = self.can_drive(ready)?;
        output.application_backpressure = !dispatch;
        let local = self.driver.status().node_id;
        let membership = self.driver.current_membership();
        let retired = membership.retired();
        let local_retired = retired.contains(&local);
        for _ in 0..budget.accepts {
            match self.listener.accept() {
                Ok((socket, _)) => {
                    if local_retired || self.lanes.len() >= self.peers.len() * 2 {
                        continue;
                    }
                    match Lane::new(socket, None, &self.tls, self.incarnation, now, &self.limits) {
                        Ok(lane) => self.lanes.push(lane),
                        Err(e) => output.faults.push((None, e)),
                    }
                }
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => {
                    break;
                }
                Err(_) => return Err(ServiceError::Transport(Error::Transport)),
            }
        }
        for (&id, &address) in &self.peers {
            if local_retired
                || retired.contains(&id)
                || id <= local
                || self.lanes.iter().any(|l| l.peer() == Some(id))
                || self.retry.get(&id).is_some_and(|n| now < *n)
            {
                continue;
            }
            self.retry.insert(id, now + self.limits.reconnect_delay);
            match mio::net::TcpStream::connect(address).and_then(|socket| {
                socket.set_nodelay(true)?;
                Ok(socket)
            }) {
                Ok(socket) => match Lane::new(
                    socket,
                    Some(id),
                    &self.tls,
                    self.incarnation,
                    now,
                    &self.limits,
                ) {
                    Ok(lane) => self.lanes.push(lane),
                    Err(e) => output.faults.push((Some(id), e)),
                },
                Err(_) => output.faults.push((Some(id), Error::Transport)),
            }
        }
        if !self.lanes.is_empty() {
            let n = self.rotation % self.lanes.len();
            self.lanes.rotate_left(n);
            self.rotation = self.rotation.wrapping_add(1);
        }
        let mut i = 0;
        while i < self.lanes.len() {
            let was_active = self.lanes[i].active();
            let expired = self.lanes[i].peer().is_some_and(|peer| {
                self.incoming.get(&peer).is_some_and(|(_, start)| {
                    now.duration_since(*start) >= Duration::from_secs(300)
                }) || self.outgoing.get(&peer).is_some_and(|(_, start)| {
                    now.duration_since(*start) >= Duration::from_secs(300)
                })
            });
            let result =
                if local_retired || self.lanes[i].peer().is_some_and(|p| retired.contains(&p)) {
                    Err(Error::Closed)
                } else if expired {
                    Err(Error::Timeout)
                } else {
                    self.lanes[i].drive(&self.tls, now, &mut budget, &self.limits, dispatch)
                };
            let failure = match result {
                Ok(lane) => {
                    output.sent_bytes += lane.sent_bytes;
                    // Incarnations are opaque identities, not ordered fencing
                    // tokens. Keep the existing session until it closes or
                    // times out. Only the newly activated lane can lose this
                    // comparison, regardless of traversal rotation. Check
                    // before dispatching any messages parsed with its Hello.
                    let duplicate = !was_active
                        && self.lanes[i].active()
                        && self.lanes.iter().enumerate().any(|(j, other)| {
                            j != i && other.active() && other.peer() == self.lanes[i].peer()
                        });
                    if duplicate {
                        Some(Error::Identity)
                    } else {
                        if !was_active && self.lanes[i].active() {
                            self.driver
                                .peer_session(
                                    self.lanes[i].peer().ok_or(ServiceError::Contract)?,
                                    self.lanes[i].session(),
                                )
                                .map_err(ServiceError::Driver)?;
                        }
                        let mut invalid = None;
                        for frame in lane.messages {
                            match self.handle_frame(frame, now) {
                                Ok(()) => output.received += 1,
                                Err(ServiceError::Transport(e)) => {
                                    invalid = Some(e);
                                    break;
                                }
                                Err(e) => return Err(e),
                            }
                        }
                        invalid.or_else(|| lane.closed.then_some(Error::Closed))
                    }
                }
                Err(e) => Some(e),
            };
            if let Some(e) = failure {
                let peer = self.lanes[i].peer();
                self.lanes.swap_remove(i);
                output.faults.push((peer, e));
                if let Some(peer) = peer
                    && !self
                        .lanes
                        .iter()
                        .any(|lane| lane.active() && lane.peer() == Some(peer))
                {
                    self.driver
                        .peer_session(peer, None)
                        .map_err(ServiceError::Driver)?;
                    self.incoming.remove(&peer);
                    self.snapshot_acks.remove(&peer);
                    if self.outgoing.remove(&peer).is_some() {
                        self.driver
                            .report_snapshot(peer, false)
                            .map_err(ServiceError::Driver)?;
                    }
                    self.retry.insert(peer, now + self.limits.reconnect_delay);
                    self.driver
                        .report_unreachable(peer)
                        .map_err(ServiceError::Driver)?;
                }
                continue;
            }
            i += 1;
        }
        if dispatch {
            for _ in 0..budget.ticks {
                if now < self.next_tick {
                    break;
                }
                self.driver.tick().map_err(ServiceError::Driver)?;
                self.next_tick += Duration::from_millis(10);
            }
            if self.can_drive(ready)? {
                let bound = self
                    .driver
                    .output_bound(ready)
                    .map_err(ServiceError::Driver)?;
                let result = self.driver.drive(ready).map_err(ServiceError::Driver)?;
                let charge = result.charge().map_err(|_| ServiceError::Contract)?;
                if charge.applied.items > bound.items
                    || charge.applied.bytes > bound.bytes
                    || result.application_scanned_through != result.raft_applied_through
                {
                    return Err(ServiceError::Contract);
                }
                for applied in result.applied {
                    let bytes = applied
                        .retained_bytes()
                        .checked_sub(std::mem::size_of::<D::Applied>())
                        .ok_or(ServiceError::Contract)?;
                    self.applied_bytes = self
                        .applied_bytes
                        .checked_add(bytes)
                        .ok_or(ServiceError::Contract)?;
                    self.applied.push_back(applied);
                }
                let mut queued: usize = self.lanes.iter().map(|l| l.queued_bytes).sum();
                for message in result.messages {
                    let peer = message.to;
                    if message.get_msg_type() == tickwire_control_raft::MessageType::MsgSnapshot {
                        if !self.outgoing.contains_key(&peer) {
                            let reader = self
                                .driver
                                .snapshot_reader()
                                .map_err(ServiceError::Driver)?;
                            if let Some(snapshot) = reader {
                                let outgoing =
                                    crate::snapshot_transfer::Outgoing::new(snapshot, &message)
                                        .map_err(ServiceError::Transport)?;
                                self.outgoing.insert(peer, (outgoing, now));
                            } else {
                                self.driver
                                    .report_snapshot(peer, false)
                                    .map_err(ServiceError::Driver)?;
                            }
                        }
                        continue;
                    }
                    let size = message.compute_size() as usize + crate::frame::HEADER_BYTES;
                    let lane = self
                        .lanes
                        .iter_mut()
                        .find(|l| l.active() && l.peer() == Some(peer));
                    let sent = if queued
                        .checked_add(size)
                        .is_some_and(|n| n <= self.limits.max_queued_bytes)
                    {
                        lane.is_some_and(|l| l.queue_message(message, &self.limits).is_ok())
                    } else {
                        false
                    };
                    if sent {
                        queued += size;
                    } else {
                        self.driver
                            .report_unreachable(peer)
                            .map_err(ServiceError::Driver)?;
                    }
                }
            } else {
                output.application_backpressure = true;
            }
        }
        // A completed receive cannot acknowledge until the driver applied its
        // whole Ready through the business reducer. Stale snapshots can only be
        // acknowledged when that already-applied prefix covers their cut.
        let applied = self.driver.status().raft_applied;
        let acks = self
            .snapshot_acks
            .iter()
            .filter(|(_, (_, index))| applied >= *index)
            .map(|(&p, &value)| (p, value))
            .collect::<Vec<_>>();
        for (peer, (digest, index)) in acks {
            if self
                .lanes
                .iter()
                .map(|l| l.queued_bytes)
                .sum::<usize>()
                .checked_add(40 + crate::frame::HEADER_BYTES)
                .is_none_or(|n| n > self.limits.max_queued_bytes)
            {
                continue;
            }
            if let Some(lane) = self
                .lanes
                .iter_mut()
                .find(|l| l.active() && l.peer() == Some(peer))
                && lane
                    .queue_frame(
                        crate::frame::Kind::SnapshotApplied,
                        crate::snapshot_transfer::acknowledgement(digest, index),
                        &self.limits,
                    )
                    .is_ok()
            {
                self.snapshot_acks.remove(&peer);
            }
        }
        let peers = self.outgoing.keys().copied().collect::<Vec<_>>();
        for peer in peers {
            if budget.frames == 0 {
                break;
            }
            let expired = self
                .outgoing
                .get(&peer)
                .is_some_and(|(_, at)| now.duration_since(*at) >= Duration::from_secs(300));
            let queued = self.lanes.iter().map(|l| l.queued_bytes).sum::<usize>();
            if !expired
                && queued
                    .checked_add(65_536 + 256 + crate::frame::HEADER_BYTES)
                    .is_none_or(|n| n > self.limits.max_queued_bytes)
            {
                continue;
            }
            let lane = self
                .lanes
                .iter_mut()
                .find(|l| l.active() && l.peer() == Some(peer));
            let mut failed = expired;
            if !expired
                && let Some(lane) = lane
                && lane.queued_bytes == 0
                && let Some((kind, bytes)) = self
                    .outgoing
                    .get_mut(&peer)
                    .ok_or(ServiceError::Contract)?
                    .0
                    .next()
                    .map_err(ServiceError::Transport)?
            {
                budget.frames -= 1;
                failed = lane.queue_frame(kind, bytes, &self.limits).is_err();
            }
            if failed {
                self.outgoing.remove(&peer);
                self.driver
                    .report_snapshot(peer, false)
                    .map_err(ServiceError::Driver)?;
            }
        }
        output.connected = self.lanes.iter().filter(|l| l.active()).count();
        Ok(output)
    }
    fn handle_frame(
        &mut self,
        frame: crate::frame::Frame,
        now: Instant,
    ) -> ServiceResult<(), D::Error> {
        use crate::{
            frame::Kind,
            snapshot_transfer::{Incoming, Offer},
        };
        let peer = frame.header.from;
        let membership = self.driver.current_membership();
        let status = self.driver.status();
        if membership.retired().contains(&peer) || membership.retired().contains(&status.node_id) {
            return Ok(());
        }
        if !membership.contains(peer) && !status.joining {
            return Ok(());
        }
        if status.joining
            && !matches!(
                frame.header.kind,
                Kind::Raft | Kind::SnapshotBegin | Kind::SnapshotChunk | Kind::SnapshotEnd
            )
        {
            return Ok(());
        }
        match frame.header.kind {
            Kind::Raft => {
                let message = frame.raft().map_err(ServiceError::Transport)?;
                if status.joining
                    && !matches!(
                        message.get_msg_type(),
                        tickwire_control_raft::MessageType::MsgAppend
                            | tickwire_control_raft::MessageType::MsgHeartbeat
                    )
                {
                    return Ok(());
                }
                self.driver
                    .step(peer, message)
                    .map_err(ServiceError::Driver)?;
            }
            Kind::SnapshotBegin => {
                if !self.incoming.is_empty() || !self.snapshot_acks.is_empty() {
                    return Err(ServiceError::Transport(Error::Protocol));
                }
                let offer = Offer::parse(&frame.payload).map_err(ServiceError::Transport)?;
                let members = offer.manifest.cut().membership();
                if offer.manifest.cut().group() != status.group
                    || !members.contains(status.node_id)
                    || (!members.voters().contains(&peer)
                        && !members.voters_outgoing().contains(&peer))
                {
                    return Err(ServiceError::Transport(Error::Identity));
                }
                let stage = self
                    .driver
                    .stage_snapshot()
                    .map_err(ServiceError::Driver)?
                    .ok_or(ServiceError::Transport(Error::Protocol))?;
                self.incoming
                    .insert(peer, (Incoming::new(offer, stage), now));
            }
            Kind::SnapshotChunk => {
                let (incoming, start) = self
                    .incoming
                    .get_mut(&peer)
                    .ok_or(ServiceError::Transport(Error::Protocol))?;
                if now.duration_since(*start) >= Duration::from_secs(300) {
                    return Err(ServiceError::Transport(Error::Timeout));
                }
                incoming
                    .chunk(&frame.payload)
                    .map_err(ServiceError::Transport)?;
            }
            Kind::SnapshotEnd => {
                let (incoming, start) = self
                    .incoming
                    .remove(&peer)
                    .ok_or(ServiceError::Transport(Error::Protocol))?;
                if now.duration_since(start) >= Duration::from_secs(300) {
                    return Err(ServiceError::Transport(Error::Timeout));
                }
                let message = incoming
                    .offer
                    .message(peer, self.driver.status().node_id)
                    .map_err(ServiceError::Transport)?;
                let cut = incoming.offer.manifest.cut().index();
                let digest = incoming.offer.digest;
                let snapshot = incoming
                    .finish(&frame.payload)
                    .map_err(ServiceError::Transport)?;
                if !self
                    .driver
                    .step_snapshot(peer, message, snapshot)
                    .map_err(ServiceError::Driver)?
                {
                    return Err(ServiceError::Transport(Error::Protocol));
                }
                self.snapshot_acks.insert(peer, (digest, cut));
            }
            Kind::SnapshotApplied => {
                let (transfer, _) = self
                    .outgoing
                    .get(&peer)
                    .ok_or(ServiceError::Transport(Error::Protocol))?;
                transfer
                    .accepted(&frame.payload)
                    .map_err(ServiceError::Transport)?;
                self.outgoing.remove(&peer);
                self.driver
                    .report_snapshot(peer, true)
                    .map_err(ServiceError::Driver)?;
            }
            _ => return Err(ServiceError::Transport(Error::Protocol)),
        }
        Ok(())
    }
    pub fn with_control<R>(
        &mut self,
        f: impl FnOnce(D::Control<'_>) -> R,
    ) -> ServiceResult<R, D::Error> {
        if self.stopped {
            return Err(ServiceError::Stopped);
        }
        Ok(f(self.driver.control()))
    }
    pub fn take_applied(&mut self, max_items: usize, max_bytes: usize) -> Vec<D::Applied> {
        use tickwire_control_raft::driver::RetainedSize;
        let mut output = Vec::new();
        let mut bytes = 0usize;
        while output.len() < max_items {
            let Some(next) = self.applied.front() else {
                break;
            };
            let size = next.retained_bytes();
            let Some(next_bytes) = bytes.checked_add(size).filter(|n| *n <= max_bytes) else {
                break;
            };
            bytes = next_bytes;
            self.applied_bytes -= size - std::mem::size_of::<D::Applied>();
            if let Some(value) = self.applied.pop_front() {
                output.push(value);
            }
        }
        output
    }
    pub fn current_membership(&self) -> tickwire_control_raft::driver::Membership {
        self.driver.current_membership()
    }
    /// Install a trusted certificate/CRL revision for the same node group.
    /// Revoke existing sessions before another I/O or Raft step can occur.
    pub fn replace_trust(&mut self, tls: PeerTls) -> ServiceResult<usize, D::Error> {
        if self.stopped {
            return Err(ServiceError::Stopped);
        }
        if tls.scope() != self.tls.scope()
            || tls.node_id() != self.tls.node_id()
            || tls.peer_ids().collect::<Vec<_>>() != self.peers.keys().copied().collect::<Vec<_>>()
        {
            return Err(ServiceError::Transport(Error::Identity));
        }
        self.tls = tls;
        let mut removed = 0;
        let mut index = 0;
        while index < self.lanes.len() {
            if self.lanes[index].revalidate(&self.tls).is_err() {
                let peer = self.lanes[index].peer();
                self.lanes.swap_remove(index);
                removed += 1;
                if let Some(peer) = peer {
                    if let Err(error) = self.driver.peer_session(peer, None) {
                        self.stop();
                        return Err(ServiceError::Driver(error));
                    }
                    self.incoming.remove(&peer);
                    self.snapshot_acks.remove(&peer);
                    let result = if self.outgoing.remove(&peer).is_some() {
                        self.driver.report_snapshot(peer, false)
                    } else {
                        Ok(())
                    };
                    if let Err(error) = result.and_then(|_| self.driver.report_unreachable(peer)) {
                        self.stop();
                        return Err(ServiceError::Driver(error));
                    }
                }
            } else {
                index += 1;
            }
        }
        Ok(removed)
    }
    pub fn status(&self) -> DriverStatus {
        self.driver.status()
    }
    pub fn stop(&mut self) {
        self.stopped = true;
        self.lanes.clear();
        self.incoming.clear();
        self.outgoing.clear();
        self.snapshot_acks.clear();
    }
}
impl Limits {
    fn validate(&self) -> crate::Result<()> {
        if self.handshake_timeout.is_zero()
            || self.handshake_timeout > Duration::from_secs(60)
            || self.frame_timeout.is_zero()
            || self.frame_timeout > Duration::from_secs(60)
            || self.idle_timeout.is_zero()
            || self.idle_timeout > Duration::from_secs(300)
            || self.reconnect_delay.is_zero()
            || self.reconnect_delay > Duration::from_secs(60)
            || !(1..=crate::frame::MAX_PAYLOAD_BYTES).contains(&self.max_payload_bytes)
            || !(1..=64).contains(&self.max_peer_frames)
            || !(1..=128 * 1024 * 1024).contains(&self.max_peer_bytes)
            || !(1..=256 * 1024 * 1024).contains(&self.max_queued_bytes)
            || self.max_peer_bytes > self.max_queued_bytes
            || !(1..=262144).contains(&self.max_applied_items)
            || !(1..=512 * 1024 * 1024).contains(&self.max_applied_bytes)
        {
            return Err(Error::Bounds);
        }
        Ok(())
    }
}
