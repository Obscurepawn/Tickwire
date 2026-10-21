use crate::{
    Error, Result,
    frame::{Decoder, Frame, Header, Kind},
    service::{Limits, PeerBudget},
    tls::{AuthenticatedPeer, PeerTls, Scope},
};
use mio::net::TcpStream;
use protobuf::Message as _;
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    time::Instant,
};
use tickwire_control_raft::Message;
struct Pending {
    bytes: Vec<u8>,
    at: usize,
}
pub(crate) struct Lane {
    pub(crate) socket: TcpStream,
    connection: rustls::Connection,
    pub(crate) expected: Option<u64>,
    principal: Option<AuthenticatedPeer>,
    scope: Scope,
    local: u64,
    local_incarnation: [u8; 16],
    challenge: [u8; 16],
    remote_challenge: Option<[u8; 16]>,
    remote_incarnation: Option<[u8; 16]>,
    session: Option<[u8; 16]>,
    tx: u64,
    rx: u64,
    decoder: Decoder,
    queue: VecDeque<Pending>,
    pub(crate) queued_bytes: usize,
    started: Instant,
    last_activity: Instant,
    frame_started: Option<Instant>,
    ended: bool,
    connected: bool,
    handshake_bytes: usize,
}
pub(crate) struct LaneOutput {
    pub messages: Vec<Frame>,
    pub closed: bool,
    pub sent_bytes: usize,
}
impl Lane {
    pub fn new(
        socket: TcpStream,
        expected: Option<u64>,
        tls: &PeerTls,
        incarnation: [u8; 16],
        now: Instant,
        limits: &Limits,
    ) -> Result<Self> {
        socket.set_nodelay(true).map_err(|_| Error::Transport)?;
        let connection = if let Some(peer) = expected {
            tls.client(peer)?
        } else {
            tls.server()?
        };
        let mut challenge = [0; 16];
        getrandom::getrandom(&mut challenge).map_err(|_| Error::Transport)?;
        if challenge == [0; 16] {
            return Err(Error::Identity);
        }
        Ok(Self {
            socket,
            connection,
            expected,
            principal: None,
            scope: tls.scope(),
            local: tls.node_id(),
            local_incarnation: incarnation,
            challenge,
            remote_challenge: None,
            remote_incarnation: None,
            session: None,
            tx: 1,
            rx: 1,
            decoder: Decoder::new(limits.max_payload_bytes)?,
            queue: VecDeque::new(),
            queued_bytes: 0,
            started: now,
            last_activity: now,
            frame_started: None,
            ended: false,
            connected: expected.is_none(),
            handshake_bytes: 0,
        })
    }
    pub fn peer(&self) -> Option<u64> {
        self.principal
            .as_ref()
            .map(|p| p.node_id())
            .or(self.expected)
    }
    pub fn session(&self) -> Option<[u8; 16]> {
        self.session
    }
    pub fn active(&self) -> bool {
        self.session.is_some()
    }
    pub fn revalidate(&self, tls: &PeerTls) -> Result<()> {
        if let Some(p) = &self.principal {
            tls.revalidate(p)?;
        }
        Ok(())
    }
    fn header(&self, kind: Kind, challenge: [u8; 16], sequence: u64) -> Result<Header> {
        Ok(Header {
            kind,
            cluster: self.scope.cluster,
            group: self.scope.group,
            from: self.local,
            to: self.peer().ok_or(Error::Identity)?,
            incarnation: self.local_incarnation,
            challenge,
            sequence,
        })
    }
    fn enqueue(&mut self, frame: Frame, limits: &Limits) -> Result<()> {
        let bytes = frame.encode()?;
        if self.queue.len() >= limits.max_peer_frames
            || self
                .queued_bytes
                .checked_add(bytes.len())
                .is_none_or(|n| n > limits.max_peer_bytes)
        {
            return Err(Error::Bounds);
        }
        self.queued_bytes += bytes.len();
        self.queue.push_back(Pending { bytes, at: 0 });
        Ok(())
    }
    pub fn queue_message(&mut self, message: Message, limits: &Limits) -> Result<()> {
        let payload = message.write_to_bytes().map_err(|_| Error::Protocol)?;
        self.queue_frame(Kind::Raft, payload, limits)
    }
    pub fn queue_frame(&mut self, kind: Kind, payload: Vec<u8>, limits: &Limits) -> Result<()> {
        let session = self.session.ok_or(Error::Protocol)?;
        if payload.len() > limits.max_payload_bytes {
            return Err(Error::Bounds);
        }
        let next = self.tx.checked_add(1).ok_or(Error::Bounds)?;
        self.enqueue(
            Frame {
                header: self.header(kind, session, self.tx)?,
                payload,
            },
            limits,
        )?;
        self.tx = next;
        Ok(())
    }
    fn accept_frame(&mut self, frame: Frame, limits: &Limits) -> Result<Option<Frame>> {
        let h = &frame.header;
        if h.cluster != self.scope.cluster
            || h.group != self.scope.group
            || h.from != self.peer().ok_or(Error::Identity)?
            || h.to != self.local
        {
            return Err(Error::Identity);
        }
        if let Some(session) = self.session {
            if matches!(h.kind, Kind::Hello | Kind::HelloAck)
                || h.challenge != session
                || Some(h.incarnation) != self.remote_incarnation
                || h.sequence != self.rx
            {
                return Err(Error::Identity);
            }
            if h.kind == Kind::Raft {
                frame.raft()?;
            }
            self.rx = self.rx.checked_add(1).ok_or(Error::Bounds)?;
            return Ok(Some(frame));
        }
        if self.expected.is_some() {
            if h.kind != Kind::HelloAck || frame.payload != self.challenge || h.sequence != 0 {
                return Err(Error::Protocol);
            }
        } else if h.kind != Kind::Hello || !frame.payload.is_empty() || h.sequence != 0 {
            return Err(Error::Protocol);
        }
        self.remote_incarnation = Some(h.incarnation);
        self.remote_challenge = Some(h.challenge);
        if self.expected.is_none() {
            self.enqueue(
                Frame {
                    header: self.header(Kind::HelloAck, self.challenge, 0)?,
                    payload: h.challenge.to_vec(),
                },
                limits,
            )?;
        }
        let (client_id, server_id, client_inc, server_inc, client_challenge, server_challenge) =
            if self.expected.is_some() {
                (
                    self.local,
                    h.from,
                    self.local_incarnation,
                    h.incarnation,
                    self.challenge,
                    h.challenge,
                )
            } else {
                (
                    h.from,
                    self.local,
                    h.incarnation,
                    self.local_incarnation,
                    h.challenge,
                    self.challenge,
                )
            };
        let mut hash = Sha256::new();
        hash.update(b"tickwire.raft.session.v1");
        hash.update(self.scope.cluster);
        hash.update(self.scope.group);
        hash.update(client_id.to_le_bytes());
        hash.update(server_id.to_le_bytes());
        hash.update(client_inc);
        hash.update(server_inc);
        hash.update(client_challenge);
        hash.update(server_challenge);
        let result = hash.finalize();
        self.session = Some(result[..16].try_into().map_err(|_| Error::Protocol)?);
        Ok(None)
    }
    pub fn drive(
        &mut self,
        tls: &PeerTls,
        now: Instant,
        budget: &mut PeerBudget,
        limits: &Limits,
        dispatch: bool,
    ) -> Result<LaneOutput> {
        if now.duration_since(self.started) >= limits.handshake_timeout && !self.active() {
            return Err(Error::Timeout);
        }
        if now.duration_since(self.last_activity) >= limits.idle_timeout {
            return Err(Error::Timeout);
        }
        if self
            .frame_started
            .is_some_and(|s| now.duration_since(s) >= limits.frame_timeout)
        {
            return Err(Error::Timeout);
        }
        if !self.connected {
            if self
                .socket
                .take_error()
                .map_err(|_| Error::Transport)?
                .is_some()
            {
                return Err(Error::Transport);
            }
            match self.socket.peer_addr() {
                Ok(_) => self.connected = true,
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::NotConnected | io::ErrorKind::WouldBlock
                    ) =>
                {
                    return Ok(LaneOutput {
                        messages: Vec::new(),
                        closed: false,
                        sent_bytes: 0,
                    });
                }
                Err(_) => return Err(Error::Transport),
            }
        }
        self.revalidate(tls)?;
        let mut sent_bytes = 0;
        // One syscall in each direction per lane and turn. Plaintext/frames have
        // their own finite work within the shared byte/frame budget below.
        if self.connection.wants_write() && budget.write_bytes > 0 {
            let mut io = WriteLimit {
                socket: &mut self.socket,
                left: budget.write_bytes,
            };
            match self.connection.write_tls(&mut io) {
                Ok(n) => {
                    budget.write_bytes -= n;
                    sent_bytes += n;
                    if !self.active() {
                        self.handshake_bytes =
                            self.handshake_bytes.checked_add(n).ok_or(Error::Bounds)?;
                    }
                    if n > 0 {
                        self.last_activity = now;
                    }
                }
                Err(e) if retry(&e) => {}
                Err(_) => return Err(Error::Transport),
            }
        }
        if !self.ended && budget.read_bytes > 0 && (!self.active() || dispatch) {
            let mut io = ReadLimit {
                socket: &mut self.socket,
                left: budget.read_bytes.min(16 * 1024),
            };
            match self.connection.read_tls(&mut io) {
                Ok(0) => self.ended = true,
                Ok(n) => {
                    budget.read_bytes -= n;
                    if !self.active() {
                        self.handshake_bytes =
                            self.handshake_bytes.checked_add(n).ok_or(Error::Bounds)?;
                    }
                    self.last_activity = now;
                    self.connection
                        .process_new_packets()
                        .map_err(|_| Error::Authentication)?;
                }
                Err(e) if retry(&e) => {}
                Err(_) => return Err(Error::Transport),
            }
        }
        if self.handshake_bytes > 2 * 1024 * 1024 {
            return Err(Error::Bounds);
        }
        if self.principal.is_none() && !self.connection.is_handshaking() {
            let peer = tls.authenticate(&self.connection, self.expected)?;
            if self.expected.is_none() && peer.node_id() >= self.local {
                return Err(Error::Identity);
            }
            self.principal = Some(peer);
            if self.expected.is_some() {
                self.enqueue(
                    Frame {
                        header: self.header(Kind::Hello, self.challenge, 0)?,
                        payload: Vec::new(),
                    },
                    limits,
                )?;
            }
        }
        let mut messages = Vec::new();
        if self.principal.is_some() && dispatch {
            while budget.frames > 0 {
                if let Some(frame) = self.decoder.next_frame()? {
                    budget.frames -= 1;
                    if let Some(message) = self.accept_frame(frame, limits)? {
                        messages.push(message);
                    }
                    if self.decoder.buffered_len() == 0 {
                        self.frame_started = None;
                    }
                    continue;
                }
                if budget.read_bytes == 0 || self.decoder.remaining() == 0 {
                    break;
                }
                let mut buf = [0; 16 * 1024];
                let limit = buf
                    .len()
                    .min(budget.read_bytes)
                    .min(self.decoder.remaining());
                match self.connection.reader().read(&mut buf[..limit]) {
                    Ok(0) => {
                        self.ended = true;
                        break;
                    }
                    Ok(n) => {
                        budget.read_bytes -= n;
                        self.frame_started.get_or_insert(now);
                        self.decoder.push(&buf[..n])?;
                    }
                    Err(e) if retry(&e) => break,
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        self.ended = true;
                        break;
                    }
                    Err(_) => return Err(Error::Transport),
                }
            }
        }
        while budget.write_bytes > 0 {
            let Some(pending) = self.queue.front_mut() else {
                break;
            };
            let end = (pending.at + budget.write_bytes).min(pending.bytes.len());
            match self
                .connection
                .writer()
                .write(&pending.bytes[pending.at..end])
            {
                Ok(0) => break,
                Ok(n) => {
                    pending.at += n;
                    budget.write_bytes -= n;
                    if pending.at == pending.bytes.len() {
                        let frame = self.queue.pop_front().ok_or(Error::Protocol)?;
                        self.queued_bytes -= frame.bytes.len();
                    }
                }
                Err(e) if retry(&e) => break,
                Err(_) => return Err(Error::Transport),
            }
        }
        let closed = self.ended && messages.is_empty() && budget.frames > 0;
        Ok(LaneOutput {
            messages,
            closed,
            sent_bytes,
        })
    }
}
fn retry(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    )
}
struct ReadLimit<'a> {
    socket: &'a mut TcpStream,
    left: usize,
}
impl Read for ReadLimit<'_> {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        let end = b.len().min(self.left);
        self.socket.read(&mut b[..end])
    }
}
struct WriteLimit<'a> {
    socket: &'a mut TcpStream,
    left: usize,
}
impl Write for WriteLimit<'_> {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.socket.write(&b[..b.len().min(self.left)])
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
