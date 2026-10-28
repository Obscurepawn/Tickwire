use super::*;
use std::os::fd::OwnedFd;
use tickwire_control_sys::PendingConnection;
mod step;

/// Work counts protocol steps and records, not a hard wall-clock deadline.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct JoinProgress {
    pub work: usize,
    pub records: usize,
    pub runnable: bool,
    pub ready: bool,
    pub next_deadline_ns: u64,
}
/// Owns an incomplete JOIN. No runnable client escapes before authenticated activation.
pub struct PendingJoin {
    state: Option<Box<State>>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Connect,
    Hello,
    Welcome,
    ValidateWelcome,
    Request,
    Joined,
    Bank,
    Snapshot,
    Cursor,
    ReadBank,
    ReadSnapshot,
    Catchup,
    Ready,
    Activation,
    WaitingDrain,
    Complete,
}
struct Outgoing {
    bytes: Vec<u8>,
    fd: Option<OwnedFd>,
}
struct State {
    config: ConnectConfig,
    domain: ClockDomain,
    expires: u64,
    last_now: u64,
    phase: Phase,
    bank: ResourceHeader,
    registry: Option<TopicRegistry>,
    pending: Option<PendingConnection>,
    connection: Option<Connection>,
    handshake: Option<ClientHandshake>,
    welcome: Option<ReceivedFrame>,
    session: Option<Session>,
    request: Option<JoinBody>,
    join: Option<JoinBody>,
    bank_fd: Option<OwnedFd>,
    outgoing: Option<Outgoing>,
    consumer: Option<JoinConsumer<4>>,
    replica: Option<MarketReplica>,
    baseline: Option<JoinBootstrap>,
    baseline_bytes: Vec<u8>,
    baseline_now: u64,
    catchup: Vec<CatchupRecord>,
    retained_bytes: usize,
    target: u64,
}
fn interrupted(error: &Error) -> bool {
    matches!(error, Error::Transport(tickwire_control_sys::Error::System { source, .. })
        if source.kind() == std::io::ErrorKind::Interrupted)
}
impl PendingJoin {
    /// # Safety
    /// The endpoint names an authenticated qualified MD writer whose immutable
    /// bank and gated reuse contracts hold for this exact approved subscription.
    pub unsafe fn new(config: &ConnectConfig, now: MonoTime) -> Result<Self> {
        if config.startup_timeout_ms == 0
            || config.startup_timeout_ms > 60000
            || config.maximum_levels == 0
            || config.maximum_levels > 10000
            || config.cursor_id == [0; 16]
            || config.subscription_generation == 0
            || config.request_id == [0; 16]
            || !matches!(config.endpoint.local.role, 3 | 4)
            || config.endpoint.peer.role != 2
        {
            return Err(Error::Identity);
        }
        let domain = config.domain()?;
        if now.domain() != domain {
            return Err(Error::Identity);
        }
        let registry = config.registry()?;
        let mut bank = ResourceHeader::decode(&config.bank_header)?;
        if bank.producer_instance != config.endpoint.peer.instance
            || bank.producer_generation != config.endpoint.peer.generation
            || bank.host_boot_id != config.endpoint.local.host_boot
            || bank.namespace_id != config.endpoint.local.namespace
            || bank.required_features & 64 == 0
            || bank.resource_kind != 1
        {
            return Err(Error::Identity);
        }
        bank.descriptor_hash = [0; 32];
        bank.descriptor_hash = Sha256::digest(wire_bytes(&bank)?).into();
        let expires = now
            .nanos()
            .checked_add(
                config
                    .startup_timeout_ms
                    .checked_mul(1_000_000)
                    .ok_or(Error::Bounds)?,
            )
            .ok_or(Error::Bounds)?;
        Ok(Self {
            state: Some(Box::new(State {
                config: config.clone(),
                domain,
                expires,
                last_now: now.nanos(),
                phase: Phase::Connect,
                bank,
                registry: Some(registry),
                pending: Some(PendingConnection::new(&config.endpoint.path)?),
                connection: None,
                handshake: Some(ClientHandshake::new(
                    config.endpoint.policy(),
                    config.request_id,
                )?),
                welcome: None,
                session: None,
                request: None,
                join: None,
                bank_fd: None,
                outgoing: None,
                consumer: None,
                replica: None,
                baseline: None,
                baseline_bytes: Vec::new(),
                baseline_now: 0,
                catchup: Vec::new(),
                retained_bytes: 0,
                target: 0,
            })),
        })
    }
    /// At most min(budget, 64) protocol attempts or catchup records; never waits.
    pub fn poll(&mut self, now: MonoTime, budget: usize) -> Result<JoinProgress> {
        let result = self
            .state
            .as_mut()
            .ok_or(Error::NotReady)?
            .poll(now, budget);
        match result {
            Err(error) => {
                self.cancel();
                Err(error)
            }
            Ok(report) => Ok(report),
        }
    }
    /// Transfers the activated client exactly once. Before activation it returns None.
    pub fn take_client(&mut self) -> Option<Client> {
        if self.state.as_ref()?.phase != Phase::Complete {
            return None;
        }
        let mut s = *self.state.take()?;
        Some(Client {
            consumer: s.consumer.take().unwrap(),
            replica: s.replica.take().unwrap(),
            connection: s.connection.take().unwrap(),
            session: s.session.take().unwrap(),
            baseline: s.baseline.take().unwrap(),
            baseline_bytes: s.baseline_bytes,
            baseline_now: s.baseline_now,
            catchup: s.catchup,
            bank: s.bank,
            join: s.join.take().unwrap(),
            domain: s.domain,
        })
    }
    /// Closes local resources. This is not evidence permitting owner gate removal.
    pub fn cancel(&mut self) {
        self.state = None;
    }
}
impl State {
    fn poll(&mut self, now: MonoTime, budget: usize) -> Result<JoinProgress> {
        if budget == 0 {
            return Err(Error::Bounds);
        }
        if now.domain() != self.domain {
            return Err(Error::Identity);
        }
        if now.nanos() < self.last_now {
            return Err(Error::Stale);
        }
        if now.nanos() >= self.expires {
            return Err(Error::Deadline);
        }
        self.last_now = now.nanos();
        let mut report = JoinProgress {
            next_deadline_ns: self.expires,
            ..Default::default()
        };
        for _ in 0..budget.min(64) {
            if self.phase == Phase::Complete {
                break;
            }
            report.work += 1;
            let step = match self.advance(now.nanos()) {
                Err(error) if interrupted(&error) => None,
                result => result?,
            };
            match step {
                Some(record) => report.records += usize::from(record),
                None => {
                    // WELCOME was received after this call's time sample. Yield so the
                    // caller can sample again before validating the peer's new lease.
                    report.runnable = self.phase == Phase::ValidateWelcome;
                    report.next_deadline_ns = self.expires;
                    return Ok(report);
                }
            }
        }
        report.ready = self.phase == Phase::Complete;
        report.runnable = !report.ready;
        report.next_deadline_ns = self.expires;
        Ok(report)
    }
    fn send(&mut self, build: impl FnOnce(&Self) -> Result<Outgoing>) -> Result<bool> {
        if self.outgoing.is_none() {
            self.outgoing = Some(build(self)?);
        }
        let outgoing = self.outgoing.as_ref().unwrap();
        let sent = match &outgoing.fd {
            Some(fd) => self
                .connection
                .as_mut()
                .unwrap()
                .try_send(&outgoing.bytes, &[fd.as_fd()])?,
            None => self
                .connection
                .as_mut()
                .unwrap()
                .try_send(&outgoing.bytes, &[])?,
        };
        if sent {
            self.outgoing = None;
        }
        Ok(sent)
    }
    fn receive(&mut self) -> Result<Option<ReceivedFrame>> {
        Ok(self.connection.as_mut().unwrap().try_recv()?)
    }
    fn drain_one(&mut self, now: u64) -> Result<bool> {
        let consumer = self.consumer.as_ref().unwrap();
        let Some(guard) = consumer.borrow()? else {
            return Ok(false);
        };
        let header = RecordHeader::decode(&guard.bytes()[..128])?;
        let payload = if header.message_type == 259 {
            guard.depth_payload()?
        } else {
            &[]
        };
        let record = CatchupRecord {
            record: guard.bytes().to_vec(),
            payload: payload.to_vec(),
            instrument: header.instrument_id,
            applied_mono_ns: now,
        };
        retain_catchup(
            &mut self.catchup,
            &mut self.retained_bytes,
            self.bank.capacity as usize,
            record,
        )?;
        self.replica
            .as_mut()
            .unwrap()
            .apply(guard.bytes(), payload)?;
        let sequence = guard.sequence();
        drop(guard);
        consumer.apply(sequence)?;
        Ok(true)
    }
}
