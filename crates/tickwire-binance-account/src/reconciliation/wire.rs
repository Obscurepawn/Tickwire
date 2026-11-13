use super::*;
use std::sync::Mutex;
use tickwire_transport::dialer::DialBaton;

pub const DEFAULT_MAX_IDLE_CONNECTIONS: usize = 4;

struct IdleEntry {
    address: SocketAddr,
    authority: &'static str,
    connection: Connection,
}

struct PoolInner {
    idle: Vec<IdleEntry>,
    max_idle: usize,
    baton: Option<Arc<DialBaton>>,
    reused_connections: u64,
    baton_handoffs: u64,
    fresh_connects: u64,
}

/// Keep-alive HTTP/1.1 connection pool and optional [`DialBaton`] handoff for read-only REST queries.
#[derive(Clone)]
pub struct WireReadPool {
    inner: Arc<Mutex<PoolInner>>,
}

impl std::fmt::Debug for WireReadPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        f.debug_struct("WireReadPool")
            .field("idle_len", &guard.idle.len())
            .field("max_idle", &guard.max_idle)
            .field("has_baton", &guard.baton.is_some())
            .field("reused_connections", &guard.reused_connections)
            .field("baton_handoffs", &guard.baton_handoffs)
            .field("fresh_connects", &guard.fresh_connects)
            .finish()
    }
}

impl Default for WireReadPool {
    fn default() -> Self {
        Self::new()
    }
}

impl WireReadPool {
    pub fn new() -> Self {
        Self::with_max_idle(DEFAULT_MAX_IDLE_CONNECTIONS)
    }

    pub fn with_max_idle(max_idle: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(PoolInner {
                idle: Vec::with_capacity(max_idle.min(DEFAULT_MAX_IDLE_CONNECTIONS)),
                max_idle,
                baton: None,
                reused_connections: 0,
                baton_handoffs: 0,
                fresh_connects: 0,
            })),
        }
    }

    pub fn with_dial_baton(self, baton: Arc<DialBaton>) -> Self {
        self.set_dial_baton(Some(baton));
        self
    }

    pub fn set_dial_baton(&self, baton: Option<Arc<DialBaton>>) {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.baton = baton;
    }

    pub fn request_prewarm(&self, due_mono_ns: u64) -> bool {
        let baton = self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .baton
            .clone();
        baton.is_some_and(|b| b.want(due_mono_ns))
    }

    pub fn put_idle(&self, address: SocketAddr, authority: &'static str, connection: Connection) {
        if !connection.is_ready() {
            return;
        }
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if guard.max_idle == 0 {
            return;
        }
        guard.idle.retain(|e| e.connection.is_ready());
        if guard.idle.len() >= guard.max_idle {
            let evicted = guard.idle.remove(0);
            drop(evicted);
        }
        guard.idle.push(IdleEntry {
            address,
            authority,
            connection,
        });
    }

    pub fn idle_len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .idle
            .len()
    }

    pub fn reused_connections(&self) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .reused_connections
    }

    pub fn baton_handoffs(&self) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .baton_handoffs
    }

    pub fn fresh_connects(&self) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .fresh_connects
    }

    pub fn clear(&self) {
        let dropped = {
            let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut guard.idle)
        };
        drop(dropped);
    }

    pub(super) fn recycle(&self, config: &ReconcileConfig, wire: WireRead) {
        if let Some(conn) = wire.take_idle_connection() {
            self.put_idle(config.address, authority(&config.scope), conn);
        }
    }

    fn take_matching(&self, address: SocketAddr, authority: &'static str) -> Option<Connection> {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut idx = guard.idle.len();
        while idx > 0 {
            idx -= 1;
            if !guard.idle[idx].connection.is_ready() {
                let stale = guard.idle.remove(idx);
                drop(stale);
                continue;
            }
            if guard.idle[idx].address == address && guard.idle[idx].authority == authority {
                let entry = guard.idle.remove(idx);
                guard.reused_connections = guard.reused_connections.saturating_add(1);
                return Some(entry.connection);
            }
        }
        if let Some(baton) = &guard.baton
            && let Some(link) = baton.take_ready()
        {
            if link.peer == address && link.connection.is_ready() {
                guard.baton_handoffs = guard.baton_handoffs.saturating_add(1);
                return Some(link.connection);
            }
            drop(link);
        }
        None
    }

    fn note_fresh_connect(&self) {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.fresh_connects = guard.fresh_connects.saturating_add(1);
    }
}

fn connect_fresh(config: &ReconcileConfig, deadline: std::time::Instant) -> Result<Connection> {
    Connection::connect(
        config.address,
        Security::Tls {
            server_name: authority(&config.scope)
                .to_owned()
                .try_into()
                .map_err(|_| Error::Identity)?,
            config: config.tls.clone(),
        },
        config.limits,
        deadline,
    )
    .map_err(Error::Transport)
}

fn response_supports_keep_alive(response: &http::Response) -> bool {
    if response.status < 200 {
        return false;
    }
    for (name, value) in &response.headers {
        if name.eq_ignore_ascii_case("connection")
            && let Ok(text) = std::str::from_utf8(value)
            && text
                .split(',')
                .any(|tok| tok.trim().eq_ignore_ascii_case("close"))
        {
            return false;
        }
    }
    response.status == 204
        || response.status == 304
        || response.header("content-length").is_some()
        || response.header("transfer-encoding").is_some()
}

pub(super) struct WireRead {
    pub connection: Connection,
    decoder: ResponseDecoder,
    raw: Vec<u8>,
    request_bytes: Vec<u8>,
    reused: bool,
    reusable: bool,
    ended: bool,
    invalid: bool,
    stamp: SignedTime,
    deadline_mono: u64,
    deadline: std::time::Instant,
}
pub(super) struct WireResponse {
    pub raw: Vec<u8>,
    pub status: u16,
    pub body: Option<Vec<u8>>,
    pub received: u64,
}
impl WireRead {
    #[allow(dead_code)]
    pub fn new(
        config: &ReconcileConfig,
        stamp: SignedTime,
        started: ClockSample,
        bytes: &[u8],
    ) -> Result<Self> {
        let deadline_mono = started
            .monotonic_ns()
            .checked_add(config.request_timeout_ns)
            .ok_or(Error::Capacity)?;
        let deadline = started
            .instant
            .checked_add(Duration::from_nanos(config.request_timeout_ns))
            .ok_or(Error::Capacity)?;
        let mut connection = connect_fresh(config, deadline)?;
        connection
            .queue(
                PreparedSend::new(bytes, deadline, config.limits.max_send_bytes)
                    .map_err(Error::Transport)?,
            )
            .map_err(Error::Transport)?;
        Ok(Self {
            connection,
            decoder: ResponseDecoder::new(config.max_response_bytes, config.max_headers)
                .map_err(Error::Transport)?,
            raw: Vec::new(),
            request_bytes: Vec::new(),
            reused: false,
            reusable: false,
            ended: false,
            invalid: false,
            stamp,
            deadline_mono,
            deadline,
        })
    }

    pub fn reuse_or_connect(
        pool: &WireReadPool,
        config: &ReconcileConfig,
        stamp: SignedTime,
        started: ClockSample,
        bytes: &[u8],
    ) -> Result<Self> {
        let deadline_mono = started
            .monotonic_ns()
            .checked_add(config.request_timeout_ns)
            .ok_or(Error::Capacity)?;
        let deadline = started
            .instant
            .checked_add(Duration::from_nanos(config.request_timeout_ns))
            .ok_or(Error::Capacity)?;
        let decoder = ResponseDecoder::new(config.max_response_bytes, config.max_headers)
            .map_err(Error::Transport)?;
        if let Some(mut connection) = pool.take_matching(config.address, authority(&config.scope)) {
            let prepared = PreparedSend::new(bytes, deadline, config.limits.max_send_bytes)
                .map_err(Error::Transport)?;
            if connection.queue(prepared).is_ok() {
                return Ok(Self {
                    connection,
                    decoder,
                    raw: Vec::new(),
                    request_bytes: bytes.to_vec(),
                    reused: true,
                    reusable: false,
                    ended: false,
                    invalid: false,
                    stamp,
                    deadline_mono,
                    deadline,
                });
            }
        }
        let mut connection = connect_fresh(config, deadline)?;
        pool.note_fresh_connect();
        connection
            .queue(
                PreparedSend::new(bytes, deadline, config.limits.max_send_bytes)
                    .map_err(Error::Transport)?,
            )
            .map_err(Error::Transport)?;
        Ok(Self {
            connection,
            decoder,
            raw: Vec::new(),
            request_bytes: Vec::new(),
            reused: false,
            reusable: false,
            ended: false,
            invalid: false,
            stamp,
            deadline_mono,
            deadline,
        })
    }

    pub fn take_idle_connection(mut self) -> Option<Connection> {
        if !self.reusable || self.ended || self.invalid || !self.connection.is_ready() {
            return None;
        }
        match self.connection.take_finished() {
            Some(report) if report.state == tickwire_transport::SendState::Flushed => {
                Some(self.connection)
            }
            _ => None,
        }
    }

    pub fn poll(
        &mut self,
        config: &ReconcileConfig,
        clock: &mut impl Clock,
        last: &mut Option<ClockSample>,
        budget: IoBudget,
        mut permit: impl FnMut(&ClockSample) -> bool,
    ) -> Result<Option<WireResponse>> {
        if budget.max_bytes == 0 || budget.max_syscalls == 0 {
            return Err(Error::Capacity);
        }
        let now = clock.sample().map_err(Error::Clock)?;
        ordered(last, now)?;
        source(&config.scope, &self.stamp, &now)?;
        if !self.ended {
            match self.connection.drive(now.instant, budget) {
                Ok(report) => self.ended = report.peer_closed,
                Err(_) => self.ended = true,
            }
            if self.ended
                && self.reused
                && self.raw.is_empty()
                && now.monotonic_ns() < self.deadline_mono
                && now.instant < self.deadline
            {
                self.reused = false;
                let mut fresh_conn = connect_fresh(config, self.deadline)?;
                fresh_conn
                    .queue(
                        PreparedSend::new(
                            &self.request_bytes,
                            self.deadline,
                            config.limits.max_send_bytes,
                        )
                        .map_err(Error::Transport)?,
                    )
                    .map_err(Error::Transport)?;
                self.request_bytes.clear();
                self.connection = fresh_conn;
                self.ended = false;
                match self.connection.drive(now.instant, budget) {
                    Ok(report) => self.ended = report.peer_closed,
                    Err(_) => self.ended = true,
                }
            }
            if !self.ended {
                let stamp = self.stamp;
                let deadline_mono = self.deadline_mono;
                let deadline = self.deadline;
                let mut clock_error = None;
                let sent = self.connection.try_send(now.instant, || {
                    let result = (|| -> Result<bool> {
                        let first = clock.sample().map_err(Error::Clock)?;
                        ordered(last, first)?;
                        source(&config.scope, &stamp, &first)?;
                        clock
                            .check_signed(&config.scope, &stamp, &first, config.recv_window_micros)
                            .map_err(Error::Clock)?;
                        if !permit(&first) {
                            return Ok(false);
                        }
                        let second = clock.sample().map_err(Error::Clock)?;
                        ordered(last, second)?;
                        source(&config.scope, &stamp, &second)?;
                        clock
                            .check_signed(&config.scope, &stamp, &second, config.recv_window_micros)
                            .map_err(Error::Clock)?;
                        let final_sample = clock.sample().map_err(Error::Clock)?;
                        ordered(last, final_sample)?;
                        source(&config.scope, &stamp, &final_sample)?;
                        Ok(final_sample.monotonic_ns() < deadline_mono
                            && final_sample.instant < deadline
                            && fresh(stamp, &final_sample, config.recv_window_micros))
                    })();
                    match result {
                        Ok(allowed) => allowed,
                        Err(error) => {
                            clock_error = Some(error);
                            false
                        }
                    }
                });
                if sent.is_err() {
                    self.ended = true;
                }
                if let Some(error) = clock_error {
                    self.ended = true;
                    self.invalid = true;
                    return Err(error);
                }
            }
        }
        let mut bytes = [0u8; 4096];
        let mut drained = 0;
        while drained < budget.max_bytes {
            let limit = bytes.len().min(budget.max_bytes - drained);
            let n = self.connection.read(&mut bytes[..limit]);
            if n == 0 {
                break;
            }
            drained += n;
            if self
                .raw
                .len()
                .checked_add(n)
                .is_none_or(|n| n > config.max_response_bytes)
            {
                self.ended = true;
                self.invalid = true;
                break;
            }
            self.raw.extend_from_slice(&bytes[..n]);
            if self.decoder.push(&bytes[..n]).is_err() {
                self.ended = true;
                self.invalid = true;
                break;
            }
        }
        if drained == budget.max_bytes {
            return Ok(None);
        }
        if self.ended {
            self.decoder.finish();
        }
        let parsed = self.decoder.next_response();
        let report = self.connection.send_report();
        let complete =
            report.is_some_and(|r| r.accepted_bytes == r.total_bytes && r.total_bytes > 0);
        let terminal = report.is_some_and(|r| {
            matches!(
                r.state,
                tickwire_transport::SendState::NoBytes(_)
                    | tickwire_transport::SendState::Failed(_)
            )
        });
        if !(parsed.as_ref().is_ok_and(Option::is_some)
            || parsed.is_err()
            || self.ended
            || terminal
            || now.monotonic_ns() >= self.deadline_mono
            || now.instant >= self.deadline)
        {
            return Ok(None);
        }
        let response = parsed.ok().flatten();
        let status = response.as_ref().map_or(0, |r| r.status);
        let extra_empty = self.decoder.take_buffered().is_empty();
        let keep_alive_ok = response.as_ref().is_some_and(response_supports_keep_alive);
        let body = response
            .filter(|_| complete && !self.invalid && extra_empty)
            .map(|r| r.body);
        self.reusable = body.is_some() && keep_alive_ok && !self.ended && !self.invalid;
        Ok(Some(WireResponse {
            raw: std::mem::take(&mut self.raw),
            status,
            body,
            received: last.unwrap_or(now).monotonic_ns(),
        }))
    }
}
pub(super) fn ordered(last: &mut Option<ClockSample>, now: ClockSample) -> Result<()> {
    if last.is_some_and(|old| {
        now.monotonic.domain() != old.monotonic.domain()
            || now.monotonic_ns() < old.monotonic_ns()
            || now.instant < old.instant
    }) {
        return Err(Error::Clock(tickwire_binance_private::Error::ClockDomain));
    }
    *last = Some(now);
    Ok(())
}
