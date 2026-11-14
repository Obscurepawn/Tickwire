//! Multi-slot execution connection pool and allocation-free EWMA route selection.
use crate::*;
use std::net::SocketAddr;
pub use tickwire_transport::order_router::{
    MAX_ORDER_CONNECTIONS, MAX_SHARED_INFLIGHT, OrderConnectionPool, OrderRoute, OrderRouter,
    OrderSlotSpec, RouteHealth, RouterConfig, SampleKind, SendSlot,
};
use tickwire_transport::{
    Limits,
    http::ResponseDecoder,
    ws::{ClientHandshake, Opcode, encode_client},
};

#[derive(Clone, Copy)]
struct SlotControl {
    token: u64,
    close: bool,
}

pub(crate) struct SlotWsState {
    handshake: Option<ClientHandshake>,
    response: ResponseDecoder,
    deadline: Instant,
    ready: bool,
    control: Option<SlotControl>,
}

#[derive(Default)]
pub(crate) struct SlotIoState {
    pub(crate) registered: bool,
    pub(crate) max_send_bytes: usize,
    pub(crate) max_receive_bytes: usize,
    pub(crate) retained_receive: VecDeque<u8>,
    pub(crate) pending_send: Option<(IntentKey, u64, u64)>,
    pub(crate) websocket: Option<SlotWsState>,
    pub(crate) rest_started: bool,
}

impl SlotIoState {
    #[inline]
    pub(crate) fn websocket_blocks_application(&self) -> bool {
        self.websocket
            .as_ref()
            .is_some_and(|s| !s.ready || s.control.is_some())
    }
}

pub(crate) struct GatewayPool {
    pub(crate) pool: OrderConnectionPool,
    pub(crate) slot_io: [SlotIoState; MAX_ORDER_CONNECTIONS],
    pub(crate) slot_bindings: BTreeMap<IntentKey, (u16, u64)>,
    pub(crate) last_conn_id: Option<u16>,
}

impl GatewayPool {
    pub(crate) fn new(pool: OrderConnectionPool) -> Self {
        Self {
            pool,
            slot_io: std::array::from_fn(|_| SlotIoState::default()),
            slot_bindings: BTreeMap::new(),
            last_conn_id: None,
        }
    }

    #[inline]
    pub(crate) fn has_any_pending(&self) -> bool {
        self.slot_io.iter().any(|io| io.pending_send.is_some())
    }

    #[inline]
    pub(crate) fn is_key_pending(&self, key: IntentKey) -> bool {
        self.slot_io
            .iter()
            .any(|io| io.pending_send.is_some_and(|(k, _, _)| k == key))
    }

    #[inline]
    pub(crate) fn has_any_retained_receive(&self) -> bool {
        self.slot_io
            .iter()
            .any(|io| !io.retained_receive.is_empty())
    }

    pub(crate) fn clear_pending_for_key(&mut self, key: IntentKey) {
        for (idx, io) in self.slot_io.iter_mut().enumerate() {
            if io.pending_send.is_some_and(|(k, _, _)| k == key)
                && let Some((_, _, epoch)) = io.pending_send.take()
            {
                let _ = self.pool.release_inflight(idx as u16, epoch);
            }
        }
    }

    pub(crate) fn halt_all(&mut self, now_mono_ns: u64) {
        for idx in 0..MAX_ORDER_CONNECTIONS {
            let conn_id = idx as u16;
            if let Some(connection) = self.pool.connection_mut(conn_id) {
                let io = &mut self.slot_io[idx];
                let mut bytes = [0u8; 4096];
                loop {
                    let available = io
                        .max_receive_bytes
                        .saturating_sub(io.retained_receive.len());
                    if available == 0 {
                        break;
                    }
                    let limit = available.min(bytes.len());
                    let n = connection.read(&mut bytes[..limit]);
                    if n == 0 {
                        break;
                    }
                    io.retained_receive.extend(&bytes[..n]);
                }
            }
            if self.slot_io[idx].registered {
                let _ = self.pool.disconnect_slot(conn_id, now_mono_ns, false);
            }
            self.slot_io[idx].pending_send = None;
            self.slot_io[idx].websocket = None;
            self.slot_io[idx].rest_started = false;
        }
    }

    fn acquire_and_queue(
        &mut self,
        conn_id: u16,
        key: IntentKey,
        send: PreparedSend,
    ) -> tickwire_transport::Result<(u64, u64)> {
        let epoch = self.pool.router_mut().acquire_inflight(conn_id)?;
        self.last_conn_id = Some(conn_id);
        let queue_res = match self.pool.connection_mut(conn_id) {
            Some(conn) => conn.queue(send),
            None => Err(tickwire_transport::Error::Closed),
        };
        match queue_res {
            Ok(token) => {
                let io = &mut self.slot_io[usize::from(conn_id)];
                if io.websocket.is_none() {
                    io.rest_started = true;
                }
                io.pending_send = Some((key, token, epoch));
                Ok((token, epoch))
            }
            Err(err) => {
                let _ = self.pool.release_inflight(conn_id, epoch);
                Err(err)
            }
        }
    }
}

pub(crate) fn validate_transport_limits(limits: Limits) -> Result<()> {
    if [
        limits.max_send_bytes,
        limits.max_receive_bytes,
        limits.tls_buffer_bytes,
        limits.max_accept_bytes,
    ]
    .iter()
    .any(|n| *n == 0 || *n > 16 * 1024 * 1024)
    {
        return Err(Error::Transport(tickwire_transport::Error::Bounds));
    }
    Ok(())
}

impl<S: Backend, A: Authority, E: Encoder> AccountGateway<S, A, E> {
    pub fn with_connection_pool(mut self, config: RouterConfig) -> Result<Self> {
        self.enable_connection_pool(config)?;
        Ok(self)
    }

    pub fn enable_connection_pool(&mut self, config: RouterConfig) -> Result<()> {
        if self.halted {
            return Err(Error::Halted);
        }
        if self.pool.is_some()
            || self.pending.is_some()
            || self.websocket.is_some()
            || !self.retained_receive.is_empty()
        {
            return Err(Error::Busy);
        }
        let pool = OrderConnectionPool::new(config)?;
        self.connection = None;
        self.rest_started = false;
        self.per_order_doubt_isolation = true;
        self.pool = Some(GatewayPool::new(pool));
        Ok(())
    }

    pub fn has_connection_pool(&self) -> bool {
        self.pool.is_some()
    }

    pub fn connection_pool(&self) -> Option<&OrderConnectionPool> {
        self.pool.as_ref().map(|p| &p.pool)
    }

    pub fn slot_route(&self, conn_id: u16) -> Option<&OrderRoute> {
        self.pool.as_ref()?.pool.route(conn_id)
    }

    pub fn has_slot_connection(&self, conn_id: u16) -> bool {
        self.pool
            .as_ref()
            .and_then(|p| p.pool.connection(conn_id))
            .is_some()
    }

    pub fn slot_transport_ready(&self, conn_id: u16) -> bool {
        self.pool
            .as_ref()
            .and_then(|p| p.pool.connection(conn_id))
            .is_some_and(Connection::is_ready)
    }

    pub fn slot_pending_intent(&self, conn_id: u16) -> Option<(IntentKey, u64)> {
        self.pool
            .as_ref()?
            .slot_io
            .get(usize::from(conn_id))?
            .pending_send
            .map(|(key, _, epoch)| (key, epoch))
    }

    pub fn intent_slot(&self, intent: IntentKey) -> Option<(u16, u64)> {
        self.pool
            .as_ref()
            .and_then(|p| p.slot_bindings.get(&intent).copied())
    }

    pub fn add_pool_slot(&mut self, spec: OrderSlotSpec, limits: Limits) -> Result<u16> {
        if self.halted {
            return Err(Error::Halted);
        }
        validate_transport_limits(limits)?;
        let gpool = self.pool.as_mut().ok_or(Error::Invalid)?;
        let conn_id = gpool.pool.add_slot(spec)?;
        gpool.slot_io[usize::from(conn_id)] = SlotIoState {
            registered: true,
            max_send_bytes: limits.max_send_bytes,
            max_receive_bytes: limits.max_receive_bytes,
            retained_receive: VecDeque::new(),
            pending_send: None,
            websocket: None,
            rest_started: false,
        };
        Ok(conn_id)
    }

    pub fn install_pool_connection(
        &mut self,
        conn_id: u16,
        connection: Connection,
        peer_addr: Option<SocketAddr>,
        limits: Limits,
    ) -> Result<u64> {
        if self.halted {
            return Err(Error::Halted);
        }
        validate_transport_limits(limits)?;
        let gpool = self.pool.as_mut().ok_or(Error::Invalid)?;
        let idx = usize::from(conn_id);
        if idx >= MAX_ORDER_CONNECTIONS || !gpool.slot_io[idx].registered {
            return Err(Error::Transport(tickwire_transport::Error::Bounds));
        }
        if gpool.slot_io[idx].pending_send.is_some()
            || !gpool.slot_io[idx].retained_receive.is_empty()
            || gpool.slot_io[idx]
                .websocket
                .as_ref()
                .is_some_and(|s| s.control.is_some())
        {
            return Err(Error::Busy);
        }
        let epoch = gpool
            .pool
            .install_connection(conn_id, connection, peer_addr)?;
        gpool.slot_io[idx].max_send_bytes = limits.max_send_bytes;
        gpool.slot_io[idx].max_receive_bytes = limits.max_receive_bytes;
        gpool.slot_io[idx].websocket = None;
        gpool.slot_io[idx].rest_started = false;
        Ok(epoch)
    }

    pub fn poll_pool_dials(&mut self, now_mono_ns: u64) -> usize {
        self.poll_pool_dials_filtered(now_mono_ns, |_| true)
    }

    pub fn poll_pool_dials_filtered(
        &mut self,
        now_mono_ns: u64,
        mut can_install: impl FnMut(u16) -> bool,
    ) -> usize {
        if now_mono_ns > 0 {
            self.last_mono_ns = self.last_mono_ns.max(now_mono_ns);
        }
        if self.halted {
            return 0;
        }
        let Some(gpool) = self.pool.as_mut() else {
            return 0;
        };
        let mut eligible = [false; MAX_ORDER_CONNECTIONS];
        for (idx, flag) in eligible.iter_mut().enumerate() {
            let cid = idx as u16;
            let io = &gpool.slot_io[idx];
            *flag = io.registered
                && gpool.pool.connection(cid).is_none()
                && io.pending_send.is_none()
                && io.retained_receive.is_empty()
                && io.websocket.as_ref().is_none_or(|s| s.control.is_none())
                && can_install(cid);
        }
        let installed = gpool
            .pool
            .poll_dials_filtered(now_mono_ns, |cid| eligible[usize::from(cid)]);
        if installed > 0 {
            for (idx, was_elig) in eligible.into_iter().enumerate() {
                if was_elig && gpool.pool.connection(idx as u16).is_some() {
                    gpool.slot_io[idx].websocket = None;
                    gpool.slot_io[idx].rest_started = false;
                }
            }
        }
        installed
    }

    pub fn pick_pool_slot(&mut self, sym_type: u32) -> Option<u16> {
        self.pick_pool_slot_filtered(sym_type, |_| true)
    }

    pub fn pick_pool_slot_filtered(
        &mut self,
        sym_type: u32,
        mut allow: impl FnMut(&OrderRoute) -> bool,
    ) -> Option<u16> {
        if self.halted {
            return None;
        }
        let gpool = self.pool.as_mut()?;
        let last_conn_id = gpool.last_conn_id;

        let mut first_eligible: Option<u16> = None;
        let mut next_after_last: Option<u16> = None;
        let mut best_measured: Option<(u16, u32)> = None;
        let mut last_measured_score: Option<u32> = None;

        for slot in gpool.pool.router().routes() {
            let Some(r) = slot.as_ref() else {
                continue;
            };
            let cid = r.conn_id;
            let idx = usize::from(cid);
            let io = &gpool.slot_io[idx];
            if !(io.registered
                && r.established
                && r.is_ready()
                && io.pending_send.is_none()
                && !io.websocket_blocks_application()
                && gpool.pool.connection(cid).is_some()
                && r.matches_product(sym_type)
                && allow(r))
            {
                continue;
            }
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
        let picked = match best_measured {
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
                    Some(last)
                } else {
                    Some(best_cid)
                }
            }
        };
        if picked.is_some() {
            gpool.last_conn_id = picked;
        }
        picked
    }

    pub fn submit_on_slot(
        &mut self,
        conn_id: u16,
        command: CanonicalCommand,
        evaluated_at: u64,
        deadline: Instant,
    ) -> Result<(Status, u64)> {
        let key = binding::intent(&command);
        if let Some(old) = self.entries.get(&key) {
            if old.command.bytes() == command.bytes() {
                let epoch = self
                    .pool
                    .as_ref()
                    .and_then(|p| p.slot_bindings.get(&key))
                    .map_or(0, |&(_, ep)| ep);
                return Ok((old.status, epoch));
            }
            self.halt();
            return Err(Error::Conflict);
        }
        if self.halted {
            return Err(Error::Halted);
        }
        let max_send_bytes = {
            let gpool = self.pool.as_ref().ok_or(Error::Invalid)?;
            let idx = usize::from(conn_id);
            if idx >= MAX_ORDER_CONNECTIONS || !gpool.slot_io[idx].registered {
                return Err(Error::Transport(tickwire_transport::Error::Bounds));
            }
            let io = &gpool.slot_io[idx];
            let Some(route) = gpool.pool.route(conn_id) else {
                return Err(Error::Transport(tickwire_transport::Error::Bounds));
            };
            if io.pending_send.is_some()
                || io.websocket_blocks_application()
                || !route.established
                || !route.is_ready()
                || gpool.pool.connection(conn_id).is_none()
            {
                return Err(Error::Busy);
            }
            io.max_send_bytes
        };
        let (send, initial_status) =
            self.admit_and_journal(key, command, evaluated_at, deadline, max_send_bytes)?;
        let acquire_res = self
            .pool
            .as_mut()
            .ok_or(Error::Halted)?
            .acquire_and_queue(conn_id, key, send);
        match acquire_res {
            Ok((_token, epoch)) => {
                if let Some(gpool) = self.pool.as_mut() {
                    gpool.slot_bindings.insert(key, (conn_id, epoch));
                }
                Ok((initial_status, epoch))
            }
            Err(error) => {
                // queue never accepts application bytes, even when it fails.
                self.abort(key)?;
                let _ = self.drop_broken_slot_isolated(
                    conn_id,
                    evaluated_at.max(self.last_mono_ns),
                    true,
                );
                Err(error.into())
            }
        }
    }

    pub fn poll_slot(&mut self, conn_id: u16, now: Instant, budget: IoBudget) -> Result<()> {
        if self.halted {
            return Err(Error::Halted);
        }
        let idx = usize::from(conn_id);
        {
            let gpool = self.pool.as_ref().ok_or(Error::Invalid)?;
            if idx >= MAX_ORDER_CONNECTIONS || !gpool.slot_io[idx].registered {
                return Err(Error::Transport(tickwire_transport::Error::Bounds));
            }
            if gpool.slot_io[idx].websocket_blocks_application() {
                return Err(Error::Busy);
            }
        }
        let driven = self
            .pool
            .as_mut()
            .unwrap()
            .pool
            .connection_mut(conn_id)
            .ok_or(Error::Busy)?
            .drive(now, budget);
        if self
            .pool
            .as_ref()
            .is_some_and(|p| p.slot_io[idx].pending_send.is_some())
        {
            let report = self
                .pool
                .as_ref()
                .and_then(|p| p.pool.connection(conn_id))
                .and_then(Connection::send_report)
                .ok_or(Error::Invalid)?;
            self.apply_slot_report(conn_id, report)?;
        }
        let pending_key = self
            .pool
            .as_ref()
            .and_then(|p| p.slot_io[idx].pending_send.map(|(k, _, _)| k));
        if let Some(key) = pending_key
            && !self.halted
            && self.has_slot_connection(conn_id)
            && driven.as_ref().is_ok_and(|r| !r.peer_closed)
        {
            let entry = &self.entries[&key];
            if entry.output.is_some_and(|p| p.accepted_bytes == 0) {
                let committed = entry.committed.as_ref().ok_or(Error::Invalid)?;
                match self
                    .authority
                    .commit_state(&committed.admission, &committed.receipt)
                {
                    Ok(CommitState::Pending) => {
                        self.entries.get_mut(&key).unwrap().status = Status::AwaitingCommit;
                        return match driven {
                            Ok(_) => Ok(()),
                            Err(error) => {
                                let _ = self.drop_broken_slot_isolated(
                                    conn_id,
                                    self.last_mono_ns,
                                    true,
                                );
                                Err(error.into())
                            }
                        };
                    }
                    Ok(CommitState::Ready) => {
                        self.entries.get_mut(&key).unwrap().status = Status::Queued;
                    }
                    Err(_) => {
                        self.cancel_slot_pending(conn_id)?;
                        return Ok(());
                    }
                }
            }
            let entry = &self.entries[&key];
            let admission = &entry.committed.as_ref().ok_or(Error::Invalid)?.admission;
            let authority = &mut self.authority;
            let account = &self.account;
            let epoch = self.config.writer_epoch;
            let mut sampled_now = 0u64;
            let conn = self
                .pool
                .as_mut()
                .and_then(|p| p.pool.connection_mut(conn_id))
                .ok_or(Error::Busy)?;
            let result = conn.try_send(now, || {
                authority
                    .current_gate(&entry.command, admission)
                    .is_ok_and(|gate| {
                        sampled_now = gate.now;
                        binding::permits(gate, entry, epoch)
                            && account.check_admission(admission, gate.now).is_ok()
                    })
            });
            if sampled_now > 0 {
                self.last_mono_ns = self.last_mono_ns.max(sampled_now);
            }
            match result {
                Ok(report) => self.apply_slot_report(conn_id, report)?,
                Err(error) => {
                    let _ = self.drop_broken_slot_isolated(conn_id, self.last_mono_ns, true);
                    return Err(error.into());
                }
            }
        }
        match driven {
            Ok(report) if !report.peer_closed => Ok(()),
            Ok(_) => {
                let _ = self.drop_broken_slot_isolated(conn_id, self.last_mono_ns, true);
                Ok(())
            }
            Err(error) => {
                let _ = self.drop_broken_slot_isolated(conn_id, self.last_mono_ns, true);
                Err(error.into())
            }
        }
    }

    pub fn cancel_slot_pending(&mut self, conn_id: u16) -> Result<()> {
        if self.halted {
            return Err(Error::Halted);
        }
        let idx = usize::from(conn_id);
        {
            let gpool = self.pool.as_ref().ok_or(Error::Invalid)?;
            if idx >= MAX_ORDER_CONNECTIONS || gpool.slot_io[idx].pending_send.is_none() {
                return Err(Error::Invalid);
            }
        }
        let cancel_res = self
            .pool
            .as_mut()
            .and_then(|p| p.pool.connection_mut(conn_id))
            .ok_or(Error::Busy)?
            .cancel();
        let report = match cancel_res {
            Ok(report) => report,
            Err(error) => {
                let _ = self.drop_broken_slot_isolated(conn_id, self.last_mono_ns, true);
                return Err(error.into());
            }
        };
        self.apply_slot_report(conn_id, report)
    }

    fn apply_slot_report(&mut self, conn_id: u16, report: SendReport) -> Result<()> {
        let idx = usize::from(conn_id);
        let (key, token, epoch) = self
            .pool
            .as_ref()
            .and_then(|p| p.slot_io.get(idx))
            .and_then(|io| io.pending_send)
            .ok_or(Error::Invalid)?;
        if token != report.id {
            self.halt();
            return Err(Error::Identity);
        }
        if let Some(output) = self.entries.get_mut(&key).and_then(|e| e.output.as_mut()) {
            if report.accepted_bytes < output.accepted_bytes
                || report.accepted_bytes > output.total_bytes
            {
                self.halt();
                return Err(Error::Invalid);
            }
            output.accepted_bytes = report.accepted_bytes;
        }
        match report.state {
            SendState::Queued => {}
            SendState::MayHaveSent => {
                self.entries.get_mut(&key).unwrap().status = Status::PartiallyAccepted;
            }
            SendState::Flushed => {
                self.entries.get_mut(&key).unwrap().status = Status::Flushed;
                let gpool = self.pool.as_mut().unwrap();
                if let Some(conn) = gpool.pool.connection_mut(conn_id) {
                    conn.take_finished();
                }
                gpool.slot_io[idx].pending_send = None;
                let stamp = self.last_mono_ns.max(self.entries[&key].evaluated_at);
                self.account.set_management_retired(key, Some(stamp));
                self.refresh_completed_entries();
            }
            SendState::NoBytes(_) => {
                if report.accepted_bytes != 0 {
                    self.halt();
                    return Err(Error::Invalid);
                }
                {
                    let gpool = self.pool.as_mut().unwrap();
                    if let Some(conn) = gpool.pool.connection_mut(conn_id) {
                        conn.take_finished();
                    }
                    gpool.slot_io[idx].pending_send = None;
                    let _ = gpool.pool.release_inflight(conn_id, epoch);
                }
                self.abort(key)?;
            }
            SendState::Failed(_) => {
                let _ = self.drop_broken_slot_isolated(conn_id, self.last_mono_ns, true);
            }
        }
        Ok(())
    }

    pub fn read_slot(&mut self, conn_id: u16, out: &mut [u8]) -> usize {
        let Some(gpool) = self.pool.as_mut() else {
            return 0;
        };
        let idx = usize::from(conn_id);
        if idx >= MAX_ORDER_CONNECTIONS || !gpool.slot_io[idx].registered {
            return 0;
        }
        let retained = out.len().min(gpool.slot_io[idx].retained_receive.len());
        for slot in &mut out[..retained] {
            *slot = gpool.slot_io[idx].retained_receive.pop_front().unwrap();
        }
        retained
            + gpool
                .pool
                .connection_mut(conn_id)
                .map_or(0, |c| c.read(&mut out[retained..]))
    }

    pub fn drain_slot_retained_receive(&mut self, conn_id: u16) {
        if let Some(gpool) = self.pool.as_mut()
            && let Some(io) = gpool.slot_io.get_mut(usize::from(conn_id))
        {
            io.retained_receive.clear();
        }
    }

    pub fn release_slot_inflight(&mut self, conn_id: u16, epoch: u64) -> Result<bool> {
        let gpool = self.pool.as_mut().ok_or(Error::Invalid)?;
        Ok(gpool.pool.release_inflight(conn_id, epoch)?)
    }

    pub fn note_slot_sample(
        &mut self,
        conn_id: u16,
        epoch: u64,
        kind: SampleKind,
        sample_us: u32,
    ) -> Result<bool> {
        let gpool = self.pool.as_mut().ok_or(Error::Invalid)?;
        Ok(gpool.pool.note_sample(conn_id, epoch, kind, sample_us)?)
    }

    pub fn drop_broken_slot_isolated(
        &mut self,
        conn_id: u16,
        now_mono_ns: u64,
        schedule_redial: bool,
    ) -> Option<IntentKey> {
        if now_mono_ns > 0 {
            self.last_mono_ns = self.last_mono_ns.max(now_mono_ns);
        }
        let effective_now = self.last_mono_ns;
        let idx = usize::from(conn_id);
        let failed_pending = {
            let gpool = self.pool.as_mut()?;
            if idx >= MAX_ORDER_CONNECTIONS || !gpool.slot_io[idx].registered {
                return None;
            }
            if let Some(connection) = gpool.pool.connection_mut(conn_id) {
                let io = &mut gpool.slot_io[idx];
                let mut bytes = [0u8; 4096];
                loop {
                    let available = io
                        .max_receive_bytes
                        .saturating_sub(io.retained_receive.len());
                    if available == 0 {
                        break;
                    }
                    let limit = available.min(bytes.len());
                    let n = connection.read(&mut bytes[..limit]);
                    if n == 0 {
                        break;
                    }
                    io.retained_receive.extend(&bytes[..n]);
                }
            }
            let was_connected = gpool.pool.connection(conn_id).is_some()
                || gpool.pool.route(conn_id).is_some_and(|r| r.established);
            if was_connected {
                let _ = gpool
                    .pool
                    .disconnect_slot(conn_id, effective_now, schedule_redial);
            }
            gpool.slot_io[idx].websocket = None;
            gpool.slot_io[idx].rest_started = false;
            gpool.slot_io[idx].pending_send.take().map(|(k, _, _)| k)
        };
        if let Some(key) = failed_pending {
            let zero_bytes_unsent = self.entries.get(&key).is_some_and(|e| {
                matches!(e.status, Status::Queued | Status::AwaitingCommit)
                    && e.output.is_some_and(|p| p.accepted_bytes == 0)
            });
            if zero_bytes_unsent {
                if self.abort(key).is_err() {
                    self.halt();
                }
            } else {
                let reason = match self.entries.get(&key).map(|e| &e.command.command().body) {
                    Some(tickwire_venue::CommandBody::Cancel(_)) => {
                        tickwire_account::DoubtReason::CancelAmbiguous
                    }
                    Some(tickwire_venue::CommandBody::AmendKeepPriority { .. }) => {
                        tickwire_account::DoubtReason::AmendAmbiguous
                    }
                    _ => tickwire_account::DoubtReason::PlaceAmbiguous,
                };
                if self.isolate_intent_in_doubt(key, reason).is_err() {
                    self.halt();
                }
            }
        }
        failed_pending
    }

    pub fn register_slot(
        &mut self,
        conn_id: u16,
        registry: &tickwire_transport::mio::Registry,
        token: tickwire_transport::mio::Token,
    ) -> Result<()> {
        if self.halted {
            return Err(Error::Halted);
        }
        let gpool = self.pool.as_mut().ok_or(Error::Invalid)?;
        let idx = usize::from(conn_id);
        if idx >= MAX_ORDER_CONNECTIONS || !gpool.slot_io[idx].registered {
            return Err(Error::Transport(tickwire_transport::Error::Bounds));
        }
        Ok(gpool
            .pool
            .connection_mut(conn_id)
            .ok_or(Error::Busy)?
            .register(registry, token)?)
    }

    pub fn reregister_slot(
        &mut self,
        conn_id: u16,
        registry: &tickwire_transport::mio::Registry,
        token: tickwire_transport::mio::Token,
    ) -> Result<()> {
        if self.halted {
            return Err(Error::Halted);
        }
        let gpool = self.pool.as_mut().ok_or(Error::Invalid)?;
        let idx = usize::from(conn_id);
        if idx >= MAX_ORDER_CONNECTIONS || !gpool.slot_io[idx].registered {
            return Err(Error::Transport(tickwire_transport::Error::Bounds));
        }
        Ok(gpool
            .pool
            .connection_mut(conn_id)
            .ok_or(Error::Busy)?
            .reregister(registry, token)?)
    }

    pub fn deregister_slot(
        &mut self,
        conn_id: u16,
        registry: &tickwire_transport::mio::Registry,
    ) -> Result<()> {
        let gpool = self.pool.as_mut().ok_or(Error::Invalid)?;
        let idx = usize::from(conn_id);
        if idx >= MAX_ORDER_CONNECTIONS || !gpool.slot_io[idx].registered {
            return Err(Error::Transport(tickwire_transport::Error::Bounds));
        }
        Ok(gpool
            .pool
            .connection_mut(conn_id)
            .ok_or(Error::Busy)?
            .deregister(registry)?)
    }

    pub fn begin_slot_websocket(&mut self, conn_id: u16, deadline: Instant) -> Result<()> {
        if self.halted {
            return Err(Error::Halted);
        }
        let path = match self.config.route.product {
            tickwire_venue::Product::Spot => "/ws-api/v3",
            tickwire_venue::Product::LinearPerpetual | tickwire_venue::Product::LinearFuture => {
                "/ws-fapi/v1"
            }
            _ => return Err(Error::Identity),
        };
        let authority = &self.config.route.authority;
        let gpool = self.pool.as_mut().ok_or(Error::Invalid)?;
        let idx = usize::from(conn_id);
        if idx >= MAX_ORDER_CONNECTIONS || !gpool.slot_io[idx].registered {
            return Err(Error::Transport(tickwire_transport::Error::Bounds));
        }
        let io = &gpool.slot_io[idx];
        if io.rest_started
            || io.websocket.is_some()
            || io.pending_send.is_some()
            || gpool.pool.connection(conn_id).is_none()
        {
            return Err(Error::Busy);
        }
        let max_send_bytes = io.max_send_bytes;
        let handshake = ClientHandshake::new(authority, path, &[], max_send_bytes)?;
        let send = PreparedSend::new(handshake.request(), deadline, max_send_bytes)?;
        let token = gpool
            .pool
            .connection_mut(conn_id)
            .ok_or(Error::Busy)?
            .queue(send)?;
        gpool.slot_io[idx].websocket = Some(SlotWsState {
            handshake: Some(handshake),
            response: ResponseDecoder::new(32_768, 64)?,
            deadline,
            ready: false,
            control: Some(SlotControl {
                token,
                close: false,
            }),
        });
        Ok(())
    }

    pub fn slot_websocket_ready(&self, conn_id: u16) -> bool {
        !self.halted
            && self.pool.as_ref().is_some_and(|p| {
                let idx = usize::from(conn_id);
                idx < MAX_ORDER_CONNECTIONS
                    && p.pool.connection(conn_id).is_some()
                    && p.slot_io[idx].websocket.as_ref().is_some_and(|s| s.ready)
            })
    }

    pub fn slot_websocket_control_pending(&self, conn_id: u16) -> bool {
        self.pool
            .as_ref()
            .and_then(|p| p.slot_io.get(usize::from(conn_id)))
            .and_then(|io| io.websocket.as_ref())
            .is_some_and(|s| s.control.is_some())
    }

    pub fn queue_slot_websocket_control(
        &mut self,
        conn_id: u16,
        control: websocket::WebSocketControl<'_>,
        deadline: Instant,
    ) -> Result<()> {
        if !self.slot_websocket_ready(conn_id) {
            return Err(Error::Halted);
        }
        if self.slot_pending_intent(conn_id).is_some()
            || self.slot_websocket_control_pending(conn_id)
        {
            return Err(Error::Busy);
        }
        let idx = usize::from(conn_id);
        let gpool = self.pool.as_mut().ok_or(Error::Invalid)?;
        let max_send_bytes = gpool.slot_io[idx].max_send_bytes;
        let (opcode, payload) = match control {
            websocket::WebSocketControl::Ping(p) => (Opcode::Ping, p),
            websocket::WebSocketControl::Pong(p) => (Opcode::Pong, p),
            websocket::WebSocketControl::Close(p) => (Opcode::Close, p),
        };
        let bytes = encode_client(opcode, payload, max_send_bytes)?;
        let token = gpool
            .pool
            .connection_mut(conn_id)
            .ok_or(Error::Busy)?
            .queue(PreparedSend::new(&bytes, deadline, max_send_bytes)?)?;
        gpool.slot_io[idx].websocket.as_mut().unwrap().control = Some(SlotControl {
            token,
            close: opcode == Opcode::Close,
        });
        Ok(())
    }

    pub fn poll_slot_websocket(
        &mut self,
        conn_id: u16,
        now: Instant,
        budget: IoBudget,
        mut permit: impl FnMut() -> bool,
    ) -> Result<()> {
        if self.halted {
            return Err(Error::Halted);
        }
        let idx = usize::from(conn_id);
        let blocks = {
            let gpool = self.pool.as_ref().ok_or(Error::Invalid)?;
            if idx >= MAX_ORDER_CONNECTIONS || gpool.slot_io[idx].websocket.is_none() {
                return Err(Error::Invalid);
            }
            gpool.slot_io[idx].websocket_blocks_application()
        };
        if !blocks {
            return self.poll_slot(conn_id, now, budget);
        }
        let result = self.poll_slot_protocol(conn_id, now, budget, &mut permit);
        if result.is_err() {
            let _ = self.drop_broken_slot_isolated(conn_id, self.last_mono_ns, true);
        }
        result
    }

    fn poll_slot_protocol(
        &mut self,
        conn_id: u16,
        now: Instant,
        budget: IoBudget,
        permit: &mut impl FnMut() -> bool,
    ) -> Result<()> {
        let idx = usize::from(conn_id);
        let driven = self
            .pool
            .as_mut()
            .and_then(|p| p.pool.connection_mut(conn_id))
            .ok_or(Error::Busy)?
            .drive(now, budget)?;
        if driven.peer_closed {
            let _ = self.drop_broken_slot_isolated(conn_id, self.last_mono_ns, true);
            return Ok(());
        }
        self.settle_slot_control(conn_id)?;
        if self.halted || !self.has_slot_connection(conn_id) {
            return Ok(());
        }
        if self.slot_websocket_control_pending(conn_id) {
            let report = self
                .pool
                .as_mut()
                .and_then(|p| p.pool.connection_mut(conn_id))
                .ok_or(Error::Busy)?
                .try_send(now, permit)?;
            self.check_slot_control_report(conn_id, report)?;
        }
        if self.halted || !self.has_slot_connection(conn_id) {
            return Ok(());
        }
        let gpool = self.pool.as_mut().ok_or(Error::Invalid)?;
        if !gpool.slot_io[idx].websocket.as_ref().unwrap().ready {
            if now >= gpool.slot_io[idx].websocket.as_ref().unwrap().deadline {
                return Err(Error::Transport(tickwire_transport::Error::Protocol(
                    "websocket upgrade deadline",
                )));
            }
            let mut bytes = [0u8; 4096];
            let count = gpool
                .pool
                .connection_mut(conn_id)
                .ok_or(Error::Busy)?
                .read(&mut bytes);
            let io = &mut gpool.slot_io[idx];
            let ws = io.websocket.as_mut().unwrap();
            if count > 0 {
                ws.response.push(&bytes[..count])?;
            }
            if let Some(response) = ws.response.next_response()? {
                ws.handshake
                    .as_ref()
                    .ok_or(Error::Invalid)?
                    .validate(&response)?;
                let tail = ws.response.take_buffered();
                if io.retained_receive.len() + tail.len() > io.max_receive_bytes {
                    return Err(Error::Capacity);
                }
                io.retained_receive.extend(tail);
                ws.handshake = None;
                ws.ready = true;
            }
        }
        Ok(())
    }

    fn settle_slot_control(&mut self, conn_id: u16) -> Result<()> {
        if !self.slot_websocket_control_pending(conn_id) {
            return Ok(());
        }
        let report = self
            .pool
            .as_ref()
            .and_then(|p| p.pool.connection(conn_id))
            .and_then(Connection::send_report)
            .ok_or(Error::Invalid)?;
        self.check_slot_control_report(conn_id, report)
    }

    fn check_slot_control_report(&mut self, conn_id: u16, report: SendReport) -> Result<()> {
        let idx = usize::from(conn_id);
        let slot = self
            .pool
            .as_ref()
            .and_then(|p| p.slot_io.get(idx))
            .and_then(|io| io.websocket.as_ref())
            .and_then(|s| s.control)
            .ok_or(Error::Invalid)?;
        if slot.token != report.id {
            return Err(Error::Identity);
        }
        match report.state {
            SendState::Queued | SendState::MayHaveSent => {}
            SendState::Flushed => {
                let gpool = self.pool.as_mut().unwrap();
                if let Some(conn) = gpool.pool.connection_mut(conn_id) {
                    conn.take_finished();
                }
                if let Some(ws) = gpool.slot_io[idx].websocket.as_mut() {
                    ws.control = None;
                }
                if slot.close {
                    let _ = self.drop_broken_slot_isolated(conn_id, self.last_mono_ns, false);
                }
            }
            SendState::NoBytes(_) | SendState::Failed(_) => {
                let _ = self.drop_broken_slot_isolated(conn_id, self.last_mono_ns, true);
            }
        }
        Ok(())
    }
}
