use crate::*;
use mio::{Interest, Registry, Token, net::TcpStream};
use std::{
    collections::VecDeque,
    io::{Read, Write},
    net::{Shutdown, SocketAddr},
};
const HARD_LIMIT: usize = 16 * 1024 * 1024;
struct SendSlot {
    prepared: PreparedSend,
    report: SendReport,
}
pub struct Connection {
    socket: Option<TcpStream>,
    tls: Option<rustls::ClientConnection>,
    connected: bool,
    limits: Limits,
    connect_deadline: Instant,
    rx: VecDeque<u8>,
    send: Option<SendSlot>,
    next_id: u64,
}
impl Connection {
    pub fn connect(
        address: SocketAddr,
        security: Security,
        limits: Limits,
        deadline: Instant,
    ) -> Result<Self> {
        if [
            limits.max_send_bytes,
            limits.max_receive_bytes,
            limits.tls_buffer_bytes,
            limits.max_accept_bytes,
        ]
        .iter()
        .any(|n| *n == 0 || *n > HARD_LIMIT)
        {
            return Err(Error::Bounds);
        }
        let tls = match security {
            Security::Plain => None,
            Security::Tls {
                server_name,
                config,
            } => {
                if config.enable_early_data {
                    return Err(Error::Protocol("early data disabled"));
                }
                let mut tls = rustls::ClientConnection::new(config, server_name)?;
                tls.set_buffer_limit(Some(limits.tls_buffer_bytes));
                Some(tls)
            }
        };
        let socket = TcpStream::connect(address)?;
        socket.set_nodelay(true)?;
        Ok(Self {
            socket: Some(socket),
            tls,
            connected: false,
            limits,
            connect_deadline: deadline,
            rx: VecDeque::with_capacity(limits.max_receive_bytes),
            send: None,
            next_id: 1,
        })
    }
    fn interest(&self) -> Interest {
        if !self.connected
            || self.tls.as_ref().is_some_and(|t| t.wants_write())
            || self.send.as_ref().is_some_and(|s| {
                matches!(s.report.state, SendState::Queued | SendState::MayHaveSent)
            })
        {
            Interest::READABLE.add(Interest::WRITABLE)
        } else {
            Interest::READABLE
        }
    }
    pub fn register(&mut self, registry: &Registry, token: Token) -> Result<()> {
        let interest = self.interest();
        registry.register(self.socket.as_mut().ok_or(Error::Closed)?, token, interest)?;
        Ok(())
    }
    pub fn reregister(&mut self, registry: &Registry, token: Token) -> Result<()> {
        let interest = self.interest();
        registry.reregister(self.socket.as_mut().ok_or(Error::Closed)?, token, interest)?;
        Ok(())
    }
    pub fn queue(&mut self, prepared: PreparedSend) -> Result<u64> {
        if self.socket.is_none() {
            return Err(Error::Closed);
        }
        if self.send.is_some() {
            return Err(Error::Busy);
        }
        if prepared.bytes.len() > self.limits.max_send_bytes {
            return Err(Error::Bounds);
        }
        let id = self.next_id;
        self.next_id = self.next_id.checked_add(1).ok_or(Error::Bounds)?;
        let total_bytes = prepared.bytes.len();
        self.send = Some(SendSlot {
            prepared,
            report: SendReport {
                id,
                state: SendState::Queued,
                accepted_bytes: 0,
                total_bytes,
            },
        });
        Ok(id)
    }
    pub fn is_ready(&self) -> bool {
        self.socket.is_some()
            && self.connected
            && self.tls.as_ref().is_none_or(|t| !t.is_handshaking())
    }
    pub fn send_report(&self) -> Option<SendReport> {
        self.send.as_ref().map(|s| s.report)
    }
    pub fn take_finished(&mut self) -> Option<SendReport> {
        if self.send.as_ref().is_some_and(|s| {
            matches!(
                s.report.state,
                SendState::Flushed | SendState::NoBytes(_) | SendState::Failed(_)
            )
        }) {
            self.send.take().map(|s| s.report)
        } else {
            None
        }
    }
    fn reject(&mut self, why: Failure) {
        if let Some(s) = self.send.as_mut()
            && matches!(s.report.state, SendState::Queued | SendState::MayHaveSent)
        {
            s.report.state = if s.report.accepted_bytes == 0 {
                SendState::NoBytes(why)
            } else {
                SendState::Failed(why)
            };
        }
    }
    fn fail(&mut self, why: Failure) {
        self.reject(why);
        if let Some(s) = self.socket.take() {
            let _ = s.shutdown(Shutdown::Both);
        }
        self.tls = None;
    }
    fn expire(&mut self, now: Instant) {
        if self.send.as_ref().is_some_and(|s| {
            now >= s.prepared.deadline
                && matches!(s.report.state, SendState::Queued | SendState::MayHaveSent)
        }) {
            if self.send.as_ref().unwrap().report.accepted_bytes > 0 {
                self.fail(Failure::Deadline)
            } else {
                self.reject(Failure::Deadline)
            }
        }
    }
    pub fn cancel(&mut self) -> Result<SendReport> {
        let accepted = self
            .send
            .as_ref()
            .ok_or(Error::Protocol("no send"))?
            .report
            .accepted_bytes;
        if accepted > 0 {
            self.fail(Failure::Cancelled)
        } else {
            self.reject(Failure::Cancelled)
        }
        Ok(self.send.as_ref().unwrap().report)
    }
    pub fn try_send(
        &mut self,
        now: Instant,
        authorize: impl FnOnce() -> bool,
    ) -> Result<SendReport> {
        self.expire(now.max(Instant::now()));
        let r = self.send_report().ok_or(Error::Protocol("no send"))?;
        if !matches!(r.state, SendState::Queued | SendState::MayHaveSent) || !self.is_ready() {
            return Ok(r);
        }
        if r.accepted_bytes == 0 {
            if !authorize() {
                self.reject(Failure::GateDenied);
                return Ok(self.send_report().unwrap());
            }
            // Authorization is consumed synchronously; expiry is checked again after it.
            self.expire(now.max(Instant::now()));
            if self.send_report().unwrap().state != SendState::Queued {
                return Ok(self.send_report().unwrap());
            }
        }
        let slot = self.send.as_mut().unwrap();
        if slot.report.accepted_bytes == slot.report.total_bytes {
            return Ok(slot.report);
        }
        let end = slot.report.total_bytes.min(
            slot.report
                .accepted_bytes
                .saturating_add(self.limits.max_accept_bytes),
        );
        let data = &slot.prepared.bytes[slot.report.accepted_bytes..end];
        let result = match self.tls.as_mut() {
            Some(t) => t.writer().write(data),
            None => self.socket.as_mut().unwrap().write(data),
        };
        match result {
            Ok(0) => {}
            Ok(n) => {
                slot.report.accepted_bytes += n;
                slot.report.state = SendState::MayHaveSent;
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) => {}
            Err(_) => self.fail(Failure::Io),
        }
        self.mark_flushed();
        Ok(self.send_report().unwrap())
    }
    fn mark_flushed(&mut self) {
        if self.socket.is_some()
            && self.tls.as_ref().is_none_or(|t| !t.wants_write())
            && let Some(s) = self.send.as_mut()
            && s.report.state == SendState::MayHaveSent
            && s.report.accepted_bytes == s.report.total_bytes
        {
            s.report.state = SendState::Flushed
        }
    }
    pub fn read(&mut self, out: &mut [u8]) -> usize {
        let n = out.len().min(self.rx.len());
        for b in &mut out[..n] {
            *b = self.rx.pop_front().unwrap();
        }
        n
    }
    fn drain_plaintext(&mut self, max_bytes: usize) -> Result<usize> {
        let mut n = 0;
        let Some(tls) = self.tls.as_mut() else {
            return Ok(0);
        };
        let mut buf = [0; 16384];
        while self.rx.len() < self.limits.max_receive_bytes && n < max_bytes {
            let cap = buf
                .len()
                .min(self.limits.max_receive_bytes - self.rx.len())
                .min(max_bytes - n);
            match tls.reader().read(&mut buf[..cap]) {
                Ok(0) => break,
                Ok(len) => {
                    self.rx.extend(&buf[..len]);
                    n += len
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(n)
    }
    pub fn drive(&mut self, now: Instant, budget: IoBudget) -> Result<DriveReport> {
        if budget.max_syscalls == 0 || budget.max_bytes == 0 {
            return Err(Error::Bounds);
        }
        self.expire(now.max(Instant::now()));
        if self.socket.is_none() {
            return Err(Error::Closed);
        }
        if !self.is_ready() && now.max(Instant::now()) >= self.connect_deadline {
            self.fail(Failure::Deadline);
            return Err(Error::Closed);
        }
        let mut report = DriveReport::default();
        if !self.connected {
            if budget.max_syscalls < 2 {
                return Ok(report);
            }
            report.syscalls += 1;
            match self.socket.as_ref().unwrap().take_error() {
                Ok(None) => {}
                Ok(Some(e)) | Err(e) => {
                    self.fail(Failure::Io);
                    return Err(e.into());
                }
            }
            report.syscalls += 1;
            match self.socket.as_ref().unwrap().peer_addr() {
                Ok(_) => self.connected = true,
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::NotConnected | io::ErrorKind::WouldBlock
                    ) =>
                {
                    return Ok(report);
                }
                Err(e) => {
                    self.fail(Failure::Io);
                    return Err(e.into());
                }
            }
        }
        match self.drain_plaintext(budget.max_bytes - report.received) {
            Ok(n) => report.received += n,
            Err(e) => {
                self.fail(Failure::Disconnected);
                return Err(e);
            }
        }
        while report.syscalls < budget.max_syscalls
            && report.network_read + report.network_written < budget.max_bytes
        {
            let before = (report.network_read, report.network_written);
            if let Some(tls) = self.tls.as_mut()
                && tls.wants_write()
            {
                let mut io = BudgetIo {
                    socket: self.socket.as_mut().unwrap(),
                    report: &mut report,
                    budget,
                };
                match tls.write_tls(&mut io) {
                    Ok(_) => {}
                    Err(e) if retry(&e) => {}
                    Err(e) => {
                        self.fail(Failure::Io);
                        return Err(e.into());
                    }
                }
            }
            // Record completed local output before a later receive-side EOF.
            self.mark_flushed();
            if self.rx.len() < self.limits.max_receive_bytes
                && report.received < budget.max_bytes
                && report.syscalls < budget.max_syscalls
                && report.network_read + report.network_written < budget.max_bytes
            {
                let result = if let Some(tls) = self.tls.as_mut() {
                    let mut io = BudgetIo {
                        socket: self.socket.as_mut().unwrap(),
                        report: &mut report,
                        budget,
                    };
                    tls.read_tls(&mut io)
                } else {
                    let mut buf = [0; 16384];
                    let cap = buf.len().min(self.limits.max_receive_bytes - self.rx.len());
                    let mut io = BudgetIo {
                        socket: self.socket.as_mut().unwrap(),
                        report: &mut report,
                        budget,
                    };
                    match io.read(&mut buf[..cap]) {
                        Ok(n) => {
                            self.rx.extend(&buf[..n]);
                            report.received += n;
                            Ok(n)
                        }
                        Err(e) => Err(e),
                    }
                };
                match result {
                    Ok(0) => report.peer_closed = true,
                    Ok(_) => {
                        if let Some(tls) = self.tls.as_mut()
                            && let Err(e) = tls.process_new_packets()
                        {
                            self.fail(Failure::Tls);
                            return Err(e.into());
                        }
                    }
                    Err(e) if retry(&e) => {}
                    Err(e) => {
                        self.fail(Failure::Io);
                        return Err(e.into());
                    }
                }
                match self.drain_plaintext(budget.max_bytes - report.received) {
                    Ok(n) => report.received += n,
                    Err(e) => {
                        self.fail(Failure::Disconnected);
                        return Err(e);
                    }
                }
            }
            if report.peer_closed {
                self.fail(Failure::Disconnected);
                break;
            }
            if before == (report.network_read, report.network_written) {
                break;
            }
        }
        self.mark_flushed();
        report.ready = self.is_ready();
        Ok(report)
    }
}
fn retry(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    )
}
struct BudgetIo<'a> {
    socket: &'a mut TcpStream,
    report: &'a mut DriveReport,
    budget: IoBudget,
}
impl BudgetIo<'_> {
    fn available(&self) -> io::Result<usize> {
        let n = self.budget.max_bytes - self.report.network_read - self.report.network_written;
        if n == 0 || self.report.syscalls >= self.budget.max_syscalls {
            Err(io::ErrorKind::WouldBlock.into())
        } else {
            Ok(n)
        }
    }
}
impl Read for BudgetIo<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let n = out.len().min(self.available()?);
        self.report.syscalls += 1;
        let n = self.socket.read(&mut out[..n])?;
        self.report.network_read += n;
        Ok(n)
    }
}
impl Write for BudgetIo<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let n = bytes.len().min(self.available()?);
        self.report.syscalls += 1;
        let n = self.socket.write(&bytes[..n])?;
        self.report.network_written += n;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
