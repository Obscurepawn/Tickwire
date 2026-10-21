//! Bounded connection workers hand authenticated requests to the single owner.
use crate::{
    Error, ReplicaControl, Result, api,
    auth::{AuthenticatedPrincipal, ServerTls},
    request::{PendingRequest, Response},
};
use std::{
    collections::VecDeque,
    io,
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tickwire_journal::store::Store;

#[derive(Debug, Clone)]
pub struct Limits {
    pub max_connections: usize,
    pub max_body_bytes: usize,
    pub timeout: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_connections: 8,
            max_body_bytes: 4 * 1024 * 1024,
            timeout: Duration::from_secs(5),
        }
    }
}
#[derive(Debug, Default, Clone, Copy)]
pub struct Status {
    pub accepted: u64,
    pub completed: u64,
    pub failed: u64,
    pub connections: usize,
    pub pending: usize,
}
struct Worker {
    socket: TcpStream,
    live: Arc<AtomicBool>,
    task: JoinHandle<Result<()>>,
}
struct RequestWork {
    principal: AuthenticatedPrincipal,
    request: api::Request,
    response: SyncSender<Response>,
    live: Arc<AtomicBool>,
    deadline: Instant,
}
struct PendingWork {
    pending: PendingRequest,
    response: SyncSender<Response>,
    live: Arc<AtomicBool>,
    deadline: Instant,
}
pub struct Server {
    listener: TcpListener,
    trust: ServerTls,
    limits: Limits,
    sender: SyncSender<RequestWork>,
    receiver: Receiver<RequestWork>,
    workers: Vec<Worker>,
    pending: VecDeque<PendingWork>,
    status: Status,
    stopped: bool,
}
impl Server {
    pub fn bind(address: SocketAddr, trust: ServerTls, limits: Limits) -> Result<Self> {
        if !(1..=64).contains(&limits.max_connections)
            || !(1..=api::MAX_BODY_BYTES).contains(&limits.max_body_bytes)
            || limits.timeout.is_zero()
            || limits.timeout > Duration::from_secs(60)
        {
            return Err(Error::Bounds);
        }
        let listener = TcpListener::bind(address).map_err(|_| Error::Transport)?;
        listener
            .set_nonblocking(true)
            .map_err(|_| Error::Transport)?;
        let (sender, receiver) = mpsc::sync_channel(limits.max_connections);
        Ok(Self {
            listener,
            trust,
            limits,
            sender,
            receiver,
            workers: Vec::new(),
            pending: VecDeque::new(),
            status: Status::default(),
            stopped: false,
        })
    }
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.listener.local_addr().map_err(|_| Error::Transport)
    }
    pub fn poll<S: Store>(
        &mut self,
        control: &mut ReplicaControl<'_, S>,
        budget: usize,
    ) -> Result<Status> {
        if !(1..=64).contains(&budget) {
            return Err(Error::Bounds);
        }
        self.reap();
        if !self.stopped {
            for _ in 0..budget {
                if self.workers.len() >= self.limits.max_connections {
                    break;
                }
                match self.listener.accept() {
                    Ok((socket, _)) => self.spawn(socket)?,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(_) => return Err(Error::Transport),
                }
            }
        }
        for _ in 0..budget {
            let Ok(work) = self.receiver.try_recv() else {
                break;
            };
            if self.stopped || !work.live.load(Ordering::Acquire) || Instant::now() >= work.deadline
            {
                continue;
            }
            match control.begin_request(&self.trust, &work.principal, work.request) {
                Ok(pending) => self.pending.push_back(PendingWork {
                    pending,
                    response: work.response,
                    live: work.live,
                    deadline: work.deadline,
                }),
                Err(error) => {
                    let (status, code) = service_error(error)?;
                    let response = Response::error_for(status, code, &work.principal)?;
                    let _ = work.response.try_send(response);
                    self.status.completed = self.status.completed.saturating_add(1);
                }
            }
        }
        for _ in 0..budget.min(self.pending.len()) {
            let mut work = self.pending.pop_front().ok_or(Error::Protocol)?;
            if self.stopped || !work.live.load(Ordering::Acquire) || Instant::now() >= work.deadline
            {
                control.cancel_request(&mut work.pending)?;
                continue;
            }
            match control.poll_request(&self.trust, &mut work.pending) {
                Ok(Some(response)) => {
                    let _ = work.response.try_send(response);
                    self.status.completed = self.status.completed.saturating_add(1);
                }
                Ok(None) => self.pending.push_back(work),
                Err(Error::Consensus(
                    tickwire_control_raft::Error::NotLeader
                    | tickwire_control_raft::Error::NotReady
                    | tickwire_control_raft::Error::Bounds,
                )) => {
                    control.cancel_request(&mut work.pending)?;
                    let _ = work
                        .response
                        .try_send(Response::unavailable(&work.pending)?);
                    self.status.completed = self.status.completed.saturating_add(1);
                }
                Err(error) => {
                    // Fail the connection and release the exact ticket. A
                    // consensus/owner failure is returned to the main loop.
                    work.live.store(false, Ordering::Release);
                    let _ = control.cancel_request(&mut work.pending);
                    return Err(error);
                }
            }
        }
        self.status.connections = self.workers.len();
        self.status.pending = self.pending.len();
        Ok(self.status)
    }
    pub fn replace_trust(&mut self, trust: ServerTls) -> Result<()> {
        if self.stopped {
            return Err(Error::NotReady);
        }
        self.trust = trust;
        Ok(())
    }
    pub fn shutdown<S: Store>(&mut self, control: &mut ReplicaControl<'_, S>) -> Result<()> {
        self.close_sockets();
        let mut error = None;
        for mut work in self.pending.drain(..) {
            if let Err(value) = control.cancel_request(&mut work.pending) {
                error.get_or_insert(value);
            }
        }
        while self.receiver.try_recv().is_ok() {}
        self.reap();
        self.status.connections = self.workers.len();
        self.status.pending = 0;
        error.map_or(Ok(()), Err)
    }
    fn spawn(&mut self, socket: TcpStream) -> Result<()> {
        let cancellation = socket.try_clone().map_err(|_| Error::Transport)?;
        let trust = self.trust.clone();
        let limits = self.limits.clone();
        let sender = self.sender.clone();
        let live = Arc::new(AtomicBool::new(true));
        let worker_live = Arc::clone(&live);
        let deadline = Instant::now() + limits.timeout;
        let task = thread::Builder::new()
            .name("tickwire-config-https".to_owned())
            .spawn(move || {
                let _lifetime = ConnectionLife(Arc::clone(&worker_live));
                connection(socket, trust, limits, sender, worker_live, deadline)
            })
            .map_err(|_| Error::Transport)?;
        self.workers.push(Worker {
            socket: cancellation,
            live,
            task,
        });
        self.status.accepted = self.status.accepted.saturating_add(1);
        Ok(())
    }
    fn reap(&mut self) {
        let mut index = 0;
        while index < self.workers.len() {
            if self.workers[index].task.is_finished() {
                let worker = self.workers.swap_remove(index);
                if !matches!(worker.task.join(), Ok(Ok(()))) {
                    self.status.failed = self.status.failed.saturating_add(1);
                }
            } else {
                index += 1;
            }
        }
    }
    fn close_sockets(&mut self) {
        self.stopped = true;
        for worker in &self.workers {
            worker.live.store(false, Ordering::Release);
            let _ = worker.socket.shutdown(Shutdown::Both);
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.close_sockets();
    }
}
struct ConnectionLife(Arc<AtomicBool>);
impl Drop for ConnectionLife {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
fn connection(
    socket: TcpStream,
    trust: ServerTls,
    limits: Limits,
    sender: SyncSender<RequestWork>,
    live: Arc<AtomicBool>,
    deadline: Instant,
) -> Result<()> {
    let mut stream = trust.accept_until(socket, deadline, 32 * 1024 * 1024)?;
    let request = match api::read_request(&mut stream, limits.max_body_bytes) {
        Ok(request) => request,
        Err(error) => {
            let code = match error.code {
                api::Code::Capacity => "RequestTooLarge",
                api::Code::Route => "NotFound",
                api::Code::Schema => "ValidationFailed",
                api::Code::Precondition => "VersionConflict",
                api::Code::Transport => "ControlUnavailable",
                _ => "Malformed",
            };
            return Response::error_for(error.status, code, stream.principal())?.write(&mut stream);
        }
    };
    if !live.load(Ordering::Acquire) || Instant::now() >= deadline {
        return Err(Error::Transport);
    }
    let (response, receiver) = mpsc::sync_channel(1);
    sender
        .try_send(RequestWork {
            principal: stream.principal().clone(),
            request,
            response,
            live: Arc::clone(&live),
            deadline,
        })
        .map_err(|_| Error::Capacity)?;
    let response = receiver
        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .map_err(|_| Error::Transport)?;
    if !live.load(Ordering::Acquire) {
        return Err(Error::Transport);
    }
    response.write(&mut stream)
}
fn service_error(error: Error) -> Result<(u16, &'static str)> {
    match error {
        Error::Authentication => Ok((401, "Unauthorized")),
        Error::Authorization => Ok((403, "PermissionDenied")),
        Error::Consensus(
            tickwire_control_raft::Error::NotLeader
            | tickwire_control_raft::Error::NotReady
            | tickwire_control_raft::Error::Bounds,
        ) => Ok((503, "ControlUnavailable")),
        error => Err(error),
    }
}
