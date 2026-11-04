//! One authenticated WebSocket producer per cash account. A native frame is stored once by
//! digest, followed by a synchronized WAL descriptor. Each row advances the local cursor.
//! The recent-status check reads only a try-locked snapshot. A drain uses the sorted message
//! index to read bounded immutable payloads from disk. Opening the archive validates its full
//! prefix. The index is capped at one million messages or transitions; exhaustion closes intake.
//! A Ready record describes the current connection. A remote history cut requires reconciliation.
//! See the [Bybit private WS](https://bybit-exchange.github.io/docs/v5/ws/connect) and
//! [OKX private WS](https://www.okx.com/docs-v5/en/#overview-websocket) contracts.
use super::capture::{Capture, Entry, Kind};
use super::{endpoint, wire};
use crate::{Credentials, SpotVenue, network, network::Result, recovery::RestReader};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    net::ToSocketAddrs,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use tickwire_account::Scope;
use tickwire_recovery::FileBlobReader;
use tickwire_transport::rustls;

#[derive(Clone)]
pub struct StreamConfig {
    pub directory: PathBuf,
    pub scope: Scope,
    pub uid: String,
    pub stream_id: [u8; 16],
    pub boot_id: [u8; 16],
}
#[derive(Debug, Copy, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub enum Channel {
    Orders,
    Fills,
    Balance,
}
#[derive(Debug, PartialEq)]
pub enum EventKind {
    Started { boot_id: [u8; 16] },
    Ready,
    Data { channel: Channel, row: Value },
    Gap { reason: String },
}
#[derive(Debug, PartialEq)]
pub struct StreamEvent {
    pub sequence: u64,
    pub epoch: u64,
    pub received_ns: u64,
    pub kind: EventKind,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct StreamStatus {
    pub epoch: u64,
    pub durable_sequence: u64,
    pub last_received_ns: u64,
    pub ready: bool,
}
impl StreamStatus {
    pub fn current(&self, epoch: u64, applied: u64, now: u64, maximum_age_ns: u64) -> bool {
        self.ready
            && epoch != 0
            && self.epoch == epoch
            && applied == self.durable_sequence
            && maximum_age_ns != 0
            && now
                .checked_sub(self.last_received_ns)
                .is_some_and(|age| age <= maximum_age_ns)
    }
}
pub(super) struct Shared {
    pub config: StreamConfig,
    pub index: Mutex<Vec<Entry>>,
    pub status: Mutex<StreamStatus>,
    pub objects: FileBlobReader,
    pub stop: AtomicBool,
    pub failed: AtomicBool,
    pub initial_sequence: u64,
}
#[derive(Clone)]
pub struct StreamReader(pub(super) Arc<Shared>);
impl StreamReader {
    /// A storage, clock or worker failure terminates intake. A lost socket reconnects instead.
    pub fn failed(&self) -> bool {
        self.0.failed.load(Ordering::Acquire)
    }
    /// Check an existing account cut against the prefix that was already durable on open.
    pub fn has_initial_cursor(&self, epoch: u64, sequence: u64) -> bool {
        self.0.has_initial_cursor(epoch, sequence)
    }
    pub fn config(&self) -> &StreamConfig {
        &self.0.config
    }
    pub fn status(&self) -> Option<StreamStatus> {
        let status = self.0.status.try_lock().ok()?;
        (!self.0.stop.load(Ordering::Acquire) && !self.failed()).then_some(*status)
    }
    /// Read at most 256 consecutive sealed rows or state transitions after a local cursor.
    pub fn drain(&self, after: u64, limit: usize) -> Result<Vec<StreamEvent>> {
        self.0.drain(after, limit)
    }
}
pub struct PrivateStream {
    reader: StreamReader,
    worker: Option<thread::JoinHandle<std::result::Result<(), String>>>,
}
impl PrivateStream {
    pub fn start(
        config: StreamConfig,
        credentials: &Credentials<'_>,
        tls: Arc<rustls::ClientConfig>,
        monotonic: impl Fn() -> u64 + Send + 'static,
    ) -> Result<Self> {
        let venue = credentials.venue;
        if !credentials.valid() || config.scope.venue != venue.id() {
            return Err("invalid private account credentials or venue".into());
        }
        let key = credentials.key.to_owned();
        let secret = credentials.secret.to_vec();
        let pass = credentials.passphrase.map(str::to_owned);
        let environment = config.scope.environment;
        let uid = config.uid.clone();
        let mut capture = Capture::open(config)?;
        let reader = capture.reader();
        let worker = thread::Builder::new()
            .name("spot-private".into())
            .spawn(move || {
                let creds = Credentials {
                    venue,
                    key: &key,
                    secret: &secret,
                    passphrase: pass.as_deref(),
                };
                let (host, path) = endpoint(venue, environment);
                let dial = || {
                    let rest = RestReader::new(&creds, environment, tls.clone())?;
                    rest.verify_uid(&uid)?;
                    let port = if venue == SpotVenue::Okx { 8443 } else { 443 };
                    let stream =
                        network::tls_at(host, (host, port).to_socket_addrs()?, tls.clone())?;
                    stream
                        .sock
                        .set_read_timeout(Some(Duration::from_millis(50)))?;
                    wire::Socket::open(
                        stream,
                        host,
                        path,
                        &creds,
                        || rest.now_ms(),
                        wire::Timing::default(),
                    )
                };
                run(&mut capture, monotonic, dial).map_err(|e| e.to_string())
            })?;
        Ok(Self {
            reader,
            worker: Some(worker),
        })
    }
    pub fn reader(&self) -> StreamReader {
        self.reader.clone()
    }
    pub fn stop(mut self) -> Result<()> {
        self.reader.0.stop.store(true, Ordering::Release);
        let worker = self.worker.take().ok_or("private reader already stopped")?;
        worker.thread().unpark();
        worker
            .join()
            .map_err(|_| "private reader panicked")?
            .map_err(Into::into)
    }
}
impl Drop for PrivateStream {
    fn drop(&mut self) {
        self.reader.0.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.thread().unpark();
            let _ = worker.join();
        }
    }
}
fn run<S: std::io::Read + std::io::Write>(
    capture: &mut Capture,
    clock: impl Fn() -> u64,
    mut dial: impl FnMut() -> Result<wire::Socket<S>>,
) -> Result<()> {
    let mut backoff = Duration::from_secs(1);
    let shared = capture.reader().0;
    if capture.open_epoch() {
        capture.gap("process_restart", clock())?;
    }
    while !shared.stop.load(Ordering::Acquire) {
        capture.begin(clock())?;
        let mut served = false;
        let mut loss = "connection_or_identity_failed";
        if let Ok(mut socket) = dial() {
            loss = "connection_lost";
            let mut ingress = clock();
            while !shared.stop.load(Ordering::Acquire) {
                let event = match socket.step(|| {
                    capture.pending();
                    ingress = clock();
                }) {
                    Ok(e) => e,
                    Err(_) => {
                        capture.pending();
                        break;
                    }
                };
                let now = ingress;
                match event {
                    wire::Event::Idle => {}
                    wire::Event::Control => capture.touch(now)?,
                    wire::Event::Ready => {
                        capture.append(Kind::Ready, now)?;
                        served = true;
                    }
                    wire::Event::Data(batch, frame) => capture.data(batch, &frame, now)?,
                }
                capture.settled(socket.awaiting_input())?;
            }
            if shared.stop.load(Ordering::Acquire) {
                let _ = socket.close();
            }
        }
        let stop = shared.stop.load(Ordering::Acquire);
        capture.gap(if stop { "stopped" } else { loss }, clock())?;
        if stop {
            break;
        }
        if served {
            backoff = Duration::from_secs(1);
        }
        thread::park_timeout(backoff);
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
