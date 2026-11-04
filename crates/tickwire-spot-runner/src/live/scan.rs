//! The network reader can wait on pagination while the account owner drains its private stream.
use super::Result;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender, TryRecvError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tickwire_spot_execution::{
    network::OwnedCredentials,
    recovery::{OrderLookup, RestEvidence, RestReader},
};
use tickwire_transport::rustls::ClientConfig;
use tickwire_venue::Environment;

pub(super) struct Scan {
    pub start_ms: u64,
    pub orders: Vec<OrderLookup>,
}

pub(super) struct Evidence {
    pub first: RestEvidence,
    pub second: RestEvidence,
    pub clock: (u64, Instant),
}
pub(super) struct Scanner {
    stopped: Arc<AtomicBool>,
    requests: Option<SyncSender<Scan>>,
    replies: Receiver<std::result::Result<Evidence, String>>,
    worker: Option<JoinHandle<()>>,
}

impl Scanner {
    pub fn start(
        credentials: OwnedCredentials,
        environment: Environment,
        tls: Arc<ClientConfig>,
        uid: String,
    ) -> Result<Self> {
        let (tx, requests) = mpsc::sync_channel::<Scan>(1);
        let (replies, rx) = mpsc::sync_channel(1);
        let stopped = Arc::new(AtomicBool::new(false));
        let cancel = stopped.clone();
        let worker = thread::Builder::new()
            .name("spot-rest-recovery".into())
            .spawn(move || {
                let credentials = credentials.credentials();
                while let Ok(request) = requests.recv() {
                    let collect = || -> Result<Evidence> {
                        let give_up = Instant::now() + Duration::from_secs(90);
                        let running =
                            || !cancel.load(Ordering::Acquire) && Instant::now() < give_up;
                        let reader = RestReader::new(&credentials, environment, tls.clone())?;
                        let end = reader.now_ms()?;
                        let first = reader.acquire_while(
                            &uid,
                            request.start_ms,
                            end,
                            &request.orders,
                            running,
                        )?;
                        let second = reader.acquire_while(
                            &uid,
                            request.start_ms,
                            reader.now_ms()?,
                            &request.orders,
                            running,
                        )?;
                        let stamp = reader.now_ms()?;
                        Ok(Evidence {
                            first,
                            second,
                            clock: (stamp, Instant::now()),
                        })
                    };
                    if replies.send(collect().map_err(|e| e.to_string())).is_err() {
                        break;
                    }
                }
            })?;
        Ok(Self {
            stopped,
            requests: Some(tx),
            replies: rx,
            worker: Some(worker),
        })
    }
    pub fn request(&self, scan: Scan) -> Result<()> {
        self.requests
            .as_ref()
            .ok_or("REST recovery worker stopped")?
            .try_send(scan)
            .map_err(|_| "REST recovery request is busy or closed".into())
    }
    pub fn poll(&self) -> Result<Option<std::result::Result<Evidence, String>>> {
        match self.replies.try_recv() {
            Ok(evidence) => Ok(Some(evidence)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err("REST recovery worker stopped".into()),
        }
    }
}
impl Drop for Scanner {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        self.requests.take();
        if let Some(thread) = self.worker.take() {
            let _ = thread.join();
        }
    }
}
