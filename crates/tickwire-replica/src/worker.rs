//! A bounded single-owner source thread. Network waits never run on the executor.
use crate::*;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
    },
    thread::{self, JoinHandle},
    time::Duration,
};
pub struct ExportWork {
    pub request_id: u64,
    pub export: DurableExport,
    pub extra_objects: BTreeMap<u64, Vec<ObjectRef>>,
}
#[derive(Debug)]
pub enum WorkerEvent {
    Committed {
        request_id: u64,
        evidence: CommitEvidence,
    },
    Rejected {
        request_id: u64,
        error: Error,
    },
    NetworkUnavailable,
    NetworkRestored,
    Frozen,
    Replicated {
        request_id: u64,
        local: LocalPrefixProof,
        durable_ack: Signed,
    },
}
pub struct WorkerHandle {
    input: SyncSender<ExportWork>,
    output: Receiver<WorkerEvent>,
    stop: Arc<AtomicBool>,
    health: Arc<Health>,
    join: Option<JoinHandle<()>>,
}
struct Health {
    frozen: AtomicBool,
    pending: AtomicBool,
    oldest: AtomicU64,
    lag: Option<u64>,
}
impl Health {
    fn update<S: BlobStore>(&self, s: &SourceWorker<S>, frozen: bool) {
        self.frozen.store(frozen || s.frozen(), Ordering::Release);
        match s.oldest_pending_ns() {
            Some(at) => {
                self.oldest.store(at, Ordering::Release);
                self.pending.store(true, Ordering::Release)
            }
            None => self.pending.store(false, Ordering::Release),
        }
    }
    fn healthy(&self, now: u64) -> bool {
        if self.frozen.load(Ordering::Acquire) {
            return false;
        }
        if let Some(limit) = self.lag
            && self.pending.load(Ordering::Acquire)
        {
            let at = self.oldest.load(Ordering::Acquire);
            // The caller samples before loading this concurrently published
            // state. A newly published timestamp may therefore be later
            // than its sample; the worker checks actual clock rollback.
            return now.saturating_sub(at) < limit;
        }
        true
    }
}
impl WorkerHandle {
    /// Evaluate with the same trusted monotonic clock used by the worker.
    pub fn healthy(&self, now_ns: u64) -> bool {
        !self.stop.load(Ordering::Acquire) && self.health.healthy(now_ns)
    }
    /// Full returns ownership of the original work; the caller must retry or halt.
    // Backpressure returns the original owned export without allocating an error box.
    #[allow(clippy::result_large_err)]
    pub fn try_export(
        &self,
        work: ExportWork,
    ) -> std::result::Result<(), TrySendError<ExportWork>> {
        self.input.try_send(work)
    }
    pub fn try_event(&self) -> Result<Option<WorkerEvent>> {
        match self.output.try_recv() {
            Ok(v) => Ok(Some(v)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(Error::Frozen),
        }
    }
    /// Request cancellation without joining the network thread.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
    }
    /// A management-thread operation; may wait for the current request deadline.
    pub fn join(mut self) -> Result<()> {
        self.stop();
        if let Some(j) = self.join.take() {
            j.join().map_err(|_| Error::Frozen)?;
        }
        Ok(())
    }
}
impl Drop for WorkerHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

pub fn spawn_source_worker<S, F>(
    mut source: SourceWorker<S>,
    mut sender: SourceSender,
    capacity: usize,
    mut now_ns: F,
) -> Result<WorkerHandle>
where
    S: BlobStore + Send + 'static,
    F: FnMut() -> u64 + Send + 'static,
{
    if capacity == 0 || capacity > 128 {
        return Err(Error::Bounds);
    }
    let (input, requests) = mpsc::sync_channel::<ExportWork>(capacity);
    let (events, output) = mpsc::sync_channel::<WorkerEvent>(capacity);
    let stop = Arc::new(AtomicBool::new(false));
    let stopped = stop.clone();
    let health = Arc::new(Health {
        frozen: AtomicBool::new(false),
        pending: AtomicBool::new(false),
        oldest: AtomicU64::new(0),
        lag: source.lag_limit_ns(),
    });
    health.update(&source, false);
    let status = health.clone();
    let join = thread::Builder::new()
        .name("tickwire-replica".into())
        .spawn(move || {
            let mut pending = VecDeque::<(u64, LocalPrefixProof)>::new();
            let mut outgoing = VecDeque::new();
            let mut last_id = 0;
            let mut last_now = now_ns();
            let mut offline = false;
            let mut frozen = false;
            while !stopped.load(Ordering::Acquire) {
                status.update(&source, frozen);
                // At most one bounded event queue plus one pending queue is retained.
                while let Some(event) = outgoing.pop_front() {
                    match events.try_send(event) {
                        Ok(()) => {}
                        Err(TrySendError::Full(e)) => {
                            outgoing.push_front(e);
                            break;
                        }
                        Err(TrySendError::Disconnected(_)) => return,
                    }
                }
                if !outgoing.is_empty() {
                    thread::sleep(Duration::from_millis(2));
                    continue;
                }
                let now = now_ns();
                if now < last_now {
                    if !frozen {
                        outgoing.push_back(WorkerEvent::Frozen);
                        frozen = true;
                    }
                    thread::sleep(Duration::from_millis(2));
                    continue;
                }
                last_now = now;
                if !frozen && pending.len() < capacity && source.has_pending_batch_slot() {
                    match requests.try_recv() {
                        Ok(work) => {
                            if work.request_id == 0 || work.request_id <= last_id {
                                outgoing.push_back(WorkerEvent::Rejected {
                                    request_id: work.request_id,
                                    error: Error::Conflict,
                                });
                                frozen = true;
                                continue;
                            }
                            last_id = work.request_id;
                            match source.ingest(&work.export, &work.extra_objects, now) {
                                Ok(proof) => pending.push_back((work.request_id, proof)),
                                Err(error) => {
                                    outgoing.push_back(WorkerEvent::Rejected {
                                        request_id: work.request_id,
                                        error,
                                    });
                                    frozen = true;
                                }
                            }
                        }
                        Err(TryRecvError::Disconnected) => return,
                        Err(TryRecvError::Empty) => {}
                    }
                }
                status.update(&source, frozen);
                // Publish LocalDurable completion before any network wait.
                while let Some((id, proof)) = pending.front() {
                    if matches!(source.stream().format, Format::Opaque { .. }) {
                        if let Some(ack) = source.replication_ack(proof) {
                            outgoing.push_back(WorkerEvent::Replicated {
                                request_id: *id,
                                local: proof.clone(),
                                durable_ack: ack,
                            });
                            pending.pop_front();
                            continue;
                        }
                        break;
                    }
                    match source.commit_evidence(proof, now) {
                        Ok(evidence) => {
                            outgoing.push_back(WorkerEvent::Committed {
                                request_id: *id,
                                evidence,
                            });
                            pending.pop_front();
                        }
                        Err(Error::Missing) => break,
                        Err(Error::Frozen) => {
                            if !frozen {
                                outgoing.push_back(WorkerEvent::Frozen);
                                frozen = true;
                            }
                            break;
                        }
                        Err(error) => {
                            outgoing.push_back(WorkerEvent::Rejected {
                                request_id: *id,
                                error,
                            });
                            pending.pop_front();
                            frozen = true;
                            break;
                        }
                    }
                }
                status.update(&source, frozen);
                // Deliver local completion before network I/O in this iteration.
                // Deferring it to the next receive loop would starve replication
                // whenever another export is ready on every iteration.
                while let Some(event) = outgoing.pop_front() {
                    match events.try_send(event) {
                        Ok(()) => {}
                        Err(TrySendError::Full(e)) => {
                            outgoing.push_front(e);
                            break;
                        }
                        Err(TrySendError::Disconnected(_)) => return,
                    }
                }
                if !outgoing.is_empty() {
                    continue;
                }
                match sender.poll(&mut source) {
                    Ok(_) => {
                        if offline {
                            outgoing.push_back(WorkerEvent::NetworkRestored);
                            offline = false;
                        }
                    }
                    Err(Error::Io(_) | Error::Missing | Error::Gap { .. }) => {
                        sender.disconnect();
                        if !offline {
                            outgoing.push_back(WorkerEvent::NetworkUnavailable);
                            offline = true;
                        }
                        thread::sleep(Duration::from_millis(50));
                    }
                    Err(_) => {
                        if !frozen {
                            outgoing.push_back(WorkerEvent::Frozen);
                            frozen = true;
                        }
                    }
                }
                thread::sleep(Duration::from_millis(2));
            }
        })?;
    Ok(WorkerHandle {
        input,
        output,
        stop,
        health,
        join: Some(join),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn concurrent_pending_publication_does_not_look_like_clock_rollback() {
        let health = Health {
            frozen: AtomicBool::new(false),
            pending: AtomicBool::new(true),
            oldest: AtomicU64::new(101),
            lag: Some(30),
        };
        assert!(health.healthy(100));
        assert!(health.healthy(130));
        assert!(!health.healthy(131));
        health.frozen.store(true, Ordering::Release);
        assert!(!health.healthy(100));
    }
}
