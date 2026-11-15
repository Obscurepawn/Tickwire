//! Supervised strategy worker launcher (`RemoteLaunch`, `RemoteHostedStrategy`) and audited
//! cross-thread SPSC SHM lane attachment helpers (`prepare_cross_thread_intent_lane`,
//! `prepare_cross_thread_report_lane`).

use std::{
    fs::OpenOptions,
    io::Write,
    os::{fd::AsFd, unix::fs::OpenOptionsExt},
    path::{Path, PathBuf},
    sync::mpsc,
    time::{Duration, Instant},
};

use serde::Serialize;
use sha2::{Digest, Sha256};
use tickwire_clock_sys::SystemClock;
use tickwire_control::{Identity, Policy, ProcessIdentity, Session};
use tickwire_control_sys::{ControlListener, PrivateDirectory};
use tickwire_ipc::{ALL_STRATS, IntentRecord, ReportBody, ResourceHeader};
use tickwire_market_source::protocol::{
    Endpoint, accept_offer, cursor_header, resource_offer, wire_bytes,
};
use tickwire_runtime_policy::OrsRuntimePolicy;
use tickwire_shm::{BankKey, Publisher, SubscriptionKey};
use tickwire_strategy_sdk::{
    Bootstrap, IntentReader, IntentWriter, ReportIdentity, ReportReceiver, ReportWriter,
};
use tickwire_supervisor::{
    AuthenticatedChild, ExitObservation, LaunchSpec, LiveResourceObservation, Registry,
};

use crate::{
    Error, ProcessResources, Result,
    observation::{
        DoubleSampledObservation, FreshObservation, ObservationError, ObservationRequest,
    },
    readiness::{ReadyProof, WorkerReadiness},
    resources,
};

/// Deferred `Send + 'static` factory invoked on `"tw-ors-req"` after CPU pinning to attach an
/// [`IntentReader`] directly on its owning thread.
pub type CrossThreadIntentReaderFactory =
    Box<dyn FnOnce() -> std::result::Result<IntentReader, String> + Send + 'static>;

/// Deferred `Send + 'static` factory invoked on `"tw-ors-resp"` after CPU pinning to create and
/// attach a [`ReportWriter`] directly on its owning thread.
pub type CrossThreadReportWriterFactory =
    Box<dyn FnOnce() -> std::result::Result<ReportWriter, String> + Send + 'static>;

/// Completes the strategy-side [`IntentWriter`] registration once `"tw-ors-req"` has attached its
/// [`IntentReader`] cursor descriptor.
pub struct CrossThreadIntentStarter {
    publisher: Publisher<1>,
    sub: SubscriptionKey,
    strat_id: u8,
    producer_instance: [u8; 16],
    producer_generation: u64,
    next_order_seqnum: u64,
    cursor_rx: mpsc::Receiver<std::os::fd::OwnedFd>,
    ack_tx: mpsc::SyncSender<()>,
}

impl CrossThreadIntentStarter {
    /// Receive the reader cursor descriptor from `"tw-ors-req"`, register it on the `Publisher<1>`,
    /// release the `"tw-ors-req"` factory barrier, and return the ready [`IntentWriter`].
    #[allow(unsafe_code)]
    pub fn finish(mut self, timeout: Duration) -> Result<IntentWriter> {
        let cursor_fd = self
            .cursor_rx
            .recv_timeout(timeout)
            .map_err(|_| Error::Timeout)?;
        // SAFETY: `cursor_fd` is the exact `Subscriber` cursor descriptor produced by
        // `IntentReader::attach` for `self.sub`.
        unsafe {
            self.publisher.register_cursor(self.sub, cursor_fd)?;
        }
        let writer = IntentWriter::with_arena(
            self.publisher,
            Some(self.sub),
            None,
            self.strat_id,
            self.producer_instance,
            self.producer_generation,
            self.next_order_seqnum,
        )?;
        let _ = self.ack_tx.send(());
        Ok(writer)
    }
}

/// Prepare a cross-thread SPSC `1,024B` `IntentRecord` SHM ring pair without `unsafe impl Send`.
///
/// Pass the returned [`CrossThreadIntentReaderFactory`] into `OrderRoutingSystem::register_intent_lane_factory`,
/// spawn `OrsProcess`, and then call [`CrossThreadIntentStarter::finish`] on the strategy thread.
#[allow(unsafe_code)]
pub fn prepare_cross_thread_intent_lane(
    bank_header: ResourceHeader,
    subscription_generation: u64,
    cursor_id: [u8; 16],
    strat_id: u8,
    next_order_seqnum: u64,
) -> Result<(CrossThreadIntentStarter, CrossThreadIntentReaderFactory)> {
    if strat_id == ALL_STRATS
        || cursor_id == [0; 16]
        || subscription_generation == 0
        || next_order_seqnum == 0
    {
        return Err(Error::Invalid);
    }
    let producer_instance = bank_header.producer_instance;
    let producer_generation = bank_header.producer_generation;
    let consumer_instance = bank_header.consumer_instance;
    let mut publisher = Publisher::<1>::create(bank_header)?;
    let sub = SubscriptionKey {
        bank: publisher.key(),
        instance: u128::from_le_bytes(consumer_instance),
        generation: subscription_generation,
        cursor: u128::from_le_bytes(cursor_id),
    };
    publisher.join(sub)?;
    let bank = publisher.header().clone();
    let bank_fd = publisher.reader_fd()?;
    let (cursor_tx, cursor_rx) = mpsc::sync_channel(1);
    let (ack_tx, ack_rx) = mpsc::sync_channel(1);

    let reader_factory: CrossThreadIntentReaderFactory = Box::new(move || {
        // SAFETY: `publisher` is freshly created, exclusively owned by `CrossThreadIntentStarter`,
        // and blocks on `ack_rx` until `register_cursor` completes before any slot is published.
        let reader = unsafe { IntentReader::attach(bank_fd, bank, sub, None) }
            .map_err(|e| format!("IntentReader::attach failed: {e}"))?;
        let cursor_fd = reader
            .cursor_fd()
            .map_err(|e| format!("IntentReader::cursor_fd failed: {e}"))?;
        cursor_tx
            .send(cursor_fd)
            .map_err(|_| "CrossThreadIntentStarter dropped before cursor handoff".to_string())?;
        ack_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|e| format!("timed out waiting for IntentWriter cursor registration: {e}"))?;
        Ok(reader)
    });

    Ok((
        CrossThreadIntentStarter {
            publisher,
            sub,
            strat_id,
            producer_instance,
            producer_generation,
            next_order_seqnum,
            cursor_rx,
            ack_tx,
        },
        reader_factory,
    ))
}

/// Completes the strategy-side [`ReportReceiver`] attachment once `"tw-ors-resp"` has created its
/// `512B` `ReportRecord` `Publisher<1>`.
pub struct CrossThreadReportStarter {
    identity: ReportIdentity,
    offer_rx: mpsc::Receiver<(std::os::fd::OwnedFd, ResourceHeader, SubscriptionKey)>,
    cursor_tx: mpsc::SyncSender<std::os::fd::OwnedFd>,
    ack_rx: mpsc::Receiver<()>,
}

impl CrossThreadReportStarter {
    /// Receive the read-only report ring `memfd` from `"tw-ors-resp"`, attach [`ReportReceiver`],
    /// return its cursor descriptor, wait for registration completion, and return [`ReportReceiver`].
    #[allow(unsafe_code)]
    pub fn finish(self, timeout: Duration) -> Result<ReportReceiver> {
        let (bank_fd, bank, sub) = self
            .offer_rx
            .recv_timeout(timeout)
            .map_err(|_| Error::Timeout)?;
        // SAFETY: `"tw-ors-resp"` exclusively owns the freshly created `Publisher<1>` and waits on
        // `cursor_tx` + `register_cursor` before publishing any `ReportRecord`.
        let receiver = unsafe { ReportReceiver::attach(bank_fd, bank, sub, None, self.identity) }?;
        let cursor_fd = receiver.cursor_fd()?;
        self.cursor_tx
            .send(cursor_fd)
            .map_err(|_| Error::Disconnected)?;
        self.ack_rx
            .recv_timeout(timeout)
            .map_err(|_| Error::Timeout)?;
        Ok(receiver)
    }
}

/// Prepare a cross-thread SPSC `512B` `ReportRecord` SHM ring pair without `unsafe impl Send`.
///
/// Pass the returned [`CrossThreadReportWriterFactory`] into `OrderRoutingSystem::register_report_lane_factory`,
/// spawn `OrsProcess`, and then call [`CrossThreadReportStarter::finish`] on the strategy thread.
#[allow(unsafe_code)]
pub fn prepare_cross_thread_report_lane(
    header: ResourceHeader,
    subscription_generation: u64,
    cursor_id: [u8; 16],
    identity: ReportIdentity,
) -> Result<(CrossThreadReportStarter, CrossThreadReportWriterFactory)> {
    if identity.strat_id == ALL_STRATS
        || identity.instance_id == [0; 16]
        || identity.strategy_generation == 0
        || cursor_id == [0; 16]
        || subscription_generation == 0
    {
        return Err(Error::Invalid);
    }
    let (offer_tx, offer_rx) = mpsc::sync_channel(1);
    let (cursor_tx, cursor_rx) = mpsc::sync_channel(1);
    let (ack_tx, ack_rx) = mpsc::sync_channel(1);
    let writer_identity = identity.clone();

    let writer_factory: CrossThreadReportWriterFactory = Box::new(move || {
        let mut publisher = Publisher::<1>::create(header)
            .map_err(|e| format!("Report Publisher::create failed: {e}"))?;
        let sub = SubscriptionKey {
            bank: publisher.key(),
            instance: u128::from_le_bytes(writer_identity.instance_id),
            generation: subscription_generation,
            cursor: u128::from_le_bytes(cursor_id),
        };
        publisher
            .join(sub)
            .map_err(|e| format!("Report Publisher::join failed: {e}"))?;
        let bank = publisher.header().clone();
        let bank_fd = publisher
            .reader_fd()
            .map_err(|e| format!("Report Publisher::reader_fd failed: {e}"))?;
        offer_tx
            .send((bank_fd, bank, sub))
            .map_err(|_| "CrossThreadReportStarter dropped before bank offer".to_string())?;
        let cursor_fd = cursor_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|e| format!("timed out waiting for ReportReceiver cursor: {e}"))?;
        // SAFETY: `cursor_fd` is the exact `Subscriber` cursor descriptor produced by
        // `ReportReceiver::attach` for `sub`.
        unsafe {
            publisher
                .register_cursor(sub, cursor_fd)
                .map_err(|e| format!("Report Publisher::register_cursor failed: {e}"))?;
        }
        let writer = ReportWriter::with_arena(publisher, Some(sub), None, writer_identity)
            .map_err(|e| format!("ReportWriter::with_arena failed: {e}"))?;
        let _ = ack_tx.send(());
        Ok(writer)
    });

    Ok((
        CrossThreadReportStarter {
            identity,
            offer_rx,
            cursor_tx,
            ack_rx,
        },
        writer_factory,
    ))
}

/// Optional callback invoked right after `RemoteHostedStrategy::launch` writes the worker's
/// `tickwire-md` admission ticket and before blocking on `intent.sock` / `report.sock` handshakes.
pub type AdmissionRegisteredHook = Box<dyn FnOnce(ProcessIdentity) -> Result<()> + Send>;

/// Static launch specification for one supervised out-of-process strategy worker.
pub struct RemoteLaunch {
    pub resources: Option<ProcessResources>,
    pub spec: LaunchSpec,
    pub bootstrap: Bootstrap,
    pub directory_parent: PathBuf,
    pub supervisor: Identity,
    pub parent_process: ProcessIdentity,
    pub md_admission_directory: PathBuf,
    pub snapshot_id: [u8; 16],
    pub on_admission_registered: Option<AdmissionRegisteredHook>,
}

/// Attached ORS-side inline SHM rings (`1,024B` `IntentReader` and `512B` `ReportWriter`) for a
/// supervised strategy worker.
pub struct StrategyLanes {
    pub intent_reader: IntentReader,
    pub report_writer: ReportWriter,
}

/// Parent-owned supervisor handle for one stateless out-of-process strategy worker.
///
/// The worker connects directly to an external `tickwire-md` SHM publisher and exchanges inline
/// `IntentRecord` (`1,024B`) and `ReportRecord` (`512B`) slots over sealed `memfd` rings.
pub struct RemoteHostedStrategy {
    readiness: ReadyProof,
    report_sub: SubscriptionKey,
    lanes: Option<StrategyLanes>,
    registry: Registry,
    identity: Identity,
    _intent: AuthenticatedChild,
    report: AuthenticatedChild,
    clock: SystemClock,
    exited: Option<ExitObservation>,
    _directory: PrivateDirectory,
    _intent_listener: ControlListener,
    _report_listener: ControlListener,
}

impl RemoteHostedStrategy {
    /// Spawn a sealed strategy worker ELF, register its one-shot `tickwire-md` admission ticket,
    /// authenticate both `intent.sock` and `report.sock` Unix `SOCK_SEQPACKET` lanes against the
    /// owned kernel `ProcessIdentity` + sealed `/proc/<pid>/exe`, and attach [`StrategyLanes`].
    ///
    /// # Safety
    /// Caller must ensure `config.spec`, `config.bootstrap`, and `config.supervisor` identify
    /// qualified `tickwire-md`, strategy, and ORS execution peers obeying the single-writer SPSC
    /// SHM ring release invariants.
    #[allow(unsafe_code)]
    pub unsafe fn launch(mut config: RemoteLaunch) -> Result<Self> {
        resources::validate(config.resources.as_ref())?;
        if config.parent_process.pid != std::process::id()
            || config.spec.uid != config.parent_process.uid
            || config.spec.gid != config.parent_process.gid
            || config.supervisor.role != 1
            || config.snapshot_id == [0; 16]
            || config.spec.identity != config.bootstrap.market.endpoint.local.identity()
        {
            return Err(Error::Invalid);
        }

        let directory = PrivateDirectory::create_in(&config.directory_parent, "tw-s")?;
        let intent_sock = directory.path().join("intent.sock");
        let report_sock = directory.path().join("report.sock");

        {
            let intent_ep = config.bootstrap.intent.as_mut().ok_or(Error::Invalid)?;
            intent_ep.path = intent_sock.clone();
            intent_ep.peer_pid = config.parent_process.pid;
            intent_ep.peer_uid = config.parent_process.uid;
            intent_ep.peer_gid = config.parent_process.gid;
        }
        {
            let report_ep = config.bootstrap.report.as_mut().ok_or(Error::Invalid)?;
            report_ep.path = report_sock.clone();
            report_ep.peer_pid = config.parent_process.pid;
            report_ep.peer_uid = config.parent_process.uid;
            report_ep.peer_gid = config.parent_process.gid;
        }

        let intent_ep = config.bootstrap.intent.clone().ok_or(Error::Invalid)?;
        let report_ep = config.bootstrap.report.clone().ok_or(Error::Invalid)?;
        if config.spec.identity != intent_ep.local.identity()
            || config.spec.identity != report_ep.local.identity()
            || report_ep.peer.identity() != intent_ep.peer.identity()
            || config.bootstrap.instance_id != config.spec.identity.instance
            || config.bootstrap.generation != config.spec.identity.generation
            || config.bootstrap.logical_strategy_id != config.spec.identity.logical_strategy
        {
            return Err(Error::Invalid);
        }

        let intent_header = config.bootstrap.intent_resource()?.ok_or(Error::Invalid)?;
        if intent_header.consumer_instance != intent_ep.peer.instance
            || intent_header.producer_instance != config.spec.identity.instance
            || intent_header.producer_generation != config.spec.identity.generation
            || intent_header.host_boot_id != config.spec.identity.host_boot
            || intent_header.namespace_id != config.spec.identity.namespace
        {
            return Err(Error::Invalid);
        }

        let report_header = config.bootstrap.report_resource()?.ok_or(Error::Invalid)?;
        if report_header.producer_instance != report_ep.peer.instance
            || report_header.producer_generation != report_ep.peer.generation
            || report_header.consumer_instance != config.spec.identity.instance
            || report_header.host_boot_id != config.spec.identity.host_boot
            || report_header.namespace_id != config.spec.identity.namespace
        {
            return Err(Error::Invalid);
        }

        let intent_listener = ControlListener::bind(&intent_sock)?;
        let report_listener = ControlListener::bind(&report_sock)?;

        let report_publisher = Publisher::<1>::create(report_header)?;
        config.bootstrap.report_header = Some(wire_bytes(report_publisher.header())?);
        let proof = ReadyProof::new(&config.bootstrap, config.resources.as_ref())?;

        let domain = config.bootstrap.market.domain()?;
        let mut clock = SystemClock::new(domain)?;
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(config.bootstrap.startup_timeout_ms))
            .ok_or(Error::Invalid)?;

        let bootstrap_file = directory.path().join("worker.json");
        write_private_json(&bootstrap_file, &config.bootstrap)?;
        if config.spec.arguments.is_empty() {
            config.spec.arguments = vec![
                "--bootstrap".into(),
                bootstrap_file.to_str().ok_or(Error::Invalid)?.into(),
            ];
        }

        let mut registry = Registry::new(1)?;
        let child = resources::spawn(
            &mut registry,
            config.spec.clone(),
            config.resources.as_ref(),
        )?;

        let admission = tickwire_md::server::Admission {
            client: config.bootstrap.market.clone(),
            pid: child.pid,
            uid: child.uid,
            gid: child.gid,
            supervisor: (&config.supervisor).into(),
            supervisor_pid: config.parent_process.pid,
            snapshot_id: config.snapshot_id,
        };
        if let Err(err) =
            tickwire_md::server::register_admission(&config.md_admission_directory, &admission)
        {
            let _ = registry.terminate(config.spec.identity.instance);
            return Err(Error::Io(std::io::Error::other(err.to_string())));
        }

        if let Some(hook) = config.on_admission_registered.take()
            && let Err(err) = hook(child)
        {
            let _ = registry.terminate(config.spec.identity.instance);
            return Err(err);
        }

        let mut report_publisher = Some(report_publisher);
        let handshake_result = (|| -> Result<(
            AuthenticatedChild,
            IntentReader,
            AuthenticatedChild,
            SubscriptionKey,
            ReportWriter,
        )> {
            let mut intent_auth = registry.accept(
                &intent_listener,
                server_policy(&intent_ep, child),
                clock.sample()?.monotonic.nanos(),
                deadline,
            )?;
            // SAFETY: `intent_auth` is the verified owned child speaking over `intent.sock`.
            let intent_reader = unsafe {
                accept_intent_ring(&config.bootstrap, &intent_ep, &mut intent_auth, &mut clock, deadline)?
            };

            let expected = registry.process(config.spec.identity.instance)?;
            let mut report_conn = report_listener.accept(deadline)?;
            let creds = report_conn.peer_credentials();
            let report_proc = ProcessIdentity {
                pid: creds.pid,
                uid: creds.uid,
                gid: creds.gid,
            };
            if report_proc != expected {
                return Err(Error::Invalid);
            }
            let hello = report_conn.recv(deadline)?;
            if !hello.fds().is_empty() {
                return Err(Error::Invalid);
            }
            let (report_session, welcome) = Session::accept(
                &server_policy(&report_ep, child),
                report_proc,
                hello.bytes(),
                clock.sample()?.monotonic.nanos(),
            )?;
            report_conn.send(&welcome, &[], deadline)?;
            registry.process(config.spec.identity.instance)?;

            let publisher = report_publisher.take().ok_or(Error::Invalid)?;
            let report_sub = SubscriptionKey {
                bank: publisher.key(),
                instance: u128::from_le_bytes(config.spec.identity.instance),
                generation: config.bootstrap.report_subscription_generation,
                cursor: u128::from_le_bytes(config.bootstrap.report_cursor_id),
            };
            let mut report_auth = AuthenticatedChild {
                connection: report_conn,
                session: report_session,
                process: report_proc,
            };
            // SAFETY: `report_auth` is the verified owned child speaking over `report.sock`.
            let report_writer = unsafe {
                offer_report_ring(
                    &config.bootstrap,
                    publisher,
                    report_sub,
                    &mut report_auth,
                    &mut clock,
                    deadline,
                )?
            };
            Ok((intent_auth, intent_reader, report_auth, report_sub, report_writer))
        })();

        let (intent, intent_reader, report, report_sub, report_writer) = match handshake_result {
            Ok(tuple) => tuple,
            Err(err) => {
                let _ = registry.terminate(config.spec.identity.instance);
                return Err(err);
            }
        };

        Ok(Self {
            readiness: proof,
            report_sub,
            lanes: Some(StrategyLanes {
                intent_reader,
                report_writer,
            }),
            registry,
            identity: config.spec.identity,
            _intent: intent,
            report,
            clock,
            exited: None,
            _directory: directory,
            _intent_listener: intent_listener,
            _report_listener: report_listener,
        })
    }

    #[inline]
    pub const fn proof(&self) -> &ReadyProof {
        &self.readiness
    }

    #[inline]
    pub const fn identity(&self) -> &Identity {
        &self.identity
    }

    #[inline]
    pub const fn report_subscription(&self) -> SubscriptionKey {
        self.report_sub
    }

    #[inline]
    pub fn child_process(&self) -> ProcessIdentity {
        self.report.process
    }

    /// Returns `(pid, instance_id, generation)` of the live worker for `tickwire-md` diagnostics.
    pub fn market_consumer(&self) -> Option<(u32, [u8; 16], u64)> {
        if self.exited.is_some() {
            return None;
        }
        Some((
            self.report.process.pid,
            self.identity.instance,
            self.identity.generation,
        ))
    }

    /// Move the attached [`StrategyLanes`] (`IntentReader` + `ReportWriter`) to a single-threaded
    /// ORS driver while keeping process supervision and cold observation on `self`.
    pub fn take_lanes(&mut self) -> Option<StrategyLanes> {
        self.lanes.take()
    }

    /// Poll one inbound `1,024B` [`IntentRecord`] when `StrategyLanes` remain owned by `self`.
    pub fn poll_intent(&mut self) -> Result<Option<(IntentRecord, Option<Vec<u8>>)>> {
        let lanes = self.lanes.as_mut().ok_or(Error::LanesDetached)?;
        Ok(lanes.intent_reader.poll()?)
    }

    /// Publish one outbound `512B` `ReportRecord` when `StrategyLanes` remain owned by `self`.
    pub fn publish_report(
        &mut self,
        body: ReportBody,
        instrument_version: u64,
        arena_payload: Option<&[u8]>,
        now_mono_ns: u64,
    ) -> Result<u64> {
        let lanes = self.lanes.as_mut().ok_or(Error::LanesDetached)?;
        Ok(lanes.report_writer.publish_report(
            body,
            instrument_version,
            arena_payload,
            now_mono_ns,
        )?)
    }

    /// Non-blocking child reap check. If the supervised worker exited, marks `strat_id` inactive
    /// on `policy` (when provided) and caches the [`ExitObservation`].
    pub fn maintain(
        &mut self,
        policy: Option<&OrsRuntimePolicy>,
    ) -> Result<Option<&ExitObservation>> {
        if self.exited.is_none() {
            let mut exits = self.registry.observe_exits()?;
            if let Some(obs) = exits.pop() {
                if let Some(pol) = policy {
                    let _ = pol.set_active(self.readiness.strat_id(), false);
                }
                self.exited = Some(obs);
            }
        }
        Ok(self.exited.as_ref())
    }

    /// Evaluate stateless [`WorkerReadiness`] against `policy`.
    pub fn readiness(&mut self, policy: &OrsRuntimePolicy) -> Result<WorkerReadiness> {
        let _ = self.maintain(Some(policy))?;
        let alive = self.exited.is_none() && self.registry.process(self.identity.instance).is_ok();
        let (last_msg, next_rep) = match self.lanes.as_ref() {
            Some(l) => (
                l.intent_reader.last_msg_seqnum(),
                l.report_writer.next_report_seq(),
            ),
            None => (0, 0),
        };
        WorkerReadiness::evaluate(
            &self.readiness,
            policy,
            None,
            alive,
            true,
            last_msg,
            next_rep,
        )
    }

    /// Explicit cold-path cgroup v2 resource sample (`LiveResourceObservation`).
    pub fn observe_resources(&mut self) -> Result<LiveResourceObservation> {
        Ok(self.registry.observe_resources(self.identity.instance)?)
    }

    /// Explicit cold-path double-sampled `/proc` + optional cgroup v2 read.
    pub fn observe_double_sampled(
        &mut self,
        require_resources: bool,
    ) -> std::result::Result<DoubleSampledObservation, ObservationError> {
        DoubleSampledObservation::sample(
            &mut self.registry,
            self.identity.instance,
            require_resources,
        )
    }

    /// Explicit cold-path bounded-age [`FreshObservation`] sample.
    pub fn observe_fresh(
        &mut self,
        policy: &OrsRuntimePolicy,
        request: &ObservationRequest,
    ) -> std::result::Result<FreshObservation, ObservationError> {
        let (last_msg, next_rep) = match self.lanes.as_ref() {
            Some(l) => (
                l.intent_reader.last_msg_seqnum(),
                l.report_writer.next_report_seq(),
            ),
            None => (0, 0),
        };
        FreshObservation::collect(
            &mut self.registry,
            &mut self.clock,
            &self.readiness,
            policy,
            request,
            true,
            last_msg,
            next_rep,
        )
    }

    /// Terminate the owned child process cleanly via `Registry::terminate` and mark `strat_id`
    /// inactive on `policy` when supplied.
    pub fn terminate(&mut self, policy: Option<&OrsRuntimePolicy>) -> Result<&ExitObservation> {
        if self.exited.is_none() {
            let obs = self.registry.terminate(self.identity.instance)?;
            if let Some(pol) = policy {
                let _ = pol.set_active(self.readiness.strat_id(), false);
            }
            self.exited = Some(obs);
        }
        Ok(self.exited.as_ref().expect("exit observation stored"))
    }
}

fn server_policy(endpoint: &Endpoint, child: ProcessIdentity) -> Policy {
    let mut p = endpoint.policy();
    std::mem::swap(&mut p.local, &mut p.peer);
    p.peer_process = child;
    p
}

fn write_private_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    let parent = path.parent().ok_or(Error::Invalid)?;
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or(Error::Invalid)?;
    let tmp = parent.join(format!(".{file_name}.tmp-{}", std::process::id()));
    let _ = std::fs::remove_file(&tmp);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[allow(unsafe_code)]
unsafe fn accept_intent_ring(
    bootstrap: &Bootstrap,
    intent_ep: &Endpoint,
    auth: &mut AuthenticatedChild,
    clock: &mut SystemClock,
    deadline: Instant,
) -> Result<IntentReader> {
    let mut expected = bootstrap.intent_resource()?.ok_or(Error::Invalid)?;
    if expected.consumer_instance != intent_ep.peer.instance
        || expected.producer_instance != bootstrap.instance_id
        || expected.producer_generation != bootstrap.generation
        || expected.host_boot_id != intent_ep.local.host_boot
        || expected.namespace_id != intent_ep.local.namespace
    {
        return Err(Error::Invalid);
    }
    expected.descriptor_hash = [0; 32];
    expected.descriptor_hash = Sha256::digest(wire_bytes(&expected)?).into();
    let membership = expected
        .initial_membership_version
        .checked_add(1)
        .ok_or(Error::Invalid)?;
    let expires = auth.session.deadline();
    let now = clock.sample()?.monotonic.nanos();
    let packet = auth.connection.recv(deadline)?;
    let bank_fd = accept_offer(
        &mut auth.session,
        packet,
        &expected,
        [7; 16],
        expires,
        membership,
        false,
        now,
    )?;
    let sub = SubscriptionKey {
        bank: BankKey {
            resource: u128::from_le_bytes(expected.resource_id),
            channel: u128::from_le_bytes(expected.channel_id),
            channel_epoch: expected.channel_epoch,
            ring_epoch: expected.ring_epoch,
            feed_generation: expected.feed_generation,
        },
        instance: u128::from_le_bytes(intent_ep.peer.instance),
        generation: bootstrap.intent_subscription_generation,
        cursor: u128::from_le_bytes(bootstrap.intent_cursor_id),
    };
    // SAFETY: Caller verified `auth` is the single authenticated strategy writer for `expected`.
    let reader = unsafe { IntentReader::attach(bank_fd, expected.clone(), sub, None)? };
    let cursor_fd = reader.cursor_fd()?;
    let ch = cursor_header(&expected, sub)?;
    let reply = resource_offer(&auth.session, &ch, [8; 16], expires, membership, false)?;
    auth.connection
        .send(&reply, &[cursor_fd.as_fd()], deadline)?;
    Ok(reader)
}

#[allow(unsafe_code)]
unsafe fn offer_report_ring(
    bootstrap: &Bootstrap,
    mut publisher: Publisher<1>,
    report_sub: SubscriptionKey,
    auth: &mut AuthenticatedChild,
    clock: &mut SystemClock,
    deadline: Instant,
) -> Result<ReportWriter> {
    publisher.join(report_sub)?;
    let membership = publisher.membership_version();
    let expires = auth.session.deadline();
    let offered = resource_offer(
        &auth.session,
        publisher.header(),
        [14; 16],
        expires,
        membership,
        false,
    )?;
    let bank_fd = publisher.reader_fd()?;
    auth.connection
        .send(&offered, &[bank_fd.as_fd()], deadline)?;
    let ch = cursor_header(publisher.header(), report_sub)?;
    let now = clock.sample()?.monotonic.nanos();
    let packet = auth.connection.recv(deadline)?;
    let cursor_fd = accept_offer(
        &mut auth.session,
        packet,
        &ch,
        [15; 16],
        expires,
        membership,
        false,
        now,
    )?;
    // SAFETY: `cursor_fd` is the exact `Subscriber` cursor descriptor verified via `accept_offer`
    // for `report_sub`.
    unsafe {
        publisher.register_cursor(report_sub, cursor_fd)?;
    }
    let writer = ReportWriter::with_arena(
        publisher,
        Some(report_sub),
        None,
        ReportIdentity {
            strat_id: bootstrap.strat_id,
            instance_id: bootstrap.instance_id,
            strategy_generation: bootstrap.generation,
        },
    )?;
    Ok(writer)
}
