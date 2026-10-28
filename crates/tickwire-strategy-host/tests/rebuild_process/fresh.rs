//! Explicit process acceptance: real registered worker, authenticated report
//! lane, original mapped consumers and complete rebuilt recovery closure.
use super::*;
use std::{process::Command, time::Instant};
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_clock_sys::SystemClock;
use tickwire_strategy_host::{FreshObservation, ObservationRequest};

struct LiveFixture {
    host: Box<tickwire_strategy_host::rebuild::Hosted<NoRecovery>>,
    market: Market,
    _directory: tickwire_control_sys::PrivateDirectory,
    baseline: tickwire_account_codec::CanonicalBaseline,
    account_input: tickwire_account_codec::InputSeal,
    account: tickwire_account::Account,
    domain: ClockDomain,
    pid: u32,
    position: u64,
}

impl LiveFixture {
    fn new() -> Self {
        Self::with_limits(16 * 1024 * 1024, Duration::from_secs(50))
    }

    fn with_limits(maximum_input_bytes: usize, lifetime: Duration) -> Self {
        let worker = fs::canonicalize(
            std::env::var_os("TICKWIRE_CAPTURE_WORKER").expect("explicit frozen release worker"),
        )
        .unwrap();
        let (started, market, directory) = launch_selected(&worker, lifetime, |capture, path| {
            let lease_ns = lifetime.as_nanos().try_into().unwrap();
            capture.market.endpoint.lease_duration_ns = lease_ns;
            capture.report.lease_duration_ns = lease_ns;
            let setup = setup(capture, path, &worker);
            let file = setup.bootstrap.recovery_template.as_ref().unwrap();
            let mut template: tickwire_strategy_sdk::recovery::Template =
                serde_json::from_slice(&fs::read(file).unwrap()).unwrap();
            template.maximum_input_bytes = maximum_input_bytes;
            fs::write(file, serde_json::to_vec(&template).unwrap()).unwrap();
            Some(setup)
        });
        let Launched::Rebuild(mut host) = started else {
            panic!("actual rebuild owner")
        };
        let pid = host.process().unwrap().pid;
        let capture = host.capture_bootstrap().clone();
        let domain = capture.market.domain().unwrap();
        let (mut snapshot, catalog, _) = account_fixture::fixture();
        snapshot.valid_until = u64::MAX;
        let account =
            tickwire_account::Account::from_snapshot(snapshot.clone(), catalog.clone()).unwrap();
        let baseline = tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap();
        assert!(host.on_account_baseline(&baseline, &account));
        wait(|| {
            host.poll().unwrap();
            capture.output_directory.join("ATTEMPT.json").exists()
        });
        market.action.send(Action::Update).unwrap();
        host.seal_capture().unwrap();
        let (operation, member) = publication::write(&host, &worker, directory.path());
        let input = host.handover_rebuild_input(&operation, &member).unwrap();
        let installed = host.handover_install_rebuild(input).unwrap();
        host.handover_finish_rebuild(installed).unwrap();
        assert_eq!(host.process().unwrap().pid, pid);
        let account_input = tickwire_account_codec::InputSeal::new(baseline.digest()).unwrap();
        Self {
            host,
            market,
            _directory: directory,
            baseline,
            account_input,
            account,
            domain,
            pid,
            position: 0,
        }
    }

    fn request(&self, id: u8, lifetime: Duration) -> ObservationRequest {
        let now = SystemClock::new(self.domain)
            .unwrap()
            .sample()
            .unwrap()
            .monotonic;
        ObservationRequest {
            request_id: [id; 16],
            deadline: MonoTime::new(
                self.domain,
                now.nanos()
                    .checked_add(lifetime.as_nanos().try_into().unwrap())
                    .unwrap(),
            ),
            require_resources: false,
        }
    }

    fn complete(&mut self) -> FreshObservation {
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(observation) = self.host.poll_observation().unwrap() {
                return observation;
            }
            assert!(Instant::now() < until, "fresh observation completion");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn observe(&mut self, id: u8) -> FreshObservation {
        self.host
            .begin_observation(self.request(id, Duration::from_secs(5)))
            .unwrap();
        self.complete()
    }

    fn report(&mut self) {
        self.position += 1;
        let record = tickwire_account_codec::CanonicalRecord::new(
            self.baseline.digest(),
            self.position,
            tickwire_account::RecoveryRecord::UnattributedCashFlow {
                id: tickwire_venue::BoundedText::new(format!(
                    "fresh-observation-{}",
                    self.position
                ))
                .unwrap(),
                delta: tickwire_account::AssetAmount {
                    key: account_fixture::key(1),
                    amount: account_fixture::f(1),
                },
            },
        )
        .unwrap();
        assert!(
            self.host
                .on_account_record(self.position, &record, &self.baseline, &self.account)
        );
        self.account_input.apply(&record).unwrap();
    }

    fn update(&self) {
        let (tx, rx) = mpsc::channel();
        self.market
            .action
            .send(Action::UpdateConfirmed(tx))
            .unwrap();
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }

    /// Read actual durable state without receiving a report-control message.
    /// This proves local callback persistence, not an authenticated fresh proof.
    fn durable_checkpoint(&self) -> tickwire_release::replay::Checkpoint {
        let directory = self._directory.path().join("commands/merged-inputs");
        let head =
            tickwire_release::replay::Head::decode(&fs::read(directory.join("HEAD.json")).unwrap())
                .unwrap();
        let hex: String = head
            .checkpoint
            .digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let bytes = fs::read(directory.join(format!("object-{hex}"))).unwrap();
        assert_eq!(Object::of(&bytes), head.checkpoint);
        let checkpoint = tickwire_release::replay::Checkpoint::decode(&bytes).unwrap();
        assert_eq!(checkpoint.runtime.accounts.len(), 1);
        assert_eq!(checkpoint.source, head.source);
        assert_eq!(checkpoint.cut, head.cut);
        checkpoint
    }

    fn durable_report_position(&self) -> u64 {
        self.durable_checkpoint().runtime.accounts[0].sequence
    }

    fn stop(&mut self) {
        self.host.stop().unwrap();
        assert!(!PathBuf::from(format!("/proc/{}", self.pid)).exists());
        let (tx, rx) = mpsc::channel();
        self.market.action.send(Action::Detached(tx)).unwrap();
        assert_eq!(rx.recv().unwrap(), (true, true));
    }
}

fn counts(observation: &FreshObservation) -> serde_json::Value {
    let object = |digest| {
        observation
            .objects()
            .iter()
            .find(|raw| tickwire_recovery::digest(raw) == digest)
            .expect("actual object named by authenticated READY")
    };
    let seal =
        tickwire_recovery::HandoverSeal::decode(object(observation.ready().input_seal_digest))
            .unwrap();
    let retained = tickwire_release::replay::retained::decode(object(seal.input_digest)).unwrap();
    let configuration = observation
        .configuration()
        .expect("current merged configuration");
    assert_eq!(
        configuration.revision(),
        retained.current().runtime.config.revision
    );
    assert_eq!(
        configuration.bytes(),
        retained.current().runtime.config.bytes
    );
    assert_eq!(
        configuration.object(),
        Object::of(&retained.current().runtime.config.bytes)
    );
    assert_eq!(
        configuration.checkpoint(),
        Object::of(&retained.current().bytes().unwrap())
    );
    assert_eq!(configuration.cut(), &retained.current().cut);
    assert_eq!(configuration.source(), &retained.current().source);
    assert_eq!(
        configuration.source().binary_digest,
        observation.process().image_hash()
    );
    assert_eq!(
        configuration.source().instance,
        observation.process().identity().instance
    );
    serde_json::from_slice(
        &tickwire_strategy_sdk::replay::application::business_bytes(&retained.current().business)
            .unwrap(),
    )
    .unwrap()
}

fn signal(pid: u32, signal: &str) {
    assert!(
        Command::new("/bin/kill")
            .args([signal, "--", &pid.to_string()])
            .status()
            .unwrap()
            .success()
    );
}

/// The actual owned child remains unreaped while this guard is alive. Always
/// resume on assertion failure so the owner can retire or collect it normally.
struct PausedChild(Option<u32>);
impl PausedChild {
    fn new(pid: u32) -> Self {
        signal(pid, "-STOP");
        let guard = Self(Some(pid));
        wait(|| {
            let stat = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
            stat.rsplit_once(") ")
                .is_some_and(|(_, tail)| tail.starts_with("T "))
        });
        guard
    }

    fn resume(mut self) {
        signal(self.0.take().unwrap(), "-CONT");
    }
}
impl Drop for PausedChild {
    fn drop(&mut self) {
        if let Some(pid) = self.0 {
            let _ = Command::new("/bin/kill")
                .args(["-CONT", "--", &pid.to_string()])
                .status();
        }
    }
}

#[test]
#[ignore = "requires frozen release TICKWIRE_CAPTURE_WORKER; actual fresh mode-2 responses"]
fn actual_fresh_queries_cross_the_report_floor_and_keep_callbacks_running() {
    let mut f = LiveFixture::new();
    let first = f.observe(161);
    let second = f.observe(162);
    assert_eq!(first.request_id(), [161; 16]);
    assert_eq!(second.request_id(), [162; 16]);
    assert_ne!(first.challenge(), second.challenge());
    assert!(second.ready().ready_sequence > first.ready().ready_sequence);
    assert_eq!(first.process().process().pid, f.pid);
    assert_eq!(
        second.before_process().start_ticks(),
        first.process().start_ticks()
    );
    assert_eq!(second.process().image_hash(), first.process().image_hash());
    assert!(first.resources().is_none());
    assert!(first.started_at().nanos() <= first.observed_at().nanos());
    assert!(first.observed_at().nanos() < first.deadline().nanos());
    assert!(second.observed_at().nanos() < second.deadline().nanos());
    assert!(
        f.host
            .begin_observation(f.request(161, Duration::from_secs(5)))
            .is_err(),
        "a completed external request ID cannot create another observation"
    );

    let paused = PausedChild::new(f.pid);
    f.host
        .begin_observation(f.request(163, Duration::from_secs(5)))
        .unwrap();
    // The authenticated request fixes floor 0 before the report owner publishes
    // real positions 1 and 2. The worker will consume both before answering.
    f.report();
    f.report();
    f.update();
    assert!(f.host.poll_observation().unwrap().is_none());
    paused.resume();
    let ahead = f.complete();
    assert_eq!(ahead.report_sequence_floor(), 0);
    assert_eq!(ahead.ready().exec_report_sequence, 2);
    assert_eq!(ahead.account_input_digest(), f.account_input.digest());
    assert!(ahead.ready().report_ring_sequence > second.ready().report_ring_sequence);
    let ahead_counts = counts(&ahead);
    assert_eq!(ahead_counts["reports"].as_u64().unwrap(), 2);

    // A completed query must not leave the business callbacks paused.
    f.report();
    f.update();
    let mut continued = f.observe(164);
    let minimum = counts(&second)["observations"].as_u64().unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    let mut request = 180;
    while counts(&continued)["observations"].as_u64().unwrap() <= minimum {
        assert!(Instant::now() < until, "market callbacks stayed paused");
        continued = f.observe(request);
        request = request.checked_add(1).unwrap();
    }
    assert_eq!(continued.ready().exec_report_sequence, 3);
    assert_eq!(continued.account_input_digest(), f.account_input.digest());
    assert_eq!(counts(&continued)["reports"].as_u64().unwrap(), 3);
    assert!(
        counts(&continued)["observations"].as_u64().unwrap()
            > counts(&second)["observations"].as_u64().unwrap()
    );
    println!(
        "{}",
        serde_json::json!({"case":"fresh_minimum_report_floor","pid":f.pid,"first_ready":first.ready().ready_sequence,"second_ready":second.ready().ready_sequence,"floor":ahead.report_sequence_floor(),"actual":ahead.ready().exec_report_sequence,"continued":continued.ready().exec_report_sequence,"same_image":true,"resources":null})
    );
    f.stop();
    assert!(
        f.host
            .begin_observation(f.request(165, Duration::from_secs(5)))
            .is_err()
    );
}

#[test]
#[ignore = "requires frozen release TICKWIRE_CAPTURE_WORKER; real SIGSTOP/timeout/recovery"]
fn query_timeout_and_missing_resources_do_not_retire_the_live_worker() {
    let mut f = LiveFixture::new();
    let initial = f.observe(171);
    let mut required = f.request(172, Duration::from_secs(5));
    required.require_resources = true;
    assert!(f.host.begin_observation(required).is_err());
    assert_eq!(f.host.process().unwrap().pid, f.pid);
    // Resource admission failed before accepting the ID or sending a challenge.
    assert!(f.observe(172).resources().is_none());
    let paused = PausedChild::new(f.pid);
    f.report();
    f.host
        .begin_observation(f.request(173, Duration::from_millis(150)))
        .unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        match f.host.poll_observation() {
            Err(_) => break,
            Ok(None) => {}
            Ok(Some(_)) => panic!("a stopped child cannot provide a fresh response"),
        }
        assert!(Instant::now() < until, "request deadline was not enforced");
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(f.host.process().unwrap().pid, f.pid);
    paused.resume();
    f.market.action.send(Action::Update).unwrap();
    let recovered = f.observe(174);
    assert_eq!(recovered.process().process().pid, f.pid);
    assert_eq!(recovered.ready().exec_report_sequence, 1);
    assert_eq!(recovered.account_input_digest(), f.account_input.digest());
    assert_ne!(recovered.challenge(), initial.challenge());
    assert!(recovered.ready().ready_sequence > initial.ready().ready_sequence);

    // Cancel does not send a cancellation frame. The old challenge still owns
    // the lane until its actual authenticated reply is received or it expires.
    let paused = PausedChild::new(f.pid);
    f.host
        .begin_observation(f.request(175, Duration::from_secs(5)))
        .unwrap();
    f.host.cancel_observation([175; 16]).unwrap();
    assert!(matches!(
        f.host
            .begin_observation(f.request(176, Duration::from_secs(5))),
        Err(tickwire_strategy_host::Error::Busy)
    ));
    assert_eq!(f.host.process().unwrap().pid, f.pid);
    f.report();
    paused.resume();
    let until = Instant::now() + Duration::from_secs(3);
    loop {
        match f
            .host
            .begin_observation(f.request(176, Duration::from_secs(5)))
        {
            Ok(()) => break,
            Err(tickwire_strategy_host::Error::Busy) => {}
            Err(e) => panic!("after actual cancelled-response drain: {e}"),
        }
        assert!(Instant::now() < until, "cancelled response was not drained");
        std::thread::sleep(Duration::from_millis(1));
    }
    let after_cancel = f.complete();
    assert_eq!(after_cancel.ready().exec_report_sequence, 2);
    assert_eq!(
        after_cancel.account_input_digest(),
        f.account_input.digest()
    );
    assert_ne!(after_cancel.challenge(), recovered.challenge());
    assert_eq!(after_cancel.process().process().pid, f.pid);
    println!(
        "{}",
        serde_json::json!({"case":"fresh_timeout_preserves_worker","pid":f.pid,"deadline_rejected":true,"missing_resources_rejected":true,"new_query_succeeded":true,"account_position":recovered.ready().exec_report_sequence,"cancel_busy_until_actual_reply":true,"after_cancel_account_position":after_cancel.ready().exec_report_sequence})
    );
    f.stop();
}

#[test]
#[ignore = "requires frozen release TICKWIRE_CAPTURE_WORKER; actual slow report-control reader"]
fn slow_report_control_reader_does_not_stop_account_or_market_callbacks() {
    // Retaining a full checkpoint per report grows the fixture's cold recovery
    // history. Provision its supported 64 MiB budget before approved launch.
    let mut f = LiveFixture::with_limits(64 * 1024 * 1024, Duration::from_secs(180));
    let initial = f.observe(191);
    f.host
        .begin_observation(f.request(192, Duration::from_secs(90)))
        .unwrap();
    // Each real durable account callback has a chance to produce a periodic
    // READY. Do not call any Host control receiver until the loop is complete.
    // Linux's default socket send budget cannot retain this many READY frames.
    let until = Instant::now() + Duration::from_secs(60);
    for position in 1..=176 {
        f.report();
        loop {
            let process = f.host.process().unwrap_or_else(|e| {
                panic!("slow reader must not kill worker at report {position}: {e}")
            });
            assert_eq!(process.pid, f.pid);
            if f.durable_report_position() >= position {
                break;
            }
            assert!(
                Instant::now() < until,
                "account input stalled at {position}"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        // Pace production independently of the socket receiver and avoid
        // turning the entire input stream into a single READY announcement.
        std::thread::sleep(Duration::from_millis(5));
        if position % 32 == 0 {
            f.update();
            println!(
                "slow-reader durable account position {position}, pid {}",
                f.pid
            );
        }
    }
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(f.host.process().unwrap().pid, f.pid);
    let queued = f.complete();
    assert_eq!(queued.report_sequence_floor(), 0);
    // Beginning the next query first validates a bounded batch of accumulated
    // periodic proofs. Include that real cold work in this request's deadline.
    f.host
        .begin_observation(f.request(193, Duration::from_secs(30)))
        .unwrap();
    // The worker's supported 64 MiB cold retention budget is larger than the
    // typed replay verifier's 16 MiB input limit. This long stream deliberately
    // crosses the latter: never claim a complete final FreshObservation or
    // raise the production limit just to make this backpressure test pass.
    let until = Instant::now() + Duration::from_secs(20);
    loop {
        match f.host.poll_observation() {
            Err(tickwire_strategy_host::Error::Sdk(tickwire_strategy_sdk::Error::Release(
                tickwire_release::Error::Bounds,
            ))) => break,
            Ok(None) => {}
            Ok(Some(_)) => panic!("oversized retained inputs must not become a fresh proof"),
            Err(e) => panic!("expected typed retained-input capacity rejection: {e}"),
        }
        assert!(Instant::now() < until, "final capacity rejection missing");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(f.host.process().unwrap().pid, f.pid);
    let checkpoint = f.durable_checkpoint();
    assert_eq!(checkpoint.runtime.accounts[0].sequence, 176);
    let business: serde_json::Value = serde_json::from_slice(
        &tickwire_strategy_sdk::replay::application::business_bytes(&checkpoint.business).unwrap(),
    )
    .unwrap();
    assert_eq!(business["reports"].as_u64(), Some(176));
    assert!(
        business["observations"].as_u64().unwrap()
            > counts(&initial)["observations"].as_u64().unwrap()
    );
    println!(
        "{}",
        serde_json::json!({"case":"slow_report_control_reader","pid":f.pid,"published_accounts":176,"queued_query_actual":queued.ready().exec_report_sequence,"durable_account_position":checkpoint.runtime.accounts[0].sequence,"final_fresh":"rejected_typed_retained_input_capacity","same_live_worker":true,"market_callbacks_continued":true})
    );
    f.stop();
}
