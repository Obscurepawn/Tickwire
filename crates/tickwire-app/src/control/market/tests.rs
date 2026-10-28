use super::*;
use tickwire_control::ProcessIdentity;
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_market_source::{
    ConnectConfig,
    protocol::{Endpoint, Instrument, wire_bytes},
};
mod clock_tests;
#[path = "../../../../tickwire-join/tests/common/mod.rs"]
mod fixture;
fn approval() -> Approval {
    let mut policy = fixture::policy(
        fixture::identity(4, 4),
        fixture::identity(3, 2),
        ProcessIdentity {
            pid: 42,
            uid: 1,
            gid: 1,
        },
        fixture::NOW,
    );
    policy.supported_features |= 64;
    let mut bank = fixture::header();
    bank.required_features |= 64;
    bank.descriptor_hash = [0; 32];
    bank.descriptor_hash = tickwire_recovery::digest(&wire_bytes(&bank).unwrap());
    let previous = ConnectConfig {
        endpoint: Endpoint::new("/tmp/qualified-md.sock".into(), &policy),
        bank_header: wire_bytes(&bank).unwrap(),
        catalog_hash: fixture::registry().catalog_hash,
        instrument_set_version: 1,
        instruments: vec![Instrument {
            id: 1001,
            version: 5,
            required_states: 1,
        }],
        cursor_id: [104; 16],
        subscription_generation: 1,
        request_id: [25; 16],
        maximum_levels: 32,
        startup_timeout_ms: 500,
        clock_generation: 1,
    };
    let mut next_template = previous.clone();
    bank.resource_id = [93; 16];
    bank.predecessor_ring_epoch = bank.ring_epoch;
    bank.ring_epoch += 1;
    bank.feed_generation += 1;
    bank.created_mono_ns = 0;
    bank.cutover_seq = 0;
    bank.first_seq = 1;
    bank.descriptor_hash = [0; 32];
    next_template.bank_header = wire_bytes(&bank).unwrap();
    next_template.cursor_id = [105; 16];
    next_template.request_id = [26; 16];
    next_template.subscription_generation += 1;
    next_template.instrument_set_version += 1;
    next_template.endpoint.control_epoch += 1;
    next_template.endpoint.nonce = [72; 32];
    next_template.instruments.push(Instrument {
        id: u64::MAX,
        version: 5,
        required_states: 1,
    });
    Approval {
        schema: 1,
        operation_id: [11; 16],
        approval_digest: [12; 32],
        revision: 2,
        previous,
        next_template,
        deadline_mono_ns: fixture::NOW + 1_000_000,
    }
}
fn commit(a: &Approval) -> Commit {
    let mut config = a.next_template.clone();
    let mut bank = ResourceHeader::decode(&config.bank_header).unwrap();
    bank.created_mono_ns = fixture::NOW;
    bank.cutover_seq = 6;
    bank.first_seq = 7;
    bank.descriptor_hash = tickwire_recovery::digest(&wire_bytes(&bank).unwrap());
    config.bank_header = wire_bytes(&bank).unwrap();
    Commit {
        schema: 1,
        operation_id: a.operation_id,
        approval_digest: a.approval_digest,
        revision: a.revision,
        config,
    }
}
fn decision(a: &Approval) -> Decision {
    let (mut policy, _) = super::super::tests::inputs();
    policy.executor_id = a.previous.endpoint.local.instance;
    policy.domain.process_instance = a.previous.endpoint.local.host_boot;
    Decision {
        binding: Binding::from_config(&policy),
        consumer: Consumer {
            pid: 42,
            instance: a.previous.endpoint.local.instance,
            generation: 1,
        },
        action: DecisionAction::Arm {
            approval: Box::new(a.clone()),
        },
    }
}
#[test]
fn decision_history_retains_exact_identity_without_reviving_runtime_operations() {
    let a = approval();
    let now = MonoTime::new(a.previous.domain().unwrap(), fixture::NOW);
    let mut history = History::default();
    let arm = decision(&a);
    history.record(arm.clone(), now, 1).unwrap();
    assert_eq!(history.len(), 1);
    assert!(history.contains(&arm).unwrap());
    let mut conflict = arm.clone();
    conflict.consumer.generation += 1;
    assert!(history.check(&conflict, now, 1).is_err());
    let mut conflict = arm.clone();
    if let DecisionAction::Arm { approval } = &mut conflict.action {
        approval.approval_digest = [1; 32];
    }
    assert!(history.contains(&conflict).is_err());
    let mut c = arm.clone();
    c.action = DecisionAction::Commit {
        commit: Box::new(commit(&a)),
        previous_applied: 6,
    };
    history.record(c.clone(), now, 1).unwrap();
    assert!(history.contains(&c).unwrap());
    let mut another = a.clone();
    another.operation_id = [44; 16];
    assert!(history.check(&decision(&another), now, 1).is_err());
    assert!(
        history
            .check(&arm, MonoTime::new(now.domain(), a.deadline_mono_ns), 1)
            .is_err()
    );
}
#[test]
fn commit_decision_requires_previous_approved_operation_and_actual_cut() {
    let a = approval();
    let now = MonoTime::new(a.previous.domain().unwrap(), fixture::NOW);
    let arm = decision(&a);
    let mut c = arm.clone();
    c.action = DecisionAction::Commit {
        commit: Box::new(commit(&a)),
        previous_applied: 6,
    };
    let mut history = History::default();
    assert!(history.check(&c, now, 8).is_err());
    history.record(arm, now, 8).unwrap();
    if let DecisionAction::Commit {
        previous_applied, ..
    } = &mut c.action
    {
        *previous_applied = 7;
    }
    assert!(history.check(&c, now, 8).is_err());
}
struct Directories {
    root: PathBuf,
    config: Config,
}
impl Directories {
    fn new() -> Self {
        use std::os::unix::fs::PermissionsExt;
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "tw-app-market-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let consumer_directory = root.join("consumer");
        let effective_directory = root.join("effective");
        for p in [&consumer_directory, &effective_directory] {
            std::fs::create_dir(p).unwrap();
            std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        Self {
            root,
            config: Config {
                consumer_directory,
                effective_directory,
                maximum_operations: 8,
                maximum_per_poll: 8,
            },
        }
    }
    fn put(&self, c: Consumer, suffix: &str, value: &impl Serialize) {
        files::replace(
            &self
                .config
                .consumer_directory
                .join(format!("{}.{suffix}.json", c.stem())),
            &serde_json::to_vec(value).unwrap(),
        )
        .unwrap();
    }
}
impl Drop for Directories {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
struct Remote {
    consumer: Consumer,
    receipt: Option<Receipt>,
    arms: usize,
    commits: usize,
}
impl Remote {
    fn new(c: Consumer) -> Self {
        Self {
            consumer: c,
            receipt: None,
            arms: 0,
            commits: 0,
        }
    }
}
impl tickwire_runtime::node::Strategy for Remote {
    fn on_market(&mut self, _: &tickwire_binance_public::Notice<'_>) -> bool {
        true
    }
    fn next(
        &mut self,
        _: &tickwire_account::Account,
        _: tickwire_runtime::node::Health,
        _: tickwire_binance_clock::LocalTime,
    ) -> Option<tickwire_runtime::node::Submission> {
        None
    }
    fn on_submission(
        &mut self,
        _: &tickwire_venue::CanonicalCommand,
        _: &std::result::Result<tickwire_account_gateway::Status, tickwire_binance_account::Error>,
    ) {
    }
}
impl AppStrategy for Remote {
    fn failed(&self) -> bool {
        false
    }
    fn market_consumers(&self, out: &mut Vec<Consumer>) {
        out.push(self.consumer);
    }
    fn arm_market(&mut self, c: Consumer, _: &Approval) -> Result<()> {
        assert_eq!(c, self.consumer);
        self.arms += 1;
        Ok(())
    }
    fn commit_market(&mut self, c: Consumer, _: &Commit) -> Result<()> {
        assert_eq!(c, self.consumer);
        self.commits += 1;
        Ok(())
    }
    fn market_transition(&self, c: Consumer) -> Option<&Receipt> {
        assert_eq!(c, self.consumer);
        self.receipt.as_ref()
    }
}
#[test]
fn durable_decision_precedes_arm_and_restart_never_replays_it() {
    let a = approval();
    let d = decision(&a);
    let now = MonoTime::new(a.previous.domain().unwrap(), fixture::NOW);
    let dirs = Directories::new();
    dirs.put(d.consumer, "arm", &a);
    let mut remote = Remote::new(d.consumer);
    let mut controller = Controller::new(dirs.config.clone(), None, History::default()).unwrap();
    assert!(
        controller
            .poll(&d.binding, &mut remote, now, |_| Err(invalid(
                "sync failed"
            )))
            .is_err()
    );
    assert_eq!(remote.arms, 0);
    assert_eq!(controller.history.len(), 0);
    let mut durable = Vec::new();
    controller
        .poll(&d.binding, &mut remote, now, |v| {
            if let Some(v) = v {
                durable.push(v.0.clone());
            }
            Ok(now)
        })
        .unwrap();
    assert_eq!(remote.arms, 1);
    assert_eq!(durable.len(), 1);
    assert!(
        !dirs
            .config
            .consumer_directory
            .join(format!("{}.receipt.json", d.consumer.stem()))
            .exists()
    );
    let mut history = History::default();
    history.record(durable.remove(0), now, 8).unwrap();
    let mut recovered = Controller::new(dirs.config.clone(), None, history).unwrap();
    recovered
        .poll(&d.binding, &mut remote, now, |v| {
            assert!(v.is_none(), "recovery must not replay");
            Ok(now)
        })
        .unwrap();
    assert!(recovered.active.is_empty());
    assert_eq!(remote.arms, 1);
}
#[test]
fn decision_sync_crossing_deadline_never_dispatches_arm() {
    let a = approval();
    let d = decision(&a);
    let now = MonoTime::new(a.previous.domain().unwrap(), fixture::NOW);
    let expired = MonoTime::new(now.domain(), a.deadline_mono_ns);
    let dirs = Directories::new();
    dirs.put(d.consumer, "arm", &a);
    let mut remote = Remote::new(d.consumer);
    let mut controller = Controller::new(dirs.config.clone(), None, History::default()).unwrap();
    let mut synchronized = false;
    let result = controller.poll(&d.binding, &mut remote, now, |decision| {
        if decision.is_some() {
            synchronized = true;
        }
        Ok(if synchronized { expired } else { now })
    });
    assert!(synchronized, "the decision reached the durable boundary");
    assert!(
        result.is_err(),
        "expired durable decision must not dispatch"
    );
    assert_eq!(remote.arms, 0);
    assert!(
        controller.history.contains(&d).unwrap(),
        "durable expiry consumes the operation"
    );
}

#[test]
fn only_actual_phase_receipts_enable_commit_and_effective_success() {
    let a = approval();
    let d = decision(&a);
    let now = MonoTime::new(a.previous.domain().unwrap(), fixture::NOW);
    let dirs = Directories::new();
    dirs.put(d.consumer, "arm", &a);
    let c = commit(&a);
    dirs.put(d.consumer, "commit", &c);
    let mut remote = Remote::new(d.consumer);
    let mut controller = Controller::new(dirs.config.clone(), None, History::default()).unwrap();
    let mut persisted = 0;
    controller
        .poll(&d.binding, &mut remote, now, |v| {
            if v.is_some() {
                persisted += 1;
            }
            Ok(now)
        })
        .unwrap();
    assert_eq!(remote.commits, 0);
    remote.receipt = Some(Receipt::armed(&a, d.consumer.pid, now).unwrap());
    controller
        .poll(&d.binding, &mut remote, now, |v| {
            assert!(v.is_none());
            Ok(now)
        })
        .unwrap();
    assert_eq!(remote.commits, 0);
    remote.receipt = Some(Receipt::quiesced(&a, d.consumer.pid, 6, now).unwrap());
    controller
        .poll(&d.binding, &mut remote, now, |v| {
            if v.is_some() {
                persisted += 1;
            }
            Ok(now)
        })
        .unwrap();
    assert_eq!(remote.commits, 1);
    assert_eq!(persisted, 2);
    let mut activated = remote.receipt.clone().unwrap();
    activated.phase = Phase::Activated;
    activated.next_descriptor_hash = Some(
        ResourceHeader::decode(&c.config.bank_header)
            .unwrap()
            .descriptor_hash,
    );
    activated.activated_sequence = Some(8);
    remote.receipt = Some(activated.clone());
    let effective = dirs
        .config
        .effective_directory
        .join(format!("{}.json", d.consumer.stem()));
    std::fs::write(effective.with_extension("pending"), b"interrupted").unwrap();
    assert!(
        controller
            .poll(&d.binding, &mut remote, now, |v| {
                assert!(v.is_none());
                Ok(now)
            })
            .is_err()
    );
    let receipt = dirs
        .config
        .consumer_directory
        .join(format!("{}.receipt.json", d.consumer.stem()));
    assert_eq!(
        Receipt::decode(&files::read(&receipt, MAX_CONTROL_BYTES).unwrap().unwrap())
            .unwrap()
            .phase,
        Phase::Quiesced
    );
    std::fs::remove_file(effective.with_extension("pending")).unwrap();
    controller
        .poll(&d.binding, &mut remote, now, |v| {
            assert!(v.is_none());
            Ok(now)
        })
        .unwrap();
    let effective: serde_json::Value =
        serde_json::from_slice(&files::read(&effective, MAX_CONTROL_BYTES).unwrap().unwrap())
            .unwrap();
    assert_eq!(
        effective["receipt"],
        serde_json::to_value(&activated).unwrap()
    );
    assert_eq!(
        Receipt::decode(&files::read(&receipt, MAX_CONTROL_BYTES).unwrap().unwrap()).unwrap(),
        activated
    );
    controller
        .poll(&d.binding, &mut remote, now, |v| {
            assert!(v.is_none());
            Ok(now)
        })
        .unwrap();
    assert_eq!(remote.commits, 1);
}
#[cfg(target_os = "linux")]
#[test]
fn actual_sources_control_wal_receipts_and_restart_are_consistent() {
    use crate::control::{ControlInbox, Record};
    use crate::market::tests::{Command as PeerCommand, Peer, Sink, now};
    use tickwire_market_source::MarketSource;
    let (peer, a) = Peer::start(true);
    let domain = a.previous.domain().unwrap();
    let mut sources = Sources::connect(
        std::slice::from_ref(&a.previous),
        domain,
        a.previous.endpoint.local.instance,
        a.previous.endpoint.local.generation,
    )
    .unwrap();
    let dirs = Directories::new();
    let consumer = sources.consumers()[0];
    dirs.put(consumer, "arm", &a);
    let (mut policy, mut quota) = super::super::tests::inputs();
    policy.executor_id = consumer.instance;
    policy.owner_generation = consumer.generation;
    policy.domain.process_instance = domain.boot_id();
    policy.domain.generation = domain.generation();
    let end = now(domain).nanos() + 60_000_000_000;
    policy.owner_lease.as_mut().unwrap().valid_until_ns = end;
    quota.consumer_lease_until_ns = end;
    quota.policy_ends_ns = end;
    quota.windows[0].ends_ns = end;
    quota.grants[0].expires_ns = end;
    let mut config = super::super::Config {
        inbox_directory: dirs.config.consumer_directory.clone(),
        journal_directory: dirs.root.join("wal"),
        stream_epoch: [31; 16],
        recover: false,
        segment_max_bytes: 4 << 20,
        maximum_commands: 8,
        maximum_per_poll: 8,
        startup_consumer_lease_until_ns: Some(end),
    };
    let clock = || tickwire_binance_clock::SystemClock::new(domain).unwrap();
    let (mut controls, mut endpoint) = ControlInbox::open(
        config.clone(),
        &policy,
        &quota,
        policy.build().unwrap(),
        clock(),
    )
    .unwrap();
    controls
        .configure_market(dirs.config.clone(), Some(sources.clone()))
        .unwrap();
    let mut strategy =
        crate::strategy::FileStrategy::new(&crate::strategy::Config::Observe).unwrap();
    controls.poll_market(&mut endpoint, &mut strategy).unwrap();
    assert_eq!(controls.next_sequence(), 1);
    assert_eq!(controls.records, 2);
    assert_eq!(
        Receipt::decode(
            &files::read(
                &dirs
                    .config
                    .consumer_directory
                    .join(format!("{}.receipt.json", consumer.stem())),
                MAX_CONTROL_BYTES
            )
            .unwrap()
            .unwrap()
        )
        .unwrap()
        .phase,
        Phase::Armed
    );
    peer.command.send(PeerCommand::Quiesce).unwrap();
    let mut sink = Sink::default();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        assert!(std::time::Instant::now() < deadline);
        sources
            .poll(now(domain), 10_000_000_000, 32, &mut sink)
            .unwrap();
        if let Ok(commit) = peer.commit.try_recv() {
            dirs.put(consumer, "commit", &commit);
        }
        controls.poll_market(&mut endpoint, &mut strategy).unwrap();
        if controls
            .market
            .as_ref()
            .unwrap()
            .active
            .get(&consumer)
            .unwrap()
            .finished
        {
            break;
        }
        std::thread::yield_now();
    }
    assert_eq!(controls.records, 3);
    assert_eq!(controls.next_sequence(), 1);
    sources
        .poll(now(domain), 10_000_000_000, 32, &mut sink)
        .unwrap();
    assert!(sources.is_live(now(domain), 10_000_000_000));
    drop(controls);
    let (journal, recovery) = tickwire_journal::Journal::recover(
        tickwire_journal::store::FileStore::open(&config.journal_directory).unwrap(),
        None,
    )
    .unwrap();
    assert_eq!(recovery.records.len(), 3);
    assert!(
        matches!(super::super::decode_record(&recovery.records[1].payload).unwrap(),Record::MarketDecision{decision,..} if matches!(read_decision(&open_objects(&config.journal_directory).unwrap(),decision).unwrap().action,DecisionAction::Arm{..}))
    );
    assert!(
        matches!(super::super::decode_record(&recovery.records[2].payload).unwrap(),Record::MarketDecision{decision,..} if matches!(read_decision(&open_objects(&config.journal_directory).unwrap(),decision).unwrap().action,DecisionAction::Commit{..}))
    );
    drop(journal);
    config.recover = true;
    let (mut restored, mut endpoint) =
        ControlInbox::open(config, &policy, &quota, policy.build().unwrap(), clock()).unwrap();
    assert_eq!(restored.next_sequence(), 1);
    assert_eq!(restored.session(), 2);
    restored
        .configure_market(dirs.config.clone(), Some(sources))
        .unwrap();
    restored.poll_market(&mut endpoint, &mut strategy).unwrap();
    assert!(restored.market.as_ref().unwrap().active.is_empty());
    assert_eq!(restored.records, 4);
}
#[test]
fn large_approved_decision_is_durable_without_exceeding_physical_wal_batch() {
    use crate::control::ControlInbox;
    let mut a = approval();
    let domain = a.previous.domain().unwrap();
    let clock = || tickwire_binance_clock::SystemClock::new(domain).unwrap();
    let at = clock().sample().unwrap().monotonic.nanos();
    a.deadline_mono_ns = at + 10_000_000_000;
    a.next_template.instruments.pop();
    for id in 1002..3002 {
        a.next_template.instruments.push(Instrument {
            id,
            version: 5,
            required_states: 1,
        });
    }
    let dirs = Directories::new();
    let d = decision(&a);
    dirs.put(d.consumer, "arm", &a);
    let (mut policy, mut quota) = super::super::tests::inputs();
    policy.executor_id = d.consumer.instance;
    policy.domain.process_instance = domain.boot_id();
    let end = at + 60_000_000_000;
    policy.owner_lease.as_mut().unwrap().valid_until_ns = end;
    quota.consumer_lease_until_ns = end;
    quota.policy_ends_ns = end;
    quota.windows[0].ends_ns = end;
    quota.grants[0].expires_ns = end;
    let mut config = super::super::Config {
        inbox_directory: dirs.config.consumer_directory.clone(),
        journal_directory: dirs.root.join("wal"),
        stream_epoch: [31; 16],
        recover: false,
        segment_max_bytes: 4 << 20,
        maximum_commands: 8,
        maximum_per_poll: 8,
        startup_consumer_lease_until_ns: Some(end),
    };
    let (mut controls, mut endpoint) = ControlInbox::open(
        config.clone(),
        &policy,
        &quota,
        policy.build().unwrap(),
        clock(),
    )
    .unwrap();
    controls
        .configure_market(dirs.config.clone(), None)
        .unwrap();
    let mut remote = Remote::new(d.consumer);
    assert!(serde_json::to_vec(&d).unwrap().len() > 65536);
    controls.poll_market(&mut endpoint, &mut remote).unwrap();
    assert_eq!(remote.arms, 1);
    assert_eq!(controls.records, 2);
    let immediate = MonoTime::new(domain, controls.state.last_ns + 1);
    let allocation = allocation_counter::measure(|| {
        assert_eq!(
            controls
                .poll_market_at(immediate, &mut endpoint, &mut remote)
                .unwrap(),
            0
        )
    });
    assert_eq!(
        allocation.count_total, 0,
        "unchanged cold control must not allocate before next scan"
    );
    let next = MonoTime::new(domain, controls.next_market_scan_ns);
    // The production boundary resamples actual time; reach the next scan before
    // measuring it instead of passing a fabricated future timestamp.
    while clock().sample().unwrap().monotonic.nanos() < next.nanos() {
        std::thread::yield_now();
    }
    let allocation = allocation_counter::measure(|| {
        assert_eq!(
            controls
                .poll_market_at(next, &mut endpoint, &mut remote)
                .unwrap(),
            1
        )
    });
    assert!(
        allocation.bytes_total < 16_384,
        "unchanged large approval must use its file identity cache: {allocation:?}"
    );
    let mut changed = a.clone();
    changed.approval_digest = [44; 32];
    dirs.put(d.consumer, "arm", &changed);
    let changed_at = MonoTime::new(domain, controls.next_market_scan_ns);
    while clock().sample().unwrap().monotonic.nanos() < changed_at.nanos() {
        std::thread::yield_now();
    }
    assert!(
        controls
            .poll_market_at(changed_at, &mut endpoint, &mut remote)
            .is_err()
    );
    assert_eq!(remote.arms, 1);
    dirs.put(d.consumer, "arm", &a);
    drop(controls);
    config.recover = true;
    let (mut restored, mut endpoint) = ControlInbox::open(
        config.clone(),
        &policy,
        &quota,
        policy.build().unwrap(),
        clock(),
    )
    .unwrap();
    let mut rejected = dirs.config.clone();
    rejected.maximum_operations = 0;
    assert!(restored.configure_market(rejected, None).is_err());
    assert_eq!(restored.state.market_history.len(), 1);
    restored
        .configure_market(dirs.config.clone(), None)
        .unwrap();
    restored.poll_market(&mut endpoint, &mut remote).unwrap();
    assert_eq!(remote.arms, 1);
    assert!(restored.market.as_ref().unwrap().active.is_empty());
    drop(restored);
    let object = std::fs::read_dir(config.journal_directory.join("market-objects"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("object-")
        })
        .unwrap();
    std::fs::remove_file(object).unwrap();
    assert!(ControlInbox::open(config, &policy, &quota, policy.build().unwrap(), clock()).is_err());
}
