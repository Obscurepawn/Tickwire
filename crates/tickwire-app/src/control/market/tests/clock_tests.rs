use super::*;

#[test]
fn admitted_decision_expiring_before_wal_append_recovers_without_replay() {
    use crate::control::ControlInbox;
    let mut a = approval();
    let domain = a.previous.domain().unwrap();
    let clock = || tickwire_binance_clock::SystemClock::new(domain).unwrap();
    let dirs = Directories::new();
    let (mut policy, mut quota) = super::super::super::tests::inputs();
    policy.executor_id = a.previous.endpoint.local.instance;
    policy.domain.process_instance = domain.boot_id();
    let end = clock().sample().unwrap().monotonic.nanos() + 60_000_000_000;
    policy.owner_lease.as_mut().unwrap().valid_until_ns = end;
    quota.consumer_lease_until_ns = end;
    quota.policy_ends_ns = end;
    quota.windows[0].ends_ns = end;
    quota.grants[0].expires_ns = end;
    let mut config = crate::control::Config {
        inbox_directory: dirs.config.consumer_directory.clone(),
        journal_directory: dirs.root.join("wal"),
        stream_epoch: [31; 16],
        recover: false,
        segment_max_bytes: 4 << 20,
        maximum_commands: 8,
        maximum_per_poll: 8,
        startup_consumer_lease_until_ns: Some(end),
    };
    let (mut controls, _) = ControlInbox::open(
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
    let admitted_at = clock().sample().unwrap().monotonic;
    a.deadline_mono_ns = admitted_at.nanos() + 1;
    let decision = decision(&a);
    let mut history = History::default();
    history.record(decision.clone(), admitted_at, 8).unwrap();
    // A real clock sample was validated before the scheduler/IO boundary. The
    // next real sample is after expiry; no sleeps or widened validity are used.
    let synchronized = controls
        .market_boundary(Some((&decision, admitted_at)))
        .unwrap();
    assert!(synchronized.nanos() >= a.deadline_mono_ns);
    assert!(a.validate(&a.previous, synchronized).is_err());
    drop(controls);
    config.recover = true;
    let (mut restored, mut endpoint) =
        ControlInbox::open(config, &policy, &quota, policy.build().unwrap(), clock())
            .expect("valid admission must remain recoverable after synchronization expires");
    assert!(restored.state.market_history.contains(&decision).unwrap());
    restored
        .configure_market(dirs.config.clone(), None)
        .unwrap();
    dirs.put(decision.consumer, "arm", &a);
    let mut remote = Remote::new(decision.consumer);
    restored.poll_market(&mut endpoint, &mut remote).unwrap();
    assert_eq!(
        remote.arms, 0,
        "recovery consumes history without live dispatch"
    );
    assert!(restored.market.as_ref().unwrap().active.is_empty());
}

#[test]
fn durable_commit_expiry_does_not_start_the_new_subscription() {
    let a = approval();
    let d = decision(&a);
    let now = MonoTime::new(a.previous.domain().unwrap(), fixture::NOW);
    let dirs = Directories::new();
    dirs.put(d.consumer, "commit", &commit(&a));
    let receipt = Receipt::quiesced(&a, d.consumer.pid, 6, now).unwrap();
    let mut remote = Remote::new(d.consumer);
    remote.receipt = Some(receipt.clone());
    let mut history = History::default();
    history.record(d.clone(), now, 8).unwrap();
    let mut controller = Controller::new(dirs.config.clone(), None, history).unwrap();
    controller.active.insert(
        d.consumer,
        Operation {
            approval: a.clone(),
            commit: None,
            receipt: Some(receipt),
            finished: false,
        },
    );
    let mut durable = false;
    assert!(
        controller
            .poll(&d.binding, &mut remote, now, |decision| {
                if let Some((decision, _)) = decision {
                    assert!(matches!(decision.action, DecisionAction::Commit { .. }));
                    durable = true;
                }
                Ok(if durable {
                    MonoTime::new(now.domain(), a.deadline_mono_ns)
                } else {
                    now
                })
            })
            .is_err()
    );
    assert!(durable);
    assert_eq!(remote.commits, 0);
    assert!(controller.active[&d.consumer].commit.is_none());
}

#[test]
fn durable_files_recheck_deadline_and_binding_before_publishing_completion() {
    // Exercise both irreversible file boundaries with real fsync/rename. The
    // boundary clock advances only after the selected artifact is on disk.
    for after_shared in [false, true] {
        for binding_changed in [false, true] {
            let a = approval();
            let d = decision(&a);
            let now = MonoTime::new(a.previous.domain().unwrap(), fixture::NOW);
            let dirs = Directories::new();
            let c = commit(&a);
            let quiesced = Receipt::quiesced(&a, d.consumer.pid, 6, now).unwrap();
            dirs.put(d.consumer, "receipt", &quiesced);
            let mut activated = quiesced.clone();
            activated.phase = Phase::Activated;
            activated.next_descriptor_hash = Some(
                ResourceHeader::decode(&c.config.bank_header)
                    .unwrap()
                    .descriptor_hash,
            );
            activated.activated_sequence = Some(8);
            let mut remote = Remote::new(d.consumer);
            remote.receipt = Some(activated.clone());
            let mut controller =
                Controller::new(dirs.config.clone(), None, History::default()).unwrap();
            controller.active.insert(
                d.consumer,
                Operation {
                    approval: a.clone(),
                    commit: Some(c),
                    receipt: Some(quiesced),
                    finished: false,
                },
            );
            let effective = dirs
                .config
                .effective_directory
                .join(format!("{}.json", d.consumer.stem()));
            let shared = dirs
                .config
                .consumer_directory
                .join(format!("{}.receipt.json", d.consumer.stem()));
            let mut observed_boundary = false;
            let result = controller.poll(&d.binding, &mut remote, now, |decision| {
                assert!(decision.is_none());
                let reached = if after_shared {
                    Receipt::decode(&files::read(&shared, MAX_CONTROL_BYTES)?.unwrap())?.phase
                        == Phase::Activated
                } else {
                    effective.exists()
                };
                if reached {
                    observed_boundary = true;
                    if binding_changed {
                        return Err(invalid("control live owner identity changed"));
                    }
                    return Ok(MonoTime::new(now.domain(), a.deadline_mono_ns));
                }
                Ok(now)
            });
            assert!(result.is_err());
            assert!(observed_boundary);
            assert!(!controller.active[&d.consumer].finished);
            let stored =
                Receipt::decode(&files::read(&shared, MAX_CONTROL_BYTES).unwrap().unwrap())
                    .unwrap();
            assert_eq!(
                stored.phase,
                if after_shared {
                    Phase::Activated
                } else {
                    Phase::Quiesced
                }
            );
            assert_eq!(
                stored.observed_mono_ns,
                now.nanos(),
                "do not manufacture a new observation time"
            );
            assert!(
                a.validate_receipt(
                    &stored,
                    d.consumer.pid,
                    stored.phase,
                    MonoTime::new(now.domain(), a.deadline_mono_ns)
                )
                .is_err()
            );
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn actual_source_stays_closed_when_receipt_sync_crosses_the_deadline() {
    use crate::market::tests::{Command as PeerCommand, Peer, now};
    use tickwire_market_source::MarketSource;
    #[derive(Default)]
    struct Sink {
        observed: usize,
    }
    impl tickwire_market_source::MarketSink for Sink {
        fn observed(&mut self, _: tickwire_market_source::FeedObservation) -> bool {
            self.observed += 1;
            true
        }
        fn invalidated(&mut self, _: tickwire_market_source::Invalidation) -> bool {
            true
        }
    }
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
    let mut d = decision(&a);
    d.consumer = consumer;
    dirs.put(consumer, "arm", &a);
    let mut controller = Controller::new(
        dirs.config.clone(),
        Some(sources.clone()),
        History::default(),
    )
    .unwrap();
    let mut strategy =
        crate::strategy::FileStrategy::new(&crate::strategy::Config::Observe).unwrap();
    controller
        .poll(&d.binding, &mut strategy, now(domain), |_| Ok(now(domain)))
        .unwrap();
    peer.command.send(PeerCommand::Quiesce).unwrap();
    let mut sink = Sink::default();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let shared = dirs
        .config
        .consumer_directory
        .join(format!("{}.receipt.json", consumer.stem()));
    loop {
        assert!(std::time::Instant::now() < deadline);
        sources
            .poll(now(domain), 10_000_000_000, 32, &mut sink)
            .unwrap();
        if let Ok(commit) = peer.commit.try_recv() {
            dirs.put(consumer, "commit", &commit);
        }
        let result = controller.poll(&d.binding, &mut strategy, now(domain), |_| {
            let stored = files::read(&shared, MAX_CONTROL_BYTES)?
                .map(|b| Receipt::decode(&b))
                .transpose()?;
            Ok(if stored.is_some_and(|r| r.phase == Phase::Activated) {
                MonoTime::new(domain, a.deadline_mono_ns)
            } else {
                now(domain)
            })
        });
        if result.is_err() {
            break;
        }
        std::thread::yield_now();
    }
    assert_eq!(sources.receipt(&a).unwrap().phase, Phase::Activated);
    assert!(!controller.active[&consumer].finished);
    assert!(
        !sources.is_live(now(domain), 10_000_000_000),
        "real re-JOIN must remain held before release"
    );
    let previous = sink.observed;
    assert!(
        sources
            .poll(
                MonoTime::new(domain, a.deadline_mono_ns),
                10_000_000_000,
                32,
                &mut sink
            )
            .is_ok()
    );
    assert_eq!(
        sink.observed, previous,
        "expired transition must never announce the new baseline"
    );
}
