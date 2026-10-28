use super::*;
use crate::recovery::{Phase as RecoveryPhase, Snapshot, Store};
use sha2::{Digest, Sha256};
use std::{fs, os::unix::fs::PermissionsExt};
use tickwire_ipc::ResourceHeader;
use tickwire_market_source::protocol::{Instrument, wire_bytes};
use tickwire_quota::QuotaStore;

const SECOND: u64 = (1 << 63) | 1001;
const THIRD: u64 = u64::MAX - 1;

fn directory(f: &Fixture) -> PathBuf {
    f.config
        .expansion
        .as_ref()
        .unwrap()
        .recovery
        .as_ref()
        .unwrap()
        .directory
        .clone()
}
// Online observation only. The actual restart uses Store::open after the owned
// process exits and verifies the complete chain under its exclusive lease.
fn checkpoint(f: &Fixture) -> Option<([u8; 32], Snapshot)> {
    let root = directory(f);
    let bytes = match config::read(&root.join("HEAD"), 512) {
        Ok(bytes) => bytes,
        Err(e)
            if e.downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
        {
            return None;
        }
        Err(e) => panic!("HEAD read: {e}"),
    };
    let head: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let hash: [u8; 32] = serde_json::from_value(head["digest"].clone()).unwrap();
    let name: String = hash.iter().map(|b| format!("{b:02x}")).collect();
    let bytes = config::read(
        &root.join(format!("object-{name}")),
        crate::recovery::MAX_SNAPSHOT_BYTES,
    )
    .unwrap();
    assert_eq!(bytes.len() as u64, head["bytes"].as_u64().unwrap());
    assert_eq!(<[u8; 32]>::from(Sha256::digest(&bytes)), hash);
    let value: Snapshot = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value.sequence, head["sequence"].as_u64().unwrap());
    assert_eq!(
        crate::expansion::config_hash(&value.config).unwrap(),
        value.config_hash
    );
    Some((hash, value))
}
fn ready(f: &mut Fixture, revision: u64) -> ([u8; 32], Snapshot) {
    let end = Instant::now() + Duration::from_secs(6);
    loop {
        f.alive();
        if let Some((hash, value)) = checkpoint(f)
            && value.phase == RecoveryPhase::Ready
            && value.revision == revision
            && value.config.identity.identity() == f.config.identity.identity()
        {
            return (hash, value);
        }
        assert!(
            Instant::now() < end,
            "actual warmed MD did not persist a Ready recovery HEAD: {}",
            f.log()
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn finish_expansion(
    f: &mut Fixture,
    source: &mut ShmSource,
    old: &Admission,
    plan: &crate::expansion::Plan,
) {
    f.write_plan(plan);
    let path = f.path("arm");
    let approval = Approval::decode(&wait_file(f, source, &path)).unwrap();
    let receipt = source
        .arm_transition(approval, &old.client, f.now())
        .unwrap();
    config::write(&f.path("receipt"), &receipt).unwrap();
    let path = f.path("commit");
    let commit = Commit::decode(&wait_file(f, source, &path)).unwrap();
    assert_eq!(source.transition_receipt().unwrap().phase, Phase::Quiesced);
    source.commit_transition(commit, f.now()).unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    while source.transition_receipt().unwrap().phase != Phase::Activated {
        assert!(Instant::now() < end, "{}", f.log());
        drive(f, source);
    }
    config::write(&f.path("receipt"), source.transition_receipt().unwrap()).unwrap();
    source.release_transition(f.now()).unwrap();
    let path = f.config.expansion.as_ref().unwrap().result_path.clone();
    let end = Instant::now() + Duration::from_secs(5);
    let result: crate::engine::expansion::Record = loop {
        if path.exists() {
            let record: crate::engine::expansion::Record =
                serde_json::from_slice(&config::read(&path, 4 << 20).unwrap()).unwrap();
            if record.operation_id == plan.operation_id {
                break record;
            }
        }
        assert!(
            Instant::now() < end,
            "current expansion result missing: {}",
            f.log()
        );
        drive(f, source);
    };
    assert_eq!(
        result.status,
        "complete",
        "{} / {}",
        result.error.unwrap_or_default(),
        f.log()
    );
    assert!(result.runtime_applied && result.effective_installed);
    assert_eq!(result.revision, plan.target_revision);
    let (_, snapshot) = ready(f, plan.target_revision);
    assert!(
        snapshot
            .operations
            .iter()
            .any(|o| o.operation_id == plan.operation_id)
    );
}
fn restart_config(f: &mut Fixture, old: &Snapshot, hash: [u8; 32]) -> config::Config {
    let root = f.config.quota_directory.parent().unwrap().join("next");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let mut next = old.config.clone();
    next.identity.instance = [50; 16];
    next.identity.generation += 1;
    next.socket = root.join("market.sock");
    next.admission_directory = root.join("admissions");
    next.control_directory = root.join("journal");
    for path in [&next.admission_directory, &root.join("consumers")] {
        fs::create_dir(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    next.quota.consumer_instance = next.identity.instance;
    next.quota.consumer_generation = next.identity.generation;
    next.quota.supervisor_generation += 1;
    for grant in &mut next.quota.grants {
        grant.request_id += 100;
    }
    for (i, feed) in next.feeds.iter_mut().enumerate() {
        feed.owner_id = [60 + i as u8; 16];
        feed.first_connection_epoch += 10;
        feed.first_operation += 10;
        feed.initial_book_generation += 10;
        for (j, event) in feed.events.iter_mut().enumerate() {
            event.owner_id = [80 + (i * 3 + j) as u8; 16];
            event.first_connection_epoch += 10;
            event.first_operation += 10;
        }
    }
    let control = next.expansion.as_mut().unwrap();
    control.plan_path = root.join("plan.json");
    control.result_path = root.join("result.json");
    control.effective_path = root.join("effective.json");
    control.consumer_directory = root.join("consumers");
    control.initial_revision = old.revision;
    control.recovery.as_mut().unwrap().restart_from = Some(hash);
    for (i, topic) in next.topics.iter_mut().enumerate() {
        let mut bank = ResourceHeader::decode(&old.banks[i]).unwrap();
        bank.producer_instance = next.identity.instance;
        bank.producer_generation = next.identity.generation;
        bank.resource_id = [51 + i as u8; 16];
        bank.channel_id = [53 + i as u8; 16];
        bank.ring_epoch += 1;
        bank.feed_generation += 1;
        bank.predecessor_ring_epoch = 0;
        bank.cutover_seq = 0;
        bank.first_seq = 1;
        bank.created_mono_ns = f.now().nanos();
        bank.descriptor_hash = [0; 32];
        bank.initial_membership_version = 1;
        topic.resource = root.join(format!("topic-{i}.bin"));
        config::write_bytes(&topic.resource, &wire_bytes(&bank).unwrap()).unwrap();
    }
    next
}
fn new_admission(f: &Fixture) -> Admission {
    let mut admission = f.admission();
    admission.client.endpoint.local.generation += 1;
    admission.client.endpoint.control_epoch += 10;
    admission.client.endpoint.nonce = [71; 32];
    admission.client.subscription_generation = 10;
    admission.client.request_id = [72; 16];
    admission.client.cursor_id = [73; 16];
    admission.snapshot_id = [74; 16];
    admission.client.instrument_set_version = f.config.topics[0].instrument_set_version;
    admission.client.instruments = f
        .config
        .feeds
        .iter()
        .map(|feed| Instrument {
            id: feed.instrument.id,
            version: feed.instrument.version,
            required_states: feed.events.iter().fold(1, |bits, e| bits | e.kind.state()),
        })
        .collect();
    admission.client.instruments.sort_by_key(|i| i.id);
    admission
}
fn quota_state(f: &Fixture) -> serde_json::Value {
    let mut store = tickwire_quota::files::FileQuotaStore::open(&f.config.quota_directory).unwrap();
    let state = store.load().unwrap();
    let bytes = config::read(&f.config.quota_directory.join("STATE"), 4 << 20).unwrap();
    assert_eq!(&bytes[..8], b"TWQUOTA1");
    assert_eq!(&bytes[8..40], Sha256::digest(&bytes[40..]).as_slice());
    let value: serde_json::Value = serde_json::from_slice(&bytes[40..]).unwrap();
    assert_eq!(
        value["supervisor_generation"].as_u64(),
        Some(state.supervisor_generation())
    );
    value
}
fn rejected_process(f: &mut Fixture, reason: &str) {
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(exit) = f.child.try_wait().unwrap() {
            assert!(!exit.success());
            break;
        }
        assert!(
            Instant::now() < end,
            "rejected MD is still live: {}",
            f.log()
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(f.log().contains(reason), "{}", f.log());
    assert!(!f.config.socket.exists());
}

#[test]
fn cold_restart_persists_ready_checkpoint_after_actual_prewarm() {
    let mut f = Fixture::new_with_recovery();
    let admission = f.admission();
    let source = f.connect(&admission);
    assert_eq!(
        source.client().replica.instruments().collect::<Vec<_>>(),
        [u64::MAX]
    );
    let (hash, online) = ready(&mut f, 1);
    f.stop_process();
    drop(source);
    let store = Store::open(&directory(&f)).unwrap();
    assert_eq!(store.current_hash(), Some(hash));
    assert_eq!(store.current().unwrap().phase, RecoveryPhase::Ready);
    assert_eq!(store.current().unwrap().instrument_ids, [u64::MAX]);
    assert_eq!(store.current().unwrap().sequence, online.sequence);
}

#[test]
fn completed_expansion_restarts_into_a_new_process_and_can_expand_again() {
    let mut f = Fixture::new_with_recovery();
    let first_pid = f.child.id();
    let first_resource = f.bank().resource_id;
    let old = f.admission();
    let mut source = f.connect(&old);
    let (initial_hash, _) = ready(&mut f, 1);
    let plan = f.plan(&old); // External IDs are deliberately not in sorted slot order.
    finish_expansion(&mut f, &mut source, &old, &plan);
    assert_eq!(
        source.client().replica.instruments().collect::<Vec<_>>(),
        [SECOND, u64::MAX]
    );
    f.stop_process();
    drop(source);
    let store = Store::open(&directory(&f)).unwrap();
    let completed = store.current().unwrap().clone();
    let hash = store.current_hash().unwrap();
    drop(store);
    assert_ne!(hash, initial_hash);
    assert_eq!(completed.phase, RecoveryPhase::Ready);
    assert_eq!(completed.instrument_ids, [u64::MAX, SECOND]);
    let before_quota = quota_state(&f);
    assert_eq!(before_quota["grants"].as_array().unwrap().len(), 1);
    let next = restart_config(&mut f, &completed, hash);
    let mut stale = next.clone();
    stale
        .expansion
        .as_mut()
        .unwrap()
        .recovery
        .as_mut()
        .unwrap()
        .restart_from = Some(initial_hash);
    f.spawn_config(stale, "stale-head");
    rejected_process(&mut f, "MD restart requires the exact current checkpoint");
    assert_eq!(quota_state(&f), before_quota);
    f.spawn_config(next, "restarted");
    let second_pid = f.child.id();
    assert_ne!(first_pid, second_pid);
    let (_, restarted) = ready(&mut f, 2);
    assert_eq!(restarted.instrument_ids, completed.instrument_ids);
    let index = config::FeedIndex::restore(&f.config.feeds, &restarted.instrument_ids).unwrap();
    for feed in &f.config.feeds {
        assert_eq!(
            index.book_key(feed).unwrap().instrument,
            if feed.instrument.id == u64::MAX { 0 } else { 1 }
        );
    }
    // Copying a historical private admission into the new launcher directory
    // grants no access. Redirect only the transport to reach the actual new PID;
    // the persisted admission and claimed protocol identity remain the old ones.
    register_admission(&f.config.admission_directory, &old).unwrap();
    let mut replayed = old.clone();
    replayed.client.endpoint.path = f.config.socket.clone();
    replayed.client.endpoint.peer_pid = second_pid;
    assert!(matches!(
        f.connect_client(&replayed),
        Err(tickwire_market_source::Error::Transport(
            tickwire_control_sys::Error::Closed
        ))
    ));
    f.alive();
    assert!(f.log().contains("admitted process identity"), "{}", f.log());
    let admission = new_admission(&f);
    let mut source = f.connect(&admission);
    assert_eq!(
        source.client().replica.instruments().collect::<Vec<_>>(),
        [SECOND, u64::MAX]
    );
    let start = source.client().consumer.watermarks().applied;
    let end = Instant::now() + Duration::from_secs(3);
    while source.client().consumer.watermarks().applied <= start {
        assert!(Instant::now() < end);
        drive(&mut f, &mut source);
    }
    let root = f.config.quota_directory.parent().unwrap();
    let mut third = network::Network::start(&root.join("third.der"), "SOLUSDT", 299, false);
    let mut addition = f.addition.clone();
    addition.instrument.id = THIRD;
    addition.instrument.symbol = "SOLUSDT".into();
    addition.instrument.base.id = 3;
    addition.instrument.base.symbol = "SOL".into();
    addition.owner_id = [100; 16];
    addition.stream_id = 3;
    addition.stream_address = third.stream.clone();
    addition.snapshot_address = third.snapshot.clone();
    addition.trust_anchors = vec![root.join("third.der")];
    addition.events.clear();
    let bank = f.bank();
    let mut template = bank.clone();
    template.resource_id = [108; 16];
    assert_ne!(template.resource_id, first_resource);
    assert_ne!(template.resource_id, bank.resource_id);
    template.ring_epoch += 1;
    template.feed_generation += 1;
    template.predecessor_ring_epoch = bank.ring_epoch;
    template.first_seq = 1;
    template.cutover_seq = 0;
    template.created_mono_ns = 0;
    template.descriptor_hash = [0; 32];
    let mut target = f.config.clone();
    target.feeds.push(addition);
    target.topics[0].instrument_set_version += 1;
    let mut new = admission.clone();
    new.client.bank_header = wire_bytes(&template).unwrap();
    new.client.instrument_set_version = target.topics[0].instrument_set_version;
    new.client.instruments.push(Instrument {
        id: THIRD,
        version: 1,
        required_states: 1,
    });
    new.client.instruments.sort_by_key(|i| i.id);
    new.client.subscription_generation += 1;
    new.client.request_id = [102; 16];
    new.client.cursor_id = [103; 16];
    new.snapshot_id = [104; 16];
    new.client.endpoint.nonce = [105; 32];
    new.client.endpoint.control_epoch += 1;
    let plan2 = crate::expansion::Plan {
        schema: 1,
        operation_id: [106; 16],
        base_revision: 2,
        base_config_hash: crate::expansion::config_hash(&f.config).unwrap(),
        base_bank: wire_bytes(&bank).unwrap(),
        target_revision: 3,
        target_bank: wire_bytes(&template).unwrap(),
        target_config: target,
        topic_id: 101,
        consumers: vec![crate::expansion::Consumer {
            previous: admission.clone(),
            next: new,
        }],
        warmup_deadline_mono_ns: f.now().nanos() + 8_000_000_000,
        maximum_age_ns: 1_000_000_000,
        peak_cache_bytes: 3 << 20,
        maximum_retained_bank_bytes: bank.resource_bytes * 2,
    };
    // A retired bank from the first PID is still consumed by the recovered
    // history, even though it differs from the current bank and all new epochs.
    let mut reused = plan2.clone();
    reused.operation_id = [107; 16];
    let mut reused_bank = template.clone();
    reused_bank.resource_id = first_resource;
    assert_ne!(reused_bank.resource_id, bank.resource_id);
    reused.target_bank = wire_bytes(&reused_bank).unwrap();
    reused.consumers[0]
        .next
        .client
        .bank_header
        .clone_from(&reused.target_bank);
    f.write_plan(&reused);
    let rejected_path = f.config.expansion.as_ref().unwrap().result_path.clone();
    let end = Instant::now() + Duration::from_secs(3);
    let rejected: crate::engine::expansion::Record = loop {
        assert!(
            !f.path("arm").exists(),
            "historical bank reuse reached actual candidate Arm; new source connections={}",
            third.connections.load(std::sync::atomic::Ordering::Relaxed)
        );
        if rejected_path.exists() {
            let result: crate::engine::expansion::Record =
                serde_json::from_slice(&config::read(&rejected_path, 4 << 20).unwrap()).unwrap();
            if result.operation_id == reused.operation_id {
                break result;
            }
        }
        assert!(
            Instant::now() < end,
            "historical bank rejection missing: {}",
            f.log()
        );
        drive(&mut f, &mut source);
    };
    assert_eq!(rejected.status, "rejected");
    assert!(!rejected.runtime_applied && !rejected.effective_installed);
    assert_eq!(
        third.connections.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(f.bank(), bank);
    assert_eq!(
        source.client().replica.instruments().collect::<Vec<_>>(),
        [SECOND, u64::MAX]
    );
    finish_expansion(&mut f, &mut source, &admission, &plan2);
    assert_eq!(
        source.client().replica.instruments().collect::<Vec<_>>(),
        [SECOND, THIRD, u64::MAX]
    );
    let (final_hash, final_state) = ready(&mut f, 3);
    assert_eq!(final_state.instrument_ids, [u64::MAX, SECOND, THIRD]);
    assert_eq!(f.child.id(), second_pid);
    f.stop_process();
    drop(source);
    third.stop();
    let store = Store::open(&directory(&f)).unwrap();
    assert_eq!(store.current_hash(), Some(final_hash));
    assert_eq!(store.current().unwrap().operations.len(), 3);
    drop(store);
    let after_quota = quota_state(&f);
    assert_eq!(after_quota["supervisor_generation"].as_u64(), Some(2));
    assert_eq!(after_quota["windows"], before_quota["windows"]);
    assert_eq!(after_quota["grants"].as_array().unwrap().len(), 2);
    assert_eq!(after_quota["grants"][0], before_quota["grants"][0]);
    println!(
        "{}",
        serde_json::json!({"event":"md_cold_restart_verified","old_pid":first_pid,"new_pid":second_pid,"revision":3,"instrument_ids":final_state.instrument_ids,"quota_grants":2,"recovery_directory":directory(&f),"checkpoint":final_hash})
    );
}

#[test]
fn non_ready_checkpoint_cannot_start_a_new_producer_or_spend_new_quota() {
    for phase in [
        RecoveryPhase::Starting,
        RecoveryPhase::Reserved,
        RecoveryPhase::Revoking,
        RecoveryPhase::Applied,
    ] {
        let mut f = Fixture::new_with_recovery();
        let old = f.admission();
        let source = f.connect(&old);
        let (hash, mut incomplete) = ready(&mut f, 1);
        f.stop_process();
        drop(source);
        // Model a crash at an explicitly persisted non-complete boundary, using
        // the real exclusive store API and its full validated history chain.
        let mut store = Store::open(&directory(&f)).unwrap();
        incomplete.sequence += 1;
        incomplete.previous = hash;
        incomplete.phase = phase;
        let incomplete_hash = store.write(incomplete.clone()).unwrap();
        drop(store);
        let before_quota = quota_state(&f);
        let next = restart_config(&mut f, &incomplete, incomplete_hash);
        f.spawn_config(next, "non-ready");
        rejected_process(&mut f, "MD restart phase/revision/producer/clock/quota");
        assert_eq!(quota_state(&f), before_quota);
        let store = Store::open(&directory(&f)).unwrap();
        assert_eq!(store.current_hash(), Some(incomplete_hash));
        assert_eq!(store.current().unwrap().phase, phase);
    }
}

#[test]
fn zero_operation_rejects_before_quiesce_without_poisoning_durable_history() {
    let mut f = Fixture::new_with_recovery();
    let admission = f.admission();
    let mut source = f.connect(&admission);
    let (_, before) = ready(&mut f, 1);
    let mut plan = f.plan(&admission);
    plan.operation_id = [0; 16];
    // This is well-formed JSON and a decoded Plan, with one inadmissible identity.
    // It must not become a Consumed record that poisons the durable store.
    f.write_plan(&plan);
    let result_path = f.config.expansion.as_ref().unwrap().result_path.clone();
    let end = Instant::now() + Duration::from_secs(3);
    while !result_path.exists() {
        f.alive();
        assert!(
            Instant::now() < end,
            "rejection receipt missing: {}",
            f.log()
        );
        source
            .poll(f.now(), 1_000_000_000, 64, &mut Sink)
            .unwrap_or_else(|error| {
                panic!(
                    "rejected plan broke actual source: {error:?}; MD log: {}",
                    f.log()
                )
            });
        std::thread::sleep(Duration::from_millis(1));
    }
    let result: crate::engine::expansion::Record =
        serde_json::from_slice(&config::read(&result_path, 4 << 20).unwrap()).unwrap();
    assert_eq!(result.status, "rejected");
    assert!(!result.runtime_applied && !result.effective_installed);
    assert!(
        result
            .error
            .unwrap()
            .contains("base/revision/deadline/bounds")
    );
    assert!(!f.path("arm").exists());
    let applied = source.client().consumer.watermarks().applied;
    let end = Instant::now() + Duration::from_secs(2);
    while source.client().consumer.watermarks().applied <= applied {
        assert!(Instant::now() < end);
        drive(&mut f, &mut source);
    }
    let (_, after) = ready(&mut f, 1);
    assert_eq!(after.operations, before.operations);
    assert_eq!(after.instrument_ids, before.instrument_ids);
    assert_eq!(after.config_hash, before.config_hash);
    assert_eq!(after.banks, before.banks);
    f.stop_process();
    drop(source);
    let store = Store::open(&directory(&f)).unwrap();
    assert_eq!(store.current().unwrap().phase, RecoveryPhase::Ready);
    assert!(store.current().unwrap().operations.is_empty());
}

#[test]
fn killed_incomplete_expansion_is_not_a_completed_restart_checkpoint() {
    for phase in [
        RecoveryPhase::Reserved,
        RecoveryPhase::Revoking,
        RecoveryPhase::Applied,
    ] {
        let mut f = Fixture::new_with_recovery();
        let old_pid = f.child.id();
        let admission = f.admission();
        let mut source = f.connect(&admission);
        ready(&mut f, 1);
        let plan = f.plan(&admission);
        f.write_plan(&plan);
        let arm_path = f.path("arm");
        let approval = Approval::decode(&wait_file(&mut f, &mut source, &arm_path)).unwrap();
        // Arm output proves candidate prewarm actually completed. Reserved has
        // no consumer approval; Revoking has real Armed, but no QUIESCED reply.
        // Applied has real QUIESCED and a delivered Commit, but no new JOIN.
        if phase != RecoveryPhase::Reserved {
            let receipt = source
                .arm_transition(approval, &admission.client, f.now())
                .unwrap();
            config::write(&f.path("receipt"), &receipt).unwrap();
        }
        if phase == RecoveryPhase::Applied {
            let commit_path = f.path("commit");
            let commit = Commit::decode(&wait_file(&mut f, &mut source, &commit_path)).unwrap();
            assert_eq!(commit.operation_id, plan.operation_id);
            assert_eq!(source.transition_receipt().unwrap().phase, Phase::Quiesced);
            // Deliberately leave the actual client before commit_transition.
        }
        let end = Instant::now() + Duration::from_secs(3);
        let (observed_hash, observed) = loop {
            f.alive();
            if let Some((hash, value)) = checkpoint(&f)
                && value.phase == phase
            {
                break (hash, value);
            }
            assert!(
                Instant::now() < end,
                "actual {phase:?} checkpoint missing: {}",
                f.log()
            );
            std::thread::sleep(Duration::from_millis(1));
        };
        assert_eq!(
            observed.revision,
            if phase == RecoveryPhase::Applied {
                2
            } else {
                1
            }
        );
        assert!(
            observed
                .operations
                .iter()
                .any(|o| o.operation_id == plan.operation_id)
        );
        assert_eq!(f.path("commit").exists(), phase == RecoveryPhase::Applied);
        // Child::kill sends SIGKILL on this Linux-only process fixture.
        f.stop_process();
        drop(source);
        let store = Store::open(&directory(&f)).unwrap();
        assert_eq!(store.current_hash(), Some(observed_hash));
        let current = store.current().unwrap().clone();
        assert_eq!(current.phase, phase);
        drop(store);
        let before_quota = quota_state(&f);
        let next = restart_config(&mut f, &current, observed_hash);
        f.spawn_config(next, "killed-incomplete");
        assert_ne!(f.child.id(), old_pid);
        rejected_process(&mut f, "MD restart phase/revision/producer/clock/quota");
        assert_eq!(quota_state(&f), before_quota);
        let store = Store::open(&directory(&f)).unwrap();
        assert_eq!(store.current_hash(), Some(observed_hash));
        println!(
            "{}",
            serde_json::json!({"event":"md_incomplete_restart_rejected","phase":phase,"killed_pid":old_pid,"rejected_pid":f.child.id(),"checkpoint":observed_hash})
        );
    }
}

// Read-only test observation of this owned child's exact bank. This is neither
// a subscriber nor an authorization: the live stream is consumed through JOIN
// below. ABI: immutable header 4096 bytes, published sequence at 4096, fixed
// record slots at 8192. No slot is read after its first possible wrap.
fn live_depth_quote(f: &Fixture) -> Option<u64> {
    use std::os::unix::fs::FileExt;
    use tickwire_ipc::MarketRecord;
    let bank = f.bank();
    for entry in fs::read_dir(format!("/proc/{}/fd", f.child.id())).unwrap() {
        let path = entry.unwrap().path();
        let Ok(target) = fs::read_link(&path) else {
            continue;
        };
        if !target.to_string_lossy().contains("memfd:tickwire-bank") {
            continue;
        }
        let Ok(file) = fs::File::open(path) else {
            continue;
        };
        let mut header = [0; 4096];
        file.read_exact_at(&mut header, 0).unwrap();
        if ResourceHeader::decode(&header).unwrap() != bank {
            continue;
        }
        let mut bytes = [0; 8];
        file.read_exact_at(&mut bytes, 4096).unwrap();
        let published = u64::from_le_bytes(bytes);
        assert!(
            published < bank.first_seq + u64::from(bank.capacity),
            "observation cannot read overwritten slots"
        );
        assert_eq!(bank.record_bytes, 512);
        for sequence in bank.first_seq..=published {
            let mut record = [0; 512];
            let slot = (sequence - bank.first_seq) & u64::from(bank.capacity - 1);
            file.read_exact_at(&mut record, 8192 + slot * 512).unwrap();
            let record = MarketRecord::decode(&record).unwrap();
            assert_eq!(record.header.sequence, sequence);
            if record.header.message_type == 257 && record.body.feed_state == 2 {
                assert_eq!(record.header.instrument_id, u64::MAX);
                assert_eq!(record.body.bid_price_ticks, 9900); // Fixture tick_size = 0.01.
                assert_eq!(record.body.ask_price_ticks, 10100);
                return Some(sequence);
            }
        }
    }
    None
}
fn depth_live_trade_pending(f: &mut Fixture) -> u64 {
    use std::sync::atomic::Ordering;
    let end = Instant::now() + Duration::from_secs(3);
    loop {
        f.alive();
        if f.old.gate.event_subscriptions.load(Ordering::Relaxed) == 1
            && f.old.gate.snapshots.load(Ordering::Relaxed) >= 1
            && let Some(sequence) = live_depth_quote(f)
        {
            let (_, head) = checkpoint(f).unwrap();
            assert_eq!(head.phase, RecoveryPhase::Starting);
            assert!(f.old.gate.paused_events.load(Ordering::Relaxed));
            assert_eq!(f.old.gate.events_sent.load(Ordering::Relaxed), 0);
            assert!(!f.config.expansion.as_ref().unwrap().effective_path.exists());
            return sequence;
        }
        assert!(
            Instant::now() < end,
            "depth Live/Trade pending not reached: {}",
            f.log()
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn starting_fault_reconnect_can_reserve_a_new_bank_and_finish_prewarm() {
    use std::sync::atomic::Ordering;
    let mut f = Fixture::new_with_recovery_paused_trade();
    let quote = depth_live_trade_pending(&mut f);
    let old = f.bank();
    f.old.gate.disconnect_depth.store(true, Ordering::Relaxed);
    let end = Instant::now() + Duration::from_secs(4);
    loop {
        f.alive();
        if f.old.gate.depth_subscriptions.load(Ordering::Relaxed) >= 2
            && f.old.gate.snapshots.load(Ordering::Relaxed) >= 2
            && f.log().contains("md_topic_invalidated")
        {
            break;
        }
        assert!(
            Instant::now() < end,
            "actual depth reconnect/Gap missing: {}",
            f.log()
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(checkpoint(&f).unwrap().1.phase, RecoveryPhase::Starting);
    let mut replacement = old.clone();
    replacement.resource_id = [109; 16];
    replacement.ring_epoch += 1;
    replacement.feed_generation += 1;
    replacement.created_mono_ns = f.now().nanos();
    replacement.descriptor_hash = [0; 32];
    config::write_bytes(
        &f.config.topics[0].resource,
        &wire_bytes(&replacement).unwrap(),
    )
    .unwrap();
    f.old.gate.paused_events.store(false, Ordering::Relaxed);
    let end = Instant::now() + Duration::from_secs(2);
    while f.old.gate.events_sent.load(Ordering::Relaxed) == 0 {
        f.alive();
        assert!(
            Instant::now() < end,
            "Trade gate did not release actual data"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    println!(
        "{}",
        serde_json::json!({"event":"md_starting_fault_preconditions","pid":f.child.id(),"depth_live_sequence":quote,"depth_subscriptions":f.old.gate.depth_subscriptions.load(Ordering::Relaxed),"snapshots":f.old.gate.snapshots.load(Ordering::Relaxed),"trade_records":f.old.gate.events_sent.load(Ordering::Relaxed),"old_resource":old.resource_id,"replacement_resource":replacement.resource_id})
    );
    let (hash, current) = ready(&mut f, 1);
    assert_eq!(f.bank().resource_id, replacement.resource_id);
    assert_eq!(f.bank().predecessor_ring_epoch, old.ring_epoch);
    assert!(f.bank().cutover_seq >= quote);
    assert!(
        current.sequence > 2,
        "Starting bank reservation must precede Ready"
    );
    assert_eq!(ResourceHeader::decode(&current.banks[0]).unwrap(), f.bank());
    let previous: String = current
        .previous
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let reserved_bytes = config::read(
        &directory(&f).join(format!("object-{previous}")),
        crate::recovery::MAX_SNAPSHOT_BYTES,
    )
    .unwrap();
    assert_eq!(
        <[u8; 32]>::from(Sha256::digest(&reserved_bytes)),
        current.previous
    );
    let reserved: Snapshot = serde_json::from_slice(&reserved_bytes).unwrap();
    assert_eq!(reserved.phase, RecoveryPhase::Starting);
    assert_eq!(reserved.banks, current.banks);
    assert_eq!(reserved.sequence + 1, current.sequence);
    let admission = f.admission();
    let mut source = f.connect(&admission);
    assert_eq!(
        source.client().replica.instruments().collect::<Vec<_>>(),
        [u64::MAX]
    );
    let applied = source.client().consumer.watermarks().applied;
    let end = Instant::now() + Duration::from_secs(2);
    while source.client().consumer.watermarks().applied <= applied {
        assert!(Instant::now() < end);
        drive(&mut f, &mut source);
    }
    f.stop_process();
    drop(source);
    let store = Store::open(&directory(&f)).unwrap();
    assert_eq!(store.current_hash(), Some(hash));
    assert!(store.seen_resource(old.resource_id));
    assert!(store.seen_resource(replacement.resource_id));
    println!(
        "{}",
        serde_json::json!({"event":"md_starting_fault_recovered","pid":f.child.id(),"depth_live_sequence":quote,"checkpoint":hash,"recovery_directory":directory(&f)})
    );
}

#[test]
fn killed_starting_process_without_trade_prewarm_cannot_restart() {
    let mut f = Fixture::new_with_recovery_paused_trade();
    let quote = depth_live_trade_pending(&mut f);
    let old_pid = f.child.id();
    let (hash, starting) = checkpoint(&f).unwrap();
    assert_eq!(starting.phase, RecoveryPhase::Starting);
    f.stop_process();
    let before_quota = quota_state(&f);
    let next = restart_config(&mut f, &starting, hash);
    f.spawn_config(next, "killed-starting");
    rejected_process(&mut f, "MD restart phase/revision/producer/clock/quota");
    assert_eq!(quota_state(&f), before_quota);
    let store = Store::open(&directory(&f)).unwrap();
    assert_eq!(store.current_hash(), Some(hash));
    println!(
        "{}",
        serde_json::json!({"event":"md_incomplete_restart_rejected","phase":"starting","killed_pid":old_pid,"rejected_pid":f.child.id(),"checkpoint":hash,"depth_live_sequence":quote})
    );
}
