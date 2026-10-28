use super::*;
use tickwire_market_source::transition::{Approval, Commit, Phase};
use tickwire_market_source::{FeedObservation, Invalidation, MarketSink, MarketSource, ShmSource};
mod fixture;
mod network;
mod restart;
mod restart_failure;
use fixture::Fixture;

#[test]
fn process_entry() {
    let Ok(path) = std::env::var("TICKWIRE_MD_RUNTIME_TEST_CONFIG") else {
        return;
    };
    let config = serde_json::from_slice(&config::read(Path::new(&path), 4 << 20).unwrap()).unwrap();
    crate::engine::run(config).unwrap();
}
struct Sink;
impl MarketSink for Sink {
    fn observed(&mut self, _: FeedObservation) -> bool {
        true
    }
    fn invalidated(&mut self, _: Invalidation) -> bool {
        true
    }
}
fn drive(f: &mut Fixture, source: &mut ShmSource) {
    f.alive();
    source.poll(f.now(), 1_000_000_000, 64, &mut Sink).unwrap();
    std::thread::sleep(Duration::from_millis(1));
}
fn wait_file(f: &mut Fixture, source: &mut ShmSource, path: &Path) -> Vec<u8> {
    let end = Instant::now() + Duration::from_secs(6);
    loop {
        if path.exists() {
            return config::read(path, 4 << 20).unwrap();
        }
        assert!(Instant::now() < end, "waiting {path:?}: {}", f.log());
        drive(f, source);
    }
}

#[test]
fn real_process_expands_with_old_connections_live_and_actual_consumer_rejoin() {
    let mut f = Fixture::new();
    let pid = f.child.id();
    let admission = f.admission();
    let mut source = f.connect(&admission);
    let plan = f.plan(&admission);
    f.write_plan(&plan);
    let arm_path = f.path("arm");
    let approval = Approval::decode(&wait_file(&mut f, &mut source, &arm_path)).unwrap();
    // Preparing cannot revoke the old cursor before a real consumer has armed.
    let before = source.client().consumer.watermarks().applied;
    let until = Instant::now() + Duration::from_millis(120);
    while Instant::now() < until {
        drive(&mut f, &mut source);
    }
    assert!(source.client().consumer.watermarks().applied > before);
    assert_eq!(f.bank().feed_generation, 1);
    let receipt = source
        .arm_transition(approval, &admission.client, f.now())
        .unwrap();
    config::write(&f.path("receipt"), &receipt).unwrap();
    let commit_path = f.path("commit");
    let commit = Commit::decode(&wait_file(&mut f, &mut source, &commit_path)).unwrap();
    assert_eq!(source.transition_receipt().unwrap().phase, Phase::Quiesced);
    source.commit_transition(commit, f.now()).unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while source.transition_receipt().unwrap().phase != Phase::Activated {
        assert!(Instant::now() < until, "{}", f.log());
        drive(&mut f, &mut source);
    }
    let result_path = f.config.expansion.as_ref().unwrap().result_path.clone();
    let until = Instant::now() + Duration::from_millis(60);
    while Instant::now() < until {
        drive(&mut f, &mut source);
    }
    assert!(
        !result_path.exists(),
        "actual Active alone cannot replace the consumer effective receipt"
    );
    config::write(&f.path("receipt"), source.transition_receipt().unwrap()).unwrap();
    source.release_transition(f.now()).unwrap();
    let result_path = f.config.expansion.as_ref().unwrap().result_path.clone();
    let record: crate::engine::expansion::Record =
        serde_json::from_slice(&wait_file(&mut f, &mut source, &result_path)).unwrap();
    assert_eq!(
        record.status,
        "complete",
        "{} / {}",
        record.error.unwrap_or_default(),
        f.log()
    );
    assert!(record.runtime_applied && record.effective_installed);
    assert_eq!(record.revision, 2);
    assert_eq!(f.child.id(), pid);
    f.assert_original_connections();
    assert_eq!(
        source.client().replica.instruments().collect::<Vec<_>>(),
        vec![(1 << 63) | 1001, u64::MAX]
    );
    let start = source.client().consumer.watermarks().applied;
    let until = Instant::now() + Duration::from_millis(150);
    while Instant::now() < until {
        drive(&mut f, &mut source);
    }
    assert!(source.client().consumer.watermarks().applied > start);
    let file = config::read(
        &f.config.expansion.as_ref().unwrap().effective_path,
        4 << 20,
    )
    .unwrap();
    let effective: crate::engine::expansion::Effective = serde_json::from_slice(&file).unwrap();
    assert_eq!(effective.revision, record.revision);
    assert_eq!(effective.config_hash, record.config_hash);
    assert_eq!(
        crate::expansion::config_hash(&effective.config).unwrap(),
        record.config_hash
    );
    let completed_bank = f.bank();
    f.write_plan(&plan);
    let until = Instant::now() + Duration::from_millis(60);
    while Instant::now() < until {
        drive(&mut f, &mut source);
    }
    assert_eq!(f.bank(), completed_bank);
    f.assert_original_connections();
    let mut conflicting = plan.clone();
    conflicting.maximum_age_ns += 1;
    f.write_plan(&conflicting);
    let until = Instant::now() + Duration::from_secs(2);
    loop {
        let value: crate::engine::expansion::Record =
            serde_json::from_slice(&config::read(&result_path, 4 << 20).unwrap()).unwrap();
        if value.status == "rejected" {
            break;
        }
        assert!(Instant::now() < until);
        drive(&mut f, &mut source);
    }
    assert_eq!(f.bank(), completed_bank);
    f.stop();
}

#[test]
fn rejected_plan_cannot_reuse_its_operation_with_corrected_content() {
    let mut f = Fixture::new();
    let admission = f.admission();
    let mut source = f.connect(&admission);
    let plan = f.plan(&admission);
    let mut invalid = plan.clone();
    invalid.base_revision += 1;
    f.write_plan(&invalid);
    let result_path = f.config.expansion.as_ref().unwrap().result_path.clone();
    let record: crate::engine::expansion::Record =
        serde_json::from_slice(&wait_file(&mut f, &mut source, &result_path)).unwrap();
    assert_eq!(record.status, "rejected");
    std::fs::remove_file(&result_path).unwrap();
    f.write_plan(&plan);
    let until = Instant::now() + Duration::from_secs(2);
    while !result_path.exists() && !f.path("arm").exists() {
        assert!(Instant::now() < until, "{}", f.log());
        drive(&mut f, &mut source);
    }
    assert!(
        !f.path("arm").exists(),
        "rejected operation changed content"
    );
    let record: crate::engine::expansion::Record =
        serde_json::from_slice(&config::read(&result_path, 4 << 20).unwrap()).unwrap();
    assert_eq!(record.status, "rejected");
    assert!(record.error.unwrap().contains("different plan"));
    assert_eq!(f.bank().feed_generation, 1);
    f.stop();
}

#[test]
fn missing_real_quiescence_expires_closed_without_committing_a_successor() {
    let mut f = Fixture::new();
    let admission = f.admission();
    let mut source = f.connect(&admission);
    let mut plan = f.plan(&admission);
    plan.warmup_deadline_mono_ns = f.now().nanos() + 1_500_000_000;
    f.write_plan(&plan);
    let path = f.path("arm");
    let approval = Approval::decode(&wait_file(&mut f, &mut source, &path)).unwrap();
    let receipt = source
        .arm_transition(approval, &admission.client, f.now())
        .unwrap();
    config::write(&f.path("receipt"), &receipt).unwrap();
    // Do not service this real consumer after Armed: no mapped QUIESCED proof exists.
    let result_path = f.config.expansion.as_ref().unwrap().result_path.clone();
    let until = Instant::now() + Duration::from_secs(3);
    while !result_path.exists() {
        f.alive();
        assert!(Instant::now() < until, "{}", f.log());
        std::thread::sleep(Duration::from_millis(1));
    }
    let record: crate::engine::expansion::Record =
        serde_json::from_slice(&config::read(&result_path, 4 << 20).unwrap()).unwrap();
    assert_eq!(record.status, "failed_closed");
    assert!(!record.runtime_applied && !record.effective_installed);
    assert_eq!(f.bank().feed_generation, 1);
    assert!(!f.path("commit").exists());
    f.stop();
}

#[test]
fn invalid_armed_receipt_rejects_before_revocation_and_preserves_old_chain() {
    let mut f = Fixture::new();
    let admission = f.admission();
    let mut source = f.connect(&admission);
    let plan = f.plan(&admission);
    f.write_plan(&plan);
    let path = f.path("arm");
    let approval = Approval::decode(&wait_file(&mut f, &mut source, &path)).unwrap();
    let mut receipt = source
        .arm_transition(approval, &admission.client, f.now())
        .unwrap();
    receipt.pid = receipt.pid.checked_add(1).unwrap();
    config::write(&f.path("receipt"), &receipt).unwrap();
    let result_path = f.config.expansion.as_ref().unwrap().result_path.clone();
    let record: crate::engine::expansion::Record =
        serde_json::from_slice(&wait_file(&mut f, &mut source, &result_path)).unwrap();
    assert_eq!(record.status, "rejected");
    assert!(!record.runtime_applied && !record.effective_installed);
    assert_eq!(f.bank().feed_generation, 1);
    assert!(!f.path("commit").exists());
    let before = source.client().consumer.watermarks().applied;
    let until = Instant::now() + Duration::from_millis(120);
    while Instant::now() < until {
        drive(&mut f, &mut source);
    }
    assert!(source.client().consumer.watermarks().applied > before);
    f.stop();
}

#[test]
fn effective_write_failure_after_commit_keeps_new_bank_closed_and_has_no_success_receipt() {
    let mut f = Fixture::new();
    let admission = f.admission();
    let mut source = f.connect(&admission);
    let plan = f.plan(&admission);
    f.write_plan(&plan);
    let path = f.path("arm");
    let approval = Approval::decode(&wait_file(&mut f, &mut source, &path)).unwrap();
    // Real rename failure after the runtime bank cut, without replacing the IO implementation.
    std::fs::create_dir(&f.config.expansion.as_ref().unwrap().effective_path).unwrap();
    let receipt = source
        .arm_transition(approval, &admission.client, f.now())
        .unwrap();
    config::write(&f.path("receipt"), &receipt).unwrap();
    let result_path = f.config.expansion.as_ref().unwrap().result_path.clone();
    let record: crate::engine::expansion::Record =
        serde_json::from_slice(&wait_file(&mut f, &mut source, &result_path)).unwrap();
    assert_eq!(record.status, "failed_closed", "{}", f.log());
    assert!(record.runtime_applied);
    assert!(!record.effective_installed);
    assert_eq!(source.transition_receipt().unwrap().phase, Phase::Quiesced);
    assert_eq!(f.bank().feed_generation, 2);
    assert!(!f.path("commit").exists());
    let mut rejected = plan.consumers[0].next.client.clone();
    rejected.bank_header = wire_bytes(&f.bank()).unwrap();
    // SAFETY: this is the same owned qualified child and exact trusted successor as above;
    // the Server must reject its admission while the post-cut failure remains latched.
    assert!(
        unsafe { tickwire_market_source::Client::connect(&rejected, &mut || Ok(f.now())) }.is_err()
    );
    f.stop();
}

#[test]
fn cold_build_failure_returns_rejection_without_stopping_existing_owners() {
    let mut f = Fixture::new();
    let admission = f.admission();
    let mut source = f.connect(&admission);
    let mut plan = f.plan(&admission);
    plan.target_config.feeds[1].trust_anchors =
        vec![f.config.socket.with_file_name("missing-ca.der")];
    f.write_plan(&plan);
    let result_path = f.config.expansion.as_ref().unwrap().result_path.clone();
    let record: crate::engine::expansion::Record =
        serde_json::from_slice(&wait_file(&mut f, &mut source, &result_path)).unwrap();
    assert_eq!(record.status, "rejected");
    assert!(!record.runtime_applied);
    assert_eq!(f.bank().feed_generation, 1);
    assert!(!f.path("arm").exists());
    let before = source.client().consumer.watermarks().applied;
    let until = Instant::now() + Duration::from_millis(120);
    while Instant::now() < until {
        drive(&mut f, &mut source);
    }
    assert!(source.client().consumer.watermarks().applied > before);
    f.stop();
}
