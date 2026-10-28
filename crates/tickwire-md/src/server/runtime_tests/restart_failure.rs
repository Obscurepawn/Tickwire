use super::*;
use crate::recovery::{Phase as RecoveryPhase, Store};
use tickwire_ipc::ResourceHeader;

fn wait_status(f: &mut Fixture, path: &Path, expected: &str) -> crate::engine::expansion::Record {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        f.alive();
        if path.exists() {
            let bytes = config::read(path, 4 << 20).unwrap();
            let record: crate::engine::expansion::Record = serde_json::from_slice(&bytes).unwrap();
            if record.status == expected {
                return record;
            }
        }
        assert!(Instant::now() < deadline, "waiting {expected}: {}", f.log());
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn rejected_request_after_irreversible_timeout_cannot_make_recovery_ready() {
    let mut f = Fixture::new_with_recovery();
    let admission = f.admission();
    let mut source = f.connect(&admission);
    let mut plan = f.plan(&admission);
    plan.warmup_deadline_mono_ns = f.now().nanos() + 1_500_000_000;
    f.write_plan(&plan);
    let arm_path = f.path("arm");
    let approval = Approval::decode(&wait_file(&mut f, &mut source, &arm_path)).unwrap();
    let receipt = source
        .arm_transition(approval, &admission.client, f.now())
        .unwrap();
    config::write(&f.path("receipt"), &receipt).unwrap();
    // Keep the real cursor attached and stop servicing it after Armed. The
    // actual server must revoke, then time out without any QUIESCED proof.
    let control = f.config.expansion.as_ref().unwrap().clone();
    let failed = wait_status(&mut f, &control.result_path, "failed_closed");
    assert_eq!(failed.operation_id, plan.operation_id);
    assert!(!failed.runtime_applied && !failed.effective_installed);
    assert!(failed.error.as_deref().unwrap().contains("deadline"));
    assert!(!f.path("commit").exists());
    assert_eq!(f.bank().feed_generation, 1);

    config::write_bytes(&control.plan_path, b"{}").unwrap();
    let rejected = wait_status(&mut f, &control.result_path, "rejected");
    assert_eq!(rejected.operation_id, [0; 16]);
    assert!(!rejected.runtime_applied && !rejected.effective_installed);
    f.stop_process();
    drop(source);

    let store = Store::open(&control.recovery.unwrap().directory).unwrap();
    let current = store.current().unwrap();
    assert!(
        current
            .operations
            .iter()
            .any(|consumed| consumed.operation_id == plan.operation_id)
    );
    assert_eq!(
        current.phase,
        RecoveryPhase::Revoking,
        "a rejected later request must not promote a failed revocation to restartable Ready"
    );
}

#[test]
fn actual_runtime_rejects_a_wrong_nonzero_resource_descriptor_before_starting() {
    let mut f = Fixture::new();
    let previous_pid = f.child.id();
    let admission = f.admission();
    let source = f.connect(&admission);
    f.stop_process();
    drop(source);
    let mut bank =
        ResourceHeader::decode(&config::read(&f.config.topics[0].resource, 4096).unwrap()).unwrap();
    bank.descriptor_hash = [77; 32];
    config::write_bytes(&f.config.topics[0].resource, &wire_bytes(&bank).unwrap()).unwrap();
    let mut next = f.config.clone();
    next.socket = next.socket.with_file_name("invalid-descriptor.sock");
    f.spawn_config(next, "invalid-descriptor");
    assert_ne!(f.child.id(), previous_pid);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = f.child.try_wait().unwrap() {
            assert!(!status.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "invalid MD is still live: {}",
            f.log()
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        f.log().contains("MD resource descriptor digest"),
        "{}",
        f.log()
    );
    assert!(!f.config.socket.exists());
}

#[test]
fn rejected_warmed_candidate_cannot_reuse_its_source_owner_in_a_new_operation() {
    let mut f = Fixture::new_with_recovery();
    let certificate = f.config.socket.with_file_name("candidate.der");
    let candidate = network::Network::start(&certificate, "ETHUSDT", 199, true);
    f.addition.stream_address = candidate.stream.clone();
    f.addition.snapshot_address = candidate.snapshot.clone();
    f.addition.trust_anchors = vec![certificate];
    f.addition.events = vec![config::EventFeed {
        owner_id: [95; 16],
        stream_id: 56,
        kind: config::EventType::Trade,
        first_connection_epoch: 1,
        first_operation: 1,
        stream_address: candidate.event.clone(),
        sbe_api_key_file: None,
        fast_mark: false,
    }];
    let admission = f.admission();
    let mut source = f.connect(&admission);
    let mut plan = f.plan(&admission);
    plan.consumers[0]
        .next
        .client
        .instruments
        .iter_mut()
        .find(|instrument| instrument.id == f.addition.instrument.id)
        .unwrap()
        .required_states = 3;
    plan.warmup_deadline_mono_ns = f.now().nanos() + 1_500_000_000;
    let initial = source.client().consumer.watermarks().applied;
    f.write_plan(&plan);
    let arm_path = f.path("arm");
    let approval = Approval::decode(&wait_file(&mut f, &mut source, &arm_path)).unwrap();
    assert_eq!(approval.operation_id, plan.operation_id);
    assert_eq!(
        candidate
            .connections
            .load(std::sync::atomic::Ordering::Relaxed),
        2
    );
    // Actual prewarm has finished, but this real consumer never returns Armed.
    // Continue consuming the original bank while the unapproved candidate expires.
    let control = f.config.expansion.as_ref().unwrap().clone();
    let first: crate::engine::expansion::Record =
        serde_json::from_slice(&wait_file(&mut f, &mut source, &control.result_path)).unwrap();
    assert_eq!(first.status, "rejected", "{}", f.log());
    assert_eq!(first.operation_id, plan.operation_id);
    assert!(first.error.as_deref().unwrap().contains("deadline"));
    assert!(!first.runtime_applied && !first.effective_installed);
    assert!(source.client().consumer.watermarks().applied > initial);
    assert_eq!(f.bank().feed_generation, 1);

    let owner = plan.target_config.feeds.last().unwrap().owner_id;
    let mut retry = plan;
    retry.operation_id[0] ^= 0x80;
    retry.warmup_deadline_mono_ns = f.now().nanos() + 8_000_000_000;
    f.write_plan(&retry);
    let deadline = Instant::now() + Duration::from_secs(5);
    let rejected = loop {
        let record: crate::engine::expansion::Record =
            serde_json::from_slice(&config::read(&control.result_path, 4 << 20).unwrap()).unwrap();
        if record.operation_id == retry.operation_id {
            break record;
        }
        assert!(
            Instant::now() < deadline,
            "retry did not finish: {}",
            f.log()
        );
        drive(&mut f, &mut source);
    };
    assert_eq!(rejected.status, "rejected");
    assert!(
        rejected
            .error
            .as_deref()
            .unwrap()
            .contains("candidate source owner was already consumed"),
        "retry error {:?}: {}",
        rejected.error,
        f.log()
    );
    assert!(!rejected.runtime_applied && !rejected.effective_installed);
    assert!(!f.path("commit").exists());
    assert_eq!(f.bank().feed_generation, 1);
    let before = source.client().consumer.watermarks().applied;
    let end = Instant::now() + Duration::from_millis(100);
    while Instant::now() < end {
        drive(&mut f, &mut source);
    }
    assert!(source.client().consumer.watermarks().applied > before);
    f.stop_process();
    drop(source);
    let store = Store::open(&control.recovery.unwrap().directory).unwrap();
    assert_eq!(store.current().unwrap().phase, RecoveryPhase::Ready);
    assert!(store.seen_owner(owner));
    assert!(store.seen_owner([95; 16]));
}
