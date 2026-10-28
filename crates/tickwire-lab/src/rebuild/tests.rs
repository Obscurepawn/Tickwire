use crate::{ScenarioKind, generate};
#[test]
fn generation_has_no_preflight_or_fabricated_window_and_declares_capture() {
    struct Temp(std::path::PathBuf);
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let base = Temp(std::path::PathBuf::from(format!(
        "/tmp/tw-rb-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )));
    std::fs::create_dir(&base.0).unwrap();
    let bins = base.0.join("bin");
    std::fs::create_dir(&bins).unwrap();
    for name in [
        "tickwire-app",
        "tickwire-md",
        "tickwire-strategy-worker",
        "tickwire-replica-receiver",
        "tickwire-lab",
        "tickwire-lifecycle",
        "tickwire-counting-v1-worker",
        "tickwire-counting-worker",
    ] {
        std::fs::write(bins.join(name), name).unwrap();
    }
    let s = generate::generate_custom_rebuild(&base.0.join("run"), &bins).unwrap();
    assert_eq!(s.kind, ScenarioKind::SpotRebuildCustom);
    let b: tickwire_strategy_sdk::bootstrap::Bootstrap =
        serde_json::from_slice(&std::fs::read(s.root.join("next/bootstrap.json")).unwrap())
            .unwrap();
    assert!(
        b.preflight_job.is_none(),
        "capture cannot follow the ordinary preflight startup"
    );
    let h: tickwire_app::hosted::HostConfig =
        serde_json::from_slice(&std::fs::read(s.root.join("next/host.json")).unwrap()).unwrap();
    assert!(h.capture.is_some());
    assert!(h.shadow);
    let t: tickwire_strategy_sdk::recovery::Template =
        serde_json::from_slice(&std::fs::read(b.recovery_template.unwrap()).unwrap()).unwrap();
    assert!(t.declared_rebuild);
    assert_eq!(t.owner, [6; 16]);
    assert_eq!(t.input_stream, [134; 16]);
    assert!(!s.root.join("capture/output").exists());
    assert!(!s.root.join("publication/objects.json").exists());
    assert!(!s.root.join("handover/coordinator.json").exists());
}

#[test]
fn action_root_comes_from_actual_admission_after_factory_refresh() {
    use tickwire_account::{Account, RecoveryRecord};
    use tickwire_account_codec::CanonicalRecord;
    let (baseline, template) = crate::model::model(1, 1000).unwrap();
    let mut updated = template.command().clone();
    updated.context.strategy.created_mono_ns = 2;
    let actual = tickwire_venue::CanonicalCommand::new(updated).unwrap();
    assert_ne!(actual.bytes(), template.bytes());
    let mut account =
        Account::from_snapshot(baseline.snapshot().clone(), baseline.catalog().clone()).unwrap();
    let admitted = account.admit(&actual, 2).unwrap();
    let record = CanonicalRecord::new(
        baseline.digest(),
        1,
        RecoveryRecord::Admit {
            command: Box::new(actual.clone()),
            evaluated_at: 2,
            expected: admitted.image(),
        },
    )
    .unwrap();
    let mut payload = b"TWAI0001".to_vec();
    payload.push(1);
    payload.extend(1u64.to_be_bytes());
    payload.extend(4u64.to_be_bytes());
    payload.extend((record.bytes().len() as u32).to_be_bytes());
    payload.extend(record.bytes());
    let row = tickwire_recovery::InputRecord {
        kind: 2,
        payload: &payload,
    };
    let input = tickwire_recovery::encode_inputs([10; 16], 1, [0; 32], &[row]).unwrap();
    assert_eq!(super::material::action_from_inputs(&input).unwrap(), actual);
    let duplicated = tickwire_recovery::encode_inputs(
        [10; 16],
        1,
        [0; 32],
        &[
            tickwire_recovery::InputRecord {
                kind: 2,
                payload: &payload,
            },
            tickwire_recovery::InputRecord {
                kind: 2,
                payload: &payload,
            },
        ],
    )
    .unwrap();
    assert!(super::material::action_from_inputs(&duplicated).is_err());
    let empty = tickwire_recovery::encode_inputs([10; 16], 1, [0; 32], &[]).unwrap();
    assert!(super::material::action_from_inputs(&empty).is_err());
}
