use tickwire_recovery::HandoverSeal;
fn base() -> HandoverSeal {
    serde_json::from_value(serde_json::json!({
        "version":1,"owner":([1;16]),"instance":([1;16]),"generation":2,"logical_strategy":3,
        "report_position":0,"ledger_sequence":1,"orders_digest":([1;32]),"checkpoint_digest":([2;32]),
        "checkpoint_bytes":97,"account_input_digest":([3;32]),"account_input_bytes":1,
        "input_digest":([4;32]),"input_bytes":1,"dependencies":(2u16..=11).map(|k|(k,[k as u8;32])).collect::<Vec<_>>()
    })).unwrap()
}
#[test]
fn rebuild_seal_requires_its_own_installation_and_preserves_legacy_bytes() {
    let legacy = base().bytes().unwrap();
    assert!(
        !String::from_utf8(legacy.clone())
            .unwrap()
            .contains("rebuild_")
    );
    assert_eq!(
        HandoverSeal::decode(&legacy).unwrap().bytes().unwrap(),
        legacy
    );
    let mut raw = serde_json::to_value(base()).unwrap();
    raw["version"] = serde_json::json!(4);
    raw["rebuild_installation_digest"] = serde_json::json!(([9; 32]));
    let initial: HandoverSeal = serde_json::from_value(raw.clone()).expect("v4 installation field");
    initial.validate().unwrap();
    assert!(initial.roots().contains(&([9; 32])));
    raw["rebuild_rollback_digest"] = serde_json::json!(([10; 32]));
    let done: HandoverSeal = serde_json::from_value(raw.clone()).unwrap();
    done.validate().unwrap();
    assert!(done.roots().contains(&([10; 32])));
    for field in [
        "preflight_receipt_digest",
        "replay_restore_digest",
        "replay_installation_digest",
        "replay_rollback_digest",
    ] {
        let mut bad = raw.clone();
        bad[field] = serde_json::json!(([11; 32]));
        assert!(
            serde_json::from_value::<HandoverSeal>(bad)
                .unwrap()
                .validate()
                .is_err()
        );
    }
    raw["rebuild_installation_digest"] = serde_json::Value::Null;
    assert!(
        serde_json::from_value::<HandoverSeal>(raw)
            .unwrap()
            .validate()
            .is_err()
    );
}
