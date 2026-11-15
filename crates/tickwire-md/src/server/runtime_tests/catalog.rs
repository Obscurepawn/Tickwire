use super::*;
use crate::catalogs::fixture as economic;
use std::sync::atomic::Ordering;

fn approved(tick: &str, invalid: bool) -> Fixture {
    Fixture::configured(
        false,
        |path, config, addition| {
            config.feeds[0].instrument.price_tick = tick.into();
            addition.instrument.price_tick = tick.into();
            config.feeds.push(addition.clone());
            economic::install(config, &path.join("catalog.json"));
            *addition = config.feeds.pop().unwrap();
            if invalid {
                config.feeds[0].instrument.price_tick = "0.2".into();
            }
        },
        !invalid,
    )
}

fn finish_expansion(
    f: &mut Fixture,
    source: &mut ShmSource,
    admission: &Admission,
    plan: &crate::expansion::Plan,
) {
    let result = f.config.expansion.as_ref().unwrap().result_path.clone();
    let _ = std::fs::remove_file(&result);
    f.write_plan(plan);
    let arm_path = f.path("arm");
    let approval = Approval::decode(&wait_file(f, source, &arm_path)).unwrap();
    let receipt = source
        .arm_transition(approval, &admission.client, f.now())
        .unwrap();
    config::write(&f.path("receipt"), &receipt).unwrap();
    let commit_path = f.path("commit");
    let commit = Commit::decode(&wait_file(f, source, &commit_path)).unwrap();
    source.commit_transition(commit, f.now()).unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while source.transition_receipt().unwrap().phase != Phase::Activated {
        assert!(Instant::now() < until, "{}", f.log());
        drive(f, source);
    }
    config::write(&f.path("receipt"), source.transition_receipt().unwrap()).unwrap();
    source.release_transition(f.now()).unwrap();
    let record: crate::engine::expansion::Record =
        serde_json::from_slice(&wait_file(f, source, &result)).unwrap();
    assert_eq!(record.status, "complete", "{}", f.log());
}

#[test]
fn actual_tls_price_uses_the_approved_economic_tick() {
    let mut identities = Vec::new();
    for (tick, bid, ask) in [("0.01", 9900, 10100), ("0.5", 198, 202)] {
        let mut f = approved(tick, false);
        let admission = f.admission();
        let source = f.connect(&admission);
        let view = source.client().replica.view(u64::MAX).unwrap();
        assert_eq!(view.best_bid().unwrap().price_ticks, bid);
        assert_eq!(view.best_ask().unwrap().price_ticks, ask);
        assert_eq!(f.old.connections.load(Ordering::Relaxed), 2);
        assert!(f.old.gate.snapshots.load(Ordering::Relaxed) > 0);
        identities.push(f.config.feeds[0].instrument.catalog_hash);
        println!(
            "{}",
            serde_json::json!({"event":"md_catalog_tick_consumed","pid":f.child.id(),"raw_bid":"99","raw_ask":"101","price_tick":tick,"bid_ticks":bid,"ask_ticks":ask,"catalog_hash":identities.last().unwrap(),"artifact":f.config.catalogs.as_ref().unwrap()[0].reference,"applied":source.client().consumer.watermarks().applied})
        );
        f.stop();
    }
    assert_ne!(identities[0], identities[1]);
}

#[test]
fn economic_mismatch_rejects_before_network_or_active_bank() {
    let mut f = approved("0.5", true);
    let end = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = f.child.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < end, "{}", f.log());
        std::thread::sleep(Duration::from_millis(5));
    };
    assert!(!status.success());
    assert!(f.log().contains("MD catalog feed economic binding"));
    assert_eq!(f.old.connections.load(Ordering::Relaxed), 0);
    assert_eq!(f.new.connections.load(Ordering::Relaxed), 0);
    assert!(
        !config::active_resource_path(&f.config.topics[0].resource)
            .unwrap()
            .exists()
    );
    f.stop();
}

#[test]
fn expansion_uses_fixed_content_and_rejects_unapproved_economics_before_warming() {
    let mut f = approved("0.5", false);
    let admission = f.admission();
    let mut source = f.connect(&admission);
    let mut bad = f.plan(&admission);
    bad.target_config.feeds[1].instrument.price_tick = "0.2".into();
    let original = f.bank();
    f.write_plan(&bad);
    let result = f.config.expansion.as_ref().unwrap().result_path.clone();
    let record: crate::engine::expansion::Record =
        serde_json::from_slice(&wait_file(&mut f, &mut source, &result)).unwrap();
    assert_eq!(record.status, "rejected");
    assert!(
        record
            .error
            .unwrap()
            .contains("catalog feed economic binding")
    );
    assert_eq!(f.new.connections.load(Ordering::Relaxed), 0);
    assert_eq!(f.bank(), original);
    assert!(!f.path("arm").exists());
    // A changed pathname cannot replace already validated economic content.
    config::write_bytes(&f.config.catalogs.as_ref().unwrap()[0].path, b"{}").unwrap();
    let mut plan = f.plan(&admission);
    plan.operation_id = [144; 16];
    finish_expansion(&mut f, &mut source, &admission, &plan);
    assert_eq!(
        source
            .client()
            .replica
            .view(f.addition.instrument.id)
            .unwrap()
            .best_bid()
            .unwrap()
            .price_ticks,
        398
    );
    let complete: crate::engine::expansion::Record =
        serde_json::from_slice(&config::read(&result, 4 << 20).unwrap()).unwrap();
    assert_eq!(complete.status, "complete");
    f.stop();
}
