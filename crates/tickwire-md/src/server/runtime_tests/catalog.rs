use super::*;
use crate::catalogs::fixture as economic;
use std::sync::atomic::Ordering;

fn approved(recovery: bool, tick: &str, invalid: bool) -> Fixture {
    Fixture::configured(
        recovery,
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
#[test]
fn actual_tls_price_uses_the_approved_economic_tick() {
    let mut identities = Vec::new();
    for (tick, bid, ask) in [("0.01", 9900, 10100), ("0.5", 198, 202)] {
        let mut f = approved(false, tick, false);
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
fn economic_mismatch_rejects_before_network_quota_bank_or_starting() {
    let mut f = approved(true, "0.5", true);
    restart::rejected_process(&mut f, "MD catalog feed economic binding");
    assert_eq!(f.old.connections.load(Ordering::Relaxed), 0);
    assert_eq!(f.new.connections.load(Ordering::Relaxed), 0);
    assert!(!f.config.quota_directory.exists());
    assert!(
        !config::active_resource_path(&f.config.topics[0].resource)
            .unwrap()
            .exists()
    );
    let store = crate::recovery::Store::open(&restart::directory(&f)).unwrap();
    assert!(store.current().is_none());
    println!(
        "{}",
        serde_json::json!({"event":"md_catalog_startup_rejected","pid":f.child.id(),"error":"MD catalog feed economic binding","connections":f.old.connections.load(Ordering::Relaxed)+f.new.connections.load(Ordering::Relaxed),"quota_created":f.config.quota_directory.exists(),"starting":store.current_hash(),"artifact":f.config.catalogs.as_ref().unwrap()[0].reference})
    );
}
#[test]
fn expansion_uses_fixed_content_and_rejects_unapproved_economics_before_warming() {
    let mut f = approved(true, "0.5", false);
    let admission = f.admission();
    let mut source = f.connect(&admission);
    restart::ready(&mut f, 1);
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
    restart::finish_expansion(&mut f, &mut source, &admission, &plan);
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
    println!(
        "{}",
        serde_json::json!({"event":"md_catalog_fixed_expansion","pid":f.child.id(),"rejected_operation":bad.operation_id,"operation":plan.operation_id,"artifact":f.config.catalogs.as_ref().unwrap()[0].reference,"pathname_replaced":true,"record":complete,"descriptor":f.bank().descriptor_hash,"added_instrument":f.addition.instrument.id,"added_bid_ticks":source.client().replica.view(f.addition.instrument.id).unwrap().best_bid().unwrap().price_ticks,"applied":source.client().consumer.watermarks().applied})
    );
    f.stop();
}
#[test]
fn cold_restart_retains_body_without_current_catalog_file() {
    let mut f = approved(true, "0.5", false);
    let admission = f.admission();
    let source = f.connect(&admission);
    let (hash, previous) = restart::ready(&mut f, 1);
    let previous_pid = f.child.id();
    assert_eq!(previous.catalog_objects.len(), 1);
    f.stop_process();
    drop(source);
    std::fs::remove_file(&f.config.catalogs.as_ref().unwrap()[0].path).unwrap();
    let next = restart::restart_config(&mut f, &previous, hash);
    f.spawn_config(next, "catalog-restart");
    let (new_hash, current) = restart::ready(&mut f, 1);
    assert_eq!(current.catalog_objects, previous.catalog_objects);
    let admission = restart::new_admission(&f);
    let source = f.connect(&admission);
    assert_eq!(
        source
            .client()
            .replica
            .view(u64::MAX)
            .unwrap()
            .best_bid()
            .unwrap()
            .price_ticks,
        198
    );
    println!(
        "{}",
        serde_json::json!({"event":"md_catalog_body_restored","previous_pid":previous_pid,"pid":f.child.id(),"previous_checkpoint":hash,"checkpoint":new_hash,"artifact":f.config.catalogs.as_ref().unwrap()[0].reference,"catalog_file_exists":f.config.catalogs.as_ref().unwrap()[0].path.exists(),"retained_body_bytes":current.catalog_objects[0].len(),"bid_ticks":source.client().replica.view(u64::MAX).unwrap().best_bid().unwrap().price_ticks,"applied":source.client().consumer.watermarks().applied})
    );
    f.stop();
}
