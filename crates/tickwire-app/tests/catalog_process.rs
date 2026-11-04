//! Actual MD Runtime TLS/WSS/REST and a separate registered worker, through safe
//! App assembly. The MD owner is in a parent thread; no gateway trading grant.
#![cfg(target_os = "linux")]
#![recursion_limit = "256"]
#[path = "../../tickwire-account/tests/common/mod.rs"]
mod account_fixture;
#[path = "inputs_process/approved.rs"]
#[allow(dead_code)]
mod approved;
#[path = "projection_launch/channels.rs"]
mod channels;
#[path = "inputs_process/evidence.rs"]
mod evidence;
#[path = "parameters_process/fixture.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "catalog_process/gates.rs"]
mod gates;
#[path = "parameters_process/inputs.rs"]
#[allow(dead_code)]
mod inputs;
#[path = "catalog_process/market.rs"]
#[allow(dead_code)]
mod market;
#[path = "catalog_process/network.rs"]
mod network;
#[path = "projection_launch/runtime.rs"]
mod runtime;
use fixture::Fixture;
use std::{path::PathBuf, sync::atomic::Ordering};

fn worker() -> PathBuf {
    std::fs::canonicalize(
        std::env::var_os("TICKWIRE_CATALOG_WORKER").expect("explicit frozen counting worker"),
    )
    .unwrap()
}
fn business(checkpoint: &tickwire_release::replay::Checkpoint) -> serde_json::Value {
    serde_json::from_slice(
        &tickwire_strategy_sdk::replay::application::business_bytes(&checkpoint.business).unwrap(),
    )
    .unwrap()
}
fn current_bid(checkpoint: &tickwire_release::replay::Checkpoint) -> i64 {
    let saved: serde_json::Value = serde_json::from_slice(&checkpoint.business).unwrap();
    let view: Vec<u8> = serde_json::from_value(saved["markets"][0]["view"].clone()).unwrap();
    let replica = tickwire_strategy_sdk::MarketReplica::restore_checkpoint(&view, 8).unwrap();
    let view = replica.view(u64::MAX).unwrap();
    assert_eq!(
        view.trigger().sequence,
        checkpoint.runtime.markets[0].sequence
    );
    view.best_bid().unwrap().price_ticks
}

#[test]
#[ignore = "requires fixed release TICKWIRE_CATALOG_WORKER; actual TLS MD and independent worker"]
fn same_raw_price_uses_economic_tick_before_actual_strategy_threshold() {
    let worker = worker();
    let mut hashes = Vec::new();
    for (tick, expected_bid, expected_effects) in [("0.01", 100, 1), ("0.02", 50, 0)] {
        let market = market::Market::with_tick(tick);
        let mut fixture = Fixture::new_with_market(&worker, 1, 75, market);
        fixture.bootstrap.instrument_catalog = Some(fixture.market.artifact.clone());
        fixture::write(
            &fixture.root.join("bootstrap.json"),
            &serde_json::to_vec(&fixture.bootstrap).unwrap(),
        );
        approved::bind(&mut fixture, true, approved::Extra::None);
        let mut owner =
            approved::build(&fixture).expect("approved complete catalog reaches worker");
        fixture.seed(&mut *owner);
        let checkpoint = fixture.wait_checkpoint(&mut *owner, |checkpoint| {
            business(checkpoint)["observations"].as_u64().unwrap() > 0
                && checkpoint.runtime.effects.count == expected_effects
        });
        assert_eq!(current_bid(&checkpoint), expected_bid);
        assert_eq!(business(&checkpoint)["spent"], expected_effects == 1);
        fixture::write(
            &fixture.root.join("threshold-checkpoint.raw"),
            &checkpoint.bytes().unwrap(),
        );
        approved::prove(&fixture, &mut *owner);
        fixture.prove(&mut *owner);
        fixture.market.save_evidence(&fixture.root);
        let proof = owner.projection_inputs(fixture.target()).unwrap();
        let observed = proof.observation();
        let current = tickwire_release::replay::Checkpoint::decode(
            &std::fs::read(fixture.root.join("current-checkpoint.raw")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            tickwire_release::Object::of(&current.bytes().unwrap()),
            proof.inputs().checkpoint()
        );
        assert_eq!(current.source, checkpoint.source);
        assert!(current.cut.sequence >= checkpoint.cut.sequence);
        assert_eq!(current_bid(&current), expected_bid);
        assert_eq!(current.runtime.effects.count, expected_effects);
        let catalog = proof.inputs().catalog().expect("actual saved catalog body");
        assert_eq!(catalog.artifact(), fixture.market.artifact);
        assert!(
            catalog.instrument_matches(tickwire_venue::Product::Spot, &fixture.market.instrument)
        );
        assert!(proof.inputs().markets()[0].join().is_some());
        assert_eq!(
            proof.inputs().accounts()[0].baseline().digest(),
            fixture.baseline.digest()
        );
        let parameters = owner.projection_parameters(fixture.target()).unwrap();
        assert!(std::ptr::eq(observed, parameters.observation()));
        let observed_pid = observed.process().process().pid;
        let observed_challenge = observed.challenge();
        let actual_hash = catalog.catalog_hash();
        hashes.push(actual_hash);
        gates::lifecycle(&fixture, &mut *owner);
        let intent = if expected_effects == 1 {
            let command = fixture.assert_command(expected_bid);
            let submission = fixture
                .next_ready(&mut *owner)
                .expect("actual command after joint scope/parameter proof");
            assert_eq!(submission.command, command);
            assert!(fixture.next_ready(&mut *owner).is_none());
            Some(command.digest())
        } else {
            assert!(fixture.next_ready(&mut *owner).is_none());
            None
        };
        assert!(!owner.failed());
        assert_eq!(
            fixture.market.network.subscriptions.load(Ordering::Relaxed),
            1
        );
        assert_eq!(fixture.market.network.snapshots.load(Ordering::Relaxed), 1);
        assert!(fixture.market.network.frames.load(Ordering::Relaxed) > 1);
        println!(
            "{}",
            serde_json::json!({"case":"catalog_actual_price_threshold","raw_price":"1.00","tick":tick,"actual_bid_ticks":expected_bid,"threshold":75,"effects":checkpoint.runtime.effects.count,"intent_digest":intent,"pid":observed_pid,"worker_binary":fixture.bootstrap.market.local.build_hash,"artifact":tickwire_release::Object::of(&fixture.market.artifact.bytes),"catalog_hash":actual_hash,"checkpoint":tickwire_release::Object::of(&checkpoint.bytes().unwrap()),"cut":checkpoint.cut,"challenge":observed_challenge,"wss_subscriptions":fixture.market.network.subscriptions.load(Ordering::Relaxed),"rest_snapshots":fixture.market.network.snapshots.load(Ordering::Relaxed),"md_owner":"actual_Runtime_in_parent_thread","arm_commit_rejected_before_dispatch":true,"no_gateway_submission":true,"root":fixture.root})
        );
        drop(owner);
    }
    assert_ne!(hashes[0], hashes[1]);
}
