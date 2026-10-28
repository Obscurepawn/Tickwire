//! Actual App assembly with an independently built registered worker and a real
//! MD JOIN server/Publisher in a parent thread. No exchange execution is enabled.
#![cfg(target_os = "linux")]
#![recursion_limit = "256"]
#[path = "../../tickwire-account/tests/common/mod.rs"]
mod account_fixture;
#[path = "projection_launch/channels.rs"]
mod channels;
#[path = "parameters_process/fixture.rs"]
mod fixture;
#[path = "parameters_process/inputs.rs"]
mod inputs;
#[path = "parameters_process/market.rs"]
mod market;
#[path = "projection_launch/runtime.rs"]
mod runtime;
use fixture::Fixture;
use std::path::PathBuf;

fn worker() -> PathBuf {
    std::fs::canonicalize(
        std::env::var_os("TICKWIRE_PARAMETERS_WORKER").expect("explicit frozen counting worker"),
    )
    .unwrap()
}

#[test]
#[ignore = "requires fixed release TICKWIRE_PARAMETERS_WORKER; actual safe App startup"]
fn projected_parameters_reject_mismatch_before_worker_start() {
    let mut f = Fixture::new(&worker(), 1, 100);
    f.bind_projection(true);
    let result = f.build();
    let error = match result {
        Ok(_) => panic!("mismatched projection parameters reached actual hosted worker"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("parameter"), "unexpected rejection: {error}");
    f.assert_no_worker();
}

#[test]
#[ignore = "requires fixed release TICKWIRE_PARAMETERS_WORKER; actual safe App startup"]
fn registered_parameters_fixture_runs_without_projection_binding() {
    let f = Fixture::new(&worker(), 1, 100);
    let mut owner = f.build().unwrap();
    f.seed(&mut *owner);
    f.market.publish(100);
    f.wait_checkpoint(&mut *owner, |checkpoint| {
        checkpoint.runtime.effects.count == 1
    });
    assert!(!owner.failed());
    let command = f.assert_command(100);
    assert_eq!(f.next_ready(&mut *owner).unwrap().command, command);
}

#[test]
#[ignore = "requires fixed release TICKWIRE_PARAMETERS_WORKER; actual parameter consumer"]
fn projected_parameters_reach_real_registered_consumer() {
    let mut f = Fixture::new(&worker(), 1, 100);
    f.bind_projection(false);
    let mut owner = f
        .build()
        .expect("matching parameters must reach the actual worker");
    f.seed(&mut *owner);
    f.market.publish(100);
    f.wait_checkpoint(&mut *owner, |checkpoint| {
        checkpoint.runtime.effects.count == 1
    });
    f.prove(&mut *owner);
    assert!(!owner.failed());
    f.assert_command(100);
}

fn business(c: &tickwire_release::replay::Checkpoint) -> serde_json::Value {
    serde_json::from_slice(
        &tickwire_strategy_sdk::replay::application::business_bytes(&c.business).unwrap(),
    )
    .unwrap()
}

#[test]
#[ignore = "requires fixed release TICKWIRE_PARAMETERS_WORKER; actual callback thresholds"]
fn projected_observation_and_bid_thresholds_change_actual_worker_effects() {
    for (minimum_observations, minimum_bid_ticks) in [(1, 100), (8, 100), (1, 101)] {
        let mut f = Fixture::new(&worker(), minimum_observations, minimum_bid_ticks);
        f.bind_projection(false);
        let mut owner = f.build().unwrap();
        f.seed(&mut *owner);
        f.prove(&mut *owner);
        let sequence = f.market.publish(100);
        let initial = f.wait_consumed(&mut *owner, sequence);
        let initial_count = business(&initial)["observations"].as_u64().unwrap();
        assert!(
            initial_count > 0 && initial_count < 8,
            "actual initial callbacks: {initial_count}"
        );
        assert_eq!(initial.runtime.config.bytes, f.params);
        assert_eq!(
            initial.runtime.effects.count,
            u64::from(minimum_observations == 1 && minimum_bid_ticks == 100)
        );
        if minimum_observations == 8 {
            let mut current = initial;
            while business(&current)["observations"].as_u64().unwrap() < 8 {
                assert_eq!(current.runtime.effects.count, 0);
                let sequence = f.market.publish(100);
                current = f.wait_consumed(&mut *owner, sequence);
            }
            assert_eq!(current.runtime.effects.count, 1);
        } else if minimum_bid_ticks == 101 {
            assert_eq!(business(&initial)["spent"], false);
            let sequence = f.market.publish(101);
            let current = f.wait_consumed(&mut *owner, sequence);
            assert_eq!(current.runtime.effects.count, 1);
        }
        let command = f.assert_command(minimum_bid_ticks);
        let sequence = f.market.publish(minimum_bid_ticks);
        let final_state = f.wait_consumed(&mut *owner, sequence);
        assert_eq!(final_state.runtime.effects.count, 1);
        assert_eq!(business(&final_state)["spent"], true);
        let submission = f
            .next_ready(&mut *owner)
            .expect("actual intent may be delivered only after parameter proof");
        assert_eq!(submission.command, command);
        assert!(f.next_ready(&mut *owner).is_none());
        println!(
            "{}",
            serde_json::json!({"case":"parameter_threshold","minimum_observations":minimum_observations,"minimum_bid_ticks":minimum_bid_ticks,"initial_observations":initial_count,"final_observations":business(&final_state)["observations"],"effects":final_state.runtime.effects.count,"intent_digest":command.digest(),"no_gateway_submission":true})
        );
    }
}

struct Paused(u32);
impl Paused {
    fn new(pid: u32) -> Self {
        assert!(
            std::process::Command::new("kill")
                .args(["-STOP", &pid.to_string()])
                .status()
                .unwrap()
                .success()
        );
        let guard = Self(pid);
        let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
            if status
                .lines()
                .any(|l| l.starts_with("State:") && l.contains('T'))
            {
                break;
            }
            assert!(std::time::Instant::now() < until);
            std::thread::yield_now();
        }
        guard
    }
}
impl Drop for Paused {
    fn drop(&mut self) {
        let _ = std::process::Command::new("kill")
            .args(["-CONT", &self.0.to_string()])
            .status();
    }
}

#[test]
#[ignore = "requires fixed release TICKWIRE_PARAMETERS_WORKER; real pending checkpoint challenge"]
fn pending_parameter_query_holds_an_actual_queued_intent_until_fresh_proof() {
    let mut f = Fixture::new(&worker(), 1, 100);
    f.bind_projection(false);
    let mut owner = f.build().unwrap();
    f.seed(&mut *owner);
    let published = f.market.publish(100);
    let checkpoint = f.wait_durable(|c| {
        c.runtime.markets[0].sequence >= published && c.runtime.effects.count == 1
    });
    assert_eq!(business(&checkpoint)["spent"], true);
    let command = f.assert_command(100);
    let pid = owner.observation_process(f.target()).unwrap().process().pid;
    let paused = Paused::new(pid);
    assert!(owner.projection_parameters(f.target()).is_none());
    for _ in 0..3 {
        assert!(
            f.next_ready(&mut *owner).is_none(),
            "queued actual intent escaped the pending parameter gate"
        );
        assert!(owner.projection_parameters(f.target()).is_none());
        assert!(!owner.failed());
    }
    assert!(
        owner.poll_observation(f.target()).is_err(),
        "external caller cannot steal startup query response"
    );
    drop(paused);
    f.prove(&mut *owner);
    let submission = f
        .next_ready(&mut *owner)
        .expect("fresh proof unlocks the original queued intent");
    assert_eq!(submission.command, command);
    assert!(f.next_ready(&mut *owner).is_none());
    println!(
        "{}",
        serde_json::json!({"case":"pending_parameter_gate","pid":pid,"effect_before_query":checkpoint.runtime.effects.count,"intent_digest":command.digest(),"pending_delivered":false,"fresh_proof_then_same_intent":true,"no_gateway_submission":true})
    );
}

#[test]
#[ignore = "requires fixed release TICKWIRE_PARAMETERS_WORKER; actual query failure closes owner"]
fn expired_parameter_query_rejects_queued_intent_and_reaps_worker() {
    let mut f = Fixture::new(&worker(), 1, 100);
    f.bind_projection(false);
    f.host["projection_parameters"]["timeout_ns"] = 50_000_000u64.into();
    let mut owner = f.build().unwrap();
    f.seed(&mut *owner);
    let published = f.market.publish(100);
    let state = f.wait_durable(|c| {
        c.runtime.markets[0].sequence >= published && c.runtime.effects.count == 1
    });
    assert_eq!(business(&state)["spent"], true);
    let pid = owner.observation_process(f.target()).unwrap().process().pid;
    let paused = Paused::new(pid);
    let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !owner.failed() {
        assert!(f.next_ready(&mut *owner).is_none());
        assert!(owner.projection_parameters(f.target()).is_none());
        assert!(
            std::time::Instant::now() < until,
            "query must fail at its original deadline"
        );
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    drop(paused);
    assert!(f.next_ready(&mut *owner).is_none());
    println!(
        "{}",
        serde_json::json!({"case":"parameter_query_expired","pid":pid,"effect_was_pending":true,"delivered":false,"proof":false,"reaped":true})
    );
}
