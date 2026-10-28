use super::{Fixture, approved, worker};
use std::time::{Duration, Instant};

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
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
            if status
                .lines()
                .any(|l| l.starts_with("State:") && l.contains('T'))
            {
                break;
            }
            assert!(Instant::now() < until);
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
#[ignore = "requires fixed release TICKWIRE_INPUTS_WORKER; real queued intent"]
fn actual_queued_intent_waits_for_scope_and_cannot_steal_internal_query() {
    let mut f = Fixture::new(&worker(), 1, 100);
    approved::bind(&mut f, false, approved::Extra::None);
    let mut owner = approved::build(&f).unwrap();
    f.seed(&mut *owner);
    let sequence = f.market.publish(100);
    f.wait_durable(|c| c.runtime.markets[0].sequence >= sequence && c.runtime.effects.count == 1);
    let command = f.assert_command(100);
    let pid = owner.observation_process(f.target()).unwrap().process().pid;
    let paused = Paused::new(pid);
    for _ in 0..3 {
        assert!(f.next_ready(&mut *owner).is_none());
        assert!(owner.projection_inputs(f.target()).is_none());
        assert!(!owner.failed());
    }
    assert!(matches!(
        owner.poll_observation(f.target()),
        Err(tickwire_strategy_host::Error::Busy)
    ));
    drop(paused);
    approved::prove(&f, &mut *owner);
    assert_eq!(f.next_ready(&mut *owner).unwrap().command, command);
    assert!(f.next_ready(&mut *owner).is_none());
    println!(
        "{}",
        serde_json::json!({"case":"input_pending_gate","pid":pid,"effect_before_query":1,"pending_delivered":false,"fresh_proof_then_same_intent":true,"intent_digest":command.digest(),"no_gateway_submission":true})
    );
}

#[test]
#[ignore = "requires fixed release TICKWIRE_INPUTS_WORKER; actual timeout cleanup"]
fn scope_timeout_reaps_worker_and_keeps_queued_intent_closed() {
    let mut f = Fixture::new(&worker(), 1, 100);
    approved::bind(&mut f, true, approved::Extra::None);
    f.host["projection_inputs"]["timeout_ns"] = 50_000_000u64.into();
    let mut owner = approved::build(&f).unwrap();
    f.seed(&mut *owner);
    let sequence = f.market.publish(100);
    f.wait_durable(|c| c.runtime.markets[0].sequence >= sequence && c.runtime.effects.count == 1);
    let pid = owner.observation_process(f.target()).unwrap().process().pid;
    let paused = Paused::new(pid);
    let until = Instant::now() + Duration::from_secs(5);
    while !owner.failed() {
        assert!(f.next_ready(&mut *owner).is_none());
        assert!(owner.projection_inputs(f.target()).is_none());
        assert!(owner.projection_parameters(f.target()).is_none());
        assert!(
            Instant::now() < until,
            "earlier scope deadline closes shared query"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    drop(paused);
    assert!(f.next_ready(&mut *owner).is_none());
    println!(
        "{}",
        serde_json::json!({"case":"input_query_expired","pid":pid,"earlier_timeout_ns":50_000_000,"parameter_timeout_ns":10_000_000_000u64,"delivered":false,"inputs_proof":false,"parameters_proof":false,"reaped":true})
    );
}

#[test]
#[ignore = "requires fixed release TICKWIRE_INPUTS_WORKER; proof remains subject to baseline"]
fn identical_baseline_is_idempotent_but_changed_baseline_closes_after_proof() {
    let mut f = Fixture::new(&worker(), 1, 100);
    approved::bind(&mut f, false, approved::Extra::None);
    let mut owner = approved::build(&f).unwrap();
    f.seed(&mut *owner);
    approved::prove(&f, &mut *owner);
    let challenge = owner
        .projection_inputs(f.target())
        .unwrap()
        .observation()
        .challenge();
    for _ in 0..3 {
        f.seed(&mut *owner);
        assert!(!owner.failed());
    }
    assert_eq!(
        owner
            .projection_inputs(f.target())
            .unwrap()
            .observation()
            .challenge(),
        challenge
    );
    // Advance an actual account input while keeping the approved baseline and
    // scope. The saved startup proof remains historical; ordinary input flow
    // must remain enabled rather than treating every new record as a reset.
    let body = tickwire_account::RecoveryRecord::UnattributedCashFlow {
        id: tickwire_venue::BoundedText::new("input-scope-continuation").unwrap(),
        delta: tickwire_account::AssetAmount {
            key: super::account_fixture::key(1),
            amount: super::account_fixture::f(1),
        },
    };
    f.account = tickwire_account::Account::recover(
        f.baseline.snapshot().clone(),
        f.baseline.catalog().clone(),
        std::slice::from_ref(&body),
    )
    .unwrap();
    let record =
        tickwire_account_codec::CanonicalRecord::new(f.baseline.digest(), 1, body).unwrap();
    assert!(owner.on_account_record(1, &record, &f.baseline, &f.account));
    f.wait_checkpoint(&mut *owner, |c| c.runtime.accounts[0].sequence == 1);
    f.seed(&mut *owner);
    assert!(!owner.failed());
    assert_eq!(
        owner
            .projection_inputs(f.target())
            .unwrap()
            .observation()
            .challenge(),
        challenge
    );
    let sequence = f.market.publish(100);
    f.wait_durable(|c| c.runtime.markets[0].sequence >= sequence && c.runtime.effects.count == 1);
    let pid = owner.observation_process(f.target()).unwrap().process().pid;
    let mut snapshot = f.baseline.snapshot().clone();
    snapshot.valid_until -= 1;
    let replacement =
        tickwire_account_codec::CanonicalBaseline::new(snapshot, f.baseline.catalog().clone())
            .unwrap();
    assert_ne!(replacement.digest(), f.baseline.digest());
    assert!(!owner.on_account_baseline(&replacement, &f.account));
    assert!(owner.failed());
    assert!(owner.projection_inputs(f.target()).is_none());
    assert!(f.next_ready(&mut *owner).is_none());
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    assert!(!owner.on_account_baseline(&f.baseline, &f.account));
    assert!(f.next_ready(&mut *owner).is_none());
    println!(
        "{}",
        serde_json::json!({"case":"input_baseline_changed","pid":pid,"original":Object::of(f.baseline.bytes()),"replacement":Object::of(replacement.bytes()),"same_scope":true,"continued_account_position":1,"continued_record":Object::of(record.bytes()),"reaped":true,"delivered":false})
    );
}
use tickwire_release::Object;
