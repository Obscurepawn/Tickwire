//! Actual safe App assembly and independent registered-worker input consumption.
#![cfg(target_os = "linux")]
#![recursion_limit = "256"]
#[path = "../../tickwire-account/tests/common/mod.rs"]
mod account_fixture;
#[path = "inputs_process/approved.rs"]
mod approved;
#[path = "projection_launch/channels.rs"]
mod channels;
#[path = "inputs_process/evidence.rs"]
mod evidence;
#[path = "parameters_process/fixture.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "inputs_process/gates.rs"]
mod gates;
#[path = "parameters_process/inputs.rs"]
#[allow(dead_code)]
mod inputs;
#[path = "inputs_process/lifecycle.rs"]
mod lifecycle;
#[path = "parameters_process/market.rs"]
mod market;
#[path = "projection_launch/runtime.rs"]
mod runtime;
use fixture::Fixture;
use std::path::PathBuf;

fn worker() -> PathBuf {
    std::fs::canonicalize(
        std::env::var_os("TICKWIRE_INPUTS_WORKER").expect("explicit frozen counting worker"),
    )
    .unwrap()
}

#[test]
#[ignore = "requires fixed release TICKWIRE_INPUTS_WORKER; actual safe App startup"]
fn actual_scope_only_consumption_matches_one_account_and_external_topic() {
    let mut f = Fixture::new(&worker(), 1, 100);
    approved::bind(&mut f, false, approved::Extra::None);
    let mut owner = approved::build(&f).expect("valid complete input scope reaches worker");
    f.seed(&mut *owner);
    let sequence = f.market.publish(100);
    f.wait_consumed(&mut *owner, sequence);
    approved::prove(&f, &mut *owner);
    assert!(owner.projection_parameters(f.target()).is_none());
    let command = f.assert_command(100);
    assert_eq!(f.next_ready(&mut *owner).unwrap().command, command);
    assert!(f.next_ready(&mut *owner).is_none());
}

#[test]
#[ignore = "requires fixed release TICKWIRE_INPUTS_WORKER; same actual fresh query"]
fn actual_scope_and_parameters_share_one_challenge_and_current_checkpoint() {
    let mut f = Fixture::new(&worker(), 1, 100);
    approved::bind(&mut f, true, approved::Extra::None);
    let mut owner = approved::build(&f).unwrap();
    f.seed(&mut *owner);
    let sequence = f.market.publish(100);
    f.wait_consumed(&mut *owner, sequence);
    approved::prove(&f, &mut *owner);
    f.prove(&mut *owner);
    let inputs = owner.projection_inputs(f.target()).unwrap().observation();
    let params = owner
        .projection_parameters(f.target())
        .unwrap()
        .observation();
    assert!(
        std::ptr::eq(inputs, params),
        "one owned fresh result is shared"
    );
    assert_eq!(inputs.challenge(), params.challenge());
    assert_eq!(
        inputs.configuration().unwrap().checkpoint(),
        params.configuration().unwrap().checkpoint()
    );
    assert_eq!(inputs.request(), params.request());
    assert_eq!(
        f.next_ready(&mut *owner).unwrap().command,
        f.assert_command(100)
    );
}

#[test]
#[ignore = "requires fixed release TICKWIRE_INPUTS_WORKER; reject before actual child"]
fn input_scope_rejects_extra_sets_and_wrong_owner_before_worker_start() {
    for case in [2, 0, 1, 3, 4, 5, 6, 7] {
        let mut f = Fixture::new(&worker(), 1, 100);
        let extra = match case {
            0 => approved::Extra::Account,
            1 => approved::Extra::Topic,
            _ => approved::Extra::None,
        };
        approved::bind(&mut f, false, extra);
        let input = &mut f.host["projection_inputs"];
        match case {
            2 => input["account"]["scope"]["account"] = 999.into(),
            3 => input["account"]["scope"]["environment"] = "production".into(),
            4 => input["account"]["report_owner_process_id"] = "exec-spot-b".into(),
            5 => input["market"]["publisher_process_id"] = "md-spot-b".into(),
            6 => input["market"]["producer"]["instance"] = serde_json::json!(([99; 16])),
            7 => input["market"]["instruments"][0]["id"] = u64::MAX.into(),
            _ => {}
        }
        let error = match approved::build(&f) {
            Ok(_) => panic!("invalid scope case {case} reached actual child construction"),
            Err(e) => e.to_string(),
        };
        assert!(
            error.contains("input"),
            "case {case}: unrelated failure {error}"
        );
        f.assert_no_worker();
    }
}
