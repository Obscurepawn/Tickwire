use super::*;
use tickwire_clock::{ClockDomain as MonoDomain,MonoTime};
fn at(n:u64)->MonoTime {MonoTime::new(MonoDomain::new([8;16],1).unwrap(),n)}
fn identity()->tickwire_release::rebuild::window::Identity {
    tickwire_release::rebuild::window::Identity {capture:[91;16],logical_strategy:8,risk_domain:[1;16],instance:[6;16],generation:7,binary:[14;32],clock:ClockDomain{boot_id:[8;16],generation:1}}
}
#[test]
fn capture_only_builds_a_window_from_actual_market_and_account_inputs() {
    let f=Fixture::new();let mut seed=f.inputs.clone();seed.warm.clear();
    let mut capture=capture::CaptureOnly::new(identity(),f.contract.clone(),seed,at(f.inputs.start_ns)).expect("create an input-only owner without a user factory");
    for row in &f.inputs.warm {capture.push(row.input.clone(),at(row.now_ns)).unwrap();}
    let done=capture.finish().unwrap();
    assert_eq!(done.inputs(),&f.inputs);
    assert_eq!(done.seal().end,f.plans.members[0].end);
    assert_eq!(done.seal().start.rows,0);
    assert_eq!(done.seal().inputs,Object::of(&f.inputs.bytes().unwrap()));
    assert_eq!(done.seal().identity,identity());
    assert!(!String::from_utf8(done.seal().bytes().unwrap()).unwrap().contains("checkpoint"));
}
