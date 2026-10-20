#[path = "../../../../crates/tickwire-account/tests/common/mod.rs"]
mod common;
use common::*;
use tickwire_venue as v;
fn main() {
    let (mut a, mut cmd) = setup();
    a.admit(&canonical(cmd.clone()), 60).unwrap();
    let first = fill(&cmd, "trade-same", "0.0004", "50000", "20");
    a.apply_event(&event(first.clone())).unwrap();
    let mut replay = first;
    let v::VenueEventBody::Fill(ref mut f) = replay.body else { unreachable!() };
    f.observation.status = v::OrderStatus::Unknown;
    let applied = a.apply_event(&event(replay));
    cmd.context.request.intent_id += 1;
    let v::CommandBody::New(ref mut o) = cmd.body else { unreachable!() };
    o.client_id = venue::text("after-unknown");
    let admission = a.admit(&canonical(cmd), 60);
    println!("same trade + Unknown: applied={:?}, frozen={}, new_admission_ok={}", applied, a.frozen(), admission.is_ok());
}
