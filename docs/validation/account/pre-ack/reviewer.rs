#[path = "../../../../crates/tickwire-account/tests/common/mod.rs"]
mod common;
use common::*;
use tickwire_account::*;
use tickwire_venue as v;
fn ack(cmd: &v::Command) -> v::CanonicalEvent {
    let mut e = fill(cmd, "unused", "0.001", "60000", "60");
    let v::VenueEventBody::Fill(f) = e.body else { unreachable!() };
    let mut o = f.observation;
    o.order.venue_order_id = Some(venue::text("server-accepted-1"));
    o.status = v::OrderStatus::New;
    o.cumulative_quantity = Some(venue::d(0, 0));
    e.body = v::VenueEventBody::Order(Box::new(o));
    event(e)
}
fn main() {
    let (mut a, cmd) = setup();
    let adm = a.admit(&canonical(cmd.clone()), 60).unwrap();
    a.apply_event(&ack(&cmd)).unwrap();
    let before = a.asset(key(2)).unwrap().reserved;
    let result = a.apply_abort(&adm);
    println!("ACK->abort: before={:?}, result={:?}, after={:?}, frozen={}", before, result, a.asset(key(2)).unwrap().reserved, a.frozen());
    let (mut a, cmd) = setup();
    let adm = a.admit(&canonical(cmd.clone()), 60).unwrap();
    a.apply_abort(&adm).unwrap();
    let result = a.apply_event(&ack(&cmd));
    println!("abort->ACK: result={:?}, reserved={:?}, frozen={}, order={:?}", result, a.asset(key(2)).unwrap().reserved, a.frozen(), a.order(&adm.orders()[0]));
}
