mod common;
use common::*;
use tickwire_venue::*;
fn structure() -> VenueEvent {
    let mut e = event();
    e.body = VenueEventBody::Structure(Box::new(StructureObservation {
        parent: StructureIdentity {
            structure_id: [44; 16],
            original_intent_id: 7,
            original_intent_exec_epoch: 3,
            kind: StructureKind::Oto,
            client_id: text("list-client"),
            venue_parent_id: Some(text("42")),
        },
        status: StructureStatus::Terminal,
        venue_status: text("ALL_DONE"),
        children: BoundedList::new(vec![]).unwrap(),
    }));
    e
}
#[test]
fn parent_observation_has_distinct_canonical_schema_and_preserves_raw_status() {
    let event = structure();
    let c = CanonicalEvent::new(event.clone()).expect("bounded parent observation");
    assert_eq!(CanonicalEvent::decode(c.bytes()).unwrap().event(), &event);
    let offset = 2 + "tickwire.venue-event".len();
    assert_eq!(c.bytes()[offset], 2);
    let mut wrong = c.bytes().to_vec();
    wrong[offset] = 1;
    assert!(CanonicalEvent::decode(&wrong).is_err());
    let old = CanonicalEvent::new(common::event()).unwrap();
    assert_eq!(old.bytes()[offset], 1);
    let mut wrong = old.bytes().to_vec();
    wrong[offset] = 2;
    assert!(CanonicalEvent::decode(&wrong).is_err());
}

fn linked() -> VenueEvent {
    let mut e = structure();
    let VenueEventBody::Fill(fill) = common::event().body else {
        unreachable!()
    };
    let mut order = fill.observation.order.clone();
    let VenueEventBody::Structure(ref mut parent) = e.body else {
        unreachable!()
    };
    order.original_intent_id = parent.parent.original_intent_id;
    order.original_intent_exec_epoch = parent.parent.original_intent_exec_epoch;
    order.order_id = [23; 16];
    order.client_id = Some(text("leg-one"));
    order.venue_order_id = Some(text("100"));
    order.list_id = Some(parent.parent.structure_id);
    order.venue_list_id = parent.parent.venue_parent_id.clone();
    order.list_client_id = Some(parent.parent.client_id.clone());
    order.parent_order_id = None;
    order.venue_algo_id = None;
    order.algo_client_id = None;
    parent.children = BoundedList::new(vec![StructureChild {
        order,
        reported_client_id: Some(text("native-client")),
    }])
    .unwrap();
    e
}
#[test]
fn structure_children_reject_cross_parent_duplicate_and_wrong_product() {
    let good = linked();
    let c = CanonicalEvent::new(good.clone()).unwrap();
    assert_eq!(CanonicalEvent::decode(c.bytes()).unwrap().event(), &good);
    for mode in 0..5 {
        let mut e = good.clone();
        let VenueEventBody::Structure(ref mut p) = e.body else {
            unreachable!()
        };
        let mut child = p.children.as_slice()[0].clone();
        match mode {
            0 => child.order.list_id = Some([22; 16]),
            1 => child.order.original_intent_exec_epoch += 1,
            2 => child.order.venue_order_id = None,
            3 => child.order.order_id = [0; 16],
            _ => p.parent.kind = StructureKind::Algo,
        }
        p.children = BoundedList::new(vec![child]).unwrap();
        assert!(CanonicalEvent::new(e).is_err());
    }
    let mut e = good.clone();
    let VenueEventBody::Structure(ref mut p) = e.body else {
        unreachable!()
    };
    let child = p.children.as_slice()[0].clone();
    p.children = BoundedList::new(vec![child.clone(), child]).unwrap();
    assert_eq!(CanonicalEvent::new(e), Err(Error::Duplicate));
    let mut e = good;
    let VenueEventBody::Structure(ref mut p) = e.body else {
        unreachable!()
    };
    p.venue_status = text("EXECUTING");
    assert_ne!(CanonicalEvent::new(e).unwrap().digest(), c.digest());
}
