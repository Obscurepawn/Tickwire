mod common;
use common::*;
use tickwire_venue::*;

fn amendment() -> VenueEvent {
    let mut e = event();
    let VenueEventBody::Fill(f) = e.body else {
        unreachable!()
    };
    let mut o = f.observation;
    o.original_quantity = Some(d(8, -4));
    e.body = VenueEventBody::Amendment(Box::new(AmendmentObservation {
        order: o.order.clone(),
        request_intent_id: 8,
        request_intent_exec_epoch: 4,
        request_digest: [6; 32],
        execution_source: AmendmentSource::RestResponse,
        execution_id: u64::MAX,
        previous_quantity: d(1, -3),
        new_quantity: d(8, -4),
        observation: Some(o),
    }));
    e
}

#[test]
fn schema_three_is_lossless_bounded_and_rejects_old_version_or_trailing_bytes() {
    let c = CanonicalEvent::new(amendment()).unwrap();
    assert_eq!(CanonicalEvent::decode(c.bytes()).unwrap(), c);
    for i in 0..c.bytes().len() {
        assert!(CanonicalEvent::decode(&c.bytes()[..i]).is_err());
    }
    let mut wrong = c.bytes().to_vec();
    let version = 2 + "tickwire.venue-event".len();
    assert_eq!(wrong[version], 3);
    wrong[version] = 2;
    assert_eq!(CanonicalEvent::decode(&wrong).unwrap_err(), Error::Schema);
    let mut tail = c.bytes().to_vec();
    tail.push(0);
    assert!(CanonicalEvent::decode(&tail).is_err());
}

#[test]
fn amendment_requires_exact_embedded_identity_and_a_strict_decrease() {
    for case in 0..4 {
        let mut e = amendment();
        let VenueEventBody::Amendment(ref mut a) = e.body else {
            unreachable!()
        };
        match case {
            0 => a.new_quantity = a.previous_quantity,
            1 => a.order.venue_order_id = None,
            2 => a.observation.as_mut().unwrap().order.order_id = [9; 16].map(|x| x + 1),
            _ => a.order.parent_order_id = Some([8; 16]),
        }
        assert!(CanonicalEvent::new(e).is_err());
    }
}
