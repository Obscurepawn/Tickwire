#![allow(dead_code)]
mod common;
use common::*;
use tickwire_venue::*;
#[test]
fn signed_multi_asset_fees_and_native_ids_survive() {
    let e = event();
    let canonical = CanonicalEvent::new(e.clone()).unwrap();
    assert_eq!(
        CanonicalEvent::decode(canonical.bytes()).unwrap().event(),
        &e
    );
    if let VenueEventBody::Fill(f) = &e.body {
        assert_eq!(f.trade_id.as_str(), "0000456");
        assert_eq!(f.observation.leaves_quantity, None);
        assert!(f.fees.as_slice()[0].amount.coefficient() < 0);
        assert!(f.fees.as_slice()[1].amount.coefficient() > 0);
    } else {
        panic!()
    }
}
#[test]
fn minimum_i128_fee_is_preserved_without_negating_it() {
    let mut e = event();
    if let VenueEventBody::Fill(ref mut f) = e.body {
        f.fees = BoundedList::new(vec![AssetDelta {
            asset: AssetRef {
                exponent: 0,
                ..asset(2, "USDT")
            },
            amount: d(i128::MIN, 0),
        }])
        .unwrap()
    }
    let c = CanonicalEvent::new(e.clone()).unwrap();
    assert_eq!(CanonicalEvent::decode(c.bytes()).unwrap().event(), &e);
}
#[test]
fn duplicate_asset_line_and_conflicting_internal_identity_reject() {
    let mut e = event();
    if let VenueEventBody::Fill(ref mut f) = e.body {
        let delta = f.fees.as_slice()[0].clone();
        f.fees = BoundedList::new(vec![delta.clone(), delta]).unwrap()
    }
    assert_eq!(CanonicalEvent::new(e).unwrap_err(), Error::Duplicate);
    let mut e = event();
    if let VenueEventBody::Fill(ref mut f) = e.body {
        f.observation.order.original_intent_exec_epoch = 0
    }
    assert_eq!(CanonicalEvent::new(e).unwrap_err(), Error::Identity);
}
#[test]
fn parent_native_ids_and_rejection_text_are_preserved() {
    let mut e = event();
    if let VenueEventBody::Fill(ref mut f) = e.body {
        f.observation.order.parent_order_id = Some([8; 16]);
        f.observation.order.venue_algo_id = Some(text("algo-002"));
        f.observation.order.algo_client_id = Some(text("algo-client"));
        f.observation.order.venue_list_id = Some(text("list-002"));
        f.observation.order.list_client_id = Some(text("list-client"));
    }
    let c = CanonicalEvent::new(e.clone()).unwrap();
    assert_eq!(CanonicalEvent::decode(c.bytes()).unwrap().event(), &e);
    e.body = VenueEventBody::Rejected {
        client_id: Some(text("client")),
        venue_code: text("-2010"),
        message: BoundedMessage::new("Insufficient balance / 余额不足").unwrap(),
    };
    let c = CanonicalEvent::new(e.clone()).unwrap();
    assert_eq!(CanonicalEvent::decode(c.bytes()).unwrap().event(), &e);
}
#[test]
fn unknown_internal_order_retains_native_evidence() {
    let mut e = event();
    if let VenueEventBody::Fill(ref mut f) = e.body {
        f.observation.order.order_id = [0; 16];
        f.observation.order.original_intent_id = 0;
        f.observation.order.original_intent_exec_epoch = 0;
    }
    let c = CanonicalEvent::new(e.clone()).unwrap();
    assert_eq!(CanonicalEvent::decode(c.bytes()).unwrap().event(), &e);
}

#[test]
fn reported_quote_amounts_and_realized_pnl_are_preserved() {
    let mut e = event();
    if let VenueEventBody::Fill(ref mut f) = e.body {
        f.observation.cumulative_quote = Some(d(300001, -4));
        f.last_quote = Some(d(300001, -4));
        f.realized_pnl = Some(AssetDelta {
            asset: asset(2, "USDT"),
            amount: d(-11, -4),
        });
    }
    let c = CanonicalEvent::new(e.clone()).unwrap();
    assert_eq!(CanonicalEvent::decode(c.bytes()).unwrap().event(), &e);
    assert_ne!(c.digest(), CanonicalEvent::new(event()).unwrap().digest());
}
#[test]
fn standalone_order_and_fee_events_roundtrip() {
    let mut e = event();
    let VenueEventBody::Fill(f) = e.body.clone() else {
        panic!()
    };
    e.body = VenueEventBody::Order(Box::new(f.observation.clone()));
    let c = CanonicalEvent::new(e.clone()).unwrap();
    assert_eq!(CanonicalEvent::decode(c.bytes()).unwrap().event(), &e);
    e.body = VenueEventBody::Fee {
        order: f.observation.order,
        fee_id: text("fee-1"),
        deltas: f.fees,
    };
    let c = CanonicalEvent::new(e.clone()).unwrap();
    assert_eq!(CanonicalEvent::decode(c.bytes()).unwrap().event(), &e);
}
