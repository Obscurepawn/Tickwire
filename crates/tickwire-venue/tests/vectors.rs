mod common;
use common::*;
use tickwire_venue::*;
fn unhex(s: &str) -> Vec<u8> {
    s.trim()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
        .collect()
}
#[test]
fn independently_transcribed_full_command_vector() {
    let bytes = unhex(include_str!("data/spot-limit.hex"));
    let hash = unhex(include_str!("data/spot-limit.sha256"));
    let c = CanonicalCommand::new(command()).unwrap();
    assert_eq!(c.bytes(), bytes);
    assert_eq!(c.digest().as_slice(), hash);
    assert_eq!(CanonicalCommand::decode(&bytes).unwrap(), c);
}
#[test]
fn independently_transcribed_multi_asset_event_vectors() {
    let bytes = unhex(include_str!("data/multi-asset-fill.hex"));
    let hash = unhex(include_str!("data/multi-asset-fill.sha256"));
    let c = CanonicalEvent::new(event()).unwrap();
    assert_eq!(c.bytes(), bytes);
    assert_eq!(c.digest().as_slice(), hash);
    assert_eq!(CanonicalEvent::decode(&bytes).unwrap(), c);
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
    let bytes = unhex(include_str!("data/minimum-i128-fee.hex"));
    let hash = unhex(include_str!("data/minimum-i128-fee.sha256"));
    let c = CanonicalEvent::new(e).unwrap();
    assert_eq!(c.bytes(), bytes);
    assert_eq!(c.digest().as_slice(), hash);
    assert_eq!(CanonicalEvent::decode(&bytes).unwrap(), c);
}
#[test]
fn every_single_byte_mutation_is_rejected_or_canonical_for_the_decoded_value() {
    let c = CanonicalCommand::new(command()).unwrap();
    for i in 0..c.bytes().len() {
        for mask in [1, 8, 128] {
            let mut bytes = c.bytes().to_vec();
            bytes[i] ^= mask;
            if let Ok(other) = CanonicalCommand::decode(&bytes) {
                assert_eq!(other.bytes(), bytes);
                assert_ne!(other.digest(), c.digest());
            }
        }
    }
}
#[test]
fn independently_transcribed_reported_cash_fields() {
    let mut e = event();
    if let VenueEventBody::Fill(ref mut f) = e.body {
        f.observation.cumulative_quote = Some(d(300001, -4));
        f.last_quote = Some(d(300001, -4));
        f.realized_pnl = Some(AssetDelta {
            asset: asset(2, "USDT"),
            amount: d(-11, -4),
        })
    }
    let c = CanonicalEvent::new(e.clone()).unwrap();
    let bytes = unhex(include_str!("data/reported-cash.hex"));
    let hash = unhex(include_str!("data/reported-cash.sha256"));
    assert_eq!(c.bytes(), bytes);
    assert_eq!(c.digest().as_slice(), hash);
    assert_eq!(CanonicalEvent::decode(&bytes).unwrap().event(), &e);
}
