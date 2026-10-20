#![allow(dead_code)]
mod common;
use common::*;
use tickwire_venue::*;
#[test]
fn each_truncated_prefix_and_trailing_data_reject() {
    let c = CanonicalCommand::new(command()).unwrap();
    for n in 0..c.bytes().len() {
        assert!(
            CanonicalCommand::decode(&c.bytes()[..n]).is_err(),
            "prefix {n}"
        )
    }
    let mut bytes = c.bytes().to_vec();
    bytes.push(0);
    assert_eq!(
        CanonicalCommand::decode(&bytes).unwrap_err(),
        Error::TrailingData
    );
    let e = CanonicalEvent::new(event()).unwrap();
    for n in 0..e.bytes().len() {
        assert!(
            CanonicalEvent::decode(&e.bytes()[..n]).is_err(),
            "event prefix {n}"
        )
    }
}
#[test]
fn non_shortest_header_unknown_schema_and_domain_reject() {
    let c = CanonicalCommand::new(command()).unwrap();
    let mut bytes = c.bytes().to_vec();
    bytes.splice(0..1, [0x98, 4]);
    assert_eq!(
        CanonicalCommand::decode(&bytes).unwrap_err(),
        Error::NonCanonical
    );
    let mut bytes = c.bytes().to_vec();
    bytes[24] = 2;
    assert_eq!(CanonicalCommand::decode(&bytes).unwrap_err(), Error::Schema);
    assert!(CanonicalEvent::decode(c.bytes()).is_err());
    let mut bytes = c.bytes().to_vec();
    bytes[0] = 0x9f;
    assert_eq!(
        CanonicalCommand::decode(&bytes).unwrap_err(),
        Error::NonCanonical
    );
}
#[test]
fn length_bombs_and_oversized_identifiers_reject_before_allocation() {
    assert_eq!(
        CanonicalCommand::decode(&vec![0; MAX_CANONICAL_BYTES + 1]).unwrap_err(),
        Error::Bounds
    );
    let mut bytes = vec![0x84, 0x7b];
    bytes.extend(u64::MAX.to_be_bytes());
    assert_eq!(CanonicalCommand::decode(&bytes).unwrap_err(), Error::Bounds);
    assert_eq!(ClientId::new("x".repeat(65)).unwrap_err(), Error::Bounds);
    assert_eq!(ClientId::new("has space").unwrap_err(), Error::Text);
    assert_eq!(
        BoundedList::<u64, 8>::new(vec![0; 9]).unwrap_err(),
        Error::Bounds
    );
}
#[test]
fn decimal_text_parser_is_exact_and_bounded() {
    for s in [
        "0",
        "-0",
        "1.2300",
        "-0.000001",
        "170141183460469231731687303715884105727",
        "-170141183460469231731687303715884105728",
    ] {
        let d = s.parse::<Decimal>().unwrap();
        assert_eq!(d.as_plain_string().parse::<Decimal>().unwrap(), d);
    }
    for s in [
        "",
        "-",
        ".1",
        "1.",
        "1e2",
        "+1",
        "NaN",
        "170141183460469231731687303715884105728",
        "-170141183460469231731687303715884105729",
        "0.000000000000000000000000000000000000001",
    ] {
        assert!(s.parse::<Decimal>().is_err(), "{s}")
    }
}
#[test]
fn equivalent_non_normalized_decimal_is_not_an_alternate_command_encoding() {
    let c = CanonicalCommand::new(command()).unwrap();
    let mut bytes = c.bytes().to_vec();
    let at = bytes.windows(3).position(|w| w == [0x82, 6, 4]).unwrap();
    bytes.splice(at..at + 3, [0x82, 0x18, 60, 3]);
    assert_eq!(
        CanonicalCommand::decode(&bytes).unwrap_err(),
        Error::NonCanonical
    );
}
