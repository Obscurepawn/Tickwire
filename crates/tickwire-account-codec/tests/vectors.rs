mod common;
use common::*;
use sha2::{Digest, Sha256};
use tickwire_account::*;
use tickwire_account_codec::Error;
use tickwire_account_codec::*;

fn hex(s: &str) -> Vec<u8> {
    s.trim()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|x| u8::from_str_radix(std::str::from_utf8(x).unwrap(), 16).unwrap())
        .collect()
}
#[test]
fn independent_reference_baselines_match_exact_bytes_and_hash() {
    for ((s, c, _), bytes, hash) in [
        (
            fixture(),
            include_str!("data/spot-baseline.hex"),
            include_str!("data/spot-baseline.sha256"),
        ),
        (
            futures_fixture(),
            include_str!("data/futures-baseline.hex"),
            include_str!("data/futures-baseline.sha256"),
        ),
    ] {
        let b = CanonicalBaseline::new(s, c).unwrap();
        assert_eq!(b.bytes(), hex(bytes));
        assert_eq!(b.digest().as_slice(), hex(hash));
        assert_eq!(CanonicalBaseline::decode(&hex(bytes)).unwrap(), b);
    }
}
#[test]
fn independently_encoded_minimum_i128_cashflow_is_preserved() {
    let bytes = hex(include_str!(
        "data/minimum-cashflow.hex"
    ));
    let out = CanonicalRecord::decode(&bytes).unwrap();
    assert_eq!(
        out.record(),
        &RecoveryRecord::UnattributedCashFlow {
            id: venue::text("external-minimum"),
            delta: AssetAmount {
                key: key(2),
                amount: Fixed::from_atoms(i128::MIN, 38).unwrap()
            },
        }
    );
    let hash = hex(include_str!(
        "data/minimum-cashflow.sha256"
    ));
    assert_eq!(out.digest().as_slice(), hash);
    assert_eq!(Sha256::digest(&bytes).as_slice(), hash);
}
#[test]
fn alternate_order_is_rejected_by_decode() {
    let bytes = hex(include_str!(
        "data/unsorted-baseline.hex"
    ));
    assert_eq!(
        CanonicalBaseline::decode(&bytes).unwrap_err(),
        Error::NonCanonical
    );
}
#[test]
fn all_single_byte_changes_have_one_accepted_representation() {
    let bytes = hex(include_str!(
        "data/spot-baseline.hex"
    ));
    for offset in 0..bytes.len() {
        let mut edited = bytes.clone();
        for value in 0..=u8::MAX {
            if value == bytes[offset] {
                continue;
            }
            edited[offset] = value;
            if let Ok(decoded) = CanonicalBaseline::decode(&edited) {
                assert_eq!(decoded.bytes(), edited);
                assert_ne!(
                    decoded.digest().as_slice(),
                    Sha256::digest(&bytes).as_slice()
                );
            }
        }
    }
}
