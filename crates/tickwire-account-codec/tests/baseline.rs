mod common;
use common::*;
use tickwire_account::AssetKey;
use tickwire_account_codec::*;

#[test]
fn complete_spot_and_futures_baselines_roundtrip() {
    for (s, c, _) in [fixture(), futures_fixture()] {
        let baseline = CanonicalBaseline::new(s, c).unwrap();
        let restored = CanonicalBaseline::decode(baseline.bytes()).unwrap();
        assert_eq!(restored, baseline);
        tickwire_account::Account::from_snapshot(
            restored.snapshot().clone(),
            restored.catalog().clone(),
        )
        .unwrap();
    }
}
#[test]
fn collections_have_one_order_and_duplicates_reject() {
    let (mut s, c, _) = fixture();
    s.assets[0].ownership = vec![(9, units(0)), (8, units(1))];
    let a = CanonicalBaseline::new(s.clone(), c.clone()).unwrap();
    s.assets.reverse();
    s.assets[1].ownership.reverse();
    assert_eq!(CanonicalBaseline::new(s.clone(), c.clone()).unwrap(), a);
    s.assets.push(s.assets[0].clone());
    assert_eq!(CanonicalBaseline::new(s, c).unwrap_err(), Error::Duplicate);
}
#[test]
fn every_prefix_truncation_and_trailing_data_reject() {
    let (s, c, _) = fixture();
    let b = CanonicalBaseline::new(s, c).unwrap();
    for len in 0..b.bytes().len() {
        assert!(
            CanonicalBaseline::decode(&b.bytes()[..len]).is_err(),
            "{len}"
        );
    }
    let mut bytes = b.bytes().to_vec();
    bytes.push(0);
    assert_eq!(
        CanonicalBaseline::decode(&bytes).unwrap_err(),
        Error::TrailingData
    );
}
#[test]
fn changes_to_fees_wallet_units_and_revision_change_identity() {
    let (s, c, _) = fixture();
    let base = CanonicalBaseline::new(s.clone(), c.clone()).unwrap();
    let mut fee = c.clone();
    fee.entries[0].fees.maximum_fills += 1;
    assert_ne!(
        CanonicalBaseline::new(s.clone(), fee).unwrap().digest(),
        base.digest()
    );
    let mut rev = s.clone();
    rev.revisions.valuation += 1;
    assert_ne!(
        CanonicalBaseline::new(rev, c.clone()).unwrap().digest(),
        base.digest()
    );
    let mut wallet = s.clone();
    for asset in &mut wallet.assets {
        asset.key.wallet_id = 2;
    }
    let mut wc = c.clone();
    wc.entries[0].wallet_id = 2;
    assert_ne!(
        CanonicalBaseline::new(wallet, wc).unwrap().digest(),
        base.digest()
    );
    let mut units_changed = s;
    units_changed.assets[0].asset.version += 1;
    assert!(CanonicalBaseline::new(units_changed, c).is_err());
}
#[test]
fn adversarial_lengths_and_invalid_inputs_reject() {
    assert!(CanonicalBaseline::decode(&vec![0; MAX_BASELINE_BYTES + 1]).is_err());
    for bytes in [
        vec![0x9f, 0xff],
        vec![0x9b, 255, 255, 255, 255, 255, 255, 255, 255],
        vec![0xfb, 0, 0, 0, 0, 0, 0, 0, 0],
    ] {
        assert!(CanonicalBaseline::decode(&bytes).is_err());
    }
    let (mut s, c, _) = fixture();
    s.assets[0].ownership = vec![(0, f(0)); MAX_OWNERS + 1];
    assert_eq!(CanonicalBaseline::new(s, c).unwrap_err(), Error::Bounds);
}
#[test]
fn asset_and_policy_identity_cannot_be_silently_replaced() {
    let (s, mut c, _) = fixture();
    c.entries.push(c.entries[0].clone());
    assert_eq!(
        CanonicalBaseline::new(s.clone(), c).unwrap_err(),
        Error::Duplicate
    );
    let (_, c, _) = fixture();
    let mut bad = s;
    bad.assets[0].key = AssetKey {
        wallet_id: 1,
        asset_id: 100,
    };
    assert!(CanonicalBaseline::new(bad, c).is_err());
}
