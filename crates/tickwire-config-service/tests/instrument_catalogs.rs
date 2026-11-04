#[allow(dead_code)]
mod deployment_support;
use deployment_support::{Fixture, NOW};
use tickwire_config_service::instrument_catalogs::Catalogs;

#[test]
fn all_global_venues_bind_actual_economic_content_and_numeric_identity() {
    let f = Fixture::new(|_| {}, |_, _| {});
    let result = Catalogs::check(&f.candidate, &f.artifacts).unwrap();
    assert_eq!(result.candidate_hash(), f.candidate.hash());
    assert_eq!(result.venues().len(), 2);
    for venue in f.candidate.value()["venues"].as_array().unwrap() {
        let bound = &result.venues()[venue["id"].as_str().unwrap()];
        assert_eq!(
            bound.reference(),
            venue["instrument_catalog_ref"].as_str().unwrap()
        );
        assert_eq!(bound.catalog().assets().len(), 3);
        assert!(bound.catalog().instrument(u64::MAX).is_some());
    }
}

#[test]
fn recomputed_valid_artifact_hash_and_signed_reports_do_not_bypass_economics() {
    for fault in 0..5 {
        let f = Fixture::with_catalogs(
            NOW,
            10_000,
            |_| {},
            |_, _| {},
            |body| match fault {
                0 => body["instruments"][0]["price_tick"] = "0".into(),
                1 => body["environment"] = "Production".into(),
                2 => body["market"] = "Spot".into(),
                3 => body["assets"][0]["exponent"] = 39.into(),
                _ => body["instruments"][0]["base"] = "9".into(),
            },
        );
        f.check_reports().unwrap();
        assert!(
            Catalogs::check(&f.candidate, &f.artifacts).is_err(),
            "fault {fault}"
        );
        assert!(
            f.check(NOW).is_err(),
            "content must invoke catalog consumer: {fault}"
        );
    }
}
