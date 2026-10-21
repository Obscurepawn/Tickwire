#[allow(dead_code)]
mod deployment_support;
use deployment_support::{Fixture, NOW, parse};
use serde_json::json;
use tickwire_config::artifacts::ArtifactCatalog;
use tickwire_config_service::{
    deployment::{ContentValidated, Limits},
    evidence::{Catalog, Kind},
};

#[test]
fn one_source_closure_validates_all_candidate_content_and_exact_expiry() {
    let f = Fixture::new(|_| {}, |_, _| {});
    f.check_reports().unwrap();
    let result = f.check(NOW).unwrap();
    assert_eq!(result.candidate_hash(), f.candidate.hash());
    assert_eq!(result.checked_unix_ms(), NOW);
    assert_eq!(result.expires_unix_ms(), NOW + 2000);
    assert_eq!(result.parameters().bindings().len(), 32);
    assert_eq!(
        result.projections().targets().len(),
        f.candidate.value()["processes"].as_array().unwrap().len()
    );
    assert_eq!(result.reports().reports().len(), 6);
    assert_eq!(result.accounts().accounts().len(), 2);
    assert!(!result.credentials().uses().is_empty());
    assert_eq!(result.resources().retained_revision(), 17);
    assert_ne!(result.sources_digest(), [0; 32]);
}
#[test]
fn every_required_check_rejects_invalid_but_signed_content() {
    for kind in [
        Kind::AccountMode,
        Kind::CredentialMetadata,
        Kind::RetainedResources,
        Kind::StoragePowerLoss,
    ] {
        let f = Fixture::new(
            |_| {},
            |k, d| {
                if k != kind {
                    return;
                }
                match k {
                    Kind::AccountMode => d["trading_enabled"] = false.into(),
                    Kind::CredentialMetadata => d["enabled"] = false.into(),
                    Kind::RetainedResources => {
                        d["retained"]["allocations"] = json!([{"resource_id":"retained",
                    "host_id":"host-a","shared_bytes":8589934592u64,"private_bytes":0,"cpus":[]}])
                    }
                    Kind::StoragePowerLoss => d["passed"] = false.into(),
                    _ => unreachable!(),
                }
            },
        );
        assert!(f.check(NOW).is_err(), "required check {kind:?}");
    }
    let f = Fixture::new(
        |v| v["strategies"][0]["params"]["signal_window_events"] = 0.into(),
        |_, _| {},
    );
    f.check_reports().unwrap();
    assert!(f.check(NOW).is_err());
}
#[test]
fn missing_sources_or_changed_candidate_cannot_reuse_partial_validation() {
    let mut f = Fixture::new(|_| {}, |_, _| {});
    f.check(NOW).unwrap();
    let mut changed = f.candidate.value().clone();
    changed["strategies"][0]["params"]["signal_window_events"] = 2048.into();
    f.candidate = parse(&changed);
    assert!(f.check(NOW).is_err());
    let mut f = Fixture::new(|_| {}, |_, _| {});
    f.credential_refs.pop();
    assert!(f.check(NOW).is_err());
    let mut f = Fixture::new(|_| {}, |_, _| {});
    f.signatures = Catalog::default();
    assert!(f.check(NOW).is_err());
    let mut f = Fixture::new(|_| {}, |_, _| {});
    f.artifacts = ArtifactCatalog::new(Default::default()).unwrap();
    assert!(f.check(NOW).is_err());
}
#[test]
fn digest_binds_source_trust_limits_and_candidate_but_not_reference_order() {
    let mut f = Fixture::new(|_| {}, |_, _| {});
    let original = f.check(NOW).unwrap();
    f.credential_refs.reverse();
    assert_eq!(
        original.sources_digest(),
        f.check(NOW).unwrap().sources_digest()
    );
    let changed = ContentValidated::check(
        &f.candidate,
        f.sources(),
        &f.trust(2),
        NOW,
        Limits::default(),
    )
    .unwrap();
    assert_ne!(original.sources_digest(), changed.sources_digest());
    let mut limits = Limits::default();
    limits.resources.max_age_ms = 6000;
    let changed =
        ContentValidated::check(&f.candidate, f.sources(), &f.trust(1), NOW, limits).unwrap();
    assert_ne!(original.sources_digest(), changed.sources_digest());
    assert_eq!(changed.expires_unix_ms(), NOW + 3000);
    let f2 = Fixture::new(
        |_| {},
        |k, v| {
            if k == Kind::RetainedResources {
                v["retained"]["revision"] = "18".into()
            }
        },
    );
    assert_eq!(f.candidate.hash(), f2.candidate.hash());
    assert_ne!(
        original.sources_digest(),
        f2.check(NOW).unwrap().sources_digest()
    );
}
#[test]
fn no_result_survives_the_earliest_source_deadline() {
    let f = Fixture::new(|_| {}, |_, _| {});
    assert!(f.check(NOW + 1999).is_ok());
    assert!(f.check(NOW + 2000).is_err());
    assert!(f.check(0).is_err());
    assert!(f.check(u64::MAX).is_err());
    let mut limits = Limits::default();
    limits.accounts.max_age_ms = 0;
    assert!(ContentValidated::check(&f.candidate, f.sources(), &f.trust(1), NOW, limits).is_err());
}
