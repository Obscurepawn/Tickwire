use super::*;
use crate::hosted::parameters::{self as binding, Config as ParametersConfig};

fn configured() -> Fixture {
    let mut f = fixture();
    let schema = br#"{"type":"object","properties":{"threshold":{"type":"integer","minimum":1},"enabled":{"type":"boolean"}},"required":["threshold","enabled"],"additionalProperties":false}"#;
    let schema_file = f.root.join("parameters-schema.json");
    fs::write(&schema_file, schema).unwrap();
    let reference = format!(
        "artifact://fixture/parameters/schema@sha256:{}",
        hex(&Sha256::digest(schema))
    );
    f.payload(|p| {
        p["strategy"]["parameter_schema_ref"] = reference.into();
        p["strategy"]["params"] = json!({"enabled":true,"threshold":23});
    });
    let profile = f.config.projection_launch.take().unwrap();
    f.config.projection_parameters = Some(ParametersConfig {
        object_file: profile.object_file,
        object: profile.object,
        scope: profile.scope,
        schema_file,
        timeout_ns: 1_000_000_000,
    });
    f.bootstrap.handover_enabled = true;
    replace_private(&mut f, br#"{"threshold":23,"enabled":true}"#);
    f
}
fn replace_private(f: &mut Fixture, raw: &[u8]) {
    let factory = f.bootstrap.factory.as_mut().unwrap();
    factory.configuration = tickwire_release::Object::of(raw);
    fs::write(&factory.configuration_file, raw).unwrap();
}
#[test]
fn parameter_values_bind_without_rewriting_private_encoding_or_claiming_consumption() {
    let f = configured();
    assert!(binding::prepare(&f.config, &f.bootstrap).unwrap().is_some());
    assert_eq!(
        fs::read(&f.bootstrap.factory.as_ref().unwrap().configuration_file).unwrap(),
        br#"{"threshold":23,"enabled":true}"#
    );
}
#[test]
fn a_matching_private_hash_cannot_hide_different_parameters() {
    let mut f = configured();
    replace_private(&mut f, br#"{"threshold":24,"enabled":true}"#);
    assert!(binding::prepare(&f.config, &f.bootstrap).is_err());
}
#[test]
fn duplicate_private_keys_do_not_collapse_into_approved_values() {
    let mut f = configured();
    replace_private(&mut f, br#"{"threshold":99,"threshold":23,"enabled":true}"#);
    assert!(binding::prepare(&f.config, &f.bootstrap).is_err());
}
#[test]
fn schema_contents_and_private_objects_remain_pinned() {
    for change_schema in [false, true] {
        let f = configured();
        if change_schema {
            fs::write(
                &f.config.projection_parameters.as_ref().unwrap().schema_file,
                b"{}",
            )
            .unwrap();
        } else {
            fs::write(
                &f.bootstrap.factory.as_ref().unwrap().configuration_file,
                b"{}",
            )
            .unwrap();
        }
        assert!(binding::prepare(&f.config, &f.bootstrap).is_err());
    }
}
#[test]
fn parameter_queries_require_support_and_a_bounded_nonzero_deadline() {
    for timeout in [0, 30_000_000_001] {
        let mut f = configured();
        f.config.projection_parameters.as_mut().unwrap().timeout_ns = timeout;
        assert!(binding::prepare(&f.config, &f.bootstrap).is_err());
    }
    let mut f = configured();
    f.bootstrap.handover_enabled = false;
    assert!(binding::prepare(&f.config, &f.bootstrap).is_err());
}
#[test]
fn omitted_parameter_binding_keeps_legacy_startup_untouched() {
    let mut f = configured();
    f.config.projection_parameters = None;
    fs::remove_file(&f.bootstrap.factory.as_ref().unwrap().configuration_file).unwrap();
    assert!(binding::prepare(&f.config, &f.bootstrap).unwrap().is_none());
}

#[test]
fn waiting_for_account_baseline_is_rate_limited_without_extending_the_deadline() {
    struct Waiting(usize);
    impl crate::hosted::startup::Owner for Waiting {
        fn begin(
            &mut self,
            _: tickwire_strategy_host::ObservationRequest,
        ) -> tickwire_strategy_host::Result<()> {
            self.0 += 1;
            Err(tickwire_strategy_host::Error::NotReady)
        }
        fn poll(
            &mut self,
        ) -> tickwire_strategy_host::Result<Option<tickwire_strategy_host::FreshObservation>>
        {
            panic!("no request accepted")
        }
    }
    let mut f = configured();
    f.config.projection_parameters.as_mut().unwrap().timeout_ns = 50_000_000;
    let mut progress = crate::hosted::startup::Progress::new(
        binding::prepare(&f.config, &f.bootstrap).unwrap(),
        None,
    );
    let domain = tickwire_clock::ClockDomain::new(
        f.bootstrap.market.local.host_boot,
        f.bootstrap.clock_generation,
    )
    .unwrap();
    let at = |offset: u64| tickwire_clock::MonoTime::new(domain, 1_000_000_000 + offset);
    let mut owner = Waiting(0);
    assert!(!progress.poll_at(&mut owner, at(0)).unwrap());
    assert!(!progress.poll_at(&mut owner, at(1)).unwrap());
    assert!(!progress.poll_at(&mut owner, at(9_999_999)).unwrap());
    assert_eq!(
        owner.0, 1,
        "not-ready must not reenter cold process observation on every market poll"
    );
    assert!(!progress.poll_at(&mut owner, at(10_000_000)).unwrap());
    assert_eq!(owner.0, 2);
    let allocations = allocation_counter::measure(|| {
        assert!(!progress.poll_at(&mut owner, at(10_000_001)).unwrap());
    });
    assert_eq!(allocations.count_total, 0);
    assert!(progress.poll_at(&mut owner, at(50_000_000)).is_err());
    assert!(
        progress.poll_at(&mut owner, at(10_000_002)).is_err(),
        "an expired gate must stay closed even if a caller later supplies an earlier time"
    );
    assert_eq!(
        owner.0, 2,
        "deadline cannot be renewed by repeated not-ready results"
    );
}
