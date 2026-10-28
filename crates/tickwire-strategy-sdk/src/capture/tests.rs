use super::*;
use tickwire_release::{
    rebuild::{MarketRequirement, Ordering},
    replay::{ClockDomain, Rng},
};
pub(crate) fn fixture() -> Bootstrap {
    let mut market = crate::transition_fixture::approval().previous;
    let mut actual = ResourceHeader::decode(&market.bank_header).unwrap();
    actual.descriptor_hash = [0; 32];
    actual.descriptor_hash =
        tickwire_recovery::digest(&crate::bootstrap::wire_bytes(&actual).unwrap());
    market.bank_header = crate::bootstrap::wire_bytes(&actual).unwrap();
    let local = market.endpoint.local.clone();
    let mut report = market.endpoint.clone();
    report.peer = (&crate::transition_fixture::identity(2, 4)).into();
    report.path = "/tmp/capture-report.sock".into();
    let mut header = ResourceHeader::decode(&market.bank_header).unwrap();
    header.resource_id = [88; 16];
    header.resource_kind = 3;
    header.required_features |= crate::reports::FEATURE;
    header.producer_instance = report.peer.instance;
    header.consumer_instance = local.instance;
    header.risk_scope = 1;
    let config = VersionedBytes {
        revision: 1,
        bytes: vec![1],
    };
    Bootstrap {
        schema: 1,
        identity: Identity {
            capture: [90; 16],
            logical_strategy: local.logical_strategy,
            risk_domain: [1; 16],
            instance: local.instance,
            generation: local.generation,
            binary: local.build_hash,
            clock: ClockDomain {
                boot_id: local.host_boot,
                generation: 1,
            },
        },
        contract: Contract {
            schema: 1,
            factory: "capture-test".into(),
            binary: local.build_hash,
            state_schema: 1,
            config: Object::of(&config.bytes),
            models: vec![],
            ordering: Ordering::ObservedRebuildWindow,
            markets: vec![MarketRequirement {
                stream: crate::replay::application::market_stream(
                    &ResourceHeader::decode(&market.bank_header).unwrap(),
                )
                .unwrap(),
                instrument: 1001,
                category: 1,
                minimum_observations: 1,
                known_bid_prices: vec![],
                known_ask_prices: vec![],
            }],
            actions: vec![],
            minimum_span_ns: 0,
            maximum_span_ns: 1_000_000_000,
            maximum_age_ns: 1_000_000_000,
            maximum_rows: 64,
            maximum_bytes: 1_000_000,
            rng: Rng::Disabled,
        },
        config,
        models: vec![],
        market,
        market_admission_ready: "/tmp/capture-md-ready.json".into(),
        report,
        report_header: crate::bootstrap::wire_bytes(&header).unwrap(),
        report_cursor_id: [89; 16],
        report_subscription_generation: 1,
        report_directory: "/tmp/capture-reports".into(),
        output_directory: "/tmp/capture-output".into(),
        initial_account_position: 0,
        deadline_mono_ns: 2_000_000_000,
        poll_records: 64,
        idle_sleep_us: 100,
    }
}
#[test]
fn valid_capture_binding_accepts_zero_account_cut_and_binds_exact_object() {
    let config = fixture();
    let now = MonoTime::new(
        config.market.domain().unwrap(),
        crate::transition_fixture::NOW,
    );
    config.validate(now).unwrap();
    let request = SealRequest {
        schema: 1,
        capture: config.identity.capture,
        bootstrap: Object::of(&config.bytes().unwrap()),
        contract: Object::of(&config.contract.bytes().unwrap()),
        account_position: 0,
        deadline_mono_ns: config.deadline_mono_ns,
    };
    request.validate(&config, now).unwrap();
    let body = wire_request(&config, &request, config.deadline_mono_ns).unwrap();
    assert_eq!(body.replay_kind, 8);
    assert_eq!(body.expected_sequence, 0);
    assert_eq!(body.expected_cut_digest, request.contract.digest);
    let envelope = ControlEnvelope {
        opcode: 41,
        flags: 1,
        request_id: body.challenge,
        ..Default::default()
    };
    verify_request(
        &envelope,
        &body,
        &config,
        &request,
        now,
        config.deadline_mono_ns,
    )
    .unwrap();
    for case in 0..5 {
        let mut bad = body.clone();
        match case {
            0 => bad.expected_sequence = 1,
            1 => bad.expected_cut_digest = [7; 32],
            2 => bad.operation_id = [7; 16],
            3 => bad.operation_revision = 2,
            _ => bad.object_digest = [7; 32],
        };
        assert!(
            verify_request(
                &envelope,
                &bad,
                &config,
                &request,
                now,
                config.deadline_mono_ns
            )
            .is_err()
        );
    }
}
#[test]
fn capture_bootstrap_rejects_cross_identity_clock_and_future_deadline() {
    let config = fixture();
    let now = MonoTime::new(
        config.market.domain().unwrap(),
        crate::transition_fixture::NOW,
    );
    for case in 0..6 {
        let mut bad = config.clone();
        match case {
            0 => bad.report.local.instance = [99; 16],
            1 => bad.identity.clock.generation += 1,
            2 => bad.deadline_mono_ns = now.nanos(),
            3 => bad.initial_account_position = u64::MAX,
            4 => bad.config.bytes = vec![2],
            _ => bad.output_directory = "relative".into(),
        };
        assert!(bad.validate(now).is_err(), "case {case}");
    }
}
#[test]
fn capture_template_and_actual_descriptor_bind_the_same_producer_stream() {
    let config = fixture();
    let now = MonoTime::new(
        config.market.domain().unwrap(),
        crate::transition_fixture::NOW,
    );
    config.validate(now).unwrap();
    let actual = config.qualified_market_header().unwrap();
    let mut template = config.clone();
    let mut header = actual.clone();
    header.descriptor_hash = [0; 32];
    template.market.bank_header = crate::bootstrap::wire_bytes(&header).unwrap();
    template.validate(now).unwrap();
    assert_eq!(template.qualified_market_header().unwrap(), actual);
    header.descriptor_hash = [9; 32];
    template.market.bank_header = crate::bootstrap::wire_bytes(&header).unwrap();
    assert!(template.validate(now).is_err());
}
