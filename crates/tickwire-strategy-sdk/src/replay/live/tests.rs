use super::*;
use crate::bootstrap::{Endpoint, IdentityConfig};
pub(crate) fn bootstrap(path: &std::path::Path) -> Bootstrap {
    let local = IdentityConfig {
        instance: [2; 16],
        generation: 5,
        role: 3,
        logical_strategy: 8,
        host_boot: [5; 16],
        namespace: [6; 16],
        build_hash: [7; 32],
        qualification_hash: [8; 32],
        manifest_hash: [9; 32],
        arch: 1,
        trust_profile: 1,
    };
    let ep = Endpoint {
        path: path.join("control"),
        local: local.clone(),
        peer: local,
        peer_pid: 1,
        peer_uid: 0,
        peer_gid: 0,
        required_features: 0,
        supported_features: 0,
        control_epoch: 1,
        lease_duration_ns: 100,
        max_frame_bytes: 4096,
        nonce: [1; 32],
    };
    Bootstrap {
        instrument_catalog: None,
        version: 1,
        market: ep.clone(),
        external_market: None,
        market_admission_ready: None,
        intent: ep.clone(),
        report: ep,
        report_header: vec![],
        report_cursor_id: [1; 16],
        report_subscription_generation: 1,
        report_directory: path.join("reports"),
        join_file: path.join("join"),
        intent_header: vec![],
        intent_cursor_id: [2; 16],
        intent_subscription_generation: 1,
        risk_domain: [1; 16],
        command_directory: path.join("commands"),
        clock_generation: 1,
        startup_timeout_ms: 100,
        maximum_market_age_ns: 1_000_000_000,
        maximum_levels: 4,
        poll_records: 8,
        idle_sleep_us: 1,
        recovery_template: Some(path.join("recovery.json")),
        publish_checkpoints: true,
        preflight_job: None,
        factory: None,
        demo_enabled: true,
        demo_command_file: None,
        demo_minimum_bid_ticks: 1,
    }
}
fn demo() -> crate::FixedDemo {
    let mut c = crate::test_command::command();
    c.context.request.deadline = 10_000_000_000;
    c.context.instrument.instrument_id = 1001;
    c.context.instrument.instrument_version = 5;
    let mut refs = c.context.triggers.as_slice().to_vec();
    refs[0].instrument_id = 1001;
    refs[0].instrument_version = 5;
    c.context.triggers = tickwire_venue::BoundedList::new(refs).unwrap();
    crate::FixedDemo::new(CanonicalCommand::new(c).unwrap(), 1, true)
}
struct Temp(std::path::PathBuf);
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn temporary() -> Temp {
    let p = std::env::temp_dir().join(format!(
        "tickwire-live-replay-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&p).unwrap();
    Temp(p)
}
#[test]
fn production_owner_has_synced_actual_record_and_checkpoint_before_returning_the_order() {
    let root = temporary();
    let config = bootstrap(&root.0);
    std::fs::create_dir(&config.command_directory).unwrap();
    let (resource, replica, raw) = application::tests::market_fixture();
    let mut owner = Live::new(&config, demo(), resource, replica, 1_500_000_000).unwrap();
    let commands = owner.market(&raw, &[], 1_600_000_001).unwrap();
    assert_eq!(commands.len(), 1);
    let head = Head::decode(&std::fs::read(owner.directory.join("HEAD.json")).unwrap()).unwrap();
    let Owner::Recorded(e) = &owner.owner else {
        panic!()
    };
    assert_eq!(head.cut.sequence, 2);
    assert_eq!(head.commit, e.commit());
    let commit = InputCommit::decode(&e.read_object(head.commit).unwrap()).unwrap();
    let checkpoint = Checkpoint::decode(&e.read_object(commit.checkpoint).unwrap()).unwrap();
    assert_eq!(checkpoint.cut, head.cut);
    let rows = verify_tail(
        &verify_anchor(
            &e.anchor().bytes().unwrap(),
            &head.source,
            Object::of(&e.anchor().runtime.config.bytes),
            &[],
        )
        .unwrap(),
        &e.read_object(commit.tail).unwrap(),
        &head.cut,
    )
    .unwrap();
    assert_eq!(
        rows.rows().last().unwrap().effects[0].command,
        commands[0].bytes()
    );
}
#[test]
fn head_publish_failure_stops_the_owner_even_after_the_input_files_are_synced() {
    let root = temporary();
    let config = bootstrap(&root.0);
    std::fs::create_dir(&config.command_directory).unwrap();
    let (resource, replica, raw) = application::tests::market_fixture();
    let mut owner = Live::new(&config, demo(), resource, replica, 1_500_000_000).unwrap();
    std::fs::write(
        owner.directory.join("HEAD.pending"),
        b"interrupted publication",
    )
    .unwrap();
    assert!(owner.market(&raw, &[], 1_600_000_001).is_err());
    let Owner::Recorded(e) = &owner.owner else {
        panic!()
    };
    assert!(e.stopped());
    assert_eq!(e.last_durable_cut().sequence, 2);
    let head = Head::decode(&std::fs::read(owner.directory.join("HEAD.json")).unwrap()).unwrap();
    assert_eq!(head.cut.sequence, 0);
    assert!(owner.market(&raw, &[], 1_600_000_002).is_err());
}
#[test]
fn transition_head_failure_never_publishes_the_new_live_stream() {
    let root = temporary();
    let config = bootstrap(&root.0);
    std::fs::create_dir(&config.command_directory).unwrap();
    let (resource, replica, _) = application::tests::market_fixture();
    let mut live = Live::new(&config, demo(), resource, replica, 1_500_000_000).unwrap();
    assert!(live.supports_market_transition());
    let old = live.market.clone();
    let Owner::Recorded(e) = &mut live.owner else {
        panic!()
    };
    let (input, _, replica) =
        application::transition_tests::fixture(e.durable_checkpoint().runtime.markets[0].clone());
    std::fs::write(
        live.directory.join("HEAD.pending"),
        b"interrupted publication",
    )
    .unwrap();
    assert!(
        persist_transition(
            e,
            &live.directory,
            input,
            &replica.checkpoint().unwrap(),
            1_600_000_003
        )
        .is_err()
    );
    assert!(e.stopped());
    assert_eq!(e.last_durable_cut().sequence, 1);
    assert_eq!(live.market, old);
    let head = Head::decode(&std::fs::read(live.directory.join("HEAD.json")).unwrap()).unwrap();
    assert_eq!(head.cut.sequence, 0);
    assert!(!live.supports_market_transition());
}

#[test]
fn transition_capacity_and_actual_view_mismatch_are_rejected_before_input_apply() {
    for oversized in [false, true] {
        let root = temporary();
        let config = bootstrap(&root.0);
        std::fs::create_dir(&config.command_directory).unwrap();
        let (resource, replica, _) = application::tests::market_fixture();
        let mut live = Live::new(&config, demo(), resource, replica, 1_500_000_000).unwrap();
        let Owner::Recorded(e) = &mut live.owner else {
            panic!()
        };
        let (mut input, _, replica) = application::transition_tests::fixture(
            e.durable_checkpoint().runtime.markets[0].clone(),
        );
        let expected = if oversized {
            let Input::MarketTransition { baseline, .. } = &mut input else {
                panic!()
            };
            baseline.resize(MAX_BYTES + 1, 0);
            replica.checkpoint().unwrap()
        } else {
            vec![1]
        };
        assert!(persist_transition(e, &live.directory, input, &expected, 1_600_000_003).is_err());
        assert!(e.stopped());
        assert_eq!(e.memory_cut().sequence, 0);
        assert_eq!(e.last_durable_cut().sequence, 0);
    }
}
