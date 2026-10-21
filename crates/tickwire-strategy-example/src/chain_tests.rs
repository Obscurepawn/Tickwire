use super::*;
use sdk::{
    registered::{Registration as _, UserFactory},
    replay::{
        Engine, ReplayableStrategy,
        application::{Application, ApplicationFactory},
        replay_source,
    },
};
use tickwire_ipc::{DepthCoverage, DepthLevel, JoinBody, MarketRecord, ResourceHeader, Wire};
use tickwire_release::{Object, replay::*};
#[path = "../../tickwire-account/tests/common/venue.rs"]
pub(super) mod venue;
fn vector(name: &str) -> Vec<u8> {
    let all: serde_json::Value =
        serde_json::from_str(include_str!("../../../specs/contracts/ipc-vectors-v1.json")).unwrap();
    let hex = all["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == name)
        .unwrap()["hex"]
        .as_str()
        .unwrap();
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}
pub(super) fn market_fixture() -> (ResourceHeader, sdk::MarketReplica, Vec<u8>) {
    use tickwire_snapshot::*;
    let mut resource = ResourceHeader::decode(&vector("normalized_resource_header")).unwrap();
    resource.required_features |= 64;
    resource.descriptor_hash = [0; 32];
    let mut join = JoinBody::decode(&vector("join_normalized_snapshot")).unwrap();
    join.snapshot_hash = [0; 32];
    join.snapshot_id = [0; 16];
    let states = [
        "normalized_state_quote",
        "normalized_state_trade",
        "normalized_state_mark",
        "normalized_state_feed",
    ]
    .map(|s| MarketRecord::decode(&vector(s)).unwrap());
    let bids = [DepthLevel::decode(&vector("normalized_depth_1")).unwrap()];
    let asks = [DepthLevel::decode(&vector("normalized_depth_2")).unwrap()];
    let specs = [InstrumentSpec {
        instrument_id: 1001,
        instrument_version: 5,
        required_states: QUOTE | TRADE | MARK_FUNDING,
    }];
    let members = [InstrumentSnapshot {
        instrument_id: 1001,
        instrument_version: 5,
        snapshot_id: [18, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        at_sequence: 1000,
        last_event_sequence: 990,
        feed_generation: 3,
        catalog_version: 5,
        catalog_hash: [0x22; 32],
        created_mono_ns: 1_000_000_000,
        expires_mono_ns: 2_000_000_000,
        state_version: 990,
        coverage_depth: 1,
        coverage_flags: 3,
        states: &states,
        bids: &bids,
        asks: &asks,
    }];
    let request = BuildRequest {
        resource: &resource,
        join: &join,
        catalog_hash: [0x22; 32],
        instrument_set_version: 5,
        sequence: 1000,
        created_mono_ns: 1_000_000_000,
        expires_mono_ns: 2_000_000_000,
        now_mono_ns: 1_500_000_000,
        logical_strategy_id: 0,
        instruments: &specs,
        members: &members,
        byte_limit: MAX_TOPIC_SNAPSHOT_BYTES,
    };
    let built = build_topic_with_coverage(
        &request,
        &[DepthCoverage {
            version: 1,
            bid_kind: 2,
            ask_kind: 2,
            bid_boundary_ticks: bids[0].price_ticks,
            ask_boundary_ticks: asks[0].price_ticks,
            source_book_generation: 3,
        }],
    )
    .unwrap();
    let validated = validate(
        built.bytes(),
        &JoinContext {
            expected_resource: built.resource(),
            catalog_hash: [0x22; 32],
            instruments: &specs,
            join: built.join(),
            now_mono_ns: 1_500_000_000,
        },
    )
    .unwrap();
    let replica = sdk::MarketReplica::load(join.topic_id, join.ring_epoch, &validated, 4).unwrap();
    let mut next = states[0].clone();
    next.header.sequence = 1001;
    next.header.flags = 6;
    next.header.record_crc32c = 0;
    next.header.ingress_mono_ns = 1_600_000_000;
    next.header.publish_mono_ns = 1_600_000_001;
    let mut bytes = vec![0; 512];
    next.encode(&mut bytes).unwrap();
    (built.resource().clone(), replica, bytes)
}
#[test]
fn custom_old_callbacks_replay_to_c_then_migration_and_config_keep_the_action_consumed() {
    let (resource, replica, record) = market_fixture();
    let stream = sdk::replay::application::market_stream(&resource).unwrap();
    let mut command = venue::command();
    command.context.request.deadline = 3_000_000_000;
    command.context.instrument.instrument_id = 1001;
    command.context.instrument.instrument_version = 5;
    let parameters = Parameters {
        schema: 1,
        enabled: true,
        minimum_observations: 1,
        minimum_bid_ticks: 0,
        command: Some(
            CanonicalCommand::new(command.clone())
                .unwrap()
                .bytes()
                .to_vec(),
        ),
    };
    let config = VersionedBytes {
        revision: 6,
        bytes: parameters.bytes().unwrap(),
    };
    let factory = Factory::from_config(V1, config.clone()).unwrap();
    let app = Application::new(
        factory.create().unwrap(),
        1_000_000_000,
        vec![(resource, replica, 4)],
    )
    .unwrap();
    let source = Source {
        logical_strategy: 8,
        instance: [2; 16],
        generation: 5,
        binary_digest: [3; 32],
        state_schema: V1,
        input_stream: [4; 16],
        clock: ClockDomain {
            boot_id: [5; 16],
            generation: 1,
        },
    };
    let runtime = RuntimeState {
        now_ns: 1_500_000_000,
        config: config.clone(),
        models: vec![],
        markets: app.market_cursors().unwrap(),
        accounts: vec![],
        external: vec![],
        timers: vec![],
        rng: Rng::Disabled,
        effects: EffectCut {
            count: 0,
            digest: [0; 32],
        },
    };
    let checkpoint = Checkpoint::new(
        source.clone(),
        0,
        [0; 32],
        app.checkpoint().unwrap(),
        runtime,
    )
    .unwrap();
    let anchor = verify_anchor(
        &checkpoint.bytes().unwrap(),
        &source,
        Object::of(&config.bytes),
        &[],
    )
    .unwrap();
    let mut live = Engine::new(&anchor, app).unwrap();
    live.apply_live(
        Input::MarketApplied {
            stream: stream.clone(),
            sequence: 1001,
            received_ns: 1_600_000_000,
            record,
            payload: vec![],
        },
        1_600_000_001,
    )
    .unwrap();
    let callback = Input::MarketCallback {
        stream: stream.clone(),
        instrument: 1001,
        invoked: true,
        maximum_age_ns: 1_000_000_000,
    };
    assert_eq!(
        live.apply_live(callback.clone(), 1_600_000_001)
            .unwrap()
            .commands
            .len(),
        1
    );
    let cut = live.checkpoint().unwrap();
    let tail = verify_tail(
        &anchor,
        &encode_tail(&anchor, live.recorded()).unwrap(),
        &cut.cut,
    )
    .unwrap();
    let replayed = replay_source(
        &ApplicationFactory {
            binary_digest: source.binary_digest,
            business: Registration::v1().source(V1).unwrap(),
        },
        &anchor,
        &tail,
    )
    .unwrap();
    assert_eq!(replayed.checkpoint(), &cut);
    let old_bytes = sdk::replay::application::business_bytes(&cut.business).unwrap();
    let state = State::at_c(&old_bytes, V1, V2).unwrap();
    assert_eq!(state.observations, 1);
    assert!(state.spent);
    command.context.strategy.instance_id = [7; 16];
    command.context.request.strategy_generation = 6;
    command.context.request.config_revision = 7;
    let new_parameters = Parameters {
        minimum_observations: 2,
        command: Some(CanonicalCommand::new(command).unwrap().bytes().to_vec()),
        ..parameters.clone()
    };
    let next = VersionedBytes {
        revision: 7,
        bytes: new_parameters.bytes().unwrap(),
    };
    let migrated = Counting {
        state,
        configuration: config.clone(),
        parameters,
    };
    let (resource, mut replica, record) = market_fixture();
    replica.apply(&record, &[]).unwrap();
    let app = Application::new(migrated, 1_000_000_000, vec![(resource, replica, 4)]).unwrap();
    let source = Source {
        instance: [7; 16],
        generation: 6,
        binary_digest: [8; 32],
        state_schema: V2,
        input_stream: [7; 16],
        ..source
    };
    let checkpoint = Checkpoint::new(
        source.clone(),
        0,
        [0; 32],
        app.checkpoint().unwrap(),
        cut.runtime,
    )
    .unwrap();
    let anchor = verify_anchor(
        &checkpoint.bytes().unwrap(),
        &source,
        Object::of(&config.bytes),
        &[],
    )
    .unwrap();
    let mut new = Engine::new(&anchor, app).unwrap();
    new.apply_live(
        Input::ConfigApply {
            previous: Object::of(&config.bytes),
            next: next.clone(),
        },
        1_600_000_002,
    )
    .unwrap();
    assert!(
        new.apply_live(callback, 1_600_000_003)
            .unwrap()
            .commands
            .is_empty()
    );
    let saved = new.checkpoint().unwrap();
    let state = State::decode(
        &sdk::replay::application::business_bytes(&saved.business).unwrap(),
        V2,
    )
    .unwrap();
    assert_eq!(state.observations, 2);
    assert!(state.spent);
    assert_eq!(saved.runtime.config, next);
    assert_eq!(saved.runtime.effects.count, 1);
}

#[test]
fn user_registration_constructs_a_running_strategy_from_the_qualified_configuration() {
    use std::os::unix::fs::PermissionsExt;
    let directory = std::env::temp_dir().join(format!("tw-counting-config-{}", std::process::id()));
    std::fs::create_dir(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    struct Clean(std::path::PathBuf);
    impl Drop for Clean {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _clean = Clean(directory.clone());
    let params = Parameters {
        schema: 1,
        enabled: false,
        minimum_observations: 2,
        minimum_bid_ticks: 0,
        command: None,
    };
    let mut artifacts = Vec::new();
    let mut locations = Vec::new();
    for (i, role) in [
        tickwire_release::Role::Binary,
        tickwire_release::Role::HostConfig,
        tickwire_release::Role::Bootstrap,
        tickwire_release::Role::StrategyConfig,
    ]
    .into_iter()
    .enumerate()
    {
        let bytes = if i == 3 {
            params.bytes().unwrap()
        } else {
            vec![i as u8 + 1; 8]
        };
        let path = directory.join(format!("{i}"));
        std::fs::write(&path, &bytes).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        artifacts.push(tickwire_release::Artifact {
            role: role.clone(),
            object: Object::of(&bytes),
        });
        locations.push(tickwire_release::files::Location { role, path });
    }
    let binding = registered::FactoryBinding {
        id: "counting_example".into(),
        configuration_file: locations[3].path.clone(),
        configuration: artifacts[3].object,
        revision: 3,
    };
    let loaded = tickwire_release::files::load(&artifacts, &locations).unwrap();
    let target = restore::Target {
        instance: [2; 16],
        generation: 5,
        logical_strategy: 8,
        command: None,
        minimum_bid_ticks: 0,
        enabled: false,
    };
    let configured =
        registered::configure(&Registration::v2(), &binding, &target, &loaded).unwrap();
    let strategy = configured.create().unwrap();
    let state = State::decode(&strategy.checkpoint().unwrap(), V2).unwrap();
    assert_eq!(state.observations, 0);
    assert!(!state.spent);
    assert_eq!(
        strategy.replay_registration().unwrap().unwrap().config,
        VersionedBytes {
            revision: 3,
            bytes: params.bytes().unwrap()
        }
    );
    assert!(Registration::v1().source(V2).is_err());
    std::fs::write(&binding.configuration_file, b"changed").unwrap();
    assert!(registered::configure(&Registration::v2(), &binding, &target, &loaded).is_err());
}
