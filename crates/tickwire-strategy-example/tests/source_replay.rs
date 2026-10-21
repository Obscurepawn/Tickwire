use std::{
    path::{Path, PathBuf},
    process::Command,
};
use tickwire_ipc::{DepthCoverage, DepthLevel, JoinBody, MarketRecord, ResourceHeader, Wire};
use tickwire_recovery::FileBlobStore;
use tickwire_release::{Binding, Object, replay::*};
use tickwire_strategy_sdk::{
    Strategy,
    registered::{Registration as _, UserFactory},
    replay::{
        ReplayableStrategy,
        application::{Application, market_stream},
        durable::DurableEngine,
        process::{SourceJob, SourceReceipt},
    },
    restore::Target,
};
use tickwire_venue::CanonicalCommand;
#[path = "../../tickwire-account/tests/common/venue.rs"]
mod test_command;
use tickwire_strategy_example::{Parameters, Registration};
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
fn market_fixture() -> (
    ResourceHeader,
    tickwire_strategy_sdk::MarketReplica,
    Vec<u8>,
) {
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
    let replica =
        tickwire_strategy_sdk::MarketReplica::load(join.topic_id, join.ring_epoch, &validated, 4)
            .unwrap();
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

fn object(dir: &Path, bytes: &[u8]) -> tickwire_strategy_sdk::preflight::ObjectFile {
    use std::io::Write;
    let o = Object::of(bytes);
    let p = dir.join(
        o.digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>(),
    );
    let mut f = std::fs::File::create(&p).unwrap();
    f.write_all(bytes).unwrap();
    f.sync_all().unwrap();
    std::fs::File::open(dir).unwrap().sync_all().unwrap();
    tickwire_strategy_sdk::preflight::ObjectFile { path: p, object: o }
}
struct Temp(PathBuf);
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[test]
fn registered_old_executable_replays_its_own_callbacks_and_returns_the_actual_child_receipt() {
    use std::os::unix::fs::DirBuilderExt;
    let temp = Temp(std::env::temp_dir().join(format!(
            "tickwire-registered-process-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )));
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&temp.0)
        .unwrap();
    let worker = env!("CARGO_BIN_EXE_tickwire-counting-v1-worker");
    let binary = tickwire_recovery::digest(&std::fs::read(worker).unwrap());
    let (resource, replica, raw) = market_fixture();
    let stream = market_stream(&resource).unwrap();
    let mut command = test_command::command();
    command.context.request.deadline = 10_000_000_000;
    command.context.instrument.instrument_id = 1001;
    command.context.instrument.instrument_version = 5;
    let mut triggers = command.context.triggers.as_slice().to_vec();
    triggers[0].instrument_id = 1001;
    triggers[0].instrument_version = 5;
    command.context.triggers = tickwire_venue::BoundedList::new(triggers).unwrap();
    let target = Target {
        instance: command.context.strategy.instance_id,
        generation: command.context.request.strategy_generation,
        logical_strategy: command.context.strategy.logical_strategy_id,
        command: None,
        minimum_bid_ticks: 0,
        enabled: false,
    };
    let config = VersionedBytes {
        revision: command.context.request.config_revision,
        bytes: Parameters {
            schema: 1,
            enabled: true,
            minimum_observations: 1,
            minimum_bid_ticks: 1,
            command: Some(CanonicalCommand::new(command).unwrap().bytes().to_vec()),
        }
        .bytes()
        .unwrap(),
    };
    let strategy = Registration::v1()
        .configure(&target, &config)
        .unwrap()
        .create()
        .unwrap();
    let registration = strategy.replay_registration().unwrap().unwrap();
    let app = Application::new(strategy, 1_000_000_000, vec![(resource, replica, 4)]).unwrap();
    let source = Source {
        logical_strategy: 8,
        instance: [2; 16],
        generation: 5,
        binary_digest: binary,
        state_schema: registration.schema,
        input_stream: [4; 16],
        clock: ClockDomain {
            boot_id: [5; 16],
            generation: 1,
        },
    };
    let runtime = RuntimeState {
        now_ns: 1_500_000_000,
        config: registration.config,
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
    let initial = Checkpoint::new(
        source.clone(),
        0,
        [0; 32],
        app.checkpoint().unwrap(),
        runtime,
    )
    .unwrap();
    let anchor = verify_anchor(
        &initial.bytes().unwrap(),
        &source,
        Object::of(&initial.runtime.config.bytes),
        &[],
    )
    .unwrap();
    let store = FileBlobStore::open(temp.0.join("inputs")).unwrap();
    let mut live = DurableEngine::new(anchor, app, store).unwrap();
    live.apply(
        Input::MarketApplied {
            stream: stream.clone(),
            sequence: 1001,
            received_ns: 1_600_000_000,
            record: raw,
            payload: vec![],
        },
        1_600_000_001,
    )
    .unwrap();
    live.apply(
        Input::MarketCallback {
            stream,
            instrument: 1001,
            invoked: true,
            maximum_age_ns: 1_000_000_000,
        },
        1_600_000_001,
    )
    .unwrap();
    let mut emitted = 0;
    live.flush()
        .unwrap()
        .publish(|_| {
            emitted += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(emitted, 1);
    assert_ne!(initial.cut.business, live.last_durable_cut().business);
    let request = ControlInput::Capture {
        source: source.clone(),
        anchor: Object::of(&initial.bytes().unwrap()),
        expected: initial.cut.clone(),
        pause: true,
    };
    let capture_object = live.capture(&request).unwrap();
    let capture_bytes = live.read_object(capture_object).unwrap();
    let captured = CapturedTail::decode(&capture_bytes).unwrap();
    let job = SourceJob {
        schema: 1,
        binding: Binding {
            operation_id: [8; 16],
            revision: 1,
            logical_strategy: 8,
            members_digest: [9; 32],
        },
        source,
        request: object(&temp.0, &request.bytes().unwrap()),
        capture: object(&temp.0, &capture_bytes),
        anchor: object(&temp.0, &live.read_object(captured.anchor).unwrap()),
        tail: object(&temp.0, &live.read_object(captured.tail).unwrap()),
        checkpoint: object(&temp.0, &live.read_object(captured.checkpoint).unwrap()),
        output_directory: temp.0.join("replayed"),
    };
    let job_path = temp.0.join("job.json");
    std::fs::write(&job_path, job.bytes().unwrap()).unwrap();
    let mut child = Command::new(worker)
        .arg("--source-replay")
        .arg(&job_path)
        .spawn()
        .unwrap();
    let pid = child.id();
    assert!(child.wait().unwrap().success());
    let receipt = SourceReceipt::decode(
        &std::fs::read(job.output_directory.join("SOURCE_REPLAY.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(receipt.pid, pid);
    assert_ne!(pid, std::process::id());
    assert_eq!(receipt.cut, *live.last_durable_cut());
    assert_eq!(receipt.output, job.checkpoint.object);
    assert_eq!(receipt.source, job.source);
    assert!(!job.output_directory.join("commands").exists());
}
