use super::{account_fixture, fixture::write};
use serde_json::{Value, json};
use std::path::Path;
use tickwire_config::{
    Candidate, Format, Limits,
    projection::{PinnedPolicy, project},
    projection_object::ProjectionObject,
};
use tickwire_recovery::digest;
use tickwire_strategy_sdk::{
    bootstrap::Bootstrap,
    recovery::{CHECKPOINT_SCHEMA, DependencyFile, Template},
};

pub fn hex(bytes: [u8; 32]) -> String {
    bytes.iter().map(|v| format!("{v:02x}")).collect()
}
pub fn projection(build: [u8; 32], params: &Value, schema: &[u8]) -> ProjectionObject {
    let process_id = "strategy-01-active";
    let bytes=serde_json::to_vec(&json!({"schema":1,"revision":1,"targets":[{"process_id":process_id,"account_ids":["spot-main","usdm-main"],"venue_ids":["binance-spot","binance-usdm"],"topic_ids":[1,2],"secret_purposes":[]}]})).unwrap();
    let reference = format!(
        "artifact://fixture/config/policy@sha256:{}",
        hex(digest(&bytes))
    );
    let mut root = Candidate::parse(
        Format::Toml,
        include_bytes!("../../../../config/reference.toml"),
        Limits::default(),
    )
    .unwrap()
    .value()
    .clone();
    root["identity"]["namespace_id"] = "09090909-0909-0909-0909-090909090909".into();
    root["local_config"]["projection_policy_ref"] = reference.clone().into();
    root["local_config"]["projection_policy_revision"] = 1.into();
    let process = root["processes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|p| p["id"] == process_id)
        .unwrap();
    process["artifact_ref"] =
        format!("artifact://fixture/strategy/worker@sha256:{}", hex(build)).into();
    process["restart_policy"] = "never".into();
    for strategy in root["strategies"].as_array_mut().unwrap() {
        if strategy["process_id"] == process_id {
            strategy["params"] = params.clone();
            strategy["parameter_schema_ref"] = format!(
                "artifact://fixture/strategy/parameters@sha256:{}",
                hex(digest(schema))
            )
            .into();
        }
    }
    let candidate = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&root).unwrap(),
        Limits::default(),
    )
    .unwrap();
    let policy = PinnedPolicy::decode(&reference, &bytes).unwrap();
    ProjectionObject::build(
        41,
        &project(&candidate, process_id, &policy).unwrap(),
        4 * 1024 * 1024,
    )
    .unwrap()
}
pub fn schema() -> Vec<u8> {
    serde_json::to_vec(&json!({"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["schema","enabled","minimum_observations","minimum_bid_ticks","command"],"properties":{"schema":{"const":1},"enabled":{"type":"boolean"},"minimum_observations":{"type":"integer","minimum":1},"minimum_bid_ticks":{"type":"integer","minimum":0},"command":{"type":"array","items":{"type":"integer","minimum":0,"maximum":255}}}})).unwrap()
}

pub fn recovery(
    root: &Path,
    worker: &Path,
    b: &Bootstrap,
    baseline: &tickwire_account_codec::CanonicalBaseline,
    params: &[u8],
    policy_versions: &tickwire_runtime_policy::Versions,
) {
    let owner = b.market.local.instance;
    let values=[baseline.bytes().to_vec(),serde_json::to_vec(&json!({"initial_strategy_sequence":1,"account_baseline":baseline.digest()})).unwrap(),b"[]".to_vec(),b"null".to_vec(),serde_json::to_vec(policy_versions).unwrap(),serde_json::to_vec(&json!({"instance":owner,"generation":b.market.local.generation,"logical_strategy":b.market.local.logical_strategy})).unwrap(),std::fs::read(worker).unwrap(),serde_json::to_vec(b).unwrap(),include_bytes!("../../../../specs/contracts/ipc-wire-v1.json").to_vec(),params.to_vec()];
    let dependencies = values
        .iter()
        .enumerate()
        .map(|(i, bytes)| {
            let path = root.join(format!("dep-{}.bin", i + 2));
            write(&path, bytes);
            DependencyFile {
                kind: i as u8 + 2,
                schema_id: if i == 9 { CHECKPOINT_SCHEMA } else { 1 },
                owner,
                path,
                digest: digest(bytes),
            }
        })
        .collect();
    let template = Template {
        dependencies,
        declared_rebuild: false,
        fault_model_id: [121; 32],
        pin_id: [122; 16],
        owner,
        input_stream: owner,
        output_directory: root.join("recovery"),
        maximum_input_bytes: 16 * 1024 * 1024,
        order_ids: vec![[123; 16]],
    };
    write(
        b.recovery_template.as_ref().unwrap(),
        &serde_json::to_vec(&template).unwrap(),
    );
}
pub fn command(
    b: &Bootstrap,
    instrument: &tickwire_venue::InstrumentBinding,
    now: u64,
) -> tickwire_venue::CanonicalCommand {
    let mut c = account_fixture::venue::command();
    c.context.request.strategy_generation = b.market.local.generation;
    c.context.request.config_revision = 6;
    c.context.request.deadline = now + 60_000_000_000;
    c.context.strategy.created_mono_ns = now;
    c.context.strategy.logical_strategy_id = b.market.local.logical_strategy;
    c.context.strategy.instance_id = b.market.local.instance;
    c.context.instrument = instrument.clone();
    c.context.triggers = tickwire_venue::BoundedList::new(vec![tickwire_venue::TriggerRef {
        topic_id: 101,
        feed_generation: 1,
        ring_epoch: 1,
        sequence: 1,
        instrument_id: instrument.instrument_id,
        instrument_version: instrument.instrument_version,
        observed_mono_ns: now,
    }])
    .unwrap();
    tickwire_venue::CanonicalCommand::new(c).unwrap()
}
