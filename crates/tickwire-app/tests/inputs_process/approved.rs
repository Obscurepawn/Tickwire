use super::{Fixture, fixture::write, inputs};
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use tickwire_app::strategy::AppStrategy;
use tickwire_config::{
    Candidate, Format, Limits,
    projection::{PinnedPolicy, project},
    projection_object::ProjectionObject,
};
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_release::Object;

#[derive(Clone, Copy)]
pub enum Extra {
    None,
    Account,
    Topic,
}

fn projection(f: &Fixture, extra: Extra) -> ProjectionObject {
    let process_id = "strategy-01-active";
    let accounts = if matches!(extra, Extra::Account) {
        json!(["spot-main", "usdm-main"])
    } else {
        json!(["spot-main"])
    };
    let topics = if matches!(extra, Extra::Topic) {
        json!([101, 2])
    } else {
        json!([101])
    };
    let venues = if matches!(extra, Extra::Account) {
        json!(["binance-spot", "binance-usdm"])
    } else {
        json!(["binance-spot"])
    };
    let bytes = serde_json::to_vec(&json!({"schema":1,"revision":1,"targets":[{"process_id":process_id,"account_ids":accounts,"venue_ids":venues,"topic_ids":topics,"secret_purposes":[]}]})).unwrap();
    let reference = format!(
        "artifact://fixture/config/policy@sha256:{}",
        inputs::hex(tickwire_recovery::digest(&bytes))
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
    // Update all references to the real topic, preserving a valid full global
    // document before projection instead of editing a finished payload.
    root["ipc"]["topics"][0]["id"] = 101.into();
    for strategy in root["strategies"].as_array_mut().unwrap() {
        for topic in strategy["topic_ids"].as_array_mut().unwrap() {
            if *topic == json!(1) {
                *topic = 101.into();
            }
        }
        if strategy["logical_strategy_id"] == json!(1) {
            strategy["account_ids"] = accounts.clone();
            strategy["topic_ids"] = topics.clone();
            strategy["risk_budgets"]
                .as_array_mut()
                .unwrap()
                .retain(|r| accounts.as_array().unwrap().contains(&r["account_id"]));
        }
        if strategy["process_id"] == process_id {
            strategy["params"] = serde_json::from_slice::<Value>(&f.params).unwrap();
            strategy["parameter_schema_ref"] = format!(
                "artifact://fixture/strategy/parameters@sha256:{}",
                inputs::hex(tickwire_recovery::digest(&inputs::schema()))
            )
            .into();
        }
    }
    let process = root["processes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|p| p["id"] == process_id)
        .unwrap();
    process["artifact_ref"] = format!(
        "artifact://fixture/strategy/worker@sha256:{}",
        inputs::hex(f.bootstrap.market.local.build_hash)
    )
    .into();
    process["restart_policy"] = "never".into();
    let candidate = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&root).unwrap(),
        Limits::default(),
    )
    .unwrap();
    candidate
        .validate_static()
        .expect("full global fixture is valid before projection");
    let policy = PinnedPolicy::decode(&reference, &bytes).unwrap();
    ProjectionObject::build(
        41,
        &project(&candidate, process_id, &policy).unwrap(),
        4 * 1024 * 1024,
    )
    .unwrap()
}

pub fn bank(f: &Fixture) -> ResourceHeader {
    let mut h =
        ResourceHeader::decode(&f.bootstrap.external_market.as_ref().unwrap().bank_header).unwrap();
    h.descriptor_hash = [0; 32];
    h.descriptor_hash =
        tickwire_recovery::digest(&tickwire_market_source::protocol::wire_bytes(&h).unwrap());
    h
}
fn deployment(f: &Fixture) -> tickwire_app::config::Deployment {
    let mut d = super::runtime::deployment(f.market.instrument.instrument_id);
    d.boot_id = f.bootstrap.market.local.host_boot;
    d.policy.domain.process_instance = d.boot_id;
    d.owner_id = f.bootstrap.report.peer.instance;
    d.policy.executor_id = d.owner_id;
    d.quota.consumer_instance = d.owner_id;
    d.policy.versions.catalog_version = f.market.instrument.catalog_version;
    d.policy.versions.catalog_hash = f.market.instrument.catalog_hash;
    d
}
pub fn policy(f: &Fixture) -> tickwire_runtime_policy::ControlPlane {
    deployment(f).policy.build().unwrap()
}
pub fn build(f: &Fixture) -> tickwire_app::Result<Box<dyn AppStrategy>> {
    let d = deployment(f);
    let runtime = d.assemble(f.baseline.clone()).unwrap();
    tickwire_app::hosted::build(
        &serde_json::from_value(f.host.clone())?,
        &runtime,
        d.policy.build().unwrap(),
    )
}
fn align_runtime(f: &mut Fixture) {
    let mut snapshot = f.baseline.snapshot().clone();
    snapshot.assets[0].asset = f.market.instrument.base.clone();
    snapshot.assets[0].key.asset_id = f.market.instrument.base.id;
    snapshot.assets[1].asset = f.market.instrument.quote.clone();
    snapshot.assets[1].key.asset_id = f.market.instrument.quote.id;
    let mut catalog = f.baseline.catalog().clone();
    catalog.entries[0].instrument = f.market.instrument.clone();
    catalog.entries[0].strategy_limits[0].0 = f.bootstrap.market.local.logical_strategy;
    f.account =
        tickwire_account::Account::from_snapshot(snapshot.clone(), catalog.clone()).unwrap();
    f.baseline = tickwire_account_codec::CanonicalBaseline::new(snapshot, catalog).unwrap();
    let d = deployment(f);
    inputs::recovery(
        &f.root,
        &super::worker(),
        &f.bootstrap,
        &f.baseline,
        &f.params,
        &d.policy.versions,
    );
}
pub fn bind(f: &mut Fixture, parameters: bool, extra: Extra) {
    align_runtime(f);
    let object = projection(f, extra);
    let object_file = f.root.join("projection.json");
    write(&object_file, object.bytes());
    let scope = json!({"cluster_id":"tickwire-qualification","namespace_id":f.bootstrap.market.local.namespace,"host_id":object.payload()["process"]["host_id"],"process_id":object.binding().process_id});
    let actual = &f.baseline.snapshot().scope;
    let external = f.bootstrap.external_market.as_ref().unwrap();
    let binding = tickwire_app::hosted::profile::Binding::from(object.binding());
    f.host["projection_inputs"] = json!({"object_file":object_file,"object":binding,"scope":scope,"timeout_ns":10_000_000_000u64,
        "account":{"account_id":"spot-main","venue_id":"binance-spot","global_risk_domain_id":1,"baseline":Object::of(f.baseline.bytes()),"scope":{"risk_domain_id":actual.risk_domain_id,"venue":actual.venue,"environment":"test","account":actual.account},"report_owner_process_id":"exec-spot-a","report_owner":f.bootstrap.report.peer},
        "market":{"topic_id":101,"publisher_process_id":"md-spot-a","producer":external.endpoint.peer,"bank":Object::of(&tickwire_market_source::protocol::wire_bytes(&bank(f)).unwrap()),"catalog_hash":external.catalog_hash,"instrument_set_version":external.instrument_set_version,"instruments":external.instruments}});
    if parameters {
        write(&f.root.join("parameter-schema.json"), &inputs::schema());
        f.host["projection_parameters"] = json!({"object_file":object_file,"object":binding,"scope":scope,"schema_file":f.root.join("parameter-schema.json"),"timeout_ns":10_000_000_000u64});
    }
}

pub fn prove(f: &Fixture, owner: &mut dyn AppStrategy) {
    let until = Instant::now() + Duration::from_secs(15);
    while owner.projection_inputs(f.target()).is_none() {
        assert!(f.tick(owner).is_none());
        assert!(!owner.failed(), "actual input-consumption query failed");
        assert!(Instant::now() < until, "input proof deadline");
        std::thread::sleep(Duration::from_millis(2));
    }
    let mut wrong = f.target();
    wrong.generation += 1;
    assert!(owner.projection_inputs(wrong).is_none());
    let proof = owner.projection_inputs(f.target()).unwrap();
    let observation = proof.observation();
    assert_eq!(proof.object().revision, 41);
    assert_eq!(
        observation.process().identity(),
        &f.bootstrap.market.local.identity()
    );
    let facts = proof.inputs();
    assert_eq!(facts.accounts().len(), 1);
    let account = &facts.accounts()[0];
    assert_eq!(account.baseline(), &f.baseline);
    assert_eq!(account.position(), observation.ready().exec_report_sequence);
    assert_eq!(account.cursor().sequence, account.position());
    assert_eq!(account.input_digest(), observation.account_input_digest());
    assert_eq!(facts.markets().len(), 1);
    let market = &facts.markets()[0];
    assert_eq!(market.bank(), &bank(f));
    assert_eq!(market.bank().topic_id, 101);
    let external = f.bootstrap.external_market.as_ref().unwrap();
    let join = market
        .join()
        .expect("actual retained JOIN belongs to current bank");
    assert_eq!(join.catalog_hash(), external.catalog_hash);
    assert_eq!(
        join.instrument_set_version(),
        external.instrument_set_version
    );
    let members: Vec<_> = external
        .instruments
        .iter()
        .map(|i| (i.id, i.version, i.required_states))
        .collect();
    assert_eq!(join.instruments(), members);
    assert_eq!(
        market.instruments(),
        members
            .iter()
            .map(|&(id, v, _)| (id, v))
            .collect::<Vec<_>>()
    );
    assert!(join.snapshot().bytes > 0);
    assert!(market.cursor().sequence >= join.baseline_sequence());
    let mut report = ResourceHeader::decode(&f.bootstrap.report_header).unwrap();
    report.descriptor_hash = [0; 32];
    report.descriptor_hash =
        tickwire_recovery::digest(&tickwire_market_source::protocol::wire_bytes(&report).unwrap());
    assert_eq!(facts.report().header(), &report);
    assert_eq!(facts.report().identity().instance_id, f.target().instance);
    assert_eq!(
        facts.report().identity().strategy_generation,
        f.target().generation
    );
    assert_eq!(
        facts.report().identity().logical_strategy_id,
        f.target().logical_strategy_id
    );
    assert_eq!(
        facts.report().identity().risk_domain,
        f.bootstrap.risk_domain
    );
    assert_eq!(
        facts.report().sequence(),
        observation.ready().report_ring_sequence
    );
    assert_eq!(facts.report().subscription(), observation.subscription());
    let configuration = observation.configuration().unwrap();
    assert_eq!(facts.checkpoint(), configuration.checkpoint());
    assert_eq!(facts.cut(), configuration.cut());
    assert_eq!(facts.source(), configuration.source());
    assert_ne!(observation.challenge(), [0; 16]);
    assert!(observation.observed_at().nanos() < observation.deadline().nanos());
    super::evidence::save(f, proof);
}
