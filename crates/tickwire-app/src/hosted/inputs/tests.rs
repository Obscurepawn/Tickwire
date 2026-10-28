use super::*;
use serde_json::{Value, json};
use std::{
    fs,
    sync::atomic::{AtomicU64, Ordering},
};
use tickwire_config::{
    Candidate, Format, Limits,
    projection::{PinnedPolicy, project},
    projection_object::ProjectionObject,
};
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_market_source::{ConnectConfig, protocol::wire_bytes};
#[path = "../../../tests/projection_launch/channels.rs"]
mod channels;
#[path = "runtime.rs"]
mod runtime;
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: PathBuf,
    config: super::super::HostConfig,
    bootstrap: Bootstrap,
    runtime: tickwire_runtime::files::Configuration,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn fixture() -> Fixture {
    let root = std::env::temp_dir().join(format!(
        "tw-input-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&root).unwrap();
    let (snapshot, catalog, _) = crate::replay::income_tests::common::fixture();
    let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
    let instrument = &baseline.catalog().entries[0].instrument;
    let mut runtime = runtime::deployment(instrument.instrument_id)
        .assemble(baseline.clone())
        .unwrap();
    let policy_raw = serde_json::to_vec(&json!({"schema":1,"revision":1,"targets":[{"process_id":"strategy-01-active","account_ids":["spot-main"],"venue_ids":["binance-spot"],"topic_ids":[1],"secret_purposes":[]}]})).unwrap();
    let policy_ref = format!(
        "artifact://fixture/config/policy@sha256:{}",
        hex(&tickwire_recovery::digest(&policy_raw))
    );
    let policy = PinnedPolicy::decode(&policy_ref, &policy_raw).unwrap();
    let mut v = Candidate::parse(
        Format::Toml,
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../config/reference.toml"
        )),
        Limits::default(),
    )
    .unwrap()
    .value()
    .clone();
    v["local_config"]["projection_policy_ref"] = policy_ref.into();
    let p = v["processes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|p| p["id"] == "strategy-01-active")
        .unwrap();
    p["restart_policy"] = "never".into();
    let s = v["strategies"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|p| p["process_id"] == "strategy-01-active")
        .unwrap();
    s["account_ids"] = json!(["spot-main"]);
    s["topic_ids"] = json!([1]);
    s["risk_budgets"]
        .as_array_mut()
        .unwrap()
        .retain(|b| b["account_id"] == "spot-main");
    let candidate = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&v).unwrap(),
        Limits::default(),
    )
    .unwrap();
    let object = ProjectionObject::build(
        41,
        &project(&candidate, "strategy-01-active", &policy).unwrap(),
        4 * 1024 * 1024,
    )
    .unwrap();
    let payload = object.payload();
    let image = tickwire_config::artifacts::reference_hash(
        payload["process"]["artifact_ref"].as_str().unwrap(),
    )
    .unwrap();
    let current = tickwire_supervisor::current_process_identity().unwrap();
    let (mut bootstrap, mut bank) = channels::bootstrap(&root, image, current.uid, current.gid);
    bootstrap.handover_enabled = true;
    runtime.command_binding.owner_id = bootstrap.report.peer.instance;
    bank.topic_id = 1;
    bank.catalog_version = instrument.catalog_version;
    let raw = wire_bytes(&bank).unwrap();
    bank.descriptor_hash = tickwire_recovery::digest(&raw);
    bootstrap.external_market = Some(ConnectConfig {
        endpoint: bootstrap.market.clone(),
        bank_header: wire_bytes(&bank).unwrap(),
        catalog_hash: instrument.catalog_hash,
        instrument_set_version: 1,
        instruments: vec![Instrument {
            id: instrument.instrument_id,
            version: instrument.instrument_version,
            required_states: 1,
        }],
        cursor_id: [90; 16],
        subscription_generation: 1,
        request_id: [91; 16],
        maximum_levels: 100,
        startup_timeout_ms: 1000,
        clock_generation: 1,
    });
    let account = &payload["account_views"][0];
    let mut report = bootstrap.report_resource().unwrap();
    report.risk_scope = account["risk_domain_id"].as_u64().unwrap();
    bootstrap.report_header = wire_bytes(&report).unwrap();
    let object_file = root.join("projection.json");
    fs::write(&object_file, object.bytes()).unwrap();
    let input = Config {
        object_file,
        object: object.binding().into(),
        scope: profile::Scope {
            cluster_id: payload["identity"]["cluster_id"].as_str().unwrap().into(),
            namespace_id: channels::NAMESPACE,
            host_id: payload["host"]["id"].as_str().unwrap().into(),
            process_id: "strategy-01-active".into(),
        },
        account: AccountBinding {
            account_id: "spot-main".into(),
            venue_id: "binance-spot".into(),
            global_risk_domain_id: account["risk_domain_id"].as_u64().unwrap(),
            baseline: Object::of(baseline.bytes()),
            scope: RuntimeAccountScope {
                risk_domain_id: baseline.snapshot().scope.risk_domain_id,
                venue: baseline.snapshot().scope.venue,
                environment: Environment::Test,
                account: baseline.snapshot().scope.account,
            },
            report_owner_process_id: account["exec_process_id"].as_str().unwrap().into(),
            report_owner: bootstrap.report.peer.clone(),
        },
        market: MarketBinding {
            topic_id: 1,
            publisher_process_id: payload["ipc"]["topics"][0]["publisher_process_id"]
                .as_str()
                .unwrap()
                .into(),
            producer: bootstrap.market.peer.clone(),
            bank: Object::of(&wire_bytes(&bank).unwrap()),
            catalog_hash: instrument.catalog_hash,
            instrument_set_version: 1,
            instruments: bootstrap
                .external_market
                .as_ref()
                .unwrap()
                .instruments
                .clone(),
        },
        timeout_ns: 1_000_000_000,
    };
    let config=serde_json::from_value(json!({"projection_inputs":input,"bootstrap":root.join("bootstrap.json"),"executable":root.join("worker"),"working_directory":root,"uid":current.uid,"gid":current.gid,"supervisor":channels::identity(9,1,[9;32]),"directory_parent":root,"control_directory":root.join("control"),"control_stream":([5;16]),"control_domain":([6;16]),"control_writer_epoch":1,"snapshot_id":([7;16]),"market_cursor_id":([8;16]),"subscription_generation":1,"instrument_set_version":1,"maximum_levels_per_side":100,"maximum_cache_bytes":1048576,"maximum_snapshot_bytes":1048576,"maximum_delta_bytes":1048576,"recovery_directory":root.join("recovery")})).unwrap();
    Fixture {
        root,
        config,
        bootstrap,
        runtime,
    }
}
impl Fixture {
    fn input(&mut self) -> &mut Config {
        self.config.projection_inputs.as_mut().unwrap()
    }
    fn ready(&self) -> Prepared {
        prepare(&self.config, &self.bootstrap, &self.runtime)
            .unwrap()
            .expect("enabled input constraint")
    }
    fn reject(&self) {
        assert!(
            prepare(&self.config, &self.bootstrap, &self.runtime).is_err(),
            "mismatched input scope accepted"
        );
    }
    fn payload(&mut self, change: impl FnOnce(&mut Value)) {
        let path = self.input().object_file.clone();
        let mut v: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        change(&mut v["payload"]);
        let p = Candidate::parse(
            Format::Json,
            &serde_json::to_vec(&v["payload"]).unwrap(),
            Limits::default(),
        )
        .unwrap();
        v["projection_hash"] = format!("sha256:{}", hex(&p.hash())).into();
        let o = Candidate::parse(
            Format::Json,
            &serde_json::to_vec(&v).unwrap(),
            Limits::default(),
        )
        .unwrap();
        self.input().object.projection_hash = p.hash();
        self.input().object.object_hash = o.hash();
        fs::write(path, o.canonical_bytes()).unwrap();
    }
}
#[test]
fn changed_account_in_same_risk_domain_is_rejected_before_launch() {
    let mut f = fixture();
    assert!(
        prepare(&f.config, &f.bootstrap, &f.runtime)
            .unwrap()
            .is_some()
    );
    f.input().account.scope.account += 1;
    f.reject();
}
#[test]
fn complete_single_input_sets_are_required() {
    for field in [
        "accounts",
        "account_views",
        "topics",
        "topic_views",
        "venues",
        "account_name",
        "venue_name",
        "risk_domain",
        "publisher",
        "exec",
    ] {
        let mut f = fixture();
        f.ready();
        f.payload(|p| match field {
            "accounts" => p["strategy"]["account_ids"] = json!(["spot-main", "usdm-main"]),
            "account_views" => {
                let mut extra = p["account_views"][0].clone();
                extra["id"] = "second-account".into();
                p["account_views"].as_array_mut().unwrap().push(extra);
            }
            "topics" => p["strategy"]["topic_ids"] = json!([1, 2]),
            "topic_views" => {
                let mut extra = p["ipc"]["topics"][0].clone();
                extra["id"] = 2.into();
                p["ipc"]["topics"].as_array_mut().unwrap().push(extra);
            }
            "venues" => {
                let mut extra = p["venue_views"][0].clone();
                extra["id"] = "other-venue".into();
                p["venue_views"].as_array_mut().unwrap().push(extra);
            }
            "account_name" => p["account_views"][0]["id"] = "other-account".into(),
            "venue_name" => p["account_views"][0]["venue_id"] = "other-venue".into(),
            "risk_domain" => p["account_views"][0]["risk_domain_id"] = 99.into(),
            "publisher" => p["ipc"]["topics"][0]["publisher_process_id"] = "other-publisher".into(),
            "exec" => p["account_views"][0]["exec_process_id"] = "other-executor".into(),
            _ => unreachable!(),
        });
        f.reject();
    }
}
#[test]
fn scope_baseline_journal_and_command_must_all_agree() {
    for field in 0..12 {
        let mut f = fixture();
        f.ready();
        match field {
            0 => f.input().account.scope.venue += 1,
            1 => f.input().account.scope.environment = Environment::Production,
            2 => f.input().account.scope.risk_domain_id[0] ^= 1,
            3 => f.input().account.baseline.digest[0] ^= 1,
            4 => f.input().account.baseline.bytes += 1,
            5 => f.runtime.account.journal.scope.account += 1,
            6 => f.runtime.account.journal.baseline_digest[0] ^= 1,
            7 => f.runtime.command_binding.account += 1,
            8 => f.runtime.command_binding.owner_id[0] ^= 1,
            9 => f.bootstrap.risk_domain[0] ^= 1,
            10 => f.runtime.account.route.environment = tickwire_venue::Environment::Production,
            _ => f.runtime.input.physical.domain[0] ^= 1,
        }
        f.reject();
    }
}
#[test]
fn product_and_environment_come_from_actual_runtime_not_names() {
    for field in 0..3 {
        let mut f = fixture();
        f.ready();
        f.payload(|p| match field {
            0 => p["venue_views"][0]["environment"] = "production".into(),
            1 => p["ipc"]["topics"][0]["product"] = "Usdm".into(),
            _ => {
                p["ipc"]["topics"][0]["product"] = "Usdm".into();
                p["venue_views"][0]["product"] = "Usdm".into();
            }
        });
        f.reject();
    }
}
#[test]
fn authenticated_report_and_md_owners_are_exact() {
    for field in 0..8 {
        let mut f = fixture();
        f.ready();
        match field {
            0 => f.input().account.report_owner.build_hash[0] ^= 1,
            1 => f.bootstrap.report.peer.generation += 1,
            2 => f.bootstrap.intent.peer.instance[0] ^= 1,
            3 => f.input().market.producer.qualification_hash[0] ^= 1,
            4 => {
                f.bootstrap
                    .external_market
                    .as_mut()
                    .unwrap()
                    .endpoint
                    .peer
                    .build_hash[0] ^= 1
            }
            5 => {
                let mut h = f.bootstrap.report_resource().unwrap();
                h.producer_instance[0] ^= 1;
                f.bootstrap.report_header = wire_bytes(&h).unwrap();
            }
            6 => {
                let mut h = f.bootstrap.report_resource().unwrap();
                h.consumer_instance[0] ^= 1;
                f.bootstrap.report_header = wire_bytes(&h).unwrap();
            }
            _ => f.input().account.report_owner_process_id = "exec-spot-standby".into(),
        }
        f.reject();
    }
}
#[test]
fn market_members_bank_and_catalog_are_complete() {
    for field in 0..10 {
        let mut f = fixture();
        f.ready();
        match field {
            0 => f.input().market.instruments[0].id += 1,
            1 => f.input().market.instruments[0].version += 1,
            2 => f.input().market.instruments[0].required_states = 2,
            3 => {
                let i = f.input().market.instruments[0].clone();
                f.input().market.instruments.push(i);
            }
            4 => f.input().market.catalog_hash[0] ^= 1,
            5 => f.input().market.instrument_set_version += 1,
            6 => f.input().market.bank.digest[0] ^= 1,
            7 => {
                let e = f.bootstrap.external_market.as_mut().unwrap();
                let mut h = ResourceHeader::decode(&e.bank_header).unwrap();
                h.descriptor_hash[0] ^= 1;
                e.bank_header = wire_bytes(&h).unwrap();
            }
            8 => {
                f.input().market.instruments[0].version += 1;
                f.bootstrap.external_market.as_mut().unwrap().instruments[0].version += 1;
            }
            _ => {
                // Same canonical symbol/catalog remains in the baseline, but
                // changing both approved IDs cannot silently reinterpret it.
                f.input().market.instruments[0].id += 1;
                f.bootstrap.external_market.as_mut().unwrap().instruments[0].id += 1;
            }
        }
        f.reject();
    }
}
#[test]
fn zero_bank_template_is_normalized_without_accepting_wrong_nonzero_hash() {
    let mut f = fixture();
    f.ready();
    let e = f.bootstrap.external_market.as_mut().unwrap();
    let mut h = ResourceHeader::decode(&e.bank_header).unwrap();
    h.descriptor_hash = [0; 32];
    e.bank_header = wire_bytes(&h).unwrap();
    f.ready();
    let e = f.bootstrap.external_market.as_mut().unwrap();
    h.descriptor_hash = [1; 32];
    e.bank_header = wire_bytes(&h).unwrap();
    f.reject();
}
#[test]
fn full_u64_member_identity_is_preserved() {
    let mut f = fixture();
    let base = &f.runtime.input.baseline;
    let mut catalog = base.catalog().clone();
    let id = u64::MAX - 3;
    catalog.entries[0].instrument.instrument_id = id;
    let baseline = CanonicalBaseline::new(base.snapshot().clone(), catalog).unwrap();
    f.runtime.account.journal.baseline_digest = baseline.digest();
    f.input().account.baseline = Object::of(baseline.bytes());
    f.runtime.input.baseline = baseline;
    f.input().market.instruments[0].id = id;
    f.bootstrap.external_market.as_mut().unwrap().instruments[0].id = id;
    f.ready();
    f.input().market.instruments[0].id = u64::from(id as u32);
    f.reject();
}
#[test]
fn unsupported_modes_and_cross_projection_bindings_are_rejected() {
    for field in 0..7 {
        let mut f = fixture();
        f.ready();
        match field {
            0 => f.bootstrap.external_market = None,
            1 => f.config.rotation_directory = Some(f.root.join("rotations")),
            2 => f.bootstrap.preflight_job = Some(f.root.join("preflight")),
            3 => {
                f.config.capture = Some(super::super::CaptureConfig {
                    bootstrap: f.root.join("capture"),
                    publication_directory: f.root.join("publication"),
                })
            }
            4 => f.bootstrap.handover_enabled = false,
            5 => f.input().timeout_ns = 0,
            _ => {
                let p = f.input().clone();
                f.config.projection_parameters = Some(super::super::parameters::Config {
                    object_file: p.object_file,
                    object: p.object,
                    scope: p.scope,
                    schema_file: f.root.join("schema"),
                    timeout_ns: 1,
                });
                f.config
                    .projection_parameters
                    .as_mut()
                    .unwrap()
                    .object
                    .revision += 1;
            }
        }
        f.reject();
    }
}
#[test]
fn absent_constraint_has_no_file_dependency() {
    let mut f = fixture();
    f.config.projection_inputs = None;
    fs::remove_file(f.root.join("projection.json")).unwrap();
    assert!(
        prepare(&f.config, &f.bootstrap, &f.runtime)
            .unwrap()
            .is_none()
    );
}
#[test]
fn guard_rejects_same_epoch_different_scope_and_never_rehashes_baseline() {
    let f = fixture();
    let guard = f.ready().guard();
    let base = &f.runtime.input.baseline;
    let account = Account::from_snapshot(base.snapshot().clone(), base.catalog().clone()).unwrap();
    let id = Some((
        f.bootstrap.market.local.instance,
        f.bootstrap.market.local.generation,
    ));
    assert!(guard.matches(Some(base), &account, id));
    assert!(guard.matches(None, &account, id));
    let mut snapshot = base.snapshot().clone();
    snapshot.scope.account += 1;
    let other = Account::from_snapshot(snapshot, base.catalog().clone()).unwrap();
    assert_eq!(other.account_epoch(), account.account_epoch());
    assert!(!guard.matches(None, &other, id));
    assert!(!guard.matches(None, &account, None));
    assert!(!guard.matches(None, &account, Some(([99; 16], 1))));
    let mut newbase = base.snapshot().clone();
    newbase.valid_until += 1;
    let newbase = CanonicalBaseline::new(newbase, base.catalog().clone()).unwrap();
    assert!(!guard.matches(Some(&newbase), &account, id));
    let mut frozen = account.clone();
    frozen.freeze();
    assert!(guard.matches(Some(base), &frozen, id));
    let stats = allocation_counter::measure(|| {
        for _ in 0..100 {
            assert!(guard.matches(Some(base), &account, id));
        }
    });
    assert_eq!(stats.count_total, 0);
}
#[test]
fn strict_config_rejects_unknown_and_duplicate_mapping_fields() {
    let f = fixture();
    let cfg = f.config.projection_inputs.as_ref().unwrap();
    let mut v = serde_json::to_value(cfg).unwrap();
    v["account"]["unknown"] = 1.into();
    assert!(serde_json::from_value::<Config>(v).is_err());
    let raw = serde_json::to_string(cfg).unwrap();
    let duplicate = raw.replacen(
        "\"account_id\":\"spot-main\"",
        "\"account_id\":\"spot-main\",\"account_id\":\"spot-main\"",
        1,
    );
    assert_ne!(raw, duplicate);
    assert!(serde_json::from_str::<Config>(&duplicate).is_err());
}
