use super::*;
use std::{fs, path::PathBuf};
struct Temp(PathBuf);
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn scenario() -> (Temp, Scenario) {
    let dir = Temp(std::env::temp_dir().join(format!(
            "tw-lab-expansion-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )));
    fs::create_dir(&dir.0).unwrap();
    let bins = dir.0.join("bin");
    fs::create_dir(&bins).unwrap();
    for name in [
        "tickwire-app",
        "tickwire-md",
        "tickwire-strategy-worker",
        "tickwire-replica-receiver",
        "tickwire-lab",
        "tickwire-lifecycle",
        "tickwire-counting-v1-worker",
        "tickwire-counting-worker",
    ] {
        fs::write(bins.join(name), name).unwrap();
    }
    let s = crate::generate::generate_market_expansion(&dir.0.join("run"), &bins).unwrap();
    (dir, s)
}
#[test]
fn expansion_generation_binds_private_controls_and_complete_initial_dependency() {
    let (_dir, s) = scenario();
    assert_eq!(s.kind, crate::ScenarioKind::SpotMarketExpansion);
    let md: tickwire_md::config::Config =
        serde_json::from_slice(&fs::read(&s.md_config).unwrap()).unwrap();
    let control = md.expansion.expect("explicit startup approval");
    assert_eq!(control.initial_revision, 1);
    assert_eq!(md.feeds.len(), 1);
    let app: serde_json::Value = serde_json::from_slice(&fs::read(&s.app_config).unwrap()).unwrap();
    assert_eq!(
        app["market_transition"]["consumer_directory"],
        serde_json::json!(control.consumer_directory)
    );
    let template: tickwire_strategy_sdk::recovery::Template =
        serde_json::from_slice(&fs::read(s.root.join("strategy/recovery-template.json")).unwrap())
            .unwrap();
    let dep = template.dependencies.iter().find(|d| d.kind == 9).unwrap();
    let bytes = fs::read(&dep.path).unwrap();
    assert_eq!(dep.digest, tickwire_recovery::digest(&bytes));
    assert_eq!(bytes, serde_json::to_vec(&app).unwrap());
    let baseline = tickwire_account_codec::CanonicalBaseline::decode(
        &fs::read(s.root.join("baseline.bin")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        baseline
            .catalog()
            .entries
            .last()
            .unwrap()
            .instrument
            .instrument_id,
        u64::MAX
    );
    assert!(!control.plan_path.exists());
    assert!(!s.root.join("market-expanded.json").exists());
}
#[test]
fn plan_preserves_complete_old_identity_and_binds_new_dense_owner() {
    use tickwire_ipc::Wire;
    let (_dir, s) = scenario();
    let config: tickwire_md::config::Config = read(&s.md_config).unwrap();
    let app: serde_json::Value = read(&s.app_config).unwrap();
    let client = serde_json::from_value(app["market_sources"][0].clone()).unwrap();
    let admission = Admission {
        client,
        pid: std::process::id(),
        uid: 0,
        gid: 0,
        supervisor: serde_json::from_value(
            app["strategy"]["members"][0]["strategy"]["config"]["supervisor"].clone(),
        )
        .unwrap(),
        supervisor_pid: std::process::id(),
        snapshot_id: [67; 16],
    };
    let mut bank = ResourceHeader::decode(&fs::read(&config.topics[0].resource).unwrap()).unwrap();
    bank.descriptor_hash = [0; 32];
    bank.descriptor_hash = digest(&wire_bytes(&bank).unwrap());
    let plan = make_plan(
        &config,
        &bank,
        vec![admission.clone()],
        now(&s).unwrap().nanos(),
    )
    .unwrap();
    assert_eq!(plan.target_config.feeds[0].instrument.id, 12);
    assert_eq!(plan.target_config.feeds[1].instrument.id, u64::MAX);
    assert_eq!(plan.target_config.feeds[1].stream_id, 2);
    assert_eq!(
        serde_json::to_vec(&plan.consumers[0].previous).unwrap(),
        serde_json::to_vec(&admission).unwrap()
    );
    let index = FeedIndex::new(&config.feeds).unwrap();
    let grown = index.expanded(&plan.target_config.feeds).unwrap();
    assert_eq!(
        index.book_key(&config.feeds[0]).unwrap(),
        grown.book_key(&plan.target_config.feeds[0]).unwrap()
    );
    assert_ne!(
        grown.book_key(&plan.target_config.feeds[0]).unwrap(),
        grown.book_key(&plan.target_config.feeds[1]).unwrap()
    );
    let mut bad = plan;
    bad.consumers[0].next.client.cursor_id = bank.resource_id;
    assert!(
        bad.prepare(
            &config,
            &index,
            1,
            &bank,
            0,
            &[admission],
            0,
            now(&s).unwrap().nanos()
        )
        .is_err()
    );
}

fn retained_branch(values: &[u8]) -> tickwire_release::replay::retained::RecordedCut {
    use tickwire_release::replay::*;
    let source = Source {
        logical_strategy: 1,
        instance: [2; 16],
        generation: 5,
        binary_digest: [3; 32],
        state_schema: 1,
        input_stream: [4; 16],
        clock: ClockDomain {
            boot_id: [5; 16],
            generation: 1,
        },
    };
    let runtime = RuntimeState {
        now_ns: 1,
        config: VersionedBytes {
            revision: 1,
            bytes: vec![1],
        },
        models: vec![],
        markets: vec![],
        accounts: vec![],
        external: vec![],
        timers: vec![],
        rng: Rng::Disabled,
        effects: EffectCut {
            count: 0,
            digest: [0; 32],
        },
    };
    let anchor = Checkpoint::new(source, 0, [0; 32], vec![1], runtime).unwrap();
    let mut current = anchor.clone();
    let mut payloads = vec![[b"TWM2A001".as_slice(), &anchor.bytes().unwrap()].concat()];
    for (i, value) in values.iter().enumerate() {
        let row = MergedInput {
            sequence: i as u64 + 1,
            now_ns: i as u64 + 2,
            input: Input::External {
                stream: StreamKey {
                    identity: [6; 32],
                    generation: 1,
                },
                sequence: i as u64 + 1,
                schema: 1,
                bytes: vec![*value],
            },
            effects: vec![],
        };
        current.cut.chain = row_chain(current.cut.chain, &row).unwrap();
        current.cut.sequence = row.sequence;
        payloads.push([b"TWM2R001".as_slice(), &serde_json::to_vec(&row).unwrap()].concat());
    }
    payloads.push([b"TWM2C001".as_slice(), &current.bytes().unwrap()].concat());
    let bytes = tickwire_recovery::encode_inputs(
        [7; 16],
        1,
        [0; 32],
        &payloads
            .iter()
            .map(|p| tickwire_recovery::InputRecord {
                kind: 5,
                payload: p,
            })
            .collect::<Vec<_>>(),
    )
    .unwrap()
    .bytes;
    retained::decode(&bytes).unwrap()
}
#[test]
fn expansion_prefix_rejects_equal_sequence_from_another_valid_branch() {
    let observed = retained_branch(&[1]);
    let fork = retained_branch(&[2]);
    assert_eq!(observed.current().cut.sequence, fork.current().cut.sequence);
    assert_ne!(observed.current().cut.chain, fork.current().cut.chain);
    assert!(proof::verify_prefix(observed.current(), &fork).is_err());
}
#[test]
fn expansion_prefix_rejects_later_sequence_from_another_valid_branch() {
    let fixed = retained_branch(&[]);
    let observed = retained_branch(&[1]);
    let continued = retained_branch(&[1, 3]);
    let fork = retained_branch(&[2, 3]);
    proof::verify_prefix(fixed.current(), &observed).unwrap();
    proof::verify_prefix(observed.current(), &continued).unwrap();
    assert!(proof::verify_prefix(observed.current(), &fork).is_err());
    let mut wrong_identity = observed.current().clone();
    wrong_identity.source.input_stream = [99; 16];
    assert!(proof::verify_prefix(&wrong_identity, &continued).is_err());
}
