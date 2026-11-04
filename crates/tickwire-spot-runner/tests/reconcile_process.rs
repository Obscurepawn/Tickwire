#![cfg(all(target_os = "linux", target_env = "gnu"))]
#[path = "reconcile_cli/market.rs"]
mod market;
use serde_json::json;
use std::{
    fs,
    path::Path,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_spot_execution::{SpotVenue, authority, private};
use tickwire_venue::Environment;
// FileStrategy consumes the original canonical order, complete recovery descriptor and the
// actual mark emitted by the shared quote publisher when this worker decides to buy.
fn publish_order(root: &Path, trigger: tickwire_venue::TriggerRef) {
    use tickwire_recovery::{InputRecord, digest, encode_inputs};
    use tickwire_venue::{BoundedList, CanonicalCommand, ClientId, CommandBody};
    let dir = root.join("trading-queue");
    let mut order = CanonicalCommand::decode(include_bytes!("../examples/order-template.cbor"))
        .unwrap()
        .command()
        .clone();
    let time = trigger.observed_mono_ns;
    let market = serde_json::to_vec(&json!({
        "topic": trigger.topic_id,
        "instrument": trigger.instrument_id,
        "version": trigger.instrument_version,
        "feed": trigger.feed_generation,
        "ring": trigger.ring_epoch,
        "seq": trigger.sequence,
        "observed": time,
    }))
    .unwrap();
    order.context.triggers = BoundedList::new(vec![trigger]).unwrap();
    let baseline =
        CanonicalBaseline::decode(&fs::read(root.join("baseline.cbor")).unwrap()).unwrap();
    order.context.request.venue = baseline.snapshot().scope.venue;
    order.context.instrument = baseline.catalog().entries[0].instrument.clone();
    order.context.request.intent_exec_epoch = 4;
    order.context.request.intent_id = 801;
    order.context.request.config_revision = 1;
    order.context.request.strategy_generation = 1;
    order.context.request.deadline = time + 5_000_000_000;
    order.context.strategy.created_mono_ns = time;
    order.context.strategy.order_config_version = 1;
    order.context.strategy.authorization_epoch = 1;
    let owner = [8; 16];
    order.context.strategy.instance_id = owner;
    let CommandBody::New(ref mut new) = order.body else {
        unreachable!()
    };
    new.client_id = ClientId::new("liveorder801").unwrap();
    let cmd = CanonicalCommand::new(order).unwrap();
    let inputs = encode_inputs(
        owner,
        1,
        [0; 32],
        &[InputRecord {
            kind: 1,
            payload: &market,
        }],
    )
    .unwrap();
    let command_path = dir.join("command.cbor");
    let input_path = dir.join("inputs.bin");
    fs::write(&command_path, cmd.bytes()).unwrap();
    fs::write(&input_path, &inputs.bytes).unwrap();
    let deps = (2..=11)
        .map(|kind| {
            let p = dir.join(format!("decision-{kind}"));
            let data = serde_json::to_vec(
                &json!({"kind":kind,"member":8,"intent":cmd.digest(),"quote_hash":digest(&market)}),
            )
            .unwrap();
            fs::write(&p, &data).unwrap();
            json!({"kind":kind,"schema_id":1,"owner":owner,"path":p,"digest":digest(&data)})
        })
        .collect::<Vec<_>>();
    let descriptor = json!({"command":command_path,"command_digest":cmd.digest(),"order_ids":[owner],"recovery":{"dependencies":deps,"inputs":input_path,"input_digest":digest(&inputs.bytes),"declared_rebuild":false,"fault_model_id":digest(b"loopback-native-exchange"),"pin_id":owner,"owner":owner}});
    fs::write(
        dir.join("descriptor.tmp"),
        serde_json::to_vec(&descriptor).unwrap(),
    )
    .unwrap();
    fs::rename(
        dir.join("descriptor.tmp"),
        dir.join("00000000000000000001.json"),
    )
    .unwrap();
}
#[test]
fn real_reconcile_process_qualifies_and_reopens_both_native_accounts() {
    let root = std::env::temp_dir().join(format!(
        "tickwire-spot-reconcile-process-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&root).unwrap();
    struct Cleanup<'a>(&'a Path);
    impl Drop for Cleanup<'_> {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(self.0);
        }
    }
    let _cleanup = Cleanup(&root);
    let sources = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/reconcile_cli");
    assert!(
        Command::new("cc")
            .args(["-Wall", "-Werror", "-shared", "-fPIC"])
            .arg(sources.join("loopback.c"))
            .arg("-ldl")
            .arg("-o")
            .arg(root.join("local.so"))
            .status()
            .unwrap()
            .success()
    );
    for v in [SpotVenue::Bybit, SpotVenue::Okx] {
        let by = v == SpotVenue::Bybit;
        let path = root.join(if by { "bybit" } else { "okx" });
        fs::create_dir(&path).unwrap();
        let sample =
            CanonicalBaseline::decode(include_bytes!("../examples/baseline-template.cbor"))
                .unwrap();
        let mut s = sample.snapshot().clone();
        s.scope.venue = v.id();
        s.valid_until = u64::MAX;
        let mut cat = sample.catalog().clone();
        cat.entries[0].instrument.symbol =
            tickwire_venue::Symbol::new(if by { "BTCUSDT" } else { "BTC-USDT" }).unwrap();
        let i = &cat.entries[0].instrument;
        let base = CanonicalBaseline::new(s.clone(), cat.clone()).unwrap();
        fs::write(path.join("baseline.cbor"), base.bytes()).unwrap();
        let baseline_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            - 60_000;
        let domain = [2; 16];
        let account_stream = [4; 16];
        let private_stream = [5; 16];
        let d = json!({"state_directory":path.join("account-state"),"baseline":path.join("baseline.cbor"),"expected_uid":"42","baseline_utc_ms":baseline_ms,
          "account_stream":account_stream,"private_stream":private_stream,"initial_writer_epoch":1,"market_sources":[],"strategies":[],"stop_file":path.join("stop"),
          "policy":{"scope":{"risk_domain_id":s.scope.risk_domain_id,"venue":v.id(),"environment":"test","account":s.scope.account},"product":"spot","executor_id":domain,"owner_generation":1,
          "writer_epoch":1,"domain":{"process_instance":domain,"generation":1},"versions":{"config_revision":1,"risk_policy_version":s.revisions.risk,"order_config_version":1,
          "catalog_version":i.catalog_version,"catalog_hash":i.catalog_hash},"max_strategies":2,"max_feeds":2,"marks_per_feed":2,"control_revision":1,"mode":"stopped","owner_lease":null,"strategy_grants":[],"management_grant":null}});
        fs::write(path.join("config.json"), serde_json::to_vec(&d).unwrap()).unwrap();
        let rest = authority(v, Environment::Test);
        let (ws, _) = private::endpoint(v, Environment::Test);
        let cert = rcgen::generate_simple_self_signed(vec![rest.into(), ws.into()]).unwrap();
        fs::write(path.join("ca.pem"), cert.cert.pem()).unwrap();
        fs::write(path.join("key.pem"), cert.signing_key.serialize_pem()).unwrap();
        let _market = {
            fs::create_dir(path.join("trading-queue")).unwrap();
            let md = market::start(&path);
            fs::write(
                path.join("market-source.json"),
                serde_json::to_vec(&md.client()).unwrap(),
            )
            .unwrap();
            md
        };
        let out = Command::new("python3")
            .arg(sources.join("exchange.py"))
            .arg(env!("CARGO_BIN_EXE_tickwire-spot-runner"))
            .arg(&path)
            .args([rest, ws])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
