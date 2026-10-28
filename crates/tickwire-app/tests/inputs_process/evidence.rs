use super::{Fixture, fixture::write, inputs};
use serde_json::json;
use tickwire_app::hosted::inputs::InputsProof;
use tickwire_market_source::protocol::wire_bytes;
use tickwire_release::Object;

pub fn save(f: &Fixture, proof: &InputsProof) {
    let observed = proof.observation();
    let facts = proof.inputs();
    let account = &facts.accounts()[0];
    let market = &facts.markets()[0];
    let join = market.join().unwrap();
    let report = facts.report();
    let sub = report.subscription();
    let directory = f.root.join("input-proof-objects");
    std::fs::create_dir(&directory).unwrap();
    let mut objects = std::collections::BTreeMap::new();
    for bytes in observed.objects() {
        let object = Object::of(bytes);
        write(&directory.join(inputs::hex(object.digest)), bytes);
        objects.insert(object.digest, object);
    }
    let ready = observed.ready();
    let seal_bytes = observed
        .objects()
        .iter()
        .find(|bytes| {
            Object::of(bytes)
                == Object {
                    digest: ready.input_seal_digest,
                    bytes: ready.input_seal_bytes,
                }
        })
        .unwrap();
    let seal = tickwire_recovery::HandoverSeal::decode(seal_bytes).unwrap();
    let retained_bytes = observed
        .objects()
        .iter()
        .find(|bytes| {
            Object::of(bytes)
                == Object {
                    digest: seal.input_digest,
                    bytes: seal.input_bytes,
                }
        })
        .unwrap();
    let retained = tickwire_release::replay::retained::decode(retained_bytes).unwrap();
    let current_bytes = retained.current().bytes().unwrap();
    assert_eq!(Object::of(&current_bytes), facts.checkpoint());
    write(&f.root.join("current-checkpoint.raw"), &current_bytes);
    write(
        &f.root.join("host-config.json"),
        &serde_json::to_vec_pretty(&f.host).unwrap(),
    );
    write(&f.root.join("baseline.raw"), f.baseline.bytes());
    write(
        &f.root.join("market-bank.raw"),
        &wire_bytes(market.bank()).unwrap(),
    );
    write(
        &f.root.join("report-bank.raw"),
        &wire_bytes(report.header()).unwrap(),
    );
    let value = json!({
        "case":"input_scope_consumed", "pid":observed.process().process().pid,
        "root":f.root,"worker_binary":facts.source().binary_digest,
        "object":proof.object(), "scope":proof.scope(),
        "request_id":observed.request().request_id,"challenge":observed.challenge(),
        "control_epoch":observed.control_epoch(),"started":observed.started_at().nanos(),
        "deadline":observed.deadline().nanos(),"observed":observed.observed_at().nanos(),
        "checkpoint":facts.checkpoint(),"cut":facts.cut(),"source":facts.source(),
        "account":{"baseline":Object::of(account.baseline().bytes()),"cursor":account.cursor(),"position":account.position(),"input_digest":account.input_digest()},
        "market":{"bank":Object::of(&wire_bytes(market.bank()).unwrap()),"topic":market.bank().topic_id,"cursor":market.cursor(),"members":market.instruments(),
            "join":{"catalog_hash":join.catalog_hash(),"instrument_set_version":join.instrument_set_version(),"members":join.instruments(),"snapshot":join.snapshot(),"baseline_sequence":join.baseline_sequence()}},
        "report":{"header":Object::of(&wire_bytes(report.header()).unwrap()),"sequence":report.sequence(),
            "identity":{"logical_strategy_id":report.identity().logical_strategy_id,"instance_id":report.identity().instance_id,"strategy_generation":report.identity().strategy_generation,"risk_domain":report.identity().risk_domain},
            "subscription":{"resource":sub.bank.resource.to_le_bytes(),"channel":sub.bank.channel.to_le_bytes(),"channel_epoch":sub.bank.channel_epoch,"ring_epoch":sub.bank.ring_epoch,"feed_generation":sub.bank.feed_generation,"instance":sub.instance.to_le_bytes(),"generation":sub.generation,"cursor":sub.cursor.to_le_bytes()}},
        "objects":objects.into_values().collect::<Vec<_>>()
    });
    write(
        &f.root.join("inputs-proof.json"),
        &serde_json::to_vec_pretty(&value).unwrap(),
    );
    println!("{value}");
}
