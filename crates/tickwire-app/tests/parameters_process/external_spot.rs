//! Linux process lab driven by scripts/run-two-strategies.py.
use super::{Fixture, market::Market, worker};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    time::{Duration, Instant},
};
use tickwire_ipc::{MarketState, ResourceHeader, Wire};
use tickwire_market_source::replica::MarketReplica;
use tickwire_release::replay::Checkpoint;

fn quote(c: &Checkpoint, instrument: u64) -> Option<(u64, i64, i64, u64)> {
    #[derive(Deserialize)]
    struct Saved {
        markets: Vec<Market>,
        strategy: Vec<u8>,
    }
    #[derive(Deserialize)]
    struct Market {
        view: Vec<u8>,
        maximum_levels: usize,
    }
    let saved: Saved = serde_json::from_slice(&c.business).unwrap();
    #[derive(Deserialize)]
    struct Counting {
        observations: u64,
        spent: bool,
    }
    let counting: Counting = serde_json::from_slice(&saved.strategy).unwrap();
    assert!(!counting.spent);
    let m = saved.markets.first()?;
    let replica = MarketReplica::restore_checkpoint(&m.view, m.maximum_levels).unwrap();
    let view = replica.view(instrument)?;
    let Some(MarketState::Exact(value)) = view.best_quote() else {
        return None;
    };
    let b = &value.body;
    (counting.observations > 0 && b.feed_state == 2 && b.bid_price_ticks < b.ask_price_ticks)
        .then_some((
            b.venue_seq_last,
            b.bid_price_ticks,
            b.ask_price_ticks,
            counting.observations,
        ))
}

#[test]
#[ignore = "single-host spot-market lab; use scripts/run-two-strategies.py"]
fn two_registered_workers_read_external_spot_market() {
    let root = PathBuf::from(std::env::var_os("TICKWIRE_SPOT_DEMO").expect("demo directory"));
    let md_pid: u32 = std::env::var("TICKWIRE_SPOT_PID").unwrap().parse().unwrap();
    let worker = worker();
    let members = [1, 2].map(|id| {
        let market = attach(&root, md_pid, id);
        Fixture::observe(&worker, market, root.join(format!("w{id}")))
    });
    let slots = members.each_ref().map(|member| {
        let path = member.root.join("host.json");
        fs::write(&path, serde_json::to_vec_pretty(&member.host).unwrap()).unwrap();
        json!({"logical_strategy_id":member.target().logical_strategy_id,
               "strategy":{"mode":"hosted","config":path}})
    });
    let group = json!({"mode":"multi","members":slots});
    fs::write(
        root.join("strategies.json"),
        serde_json::to_vec_pretty(&group).unwrap(),
    )
    .unwrap();
    let first = &members[0];
    let mut owner = tickwire_app::launch::build(
        &serde_json::from_value(group).unwrap(),
        &first.runtime,
        first.policy.clone(),
    )
    .unwrap();
    first.seed(&mut *owner);
    let pids = members
        .each_ref()
        .map(|m| owner.observation_process(m.target()).unwrap().process().pid);
    assert_ne!(pids[0], pids[1]);
    fs::write(root.join("workers.ready"), b"joined").unwrap();
    let expected: Option<u64> = std::env::var("TICKWIRE_SPOT_FIXTURE_SEQUENCE")
        .ok()
        .map(|n| n.parse().unwrap());
    let mut observed: [Option<Value>; 2] = [None, None];
    let deadline = Instant::now() + Duration::from_secs(45);
    while observed.iter().any(Option::is_none) {
        assert!(
            first.tick(&mut *owner).is_none(),
            "observation-only member emitted an intent"
        );
        assert!(!owner.failed(), "hosted group stopped");
        for (i, f) in members.iter().enumerate() {
            if observed[i].is_some() {
                continue;
            }
            if let Some(c) = f.checkpoint()
                && let Some((sequence, bid, ask, observations)) =
                    quote(&c, f.market.instrument.instrument_id)
            {
                if expected.is_some_and(|required| sequence != required) {
                    continue;
                }
                assert_eq!(c.source.instance, f.target().instance);
                assert_eq!(c.runtime.effects.count, 0);
                observed[i] = Some(
                    json!({"strategy":f.target().logical_strategy_id,"pid":pids[i],
                    "quote_sequence":sequence,"applied_cursor":c.runtime.markets[0].sequence,
                    "bid_ticks":bid,"ask_ticks":ask,"callbacks":observations}),
                );
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for a live quote in both worker checkpoints"
        );
        std::thread::sleep(Duration::from_millis(4));
    }
    let result = json!({"event":"two_strategies_observed","market_pid":md_pid,
        "topic":ResourceHeader::decode(&first.market.admission.client.bank_header).unwrap().topic_id,
        "workers":observed,"strategy_effects":0});
    fs::write(
        root.join("result.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
    println!("{result}");
}

// The runner supplies an already-started MD process and its exported active header.
fn attach(root: &std::path::Path, pid: u32, id: u8) -> Market {
    use std::os::unix::fs::MetadataExt;
    use tickwire_md::config::{ClientTemplate, ResourceSpec};
    let cfg: tickwire_md::spot_market::Config =
        serde_json::from_slice(&std::fs::read(root.join("market.json")).unwrap()).unwrap();
    let spec: ResourceSpec =
        serde_json::from_slice(&std::fs::read(&cfg.resource).unwrap()).unwrap();
    let catalog_bytes = std::fs::read(&cfg.catalog.path).unwrap();
    let digest = tickwire_config::artifacts::reference_hash(&cfg.catalog.reference).unwrap();
    let catalog = tickwire_instrument_catalog::Catalog::decode(&catalog_bytes, digest).unwrap();
    let instrument = catalog
        .instrument(cfg.source.instrument_id)
        .unwrap()
        .binding()
        .clone();
    let mut template: ClientTemplate = serde_json::from_str(include_str!(
        "../../../tickwire-md/examples/exec-client.json"
    ))
    .unwrap();
    let meta = std::fs::metadata("/proc/self").unwrap();
    let endpoint = &mut template.endpoint;
    endpoint.peer = spec.identity.clone();
    endpoint.local = spec.identity.clone();
    endpoint.local.instance = [20 + id; 16];
    endpoint.local.role = 3;
    endpoint.local.logical_strategy = u64::from(id);
    endpoint.path = cfg.server.socket;
    endpoint.peer_pid = pid;
    endpoint.peer_uid = meta.uid();
    endpoint.peer_gid = meta.gid();
    endpoint.required_features = 0x1c;
    endpoint.supported_features = 1023;
    endpoint.lease_duration_ns = 60_000_000_000;
    endpoint.nonce = [id; 32];
    template.resource = cfg.server.control_directory.join("spot-market.active");
    template.catalog_hash = catalog.catalog_hash();
    template.instrument_set_version = cfg.instrument_set_version;
    template.instruments = vec![tickwire_market_source::protocol::Instrument {
        id: instrument.instrument_id,
        version: instrument.instrument_version,
        required_states: tickwire_snapshot::QUOTE,
    }];
    template.cursor_id = [60 + id; 16];
    template.request_id = [40 + id; 16];
    template.clock_generation = cfg.clock_generation;
    template.startup_timeout_ms = 30_000;
    template.maximum_levels = 8;
    let mut supervisor = spec.identity;
    supervisor.instance = [11; 16];
    supervisor.role = 1;
    Market::subscriber(
        tickwire_md::server::Admission {
            client: template.build().unwrap(),
            pid: std::process::id(),
            uid: meta.uid(),
            gid: meta.gid(),
            supervisor,
            supervisor_pid: std::process::id(),
            snapshot_id: [80 + id; 16],
        },
        cfg.server.admission_directory,
        instrument,
    )
}
