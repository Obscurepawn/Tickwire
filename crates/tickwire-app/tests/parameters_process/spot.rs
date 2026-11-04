use super::{Fixture, market::Market, worker};
use serde::Deserialize;
use tickwire_ipc::MarketState;
use tickwire_release::replay::Checkpoint;

fn check_observed(checkpoint: &Checkpoint, instrument: u64, venue: u32, observation: u64) {
    assert_eq!(super::business(checkpoint)["observations"], observation);
    #[derive(Deserialize)]
    struct SavedMarket {
        view: Vec<u8>,
        maximum_levels: usize,
    }
    #[derive(Deserialize)]
    struct Saved {
        markets: Vec<SavedMarket>,
    }
    let saved: Saved = serde_json::from_slice(&checkpoint.business).unwrap();
    assert_eq!(saved.markets.len(), 1);
    let m = &saved.markets[0];
    let replica = tickwire_market_source::replica::MarketReplica::restore_checkpoint(
        &m.view,
        m.maximum_levels,
    )
    .unwrap();
    let view = replica.view(instrument).unwrap();
    assert!(view.continuous());
    assert!(view.best_bid().is_none() && view.best_ask().is_none());
    assert!(view.bids().is_empty() && view.asks().is_empty());
    let Some(MarketState::Exact(bbo)) = view.best_quote() else {
        panic!("missing exact venue quote")
    };
    assert_eq!(
        (
            bbo.body.source_id,
            bbo.body.source_connection_epoch,
            bbo.body.venue_seq_last
        ),
        (u64::from(venue), 1, observation + 2)
    );
    assert_eq!(
        (
            bbo.body.bid_price_ticks,
            bbo.body.ask_price_ticks,
            bbo.body.bid_quantity_lots,
            bbo.body.ask_quantity_lots
        ),
        (102 + observation as i64, 103 + observation as i64, 1, 1)
    );
    assert_eq!(bbo.body.feed_state, 2);
    assert_eq!(checkpoint.runtime.effects.count, 0);
}
#[test]
#[ignore = "requires fixed TICKWIRE_PARAMETERS_WORKER; two checkpointing strategy processes"]
fn two_workers_observe_one_shared_spot_snapshot_without_depth_or_orders() {
    let worker = worker();
    for venue in [2, 3] {
        let first = Fixture::new_with_market(&worker, 1, 100, Market::start_source(Some(venue)));
        let second = Fixture::new_with_market(&worker, 1, 100, first.market.peer(2));
        let slots=[&first,&second].map(|f| {
            let file=f.root.join("host.json");
            std::fs::write(&file,serde_json::to_vec(&f.host).unwrap()).unwrap();
            serde_json::json!({"logical_strategy_id":f.target().logical_strategy_id,"strategy":{"mode":"hosted","config":file}})
        });
        let group =
            serde_json::from_value(serde_json::json!({"mode":"multi","members":slots})).unwrap();
        let mut owner =
            tickwire_app::launch::build(&group, &first.runtime, first.policy.clone()).unwrap();
        first.seed(&mut *owner);
        let pids =
            [&first, &second].map(|f| owner.observation_process(f.target()).unwrap().process().pid);
        assert_ne!(pids[0], pids[1]);
        for observation in 1..=2 {
            let sequence = first.market.publish(102 + observation as i64);
            let a = first.wait_consumed(&mut *owner, sequence);
            let b = second.wait_consumed(&mut *owner, sequence);
            assert_eq!(a.runtime.markets, b.runtime.markets);
            for c in [&a, &b] {
                check_observed(c, first.market.instrument.instrument_id, venue, observation);
            }
        }
        assert!(first.next_ready(&mut *owner).is_none());
        assert!(!owner.failed());
    }
}
