// The driver copies the repository's authenticated JOIN fixture next to this file.
#[path = "fixture.rs"]
mod fixture;

use std::{
    fs,
    path::Path,
    thread,
    time::{Duration, Instant},
};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_app::market::Sources;
use tickwire_binance_clock::SystemClock;
use tickwire_market_source::{
    ConnectConfig, FeedObservation, Invalidation, MarketSink, MarketSource,
};
use tickwire_runtime::node::Observer;
use tickwire_runtime_policy::{Config, ControlPlane, NodeObserver, Versions};
use tickwire_venue::{Product, TriggerRef};

fn publish_order(_: &Path, _: TriggerRef) {
    unreachable!("probe does not submit orders")
}

struct Observations {
    owner: NodeObserver,
    latest: Vec<(u64, bool, bool)>,
}
impl MarketSink for Observations {
    fn observed(&mut self, event: FeedObservation) -> bool {
        let (seq, live) = (event.sequence, event.live);
        let accepted = self.owner.market_observed(event);
        self.latest.push((seq, live, accepted));
        accepted
    }
    fn invalidated(&mut self, event: Invalidation) -> bool {
        self.owner.market_invalidated(event)
    }
}

#[test]
fn unavailable_pair_permanently_fails_healthy_sources_member() {
    let root = std::env::temp_dir().join(format!("tw-quote-proof-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    struct Cleanup<'a>(&'a Path);
    impl Drop for Cleanup<'_> {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(self.0);
        }
    }
    let _cleanup = Cleanup(&root);
    let template = fs::read(std::env::var_os("TW_PROBE_BASELINE").unwrap()).unwrap();
    let account = CanonicalBaseline::decode(&template).unwrap();
    let mut base = account.snapshot().clone();
    base.scope.venue = 2;
    fs::write(
        root.join("baseline.cbor"),
        CanonicalBaseline::new(base.clone(), account.catalog().clone())
            .unwrap()
            .bytes(),
    )
    .unwrap();

    let server = fixture::start(&root);
    let cfg: ConnectConfig = serde_json::from_value(server.client()).unwrap();
    fs::write(root.join("trader-pid"), std::process::id().to_string()).unwrap();
    let domain = cfg.domain().unwrap();
    let mut clock = SystemClock::new(domain).unwrap();
    let mut now = || clock.sample().unwrap().monotonic;
    let mut source = Sources::connect(std::slice::from_ref(&cfg), domain, [2; 16], 1).unwrap();
    let plane = ControlPlane::new(Config {
        scope: base.scope.clone(),
        product: Product::Spot,
        executor_id: [2; 16],
        owner_generation: 1,
        writer_epoch: 1,
        domain,
        versions: Versions {
            config_revision: 1,
            risk_policy_version: 1,
            order_config_version: 1,
            catalog_version: 1,
            catalog_hash: cfg.catalog_hash,
        },
        max_strategies: 2,
        max_feeds: 4,
        marks_per_feed: 16,
    })
    .unwrap();
    let mut sink = Observations {
        owner: plane.observer(),
        latest: Vec::new(),
    };
    let age = 2_000_000_000;
    source.poll(now(), age, 10, &mut sink).unwrap();
    assert!(source.is_live(now(), age));
    fs::write(root.join("unavailable"), b"").unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while !sink.latest.iter().any(|e| !e.2) {
        source.poll(now(), age, 1, &mut sink).unwrap();
        assert!(
            Instant::now() < deadline,
            "no rejected observation: {:?}",
            sink.latest
        );
        thread::sleep(Duration::from_millis(1));
    }
    let n = sink.latest.len();
    let previous = sink.latest[n - 2];
    let rejected = sink.latest[n - 1];
    assert!(!previous.1 && previous.2 && !rejected.1 && !rejected.2);
    assert_eq!(rejected.0, previous.0 + 1);
    while !root.join("published-after-gap").exists() {
        assert!(
            Instant::now() < deadline,
            "publisher did not deliver the next good BBO"
        );
        thread::sleep(Duration::from_millis(1));
    }
    for _ in 0..3 {
        assert_eq!(source.poll(now(), age, 16, &mut sink).unwrap().records, 0);
    }
    assert_eq!(sink.latest.len(), n);
    assert!(!source.is_live(now(), age));
    println!(
        "accepted_stale={}, rejected_stale={}, next_live_pair_published=true, subsequent_records=0",
        previous.0, rejected.0
    );
}
