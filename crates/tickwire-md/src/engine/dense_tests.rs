use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    config: Config,
    header: ResourceHeader,
    directory: std::path::PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "tickwire-md-dense-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        let mut config: Config =
            serde_json::from_str(include_str!("../../examples/md.json")).unwrap();
        // Equal low 32 bits, opposite order to the topic's sorted directory.
        config.feeds[0].instrument.id = u64::MAX;
        config.feeds[1].instrument.id = u64::from(u32::MAX);
        config.feeds[1].instrument.catalog_version = config.feeds[0].instrument.catalog_version;
        config.feeds[1].instrument.catalog_hash = config.feeds[0].instrument.catalog_hash;
        let mut spec: config::ResourceSpec =
            serde_json::from_str(include_str!("../../examples/topic-101.json")).unwrap();
        spec.identity.arch = if cfg!(target_arch = "x86_64") { 1 } else { 2 };
        spec.catalog_version = config.feeds[0].instrument.catalog_version;
        let header = spec.header().unwrap();
        config.topics.truncate(1);
        config.topics[0].resource = directory.join("topic.bin");
        config::write_bytes(
            &config.topics[0].resource,
            &tickwire_market_source::protocol::wire_bytes(&header).unwrap(),
        )
        .unwrap();
        Self {
            config,
            header,
            directory,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

#[test]
fn topic_sorting_preserves_the_process_book_key_binding_for_full_width_ids() {
    let f = Fixture::new();
    let index = config::FeedIndex::new(&f.config.feeds).unwrap();
    let (registry, instruments) = topic_registry(&f.config, &f.header, &index).unwrap();
    assert_eq!(registry.instruments[0].instrument_id, u64::from(u32::MAX));
    assert_eq!(registry.instruments[1].instrument_id, u64::MAX);
    assert_eq!(instruments[0].key.instrument, 1);
    assert_eq!(instruments[1].key.instrument, 0);
    assert_ne!(instruments[0].key, instruments[1].key);
    for instrument in instruments {
        let feed = f
            .config
            .feeds
            .iter()
            .find(|feed| feed.instrument.id == instrument.instrument.instrument_id)
            .unwrap();
        assert_eq!(instrument.key, index.book_key(feed).unwrap());
    }
}

#[test]
fn feed_build_binds_full_width_ids_before_depth_and_event_owner_construction() {
    let mut f = Fixture::new();
    let certificate = rcgen::generate_simple_self_signed(vec![
        "stream.binance.com".into(),
        "api.binance.com".into(),
    ])
    .unwrap();
    let root = f.directory.join("root.der");
    std::fs::write(&root, certificate.cert.der()).unwrap();
    for feed in &mut f.config.feeds {
        feed.trust_anchors = vec![root.clone()];
        feed.stream_address = "127.0.0.1:443".into();
        feed.snapshot_address = "127.0.0.1:443".into();
        feed.snapshot_limit = 5;
    }
    let index = config::FeedIndex::new(&f.config.feeds).unwrap();
    let domain = tickwire_clock::ClockDomain::new([8; 16], 1).unwrap();
    for (slot, feed) in f.config.feeds.iter().enumerate() {
        let (cfg, _) = feed.build(domain, &index).unwrap();
        assert_eq!(cfg.depth.instrument.instrument_id, feed.instrument.id);
        assert_eq!(cfg.depth.key.instrument, u32::try_from(slot).unwrap());
        assert_eq!(cfg.depth.key.source, u32::try_from(feed.stream_id).unwrap());
        assert!(
            tickwire_binance::depth_sync::DepthSync::<tickwire_book::BinanceSpot, 8, 8, 8>::new(
                cfg.depth
            )
            .is_ok()
        );
    }
    let event = config::EventFeed {
        owner_id: [90; 16],
        stream_id: u64::MAX,
        kind: config::EventType::Trade,
        first_connection_epoch: 1,
        first_operation: 1,
        stream_address: "127.0.0.1:443".into(),
        sbe_api_key_file: None,
        fast_mark: false,
    };
    let (event_config, _) = event.build(&f.config.feeds[0], domain, &index).unwrap();
    assert_eq!(event_config.context.identity.instrument_id, u64::MAX);
    assert_eq!(event_config.context.identity.source_id, u64::MAX);
}

#[test]
fn feed_index_reuses_ids_and_rejects_unknowns_without_truncating_keys() {
    let mut f = Fixture::new();
    let mut repeated = f.config.feeds[0].clone();
    repeated.stream_id = 3;
    f.config.feeds.push(repeated);
    let index = config::FeedIndex::new(&f.config.feeds).unwrap();
    assert_eq!(index.book_key(&f.config.feeds[0]).unwrap().instrument, 0);
    assert_eq!(index.book_key(&f.config.feeds[1]).unwrap().instrument, 1);
    assert_eq!(index.book_key(&f.config.feeds[2]).unwrap().instrument, 0);
    let mut unknown = f.config.feeds[0].clone();
    unknown.instrument.id = 0x1234_5678_ffff_ffff;
    assert!(index.book_key(&unknown).is_err());
    let domain = tickwire_clock::ClockDomain::new([8; 16], 1).unwrap();
    // Reject before opening trust files or resolving network endpoints.
    assert_eq!(
        unknown.build(domain, &index).err().unwrap().to_string(),
        "unknown MD feed instrument"
    );
    for stream in [0, u64::from(u32::MAX) + 1, u64::MAX] {
        let mut invalid = f.config.feeds[0].clone();
        invalid.stream_id = stream;
        assert!(index.book_key(&invalid).is_err());
    }
    assert!(config::FeedIndex::new(&[]).is_err());
    assert!(config::FeedIndex::new(&vec![f.config.feeds[0].clone(); 33]).is_err());
    assert!(config::FeedIndex::new(&vec![f.config.feeds[0].clone(); 32]).is_ok());
    unknown.instrument.id = 0;
    assert!(config::FeedIndex::new(&[unknown]).is_err());
}
