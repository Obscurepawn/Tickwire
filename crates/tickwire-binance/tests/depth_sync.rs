mod common;
use tickwire_binance::{depth_sync::*, feed::TimeUnit};
use tickwire_book::{
    BinanceSpot, BinanceUsdM, BookKey, PriceTicks, QuantityKnowledge, Side, ViewError,
};
use tickwire_venue::ContractKind;
fn config<P: DepthPolicy>() -> Config {
    let mut instrument = common::command().context.instrument;
    if P::MARKET == tickwire_binance::request::Market::Usdm {
        instrument.contract = ContractKind::Perpetual;
    }
    Config {
        instrument,
        key: BookKey {
            venue: 1,
            instrument: 12,
            source: 1,
        },
        time_unit: TimeUnit::Milliseconds,
        snapshot_limit: 5,
        max_message_bytes: 4096,
        initial_book_generation: 0,
        previous_connection_epoch: 0,
    }
}
fn delta<P: DepthPolicy>(first: u64, last: u64, previous: u64) -> Vec<u8> {
    let extra = if P::MARKET == tickwire_binance::request::Market::Usdm {
        format!(r#", "T":1,"pu":{previous},"st":1"#)
    } else {
        String::new()
    };
    format!(r#"{{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":{first},"u":{last},"b":[["99","2"]],"a":[]{extra}}}"#).into_bytes()
}
fn snapshot<P: DepthPolicy>(last: u64) -> Vec<u8> {
    let extra = if P::MARKET == tickwire_binance::request::Market::Usdm {
        r#", "E":1,"T":1"#
    } else {
        ""
    };
    format!(r#"{{"lastUpdateId":{last},"bids":[["99","1"]],"asks":[["101","2"]]{extra}}}"#)
        .into_bytes()
}
#[test]
fn compact_book_key_survives_snapshot_bridge_and_reconnect_for_both_products() {
    fn exercise<P: DepthPolicy>() {
        let mut cfg = config::<P>();
        cfg.instrument.instrument_id = u64::MAX;
        cfg.key.instrument = 0;
        let key = cfg.key;
        let mut sync = DepthSync::<P, 8, 8, 4>::new(cfg).unwrap();
        for epoch in 1..=2 {
            let token = sync.connect(epoch).unwrap();
            sync.subscription_ready(token).unwrap();
            sync.on_delta(token, &delta::<P>(101, 102, 100), epoch * 10)
                .unwrap();
            let request = sync.request_snapshot(epoch * 10 + 1, 100).unwrap();
            assert!(request.path().contains("symbol=BTCUSDT"));
            assert!(matches!(
                sync.on_snapshot(request.token(), 200, &snapshot::<P>(101), epoch * 10 + 2)
                    .unwrap(),
                Outcome::SnapshotInstalled { live: true, .. }
            ));
            let view = sync.view(epoch * 10 + 2, 100).unwrap();
            assert_eq!(view.key(), key);
            assert_eq!(view.generation(), epoch);
            assert_eq!(sync.last_update_id(), Some(102));
        }
    }
    exercise::<BinanceSpot>();
    exercise::<BinanceUsdM>();
}
#[test]
fn spot_buffers_before_subscription_then_replays_stale_and_adjacent_bridge() {
    let mut sync = DepthSync::<BinanceSpot, 8, 8, 4>::new(config::<BinanceSpot>()).unwrap();
    let token = sync
        .connect(1)
        .expect("a fresh connection must arm buffering");
    assert_eq!(
        sync.on_delta(token, &delta::<BinanceSpot>(99, 100, 0), 10)
            .unwrap(),
        Outcome::Buffered { events: 1 }
    );
    assert_eq!(
        sync.request_snapshot(10, 100).unwrap_err(),
        SyncError::InvalidState
    );
    assert!(sync.subscription_ready(token).unwrap());
    let request = sync.request_snapshot(11, 100).unwrap();
    assert_eq!(request.path(), "/api/v3/depth?symbol=BTCUSDT&limit=5");
    sync.on_delta(token, &delta::<BinanceSpot>(101, 102, 0), 12)
        .unwrap();
    assert_eq!(
        sync.on_snapshot(request.token(), 200, &snapshot::<BinanceSpot>(100), 20)
            .unwrap(),
        Outcome::SnapshotInstalled {
            applied: 1,
            discarded: 1,
            live: true
        }
    );
    assert_eq!(sync.last_update_id(), Some(102));
    let view = sync.view(20, 10).unwrap();
    assert_eq!(
        view.quantity(Side::Bid, PriceTicks(9800)),
        QuantityKnowledge::Unknown
    );
    assert!(matches!(sync.view(23, 10), Err(ViewError::Stale)));
}
#[test]
fn usdm_keeps_equal_watermark_bridge_then_requires_pu() {
    let mut sync = DepthSync::<BinanceUsdM, 8, 8, 4>::new(config::<BinanceUsdM>()).unwrap();
    let token = sync
        .connect(1)
        .expect("a fresh connection must arm buffering");
    sync.subscription_ready(token).unwrap();
    sync.on_delta(token, &delta::<BinanceUsdM>(99, 100, 98), 10)
        .unwrap();
    let request = sync.request_snapshot(11, 100).unwrap();
    assert_eq!(
        sync.on_snapshot(request.token(), 200, &snapshot::<BinanceUsdM>(100), 12)
            .unwrap(),
        Outcome::SnapshotInstalled {
            applied: 1,
            discarded: 0,
            live: true
        }
    );
    let generation = sync.generation();
    assert!(matches!(
        sync.on_delta(token, &delta::<BinanceUsdM>(101, 102, 99), 13)
            .unwrap(),
        Outcome::Rebuild(RebuildReason::Book(_))
    ));
    assert_eq!(sync.generation(), generation + 1);
    assert!(sync.view(13, 10).is_err());
}
fn armed<P: DepthPolicy, const B: usize>() -> (DepthSync<P, 8, 8, B>, ConnectionToken) {
    let mut s = DepthSync::new(config::<P>()).unwrap();
    let t = s.connect(1).unwrap();
    s.subscription_ready(t).unwrap();
    (s, t)
}
#[test]
fn spot_retries_snapshot_older_than_original_first_u_without_losing_cache() {
    let (mut s, t) = armed::<BinanceSpot, 4>();
    s.on_delta(t, &delta::<BinanceSpot>(101, 102, 0), 10)
        .unwrap();
    let r = s.request_snapshot(11, 100).unwrap();
    assert_eq!(
        s.on_snapshot(r.token(), 200, &snapshot::<BinanceSpot>(100), 12)
            .unwrap(),
        Outcome::RetrySnapshot(RetryReason::TooOld)
    );
    assert_eq!(s.buffered(), 1);
    assert_eq!(s.last_update_id(), None);
    let r = s.request_snapshot(13, 100).unwrap();
    s.on_snapshot(r.token(), 200, &snapshot::<BinanceSpot>(101), 14)
        .unwrap();
    assert_eq!(s.state(), State::Live);
    assert_eq!(s.last_update_id(), Some(102));
}
#[test]
fn snapshot_ahead_of_buffer_waits_for_a_future_bridge_without_freshening() {
    let (mut s, t) = armed::<BinanceSpot, 4>();
    s.on_delta(t, &delta::<BinanceSpot>(99, 100, 0), 10)
        .unwrap();
    let r = s.request_snapshot(11, 100).unwrap();
    assert_eq!(
        s.on_snapshot(r.token(), 200, &snapshot::<BinanceSpot>(110), 12)
            .unwrap(),
        Outcome::SnapshotInstalled {
            applied: 0,
            discarded: 1,
            live: false
        }
    );
    assert_eq!(s.state(), State::Bridging);
    assert!(s.view(12, 100).is_err());
    s.on_delta(t, &delta::<BinanceSpot>(111, 112, 0), 13)
        .unwrap();
    assert_eq!(s.state(), State::Live);
}
#[test]
fn reconnect_ignores_old_connection_snapshot_and_disconnect_callbacks_before_parsing() {
    let (mut s, t) = armed::<BinanceSpot, 4>();
    s.on_delta(t, &delta::<BinanceSpot>(99, 100, 0), 10)
        .unwrap();
    let r = s.request_snapshot(11, 100).unwrap();
    let t2 = s.connect(2).unwrap();
    let generation = s.generation();
    assert_eq!(s.on_delta(t, b"bad", 0).unwrap(), Outcome::IgnoredObsolete);
    assert_eq!(
        s.on_snapshot(r.token(), 200, b"bad", 0).unwrap(),
        Outcome::IgnoredObsolete
    );
    assert_eq!(s.disconnect(t).unwrap(), Outcome::IgnoredObsolete);
    assert!(!s.subscription_ready(t).unwrap());
    assert_eq!(s.generation(), generation);
    assert_eq!(s.connect(2).unwrap_err(), SyncError::EpochNotIncreasing);
    s.subscription_ready(t2).unwrap();
    s.on_delta(t2, &delta::<BinanceSpot>(200, 202, 0), 12)
        .unwrap();
    let r = s.request_snapshot(13, 100).unwrap();
    s.on_snapshot(r.token(), 200, &snapshot::<BinanceSpot>(201), 14)
        .unwrap();
    assert_eq!(s.last_update_id(), Some(202));
}
#[test]
fn expiry_and_retries_invalidate_old_request_tokens_without_discarding_deltas() {
    let (mut s, t) = armed::<BinanceSpot, 4>();
    s.on_delta(t, &delta::<BinanceSpot>(99, 101, 0), 10)
        .unwrap();
    let old = s.request_snapshot(11, 20).unwrap();
    assert_eq!(
        s.request_snapshot(12, 30).unwrap_err(),
        SyncError::RequestPending
    );
    assert_eq!(s.expire_snapshot(19).unwrap(), None);
    assert_eq!(
        s.expire_snapshot(20).unwrap(),
        Some(Outcome::RetrySnapshot(RetryReason::Expired))
    );
    let new = s.request_snapshot(21, 40).unwrap();
    assert_ne!(old.token(), new.token());
    assert_eq!(
        s.on_snapshot(old.token(), 200, b"invalid", 0).unwrap(),
        Outcome::IgnoredObsolete
    );
    s.on_snapshot(new.token(), 200, &snapshot::<BinanceSpot>(100), 22)
        .unwrap();
    assert_eq!(s.last_update_id(), Some(101));
    assert_eq!(
        s.on_snapshot(new.token(), 200, b"invalid", 0).unwrap(),
        Outcome::IgnoredObsolete
    );
}
#[test]
fn response_at_deadline_cannot_install_and_transport_http_failure_can_retry() {
    let (mut s, t) = armed::<BinanceSpot, 4>();
    s.on_delta(t, &delta::<BinanceSpot>(99, 101, 0), 10)
        .unwrap();
    let r = s.request_snapshot(11, 20).unwrap();
    assert_eq!(
        s.on_snapshot(r.token(), 200, b"invalid", 20).unwrap(),
        Outcome::RetrySnapshot(RetryReason::Expired)
    );
    let r = s.request_snapshot(21, 100).unwrap();
    assert_eq!(
        s.on_snapshot(r.token(), 429, b"not depth", 22).unwrap(),
        Outcome::RetrySnapshot(RetryReason::Http(429))
    );
    let r = s.request_snapshot(23, 100).unwrap();
    assert_eq!(
        s.snapshot_failed(r.token(), 24).unwrap(),
        Outcome::RetrySnapshot(RetryReason::Transport)
    );
    assert_eq!(s.buffered(), 1);
}
#[test]
fn overflow_drops_entire_attempt_and_old_snapshot_cannot_restore_it() {
    let (mut s, t) = armed::<BinanceSpot, 1>();
    s.on_delta(t, &delta::<BinanceSpot>(99, 101, 0), 10)
        .unwrap();
    let r = s.request_snapshot(11, 100).unwrap();
    let generation = s.generation();
    assert_eq!(
        s.on_delta(t, &delta::<BinanceSpot>(102, 103, 0), 12)
            .unwrap(),
        Outcome::Rebuild(RebuildReason::BufferOverflow)
    );
    assert_eq!(s.generation(), generation + 1);
    assert_eq!(s.buffered(), 0);
    assert!(s.view(12, 100).is_err());
    assert_eq!(
        s.on_snapshot(r.token(), 200, &snapshot::<BinanceSpot>(100), 13)
            .unwrap(),
        Outcome::IgnoredObsolete
    );
    assert_eq!(
        s.request_snapshot(14, 100).unwrap_err(),
        SyncError::InvalidState
    );
    s.on_delta(t, &delta::<BinanceSpot>(110, 112, 0), 15)
        .unwrap();
    let r = s.request_snapshot(16, 100).unwrap();
    s.on_snapshot(r.token(), 200, &snapshot::<BinanceSpot>(111), 17)
        .unwrap();
    assert_eq!(s.last_update_id(), Some(112));
}
#[test]
fn gap_late_in_buffer_replay_never_exposes_partly_replayed_live_book() {
    let (mut s, t) = armed::<BinanceSpot, 4>();
    s.on_delta(t, &delta::<BinanceSpot>(99, 101, 0), 10)
        .unwrap();
    let r = s.request_snapshot(11, 100).unwrap();
    s.on_delta(t, &delta::<BinanceSpot>(103, 104, 0), 12)
        .unwrap();
    assert!(matches!(
        s.on_snapshot(r.token(), 200, &snapshot::<BinanceSpot>(100), 13)
            .unwrap(),
        Outcome::Rebuild(RebuildReason::Book(_))
    ));
    assert_eq!(s.state(), State::Buffering);
    assert_eq!(s.last_update_id(), None);
    assert!(s.view(13, 100).is_err());
}
#[test]
fn disconnect_closes_live_and_duplicate_close_is_harmless() {
    let (mut s, t) = armed::<BinanceSpot, 4>();
    s.on_delta(t, &delta::<BinanceSpot>(99, 101, 0), 10)
        .unwrap();
    let r = s.request_snapshot(11, 100).unwrap();
    s.on_snapshot(r.token(), 200, &snapshot::<BinanceSpot>(100), 12)
        .unwrap();
    assert_eq!(s.disconnect(t).unwrap(), Outcome::Disconnected);
    assert!(s.view(12, 100).is_err());
    assert_eq!(s.disconnect(t).unwrap(), Outcome::IgnoredObsolete);
    assert_eq!(s.on_delta(t, b"bad", 13).unwrap(), Outcome::IgnoredObsolete);
    assert_eq!(
        s.request_snapshot(13, 100).unwrap_err(),
        SyncError::InvalidState
    );
}
#[test]
fn current_malformed_delta_snapshot_and_clock_regression_require_new_attempt() {
    let (mut s, t) = armed::<BinanceSpot, 4>();
    let g = s.generation();
    assert!(matches!(
        s.on_delta(t, b"bad", 10).unwrap(),
        Outcome::Rebuild(RebuildReason::Decode(_))
    ));
    assert_eq!(s.generation(), g + 1);
    s.on_delta(t, &delta::<BinanceSpot>(99, 101, 0), 11)
        .unwrap();
    let r = s.request_snapshot(12, 100).unwrap();
    assert!(matches!(
        s.on_snapshot(r.token(), 200, b"bad", 13).unwrap(),
        Outcome::Rebuild(RebuildReason::Decode(_))
    ));
    assert_eq!(
        s.on_delta(t, &delta::<BinanceSpot>(99, 101, 0), 12)
            .unwrap(),
        Outcome::Rebuild(RebuildReason::ClockRegressed)
    );
    assert!(s.view(14, 100).is_err());
}
#[test]
fn configuration_and_generation_exhaustion_fail_closed() {
    let mut c = config::<BinanceSpot>();
    c.snapshot_limit = 9;
    assert!(DepthSync::<BinanceSpot, 8, 8, 4>::new(c).is_err());
    assert!(DepthSync::<BinanceSpot, 8, 8, 0>::new(config::<BinanceSpot>()).is_err());
    let mut c = config::<BinanceUsdM>();
    c.snapshot_limit = 6;
    assert!(DepthSync::<BinanceUsdM, 8, 8, 4>::new(c).is_err());
    let mut c = config::<BinanceSpot>();
    c.initial_book_generation = u64::MAX;
    let mut s = DepthSync::<BinanceSpot, 8, 8, 4>::new(c).unwrap();
    assert_eq!(s.connect(1).unwrap_err(), SyncError::Exhausted);
    assert_eq!(s.state(), State::Exhausted);
    assert!(s.view(0, 100).is_err());
}
#[test]
fn usdm_strict_stale_filter_and_larger_first_u_force_rebuild() {
    let (mut s, t) = armed::<BinanceUsdM, 4>();
    s.on_delta(t, &delta::<BinanceUsdM>(95, 99, 94), 10)
        .unwrap();
    s.on_delta(t, &delta::<BinanceUsdM>(99, 100, 98), 11)
        .unwrap();
    let r = s.request_snapshot(12, 100).unwrap();
    assert_eq!(
        s.on_snapshot(r.token(), 200, &snapshot::<BinanceUsdM>(100), 13)
            .unwrap(),
        Outcome::SnapshotInstalled {
            applied: 1,
            discarded: 1,
            live: true
        }
    );
    // A stale duplicate with mismatched pu does not update receipt freshness.
    assert!(matches!(
        s.on_delta(t, &delta::<BinanceUsdM>(99, 100, 0), 14)
            .unwrap(),
        Outcome::Applied(tickwire_book::ApplyOutcome::IgnoredStale)
    ));
    assert!(matches!(s.view(22, 10), Err(ViewError::Stale)));
    let newer = s.connect(2).unwrap();
    s.subscription_ready(newer).unwrap();
    s.on_delta(newer, &delta::<BinanceUsdM>(101, 102, 100), 15)
        .unwrap();
    let r = s.request_snapshot(16, 100).unwrap();
    assert!(matches!(
        s.on_snapshot(r.token(), 200, &snapshot::<BinanceUsdM>(100), 17)
            .unwrap(),
        Outcome::Rebuild(_)
    ));
    assert!(s.view(17, 100).is_err());
}
#[test]
fn request_requires_first_message_and_readiness_and_valid_future_deadline() {
    let mut s = DepthSync::<BinanceSpot, 8, 8, 4>::new(config::<BinanceSpot>()).unwrap();
    let t = s.connect(1).unwrap();
    assert_eq!(
        s.request_snapshot(1, 100).unwrap_err(),
        SyncError::InvalidState
    );
    s.subscription_ready(t).unwrap();
    assert_eq!(
        s.request_snapshot(2, 100).unwrap_err(),
        SyncError::InvalidState
    );
    s.on_delta(t, &delta::<BinanceSpot>(1, 2, 0), 3).unwrap();
    assert_eq!(
        s.request_snapshot(4, 4).unwrap_err(),
        SyncError::InvalidDeadline
    );
    assert_eq!(
        s.request_snapshot(5, 4).unwrap_err(),
        SyncError::InvalidDeadline
    );
}
#[test]
fn over_limit_snapshot_and_total_update_budget_rebuild_before_install() {
    let mut c = config::<BinanceSpot>();
    c.snapshot_limit = 1;
    let mut s = DepthSync::<BinanceSpot, 8, 2, 4>::new(c).unwrap();
    let t = s.connect(1).unwrap();
    s.subscription_ready(t).unwrap();
    s.on_delta(t, &delta::<BinanceSpot>(99, 101, 0), 10)
        .unwrap();
    let r = s.request_snapshot(11, 100).unwrap();
    let body = br#"{"lastUpdateId":100,"bids":[["99","1"],["98","2"]],"asks":[]}"#;
    assert_eq!(
        s.on_snapshot(r.token(), 200, body, 12).unwrap(),
        Outcome::Rebuild(RebuildReason::Decode(tickwire_binance::Error::Capacity))
    );
    assert_eq!(s.last_update_id(), None);
    let body=br#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":110,"u":112,"b":[["99","1"],["98","2"]],"a":[["101","2"]]}"#;
    assert_eq!(
        s.on_delta(t, body, 13).unwrap(),
        Outcome::Rebuild(RebuildReason::Book(
            tickwire_book::BookError::TooManyUpdates
        ))
    );
    assert_eq!(s.buffered(), 0);
}
#[test]
fn book_capacity_and_duplicate_batch_fail_closed_after_valid_bridge() {
    let mut c = config::<BinanceSpot>();
    c.snapshot_limit = 1;
    let mut s = DepthSync::<BinanceSpot, 1, 4, 4>::new(c).unwrap();
    let t = s.connect(1).unwrap();
    s.subscription_ready(t).unwrap();
    s.on_delta(t, &delta::<BinanceSpot>(99, 101, 0), 10)
        .unwrap();
    let r = s.request_snapshot(11, 100).unwrap();
    s.on_snapshot(r.token(), 200, &snapshot::<BinanceSpot>(100), 12)
        .unwrap();
    let body =
        br#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":102,"u":102,"b":[["98","2"]],"a":[]}"#;
    assert!(matches!(
        s.on_delta(t, body, 13).unwrap(),
        Outcome::Rebuild(RebuildReason::Book(
            tickwire_book::BookError::Capacity { .. }
        ))
    ));
    assert!(s.view(13, 100).is_err());
    let body=br#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":110,"u":112,"b":[["99","2"],["99","3"]],"a":[]}"#;
    s.on_delta(t, body, 14).unwrap();
    let r = s.request_snapshot(15, 100).unwrap();
    assert!(matches!(
        s.on_snapshot(r.token(), 200, &snapshot::<BinanceSpot>(111), 16)
            .unwrap(),
        Outcome::Rebuild(RebuildReason::Book(
            tickwire_book::BookError::DuplicatePrice { .. }
        ))
    ));
    assert_eq!(s.last_update_id(), None);
}
#[test]
fn snapshot_request_encodes_unicode_once_and_product_path_is_fixed() {
    let mut c = config::<BinanceSpot>();
    c.instrument.symbol = tickwire_venue::Symbol::new("币USDT").unwrap();
    let mut s = DepthSync::<BinanceSpot, 8, 8, 4>::new(c).unwrap();
    let t = s.connect(1).unwrap();
    s.subscription_ready(t).unwrap();
    let bytes = r#"{"e":"depthUpdate","E":1,"s":"币USDT","U":1,"u":2,"b":[],"a":[]}"#;
    s.on_delta(t, bytes.as_bytes(), 10).unwrap();
    assert_eq!(
        s.request_snapshot(11, 100).unwrap().path(),
        "/api/v3/depth?symbol=%E5%B8%81USDT&limit=5"
    );
    let (mut s, t) = armed::<BinanceUsdM, 4>();
    s.on_delta(t, &delta::<BinanceUsdM>(1, 2, 0), 10).unwrap();
    assert_eq!(
        s.request_snapshot(11, 100).unwrap().path(),
        "/fapi/v1/depth?symbol=BTCUSDT&limit=5"
    );
}
#[test]
fn removing_last_visible_price_keeps_unknown_coverage_after_live_transition() {
    let (mut s, t) = armed::<BinanceSpot, 4>();
    s.on_delta(t, &delta::<BinanceSpot>(99, 101, 0), 10)
        .unwrap();
    let r = s.request_snapshot(11, 100).unwrap();
    s.on_snapshot(r.token(), 200, &snapshot::<BinanceSpot>(100), 12)
        .unwrap();
    let body =
        br#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":102,"u":102,"b":[["99","0"]],"a":[]}"#;
    s.on_delta(t, body, 13).unwrap();
    let view = s.view(13, 100).unwrap();
    assert_eq!(view.best_bid(), tickwire_book::BestLevel::Unknown);
    assert_eq!(
        view.quantity(Side::Bid, PriceTicks(9800)),
        QuantityKnowledge::Unknown
    );
}
