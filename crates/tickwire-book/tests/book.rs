use tickwire_book::*;

const KEY: BookKey = BookKey {
    venue: 1,
    instrument: 7,
    source: 2,
};
const BID: Level = Level {
    price: PriceTicks(100),
    quantity: QtyLots(5),
};
const ASK: Level = Level {
    price: PriceTicks(110),
    quantity: QtyLots(6),
};

fn level(price: i64, quantity: i64) -> Level {
    Level {
        price: PriceTicks(price),
        quantity: QtyLots(quantity),
    }
}

fn snapshot<'a>(bids: &'a [Level], asks: &'a [Level]) -> Snapshot<'a> {
    Snapshot {
        key: KEY,
        generation: 1,
        last_update_id: 10,
        bids,
        asks,
        bids_complete: false,
        asks_complete: false,
    }
}

fn event<'a>(
    first: u64,
    last: u64,
    previous: Option<u64>,
    bids: &'a [Level],
    asks: &'a [Level],
) -> DepthEvent<'a> {
    DepthEvent {
        key: KEY,
        generation: 1,
        first_update_id: first,
        last_update_id: last,
        previous_update_id: previous,
        received_at_ns: 100 + last,
        bids,
        asks,
    }
}

fn spot<const N: usize, const M: usize>() -> Book<BinanceSpot, N, M> {
    let mut book = Book::new(KEY, 1);
    book.install_snapshot(snapshot(&[BID], &[ASK])).unwrap();
    book
}

#[test]
fn snapshot_needs_bridge_and_spot_boundary_matrix() {
    for (first, last, expected) in [
        (
            10,
            11,
            Ok(ApplyOutcome::Applied {
                last_update_id: 11,
                became_live: true,
            }),
        ),
        (
            11,
            11,
            Ok(ApplyOutcome::Applied {
                last_update_id: 11,
                became_live: true,
            }),
        ),
        (12, 12, Err(BookError::Sequence(SequenceError::Gap))),
        (10, 10, Ok(ApplyOutcome::IgnoredStale)),
    ] {
        let mut book = spot::<4, 8>();
        assert_eq!(book.state(), BookState::Bridging);
        assert_eq!(book.view(110, 10).unwrap_err(), ViewError::NotLive);
        assert_eq!(book.apply(event(first, last, None, &[], &[])), expected);
        assert_eq!(book.state() == BookState::Live, last == 11);
    }
}

#[test]
fn spot_overlapping_updates_stale_and_gap() {
    let mut book = spot::<4, 8>();
    book.apply(event(9, 12, None, &[level(100, 7)], &[]))
        .unwrap();
    assert_eq!(
        book.apply(event(9, 12, None, &[level(100, 99)], &[])),
        Ok(ApplyOutcome::IgnoredStale)
    );
    assert_eq!(
        book.view(112, 0)
            .unwrap()
            .quantity(Side::Bid, PriceTicks(100)),
        QuantityKnowledge::Known(QtyLots(7))
    );
    book.apply(event(12, 15, None, &[level(100, 8)], &[]))
        .unwrap();
    assert_eq!(
        book.apply(event(17, 18, None, &[], &[])),
        Err(BookError::Sequence(SequenceError::Gap))
    );
    assert_eq!(book.last_update_id(), Some(15));
    assert_eq!(book.state(), BookState::Invalid(InvalidReason::Sequence));
    assert_eq!(book.view(118, 10).unwrap_err(), ViewError::NotLive);
    assert_eq!(
        book.apply(event(16, 16, None, &[], &[])),
        Err(BookError::NeedsRebuild)
    );
}

#[test]
fn futures_equal_snapshot_id_bridges_then_requires_pu() {
    let mut book: Book<BinanceUsdM, 4, 8> = Book::new(KEY, 1);
    book.install_snapshot(snapshot(&[BID], &[ASK])).unwrap();
    assert_eq!(
        book.apply(event(8, 9, Some(7), &[], &[])),
        Ok(ApplyOutcome::IgnoredStale)
    );
    assert_eq!(
        book.apply(event(9, 10, Some(2), &[], &[])),
        Ok(ApplyOutcome::Applied {
            last_update_id: 10,
            became_live: true
        })
    );
    assert_eq!(
        book.apply(event(9, 10, Some(2), &[level(100, 99)], &[])),
        Ok(ApplyOutcome::IgnoredStale)
    );
    book.apply(event(14, 16, Some(10), &[], &[])).unwrap();
    assert_eq!(
        book.apply(event(17, 18, Some(15), &[], &[])),
        Err(BookError::Sequence(SequenceError::Gap))
    );
}

#[test]
fn futures_bridge_rejects_spot_next_id_and_missing_pu() {
    let mut book: Book<BinanceUsdM, 4, 8> = Book::new(KEY, 1);
    book.install_snapshot(snapshot(&[BID], &[ASK])).unwrap();
    assert_eq!(
        book.apply(event(11, 12, Some(10), &[], &[])),
        Err(BookError::Sequence(SequenceError::Gap))
    );
    let mut book: Book<BinanceUsdM, 4, 8> = Book::new(KEY, 1);
    book.install_snapshot(snapshot(&[BID], &[ASK])).unwrap();
    assert_eq!(
        book.apply(event(10, 11, None, &[], &[])),
        Err(BookError::Sequence(SequenceError::MissingPrevious))
    );
}

#[test]
fn wrong_key_and_generation_cannot_poison_live_book() {
    let mut book = spot::<4, 8>();
    book.apply(event(11, 11, None, &[], &[])).unwrap();
    let mut old = event(999, 1000, None, &[], &[]);
    old.generation = 0;
    assert_eq!(book.apply(old), Err(BookError::WrongGeneration));
    old.generation = 1;
    old.key.instrument = 8;
    assert_eq!(book.apply(old), Err(BookError::WrongBook));
    assert_eq!(book.state(), BookState::Live);
    assert_eq!(book.last_update_id(), Some(11));
    assert_eq!(
        book.install_snapshot(snapshot(&[BID], &[ASK])),
        Err(BookError::SnapshotWhileLive)
    );
    assert_eq!(book.state(), BookState::Live);
}

#[test]
fn rebuild_requires_new_generation_and_old_snapshot_cannot_reactivate() {
    let mut book = spot::<4, 8>();
    book.apply(event(11, 11, None, &[], &[])).unwrap();
    assert_eq!(
        book.begin_rebuild(1),
        Err(BookError::GenerationNotIncreasing)
    );
    book.begin_rebuild(2).unwrap();
    assert_eq!(book.state(), BookState::AwaitingSnapshot);
    assert_eq!(book.last_update_id(), None);
    assert_eq!(
        book.install_snapshot(snapshot(&[BID], &[ASK])),
        Err(BookError::WrongGeneration)
    );
    let mut snap = snapshot(&[BID], &[ASK]);
    snap.generation = 2;
    book.install_snapshot(snap).unwrap();
    let mut bridge = event(11, 11, None, &[], &[]);
    bridge.generation = 2;
    book.apply(bridge).unwrap();
    assert_eq!(book.view(111, 0).unwrap().generation(), 2);
}

#[test]
fn zero_deletes_and_absent_zero_is_idempotent() {
    let mut book = spot::<4, 8>();
    book.apply(event(
        11,
        11,
        None,
        &[level(100, 0), level(99, 0)],
        &[level(110, 9)],
    ))
    .unwrap();
    let view = book.view(111, 0).unwrap();
    assert!(view.known_bids().is_empty());
    assert_eq!(view.known_asks(), &[level(110, 9)]);
}

#[test]
fn capacity_checks_final_batch_before_any_side_changes() {
    let mut book = spot::<1, 8>();
    book.apply(event(11, 11, None, &[level(99, 4), level(100, 0)], &[]))
        .unwrap();
    assert_eq!(book.view(111, 0).unwrap().known_bids(), &[level(99, 4)]);
    assert_eq!(
        book.apply(event(12, 12, None, &[level(99, 8)], &[level(111, 1)])),
        Err(BookError::Capacity { side: Side::Ask })
    );
    assert_eq!(book.diagnostic_levels(Side::Bid), &[level(99, 4)]);
    assert_eq!(book.diagnostic_levels(Side::Ask), &[ASK]);
    assert_eq!(book.last_update_id(), Some(11));
    assert_eq!(book.state(), BookState::Invalid(InvalidReason::Capacity));
}

#[test]
fn malformed_batches_do_not_partially_modify_either_side() {
    for (updates, expected) in [
        (vec![level(110, -1)], BookError::InvalidLevel),
        (vec![level(0, 3)], BookError::InvalidLevel),
        (
            vec![level(110, 3), level(110, 4)],
            BookError::DuplicatePrice { side: Side::Ask },
        ),
        (vec![level(99, 3)], BookError::CrossedBook),
    ] {
        let mut book = spot::<4, 8>();
        let result = book.apply(event(11, 11, None, &[level(100, 1)], &updates));
        assert_eq!(result, Err(expected));
        assert_eq!(book.diagnostic_levels(Side::Bid), &[BID]);
        assert_eq!(book.diagnostic_levels(Side::Ask), &[ASK]);
        assert_eq!(book.last_update_id(), Some(10));
        assert_eq!(book.view(111, 0).unwrap_err(), ViewError::NotLive);
    }
}

#[test]
fn snapshot_errors_are_atomic_and_rebuild_is_required() {
    let mut book = spot::<2, 8>();
    assert_eq!(
        book.install_snapshot(snapshot(&[level(101, 8)], &[level(0, 1)])),
        Err(BookError::InvalidLevel)
    );
    assert_eq!(book.diagnostic_levels(Side::Bid), &[BID]);
    assert_eq!(book.last_update_id(), Some(10));
    assert_eq!(
        book.install_snapshot(snapshot(&[BID], &[ASK])),
        Err(BookError::NeedsRebuild)
    );
}

#[test]
fn event_update_limit_and_invalid_range_fail_closed() {
    let mut book = spot::<4, 1>();
    assert_eq!(
        book.apply(event(11, 11, None, &[BID], &[ASK])),
        Err(BookError::TooManyUpdates)
    );
    assert_eq!(book.diagnostic_levels(Side::Bid), &[BID]);
    let mut book = spot::<4, 8>();
    assert_eq!(
        book.apply(event(12, 11, None, &[], &[])),
        Err(BookError::Sequence(SequenceError::InvalidRange))
    );
}

#[test]
fn truncated_snapshot_retains_unknown_depth_and_best_quote_uncertainty() {
    let mut book = spot::<4, 8>();
    book.apply(event(11, 11, None, &[level(90, 4)], &[level(120, 2)]))
        .unwrap();
    let view = book.view(111, 0).unwrap();
    assert_eq!(
        view.quantity(Side::Bid, PriceTicks(99)),
        QuantityKnowledge::Unknown
    );
    assert_eq!(
        view.quantity(Side::Bid, PriceTicks(101)),
        QuantityKnowledge::Known(QtyLots(0))
    );
    assert_eq!(
        view.quantity(Side::Bid, PriceTicks(90)),
        QuantityKnowledge::Known(QtyLots(4))
    );
    assert_eq!(view.best_bid(), BestLevel::Known(BID));
    book.apply(event(12, 12, None, &[level(100, 0)], &[level(110, 0)]))
        .unwrap();
    let view = book.view(112, 0).unwrap();
    assert_eq!(view.best_bid(), BestLevel::Unknown);
    assert_eq!(view.best_ask(), BestLevel::Unknown);
}

#[test]
fn complete_empty_snapshot_has_known_zero_and_empty_best() {
    let mut book: Book<BinanceSpot, 4, 8> = Book::new(KEY, 1);
    let mut snap = snapshot(&[], &[]);
    snap.bids_complete = true;
    snap.asks_complete = true;
    book.install_snapshot(snap).unwrap();
    book.apply(event(11, 11, None, &[], &[])).unwrap();
    let view = book.view(111, 0).unwrap();
    assert_eq!(view.best_bid(), BestLevel::Empty);
    assert_eq!(view.best_ask(), BestLevel::Empty);
    assert_eq!(
        view.quantity(Side::Bid, PriceTicks(1)),
        QuantityKnowledge::Known(QtyLots(0))
    );
}

#[test]
fn duplicate_event_does_not_refresh_freshness_and_backward_clock_is_rejected() {
    let mut book = spot::<4, 8>();
    book.apply(event(11, 11, None, &[], &[])).unwrap();
    let mut duplicate = event(11, 11, None, &[], &[]);
    duplicate.received_at_ns = 500;
    book.apply(duplicate).unwrap();
    assert_eq!(book.view(122, 10).unwrap_err(), ViewError::Stale);
    assert_eq!(book.view(110, 10).unwrap_err(), ViewError::ClockRegressed);
    let mut backwards = event(12, 12, None, &[], &[]);
    backwards.received_at_ns = 110;
    assert_eq!(book.apply(backwards), Err(BookError::ClockRegressed));
    assert_eq!(book.last_update_id(), Some(11));
    assert_eq!(book.state(), BookState::Invalid(InvalidReason::Clock));
}

#[test]
fn u64_max_snapshot_never_overflows_spot_next_update_check() {
    let mut book: Book<BinanceSpot, 4, 8> = Book::new(KEY, 1);
    let mut snap = snapshot(&[BID], &[ASK]);
    snap.last_update_id = u64::MAX;
    book.install_snapshot(snap).unwrap();
    let mut last = event(0, 0, None, &[], &[]);
    last.first_update_id = u64::MAX;
    last.last_update_id = u64::MAX;
    assert_eq!(book.apply(last), Ok(ApplyOutcome::IgnoredStale));
    assert_eq!(book.state(), BookState::Bridging);
}

#[test]
fn external_buffer_loss_and_disconnect_require_current_generation() {
    let mut book = spot::<4, 8>();
    book.apply(event(11, 11, None, &[], &[])).unwrap();
    assert_eq!(
        book.invalidate(0, InvalidReason::Input),
        Err(BookError::WrongGeneration)
    );
    assert_eq!(book.state(), BookState::Live);
    book.invalidate(1, InvalidReason::Input).unwrap();
    assert_eq!(book.view(111, 0).unwrap_err(), ViewError::NotLive);
    assert_eq!(book.diagnostic_levels(Side::Bid), &[BID]);
}

#[test]
fn snapshot_capacity_duplicate_price_and_zero_capacity_fail_closed() {
    let mut book: Book<BinanceSpot, 1, 8> = Book::new(KEY, 1);
    assert_eq!(
        book.install_snapshot(snapshot(&[BID, level(99, 1)], &[ASK])),
        Err(BookError::Capacity { side: Side::Bid })
    );
    assert_eq!(book.last_update_id(), None);
    assert!(book.diagnostic_levels(Side::Bid).is_empty());

    let mut book: Book<BinanceSpot, 2, 8> = Book::new(KEY, 1);
    assert_eq!(
        book.install_snapshot(snapshot(&[BID, BID], &[ASK])),
        Err(BookError::DuplicatePrice { side: Side::Bid })
    );

    let mut book: Book<BinanceSpot, 0, 0> = Book::new(KEY, 1);
    book.install_snapshot(snapshot(&[], &[])).unwrap();
    book.apply(event(11, 11, None, &[], &[])).unwrap();
    assert_eq!(book.view(111, 0).unwrap().best_bid(), BestLevel::Unknown);
    assert_eq!(
        book.apply(event(12, 12, None, &[BID], &[])),
        Err(BookError::TooManyUpdates)
    );
}

#[test]
fn unsorted_snapshots_and_updates_are_normalized() {
    let mut book: Book<BinanceSpot, 8, 8> = Book::new(KEY, 1);
    book.install_snapshot(snapshot(
        &[level(99, 2), BID, level(98, 3)],
        &[level(112, 3), ASK, level(111, 2)],
    ))
    .unwrap();
    book.apply(event(
        11,
        11,
        None,
        &[level(97, 4), level(101, 5)],
        &[level(114, 5), level(109, 4)],
    ))
    .unwrap();
    let view = book.view(111, 0).unwrap();
    assert_eq!(
        view.known_bids(),
        &[level(101, 5), BID, level(99, 2), level(98, 3), level(97, 4)]
    );
    assert_eq!(
        view.known_asks(),
        &[
            level(109, 4),
            ASK,
            level(111, 2),
            level(112, 3),
            level(114, 5)
        ]
    );
    assert_eq!(
        view.coverage(Side::Bid),
        SideCoverage::Through(PriceTicks(98))
    );
    assert_eq!(
        view.coverage(Side::Ask),
        SideCoverage::Through(PriceTicks(112))
    );
}

#[test]
fn bid_and_ask_moves_can_cross_old_opposite_quote_but_not_final_quote() {
    let mut book = spot::<4, 8>();
    book.apply(event(
        11,
        11,
        None,
        &[level(115, 1), level(100, 0)],
        &[level(120, 2), level(110, 0)],
    ))
    .unwrap();
    let view = book.view(111, 0).unwrap();
    assert_eq!(view.best_bid(), BestLevel::Known(level(115, 1)));
    assert_eq!(view.known_asks(), &[level(120, 2)]);
    // 120 is outside the original known ask coverage; 111..119 remain unknown.
    assert_eq!(view.best_ask(), BestLevel::Unknown);
}

#[test]
fn no_event_is_applied_before_snapshot_and_generation_never_wraps() {
    let mut book: Book<BinanceSpot, 4, 8> = Book::new(KEY, 1);
    assert_eq!(
        book.apply(event(11, 11, None, &[], &[])),
        Err(BookError::NeedsSnapshot)
    );
    book.begin_rebuild(u64::MAX).unwrap();
    assert_eq!(
        book.begin_rebuild(0),
        Err(BookError::GenerationNotIncreasing)
    );
    assert_eq!(book.generation(), u64::MAX);
}

#[test]
fn storage_can_use_an_independent_exchange_policy() {
    struct StrictNext;
    impl SequencePolicy for StrictNext {
        fn classify(input: SequenceInput) -> Result<SequenceDecision, SequenceError> {
            if input.first_update_id == input.last_update_id
                && input.local_id.checked_add(1) == Some(input.last_update_id)
            {
                Ok(SequenceDecision::Apply)
            } else {
                Err(SequenceError::Gap)
            }
        }
    }
    let mut book: Book<StrictNext, 4, 8> = Book::new(KEY, 1);
    book.install_snapshot(snapshot(&[BID], &[ASK])).unwrap();
    book.apply(event(11, 11, None, &[level(100, 8)], &[]))
        .unwrap();
    assert_eq!(
        book.view(111, 0).unwrap().best_bid(),
        BestLevel::Known(level(100, 8))
    );
    assert_eq!(
        book.apply(event(12, 13, None, &[], &[])),
        Err(BookError::Sequence(SequenceError::Gap))
    );
}

#[test]
fn replay_matches_independent_btree_map_oracle() {
    use std::collections::BTreeMap;

    for seed in [1_u64, 17, 2026, 999_999] {
        let mut random = seed;
        let mut book: Book<BinanceSpot, 16, 6> = Book::new(KEY, 1);
        let mut snap = snapshot(&[], &[]);
        snap.bids_complete = true;
        snap.asks_complete = true;
        book.install_snapshot(snap).unwrap();
        let mut expected_bids = BTreeMap::<i64, i64>::new();
        let mut expected_asks = BTreeMap::<i64, i64>::new();
        for step in 0..2000 {
            let mut bids = [level(0, 0); 3];
            let mut asks = [level(0, 0); 3];
            for index in 0..3 {
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                bids[index] = level(
                    80 + (step * 3 + index as u64) as i64 % 16,
                    (random >> 32) as i64 % 21,
                );
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                asks[index] = level(
                    110 + (step * 5 + index as u64) as i64 % 16,
                    (random >> 32) as i64 % 21,
                );
            }
            for (levels, expected) in [(&bids, &mut expected_bids), (&asks, &mut expected_asks)] {
                for Level {
                    price: PriceTicks(price),
                    quantity: QtyLots(quantity),
                } in levels
                {
                    if *quantity == 0 {
                        expected.remove(price);
                    } else {
                        expected.insert(*price, *quantity);
                    }
                }
            }
            let last = 13 + step * 3;
            book.apply(event(last - 2, last, None, &bids, &asks))
                .unwrap();
            let view = book.view(100 + last, 0).unwrap();
            let sorted_bids: Vec<_> = expected_bids
                .iter()
                .rev()
                .map(|(p, q)| level(*p, *q))
                .collect();
            let sorted_asks: Vec<_> = expected_asks.iter().map(|(p, q)| level(*p, *q)).collect();
            assert_eq!(view.known_bids(), sorted_bids);
            assert_eq!(view.known_asks(), sorted_asks);
            for price in 70..130 {
                assert_eq!(
                    view.quantity(Side::Bid, PriceTicks(price)),
                    QuantityKnowledge::Known(QtyLots(*expected_bids.get(&price).unwrap_or(&0)))
                );
                assert_eq!(
                    view.quantity(Side::Ask, PriceTicks(price)),
                    QuantityKnowledge::Known(QtyLots(*expected_asks.get(&price).unwrap_or(&0)))
                );
            }
        }
    }
}
