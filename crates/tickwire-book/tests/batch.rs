use tickwire_book::*;

const KEY: BookKey = BookKey {
    venue: 1,
    instrument: 1,
    source: 1,
};
fn level(price: i64, quantity: i64) -> Level {
    Level {
        price: PriceTicks(price),
        quantity: QtyLots(quantity),
    }
}
fn event<'a>(id: u64, bids: &'a [Level], asks: &'a [Level]) -> DepthEvent<'a> {
    DepthEvent {
        key: KEY,
        generation: 1,
        first_update_id: id,
        last_update_id: id,
        previous_update_id: None,
        received_at_ns: id,
        bids,
        asks,
    }
}
fn initial() -> Book<BinanceSpot, 128, 512> {
    let bids: Vec<_> = (0..128).map(|i| level(10_000 - 2 * i, 1)).collect();
    let asks: Vec<_> = (0..128).map(|i| level(20_000 + 2 * i, 1)).collect();
    let mut book = Book::new(KEY, 1);
    book.install_snapshot(Snapshot {
        key: KEY,
        generation: 1,
        last_update_id: 1,
        bids: &bids,
        asks: &asks,
        bids_complete: false,
        asks_complete: false,
    })
    .unwrap();
    book
}

#[test]
fn full_capacity_replacement_and_reverse_batches_preserve_unknown_coverage() {
    for reverse in [false, true] {
        let mut book = initial();
        // Insert all new far-side prices before removing the complete old side.
        let mut bids: Vec<_> = (0..128).map(|i| level(9_000 - 2 * i, 2)).collect();
        bids.extend((0..128).map(|i| level(10_000 - 2 * i, 0)));
        let mut asks: Vec<_> = (0..128).map(|i| level(21_000 + 2 * i, 3)).collect();
        asks.extend((0..128).map(|i| level(20_000 + 2 * i, 0)));
        if reverse {
            bids.reverse();
            asks.reverse();
        }
        book.apply(event(2, &bids, &asks)).unwrap();
        let view = book.view(2, 0).unwrap();
        assert_eq!(view.known_bids().len(), 128);
        assert_eq!(view.known_asks().len(), 128);
        assert_eq!(view.best_bid(), BestLevel::Unknown);
        assert_eq!(view.best_ask(), BestLevel::Unknown);
        assert_eq!(
            view.quantity(Side::Bid, PriceTicks(9_000)),
            QuantityKnowledge::Known(QtyLots(2))
        );
        assert_eq!(
            view.quantity(Side::Bid, PriceTicks(9_001)),
            QuantityKnowledge::Unknown
        );
        assert_eq!(
            view.quantity(Side::Bid, PriceTicks(10_000)),
            QuantityKnowledge::Known(QtyLots(0))
        );
        assert_eq!(
            view.coverage(Side::Bid),
            SideCoverage::Through(PriceTicks(9_746))
        );
        assert_eq!(
            view.coverage(Side::Ask),
            SideCoverage::Through(PriceTicks(20_254))
        );
    }
}

#[test]
fn bad_large_second_side_never_commits_first_side_or_sequence() {
    for error in 0..4 {
        let mut book = initial();
        book.apply(event(2, &[], &[])).unwrap();
        let prior_bids = book.diagnostic_levels(Side::Bid).to_vec();
        let prior_asks = book.diagnostic_levels(Side::Ask).to_vec();
        let bids: Vec<_> = (0..128).map(|i| level(10_000 - 2 * i, 7)).collect();
        let mut asks: Vec<_> = (0..128).map(|i| level(20_000 + 2 * i, 8)).collect();
        match error {
            0 => asks.push(asks[64]), // Distant duplicate, not adjacent in input.
            1 => asks[127].quantity = QtyLots(-1),
            2 => asks.push(level(22_000, 1)), // Final capacity exceeds 128.
            _ => {
                asks[127].quantity = QtyLots(0);
                asks.push(level(9_999, 1)); // Capacity unchanged, but crossed.
            }
        }
        let expected = match error {
            0 => BookError::DuplicatePrice { side: Side::Ask },
            1 => BookError::InvalidLevel,
            2 => BookError::Capacity { side: Side::Ask },
            _ => BookError::CrossedBook,
        };
        assert_eq!(book.apply(event(3, &bids, &asks)), Err(expected));
        assert_eq!(book.diagnostic_levels(Side::Bid), prior_bids);
        assert_eq!(book.diagnostic_levels(Side::Ask), prior_asks);
        assert_eq!(book.last_update_id(), Some(2));
        assert!(matches!(book.state(), BookState::Invalid(_)));
        assert_eq!(book.view(3, 0).unwrap_err(), ViewError::NotLive);
    }
}
