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
fn book() -> Book<BinanceSpot, 512, 64> {
    let mut book = Book::new(KEY, 1);
    let bids = core::array::from_fn::<_, 512, _>(|i| level(10_000 - 2 * i as i64, 1));
    let asks = core::array::from_fn::<_, 512, _>(|i| level(20_000 + 2 * i as i64, 1));
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

#[test]
fn scattered_replacements_handle_collisions_and_preserve_coverage() {
    let mut book = book();
    // Every old index has the same low six bits; all eight prices are distinct.
    let updates =
        core::array::from_fn::<_, 8, _>(|i| level(10_000 - 2 * (((i * 3) % 8) * 64) as i64, 2));
    book.apply(event(2, &updates, &[])).unwrap();
    let view = book.view(2, 0).unwrap();
    assert_eq!(
        view.coverage(Side::Bid),
        SideCoverage::Through(PriceTicks(8978))
    );
    assert_eq!(view.best_bid(), BestLevel::Known(level(10_000, 2)));
    for update in updates {
        assert_eq!(
            view.quantity(Side::Bid, update.price),
            QuantityKnowledge::Known(QtyLots(2))
        );
    }
    assert_eq!(
        view.quantity(Side::Bid, PriceTicks(8976)),
        QuantityKnowledge::Unknown
    );
}

#[test]
fn late_duplicate_or_missing_price_cannot_partially_commit_either_side() {
    for failure in 0..3 {
        let mut book = book();
        let before_bids = book.diagnostic_levels(Side::Bid).to_vec();
        let before_asks = book.diagnostic_levels(Side::Ask).to_vec();
        let bids = [
            level(9996, 3),
            level(10_000, 3),
            level(9994, 3),
            level(9998, 3),
        ];
        let mut asks = [
            level(20004, 2),
            level(20000, 2),
            level(20002, 2),
            level(20006, 2),
        ];
        let expected = match failure {
            0 => {
                asks[3] = asks[0];
                BookError::DuplicatePrice { side: Side::Ask }
            }
            1 => {
                asks[3] = level(22000, 2); // positive absent price, at full capacity
                BookError::Capacity { side: Side::Ask }
            }
            _ => {
                asks[3].quantity = QtyLots(-1);
                BookError::InvalidLevel
            }
        };
        assert_eq!(book.apply(event(2, &bids, &asks)), Err(expected));
        assert_eq!(book.diagnostic_levels(Side::Bid), before_bids);
        assert_eq!(book.diagnostic_levels(Side::Ask), before_asks);
        assert_eq!(book.last_update_id(), Some(1));
        assert!(matches!(book.state(), BookState::Invalid(_)));
        assert!(book.view(2, 0).is_err());
    }
}

#[test]
fn absent_positive_fallback_and_zero_delete_keep_final_capacity_semantics() {
    let mut book = book();
    book.apply(event(2, &[level(9996, 0)], &[])).unwrap();
    // Existing values precede a missing positive price in an unordered batch.
    let updates = [
        level(9998, 7),
        level(9990, 5),
        level(10_000, 6),
        level(9997, 8),
    ];
    book.apply(event(3, &updates, &[])).unwrap();
    assert_eq!(book.diagnostic_levels(Side::Bid).len(), 512);
    let view = book.view(3, 0).unwrap();
    assert_eq!(
        view.quantity(Side::Bid, PriceTicks(9997)),
        QuantityKnowledge::Known(QtyLots(8))
    );
    assert_eq!(
        view.quantity(Side::Bid, PriceTicks(9996)),
        QuantityKnowledge::Known(QtyLots(0))
    );
}
