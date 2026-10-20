use super::*;

const KEY: BookKey = BookKey {
    venue: 1,
    instrument: 1,
    source: 1,
};
fn level(p: i64, q: i64) -> Level {
    Level {
        price: PriceTicks(p),
        quantity: QtyLots(q),
    }
}
fn book<const N: usize, const M: usize>() -> Book<BinanceSpot, N, M> {
    let mut book = Book::new(KEY, 1);
    let bids = core::array::from_fn::<_, N, _>(|i| level(10_000 - 2 * i as i64, 1));
    book.install_snapshot(Snapshot {
        key: KEY,
        generation: 1,
        last_update_id: 1,
        bids: &bids,
        asks: &[],
        bids_complete: true,
        asks_complete: true,
    })
    .unwrap();
    book
}
fn event(bids: &[Level]) -> DepthEvent<'_> {
    DepthEvent {
        key: KEY,
        generation: 1,
        first_update_id: 2,
        last_update_id: 2,
        previous_update_id: None,
        received_at_ns: 2,
        bids,
        asks: &[],
    }
}

#[test]
fn large_batch_has_bounded_price_comparisons() {
    let mut book = book::<1024, 512>();
    let updates =
        core::array::from_fn::<_, 512, _>(|i| level(10_000 - 2 * ((i * 73) % 512) as i64, 2));
    work::reset();
    book.apply(event(&updates)).unwrap();
    let comparisons = work::counts().0;
    std::println!("512-update price comparisons: {comparisons}");
    assert!(
        comparisons <= 512 * 32,
        "512 updates used {comparisons} price comparisons"
    );
}

#[test]
fn bulk_structure_changes_do_not_move_a_tail_for_every_update() {
    let mut book = book::<1024, 128>();
    let updates = core::array::from_fn::<_, 128, _>(|i| {
        if i % 2 == 0 {
            level(10_000 - 2 * (i / 2) as i64, 0)
        } else {
            level(10_001 + 2 * (i / 2) as i64, 1)
        }
    });
    work::reset();
    book.apply(event(&updates)).unwrap();
    let moved = work::counts().1;
    std::println!("128-update moved levels: {moved}");
    assert!(moved <= 4 * 1024, "128 updates moved {moved} levels");
    assert_eq!(book.diagnostic_levels(Side::Bid).len(), 1024);
}

#[test]
fn one_replacement_does_not_scan_or_copy_the_full_book() {
    let mut book = book::<4096, 512>();
    work::reset();
    book.apply(event(&[level(6000, 2)])).unwrap();
    let (comparisons, moved) = work::counts();
    std::println!(
        "single replacement comparisons={comparisons} moved={moved}; Book<4096,512> bytes={}",
        core::mem::size_of_val(&book)
    );
    assert!(
        comparisons < 128,
        "single replacement used {comparisons} comparisons"
    );
    assert!(moved <= 4, "single replacement moved {moved} levels");
}
