use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use tickwire_book::*;

struct CountingAllocator;

thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

fn record() {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
    }
}

// Test-only instrumentation delegates every operation to the system allocator.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record();
        unsafe { System.realloc(pointer, layout, size) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[test]
fn construction_snapshot_updates_views_and_rebuild_allocate_nothing() {
    let key = BookKey {
        venue: 1,
        instrument: 2,
        source: 3,
    };
    let bid = Level {
        price: PriceTicks(100),
        quantity: QtyLots(5),
    };
    let ask = Level {
        price: PriceTicks(110),
        quantity: QtyLots(6),
    };
    ALLOCATIONS.with(|count| count.set(0));
    TRACK.with(|track| track.set(true));

    let mut book: Book<BinanceSpot, 64, 16> = Book::new(key, 1);
    book.install_snapshot(Snapshot {
        key,
        generation: 1,
        last_update_id: 10,
        bids: &[bid],
        asks: &[ask],
        bids_complete: false,
        asks_complete: false,
    })
    .unwrap();
    for id in 11..1011 {
        book.apply(DepthEvent {
            key,
            generation: 1,
            first_update_id: id,
            last_update_id: id,
            previous_update_id: None,
            received_at_ns: id,
            bids: &[bid],
            asks: &[ask],
        })
        .unwrap();
        let view = book.view(id, 0).unwrap();
        assert_eq!(view.best_bid(), BestLevel::Known(bid));
        assert_eq!(view.best_ask(), BestLevel::Known(ask));
    }
    book.begin_rebuild(2).unwrap();

    TRACK.with(|track| track.set(false));
    assert_eq!(ALLOCATIONS.with(Cell::get), 0);
}

#[test]
fn sorted_work_area_and_bulk_merge_allocate_nothing() {
    let key = BookKey {
        venue: 1,
        instrument: 2,
        source: 3,
    };
    let make = |p, q| Level {
        price: PriceTicks(p),
        quantity: QtyLots(q),
    };
    let bids = core::array::from_fn::<_, 512, _>(|i| make(10_000 - 2 * i as i64, 1));
    let asks = core::array::from_fn::<_, 512, _>(|i| make(20_000 + 2 * i as i64, 1));
    let mut updates = [make(1, 0); 256];
    ALLOCATIONS.with(|count| count.set(0));
    TRACK.with(|track| track.set(true));
    let mut book: Book<BinanceSpot, 512, 256> = Book::new(key, 1);
    book.install_snapshot(Snapshot {
        key,
        generation: 1,
        last_update_id: 1,
        bids: &bids,
        asks: &asks,
        bids_complete: false,
        asks_complete: false,
    })
    .unwrap();
    for iteration in 0..1000 {
        for rank in 0..128 {
            let from = iteration % 2;
            let to = 1 - from;
            // Input is interleaved and reversed relative to the side order.
            updates[2 * rank] = make(10_000 - 2 * rank as i64 + from, 0);
            updates[2 * rank + 1] = make(10_000 - 2 * rank as i64 + to, 3);
        }
        updates.reverse();
        let id = iteration as u64 + 2;
        book.apply(DepthEvent {
            key,
            generation: 1,
            first_update_id: id,
            last_update_id: id,
            previous_update_id: None,
            received_at_ns: id,
            bids: &updates,
            asks: &[],
        })
        .unwrap();
        assert_eq!(book.view(id, 0).unwrap().known_bids().len(), 512);
    }
    TRACK.with(|track| track.set(false));
    assert_eq!(ALLOCATIONS.with(Cell::get), 0);
}

#[test]
fn unordered_small_quantity_batches_allocate_nothing() {
    let key = BookKey {
        venue: 1,
        instrument: 2,
        source: 3,
    };
    let make = |p, q| Level {
        price: PriceTicks(p),
        quantity: QtyLots(q),
    };
    let bids = core::array::from_fn::<_, 512, _>(|i| make(10_000 - 2 * i as i64, 1));
    let asks = core::array::from_fn::<_, 512, _>(|i| make(20_000 + 2 * i as i64, 1));
    let bid_updates =
        core::array::from_fn::<_, 32, _>(|i| make(10_000 - 2 * (((i * 13) % 32) * 13) as i64, 3));
    let ask_updates =
        core::array::from_fn::<_, 32, _>(|i| make(20_000 + 2 * (((i * 13) % 32) * 13) as i64, 4));
    let cyclic_bids =
        core::array::from_fn::<_, 32, _>(|i| make(10_000 - 2 * ((i + 500) % 512) as i64, 5));
    let cyclic_asks =
        core::array::from_fn::<_, 32, _>(|i| make(20_000 + 2 * ((i + 500) % 512) as i64, 6));
    ALLOCATIONS.with(|count| count.set(0));
    TRACK.with(|track| track.set(true));
    let mut book: Book<BinanceSpot, 512, 64> = Book::new(key, 1);
    book.install_snapshot(Snapshot {
        key,
        generation: 1,
        last_update_id: 1,
        bids: &bids,
        asks: &asks,
        bids_complete: false,
        asks_complete: false,
    })
    .unwrap();
    for id in 2..1002 {
        book.apply(DepthEvent {
            key,
            generation: 1,
            first_update_id: id,
            last_update_id: id,
            previous_update_id: None,
            received_at_ns: id,
            bids: if id % 2 == 0 {
                &bid_updates
            } else {
                &cyclic_bids
            },
            asks: if id % 2 == 0 {
                &ask_updates
            } else {
                &cyclic_asks
            },
        })
        .unwrap();
        assert_eq!(book.view(id, 0).unwrap().known_bids().len(), 512);
    }
    TRACK.with(|track| track.set(false));
    assert_eq!(ALLOCATIONS.with(Cell::get), 0);
}
