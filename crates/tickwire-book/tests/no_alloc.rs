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
