use super::{common::*, inputs};
use tickwire_binance_public::DepthEncoding;
use tickwire_ipc::MarketStateRecord;

pub fn depth_sequence(encoding: DepthEncoding) {
    let mut f = Fixture::new(&[], 64, 32);
    let reserved = f.publisher.reserved_cache_bytes();
    for (i, count) in [12, 13, 4096, 1].into_iter().enumerate() {
        let n = i as u64 + 2;
        // The complete book retains all levels even when a later delta is short.
        let book = inputs::depth_book(4096, n);
        let raw = match encoding {
            DepthEncoding::Json => inputs::json(count, n),
            DepthEncoding::SpotSbeV1 => inputs::sbe(count, n),
        };
        let notice = inputs::notice(&book, Some(&raw), encoding, n);
        let before = f.publisher.state().sequence();
        let allocations = allocation_counter::measure(|| {
            f.publisher
                .on_notice_slot(f.slot, &notice, n, &mut f.faults)
                .unwrap();
        });
        eprintln!(
            "{encoding:?}_{count}: allocations={} bytes={}",
            allocations.count_total, allocations.bytes_total
        );
        assert_eq!(f.publisher.state().sequence(), before + 3);
        assert_eq!(f.faults.0, 0);
        let (record, levels) = f.depth(before + 1);
        assert_eq!(record.body.venue_seq_last, n);
        assert_eq!(record.header.ingress_mono_ns, n);
        assert_eq!(record.body.bid_count, 0);
        assert_eq!(record.body.ask_count as usize, count);
        for (i, level) in levels.iter().enumerate() {
            assert_eq!((level.price_ticks, level.qty_lots), (20000 + i as i64, 100));
        }
        assert_eq!(f.record(before + 2).header().message_type, 257);
        assert_eq!(f.record(before + 3).header().message_type, 261);
        assert_eq!(allocations.count_total, 0);
        assert_eq!(f.publisher.reserved_cache_bytes(), reserved);
    }
    f.snapshot(10);
    let _retired = f.rotate(11);
    let book = inputs::depth_book(4096, 12);
    let raw = match encoding {
        DepthEncoding::Json => inputs::json(13, 12),
        DepthEncoding::SpotSbeV1 => inputs::sbe(13, 12),
    };
    let notice = inputs::notice(&book, Some(&raw), encoding, 12);
    let before = f.publisher.state().sequence();
    let allocations = allocation_counter::measure(|| {
        f.publisher
            .on_notice_slot(f.slot, &notice, 12, &mut f.faults)
            .unwrap();
    });
    assert_eq!(allocations.count_total, 0);
    assert_eq!(f.publisher.reserved_cache_bytes(), reserved);
    assert_eq!(f.depth(before + 1).1.len(), 13);
    f.snapshot(13);
}

fn failed(f: &mut Fixture, published: u64) {
    assert_eq!(f.publisher.state().sequence(), published);
    assert_eq!(f.publisher.split().0.published(), published);
    assert!(f.publisher.needs_bank());
    assert!(!f.publisher.state().ready_for_snapshot());
    assert_eq!(f.faults.0, 1);
}

pub fn failures() {
    for raw in [b"bad".to_vec(), inputs::json(13, 99), inputs::json(4097, 3)] {
        let mut f = Fixture::new(&[], 64, 32);
        let base = book();
        f.publisher
            .on_notice_slot(
                f.slot,
                &inputs::notice(&base, None, DepthEncoding::Json, 2),
                2,
                &mut f.faults,
            )
            .unwrap();
        let before = f.publisher.state().sequence();
        let quote = f.record(before - 1);
        let updated = inputs::depth_book(4096, 3);
        assert!(
            f.publisher
                .on_notice_slot(
                    f.slot,
                    &inputs::notice(&updated, Some(&raw), DepthEncoding::Json, 3),
                    3,
                    &mut f.faults
                )
                .is_err()
        );
        failed(&mut f, before);
        assert_eq!(f.record(before - 1), quote);
        // A failure can retain the current source view, but cannot make it visible until re-JOIN.
        let _retired = f.rotate(4);
        f.snapshot(5);
    }
    let mut ring = Fixture::new(&[], 64, 32);
    let book = book();
    let notice = inputs::notice(&book, None, DepthEncoding::Json, 2);
    for _ in 0..31 {
        ring.publisher
            .on_notice_slot(ring.slot, &notice, 2, &mut ring.faults)
            .unwrap();
    }
    assert_eq!(ring.publisher.state().sequence(), 62);
    let raw = inputs::json(13, 3);
    let updated = inputs::depth_book(13, 3);
    // Two free slots cannot admit a three-record depth/quote/feed batch.
    assert!(
        ring.publisher
            .on_notice_slot(
                ring.slot,
                &inputs::notice(&updated, Some(&raw), DepthEncoding::Json, 3),
                3,
                &mut ring.faults
            )
            .is_err()
    );
    failed(&mut ring, 62);
    assert_eq!(ring.record(61).header().message_type, 257);
    assert_eq!(ring.record(62).header().message_type, 261);

    // Pending JOIN pins the 32 real arena blocks while 128 ring slots remain sufficient.
    let mut arena = Fixture::new(&[], 128, 32);
    for i in 0..32 {
        let n = i + 2;
        let book = inputs::depth_book(13, n);
        let raw = inputs::json(13, n);
        arena
            .publisher
            .on_notice_slot(
                arena.slot,
                &inputs::notice(&book, Some(&raw), DepthEncoding::Json, n),
                n,
                &mut arena.faults,
            )
            .unwrap();
    }
    let book = inputs::depth_book(13, 34);
    let raw = inputs::json(13, 34);
    assert!(
        arena
            .publisher
            .on_notice_slot(
                arena.slot,
                &inputs::notice(&book, Some(&raw), DepthEncoding::Json, 34),
                34,
                &mut arena.faults
            )
            .is_err()
    );
    failed(&mut arena, 96);
    assert_eq!(arena.depth(1).1.len(), 13);
    assert_eq!(arena.depth(94).1.len(), 13);
    let _retired = arena.rotate(35);
    arena.snapshot(36);
}
