#![forbid(unsafe_code)]
#![cfg(target_os = "linux")]

#[path = "common/publisher_hot.rs"]
mod common;
#[path = "common/publisher_inputs.rs"]
mod inputs;
#[path = "common/event_join.rs"]
mod join;
#[path = "common/publisher_scenarios.rs"]
mod scenarios;
#[path = "../../tickwire-binance/tests/common/mod.rs"]
mod venue;
use common::*;
use tickwire_binance::market::EventKind;
use tickwire_binance_public::{DepthEncoding, EventNotice, Notice, SyncOutcome};
use tickwire_ipc::MarketStateRecord;

#[test]
fn reserved_cache_budget_is_exact_and_stays_fixed_after_publication() {
    let f = Fixture::new(&[], 64, 32);
    let required = f.publisher.reserved_cache_bytes();
    assert!(required > 0);
    assert!(matches!(
        Fixture::with_budget(&[], 64, 32, required - 1),
        Err(tickwire_market_publisher::Error::Bounds)
    ));
    let mut exact = Fixture::with_budget(&[], 64, 32, required).unwrap();
    let book = book();
    let notice = inputs::notice(&book, None, DepthEncoding::Json, 2);
    exact
        .publisher
        .on_notice_slot(exact.slot, &notice, 2, &mut exact.faults)
        .unwrap();
    assert_eq!(exact.publisher.reserved_cache_bytes(), required);
}

#[test]
fn first_quote_publishes_to_real_bank_without_allocations() {
    let mut f = Fixture::new(&[], 64, 32);
    let book = book();
    let notice = Notice::Sync {
        connection_epoch: 1,
        book_generation: 1,
        received_mono_ns: Some(2),
        raw_delta: None,
        encoding: DepthEncoding::Json,
        outcome: SyncOutcome::Applied(tickwire_book::ApplyOutcome::Applied {
            last_update_id: 2,
            became_live: true,
        }),
        view: Some(book.view(2, 100).unwrap()),
    };
    let allocations = allocation_counter::measure(|| {
        f.publisher
            .on_notice_slot(f.slot, &notice, 2, &mut f.faults)
            .unwrap();
    });
    eprintln!(
        "first_quote: allocations={} bytes={}",
        allocations.count_total, allocations.bytes_total
    );
    assert_eq!(f.publisher.state().sequence(), 2);
    assert_eq!(f.faults.0, 0);
    assert_eq!(f.record(1).header().message_type, 257);
    assert_eq!(f.record(2).header().message_type, 261);
    assert_eq!(allocations.count_total, 0);
}

#[test]
fn first_exact_categories_and_post_handover_event_allocate_nothing() {
    for kind in [
        EventKind::Trade,
        EventKind::BestBidAsk,
        EventKind::MarkFunding,
    ] {
        let mut f = Fixture::new(&[kind], 64, 32);
        let reserved = f.publisher.reserved_cache_bytes();
        let event = inputs::event(kind, 2);
        let allocations = allocation_counter::measure(|| {
            f.publisher
                .on_event_slot(
                    f.slot,
                    &EventNotice::Event {
                        event: &event,
                        raw: &[],
                        usable: true,
                    },
                    2,
                    &mut f.faults,
                )
                .unwrap();
        });
        eprintln!(
            "first_{kind:?}: allocations={} bytes={}",
            allocations.count_total, allocations.bytes_total
        );
        assert_eq!(f.publisher.state().sequence(), 2);
        assert_eq!(f.faults.0, 0);
        assert_eq!(
            f.record(1).header().message_type,
            match kind {
                EventKind::Trade => 264,
                EventKind::BestBidAsk => 265,
                EventKind::MarkFunding => 266,
            }
        );
        assert_eq!(f.record(2).header().message_type, 261);
        assert_eq!(allocations.count_total, 0);
        assert_eq!(f.publisher.reserved_cache_bytes(), reserved);
    }
    let kinds = [
        EventKind::Trade,
        EventKind::BestBidAsk,
        EventKind::MarkFunding,
    ];
    let mut f = Fixture::new(&kinds, 64, 32);
    let reserved = f.publisher.reserved_cache_bytes();
    let mut book = book();
    let notice = inputs::notice(&book, None, DepthEncoding::Json, 2);
    let allocations = allocation_counter::measure(|| {
        f.publisher
            .on_notice_slot(f.slot, &notice, 2, &mut f.faults)
            .unwrap();
    });
    assert_eq!(allocations.count_total, 0);
    // The independently selected BBA stream suppresses legacy Quote publication.
    assert_eq!(f.publisher.state().sequence(), 1);
    assert_eq!(f.record(1).header().message_type, 261);
    book.apply(tickwire_book::DepthEvent {
        key: KEY,
        generation: 1,
        first_update_id: 3,
        last_update_id: 3,
        previous_update_id: None,
        received_at_ns: 3,
        bids: &[],
        asks: &[],
    })
    .unwrap();
    let raw = br#"{"e":"depth\u0055pdate","s":"BTC\u0055SDT","E":1,"T":1,"U":3,"u":3,"pu":2,"b":[],"a":[["200.\u0030\u0030","0.001"]]}"#;
    let notice = inputs::notice(&book, Some(raw), DepthEncoding::Json, 3);
    let allocations = allocation_counter::measure(|| {
        f.publisher
            .on_notice_slot(f.slot, &notice, 3, &mut f.faults)
            .unwrap();
    });
    assert_eq!(allocations.count_total, 0);
    assert_eq!(f.publisher.state().sequence(), 3);
    assert_eq!(f.depth(2).1[0].price_ticks, 20000);
    assert_eq!(f.record(3).header().message_type, 261);
    for (i, kind) in kinds.into_iter().enumerate() {
        let n = i as u64 + 4;
        let event = inputs::event(kind, n);
        let allocations = allocation_counter::measure(|| {
            f.publisher
                .on_event_slot(
                    f.slot,
                    &EventNotice::Event {
                        event: &event,
                        raw: &[],
                        usable: true,
                    },
                    n,
                    &mut f.faults,
                )
                .unwrap();
        });
        assert_eq!(allocations.count_total, 0);
        assert_eq!(f.publisher.reserved_cache_bytes(), reserved);
        assert_eq!(f.publisher.state().sequence(), 5 + i as u64 * 2);
        assert_eq!(
            f.record(4 + i as u64 * 2).header().message_type,
            264 + i as u16
        );
    }
    let old_sequence = f.publisher.state().sequence();
    let _old_bank = f.rotate(7);
    // Bank replay retains the old message-type order, including FeedState before exact events.
    for (i, kind) in [261, 264, 265, 266].into_iter().enumerate() {
        let record = f.record(old_sequence + i as u64 + 1);
        assert_eq!(record.header().message_type, kind);
        assert_eq!(record.header().feed_generation, 2);
    }
    let event = inputs::event(EventKind::Trade, 8);
    let allocations = allocation_counter::measure(|| {
        f.publisher
            .on_event_slot(
                f.slot,
                &EventNotice::Event {
                    event: &event,
                    raw: &[],
                    usable: true,
                },
                8,
                &mut f.faults,
            )
            .unwrap();
    });
    assert_eq!(allocations.count_total, 0);
    let seq = f.publisher.state().sequence();
    assert_eq!(f.record(seq - 1).header().message_type, 264);
    f.snapshot(9);
}

fn child(case: &str) {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "publisher_hot_worker",
            "--ignored",
            "--nocapture",
        ])
        .env("TICKWIRE_PUBLISHER_HOT_CASE", case)
        .output()
        .unwrap();
    eprint!("{}", String::from_utf8_lossy(&output.stdout));
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    assert!(
        output.status.success(),
        "publisher case {case}: {}",
        output.status
    );
}

#[test]
fn json_depth_inline_arena_and_long_then_short_are_allocation_free_on_two_mib_stack() {
    child("json");
}
#[test]
fn sbe_depth_inline_arena_and_long_then_short_are_allocation_free_on_two_mib_stack() {
    child("sbe");
}
#[test]
fn failures_close_the_bank_without_advancing_the_published_prefix() {
    child("failures");
}

#[test]
#[ignore = "invoked by the stack-isolating subprocess tests"]
fn publisher_hot_worker() {
    let case = std::env::var("TICKWIRE_PUBLISHER_HOT_CASE").unwrap();
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(move || match case.as_str() {
            "json" => scenarios::depth_sequence(DepthEncoding::Json),
            "sbe" => scenarios::depth_sequence(DepthEncoding::SpotSbeV1),
            "failures" => scenarios::failures(),
            _ => panic!("unknown child case"),
        })
        .unwrap()
        .join()
        .unwrap();
}
