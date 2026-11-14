//! Shared-memory `RecordBorrow` benchmark comparing 512-byte stack `decode_record`
//! against zero-copy `RecordBorrow::as_market_record` over a real sealed Linux `memfd` bank.
#![allow(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]

use std::hint::black_box;
use std::time::Instant;
use tickwire_ipc::{
    DepthLevel, MarketBody, MarketRecord, RecordHeader, ResourceHeader, decode_record,
};
use tickwire_shm::{
    BankKey, MarketMessage, Publisher, Subscriber, SubscriptionKey, configure_market_arena,
};

fn market_header(capacity: u32) -> ResourceHeader {
    let mut header = ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
        header_bytes: 4096,
        resource_kind: 1,
        target_arch: if cfg!(target_arch = "x86_64") { 1 } else { 2 },
        endian: 1,
        atomic_width: 8,
        page_shift: 12,
        isolation_shift: 7,
        record_bytes: 512,
        capacity,
        resource_bytes: 8192 + u64::from(capacity) * 512,
        resource_id: [11; 16],
        channel_id: [22; 16],
        producer_instance: [33; 16],
        producer_generation: 1,
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        first_seq: 1,
        initial_membership_version: 1,
        producer_page_offset: 4096,
        slot_offset: 8192,
        required_features: 0x1c,
        max_consumers: 4,
        schema_major: 1,
        topic_id: 7,
        clock_domain: 1,
        ..Default::default()
    };
    configure_market_arena(&mut header, &[(512, 64)]).unwrap();
    header
}

fn measure_ps<F: FnMut()>(name: &str, batch: u64, samples: usize, mut op: F) {
    let bench_mode = std::env::args().any(|a| a == "--bench");
    let (warmup, batch, samples) = if bench_mode {
        (32, batch, samples)
    } else {
        (1, 2, 2)
    };
    for _ in 0..warmup {
        for _ in 0..batch {
            op();
        }
    }
    let mut ps_per_op = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        for _ in 0..batch {
            op();
        }
        let elapsed_ps = start.elapsed().as_nanos() * 1_000;
        ps_per_op.push((elapsed_ps / u128::from(batch)) as u64);
    }
    ps_per_op.sort_unstable();
    let p50 = ps_per_op[samples * 50 / 100];
    let p99 = ps_per_op[samples * 99 / 100];
    let p999 = ps_per_op[(samples * 999 / 1000).min(samples - 1)];
    println!(
        "{name:<52} P50={:>5}.{:03} ns  P99={:>5}.{:03} ns  P99.9={:>5}.{:03} ns",
        p50 / 1_000,
        p50 % 1_000,
        p99 / 1_000,
        p99 % 1_000,
        p999 / 1_000,
        p999 % 1_000,
    );
}

fn print_env() {
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("model name"))
                .map(|l| l.split(':').nth(1).unwrap_or("").trim().to_string())
        })
        .unwrap_or_else(|| "unknown".to_string());
    let kernel = std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let load = std::fs::read_to_string("/proc/loadavg")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    println!(
        "env: cpu=\"{cpu}\" kernel=\"{kernel}\" loadavg=\"{load}\" feature_crc32={}",
        cfg!(feature = "crc32")
    );
}

fn main() {
    print_env();
    let header = market_header(1024);
    let mut publisher = Publisher::<4>::create(header).unwrap();
    let bank_key = BankKey {
        resource: u128::from_le_bytes([11; 16]),
        channel: u128::from_le_bytes([22; 16]),
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
    };
    let sub_key = SubscriptionKey {
        bank: bank_key,
        instance: u128::from_le_bytes([44; 16]),
        generation: 1,
        cursor: u128::from_le_bytes([55; 16]),
    };
    let baseline = publisher.join(sub_key).unwrap() - 1;
    // SAFETY: single-owner Publisher created in this process obeys the SDK retention contract.
    let subscriber = unsafe {
        Subscriber::<64>::attach(
            publisher.reader_fd().unwrap(),
            publisher.header().clone(),
            sub_key,
            baseline,
        )
        .unwrap()
    };
    // SAFETY: cursor FD belongs to the `Subscriber` attached above for `sub_key`.
    unsafe {
        publisher
            .register_cursor(sub_key, subscriber.cursor_fd().unwrap())
            .unwrap();
    }

    let levels = [
        DepthLevel {
            price_ticks: 100_000,
            qty_lots: 10,
        },
        DepthLevel {
            price_ticks: 99_990,
            qty_lots: 25,
        },
        DepthLevel {
            price_ticks: 100_010,
            qty_lots: 12,
        },
        DepthLevel {
            price_ticks: 100_020,
            qty_lots: 18,
        },
    ];
    let first_record = MarketRecord {
        header: RecordHeader {
            sequence: 1,
            message_type: 259,
            schema_major: 1,
            header_bytes: 128,
            flags: if cfg!(feature = "crc32") { 6 | 8 } else { 6 },
            body_bytes: 384,
            topic_or_scope_id: 7,
            feed_generation: 1,
            producer_generation: 1,
            instrument_id: 101,
            instrument_version: 1,
            ingress_mono_ns: 1_000_000,
            publish_mono_ns: 1_000_005,
            ..Default::default()
        },
        body: MarketBody {
            venue_seq_first: 1,
            venue_seq_last: 1,
            book_revision: 1,
            bid_count: 2,
            ask_count: 2,
            depth_flags: 1,
            ..Default::default()
        },
    };
    publisher
        .publish_market(&[MarketMessage {
            record: &first_record,
            levels: &levels,
        }])
        .unwrap();

    // Hold one live RecordBorrow over the mapped memfd slot to compare isolated decode vs zero-copy view,
    // then also measure full publish -> borrow -> read -> apply -> drop roundtrips.
    let guard = subscriber.borrow().unwrap().unwrap();
    measure_ps("shm_guard/512B/stack_decode_record", 1024, 500, || {
        let rec = decode_record(black_box(guard.bytes())).unwrap();
        black_box(&rec);
    });
    measure_ps(
        "shm_guard/512B/zerocopy_as_market_record",
        2048,
        500,
        || {
            let rec = black_box(&guard).as_market_record().unwrap();
            black_box(rec.body.inline_levels[0].price_ticks);
        },
    );
    measure_ps("shm_guard/512B/zerocopy_depth_payload", 2048, 500, || {
        let payload = black_box(&guard).depth_payload().unwrap();
        black_box(payload.len());
    });
    let seq = guard.sequence();
    drop(guard);
    subscriber.apply(seq).unwrap();

    let mut next_seq = 2u64;
    measure_ps(
        "shm_ring_e2e/publish_borrow_stack_decode_drop",
        64,
        500,
        || {
            let rec = MarketRecord {
                header: RecordHeader {
                    sequence: next_seq,
                    message_type: 257,
                    schema_major: 1,
                    header_bytes: 128,
                    flags: if cfg!(feature = "crc32") { 6 | 8 } else { 6 },
                    body_bytes: 384,
                    topic_or_scope_id: 7,
                    feed_generation: 1,
                    producer_generation: 1,
                    instrument_id: 101,
                    instrument_version: 1,
                    ingress_mono_ns: 1_000_000 + next_seq,
                    publish_mono_ns: 1_000_005 + next_seq,
                    ..Default::default()
                },
                body: MarketBody {
                    exchange_event_ns: next_seq,
                    bid_price_ticks: 100_000,
                    bid_qty_lots: 10,
                    ask_price_ticks: 100_010,
                    ask_qty_lots: 12,
                    feed_state: 2,
                    ..Default::default()
                },
            };
            publisher
                .publish_market(&[MarketMessage {
                    record: &rec,
                    levels: &[],
                }])
                .unwrap();
            let g = subscriber.borrow().unwrap().unwrap();
            let decoded = decode_record(g.bytes()).unwrap();
            black_box(&decoded);
            let s = g.sequence();
            drop(g);
            subscriber.apply(s).unwrap();
            next_seq += 1;
        },
    );

    measure_ps("shm_ring_e2e/publish_borrow_zerocopy_drop", 64, 500, || {
        let rec = MarketRecord {
            header: RecordHeader {
                sequence: next_seq,
                message_type: 257,
                schema_major: 1,
                header_bytes: 128,
                flags: if cfg!(feature = "crc32") { 6 | 8 } else { 6 },
                body_bytes: 384,
                topic_or_scope_id: 7,
                feed_generation: 1,
                producer_generation: 1,
                instrument_id: 101,
                instrument_version: 1,
                ingress_mono_ns: 1_000_000 + next_seq,
                publish_mono_ns: 1_000_005 + next_seq,
                ..Default::default()
            },
            body: MarketBody {
                exchange_event_ns: next_seq,
                bid_price_ticks: 100_000,
                bid_qty_lots: 10,
                ask_price_ticks: 100_010,
                ask_qty_lots: 12,
                feed_state: 2,
                ..Default::default()
            },
        };
        publisher
            .publish_market(&[MarketMessage {
                record: &rec,
                levels: &[],
            }])
            .unwrap();
        let g = subscriber.borrow().unwrap().unwrap();
        let borrowed = g.as_market_record().unwrap();
        black_box(borrowed.body.bid_price_ticks);
        let s = g.sequence();
        drop(g);
        subscriber.apply(s).unwrap();
        next_seq += 1;
    });
}
