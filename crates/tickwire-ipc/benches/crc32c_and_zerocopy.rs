//! Microbenchmarks for Castagnoli CRC-32C (bit-by-bit baseline vs Slicing-by-8 vs HW)
//! and 512-byte `MarketRecord` stack `decode_record` vs zero-copy `from_aligned_bytes`.
use std::hint::black_box;
use std::time::Instant;
use tickwire_ipc::{
    DepthLevel, MarketBody, MarketRecord, RecordHeader, Wire, crc32c, crc32c_masked,
    crc32c_slicing8, decode_record,
};

#[repr(C, align(64))]
struct AlignedBuf<const N: usize>([u8; N]);

fn crc32c_bit_by_bit_baseline(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f6_3b78 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

fn fill_deterministic(bytes: &mut [u8], mut seed: u64) {
    for b in bytes {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        *b = (seed & 0xff) as u8;
    }
}

fn measure_ps<F: FnMut()>(name: &str, batch: u64, samples: usize, mut op: F) {
    let bench_mode = std::env::args().any(|a| a == "--bench");
    let (warmup, batch, samples) = if bench_mode {
        (64, batch, samples)
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
        "{name:<48} P50={:>5}.{:03} ns  P99={:>5}.{:03} ns  P99.9={:>5}.{:03} ns",
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

fn sample_market_record() -> AlignedBuf<512> {
    let mut record = MarketRecord {
        header: RecordHeader {
            sequence: 42,
            message_type: 259,
            schema_major: 1,
            header_bytes: 128,
            flags: if cfg!(feature = "crc32") { 6 | 8 } else { 6 },
            payload_kind: 1,
            body_bytes: 384,
            payload_length: 64,
            topic_or_scope_id: 7,
            feed_generation: 1,
            producer_generation: 1,
            instrument_id: 101,
            instrument_version: 1,
            ingress_mono_ns: 1_000_000,
            publish_mono_ns: 1_000_010,
            ..Default::default()
        },
        body: MarketBody {
            venue_seq_first: 10,
            venue_seq_last: 11,
            venue_seq_previous: 9,
            book_revision: 5,
            level_count: 4,
            bid_count: 2,
            ask_count: 2,
            depth_flags: 1,
            ..Default::default()
        },
    };
    record.body.inline_levels[0] = DepthLevel {
        price_ticks: 100_000,
        qty_lots: 15,
    };
    record.body.inline_levels[1] = DepthLevel {
        price_ticks: 99_990,
        qty_lots: 20,
    };
    record.body.inline_levels[2] = DepthLevel {
        price_ticks: 100_010,
        qty_lots: 12,
    };
    record.body.inline_levels[3] = DepthLevel {
        price_ticks: 100_020,
        qty_lots: 30,
    };
    let mut buf = AlignedBuf([0u8; 512]);
    record.encode(&mut buf.0).unwrap();
    if cfg!(feature = "crc32") {
        record.header.payload_crc32c = crc32c(&buf.0[320..384]);
        record.encode(&mut buf.0).unwrap();
        record.header.record_crc32c = crc32c_masked(&buf.0, 112..120);
        record.encode(&mut buf.0).unwrap();
    }
    buf
}

fn main() {
    print_env();
    let mut b128 = AlignedBuf([0u8; 128]);
    let mut b512 = AlignedBuf([0u8; 512]);
    let mut b4096 = AlignedBuf([0u8; 4096]);
    fill_deterministic(&mut b128.0, 1);
    fill_deterministic(&mut b512.0, 2);
    fill_deterministic(&mut b4096.0, 3);

    assert_eq!(
        crc32c_bit_by_bit_baseline(&b4096.0),
        crc32c_slicing8(&b4096.0)
    );
    assert_eq!(crc32c(&b4096.0), crc32c_slicing8(&b4096.0));

    measure_ps("crc32c/128B/bit_by_bit_baseline", 256, 500, || {
        black_box(crc32c_bit_by_bit_baseline(black_box(&b128.0)));
    });
    measure_ps("crc32c/128B/slicing8", 1024, 500, || {
        black_box(crc32c_slicing8(black_box(&b128.0)));
    });
    measure_ps("crc32c/128B/hw_auto", 2048, 500, || {
        black_box(crc32c(black_box(&b128.0)));
    });

    measure_ps("crc32c/512B/bit_by_bit_baseline", 64, 500, || {
        black_box(crc32c_bit_by_bit_baseline(black_box(&b512.0)));
    });
    measure_ps("crc32c/512B/slicing8", 512, 500, || {
        black_box(crc32c_slicing8(black_box(&b512.0)));
    });
    measure_ps("crc32c/512B/hw_auto", 2048, 500, || {
        black_box(crc32c(black_box(&b512.0)));
    });
    measure_ps("crc32c/512B/hw_masked_112..120", 2048, 500, || {
        black_box(crc32c_masked(black_box(&b512.0), 112..120));
    });

    measure_ps("crc32c/4096B/bit_by_bit_baseline", 16, 500, || {
        black_box(crc32c_bit_by_bit_baseline(black_box(&b4096.0)));
    });
    measure_ps("crc32c/4096B/slicing8", 128, 500, || {
        black_box(crc32c_slicing8(black_box(&b4096.0)));
    });
    measure_ps("crc32c/4096B/hw_auto", 512, 500, || {
        black_box(crc32c(black_box(&b4096.0)));
    });

    let slot = sample_market_record();
    measure_ps("market_record/512B/stack_decode_record", 1024, 500, || {
        let rec = decode_record(black_box(&slot.0)).unwrap();
        black_box(&rec);
    });
    measure_ps(
        "market_record/512B/zerocopy_from_aligned_bytes",
        2048,
        500,
        || {
            let rec = MarketRecord::from_aligned_bytes(black_box(&slot.0)).unwrap();
            black_box(rec.body.inline_levels[0].price_ticks);
        },
    );
}
