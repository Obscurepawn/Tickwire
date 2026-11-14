//! WAL v1 frame encode/decode and Castagnoli CRC-32C latency benchmark (`harness = false`).
#![forbid(unsafe_code)]

use sha2::{Digest, Sha256};
use std::fs;
use std::hint::black_box;
use std::time::Instant;
use tickwire_journal::crc32c::{crc32c, crc32c_header_masked, crc32c_slicing8};
use tickwire_journal::frame::{
    self, END_MAGIC, FrameType, HEADER_BYTES, Header, MAGIC, TRAILER_BYTES,
};
use tickwire_journal::store::InMemoryStore;
use tickwire_journal::{Config, Journal};

/// Legacy bit-by-bit CRC-32C baseline retained exclusively in this benchmark for comparison.
#[inline(never)]
fn crc32c_bitwise_baseline(input: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in input {
        crc ^= u32::from(byte);
        let mut step = 0u32;
        while step < 8 {
            crc = (crc >> 1) ^ (0x82f6_3b78 & 0u32.wrapping_sub(crc & 1));
            step += 1;
        }
    }
    !crc
}

/// Legacy WAL frame encoder using `crc32c_bitwise_baseline` for apples-to-apples comparison.
#[inline(never)]
fn encode_legacy_bitwise(header: &Header, payload: &[u8]) -> Vec<u8> {
    let total = (HEADER_BYTES + payload.len() + TRAILER_BYTES + 7) & !7;
    let tail = total - TRAILER_BYTES;
    let mut b = vec![0u8; total];
    b[..8].copy_from_slice(MAGIC);
    b[8..10].copy_from_slice(&1u16.to_le_bytes());
    b[10..12].copy_from_slice(&(HEADER_BYTES as u16).to_le_bytes());
    b[12..16].copy_from_slice(&(total as u32).to_le_bytes());
    b[16..18].copy_from_slice(&(header.frame_type as u16).to_le_bytes());
    b[18..20].copy_from_slice(&header.flags.to_le_bytes());
    b[20..24].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    b[24..40].copy_from_slice(&header.stream_epoch);
    b[40..56].copy_from_slice(&header.domain);
    for (n, value) in [
        (56, header.segment_id),
        (64, header.lsn),
        (72, header.record_seq),
        (80, header.exec_epoch),
        (88, header.batch_number),
    ] {
        b[n..n + 8].copy_from_slice(&value.to_le_bytes());
    }
    b[96..128].copy_from_slice(&header.previous_hash);
    b[132..136].copy_from_slice(&crc32c_bitwise_baseline(payload).to_le_bytes());
    let header_crc = crc32c_bitwise_baseline(&b[..HEADER_BYTES]);
    b[128..132].copy_from_slice(&header_crc.to_le_bytes());
    b[HEADER_BYTES..HEADER_BYTES + payload.len()].copy_from_slice(payload);
    b[tail + 32..tail + 36].copy_from_slice(&(total as u32).to_le_bytes());
    b[tail + 36..].copy_from_slice(END_MAGIC);
    let mut hash = Sha256::new();
    hash.update(&b[..tail]);
    hash.update(&b[tail + 32..]);
    b[tail..tail + 32].copy_from_slice(&hash.finalize());
    b
}

/// Legacy WAL frame decoder using `crc32c_bitwise_baseline` + 136-byte stack copy.
#[inline(never)]
fn decode_legacy_bitwise(input: &[u8]) -> usize {
    let b = &input[..HEADER_BYTES];
    let total = u32::from_le_bytes(b[12..16].try_into().unwrap()) as usize;
    let payload_len = u32::from_le_bytes(b[20..24].try_into().unwrap()) as usize;
    let mut crc_header = [0u8; HEADER_BYTES];
    crc_header.copy_from_slice(b);
    crc_header[128..132].fill(0);
    assert_eq!(
        crc32c_bitwise_baseline(&crc_header),
        u32::from_le_bytes(b[128..132].try_into().unwrap())
    );
    let tail = total - TRAILER_BYTES;
    let payload = &input[HEADER_BYTES..HEADER_BYTES + payload_len];
    assert_eq!(
        crc32c_bitwise_baseline(payload),
        u32::from_le_bytes(b[132..136].try_into().unwrap())
    );
    let mut hash = Sha256::new();
    hash.update(&input[..tail]);
    hash.update(&input[tail + 32..total]);
    let digest: [u8; 32] = hash.finalize().into();
    assert_eq!(&input[tail..tail + 32], &digest);
    total
}

fn cpu_model() -> String {
    if let Ok(cpuinfo) = fs::read_to_string("/proc/cpuinfo") {
        for line in cpuinfo.lines() {
            if let Some((_, val)) = line.split_once(':')
                && (line.starts_with("model name") || line.starts_with("Model"))
            {
                return val.trim().to_owned();
            }
        }
    }
    std::env::consts::ARCH.to_owned()
}

fn os_release() -> String {
    fs::read_to_string("/proc/sys/kernel/osrelease")
        .map(|s| format!("{} {}", std::env::consts::OS, s.trim()))
        .unwrap_or_else(|_| std::env::consts::OS.to_owned())
}

fn load_avg() -> String {
    fs::read_to_string("/proc/loadavg")
        .map(|s| s.split_whitespace().take(3).collect::<Vec<_>>().join(" "))
        .unwrap_or_else(|_| "unknown".to_owned())
}

fn hw_accel_label() -> &'static str {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("sse4.2") {
            return "x86_64-sse4.2";
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("crc") {
            return "aarch64-crc";
        }
    }
    "software-slicing8"
}

fn measure_ns(
    warmup: usize,
    samples: usize,
    ops_per_sample: usize,
    mut op: impl FnMut(),
) -> (f64, f64, f64) {
    // Keep `cargo test --all-targets` debug smoke runs sub-second while running
    // full sample counts under `cargo bench` / `--release`.
    let (warmup, samples, ops_per_sample) = if cfg!(debug_assertions) {
        (2, 8, 1)
    } else {
        (warmup, samples, ops_per_sample)
    };
    for _ in 0..warmup {
        for _ in 0..ops_per_sample {
            op();
        }
    }
    let mut latencies_ns = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        for _ in 0..ops_per_sample {
            op();
        }
        latencies_ns.push(start.elapsed().as_nanos() as f64 / ops_per_sample as f64);
    }
    latencies_ns.sort_by(|a, b| a.total_cmp(b));
    let pct = |permille: usize| -> f64 {
        let idx = ((latencies_ns.len() * permille).div_ceil(1000)).saturating_sub(1);
        latencies_ns[idx.min(latencies_ns.len() - 1)]
    };
    (pct(500), pct(990), pct(999))
}

fn print_row(case: &str, bytes: usize, p50: f64, p99: f64, p999: f64) {
    println!(
        "{case:<42} | {bytes:>6} B | p50={p50:>8.2} ns | p99={p99:>8.2} ns | p99.9={p999:>9.2} ns"
    );
}

fn sample_buffer(len: usize) -> Vec<u8> {
    let mut out = vec![0u8; len];
    let mut s = 0x243f_6a88_85a3_08d3u64;
    for b in &mut out {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        *b = (s & 0xff) as u8;
    }
    out
}

fn main() {
    let feature_mode = if cfg!(feature = "crc32") {
        "crc32=ON"
    } else {
        "crc32=OFF (default)"
    };
    println!(
        "# tickwire-journal frame & CRC-32C benchmark\n# cpu={} | os={} | arch={} | hw={} | feature={} | loadavg={}",
        cpu_model(),
        os_release(),
        std::env::consts::ARCH,
        hw_accel_label(),
        feature_mode,
        load_avg(),
    );
    println!("{}", "-".repeat(104));

    // Verify exact agreement before timing.
    for size in [136usize, 256, 1024, 4096] {
        let data = sample_buffer(size);
        let ref_crc = crc32c_bitwise_baseline(&data);
        assert_eq!(crc32c_slicing8(&data), ref_crc);
        assert_eq!(crc32c(&data), ref_crc);
    }

    // 1. Pure CRC-32C microbenchmarks (64 ops/sample to amortize clock_gettime vDSO overhead).
    for size in [136usize, 256, 1024, 4096] {
        let data = sample_buffer(size);
        let (p50, p99, p999) = measure_ns(200, 4000, 32, || {
            black_box(crc32c_bitwise_baseline(black_box(data.as_slice())));
        });
        print_row("crc32c_bitwise_baseline", size, p50, p99, p999);

        let (p50, p99, p999) = measure_ns(200, 4000, 64, || {
            black_box(crc32c_slicing8(black_box(data.as_slice())));
        });
        print_row("crc32c_slicing8 (const 8KiB table)", size, p50, p99, p999);

        let (p50, p99, p999) = measure_ns(200, 4000, 64, || {
            black_box(crc32c(black_box(data.as_slice())));
        });
        print_row("crc32c_hw (SSE4.2 / ARMv8)", size, p50, p99, p999);
    }

    let hdr_buf: [u8; HEADER_BYTES] = sample_buffer(HEADER_BYTES).try_into().unwrap();
    let (p50, p99, p999) = measure_ns(200, 4000, 64, || {
        black_box(crc32c_header_masked(black_box(&hdr_buf)));
    });
    print_row(
        "crc32c_header_masked (zero-alloc 136B)",
        HEADER_BYTES,
        p50,
        p99,
        p999,
    );
    println!("{}", "-".repeat(104));

    // 2. WAL v1 Frame encode & decode (256B and 1024B payloads).
    let hdr = Header {
        frame_type: FrameType::DataRecord,
        flags: 0,
        stream_epoch: [0x11; 16],
        domain: [0x22; 16],
        segment_id: 1,
        lsn: 0,
        record_seq: 1,
        exec_epoch: 1,
        batch_number: 1,
        previous_hash: [0x33; 32],
    };

    for size in [256usize, 1024] {
        let payload = sample_buffer(size);
        let legacy_encoded = encode_legacy_bitwise(&hdr, &payload);
        let active_encoded = frame::encode(&hdr, &payload).unwrap();

        let (p50, p99, p999) = measure_ns(200, 4000, 8, || {
            black_box(encode_legacy_bitwise(
                black_box(&hdr),
                black_box(payload.as_slice()),
            ));
        });
        print_row("frame::encode (legacy bitwise CRC)", size, p50, p99, p999);

        let encode_label = if cfg!(feature = "crc32") {
            "frame::encode (feature=crc32 HW)"
        } else {
            "frame::encode (default crc32=OFF)"
        };
        let (p50, p99, p999) = measure_ns(200, 4000, 8, || {
            black_box(frame::encode(black_box(&hdr), black_box(payload.as_slice())).unwrap());
        });
        print_row(encode_label, size, p50, p99, p999);

        let (p50, p99, p999) = measure_ns(200, 4000, 8, || {
            black_box(decode_legacy_bitwise(black_box(legacy_encoded.as_slice())));
        });
        print_row("frame::decode (legacy bitwise CRC)", size, p50, p99, p999);

        let decode_label = if cfg!(feature = "crc32") {
            "frame::decode (feature=crc32 HW)"
        } else {
            "frame::decode (default crc32=OFF)"
        };
        let (p50, p99, p999) = measure_ns(200, 4000, 8, || {
            black_box(frame::decode(black_box(active_encoded.as_slice())).unwrap());
        });
        print_row(decode_label, size, p50, p99, p999);
    }
    println!("{}", "-".repeat(104));

    // 3. Full Journal<InMemoryStore>::append_batch (256B payload -> BatchBegin + DataRecord + BatchSeal).
    let payload_256 = sample_buffer(256);
    let cfg = Config {
        stream_epoch: [0x44; 16],
        domain: [0x55; 16],
        segment_max_bytes: 256 * 1024 * 1024,
    };
    let mut journal = Journal::create(InMemoryStore::new(), cfg).unwrap();
    let batch_label = if cfg!(feature = "crc32") {
        "Journal<InMemory>::append_batch (crc32=ON)"
    } else {
        "Journal<InMemory>::append_batch (crc32=OFF)"
    };
    let (p50, p99, p999) = measure_ns(200, 4000, 4, || {
        let slice = black_box(payload_256.as_slice());
        black_box(journal.append_batch(1, &[slice]).unwrap());
    });
    print_row(batch_label, 256, p50, p99, p999);
}
