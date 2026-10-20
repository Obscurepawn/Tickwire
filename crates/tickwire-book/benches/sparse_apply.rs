//! Per-event measurements; deterministic synthetic levels, no feed, TLS, IPC or disk.
use std::{hint::black_box, time::Instant};
use tickwire_book::*;

fn percentile(sorted: &[u64], p: usize) -> u64 {
    sorted[(sorted.len() * p).div_ceil(1000).saturating_sub(1)]
}
fn summarize(name: &str, depth: usize, updates: usize, mut samples: Vec<u64>) {
    samples.sort_unstable();
    println!(
        "{name},{depth},{updates},{},{},{},{},{}",
        samples.len(),
        percentile(&samples, 500),
        percentile(&samples, 990),
        percentile(&samples, 999),
        samples[samples.len() - 1]
    );
}
fn run<const N: usize>(updates: usize, churn: bool, order: &str, count: usize) {
    let key = BookKey {
        venue: 1,
        instrument: 1,
        source: 1,
    };
    let mut book = Book::<BinanceSpot, N, 1024>::new(key, 1);
    let level = |p, q| Level {
        price: PriceTicks(p),
        quantity: QtyLots(q),
    };
    let bids: Vec<_> = (0..N)
        .map(|i| level(1_000_000 - 2 * i as i64, 100))
        .collect();
    let asks: Vec<_> = (0..N)
        .map(|i| level(1_000_002 + 2 * i as i64, 100))
        .collect();
    book.install_snapshot(Snapshot {
        key,
        generation: 1,
        last_update_id: 1,
        bids: &bids,
        asks: &asks,
        bids_complete: true,
        asks_complete: true,
    })
    .unwrap();
    let mut samples = Vec::with_capacity(count);
    let mut rng = 0xabcdef123456789u64;
    let mut bid_updates = [level(0, 0); 512];
    let mut ask_updates = [level(0, 0); 512];
    for i in 0..(count + 10_000) {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        if churn {
            // Replace adjacent ticks for each touched rank, holding occupancy fixed.
            let from = (i % 2) as i64;
            let to = 1 - from;
            for j in 0..updates / 2 {
                let rank = ((j * 73) % N) as i64;
                bid_updates[2 * j] = level(1_000_000 - 2 * rank + from, 0);
                bid_updates[2 * j + 1] = level(1_000_000 - 2 * rank + to, 100);
                ask_updates[2 * j] = level(1_000_002 + 2 * rank + from, 0);
                ask_updates[2 * j + 1] = level(1_000_002 + 2 * rank + to, 100);
            }
        } else {
            // Distinct scattered ranks: odd stride 73 permutes each power-of-two depth.
            for j in 0..updates {
                let rank = (rng as usize + j * 73) % N;
                bid_updates[j] = level(1_000_000 - 2 * rank as i64, 1 + (i % 100) as i64);
                ask_updates[j] = level(1_000_002 + 2 * rank as i64, 1 + (i % 100) as i64);
            }
        }
        match order {
            "reverse" => {
                bid_updates[..updates].reverse();
                ask_updates[..updates].reverse();
            }
            "shuffle" => {
                let mut permutation = rng;
                for j in (1..updates).rev() {
                    permutation ^= permutation << 13;
                    permutation ^= permutation >> 7;
                    permutation ^= permutation << 17;
                    let k = permutation as usize % (j + 1);
                    bid_updates.swap(j, k);
                    ask_updates.swap(j, k);
                }
            }
            _ => {}
        }
        let sequence = (i + 2) as u64;
        let event = DepthEvent {
            key,
            generation: 1,
            first_update_id: sequence,
            last_update_id: sequence,
            previous_update_id: None,
            received_at_ns: sequence,
            bids: &bid_updates[..updates],
            asks: &ask_updates[..updates],
        };
        let start = Instant::now();
        let result = book.apply(black_box(event));
        let view = book.view(sequence, 0);
        if let Ok(ref view) = view {
            black_box((view.best_bid(), view.best_ask()));
        }
        let elapsed = start.elapsed().as_nanos() as u64;
        result.unwrap();
        view.unwrap();
        if i >= 10_000 {
            samples.push(elapsed);
        }
    }
    summarize(
        &format!(
            "{}_scatter_{}",
            if churn { "churn" } else { "replace" },
            order
        ),
        N,
        updates * 2,
        samples,
    );
}
fn main() {
    let count = std::env::var("TICKWIRE_BENCH_SAMPLES")
        .ok()
        .map(|n| n.parse::<usize>().expect("positive sample count"))
        .unwrap_or_else(|| {
            if cfg!(debug_assertions) || std::env::args().any(|arg| arg == "--test") {
                1_000
            } else {
                200_000
            }
        });
    assert!(count > 0);
    println!("workload,levels_per_side,updates_total,samples,p50_ns,p99_ns,p999_ns,max_ns");
    let timer: Vec<_> = (0..count)
        .map(|_| {
            let t = Instant::now();
            black_box(());
            t.elapsed().as_nanos() as u64
        })
        .collect();
    summarize("timer", 0, 0, timer);
    run::<512>(8, false, "shuffle", count);
    run::<512>(32, false, "shuffle", count);
    run::<512>(256, false, "shuffle", count);
    run::<512>(128, true, "shuffle", count);
    run::<4096>(8, false, "shuffle", count);
    run::<4096>(32, false, "shuffle", count);
    run::<4096>(256, false, "shuffle", count);
    run::<4096>(128, true, "shuffle", count);
}
