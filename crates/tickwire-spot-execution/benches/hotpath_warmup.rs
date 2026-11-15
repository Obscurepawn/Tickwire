//! Microbenchmark comparing cold-cache vs. `UtaHotPathWarmer`-warmed UTA `REQ` + TLS + `RESP`
//! latency across Bybit V5 and OKX V5 with zero disk I/O and zero runtime heap allocation.

#![forbid(unsafe_code)]

use std::{hint::black_box, time::Instant};
use tickwire_spot_execution::{SpotVenue, UtaHotPathWarmer, UtaWarmupConfig};
use tickwire_venue::Environment;

struct CacheEvictor {
    llc_buf: Vec<u64>,
    seed: u64,
}

impl CacheEvictor {
    fn new() -> Self {
        let mut llc_buf = vec![0u64; 4 * 1024 * 1024];
        for (i, slot) in llc_buf.iter_mut().enumerate() {
            *slot = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        }
        Self { llc_buf, seed: 1 }
    }

    #[inline(never)]
    fn thrash(&mut self) -> u64 {
        self.seed = self.seed.wrapping_add(1);
        let mut acc = self.seed;
        let mut idx = 0usize;
        while idx < self.llc_buf.len() {
            let v = self.llc_buf[idx].wrapping_add(acc);
            self.llc_buf[idx] = v;
            acc ^= v.rotate_left(13);
            idx += 8;
        }
        black_box(acc)
    }
}

fn percentile_ns(sorted: &[u64], num: usize, den: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) * num) / den;
    sorted[idx]
}

fn bench_venue(venue: SpotVenue, evictor: &mut CacheEvictor) {
    let mut warmer = UtaHotPathWarmer::new(UtaWarmupConfig::new(venue, Environment::Test)).unwrap();
    let samples = 64usize;
    let mut cold_ns = Vec::with_capacity(samples);
    let mut warm_ns = Vec::with_capacity(samples);
    let mut mono = 1_000_000_000u64;
    let utc_ms = 1_700_000_000_123u64;

    for _ in 0..samples {
        evictor.thrash();
        mono += 1_000_000;
        let t0 = Instant::now();
        let r_cold = black_box(warmer.warm_once(mono, utc_ms).unwrap());
        cold_ns.push(t0.elapsed().as_nanos() as u64);
        assert_eq!(r_cold.http_status, 200);

        mono += 1_000_000;
        let t1 = Instant::now();
        let r_warm = black_box(warmer.warm_once(mono, utc_ms).unwrap());
        warm_ns.push(t1.elapsed().as_nanos() as u64);
        assert_eq!(r_warm.http_status, 200);
    }

    cold_ns.sort_unstable();
    warm_ns.sort_unstable();
    println!(
        "venue={} samples={} cold_p50_ns={} cold_p99_ns={} warm_p50_ns={} warm_p99_ns={}",
        venue.as_str(),
        samples,
        percentile_ns(&cold_ns, 50, 100),
        percentile_ns(&cold_ns, 99, 100),
        percentile_ns(&warm_ns, 50, 100),
        percentile_ns(&warm_ns, 99, 100),
    );
}

fn main() {
    let mut evictor = CacheEvictor::new();
    bench_venue(SpotVenue::Bybit, &mut evictor);
    bench_venue(SpotVenue::Okx, &mut evictor);
}
