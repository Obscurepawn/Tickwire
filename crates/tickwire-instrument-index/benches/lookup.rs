//! Synthetic single-owner index measurements. No feed, IPC, risk or network work.
use std::{
    collections::{BTreeMap, HashMap},
    hint::black_box,
    time::Instant,
};
use tickwire_instrument_index::{AppendIndex, FixedIndex};

const QUERIES: usize = 4096;

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58476d1ce4e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

fn report(
    method: &str,
    dataset: &str,
    count: usize,
    operation: &str,
    operations_per_sample: usize,
    mut times: Vec<u128>,
) {
    times.sort_unstable();
    let percentile = |per_mille: usize| {
        times[(times.len() * per_mille).div_ceil(1000).saturating_sub(1)] as f64
            / operations_per_sample as f64
    };
    println!(
        "{method},{dataset},{count},{operation},{},{operations_per_sample},{:.3},{:.3},{:.3}",
        times.len(),
        percentile(500),
        percentile(990),
        percentile(1000),
    );
}

fn lookup(
    method: &str,
    dataset: &str,
    count: usize,
    operation: &str,
    queries: &[u64],
    samples: usize,
    mut get: impl FnMut(u64) -> Option<usize>,
) {
    for &query in queries {
        black_box(get(black_box(query)));
    }
    let mut times = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        for &query in black_box(queries) {
            black_box(get(black_box(query)));
        }
        times.push(start.elapsed().as_nanos());
    }
    report(method, dataset, count, operation, queries.len(), times);
}

fn insert<T>(
    method: &str,
    dataset: &str,
    keys: &[u64],
    samples: usize,
    mut create: impl FnMut() -> T,
    mut add: impl FnMut(&mut T, u64, usize) -> usize,
) {
    let mut times = Vec::with_capacity(samples);
    for _ in 0..samples {
        // Index/table/vector reservation is deliberately outside the interval.
        let mut index = create();
        let start = Instant::now();
        for (dense, &key) in black_box(keys).iter().enumerate() {
            black_box(add(black_box(&mut index), black_box(key), dense));
        }
        times.push(start.elapsed().as_nanos());
        black_box(&index);
    }
    report(method, dataset, keys.len(), "insert", keys.len(), times);
}

fn run(dataset: &str, count: usize, samples: usize) {
    let keys: Vec<_> = (0..count as u64)
        .map(|i| match dataset {
            "clustered" => (i << 17) | 0x5000_0000_0000_0042,
            "scattered" => mix(i.wrapping_add(0x123456789abcdef0)),
            _ => unreachable!(),
        })
        .collect();
    let fixed = FixedIndex::new(&keys).unwrap();
    let mut append = AppendIndex::with_capacity(count).unwrap();
    for &key in &keys {
        append.insert(key).unwrap();
    }
    for (method, memory) in [
        ("fixed", fixed.memory_stats()),
        ("append", append.memory_stats()),
    ] {
        println!(
            "# memory,{method},{dataset},{count},key_bytes={},slot_bytes={},heap_bytes={}",
            memory.key_bytes,
            memory.slot_bytes,
            memory.total_bytes()
        );
    }
    let tree: BTreeMap<_, _> = keys.iter().copied().zip(0..).collect();
    let map: HashMap<_, _> = keys.iter().copied().zip(0..).collect();
    let mut sorted: Vec<_> = keys.iter().copied().zip(0..).collect();
    sorted.sort_unstable_by_key(|pair| pair.0);
    for (operation, missing) in [("hit", false), ("miss", true)] {
        let queries: Vec<_> = (0..QUERIES)
            .map(|i| {
                let key = keys[(i.wrapping_mul(4051) + 17) % count];
                if !missing {
                    return key;
                }
                let mut unknown = key ^ (1 << 50);
                while tree.contains_key(&unknown) {
                    unknown = unknown.wrapping_add(1);
                }
                unknown
            })
            .collect();
        for &key in &queries {
            let want = tree.get(&key).copied();
            assert_eq!(fixed.lookup(key).map(|id| id.index()), want);
            assert_eq!(append.lookup(key).map(|id| id.index()), want);
        }
        lookup(
            "fixed",
            dataset,
            count,
            operation,
            &queries,
            samples,
            |key| black_box(&fixed).lookup(key).map(|id| id.index()),
        );
        lookup(
            "append",
            dataset,
            count,
            operation,
            &queries,
            samples,
            |key| black_box(&append).lookup(key).map(|id| id.index()),
        );
        lookup(
            "btree",
            dataset,
            count,
            operation,
            &queries,
            samples,
            |key| black_box(&tree).get(&key).copied(),
        );
        lookup(
            "hash",
            dataset,
            count,
            operation,
            &queries,
            samples,
            |key| black_box(&map).get(&key).copied(),
        );
        lookup(
            "binary_search",
            dataset,
            count,
            operation,
            &queries,
            samples,
            |key| {
                black_box(&sorted)
                    .binary_search_by_key(&key, |pair| pair.0)
                    .ok()
                    .map(|i| sorted[i].1)
            },
        );
    }
    insert(
        "append",
        dataset,
        &keys,
        samples,
        || AppendIndex::with_capacity(count).unwrap(),
        |index, key, _| index.insert(key).unwrap().index(),
    );
    insert(
        "btree",
        dataset,
        &keys,
        samples,
        BTreeMap::new,
        |index, key, dense| {
            assert_eq!(index.insert(key, dense), None);
            dense
        },
    );
    insert(
        "hash",
        dataset,
        &keys,
        samples,
        || HashMap::with_capacity(count),
        |index, key, dense| {
            assert_eq!(index.insert(key, dense), None);
            dense
        },
    );
    insert(
        "binary_search",
        dataset,
        &keys,
        samples,
        || Vec::with_capacity(count),
        |index: &mut Vec<(u64, usize)>, key, dense| {
            let slot = index.binary_search_by_key(&key, |pair| pair.0).unwrap_err();
            index.insert(slot, (key, dense));
            dense
        },
    );
}

fn main() {
    let samples = std::env::var("TICKWIRE_BENCH_SAMPLES")
        .map(|value| value.parse::<usize>().expect("positive sample count"))
        .unwrap_or(200);
    assert!((1..=100_000).contains(&samples));
    println!(
        "method,dataset,keys,operation,samples,operations_per_sample,p50_ns_per_op,p99_ns_per_op,max_ns_per_op"
    );
    for dataset in ["clustered", "scattered"] {
        for count in [32, 256, 4096] {
            run(dataset, count, samples);
        }
    }
}
