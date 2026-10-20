// Independent review oracle, retained as a deterministic regression.
use std::collections::BTreeMap;
use tickwire_book::*;
const KEY: BookKey = BookKey {
    venue: 1,
    instrument: 1,
    source: 1,
};
fn next(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}
fn levels(m: &BTreeMap<i64, i64>, bid: bool) -> Vec<Level> {
    let mut v: Vec<_> = m
        .iter()
        .map(|(&p, &q)| Level {
            price: PriceTicks(p),
            quantity: QtyLots(q),
        })
        .collect();
    if bid {
        v.reverse();
    }
    v
}
#[test]
fn independent_small_books_match_btree_map() {
    let mut seed = 0xded0beef12345678;
    let (mut ok, mut rejected) = (0, 0);
    for _ in 0..40000 {
        let mut old: [BTreeMap<i64, i64>; 2] = Default::default();
        let mut updates: [Vec<Level>; 2] = Default::default();
        for side in 0..2 {
            let base = if side == 0 { 0 } else { 100 };
            for p in 1..=8 {
                if next(&mut seed).is_multiple_of(3) && old[side].len() < 6 {
                    old[side].insert(base + p, 1 + (next(&mut seed) % 3) as i64);
                }
            }
            let mut changes = BTreeMap::new();
            for _ in 0..next(&mut seed) % 9 {
                changes.insert(
                    base + 1 + (next(&mut seed) % 12) as i64,
                    (next(&mut seed) % 4) as i64,
                );
            }
            updates[side] = levels(&changes, false);
            let len = updates[side].len();
            for i in 0..len {
                let j = next(&mut seed) as usize % len;
                updates[side].swap(i, j);
            }
        }
        let bids = levels(&old[0], true);
        let asks = levels(&old[1], false);
        let mut b = Book::<BinanceSpot, 6, 16>::new(KEY, 1);
        b.install_snapshot(Snapshot {
            key: KEY,
            generation: 1,
            last_update_id: 1,
            bids: &bids,
            asks: &asks,
            bids_complete: false,
            asks_complete: false,
        })
        .unwrap();
        let mut expected = old.clone();
        for side in 0..2 {
            for u in &updates[side] {
                if u.quantity.0 == 0 {
                    expected[side].remove(&u.price.0);
                } else {
                    expected[side].insert(u.price.0, u.quantity.0);
                }
            }
        }
        let result = b.apply(DepthEvent {
            key: KEY,
            generation: 1,
            first_update_id: 2,
            last_update_id: 2,
            previous_update_id: None,
            received_at_ns: 2,
            bids: &updates[0],
            asks: &updates[1],
        });
        if expected[0].len() > 6 || expected[1].len() > 6 {
            assert!(matches!(result, Err(BookError::Capacity { .. })));
            assert_eq!(b.diagnostic_levels(Side::Bid), bids);
            assert_eq!(b.diagnostic_levels(Side::Ask), asks);
            assert_eq!(b.last_update_id(), Some(1));
            rejected += 1;
        } else {
            result.unwrap();
            let v = b.view(2, 0).unwrap();
            assert_eq!(v.known_bids(), levels(&expected[0], true));
            assert_eq!(v.known_asks(), levels(&expected[1], false));
            for (side, initial) in [(Side::Bid, &bids), (Side::Ask, &asks)] {
                let c = initial
                    .last()
                    .map_or(SideCoverage::Unknown, |l| SideCoverage::Through(l.price));
                assert_eq!(v.coverage(side), c);
            }
            ok += 1;
        }
    }
    println!("40000 differential batches: {ok} applied, {rejected} atomic capacity rejections");
}
