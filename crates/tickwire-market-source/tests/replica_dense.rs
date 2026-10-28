#![forbid(unsafe_code)]

use sha2::{Digest, Sha256};
use tickwire_ipc::{
    DepthCoverage, DepthLevel, MarketBody, MarketExactBody, MarketExactRecord, MarketRecord,
    MarketStateRecord, RecordHeader, Wire,
};
use tickwire_market_source::replica::MarketReplica;

fn level(price_ticks: i64, qty_lots: i64) -> DepthLevel {
    DepthLevel {
        price_ticks,
        qty_lots,
    }
}
fn header(id: u64, sequence: u64, message_type: u16) -> RecordHeader {
    RecordHeader {
        sequence,
        message_type,
        schema_major: 1,
        header_bytes: 128,
        body_bytes: 384,
        flags: 6,
        topic_or_scope_id: 1,
        feed_generation: 2,
        producer_generation: 1,
        instrument_id: id,
        instrument_version: 13,
        ingress_mono_ns: 49,
        publish_mono_ns: 50,
        ..Default::default()
    }
}
// Independent encoder of the existing TWMR0001 layout. No implementation-owned
// dense index, workspace or capacity allocation is part of the durable bytes.
fn legacy_checkpoint(ids: &[u64], capacity: usize) -> Vec<u8> {
    let mut out = b"TWMR0001".to_vec();
    for n in [1, 2, 3, 5, capacity as u64] {
        out.extend(n.to_le_bytes());
    }
    out.extend((ids.len() as u32).to_le_bytes());
    out.extend([0; 4]);
    for id in ids {
        for n in [*id, 13] {
            out.extend(n.to_le_bytes());
        }
        out.extend(2u16.to_le_bytes());
        out.extend([0; 6]);
        for n in [49u64, 50, 5] {
            out.extend(n.to_le_bytes());
        }
        let mut coverage = [0; 32];
        DepthCoverage {
            version: 1,
            bid_kind: 2,
            ask_kind: 2,
            bid_boundary_ticks: 99,
            ask_boundary_ticks: 103,
            source_book_generation: 4,
        }
        .encode(&mut coverage)
        .unwrap();
        out.extend(coverage);
        for n in [2u32, 2, 1, 0] {
            out.extend(n.to_le_bytes());
        }
        let mut state = [0; 512];
        MarketRecord {
            header: header(*id, 5, 261),
            body: MarketBody {
                feed_state: 2,
                ..Default::default()
            },
        }
        .encode(&mut state)
        .unwrap();
        out.extend(state);
        for value in [level(100, 2), level(99, 4), level(102, 3), level(103, 5)] {
            let mut raw = [0; 16];
            value.encode(&mut raw).unwrap();
            out.extend(raw);
        }
    }
    out.extend(Sha256::digest(&out));
    out
}
fn delta(id: u64, seq: u64, bids: &[DepthLevel], asks: &[DepthLevel]) -> ([u8; 512], Vec<u8>) {
    let mut payload = Vec::new();
    for value in bids.iter().chain(asks) {
        let mut bytes = [0; 16];
        value.encode(&mut bytes).unwrap();
        payload.extend(bytes);
    }
    let mut h = header(id, seq, 259);
    h.payload_kind = 1;
    h.payload_length = payload.len() as u32;
    let value = MarketRecord {
        header: h,
        body: MarketBody {
            bid_count: bids.len() as u32,
            ask_count: asks.len() as u32,
            depth_flags: 1,
            level_count: (bids.len() + asks.len()) as u32,
            feed_state: 2,
            ..Default::default()
        },
    };
    let mut bytes = [0; 512];
    value.encode(&mut bytes).unwrap();
    bytes[320..320 + payload.len()].copy_from_slice(&payload);
    (bytes, payload)
}

#[test]
fn sparse_high_bit_instruments_preserve_legacy_checkpoint_bytes() {
    let ids = [7, 1 << 48, u64::MAX];
    let bytes = legacy_checkpoint(&ids, 2);
    let mut replica = MarketReplica::restore_checkpoint(&bytes, 2).unwrap();
    assert_eq!(replica.instruments().collect::<Vec<_>>(), ids);
    assert_eq!(replica.checkpoint().unwrap(), bytes);
    let (raw, payload) = delta(ids[2], 6, &[level(100, 8)], &[]);
    assert_eq!(replica.apply(&raw, &payload).unwrap(), ids[2]);
    assert_eq!(replica.view(ids[2]).unwrap().bids()[0].qty_lots, 8);
    assert_eq!(replica.view(ids[0]).unwrap().bids()[0].qty_lots, 2);
    assert_eq!(
        replica.view(ids[2]).unwrap().trigger().instrument_id,
        ids[2]
    );
    let saved = replica.checkpoint().unwrap();
    assert_eq!(
        MarketReplica::restore_checkpoint(&saved, 2)
            .unwrap()
            .checkpoint()
            .unwrap(),
        saved
    );
}

#[test]
fn unknown_instrument_cannot_alias_a_registered_dense_slot() {
    let mut replica =
        MarketReplica::restore_checkpoint(&legacy_checkpoint(&[7, 1 << 48], 2), 2).unwrap();
    assert!(replica.view(6).is_none());
    let (raw, payload) = delta(6, 6, &[level(100, 9)], &[]);
    assert!(replica.apply(&raw, &payload).is_err());
    assert_eq!(replica.sequence(), 5);
    assert_eq!(replica.view(7).unwrap().bids()[0].qty_lots, 2);
    assert!(!replica.view(7).unwrap().live());
}

#[test]
fn capacity_full_delete_insert_is_atomic_and_keeps_unknown_coverage() {
    let mut replica = MarketReplica::restore_checkpoint(&legacy_checkpoint(&[7], 2), 2).unwrap();
    let (raw, payload) = delta(7, 6, &[level(101, 9), level(100, 0)], &[]);
    replica.apply(&raw, &payload).unwrap();
    let view = replica.view(7).unwrap();
    assert_eq!(view.bids(), &[level(101, 9), level(99, 4)]);
    assert_eq!(view.quantity(true, 98), None);
    let (raw, payload) = delta(7, 7, &[level(101, 10)], &[level(104, 1)]);
    assert!(replica.apply(&raw, &payload).is_err());
    let view = replica.view(7).unwrap();
    assert_eq!(view.bids(), &[level(101, 9), level(99, 4)]);
    assert_eq!(view.asks(), &[level(102, 3), level(103, 5)]);
    assert_eq!(replica.sequence(), 6);
    assert!(!view.live());
}

#[test]
fn preallocated_depth_updates_and_queries_allocate_nothing() {
    let mut replica =
        MarketReplica::restore_checkpoint(&legacy_checkpoint(&[7, 1 << 48], 2), 2).unwrap();
    let (mut raw, payload) = delta(1 << 48, 6, &[level(100, 8)], &[level(102, 9)]);
    let allocations = allocation_counter::measure(|| {
        for seq in 6u64..106 {
            raw[..8].copy_from_slice(&seq.to_le_bytes());
            let dense = replica.apply_dense(&raw, &payload).unwrap();
            assert_eq!(dense.index(), 1);
            let view = replica.view_dense(dense).unwrap();
            assert_eq!(view.bids()[0].qty_lots, 8);
            assert_eq!(view.quantity(true, 100), Some(8));
            assert_eq!(view.quantity(false, 102), Some(9));
            assert_eq!(view.quantity(true, 98), None);
            assert!(replica.view(999).is_none());
        }
    });
    assert_eq!(allocations.count_total, 0);
}

#[test]
fn first_exact_trade_uses_a_preallocated_category_and_preserves_checkpoint_order() {
    let mut replica = MarketReplica::restore_checkpoint(&legacy_checkpoint(&[7], 2), 2).unwrap();
    let value = MarketExactRecord {
        header: header(7, 6, 264),
        body: MarketExactBody {
            source_id: 2,
            source_connection_epoch: 8,
            source_encoding: 1,
            feed_state: 2,
            trade_id: 90,
            price_ticks: 101,
            quantity_lots: 4,
            side: 1,
            ..Default::default()
        },
    };
    let mut raw = [0; 512];
    value.encode(&mut raw).unwrap();
    let allocations = allocation_counter::measure(|| {
        let dense = replica.apply_dense(&raw, &[]).unwrap();
        assert_eq!(
            replica
                .view_dense(dense)
                .unwrap()
                .last_trade()
                .unwrap()
                .header()
                .message_type,
            264
        );
    });
    assert_eq!(allocations.count_total, 0);
    assert_eq!(replica.view(7).unwrap().received_mono_ns(), 49);
    let bytes = replica.checkpoint().unwrap();
    // Per-book state starts at 56 + 96. Trade(category 2) precedes FeedState(8).
    assert_eq!(
        RecordHeader::decode(&bytes[152..280]).unwrap().message_type,
        264
    );
    assert_eq!(
        RecordHeader::decode(&bytes[664..792]).unwrap().message_type,
        261
    );
    let restored = MarketReplica::restore_checkpoint(&bytes, 2).unwrap();
    assert_eq!(restored.checkpoint().unwrap(), bytes);
}

#[test]
fn invalid_late_ask_or_crossed_result_never_commits_the_bid_work_area() {
    for asks in [vec![level(102, 7), level(103, -1)], vec![level(100, 7)]] {
        let mut replica =
            MarketReplica::restore_checkpoint(&legacy_checkpoint(&[7], 4), 4).unwrap();
        let (raw, payload) = delta(7, 6, &[level(100, 8)], &asks);
        assert!(replica.apply(&raw, &payload).is_err());
        let view = replica.view(7).unwrap();
        assert_eq!(view.bids(), &[level(100, 2), level(99, 4)]);
        assert_eq!(view.asks(), &[level(102, 3), level(103, 5)]);
        assert_eq!(replica.sequence(), 5);
        assert!(!view.live());
    }
}

#[test]
fn streaming_merge_matches_independent_ordered_maps() {
    use std::collections::BTreeMap;
    let mut replica = MarketReplica::restore_checkpoint(&legacy_checkpoint(&[7], 8), 8).unwrap();
    let mut bids = BTreeMap::from([(99, 4), (100, 2)]);
    let mut asks = BTreeMap::from([(102, 3), (103, 5)]);
    let mut seed = 0xd7a2_791e_48a1_b05cu64;
    for _ in 0..400 {
        let mut make = |base| {
            let mut values = BTreeMap::new();
            for _ in 0..4 {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                values.insert(base + (seed % 12) as i64, ((seed >> 16) % 7) as i64);
            }
            values
                .into_iter()
                .map(|(p, q)| level(p, q))
                .collect::<Vec<_>>()
        };
        let mut bid_updates = make(90);
        bid_updates.reverse();
        let ask_updates = make(102);
        let mut expected_bids = bids.clone();
        let mut expected_asks = asks.clone();
        for (map, updates) in [
            (&mut expected_bids, &bid_updates),
            (&mut expected_asks, &ask_updates),
        ] {
            for value in updates {
                if value.qty_lots == 0 {
                    map.remove(&value.price_ticks);
                } else {
                    map.insert(value.price_ticks, value.qty_lots);
                }
            }
        }
        let before = replica.checkpoint().unwrap();
        let (raw, payload) = delta(7, replica.sequence() + 1, &bid_updates, &ask_updates);
        let result = replica.apply(&raw, &payload);
        if expected_bids.len() > 8 || expected_asks.len() > 8 {
            assert!(result.is_err());
            assert_eq!(
                replica.view(7).unwrap().bids(),
                bids.iter()
                    .rev()
                    .map(|(&p, &q)| level(p, q))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                replica.view(7).unwrap().asks(),
                asks.iter().map(|(&p, &q)| level(p, q)).collect::<Vec<_>>()
            );
            assert!(!replica.view(7).unwrap().live());
            replica = MarketReplica::restore_checkpoint(&before, 8).unwrap();
        } else {
            result.unwrap();
            bids = expected_bids;
            asks = expected_asks;
            assert_eq!(
                replica.view(7).unwrap().bids(),
                bids.iter()
                    .rev()
                    .map(|(&p, &q)| level(p, q))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                replica.view(7).unwrap().asks(),
                asks.iter().map(|(&p, &q)| level(p, q)).collect::<Vec<_>>()
            );
        }
        let view = replica.view(7).unwrap();
        for price in 89..115 {
            assert_eq!(
                view.quantity(true, price),
                bids.get(&price)
                    .copied()
                    .or_else(|| (price >= 99).then_some(0))
            );
            assert_eq!(
                view.quantity(false, price),
                asks.get(&price)
                    .copied()
                    .or_else(|| (price <= 103).then_some(0))
            );
        }
    }
}
