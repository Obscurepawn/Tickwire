use super::*;

fn feeds(ids: &[u64]) -> Vec<Feed> {
    let config: super::super::Config =
        serde_json::from_str(include_str!("../../examples/md.json")).unwrap();
    ids.iter()
        .map(|id| {
            let mut feed = config.feeds[0].clone();
            feed.instrument.id = *id;
            feed
        })
        .collect()
}

#[test]
fn restore_preserves_recorded_slots_when_feeds_are_reordered_or_repeated() {
    let ids = [u64::MAX, u64::from(u32::MAX), 1 << 63];
    let inputs = feeds(&[ids[2], ids[1], ids[0], ids[2]]);
    let restored = FeedIndex::restore(&inputs, &ids).unwrap();
    assert_eq!(restored.instrument_ids(), ids);
    for (feed, slot) in inputs.iter().zip([2, 1, 0, 2]) {
        assert_eq!(restored.book_key(feed).unwrap().instrument, slot);
    }
    assert!(restored.book_key(&feeds(&[17])[0]).is_err());
}

#[test]
fn restore_requires_the_complete_unique_nonzero_instrument_set() {
    let inputs = feeds(&[1, u64::MAX, 1]);
    for ids in [
        vec![],
        vec![1],
        vec![u64::MAX],
        vec![1, u64::MAX, 2],
        vec![1, 1, u64::MAX],
        vec![0, 1, u64::MAX],
        vec![0, u64::MAX],
    ] {
        assert!(FeedIndex::restore(&inputs, &ids).is_err(), "{ids:?}");
    }
    assert!(FeedIndex::restore(&[], &[1]).is_err());
    assert!(FeedIndex::restore(&feeds(&[0, 1]), &[1]).is_err());
    assert!(FeedIndex::restore(&feeds(&[0]), &[0]).is_err());
}

#[test]
fn restore_enforces_both_feed_and_recorded_directory_capacities() {
    let ids: Vec<u64> = (1..=32).rev().collect();
    let mut inputs = feeds(&ids);
    inputs.reverse();
    let restored = FeedIndex::restore(&inputs, &ids).unwrap();
    assert_eq!(restored.instrument_ids(), ids);
    for (slot, id) in ids.iter().enumerate() {
        let feed = inputs.iter().find(|f| f.instrument.id == *id).unwrap();
        assert_eq!(restored.book_key(feed).unwrap().instrument as usize, slot);
    }
    assert!(FeedIndex::restore(&feeds(&[1; 32]), &[1]).is_ok());
    assert!(FeedIndex::restore(&feeds(&[1; 33]), &[1]).is_err());
    let too_many: Vec<u64> = (1..=33).collect();
    assert!(FeedIndex::restore(&feeds(&too_many), &too_many).is_err());
    assert!(FeedIndex::restore(&inputs, &too_many).is_err());
}

#[test]
fn restored_directory_can_append_without_changing_old_slots() {
    let original = feeds(&[u64::MAX, 9, u64::MAX]);
    let startup = FeedIndex::new(&original).unwrap();
    assert_eq!(startup.instrument_ids(), [u64::MAX, 9]);
    let expanded_feeds = feeds(&[3, 9, u64::MAX]);
    let expanded = startup.expanded(&expanded_feeds).unwrap();
    assert_eq!(expanded.instrument_ids(), [u64::MAX, 9, 3]);
    let restored = FeedIndex::restore(&expanded_feeds, expanded.instrument_ids()).unwrap();
    let next_feeds = feeds(&[1 << 63, 3, u64::MAX, 9]);
    let next = restored.expanded(&next_feeds).unwrap();
    assert_eq!(next.instrument_ids(), [u64::MAX, 9, 3, 1 << 63]);
    for feed in &expanded_feeds {
        assert_eq!(
            expanded.book_key(feed).unwrap(),
            restored.book_key(feed).unwrap()
        );
        assert_eq!(
            restored.book_key(feed).unwrap(),
            next.book_key(feed).unwrap()
        );
    }
    assert_eq!(startup.instrument_ids(), [u64::MAX, 9]);
    assert_eq!(restored.instrument_ids(), [u64::MAX, 9, 3]);
    assert!(restored.expanded(&feeds(&[9, 3, 1 << 63])).is_err());
    assert_eq!(restored.instrument_ids(), [u64::MAX, 9, 3]);
}
