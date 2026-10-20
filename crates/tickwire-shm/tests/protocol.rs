use tickwire_shm::*;

fn bank() -> BankKey {
    BankKey {
        resource: 1,
        channel: 2,
        channel_epoch: 3,
        ring_epoch: 4,
        feed_generation: 5,
    }
}
fn member(instance: u128) -> SubscriptionKey {
    SubscriptionKey {
        bank: bank(),
        instance,
        generation: 1,
        cursor: instance + 100,
    }
}
fn producer() -> Producer<4> {
    let mut p = Producer::new(bank(), 64, 1).unwrap();
    p.activate().unwrap();
    p
}
fn publish<const M: usize>(p: &mut Producer<M>, count: u8) {
    let claim = p.claim(count).unwrap();
    p.publish(claim).unwrap();
}

#[test]
fn capacity_is_protected_by_every_reader() {
    let mut p = producer();
    p.join(member(1)).unwrap();
    p.join_ready(member(1), 0).unwrap();
    p.join(member(2)).unwrap();
    p.join_ready(member(2), 0).unwrap();
    publish(&mut p, 64);
    p.observe_release(member(1), 64).unwrap();
    assert_eq!(p.claim(1).unwrap_err(), Error::Full);
    p.observe_release(member(2), 1).unwrap();
    let c = p.claim(1).unwrap();
    assert_eq!((c.first(), c.last()), (65, 65));
    p.publish(c).unwrap();
    assert_eq!(p.slot(1).unwrap(), p.slot(65).unwrap());
    assert_eq!(p.claim(1).unwrap_err(), Error::Full);
}

#[test]
fn pending_join_baseline_cannot_be_advanced_by_a_cursor() {
    let mut p = producer();
    publish(&mut p, 64);
    assert_eq!(p.join(member(1)).unwrap(), 65);
    publish(&mut p, 64);
    assert_eq!(p.observe_release(member(1), 128), Err(Error::NotActive));
    assert_eq!(p.claim(1).unwrap_err(), Error::Full);
    p.join_ready(member(1), 64).unwrap();
    assert_eq!(p.claim(1).unwrap_err(), Error::Full);
    p.observe_release(member(1), 65).unwrap();
    publish(&mut p, 1);
}

#[test]
fn join_invalidates_the_no_reader_cached_minimum() {
    let mut p = producer();
    for _ in 0..3 {
        publish(&mut p, 64);
    }
    p.join(member(1)).unwrap();
    p.join_ready(member(1), 192).unwrap();
    publish(&mut p, 64);
    assert_eq!(p.claim(1).unwrap_err(), Error::Full);
}

#[test]
fn a_batch_is_all_or_nothing_and_blocks_membership_and_freeze() {
    let mut p = producer();
    p.prepare_freeze(8, 3).unwrap();
    let c = p.claim(3).unwrap();
    assert_eq!(p.published(), 0);
    assert_eq!(p.claim(1).unwrap_err(), Error::OutstandingWrite);
    assert_eq!(p.join(member(1)), Err(Error::OutstandingWrite));
    assert_eq!(p.freeze(), Err(Error::OutstandingWrite));
    p.abort(c).unwrap();
    assert_eq!(p.published(), 0);
    let c = p.claim(3).unwrap();
    p.publish(c).unwrap();
    let frozen = p.freeze().unwrap();
    assert_eq!((frozen.cutover, frozen.successor_ring_epoch), (3, 8));
    assert_eq!(p.claim(1).unwrap_err(), Error::NotActive);
    assert_eq!(p.freeze().unwrap(), frozen);
}

#[test]
fn malformed_batches_and_sequence_exhaustion_do_not_change_state() {
    let mut p = producer();
    assert_eq!(p.claim(0).unwrap_err(), Error::BatchSize);
    assert_eq!(p.claim(65).unwrap_err(), Error::BatchSize);
    assert_eq!(p.published(), 0);
    let mut high = Producer::<1>::new(bank(), 64, (1 << 63) - 1).unwrap();
    high.activate().unwrap();
    publish(&mut high, 1);
    assert_eq!(high.claim(1).unwrap_err(), Error::SequenceExhausted);
    assert!(Producer::<1>::new(bank(), 63, 1).is_err());
    assert!(Producer::<1>::new(bank(), 128, 0).is_err());
}

#[test]
fn cutoff_never_publishes_a_partial_batch() {
    let mut p = producer();
    p.prepare_freeze(8, 3).unwrap();
    assert_eq!(p.claim(4).unwrap_err(), Error::Cutover);
    assert_eq!(p.freeze(), Err(Error::Cutover));
    assert_eq!(p.published(), 0);
    publish(&mut p, 3);
    p.freeze().unwrap();
}

#[test]
fn invalid_or_stale_cursor_updates_cannot_release_capacity() {
    let mut p = producer();
    p.join(member(1)).unwrap();
    p.join_ready(member(1), 0).unwrap();
    publish(&mut p, 64);
    assert_eq!(p.observe_release(member(1), 65), Err(Error::Cursor));
    let mut stale = member(1);
    stale.generation += 1;
    assert_eq!(p.observe_release(stale, 64), Err(Error::Identity));
    assert_eq!(p.claim(1).unwrap_err(), Error::Full);
    p.observe_release(member(1), 20).unwrap();
    assert_eq!(p.observe_release(member(1), 19), Err(Error::Cursor));
    publish(&mut p, 20);
    assert_eq!(p.claim(1).unwrap_err(), Error::Full);
}

#[test]
fn reader_keeps_release_and_application_gaps_separate() {
    let mut r = Reader::<4>::new(member(1), 0).unwrap();
    let a = r.borrow_next(3).unwrap().unwrap();
    let b = r.borrow_next(3).unwrap().unwrap();
    let c = r.borrow_next(3).unwrap().unwrap();
    r.release(c).unwrap();
    r.apply(3).unwrap();
    r.release(b).unwrap();
    assert_eq!(
        r.watermarks(),
        Watermarks {
            read: 3,
            applied: 0,
            released: 0
        }
    );
    r.release(a).unwrap();
    assert_eq!(r.watermarks().released, 3);
    assert_eq!(r.watermarks().applied, 0);
    r.apply(1).unwrap();
    assert_eq!(r.watermarks().applied, 1);
    r.apply(2).unwrap();
    assert_eq!(r.watermarks().applied, 3);
    assert_eq!(r.outstanding(), 0);
}

#[test]
fn application_does_not_release_a_live_borrow() {
    let mut r = Reader::<1>::new(member(1), 0).unwrap();
    let a = r.borrow_next(2).unwrap().unwrap();
    r.apply(1).unwrap();
    assert_eq!(r.watermarks().released, 0);
    assert_eq!(r.borrow_next(2).unwrap_err(), Error::BorrowCapacity);
    r.release(a).unwrap();
    assert_eq!(r.borrow_next(2).unwrap().unwrap().sequence(), 2);
}

#[test]
fn local_window_is_bounded_even_after_early_release() {
    let mut r = Reader::<1>::new(member(1), 0).unwrap();
    let a = r.borrow_next(2).unwrap().unwrap();
    r.release(a).unwrap();
    assert_eq!(r.borrow_next(2).unwrap_err(), Error::BorrowCapacity);
    r.apply(1).unwrap();
    assert_eq!(r.borrow_next(2).unwrap().unwrap().sequence(), 2);
}

#[test]
fn quiesce_requires_every_borrow_but_not_business_application() {
    let mut p = producer();
    p.join(member(1)).unwrap();
    p.join_ready(member(1), 0).unwrap();
    publish(&mut p, 64);
    let mut r = Reader::<4>::new(member(1), 0).unwrap();
    let a = r.borrow_next(64).unwrap().unwrap();
    r.start_quiesce();
    assert_eq!(r.borrow_next(64).unwrap_err(), Error::NotActive);
    assert_eq!(r.quiesced().unwrap_err(), Error::OutstandingRead);
    assert_eq!(p.claim(1).unwrap_err(), Error::Full);
    r.release(a).unwrap();
    let evidence = r.quiesced().unwrap();
    assert_eq!(evidence.watermarks().released, 1);
    assert_eq!(evidence.watermarks().applied, 0);
    p.detach(evidence).unwrap();
    publish(&mut p, 64);
}

#[test]
fn old_quiesce_ack_cannot_remove_a_new_subscription() {
    let mut p = producer();
    let mut old = Reader::<1>::new(member(1), 0).unwrap();
    old.start_quiesce();
    let old_ack = old.quiesced().unwrap();
    let mut new_key = member(1);
    new_key.generation = 2;
    p.join(new_key).unwrap();
    assert_eq!(p.detach(old_ack), Err(Error::Identity));
    publish(&mut p, 64);
    assert_eq!(p.claim(1).unwrap_err(), Error::Full);
}

#[test]
fn reader_rejects_regressing_published_and_future_application() {
    let mut r = Reader::<2>::new(member(1), 10).unwrap();
    assert_eq!(r.borrow_next(9).unwrap_err(), Error::Cursor);
    assert_eq!(r.apply(11), Err(Error::Cursor));
    assert!(r.borrow_next(10).unwrap().is_none());
    assert_eq!(
        r.borrow_next(1 << 63).unwrap_err(),
        Error::SequenceExhausted
    );
}
