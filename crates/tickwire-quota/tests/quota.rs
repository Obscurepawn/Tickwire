use tickwire_quota::*;

fn now(n: u64) -> MonoTime {
    MonoTime::new(ClockDomain::new([1; 16], 1).unwrap(), n)
}
fn key(pool: u64) -> WindowKey {
    WindowKey {
        bucket: BucketId {
            namespace: [2; 16],
            scope: ScopeKind::Ip,
            subject: [3; 16],
            pool,
        },
        window: 1,
    }
}
fn config(pool: u64) -> WindowConfig {
    WindowConfig {
        key: key(pool),
        revision: 1,
        starts_ns: 0,
        ends_ns: 100,
        limit: 10,
        recovery_reserve: 2,
    }
}
fn attempt(sequence: u64) -> AttemptId {
    AttemptId {
        producer: [6; 16],
        sequence,
    }
}
fn cost(pool: u64, units: u64) -> Cost {
    Cost {
        window: key(pool),
        units,
    }
}
fn setup() -> (Coordinator<MockQuotaStore>, ExecLimiter) {
    let mut c = Coordinator::open(MockQuotaStore::default(), 1, now(0)).unwrap();
    c.add_window(config(1), now(0)).unwrap();
    c.add_window(config(2), now(0)).unwrap();
    let e = c
        .register_consumer(
            ConsumerId {
                instance: [4; 16],
                generation: 1,
            },
            90,
            now(0),
        )
        .unwrap();
    (c, e)
}
#[test]
fn shared_multi_bucket_allocation_and_local_consumption_are_atomic() {
    let (mut c, mut e) = setup();
    let g = c
        .issue(1, &e, Class::Normal, &[cost(1, 5), cost(2, 3)], 80, now(0))
        .unwrap();
    e.install(g, now(0)).unwrap();
    assert_eq!(
        e.consume(
            attempt(1),
            [7; 32],
            Class::Normal,
            &[cost(1, 4), cost(2, 4)],
            now(1)
        ),
        Err(Error::Exhausted)
    );
    assert_eq!(e.remaining(key(1), Class::Normal), 5);
    assert_eq!(
        e.consume(
            attempt(2),
            [8; 32],
            Class::Normal,
            &[cost(1, 4), cost(2, 2)],
            now(1)
        ),
        Ok(Consumed::New)
    );
    assert_eq!(e.remaining(key(1), Class::Normal), 1);
    assert_eq!(e.remaining(key(2), Class::Normal), 1);
}
#[test]
fn exact_retry_rechecks_expiry_and_never_refunds_a_no_bytes_attempt() {
    let (mut c, mut e) = setup();
    let g = c
        .issue(1, &e, Class::Normal, &[cost(1, 5)], 20, now(0))
        .unwrap();
    e.install(g, now(0)).unwrap();
    assert_eq!(
        e.consume(attempt(1), [7; 32], Class::Normal, &[cost(1, 2)], now(1)),
        Ok(Consumed::New)
    );
    assert_eq!(
        e.consume(attempt(1), [7; 32], Class::Normal, &[cost(1, 2)], now(2)),
        Ok(Consumed::Duplicate)
    );
    assert_eq!(e.remaining(key(1), Class::Normal), 3);
    assert_eq!(
        e.consume(attempt(1), [7; 32], Class::Normal, &[cost(1, 2)], now(20)),
        Err(Error::Expired)
    );
}
#[test]
fn recovery_capacity_and_stopped_receipt_do_not_regrant_spent_units() {
    let (mut c, mut e) = setup();
    let g = c
        .issue(1, &e, Class::Normal, &[cost(1, 8)], 80, now(0))
        .unwrap();
    e.install(g, now(0)).unwrap();
    assert!(matches!(
        c.issue(2, &e, Class::Normal, &[cost(1, 1)], 80, now(0)),
        Err(Error::Exhausted)
    ));
    let recovery = c
        .issue(3, &e, Class::Recovery, &[cost(1, 2)], 80, now(0))
        .unwrap();
    e.install(recovery, now(0)).unwrap();
    e.consume(attempt(1), [7; 32], Class::Normal, &[cost(1, 3)], now(1))
        .unwrap();
    c.reclaim(e.stop(), now(2)).unwrap();
    assert_eq!(c.accounted(key(1)).unwrap(), 3);
}
#[test]
fn restart_retains_unacknowledged_allocations_and_does_not_remint_owner() {
    let (mut c, e) = setup();
    let _lost = c
        .issue(1, &e, Class::Normal, &[cost(1, 8)], 80, now(0))
        .unwrap();
    let mut recovered = Coordinator::open(c.into_store(), 2, now(1)).unwrap();
    assert_eq!(recovered.accounted(key(1)).unwrap(), 8);
    assert!(matches!(
        recovered.register_consumer(
            ConsumerId {
                instance: [4; 16],
                generation: 1
            },
            90,
            now(1)
        ),
        Err(Error::Identity)
    ));
}

#[test]
fn commit_failure_poison_and_recovery_preserve_maybe_issued_capacity() {
    let store = MockQuotaStore::default();
    let control = store.clone();
    let mut c = Coordinator::open(store, 1, now(0)).unwrap();
    c.add_window(config(1), now(0)).unwrap();
    let e = c
        .register_consumer(
            ConsumerId {
                instance: [4; 16],
                generation: 1,
            },
            90,
            now(0),
        )
        .unwrap();
    control.fail_next(MockFailure::AfterCommit);
    assert!(matches!(
        c.issue(1, &e, Class::Normal, &[cost(1, 8)], 80, now(1)),
        Err(Error::Store)
    ));
    assert_eq!(c.accounted(key(1)), Ok(0));
    assert!(matches!(
        c.issue(2, &e, Class::Normal, &[cost(1, 1)], 80, now(2)),
        Err(Error::Poisoned)
    ));
    let mut recovered = Coordinator::open(c.into_store(), 2, now(2)).unwrap();
    assert_eq!(recovered.accounted(key(1)), Ok(8));
    let e2 = recovered
        .register_consumer(
            ConsumerId {
                instance: [4; 16],
                generation: 2,
            },
            90,
            now(2),
        )
        .unwrap();
    assert!(matches!(
        recovered.issue(1, &e2, Class::Normal, &[cost(1, 8)], 80, now(2)),
        Err(Error::AlreadyIssued)
    ));
    assert!(matches!(
        recovered.issue(2, &e2, Class::Normal, &[cost(1, 1)], 80, now(2)),
        Err(Error::Exhausted)
    ));
}

#[test]
fn cas_prevents_stale_coordinator_allocation_and_live_owners_share_limits() {
    let store = MockQuotaStore::default();
    let mut a = Coordinator::open(store.clone(), 1, now(0)).unwrap();
    a.add_window(config(1), now(0)).unwrap();
    let e = a
        .register_consumer(
            ConsumerId {
                instance: [4; 16],
                generation: 1,
            },
            90,
            now(0),
        )
        .unwrap();
    let mut b = Coordinator::open(store, 2, now(1)).unwrap();
    assert!(matches!(
        a.issue(1, &e, Class::Normal, &[cost(1, 8)], 80, now(1)),
        Err(Error::Store)
    ));
    let e2 = b
        .register_consumer(
            ConsumerId {
                instance: [5; 16],
                generation: 1,
            },
            90,
            now(1),
        )
        .unwrap();
    let _grant = b
        .issue(2, &e2, Class::Normal, &[cost(1, 8)], 80, now(1))
        .unwrap();
    assert_eq!(b.accounted(key(1)), Ok(8));
}

#[test]
fn domain_rollback_conflict_and_cooldown_never_reuse_old_permission() {
    let (mut c, mut e) = setup();
    let g = c
        .issue(1, &e, Class::Normal, &[cost(1, 8)], 80, now(0))
        .unwrap();
    e.install(g, now(0)).unwrap();
    e.consume(attempt(1), [7; 32], Class::Normal, &[cost(1, 1)], now(1))
        .unwrap();
    e.cooldown(5, now(2)).unwrap();
    assert_eq!(
        e.consume(attempt(1), [7; 32], Class::Normal, &[cost(1, 1)], now(3)),
        Err(Error::Cooldown)
    );
    assert_eq!(
        e.consume(attempt(1), [7; 32], Class::Normal, &[cost(1, 1)], now(5)),
        Ok(Consumed::Duplicate)
    );
    assert_eq!(
        e.consume(attempt(1), [8; 32], Class::Normal, &[cost(1, 1)], now(5)),
        Err(Error::Conflict)
    );
    assert!(e.is_frozen());
    let foreign = MonoTime::new(ClockDomain::new([9; 16], 1).unwrap(), 6);
    assert!(matches!(
        Coordinator::open(c.into_store(), 2, foreign),
        Err(Error::Domain)
    ));
    let (mut c, mut e) = setup();
    let g = c
        .issue(1, &e, Class::Normal, &[cost(1, 1)], 80, now(0))
        .unwrap();
    e.install(g, now(5)).unwrap();
    assert_eq!(
        e.consume(attempt(1), [7; 32], Class::Normal, &[cost(1, 1)], now(4)),
        Err(Error::ClockRegression)
    );
    assert!(e.is_frozen());
}

#[test]
fn window_rotation_never_resets_or_overlaps_prior_window_capacity() {
    let (mut c, mut e) = setup();
    let mut next = config(1);
    next.key.window = 2;
    next.starts_ns = 99;
    next.ends_ns = 200;
    assert_eq!(c.add_window(next.clone(), now(0)), Err(Error::Conflict));
    next.starts_ns = 100;
    c.add_window(next.clone(), now(0)).unwrap();
    let mut modified = config(1);
    modified.limit = 100;
    assert_eq!(c.add_window(modified, now(0)), Err(Error::Conflict));
    let g = c
        .issue(1, &e, Class::Normal, &[cost(1, 8)], 80, now(0))
        .unwrap();
    e.install(g, now(0)).unwrap();
    e.consume(attempt(1), [7; 32], Class::Normal, &[cost(1, 8)], now(1))
        .unwrap();
    assert_eq!(
        e.consume(attempt(2), [8; 32], Class::Normal, &[cost(1, 1)], now(2)),
        Err(Error::Exhausted)
    );
}

#[test]
fn lease_renewal_does_not_extend_old_grant_or_resurrect_expired_owner() {
    let (mut c, mut e) = setup();
    let g = c
        .issue(1, &e, Class::Normal, &[cost(1, 4)], 80, now(0))
        .unwrap();
    e.install(g, now(0)).unwrap();
    let renewal = c.renew_consumer(&e, 200, now(1)).unwrap();
    e.renew(renewal, now(1)).unwrap();
    assert_eq!(
        e.consume(attempt(1), [7; 32], Class::Normal, &[cost(1, 1)], now(80)),
        Err(Error::Expired)
    );
    let fresh = c
        .issue(2, &e, Class::Normal, &[cost(1, 2)], 100, now(80))
        .unwrap();
    e.install(fresh, now(80)).unwrap();
    e.consume(attempt(2), [8; 32], Class::Normal, &[cost(1, 1)], now(81))
        .unwrap();
    c.reclaim(e.stop(), now(82)).unwrap();
    assert_eq!(c.accounted(key(1)), Ok(1));
    let (mut c, e) = setup();
    assert!(matches!(
        c.renew_consumer(&e, 200, now(90)),
        Err(Error::Expired)
    ));
}

#[test]
fn independent_producers_can_each_start_at_sequence_one() {
    let (mut c, mut e) = setup();
    let g = c
        .issue(1, &e, Class::Normal, &[cost(1, 2)], 80, now(0))
        .unwrap();
    e.install(g, now(0)).unwrap();
    assert_eq!(
        e.consume(
            AttemptId {
                producer: [6; 16],
                sequence: 1
            },
            [7; 32],
            Class::Normal,
            &[cost(1, 1)],
            now(1)
        ),
        Ok(Consumed::New)
    );
    assert_eq!(
        e.consume(
            AttemptId {
                producer: [7; 16],
                sequence: 1
            },
            [8; 32],
            Class::Normal,
            &[cost(1, 1)],
            now(1)
        ),
        Ok(Consumed::New)
    );
    assert_eq!(e.remaining(key(1), Class::Normal), 0);
}
