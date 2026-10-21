use super::*;
use tickwire_account::Account;
#[path = "../../tickwire-account/tests/common/mod.rs"]
mod fixture;
fn domain() -> ClockDomain {
    ClockDomain::new([8; 16], 1).unwrap()
}
fn now(n: u64) -> MonoTime {
    MonoTime::new(domain(), n)
}
fn feed_key() -> FeedKey {
    FeedKey {
        topic_id: 1,
        instrument_id: 12,
    }
}
fn setup() -> (ControlPlane, Account, CanonicalCommand, Admission) {
    let (mut account, c) = fixture::setup();
    let command = fixture::canonical(c);
    let admission = account.admit(&command, 60).unwrap();
    let plane = ControlPlane::new(Config {
        scope: fixture::fixture().0.scope,
        product: Product::Spot,
        executor_id: [1; 16],
        owner_generation: 1,
        writer_epoch: 4,
        domain: domain(),
        versions: Versions {
            config_revision: 6,
            risk_policy_version: 10,
            order_config_version: 11,
            catalog_version: 14,
            catalog_hash: [5; 32],
        },
        max_strategies: 4,
        max_feeds: 4,
        marks_per_feed: 2,
    })
    .unwrap();
    (plane, account, command, admission)
}
fn grant() -> StrategyGrant {
    StrategyGrant {
        logical_strategy_id: 8,
        instance_id: [2; 16],
        strategy_generation: 5,
        authorization_epoch: 9,
        config_revision: 6,
        valid_until_ns: 1000,
        actions: Actions {
            submit: true,
            cancel: true,
            query: true,
            amend: true,
        },
        required_feeds: vec![FeedRequirement {
            key: feed_key(),
            instrument_version: 13,
            max_age_ns: 30,
            max_sequence_lag: 2,
        }],
    }
}
#[test]
fn shadow_failure_preserves_the_exact_active_grant_and_its_expiry() {
    let (plane, _, _, _) = setup();
    prepare(&plane);
    plane.stop_strategy_instance(8, [73; 16], 6).unwrap();
    assert_eq!(
        plane.strategy_instance_authorized(8, [2; 16], 5, now(70)),
        Ok(true)
    );
    assert_eq!(
        plane.strategy_instance_authorized(8, [73; 16], 6, now(70)),
        Ok(false)
    );
    assert_eq!(
        plane.strategy_instance_authorized(8, [2; 16], 5, now(1000)),
        Ok(false)
    );
    plane.stop_strategy_instance(8, [2; 16], 5).unwrap();
    assert_eq!(
        plane.strategy_instance_authorized(8, [2; 16], 5, now(70)),
        Ok(false)
    );
    assert_eq!(plane.install_strategy_grant(grant()), Err(Error::Stale));
}
#[test]
fn handover_closes_instance_activation_until_its_exact_release() {
    let (plane, _, _, _) = setup();
    prepare(&plane);
    let binding = HandoverBinding {
        operation_id: [71; 16],
        operation_revision: 1,
        members_digest: [72; 32],
        logical_strategy_id: 8,
        instance_id: [73; 16],
        strategy_generation: 6,
        valid_until_ns: 1000,
    };
    let prepared = plane.begin_handover(binding).unwrap();
    assert_eq!(
        plane.strategy_instance_authorized(8, [2; 16], 5, now(70)),
        Ok(false)
    );
    let committed = plane.handover_committed(&prepared, 10, [74; 32]).unwrap();
    let mut target = grant();
    target.instance_id = [73; 16];
    target.strategy_generation = 6;
    target.authorization_epoch = 10;
    plane.release_handover(&committed, target, now(70)).unwrap();
    assert_eq!(
        plane.strategy_instance_authorized(8, [73; 16], 6, now(71)),
        Ok(true)
    );
    plane.stop_strategy_instance(8, [2; 16], 5).unwrap();
    assert_eq!(
        plane.strategy_instance_authorized(8, [73; 16], 6, now(71)),
        Ok(true)
    );
}
#[test]
fn closed_handover_gate_rejects_an_old_group_release() {
    let (plane, _, _, _) = setup();
    prepare(&plane);
    let binding = HandoverBinding {
        operation_id: [71; 16],
        operation_revision: 1,
        members_digest: [72; 32],
        logical_strategy_id: 8,
        instance_id: [73; 16],
        strategy_generation: 6,
        valid_until_ns: 1000,
    };
    let prepared = plane.begin_handover(binding.clone()).unwrap();
    let committed = plane.handover_committed(&prepared, 10, [74; 32]).unwrap();
    let mut target = grant();
    target.instance_id = binding.instance_id;
    target.strategy_generation = 6;
    target.authorization_epoch = 10;
    plane.stop_strategy(8).unwrap();
    assert_eq!(
        plane.release_handover(&committed, target, now(70)),
        Err(Error::Stale)
    );
    assert!(
        plane.handover_gate(8).unwrap().unwrap().risk_gate_revision > committed.risk_gate_revision
    );
}
#[test]
fn revoke_never_restores_an_old_generation_with_a_new_authorization_epoch() {
    let (plane, _, _, _) = setup();
    prepare(&plane);
    plane.stop_strategy(8).unwrap();
    let mut old = grant();
    old.authorization_epoch += 1;
    assert_eq!(plane.install_strategy_grant(old.clone()), Err(Error::Stale));
    old.strategy_generation += 1;
    old.instance_id = [75; 16];
    plane.install_strategy_grant(old).unwrap();
}
fn owner() -> OwnerLease {
    OwnerLease {
        executor_id: [1; 16],
        owner_generation: 1,
        writer_epoch: 4,
        lease_epoch: 1,
        domain: domain(),
        valid_until_ns: 1000,
        isolation_record: [1; 32],
    }
}
fn prepare(plane: &ControlPlane) {
    plane.install_owner_lease(owner()).unwrap();
    plane.install_strategy_grant(grant()).unwrap();
    plane.set_mode(1, Mode::Trading).unwrap();
    plane
        .update_feed(FeedUpdate {
            key: feed_key(),
            instrument_version: 13,
            feed_generation: 1,
            ring_epoch: 1,
            sequence: 1,
            published_at: now(48),
        })
        .unwrap();
    plane.private_connected(1).unwrap();
    plane.private_gap(1, 1).unwrap();
    plane.private_connected(1).unwrap();
}
struct Verifier;
impl ReconciliationVerifier for Verifier {
    fn verify(&mut self, r: &ReconciliationRequest) -> Result<()> {
        if r.query_coverage_digest != [3; 32] {
            Err(Error::Unreconciled)
        } else {
            Ok(())
        }
    }
}
fn reconciliation(plane: &ControlPlane) -> ReconciliationRequest {
    let f = plane.private_frontier().unwrap();
    ReconciliationRequest {
        scope: fixture::fixture().0.scope,
        product: Product::Spot,
        input_stream: [9; 16],
        writer_epoch: 4,
        connection_epoch: f.connection_epoch,
        gap_generation: f.gap_generation,
        through_sequence: f.through_sequence,
        account_epoch: 1,
        ledger_sequence: 0,
        baseline_digest: [1; 32],
        history_digest: [2; 32],
        query_coverage_digest: [3; 32],
        valid_until: now(1000),
    }
}
#[test]
fn initial_and_receiving_states_cannot_trade_until_reconciliation() {
    let (p, _, c, a) = setup();
    assert!(p.evaluate(&c, &a, now(60)).is_err());
    prepare(&p);
    assert_eq!(p.evaluate(&c, &a, now(60)), Err(Error::Unreconciled));
    p.complete_reconciliation(reconciliation(&p), &mut Verifier)
        .unwrap();
    let gate = p.evaluate(&c, &a, now(60)).unwrap();
    assert!(gate.market_valid);
    assert_eq!(gate.lease_until, 78); // feed publication + max age is a final-gate deadline
}
#[test]
fn phases_do_not_mint_source_gaps_and_new_writer_revokes_old_permissions() {
    let (p, _, _, _) = setup();
    prepare(&p);
    p.private_disconnected(1).unwrap();
    p.private_connected(1).unwrap();
    p.private_uncertain(1).unwrap();
    assert_eq!(p.private_frontier().unwrap().gap_generation, 1);
    let mut next = owner();
    next.writer_epoch += 1;
    next.lease_epoch += 1;
    p.replace_writer(2, next).unwrap();
    let s = p.0.borrow();
    assert_eq!(s.mode, Mode::Stopped);
    assert!(s.strategies.values().all(|g| g.current.is_none()));
    assert!(s.private.reconciled.is_none());
    drop(s);
    assert!(p.install_owner_lease(owner()).is_err());
    assert!(p.install_strategy_grant(grant()).is_err());
}
#[test]
fn revoked_grants_and_gap_tokens_cannot_be_reinstalled_or_replayed() {
    let (p, _, c, a) = setup();
    prepare(&p);
    let old = reconciliation(&p);
    p.complete_reconciliation(old.clone(), &mut Verifier)
        .unwrap();
    p.private_gap(1, 2).unwrap();
    p.private_connected(1).unwrap();
    assert!(p.complete_reconciliation(old, &mut Verifier).is_err());
    assert!(p.evaluate(&c, &a, now(60)).is_err());
    p.complete_reconciliation(reconciliation(&p), &mut Verifier)
        .unwrap();
    p.revoke_strategy(8, 9).unwrap();
    assert!(p.install_strategy_grant(grant()).is_err());
    assert!(p.evaluate(&c, &a, now(60)).is_err());
}
#[test]
fn market_generations_missing_dependencies_and_expiry_deny() {
    let (p, _, c, a) = setup();
    prepare(&p);
    p.complete_reconciliation(reconciliation(&p), &mut Verifier)
        .unwrap();
    assert!(p.evaluate(&c, &a, now(78)).is_err());
    let (p, _, c, a) = setup();
    prepare(&p);
    p.complete_reconciliation(reconciliation(&p), &mut Verifier)
        .unwrap();
    p.update_feed(FeedUpdate {
        key: feed_key(),
        instrument_version: 13,
        feed_generation: 2,
        ring_epoch: 2,
        sequence: 1,
        published_at: now(49),
    })
    .unwrap();
    assert!(p.evaluate(&c, &a, now(60)).is_err());
    let mut g = grant();
    g.authorization_epoch = 10;
    g.required_feeds.push(FeedRequirement {
        key: FeedKey {
            topic_id: 2,
            instrument_id: 12,
        },
        instrument_version: 13,
        max_age_ns: 30,
        max_sequence_lag: 0,
    });
    p.install_strategy_grant(g).unwrap();
    assert!(p.evaluate(&c, &a, now(60)).is_err());
}
#[test]
fn reconciliation_rechecks_live_prefix_after_the_verifier_returns() {
    struct Racing {
        plane: ControlPlane,
    }
    impl ReconciliationVerifier for Racing {
        fn verify(&mut self, _: &ReconciliationRequest) -> Result<()> {
            self.plane.private_gap(1, 2)
        }
    }
    let (p, _, c, a) = setup();
    prepare(&p);
    assert!(
        p.complete_reconciliation(reconciliation(&p), &mut Racing { plane: p.clone() })
            .is_err()
    );
    assert!(p.evaluate(&c, &a, now(60)).is_err());
}
#[test]
fn management_query_survives_strategy_revoke_and_private_gap() {
    let (p, mut account, c, _) = setup();
    prepare(&p);
    p.revoke_strategy(8, 9).unwrap();
    p.private_gap(1, 2).unwrap();
    p.invalidate_feed(feed_key(), 1, 1).unwrap();
    p.install_management_grant(ManagementGrant {
        authorization_epoch: 10,
        config_revision: 6,
        valid_until_ns: 1000,
        cancel: true,
        query: true,
        strategies: vec![8],
    })
    .unwrap();
    let mut query = c.command().clone();
    query.context.request.intent_id = 4;
    query.context.strategy.authorization_epoch = 10;
    query.body = CommandBody::Query(tickwire_venue::OrderTarget {
        order_id: [9; 16],
        expected_owner_revision: 1,
        original_intent_exec_epoch: 4,
        client_id: Some(fixture::venue::text("test-1")),
        venue_order_id: None,
    });
    let query = fixture::canonical(query);
    let admitted = account.admit(&query, 60).unwrap();
    assert!(p.evaluate(&query, &admitted, now(60)).is_ok());
    p.revoke_owner(1).unwrap();
    assert!(p.evaluate(&query, &admitted, now(60)).is_err());
}
#[test]
fn reducing_amend_uses_its_strategy_permission_while_private_and_market_are_frozen() {
    use tickwire_venue as v;
    let (p, mut account, c, _) = setup();
    prepare(&p);
    let mut ack = fixture::venue::event();
    let v::VenueEventBody::Fill(f) = ack.body else {
        unreachable!()
    };
    let mut observation = f.observation;
    observation.status = v::OrderStatus::New;
    observation.cumulative_quantity = Some(fixture::venue::d(0, 0));
    let target = v::OrderTarget {
        order_id: observation.order.order_id,
        expected_owner_revision: 1,
        original_intent_exec_epoch: 4,
        client_id: observation.order.client_id.clone(),
        venue_order_id: observation.order.venue_order_id.clone(),
    };
    ack.body = v::VenueEventBody::Order(Box::new(observation));
    account.apply_event(&fixture::event(ack)).unwrap();
    account.freeze();
    p.private_gap(1, 2).unwrap();
    p.invalidate_feed(feed_key(), 1, 1).unwrap();
    let mut command = c.command().clone();
    command.context.request.intent_id = 4;
    command.body = CommandBody::AmendKeepPriority {
        target,
        new_quantity: fixture::venue::d(8, -4),
        new_client_id: Some(fixture::venue::text("test-1")),
    };
    let command = fixture::canonical(command);
    let admission = account.admit(&command, 61).unwrap();
    let gate = p
        .evaluate(&command, &admission, now(62))
        .expect("explicit amend grant permits a verified decrease");
    assert!(!gate.market_valid);
    p.revoke_strategy(8, 9).unwrap();
    p.install_management_grant(ManagementGrant {
        authorization_epoch: 9,
        config_revision: 6,
        valid_until_ns: 1000,
        cancel: true,
        query: true,
        strategies: vec![8],
    })
    .unwrap();
    assert!(
        p.evaluate(&command, &admission, now(63)).is_err(),
        "cancel/query grants cannot authorize amendment"
    );
}
#[test]
fn current_clock_domain_and_monotonicity_are_sticky() {
    struct Clock(MonoTime);
    impl PolicyClock for Clock {
        fn domain(&self) -> ClockDomain {
            domain()
        }
        fn sample(&mut self) -> Result<MonoTime> {
            Ok(self.0)
        }
    }
    let (p, _, c, a) = setup();
    prepare(&p);
    p.complete_reconciliation(reconciliation(&p), &mut Verifier)
        .unwrap();
    let mut provider = p.provider(Clock(now(60))).unwrap();
    assert!(provider.current(&c, &a).is_ok());
    provider.clock.0 = now(59);
    assert!(provider.current(&c, &a).is_err());
    provider.clock.0 = now(61);
    assert!(provider.current(&c, &a).is_err());
}

#[test]
fn health_must_sample_after_a_gate_callback_advanced_the_shared_clock() {
    use tickwire_runtime::node::{Health, Observer};

    let local = |n| tickwire_binance_clock::LocalTime {
        instant: std::time::Instant::now(),
        monotonic: now(n),
    };
    let (stale, _, command, admission) = setup();
    prepare(&stale);
    stale
        .complete_reconciliation(reconciliation(&stale), &mut Verifier)
        .unwrap();
    let sampled_before_poll = local(60);
    assert!(stale.evaluate(&command, &admission, now(61)).is_ok());
    assert!(
        !stale
            .observer()
            .health(Health::default(), sampled_before_poll)
    );
    // A later timestamp cannot undo a real observer rollback fault.
    assert_eq!(
        stale.evaluate(&command, &admission, now(62)),
        Err(Error::Clock)
    );

    let (fresh, _, command, admission) = setup();
    prepare(&fresh);
    fresh
        .complete_reconciliation(reconciliation(&fresh), &mut Verifier)
        .unwrap();
    assert!(fresh.evaluate(&command, &admission, now(61)).is_ok());
    assert!(fresh.observer().health(Health::default(), local(62)));
    assert!(fresh.evaluate(&command, &admission, now(63)).is_ok());
}
