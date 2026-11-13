mod common;

use common::*;
use tickwire_account::{
    Account, Applied, DEFAULT_RETENTION_WINDOW_NS, Error, IntentKey, MAX_MANAGEMENT_ADMISSIONS,
    MAX_ORDERS, OrderKey, PoolFull, WindowedTable,
};
use tickwire_venue as v;

#[test]
fn windowed_table_preallocates_once_protects_active_and_reclaims_expired() {
    let mut table: WindowedTable<u64, u64> = WindowedTable::with_capacity(64);
    let initial_slots_addr = table.slots_addr();
    let initial_buckets_addr = table.buckets_addr();
    let window_ns = Some(1_000u64);

    // Fill all 64 slots with active (non-retired) entries.
    for k in 0..64u64 {
        table
            .insert_or_evict(k, k * 10, None, 100, window_ns)
            .unwrap();
    }
    assert_eq!(table.len(), 64);
    assert_eq!(table.active_len(), 64);

    // Even if mono_ns advances by weeks, active entries (retired_at == None) are never evicted.
    let far_future = 30 * DEFAULT_RETENTION_WINDOW_NS;
    assert_eq!(
        table.insert_or_evict(999, 1, None, far_future, window_ns),
        Err(PoolFull)
    );
    assert_eq!(table.len(), 64);

    // Retire slot 0 at t = 5_000, then reactivate it before expiry and ensure it stays protected.
    assert!(table.retire(&0, 5_000));
    assert_eq!(table.retired_at(&0), Some(Some(5_000)));
    assert!(table.reactivate(&0));
    assert_eq!(table.retired_at(&0), Some(None));
    assert_eq!(table.evict_expired(10_000, window_ns), 0);

    // Retire slot 1 at t = 5_000: before t = 6_000 it is still live; at t = 6_000 get_live hides it.
    assert!(table.retire(&1, 5_000));
    assert_eq!(table.get_live(&1, 5_999, window_ns), Some(&10));
    assert_eq!(table.get_live(&1, 6_000, window_ns), None);
    assert_eq!(table.get(&1), Some(&10));

    // Retire remaining slots and stream > 12,000 inserts across advancing windows without reallocating.
    for k in 0..64u64 {
        let _ = table.retire(&k, 5_000);
    }
    for i in 0..12_000u64 {
        let key = 1_000 + i;
        let now = 10_000 + (i / 32) * 2_000;
        table
            .insert_or_evict(key, i, Some(now), now, window_ns)
            .unwrap();
        assert_eq!(table.slots_addr(), initial_slots_addr);
        assert_eq!(table.buckets_addr(), initial_buckets_addr);
    }
    assert!(table.len() <= 64);
}

#[test]
fn account_processes_over_2500_orders_and_20000_fills_without_capacity_or_freeze() {
    let (mut snapshot, mut catalog, base_cmd) = fixture();
    snapshot.valid_until = u64::MAX / 2;
    snapshot.assets[0].free = units(1);
    snapshot.assets[1].free = units(1_000_000_000);
    catalog.entries[0].max_absolute_position = units(100_000);
    catalog.entries[0].max_gross_quantity = units(100_000);
    catalog.entries[0].strategy_limits = vec![(8, units(100_000))];

    let mut account = Account::from_snapshot(snapshot, catalog)
        .unwrap()
        .with_retention_window_ns(Some(1_000));
    assert_eq!(account.retention_window_ns(), Some(1_000));

    // Process 2,600 orders (> 2 * MAX_ORDERS) x 8 partial fills each = 20,800 fills (> MAX_FACTS).
    let total_orders = 2_600u64;
    let fills_per_order = 8u64;
    let mut now = 10_000u64;

    for idx in 1..=total_orders {
        now += 100;
        let mut cmd = base_cmd.clone();
        cmd.context.request.intent_id = idx;
        cmd.context.request.deadline = now + 10_000;
        cmd.context.strategy.strategy_intent_seq = idx;
        let cid_str = format!("ord-{idx}");
        let cid = venue::text(&cid_str);
        if let v::CommandBody::New(ref mut spec) = cmd.body {
            spec.client_id = cid.clone();
            spec.quantity = v::Quantity::Base("0.008".parse().unwrap());
            spec.price = Some("60000".parse().unwrap());
        }

        let canonical_cmd = canonical(cmd.clone());
        let admission = account.admit(&canonical_cmd, now).unwrap();
        let order_key = OrderKey {
            intent: admission.intent(),
            client_id: cid.clone(),
        };

        let mut order_id_bytes = [0u8; 16];
        order_id_bytes[..8].copy_from_slice(&idx.to_le_bytes());
        order_id_bytes[8] = 1;
        let native_id = venue::text(&format!("venue-{idx}"));

        for f_idx in 1..=fills_per_order {
            let trade_id = format!("t-{idx}-{f_idx}");
            let mut ev = fill(&cmd, &trade_id, "0.001", "60000", "60");
            ev.identity.received_mono_ns = now + f_idx;
            if let v::VenueEventBody::Fill(ref mut body) = ev.body {
                body.observation.order.order_id = order_id_bytes;
                body.observation.order.venue_order_id = Some(native_id.clone());
                // Exercise both ClientId-present and ClientId-absent O(log N) resolution paths.
                if f_idx % 2 == 0 {
                    body.observation.order.client_id = None;
                }
                if f_idx == fills_per_order {
                    body.observation.status = v::OrderStatus::Filled;
                    body.observation.cumulative_quantity = Some("0.008".parse().unwrap());
                } else {
                    body.observation.status = v::OrderStatus::PartiallyFilled;
                    body.observation.cumulative_quantity =
                        Some(format!("0.00{f_idx}").parse().unwrap());
                }
            }
            assert_eq!(account.apply_event(&event(ev)).unwrap(), Applied::New);
        }

        assert_eq!(
            account.reconcile_fees(&order_key, f(800_000)).unwrap(),
            Applied::New
        );
        assert!(!account.frozen());
    }

    // Advance past the retention window and evict the final batch.
    let evicted = account.evict_expired(now + 10_000);
    assert!(evicted > 0);
    assert_eq!(account.order_keys().count(), 0);
    assert!(!account.frozen());
}

#[test]
fn active_and_unreconciled_orders_are_never_evicted_regardless_of_age() {
    let (mut snapshot, catalog, base_cmd) = fixture();
    snapshot.valid_until = u64::MAX / 2;
    snapshot.assets[1].free = units(10_000_000);

    let mut account = Account::from_snapshot(snapshot, catalog)
        .unwrap()
        .with_retention_window_ns(Some(1_000));

    // 1. Live resting order with an early partial fill at t = 1_000.
    let mut c1 = base_cmd.clone();
    c1.context.request.intent_id = 1;
    c1.context.request.deadline = 100_000;
    if let v::CommandBody::New(ref mut s) = c1.body {
        s.client_id = venue::text("live-resting");
        s.quantity = v::Quantity::Base("0.002".parse().unwrap());
    }
    let a1 = account.admit(&canonical(c1.clone()), 1_000).unwrap();
    let k1 = a1.orders()[0].clone();
    let mut f1 = fill(&c1, "early-trade-1", "0.001", "60000", "60");
    f1.identity.received_mono_ns = 1_100;
    account.apply_event(&event(f1.clone())).unwrap();

    // 2. Terminal order whose fees are NOT reconciled yet (!fees_closed).
    let mut c2 = base_cmd.clone();
    c2.context.request.intent_id = 2;
    c2.context.request.deadline = 100_000;
    if let v::CommandBody::New(ref mut s) = c2.body {
        s.client_id = venue::text("unclosed-fees");
    }
    let a2 = account.admit(&canonical(c2.clone()), 1_000).unwrap();
    let k2 = a2.orders()[0].clone();
    let mut f2 = fill(&c2, "fill-2", "0.001", "60000", "60");
    f2.identity.received_mono_ns = 1_200;
    if let v::VenueEventBody::Fill(ref mut b) = f2.body {
        b.observation.status = v::OrderStatus::Filled;
        b.observation.cumulative_quantity = Some("0.001".parse().unwrap());
    }
    account.apply_event(&event(f2)).unwrap();
    assert!(account.order(&k2).unwrap().terminal);
    assert!(!account.order(&k2).unwrap().fee_reconciled);

    // 3. Confirmed resting order with an in-flight unconfirmed AmendKeepPriority.
    let mut c3 = base_cmd.clone();
    c3.context.request.intent_id = 3;
    c3.context.request.deadline = 100_000;
    if let v::CommandBody::New(ref mut s) = c3.body {
        s.client_id = venue::text("amend-target");
        s.quantity = v::Quantity::Base("0.003".parse().unwrap());
    }
    let a3 = account.admit(&canonical(c3.clone()), 1_000).unwrap();
    let k3 = a3.orders()[0].clone();
    let mut ack3 = terminal(&c3, "0");
    ack3.identity.received_mono_ns = 1_300;
    if let v::VenueEventBody::Order(ref mut obs) = ack3.body {
        obs.status = v::OrderStatus::New;
        obs.order.order_id = [0x33; 16];
        obs.order.venue_order_id = Some(venue::text("native-3"));
    }
    account.apply_event(&event(ack3)).unwrap();

    let mut amend = base_cmd.clone();
    amend.context.request.intent_id = 4;
    amend.context.request.deadline = 100_000;
    amend.body = v::CommandBody::AmendKeepPriority {
        target: v::OrderTarget {
            order_id: [0x33; 16],
            expected_owner_revision: 1,
            original_intent_exec_epoch: 4,
            client_id: Some(venue::text("amend-target")),
            venue_order_id: Some(venue::text("native-3")),
        },
        new_quantity: "0.002".parse().unwrap(),
        new_client_id: Some(venue::text("amend-target")),
    };
    let a_amend = account.admit(&canonical(amend), 1_400).unwrap();

    // Advance mono_ns by 30 days: NONE of k1, k2, k3, or early-trade-1 may be evicted!
    let thirty_days_ns = 30 * DEFAULT_RETENTION_WINDOW_NS;
    assert_eq!(account.evict_expired(thirty_days_ns), 0);
    assert!(account.order(&k1).is_some());
    assert!(account.order(&k2).is_some());
    assert!(account.order(&k3).is_some());
    assert!(account.is_intent_active(k1.intent));
    assert!(account.is_intent_active(k2.intent));
    assert!(account.is_intent_active(k3.intent));
    assert!(account.is_intent_active(a_amend.intent()));

    // Early trade fact on k1 is still retained and deduplicated after 30 days!
    let mut f1_dup = f1;
    f1_dup.identity.received_mono_ns = thirty_days_ns + 10;
    assert_eq!(
        account.apply_event(&event(f1_dup)).unwrap(),
        Applied::Duplicate
    );

    // Once k2's fees are reconciled and another window passes, ONLY k2 is evicted!
    account.reconcile_fees(&k2, f(100_000)).unwrap();
    assert_eq!(account.evict_expired(thirty_days_ns + 2_000), 1);
    assert!(account.order(&k2).is_none());
    assert!(account.order(&k1).is_some());
    assert!(account.order(&k3).is_some());
}

#[test]
fn management_commands_succeed_when_all_max_orders_slots_hold_live_resting_orders() {
    let (mut snapshot, mut catalog, base_cmd) = fixture();
    snapshot.valid_until = u64::MAX / 2;
    snapshot.assets[1].free = units(500_000_000);
    catalog.entries[0].max_absolute_position = units(100_000);
    catalog.entries[0].max_gross_quantity = units(100_000);
    catalog.entries[0].strategy_limits = vec![(8, units(100_000))];

    let mut account = Account::from_snapshot(snapshot, catalog).unwrap();

    // Occupy all MAX_ORDERS (1,024) slots with live resting Buy orders.
    for idx in 1..=(MAX_ORDERS as u64) {
        let mut cmd = base_cmd.clone();
        cmd.context.request.intent_id = idx;
        cmd.context.request.deadline = 1_000_000;
        if let v::CommandBody::New(ref mut spec) = cmd.body {
            spec.client_id = venue::text(&format!("open-{idx}"));
        }
        account.admit(&canonical(cmd), 100).unwrap();
    }
    assert_eq!(account.order_keys().count(), MAX_ORDERS);

    // A 1,025th order-creating command must fail with Error::Capacity.
    let mut extra = base_cmd.clone();
    extra.context.request.intent_id = (MAX_ORDERS as u64) + 1;
    extra.context.request.deadline = 1_000_000;
    if let v::CommandBody::New(ref mut spec) = extra.body {
        spec.client_id = venue::text("overflow-order");
    }
    assert_eq!(
        account.admit(&canonical(extra), 100).unwrap_err(),
        Error::Capacity
    );

    // Even within the 24h window (no time-window expiry yet), Cancel and Query commands succeed
    // across > MAX_MANAGEMENT_ADMISSIONS (4,096) invocations via dedicated headroom + reclamation!
    let total_mgmt = (MAX_MANAGEMENT_ADMISSIONS as u64) + 512;
    for m_idx in 0..total_mgmt {
        let now = 150 + (m_idx / 10);
        let target_idx = (m_idx % (MAX_ORDERS as u64)) + 1;
        let mut mgmt = base_cmd.clone();
        mgmt.context.request.intent_id = 10_000 + m_idx;
        mgmt.context.request.deadline = now + 10_000;
        let target = v::OrderTarget {
            order_id: [9; 16],
            expected_owner_revision: 1,
            original_intent_exec_epoch: 4,
            client_id: Some(venue::text(&format!("open-{target_idx}"))),
            venue_order_id: None,
        };
        mgmt.body = if m_idx % 2 == 0 {
            v::CommandBody::Cancel(target)
        } else {
            v::CommandBody::Query(target)
        };
        let admitted = account.admit(&canonical(mgmt), now).unwrap();
        assert!(!admitted.increases_risk());
        assert_eq!(
            admitted.intent(),
            IntentKey {
                epoch: 4,
                intent_id: 10_000 + m_idx,
            }
        );
    }
}

#[test]
fn confirmed_amendments_do_not_starve_cancel_or_query_management_headroom() {
    let (mut snapshot, mut catalog, base_cmd) = fixture();
    snapshot.valid_until = u64::MAX / 2;
    snapshot.assets[1].free = units(1_000_000_000);
    catalog.entries[0].max_absolute_position = units(100_000);
    catalog.entries[0].max_gross_quantity = units(100_000);
    catalog.entries[0].strategy_limits = vec![(8, units(100_000))];

    let mut account = Account::from_snapshot(snapshot, catalog).unwrap();

    // Place a single long-lived resting Spot Limit GTC order of 10.00000000 BTC.
    let mut cmd = base_cmd.clone();
    cmd.context.request.intent_id = 1;
    cmd.context.request.deadline = 1_000_000;
    if let v::CommandBody::New(ref mut spec) = cmd.body {
        spec.client_id = venue::text("long-lived-amend");
        spec.quantity = v::Quantity::Base(v::Decimal::new(1_000_000_000, -8).unwrap());
    }
    account.admit(&canonical(cmd.clone()), 100).unwrap();

    let mut ack = terminal(&cmd, "0");
    ack.identity.received_mono_ns = 110;
    let order_identity = if let v::VenueEventBody::Order(ref mut obs) = ack.body {
        obs.status = v::OrderStatus::New;
        obs.order.order_id = [0x44; 16];
        obs.order.venue_order_id = Some(venue::text("native-amend"));
        obs.original_quantity = Some(v::Decimal::new(1_000_000_000, -8).unwrap());
        obs.order.clone()
    } else {
        unreachable!()
    };
    account.apply_event(&event(ack)).unwrap();

    // Confirm MAX_MANAGEMENT_ADMISSIONS + 64 sequential AmendKeepPriority reductions on the same order.
    let total_amends = (MAX_MANAGEMENT_ADMISSIONS as u64) + 64;
    let mut prev_atoms = 1_000_000_000i128;
    for step in 1..=total_amends {
        let next_atoms = prev_atoms - 1_000;
        let prev_dec = v::Decimal::new(prev_atoms, -8).unwrap();
        let next_dec = v::Decimal::new(next_atoms, -8).unwrap();
        let mut amend_cmd = base_cmd.clone();
        amend_cmd.context.request.intent_id = 100 + step;
        amend_cmd.context.request.deadline = 1_000_000;
        amend_cmd.body = v::CommandBody::AmendKeepPriority {
            target: v::OrderTarget {
                order_id: [0x44; 16],
                expected_owner_revision: 1,
                original_intent_exec_epoch: 4,
                client_id: Some(venue::text("long-lived-amend")),
                venue_order_id: Some(venue::text("native-amend")),
            },
            new_quantity: next_dec,
            new_client_id: Some(venue::text("long-lived-amend")),
        };
        let canonical_amend = canonical(amend_cmd);
        let admitted = account.admit(&canonical_amend, 200 + step).unwrap();

        let mut ev = venue::event();
        ev.identity.received_mono_ns = 200 + step;
        ev.body = v::VenueEventBody::Amendment(Box::new(v::AmendmentObservation {
            order: order_identity.clone(),
            request_intent_id: admitted.intent().intent_id,
            request_intent_exec_epoch: admitted.intent().epoch,
            request_digest: canonical_amend.digest(),
            execution_source: v::AmendmentSource::RestResponse,
            execution_id: step,
            previous_quantity: prev_dec,
            new_quantity: next_dec,
            observation: None,
        }));
        assert_eq!(account.apply_event(&event(ev)).unwrap(), Applied::New);
        prev_atoms = next_atoms;
    }

    // Cancel and Query commands still succeed cleanly without Error::Capacity!
    let target = v::OrderTarget {
        order_id: [0x44; 16],
        expected_owner_revision: 1,
        original_intent_exec_epoch: 4,
        client_id: Some(venue::text("long-lived-amend")),
        venue_order_id: Some(venue::text("native-amend")),
    };
    let mut query = base_cmd.clone();
    query.context.request.intent_id = 50_000;
    query.context.request.deadline = 1_000_000;
    query.body = v::CommandBody::Query(target.clone());
    assert!(account.admit(&canonical(query), 10_000).is_ok());

    let mut cancel = base_cmd;
    cancel.context.request.intent_id = 50_001;
    cancel.context.request.deadline = 1_000_000;
    cancel.body = v::CommandBody::Cancel(target);
    assert!(account.admit(&canonical(cancel), 10_001).is_ok());
}
