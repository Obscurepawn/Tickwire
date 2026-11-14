use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use tickwire_quota::rate_limiter::{
    DEFAULT_EXCH_WARN_PCT, IpKey, MAX_SPENDS_PER_CALL, RateFlags, RateLimiter, RateReject,
    RateRule, Spend, VenuePreset, VenueRateLimiter, WindowSpec,
};

const MS: u64 = 1_000_000;
const SEC: u64 = 1_000_000_000;

fn ip4(a: u8, b: u8, c: u8, d: u8) -> IpKey {
    IpKey::from_v4(Ipv4Addr::new(a, b, c, d))
}

#[test]
fn ip_key_losslessly_encodes_ipv4_ipv6_and_none() {
    assert!(IpKey::NONE.is_none());
    assert_eq!(IpKey::NONE.to_ip(), None);
    assert_eq!(IpKey::from_opt_ip(None), IpKey::NONE);

    let v4_zero = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
    let v4_max = IpAddr::V4(Ipv4Addr::BROADCAST);
    let v4_priv = IpAddr::V4(Ipv4Addr::new(10, 24, 0, 7));
    let v6_zero = IpAddr::V6(Ipv6Addr::UNSPECIFIED);
    let v6_loop = IpAddr::V6(Ipv6Addr::LOCALHOST);
    let v6_max = IpAddr::V6(Ipv6Addr::from_bits(u128::MAX));
    let v6_doc = IpAddr::V6("2001:db8::dead:beef".parse().unwrap());

    for addr in [v4_zero, v4_max, v4_priv, v6_zero, v6_loop, v6_max, v6_doc] {
        let key = IpKey::from_ip(addr);
        assert!(!key.is_none());
        assert_ne!(key, IpKey::NONE);
        assert_eq!(key.to_ip(), Some(addr));
        assert_eq!(IpKey::from_opt_ip(Some(addr)), key);
    }

    assert_ne!(IpKey::from_ip(v4_zero), IpKey::from_ip(v6_zero));
}

#[test]
fn fixed_window_epoch_rollover_clock_regression_and_put_back_rollback() {
    let mut limiter = RateLimiter::new();
    let ip_a = ip4(10, 0, 1, 1);

    let w1 = limiter
        .add(RateRule::account(42, 10, RateFlags::ORDER, 100))
        .unwrap()
        .id();
    let w2 = limiter
        .add(RateRule::account(42, 86_400, RateFlags::ORDER, 200_000))
        .unwrap()
        .id();
    let w3 = limiter
        .add(RateRule::ip(
            ip_a,
            60,
            RateFlags::REQUEST | RateFlags::WEIGHT,
            10,
        ))
        .unwrap()
        .id();

    // Spend 9 units on w3 and 2 orders on w1, w2 inside epoch 0 (t = 3.5s).
    limiter
        .allow(
            &[Spend::new(w1, 2), Spend::new(w2, 2), Spend::new(w3, 9)],
            3_500 * MS,
        )
        .unwrap();
    assert_eq!(limiter.window_used(w1, 3_500 * MS), Some(2));
    assert_eq!(limiter.window_used(w2, 3_500 * MS), Some(2));
    assert_eq!(limiter.window_used(w3, 3_500 * MS), Some(9));

    // Monotonic clock regression must be rejected without mutating any counter.
    assert_eq!(
        limiter.allow(&[Spend::new(w1, 1)], 3_499 * MS),
        Err(RateReject::ClockRegression)
    );
    assert_eq!(limiter.window_used(w1, 3_500 * MS), Some(2));

    // Third spend in [w1, w2, w3] exceeds w3's limit (9 + 2 > 10): w1 and w2 must roll back to 2!
    let err = limiter
        .allow(
            &[Spend::new(w1, 5), Spend::new(w2, 5), Spend::new(w3, 2)],
            4_000 * MS,
        )
        .unwrap_err();
    assert_eq!(
        err,
        RateReject::Exhausted {
            id: w3,
            limit: 10,
            used: 9,
            requested: 2,
            retry_after_ns: 56 * SEC,
        }
    );
    assert_eq!(limiter.window_used(w1, 4_000 * MS), Some(2));
    assert_eq!(limiter.window_used(w2, 4_000 * MS), Some(2));
    assert_eq!(limiter.window_used(w3, 4_000 * MS), Some(9));

    // Duplicate window entries in one batch [w1, w1, w3] must also roll back completely to 2.
    assert!(
        limiter
            .allow(
                &[Spend::new(w1, 3), Spend::new(w1, 4), Spend::new(w3, 2)],
                4_500 * MS,
            )
            .is_err()
    );
    assert_eq!(limiter.window_used(w1, 4_500 * MS), Some(2));

    // Exhaust w1 to 100 at t = 9_999ms, then verify exact rollover at t = 10_000ms.
    limiter.allow(&[Spend::new(w1, 98)], 9_999 * MS).unwrap();
    assert_eq!(
        limiter.allow(&[Spend::new(w1, 1)], 9_999 * MS),
        Err(RateReject::Exhausted {
            id: w1,
            limit: 100,
            used: 100,
            requested: 1,
            retry_after_ns: MS,
        })
    );
    limiter
        .allow(&[Spend::new(w1, 1), Spend::new(w2, 1)], 10_000 * MS)
        .unwrap();
    assert_eq!(limiter.window_used(w1, 10_000 * MS), Some(1));
    // w2 (86400s) is still in day epoch 0, so it accumulated 2 + 1 = 3.
    assert_eq!(limiter.window_used(w2, 10_000 * MS), Some(3));

    // Invalid spend validation: empty, > MAX_SPENDS_PER_CALL, or zero units.
    assert_eq!(limiter.allow(&[], 10_000 * MS), Err(RateReject::Invalid));
    assert_eq!(
        limiter.allow(&[Spend::new(w1, 0)], 10_000 * MS),
        Err(RateReject::Invalid)
    );
    let too_many = [Spend::new(w1, 1); MAX_SPENDS_PER_CALL + 1];
    assert_eq!(
        limiter.allow(&too_many, 10_000 * MS),
        Err(RateReject::Invalid)
    );
}

#[test]
fn rolling_window_sliding_expiration_burst_coalescing_rollback_and_zero_reallocation() {
    let mut limiter = RateLimiter::with_config(16, DEFAULT_EXCH_WARN_PCT).unwrap();
    let sym_order = RateRule::symbol(7, 2, RateFlags::ORDER | RateFlags::ROLLING, 10, 1001).id;
    let guard_cap = RateRule::ip(ip4(10, 0, 2, 1), 60, RateFlags::REQUEST, 5).id;

    limiter
        .register_window(WindowSpec::with_ring_capacity(sym_order, 10, 64))
        .unwrap();
    limiter
        .register_window(WindowSpec::new(guard_cap, 5))
        .unwrap();

    let initial_win_cap = limiter.window_capacity();
    let initial_ring_id = limiter
        .rolling_ring_allocation_identity(sym_order)
        .expect("sym_order is rolling");

    // Take 4 units at t = 0ms, 3 units at t = 500ms, and 3 coalesced bursts of 1 unit at t = 1000ms.
    limiter.allow(&[Spend::new(sym_order, 4)], 0).unwrap();
    limiter
        .allow(&[Spend::new(sym_order, 3)], 500 * MS)
        .unwrap();
    for _ in 0..3 {
        limiter
            .allow(&[Spend::new(sym_order, 1)], 1_000 * MS)
            .unwrap();
    }
    assert_eq!(limiter.window_used(sym_order, 1_999 * MS), Some(10));

    // At t = 1999ms window is still 100% full; retry_after_ns must be 1ms (until t = 2000ms).
    assert_eq!(
        limiter.allow(&[Spend::new(sym_order, 2)], 1_999 * MS),
        Err(RateReject::Exhausted {
            id: sym_order,
            limit: 10,
            used: 10,
            requested: 2,
            retry_after_ns: MS,
        })
    );
    // Asking for 6 units at t = 1999ms requires freeing BOTH t=0 (4) and t=500ms (3) -> expires at 2500ms (retry_after = 501ms).
    assert_eq!(
        limiter.can_afford(&[Spend::new(sym_order, 6)], 1_999 * MS),
        Err(RateReject::Exhausted {
            id: sym_order,
            limit: 10,
            used: 10,
            requested: 6,
            retry_after_ns: 501 * MS,
        })
    );

    // At exact t = 2000ms, the t = 0 slice (4 units) drops, leaving 6 used and freeing 4 units.
    assert_eq!(limiter.window_used(sym_order, 2_000 * MS), Some(6));
    limiter
        .allow(&[Spend::new(sym_order, 2)], 2_000 * MS)
        .unwrap();
    assert_eq!(limiter.window_used(sym_order, 2_000 * MS), Some(8));

    // Drain guard_cap (5 units), then test rollback of both a same-ns coalesced spend AND a new-ts pushed spend!
    limiter
        .allow(&[Spend::new(guard_cap, 5)], 2_000 * MS)
        .unwrap();

    // Same-ns (t = 2000ms) coalesces into tail of sym_order, then guard_cap fails -> must roll back to 8!
    assert!(
        limiter
            .allow(
                &[Spend::new(sym_order, 2), Spend::new(guard_cap, 1)],
                2_000 * MS,
            )
            .is_err()
    );
    assert_eq!(limiter.window_used(sym_order, 2_000 * MS), Some(8));

    // New timestamp (t = 2100ms) pushes a new slot onto sym_order, then guard_cap fails -> must pop back to 8!
    assert!(
        limiter
            .allow(
                &[Spend::new(sym_order, 2), Spend::new(guard_cap, 1)],
                2_100 * MS,
            )
            .is_err()
    );
    assert_eq!(limiter.window_used(sym_order, 2_100 * MS), Some(8));

    // Run 2,000 sliding cycles and confirm zero reallocation of both the window table and ring slice.
    for step in 0..2_000u64 {
        let t = (5_000 + step * 300) * MS;
        limiter.allow(&[Spend::new(sym_order, 1)], t).unwrap();
    }
    assert_eq!(limiter.window_capacity(), initial_win_cap);
    assert_eq!(
        limiter.rolling_ring_allocation_identity(sym_order),
        Some(initial_ring_id)
    );
}

#[test]
fn binance_spot_order_exhaustion_preserves_cancels_queries_and_multi_ip_isolation() {
    let ip_a = ip4(192, 0, 2, 10);
    let ip_b = ip4(192, 0, 2, 11);
    let mut venue = VenueRateLimiter::new(VenuePreset::BinanceSpot, 9001, &[ip_a, ip_b]).unwrap();

    // Exhaust 100 / 10s account ORDER cap across ip_a and ip_b (50 each).
    for i in 0..50 {
        venue.allow_place(ip_a, 1, 1, i * MS).unwrap();
        venue.allow_place(ip_b, 1, 1, i * MS).unwrap();
    }

    // 101st order placement is rejected, while cancels, non-order amends, and queries keep succeeding!
    assert!(matches!(
        venue.allow_place(ip_a, 1, 1, 100 * MS),
        Err(RateReject::Exhausted { limit: 100, .. })
    ));
    for i in 0..25 {
        venue.allow_cancel(ip_a, 1, 1, (101 + i) * MS).unwrap();
        venue.allow_query(ip_a, 4, (101 + i) * MS).unwrap();
        venue
            .allow_modify(ip_b, 1, 4, false, (101 + i) * MS)
            .unwrap();
    }

    // Advance to next 10s epoch (t = 10s): exhaust ip_a's 6000/60s weight via queries.
    let ip_a_id = venue.ip_window_id(ip_a).unwrap();
    let used_a = venue.limiter().window_used(ip_a_id, 10 * SEC).unwrap();
    let rem_a = (6_000 - used_a) as u32;
    venue.allow_query(ip_a, rem_a, 10 * SEC).unwrap();
    assert_eq!(venue.limiter().window_used(ip_a_id, 10 * SEC), Some(6_000));

    // Attempting allow_place on exhausted ip_a must fail AND roll back both account ORDER windows!
    let ord_10s = venue.account_order_window_ids()[0];
    let ord_day = venue.account_order_window_ids()[1];
    assert_eq!(venue.limiter().window_used(ord_10s, 10 * SEC), Some(0));
    assert_eq!(venue.limiter().window_used(ord_day, 10 * SEC), Some(100));

    assert!(matches!(
        venue.allow_place(ip_a, 1, 1, 10 * SEC),
        Err(RateReject::Exhausted { limit: 6_000, .. })
    ));
    assert_eq!(venue.limiter().window_used(ord_10s, 10 * SEC), Some(0));
    assert_eq!(venue.limiter().window_used(ord_day, 10 * SEC), Some(100));

    // Sibling ip_b immediately places, cancels, and queries!
    assert!(venue.can_afford_place(ip_b, 1, 1, 10 * SEC).is_ok());
    assert!(venue.can_afford_place(ip_a, 1, 1, 10 * SEC).is_err());
    venue.allow_place(ip_b, 1, 1, 10 * SEC).unwrap();
    venue.allow_cancel(ip_b, 1, 1, 10 * SEC).unwrap();
    venue.allow_query(ip_b, 4, 10 * SEC).unwrap();
    assert_eq!(venue.limiter().window_used(ord_10s, 10 * SEC), Some(1));
    assert_eq!(venue.limiter().window_used(ord_day, 10 * SEC), Some(101));

    // Per-IP 429 cooldown on ip_b isolates strictly to ip_b; at t = 60s ip_a's weight resets and works!
    venue.note_ip_cooldown(ip_b, 70 * SEC).unwrap();
    assert!(matches!(
        venue.allow_place(ip_b, 1, 1, 60 * SEC),
        Err(RateReject::IpCooldown { local_ip, .. }) if local_ip == ip_b
    ));
    venue.allow_place(ip_a, 1, 1, 60 * SEC).unwrap();
}

#[test]
fn binance_usdm_bybit_spot_and_okx_spot_presets_enforce_action_class_and_symbol_isolation() {
    let ip_a = ip4(198, 51, 100, 1);
    let ip_b = ip4(198, 51, 100, 2);

    // 1. Binance USD-M: 300/10s & 1200/60s ORDER + 2400/60s IP weight (supports WS weight == 0).
    let mut usdm = VenueRateLimiter::new(VenuePreset::BinanceUsdm, 501, &[ip_a, ip_b]).unwrap();
    for i in 0..300 {
        // Alternate WS (weight 0) and REST (weight 1).
        let w = (i % 2) as u32;
        usdm.allow_place(ip_a, 10, w, i * MS).unwrap();
    }
    assert!(matches!(
        usdm.allow_place(ip_a, 10, 0, 500 * MS),
        Err(RateReject::Exhausted { limit: 300, .. })
    ));
    // Both WS (weight 0) and REST (weight 1) cancels plus queries succeed while ORDER is exhausted!
    usdm.allow_cancel(ip_a, 10, 0, 500 * MS).unwrap();
    usdm.allow_cancel(ip_a, 10, 1, 500 * MS).unwrap();
    usdm.allow_query(ip_a, 1, 500 * MS).unwrap();
    // IP cooldown still blocks WS weight == 0 on ip_a while ip_b works!
    usdm.note_ip_cooldown(ip_a, 5 * SEC).unwrap();
    assert!(matches!(
        usdm.allow_cancel(ip_a, 10, 0, SEC),
        Err(RateReject::IpCooldown { .. })
    ));
    usdm.allow_cancel(ip_b, 10, 0, SEC).unwrap();

    // 2. Bybit Spot: 20/1s ORDER, 20/1s CANCEL, 50/1s REQUEST, 600/5s IP REQUEST (all Rolling).
    let mut bybit = VenueRateLimiter::new(VenuePreset::BybitSpot, 502, &[ip_a, ip_b]).unwrap();
    for i in 0..20 {
        bybit.allow_place(ip_a, 1, 1, i * MS).unwrap();
    }
    assert!(matches!(
        bybit.allow_place(ip_a, 1, 1, 500 * MS),
        Err(RateReject::Exhausted { limit: 20, .. })
    ));
    for i in 0..20 {
        bybit.allow_cancel(ip_a, 1, 1, (500 + i) * MS).unwrap();
    }
    assert!(matches!(
        bybit.allow_cancel(ip_a, 1, 1, 600 * MS),
        Err(RateReject::Exhausted { limit: 20, .. })
    ));
    // Account REQUEST (50/1s) is still completely unaffected by exhausted ORDER and CANCEL!
    for i in 0..50 {
        bybit.allow_query(ip_a, 1, (600 + i) * MS).unwrap();
    }
    assert!(matches!(
        bybit.allow_query(ip_a, 1, 700 * MS),
        Err(RateReject::Exhausted { limit: 50, .. })
    ));

    // 3. OKX Spot: 1000/2s Account ORDER, 60/2s Symbol ORDER, 60/2s Symbol CANCEL, 20/2s Account REQUEST, 600/2s IP.
    let mut okx = VenueRateLimiter::new(VenuePreset::OkxSpot, 503, &[ip_a, ip_b]).unwrap();
    let win_cap_before = okx.limiter().window_capacity();
    let btc = 101u64;
    let eth = 102u64;
    for i in 0..60 {
        okx.allow_place(ip_a, btc, 1, i * MS).unwrap();
    }
    // BTC symbol order cap (60/2s) is exhausted, but BTC cancel, ETH place, and Account queries all succeed!
    assert!(matches!(
        okx.allow_place(ip_a, btc, 1, 200 * MS),
        Err(RateReject::Exhausted { limit: 60, .. })
    ));
    okx.allow_cancel(ip_a, btc, 1, 200 * MS).unwrap();
    okx.allow_place(ip_a, eth, 1, 200 * MS).unwrap();
    okx.allow_query(ip_a, 1, 200 * MS).unwrap();
    // Preallocated symbol pool prevented outer window table reallocation even on unseen symbols.
    assert_eq!(okx.limiter().window_capacity(), win_cap_before);
}

#[test]
fn reconcile_exchange_usage_raises_conservative_floor_and_triggers_80_pct_warning() {
    let ip_a = ip4(203, 0, 113, 5);
    let mut venue = VenueRateLimiter::new(VenuePreset::BinanceSpot, 777, &[ip_a]).unwrap();
    let ip_win = venue.ip_window_id(ip_a).unwrap();

    venue.allow_query(ip_a, 100, SEC).unwrap();

    // Lower exchange header (50 < local 100) never lowers local conservative counter.
    let r1 = venue.reconcile_exchange_usage(ip_win, 50, 2 * SEC).unwrap();
    assert_eq!(r1.previous_used, 100);
    assert_eq!(r1.effective_used, 100);
    assert!(!r1.warn);
    assert!(!r1.newly_warned);

    // 4799 / 6000 is just below 80% (4800): raises floor to 4799 without warning.
    let r2 = venue
        .reconcile_exchange_usage(ip_win, 4_799, 3 * SEC)
        .unwrap();
    assert_eq!(r2.effective_used, 4_799);
    assert!(!r2.warn);
    assert!(!r2.newly_warned);

    // 4800 / 6000 hits exact 80% threshold: warn = true and newly_warned = true.
    let r3 = venue
        .reconcile_exchange_usage(ip_win, 4_800, 4 * SEC)
        .unwrap();
    assert_eq!(r3.effective_used, 4_800);
    assert!(r3.warn);
    assert!(r3.newly_warned);

    // Subsequent 5000 / 6000 within same epoch stays in warn = true, newly_warned = false.
    let r4 = venue
        .reconcile_exchange_usage(ip_win, 5_000, 5 * SEC)
        .unwrap();
    assert_eq!(r4.effective_used, 5_000);
    assert!(r4.warn);
    assert!(!r4.newly_warned);

    // Remaining headroom on ip_a is now 1000 (not 5900), enforced immediately by allow_query.
    assert!(venue.allow_query(ip_a, 1_001, 6 * SEC).is_err());
    venue.allow_query(ip_a, 1_000, 6 * SEC).unwrap();
}

#[test]
fn can_afford_matches_allow_across_duplicate_spends_large_coalescing_and_presets() {
    // 1. Duplicate RateLimitIds in one Spend slice report identical Exhausted.used on Fixed & Rolling windows!
    let mut lim = RateLimiter::new();
    let w_fix = lim
        .add(RateRule::account(88, 10, RateFlags::ORDER, 10))
        .unwrap()
        .id();
    let w_rol = lim
        .add(RateRule::account(
            88,
            10,
            RateFlags::CANCEL | RateFlags::ROLLING,
            10,
        ))
        .unwrap()
        .id();
    lim.allow(&[Spend::new(w_fix, 3), Spend::new(w_rol, 3)], SEC)
        .unwrap();

    let dup_fix = [Spend::new(w_fix, 6), Spend::new(w_fix, 6)];
    let cf_fix = lim.can_afford(&dup_fix, 2 * SEC);
    let al_fix = lim.allow(&dup_fix, 2 * SEC).map(|_| ());
    assert_eq!(cf_fix, al_fix);
    assert_eq!(lim.window_used(w_fix, 2 * SEC), Some(3));

    let dup_rol = [Spend::new(w_rol, 6), Spend::new(w_rol, 6)];
    let cf_rol = lim.can_afford(&dup_rol, 2 * SEC);
    let al_rol = lim.allow(&dup_rol, 2 * SEC).map(|_| ());
    assert_eq!(cf_rol, al_rol);
    assert_eq!(lim.window_used(w_rol, 2 * SEC), Some(3));

    // 2. RollingWindow coalescing beyond u32::MAX when limit > u32::MAX agrees with can_afford and rolls back!
    let mut big_lim = RateLimiter::new();
    let big_id = RateRule::ip(
        ip4(10, 9, 0, 1),
        60,
        RateFlags::REQUEST | RateFlags::WEIGHT | RateFlags::ROLLING,
        10_000_000_000,
    )
    .id;
    let guard_id = RateRule::account(88, 10, RateFlags::ORDER, 1).id;
    big_lim
        .register_window(WindowSpec::with_ring_capacity(big_id, 10_000_000_000, 16))
        .unwrap();
    big_lim
        .register_window(WindowSpec::new(guard_id, 1))
        .unwrap();

    let big_spends = [Spend::new(big_id, u32::MAX), Spend::new(big_id, 10)];
    assert_eq!(big_lim.can_afford(&big_spends, SEC), Ok(()));
    assert!(big_lim.allow(&big_spends, SEC).is_ok());
    assert_eq!(
        big_lim.window_used(big_id, SEC),
        Some(u64::from(u32::MAX) + 10)
    );

    // Exhaust guard_id, then verify u64 coalesced spend rolls back cleanly on later failure!
    big_lim.allow(&[Spend::new(guard_id, 1)], SEC).unwrap();
    let fail_big = [Spend::new(big_id, 500), Spend::new(guard_id, 1)];
    assert_eq!(
        big_lim.can_afford(&fail_big, SEC),
        big_lim.allow(&fail_big, SEC).map(|_| ())
    );
    assert_eq!(
        big_lim.window_used(big_id, SEC),
        Some(u64::from(u32::MAX) + 10)
    );

    // 3. BinanceUsdm weight == 0 on clock regression agrees between can_afford_* and allow_*!
    let ip_a = ip4(192, 0, 2, 55);
    let mut usdm = VenueRateLimiter::new(VenuePreset::BinanceUsdm, 701, &[ip_a]).unwrap();
    usdm.allow_query(ip_a, 1, 5 * SEC).unwrap();
    assert_eq!(
        usdm.can_afford_cancel(ip_a, 1, 0, 4 * SEC),
        Err(RateReject::ClockRegression)
    );
    assert_eq!(
        usdm.allow_cancel(ip_a, 1, 0, 4 * SEC),
        Err(RateReject::ClockRegression)
    );
    assert_eq!(
        usdm.can_afford_modify(ip_a, 1, 0, false, 4 * SEC),
        Err(RateReject::ClockRegression)
    );
    assert_eq!(
        usdm.allow_modify(ip_a, 1, 0, false, 4 * SEC),
        Err(RateReject::ClockRegression)
    );

    // 4. Unseen OkxSpot symbol edge cases (order_units == 0, weight == 0, order_units > 60) match allow_place_orders!
    let mut okx = VenueRateLimiter::new(VenuePreset::OkxSpot, 702, &[ip_a]).unwrap();
    assert_eq!(
        okx.can_afford_place_orders(ip_a, 999, 0, 1, SEC),
        Err(RateReject::Invalid)
    );
    assert_eq!(
        okx.can_afford_place_orders(ip_a, 999, 1, 0, SEC),
        Err(RateReject::Invalid)
    );
    let over_cf = okx.can_afford_place_orders(ip_a, 999, 61, 1, SEC);
    let over_al = okx.allow_place_orders(ip_a, 999, 61, 1, SEC).map(|_| ());
    assert_eq!(over_cf, over_al);

    // 5. u64::MAX limit windows reject overflow via Exhausted without saturating_add bypass or panic!
    let mut max_lim = RateLimiter::new();
    let max_fix = RateRule::account(89, 10, RateFlags::ORDER, u64::MAX).id;
    max_lim
        .register_window(WindowSpec::new(max_fix, u64::MAX))
        .unwrap();
    let _ = max_lim
        .reconcile_exchange_usage(max_fix, u64::MAX - 2, SEC)
        .unwrap();
    let cf = max_lim.can_afford(&[Spend::new(max_fix, 5)], SEC);
    let al = max_lim.allow(&[Spend::new(max_fix, 5)], SEC).map(|_| ());
    assert_eq!(cf, al);
    assert!(matches!(cf, Err(RateReject::Exhausted { .. })));
}
