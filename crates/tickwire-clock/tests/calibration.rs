use tickwire_clock::*;

fn domain() -> ClockDomain {
    ClockDomain::new([3; 16], 7).unwrap()
}
fn at(ns: u64) -> MonoTime {
    MonoTime::new(domain(), ns)
}
fn config() -> CalibrationConfig {
    CalibrationConfig {
        max_round_trip_ns: 20 * MILLISECOND,
        max_width_ns: 30 * MILLISECOND,
        max_age_ns: 10 * SECOND,
        quantization_error_ns: MILLISECOND,
        drift_ppb: 0,
        max_adjustment_ns: 5 * MILLISECOND,
    }
}
fn clock() -> Calibrator {
    Calibrator::new(config(), Source::SpotProduction, domain(), 1).unwrap()
}
fn sample(c: &mut Calibrator, sent: u64, received: u64, server_ms: u64) -> Reading {
    let token = c.begin(at(sent)).unwrap();
    c.finish(&token, at(received), server_ms).unwrap()
}

#[test]
fn asymmetric_round_trip_keeps_the_whole_uncertainty_interval() {
    let mut c = clock();
    let r = sample(&mut c, 100 * MILLISECOND, 110 * MILLISECOND, 1000);
    assert_eq!(
        (r.lower_ns(), r.upper_ns()),
        (999 * MILLISECOND, 1011 * MILLISECOND)
    );
    let r = c.read(at(120 * MILLISECOND)).unwrap();
    assert_eq!(
        (r.lower_ns(), r.upper_ns()),
        (1009 * MILLISECOND, 1021 * MILLISECOND)
    );
    let t = c
        .sign_time(at(120 * MILLISECOND), TimeUnit::Milliseconds)
        .unwrap();
    assert_eq!(t.timestamp(), 1015);
}

#[test]
fn drift_rounds_each_bound_outward_instead_of_rounding_to_nearest() {
    let mut cfg = config();
    cfg.drift_ppb = 1;
    let mut c = Calibrator::new(cfg, Source::SpotProduction, domain(), 1).unwrap();
    sample(&mut c, 0, 0, 1000);
    let r = c.read(at(1)).unwrap();
    assert_eq!(r.lower_ns(), 999 * MILLISECOND);
    assert_eq!(r.upper_ns(), 1001 * MILLISECOND + 2);
}

#[test]
fn error_grows_with_age_and_reading_stops_at_the_width_limit() {
    let mut cfg = config();
    cfg.max_width_ns = 2 * MILLISECOND + 2;
    cfg.drift_ppb = 1;
    let mut c = Calibrator::new(cfg, Source::SpotProduction, domain(), 1).unwrap();
    sample(&mut c, 0, 0, 1000);
    assert!(c.read(at(1)).is_ok());
    assert_eq!(c.read(at(SECOND + 1)), Err(Error::Uncertain));
}

#[test]
fn expiry_is_exclusive_and_a_send_budget_must_fit_before_it() {
    let mut cfg = config();
    cfg.max_age_ns = 10 * MILLISECOND;
    let mut c = Calibrator::new(cfg, Source::SpotProduction, domain(), 1).unwrap();
    sample(&mut c, 0, 0, 1000);
    let stamp = c.sign_time(at(0), TimeUnit::Milliseconds).unwrap();
    assert!(
        c.check_send(
            &stamp,
            at(9 * MILLISECOND),
            20 * MILLISECOND,
            MILLISECOND - 1
        )
        .is_ok()
    );
    assert_eq!(
        c.check_send(&stamp, at(9 * MILLISECOND), 20 * MILLISECOND, MILLISECOND),
        Err(Error::Expired)
    );
    assert_eq!(c.read(at(10 * MILLISECOND)), Err(Error::Expired));
}

#[test]
fn transmission_budget_uses_the_server_rate_upper_bound() {
    let mut cfg = config();
    cfg.drift_ppb = 1_000_000;
    let mut c = Calibrator::new(cfg, Source::SpotProduction, domain(), 1).unwrap();
    sample(&mut c, 0, 0, 1000);
    let stamp = c.sign_time(at(0), TimeUnit::Milliseconds).unwrap();
    assert_eq!(
        c.check_send(&stamp, at(0), 2 * MILLISECOND, MILLISECOND),
        Err(Error::TooOld)
    );
    assert!(
        c.check_send(&stamp, at(0), 2 * MILLISECOND + MICROSECOND, MILLISECOND)
            .is_ok()
    );
}

#[test]
fn recv_window_equality_is_allowed_but_one_microsecond_less_is_not() {
    let mut c = clock();
    sample(&mut c, 0, 0, 1000);
    let stamp = c.sign_time(at(0), TimeUnit::Milliseconds).unwrap();
    assert!(
        c.check_send(&stamp, at(5 * MILLISECOND), 6 * MILLISECOND, 0)
            .is_ok()
    );
    assert_eq!(
        c.check_send(
            &stamp,
            at(5 * MILLISECOND),
            6 * MILLISECOND - MICROSECOND,
            0
        ),
        Err(Error::TooOld)
    );
}

#[test]
fn a_new_calibration_rechecks_the_exact_old_timestamp() {
    let mut c = clock();
    sample(&mut c, 0, 0, 1000);
    let stamp = c.sign_time(at(0), TimeUnit::Milliseconds).unwrap();
    sample(&mut c, 2 * MILLISECOND, 2 * MILLISECOND, 1002);
    assert!(
        c.check_send(&stamp, at(2 * MILLISECOND), 3 * MILLISECOND, 0)
            .is_ok()
    );
    assert_eq!(stamp.sample_serial(), 1);
    assert_eq!(c.read(at(2 * MILLISECOND)).unwrap().sample_serial(), 2);
}

#[test]
fn the_one_second_future_boundary_is_strict() {
    let mut cfg = config();
    cfg.max_adjustment_ns = 2 * SECOND;
    let mut c = Calibrator::new(cfg, Source::SpotProduction, domain(), 1).unwrap();
    sample(&mut c, 0, 0, 1999);
    let stamp = c.sign_time(at(0), TimeUnit::Milliseconds).unwrap();
    sample(&mut c, 0, 0, 1000);
    // Current lower bound is 999ms; T=1999ms equals lower+1s.
    assert_eq!(c.check_send(&stamp, at(0), SECOND, 0), Err(Error::Future));
}

#[test]
fn old_replies_are_rejected_before_examining_their_clock_or_payload() {
    let mut c = clock();
    let old = c.begin(at(1)).unwrap();
    c.cancel(&old).unwrap();
    let new = c.begin(at(5)).unwrap();
    assert_eq!(c.finish(&old, at(0), u64::MAX), Err(Error::Stale));
    assert!(c.finish(&new, at(6), 1000).is_ok());
}

#[test]
fn wrong_source_tokens_cannot_close_a_current_sample() {
    let mut a = clock();
    let mut b = Calibrator::new(config(), Source::SpotTest, domain(), 1).unwrap();
    let a_token = a.begin(at(0)).unwrap();
    let b_token = b.begin(at(0)).unwrap();
    assert_eq!(a.finish(&b_token, at(0), 1000), Err(Error::Stale));
    assert!(a.finish(&a_token, at(0), 1000).is_ok());
}

#[test]
fn a_bad_latency_sample_does_not_extend_the_old_expiry() {
    let mut c = clock();
    let first = sample(&mut c, 0, 0, 1000);
    let slow = c.begin(at(MILLISECOND)).unwrap();
    assert_eq!(
        c.finish(&slow, at(22 * MILLISECOND), 1022),
        Err(Error::RoundTrip)
    );
    let r = c.read(at(22 * MILLISECOND)).unwrap();
    assert_eq!(r.valid_until_ns(), first.valid_until_ns());
    assert_eq!(r.sample_serial(), first.sample_serial());
    assert!(c.begin(at(22 * MILLISECOND)).is_ok());
}

#[test]
fn a_rejected_wide_sample_keeps_an_existing_narrow_calibration() {
    let mut cfg = config();
    cfg.max_width_ns = 3 * MILLISECOND;
    let mut c = Calibrator::new(cfg, Source::SpotProduction, domain(), 1).unwrap();
    let first = sample(&mut c, 0, 0, 1000);
    let wide = c.begin(at(MILLISECOND)).unwrap();
    assert_eq!(
        c.finish(&wide, at(3 * MILLISECOND), 1002),
        Err(Error::Uncertain)
    );
    assert_eq!(
        c.read(at(3 * MILLISECOND)).unwrap().valid_until_ns(),
        first.valid_until_ns()
    );
}

#[test]
fn backward_local_time_faults_the_clock_until_a_new_domain_is_created() {
    let mut c = clock();
    sample(&mut c, 1, 2, 1000);
    assert!(c.read(at(3)).is_ok());
    assert_eq!(c.read(at(2)), Err(Error::TimeBackwards));
    assert_eq!(c.status(), CalibrationStatus::Faulted(Error::TimeBackwards));
    assert_eq!(c.begin(at(4)), Err(Error::Faulted));
}

#[test]
fn wrong_clock_domain_faults_a_live_clock() {
    let mut c = clock();
    sample(&mut c, 0, 0, 1000);
    let wrong = ClockDomain::new([3; 16], 8).unwrap();
    assert_eq!(c.read(MonoTime::new(wrong, 1)), Err(Error::Domain));
    assert_eq!(
        c.sign_time(at(2), TimeUnit::Milliseconds),
        Err(Error::Faulted)
    );
}

#[test]
fn a_server_time_jump_revokes_the_old_calibration_and_stamp() {
    let mut c = clock();
    sample(&mut c, 0, 0, 1000);
    let stamp = c.sign_time(at(0), TimeUnit::Milliseconds).unwrap();
    let jumped = c.begin(at(MILLISECOND)).unwrap();
    assert_eq!(
        c.finish(&jumped, at(MILLISECOND), 2000),
        Err(Error::ClockJump)
    );
    assert_eq!(
        c.check_send(&stamp, at(2 * MILLISECOND), SECOND, 0),
        Err(Error::Faulted)
    );
}

#[test]
fn signature_precision_is_explicit_and_usdm_rejects_microseconds() {
    let mut c = clock();
    sample(&mut c, 0, 0, 1000);
    let micros = c.sign_time(at(1_234_567), TimeUnit::Microseconds).unwrap();
    let millis = c.sign_time(at(1_234_567), TimeUnit::Milliseconds).unwrap();
    assert_eq!(micros.timestamp(), 1_001_234);
    assert_eq!(millis.timestamp(), 1_001);
    let mut usdm = Calibrator::new(config(), Source::UsdmProduction, domain(), 1).unwrap();
    sample(&mut usdm, 0, 0, 1000);
    assert_eq!(
        usdm.sign_time(at(0), TimeUnit::Microseconds),
        Err(Error::Invalid)
    );
}

#[test]
fn sample_serial_exhaustion_cannot_reuse_a_token() {
    let mut c = Calibrator::new(config(), Source::SpotProduction, domain(), u64::MAX).unwrap();
    let last = c.begin(at(0)).unwrap();
    assert_eq!(last.serial(), u64::MAX);
    c.finish(&last, at(0), 1000).unwrap();
    assert_eq!(c.begin(at(0)), Err(Error::Overflow));
    assert_eq!(c.read(at(0)), Err(Error::Faulted));
}

#[test]
fn epoch_and_interval_arithmetic_overflow_fault_instead_of_wrapping() {
    let mut c = clock();
    let t = c.begin(at(0)).unwrap();
    assert_eq!(c.finish(&t, at(0), u64::MAX), Err(Error::Overflow));
    assert_eq!(c.status(), CalibrationStatus::Faulted(Error::Overflow));
    let mut c = clock();
    let t = c.begin(at(u64::MAX)).unwrap();
    assert_eq!(c.finish(&t, at(u64::MAX), 1000), Err(Error::Overflow));
}

#[test]
fn no_timestamp_exists_before_calibration_and_only_one_sample_is_pending() {
    let mut c = clock();
    assert_eq!(
        c.sign_time(at(0), TimeUnit::Milliseconds),
        Err(Error::Uncalibrated)
    );
    let first = c.begin(at(0)).unwrap();
    assert_eq!(c.begin(at(0)), Err(Error::Busy));
    c.cancel(&first).unwrap();
    assert_eq!(c.cancel(&first), Err(Error::Stale));
    assert_eq!(c.begin(at(0)).unwrap().serial(), 2);
}

#[test]
fn zero_limits_and_unbounded_rate_error_are_invalid_configuration() {
    for bad in [
        CalibrationConfig {
            max_age_ns: 0,
            ..config()
        },
        CalibrationConfig {
            max_round_trip_ns: 0,
            ..config()
        },
        CalibrationConfig {
            max_width_ns: 0,
            ..config()
        },
        CalibrationConfig {
            quantization_error_ns: 0,
            ..config()
        },
        CalibrationConfig {
            quantization_error_ns: MILLISECOND - 1,
            ..config()
        },
        CalibrationConfig {
            drift_ppb: PPB,
            ..config()
        },
    ] {
        assert!(Calibrator::new(bad, Source::SpotProduction, domain(), 1).is_err());
    }
    assert!(Calibrator::new(config(), Source::SpotProduction, domain(), 0).is_err());
}

#[test]
fn recv_window_precision_matches_each_product() {
    let mut spot = clock();
    sample(&mut spot, 0, 0, 1000);
    let stamp = spot.sign_time(at(0), TimeUnit::Microseconds).unwrap();
    assert!(
        spot.check_send(&stamp, at(0), MILLISECOND + MICROSECOND, 0)
            .is_ok()
    );
    for window in [0, MILLISECOND + 1, 60 * SECOND + MICROSECOND] {
        assert_eq!(
            spot.check_send(&stamp, at(0), window, 0),
            Err(Error::Invalid)
        );
    }
    let mut usdm = Calibrator::new(config(), Source::UsdmTest, domain(), 1).unwrap();
    sample(&mut usdm, 0, 0, 1000);
    let stamp = usdm.sign_time(at(0), TimeUnit::Milliseconds).unwrap();
    assert!(usdm.check_send(&stamp, at(0), MILLISECOND, 0).is_ok());
    assert_eq!(
        usdm.check_send(&stamp, at(0), MILLISECOND + MICROSECOND, 0),
        Err(Error::Invalid)
    );
}

#[test]
fn extrapolation_overflow_revokes_a_still_young_calibration() {
    let mut c = clock();
    sample(&mut c, 0, 0, u64::MAX / MILLISECOND - 1);
    assert_eq!(c.read(at(MILLISECOND)), Err(Error::Overflow));
    assert_eq!(c.status(), CalibrationStatus::Faulted(Error::Overflow));
}

#[test]
fn send_budget_overflow_revokes_calibration() {
    let mut c = clock();
    sample(&mut c, 0, 0, u64::MAX / MILLISECOND - 1);
    let stamp = c.sign_time(at(0), TimeUnit::Milliseconds).unwrap();
    assert_eq!(
        c.check_send(&stamp, at(0), SECOND, MILLISECOND),
        Err(Error::Overflow)
    );
    assert_eq!(c.status(), CalibrationStatus::Faulted(Error::Overflow));

    let mut c = clock();
    let now = u64::MAX - config().max_age_ns;
    sample(&mut c, now, now, 1000);
    let stamp = c.sign_time(at(now), TimeUnit::Milliseconds).unwrap();
    assert_eq!(
        c.check_send(&stamp, at(now), SECOND, config().max_age_ns + 1),
        Err(Error::Overflow)
    );
    assert_eq!(c.status(), CalibrationStatus::Faulted(Error::Overflow));
}

#[test]
fn a_new_sample_after_expiry_does_not_assume_old_clock_continuity() {
    let mut cfg = config();
    cfg.max_age_ns = MILLISECOND;
    let mut c = Calibrator::new(cfg, Source::SpotProduction, domain(), 1).unwrap();
    sample(&mut c, 0, 0, 1000);
    let old = c.sign_time(at(0), TimeUnit::Milliseconds).unwrap();
    sample(&mut c, MILLISECOND, MILLISECOND, 2000);
    assert_eq!(c.read(at(MILLISECOND)).unwrap().sample_serial(), 2);
    assert_eq!(
        c.check_send(&old, at(MILLISECOND), SECOND, 0),
        Err(Error::TooOld)
    );
}

#[test]
fn stamps_from_another_environment_or_domain_do_not_touch_current_time() {
    let mut a = clock();
    sample(&mut a, 10, 10, 1000);
    let mut b = Calibrator::new(config(), Source::SpotTest, domain(), 1).unwrap();
    sample(&mut b, 0, 0, 1000);
    let stamp = b.sign_time(at(0), TimeUnit::Milliseconds).unwrap();
    assert_eq!(a.check_send(&stamp, at(0), SECOND, 0), Err(Error::Stale));
    assert!(a.read(at(10)).is_ok());

    let other = ClockDomain::new([3; 16], 8).unwrap();
    let mut b = Calibrator::new(config(), Source::SpotProduction, other, 1).unwrap();
    let time = MonoTime::new(other, 0);
    let token = b.begin(time).unwrap();
    b.finish(&token, time, 1000).unwrap();
    let stamp = b.sign_time(time, TimeUnit::Milliseconds).unwrap();
    assert_eq!(a.check_send(&stamp, at(0), SECOND, 0), Err(Error::Stale));
    assert!(a.read(at(10)).is_ok());
}
