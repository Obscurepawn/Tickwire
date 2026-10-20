mod common;
use common::*;
use std::{
    io::Read,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tickwire_binance_private::*;
use tickwire_clock::{CalibrationConfig, Calibrator, Source, TimeUnit};

#[test]
fn prepared_signature_requires_matching_product_environment_and_clock_domain() {
    let key = SigningKey::hmac(SECRET).unwrap();
    let auth = Authentication::SpotSignature {
        api_key: KEY,
        signing_key: &key,
    };
    let token = RequestToken::new(1, 1, Operation::SpotSubscribeSignature).unwrap();
    let mono = MonoTime::new(test_domain(), 100);
    for source in [Source::SpotProduction, Source::UsdmTest] {
        let mut c = calibrated(test_domain(), source, 100, 1_770_000_000_000);
        let stamp = c.sign_time(mono, TimeUnit::Milliseconds).unwrap();
        let sample = ClockSample {
            instant: Instant::now(),
            monotonic: mono,
            signing_time: Some(stamp),
        };
        assert!(
            request::prepare(
                &scope(Market::Spot),
                &auth,
                token,
                &sample,
                5_000_000,
                16384
            )
            .is_err()
        );
    }
    let mut sample = test_sample(100, 1_770_000_000_000);
    sample.monotonic = MonoTime::new(ClockDomain::new([42; 16], 2).unwrap(), 100);
    assert!(
        request::prepare(
            &scope(Market::Spot),
            &auth,
            token,
            &sample,
            5_000_000,
            16384
        )
        .is_err()
    );
}
#[test]
fn an_original_signature_cannot_be_refreshed_using_a_new_clock_domain() {
    let key = SigningKey::hmac(SECRET).unwrap();
    let auth = Authentication::SpotSignature {
        api_key: KEY,
        signing_key: &key,
    };
    let sample = test_sample(100, 1_770_000_000_000);
    let p = request::prepare(
        &scope(Market::Spot),
        &auth,
        RequestToken::new(1, 1, Operation::SpotSubscribeSignature).unwrap(),
        &sample,
        5_000_000,
        16384,
    )
    .unwrap();
    let other = ClockDomain::new([42; 16], 2).unwrap();
    let mono = MonoTime::new(other, 100);
    let mut c = calibrated(other, Source::SpotTest, 100, 1_770_000_000_000);
    let now = ClockSample {
        instant: Instant::now(),
        monotonic: mono,
        signing_time: Some(c.sign_time(mono, TimeUnit::Milliseconds).unwrap()),
    };
    assert!(!p.fresh(&now));
}
struct IntervalClock {
    start: Instant,
    calibrator: Calibrator,
    budget: Arc<AtomicU64>,
    checks: Arc<AtomicU64>,
}
impl Clock for IntervalClock {
    fn sample(&mut self) -> Result<ClockSample> {
        let mono = MonoTime::new(test_domain(), self.start.elapsed().as_nanos() as u64);
        Ok(ClockSample {
            instant: Instant::now(),
            monotonic: mono,
            signing_time: self.calibrator.sign_time(mono, TimeUnit::Milliseconds).ok(),
        })
    }
    fn check_signed(
        &mut self,
        scope: &AccountScope,
        stamp: &SignedTime,
        now: &ClockSample,
        window: u64,
    ) -> Result<()> {
        assert_eq!(scope.market, Market::Spot);
        self.checks.fetch_add(1, Ordering::SeqCst);
        self.calibrator
            .check_send(
                stamp,
                now.monotonic,
                window * 1000,
                self.budget.load(Ordering::SeqCst),
            )
            .map_err(|_| Error::Clock)
    }
}
fn clock_gate(wide: bool) {
    let (tls, server) = certs();
    let l = listener();
    let addr = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let mut s = accept(&l, server);
        upgrade(&mut s, "/ws-api/v3");
        let mut b = [0];
        assert_eq!(s.read(&mut b).unwrap_or(0), 0);
    });
    let key = SigningKey::hmac(SECRET).unwrap();
    let mut cfg = SessionConfig::new(scope(Market::Spot));
    if wide {
        cfg.timing.recv_window_micros = 5_000;
    }
    let mut c = PrivateClient::new(
        cfg,
        Authentication::SpotSignature {
            api_key: KEY,
            signing_key: &key,
        },
        NetworkConfig {
            stream_address: addr,
            management_address: None,
            tls,
        },
        1,
    )
    .unwrap();
    let budget = Arc::new(AtomicU64::new(0));
    let checks = Arc::new(AtomicU64::new(0));
    let mut calibrator = Calibrator::new(
        CalibrationConfig {
            max_round_trip_ns: 1_000_000,
            max_width_ns: 100_000_000,
            max_age_ns: 60_000_000_000,
            quantization_error_ns: if wide { 20_000_000 } else { 1_000_000 },
            drift_ppb: 0,
            max_adjustment_ns: 1_000_000,
        },
        Source::SpotTest,
        test_domain(),
        1,
    )
    .unwrap();
    let t = calibrator.begin(MonoTime::new(test_domain(), 0)).unwrap();
    calibrator
        .finish(&t, MonoTime::new(test_domain(), 0), 1_770_000_000_000)
        .unwrap();
    let mut clock = IntervalClock {
        start: Instant::now(),
        calibrator,
        budget: budget.clone(),
        checks: checks.clone(),
    };
    let mut permit = |q: &Charge, _: &ClockSample| {
        if !wide && matches!(q.kind, ChargeKind::Api(_)) {
            budget.store(10_000_000_000, Ordering::SeqCst);
        }
        true
    };
    let end = Instant::now() + Duration::from_secs(3);
    let mut outcome = None;
    while outcome.is_none() && Instant::now() < end {
        c.poll(&mut clock, PollBudget::default(), &mut permit, |d| {
            if let Delivery::Control(o) = d {
                outcome = Some(o.state);
            }
            Ok(())
        })
        .unwrap();
        thread::yield_now();
    }
    c.stop(StopMode::Disconnect).unwrap();
    drop(c);
    h.join().unwrap();
    assert_eq!(outcome, Some(ControlState::NoBytes));
    assert!(checks.load(Ordering::SeqCst) >= if wide { 1 } else { 2 });
}
#[test]
fn uncertainty_interval_is_checked_before_first_tls_acceptance() {
    clock_gate(true);
}
#[test]
fn delivery_budget_is_rechecked_after_external_permission() {
    clock_gate(false);
}
struct DomainClock {
    start: Instant,
    generation: u64,
}
impl Clock for DomainClock {
    fn sample(&mut self) -> Result<ClockSample> {
        let ns = self.start.elapsed().as_nanos() as u64;
        let domain = ClockDomain::new([42; 16], self.generation).unwrap();
        let mono = MonoTime::new(domain, ns);
        let mut c = calibrated(
            domain,
            Source::SpotTest,
            ns,
            1_770_000_000_000 + ns / 1_000_000,
        );
        Ok(ClockSample {
            instant: Instant::now(),
            monotonic: mono,
            signing_time: Some(c.sign_time(mono, TimeUnit::Milliseconds).unwrap()),
        })
    }
    fn check_signed(
        &mut self,
        s: &AccountScope,
        t: &SignedTime,
        n: &ClockSample,
        w: u64,
    ) -> Result<()> {
        check_test_stamp(s, t, n, w)
    }
}
#[test]
fn changing_clock_domain_permanently_stops_the_current_owner() {
    let (tls, server) = certs();
    let l = listener();
    let addr = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let mut s = accept(&l, server);
        upgrade(&mut s, "/ws-api/v3");
        let q = json(&mut s);
        reply(&mut s, &q["id"], serde_json::json!({"subscriptionId":0}));
        let mut b = [0];
        let _ = s.read(&mut b);
    });
    let key = SigningKey::hmac(SECRET).unwrap();
    let mut c = PrivateClient::new(
        SessionConfig::new(scope(Market::Spot)),
        Authentication::SpotSignature {
            api_key: KEY,
            signing_key: &key,
        },
        NetworkConfig {
            stream_address: addr,
            management_address: None,
            tls,
        },
        3,
    )
    .unwrap();
    let mut clock = DomainClock {
        start: Instant::now(),
        generation: 1,
    };
    let mut permit = |_: &Charge, _: &ClockSample| true;
    let end = Instant::now() + Duration::from_secs(3);
    while c.status().phase != Phase::Receiving && Instant::now() < end {
        c.poll(&mut clock, PollBudget::default(), &mut permit, |_| Ok(()))
            .unwrap();
        thread::yield_now();
    }
    assert_eq!(c.status().phase, Phase::Receiving);
    clock.generation = 2;
    let rejected = c
        .poll(&mut clock, PollBudget::default(), &mut permit, |_| Ok(()))
        .is_err();
    let status = c.status();
    c.stop(StopMode::Disconnect).unwrap();
    drop(c);
    h.join().unwrap();
    assert!(rejected);
    assert_eq!(status.phase, Phase::Stopped);
    assert_eq!(status.connection_epoch, 3);
}

struct FaultClock {
    inner: TestClock,
    armed: Arc<AtomicU64>,
    mode: u64,
    checks: u64,
}
impl Clock for FaultClock {
    fn sample(&mut self) -> Result<ClockSample> {
        if self.mode == 0 && self.armed.swap(0, Ordering::SeqCst) == 1 {
            return Err(Error::ClockDomain);
        }
        self.inner.sample()
    }
    fn check_signed(
        &mut self,
        s: &AccountScope,
        t: &SignedTime,
        n: &ClockSample,
        w: u64,
    ) -> Result<()> {
        self.checks += 1;
        if self.mode == self.checks {
            return Err(Error::ClockDomain);
        }
        self.inner.check_signed(s, t, n, w)
    }
}
#[test]
fn explicit_clock_domain_error_inside_first_acceptance_is_permanent() {
    for mode in 0..3 {
        let (tls, server) = certs();
        let l = listener();
        let addr = l.local_addr().unwrap();
        let h = thread::spawn(move || {
            let mut s = accept(&l, server);
            upgrade(&mut s, "/ws-api/v3");
            let mut b = [0];
            assert_eq!(s.read(&mut b).unwrap_or(0), 0);
        });
        let key = SigningKey::hmac(SECRET).unwrap();
        let mut c = PrivateClient::new(
            SessionConfig::new(scope(Market::Spot)),
            Authentication::SpotSignature {
                api_key: KEY,
                signing_key: &key,
            },
            NetworkConfig {
                stream_address: addr,
                management_address: None,
                tls,
            },
            1,
        )
        .unwrap();
        let armed = Arc::new(AtomicU64::new(0));
        let mut clock = FaultClock {
            inner: TestClock {
                start: Instant::now(),
            },
            armed: armed.clone(),
            mode,
            checks: 0,
        };
        let mut permit = |q: &Charge, _: &ClockSample| {
            if matches!(q.kind, ChargeKind::Api(_)) {
                armed.store(1, Ordering::SeqCst);
            }
            true
        };
        let mut outcome = None;
        let end = Instant::now() + Duration::from_secs(3);
        while outcome.is_none() && Instant::now() < end {
            let _ = c.poll(&mut clock, PollBudget::default(), &mut permit, |d| {
                if let Delivery::Control(o) = d {
                    outcome = Some(o.state);
                }
                Ok(())
            });
            thread::yield_now();
        }
        let phase = c.status().phase;
        c.stop(StopMode::Disconnect).unwrap();
        drop(c);
        h.join().unwrap();
        assert_eq!(outcome, Some(ControlState::NoBytes));
        assert_eq!(phase, Phase::Stopped, "mode {mode}");
    }
}
