#![cfg(any(target_os = "linux", target_os = "macos"))]
use std::{thread, time::Duration};
use tickwire_clock::ClockDomain;
use tickwire_clock_sys::{SystemClock, TscCalibration, rdtsc, rdtsc_serialized};

#[test]
fn actual_native_clock_keeps_its_domain_and_monotonic_order() {
    let domain = ClockDomain::new([7; 16], 3).unwrap();
    let mut clock = SystemClock::new(domain).unwrap();
    assert_eq!(clock.domain(), domain);
    let first = clock.sample().unwrap();
    let second = clock.sample().unwrap();
    assert_eq!(first.monotonic.domain(), domain);
    assert_eq!(second.monotonic.domain(), domain);
    assert!(second.monotonic.nanos() >= first.monotonic.nanos());
    assert!(second.instant >= first.instant);
}

#[test]
fn actual_native_clock_advances_while_the_thread_is_parked() {
    let mut clock = SystemClock::new(ClockDomain::new([8; 16], 1).unwrap()).unwrap();
    let first = clock.sample().unwrap();
    thread::sleep(Duration::from_millis(2));
    let second = clock.sample().unwrap();
    assert!(second.monotonic.nanos() - first.monotonic.nanos() >= 2_000_000);
}

#[test]
fn rdtsc_monotonically_advances_and_integer_calibration_converts_accurately() {
    assert_eq!(TscCalibration::from_cycles_per_us(0), None);
    assert_eq!(TscCalibration::from_cycles_per_ms(999), None);

    let fixed = TscCalibration::from_cycles_per_us(3_200).unwrap();
    assert_eq!(fixed.cycles_per_us(), 3_200);
    assert_eq!(fixed.cycles_per_ms(), 3_200_000);
    assert_eq!(fixed.us_to_cycles(10), 32_000);
    assert_eq!(fixed.ns_to_cycles(500), 1_600);
    assert_eq!(fixed.cycles_to_ns(3_200), 1_000);
    assert_eq!(fixed.cycles_to_us(32_000), 10);

    let c0 = rdtsc_serialized();
    let mut prev = rdtsc();
    for _ in 0..64 {
        let next = rdtsc_serialized();
        assert!(next >= prev);
        prev = next;
    }
    thread::sleep(Duration::from_millis(1));
    let c1 = rdtsc_serialized();
    assert!(c1 > c0);

    let cal = TscCalibration::calibrate_default();
    assert!(cal.cycles_per_ms() >= 1_000);
    assert!(cal.cycles_per_us() >= 1);
    let one_ms_cycles = cal.us_to_cycles(1_000);
    assert_eq!(cal.cycles_to_us(one_ms_cycles), 1_000);
}
