#![cfg(any(target_os = "linux", target_os = "macos"))]
use std::{thread, time::Duration};
use tickwire_clock::ClockDomain;
use tickwire_clock_sys::SystemClock;

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
