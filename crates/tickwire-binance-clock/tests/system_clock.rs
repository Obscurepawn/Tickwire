#![cfg(any(target_os = "linux", target_os = "macos"))]
use tickwire_binance_clock::{ClockDomain, MonotonicClock, SystemClock};

#[test]
fn native_provider_implements_the_production_network_clock_contract() {
    let domain = ClockDomain::new([9; 16], 7).unwrap();
    let mut clock = SystemClock::new(domain).unwrap();
    let first = MonotonicClock::sample(&mut clock).unwrap();
    let second = MonotonicClock::sample(&mut clock).unwrap();
    assert_eq!(first.monotonic.domain(), domain);
    assert_eq!(second.monotonic.domain(), domain);
    assert!(second.monotonic.nanos() >= first.monotonic.nanos());
    assert!(second.instant >= first.instant);
}
