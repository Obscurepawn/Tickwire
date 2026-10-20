#[cfg(not(target_os = "linux"))]
#[test]
fn production_transport_is_linux_only() {
    assert!(matches!(
        tickwire_control_sys::monotonic_ns(),
        Err(tickwire_control_sys::Error::UnsupportedPlatform)
    ));
}
