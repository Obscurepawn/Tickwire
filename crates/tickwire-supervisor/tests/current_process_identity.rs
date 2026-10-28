#[cfg(target_os = "linux")]
#[test]
fn current_identity_matches_the_calling_process_kernel_credentials() {
    let actual = tickwire_supervisor::current_process_identity().unwrap();
    assert_eq!(actual.pid, std::process::id());
    let status = std::fs::read("/proc/self/status").unwrap();
    for (prefix, expected) in [(b"Uid:".as_slice(), actual.uid), (b"Gid:", actual.gid)] {
        let row = status
            .split(|byte| *byte == b'\n')
            .find_map(|row| row.strip_prefix(prefix))
            .unwrap();
        let values: Vec<u32> = std::str::from_utf8(row)
            .unwrap()
            .split_whitespace()
            .map(|value| value.parse().unwrap())
            .collect();
        assert_eq!(values, [expected; 4]);
    }
    assert_eq!(
        tickwire_supervisor::current_process_identity().unwrap(),
        actual
    );
}

#[cfg(not(target_os = "linux"))]
#[test]
fn current_identity_refuses_unsupported_platforms() {
    assert!(matches!(
        tickwire_supervisor::current_process_identity(),
        Err(tickwire_supervisor::Error::Transport(
            tickwire_control_sys::Error::UnsupportedPlatform
        ))
    ));
}
