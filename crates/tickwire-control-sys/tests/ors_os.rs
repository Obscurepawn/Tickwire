#![forbid(unsafe_code)]

use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, TcpListener, UdpSocket};
use std::os::fd::AsFd;
use std::time::Duration;
use tickwire_control_sys::{
    Error, MAX_INTERFACE_NAME_LEN, OrsSignal, bind_socket_device, connect_bound_tcp_blocking,
    open_bound_nonblocking_tcp_stream,
};

#[test]
fn bind_socket_device_validates_interface_names_and_invokes_kernel_setsockopt() {
    assert_eq!(MAX_INTERFACE_NAME_LEN, 15);
    assert_eq!(OrsSignal::SigInt.as_str(), "SIGINT");
    assert_eq!(OrsSignal::SigTerm.as_str(), "SIGTERM");
    assert_eq!(OrsSignal::SigUsr1.as_str(), "SIGUSR1");
    assert_eq!(OrsSignal::SigUsr2.as_str(), "SIGUSR2");
    assert!(OrsSignal::SigInt.is_shutdown());
    assert!(OrsSignal::SigTerm.is_shutdown());
    assert!(!OrsSignal::SigUsr1.is_shutdown());
    assert!(!OrsSignal::SigUsr2.is_shutdown());

    let sock = UdpSocket::bind("127.0.0.1:0").unwrap();

    for bad in [
        "",
        "0123456789abcdef", // 16 bytes == IFNAMSIZ -> too long
        "eth01234567890123",
        "eth\0x",
        "eth 0",
        " eth0",
        "eth0\n",
        "eth\t0",
        "eth\x7f",
        "eth\u{00e9}",
    ] {
        assert!(
            matches!(
                bind_socket_device(sock.as_fd(), bad),
                Err(Error::InvalidResources)
            ),
            "expected InvalidResources for {bad:?}"
        );
    }

    #[cfg(target_os = "linux")]
    {
        // Valid 15-byte nonexistent interface name passes validation and hits setsockopt (ENODEV or EPERM).
        let bogus = bind_socket_device(sock.as_fd(), "tw_no_dev_12345");
        assert!(
            matches!(
                bogus,
                Err(Error::System {
                    operation: "setsockopt SO_BINDTODEVICE",
                    ..
                })
            ),
            "expected kernel setsockopt error for nonexistent device, got {bogus:?}"
        );

        // Fresh socket bound to "lo": succeeds on Linux >= 5.7 or returns EPERM in restricted sandboxes.
        let lo_res = bind_socket_device(sock.as_fd(), "lo");
        match lo_res {
            Ok(()) => {}
            Err(Error::System {
                operation: "setsockopt SO_BINDTODEVICE",
                source,
            }) => {
                assert_eq!(source.kind(), std::io::ErrorKind::PermissionDenied);
            }
            other => panic!("unexpected result binding to lo: {other:?}"),
        }
    }
}

#[test]
fn bound_tcp_helpers_connect_exchange_bytes_and_enforce_validation() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = listener.local_addr().unwrap();

    // Family mismatch rejected when interface is None.
    assert!(matches!(
        open_bound_nonblocking_tcp_stream(peer, Some(IpAddr::V6(Ipv6Addr::LOCALHOST)), None),
        Err(Error::InvalidResources)
    ));
    assert!(matches!(
        connect_bound_tcp_blocking(
            peer,
            Some(IpAddr::V6(Ipv6Addr::LOCALHOST)),
            None,
            Duration::from_secs(1)
        ),
        Err(Error::InvalidResources)
    ));

    // Zero timeout rejected cleanly.
    assert!(matches!(
        connect_bound_tcp_blocking(peer, None, None, Duration::ZERO),
        Err(Error::Timeout)
    ));

    // Invalid interface rejected even if local_ip is also provided (interface takes precedence).
    assert!(matches!(
        connect_bound_tcp_blocking(
            peer,
            Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            Some("bad name"),
            Duration::from_secs(1)
        ),
        Err(Error::InvalidResources)
    ));

    #[cfg(target_os = "linux")]
    {
        let mut client = connect_bound_tcp_blocking(
            peer,
            Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            None,
            Duration::from_secs(2),
        )
        .unwrap();
        let (mut srv, from) = listener.accept().unwrap();
        assert_eq!(from.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));

        client.write_all(b"ping").unwrap();
        let mut buf = [0u8; 4];
        srv.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"ping");

        // Nonblocking connect helper produces a socket usable with std/mio.
        let nb =
            open_bound_nonblocking_tcp_stream(peer, Some(IpAddr::V4(Ipv4Addr::LOCALHOST)), None)
                .unwrap();
        let (_srv2, from2) = listener.accept().unwrap();
        assert_eq!(from2.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        drop(nb);
    }
}
