mod common;
use common::*;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};
use tickwire_control_peer::{Error, Result, tls::*};
fn exchange(
    server: PeerTls,
    client: PeerTls,
    check: impl FnOnce(&AuthenticatedPeer) + Send + 'static,
) -> (Result<u64>, Result<u64>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let worker = thread::spawn(move || -> Result<u64> {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut connection = server.server()?;
        while connection.is_handshaking() {
            connection
                .complete_io(&mut socket)
                .map_err(|_| Error::Authentication)?;
        }
        let peer = server.authenticate(&connection, None)?;
        check(&peer);
        connection
            .writer()
            .write_all(b"OKAY")
            .map_err(|_| Error::Transport)?;
        while connection.wants_write() {
            connection
                .write_tls(&mut socket)
                .map_err(|_| Error::Transport)?;
        }
        Ok(peer.node_id())
    });
    let run = || -> Result<u64> {
        let mut socket = TcpStream::connect(address).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut connection = client.client(2)?;
        while connection.is_handshaking() {
            connection
                .complete_io(&mut socket)
                .map_err(|_| Error::Authentication)?;
        }
        let peer = client.authenticate(&connection, Some(2))?;
        let mut bytes = [0; 4];
        loop {
            match connection.reader().read(&mut bytes) {
                Ok(4) => break,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    connection
                        .complete_io(&mut socket)
                        .map_err(|_| Error::Transport)?;
                }
                _ => return Err(Error::Transport),
            }
        }
        assert_eq!(&bytes, b"OKAY");
        Ok(peer.node_id())
    };
    let client = run();
    (worker.join().unwrap(), client)
}
#[test]
fn real_mutual_tls_pins_node_identity_in_both_directions_and_revalidates_revocation() {
    let f = Fixture::new();
    let revoked = PeerTls::new(scope(), 2, f.material(2, Some(1), false), f.peers(2)).unwrap();
    let (s, c) = exchange(f.tls(2), f.tls(1), move |peer| {
        assert_eq!(revoked.revalidate(peer), Err(Error::Authentication))
    });
    assert_eq!(s, Ok(1));
    assert_eq!(c, Ok(2));
}
#[test]
fn wrong_pin_stale_crl_and_untrusted_ca_never_produce_peer_capability() {
    let f = Fixture::new();
    let mut pins = f.peers(2);
    pins[0].leaf_sha256 = [99; 32];
    for server in [
        PeerTls::new(scope(), 2, f.material(2, None, false), pins).unwrap(),
        PeerTls::new(scope(), 2, f.material(2, None, true), f.peers(2)).unwrap(),
        Fixture::new().tls(2),
    ] {
        let (s, _) = exchange(server, f.tls(1), |_| panic!("unauthenticated peer"));
        assert!(s.is_err());
    }
}

#[test]
fn hostname_validation_and_local_identity_material_are_not_bypassed() {
    let f = Fixture::new();
    let mut peers = f.peers(1);
    peers
        .iter_mut()
        .find(|p| p.node_id == 2)
        .unwrap()
        .server_name = "other.invalid".into();
    let bad = PeerTls::new(scope(), 1, f.material(1, None, false), peers).unwrap();
    let (server, client) = exchange(f.tls(2), bad, |_| panic!("wrong server name"));
    assert!(server.is_err());
    assert!(client.is_err());
    assert!(PeerTls::new(scope(), 2, f.material(1, None, false), f.peers(2)).is_err());
    let mut material = f.material(1, None, false);
    material.crls_der.clear();
    assert!(PeerTls::new(scope(), 1, material, f.peers(1)).is_err());
}
