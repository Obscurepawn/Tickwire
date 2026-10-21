mod common;
use common::*;
use protobuf::Message as _;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::{ErrorKind, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
use tickwire_config_service::{Config, ObjectLimits, Replica};
use tickwire_control_peer::{
    Error,
    frame::{Decoder, Frame, Header, Kind},
    service::{Limits, PeerBudget, PeerService},
    tls::PeerTls,
};
use tickwire_control_raft::{Message, MessageType, Node};
use tickwire_journal::store::FileStore;

static NEXT: AtomicU64 = AtomicU64::new(1);
struct Server {
    service: Option<PeerService<Replica<FileStore>>>,
    address: SocketAddr,
    path: PathBuf,
}
impl Server {
    fn new(f: &Fixture) -> Self {
        let path = std::env::temp_dir().join(format!(
            "tickwire-peer-reconnect-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let node = Node::create(
            FileStore::open(path.join("node")).unwrap(),
            tickwire_control_raft::Config {
                physical: tickwire_journal::Config::new([3; 16], scope().group),
                node_id: 3,
                voters: [1, 2, 3],
                writer_epoch: 1,
                max_command_bytes: 1024 * 1024,
                max_retained_bytes: 8 * 1024 * 1024,
            },
        )
        .unwrap();
        let replica = Replica::new(
            node,
            Config {
                cluster: scope().cluster,
                objects: ObjectLimits {
                    max_objects: 8,
                    max_inflight: 4,
                    max_payload_bytes: 4 * 1024 * 1024,
                },
            },
        )
        .unwrap();
        let service = PeerService::new(
            replica,
            listener,
            [3; 16],
            BTreeMap::from([
                (1, "127.0.0.1:9".parse().unwrap()),
                (2, "127.0.0.1:9".parse().unwrap()),
            ]),
            f.tls(3),
            Limits::default(),
        )
        .unwrap();
        Self {
            service: Some(service),
            address,
            path,
        }
    }
    fn service(&mut self) -> &mut PeerService<Replica<FileStore>> {
        self.service.as_mut().unwrap()
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.service.take();
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

// Actual nonblocking TLS clients keep both TCP connections open. The replacement
// has the same pinned certificate but a new process incarnation, as after restart.
struct Client {
    socket: TcpStream,
    tls: rustls::Connection,
    trust: PeerTls,
    decoder: Decoder,
    incarnation: [u8; 16],
    challenge: [u8; 16],
    hello: bool,
    session: Option<[u8; 16]>,
}
impl Client {
    fn new(address: SocketAddr, trust: PeerTls, incarnation: [u8; 16]) -> Self {
        let socket = TcpStream::connect(address).unwrap();
        socket.set_nonblocking(true).unwrap();
        socket.set_nodelay(true).unwrap();
        Self {
            socket,
            tls: trust.client(3).unwrap(),
            trust,
            decoder: Decoder::new(4096).unwrap(),
            incarnation,
            challenge: [7; 16],
            hello: false,
            session: None,
        }
    }
    fn send(&mut self, kind: Kind, challenge: [u8; 16], sequence: u64, payload: Vec<u8>) {
        let bytes = Frame {
            header: Header {
                kind,
                cluster: scope().cluster,
                group: scope().group,
                from: 1,
                to: 3,
                incarnation: self.incarnation,
                challenge,
                sequence,
            },
            payload,
        }
        .encode()
        .unwrap();
        self.tls.writer().write_all(&bytes).unwrap();
    }
    fn heartbeat(&mut self, term: u64) {
        let mut message = Message::new();
        message.set_msg_type(MessageType::MsgHeartbeat);
        message.from = 1;
        message.to = 3;
        message.term = term;
        self.send(
            Kind::Raft,
            self.session.unwrap(),
            1,
            message.write_to_bytes().unwrap(),
        );
    }
    fn drive(&mut self) {
        if self.tls.wants_write() {
            match self.tls.write_tls(&mut self.socket) {
                Ok(_) => {}
                Err(e) if e.kind() == ErrorKind::WouldBlock => {}
                Err(e) => panic!("TLS write: {e}"),
            }
        }
        match self.tls.read_tls(&mut self.socket) {
            Ok(0) => return,
            Ok(_) => {
                self.tls.process_new_packets().unwrap();
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => {}
            Err(e) => panic!("TLS read: {e}"),
        }
        if !self.tls.is_handshaking() && !self.hello {
            self.trust.authenticate(&self.tls, Some(3)).unwrap();
            self.send(Kind::Hello, self.challenge, 0, Vec::new());
            self.hello = true;
        }
        let mut bytes = [0; 4096];
        match self.tls.reader().read(&mut bytes) {
            Ok(n) => self.decoder.push(&bytes[..n]).unwrap(),
            Err(e) if e.kind() == ErrorKind::WouldBlock => {}
            Err(e) => panic!("plaintext: {e}"),
        }
        while let Some(frame) = self.decoder.next_frame().unwrap() {
            if frame.header.kind == Kind::HelloAck {
                assert_eq!(frame.payload, self.challenge);
                let mut hash = Sha256::new();
                hash.update(b"tickwire.raft.session.v1");
                hash.update(scope().cluster);
                hash.update(scope().group);
                hash.update(1u64.to_le_bytes());
                hash.update(3u64.to_le_bytes());
                hash.update(self.incarnation);
                hash.update(frame.header.incarnation);
                hash.update(self.challenge);
                hash.update(frame.header.challenge);
                self.session = Some(hash.finalize()[..16].try_into().unwrap());
            }
        }
    }
}
fn wait(mut action: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(2);
    while !action() {
        assert!(Instant::now() < end, "peer transition did not complete");
        std::thread::sleep(Duration::from_millis(1));
    }
}
#[test]
fn duplicate_and_restarted_peer_connections_do_not_stop_the_service() {
    for incarnation in [[1; 16], [2; 16]] {
        let f = Fixture::new();
        let mut server = Server::new(&f);
        let mut old = Client::new(server.address, f.tls(1), [1; 16]);
        wait(|| {
            old.drive();
            let status = server
                .service()
                .poll(Instant::now(), PeerBudget::default())
                .unwrap();
            status.connected == 1 && old.session.is_some()
        });
        let mut replacement = Client::new(server.address, f.tls(1), incarnation);
        wait(|| {
            old.drive();
            replacement.drive();
            let status = server
                .service()
                .poll(Instant::now(), PeerBudget::default())
                .expect("a duplicate authenticated peer must only close its own lane");
            assert_eq!(status.connected, 1);
            status.faults.contains(&(Some(1), Error::Identity))
        });
        assert_eq!(server.service().status().term, 0);
        old.heartbeat(7);
        wait(|| {
            old.drive();
            server
                .service()
                .poll(Instant::now(), PeerBudget::default())
                .unwrap();
            server.service().status().term == 7
        });
        drop(replacement);
        drop(old);
        wait(|| {
            server
                .service()
                .poll(Instant::now(), PeerBudget::default())
                .unwrap()
                .connected
                == 0
        });
        let mut retry = Client::new(server.address, f.tls(1), [3; 16]);
        wait(|| {
            retry.drive();
            let status = server
                .service()
                .poll(Instant::now(), PeerBudget::default())
                .unwrap();
            status.connected == 1 && retry.session.is_some()
        });
        retry.heartbeat(8);
        wait(|| {
            retry.drive();
            server
                .service()
                .poll(Instant::now(), PeerBudget::default())
                .unwrap();
            server.service().status().term == 8
        });
    }
}
