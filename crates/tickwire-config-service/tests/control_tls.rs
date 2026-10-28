use rcgen::{
    BasicConstraints, Certificate, CertificateParams, CertificateRevocationListParams,
    ExtendedKeyUsagePurpose, IsCa, Issuer, KeyIdMethod, KeyPair, KeyUsagePurpose,
    RevokedCertParams, SanType, SerialNumber,
};
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use tickwire_config::{Candidate, Format, Limits, changes::ChangeSet};
use tickwire_config_service::authorization::RoleBindings;
use tickwire_config_service::{Error, Hash, Result, auth::*};
use time::OffsetDateTime;

const SUBJECT: &str = "spiffe://tickwire.test/control/operator";
#[allow(dead_code)]
mod deployment_support;
struct Fixture {
    issuer: Issuer<'static, KeyPair>,
    root: Certificate,
    server_cert: Certificate,
    server_key: KeyPair,
    client_cert: Certificate,
    client_key: KeyPair,
}
impl Fixture {
    fn new(subjects: &[&str], expired: bool) -> Self {
        let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca_key = KeyPair::generate().unwrap();
        let root = ca.self_signed(&ca_key).unwrap();
        let issuer = Issuer::new(ca, ca_key);
        let server_key = KeyPair::generate().unwrap();
        let mut server = CertificateParams::new(vec!["localhost".into()]).unwrap();
        server.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_cert = server.signed_by(&server_key, &issuer).unwrap();
        let client_key = KeyPair::generate().unwrap();
        let mut client = CertificateParams::new(Vec::<String>::new()).unwrap();
        client.subject_alt_names = subjects
            .iter()
            .map(|s| SanType::URI((*s).try_into().unwrap()))
            .collect();
        client.serial_number = Some(SerialNumber::from(7u64));
        client.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let now = OffsetDateTime::now_utc();
        client.not_before = now - time::Duration::hours(2);
        client.not_after = if expired {
            now - time::Duration::hours(1)
        } else {
            now + time::Duration::hours(1)
        };
        let client_cert = client.signed_by(&client_key, &issuer).unwrap();
        Self {
            issuer,
            root,
            server_cert,
            server_key,
            client_cert,
            client_key,
        }
    }
    fn material(&self, revoked: bool, stale_crl: bool) -> IdentityMaterial {
        let now = OffsetDateTime::now_utc();
        let crl = CertificateRevocationListParams {
            this_update: now - time::Duration::hours(2),
            next_update: if stale_crl {
                now - time::Duration::hours(1)
            } else {
                now + time::Duration::hours(1)
            },
            crl_number: SerialNumber::from(if revoked { 2u64 } else { 1 }),
            issuing_distribution_point: None,
            revoked_certs: if revoked {
                vec![RevokedCertParams {
                    serial_number: SerialNumber::from(7u64),
                    revocation_time: now - time::Duration::hours(1),
                    reason_code: None,
                    invalidity_date: None,
                }]
            } else {
                vec![]
            },
            key_identifier_method: KeyIdMethod::Sha256,
        }
        .signed_by(&self.issuer)
        .unwrap();
        IdentityMaterial {
            chain_der: vec![self.server_cert.der().to_vec()],
            private_key_der: self.server_key.serialize_der(),
            roots_der: vec![self.root.der().to_vec()],
            crls_der: vec![crl.der().to_vec()],
        }
    }
    fn peer(&self) -> Peer {
        Peer {
            subject: SUBJECT.into(),
            leaf_sha256: Sha256::digest(self.client_cert.der()).into(),
        }
    }
    fn client(&self, certificate: bool) -> Arc<rustls::ClientConfig> {
        self.client_with_server_root(certificate, &self.root)
    }
    fn client_with_server_root(
        &self,
        certificate: bool,
        server_root: &Certificate,
    ) -> Arc<rustls::ClientConfig> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(server_root.der().clone()).unwrap();
        let builder =
            rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_root_certificates(roots);
        let mut client = if certificate {
            builder
                .with_client_auth_cert(
                    vec![self.client_cert.der().clone()],
                    rustls::pki_types::PrivatePkcs8KeyDer::from(self.client_key.serialize_der())
                        .into(),
                )
                .unwrap()
        } else {
            builder.with_no_client_auth()
        };
        client.alpn_protocols = vec![b"http/1.1".to_vec()];
        Arc::new(client)
    }
    fn server(&self) -> ServerTls {
        ServerTls::new(self.material(false, false), vec![self.peer()]).unwrap()
    }
}
fn exchange(
    server: ServerTls,
    client: Arc<rustls::ClientConfig>,
    inspect: impl FnOnce(&AuthenticatedPrincipal) + Send + 'static,
) -> (Result<(String, Hash)>, bool) {
    exchange_stream(server, client, |stream| inspect(stream.principal()))
}
fn exchange_stream(
    server: ServerTls,
    client: Arc<rustls::ClientConfig>,
    inspect: impl FnOnce(&mut AuthenticatedStream) + Send + 'static,
) -> (Result<(String, Hash)>, bool) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let worker = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let mut connection = server.accept(socket, Duration::from_secs(2), 65536)?;
        let mut bytes = [0; 4];
        connection
            .read_exact(&mut bytes)
            .map_err(|_| Error::Transport)?;
        assert_eq!(&bytes, b"PING");
        server.revalidate(connection.principal())?;
        inspect(&mut connection);
        connection
            .write_all(b"PONG")
            .map_err(|_| Error::Transport)?;
        connection.flush().map_err(|_| Error::Transport)?;
        Ok((
            connection.principal().subject().to_owned(),
            connection.principal().leaf_sha256(),
        ))
    });
    let socket = TcpStream::connect(address).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let conn = rustls::ClientConnection::new(client, "localhost".try_into().unwrap()).unwrap();
    let mut stream = rustls::StreamOwned::new(conn, socket);
    let sent = stream.write_all(b"PING").and_then(|_| stream.flush());
    let mut answer = [0; 4];
    let received = sent.is_ok() && stream.read_exact(&mut answer).is_ok() && &answer == b"PONG";
    (worker.join().unwrap(), received)
}
#[test]
fn actual_mutual_tls_produces_the_certificate_subject_and_pin() {
    let f = Fixture::new(&[SUBJECT], false);
    let pin = f.peer().leaf_sha256;
    let (result, received) = exchange(f.server(), f.client(true), |_| {});
    assert_eq!(result, Ok((SUBJECT.into(), pin)));
    assert!(received);
}
#[test]
fn a_current_crl_and_allowlist_recheck_revoke_an_existing_authenticated_principal() {
    let f = Fixture::new(&[SUBJECT], false);
    let revoked = ServerTls::new(f.material(true, false), vec![f.peer()]).unwrap();
    let changed = ServerTls::new(
        f.material(false, false),
        vec![Peer {
            subject: SUBJECT.into(),
            leaf_sha256: [11; 32],
        }],
    )
    .unwrap();
    let (result, received) = exchange(f.server(), f.client(true), move |p| {
        assert_eq!(revoked.revalidate(p), Err(Error::Authentication));
        assert_eq!(changed.revalidate(p), Err(Error::Authentication));
    });
    assert!(result.is_ok());
    assert!(received);
}
#[test]
fn revoked_expired_and_untrusted_client_chains_are_rejected_by_the_actual_handshake() {
    let good = Fixture::new(&[SUBJECT], false);
    let expired = Fixture::new(&[SUBJECT], true);
    for (server, client) in [
        (
            ServerTls::new(good.material(true, false), vec![good.peer()]).unwrap(),
            good.client(true),
        ),
        (expired.server(), expired.client(true)),
        (
            ServerTls::new(good.material(false, true), vec![good.peer()]).unwrap(),
            good.client(true),
        ),
        (good.server(), good.client(false)),
        (
            good.server(),
            Fixture::new(&[SUBJECT], false).client_with_server_root(true, &good.root),
        ),
    ] {
        let (result, received) = exchange(server, client, |_| panic!("unauthenticated callback"));
        assert_eq!(result, Err(Error::Authentication));
        assert!(!received);
    }
}
#[test]
fn san_subject_and_certificate_pin_must_both_match_without_ambiguity() {
    for subjects in [
        vec![],
        vec!["spiffe://tickwire.test/control/other"],
        vec![SUBJECT, SUBJECT],
    ] {
        let f = Fixture::new(&subjects, false);
        let (result, received) = exchange(f.server(), f.client(true), |_| panic!("wrong SAN"));
        assert_eq!(result, Err(Error::Authentication));
        assert!(!received);
    }
    let f = Fixture::new(&[SUBJECT], false);
    let server = ServerTls::new(
        f.material(false, false),
        vec![Peer {
            subject: SUBJECT.into(),
            leaf_sha256: [11; 32],
        }],
    )
    .unwrap();
    assert!(
        exchange(server, f.client(true), |_| panic!("wrong pin"))
            .0
            .is_err()
    );
}
#[test]
fn trust_material_and_allowlists_have_strict_nonempty_bounds() {
    let f = Fixture::new(&[SUBJECT], false);
    let mut no_crl = f.material(false, false);
    no_crl.crls_der.clear();
    assert!(ServerTls::new(no_crl, vec![f.peer()]).is_err());
    assert!(ServerTls::new(f.material(false, false), vec![]).is_err());
    assert!(ServerTls::new(f.material(false, false), vec![f.peer(), f.peer()]).is_err());
}
#[test]
fn handshake_wait_uses_one_absolute_deadline() {
    let f = Fixture::new(&[SUBJECT], false);
    let server = f.server();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let _silent_client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (socket, _) = listener.accept().unwrap();
    let start = Instant::now();
    assert!(matches!(
        server.accept(socket, Duration::from_millis(50), 65536),
        Err(Error::Transport)
    ));
    assert!(start.elapsed() < Duration::from_secs(3));
}

#[test]
fn a_crl_issued_in_the_future_cannot_authorize_a_current_connection() {
    let f = Fixture::new(&[SUBJECT], false);
    let now = OffsetDateTime::now_utc();
    let future = CertificateRevocationListParams {
        this_update: now + time::Duration::hours(1),
        next_update: now + time::Duration::hours(2),
        crl_number: SerialNumber::from(3u64),
        issuing_distribution_point: None,
        revoked_certs: vec![],
        key_identifier_method: KeyIdMethod::Sha256,
    }
    .signed_by(&f.issuer)
    .unwrap();
    let mut material = f.material(false, false);
    material.crls_der = vec![future.der().to_vec()];
    let server = ServerTls::new(material, vec![f.peer()]).unwrap();
    let (result, received) = exchange(server, f.client(true), |_| {});
    assert_eq!(result, Err(Error::Authentication));
    assert!(!received);
}

#[test]
fn encrypted_handshake_bytes_are_bounded_before_application_data() {
    let f = Fixture::new(&[SUBJECT], false);
    let server = f.server();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let worker = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        assert!(matches!(
            server.accept(socket, Duration::from_secs(1), 128),
            Err(Error::Transport)
        ));
    });
    let socket = TcpStream::connect(address).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let conn =
        rustls::ClientConnection::new(f.client(true), "localhost".try_into().unwrap()).unwrap();
    let mut stream = rustls::StreamOwned::new(conn, socket);
    let _ = stream.write_all(b"PING").and_then(|_| stream.flush());
    let mut byte = [0];
    assert!(stream.read_exact(&mut byte).is_err());
    worker.join().unwrap();
}

fn candidate_with_bindings(bindings: serde_json::Value) -> Candidate {
    let reference = Candidate::parse(
        Format::Toml,
        include_bytes!("../../../config/reference.toml"),
        Limits::default(),
    )
    .unwrap();
    let mut value = reference.value().clone();
    value["control"]["role_bindings"] = bindings;
    Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&value).unwrap(),
        Limits::default(),
    )
    .unwrap()
}
fn changed(candidate: &Candidate, edit: impl FnOnce(&mut serde_json::Value)) -> Candidate {
    let mut value = candidate.value().clone();
    edit(&mut value);
    Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&value).unwrap(),
        Limits::default(),
    )
    .unwrap()
}
#[test]
fn current_role_bindings_authorize_the_tls_subject_and_revocation_takes_effect_on_retry() {
    let base = candidate_with_bindings(serde_json::json!([
        {"subject":SUBJECT,"role":"config-editor","account_ids":["spot-main"]}
    ]));
    let change = ChangeSet::between(
        &base,
        &changed(&base, |v| {
            v["risk_policies"][0]["max_gross_quote"] = "10000".into();
        }),
    )
    .unwrap();
    let roles = RoleBindings::from_candidate(&base).unwrap();
    assert_eq!(roles.config_hash(), base.hash());
    let revoked = RoleBindings::from_candidate(&candidate_with_bindings(serde_json::json!([
        {"subject":"spiffe://tickwire.test/control/replacement","role":"config-editor","account_ids":["spot-main"]}
    ]))).unwrap();
    let f = Fixture::new(&[SUBJECT], false);
    let current_trust = f.server();
    let (result, received) = exchange(f.server(), f.client(true), move |principal| {
        let authority = roles.authorize(&current_trust, principal).unwrap();
        change.authorize_plan(&authority).unwrap();
        assert!(change.authorize_commit(&authority).is_err());
        assert!(matches!(
            revoked.authorize(&current_trust, principal),
            Err(Error::Authorization)
        ));
    });
    assert!(result.is_ok());
    assert!(received);
}
#[test]
fn empty_account_scope_does_not_grant_global_authority_without_an_explicit_flag() {
    let empty = RoleBindings::from_candidate(&candidate_with_bindings(serde_json::json!([
        {"subject":SUBJECT,"role":"config-editor","account_ids":[]}
    ])))
    .unwrap();
    let base = candidate_with_bindings(serde_json::json!([
        {"subject":SUBJECT,"role":"config-editor","account_ids":[],"cluster_scope":true}
    ]));
    let global = RoleBindings::from_candidate(&base).unwrap();
    let changes = ChangeSet::between(
        &base,
        &changed(&base, |v| {
            v["observability"]["diagnostic_level"] = "debug".into();
        }),
    )
    .unwrap();
    let f = Fixture::new(&[SUBJECT], false);
    let trust = f.server();
    let (result, received) = exchange(f.server(), f.client(true), move |principal| {
        assert!(matches!(
            empty.authorize(&trust, principal),
            Err(Error::Authorization)
        ));
        changes
            .authorize_plan(&global.authorize(&trust, principal).unwrap())
            .unwrap();
    });
    assert!(result.is_ok());
    assert!(received);
}
#[test]
fn global_scope_cannot_be_combined_with_an_account_list() {
    let ambiguous = candidate_with_bindings(serde_json::json!([
        {"subject":SUBJECT,"role":"config-editor","account_ids":["spot-main"],"cluster_scope":true}
    ]));
    assert!(RoleBindings::from_candidate(&ambiguous).is_err());
}

#[test]
fn fragmented_http_request_over_real_tls_reaches_the_fixed_route_and_body_parser() {
    use tickwire_config_service::api::{self, Endpoint};
    let f = Fixture::new(&[SUBJECT], false);
    let server = f.server();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let worker = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let mut stream = server
            .accept(socket, Duration::from_secs(2), 65536)
            .unwrap();
        server.revalidate(stream.principal()).unwrap();
        let result = api::read_request(&mut stream, 4096);
        if result.is_ok() {
            stream.write_all(b"OK").unwrap();
            stream.flush().unwrap();
        }
        result
    });
    let socket = TcpStream::connect(address).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let conn =
        rustls::ClientConnection::new(f.client(true), "localhost".try_into().unwrap()).unwrap();
    let mut stream = rustls::StreamOwned::new(conn, socket);
    let body = r#"{"candidate":{"format":"toml","document":"[identity]\nmode='test'"},"validation_level":"structural"}"#;
    let request = format!(
        "POST /v1/config/validate HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    for chunk in request.as_bytes().chunks(7) {
        if stream
            .write_all(chunk)
            .and_then(|_| stream.flush())
            .is_err()
        {
            break;
        }
    }
    let mut reply = [0; 2];
    let replied = stream.read_exact(&mut reply).is_ok();
    let request = worker.join().unwrap().unwrap();
    assert_eq!(request.endpoint(), &Endpoint::Validate);
    assert_eq!(request.body().unwrap()["validation_level"], "structural");
    assert!(replied);
    assert_eq!(&reply, b"OK");
}

struct ApiCluster {
    dir: std::path::PathBuf,
    nodes: Vec<tickwire_config_service::Replica<tickwire_journal::store::FileStore>>,
}
impl ApiCluster {
    fn new(candidate: &Candidate) -> Self {
        use ring::signature::{Ed25519KeyPair, KeyPair as _};
        use tickwire_config_service::{bootstrap::*, *};
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "tickwire-api-owner-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir(&dir).unwrap();
        let key = Ed25519KeyPair::from_seed_unchecked(&[31; 32]).unwrap();
        let trust = BootstrapTrust::new(
            [19; 16],
            [0, 0, 0, 0, 0, 0, 64, 0, 128, 0, 0, 0, 0, 0, 0, 1],
            "tickwire-qualification".into(),
            key.public_key().as_ref().try_into().unwrap(),
        )
        .unwrap();
        let nodes = (1..=3)
            .map(|id| {
                Replica::new_with_bootstrap(
                    tickwire_control_raft::Node::create(
                        tickwire_journal::store::FileStore::open(dir.join(id.to_string())).unwrap(),
                        tickwire_control_raft::Config {
                            physical: tickwire_journal::Config::new([id as u8; 16], [18; 16]),
                            node_id: id,
                            voters: [1, 2, 3],
                            writer_epoch: 1,
                            max_command_bytes: MAX_COMMAND_BYTES,
                            max_retained_bytes: 32 * 1024 * 1024,
                        },
                    )
                    .unwrap(),
                    Config {
                        cluster: [19; 16],
                        objects: ObjectLimits {
                            max_objects: 8,
                            max_inflight: 4,
                            max_payload_bytes: 4 * 1024 * 1024,
                        },
                    },
                    trust.clone(),
                )
                .unwrap()
            })
            .collect();
        let mut c = Self { dir, nodes };
        c.nodes[0].campaign().unwrap();
        c.pump(false);
        c.nodes[0].initialize().unwrap();
        c.pump(false);
        let object = [6; 16];
        let owner = [7; 16];
        c.nodes[0]
            .propose(ObjectCommand::Begin(ObjectSpec {
                key: object,
                owner,
                purpose: Purpose::Candidate,
                length: candidate.canonical_bytes().len() as u32,
                hash: candidate.hash(),
            }))
            .unwrap();
        c.pump(false);
        for (i, bytes) in candidate.canonical_bytes().chunks(CHUNK_BYTES).enumerate() {
            c.nodes[0]
                .propose(ObjectCommand::Chunk {
                    key: object,
                    owner,
                    offset: (i * CHUNK_BYTES) as u32,
                    bytes: bytes.to_vec(),
                })
                .unwrap();
            c.pump(false);
        }
        c.nodes[0]
            .propose(ObjectCommand::Seal { key: object, owner })
            .unwrap();
        c.pump(false);
        let nonce = [10; 16];
        let signature = key
            .sign(
                &trust
                    .signing_bytes(object, candidate.hash(), nonce)
                    .unwrap(),
            )
            .as_ref()
            .try_into()
            .unwrap();
        c.nodes[0]
            .bootstrap(Bootstrap {
                candidate: object,
                candidate_hash: candidate.hash(),
                nonce,
                signature,
            })
            .unwrap();
        c.pump(false);
        c
    }
    fn pump(&mut self, isolated: bool) {
        for _ in 0..128 {
            let mut messages = Vec::new();
            for node in &mut self.nodes {
                let out = node.drive().unwrap();
                assert!(out.applied.iter().all(|a| a.result().is_ok()));
                messages.extend(out.messages);
            }
            if messages.is_empty() {
                return;
            }
            for message in messages {
                if isolated && (message.from == 1 || message.to == 1) {
                    continue;
                }
                self.nodes[(message.to - 1) as usize]
                    .step(message.from, message)
                    .unwrap();
            }
        }
        panic!("raft did not settle")
    }
}
impl Drop for ApiCluster {
    fn drop(&mut self) {
        self.nodes.clear();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
fn api_candidate(subject: &str, cluster_scope: bool) -> Candidate {
    candidate_with_bindings(serde_json::json!([{"subject":subject,"role":"config-editor","account_ids":if cluster_scope{vec![]}else{vec!["spot-main"]},"cluster_scope":cluster_scope}]))
        .normalized_configuration(Limits::default()).unwrap()
}
fn deployment_fixture() -> deployment_support::Fixture {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    deployment_support::Fixture::new_at(
        now,
        |v| {
            v["control"]["role_bindings"] = serde_json::json!([{
                "subject":SUBJECT,"role":"config-editor","account_ids":[],"cluster_scope":true
            }]);
        },
        |_, _| {},
    )
}
fn source_owner(
    f: &deployment_support::Fixture,
) -> tickwire_config_service::deployment_owner::Owner {
    let mut limits = tickwire_config_service::deployment::Limits::default();
    limits.accounts.max_age_ms = 60_000;
    limits.credentials.max_age_ms = 60_000;
    limits.resources.max_age_ms = 60_000;
    tickwire_config_service::deployment_owner::Owner::new(
        tickwire_config_service::source_bundle::Bundle::decode(
            &f.bundle_bytes(),
            Default::default(),
        )
        .unwrap(),
        f.trust(1),
        limits,
        2,
    )
    .unwrap()
}
fn api_exchange(
    f: &Fixture,
    path: &str,
    body: Option<serde_json::Value>,
    inspect: impl FnOnce(&ServerTls, &mut AuthenticatedStream, tickwire_config_service::api::Request)
    + Send
    + 'static,
) -> (u16, serde_json::Value) {
    let server = f.server();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let worker = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        let mut stream = server
            .accept(socket, Duration::from_secs(10), 32 * 1024 * 1024)
            .unwrap();
        let request =
            tickwire_config_service::api::read_request(&mut stream, 16 * 1024 * 1024).unwrap();
        inspect(&server, &mut stream, request);
    });
    let socket = TcpStream::connect(address).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(12)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(12)))
        .unwrap();
    let conn =
        rustls::ClientConnection::new(f.client(true), "localhost".try_into().unwrap()).unwrap();
    let mut stream = rustls::StreamOwned::new(conn, socket);
    let (method, body) = body.map_or(("GET", String::new()), |v| ("POST", v.to_string()));
    write!(stream,"{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",body.len()).unwrap();
    stream.flush().unwrap();
    let mut bytes = Vec::new();
    let mut byte = [0];
    let header_result = loop {
        match stream.read_exact(&mut byte) {
            Ok(()) => {
                bytes.push(byte[0]);
                if bytes.ends_with(b"\r\n\r\n") {
                    break Ok(());
                }
                assert!(bytes.len() < 16384);
            }
            Err(e) => break Err(e),
        }
    };
    worker.join().unwrap();
    header_result.unwrap();
    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut parsed = httparse::Response::new(&mut headers);
    parsed.parse(&bytes).unwrap();
    let status = parsed.code.unwrap();
    for (name, value) in [
        ("Content-Type", b"application/json".as_slice()),
        ("Cache-Control", b"no-store"),
        ("Connection", b"close"),
    ] {
        assert!(
            parsed
                .headers
                .iter()
                .any(|h| h.name.eq_ignore_ascii_case(name) && h.value == value)
        );
    }
    let length = parsed
        .headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case("content-length"))
        .map(|h| {
            std::str::from_utf8(h.value)
                .unwrap()
                .parse::<usize>()
                .unwrap()
        })
        .unwrap();
    let mut body = vec![0; length];
    stream.read_exact(&mut body).unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    if let Some(etag) = body.get("etag").and_then(serde_json::Value::as_str) {
        assert!(
            parsed
                .headers
                .iter()
                .any(|h| h.name.eq_ignore_ascii_case("etag") && h.value == etag.as_bytes())
        );
    }
    (status, body)
}
#[test]
fn executed_schema_waits_for_actual_quorum_and_returns_http_on_the_authenticated_connection() {
    let f = Fixture::new(&[SUBJECT], false);
    let mut cluster = ApiCluster::new(&api_candidate(SUBJECT, true));
    let (status, body) = api_exchange(
        &f,
        "/v1/config/schema",
        None,
        move |trust, stream, request| {
            let mut pending =
                tickwire_control_raft::driver::PeerDriver::control(&mut cluster.nodes[0])
                    .begin_request(trust, stream.principal(), request)
                    .unwrap();
            assert!(
                tickwire_control_raft::driver::PeerDriver::control(&mut cluster.nodes[0])
                    .poll_request(trust, &mut pending)
                    .unwrap()
                    .is_none()
            );
            cluster.pump(true);
            assert!(
                tickwire_control_raft::driver::PeerDriver::control(&mut cluster.nodes[0])
                    .poll_request(trust, &mut pending)
                    .unwrap()
                    .is_none()
            );
            for _ in 0..6 {
                cluster.nodes[0].tick().unwrap();
            }
            cluster.pump(false);
            let response =
                tickwire_control_raft::driver::PeerDriver::control(&mut cluster.nodes[0])
                    .poll_request(trust, &mut pending)
                    .unwrap()
                    .unwrap();
            assert!(
                tickwire_control_raft::driver::PeerDriver::control(&mut cluster.nodes[0])
                    .poll_request(trust, &mut pending)
                    .is_err()
            );
            response.write(stream).unwrap();
        },
    );
    assert_eq!(status, 200);
    assert_eq!(
        body["$id"],
        "https://tickwire.invalid/contracts/system-config.schema.json"
    );
}
#[test]
fn executed_validation_reports_structural_result_and_deployment_pending_without_mutating_head() {
    for (document, level, valid) in [
        (
            include_str!("../../../config/reference.toml"),
            "structural",
            true,
        ),
        (
            include_str!("../../../config/reference.toml"),
            "deployment",
            false,
        ),
        ("[identity]\nmode='test'", "structural", false),
    ] {
        let f = Fixture::new(&[SUBJECT], false);
        let base = api_candidate(SUBJECT, true);
        let mut cluster = ApiCluster::new(&base);
        let body = serde_json::json!({"candidate":{"format":"toml","document":document},"validation_level":level});
        let (status, body) = api_exchange(
            &f,
            "/v1/config/validate",
            Some(body),
            move |trust, stream, request| {
                let before = cluster.nodes[0].scanned_through();
                let mut pending =
                    tickwire_control_raft::driver::PeerDriver::control(&mut cluster.nodes[0])
                        .begin_request(trust, stream.principal(), request)
                        .unwrap();
                cluster.pump(false);
                let response =
                    tickwire_control_raft::driver::PeerDriver::control(&mut cluster.nodes[0])
                        .poll_request(trust, &mut pending)
                        .unwrap()
                        .unwrap();
                assert_eq!(cluster.nodes[0].scanned_through(), before);
                response.write(stream).unwrap();
            },
        );
        assert_eq!(status, 200);
        assert_eq!(body["valid"], valid);
        assert_eq!(body["validation_level"], level);
        assert!(!body["checks_pending"].as_array().unwrap().is_empty());
    }
}
#[test]
fn deployment_content_executes_off_owner_and_waits_for_second_actual_read_index() {
    use tickwire_config_service::request::Progress;
    use tickwire_control_raft::driver::PeerDriver;
    let f = Fixture::new(&[SUBJECT], false);
    let sources = deployment_fixture();
    let mut cluster = ApiCluster::new(&sources.candidate);
    cluster.nodes[0]
        .control()
        .install_deployment_owner(source_owner(&sources))
        .unwrap();
    let body = serde_json::json!({"candidate":{"format":"json","document":sources.candidate.value()},"validation_level":"deployment"});
    let (status, body) = api_exchange(
        &f,
        "/v1/config/validate",
        Some(body),
        move |trust, stream, request| {
            let mut pending = cluster.nodes[0]
                .control()
                .begin_request(trust, stream.principal(), request)
                .unwrap();
            assert!(
                cluster.nodes[0]
                    .control()
                    .poll_request(trust, &mut pending)
                    .unwrap()
                    .is_none()
            );
            cluster.pump(false);
            assert!(
                cluster.nodes[0]
                    .control()
                    .poll_request(trust, &mut pending)
                    .unwrap()
                    .is_none(),
                "content computation must be asynchronous"
            );
            assert_eq!(pending.progress(), Progress::Validating);
            let before = cluster.nodes[0].scanned_through();
            cluster.nodes[0]
                .propose(tickwire_config_service::ObjectCommand::Begin(
                    tickwire_config_service::ObjectSpec {
                        key: [55; 16],
                        owner: [56; 16],
                        purpose: tickwire_config_service::Purpose::Candidate,
                        length: 1,
                        hash: [57; 32],
                    },
                ))
                .unwrap();
            cluster.pump(false);
            assert!(
                cluster.nodes[0].scanned_through() > before,
                "same owner still commits business records"
            );
            let deadline = Instant::now() + Duration::from_secs(5);
            while pending.progress() == Progress::Validating {
                assert!(Instant::now() < deadline);
                assert!(
                    cluster.nodes[0]
                        .control()
                        .poll_request(trust, &mut pending)
                        .unwrap()
                        .is_none()
                );
                thread::sleep(Duration::from_millis(1));
            }
            assert_eq!(pending.progress(), Progress::Reading);
            assert!(
                cluster.nodes[0]
                    .control()
                    .poll_request(trust, &mut pending)
                    .unwrap()
                    .is_none(),
                "computed result needs a new quorum read"
            );
            cluster.pump(false);
            let response = cluster.nodes[0]
                .control()
                .poll_request(trust, &mut pending)
                .unwrap()
                .unwrap();
            assert_eq!(pending.progress(), Progress::Completed);
            response.write(stream).unwrap();
        },
    );
    assert_eq!(status, 200);
    assert_eq!(body["valid"], false);
    let completed = body["checks_completed"].as_array().unwrap();
    for check in [
        "artifact_closure",
        "parameter_schemas",
        "projection_set",
        "account_modes",
        "credential_references",
        "retained_capacity",
    ] {
        assert!(
            completed.contains(&check.into()),
            "missing actual completed check {check}"
        );
    }
    assert_eq!(
        body["checks_pending"],
        serde_json::json!(["retained_registry_head", "runtime_targets"])
    );
}
#[test]
fn deployment_content_does_not_survive_source_replacement_during_computation() {
    use tickwire_control_raft::driver::PeerDriver;
    let f = Fixture::new(&[SUBJECT], false);
    let sources = deployment_fixture();
    let mut cluster = ApiCluster::new(&sources.candidate);
    cluster.nodes[0]
        .control()
        .install_deployment_owner(source_owner(&sources))
        .unwrap();
    let body = serde_json::json!({"candidate":{"format":"json","document":sources.candidate.value()},"validation_level":"deployment"});
    let (status, body) = api_exchange(
        &f,
        "/v1/config/validate",
        Some(body),
        move |trust, stream, request| {
            let mut pending = cluster.nodes[0]
                .control()
                .begin_request(trust, stream.principal(), request)
                .unwrap();
            cluster.pump(false);
            assert!(
                cluster.nodes[0]
                    .control()
                    .poll_request(trust, &mut pending)
                    .unwrap()
                    .is_none()
            );
            cluster.nodes[0]
                .control()
                .replace_deployment_sources(
                    tickwire_config_service::source_bundle::Bundle::decode(
                        &sources.bundle_bytes(),
                        Default::default(),
                    )
                    .unwrap(),
                    sources.trust(2),
                    Default::default(),
                )
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                assert!(Instant::now() < deadline);
                if let Some(response) = cluster.nodes[0]
                    .control()
                    .poll_request(trust, &mut pending)
                    .unwrap()
                {
                    response.write(stream).unwrap();
                    break;
                }
                cluster.pump(false);
                thread::sleep(Duration::from_millis(1));
            }
        },
    );
    assert_eq!(status, 200);
    assert_eq!(body["valid"], false);
    assert!(
        !body["checks_completed"]
            .as_array()
            .unwrap()
            .contains(&"artifact_closure".into())
    );
    assert_eq!(body["issues"][0]["code"], "DeploymentContentFailed");
}
#[test]
fn deployment_content_rechecks_tls_revocation_and_can_cancel_during_computation() {
    use tickwire_control_raft::driver::PeerDriver;
    for revoke in [false, true] {
        let f = Fixture::new(&[SUBJECT], false);
        let revoked = ServerTls::new(f.material(true, false), vec![f.peer()]).unwrap();
        let sources = deployment_fixture();
        let mut cluster = ApiCluster::new(&sources.candidate);
        cluster.nodes[0]
            .control()
            .install_deployment_owner(source_owner(&sources))
            .unwrap();
        let body = serde_json::json!({"candidate":{"format":"json","document":sources.candidate.value()},"validation_level":"deployment"});
        let (status, body) = api_exchange(
            &f,
            "/v1/config/validate",
            Some(body),
            move |trust, stream, request| {
                let mut pending = cluster.nodes[0]
                    .control()
                    .begin_request(trust, stream.principal(), request)
                    .unwrap();
                cluster.pump(false);
                assert!(
                    cluster.nodes[0]
                        .control()
                        .poll_request(trust, &mut pending)
                        .unwrap()
                        .is_none()
                );
                if revoke {
                    cluster.nodes[0]
                        .control()
                        .poll_request(&revoked, &mut pending)
                        .unwrap()
                        .unwrap()
                        .write(stream)
                        .unwrap();
                } else {
                    assert!(
                        cluster.nodes[0]
                            .control()
                            .cancel_request(&mut pending)
                            .unwrap()
                    );
                    assert!(
                        !cluster.nodes[0]
                            .control()
                            .cancel_request(&mut pending)
                            .unwrap()
                    );
                    assert!(
                        cluster.nodes[0]
                            .control()
                            .poll_request(trust, &mut pending)
                            .is_err()
                    );
                    let request = tickwire_config_service::api::Request::parse(
                        b"GET /v1/config/schema HTTP/1.1\r\nHost: localhost\r\n\r\n",
                        4096,
                    )
                    .unwrap();
                    let mut next = cluster.nodes[0]
                        .control()
                        .begin_request(trust, stream.principal(), request)
                        .unwrap();
                    cluster.pump(false);
                    cluster.nodes[0]
                        .control()
                        .poll_request(trust, &mut next)
                        .unwrap()
                        .unwrap()
                        .write(stream)
                        .unwrap();
                }
            },
        );
        assert_eq!(status, if revoke { 401 } else { 200 });
        if revoke {
            assert_eq!(body["code"], "Unauthorized");
        }
    }
}
#[test]
fn executed_request_uses_committed_roles_and_rechecks_certificate_before_completion() {
    for (subject, scope, revoked, expected) in [
        ("spiffe://tickwire.test/control/other", true, false, 403),
        (SUBJECT, false, false, 403),
        (SUBJECT, true, true, 401),
    ] {
        let f = Fixture::new(&[SUBJECT], false);
        let mut cluster = ApiCluster::new(&api_candidate(subject, scope));
        let revoked_trust = ServerTls::new(f.material(true, false), vec![f.peer()]).unwrap();
        let request = serde_json::json!({"candidate":{"format":"toml","document":include_str!("../../../config/reference.toml")},"validation_level":"structural"});
        let (status, body) = api_exchange(
            &f,
            "/v1/config/validate",
            Some(request),
            move |trust, stream, request| {
                let mut pending =
                    tickwire_control_raft::driver::PeerDriver::control(&mut cluster.nodes[0])
                        .begin_request(trust, stream.principal(), request)
                        .unwrap();
                cluster.pump(false);
                let trust = if revoked { &revoked_trust } else { trust };
                tickwire_control_raft::driver::PeerDriver::control(&mut cluster.nodes[0])
                    .poll_request(trust, &mut pending)
                    .unwrap()
                    .unwrap()
                    .write(stream)
                    .unwrap();
            },
        );
        assert_eq!(status, expected);
        assert_eq!(
            body["code"],
            if revoked {
                "Unauthorized"
            } else {
                "PermissionDenied"
            }
        );
        assert!(!body.to_string().contains("spot-main"));
    }
}

#[test]
fn executed_scoped_validation_binds_body_etag_and_does_not_expose_other_accounts() {
    use tickwire_control_raft::driver::PeerDriver;
    for (stale, foreign, expected) in [(false, false, 200), (true, false, 412), (false, true, 403)]
    {
        let f = Fixture::new(&[SUBJECT], false);
        let base = api_candidate(SUBJECT, false);
        let mut cluster = ApiCluster::new(&base);
        let etag = tickwire_config::patch::Head::new(if stale { 2 } else { 1 }, &base)
            .unwrap()
            .etag();
        let body = serde_json::json!({"candidate":{"format":"scoped-patch","document":{"base_etag":etag,"changes":[{"op":"replace","selector":{"collection":"risk_policies","id":base.value()["risk_policies"][usize::from(foreign)]["id"]},"path":"/max_gross_quote","value":"10000"}]}},"validation_level":"structural"});
        let (status, body) = api_exchange(
            &f,
            "/v1/config/validate",
            Some(body),
            move |trust, stream, request| {
                let mut pending = cluster.nodes[0]
                    .control()
                    .begin_request(trust, stream.principal(), request)
                    .unwrap();
                cluster.pump(false);
                cluster.nodes[0]
                    .control()
                    .poll_request(trust, &mut pending)
                    .unwrap()
                    .unwrap()
                    .write(stream)
                    .unwrap();
            },
        );
        assert_eq!(status, expected);
        assert!(!body.to_string().contains("usdm-main"));
        if expected == 200 {
            assert_eq!(body["valid"], true);
            assert!(body.get("config").is_none());
        }
    }
}
#[test]
fn executed_response_cannot_be_written_to_another_authenticated_leaf() {
    use tickwire_control_raft::driver::PeerDriver;
    let f = Fixture::new(&[SUBJECT], false);
    let trust = f.server();
    let mut cluster = ApiCluster::new(&api_candidate(SUBJECT, true));
    let (tx, rx) = std::sync::mpsc::channel();
    let (result, received) = exchange(f.server(), f.client(true), move |principal| {
        let request = tickwire_config_service::api::Request::parse(
            b"GET /v1/config/schema HTTP/1.1\r\nHost: localhost\r\n\r\n",
            4096,
        )
        .unwrap();
        let mut pending = cluster.nodes[0]
            .control()
            .begin_request(&trust, principal, request)
            .unwrap();
        cluster.pump(false);
        tx.send(
            cluster.nodes[0]
                .control()
                .poll_request(&trust, &mut pending)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
    });
    assert!(result.is_ok() && received);
    let response = rx.recv().unwrap();
    let other = Fixture::new(&[SUBJECT], false);
    let (result, received) = exchange_stream(other.server(), other.client(true), move |stream| {
        assert_eq!(response.write(stream), Err(Error::Authentication))
    });
    assert!(result.is_ok() && received);
}
#[test]
fn executed_unimplemented_endpoint_is_distinct_from_unavailable_without_creating_a_revision() {
    use tickwire_control_raft::driver::PeerDriver;
    let f = Fixture::new(&[SUBJECT], false);
    let mut cluster = ApiCluster::new(&api_candidate(SUBJECT, true));
    let (status, body) = api_exchange(
        &f,
        "/v1/config/effective",
        None,
        move |trust, stream, request| {
            let before = cluster.nodes[0].scanned_through();
            let mut pending = cluster.nodes[0]
                .control()
                .begin_request(trust, stream.principal(), request)
                .unwrap();
            cluster.pump(false);
            cluster.nodes[0]
                .control()
                .poll_request(trust, &mut pending)
                .unwrap()
                .unwrap()
                .write(stream)
                .unwrap();
            assert_eq!(cluster.nodes[0].scanned_through(), before);
        },
    );
    assert_eq!(status, 503);
    assert_eq!(body["code"], "UnsupportedEndpoint");
}

#[test]
fn executed_validation_enforces_the_committed_request_budget_after_the_safe_read() {
    use tickwire_control_raft::driver::PeerDriver;
    let f = Fixture::new(&[SUBJECT], false);
    let base = changed(&api_candidate(SUBJECT, true), |v| {
        v["control"]["max_request_bytes"] = 1024.into()
    });
    let mut cluster = ApiCluster::new(&base);
    let body = serde_json::json!({"candidate":{"format":"toml","document":include_str!("../../../config/reference.toml")},"validation_level":"structural"});
    let (status, body) = api_exchange(
        &f,
        "/v1/config/validate",
        Some(body),
        move |trust, stream, request| {
            let mut pending = cluster.nodes[0]
                .control()
                .begin_request(trust, stream.principal(), request)
                .unwrap();
            cluster.pump(false);
            cluster.nodes[0]
                .control()
                .poll_request(trust, &mut pending)
                .unwrap()
                .unwrap()
                .write(stream)
                .unwrap();
        },
    );
    assert_eq!(status, 413);
    assert_eq!(body["code"], "RequestTooLarge");
}

#[test]
fn executed_cancellation_keeps_isolated_read_backlog_bounded_and_reclaims_after_quorum() {
    use tickwire_control_raft::driver::PeerDriver;
    let f = Fixture::new(&[SUBJECT], false);
    let trust = f.server();
    let mut cluster = ApiCluster::new(&api_candidate(SUBJECT, true));
    let (result, received) = exchange(f.server(), f.client(true), move |principal| {
        let request = || {
            tickwire_config_service::api::Request::parse(
                b"GET /v1/config/schema HTTP/1.1\r\nHost: localhost\r\n\r\n",
                4096,
            )
            .unwrap()
        };
        for _ in 0..1024 {
            let mut pending = cluster.nodes[0]
                .control()
                .begin_request(&trust, principal, request())
                .unwrap();
            assert!(
                cluster.nodes[0]
                    .control()
                    .cancel_request(&mut pending)
                    .unwrap()
            );
            assert!(
                !cluster.nodes[0]
                    .control()
                    .cancel_request(&mut pending)
                    .unwrap()
            );
            assert!(
                cluster.nodes[0]
                    .control()
                    .poll_request(&trust, &mut pending)
                    .is_err()
            );
        }
        assert!(matches!(
            cluster.nodes[0]
                .control()
                .begin_request(&trust, principal, request()),
            Err(Error::Consensus(tickwire_control_raft::Error::Bounds))
        ));
        cluster.pump(false);
        let mut pending = cluster.nodes[0]
            .control()
            .begin_request(&trust, principal, request())
            .unwrap();
        cluster.pump(false);
        let response = cluster.nodes[0]
            .control()
            .poll_request(&trust, &mut pending)
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), 200);
    });
    assert!(result.is_ok() && received);
}

fn read_http(stream: &mut impl Read) -> (u16, serde_json::Value) {
    let mut header = Vec::new();
    let mut byte = [0];
    while !header.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).unwrap();
        header.push(byte[0]);
        assert!(header.len() < 16384);
    }
    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut parsed = httparse::Response::new(&mut headers);
    parsed.parse(&header).unwrap();
    let status = parsed.code.unwrap();
    let length = parsed
        .headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case("content-length"))
        .map(|h| {
            std::str::from_utf8(h.value)
                .unwrap()
                .parse::<usize>()
                .unwrap()
        })
        .unwrap();
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}
fn start_api_client(
    client: Arc<rustls::ClientConfig>,
    address: std::net::SocketAddr,
    request: &'static [u8],
) -> thread::JoinHandle<(u16, serde_json::Value)> {
    thread::spawn(move || {
        let socket = TcpStream::connect(address).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let conn = rustls::ClientConnection::new(client, "localhost".try_into().unwrap()).unwrap();
        let mut stream = rustls::StreamOwned::new(conn, socket);
        stream.write_all(request).unwrap();
        stream.flush().unwrap();
        read_http(&mut stream)
    })
}
#[test]
fn https_owner_workers_deliver_real_tls_requests_through_the_same_raft_owner() {
    use tickwire_config_service::https::{Limits as HttpsLimits, Server};
    use tickwire_control_raft::driver::PeerDriver;
    let f = Fixture::new(&[SUBJECT], false);
    let mut cluster = ApiCluster::new(&api_candidate(SUBJECT, true));
    let mut server = Server::bind(
        "127.0.0.1:0".parse().unwrap(),
        f.server(),
        HttpsLimits::default(),
    )
    .unwrap();
    let client = start_api_client(
        f.client(true),
        server.local_addr().unwrap(),
        b"GET /v1/config/schema HTTP/1.1\r\nHost: localhost\r\n\r\n",
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut completed = 0;
    while !client.is_finished() {
        assert!(Instant::now() < deadline);
        cluster.pump(false);
        completed = server
            .poll(&mut cluster.nodes[0].control(), 8)
            .unwrap()
            .completed;
        thread::yield_now();
    }
    let (status, body) = client.join().unwrap();
    assert_eq!(status, 200);
    assert_eq!(
        body["$id"],
        "https://tickwire.invalid/contracts/system-config.schema.json"
    );
    assert_eq!(completed, 1);
    server.shutdown(&mut cluster.nodes[0].control()).unwrap();
}
#[test]
fn https_owner_partial_body_cannot_block_consensus_and_connections_are_bounded() {
    use tickwire_config_service::{
        ObjectCommand, ObjectSpec, Purpose,
        https::{Limits as HttpsLimits, Server},
    };
    use tickwire_control_raft::driver::PeerDriver;
    let f = Fixture::new(&[SUBJECT], false);
    let mut cluster = ApiCluster::new(&api_candidate(SUBJECT, true));
    let mut server = Server::bind(
        "127.0.0.1:0".parse().unwrap(),
        f.server(),
        HttpsLimits {
            max_connections: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let address = server.local_addr().unwrap();
    let client_config = f.client(true);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let client = thread::spawn(move || {
        let socket = TcpStream::connect(address).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let conn =
            rustls::ClientConnection::new(client_config, "localhost".try_into().unwrap()).unwrap();
        let mut stream = rustls::StreamOwned::new(conn, socket);
        stream.write_all(b"POST /v1/config/validate HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{").unwrap();
        stream.flush().unwrap();
        ready_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < deadline);
        server.poll(&mut cluster.nodes[0].control(), 8).unwrap();
        if ready_rx.try_recv().is_ok() {
            break;
        }
        thread::yield_now();
    }
    let extra = TcpStream::connect(address).unwrap();
    let state = server.poll(&mut cluster.nodes[0].control(), 8).unwrap();
    assert_eq!(state.accepted, 1);
    assert_eq!(state.connections, 1);
    assert_eq!(state.pending, 0);
    let before = cluster.nodes[0].scanned_through();
    cluster.nodes[0]
        .propose(ObjectCommand::Begin(ObjectSpec {
            key: [81; 16],
            owner: [82; 16],
            purpose: Purpose::Candidate,
            length: 1,
            hash: Sha256::digest([3]).into(),
        }))
        .unwrap();
    cluster.pump(false);
    assert!(cluster.nodes[0].scanned_through() > before);
    server.shutdown(&mut cluster.nodes[0].control()).unwrap();
    drop(extra);
    release_tx.send(()).unwrap();
    client.join().unwrap();
}
#[test]
fn https_owner_revalidates_new_trust_for_an_already_authenticated_pending_request() {
    use tickwire_config_service::https::{Limits as HttpsLimits, Server};
    use tickwire_control_raft::driver::PeerDriver;
    let f = Fixture::new(&[SUBJECT], false);
    let mut cluster = ApiCluster::new(&api_candidate(SUBJECT, true));
    let mut server = Server::bind(
        "127.0.0.1:0".parse().unwrap(),
        f.server(),
        HttpsLimits::default(),
    )
    .unwrap();
    let client = start_api_client(
        f.client(true),
        server.local_addr().unwrap(),
        b"GET /v1/config/schema HTTP/1.1\r\nHost: localhost\r\n\r\n",
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < deadline);
        if server
            .poll(&mut cluster.nodes[0].control(), 8)
            .unwrap()
            .pending
            == 1
        {
            break;
        }
        thread::yield_now();
    }
    server
        .replace_trust(ServerTls::new(f.material(true, false), vec![f.peer()]).unwrap())
        .unwrap();
    while !client.is_finished() {
        assert!(Instant::now() < deadline);
        server.poll(&mut cluster.nodes[0].control(), 8).unwrap();
        thread::yield_now();
    }
    let (status, body) = client.join().unwrap();
    assert_eq!(status, 401);
    assert_eq!(body["code"], "Unauthorized");
    server.shutdown(&mut cluster.nodes[0].control()).unwrap();
}

#[test]
fn https_owner_invalid_http_is_answered_without_a_consensus_ticket() {
    use tickwire_config_service::https::{Limits as HttpsLimits, Server};
    use tickwire_control_raft::driver::PeerDriver;
    let f = Fixture::new(&[SUBJECT], false);
    let mut cluster = ApiCluster::new(&api_candidate(SUBJECT, true));
    let mut server = Server::bind(
        "127.0.0.1:0".parse().unwrap(),
        f.server(),
        HttpsLimits::default(),
    )
    .unwrap();
    let client = start_api_client(
        f.client(true),
        server.local_addr().unwrap(),
        b"GET /v1/config/schema HTTP/1.1\r\nHost: localhost\r\nHost: duplicate\r\n\r\n",
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    while !client.is_finished() {
        assert!(Instant::now() < deadline);
        let status = server.poll(&mut cluster.nodes[0].control(), 8).unwrap();
        assert_eq!(status.pending, 0);
        thread::yield_now();
    }
    let (status, body) = client.join().unwrap();
    assert_eq!(status, 400);
    assert_eq!(body["code"], "Malformed");
    server.shutdown(&mut cluster.nodes[0].control()).unwrap();
}

#[test]
fn https_owner_silent_handshake_expires_and_releases_connection_capacity() {
    use tickwire_config_service::https::{Limits as HttpsLimits, Server};
    use tickwire_control_raft::driver::PeerDriver;
    let f = Fixture::new(&[SUBJECT], false);
    let mut cluster = ApiCluster::new(&api_candidate(SUBJECT, true));
    let mut server = Server::bind(
        "127.0.0.1:0".parse().unwrap(),
        f.server(),
        HttpsLimits {
            max_connections: 1,
            timeout: Duration::from_millis(500),
            ..Default::default()
        },
    )
    .unwrap();
    let stalled = TcpStream::connect(server.local_addr().unwrap()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        assert!(Instant::now() < deadline);
        let status = server.poll(&mut cluster.nodes[0].control(), 8).unwrap();
        if status.failed == 1 {
            assert_eq!(status.connections, 0);
            assert_eq!(status.pending, 0);
            break;
        }
        thread::yield_now();
    }
    drop(stalled);
    let client = start_api_client(
        f.client(true),
        server.local_addr().unwrap(),
        b"GET /v1/config/schema HTTP/1.1\r\nHost: localhost\r\n\r\n",
    );
    while !client.is_finished() {
        assert!(Instant::now() < deadline);
        cluster.pump(false);
        server.poll(&mut cluster.nodes[0].control(), 8).unwrap();
        thread::yield_now();
    }
    assert_eq!(client.join().unwrap().0, 200);
    server.shutdown(&mut cluster.nodes[0].control()).unwrap();
}

#[test]
fn https_owner_leadership_loss_returns_unavailable_and_cancels_the_pending_ticket() {
    use tickwire_config_service::https::{Limits as HttpsLimits, Server};
    use tickwire_control_raft::driver::{PeerDriver, Role};
    let f = Fixture::new(&[SUBJECT], false);
    let mut cluster = ApiCluster::new(&api_candidate(SUBJECT, true));
    let mut server = Server::bind(
        "127.0.0.1:0".parse().unwrap(),
        f.server(),
        HttpsLimits::default(),
    )
    .unwrap();
    let client = start_api_client(
        f.client(true),
        server.local_addr().unwrap(),
        b"GET /v1/config/schema HTTP/1.1\r\nHost: localhost\r\n\r\n",
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < deadline);
        if server
            .poll(&mut cluster.nodes[0].control(), 8)
            .unwrap()
            .pending
            == 1
        {
            break;
        }
        thread::yield_now();
    }
    // The other two actual voters elect a new leader while voter 1 is isolated.
    for _ in 0..110 {
        cluster.nodes[1].tick().unwrap();
        cluster.nodes[2].tick().unwrap();
        cluster.pump(true);
    }
    assert!(
        cluster.nodes[1..]
            .iter()
            .any(|node| node.status().role == Role::Leader)
    );
    for _ in 0..5 {
        for node in &mut cluster.nodes[1..] {
            node.tick().unwrap();
        }
        cluster.pump(false);
    }
    assert_eq!(cluster.nodes[0].status().role, Role::Follower);
    while !client.is_finished() {
        assert!(Instant::now() < deadline);
        server.poll(&mut cluster.nodes[0].control(), 8).unwrap();
        thread::yield_now();
    }
    let (status, body) = client.join().unwrap();
    assert_eq!(status, 503);
    assert_eq!(body["code"], "ControlUnavailable");
    assert_eq!(
        server
            .poll(&mut cluster.nodes[0].control(), 8)
            .unwrap()
            .pending,
        0
    );
    server.shutdown(&mut cluster.nodes[0].control()).unwrap();
}

#[test]
fn https_owner_limits_and_expired_absolute_tls_deadline_fail_before_handshake() {
    use tickwire_config_service::https::{Limits as HttpsLimits, Server};
    let f = Fixture::new(&[SUBJECT], false);
    for limits in [
        HttpsLimits {
            max_connections: 0,
            ..Default::default()
        },
        HttpsLimits {
            max_connections: 65,
            ..Default::default()
        },
        HttpsLimits {
            max_body_bytes: 0,
            ..Default::default()
        },
        HttpsLimits {
            max_body_bytes: 16 * 1024 * 1024 + 1,
            ..Default::default()
        },
        HttpsLimits {
            timeout: Duration::ZERO,
            ..Default::default()
        },
        HttpsLimits {
            timeout: Duration::from_secs(61),
            ..Default::default()
        },
    ] {
        assert!(matches!(
            Server::bind("127.0.0.1:0".parse().unwrap(), f.server(), limits),
            Err(Error::Bounds)
        ));
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (socket, _) = listener.accept().unwrap();
    assert!(matches!(
        f.server()
            .accept_until(socket, Instant::now() - Duration::from_millis(1), 65536),
        Err(Error::Bounds)
    ));
}

#[test]
fn https_owner_shutdown_and_drop_close_pending_connections_without_stopping_raft() {
    use tickwire_config_service::https::{Limits as HttpsLimits, Server};
    use tickwire_control_raft::driver::PeerDriver;
    let f = Fixture::new(&[SUBJECT], false);
    let mut cluster = ApiCluster::new(&api_candidate(SUBJECT, true));
    for explicit_shutdown in [true, false] {
        let mut server = Server::bind(
            "127.0.0.1:0".parse().unwrap(),
            f.server(),
            HttpsLimits::default(),
        )
        .unwrap();
        let client_config = f.client(true);
        let address = server.local_addr().unwrap();
        let client = thread::spawn(move || {
            let socket = TcpStream::connect(address).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let conn =
                rustls::ClientConnection::new(client_config, "localhost".try_into().unwrap())
                    .unwrap();
            let mut stream = rustls::StreamOwned::new(conn, socket);
            stream
                .write_all(b"GET /v1/config/schema HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .unwrap();
            stream.flush().unwrap();
            stream.read_exact(&mut [0]).is_err()
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(Instant::now() < deadline);
            if server
                .poll(&mut cluster.nodes[0].control(), 8)
                .unwrap()
                .pending
                == 1
            {
                break;
            }
            thread::yield_now();
        }
        if explicit_shutdown {
            server.shutdown(&mut cluster.nodes[0].control()).unwrap();
            assert_eq!(
                server
                    .poll(&mut cluster.nodes[0].control(), 8)
                    .unwrap()
                    .pending,
                0
            );
            assert!(matches!(
                server.replace_trust(f.server()),
                Err(Error::NotReady)
            ));
        }
        drop(server);
        assert!(client.join().unwrap());
        // Complete the actual canceled ReadIndex, including its drop lease.
        cluster.pump(false);
    }
    let mut server = Server::bind(
        "127.0.0.1:0".parse().unwrap(),
        f.server(),
        HttpsLimits::default(),
    )
    .unwrap();
    let client = start_api_client(
        f.client(true),
        server.local_addr().unwrap(),
        b"GET /v1/config/schema HTTP/1.1\r\nHost: localhost\r\n\r\n",
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    while !client.is_finished() {
        assert!(Instant::now() < deadline);
        cluster.pump(false);
        server.poll(&mut cluster.nodes[0].control(), 8).unwrap();
        thread::yield_now();
    }
    assert_eq!(client.join().unwrap().0, 200);
    server.shutdown(&mut cluster.nodes[0].control()).unwrap();
}

mod control_planning;
#[path = "control_reads/mod.rs"]
mod control_reads;
