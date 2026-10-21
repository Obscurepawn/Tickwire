mod common;
use common::{Fixture, scope};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};
use tickwire_control_peer::{
    config_runner::{self, Configuration},
    service::PeerBudget,
};
use tickwire_control_raft::driver::Role;
struct Files(PathBuf);
impl Files {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "tickwire-config-startup-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn put(&self, name: &str, bytes: &[u8]) -> PathBuf {
        let p = self.0.join(name);
        fs::write(&p, bytes).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&p, fs::Permissions::from_mode(0o600)).unwrap();
        }
        p
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
fn value(files: &Files, f: &Fixture, id: u64, addresses: &[SocketAddr; 3]) -> Value {
    let m = f.material(id, None, false);
    let identity = json!({"chain_der":[files.put(&format!("{id}-cert"),&m.chain_der[0])],"private_key_der":files.put(&format!("{id}-key"),&m.private_key_der),"roots_der":[files.put(&format!("{id}-root"),&m.roots_der[0])],"crls_der":[files.put(&format!("{id}-crl"),&m.crls_der[0])]});
    let peers:Vec<_>=f.peers(id).into_iter().map(|p|json!({"node_id":p.node_id,"address":addresses[p.node_id as usize-1],"server_name":p.server_name,"leaf_sha256":p.leaf_sha256})).collect();
    let peer = json!({"cluster":scope().cluster,"group":scope().group,"node_id":id,"initial_voters":[1,2,3],"writer_epoch":1,"stream_epoch":vec![id as u8;16],"mode":"create","directory":files.0.join(format!("node-{id}")),"listen":addresses[id as usize-1],"identity":identity,"peers":peers,"application":{"max_objects":8,"max_inflight":4,"max_payload_bytes":4194304},"max_command_bytes":1048576,"max_retained_bytes":33554432,"bootstrap":null});
    let peer_file = files.put(
        &format!("{id}-peer.json"),
        &serde_json::to_vec(&peer).unwrap(),
    );
    json!({"schema":1,"peer_config_file":peer_file,"http":{"listen":address(),"identity":identity,"clients":[{"subject":tickwire_control_peer::tls::peer_uri(scope(),2),"leaf_sha256":Sha256::digest(f.certs[1].der()).to_vec()}],"max_connections":2,"max_body_bytes":4194304,"timeout_ms":3000},"deployment":null,"stop_file":files.0.join("STOP")})
}
fn parsed(v: &Value) -> Configuration {
    Configuration::parse(&serde_json::to_vec(v).unwrap()).unwrap()
}
#[test]
fn malformed_sources_and_private_key_fail_before_any_listener_or_wal() {
    let files = Files::new();
    let f = Fixture::new();
    let addresses = [address(), address(), address()];
    let mut v = value(&files, &f, 1, &addresses);
    let http: SocketAddr = v["http"]["listen"].as_str().unwrap().parse().unwrap();
    let trust=files.put("evidence.json",&serde_json::to_vec(&json!({"schema":1,"cluster_id":"lab","namespace_id":"11111111-2222-3333-4444-555555555555","revision":1,"issuers":[{"id":"independent-observer","public_key":vec![9;32],"kinds":["retained_resources"]}]})).unwrap());
    let bundle = files.put("bundle", b"corrupt source package");
    v["deployment"] = json!({"bundle_file":bundle,"evidence_trust_file":trust,"max_bundle_bytes":4096,"max_jobs":1,"account_max_age_ms":5000,"credential_max_age_ms":5000,"resource_max_age_ms":5000});
    assert!(config_runner::prepare(&parsed(&v)).is_err());
    assert!(!files.0.join("node-1").exists());
    drop(TcpListener::bind(http).unwrap());
    drop(TcpListener::bind(addresses[0]).unwrap());
    v["deployment"] = Value::Null;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let key = PathBuf::from(v["http"]["identity"]["private_key_der"].as_str().unwrap());
        fs::set_permissions(&key, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(config_runner::prepare(&parsed(&v)).is_err());
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
        let link = files.0.join("key-link");
        symlink(&key, &link).unwrap();
        v["http"]["identity"]["private_key_der"] = json!(link);
        assert!(config_runner::prepare(&parsed(&v)).is_err());
    }
    assert!(!files.0.join("node-1").exists());
    drop(TcpListener::bind(http).unwrap());
}
#[test]
fn actual_startup_files_form_three_tls_peers_and_https_without_automatic_bootstrap() {
    let files = Files::new();
    let f = Fixture::new();
    let addresses = [address(), address(), address()];
    let mut owners = Vec::new();
    for id in 1..=3 {
        let v = value(&files, &f, id, &addresses);
        let path = files.put(
            &format!("{id}-server.json"),
            &serde_json::to_vec(&v).unwrap(),
        );
        let c = config_runner::load(&path).unwrap();
        let p = config_runner::prepare(&c).unwrap();
        // Preparation has no bound listener or durable owner.
        drop(TcpListener::bind(c.http.listen).unwrap());
        assert!(!files.0.join(format!("node-{id}")).exists());
        owners.push(p.assemble().unwrap());
    }
    let until = Instant::now() + Duration::from_secs(10);
    let leader = loop {
        for o in &mut owners {
            o.poll(Instant::now(), PeerBudget::default(), 8).unwrap();
        }
        if let Some(i) = owners
            .iter()
            .position(|o| o.status().role == Role::Leader && o.status().raft_applied > 0)
        {
            break i;
        }
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(1));
    };
    let addr = owners[leader].http_addr().unwrap();
    let m = f.material(2, None, false);
    let mut roots = rustls::RootCertStore::empty();
    for r in m.roots_der {
        roots.add(r.into()).unwrap();
    }
    let mut tls = rustls::ClientConfig::builder_with_provider(
        rustls::crypto::ring::default_provider().into(),
    )
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_client_auth_cert(
        m.chain_der.into_iter().map(Into::into).collect(),
        rustls::pki_types::PrivateKeyDer::try_from(m.private_key_der).unwrap(),
    )
    .unwrap();
    tls.alpn_protocols = vec![b"http/1.1".to_vec()];
    let (tx, rx) = mpsc::channel();
    let client = std::thread::spawn(move || {
        let socket = TcpStream::connect(addr).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(4)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(4)))
            .unwrap();
        let conn =
            rustls::ClientConnection::new(tls.into(), "localhost".try_into().unwrap()).unwrap();
        let mut io = rustls::StreamOwned::new(conn, socket);
        io.write_all(
            b"GET /v1/config/schema HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .unwrap();
        io.flush().unwrap();
        let mut data = Vec::new();
        let _ = io.read_to_end(&mut data);
        tx.send(data).unwrap();
    });
    let response = loop {
        for o in &mut owners {
            o.poll(Instant::now(), PeerBudget::default(), 8).unwrap();
        }
        if let Ok(b) = rx.try_recv() {
            break b;
        }
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(1));
    };
    client.join().unwrap();
    assert!(
        response.starts_with(b"HTTP/1.1 503"),
        "{}",
        String::from_utf8_lossy(&response)
    );
    for o in &mut owners {
        assert!(o.take_applied(100, 1048576).is_empty());
        o.shutdown().unwrap();
        assert!(o.http_addr().is_err());
    }
}
#[test]
fn executable_uses_startup_config_and_graceful_stop_marker() {
    let files = Files::new();
    let f = Fixture::new();
    let addresses = [address(), address(), address()];
    let v = value(&files, &f, 1, &addresses);
    let config = files.put("server.json", &serde_json::to_vec(&v).unwrap());
    files.put("STOP", b"stop");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_tickwire-config-server"))
        .arg("--config")
        .arg(&config)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("config_server_started"));
    assert!(files.0.join("node-1").is_dir());
    let http = v["http"]["listen"].as_str().unwrap();
    drop(TcpListener::bind(http).unwrap());
    // Recovering the same physical owner does not synthesize a business bootstrap.
    let peer = Path::new(v["peer_config_file"].as_str().unwrap());
    let mut p: Value = serde_json::from_slice(&fs::read(peer).unwrap()).unwrap();
    p["mode"] = "recover".into();
    p["writer_epoch"] = 2.into();
    fs::write(peer, serde_json::to_vec(&p).unwrap()).unwrap();
    let o = config_runner::prepare(&parsed(&v))
        .unwrap()
        .assemble()
        .unwrap();
    assert_eq!(o.status().raft_applied, 0);
}

#[test]
fn prepared_source_and_separate_issuer_trust_are_owned_before_listener_assembly() {
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use tickwire_config_service::evidence::{Claim, Kind};
    let files = Files::new();
    let f = Fixture::new();
    let addresses = [address(), address(), address()];
    let mut v = value(&files, &f, 1, &addresses);
    let object = b"{\"schema\":1,\"retained\":[]}";
    let hash = Sha256::digest(object)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let reference = format!("artifact://fixture/retained@sha256:{hash}");
    let claim = Claim {
        schema: 1,
        cluster_id: "lab".into(),
        namespace_id: "11111111-2222-3333-4444-555555555555".into(),
        issuer_id: "observer".into(),
        kind: Kind::RetainedResources,
        artifact_ref: reference.clone(),
        issued_unix_ms: 1_700_000_000_000,
        expires_unix_ms: 1_700_000_005_000,
    };
    let key = Ed25519KeyPair::from_seed_unchecked(&[7; 32]).unwrap();
    let signature = key
        .sign(&claim.signing_bytes().unwrap())
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let header = serde_json::to_vec(&json!({"schema":1,"artifacts":[{"reference":reference,"byte_len":object.len()}],"evidence":[{"claim":claim,"signature":signature}],"retained_ref":reference,"credential_refs":[]})).unwrap();
    let mut bytes = b"TWDSRC01".to_vec();
    bytes.extend_from_slice(&(header.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&header);
    bytes.extend_from_slice(object);
    let bundle = files.put("sources.bin", &bytes);
    let trust = files.put("independent-evidence.json",&serde_json::to_vec(&json!({"schema":1,"cluster_id":"lab","namespace_id":"11111111-2222-3333-4444-555555555555","revision":1,"issuers":[{"id":"observer","public_key":key.public_key().as_ref(),"kinds":["retained_resources"]}]})).unwrap());
    v["deployment"] = json!({"bundle_file":bundle,"evidence_trust_file":trust,"max_bundle_bytes":4096,"max_jobs":1,"account_max_age_ms":5000,"credential_max_age_ms":5000,"resource_max_age_ms":5000});
    let prepared = config_runner::prepare(&parsed(&v)).unwrap();
    fs::write(&bundle, b"replacement cannot change prepared bytes").unwrap();
    fs::write(&trust, b"replacement cannot add a trusted issuer").unwrap();
    assert!(config_runner::prepare(&parsed(&v)).is_err());
    let mut owner = prepared.assemble().unwrap();
    assert_eq!(owner.status().raft_applied, 0);
    owner.shutdown().unwrap();
    // Loading an envelope does not grant current deployment authority. Its old
    // claim is revalidated against service time only when a candidate is checked.
}

#[test]
fn invalid_peer_material_is_rejected_during_preparation_before_http_or_wal() {
    let files = Files::new();
    let f = Fixture::new();
    let addresses = [address(), address(), address()];
    let v = value(&files, &f, 1, &addresses);
    let c = parsed(&v);
    // Give peer credentials their own paths so the valid HTTP identity remains intact.
    let peer_path = Path::new(v["peer_config_file"].as_str().unwrap());
    let mut peer: Value = serde_json::from_slice(&fs::read(peer_path).unwrap()).unwrap();
    peer["identity"]["private_key_der"] =
        json!(files.put("bad-peer-key", b"not a DER private key"));
    fs::write(peer_path, serde_json::to_vec(&peer).unwrap()).unwrap();
    assert!(
        config_runner::prepare(&c).is_err(),
        "peer credentials must be checked before Prepared is published"
    );
    assert!(!files.0.join("node-1").exists());
    drop(TcpListener::bind(c.http.listen).unwrap());
    drop(TcpListener::bind(addresses[0]).unwrap());
}
#[test]
fn peer_key_crl_and_configuration_replacement_cannot_change_owned_preparation() {
    let files = Files::new();
    let f = Fixture::new();
    let addresses = [address(), address(), address()];
    let v = value(&files, &f, 1, &addresses);
    let c = parsed(&v);
    let prepared = config_runner::prepare(&c).unwrap();
    // Both HTTP and peer material were already read. No assembly step may reopen them.
    for path in [
        &c.http.identity.private_key_der,
        &c.http.identity.crls_der[0],
        &c.peer_config_file,
    ] {
        fs::write(path, b"replaced after preparation").unwrap();
    }
    let mut owner = prepared
        .assemble()
        .expect("assembly must consume the original owned peer material");
    assert_eq!(owner.status().raft_applied, 0);
    owner.shutdown().unwrap();
}
