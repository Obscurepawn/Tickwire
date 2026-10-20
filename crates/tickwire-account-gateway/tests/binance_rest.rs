mod common;
use common::*;
use std::{collections::BTreeMap, time::Instant};
use tickwire_account_gateway::*;
use tickwire_account_journal::FileBackend;
use tickwire_binance::{
    auth::SigningKey,
    request::{Credentials, Market, Timestamp},
};
use tickwire_gateway::binance::RestEncoder;
use tickwire_recovery::{BlobStore, FileBlobStore, Persistence, VerifiedClosure};
use tickwire_transport::IoBudget;
use tickwire_venue::Environment;

/// Real immutable file persistence; only the policy/control service is mocked.
struct DiskAuthority {
    policy: MockAuthority,
    persistence: Persistence<FileBlobStore>,
}
impl Authority for DiskAuthority {
    fn prepare(
        &mut self,
        r: AdmissionRequest<'_>,
    ) -> std::result::Result<VerifiedClosure, AuthorityError> {
        let capability = self.policy.prepare(r)?;
        let bytes = self
            .policy
            .blobs
            .get(
                tickwire_recovery::StoreKey::Manifest(capability.manifest_root()),
                tickwire_recovery::MAX_MANIFEST_BYTES,
            )
            .map_err(AuthorityError::Recovery)?;
        let manifest = tickwire_recovery::RecoveryManifest::decode(&bytes)
            .map_err(AuthorityError::Recovery)?;
        for object in &manifest.objects {
            let bytes = self
                .policy
                .blobs
                .get(
                    tickwire_recovery::StoreKey::Object(object.content_hash),
                    object.byte_length as usize,
                )
                .map_err(AuthorityError::Recovery)?;
            self.persistence
                .put_object(&bytes)
                .map_err(AuthorityError::Recovery)?;
        }
        self.persistence
            .publish(&manifest)
            .map_err(AuthorityError::Recovery)
    }
    fn verify(
        &mut self,
        r: AdmissionRequest<'_>,
    ) -> std::result::Result<VerifiedClosure, AuthorityError> {
        self.persistence
            .verify(&r.binding)
            .map_err(AuthorityError::Recovery)
    }
    fn current_gate(
        &mut self,
        c: &tickwire_venue::CanonicalCommand,
        a: &tickwire_account::Admission,
    ) -> std::result::Result<Gate, AuthorityError> {
        self.policy.current_gate(c, a)
    }
}

fn verify_wire(bytes: &[u8], market: Market) {
    let request = std::str::from_utf8(bytes).unwrap();
    let first = request.lines().next().unwrap();
    assert!(first.starts_with(if market == Market::Spot {
        "POST /api/v3/order?"
    } else {
        "POST /fapi/v1/order?"
    }));
    assert!(request.contains("X-MBX-APIKEY: local-protocol-key\r\n"));
    let query = first
        .split_whitespace()
        .nth(1)
        .unwrap()
        .split_once('?')
        .unwrap()
        .1;
    let (body, signature) = query.rsplit_once("&signature=").unwrap();
    let signature: Vec<_> = (0..signature.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&signature[i..i + 2], 16).unwrap())
        .collect();
    ring::hmac::verify(
        &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"local-protocol-secret"),
        body.as_bytes(),
        &signature,
    )
    .unwrap();
    let fields: BTreeMap<_, _> = body
        .split('&')
        .map(|kv| kv.split_once('=').unwrap())
        .collect();
    assert_eq!(fields["symbol"], "BTCUSDT");
    assert_eq!(fields["side"], "BUY");
    assert_eq!(fields["newClientOrderId"], "test-1");
    assert_eq!(fields["timestamp"], "1668481559918");
    assert_eq!(
        fields["quantity"],
        if market == Market::Spot { "0.001" } else { "1" }
    );
    assert_eq!(
        fields["price"],
        if market == Market::Spot {
            "60000"
        } else {
            "100"
        }
    );
    if market == Market::Usdm {
        assert_eq!(fields["positionSide"], "BOTH");
    }
}
fn roundtrip(market: Market) {
    let wal = Directory::new();
    let blobs = Directory::new();
    let (b, c) = baseline(market == Market::Usdm);
    let cfg = config(&b);
    let (connection, peer) = peer(&cfg.route.authority, 13);
    let policy = MockAuthority::default();
    let auth = DiskAuthority {
        policy: policy.clone(),
        persistence: Persistence::new(FileBlobStore::open(&blobs.0).unwrap()),
    };
    let key = SigningKey::hmac(b"local-protocol-secret").unwrap();
    let credentials = Credentials {
        account: 2,
        environment: Environment::Test,
        market,
        api_key: "local-protocol-key",
        signing_key: &key,
    };
    let encoder = RestEncoder::new(
        credentials,
        || Ok(Timestamp::Milliseconds(1_668_481_559_918)),
        5_000_000,
        32768,
    )
    .unwrap();
    let mut g = AccountGateway::create(
        FileBackend::open(&wal.0).unwrap(),
        cfg,
        b,
        auth,
        encoder,
        connection,
    )
    .unwrap();
    g.submit(c.clone(), 60, until()).unwrap();
    let required = g
        .committed(intent())
        .unwrap()
        .receipt()
        .physical_frontier()
        .clone();
    assert_eq!(g.journal_position(), 1);
    let deadline = until();
    while g.status(intent()) != Some(Status::Flushed) {
        assert!(Instant::now() < deadline);
        g.poll(Instant::now(), IoBudget::default()).unwrap();
        std::thread::yield_now();
    }
    let before = g.account().asset(fixture::key(2)).unwrap();
    assert_eq!(policy.policy.lock().unwrap().gate_calls, 1);
    drop(g);
    verify_wire(&peer.join().unwrap(), market);
    let (b, _) = baseline(market == Market::Usdm);
    let mut cfg = config(&b);
    cfg.writer_epoch = 5;
    let auth = DiskAuthority {
        policy,
        persistence: Persistence::new(FileBlobStore::open(&blobs.0).unwrap()),
    };
    let mut restored = AccountGateway::recover(
        FileBackend::open(&wal.0).unwrap(),
        cfg,
        auth,
        TestEncoder,
        Some(&required),
    )
    .unwrap();
    assert!(restored.halted());
    assert_eq!(restored.account().asset(fixture::key(2)).unwrap(), before);
    assert_eq!(restored.submit(c, 6000, until()).unwrap(), Status::Unknown);
    assert!(restored.committed(intent()).is_none());
}
#[test]
fn spot_signed_tls_request_with_real_wal_and_recovery_files() {
    roundtrip(Market::Spot)
}
#[test]
fn usdm_signed_tls_request_with_real_wal_and_recovery_files() {
    roundtrip(Market::Usdm)
}
