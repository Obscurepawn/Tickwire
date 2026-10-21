#![allow(dead_code)]
use rcgen::*;
use sha2::{Digest, Sha256};
use tickwire_control_peer::tls::*;
use time::OffsetDateTime;
pub struct Fixture {
    pub certs: Vec<Certificate>,
    pub keys: Vec<KeyPair>,
    root: Certificate,
    issuer: Issuer<'static, KeyPair>,
}
impl Fixture {
    pub fn new() -> Self {
        Self::with_nodes(3)
    }
    pub fn with_nodes(count: u64) -> Self {
        let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let key = KeyPair::generate().unwrap();
        let root = ca.self_signed(&key).unwrap();
        let issuer = Issuer::new(ca, key);
        let mut certs = Vec::new();
        let mut keys = Vec::new();
        for id in 1..=count {
            let mut p = CertificateParams::new(vec!["localhost".into()]).unwrap();
            p.subject_alt_names
                .push(SanType::URI(peer_uri(scope(), id).try_into().unwrap()));
            p.serial_number = Some(SerialNumber::from(id));
            p.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ];
            let key = KeyPair::generate().unwrap();
            certs.push(p.signed_by(&key, &issuer).unwrap());
            keys.push(key);
        }
        Self {
            certs,
            keys,
            root,
            issuer,
        }
    }
    pub fn peers(&self, id: u64) -> Vec<PeerCertificate> {
        (1..=self.certs.len() as u64)
            .filter(|n| *n != id)
            .map(|n| PeerCertificate {
                node_id: n,
                server_name: "localhost".into(),
                leaf_sha256: Sha256::digest(self.certs[(n - 1) as usize].der()).into(),
            })
            .collect()
    }
    pub fn material(&self, id: u64, revoked: Option<u64>, stale: bool) -> IdentityMaterial {
        let now = OffsetDateTime::now_utc();
        let crl = CertificateRevocationListParams {
            this_update: now - time::Duration::hours(2),
            next_update: now + time::Duration::hours(if stale { -1 } else { 1 }),
            crl_number: SerialNumber::from(1u64),
            issuing_distribution_point: None,
            key_identifier_method: KeyIdMethod::Sha256,
            revoked_certs: revoked
                .map(|id| RevokedCertParams {
                    serial_number: SerialNumber::from(id),
                    revocation_time: now - time::Duration::hours(1),
                    reason_code: None,
                    invalidity_date: None,
                })
                .into_iter()
                .collect(),
        }
        .signed_by(&self.issuer)
        .unwrap();
        IdentityMaterial {
            chain_der: vec![self.certs[(id - 1) as usize].der().to_vec()],
            private_key_der: self.keys[(id - 1) as usize].serialize_der(),
            roots_der: vec![self.root.der().to_vec()],
            crls_der: vec![crl.der().to_vec()],
        }
    }
    pub fn tls(&self, id: u64) -> PeerTls {
        PeerTls::new(scope(), id, self.material(id, None, false), self.peers(id)).unwrap()
    }
}
pub fn scope() -> Scope {
    Scope {
        cluster: [1; 16],
        group: [2; 16],
    }
}
