use super::*;
use crate::reconciliation::income_closure::encoding::{Reader, Writer};
use tickwire_account::Scope;
use tickwire_clock::{ClockDomain, MonoTime};
const CERT_DOMAIN: &[u8] = b"tickwire.usdm.income-association-authority.v1";
const APPROVAL_DOMAIN: &[u8] = b"tickwire.usdm.income-association.v1";
pub const MAX_SIGNED_BYTES: usize = 1024;
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssociationAuthority {
    pub controller_id: [u8; 16],
    /// Dedicated root key: cannot be an ordinary grant/baseline authority.
    pub public_key: [u8; 32],
    /// Current deployment-approved leaf certificate. None denies new approvals;
    /// old durable records are still verified using the same pinned root.
    pub active_policy: Option<ActiveAssociationPolicy>,
    /// Actual executor identity from trusted deployment, never from a request.
    pub execution_instance: [u8; 16],
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveAssociationPolicy {
    pub certificate_digest: v::Hash,
    pub authorization_revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorityClaims {
    pub controller_id: [u8; 16],
    pub key_id: [u8; 16],
    pub revision: u64,
    pub scope: Scope,
    pub wallet_id: u64,
    pub execution_instance: [u8; 16],
    pub writer_epoch: u64,
    pub clock_domain: ClockDomain,
    pub issued: u64,
    pub expires: u64,
    pub public_key: [u8; 32],
    /// Immutable authenticated RiskAdmin authorization/audit source, checked by
    /// the external dedicated root issuer before it signs this certificate.
    pub authorization_digest: v::Hash,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedAssociationAuthority {
    payload: Vec<u8>,
    signature: [u8; 64],
    claims: AuthorityClaims,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedAssociationApproval {
    pub(super) authority: SignedAssociationAuthority,
    pub(super) payload: Vec<u8>,
    pub(super) signature: [u8; 64],
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ApprovalClaims {
    pub request: [u8; 16],
    pub manifest: v::Hash,
    pub authority: v::Hash,
    pub audit: v::Hash,
    pub issued: u64,
    pub expires: u64,
}
impl AssociationAuthority {
    pub fn validate(&self) -> Result<()> {
        if self.controller_id == [0; 16]
            || self.public_key == [0; 32]
            || self.execution_instance == [0; 16]
            || self
                .active_policy
                .as_ref()
                .is_some_and(|p| p.certificate_digest == [0; 32] || p.authorization_revision == 0)
        {
            Err(Error::Identity)
        } else {
            Ok(())
        }
    }
    pub(super) fn verify_certificate(
        &self,
        cert: &SignedAssociationAuthority,
        now: MonoTime,
        live: bool,
    ) -> Result<()> {
        self.validate()?;
        let a = &cert.claims;
        if a.controller_id != self.controller_id
            || a.clock_domain != now.domain()
            || a.issued > now.nanos()
            || now.nanos() >= a.expires
        {
            return Err(Error::Identity);
        }
        if live
            && (a.execution_instance != self.execution_instance
                || self.active_policy.as_ref()
                    != Some(&ActiveAssociationPolicy {
                        certificate_digest: cert.digest(),
                        authorization_revision: a.revision,
                    }))
        {
            return Err(Error::Identity);
        }
        ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, self.public_key)
            .verify(&cert.payload, &cert.signature)
            .map_err(|_| Error::Identity)
    }
    pub(super) fn verify(
        &self,
        approval: &SignedAssociationApproval,
        now: MonoTime,
        live: bool,
    ) -> Result<ApprovalClaims> {
        self.verify_certificate(&approval.authority, now, live)?;
        let claims = ApprovalClaims::decode(&approval.payload)?;
        let a = &approval.authority.claims;
        if claims.authority != approval.authority.digest()
            || claims.issued < a.issued
            || claims.expires > a.expires
            || claims.issued > now.nanos()
            || now.nanos() >= claims.expires
        {
            return Err(Error::Identity);
        }
        ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, a.public_key)
            .verify(&approval.payload, &approval.signature)
            .map_err(|_| Error::Identity)?;
        Ok(claims)
    }
}
impl AuthorityClaims {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut w = Writer(Vec::new());
        w.array(18);
        w.blob(CERT_DOMAIN);
        w.uint(1);
        w.blob(&self.controller_id);
        w.blob(&self.key_id);
        w.uint(self.revision);
        w.array(4);
        w.blob(&self.scope.risk_domain_id);
        w.uint(self.scope.venue as u64);
        w.uint(match self.scope.environment {
            v::Environment::Production => 1,
            v::Environment::Test => 2,
        });
        w.uint(self.scope.account);
        w.uint(self.wallet_id);
        w.blob(&self.execution_instance);
        w.uint(self.writer_epoch);
        w.blob(&self.clock_domain.boot_id());
        w.uint(self.clock_domain.generation());
        w.uint(self.issued);
        w.uint(self.expires);
        w.blob(&self.public_key);
        w.blob(&self.authorization_digest);
        // The leaf is restricted to this exact approval purpose, including schema.
        w.blob(APPROVAL_DOMAIN);
        w.uint(1);
        w.uint(1); // scoped RiskAdmin decision, not a general role string
        Ok(w.0)
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_SIGNED_BYTES {
            return Err(Error::Capacity);
        }
        let mut r = Reader { bytes, pos: 0 };
        r.array(18)?;
        if r.blob(64)? != CERT_DOMAIN || r.uint()? != 1 {
            return Err(Error::Encoding);
        }
        let controller_id = r.fixed()?;
        let key_id = r.fixed()?;
        let revision = r.uint()?;
        r.array(4)?;
        let scope = Scope {
            risk_domain_id: r.fixed()?,
            venue: r.uint()?.try_into().map_err(|_| Error::Encoding)?,
            environment: match r.uint()? {
                1 => v::Environment::Production,
                2 => v::Environment::Test,
                _ => return Err(Error::Encoding),
            },
            account: r.uint()?,
        };
        let wallet_id = r.uint()?;
        let execution_instance = r.fixed()?;
        let writer_epoch = r.uint()?;
        let clock_domain = ClockDomain::new(r.fixed()?, r.uint()?).map_err(|_| Error::Identity)?;
        let issued = r.uint()?;
        let expires = r.uint()?;
        let public_key = r.fixed()?;
        let authorization_digest = r.fixed()?;
        if r.blob(64)? != APPROVAL_DOMAIN
            || r.uint()? != 1
            || r.uint()? != 1
            || r.pos != bytes.len()
        {
            return Err(Error::Encoding);
        }
        let out = Self {
            controller_id,
            key_id,
            revision,
            scope,
            wallet_id,
            execution_instance,
            writer_epoch,
            clock_domain,
            issued,
            expires,
            public_key,
            authorization_digest,
        };
        out.validate()?;
        if out.bytes()? != bytes {
            return Err(Error::Encoding);
        }
        Ok(out)
    }
    fn validate(&self) -> Result<()> {
        if self.controller_id == [0; 16]
            || self.key_id == [0; 16]
            || self.revision == 0
            || self.scope.risk_domain_id == [0; 16]
            || self.scope.venue == 0
            || self.scope.account == 0
            || self.wallet_id == 0
            || self.execution_instance == [0; 16]
            || self.writer_epoch == 0
            || self.issued >= self.expires
            || self.public_key == [0; 32]
            || self.authorization_digest == [0; 32]
        {
            Err(Error::Identity)
        } else {
            Ok(())
        }
    }
}
impl SignedAssociationAuthority {
    pub fn new(payload: &[u8], signature: &[u8]) -> Result<Self> {
        let claims = AuthorityClaims::decode(payload)?;
        Ok(Self {
            payload: payload.to_vec(),
            signature: signature.try_into().map_err(|_| Error::Encoding)?,
            claims,
        })
    }
    pub fn claims(&self) -> &AuthorityClaims {
        &self.claims
    }
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
    pub fn signature(&self) -> &[u8; 64] {
        &self.signature
    }
    pub fn bytes(&self) -> Vec<u8> {
        let mut b = self.payload.clone();
        b.extend(self.signature);
        b
    }
    pub fn digest(&self) -> v::Hash {
        hash(&self.bytes())
    }
}
impl SignedAssociationApproval {
    pub fn payload_to_sign(
        authority: &SignedAssociationAuthority,
        request: [u8; 16],
        manifest: v::Hash,
        audit: v::Hash,
        issued: u64,
        expires: u64,
    ) -> Result<Vec<u8>> {
        ApprovalClaims {
            request,
            manifest,
            authority: authority.digest(),
            audit,
            issued,
            expires,
        }
        .bytes()
    }
    pub fn new(
        authority: SignedAssociationAuthority,
        payload: &[u8],
        signature: &[u8],
    ) -> Result<Self> {
        let claims = ApprovalClaims::decode(payload)?;
        if claims.authority != authority.digest() {
            return Err(Error::Identity);
        }
        Ok(Self {
            authority,
            payload: payload.to_vec(),
            signature: signature.try_into().map_err(|_| Error::Encoding)?,
        })
    }
    pub fn manifest_digest(&self) -> Result<v::Hash> {
        Ok(ApprovalClaims::decode(&self.payload)?.manifest)
    }
    pub fn request_id(&self) -> Result<[u8; 16]> {
        Ok(ApprovalClaims::decode(&self.payload)?.request)
    }
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
    pub fn signature(&self) -> &[u8; 64] {
        &self.signature
    }
    pub fn authority(&self) -> &SignedAssociationAuthority {
        &self.authority
    }
    pub fn bytes(&self) -> Vec<u8> {
        let mut w = Writer(Vec::new());
        w.array(4);
        w.blob(&self.authority.payload);
        w.blob(&self.authority.signature);
        w.blob(&self.payload);
        w.blob(&self.signature);
        w.0
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > 2 * MAX_SIGNED_BYTES + 160 {
            return Err(Error::Capacity);
        }
        let mut r = Reader { bytes, pos: 0 };
        r.array(4)?;
        let cert = r.blob(MAX_SIGNED_BYTES)?;
        let signature = r.fixed::<64>()?;
        let authority = SignedAssociationAuthority::new(cert, &signature)?;
        let payload = r.blob(MAX_SIGNED_BYTES)?;
        let signature = r.fixed::<64>()?;
        if r.pos != bytes.len() {
            return Err(Error::Encoding);
        }
        Self::new(authority, payload, &signature)
    }
    pub fn digest(&self) -> v::Hash {
        hash(&self.bytes())
    }
}
impl ApprovalClaims {
    fn bytes(&self) -> Result<Vec<u8>> {
        if self.request == [0; 16]
            || [self.manifest, self.authority, self.audit].contains(&[0; 32])
            || self.issued >= self.expires
        {
            return Err(Error::Identity);
        }
        let mut w = Writer(Vec::new());
        w.array(8);
        w.blob(APPROVAL_DOMAIN);
        w.uint(1);
        w.blob(&self.request);
        w.blob(&self.manifest);
        w.blob(&self.authority);
        w.blob(&self.audit);
        w.uint(self.issued);
        w.uint(self.expires);
        Ok(w.0)
    }
    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_SIGNED_BYTES {
            return Err(Error::Capacity);
        }
        let mut r = Reader { bytes, pos: 0 };
        r.array(8)?;
        if r.blob(64)? != APPROVAL_DOMAIN || r.uint()? != 1 {
            return Err(Error::Encoding);
        }
        let out = Self {
            request: r.fixed()?,
            manifest: r.fixed()?,
            authority: r.fixed()?,
            audit: r.fixed()?,
            issued: r.uint()?,
            expires: r.uint()?,
        };
        if r.pos != bytes.len() || out.bytes()? != bytes {
            return Err(Error::Encoding);
        }
        Ok(out)
    }
}
pub(super) fn hash(bytes: &[u8]) -> v::Hash {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .try_into()
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::signature::{Ed25519KeyPair, KeyPair};
    #[test]
    fn association_dedicated_root_leaf_and_original_acceptance_are_all_required() {
        let root = Ed25519KeyPair::from_seed_unchecked(&[50; 32]).unwrap();
        let leaf = Ed25519KeyPair::from_seed_unchecked(&[51; 32]).unwrap();
        let domain = ClockDomain::new([8; 16], 1).unwrap();
        let claims = AuthorityClaims {
            controller_id: [1; 16],
            key_id: [2; 16],
            revision: 3,
            scope: Scope {
                risk_domain_id: [4; 16],
                venue: 1,
                environment: v::Environment::Test,
                account: 2,
            },
            wallet_id: 1,
            execution_instance: [5; 16],
            writer_epoch: 7,
            clock_domain: domain,
            issued: 10,
            expires: 100,
            public_key: leaf.public_key().as_ref().try_into().unwrap(),
            authorization_digest: [6; 32],
        };
        let bytes = claims.bytes().unwrap();
        let cert = SignedAssociationAuthority::new(&bytes, root.sign(&bytes).as_ref()).unwrap();
        let payload =
            SignedAssociationApproval::payload_to_sign(&cert, [7; 16], [8; 32], [9; 32], 20, 80)
                .unwrap();
        let approval =
            SignedAssociationApproval::new(cert.clone(), &payload, leaf.sign(&payload).as_ref())
                .unwrap();
        let mut authority = AssociationAuthority {
            controller_id: [1; 16],
            public_key: root.public_key().as_ref().try_into().unwrap(),
            active_policy: Some(ActiveAssociationPolicy {
                certificate_digest: cert.digest(),
                authorization_revision: 3,
            }),
            execution_instance: [5; 16],
        };
        assert!(
            authority
                .verify(&approval, MonoTime::new(domain, 30), true)
                .is_ok()
        );
        assert!(
            authority
                .verify(&approval, MonoTime::new(domain, 80), true)
                .is_err()
        );
        authority.active_policy = None;
        assert!(
            authority
                .verify(&approval, MonoTime::new(domain, 30), true)
                .is_err()
        );
        assert!(
            authority
                .verify(&approval, MonoTime::new(domain, 30), false)
                .is_ok()
        );
        authority.public_key = leaf.public_key().as_ref().try_into().unwrap();
        assert!(
            authority
                .verify(&approval, MonoTime::new(domain, 30), false)
                .is_err()
        );
    }
}
