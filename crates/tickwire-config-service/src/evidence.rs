//! Authenticated origin of pinned deployment evidence, without deployment authority.
use crate::{Error, Hash, Result};
use ring::signature::{ED25519, UnparsedPublicKey};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use tickwire_config::{
    Candidate, Format, Limits,
    artifacts::{ArtifactCatalog, reference_hash},
};

const MAX_ENVELOPE_BYTES: usize = 16 * 1024;
const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;
const MAX_LIFETIME_MS: u64 = 7 * 24 * 60 * 60 * 1000;
const SIGNING_DOMAIN: &[u8] = b"tickwire.config.evidence.v1\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    HostInventory,
    DeploymentManifest,
    ProtocolEntitlements,
    StoragePowerLoss,
    NetworkIsolation,
    RiskAcceptance,
    RetainedResources,
    AccountMode,
    CredentialMetadata,
    RuntimeTargets,
}
pub struct Issuer {
    pub id: String,
    pub public_key: Hash,
    pub kinds: BTreeSet<Kind>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Claim {
    pub schema: u32,
    pub cluster_id: String,
    pub namespace_id: String,
    pub issuer_id: String,
    pub kind: Kind,
    pub artifact_ref: String,
    pub issued_unix_ms: u64,
    pub expires_unix_ms: u64,
}
pub struct Trust {
    cluster: String,
    namespace: String,
    issuers: BTreeMap<String, Issuer>,
    digest: Hash,
    canonical: Vec<u8>,
}
pub struct VerifiedEvidence {
    envelope: Vec<u8>,
    claim: Claim,
    artifact_hash: Hash,
    trust_hash: Hash,
    checked_unix_ms: u64,
}
#[derive(Default)]
pub struct Catalog {
    envelopes: BTreeMap<(Kind, String), Vec<u8>>,
    bytes: usize,
}
impl Catalog {
    pub fn insert(&mut self, envelope: &[u8]) -> Result<()> {
        let canonical = strict(envelope)?;
        let parsed: Envelope =
            serde_json::from_value(canonical.value().clone()).map_err(|_| Error::Protocol)?;
        parsed.claim.signing_bytes()?;
        signature_bytes(&parsed.signature)?;
        let key = (parsed.claim.kind, parsed.claim.artifact_ref);
        if let Some(prior) = self.envelopes.get(&key) {
            return if prior == canonical.canonical_bytes() {
                Ok(())
            } else {
                Err(Error::Conflict)
            };
        }
        let bytes = self
            .bytes
            .checked_add(canonical.canonical_bytes().len())
            .ok_or(Error::Bounds)?;
        if self.envelopes.len() >= 4096 || bytes > 64 * 1024 * 1024 {
            return Err(Error::Bounds);
        }
        self.envelopes
            .insert(key, canonical.canonical_bytes().to_vec());
        self.bytes = bytes;
        Ok(())
    }
    pub(crate) fn get(&self, kind: Kind, reference: &str) -> Option<&[u8]> {
        self.envelopes
            .get(&(kind, reference.to_owned()))
            .map(Vec::as_slice)
    }
    /// Canonical unverified envelopes, sorted by kind and full reference.
    pub fn envelopes(&self) -> impl Iterator<Item = (Kind, &str, &[u8])> {
        self.envelopes
            .iter()
            .map(|((kind, reference), bytes)| (*kind, reference.as_str(), bytes.as_slice()))
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    claim: Claim,
    signature: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustDescription {
    schema: u32,
    cluster_id: String,
    namespace_id: String,
    revision: String,
    issuers: Vec<IssuerDescription>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IssuerDescription {
    id: String,
    public_key: Hash,
    kinds: Vec<Kind>,
}
impl Trust {
    pub fn new(
        cluster: String,
        namespace: String,
        revision: u64,
        issuers: Vec<Issuer>,
    ) -> Result<Self> {
        if !valid_id(&cluster)
            || !valid_namespace(&namespace)
            || revision == 0
            || revision >= 1 << 63
        {
            return Err(Error::Identity);
        }
        if issuers.is_empty() || issuers.len() > 32 {
            return Err(Error::Bounds);
        }
        let mut by_id = BTreeMap::new();
        let mut keys = BTreeSet::new();
        for issuer in issuers {
            if !valid_id(&issuer.id)
                || issuer.public_key == [0; 32]
                || issuer.kinds.is_empty()
                || !keys.insert(issuer.public_key)
                || by_id.insert(issuer.id.clone(), issuer).is_some()
            {
                return Err(Error::Identity);
            }
        }
        let description = json!({
            "schema":1,
            "cluster_id":cluster,
            "namespace_id":namespace,
            "revision":revision.to_string(),
            "issuers":by_id.values().map(|issuer| json!({
                "id":issuer.id,"public_key":issuer.public_key,"kinds":issuer.kinds
            })).collect::<Vec<_>>()
        });
        let canonical = strict(&serde_json::to_vec(&description).map_err(|_| Error::Protocol)?)?;
        let mut hash = Sha256::new();
        hash.update(b"tickwire.config.evidence-trust.v1\0");
        hash.update(canonical.canonical_bytes());
        Ok(Self {
            cluster,
            namespace,
            issuers: by_id,
            digest: hash.finalize().into(),
            canonical: canonical.canonical_bytes().to_vec(),
        })
    }
    /// The exact bounded description used by the existing trust digest.
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }
    /// Restores configuration content, not approval to use it as current trust.
    pub fn decode(bytes: &[u8], expected_digest: Hash) -> Result<Self> {
        let canonical = strict(bytes)?;
        if canonical.canonical_bytes() != bytes {
            return Err(Error::Protocol);
        }
        let description: TrustDescription =
            serde_json::from_value(canonical.value().clone()).map_err(|_| Error::Protocol)?;
        if description.schema != 1 {
            return Err(Error::Protocol);
        }
        let revision: u64 = description.revision.parse().map_err(|_| Error::Identity)?;
        if revision.to_string() != description.revision {
            return Err(Error::Identity);
        }
        if description
            .issuers
            .windows(2)
            .any(|pair| pair[0].id >= pair[1].id)
        {
            return Err(Error::Protocol);
        }
        let issuers = description
            .issuers
            .into_iter()
            .map(|issuer| {
                if issuer.kinds.windows(2).any(|pair| pair[0] >= pair[1]) {
                    return Err(Error::Protocol);
                }
                Ok(Issuer {
                    id: issuer.id,
                    public_key: issuer.public_key,
                    kinds: issuer.kinds.into_iter().collect(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let trust = Self::new(
            description.cluster_id,
            description.namespace_id,
            revision,
            issuers,
        )?;
        if trust.canonical_bytes() != bytes {
            return Err(Error::Protocol);
        }
        if trust.digest() != expected_digest {
            return Err(Error::Digest);
        }
        Ok(trust)
    }
    pub fn digest(&self) -> Hash {
        self.digest
    }
    pub(crate) fn permits(&self, issuer: &str, kind: Kind) -> bool {
        self.issuers
            .get(issuer)
            .is_some_and(|i| i.kinds.contains(&kind))
    }
    pub fn verify(
        &self,
        envelope: &[u8],
        catalog: &ArtifactCatalog,
        kind: Kind,
        reference: &str,
        now_unix_ms: u64,
    ) -> Result<VerifiedEvidence> {
        if now_unix_ms == 0 || now_unix_ms > MAX_SAFE_INTEGER {
            return Err(Error::Bounds);
        }
        let canonical = strict(envelope)?;
        let parsed: Envelope =
            serde_json::from_value(canonical.value().clone()).map_err(|_| Error::Protocol)?;
        let claim = parsed.claim;
        let signing = claim.signing_bytes()?;
        if claim.cluster_id != self.cluster
            || claim.namespace_id != self.namespace
            || claim.kind != kind
            || claim.artifact_ref != reference
        {
            return Err(Error::Identity);
        }
        let issuer = self
            .issuers
            .get(&claim.issuer_id)
            .filter(|issuer| issuer.kinds.contains(&claim.kind))
            .ok_or(Error::Authentication)?;
        let signature = signature_bytes(&parsed.signature)?;
        UnparsedPublicKey::new(&ED25519, issuer.public_key)
            .verify(&signing, &signature)
            .map_err(|_| Error::Authentication)?;
        if now_unix_ms < claim.issued_unix_ms || now_unix_ms >= claim.expires_unix_ms {
            return Err(Error::Authentication);
        }
        let bytes = catalog.get(reference).ok_or(Error::NotFound)?;
        let artifact_hash: Hash = Sha256::digest(bytes).into();
        if artifact_hash != reference_hash(reference).map_err(Error::Configuration)? {
            return Err(Error::Digest);
        }
        Ok(VerifiedEvidence {
            envelope: canonical.canonical_bytes().to_vec(),
            claim,
            artifact_hash,
            trust_hash: self.digest,
            checked_unix_ms: now_unix_ms,
        })
    }
}
impl Claim {
    pub fn signing_bytes(&self) -> Result<Vec<u8>> {
        if self.schema != 1
            || !valid_id(&self.cluster_id)
            || !valid_namespace(&self.namespace_id)
            || !valid_id(&self.issuer_id)
        {
            return Err(Error::Identity);
        }
        reference_hash(&self.artifact_ref).map_err(Error::Configuration)?;
        if self.issued_unix_ms == 0
            || self.expires_unix_ms > MAX_SAFE_INTEGER
            || self
                .expires_unix_ms
                .checked_sub(self.issued_unix_ms)
                .is_none_or(|lifetime| lifetime == 0 || lifetime > MAX_LIFETIME_MS)
        {
            return Err(Error::Bounds);
        }
        let canonical = strict(&serde_json::to_vec(self).map_err(|_| Error::Protocol)?)?;
        let mut bytes =
            Vec::with_capacity(SIGNING_DOMAIN.len() + canonical.canonical_bytes().len());
        bytes.extend_from_slice(SIGNING_DOMAIN);
        bytes.extend_from_slice(canonical.canonical_bytes());
        Ok(bytes)
    }
}
impl VerifiedEvidence {
    pub fn claim(&self) -> &Claim {
        &self.claim
    }
    pub fn envelope(&self) -> &[u8] {
        &self.envelope
    }
    pub fn artifact_hash(&self) -> Hash {
        self.artifact_hash
    }
    pub fn trust_hash(&self) -> Hash {
        self.trust_hash
    }
    pub fn checked_unix_ms(&self) -> u64 {
        self.checked_unix_ms
    }
    pub fn revalidate(
        &self,
        trust: &Trust,
        catalog: &ArtifactCatalog,
        now_unix_ms: u64,
    ) -> Result<VerifiedEvidence> {
        trust.verify(
            &self.envelope,
            catalog,
            self.claim.kind,
            &self.claim.artifact_ref,
            now_unix_ms,
        )
    }
}
fn strict(bytes: &[u8]) -> Result<Candidate> {
    Candidate::parse(
        Format::Json,
        bytes,
        Limits {
            max_bytes: MAX_ENVELOPE_BYTES,
            max_depth: 8,
        },
    )
    .map_err(Error::Configuration)
}
fn valid_id(value: &str) -> bool {
    (1..=63).contains(&value.len())
        && value.as_bytes()[0].is_ascii_lowercase()
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}
fn valid_namespace(value: &str) -> bool {
    value.len() == 36
        && value != "00000000-0000-0000-0000-000000000000"
        && value.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
}
fn signature_bytes(value: &str) -> Result<[u8; 64]> {
    if value.len() != 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Error::Authentication);
    }
    let mut output = [0; 64];
    for (out, pair) in output.iter_mut().zip(value.as_bytes().as_chunks::<2>().0) {
        let digit = |b: u8| if b <= b'9' { b - b'0' } else { b - b'a' + 10 };
        *out = digit(pair[0]) * 16 + digit(pair[1]);
    }
    Ok(output)
}
