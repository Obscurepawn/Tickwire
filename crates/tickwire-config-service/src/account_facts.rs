//! Pinned, authenticated account observations for a configuration candidate.
use crate::{
    Error, Hash, Result,
    evidence::{Catalog, Kind, Trust, VerifiedEvidence},
};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tickwire_config::{Candidate, Format, Limits as ParseLimits, artifacts::ArtifactCatalog};

#[derive(Clone, Copy)]
pub struct Limits {
    pub max_age_ms: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self { max_age_ms: 5_000 }
    }
}
pub struct AccountFact {
    identity_hash: Hash,
    mode_hash: Hash,
    observed_unix_ms: u64,
    funds_identity: String,
    evidence: VerifiedEvidence,
}
impl AccountFact {
    pub fn identity_hash(&self) -> Hash {
        self.identity_hash
    }
    pub fn mode_hash(&self) -> Hash {
        self.mode_hash
    }
    pub fn observed_unix_ms(&self) -> u64 {
        self.observed_unix_ms
    }
    pub fn funds_identity(&self) -> &str {
        &self.funds_identity
    }
    pub fn evidence(&self) -> &VerifiedEvidence {
        &self.evidence
    }
}
pub struct AccountModes {
    candidate_hash: Hash,
    accounts: BTreeMap<String, AccountFact>,
    expires_unix_ms: u64,
}
impl AccountModes {
    pub fn check(
        candidate: &Candidate,
        artifacts: &ArtifactCatalog,
        trust: &Trust,
        signatures: &Catalog,
        now_unix_ms: u64,
        limits: Limits,
    ) -> Result<Self> {
        if !(1..=60_000).contains(&limits.max_age_ms) {
            return Err(Error::Bounds);
        }
        candidate.validate_static().map_err(Error::Configuration)?;
        let value = candidate.value();
        let cluster = text(&value["identity"], "cluster_id")?;
        let namespace = text(&value["identity"], "namespace_id")?;
        let mut accounts = BTreeMap::new();
        let mut funds = BTreeMap::new();
        let mut expires_unix_ms = u64::MAX;
        for account in array(value, "accounts")? {
            let id = text(account, "id")?;
            let venue_id = text(account, "venue_id")?;
            let venue = array(value, "venues")?
                .iter()
                .find(|venue| venue["id"] == venue_id)
                .ok_or(Error::Identity)?;
            let identity_ref = text(account, "account_identity_ref")?;
            let mode_ref = text(account, "mode_snapshot_ref")?;
            let evidence = trust.verify(
                signatures
                    .get(Kind::AccountMode, mode_ref)
                    .ok_or(Error::NotFound)?,
                artifacts,
                Kind::AccountMode,
                mode_ref,
                now_unix_ms,
            )?;
            if evidence.claim().cluster_id != cluster || evidence.claim().namespace_id != namespace
            {
                return Err(Error::Identity);
            }
            let identity_bytes = artifacts.get(identity_ref).ok_or(Error::NotFound)?;
            let identity: AccountIdentity = decode(identity_bytes)?;
            let mode: ModeSnapshot = decode(artifacts.get(mode_ref).ok_or(Error::NotFound)?)?;
            if identity.schema != 1
                || venue["venue"] != identity.venue
                || venue["environment"] != identity.environment
                || venue["product"] != identity.product
                || !opaque_identity(&identity.exchange_account_id, 128)
                || !opaque_identity(&identity.funds_identity, 192)
                || mode.schema != 1
                || mode.cluster_id != cluster
                || mode.namespace_id != namespace
                || mode.account_id != id
                || mode.venue_id != venue_id
                || mode.environment != identity.environment
                || mode.product != identity.product
                || mode.account_identity_ref != identity_ref
                || mode.observed_exchange_account_id != identity.exchange_account_id
                || mode.observed_funds_identity != identity.funds_identity
                || account["position_mode"] != mode.position_mode
                || account["margin_mode"] != mode.margin_mode
                || account["asset_mode"] != mode.asset_mode
                || !mode.trading_enabled
            {
                return Err(Error::Identity);
            }
            let observation_expires = mode
                .observed_unix_ms
                .checked_add(limits.max_age_ms)
                .ok_or(Error::Bounds)?;
            if mode.observed_unix_ms == 0
                || mode.observed_unix_ms > evidence.claim().issued_unix_ms
                || now_unix_ms >= observation_expires
            {
                return Err(Error::NotReady);
            }
            let risk_domain = account["risk_domain_id"].as_u64().ok_or(Error::Protocol)?;
            let pool = (
                identity.venue,
                identity.environment,
                identity.funds_identity.clone(),
            );
            if let Some(prior) = funds.insert(pool, risk_domain)
                && prior != risk_domain
            {
                return Err(Error::Conflict);
            }
            expires_unix_ms = expires_unix_ms
                .min(evidence.claim().expires_unix_ms)
                .min(observation_expires);
            accounts.insert(
                id.to_owned(),
                AccountFact {
                    identity_hash: Sha256::digest(identity_bytes).into(),
                    mode_hash: evidence.artifact_hash(),
                    observed_unix_ms: mode.observed_unix_ms,
                    funds_identity: identity.funds_identity,
                    evidence,
                },
            );
        }
        Ok(Self {
            candidate_hash: candidate.hash(),
            accounts,
            expires_unix_ms,
        })
    }
    pub fn candidate_hash(&self) -> Hash {
        self.candidate_hash
    }
    pub fn accounts(&self) -> &BTreeMap<String, AccountFact> {
        &self.accounts
    }
    pub fn expires_unix_ms(&self) -> u64 {
        self.expires_unix_ms
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AccountIdentity {
    schema: u32,
    venue: String,
    environment: String,
    product: String,
    exchange_account_id: String,
    funds_identity: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModeSnapshot {
    schema: u32,
    cluster_id: String,
    namespace_id: String,
    account_id: String,
    venue_id: String,
    environment: String,
    product: String,
    account_identity_ref: String,
    observed_exchange_account_id: String,
    observed_funds_identity: String,
    position_mode: String,
    margin_mode: String,
    asset_mode: String,
    trading_enabled: bool,
    observed_unix_ms: u64,
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let parsed = Candidate::parse(
        Format::Json,
        bytes,
        ParseLimits {
            max_bytes: 1024 * 1024,
            max_depth: 8,
        },
    )
    .map_err(Error::Configuration)?;
    serde_json::from_value(parsed.value().clone()).map_err(|_| Error::Protocol)
}
fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field].as_str().ok_or(Error::Protocol)
}
fn array<'a>(value: &'a Value, field: &str) -> Result<&'a [Value]> {
    value[field]
        .as_array()
        .map(Vec::as_slice)
        .ok_or(Error::Protocol)
}
fn opaque_identity(value: &str, maximum: usize) -> bool {
    (1..=maximum).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_./:".contains(&b))
}
