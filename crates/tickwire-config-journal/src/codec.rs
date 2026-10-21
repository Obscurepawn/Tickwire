use crate::{Config, Error, Event, Hash, Result, Scope};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const MAX_RECORD: usize = 16 * 1024;
pub(crate) const SEGMENT_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "record", deny_unknown_fields)]
pub(crate) enum Body {
    Header { scope: Scope },
    Event { event: Event },
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    schema: u32,
    scope_hash: Hash,
    body: Body,
}
pub(crate) fn hash(bytes: &[u8]) -> Hash {
    Sha256::digest(bytes).into()
}
pub(crate) fn scope_hash(scope: &Scope) -> Result<Hash> {
    let bytes = serde_json::to_vec(scope).map_err(|_| Error::Encoding)?;
    let mut h = Sha256::new();
    h.update(b"tickwire.config.local.scope.v1\0");
    h.update(bytes);
    Ok(h.finalize().into())
}
pub(crate) fn physical(config: &Config) -> Result<tickwire_journal::Config> {
    check_config(config)?;
    let hash = scope_hash(&config.scope)?;
    Ok(tickwire_journal::Config {
        stream_epoch: config.stream_epoch,
        domain: hash[..16].try_into().map_err(|_| Error::Encoding)?,
        segment_max_bytes: SEGMENT_BYTES,
    })
}
fn name(value: &str) -> bool {
    (1..=63).contains(&value.len())
        && value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && value
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
}
pub(crate) fn check_config(config: &Config) -> Result<()> {
    let scope = &config.scope;
    if !name(&scope.cluster)
        || scope.namespace == [0; 16]
        || scope.slot == [0; 16]
        || scope.locator_generation == 0
        || !name(&scope.host)
        || !name(&scope.process)
        || config.stream_epoch == [0; 16]
        || config.writer_epoch == 0
        || !(1..=4096).contains(&config.max_operations)
        || !(512..=64 * 1024 * 1024).contains(&config.max_recorded_bytes)
    {
        return Err(Error::Invalid("scope or journal limits"));
    }
    Ok(())
}
pub(crate) fn encode(scope_hash: Hash, body: Body) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(&Envelope {
        schema: 1,
        scope_hash,
        body,
    })
    .map_err(|_| Error::Encoding)?;
    if bytes.len() > MAX_RECORD {
        return Err(Error::Capacity);
    }
    Ok(bytes)
}
pub(crate) fn decode(scope_hash: Hash, bytes: &[u8]) -> Result<Body> {
    if bytes.len() > MAX_RECORD {
        return Err(Error::Capacity);
    }
    let envelope: Envelope = serde_json::from_slice(bytes).map_err(|_| Error::Encoding)?;
    if envelope.schema != 1
        || envelope.scope_hash != scope_hash
        || encode(scope_hash, envelope.body.clone())? != bytes
    {
        return Err(Error::Invalid("noncanonical record or scope"));
    }
    Ok(envelope.body)
}
