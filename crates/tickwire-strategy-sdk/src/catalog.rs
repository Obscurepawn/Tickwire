//! Stateless economic instrument catalog verification (`catalog::from_json_bytes`,
//! `validate_startup`, `validate_market`, `validate_join`, `validate_command`).
#![forbid(unsafe_code)]

use std::sync::Arc;

use sha2::{Digest, Sha256};
pub use tickwire_instrument_catalog::{Artifact, Catalog, Market};
use tickwire_ipc::{ResourceHeader, Wire};

use crate::{
    Error, Result,
    bootstrap::{Bootstrap, JoinBootstrap, MAX_PARAM_BYTES},
};

/// Decode and verify an immutable `Catalog` JSON payload (`<= 2 MiB`), optionally checking an
/// expected artifact SHA-256 digest before canonical economic decoding.
pub fn from_json_bytes(raw: &[u8], expected_sha256: Option<[u8; 32]>) -> Result<Catalog> {
    if raw.len() > tickwire_instrument_catalog::MAX_BYTES {
        return Err(Error::Bounds);
    }
    let actual: [u8; 32] = Sha256::digest(raw).into();
    if let Some(expected) = expected_sha256
        && expected != actual
    {
        return Err(Error::Identity);
    }
    Ok(Catalog::decode(raw, actual)?)
}

/// Resolve an optional startup [`Catalog`] from either `config.instrument_catalog` or
/// `config.catalog_file` (with optional `config.catalog_digest`), rejecting ambiguous combinations.
pub fn resolve_bootstrap(config: &Bootstrap) -> Result<Option<Arc<Catalog>>> {
    if config.instrument_catalog.is_some() && config.catalog_file.is_some() {
        return Err(Error::Identity);
    }
    if config.catalog_digest.is_some() && config.catalog_file.is_none() {
        return Err(Error::Identity);
    }
    if let Some(artifact) = &config.instrument_catalog {
        return Ok(Some(Arc::new(artifact.catalog()?)));
    }
    if let Some(path) = &config.catalog_file {
        if !path.is_absolute() {
            return Err(Error::Identity);
        }
        let raw = crate::worker::read_trusted_bytes(path, MAX_PARAM_BYTES)?;
        if let Some(digest) = &config.catalog_digest {
            digest.verify(&raw, MAX_PARAM_BYTES)?;
        }
        let catalog = from_json_bytes(&raw, config.catalog_digest.map(|d| d.sha256))?;
        return Ok(Some(Arc::new(catalog)));
    }
    Ok(None)
}

/// Validate the optional startup [`Catalog`] against the worker's subscribed `tickwire-md` topic
/// (`catalog_hash`, `catalog_version`, and per-instrument `(instrument_id, instrument_version)`).
pub fn validate_startup(config: &Bootstrap) -> Result<Option<Arc<Catalog>>> {
    let Some(catalog) = resolve_bootstrap(config)? else {
        return Ok(None);
    };
    if config.market.catalog_hash != catalog.catalog_hash() {
        return Err(Error::Identity);
    }
    let bank = ResourceHeader::decode(&config.market.bank_header).map_err(|_| Error::Identity)?;
    validate_market(
        &catalog,
        &bank,
        config.market.instruments.iter().map(|i| (i.id, i.version)),
    )?;
    Ok(Some(catalog))
}

/// Verify that a market ring `ResourceHeader` and subscribed `(instrument_id, instrument_version)`
/// list match `catalog`.
pub fn validate_market(
    catalog: &Catalog,
    bank: &ResourceHeader,
    instruments: impl IntoIterator<Item = (u64, u64)>,
) -> Result<()> {
    if bank.catalog_version != catalog.catalog_version() {
        return Err(Error::Identity);
    }
    let mut count = 0usize;
    for (id, version) in instruments {
        if catalog
            .instrument(id)
            .is_none_or(|i| i.binding().instrument_version != version)
        {
            return Err(Error::Identity);
        }
        count += 1;
    }
    if count == 0 {
        return Err(Error::Identity);
    }
    Ok(())
}

/// Verify a `JoinBootstrap` descriptor against `catalog`.
pub fn validate_join(catalog: &Catalog, join: &JoinBootstrap) -> Result<()> {
    let (bank, _, _, registry) = join.decode().map_err(|_| Error::Identity)?;
    if registry.catalog_hash != catalog.catalog_hash() {
        return Err(Error::Identity);
    }
    validate_market(
        catalog,
        &bank,
        registry
            .instruments
            .iter()
            .map(|i| (i.instrument_id, i.instrument_version)),
    )
}

/// Verify that a [`tickwire_venue::CanonicalCommand`] matches `catalog` venue, environment,
/// product, and `InstrumentBinding`.
pub fn validate_command(
    catalog: &Catalog,
    command: &tickwire_venue::CanonicalCommand,
) -> Result<()> {
    let context = &command.command().context;
    if context.request.venue != catalog.venue()
        || context.request.environment != catalog.environment()
        || !catalog.instrument_matches(context.request.product, &context.instrument)
    {
        return Err(Error::Identity);
    }
    Ok(())
}
