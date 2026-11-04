//! Economic context shared by live execution, saved views and source replay.
use crate::{
    bootstrap::{Bootstrap, JoinBootstrap, MAX_BOOTSTRAP_BYTES},
    replay::{Error, Result},
};
use std::sync::Arc;
pub use tickwire_instrument_catalog::Catalog;
use tickwire_ipc::Wire;
use tickwire_release::{Object, replay::Source};

pub fn object(catalog: &Catalog) -> Object {
    // Catalog construction verified this digest; its private raw body is immutable.
    Object {
        digest: catalog.artifact_hash(),
        bytes: catalog.raw_bytes().len() as u64,
    }
}
pub fn resolve_bootstrap(config: &Bootstrap) -> Result<Option<Arc<Catalog>>> {
    config
        .instrument_catalog
        .as_ref()
        .map(|a| a.catalog().map(Arc::new).map_err(|_| Error::Identity))
        .transpose()
}
pub fn reject_lifecycle(config: &Bootstrap) -> Result<()> {
    if config.instrument_catalog.is_some() {
        Err(Error::Unsupported)
    } else {
        Ok(())
    }
}
/// Older generic factories may name opaque deployment inputs. Only an enabled
/// typed catalog is new authority here; ordinary Bootstrap decoders remain strict.
pub(crate) fn reject_lifecycle_bytes(bytes: &[u8]) -> Result<()> {
    if serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|v| v.get("instrument_catalog").cloned())
        .is_some_and(|v| !v.is_null())
    {
        return Err(Error::Unsupported);
    }
    Ok(())
}
pub fn resolve_dependency9(
    bytes: &[u8],
    source: &Source,
    expected: Option<Object>,
) -> Result<Option<Arc<Catalog>>> {
    if bytes.len() > MAX_BOOTSTRAP_BYTES {
        return Err(Error::Capacity);
    }
    let config: Bootstrap = serde_json::from_slice(bytes).map_err(|_| Error::Identity)?;
    let catalog = resolve_bootstrap(&config)?;
    if catalog.as_deref().map(object) != expected {
        return Err(Error::Identity);
    }
    if catalog.is_some() {
        let id = &config.market.local;
        if id.instance != source.instance
            || id.generation != source.generation
            || id.logical_strategy != source.logical_strategy
            || id.build_hash != source.binary_digest
            || id.host_boot != source.clock.boot_id
            || config.clock_generation != source.clock.generation
        {
            return Err(Error::Identity);
        }
    }
    Ok(catalog)
}
/// Called before construction and again against the writer's copied kind9.
pub fn validate_dependency(config: &Bootstrap, bytes: &[u8]) -> Result<()> {
    if config.instrument_catalog.is_none() {
        return Ok(());
    }
    if bytes.len() > MAX_BOOTSTRAP_BYTES {
        return Err(Error::Capacity);
    }
    let saved: Bootstrap = serde_json::from_slice(bytes).map_err(|_| Error::Identity)?;
    if saved.instrument_catalog != config.instrument_catalog
        || saved.market.local.identity() != config.market.local.identity()
        || saved.clock_generation != config.clock_generation
    {
        return Err(Error::Identity);
    }
    resolve_bootstrap(&saved)?;
    Ok(())
}
pub fn validate_startup(config: &Bootstrap) -> crate::Result<()> {
    let Some(catalog) = resolve_bootstrap(config)? else {
        return Ok(());
    };
    if config.preflight_job.is_some() || config.factory.is_none() || !config.handover_enabled {
        return Err(Error::Unsupported.into());
    }
    let path = config.recovery_template.as_ref().ok_or(Error::Identity)?;
    let template: crate::recovery::Template = crate::worker::read_config(path, 2 * 1024 * 1024)?;
    let dependency = template
        .dependencies
        .iter()
        .find(|d| d.kind == 9)
        .ok_or(Error::Identity)?;
    let bytes = crate::recovery::read(&dependency.path, MAX_BOOTSTRAP_BYTES)?;
    if tickwire_recovery::digest(&bytes) != dependency.digest {
        return Err(Error::Identity.into());
    }
    validate_dependency(config, &bytes)?;
    let market = config.external_market.as_ref().ok_or(Error::Unsupported)?;
    if market.catalog_hash != catalog.catalog_hash() {
        return Err(Error::Identity.into());
    }
    let bank =
        tickwire_ipc::ResourceHeader::decode(&market.bank_header).map_err(|_| Error::Identity)?;
    validate_market(
        &catalog,
        &bank,
        market.instruments.iter().map(|i| (i.id, i.version)),
    )?;
    Ok(())
}
pub fn validate_baseline(
    catalog: &Catalog,
    baseline: &tickwire_account_codec::CanonicalBaseline,
) -> Result<()> {
    let snapshot = baseline.snapshot();
    if snapshot.scope.venue != catalog.venue()
        || snapshot.scope.environment != catalog.environment()
        || snapshot
            .assets
            .iter()
            .any(|a| !catalog.asset_matches(&a.asset))
        || baseline
            .catalog()
            .entries
            .iter()
            .any(|i| !catalog.instrument_matches(i.product, &i.instrument))
    {
        return Err(Error::Identity);
    }
    Ok(())
}
pub fn validate_market(
    catalog: &Catalog,
    bank: &tickwire_ipc::ResourceHeader,
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
pub fn validate_checkpoint(
    checkpoint: &tickwire_release::replay::Checkpoint,
    catalog: Option<&Catalog>,
) -> Result<()> {
    if tickwire_release::replay::source::catalog_binding(checkpoint)? != catalog.map(object) {
        return Err(Error::Identity);
    }
    let views = crate::replay::application::observed::inspect(checkpoint)?;
    if let Some(catalog) = catalog {
        for account in views.accounts() {
            validate_baseline(catalog, account.baseline())?;
        }
        for market in views.markets() {
            validate_market(catalog, market.bank(), market.instruments().iter().copied())?;
        }
    }
    Ok(())
}
