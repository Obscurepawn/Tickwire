//! Real IPC/account codecs around strategy callbacks. The snapshot contains
//! materialized views, so replay starts at S rather than at the latest JOIN.
use super::*;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use tickwire_instrument_catalog::Catalog;
use tickwire_ipc::{RecordHeader, ResourceHeader, Wire};
pub mod observed;
mod transition;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedMarket {
    resource: Vec<u8>,
    maximum_levels: usize,
    view: Vec<u8>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedAccount {
    stream: StreamKey,
    view: Vec<u8>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Saved {
    schema: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    instrument_catalog: Option<Object>,
    strategy: Vec<u8>,
    maximum_age_ns: u64,
    markets: Vec<SavedMarket>,
    accounts: Vec<SavedAccount>,
}
struct Market {
    resource: ResourceHeader,
    maximum_levels: usize,
    replica: crate::MarketReplica,
}
struct Views<'a> {
    markets: &'a BTreeMap<StreamKey, Market>,
    accounts: &'a BTreeMap<StreamKey, account::AccountReplica>,
}
impl ReplayViews for Views<'_> {
    fn market(&self, stream: &StreamKey, instrument: u64) -> Option<crate::MarketView<'_>> {
        self.markets.get(stream)?.replica.view(instrument)
    }
    fn account(&self, stream: &StreamKey) -> Option<&tickwire_account::Account> {
        self.accounts.get(stream).map(|a| a.account())
    }
}
pub struct Application<S> {
    strategy: S,
    catalog: Option<Arc<Catalog>>,
    maximum_age_ns: u64,
    markets: BTreeMap<StreamKey, Market>,
    accounts: BTreeMap<StreamKey, account::AccountReplica>,
}
pub fn market_stream(resource: &ResourceHeader) -> Result<StreamKey> {
    let bytes = crate::bootstrap::wire_bytes(resource).map_err(|_| Error::Identity)?;
    Ok(StreamKey {
        identity: tickwire_recovery::digest(&bytes),
        generation: resource.feed_generation,
    })
}
fn require_members(
    baseline: &tickwire_account_codec::CanonicalBaseline,
    replica: &crate::MarketReplica,
) -> Result<()> {
    if replica.instruments().any(|id| {
        !baseline
            .catalog()
            .entries
            .iter()
            .any(|entry| entry.instrument.instrument_id == id)
    }) {
        return Err(Error::Identity);
    }
    Ok(())
}
fn market(
    resource: ResourceHeader,
    replica: crate::MarketReplica,
    maximum_levels: usize,
) -> Result<Market> {
    if maximum_levels == 0 || maximum_levels > 10000 {
        return Err(Error::Capacity);
    }
    let mut found = false;
    for id in replica.instruments() {
        let mark = replica.view(id).ok_or(Error::Identity)?.trigger();
        if mark.topic_id != resource.topic_id
            || mark.feed_generation != resource.feed_generation
            || mark.ring_epoch != resource.ring_epoch
        {
            return Err(Error::Identity);
        }
        found = true;
    }
    if !found {
        return Err(Error::Identity);
    }
    Ok(Market {
        resource,
        replica,
        maximum_levels,
    })
}
impl<S: crate::Strategy> Application<S> {
    pub(super) fn transition_state(&self, input: &Input, now: u64) -> Result<Vec<u8>> {
        let Input::MarketTransition { previous, next, .. } = input else {
            return Err(Error::Identity);
        };
        if self.markets.contains_key(next) {
            return Err(Error::Identity);
        }
        transition::replacement(
            self.markets.get(&previous.stream).ok_or(Error::Identity)?,
            input,
            now,
        )?
        .replica
        .checkpoint()
        .map_err(|_| Error::Identity)
    }
    pub fn new(
        strategy: S,
        maximum_age_ns: u64,
        markets: Vec<(ResourceHeader, crate::MarketReplica, usize)>,
    ) -> Result<Self> {
        Self::new_with_catalog(strategy, maximum_age_ns, markets, None)
    }
    pub fn new_with_catalog(
        strategy: S,
        maximum_age_ns: u64,
        markets: Vec<(ResourceHeader, crate::MarketReplica, usize)>,
        catalog: Option<Arc<Catalog>>,
    ) -> Result<Self> {
        if maximum_age_ns == 0 || markets.is_empty() || markets.len() > MAX_STREAMS {
            return Err(Error::Capacity);
        }
        let mut out = Self {
            strategy,
            catalog,
            maximum_age_ns,
            markets: BTreeMap::new(),
            accounts: BTreeMap::new(),
        };
        for (resource, replica, maximum_levels) in markets {
            if out
                .markets
                .insert(
                    market_stream(&resource)?,
                    market(resource, replica, maximum_levels)?,
                )
                .is_some()
            {
                return Err(Error::Identity);
            }
        }
        out.validate_catalog()?;
        Ok(out)
    }
    pub(crate) fn callbacks_ready(&self) -> bool {
        self.catalog.is_none() || !self.accounts.is_empty()
    }
    pub(crate) fn catalog(&self) -> Option<&Arc<Catalog>> {
        self.catalog.as_ref()
    }
    fn validate_catalog(&self) -> Result<()> {
        let Some(catalog) = &self.catalog else {
            return Ok(());
        };
        for market in self.markets.values() {
            crate::catalog::validate_market(
                catalog,
                &market.resource,
                market.replica.instruments().map(|id| {
                    (
                        id,
                        market
                            .replica
                            .view(id)
                            .unwrap()
                            .trigger()
                            .instrument_version,
                    )
                }),
            )?;
        }
        for account in self.accounts.values() {
            crate::catalog::validate_baseline(catalog, account.baseline())?;
        }
        Ok(())
    }
    pub fn market_cursors(&self) -> Result<Vec<Cursor>> {
        self.markets
            .iter()
            .map(|(stream, m)| {
                Ok(Cursor {
                    stream: stream.clone(),
                    sequence: m.replica.sequence(),
                    content_digest: tickwire_recovery::digest(
                        &m.replica.checkpoint().map_err(|_| Error::Identity)?,
                    ),
                })
            })
            .collect()
    }
    pub fn market_view(
        &self,
        stream: &StreamKey,
        instrument: u64,
    ) -> Option<crate::MarketView<'_>> {
        self.markets.get(stream)?.replica.view(instrument)
    }
    pub fn account_view(&self, stream: &StreamKey) -> Option<&tickwire_account::Account> {
        self.accounts.get(stream).map(|a| a.account())
    }
    pub fn business_state(&self) -> Result<Vec<u8>> {
        self.strategy.checkpoint().map_err(|_| Error::Callback)
    }
    pub fn restore(bytes: &[u8], restore: impl FnOnce(&[u8]) -> Result<S>) -> Result<Self> {
        Self::restore_with_catalog(bytes, None, restore)
    }
    pub fn restore_with_catalog(
        bytes: &[u8],
        catalog: Option<Arc<Catalog>>,
        restore: impl FnOnce(&[u8]) -> Result<S>,
    ) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > MAX_BYTES {
            return Err(Error::Capacity);
        }
        let saved: Saved = serde_json::from_slice(bytes).map_err(|_| Error::Identity)?;
        if saved.schema != 1
            || canonical(&saved)? != bytes
            || saved.maximum_age_ns == 0
            || saved.markets.is_empty()
            || saved.accounts.len() > MAX_STREAMS
            || saved.markets.len() > MAX_STREAMS
            || saved.instrument_catalog != catalog.as_deref().map(crate::catalog::object)
        {
            return Err(Error::Identity);
        }
        // Validate all saved SDK-owned views before invoking any user restore code.
        let mut markets = BTreeMap::new();
        for m in saved.markets {
            let resource = ResourceHeader::decode(&m.resource).map_err(|_| Error::Identity)?;
            let replica = crate::MarketReplica::restore_checkpoint(&m.view, m.maximum_levels)
                .map_err(|_| Error::Identity)?;
            if let Some(catalog) = &catalog {
                crate::catalog::validate_market(
                    catalog,
                    &resource,
                    replica
                        .instruments()
                        .map(|id| (id, replica.view(id).unwrap().trigger().instrument_version)),
                )?;
            }
            let view = market(resource, replica, m.maximum_levels)?;
            if markets
                .insert(market_stream(&view.resource)?, view)
                .is_some()
            {
                return Err(Error::Identity);
            }
        }
        let mut accounts = BTreeMap::new();
        for a in saved.accounts {
            let replica = account::AccountReplica::restore(&a.view)?;
            if let Some(catalog) = &catalog {
                crate::catalog::validate_baseline(catalog, replica.baseline())?;
            }
            if a.stream != account::AccountReplica::stream(replica.baseline())
                || accounts.insert(a.stream, replica).is_some()
            {
                return Err(Error::Identity);
            }
        }
        if catalog.is_some() {
            for a in accounts.values() {
                for m in markets.values() {
                    require_members(a.baseline(), &m.replica)?;
                }
            }
        }
        let strategy = restore(&saved.strategy)?;
        if strategy.checkpoint().map_err(|_| Error::Callback)? != saved.strategy {
            return Err(Error::StateMismatch);
        }
        let out = Self {
            strategy,
            maximum_age_ns: saved.maximum_age_ns,
            markets,
            accounts,
            catalog,
        };
        if out.checkpoint()? != bytes {
            return Err(Error::Identity);
        }
        Ok(out)
    }
}
impl<S: crate::Strategy> ReplayableStrategy for Application<S> {
    fn checkpoint(&self) -> Result<Vec<u8>> {
        let markets = self
            .markets
            .values()
            .map(|m| {
                Ok(SavedMarket {
                    resource: crate::bootstrap::wire_bytes(&m.resource)
                        .map_err(|_| Error::Identity)?,
                    maximum_levels: m.maximum_levels,
                    view: m.replica.checkpoint().map_err(|_| Error::Identity)?,
                })
            })
            .collect::<Result<_>>()?;
        let accounts = self
            .accounts
            .iter()
            .map(|(stream, a)| {
                Ok(SavedAccount {
                    stream: stream.clone(),
                    view: a.checkpoint()?,
                })
            })
            .collect::<Result<_>>()?;
        Ok(canonical(&Saved {
            schema: 1,
            instrument_catalog: self.catalog.as_deref().map(crate::catalog::object),
            strategy: self.business_state()?,
            maximum_age_ns: self.maximum_age_ns,
            markets,
            accounts,
        })?)
    }
    fn apply(
        &mut self,
        input: &Input,
        context: &mut ReplayContext<'_>,
        effects: &mut ReplayEffects,
    ) -> Result<()> {
        if !self.callbacks_ready()
            && matches!(
                input,
                Input::TimerFired { .. }
                    | Input::ConfigApply { .. }
                    | Input::ModelApply { .. }
                    | Input::External { .. }
            )
        {
            return Err(Error::Unsupported);
        }
        let output = match input {
            Input::MarketTransition { baseline, .. } => {
                if let Some(catalog) = &self.catalog {
                    if baseline.len() < 28 {
                        return Err(Error::Identity);
                    }
                    let len = u32::from_le_bytes(baseline[24..28].try_into().unwrap()) as usize;
                    let join: crate::bootstrap::JoinBootstrap = serde_json::from_slice(
                        baseline
                            .get(28..28usize.checked_add(len).ok_or(Error::Capacity)?)
                            .ok_or(Error::Identity)?,
                    )
                    .map_err(|_| Error::Identity)?;
                    crate::catalog::validate_join(catalog, &join)?;
                }
                transition::replace(&mut self.markets, input, context.now_ns())?;
                None
            }
            Input::MarketApplied {
                stream,
                sequence,
                received_ns,
                record,
                payload,
            } => {
                let header = RecordHeader::decode(record.get(..128).ok_or(Error::Identity)?)
                    .map_err(|_| Error::Identity)?;
                // Cold construction/restore/transition bind the complete member set.
                // Replica::apply performs the one dense lookup and checks its version.
                if header.sequence != *sequence || header.ingress_mono_ns != *received_ns {
                    return Err(Error::Identity);
                }
                self.markets
                    .get_mut(stream)
                    .ok_or(Error::Identity)?
                    .replica
                    .apply(record, payload)
                    .map_err(|_| Error::Callback)?;
                None
            }
            Input::MarketCallback {
                stream,
                instrument,
                invoked,
                maximum_age_ns,
            } => {
                if *maximum_age_ns != self.maximum_age_ns {
                    return Err(Error::Identity);
                }
                let view = self
                    .markets
                    .get(stream)
                    .ok_or(Error::Identity)?
                    .replica
                    .view(*instrument)
                    .ok_or(Error::Identity)?;
                let eligible = self.callbacks_ready()
                    && view.callback_ready_at(context.now_ns(), *maximum_age_ns);
                if eligible != *invoked {
                    return Err(Error::Identity);
                }
                if eligible {
                    self.strategy
                        .on_market_recorded(
                            view,
                            &mut context.with_views(&Views {
                                markets: &self.markets,
                                accounts: &self.accounts,
                            }),
                        )
                        .map_err(|_| Error::Callback)?
                } else {
                    None
                }
            }
            Input::AccountCallback {
                stream,
                position,
                canonical,
            } => {
                let report = if *position == 0 {
                    if self.accounts.contains_key(stream) || self.accounts.len() == MAX_STREAMS {
                        return Err(Error::Sequence);
                    }
                    let replica = account::AccountReplica::from_baseline(stream, canonical)?;
                    if let Some(catalog) = &self.catalog {
                        crate::catalog::validate_baseline(catalog, replica.baseline())?;
                        for market in self.markets.values() {
                            require_members(replica.baseline(), &market.replica)?;
                        }
                    }
                    let report =
                        crate::reports::AccountReport::Baseline(replica.baseline().clone());
                    self.accounts.insert(stream.clone(), replica);
                    report
                } else {
                    self.accounts
                        .get_mut(stream)
                        .ok_or(Error::Identity)?
                        .apply(stream, *position, canonical)?
                };
                self.strategy
                    .on_report_recorded(
                        &report,
                        &mut context.with_views(&Views {
                            markets: &self.markets,
                            accounts: &self.accounts,
                        }),
                    )
                    .map_err(|_| Error::Callback)?
            }
            Input::TimerFired { id, generation, .. } => self
                .strategy
                .on_timer(
                    *id,
                    *generation,
                    &mut context.with_views(&Views {
                        markets: &self.markets,
                        accounts: &self.accounts,
                    }),
                )
                .map_err(|_| Error::Callback)?,
            Input::ConfigApply { next, .. } => {
                self.strategy
                    .apply_config(
                        &next.bytes,
                        &mut context.with_views(&Views {
                            markets: &self.markets,
                            accounts: &self.accounts,
                        }),
                    )
                    .map_err(|_| Error::Unsupported)?;
                None
            }
            Input::ModelApply { id, next, .. } => {
                self.strategy
                    .apply_model(
                        *id,
                        &next.bytes,
                        &mut context.with_views(&Views {
                            markets: &self.markets,
                            accounts: &self.accounts,
                        }),
                    )
                    .map_err(|_| Error::Unsupported)?;
                None
            }
            Input::External {
                stream,
                schema,
                bytes,
                ..
            } => self
                .strategy
                .on_external(
                    stream,
                    *schema,
                    bytes,
                    &mut context.with_views(&Views {
                        markets: &self.markets,
                        accounts: &self.accounts,
                    }),
                )
                .map_err(|_| Error::Callback)?,
            Input::Entropy { .. } => None,
        };
        if let Some(command) = output {
            if let Some(catalog) = &self.catalog {
                crate::catalog::validate_command(catalog, &command)?;
            }
            effects.emit(command)?;
        }
        Ok(())
    }
}

/// Decode the saved SDK views and extract only the exact strategy-owned bytes.
/// This is not restoration and cannot construct a running strategy.
pub fn business_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    if bytes.len() > MAX_BYTES {
        return Err(Error::Capacity);
    }
    let saved: Saved = serde_json::from_slice(bytes).map_err(|_| Error::Identity)?;
    if saved.schema != 1 || canonical(&saved)? != bytes {
        return Err(Error::Identity);
    }
    Ok(saved.strategy)
}

pub trait BusinessFactory {
    type Running: crate::Strategy;
    fn schema(&self) -> u64;
    fn restore(&self, _state: &[u8], _runtime: &RuntimeState) -> Result<Self::Running> {
        Err(Error::Unsupported)
    }
}
pub struct BoundApplicationFactory<F> {
    factory: ApplicationFactory<F>,
    catalog: Option<Arc<Catalog>>,
}
impl<F> ApplicationFactory<F> {
    pub fn with_catalog(self, catalog: Option<Arc<Catalog>>) -> BoundApplicationFactory<F> {
        BoundApplicationFactory {
            factory: self,
            catalog,
        }
    }
}
impl<F: BusinessFactory> SourceReplayFactory for BoundApplicationFactory<F> {
    type Running = Application<F::Running>;
    fn binary_digest(&self) -> Hash {
        self.factory.binary_digest
    }
    fn state_schema(&self) -> u64 {
        self.factory.business.schema()
    }
    fn restore_source(&self, source: &VerifiedAnchor) -> Result<Self::Running> {
        let c = source.checkpoint();
        Application::restore_with_catalog(&c.business, self.catalog.clone(), |state| {
            self.factory.business.restore(state, &c.runtime)
        })
    }
}
pub struct ApplicationFactory<F> {
    pub binary_digest: Hash,
    pub business: F,
}
impl<F: BusinessFactory> SourceReplayFactory for ApplicationFactory<F> {
    type Running = Application<F::Running>;
    fn binary_digest(&self) -> Hash {
        self.binary_digest
    }
    fn state_schema(&self) -> u64 {
        self.business.schema()
    }
    fn restore_source(&self, source: &VerifiedAnchor) -> Result<Self::Running> {
        let checkpoint = source.checkpoint();
        Application::restore(&checkpoint.business, |state| {
            self.business.restore(state, &checkpoint.runtime)
        })
    }
}
#[cfg(test)]
pub(crate) mod catalog_tests;
#[cfg(test)]
pub(super) mod tests;
#[cfg(test)]
pub(super) mod transition_tests;
