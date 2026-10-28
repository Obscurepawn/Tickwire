//! Real IPC/account codecs around strategy callbacks. The snapshot contains
//! materialized views, so replay starts at S rather than at the latest JOIN.
use super::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
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
        if maximum_age_ns == 0 || markets.is_empty() || markets.len() > MAX_STREAMS {
            return Err(Error::Capacity);
        }
        let mut out = Self {
            strategy,
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
        Ok(out)
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
    pub(super) fn seed_rebuild_account(&mut self, account: account::AccountReplica) -> Result<()> {
        let stream = account::AccountReplica::stream(account.baseline());
        if self.accounts.contains_key(&stream) || self.accounts.len() == MAX_STREAMS {
            return Err(Error::Identity);
        }
        self.accounts.insert(stream, account);
        Ok(())
    }
    pub(super) fn account_seal(&self, stream: &StreamKey) -> Result<(u64, Hash)> {
        let account = self.accounts.get(stream).ok_or(Error::Identity)?;
        Ok((account.position(), account.seal().digest()))
    }
    pub(crate) fn maximum_age(&self) -> u64 {
        self.maximum_age_ns
    }
    pub(crate) fn configure_views(
        &mut self,
        maximum_age: u64,
        maximum_levels: usize,
    ) -> Result<()> {
        if maximum_age == 0
            || self
                .markets
                .values()
                .any(|m| m.maximum_levels != maximum_levels)
        {
            return Err(Error::Identity);
        }
        self.maximum_age_ns = maximum_age;
        Ok(())
    }
    pub(crate) fn market_states(&self) -> Result<BTreeMap<StreamKey, (u64, Vec<u8>)>> {
        self.markets
            .iter()
            .map(|(key, market)| {
                Ok((
                    key.clone(),
                    (
                        market.replica.sequence(),
                        market.replica.checkpoint().map_err(|_| Error::Identity)?,
                    ),
                ))
            })
            .collect()
    }
    pub(crate) fn account_states(&self) -> Result<BTreeMap<StreamKey, (u64, Vec<u8>)>> {
        self.accounts
            .iter()
            .map(|(key, account)| Ok((key.clone(), (account.position(), account.checkpoint()?))))
            .collect()
    }
    pub(crate) fn prepare_replayed(
        &self,
        release: &tickwire_release::MemberRelease<'_>,
        target: crate::restore::Target,
        proof: &tickwire_release::replay::source::VerifiedSourceReplay,
        source: Source,
        config: VersionedBytes,
    ) -> Result<install::Candidate<S>> {
        self.strategy
            .prepare_replayed(release, target, proof, source, config)
    }
    pub fn business_state(&self) -> Result<Vec<u8>> {
        self.strategy.checkpoint().map_err(|_| Error::Callback)
    }
    pub fn restore(bytes: &[u8], restore: impl FnOnce(&[u8]) -> Result<S>) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > MAX_BYTES {
            return Err(Error::Capacity);
        }
        let saved: Saved = serde_json::from_slice(bytes).map_err(|_| Error::Identity)?;
        if saved.schema != 1
            || canonical(&saved)? != bytes
            || saved.accounts.len() > MAX_STREAMS
            || saved.markets.len() > MAX_STREAMS
        {
            return Err(Error::Identity);
        }
        let strategy = restore(&saved.strategy)?;
        if strategy.checkpoint().map_err(|_| Error::Callback)? != saved.strategy {
            return Err(Error::StateMismatch);
        }
        let mut markets = Vec::new();
        for m in saved.markets {
            let resource = ResourceHeader::decode(&m.resource).map_err(|_| Error::Identity)?;
            let replica = crate::MarketReplica::restore_checkpoint(&m.view, m.maximum_levels)
                .map_err(|_| Error::Identity)?;
            markets.push((resource, replica, m.maximum_levels));
        }
        let mut out = Self::new(strategy, saved.maximum_age_ns, markets)?;
        for a in saved.accounts {
            let replica = account::AccountReplica::restore(&a.view)?;
            if a.stream != account::AccountReplica::stream(replica.baseline())
                || out.accounts.insert(a.stream, replica).is_some()
            {
                return Err(Error::Identity);
            }
        }
        if out.checkpoint()? != bytes {
            return Err(Error::Identity);
        }
        Ok(out)
    }
    /// Replace only the isolated business state after an approved migration.
    /// All materialized markets, coverage, account history and resource bindings
    /// are restored from C through the ordinary strict checkpoint decoder.
    pub(crate) fn with_migrated_strategy(bytes: &[u8], strategy: S) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > MAX_BYTES {
            return Err(Error::Capacity);
        }
        let mut saved: Saved = serde_json::from_slice(bytes).map_err(|_| Error::Identity)?;
        if saved.schema != 1 || canonical(&saved)? != bytes {
            return Err(Error::Identity);
        }
        saved.strategy = strategy.checkpoint().map_err(|_| Error::Callback)?;
        Self::restore(&canonical(&saved)?, |_| Ok(strategy))
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
        let output = match input {
            Input::MarketTransition { .. } => {
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
                let eligible = view.live()
                    && context
                        .now_ns()
                        .checked_sub(view.received_mono_ns())
                        .is_some_and(|age| age <= *maximum_age_ns);
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

/// Independently materialize the approved rebuild window and compare every
/// resource/view byte at its cut. A matching sequence alone is insufficient.
pub(super) fn verify_rebuild_views(
    bytes: &[u8],
    input: &tickwire_release::rebuild::Inputs,
) -> Result<()> {
    if bytes.len() > MAX_BYTES {
        return Err(Error::Capacity);
    }
    let saved: Saved = serde_json::from_slice(bytes).map_err(|_| Error::Identity)?;
    if saved.schema != 1 || canonical(&saved)? != bytes {
        return Err(Error::Identity);
    }
    let mut markets = BTreeMap::new();
    for seed in &input.markets {
        let resource = ResourceHeader::decode(&seed.resource).map_err(|_| Error::Identity)?;
        let key = market_stream(&resource)?;
        let replica =
            crate::MarketReplica::restore_checkpoint(&seed.checkpoint, seed.maximum_levels)
                .map_err(|_| Error::Identity)?;
        if markets
            .insert(key, market(resource, replica, seed.maximum_levels)?)
            .is_some()
        {
            return Err(Error::Identity);
        }
    }
    let mut accounts = BTreeMap::new();
    for prefix in &input.accounts {
        let baseline = tickwire_account_codec::CanonicalBaseline::decode(&prefix.baseline)
            .map_err(|_| Error::Identity)?;
        let key = account::AccountReplica::stream(&baseline);
        let mut replica = account::AccountReplica::from_baseline(&key, &prefix.baseline)?;
        for raw in &prefix.records {
            let record = tickwire_account_codec::CanonicalRecord::decode(raw)
                .map_err(|_| Error::Identity)?;
            replica.apply(&key, record.sequence(), raw)?;
        }
        if accounts.insert(key, replica).is_some() {
            return Err(Error::Identity);
        }
    }
    for row in &input.warm {
        match &row.input {
            Input::MarketTransition { .. } => {
                transition::replace(&mut markets, &row.input, row.now_ns)?
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
                if header.sequence != *sequence || header.ingress_mono_ns != *received_ns {
                    return Err(Error::Identity);
                }
                markets
                    .get_mut(stream)
                    .ok_or(Error::Identity)?
                    .replica
                    .apply(record, payload)
                    .map_err(|_| Error::Callback)?;
            }
            Input::AccountCallback {
                stream,
                position,
                canonical,
            } => {
                accounts
                    .get_mut(stream)
                    .ok_or(Error::Identity)?
                    .apply(stream, *position, canonical)?;
            }
            Input::ConfigApply { .. } | Input::ModelApply { .. } => return Err(Error::Unsupported),
            _ => {}
        }
    }
    if markets.len() != saved.markets.len() || accounts.len() != saved.accounts.len() {
        return Err(Error::StateMismatch);
    }
    for (actual, expected) in markets.values().zip(&saved.markets) {
        if crate::bootstrap::wire_bytes(&actual.resource).map_err(|_| Error::Identity)?
            != expected.resource
            || actual.maximum_levels != expected.maximum_levels
            || actual.replica.checkpoint().map_err(|_| Error::Identity)? != expected.view
        {
            return Err(Error::StateMismatch);
        }
    }
    for ((stream, actual), expected) in accounts.iter().zip(&saved.accounts) {
        if *stream != expected.stream || actual.checkpoint()? != expected.view {
            return Err(Error::StateMismatch);
        }
    }
    Ok(())
}

pub trait BusinessFactory {
    type Running: crate::Strategy;
    fn schema(&self) -> u64;
    fn restore(&self, _state: &[u8], _runtime: &RuntimeState) -> Result<Self::Running> {
        Err(Error::Unsupported)
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
pub(super) mod tests;
#[cfg(test)]
pub(super) mod transition_tests;
