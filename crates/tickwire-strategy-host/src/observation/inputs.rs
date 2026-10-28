//! Actual SDK-owned input views and the authenticated report consumption lane.
use super::*;
use tickwire_ipc::ResourceHeader;
use tickwire_release::{
    Object,
    replay::{Cursor, Cut, Source},
};
use tickwire_strategy_sdk::{replay::application::observed, reports::ReportIdentity};
mod join;
pub use observed::AccountInput as ObservedAccountInput;

#[derive(Debug)]
pub struct ObservedInputs {
    source: Source,
    cut: Cut,
    checkpoint: Object,
    accounts: Vec<ObservedAccountInput>,
    markets: Vec<ObservedMarketInput>,
    report: ObservedReportInput,
}
impl ObservedInputs {
    pub fn source(&self) -> &Source {
        &self.source
    }
    pub fn cut(&self) -> &Cut {
        &self.cut
    }
    pub fn checkpoint(&self) -> Object {
        self.checkpoint
    }
    pub fn accounts(&self) -> &[ObservedAccountInput] {
        &self.accounts
    }
    pub fn markets(&self) -> &[ObservedMarketInput] {
        &self.markets
    }
    pub fn report(&self) -> &ObservedReportInput {
        &self.report
    }
}
#[derive(Debug)]
pub struct ObservedMarketInput {
    view: observed::MarketInput,
    join: Option<ObservedJoin>,
}
impl ObservedMarketInput {
    pub fn bank(&self) -> &ResourceHeader {
        self.view.bank()
    }
    pub fn cursor(&self) -> &Cursor {
        self.view.cursor()
    }
    pub fn instruments(&self) -> &[(u64, u64)] {
        self.view.instruments()
    }
    pub fn join(&self) -> Option<&ObservedJoin> {
        self.join.as_ref()
    }
}
#[derive(Debug)]
pub struct ObservedJoin {
    catalog_hash: [u8; 32],
    instrument_set_version: u64,
    instruments: Vec<(u64, u64, u8)>,
    snapshot: Object,
    baseline_sequence: u64,
}
impl ObservedJoin {
    pub fn catalog_hash(&self) -> [u8; 32] {
        self.catalog_hash
    }
    pub fn instrument_set_version(&self) -> u64 {
        self.instrument_set_version
    }
    pub fn instruments(&self) -> &[(u64, u64, u8)] {
        &self.instruments
    }
    pub fn snapshot(&self) -> Object {
        self.snapshot
    }
    pub fn baseline_sequence(&self) -> u64 {
        self.baseline_sequence
    }
}
#[derive(Debug)]
pub struct ObservedReportInput {
    header: ResourceHeader,
    identity: ReportIdentity,
    subscription: tickwire_shm::SubscriptionKey,
    sequence: u64,
}
impl ObservedReportInput {
    pub fn header(&self) -> &ResourceHeader {
        &self.header
    }
    pub fn identity(&self) -> &ReportIdentity {
        &self.identity
    }
    pub fn subscription(&self) -> tickwire_shm::SubscriptionKey {
        self.subscription
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}
pub(crate) fn extract(
    current: &configuration::Current,
    objects: &[Vec<u8>],
    ready: &HandoverReadyBody,
    report: ObservedReportInput,
) -> Result<Option<ObservedInputs>> {
    let checkpoint = &current.checkpoint;
    let views = observed::inspect(checkpoint).map_err(|_| Error::Invalid)?;
    // An otherwise valid saved application may have no account yet. This host
    // owns one report lane; it cannot attest mappings for a multi-account view.
    if views.accounts().len() != 1 {
        return Ok(None);
    }
    let seal = &current.seal;
    let raw = configuration::find(
        objects,
        Object {
            digest: seal.checkpoint_digest,
            bytes: seal.checkpoint_bytes,
        },
    )?;
    let inputs = configuration::find(
        objects,
        Object {
            digest: seal.input_digest,
            bytes: seal.input_bytes,
        },
    )?;
    let account = configuration::find(
        objects,
        Object {
            digest: seal.account_input_digest,
            bytes: seal.account_input_bytes,
        },
    )?;
    let verified =
        tickwire_handover::verify(seal, raw, inputs, account).map_err(|_| Error::Invalid)?;
    let view = &views.accounts()[0];
    let source = &checkpoint.source;
    let bank = &report.header;
    let sub = report.subscription;
    if view.baseline() != verified.baseline()
        || view.position() != seal.report_position
        || view.position() != ready.exec_report_sequence
        || view.input_digest() != seal.account_input_digest
        || report.identity.logical_strategy_id != source.logical_strategy
        || report.identity.instance_id != source.instance
        || report.identity.strategy_generation != source.generation
        || report.identity.risk_domain != view.baseline().snapshot().scope.risk_domain_id
        || bank.resource_kind != 3
        || bank.record_bytes != 512
        || bank.required_features & tickwire_strategy_sdk::reports::FEATURE == 0
        || bank.consumer_instance != source.instance
        || bank.risk_scope == 0
        || bank.first_seq.checked_add(view.position()) != Some(report.sequence)
        || report.sequence != ready.report_ring_sequence
        || sub.instance.to_le_bytes() != source.instance
        || sub.bank.resource.to_le_bytes() != bank.resource_id
        || sub.bank.channel.to_le_bytes() != bank.channel_id
        || sub.bank.channel_epoch != bank.channel_epoch
        || sub.bank.ring_epoch != bank.ring_epoch
        || sub.bank.feed_generation != bank.feed_generation
        || sub.cursor == 0
        || sub.generation == 0
    {
        return Err(Error::Invalid);
    }
    let original = seal
        .dependencies
        .iter()
        .find(|(kind, _)| *kind == 11)
        .ok_or(Error::Invalid)?
        .1;
    let original = objects
        .iter()
        .find(|bytes| Object::of(bytes).digest == original)
        .ok_or(Error::Invalid)?;
    let (accounts, markets) = views.into_parts();
    let markets = markets
        .into_iter()
        .map(|view| {
            let join = join::extract(original, &view, checkpoint.runtime.now_ns)?;
            Ok(ObservedMarketInput { view, join })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(ObservedInputs {
        source: source.clone(),
        cut: checkpoint.cut.clone(),
        checkpoint: Object::of(&checkpoint.bytes()?),
        accounts,
        markets,
        report,
    }))
}

pub(crate) fn report(
    reports: &Reports,
    subscription: tickwire_shm::SubscriptionKey,
    sequence: u64,
) -> ObservedReportInput {
    ObservedReportInput {
        header: reports.header().clone(),
        identity: reports.identity().clone(),
        subscription,
        sequence,
    }
}

#[cfg(test)]
mod tests;
