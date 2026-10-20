use crate::*;
use serde::Serialize;
use tickwire_venue as v;

#[derive(Serialize)]
struct Bootstrap<'a> {
    format: &'static str,
    version: u32,
    account_codec_version: u32,
    physical: &'a tickwire_journal::Config,
    baseline_digest: Hash,
    initial_writer_epoch: u64,
    risk_domain_id: [u8; 16],
    venue: u32,
    account: u64,
    environment: u8,
}
pub(crate) fn bootstrap(c: &Config) -> Result<Vec<u8>> {
    serde_json::to_vec(&Bootstrap {
        format: "tickwire.account-journal",
        version: 1,
        account_codec_version: 1,
        physical: &c.physical,
        baseline_digest: c.baseline_digest,
        initial_writer_epoch: c.initial_writer_epoch,
        risk_domain_id: c.scope.risk_domain_id,
        venue: c.scope.venue,
        account: c.scope.account,
        environment: match c.scope.environment {
            v::Environment::Production => 1,
            v::Environment::Test => 2,
        },
    })
    .map_err(|_| Error::Identity)
}
pub(crate) fn epoch(epoch: u64) -> Result<()> {
    if epoch == 0 || epoch >= 1 << 63 {
        Err(Error::Epoch)
    } else {
        Ok(())
    }
}
pub(crate) fn config(c: &Config, baseline: &CanonicalBaseline) -> Result<()> {
    epoch(c.initial_writer_epoch)?;
    if c.physical.domain != c.scope.risk_domain_id
        || c.physical.domain == [0; 16]
        || c.physical.stream_epoch == [0; 16]
        || !(528..=tickwire_journal::SEGMENT_MAX_BYTES).contains(&c.physical.segment_max_bytes)
        || !c.physical.segment_max_bytes.is_multiple_of(8)
        || c.baseline_digest != baseline.digest()
        || c.scope != baseline.snapshot().scope
    {
        return Err(Error::Identity);
    }
    Ok(())
}
fn scope(
    c: &Config,
    domain: [u8; 16],
    venue: u32,
    account: u64,
    environment: v::Environment,
) -> Result<()> {
    if c.scope.risk_domain_id != domain
        || c.scope.venue != venue
        || c.scope.account != account
        || c.scope.environment != environment
    {
        Err(Error::Identity)
    } else {
        Ok(())
    }
}
fn original_epoch(original: u64, writer: u64) -> Result<()> {
    epoch(original)?;
    if original > writer {
        Err(Error::Epoch)
    } else {
        Ok(())
    }
}
pub(crate) fn record(
    c: &Config,
    baseline: &CanonicalBaseline,
    writer: u64,
    record: &RecoveryRecord,
) -> Result<()> {
    match record {
        RecoveryRecord::Admit {
            command, expected, ..
        } => {
            let ctx = &command.command().context;
            let r = &ctx.request;
            scope(c, r.risk_domain_id, r.venue, r.account, r.environment)?;
            if r.intent_exec_epoch != writer {
                return Err(Error::Epoch);
            }
            if expected.account_epoch != baseline.snapshot().account_epoch
                || expected.revisions != baseline.snapshot().revisions
                || !baseline
                    .catalog()
                    .entries
                    .iter()
                    .any(|p| p.instrument == ctx.instrument && p.product == r.product)
            {
                return Err(Error::Identity);
            }
        }
        RecoveryRecord::VenueEvent(event) => {
            let event = event.event();
            let id = &event.identity;
            scope(c, id.risk_domain_id, id.venue, id.account, id.environment)?;
            if !baseline.catalog().entries.iter().any(|p| {
                p.instrument.instrument_id == id.instrument_id
                    && p.instrument.instrument_version == id.instrument_version
                    && p.product == id.product
            }) {
                return Err(Error::Identity);
            }
            let order = match &event.body {
                v::VenueEventBody::Order(o) => Some(&o.order),
                v::VenueEventBody::Fill(f) => Some(&f.observation.order),
                v::VenueEventBody::Fee { order, .. } => Some(order),
                v::VenueEventBody::Rejected { .. } => None,
            };
            if let Some(order) = order {
                original_epoch(order.original_intent_exec_epoch, writer)?;
            }
        }
        RecoveryRecord::Aborted { intent, .. } => original_epoch(intent.epoch, writer)?,
        RecoveryRecord::FeesReconciled { order, .. } => original_epoch(order.intent.epoch, writer)?,
        // These records have no external account/epoch fields: the fixed canonical
        // envelope binds them to this baseline. Reducer replay checks asset/instrument units.
        RecoveryRecord::UnattributedCashFlow { .. }
        | RecoveryRecord::ExternalPosition { .. }
        | RecoveryRecord::TransferAsset { .. }
        | RecoveryRecord::TransferPosition { .. } => {}
    }
    Ok(())
}
