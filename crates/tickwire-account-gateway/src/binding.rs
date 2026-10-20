use crate::*;
use tickwire_venue::Product;

pub(crate) fn config(c: &Config) -> Result<()> {
    let scope = &c.journal.scope;
    if c.writer_epoch == 0
        || c.writer_epoch >= 1 << 63
        || c.writer_epoch < c.journal.initial_writer_epoch
        || c.max_intents == 0
        || c.max_intents > tickwire_account::MAX_ORDERS
        || c.route.venue != scope.venue
        || c.route.account != scope.account
        || c.route.environment != scope.environment
        || c.journal.physical.domain != scope.risk_domain_id
        || !matches!(
            c.route.product,
            Product::Spot | Product::LinearPerpetual | Product::LinearFuture
        )
    {
        return Err(Error::Identity);
    }
    Ok(())
}
pub(crate) fn baseline(c: &Config, b: &CanonicalBaseline) -> Result<()> {
    if b.digest() != c.journal.baseline_digest
        || b.snapshot().scope != c.journal.scope
        || b.catalog()
            .entries
            .iter()
            .any(|p| p.product != c.route.product)
    {
        return Err(Error::Identity);
    }
    Ok(())
}
pub(crate) fn intent(command: &CanonicalCommand) -> IntentKey {
    let r = &command.command().context.request;
    IntentKey {
        epoch: r.intent_exec_epoch,
        intent_id: r.intent_id,
    }
}
pub(crate) fn command(c: &Config, command: &CanonicalCommand, fresh: bool) -> Result<()> {
    let r = &command.command().context.request;
    if r.risk_domain_id != c.journal.scope.risk_domain_id
        || r.venue != c.route.venue
        || r.account != c.route.account
        || r.environment != c.route.environment
        || r.product != c.route.product
        || r.intent_exec_epoch > c.writer_epoch
        || (fresh && r.intent_exec_epoch != c.writer_epoch)
    {
        return Err(Error::Identity);
    }
    Ok(())
}
pub(crate) fn request<'a>(
    baseline: &'a CanonicalBaseline,
    record: &'a CanonicalRecord,
    preceding: &'a HistoryFrontier,
) -> Result<AdmissionRequest<'a>> {
    let RecoveryRecord::Admit { command, .. } = record.record() else {
        return Err(Error::Invalid);
    };
    let r = &command.command().context.request;
    if record.baseline() != baseline.digest()
        || preceding.baseline_digest() != baseline.digest()
        || preceding.scope() != &baseline.snapshot().scope
        || preceding.record_count().checked_add(1) != Some(record.sequence())
    {
        return Err(Error::Identity);
    }
    Ok(AdmissionRequest {
        baseline,
        record,
        preceding,
        binding: ClosureBinding {
            risk_domain_id: r.risk_domain_id,
            intent_id: r.intent_id,
            intent_exec_epoch: r.intent_exec_epoch,
            business_digest: record.digest(),
            strategy_generation: r.strategy_generation,
            config_revision: r.config_revision,
            recovery_object: r.recovery_object,
        },
    })
}
pub(crate) fn closure(
    baseline: &CanonicalBaseline,
    record: &CanonicalRecord,
    preceding: &HistoryFrontier,
    capability: &VerifiedClosure,
) -> Result<()> {
    let expected = request(baseline, record, preceding)?;
    let RecoveryRecord::Admit { command, .. } = record.record() else {
        return Err(Error::Invalid);
    };
    if capability.binding() != &expected.binding
        || capability.command_digest() != Some(command.digest())
        || capability.account_baseline_digest() != Some(baseline.digest())
        || capability.account_history_digest() != Some(preceding.digest())
    {
        return Err(Error::Identity);
    }
    Ok(())
}
pub(crate) fn permits(gate: Gate, entry: &Entry, writer: u64) -> bool {
    let r = &entry.command.command().context.request;
    gate.action_permitted
        && gate.owner_isolated
        && gate.now >= entry.evaluated_at
        && gate.now < gate.lease_until
        && gate.now < r.deadline
        && gate.writer_epoch == writer
        && gate.writer_epoch == r.intent_exec_epoch
        && gate.strategy_generation == r.strategy_generation
        && gate.config_revision == r.config_revision
        && (!entry.increases || gate.market_valid)
}
