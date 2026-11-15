//! Stateless command identity, baseline scope, and hot-gate validation for `AccountGateway`.

use crate::{Config, Entry, Error, Gate, Result};
use tickwire_account::IntentKey;
use tickwire_account_codec::CanonicalBaseline;
use tickwire_venue::{CanonicalCommand, Product};

pub(crate) fn config(c: &Config) -> Result<()> {
    if c.writer_epoch == 0
        || c.writer_epoch >= 1 << 63
        || c.max_intents == 0
        || c.max_intents > tickwire_account::MAX_ORDERS
        || c.route.authority.trim().is_empty()
        || c.route.venue != c.scope.venue
        || c.route.account != c.scope.account
        || c.route.environment != c.scope.environment
        || !matches!(
            c.route.product,
            Product::Spot
                | Product::LinearPerpetual
                | Product::LinearFuture
                | Product::InversePerpetual
                | Product::InverseFuture
                | Product::Option
        )
    {
        return Err(Error::Identity);
    }
    Ok(())
}

pub(crate) fn baseline(c: &Config, b: &CanonicalBaseline) -> Result<()> {
    if b.digest() != c.baseline_digest
        || b.snapshot().scope != c.scope
        || b.catalog()
            .entries
            .iter()
            .any(|p| p.product != c.route.product)
    {
        return Err(Error::Identity);
    }
    Ok(())
}

#[inline]
pub(crate) fn intent(command: &CanonicalCommand) -> IntentKey {
    let r = &command.command().context.request;
    IntentKey {
        epoch: r.intent_exec_epoch,
        intent_id: r.intent_id,
    }
}

pub(crate) fn command(c: &Config, command: &CanonicalCommand, fresh: bool) -> Result<()> {
    let r = &command.command().context.request;
    if r.risk_domain_id != c.scope.risk_domain_id
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

#[inline]
pub(crate) fn permits(gate: Gate, entry: &Entry, writer: u64) -> bool {
    let r = &entry.command.command().context.request;
    gate.action_permitted
        && gate.now >= entry.evaluated_at
        && gate.now < r.deadline
        && gate.writer_epoch == writer
        && gate.writer_epoch == r.intent_exec_epoch
        && gate.strategy_generation == r.strategy_generation
        && gate.config_revision == r.config_revision
        && (!entry.increases || gate.market_valid)
}
