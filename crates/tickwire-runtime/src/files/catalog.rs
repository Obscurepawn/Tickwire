use super::Configuration;
use crate::node;
use tickwire_clock::Source;
use tickwire_instrument_catalog::{Catalog, Market};
use tickwire_venue::Environment;

pub(super) fn baseline_matches(
    catalog: &Catalog,
    baseline: &tickwire_account_codec::CanonicalBaseline,
) -> bool {
    let snapshot = baseline.snapshot();
    snapshot.scope.venue == catalog.venue()
        && snapshot.scope.environment == catalog.environment()
        && snapshot
            .assets
            .iter()
            .all(|a| catalog.asset_matches(&a.asset))
        && baseline
            .catalog()
            .entries
            .iter()
            .all(|i| catalog.instrument_matches(i.product, &i.instrument))
}

impl Configuration {
    /// Rechecks the retained, immutable directory against actual startup inputs.
    /// Called before creating directories, WAL owners, or network clients.
    pub fn validate_instrument_catalog(&self) -> node::Result<()> {
        let Some(catalog) = &self.instrument_catalog else {
            return Ok(());
        };
        let baseline = &self.input.baseline;
        let scope = &baseline.snapshot().scope;
        let route = &self.account.route;
        let command = &self.command_binding;
        let private = &self.private.scope;
        let source = match (catalog.market(), catalog.environment()) {
            (Market::Spot, Environment::Production) => Source::SpotProduction,
            (Market::Spot, Environment::Test) => Source::SpotTest,
            (Market::Usdm, Environment::Production) => Source::UsdmProduction,
            (Market::Usdm, Environment::Test) => Source::UsdmTest,
        };
        let private_market = match catalog.market() {
            Market::Spot => tickwire_binance::request::Market::Spot,
            Market::Usdm => tickwire_binance::request::Market::Usdm,
        };
        if !baseline_matches(catalog, baseline)
            || self.account.journal.scope != *scope
            || self.account.journal.baseline_digest != baseline.digest()
            || route.venue != scope.venue
            || route.environment != scope.environment
            || route.account != scope.account
            || command.environment != scope.environment
            || command.account != scope.account
            || command.risk_domain_id != scope.risk_domain_id
            || command.product != route.product
            || self.market.source != source
            || self.time.source != source
            || self.market.depth.key.venue != scope.venue
            || !catalog.instrument_matches(route.product, &self.market.depth.instrument)
            || private.venue != scope.venue
            || private.environment != scope.environment
            || private.account != scope.account
            || private.risk_domain_id != scope.risk_domain_id
            || private.market != private_market
        {
            return Err(node::Error::Config);
        }
        Ok(())
    }
}
