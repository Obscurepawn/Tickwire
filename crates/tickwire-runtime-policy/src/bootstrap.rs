//! Explicit deployment inputs. Loading configuration grants no private-stream
//! reconciliation status; all absolute deadlines use the declared clock domain.
use super::*;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Environment {
    Production,
    Test,
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Product {
    Spot,
    LinearPerpetual,
    LinearFuture,
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Domain {
    pub process_instance: [u8; 16],
    pub generation: u64,
}
impl Domain {
    pub fn build(self) -> Result<ClockDomain> {
        ClockDomain::new(self.process_instance, self.generation).map_err(|_| Error::Identity)
    }
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub risk_domain_id: [u8; 16],
    pub venue: u32,
    pub environment: Environment,
    pub account: u64,
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Lease {
    pub lease_epoch: u64,
    pub valid_until_ns: u64,
    pub isolation_record: Hash,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub scope: Scope,
    pub product: Product,
    pub executor_id: [u8; 16],
    pub owner_generation: u64,
    pub writer_epoch: u64,
    pub domain: Domain,
    pub versions: Versions,
    pub max_strategies: usize,
    pub max_feeds: usize,
    pub marks_per_feed: usize,
    pub control_revision: u64,
    pub mode: Mode,
    pub owner_lease: Option<Lease>,
    pub strategy_grants: Vec<StrategyGrant>,
    pub management_grant: Option<ManagementGrant>,
}
impl Config {
    pub fn domain(&self) -> Result<ClockDomain> {
        self.domain.build()
    }
    pub fn scope(&self) -> tickwire_account::Scope {
        tickwire_account::Scope {
            risk_domain_id: self.scope.risk_domain_id,
            venue: self.scope.venue,
            environment: match self.scope.environment {
                Environment::Production => tickwire_venue::Environment::Production,
                Environment::Test => tickwire_venue::Environment::Test,
            },
            account: self.scope.account,
        }
    }
    pub fn product(&self) -> tickwire_venue::Product {
        match self.product {
            Product::Spot => tickwire_venue::Product::Spot,
            Product::LinearPerpetual => tickwire_venue::Product::LinearPerpetual,
            Product::LinearFuture => tickwire_venue::Product::LinearFuture,
        }
    }
    pub fn build(&self) -> Result<ControlPlane> {
        if self.strategy_grants.len() > self.max_strategies {
            return Err(Error::Capacity);
        }
        let domain = self.domain()?;
        let plane = ControlPlane::new(super::Config {
            scope: self.scope(),
            product: self.product(),
            executor_id: self.executor_id,
            owner_generation: self.owner_generation,
            writer_epoch: self.writer_epoch,
            domain,
            versions: self.versions,
            max_strategies: self.max_strategies,
            max_feeds: self.max_feeds,
            marks_per_feed: self.marks_per_feed,
        })?;
        if let Some(lease) = self.owner_lease {
            plane.install_owner_lease(OwnerLease {
                executor_id: self.executor_id,
                owner_generation: self.owner_generation,
                writer_epoch: self.writer_epoch,
                lease_epoch: lease.lease_epoch,
                domain,
                valid_until_ns: lease.valid_until_ns,
                isolation_record: lease.isolation_record,
            })?;
        }
        for grant in &self.strategy_grants {
            plane.install_strategy_grant(grant.clone())?;
        }
        if let Some(grant) = &self.management_grant {
            plane.install_management_grant(grant.clone())?;
        }
        plane.set_mode(self.control_revision, self.mode)?;
        Ok(plane)
    }
    pub fn native_provider(
        &self,
        plane: &ControlPlane,
    ) -> Result<PolicyGate<tickwire_clock_sys::SystemClock>> {
        let clock =
            tickwire_clock_sys::SystemClock::new(self.domain()?).map_err(|_| Error::Clock)?;
        plane.provider(clock)
    }
}
