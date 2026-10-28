//! Approved account and market inputs, separate from trading authority.
//!
//! Startup-only mappings bind one global account name to the real execution
//! baseline/report owner, and one topic to the complete approved external JOIN.
//! `allowed_symbols`, risk budgets and catalog artifact economic content are not
//! consumed here. Current inputs are accepted only from Host's fresh checkpoint.
//! NormalizedMarket header venue/product fields are not product credentials;
//! this profile requires every market member in the actual account catalog.
use super::{profile, startup};
use crate::{Result, invalid};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc};
use tickwire_account::{Account, Scope};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_ipc::{ResourceHeader, Wire};
use tickwire_market_source::protocol::wire_bytes;
use tickwire_market_source::protocol::{IdentityConfig, Instrument};
use tickwire_release::Object;
use tickwire_strategy_host::{FreshObservation, ObservationRequest};
use tickwire_strategy_sdk::bootstrap::Bootstrap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Environment {
    Production,
    Test,
}
impl Environment {
    fn actual(self) -> tickwire_venue::Environment {
        match self {
            Self::Production => tickwire_venue::Environment::Production,
            Self::Test => tickwire_venue::Environment::Test,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeAccountScope {
    pub risk_domain_id: [u8; 16],
    pub venue: u32,
    pub environment: Environment,
    pub account: u64,
}
impl RuntimeAccountScope {
    fn actual(&self) -> Scope {
        Scope {
            risk_domain_id: self.risk_domain_id,
            venue: self.venue,
            environment: self.environment.actual(),
            account: self.account,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountBinding {
    pub account_id: String,
    pub venue_id: String,
    pub global_risk_domain_id: u64,
    pub baseline: Object,
    pub scope: RuntimeAccountScope,
    pub report_owner_process_id: String,
    pub report_owner: IdentityConfig,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketBinding {
    pub topic_id: u64,
    pub publisher_process_id: String,
    pub producer: IdentityConfig,
    pub bank: Object,
    pub catalog_hash: [u8; 32],
    pub instrument_set_version: u64,
    pub instruments: Vec<Instrument>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub object_file: PathBuf,
    pub object: profile::Binding,
    pub scope: profile::Scope,
    pub account: AccountBinding,
    pub market: MarketBinding,
    pub timeout_ns: u64,
}
/// Historical input consumption at the same real checkpoint as the query.
pub struct InputsProof {
    object: profile::Binding,
    scope: profile::Scope,
    observation: Arc<FreshObservation>,
}
impl InputsProof {
    pub fn object(&self) -> &profile::Binding {
        &self.object
    }
    pub fn scope(&self) -> &profile::Scope {
        &self.scope
    }
    pub fn observation(&self) -> &FreshObservation {
        &self.observation
    }
    pub fn inputs(&self) -> &tickwire_strategy_host::ObservedInputs {
        self.observation
            .inputs()
            .expect("InputsProof requires actual current inputs")
    }
}
#[derive(Clone)]
pub(super) struct Guard {
    baseline: Object,
    scope: Scope,
    account_epoch: u64,
    instance: ([u8; 16], u64),
}
impl Guard {
    pub(super) fn matches(
        &self,
        baseline: Option<&CanonicalBaseline>,
        account: &Account,
        instance: Option<([u8; 16], u64)>,
    ) -> bool {
        baseline.is_none_or(|b| {
            self.baseline.digest == b.digest()
                && self.baseline.bytes == b.bytes().len() as u64
                && b.snapshot().scope == self.scope
        }) && account.scope() == &self.scope
            && account.account_epoch() == self.account_epoch
            && instance == Some(self.instance)
    }
}
pub(super) struct Prepared {
    pub(super) binding: startup::Binding,
    guard: Guard,
    account: AccountBinding,
    market: MarketBinding,
    report: Object,
    report_subscription: tickwire_shm::SubscriptionKey,
}
impl Prepared {
    pub(super) fn guard(&self) -> Guard {
        self.guard.clone()
    }
    pub(super) fn observe(
        self,
        request: &ObservationRequest,
        observed: Arc<FreshObservation>,
    ) -> Result<InputsProof> {
        self.binding.verify(request, &observed)?;
        let inputs = observed
            .inputs()
            .ok_or_else(|| invalid("projection inputs missing current views"))?;
        check(
            inputs.accounts().len() == 1 && inputs.markets().len() == 1,
            "actual input set",
        )?;
        let account = &inputs.accounts()[0];
        let market = &inputs.markets()[0];
        let join = market
            .join()
            .ok_or_else(|| invalid("projection inputs missing actual JOIN"))?;
        let report = inputs.report();
        let ready = observed.ready();
        let identity = &self.binding.identity;
        check(
            inputs.source().instance == identity.instance
                && inputs.source().generation == identity.generation
                && inputs.source().logical_strategy == identity.logical_strategy
                && inputs.source().binary_digest == identity.build_hash,
            "actual source identity",
        )?;
        check(
            account.baseline().digest() == self.account.baseline.digest
                && account.baseline().bytes().len() as u64 == self.account.baseline.bytes
                && account.baseline().snapshot().scope == self.guard.scope
                && account.baseline().snapshot().account_epoch == self.guard.account_epoch
                && account.position() == ready.exec_report_sequence
                && account.cursor().sequence == account.position()
                && account.input_digest() == observed.account_input_digest(),
            "actual account consumption",
        )?;
        check(
            Object::of(&wire_bytes(report.header())?) == self.report
                && report.subscription() == self.report_subscription
                && report.identity().risk_domain == self.guard.scope.risk_domain_id
                && report.identity().instance_id == identity.instance
                && report.identity().strategy_generation == identity.generation
                && report.identity().logical_strategy_id == identity.logical_strategy
                && report.sequence() == ready.report_ring_sequence,
            "actual report lane",
        )?;
        let members = members(&self.market.instruments)?;
        let mut joined = join.instruments().to_vec();
        joined.sort_unstable();
        let mut current = market.instruments().to_vec();
        current.sort_unstable();
        check(
            Object::of(&wire_bytes(market.bank())?) == self.market.bank
                && join.catalog_hash() == self.market.catalog_hash
                && join.instrument_set_version() == self.market.instrument_set_version
                && joined == members
                && current
                    .iter()
                    .copied()
                    .eq(members.iter().map(|&(id, version, _)| (id, version))),
            "actual market input set",
        )?;
        if let Some(configuration) = observed.configuration() {
            check(
                configuration.checkpoint() == inputs.checkpoint()
                    && configuration.cut() == inputs.cut()
                    && configuration.source() == inputs.source(),
                "current checkpoint binding",
            )?;
        }
        Ok(InputsProof {
            object: self.binding.object,
            scope: self.binding.scope,
            observation: observed,
        })
    }
}
pub(super) fn prepare(
    config: &super::HostConfig,
    bootstrap: &Bootstrap,
    runtime: &tickwire_runtime::files::Configuration,
) -> Result<Option<Prepared>> {
    let Some(input) = &config.projection_inputs else {
        return Ok(None);
    };
    let binding = startup::Binding::new(
        config,
        bootstrap,
        &input.object,
        &input.scope,
        input.timeout_ns,
    )?;
    let external = bootstrap
        .external_market
        .as_ref()
        .ok_or_else(|| invalid("projection inputs external market required"))?;
    let object = profile::read_projection(
        config,
        bootstrap,
        &input.object_file,
        &input.object,
        &input.scope,
    )?;
    let payload = object.payload();
    let account = single(&payload["account_views"], "account view set")?;
    let venue = single(&payload["venue_views"], "venue view set")?;
    let topic = single(&payload["ipc"]["topics"], "topic set")?;
    let a = &input.account;
    let m = &input.market;
    owner_identity(&a.report_owner, 4)?;
    owner_identity(&m.producer, 2)?;
    check(
        single(&payload["strategy"]["account_ids"], "account ID set")?.as_str()
            == Some(a.account_id.as_str())
            && account["id"] == a.account_id
            && account["venue_id"] == a.venue_id
            && venue["id"] == a.venue_id
            && account["risk_domain_id"].as_u64() == Some(a.global_risk_domain_id)
            && account["exec_process_id"] == a.report_owner_process_id,
        "approved account mapping",
    )?;
    check(
        single(&payload["strategy"]["topic_ids"], "topic ID set")?.as_u64() == Some(m.topic_id)
            && topic["id"].as_u64() == Some(m.topic_id)
            && topic["publisher_process_id"] == m.publisher_process_id
            && topic["stream"] == "NormalizedMarket"
            && topic["qos"] == "StrictTrading",
        "approved topic mapping",
    )?;
    check(
        venue["venue"] == "Binance" && topic["product"] == venue["product"],
        "venue/topic product",
    )?;
    let scope = a.scope.actual();
    let baseline = &runtime.input.baseline;
    let route = &runtime.account.route;
    let command = &runtime.command_binding;
    check(
        a.baseline.digest == baseline.digest()
            && a.baseline.bytes == baseline.bytes().len() as u64
            && baseline.snapshot().scope == scope
            && runtime.account.journal.baseline_digest == baseline.digest()
            && runtime.account.journal.scope == scope
            && runtime.account.journal.physical.domain == scope.risk_domain_id
            && runtime.input.physical.domain == scope.risk_domain_id
            && command.risk_domain_id == scope.risk_domain_id
            && command.account == scope.account
            && command.environment == scope.environment
            && route.venue == scope.venue
            && route.account == scope.account
            && route.environment == scope.environment
            && route.product == command.product
            && bootstrap.risk_domain == scope.risk_domain_id
            && runtime.time.domain == binding.domain
            && command.domain == binding.domain
            && external.domain()? == binding.domain,
        "actual runtime account binding",
    )?;
    let product_matches = match venue["product"].as_str() {
        Some("Spot") => route.product == tickwire_venue::Product::Spot,
        Some("Usdm") => matches!(
            route.product,
            tickwire_venue::Product::LinearPerpetual | tickwire_venue::Product::LinearFuture
        ),
        _ => false,
    };
    check(
        product_matches
            && match venue["environment"].as_str() {
                Some("simulated") => scope.environment == tickwire_venue::Environment::Test,
                Some("production") => scope.environment == tickwire_venue::Environment::Production,
                _ => false,
            },
        "actual product/environment",
    )?;
    check(
        a.report_owner.identity() == bootstrap.report.peer.identity()
            && a.report_owner.identity() == bootstrap.intent.peer.identity()
            && a.report_owner.role == 4
            && a.report_owner.instance == command.owner_id
            && m.producer.identity() == external.endpoint.peer.identity()
            && m.producer.identity() == bootstrap.market.peer.identity()
            && m.producer.role == 2
            && external.endpoint.local.identity() == binding.identity,
        "actual endpoint owner",
    )?;
    let report = canonical_bank(&bootstrap.report_header)?;
    let intent = canonical_bank(&bootstrap.intent_header)?;
    check(
        report.resource_kind == 3
            && report.producer_instance == a.report_owner.instance
            && report.producer_generation == a.report_owner.generation
            && report.consumer_instance == binding.identity.instance
            && report.risk_scope == a.global_risk_domain_id
            && report.namespace_id == binding.identity.namespace
            && report.host_boot_id == binding.identity.host_boot
            && a.report_owner.namespace == binding.identity.namespace
            && a.report_owner.host_boot == binding.identity.host_boot
            && intent.resource_kind == 2
            && intent.producer_instance == binding.identity.instance
            && intent.producer_generation == binding.identity.generation
            && intent.consumer_instance == a.report_owner.instance
            && intent.risk_scope == a.global_risk_domain_id
            && intent.namespace_id == binding.identity.namespace
            && intent.host_boot_id == binding.identity.host_boot,
        "report/intent lane identity",
    )?;
    let bank = canonical_bank(&external.bank_header)?;
    check(
        m.bank == Object::of(&wire_bytes(&bank)?)
            && bank.topic_id == m.topic_id
            && bank.resource_kind == 1
            && bank.event_class == 7
            && bank.qos == 1
            && bank.producer_instance == m.producer.instance
            && bank.producer_generation == m.producer.generation
            && bank.namespace_id == binding.identity.namespace
            && bank.namespace_id == m.producer.namespace
            && bank.host_boot_id == m.producer.host_boot
            && m.producer.host_boot == binding.identity.host_boot
            && bank.clock_domain == 1
            && external.catalog_hash == m.catalog_hash
            && m.catalog_hash != [0; 32]
            && external.instrument_set_version == m.instrument_set_version
            && m.instrument_set_version != 0,
        "market bank/catalog binding",
    )?;
    let members = members(&m.instruments)?;
    check(
        members == self::members(&external.instruments)?,
        "complete market members",
    )?;
    // NormalizedMarket banks carry zero venue/product discriminators. Product
    // comes from the actual execution catalog; this profile cannot qualify an
    // external member whose product is absent from that approved baseline.
    for (id, version, _) in &members {
        let entry = baseline
            .catalog()
            .entries
            .iter()
            .find(|e| e.instrument.instrument_id == *id)
            .ok_or_else(|| {
                invalid("projection inputs market member absent from account catalog")
            })?;
        check(
            entry.product == route.product
                && entry.instrument.instrument_version == *version
                && entry.instrument.catalog_hash == m.catalog_hash
                && entry.instrument.catalog_version == bank.catalog_version,
            "member catalog/product/version",
        )?;
    }
    let report_subscription = tickwire_shm::SubscriptionKey {
        bank: tickwire_shm::BankKey {
            resource: u128::from_le_bytes(report.resource_id),
            channel: u128::from_le_bytes(report.channel_id),
            channel_epoch: report.channel_epoch,
            ring_epoch: report.ring_epoch,
            feed_generation: report.feed_generation,
        },
        instance: u128::from_le_bytes(binding.identity.instance),
        generation: bootstrap.report_subscription_generation,
        cursor: u128::from_le_bytes(bootstrap.report_cursor_id),
    };
    Ok(Some(Prepared {
        guard: Guard {
            baseline: a.baseline,
            scope,
            account_epoch: baseline.snapshot().account_epoch,
            instance: (binding.identity.instance, binding.identity.generation),
        },
        binding,
        account: a.clone(),
        market: m.clone(),
        report: Object::of(&wire_bytes(&report)?),
        report_subscription,
    }))
}
fn owner_identity(owner: &IdentityConfig, role: u16) -> Result<()> {
    check(
        owner.role == role
            && owner.instance != [0; 16]
            && (1..1u64 << 63).contains(&owner.generation)
            && owner.logical_strategy < 1u64 << 63
            && owner.host_boot != [0; 16]
            && owner.namespace != [0; 16]
            && owner.build_hash != [0; 32]
            && owner.qualification_hash != [0; 32]
            && owner.manifest_hash != [0; 32]
            && (1..=2).contains(&owner.arch)
            && owner.trust_profile == 1,
        "qualified owner identity",
    )
}
fn check(valid: bool, reason: &str) -> Result<()> {
    if valid {
        Ok(())
    } else {
        Err(invalid(&format!("projection inputs {reason}")))
    }
}
fn single<'a>(value: &'a serde_json::Value, reason: &str) -> Result<&'a serde_json::Value> {
    let values = value
        .as_array()
        .ok_or_else(|| invalid(&format!("projection inputs {reason}")))?;
    check(values.len() == 1, reason)?;
    Ok(&values[0])
}
fn members(values: &[Instrument]) -> Result<Vec<(u64, u64, u8)>> {
    check(!values.is_empty() && values.len() <= 4096, "member bounds")?;
    let mut result = Vec::with_capacity(values.len());
    for i in values {
        check(
            i.id != 0 && i.version != 0 && i.required_states != 0 && i.required_states & !15 == 0,
            "member identity/states",
        )?;
        result.push((i.id, i.version, i.required_states));
    }
    result.sort_unstable();
    check(
        result.windows(2).all(|w| w[0].0 < w[1].0),
        "duplicate market member",
    )?;
    Ok(result)
}
fn canonical_bank(raw: &[u8]) -> Result<ResourceHeader> {
    let mut h = ResourceHeader::decode(raw)?;
    tickwire_shm::validate_bank(&h)?;
    let original = h.descriptor_hash;
    h.descriptor_hash = [0; 32];
    let expected = tickwire_recovery::digest(&wire_bytes(&h)?);
    check(
        original == [0; 32] || original == expected,
        "bank descriptor digest",
    )?;
    h.descriptor_hash = expected;
    Ok(h)
}

#[cfg(test)]
mod tests;
