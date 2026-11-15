//! Statically registered stateless reference strategy (`Counting`) and `V1`/`V2` `Registration`
//! schemas consuming `tickwire-md` SHM broadcast rings and exchanging inline 1,024-byte
//! `IntentRecord` and 512-byte `ReportRecord` slots over SHM (`0` disk persistence).
#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tickwire_strategy_sdk::{
    self as sdk, ExecutionReport, ExecutionReportKind, FillKind, OutboundIntent, Strategy,
    registered::{self, TargetIdentity, VersionedBytes},
};
use tickwire_venue::{CanonicalCommand, CommandBody, OrderType, Product};

pub const V1: u64 = 0x5457_434e_0001;
pub const V2: u64 = 0x5457_434e_0002;

/// Immutable startup parameter document bound to `Counting` via `FactoryBinding` SHA-256 verification.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Parameters {
    pub schema: u16,
    pub enabled: bool,
    pub minimum_observations: u64,
    pub minimum_bid_ticks: i64,
    pub command: Option<Vec<u8>>,
}

impl Parameters {
    pub fn bytes(&self) -> sdk::Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|_| sdk::Error::Input)
    }

    pub fn decode(bytes: &[u8]) -> sdk::Result<Self> {
        let p: Self = serde_json::from_slice(bytes).map_err(|_| sdk::Error::Input)?;
        if p.schema != 1
            || p.minimum_observations == 0
            || p.minimum_bid_ticks < 0
            || p.bytes()? != bytes
            || (p.enabled && p.command.is_none())
        {
            return Err(sdk::Error::Input);
        }
        if let Some(raw) = &p.command {
            let c = CanonicalCommand::decode(raw).map_err(|_| sdk::Error::Input)?;
            if c.command().context.request.product != Product::Spot
                || !matches!(
                    &c.command().body,
                    CommandBody::New(o) if o.order_type == OrderType::Limit
                )
            {
                return Err(sdk::Error::Unsupported);
            }
        }
        Ok(p)
    }

    pub fn action_digest(&self) -> sdk::Result<[u8; 32]> {
        self.command
            .as_ref()
            .map(|raw| {
                CanonicalCommand::decode(raw)
                    .map(|c| Sha256::digest(c.command().context.strategy.economic_intent_id).into())
                    .map_err(|_| sdk::Error::Input)
            })
            .transpose()
            .map(|v| v.unwrap_or([0; 32]))
    }
}

/// Validated `Counting` factory constructed during cold startup before SHM or DOP connections open.
#[derive(Clone, Debug)]
pub struct Factory {
    schema: u64,
    target: TargetIdentity,
    configuration: VersionedBytes,
    parameters: Parameters,
}

impl Factory {
    fn from_config(
        schema: u64,
        target: TargetIdentity,
        configuration: VersionedBytes,
    ) -> sdk::Result<Self> {
        if ![V1, V2].contains(&schema) || configuration.revision == 0 {
            return Err(sdk::Error::Unsupported);
        }
        target.validate()?;
        let parameters = Parameters::decode(&configuration.bytes)?;
        Ok(Self {
            schema,
            target,
            configuration,
            parameters,
        })
    }
}

impl registered::UserFactory for Factory {
    type Running = Counting;

    fn state_schema(&self) -> u64 {
        self.schema
    }

    fn configuration(&self, target: &TargetIdentity) -> sdk::Result<VersionedBytes> {
        if target != &self.target {
            return Err(sdk::Error::Identity);
        }
        Ok(self.configuration.clone())
    }

    fn create(&self) -> sdk::Result<Counting> {
        Ok(Counting {
            schema: self.schema,
            observations: 0,
            reports: 0,
            fills: 0,
            spent: false,
            action: self.parameters.action_digest()?,
            strat_id: self.target.strat_id,
            account_ref: self.target.account_ref,
            parameters: self.parameters.clone(),
        })
    }
}

/// Static registration binding `"counting_example"` to either `V1` (`tickwire-counting-v1-worker`)
/// or `V2` (`tickwire-counting-worker`).
#[derive(Clone, Copy, Debug)]
pub struct Registration {
    schema: u64,
}

impl Registration {
    pub const fn v1() -> Self {
        Self { schema: V1 }
    }

    pub const fn v2() -> Self {
        Self { schema: V2 }
    }
}

impl registered::Registration for Registration {
    type Factory = Factory;

    fn id(&self) -> &'static str {
        "counting_example"
    }

    fn configure(&self, target: &TargetIdentity, c: &VersionedBytes) -> sdk::Result<Self::Factory> {
        let factory = Factory::from_config(self.schema, *target, c.clone())?;
        if let Some(raw) = &factory.parameters.command {
            let command = CanonicalCommand::decode(raw).map_err(|_| sdk::Error::Input)?;
            let ctx = &command.command().context;
            if ctx.request.config_revision != c.revision
                || ctx.strategy.instance_id != target.instance_id
                || ctx.request.strategy_generation != target.generation
                || ctx.strategy.logical_strategy_id != target.logical_strategy_id
            {
                return Err(sdk::Error::Identity);
            }
        }
        Ok(factory)
    }
}

/// Stateless in-memory counting strategy emitting at most one inline `IntentRecord` when armed.
#[derive(Clone, Debug)]
pub struct Counting {
    schema: u64,
    observations: u64,
    reports: u64,
    fills: u64,
    spent: bool,
    action: [u8; 32],
    strat_id: u8,
    account_ref: u8,
    parameters: Parameters,
}

impl Counting {
    #[inline]
    pub const fn schema(&self) -> u64 {
        self.schema
    }

    #[inline]
    pub const fn observations(&self) -> u64 {
        self.observations
    }

    #[inline]
    pub const fn reports(&self) -> u64 {
        self.reports
    }

    #[inline]
    pub const fn fills(&self) -> u64 {
        self.fills
    }

    #[inline]
    pub const fn spent(&self) -> bool {
        self.spent
    }

    #[inline]
    pub const fn action_digest(&self) -> [u8; 32] {
        self.action
    }
}

impl Strategy for Counting {
    fn on_market(
        &mut self,
        view: sdk::MarketView<'_>,
        now_ns: u64,
    ) -> sdk::Result<Option<OutboundIntent>> {
        if !view.callback_ready_at(now_ns, u64::MAX) {
            return Ok(None);
        }
        self.observations = self.observations.checked_add(1).ok_or(sdk::Error::Bounds)?;
        if self.spent
            || !self.parameters.enabled
            || self.observations < self.parameters.minimum_observations
        {
            return Ok(None);
        }
        let raw = self
            .parameters
            .command
            .as_ref()
            .ok_or(sdk::Error::Identity)?;
        let template = CanonicalCommand::decode(raw)?;
        if template.command().context.instrument.instrument_id != view.instrument()
            || view
                .best_bid()
                .is_none_or(|p| p.price_ticks < self.parameters.minimum_bid_ticks)
            || now_ns >= template.command().context.request.deadline
        {
            return Ok(None);
        }
        let mut c = template.command().clone();
        c.context.strategy.created_mono_ns = now_ns;
        c.context.triggers = tickwire_venue::BoundedList::new(vec![view.trigger_at(now_ns)])?;
        let canonical = CanonicalCommand::new(c)?;
        let intent = OutboundIntent::from_canonical_command(
            &canonical,
            self.strat_id,
            self.account_ref,
            view.tick_mono_ns(),
        )?;
        self.spent = true;
        Ok(Some(intent))
    }

    fn on_report(
        &mut self,
        report: &ExecutionReport,
        _now_ns: u64,
    ) -> sdk::Result<Option<OutboundIntent>> {
        self.reports = self.reports.checked_add(1).ok_or(sdk::Error::Bounds)?;
        if matches!(
            &report.kind,
            ExecutionReportKind::Fill(f) if f.kind == FillKind::Execution
        ) {
            self.fills = self.fills.checked_add(1).ok_or(sdk::Error::Bounds)?;
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_configuration_rejects_unknown_fields_and_whitespace() {
        let p = Parameters {
            schema: 1,
            enabled: false,
            minimum_observations: 2,
            minimum_bid_ticks: 0,
            command: None,
        };
        let bytes = p.bytes().unwrap();
        assert!(Parameters::decode(&bytes).is_ok());
        let mut extra = bytes.clone();
        extra.push(b' ');
        assert!(Parameters::decode(&extra).is_err());
        assert!(Parameters::decode(br#"{"schema":1,"enabled":false,"minimum_observations":2,"minimum_bid_ticks":0,"command":null,"extra":1}"#).is_err());
    }
}
