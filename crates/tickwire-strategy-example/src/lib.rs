//! Independent statically registered strategy example. Two actual executables
//! carry different state schemas; neither receives a trading grant by registering.
#![deny(unsafe_code)]
use serde::{Deserialize, Serialize};
use tickwire_release::replay::{Rng, RuntimeState, VersionedBytes};
use tickwire_strategy_sdk::{
    self as sdk, Strategy, registered,
    restore::{self, RestoreFactory},
};
use tickwire_venue::CanonicalCommand;
pub const V1: u64 = 0x5457434e0001;
pub const V2: u64 = 0x5457434e0002;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Parameters {
    pub schema: u16,
    pub enabled: bool,
    pub minimum_observations: u64,
    pub minimum_bid_ticks: i64,
    pub command: Option<Vec<u8>>,
}
impl Parameters {
    pub fn bytes(&self) -> restore::Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|_| restore::Error::Input)
    }
    pub fn decode(bytes: &[u8]) -> restore::Result<Self> {
        let p: Self = serde_json::from_slice(bytes).map_err(|_| restore::Error::Input)?;
        if p.schema != 1
            || p.minimum_observations == 0
            || p.minimum_bid_ticks < 0
            || p.bytes()? != bytes
            || (p.enabled && p.command.is_none())
        {
            return Err(restore::Error::Input);
        }
        if let Some(raw) = &p.command {
            let c = CanonicalCommand::decode(raw).map_err(|_| restore::Error::Input)?;
            if c.command().context.request.product != tickwire_venue::Product::Spot
                || !matches!(&c.command().body,tickwire_venue::CommandBody::New(o) if o.order_type==tickwire_venue::OrderType::Limit)
            {
                return Err(restore::Error::Unsupported);
            }
        }
        Ok(p)
    }
    fn action(&self) -> restore::Result<[u8; 32]> {
        self.command
            .as_ref()
            .map(|b| {
                CanonicalCommand::decode(b)
                    .map(|c| {
                        tickwire_recovery::digest(&c.command().context.strategy.economic_intent_id)
                    })
                    .map_err(|_| restore::Error::Input)
            })
            .transpose()
            .map(|v| v.unwrap_or([0; 32]))
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    schema: u64,
    observations: u64,
    reports: u64,
    spent: bool,
    action: [u8; 32],
}
impl State {
    fn bytes(&self) -> restore::Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|_| restore::Error::State)
    }
    fn decode(bytes: &[u8], schema: u64) -> restore::Result<Self> {
        let state: Self = serde_json::from_slice(bytes).map_err(|_| restore::Error::State)?;
        if ![V1, V2].contains(&schema) || state.schema != schema || state.bytes()? != bytes {
            return Err(restore::Error::State);
        }
        Ok(state)
    }
}
#[derive(Clone)]
pub struct Factory {
    schema: u64,
    configuration: VersionedBytes,
    parameters: Parameters,
}
pub struct Counting {
    state: State,
    configuration: VersionedBytes,
    parameters: Parameters,
}
impl Factory {
    fn from_config(schema: u64, configuration: VersionedBytes) -> restore::Result<Self> {
        if ![V1, V2].contains(&schema) || configuration.revision == 0 {
            return Err(restore::Error::Unsupported);
        }
        let parameters = Parameters::decode(&configuration.bytes)?;
        Ok(Self {
            schema,
            configuration,
            parameters,
        })
    }
}
impl RestoreFactory for Factory {
    type Running = Counting;
    fn state_schema(&self) -> u64 {
        self.schema
    }
    fn configuration(&self, _: &restore::Target) -> restore::Result<VersionedBytes> {
        Ok(self.configuration.clone())
    }
    fn validate_checkpoint(&self, bytes: &[u8]) -> restore::Result<()> {
        State::decode(bytes, self.schema).map(|_| ())
    }
}
impl registered::UserFactory for Factory {
    fn create(&self) -> restore::Result<Counting> {
        Ok(Counting {
            state: State {
                schema: self.schema,
                observations: 0,
                reports: 0,
                spent: false,
                action: self.parameters.action()?,
            },
            configuration: self.configuration.clone(),
            parameters: self.parameters.clone(),
        })
    }
}
pub struct Registration {
    schema: u64,
}
impl Registration {
    pub fn v1() -> Self {
        Self { schema: V1 }
    }
    pub fn v2() -> Self {
        Self { schema: V2 }
    }
}
impl registered::Registration for Registration {
    type Factory = Factory;
    type Source = SourceFactory;
    fn id(&self) -> &'static str {
        "counting_example"
    }
    fn configure(&self, target: &restore::Target, c: &VersionedBytes) -> restore::Result<Factory> {
        let factory = Factory::from_config(self.schema, c.clone())?;
        if let Some(raw) = &factory.parameters.command {
            let command = CanonicalCommand::decode(raw).map_err(|_| restore::Error::Input)?;
            if command.command().context.request.config_revision != c.revision
                || command.command().context.strategy.instance_id != target.instance
                || command.command().context.request.strategy_generation != target.generation
                || command.command().context.strategy.logical_strategy_id != target.logical_strategy
            {
                return Err(restore::Error::Identity);
            }
        }
        Ok(factory)
    }
    fn source(&self, schema: u64) -> sdk::replay::Result<SourceFactory> {
        if schema != self.schema {
            return Err(sdk::replay::Error::Unsupported);
        }
        Ok(SourceFactory { schema })
    }
}
pub struct SourceFactory {
    schema: u64,
}
impl sdk::replay::application::BusinessFactory for SourceFactory {
    type Running = Counting;
    fn schema(&self) -> u64 {
        self.schema
    }
    fn restore(&self, bytes: &[u8], runtime: &RuntimeState) -> sdk::replay::Result<Counting> {
        let state =
            State::decode(bytes, self.schema).map_err(|_| sdk::replay::Error::StateMismatch)?;
        let parameters =
            Parameters::decode(&runtime.config.bytes).map_err(|_| sdk::replay::Error::Identity)?;
        if parameters
            .action()
            .map_err(|_| sdk::replay::Error::Identity)?
            != state.action
        {
            return Err(sdk::replay::Error::Identity);
        }
        Ok(Counting {
            state,
            configuration: runtime.config.clone(),
            parameters,
        })
    }
}
impl Strategy for Counting {
    fn replay_registration(&self) -> sdk::Result<Option<sdk::replay::native::Registration>> {
        Ok(Some(sdk::replay::native::Registration {
            schema: self.state.schema,
            config: self.configuration.clone(),
            models: vec![],
            rng: Rng::Disabled,
        }))
    }
    fn checkpoint(&self) -> sdk::Result<Vec<u8>> {
        Ok(self.state.bytes()?)
    }
    fn on_market(
        &mut self,
        view: sdk::MarketView<'_>,
        now: u64,
    ) -> sdk::Result<Option<CanonicalCommand>> {
        if !view.callback_ready_at(now, u64::MAX) {
            return Ok(None);
        }
        self.state.observations = self
            .state
            .observations
            .checked_add(1)
            .ok_or(sdk::Error::Bounds)?;
        if self.state.spent
            || !self.parameters.enabled
            || self.state.observations < self.parameters.minimum_observations
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
            || now >= template.command().context.request.deadline
        {
            return Ok(None);
        }
        let mut c = template.command().clone();
        c.context.strategy.created_mono_ns = now;
        c.context.triggers = tickwire_venue::BoundedList::new(vec![view.trigger_at(now)])?;
        let c = CanonicalCommand::new(c)?;
        self.state.spent = true;
        Ok(Some(c))
    }
    fn on_report(
        &mut self,
        _: &sdk::reports::AccountReport,
        _: u64,
    ) -> sdk::Result<Option<CanonicalCommand>> {
        self.state.reports = self
            .state
            .reports
            .checked_add(1)
            .ok_or(sdk::Error::Bounds)?;
        Ok(None)
    }
    fn apply_config(
        &mut self,
        bytes: &[u8],
        context: &mut sdk::replay::ReplayContext<'_>,
    ) -> sdk::Result<()> {
        let parameters = Parameters::decode(bytes)?;
        if parameters.action()? != self.state.action || context.config().bytes != bytes {
            return Err(sdk::Error::Identity);
        }
        self.parameters = parameters;
        self.configuration = context.config().clone();
        Ok(())
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
