//! Explicit old counting-state reconstruction from the approved account prefix.
use super::*;
use sdk::replay::{self, SourceReplayFactory, rebuild as api};
use tickwire_release::{Hash, Role};

struct Rebuilder {
    factory: Factory,
    binary: Hash,
}
impl Rebuilder {
    fn initial(&self, actions: &[api::KnownAction]) -> replay::Result<Counting> {
        let spent = if let Some(raw) = &self.factory.parameters.command {
            let command = CanonicalCommand::decode(raw).map_err(|_| replay::Error::Identity)?;
            let action = command.command().context.strategy.economic_intent_id;
            // This example has exactly one economic action. Missing acceptance
            // evidence cannot initialize that historical action as unspent.
            if actions.len() != 1
                || actions[0].economic_intent != action
                || actions[0]
                    .command
                    .command()
                    .context
                    .strategy
                    .economic_intent_id
                    != action
            {
                return Err(replay::Error::Identity);
            }
            true
        } else {
            if !actions.is_empty() {
                return Err(replay::Error::Unsupported);
            }
            false
        };
        Ok(Counting {
            state: State {
                schema: self.factory.schema,
                observations: 0,
                reports: 0,
                spent,
                action: self
                    .factory
                    .parameters
                    .action()
                    .map_err(|_| replay::Error::Identity)?,
            },
            configuration: self.factory.configuration.clone(),
            parameters: self.factory.parameters.clone(),
        })
    }
    fn consumed(&self, bytes: &[u8]) -> replay::Result<Vec<[u8; 16]>> {
        let state =
            State::decode(bytes, self.factory.schema).map_err(|_| replay::Error::StateMismatch)?;
        if state.action
            != self
                .factory
                .parameters
                .action()
                .map_err(|_| replay::Error::Identity)?
        {
            return Err(replay::Error::Identity);
        }
        if !state.spent {
            return Ok(vec![]);
        }
        let command = CanonicalCommand::decode(
            self.factory
                .parameters
                .command
                .as_ref()
                .ok_or(replay::Error::Identity)?,
        )
        .map_err(|_| replay::Error::Identity)?;
        Ok(vec![command.command().context.strategy.economic_intent_id])
    }
}
impl api::Factory for Rebuilder {
    type Running = Counting;
    fn binding(&self) -> api::CompiledFactory {
        api::CompiledFactory {
            id: "counting_example".into(),
            binary: self.binary,
            state_schema: self.factory.schema,
        }
    }
    fn begin(&self, input: &api::FactoryInput) -> replay::Result<Counting> {
        if !input.contract().models.is_empty()
            || input.contract().config
                != tickwire_release::Object::of(&self.factory.configuration.bytes)
        {
            return Err(replay::Error::Identity);
        }
        self.initial(input.actions())
    }
    fn consumed_actions(&self, b: &[u8]) -> replay::Result<Vec<[u8; 16]>> {
        self.consumed(b)
    }
    fn validate_checkpoint(&self, b: &[u8]) -> replay::Result<()> {
        self.consumed(b).map(|_| ())
    }
}
impl SourceReplayFactory for Rebuilder {
    type Running = replay::application::Application<Counting>;
    fn binary_digest(&self) -> Hash {
        self.binary
    }
    fn state_schema(&self) -> u64 {
        self.factory.schema
    }
    fn restore_source(
        &self,
        a: &tickwire_release::replay::VerifiedAnchor,
    ) -> replay::Result<Self::Running> {
        replay::application::ApplicationFactory {
            binary_digest: self.binary,
            business: SourceFactory {
                schema: self.factory.schema,
            },
        }
        .restore_source(a)
    }
}
impl api::rollback::ExactFactory for Rebuilder {
    fn factory_id(&self) -> &str {
        "counting_example"
    }
    fn consumed_actions(&self, b: &[u8]) -> replay::Result<Vec<[u8; 16]>> {
        self.consumed(b)
    }
}
pub(super) fn probe(
    schema: u64,
    input: &tickwire_release::rebuild::rollback::Verified,
    files: &tickwire_release::files::LoadedFiles,
) -> sdk::Result<api::rollback::Computation> {
    if input.plan().factory != "counting_example"
        || input.plan().capabilities.state_schema != schema
    {
        return Err(sdk::Error::Identity);
    }
    let configuration = if let Some(exact) = input.exact() {
        exact.checkpoint().runtime.config.clone()
    } else {
        input
            .declared()
            .ok_or(sdk::Error::Identity)?
            .inputs()
            .config
            .clone()
    };
    if files.bytes(&Role::StrategyConfig) != Some(configuration.bytes.as_slice()) {
        return Err(sdk::Error::Identity);
    }
    let factory = Rebuilder {
        factory: Factory::from_config(schema, configuration)?,
        binary: files
            .object(&Role::Binary)
            .ok_or(sdk::Error::Identity)?
            .digest,
    };
    let mut supported = factory.factory.capabilities();
    if let Some(declared) = input.declared() {
        supported.methods.push(Method::Rebuild {
            target_schema: schema,
            contract: tickwire_release::Object::of(&declared.contract().bytes()?).digest,
        });
    }
    let requested = &input.plan().capabilities;
    if requested.state_schema != supported.state_schema
        || requested.input_schema != supported.input_schema
        || requested.output_schema != supported.output_schema
        || requested.products != supported.products
        || requested.position_modes != supported.position_modes
        || requested.orders != supported.orders
        || requested
            .methods
            .iter()
            .any(|m| !supported.methods.contains(m))
    {
        return Err(sdk::Error::Identity);
    }
    if input.exact().is_some() {
        Ok(api::rollback::prepare_exact(&factory, input)?.into())
    } else {
        Ok(api::rollback::prepare_declared(&factory, input)?.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn declared_old_factory_reconstructs_its_known_action_as_spent() {
        let command = CanonicalCommand::new(crate::chain_tests::venue::command()).unwrap();
        let parameters = Parameters {
            schema: 1,
            enabled: true,
            minimum_observations: 2,
            minimum_bid_ticks: 1,
            command: Some(command.bytes().to_vec()),
        };
        let factory = Rebuilder {
            factory: Factory::from_config(
                V1,
                VersionedBytes {
                    revision: 1,
                    bytes: parameters.bytes().unwrap(),
                },
            )
            .unwrap(),
            binary: [7; 32],
        };
        let actions = [api::KnownAction {
            economic_intent: command.command().context.strategy.economic_intent_id,
            command,
        }];
        let state = factory.initial(&actions);
        assert!(
            state.is_ok(),
            "actual old factory: {:?}",
            state.as_ref().err()
        );
        let mut state = state.unwrap();
        assert!(state.state.spent);
        assert_eq!(state.state.observations, 0);
        assert_eq!(
            factory.consumed(&state.checkpoint().unwrap()).unwrap(),
            vec![actions[0].economic_intent]
        );
        let (_, replica, _) = crate::chain_tests::market_fixture();
        assert!(
            state
                .on_market(replica.view(1001).unwrap(), 1_600_000_000)
                .unwrap()
                .is_none()
        );
        assert_eq!(state.state.observations, 1);
        assert!(state.state.spent);
        assert!(factory.initial(&[]).is_err());
        let mut extra = actions.to_vec();
        extra[0].economic_intent = [99; 16];
        assert!(factory.initial(&extra).is_err());
    }
}
