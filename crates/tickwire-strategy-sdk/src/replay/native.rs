//! Built-in source factories restore the old parameters and action-consumed bit.
use super::*;
use crate::Strategy;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Parameters {
    pub schema: u16,
    pub command: Option<Vec<u8>>,
    pub minimum_bid_ticks: i64,
    pub enabled: bool,
}
impl Parameters {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        Ok(canonical(self)?)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let p: Self = serde_json::from_slice(bytes).map_err(|_| Error::Identity)?;
        if p.schema != 1 || p.bytes()? != bytes {
            return Err(Error::Identity);
        }
        Ok(p)
    }
}
#[derive(Clone, Debug)]
pub struct Registration {
    pub schema: u64,
    pub config: VersionedBytes,
    pub models: Vec<Model>,
    pub rng: Rng,
}
pub fn observe() -> Registration {
    Registration {
        schema: crate::restore::OBSERVE_SCHEMA,
        config: VersionedBytes {
            revision: 1,
            bytes: Parameters {
                schema: 1,
                command: None,
                minimum_bid_ticks: 0,
                enabled: false,
            }
            .bytes()
            .expect("fixed parameters"),
        },
        models: vec![],
        rng: Rng::Disabled,
    }
}
pub fn demo(s: &crate::FixedDemo, schema: u64) -> Result<Registration> {
    // V1 encoded enabled && pending in one bit. It cannot distinguish a
    // disabled pending action from one already consumed.
    if schema == crate::restore::DEMO_V1_SCHEMA && !s.enabled {
        return Err(Error::Unsupported);
    }
    Ok(Registration {
        schema,
        config: VersionedBytes {
            revision: s.original.command().context.request.config_revision,
            bytes: Parameters {
                schema: 1,
                command: Some(s.original.bytes().to_vec()),
                minimum_bid_ticks: s.minimum_bid_ticks,
                enabled: s.enabled,
            }
            .bytes()?,
        },
        models: vec![],
        rng: Rng::Disabled,
    })
}
pub enum Native {
    Observe(crate::ObserveOnly),
    V1(crate::FixedDemo),
    V2(crate::restore::RestoredDemo),
}
impl Strategy for Native {
    fn apply_config(&mut self, bytes: &[u8], context: &mut ReplayContext<'_>) -> crate::Result<()> {
        match self {
            Self::Observe(s) => s.apply_config(bytes, context),
            Self::V1(s) => s.apply_config(bytes, context),
            Self::V2(s) => s.apply_config(bytes, context),
        }
    }
    fn checkpoint(&self) -> crate::Result<Vec<u8>> {
        match self {
            Self::Observe(s) => s.checkpoint(),
            Self::V1(s) => s.checkpoint(),
            Self::V2(s) => s.checkpoint(),
        }
    }
    fn on_market(
        &mut self,
        v: crate::MarketView<'_>,
        now: u64,
    ) -> crate::Result<Option<CanonicalCommand>> {
        match self {
            Self::Observe(s) => s.on_market(v, now),
            Self::V1(s) => s.on_market(v, now),
            Self::V2(s) => s.on_market(v, now),
        }
    }
}
pub struct Factory {
    pub schema: u64,
}
impl application::BusinessFactory for Factory {
    type Running = Native;
    fn schema(&self) -> u64 {
        self.schema
    }
    fn restore(&self, state: &[u8], runtime: &RuntimeState) -> Result<Native> {
        let p = Parameters::decode(&runtime.config.bytes)?;
        if self.schema == crate::restore::OBSERVE_SCHEMA {
            if state != b"TWOBS001" || p.command.is_some() || p.enabled || p.minimum_bid_ticks != 0
            {
                return Err(Error::Identity);
            }
            return Ok(Native::Observe(crate::ObserveOnly));
        }
        let command = CanonicalCommand::decode(p.command.as_deref().ok_or(Error::Identity)?)
            .map_err(|_| Error::Identity)?;
        let mut inner = crate::FixedDemo::new(command.clone(), p.minimum_bid_ticks, p.enabled);
        let out = match self.schema {
            crate::restore::DEMO_V1_SCHEMA => {
                if !p.enabled {
                    return Err(Error::Unsupported);
                }
                if state.len() < 21
                    || &state[..8] != b"TWFD0001"
                    || state[8] > 1
                    || (!p.enabled && state[8] != 0)
                {
                    return Err(Error::Identity);
                }
                if state[8] == 0 {
                    inner.command = None;
                }
                Native::V1(inner)
            }
            crate::restore::DEMO_SCHEMA => {
                if state.len() != 41 || &state[..8] != b"TWFD0002" || state[8] > 1 {
                    return Err(Error::Identity);
                }
                let action = tickwire_recovery::digest(
                    &command.command().context.strategy.economic_intent_id,
                );
                if state[9..] != action {
                    return Err(Error::Identity);
                }
                if state[8] == 1 {
                    inner.command = None;
                }
                Native::V2(crate::restore::RestoredDemo { inner, action })
            }
            _ => return Err(Error::Unsupported),
        };
        if out.checkpoint().map_err(|_| Error::Callback)? != state {
            return Err(Error::StateMismatch);
        }
        Ok(out)
    }
}
pub(crate) fn apply_observe(bytes: &[u8], context: &ReplayContext<'_>) -> crate::Result<()> {
    let p = Parameters::decode(bytes)?;
    if bytes != context.config().bytes
        || p.command.is_some()
        || p.enabled
        || p.minimum_bid_ticks != 0
    {
        return Err(crate::Error::Identity);
    }
    Ok(())
}
/// The demonstration permits threshold/enable changes and a new configuration
/// revision. Its original action and authority remain fixed; a consumed action
/// stays consumed even when a new command template is installed.
pub(crate) fn apply_demo(
    s: &mut crate::FixedDemo,
    v1: bool,
    bytes: &[u8],
    context: &ReplayContext<'_>,
) -> crate::Result<()> {
    let p = Parameters::decode(bytes)?;
    let command = CanonicalCommand::decode(p.command.as_deref().ok_or(crate::Error::Identity)?)?;
    let mut expected = s.original.command().clone();
    expected.context.request.config_revision = context.config().revision;
    if bytes != context.config().bytes || command.command() != &expected || (v1 && !p.enabled) {
        return Err(crate::Error::Identity);
    }
    if s.command.is_some() {
        s.command = Some(command.clone());
    }
    s.original = command;
    s.minimum_bid_ticks = p.minimum_bid_ticks;
    s.enabled = p.enabled;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use application::BusinessFactory;
    fn runtime(r: Registration) -> RuntimeState {
        RuntimeState {
            now_ns: 1,
            config: r.config,
            models: vec![],
            markets: vec![],
            accounts: vec![],
            external: vec![],
            timers: vec![],
            rng: r.rng,
            effects: EffectCut {
                count: 0,
                digest: [0; 32],
            },
        }
    }
    #[test]
    fn built_in_factory_restores_the_actual_old_parameters_and_consumed_flag() {
        let command = CanonicalCommand::new(crate::test_command::command()).unwrap();
        let mut old = crate::FixedDemo::new(command, 65000, true);
        let r = demo(&old, crate::restore::DEMO_V1_SCHEMA).unwrap();
        old.command = None;
        let state = old.checkpoint().unwrap();
        let out = Factory { schema: r.schema }
            .restore(&state, &runtime(r))
            .unwrap();
        assert_eq!(out.checkpoint().unwrap(), state);
        let Native::V1(s) = out else { panic!() };
        assert!(s.command.is_none());
        assert!(s.enabled);
        assert_eq!(s.minimum_bid_ticks, 65000);
    }
    #[test]
    fn source_parameters_cannot_silently_change_during_restore() {
        let command = CanonicalCommand::new(crate::test_command::command()).unwrap();
        let old = crate::FixedDemo::new(command, 65000, true);
        let mut r = demo(&old, crate::restore::DEMO_V1_SCHEMA).unwrap();
        let mut p = Parameters::decode(&r.config.bytes).unwrap();
        p.minimum_bid_ticks = 1;
        r.config.bytes = p.bytes().unwrap();
        assert!(
            Factory { schema: r.schema }
                .restore(&old.checkpoint().unwrap(), &runtime(r))
                .is_err()
        );
    }
    #[test]
    fn observe_factory_is_actual_and_does_not_accept_demo_parameters() {
        let r = observe();
        let out = Factory { schema: r.schema }
            .restore(b"TWOBS001", &runtime(r))
            .unwrap();
        assert_eq!(out.checkpoint().unwrap(), b"TWOBS001");
    }
    #[test]
    fn config_apply_changes_actual_parameters_without_reviving_consumed_action() {
        let command = CanonicalCommand::new(crate::test_command::command()).unwrap();
        let mut old = crate::FixedDemo::new(command.clone(), 65000, true);
        old.command = None;
        let action =
            tickwire_recovery::digest(&command.command().context.strategy.economic_intent_id);
        let mut strategy = crate::restore::RestoredDemo { inner: old, action };
        let mut revised = command.command().clone();
        revised.context.request.config_revision += 1;
        let command = CanonicalCommand::new(revised).unwrap();
        let p = Parameters {
            schema: 1,
            command: Some(command.bytes().to_vec()),
            minimum_bid_ticks: 1,
            enabled: true,
        };
        let mut rt = runtime(demo(&strategy.inner, crate::restore::DEMO_SCHEMA).unwrap());
        rt.config = VersionedBytes {
            revision: command.command().context.request.config_revision,
            bytes: p.bytes().unwrap(),
        };
        let bytes = rt.config.bytes.clone();
        strategy
            .apply_config(
                &bytes,
                &mut ReplayContext {
                    views: None,
                    runtime: &mut rt,
                },
            )
            .unwrap();
        assert_eq!(strategy.inner.minimum_bid_ticks, 1);
        assert_eq!(strategy.inner.original, command);
        assert!(strategy.inner.command.is_none());
        assert_eq!(strategy.checkpoint().unwrap()[8], 1);
    }
    #[test]
    fn registration_keeps_actual_config_revision_and_rejects_ambiguous_v1_state() {
        let mut command = crate::test_command::command();
        command.context.request.config_revision = 73;
        let command = CanonicalCommand::new(command).unwrap();
        let strategy = crate::FixedDemo::new(command.clone(), 65000, true);
        assert_eq!(
            demo(&strategy, crate::restore::DEMO_V1_SCHEMA)
                .unwrap()
                .config
                .revision,
            73
        );
        let disabled = crate::FixedDemo::new(command, 65000, false);
        assert!(demo(&disabled, crate::restore::DEMO_V1_SCHEMA).is_err());
    }
    #[test]
    fn observe_config_apply_executes_and_rejects_a_different_strategy() {
        let mut strategy = crate::ObserveOnly;
        let mut rt = runtime(observe());
        let bytes = rt.config.bytes.clone();
        strategy
            .apply_config(
                &bytes,
                &mut ReplayContext {
                    views: None,
                    runtime: &mut rt,
                },
            )
            .unwrap();
        let mut p = Parameters::decode(&bytes).unwrap();
        p.enabled = true;
        assert!(
            strategy
                .apply_config(
                    &p.bytes().unwrap(),
                    &mut ReplayContext {
                        views: None,
                        runtime: &mut rt
                    }
                )
                .is_err()
        );
    }
}
