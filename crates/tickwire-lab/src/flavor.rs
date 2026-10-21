//! Explicit laboratory strategy selection; production workers select their own registry.
use crate::{Result, ScenarioKind};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tickwire_release::{Capabilities, Object, replay::VersionedBytes};
use tickwire_strategy_sdk::{
    bootstrap::Bootstrap,
    registered::{FactoryBinding, Registration},
    restore::{self, RestoreFactory},
};
use tickwire_venue::CanonicalCommand;

pub(crate) fn custom(kind: ScenarioKind) -> bool {
    kind == ScenarioKind::SpotHandoverCustom
}
pub(crate) fn binary(kind: ScenarioKind, source: bool) -> &'static str {
    if custom(kind) {
        if source {
            "tickwire-counting-v1-worker"
        } else {
            "tickwire-counting-worker"
        }
    } else {
        "tickwire-strategy-worker"
    }
}
pub(crate) fn config_path(kind: ScenarioKind, dir: &Path) -> PathBuf {
    dir.join(if custom(kind) {
        "parameters.json"
    } else {
        "command.bin"
    })
}
pub(crate) fn configure(
    kind: ScenarioKind,
    bootstrap: &mut Bootstrap,
    dir: &Path,
    command: &CanonicalCommand,
    minimum_bid_ticks: i64,
) -> Result<()> {
    if !custom(kind) {
        return Ok(());
    }
    let parameters = tickwire_strategy_example::Parameters {
        schema: 1,
        enabled: true,
        minimum_observations: 1,
        minimum_bid_ticks,
        command: Some(command.bytes().to_vec()),
    };
    let bytes = parameters.bytes()?;
    let path = config_path(kind, dir);
    crate::generate::write(&path, &bytes)?;
    bootstrap.factory = Some(FactoryBinding {
        id: "counting_example".into(),
        configuration_file: path,
        configuration: Object::of(&bytes),
        revision: command.command().context.request.config_revision,
    });
    bootstrap.demo_enabled = false;
    bootstrap.demo_command_file = None;
    bootstrap.demo_minimum_bid_ticks = 0;
    Ok(())
}
pub(crate) fn schemas(kind: ScenarioKind) -> (u64, u64, u64) {
    if custom(kind) {
        (
            tickwire_strategy_example::V1,
            tickwire_strategy_example::V2,
            tickwire_strategy_example::MIGRATE,
        )
    } else {
        (
            restore::DEMO_V1_SCHEMA,
            restore::DEMO_SCHEMA,
            restore::DEMO_MIGRATION,
        )
    }
}
pub(crate) fn capabilities(kind: ScenarioKind, bootstrap: &Bootstrap) -> Result<Capabilities> {
    if !custom(kind) {
        return Ok(restore::FixedDemoFactory.capabilities());
    }
    let b = bootstrap.factory.as_ref().ok_or("custom factory")?;
    let bytes = std::fs::read(&b.configuration_file)?;
    b.configuration.verify(&bytes, 2 * 1024 * 1024)?;
    let target = restore::Target {
        instance: bootstrap.market.local.instance,
        generation: bootstrap.market.local.generation,
        logical_strategy: bootstrap.market.local.logical_strategy,
        command: None,
        minimum_bid_ticks: 0,
        enabled: false,
    };
    Ok(tickwire_strategy_example::Registration::v2()
        .configure(
            &target,
            &VersionedBytes {
                revision: b.revision,
                bytes,
            },
        )?
        .capabilities())
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CountingState {
    pub schema: u64,
    pub observations: u64,
    pub reports: u64,
    pub spent: bool,
    pub action: [u8; 32],
}
impl CountingState {
    pub fn decode(bytes: &[u8], schema: u64, spent: bool) -> Result<Self> {
        let s: Self = serde_json::from_slice(bytes)?;
        if s.schema != schema
            || s.spent != spent
            || s.action == [0; 32]
            || serde_json::to_vec(&s)? != bytes
        {
            return Err("counting strategy state binding".into());
        }
        Ok(s)
    }
}
pub(crate) fn source_state(kind: ScenarioKind, checkpoint: &[u8]) -> Result<()> {
    let state = checkpoint.get(96..).ok_or("source checkpoint")?;
    if custom(kind) {
        CountingState::decode(state, tickwire_strategy_example::V1, false)?;
    } else if state.get(..8) != Some(b"TWFD0001") || state.get(8) != Some(&1) {
        return Err("laboratory source action is not pending".into());
    }
    Ok(())
}
pub(crate) fn target_state(kind: ScenarioKind, state: &[u8], spent: bool) -> Result<()> {
    if custom(kind) {
        CountingState::decode(state, tickwire_strategy_example::V2, spent)?;
    } else if state.len() != 41 || &state[..8] != b"TWFD0002" || state[8] != u8::from(spent) {
        return Err("actual restored action state differs from expected cut".into());
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(crate) enum ProgressPhase {
    Installed,
    Final,
}
pub(crate) fn counting_progress(
    c: &CountingState,
    d: &CountingState,
    phase: ProgressPhase,
) -> Result<()> {
    if d.observations < c.observations
        || d.reports < c.reports
        || d.action != c.action
        || !d.spent
        || (matches!(phase, ProgressPhase::Final)
            && (d.observations == c.observations || d.reports == c.reports))
    {
        return Err(
            "counting strategy has not consumed the required post-install callbacks".into(),
        );
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn installation_before_late_fill_keeps_exact_c_and_final_requires_real_progress() {
        // Actual first custom Linux C and the latest authenticated checkpoint.
        let c = CountingState {
            schema: tickwire_strategy_example::V2,
            observations: 3,
            reports: 4,
            spent: true,
            action: [9; 32],
        };
        counting_progress(&c, &c, ProgressPhase::Installed).unwrap();
        assert!(counting_progress(&c, &c, ProgressPhase::Final).is_err());
        let d = CountingState {
            observations: 24,
            reports: 5,
            ..c.clone()
        };
        counting_progress(&c, &d, ProgressPhase::Final).unwrap();
        let stale = CountingState {
            reports: 3,
            ..d.clone()
        };
        assert!(counting_progress(&c, &stale, ProgressPhase::Installed).is_err());
        let other = CountingState {
            action: [8; 32],
            ..d
        };
        assert!(counting_progress(&c, &other, ProgressPhase::Final).is_err());
    }
}
