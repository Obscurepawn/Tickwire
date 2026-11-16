//! Stateless UTA ORS process launcher driving `OrsProcess<UtaVenueSession>` and `SignalWaiter`
//! (`SIGUSR1` RMS reload + `QueryOpen` + `Dump`, `SIGUSR2` `CancelAll`, and `SIGINT`/`SIGTERM`
//! `2s` open-order/quote drain + `OrsShutDown`).

use std::time::Duration;

use serde::Serialize;
use tickwire_control_sys::SignalWaiter;
use tickwire_runtime::{OrderRoutingSystem, OrsProcess, OrsSignalOutcome};

use crate::config::SpotRunnerConfig;
use crate::session::UtaVenueSession;

/// Inspection summary emitted by `tickwire-spot-runner inspect --config <path>`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SpotInspectSummary {
    pub venue: String,
    pub environment: String,
    pub transport: String,
    pub account_count: usize,
    pub symbol_count: usize,
    pub asset_count: usize,
    pub allowed_strategies: Vec<u8>,
    pub max_order_age_ns: u64,
    pub req_cpu: Option<u32>,
    pub resp_cpu: Option<u32>,
}

/// Builds a [`SpotInspectSummary`] after validating `config` and warming the hot-path wire/JSON
/// pipeline.
pub fn inspect_config(config: &SpotRunnerConfig) -> Result<SpotInspectSummary, String> {
    let prepared = config.prepare()?;
    for (_, session) in &prepared.sessions {
        let _ = session.warm_hot_path(2)?;
    }
    Ok(SpotInspectSummary {
        venue: config.venue.to_spot_venue().as_str().to_owned(),
        environment: format!("{:?}", config.environment.to_environment()),
        transport: format!("{:?}", config.transport.to_transport_mode()),
        account_count: prepared.account_manager.specs().len(),
        symbol_count: config.symbols.len(),
        asset_count: config.assets.len(),
        allowed_strategies: prepared.allowed_strategies,
        max_order_age_ns: prepared.runtime_config.max_order_age_ns,
        req_cpu: prepared.runtime_config.cpu.req_cpu,
        resp_cpu: prepared.runtime_config.cpu.resp_cpu,
    })
}

/// Spawns [`OrsProcess<UtaVenueSession>`] from `config` without blocking on signals.
pub fn spawn_uta_ors_process(
    config: &SpotRunnerConfig,
) -> Result<OrsProcess<UtaVenueSession>, String> {
    let prepared = config.prepare()?;
    for (_, session) in &prepared.sessions {
        let _ = session.warm_hot_path(2)?;
    }
    OrsProcess::build_and_spawn(
        prepared.runtime_config,
        &prepared.account_manager,
        &prepared.allowed_strategies,
        prepared.order_manager_config,
        prepared.rms_engine,
        prepared.sessions,
        prepared.process_config,
        |_ors: &mut OrderRoutingSystem<UtaVenueSession>| Ok(()),
    )
}

/// Spawns [`OrsProcess<UtaVenueSession>`] and parks `main` on [`SignalWaiter`] until `SIGINT` or
/// `SIGTERM` completes ordered graceful shutdown.
pub fn run_until_signal(config: &SpotRunnerConfig) -> Result<(), String> {
    let waiter = SignalWaiter::new().map_err(|e| format!("SignalWaiter::new failed: {e}"))?;
    let mut cfg = config.clone();
    let _ = &mut cfg;
    let mut prepared = config.prepare()?;
    prepared.process_config.block_signals_on_spawn = false;
    for (_, session) in &prepared.sessions {
        let _ = session.warm_hot_path(2)?;
    }
    let process = OrsProcess::build_and_spawn(
        prepared.runtime_config,
        &prepared.account_manager,
        &prepared.allowed_strategies,
        prepared.order_manager_config,
        prepared.rms_engine,
        prepared.sessions,
        prepared.process_config,
        |_ors: &mut OrderRoutingSystem<UtaVenueSession>| Ok(()),
    )?;
    loop {
        if let Some(sig) = waiter
            .wait_timeout(Duration::from_millis(250))
            .map_err(|e| format!("SignalWaiter::wait_timeout failed: {e}"))?
        {
            let outcome = process.handle_signal(sig);
            if outcome == OrsSignalOutcome::ShutdownInitiated {
                break;
            }
        }
        if process.is_shut_down() {
            break;
        }
    }
    Ok(())
}
