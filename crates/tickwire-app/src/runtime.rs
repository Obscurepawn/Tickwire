//! Unified stateless ORS process runtime (`OrsAppHandle`, `inspect_ors_config`,
//! `spawn_unified_ors_process`, `run_unified_ors_until_signal`) driving
//! `OrsProcess<UnifiedVenueSession>` and `SignalWaiter` (`SIGUSR1` RMS reload + `QueryOpen` +
//! `Dump`, `SIGUSR2` `CancelAll` + `OrsRestricted`, and `SIGINT`/`SIGTERM` `2s` drain +
//! `OrsShutDown`).

#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tickwire_control_sys::SignalWaiter;
use tickwire_runtime::{OrderRoutingSystem, OrsProcess, OrsSignalOutcome};
use tickwire_venue::AssetKey;

use crate::config::OrsAppConfig;
use crate::external_lanes::{ExternalLaneKeepAlive, prepare_external_strategy_lanes};
use crate::venue::UnifiedVenueSession;

/// Cold-startup inspection summary emitted by `tickwire-app inspect --config <path>`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrsInspectSummary {
    pub venue: String,
    pub environment: String,
    pub account_count: usize,
    pub symbol_count: usize,
    pub allowed_strategies: Vec<u8>,
    pub external_strategy_lanes: usize,
    pub catalog_version: Option<u64>,
    pub max_order_age_ns: u64,
    pub req_cpu: Option<u32>,
    pub resp_cpu: Option<u32>,
}

/// Final in-memory balance entry exported on optional shutdown summary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrsWalletBalanceSummary {
    pub account_ref: u8,
    pub wallet_id: u16,
    pub asset_id: u64,
    pub free_atoms: String,
    pub locked_atoms: String,
}

/// Final in-memory position entry exported on optional shutdown summary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrsPositionSummary {
    pub account_ref: u8,
    pub symbol_id: u32,
    pub signed_lots: i64,
}

/// Diagnostic JSON summary optionally written to `summary_output_path` after graceful shutdown.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrsShutdownSummary {
    pub venue: String,
    pub open_order_and_quote_count: usize,
    pub balances: Vec<OrsWalletBalanceSummary>,
    pub positions: Vec<OrsPositionSummary>,
}

/// Live handle wrapping `OrsProcess<UnifiedVenueSession>` and any external strategy Unix socket
/// keep-alives.
pub struct OrsAppHandle {
    pub process: OrsProcess<UnifiedVenueSession>,
    venue: String,
    tracked_assets: Vec<AssetKey>,
    symbol_ids: Vec<u32>,
    _lane_keep_alives: Vec<ExternalLaneKeepAlive>,
}

impl OrsAppHandle {
    /// Captures a deterministic snapshot of the in-memory `UtaWalletLedger` and open-order count.
    pub fn snapshot_summary(&self) -> OrsShutdownSummary {
        let om = self.process.order_manager();
        let ledger = om.wallet_ledger();
        let mut balances = Vec::new();
        let mut positions = Vec::new();
        for acct in self.process.accounts() {
            let aref = acct.account_ref();
            for &key in &self.tracked_assets {
                if let Some((free, locked)) = ledger.balance(aref, key) {
                    balances.push(OrsWalletBalanceSummary {
                        account_ref: aref,
                        wallet_id: key.wallet_id,
                        asset_id: key.asset_id,
                        free_atoms: free.to_string(),
                        locked_atoms: locked.to_string(),
                    });
                }
            }
            for &sid in &self.symbol_ids {
                if let Some(lots) = ledger.position_lots(aref, sid as usize)
                    && lots != 0
                {
                    positions.push(OrsPositionSummary {
                        account_ref: aref,
                        symbol_id: sid,
                        signed_lots: lots,
                    });
                }
            }
        }
        OrsShutdownSummary {
            venue: self.venue.clone(),
            open_order_and_quote_count: self.process.open_order_and_quote_count(),
            balances,
            positions,
        }
    }
}

fn collect_tracked_assets(config: &OrsAppConfig) -> Vec<AssetKey> {
    let mut set = BTreeSet::new();
    for a in &config.binance_assets {
        set.insert((a.wallet_id, a.asset_id));
    }
    for s in &config.binance_symbols {
        set.insert((s.wallet_id, s.base_asset_id));
        set.insert((s.wallet_id, s.quote_asset_id));
        set.insert((s.wallet_id, s.settlement_asset_id));
    }
    for a in &config.uta_assets {
        set.insert((a.wallet_id, a.asset_id));
    }
    for s in &config.uta_symbols {
        set.insert((s.wallet_id, s.base_asset_id));
        set.insert((s.wallet_id, s.quote_asset_id));
        set.insert((s.wallet_id, s.settlement_asset_id));
    }
    set.into_iter()
        .map(|(wallet_id, asset_id)| AssetKey {
            wallet_id,
            asset_id,
        })
        .collect()
}

/// Validates `config`, runs hot-path wire/decoder warmup, and returns an [`OrsInspectSummary`].
pub fn inspect_ors_config(config: &OrsAppConfig) -> Result<OrsInspectSummary, String> {
    let prepared = config.prepare()?;
    for (_, session) in &prepared.sessions {
        session.warm_hot_path(2)?;
    }
    Ok(OrsInspectSummary {
        venue: prepared.venue.as_str().to_owned(),
        environment: format!("{:?}", config.environment.to_environment()),
        account_count: prepared.account_manager.specs().len(),
        symbol_count: prepared.order_manager_config.symbols.len(),
        allowed_strategies: prepared.allowed_strategies,
        external_strategy_lanes: prepared.external_lanes.len(),
        catalog_version: prepared.catalog_version,
        max_order_age_ns: prepared.runtime_config.max_order_age_ns,
        req_cpu: prepared.runtime_config.cpu.req_cpu,
        resp_cpu: prepared.runtime_config.cpu.resp_cpu,
    })
}

/// Spawns [`OrsProcess<UnifiedVenueSession>`] from `config` without blocking on signals.
pub fn spawn_unified_ors_process(
    config: &OrsAppConfig,
    block_signals: bool,
) -> Result<OrsAppHandle, String> {
    let tracked_assets = collect_tracked_assets(config);
    let mut prepared = config.prepare()?;
    prepared.process_config.block_signals_on_spawn = block_signals;
    let symbol_ids: Vec<u32> = prepared
        .order_manager_config
        .symbols
        .iter()
        .map(|s| s.symbol_id)
        .collect();

    for (_, session) in &prepared.sessions {
        session.warm_hot_path(2)?;
    }

    let mut factories = Vec::with_capacity(prepared.external_lanes.len());
    let mut keep_alives = Vec::with_capacity(prepared.external_lanes.len());
    for ext in prepared.external_lanes {
        let strat_id = ext.strat_id;
        let (ifac, rfac, keep) = prepare_external_strategy_lanes(ext)?;
        factories.push((strat_id, ifac, rfac));
        keep_alives.push(keep);
    }

    let venue_name = prepared.venue.as_str().to_owned();
    let process = OrsProcess::build_and_spawn(
        prepared.runtime_config,
        &prepared.account_manager,
        &prepared.allowed_strategies,
        prepared.order_manager_config,
        prepared.rms_engine,
        prepared.sessions,
        prepared.process_config,
        move |ors: &mut OrderRoutingSystem<UnifiedVenueSession>| {
            for (strat_id, ifac, rfac) in factories {
                ors.attach_strategy_lane_factories(strat_id, ifac, rfac)?;
            }
            Ok(())
        },
    )?;

    Ok(OrsAppHandle {
        process,
        venue: venue_name,
        tracked_assets,
        symbol_ids,
        _lane_keep_alives: keep_alives,
    })
}

fn write_summary_if_configured(
    path_opt: Option<&Path>,
    summary: &OrsShutdownSummary,
) -> Result<(), String> {
    let Some(path) = path_opt else {
        return Ok(());
    };
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(summary)
        .map_err(|e| format!("failed to serialize OrsShutdownSummary: {e}"))?;
    std::fs::write(path, &bytes).map_err(|e| format!("failed to write {}: {e}", path.display()))
}

/// Spawns [`OrsProcess<UnifiedVenueSession>`] and parks `main` on [`SignalWaiter`] until `SIGINT`
/// or `SIGTERM` completes ordered graceful shutdown (`2s` open-order/quote drain + `OrsShutDown`).
pub fn run_unified_ors_until_signal(config: &OrsAppConfig) -> Result<OrsShutdownSummary, String> {
    let waiter = SignalWaiter::new().map_err(|e| format!("SignalWaiter::new failed: {e}"))?;
    let handle = spawn_unified_ors_process(config, false)?;
    if let Some(err) = handle.process.worker_init_error() {
        handle.process.shutdown();
        return Err(format!("ORS worker initialization failed: {err}"));
    }

    loop {
        if let Some(err) = handle.process.worker_init_error() {
            handle.process.shutdown();
            return Err(format!("ORS worker initialization failed: {err}"));
        }
        if let Some(sig) = waiter
            .wait_timeout(Duration::from_millis(200))
            .map_err(|e| format!("SignalWaiter::wait_timeout failed: {e}"))?
        {
            let outcome = handle.process.handle_signal(sig);
            if outcome == OrsSignalOutcome::ShutdownInitiated {
                break;
            }
        }
        if handle.process.is_shut_down() {
            break;
        }
    }

    let summary = handle.snapshot_summary();
    write_summary_if_configured(config.summary_output_path.as_deref(), &summary)?;
    Ok(summary)
}
