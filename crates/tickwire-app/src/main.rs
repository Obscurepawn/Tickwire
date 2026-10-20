use std::{path::PathBuf, time::Instant};
use tickwire_app::{
    Result,
    config::{Deployment, ProductName},
    strategy::FileStrategy,
};
use tickwire_binance_public::DepthPolicy;
use tickwire_runtime::{files, node::NoPublisher};

fn main() {
    if let Err(error) = execute() {
        eprintln!(
            "{}",
            serde_json::json!({"event":"fatal","error":error.to_string()})
        );
        std::process::exit(1);
    }
}
fn execute() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let Some(command) = args.next() else {
        return usage();
    };
    if command == "--help" || command == "-h" {
        return usage();
    }
    let Some(flag) = args.next() else {
        return usage();
    };
    if flag != "--config" {
        return usage();
    }
    let path = PathBuf::from(args.next().ok_or("missing configuration path")?);
    let output = if command == "export" {
        if args.next().as_deref() != Some(std::ffi::OsStr::new("--out")) {
            return Err("export requires --out /new/directory".into());
        }
        Some(PathBuf::from(
            args.next().ok_or("missing export directory")?,
        ))
    } else {
        None
    };
    if args.next().is_some() {
        return Err("unexpected argument".into());
    }
    let config = Deployment::read(&path)?;
    if command == "replay" {
        println!(
            "{}",
            serde_json::to_string_pretty(&tickwire_app::replay::run(&config)?)?
        );
        return Ok(());
    }
    if let Some(output) = output {
        println!(
            "{}",
            serde_json::to_string_pretty(&tickwire_app::replay::export(&config, &output)?)?
        );
        return Ok(());
    }
    if command == "inspect" {
        let baseline = config.baseline()?;
        let _ = config.assemble(baseline.clone())?;
        println!(
            "{}",
            serde_json::json!({"event":"configuration","account":baseline.snapshot().scope.account,"account_epoch":baseline.snapshot().account_epoch,"ledger_sequence":baseline.snapshot().ledger_sequence,"instruments":baseline.catalog().entries.len(),"recover":config.recover})
        );
        return Ok(());
    }
    if command != "run" {
        return usage();
    }
    match config.product {
        ProductName::Spot => run::<tickwire_book::BinanceSpot>(config),
        ProductName::Usdm | ProductName::UsdmFuture => run::<tickwire_book::BinanceUsdM>(config),
    }
}
fn usage() -> Result<()> {
    println!(
        "tickwire-app inspect --config /absolute/deployment.json\n\
              tickwire-app run --config /absolute/deployment.json\n\
              tickwire-app replay --config /absolute/deployment.json\n\
              tickwire-app export --config /absolute/deployment.json --out /absolute/new-export"
    );
    Ok(())
}
fn run<P: DepthPolicy>(config: Deployment) -> Result<()> {
    if !config.stop_file.is_absolute() {
        return Err("stop_file must be absolute".into());
    }
    if config.stop_file.exists() {
        return Err("stop_file already exists".into());
    }
    let baseline = config.baseline()?;
    let material = config.credential.load()?;
    let domain = config.domain()?;
    let mut clock = tickwire_binance_clock::SystemClock::new(domain)
        .map_err(|e| format!("system clock: {e:?}"))?;
    let now = clock
        .sample()
        .map_err(|e| format!("system clock: {e:?}"))?
        .monotonic;
    let plane = config
        .policy
        .build()
        .map_err(|e| format!("control configuration: {e:?}"))?;
    let policy = plane
        .provider(clock)
        .map_err(|e| format!("control clock: {e:?}"))?;
    let (mut controls, endpoint, _quota_coordinator) = if let Some(control) = &config.control {
        let control_clock = tickwire_binance_clock::SystemClock::new(domain)
            .map_err(|e| format!("control clock: {e:?}"))?;
        let (inbox, endpoint) = tickwire_app::control::ControlInbox::open(
            control.clone(),
            &config.policy,
            &config.quota,
            plane.clone(),
            control_clock,
        )?;
        (Some(inbox), endpoint, None)
    } else {
        let built = config
            .quota
            .build(tickwire_quota::MockQuotaStore::default(), now)
            .map_err(|e| format!("quota configuration: {e:?}"))?;
        (None, built.endpoint, Some(built.coordinator))
    };
    let networks = config.network.resolve(config.tuning.connect_timeout_ns)?;
    let credentials = config.credentials(&baseline, &material);
    let reconciliation_credentials = config.credentials(&baseline, &material);
    let authentication = config.authentication(&material);
    let settings = config.assemble(baseline)?;
    let strategy = build_strategy(&config.strategy, &settings, plane.clone())?;
    if config.reconciliation.first_execution_connection_epoch <= config.first_connection_epoch {
        return Err("reconciliation REST epoch must exceed the initial REST epoch".into());
    }
    let mut reconciliation = tickwire_app::reconciliation::Controller::new(
        config.reconciliation.clone(),
        tickwire_binance_account::reconciliation::ReconcileConfig {
            scope: settings.private.scope.clone(),
            address: networks.rest.address,
            tls: networks.rest.tls.clone(),
            limits: networks.rest.limits,
            first_connection_epoch: config.reconciliation.first_request_epoch,
            request_timeout_ns: config.tuning.request_timeout_ns,
            recv_window_micros: config.tuning.recv_window_micros,
            max_response_bytes: 32 * 1024,
            max_headers: 64,
        },
        tickwire_binance_quota::ReadBinding {
            owner_id: config.reconciliation.reader_id,
            scope: settings.private.scope.clone(),
            domain,
        },
        plane.clone(),
        reconciliation_credentials,
        config.writer_epoch,
        config.tuning.delivery_budget_ns,
        config.tuning.connect_timeout_ns,
    )?;
    let mut node = files::open::<P, _, _, _, 8192, 8192, 8192>(
        settings,
        networks,
        credentials,
        authentication,
        endpoint,
        policy,
        strategy,
        NoPublisher,
        Box::new(plane.observer()),
    )?;
    // Complete durable input replay before accepting a fresh private-session input.
    while node.replay_pending(1024)? != 0 {}
    let started = Instant::now();
    eprintln!(
        "{}",
        serde_json::json!({"event":"started","recover":config.recover})
    );
    node.run_with_maintenance(
        |node, registry| {
            if let Some(control) = &mut controls {
                node.session()
                    .with_resources(|_, endpoint| control.poll(endpoint))
                    .map_err(tickwire_runtime::node::Error::Shared)?
                    .map_err(|error| {
                        eprintln!(
                            "{}",
                            serde_json::json!({"event":"control_error","error":error.to_string()})
                        );
                        tickwire_runtime::node::Error::Strategy
                    })?;
            }
            reconciliation.drive(node, registry)
        },
        |node, _| {
            !node.strategy().failed()
                && !config.stop_file.exists()
                && (config.maximum_runtime_ns == 0
                    || started.elapsed().as_nanos() < u128::from(config.maximum_runtime_ns))
        },
    )?;
    eprintln!(
        "{}",
        serde_json::json!({"event":"stopped","ledger_sequence":node.account().ledger_sequence(),"account_frozen":node.account().frozen()})
    );
    Ok(())
}

fn build_strategy(
    config: &tickwire_app::strategy::Config,
    settings: &files::Configuration,
    plane: tickwire_runtime_policy::ControlPlane,
) -> Result<Box<dyn tickwire_app::strategy::AppStrategy>> {
    use tickwire_app::strategy::{Config, MultiStrategy};
    use tickwire_ipc::Wire;
    match config {
        Config::Hosted { config: host } => tickwire_app::hosted::build(host, settings, plane),
        Config::Multi { members } => {
            if members.is_empty() || members.len() > 32 {
                return Err("multi strategy requires 1 through 32 members".into());
            }
            let mut seen = std::collections::BTreeSet::new();
            let mut topics = std::collections::BTreeSet::new();
            let mut children = Vec::with_capacity(members.len());
            for member in members {
                if member.logical_strategy_id == 0
                    || !seen.insert(member.logical_strategy_id)
                    || matches!(member.strategy.as_ref(), Config::Multi { .. })
                {
                    return Err(
                        "multi strategy members require unique nonzero logical IDs and cannot nest"
                            .into(),
                    );
                }
                if let Config::Hosted { config: host } = member.strategy.as_ref() {
                    let header = tickwire_ipc::ResourceHeader::decode(
                        &tickwire_app::config::read_file(&host.market_resource, 4096)?,
                    )
                    .map_err(|e| format!("market resource: {e:?}"))?;
                    if !topics.insert(header.topic_id) {
                        return Err(
                            "independent strategy publishers require distinct topic IDs".into()
                        );
                    }
                    let bootstrap: tickwire_strategy_sdk::bootstrap::Bootstrap =
                        serde_json::from_slice(&tickwire_app::config::read_file(
                            &host.bootstrap,
                            1024 * 1024,
                        )?)?;
                    if bootstrap.market.local.logical_strategy != member.logical_strategy_id {
                        return Err("strategy member does not match its process identity".into());
                    }
                }
                let strategy = build_strategy(&member.strategy, settings, plane.clone())?;
                children.push((member.logical_strategy_id, strategy));
            }
            Ok(Box::new(MultiStrategy::new(children, plane)?))
        }
        other => Ok(Box::new(FileStrategy::new(other)?)),
    }
}
