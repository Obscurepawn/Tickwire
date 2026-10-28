use std::{path::PathBuf, time::Instant};
use tickwire_app::{
    Result,
    config::{
        Deployment, PUBLIC_BOOK_LEVELS, PUBLIC_BUFFERED_EVENTS, PUBLIC_DEPTH_UPDATES, ProductName,
    },
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
    if command == "seal" {
        let configuration = tickwire_app::failover::SealConfig::read(&path)?;
        println!(
            "{}",
            serde_json::to_string_pretty(&tickwire_app::failover::seal(&configuration)?)?
        );
        return Ok(());
    }
    if command == "restore" {
        let configuration = tickwire_app::failover::RestoreConfig::read(&path)?;
        println!(
            "{}",
            serde_json::to_string_pretty(&tickwire_app::failover::restore(&configuration)?)?
        );
        return Ok(());
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
              tickwire-app export --config /absolute/deployment.json --out /absolute/new-export\n\
              tickwire-app seal --config /absolute/seal.json\n\
              tickwire-app restore --config /absolute/restore.json"
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
    let credentials = config.credentials(&baseline, &material);
    let reconciliation_credentials = config.credentials(&baseline, &material);
    let authentication = config.authentication(&material);
    let restored_frontiers = tickwire_app::failover::validate_start(&config)?;
    let (mut replication, mut services) = if let Some(replica) = &config.replication {
        let (driver, services) =
            tickwire_app::replication::Driver::open(replica, &config, &baseline)
                .map_err(|e| format!("replication startup: {e}"))?;
        (Some(driver), services)
    } else {
        (None, files::Services::default())
    };
    let mut lifecycle = if let Some(lifecycle) = &config.lifecycle {
        if services.recovery_store.is_none() {
            if let Some(parent) = config.directories.recovery.parent() {
                std::fs::create_dir_all(parent)?;
            }
            services.recovery_store = Some(tickwire_recovery::SharedBlobStore::new(
                tickwire_recovery::FileBlobStore::open(&config.directories.recovery)?,
            ));
        }
        Some(tickwire_app::lifecycle::Inbox::open(
            lifecycle.clone(),
            &config,
            plane.clone(),
            services
                .recovery_store
                .as_ref()
                .ok_or("missing lifecycle recovery store")?
                .clone(),
        )?)
    } else {
        None
    };
    let shadow_settings = config
        .shadow_loader
        .as_ref()
        .map(|_| config.assemble(baseline.clone()))
        .transpose()?;
    let mut settings = config.assemble(baseline)?;
    if let Some((account_required, input_required)) = restored_frontiers {
        settings.mode = files::OpenMode::RecoverFrozen {
            account_required,
            input_required: Some(input_required),
        };
    }
    let mut shadows = config
        .shadow_loader
        .as_ref()
        .map(|c| tickwire_app::launch::ShadowLoader::open(c.clone(), &settings))
        .transpose()?;
    let strategy = tickwire_app::launch::build(&config.strategy, &settings, plane.clone())
        .map_err(|e| format!("strategy startup: {e}"))?;
    let market = if config.market_sources.is_empty() {
        None
    } else {
        Some(
            tickwire_app::market::Sources::connect(
                &config.market_sources,
                domain,
                config.owner_id,
                config.policy.owner_generation,
            )
            .map_err(|e| format!("market subscription startup: {e}"))?,
        )
    };
    if let Some(approved) = &config.market_transition {
        controls
            .as_mut()
            .ok_or("market transition requires trusted control inbox")?
            .configure_market(approved.clone(), market.clone())?;
    }
    let networks = config.network.resolve(config.tuning.connect_timeout_ns)?;
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
    )
    .map_err(|e| format!("reconciliation startup: {e}"))?
    .with_execution_network(networks.websocket.as_ref().unwrap_or(&networks.rest));
    let mut node = files::open_with_services::<
        P,
        _,
        _,
        _,
        PUBLIC_BOOK_LEVELS,
        PUBLIC_DEPTH_UPDATES,
        PUBLIC_BUFFERED_EVENTS,
    >(
        settings,
        networks,
        credentials,
        authentication,
        endpoint,
        policy,
        strategy,
        NoPublisher,
        Box::new(plane.observer()),
        services,
    )
    .map_err(|e| format!("account runtime startup: {e}"))?;
    if let Some(market) = market {
        node.set_market_source(Box::new(market))?;
    }
    // Complete durable input replay before accepting a fresh private-session input.
    while node.replay_pending(1024)? != 0 {}
    if let Some(replica) = &mut replication {
        let now = node.local_time()?.monotonic;
        node.with_account_resources(|rest, _, _| replica.start(rest, now))??;
    }
    let started = Instant::now();
    eprintln!(
        "{}",
        serde_json::json!({"event":"started","recover":config.recover})
    );
    node.run_with_maintenance(
        |node, registry| {
            if let Some(replica)=&mut replication {
                let now=node.local_time()?.monotonic;
                node.with_account_resources(|rest,_,_|replica.drive(rest,now))?
                    .map_err(|error|{
                        eprintln!("{}",serde_json::json!({"event":"replication_error","error":error.to_string()}));
                        tickwire_runtime::node::Error::Strategy
                    })?;
            }
            if let Some(control) = &mut controls {
                node.with_strategy_and_account_resources(|strategy,_,_,endpoint| {
                    control.poll(endpoint)?;
                    control.poll_market(endpoint,strategy.as_mut())
                })?
                    .map_err(|error| {
                        eprintln!(
                            "{}",
                            serde_json::json!({"event":"control_error","error":error.to_string()})
                        );
                        tickwire_runtime::node::Error::Strategy
                    })?;
            }
            if let (Some(loader),Some(settings))=(&mut shadows,&shadow_settings) {
                loader.poll(node.strategy_mut().as_mut(),settings,plane.clone()).map_err(|error|{
                    eprintln!("{}",serde_json::json!({"event":"shadow_loader_error","error":error.to_string()}));
                    tickwire_runtime::node::Error::Strategy
                })?;
            }
            if let Some(lifecycle)=&mut lifecycle {
                let now=node.local_time()?.monotonic;
                node.with_strategy_and_account_resources(|strategy,rest,_,_| {
                    rest.with_gateway(|gateway|lifecycle.poll(strategy,gateway,now))
                })?.map_err(|error|{
                    eprintln!("{}",serde_json::json!({"event":"lifecycle_error","error":error.to_string()}));
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
    if node.strategy().failed() {
        return Err("strategy failure stopped the execution runtime".into());
    }
    eprintln!(
        "{}",
        serde_json::json!({"event":"stopped","ledger_sequence":node.account().ledger_sequence(),"account_frozen":node.account().frozen()})
    );
    Ok(())
}
