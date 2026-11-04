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
    if !matches!(
        command.to_str(),
        Some("inspect" | "run" | "replay" | "export")
    ) {
        return Err(format!("unknown command: {}", command.to_string_lossy()).into());
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
        let runtime = config.assemble(baseline.clone())?;
        let scope = &baseline.snapshot().scope;
        let mut result = serde_json::json!({"event":"configuration","account":scope.account,"risk_domain_id":scope.risk_domain_id,"account_epoch":baseline.snapshot().account_epoch,"ledger_sequence":baseline.snapshot().ledger_sequence,"instruments":baseline.catalog().entries.len(),"recover":config.recover});
        if let tickwire_app::strategy::Config::Multi { members } = &config.strategy {
            result["strategy_members"] =
                serde_json::to_value(tickwire_app::launch::group::inspect(
                    members,
                    runtime.command_binding.risk_domain_id,
                )?)?;
        }
        println!("{result}");
        return Ok(());
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
    let mut observation = if let Some(settings) = &config.observation {
        let mut clock = tickwire_binance_clock::SystemClock::new(domain)
            .map_err(|e| format!("observation clock: {e:?}"))?;
        let inbox = tickwire_app::observation::ObservationInbox::open(
            settings.clone(),
            &config.policy,
            plane.clone(),
            &mut clock,
        )?;
        Some((inbox, clock))
    } else {
        None
    };
    let credentials = config.credentials(&baseline, &material);
    let reconciliation_credentials = config.credentials(&baseline, &material);
    let authentication = config.authentication(&material);
    let settings = config.assemble(baseline)?;
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
    let mut node = files::open::<
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
    )
    .map_err(|e| format!("account runtime startup: {e}"))?;
    if let Some(market) = market {
        node.set_market_source(Box::new(market))?;
    }
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
            if let Some((inbox, clock)) = &mut observation
                && let Err(error) = inbox.poll(node.strategy_mut().as_mut(), clock) {
                eprintln!("{}",serde_json::json!({"event":"observation_provider_closed","error":error.to_string()}));
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
