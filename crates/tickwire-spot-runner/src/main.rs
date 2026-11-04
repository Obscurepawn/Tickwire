//! Runnable local cash-market exchange with two real file-queue strategy producers.
mod paper_peer;
mod producer;
use std::{
    collections::BTreeMap,
    env, fs,
    path::Path,
    process::Command,
    time::{Duration, Instant},
};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::{AccountGateway, AuthorityError, Gate};
use tickwire_account_journal::FileBackend;
use tickwire_app::strategy::{AppStrategy, Config, FileStrategy, MultiStrategy};
use tickwire_binance_clock::LocalTime;
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_recovery::{FileBlobStore, SharedBlobStore};
use tickwire_runtime::{
    authority::{LocalAuthority, RecoveryQueue},
    node::Health,
};
use tickwire_runtime_policy::{Config as PolicyConfig, ControlPlane, Versions};
use tickwire_spot_execution::{Credentials, Reply, RestSession, SpotEncoder, SpotVenue};
use tickwire_spot_runner::AccountRunner;
use tickwire_venue::{CanonicalCommand, CommandBody, Environment};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
fn protocol_error(e: impl std::fmt::Debug) -> Box<dyn std::error::Error> {
    format!("cash protocol {e:?}").into()
}
const LIMIT_NS: u64 = 30_000_000_000;
use paper_peer::{FIXED_PRICE, SECRET};
fn template(venue: SpotVenue, member: u64) -> Result<CanonicalCommand> {
    let mut c = CanonicalCommand::decode(include_bytes!("../examples/order-template.cbor"))
        .map_err(protocol_error)?
        .command()
        .clone();
    c.context.request.venue = venue.id();
    c.context.request.environment = Environment::Test;
    c.context.request.intent_id = member;
    c.context.request.recovery_object = member;
    c.context.request.deadline = LIMIT_NS;
    c.context.strategy.logical_strategy_id = member;
    c.context.strategy.instance_id = [member as u8; 16];
    c.context.strategy.economic_intent_id = [member as u8; 16];
    c.context.instrument.symbol = tickwire_venue::Symbol::new(if venue == SpotVenue::Bybit {
        "BTCUSDT"
    } else {
        "BTC-USDT"
    })
    .map_err(protocol_error)?;
    let CommandBody::New(ref mut o) = c.body else {
        return Err("invalid paper command template".into());
    };
    o.client_id =
        tickwire_venue::ClientId::new(format!("paper{member}")).map_err(protocol_error)?;
    o.price = Some(FIXED_PRICE.parse().map_err(protocol_error)?);
    o.quantity = tickwire_venue::Quantity::Base("0.001".parse().map_err(protocol_error)?);
    CanonicalCommand::new(c).map_err(protocol_error)
}
fn baseline(cmd: &CanonicalCommand) -> Result<CanonicalBaseline> {
    let sample = CanonicalBaseline::decode(include_bytes!("../examples/baseline-template.cbor"))
        .map_err(protocol_error)?;
    let (mut s, mut k) = (sample.snapshot().clone(), sample.catalog().clone());
    s.valid_until = LIMIT_NS;
    s.scope.venue = cmd.command().context.request.venue;
    s.scope.environment = Environment::Test;
    k.entries[0].instrument = cmd.command().context.instrument.clone();
    k.entries[0].strategy_limits = vec![
        (8, k.entries[0].max_gross_quantity),
        (9, k.entries[0].max_gross_quantity),
    ];
    CanonicalBaseline::new(s, k).map_err(protocol_error)
}
fn venue(value: &str) -> Result<SpotVenue> {
    match value {
        "bybit" => Ok(SpotVenue::Bybit),
        "okx" => Ok(SpotVenue::Okx),
        _ => Err("expected bybit or okx".into()),
    }
}
fn elapsed(origin: Instant) -> u64 {
    origin
        .elapsed()
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX)
        .saturating_add(1)
}
fn run_paper(name: &str, root: &Path) -> Result<()> {
    let venue = venue(name)?;
    // A fresh, exclusively created root prevents an old paper intent being sent again.
    fs::DirBuilder::new()
        .create(root)
        .map_err(|_| "paper requires a new state directory")?;
    let root = fs::canonicalize(root)?;
    let first = template(venue, 8)?;
    let base = baseline(&first)?;
    let c = first.command();
    let quote_key = tickwire_account::AssetKey {
        wallet_id: base.catalog().entries[0].wallet_id,
        asset_id: c.context.instrument.quote.id,
    };
    let start = Instant::now();
    let domain = ClockDomain::new([21; 16], 1).map_err(|_| "paper clock domain")?;
    let now = || LocalTime {
        instant: Instant::now(),
        monotonic: MonoTime::new(domain, elapsed(start)),
    };
    let market =
        serde_json::json!({"symbol":c.context.instrument.symbol.as_str(),"ask":60000,"bid":59999});
    fs::write(root.join("quote.json"), serde_json::to_vec(&market)?)?;
    let mut children = vec![];
    for member in [8, 9] {
        children.push(
            Command::new(env::current_exe()?)
                .arg("publish-paper")
                .arg(name)
                .arg(&root)
                .arg(member.to_string())
                .spawn()?,
        );
    }
    for mut child in children {
        if !child.wait()?.success() {
            return Err("paper strategy producer failed".into());
        }
    }
    let policy = ControlPlane::new(PolicyConfig {
        scope: base.snapshot().scope.clone(),
        product: c.context.request.product,
        executor_id: [2; 16],
        owner_generation: 1,
        writer_epoch: c.context.request.intent_exec_epoch,
        domain,
        versions: Versions {
            config_revision: c.context.request.config_revision,
            risk_policy_version: c.context.strategy.risk_policy_version,
            order_config_version: c.context.strategy.order_config_version,
            catalog_version: c.context.instrument.catalog_version,
            catalog_hash: c.context.instrument.catalog_hash,
        },
        max_strategies: 2,
        max_feeds: 1,
        marks_per_feed: 1,
    })
    .map_err(|_| "paper group configuration")?;
    let mut members: Vec<(u64, Box<dyn AppStrategy>)> = Vec::new();
    let mut permitted = BTreeMap::new();
    for member in [8, 9] {
        let file_strategy = FileStrategy::new(&Config::FileQueue {
            directory: root.join(member.to_string()),
            first_sequence: 1,
            enabled: true,
        })?;
        members.push((member, Box::new(file_strategy)));
        permitted.insert(member, template(venue, member)?.digest());
    }
    let mut group = MultiStrategy::new(members, policy)?;
    let objects = SharedBlobStore::new(FileBlobStore::open(root.join("objects"))?);
    let queue = RecoveryQueue::new(2, 1024 * 1024).map_err(|_| "paper recovery capacity")?;
    // This gate is valid only for the two exact commands against the private loopback peer
    // created below. It never accepts user-selected network addresses, credentials or roots.
    let gate = move |cmd: &CanonicalCommand, _: &tickwire_account::Admission| {
        let ctx = &cmd.command().context;
        if ctx.request.environment != Environment::Test
            || permitted.get(&ctx.strategy.logical_strategy_id) != Some(&cmd.digest())
        {
            return Err(AuthorityError::Denied);
        }
        Ok(Gate {
            now: elapsed(start),
            lease_until: LIMIT_NS,
            writer_epoch: ctx.request.intent_exec_epoch,
            strategy_generation: ctx.request.strategy_generation,
            config_revision: ctx.request.config_revision,
            owner_isolated: true,
            action_permitted: true,
            market_valid: true,
        })
    };
    let authority = LocalAuthority::new(objects.clone(), queue.clone(), gate, [2; 16]);
    let creds = if venue == SpotVenue::Bybit {
        Credentials::bybit("paper", SECRET)
    } else {
        Credentials::okx("paper", SECRET, "paper-pass")
    };
    let encoder = SpotEncoder::new(creds, Environment::Test, c.context.request.account, || {
        Ok(1_700_000_000_000 + elapsed(start) / 1_000_000)
    })
    .map_err(protocol_error)?;
    let config = tickwire_account_gateway::Config {
        journal: tickwire_account_journal::Config {
            physical: tickwire_journal::Config::new([17; 16], base.snapshot().scope.risk_domain_id),
            baseline_digest: base.digest(),
            initial_writer_epoch: c.context.request.intent_exec_epoch,
            scope: base.snapshot().scope.clone(),
        },
        writer_epoch: c.context.request.intent_exec_epoch,
        route: encoder.route(),
        max_intents: 32,
    };
    let (connection, peer) = paper_peer::start(venue, 2)?;
    let gateway = AccountGateway::create(
        FileBackend::open(root.join("account"))?,
        config,
        base,
        authority,
        encoder,
        connection,
    )?;
    let session = RestSession::new(gateway, 1).map_err(|e| format!("paper session {e:?}"))?;
    let mut runner = AccountRunner::new(session, queue, objects);
    let health = Health {
        calibrated: true,
        market_live: true,
        private_receiving: true,
        account_frozen: false,
        sender_halted: false,
    };
    let mut replies = Vec::new();
    while start.elapsed() < Duration::from_secs(10) && replies.len() < 2 {
        if let Some(completion) =
            runner.poll(&mut group, now(), tickwire_transport::IoBudget::default())?
        {
            if !matches!(completion.response, Reply::Acknowledged { .. }) {
                return Err("unexpected paper venue reply".into());
            }
            replies.push(completion.logical_strategy_id);
        }
        runner.poll_strategy(&mut group, health, now())?;
        if group.failed() {
            return Err("file strategy group failed".into());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    if replies.len() != 2 {
        return Err("paper strategy timeout".into());
    }
    let account = runner.account();
    let open = account
        .order_keys()
        .filter_map(|k| account.order(k))
        .filter(|order| !order.terminal && !order.aborted)
        .count();
    let quote = account.asset(quote_key).map_err(protocol_error)?;
    let summary = serde_json::json!({"profile":"local-paper","venue":name,"strategies":replies,"native_acknowledgements":replies.len(),"open_orders":open,"reserved_quote_atoms":quote.reserved.atoms().to_string(),"quote_exponent":c.context.instrument.quote.exponent,"account_wal_position":runner.position()});
    drop(runner);
    peer.join()
        .map_err(|_| "local paper peer panicked")?
        .map_err(|_| "local paper peer rejected native request")?;
    println!("{summary}");
    Ok(())
}
fn run() -> Result<()> {
    let args: Vec<_> = env::args().skip(1).collect();
    match args.as_slice() {
        [a] if a == "--help" || a == "-h" => {
            println!(
                "tickwire-spot-runner paper <bybit|okx> <NEW_STATE_DIR>\nStarts two real strategy queues on one file-backed account. Native orders go only to the in-process local TLS exchange."
            );
            Ok(())
        }
        [a, v, path] if a == "paper" => run_paper(v, Path::new(path)),
        [a, v, path, id] if a == "publish-paper" => {
            let id = id.parse()?;
            if !matches!(id, 8 | 9) {
                return Err("unknown paper strategy".into());
            }
            producer::publish(venue(v)?, Path::new(path), id)
        }
        _ => Err("expected paper <bybit|okx> <NEW_STATE_DIR>".into()),
    }
}
fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("spot runner: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
