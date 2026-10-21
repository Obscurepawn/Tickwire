use tickwire_strategy_sdk::{FixedDemo, ObserveOnly, bootstrap::Bootstrap, worker};
fn main() {
    if let Err(error) = run() {
        eprintln!("strategy worker stopped: {error}");
        std::process::exit(1);
    }
}
#[allow(unsafe_code)]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let mode = args.next();
    if mode.as_deref() == Some(std::ffi::OsStr::new("--replay-rollback")) {
        let path = args.next().ok_or("missing installed rollback job")?;
        if args.next().is_some() {
            return Err("unexpected argument".into());
        }
        let job = worker::read_config(std::path::Path::new(&path), 2 * 1024 * 1024)?;
        tickwire_strategy_sdk::replay::rollback::run_native(&job)?;
        return Ok(());
    }
    if mode.as_deref() == Some(std::ffi::OsStr::new("--source-replay")) {
        let path = args.next().ok_or("missing source replay job")?;
        if args.next().is_some() {
            return Err("unexpected argument".into());
        }
        let job = worker::read_config(std::path::Path::new(&path), 2 * 1024 * 1024)?;
        tickwire_strategy_sdk::replay::process::run_native(&job)?;
        return Ok(());
    }
    if mode.as_deref() == Some(std::ffi::OsStr::new("--restore-probe")) {
        let path = args.next().ok_or("missing probe path")?;
        if args.next().is_some() {
            return Err("unexpected argument".into());
        }
        let job = worker::read_config(std::path::Path::new(&path), 2 * 1024 * 1024)?;
        tickwire_strategy_sdk::preflight::run_probe(&job)?;
        return Ok(());
    }
    if mode.as_deref() != Some(std::ffi::OsStr::new("--bootstrap")) {
        return Err("usage: tickwire-strategy-worker --bootstrap PATH".into());
    }
    let path = args.next().ok_or("missing bootstrap path")?;
    if args.next().is_some() {
        return Err("unexpected argument".into());
    }
    let config: Bootstrap = worker::read_config(std::path::Path::new(&path), 2 * 1024 * 1024)?;
    if config.factory.is_some() {
        return Err("this executable has no user factory registration".into());
    }
    if let Some(job_path) = &config.preflight_job {
        let job: tickwire_strategy_sdk::preflight::Job =
            worker::read_config(job_path, 2 * 1024 * 1024)?;
        let effective = std::fs::read(&path)?;
        let (strategy, completed) =
            tickwire_strategy_sdk::preflight::start(&job, &config, &effective)?;
        // SAFETY: the same qualified supervisor contract applies after actual restoration.
        unsafe {
            worker::run_restored(config, strategy, completed)?;
        }
        return Ok(());
    }
    if config.demo_enabled {
        let path = config
            .demo_command_file
            .as_ref()
            .ok_or("demo requires a complete canonical command file")?;
        let bytes = std::fs::read(path)?;
        let command = tickwire_venue::CanonicalCommand::decode(&bytes)
            .map_err(tickwire_strategy_sdk::Error::Command)?;
        let strategy = FixedDemo::new(command, config.demo_minimum_bid_ticks, true);
        // SAFETY: this executable is launched by the qualified supervisor with its trusted configuration.
        unsafe {
            worker::run(config, strategy)?;
        }
    } else {
        // SAFETY: the same supervisor contract applies to the observe-only worker.
        unsafe {
            worker::run(config, ObserveOnly)?;
        }
    }
    Ok(())
}
