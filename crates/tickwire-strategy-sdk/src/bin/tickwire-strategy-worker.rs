//! Stateless built-in strategy worker executable (`ObserveOnly` / `FixedDemo`).
#![forbid(unsafe_code)]

use std::path::Path;

use tickwire_strategy_sdk::{
    bootstrap::{Bootstrap, MAX_BOOTSTRAP_BYTES},
    worker,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("strategy worker stopped: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let mode = args.next();
    if mode.as_deref() != Some(std::ffi::OsStr::new("--bootstrap")) {
        return Err("usage: tickwire-strategy-worker --bootstrap PATH".into());
    }
    let path = args.next().ok_or("missing bootstrap path")?;
    if args.next().is_some() {
        return Err("unexpected argument".into());
    }
    let config: Bootstrap = worker::read_config(Path::new(&path), MAX_BOOTSTRAP_BYTES)?;
    worker::run_builtin(config)?;
    Ok(())
}
