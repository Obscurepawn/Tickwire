//! CLI entrypoint for the unified stateless single-venue ORS execution process (`tickwire-app`).

#![forbid(unsafe_code)]

use std::path::Path;

use tickwire_app::{OrsAppConfig, inspect_ors_config, run_unified_ors_until_signal};

fn usage() -> &'static str {
    "usage: tickwire-app <inspect --config PATH | run --config PATH>"
}

fn parse_config_arg(mut args: impl Iterator<Item = String>) -> Result<String, String> {
    let flag = args.next().ok_or_else(|| usage().to_owned())?;
    if flag != "--config" {
        return Err(usage().to_owned());
    }
    let path = args.next().ok_or_else(|| usage().to_owned())?;
    if args.next().is_some() {
        return Err(usage().to_owned());
    }
    Ok(path)
}

fn execute() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let Some(cmd) = args.next() else {
        return Err(usage().to_owned());
    };
    match cmd.as_str() {
        "inspect" => {
            let path = parse_config_arg(args)?;
            let cfg = OrsAppConfig::from_file(Path::new(&path))?;
            let summary = inspect_ors_config(&cfg)?;
            let out = serde_json::to_string_pretty(&summary)
                .map_err(|e| format!("failed to serialize inspect summary: {e}"))?;
            println!("{out}");
            Ok(())
        }
        "run" => {
            let path = parse_config_arg(args)?;
            let cfg = OrsAppConfig::from_file(Path::new(&path))?;
            let _ = run_unified_ors_until_signal(&cfg)?;
            Ok(())
        }
        _ => Err(usage().to_owned()),
    }
}

fn main() {
    if let Err(err) = execute() {
        eprintln!("tickwire-app error: {err}");
        std::process::exit(1);
    }
}
