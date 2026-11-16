//! CLI entrypoint for the stateless Unified Trading Account ORS runner (`Bybit V5` and `OKX V5`).

#![forbid(unsafe_code)]

use std::path::Path;

use tickwire_spot_runner::{
    SpotRunnerConfig, SpotVenue, inspect_config, run_paper_demo, run_until_signal,
};

fn usage() -> &'static str {
    "usage: tickwire-spot-runner <inspect --config PATH | run --config PATH | paper <bybit|okx> [SUMMARY_PATH]>"
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
            let cfg = SpotRunnerConfig::from_file(Path::new(&path))?;
            let summary = inspect_config(&cfg)?;
            let out = serde_json::to_string_pretty(&summary)
                .map_err(|e| format!("failed to serialize inspect summary: {e}"))?;
            println!("{out}");
            Ok(())
        }
        "run" => {
            let path = parse_config_arg(args)?;
            let cfg = SpotRunnerConfig::from_file(Path::new(&path))?;
            run_until_signal(&cfg)
        }
        "paper" => {
            let venue_raw = args.next().ok_or_else(|| usage().to_owned())?;
            let venue = match venue_raw.as_str() {
                "bybit" => SpotVenue::Bybit,
                "okx" => SpotVenue::Okx,
                _ => {
                    return Err(format!(
                        "unknown paper venue '{venue_raw}'; expected bybit or okx"
                    ));
                }
            };
            let optional_out = args.next();
            if args.next().is_some() {
                return Err(usage().to_owned());
            }
            let summary = run_paper_demo(venue)?;
            let json = serde_json::to_string_pretty(&summary)
                .map_err(|e| format!("failed to serialize paper summary: {e}"))?;
            if let Some(out_path) = optional_out {
                let target = Path::new(&out_path);
                let file_path = if target.is_dir() || out_path.ends_with('/') {
                    std::fs::create_dir_all(target)
                        .map_err(|e| format!("failed to create {}: {e}", target.display()))?;
                    target.join("paper-summary.json")
                } else {
                    if let Some(parent) = target.parent()
                        && !parent.as_os_str().is_empty()
                    {
                        std::fs::create_dir_all(parent)
                            .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
                    }
                    target.to_path_buf()
                };
                std::fs::write(&file_path, json.as_bytes())
                    .map_err(|e| format!("failed to write {}: {e}", file_path.display()))?;
            }
            println!("{json}");
            Ok(())
        }
        _ => Err(usage().to_owned()),
    }
}

fn main() {
    if let Err(err) = execute() {
        eprintln!("tickwire-spot-runner error: {err}");
        std::process::exit(1);
    }
}
