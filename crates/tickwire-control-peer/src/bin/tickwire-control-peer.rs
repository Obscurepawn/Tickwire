use std::path::PathBuf;
use tickwire_control_peer::runner;
fn main() {
    let mut args = std::env::args().skip(1);
    let mut config = None;
    let mut turns = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => config = args.next().map(PathBuf::from),
            "--turns" => {
                turns = Some(
                    args.next()
                        .and_then(|v| v.parse::<u64>().ok())
                        .filter(|n| *n > 0)
                        .unwrap_or_else(|| usage()),
                )
            }
            _ => usage(),
        }
    }
    let path = config.unwrap_or_else(|| usage());
    if !path.is_absolute() {
        usage()
    }
    if let Err(error) = runner::load(&path).and_then(|c| runner::run(&c, turns)) {
        eprintln!("control-peer: {error:?}");
        std::process::exit(1)
    }
}
fn usage() -> ! {
    eprintln!("usage: tickwire-control-peer --config /absolute/node.json [--turns positive-count]");
    std::process::exit(2)
}
