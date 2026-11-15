//! Stateless `V2` counting strategy worker executable (`#forbid(unsafe_code)`).
#![forbid(unsafe_code)]

fn main() {
    if let Err(e) = run() {
        eprintln!("counting strategy stopped: {e}");
        std::process::exit(1);
    }
}

fn run() -> tickwire_strategy_sdk::Result<()> {
    tickwire_strategy_sdk::registered::run(
        &tickwire_strategy_example::Registration::v2(),
        std::env::args_os().skip(1),
    )
}
