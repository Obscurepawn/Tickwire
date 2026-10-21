fn main() {
    if let Err(e) = run() {
        eprintln!("counting v1 strategy stopped: {e}");
        std::process::exit(1);
    }
}
#[allow(unsafe_code)]
fn run() -> tickwire_strategy_sdk::Result<()> {
    // SAFETY: this binary is admitted by the same qualified Host/SHM supervisor contract.
    unsafe {
        tickwire_strategy_sdk::registered::run(
            &tickwire_strategy_example::Registration::v1(),
            std::env::args_os().skip(1),
        )
    }
}
