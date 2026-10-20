fn main() {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let halt = args.first().is_some_and(|a| a == "halt");
    let args = if halt { &args[1..] } else { &args[..] };
    if args.len() != 2 || args[0] != "--config" {
        eprintln!("usage: tickwire-lifecycle [halt] --config /absolute/coordinator.json");
        std::process::exit(2);
    }
    if let Err(error) = tickwire_lifecycle::cli::run(std::path::Path::new(&args[1]), halt) {
        eprintln!("coordinator stopped: {error}");
        std::process::exit(1);
    }
}
