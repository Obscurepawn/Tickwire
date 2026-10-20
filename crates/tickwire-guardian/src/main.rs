fn main() {
    if let Err(error) = tickwire_guardian::cli::main_entry() {
        eprintln!("guardian: {error}");
        std::process::exit(1);
    }
}
